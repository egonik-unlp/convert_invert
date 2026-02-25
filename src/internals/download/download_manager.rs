use crate::internals::{
    context::context_manager::{
        DownloadedFile, RejectReason, RejectedTrack, RetryRequest, Track, send,
    },
    database::{establish_connection, manager::DatabaseManager},
    search::search_manager::JudgeSubmission,
};
use anyhow::Context;
use redis::TypedCommands;
use soulseek_rs::{Client, DownloadStatus};
use std::time::{Duration, Instant};
use std::{path::PathBuf, str::FromStr, sync::Arc};
use tokio::{
    sync::{Semaphore, mpsc::Sender},
    task::JoinHandle,
};

fn is_audio_file(filename: String) -> bool {
    let lc = filename.to_lowercase();
    lc.ends_with(".mp3") || lc.ends_with(".flac") || lc.ends_with(".aiff") || lc.ends_with(".aac")
}

const MAX_DOWNLOAD_TIMEOUTS: u32 = 3;
const TIMEOUT_TTL_SECS: i64 = 6 * 60 * 60;

pub struct DownloadManager {
    client: Arc<Client>,
    root_location: PathBuf,
    timeout_multiplier: f64,
}

impl DownloadManager {
    pub fn new(client: Arc<Client>, root_location: PathBuf, timeout_multiplier: f64) -> Self {
        DownloadManager {
            client,
            root_location,
            timeout_multiplier,
        }
    }
    pub async fn run(
        &self,
        track: JudgeSubmission,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
        mut redis_client: redis::Client,
    ) -> anyhow::Result<()> {
        let client = Arc::clone(&self.client);
        let download_location = self.root_location.clone();
        let id = format!("{}", track.track.track_id);
        let is_completed = redis_client.sismember("dl:completed", &id).unwrap_or(false);
        if is_audio_file(track.query.filename.clone()) && !is_completed {
            let claimed: usize = redis_client.sadd("dl:inflight", &id).unwrap_or(0);
            if claimed == 0 {
                let reject = RejectedTrack::new(track.clone(), RejectReason::AlreadyDownloaded);
                send(Track::Reject(reject), &sender)
                    .await
                    .context("Rejection sending to chan")?;
                tracing::info!(track.query.filename, "Skipped: already downloading");
                return Ok(());
            }
            tracing::info!(
                available = semaphore.available_permits(),
                "Waiting for download permit"
            );
            let permit = semaphore.acquire().await.context("acquiring semaphore")?;
            tracing::info!(
                available = semaphore.available_permits(),
                "Acquired download permit"
            );
            tracing::info!(track.query.filename, "send to download");
            let track = match download_track(
                track,
                download_location.clone(),
                client,
                redis_client.clone(),
                self.timeout_multiplier,
            )
            .await
            {
                Ok(track) => track,
                Err(err) => {
                    let _ = redis_client.srem("dl:inflight", &id);
                    drop(permit);
                    tracing::info!(
                        available = semaphore.available_permits(),
                        "Released download permit (error)"
                    );
                    return Err(err).context("Downloading track");
                }
            };
            send(track, &sender).await.context("Sending to finish")?;
            drop(permit);
            tracing::info!(
                available = semaphore.available_permits(),
                "Released download permit"
            );
        } else {
            let reason = if is_audio_file(track.query.filename.clone()) {
                RejectReason::AlreadyDownloaded
            } else {
                RejectReason::NotMusic(track.query.filename.clone())
            };
            let reject = RejectedTrack::new(track.clone(), reason);
            send(Track::Reject(reject), &sender)
                .await
                .context("Rejection sending to chan")?;
            tracing::info!(
                track.query.filename,
                "Rejected non song & already downloaded file",
            );
        }
        Ok(())
    }
}

#[tracing::instrument(name = "DownloadManager::download_track", skip(song, path, client ), fields(
    id = song.track.track_id,
    song_name = song.query.filename,
    user_name = song.query.username,
))]
async fn download_track(
    song: JudgeSubmission,
    path: PathBuf,
    client: Arc<Client>,
    redis_client: redis::Client,
    timeout_multiplier: f64,
) -> anyhow::Result<Track> {
    let song_path = PathBuf::from_str(&song.query.filename).context("Can't parse filename")?;
    let path = path.join(song_path.file_name().context("Cannot create file")?);
    let path_str = path.as_path().to_str().context("Non valid path")?;
    let rec = client.download(
        song.query.filename.clone(),
        song.query.username.clone(),
        song.query.size as u64,
        path_str.to_string(),
    )?;

    let started = Instant::now();
    let mut queued_since: Option<Instant> = None;

    let mut last_progress = Instant::now();
    let mut last_bytes: u64 = 0;
    let mut last_log = Instant::now();

    let hard_deadline = Duration::from_secs(3 * 60);
    let max_queued = Duration::from_secs(60);
    let max_no_progress = Duration::from_secs(20);
    let log_every = Duration::from_secs(10);
    let download_handle: JoinHandle<anyhow::Result<Track>> =
        tokio::task::spawn_blocking(move || {
            let connection = &mut establish_connection();
            let mut redis_con = redis_client.get_connection().unwrap();
            let track_id = DatabaseManager::get_judge_submission_id(connection, &song)
                .context("Getting js id in download")?;
            let key = format!("dl:{track_id}:progress");
            let id = format!("{}", song.track.track_id);
            let timeout_key = format!("dl:{id}:timeouts");
            let note_timeout = |redis_con: &mut redis::Connection| -> bool {
                let count: i64 = redis_con
                    .incr(timeout_key.as_str(), 1)
                    .unwrap_or(0)
                    .try_into()
                    .unwrap_or(i64::MAX);
                let _ = redis_con.expire(timeout_key.as_str(), TIMEOUT_TTL_SECS);
                count < i64::from(MAX_DOWNLOAD_TIMEOUTS)
            };
            let track = loop {
                if started.elapsed() > hard_deadline {
                    let _ = redis_con.srem("dl:inflight", &id);
                    if note_timeout(&mut redis_con) {
                        let retry_request = RetryRequest {
                            request: song.clone(),
                            retry_attempts: 0,
                            failed_download_result: song.query.clone(),
                        };
                        break Track::Retry(retry_request);
                    } else {
                        tracing::warn!(%id, "Giving up after repeated timeouts");
                        let reject = RejectedTrack::new(
                            song.clone(),
                            RejectReason::AbandonedAttemptingSearch,
                        );
                        break Track::Reject(reject);
                    }
                }
                let status = rec.recv_timeout(Duration::from_secs(60));
                match status {
                    Ok(DownloadStatus::Queued) => {
                        let qs = queued_since.get_or_insert(Instant::now());
                        if qs.elapsed() > max_queued {
                            let _ = redis_con.srem("dl:inflight", &id);
                            if note_timeout(&mut redis_con) {
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                break Track::Retry(retry_request);
                            } else {
                                tracing::warn!(%id, "Giving up after repeated timeouts");
                                let reject = RejectedTrack::new(
                                    song.clone(),
                                    RejectReason::AbandonedAttemptingSearch,
                                );
                                break Track::Reject(reject);
                            }
                        }
                        if last_log.elapsed() > log_every {
                            tracing::info!("Still queued: {}", song.query.filename);
                            last_log = Instant::now();
                        }
                        continue;
                    }
                    Ok(DownloadStatus::InProgress {
                        bytes_downloaded,
                        total_bytes,
                        speed_bytes_per_sec,
                    }) => {
                        queued_since = None;
                        if bytes_downloaded > last_bytes {
                            last_bytes = bytes_downloaded;
                            last_progress = Instant::now();
                        } else if last_progress.elapsed() > max_no_progress {
                            let _ = redis_con.srem("dl:inflight", &id);
                            if note_timeout(&mut redis_con) {
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                break Track::Retry(retry_request);
                            } else {
                                tracing::warn!(%id, "Giving up after repeated timeouts");
                                let reject = RejectedTrack::new(
                                    song.clone(),
                                    RejectReason::AbandonedAttemptingSearch,
                                );
                                break Track::Reject(reject);
                            }
                        }
                        if last_log.elapsed() > log_every {
                            tracing::info!(
                                "Downloaded {} of {} at {} B/s for {}",
                                bytes_downloaded,
                                total_bytes,
                                speed_bytes_per_sec,
                                song.query.filename
                            );
                            last_log = Instant::now();
                        }

                        let values = [
                            (
                                "bytes_downloaded".to_string(),
                                format!("{bytes_downloaded}"),
                            ),
                            ("total_bytes".to_string(), format!("{total_bytes}")),
                            (
                                "speed_bytes_per_sec".to_string(),
                                format!("{speed_bytes_per_sec}"),
                            ),
                        ];
                        redis_con
                            .hset_multiple::<String, String, _>(key.clone(), &values)
                            .unwrap();
                        // update redis (idealmente también rate-limited)
                        continue;
                    }
                    Ok(DownloadStatus::Completed) => {
                        redis_con
                            .hset(key.clone(), "completed".to_string(), format!("{}", true))
                            .unwrap();
                        let _ = redis_con.srem("dl:inflight", &id);
                        let _ = redis_con.sadd("dl:completed", &id);
                        break Track::File(DownloadedFile {
                            filename: song.query.filename,
                        });
                    }
                    Ok(DownloadStatus::Failed | DownloadStatus::TimedOut) => {
                        tracing::error!(?song, "Error descargando, se salio del loop");
                        let _ = redis_con.srem("dl:inflight", &id);
                        if note_timeout(&mut redis_con) {
                            break Track::Retry(RetryRequest {
                                request: song.clone(),
                                retry_attempts: 0,
                                failed_download_result: song.query,
                            });
                        } else {
                            tracing::warn!(%id, "Giving up after repeated timeouts");
                            let reject = RejectedTrack::new(
                                song.clone(),
                                RejectReason::AbandonedAttemptingSearch,
                            );
                            break Track::Reject(reject);
                        }
                    }
                    Err(retry_or_tout) => {
                        tracing::error!(?retry_or_tout, "Error downloadning");
                        // si no recibís eventos, tratá esto como “posible stall”
                        if last_progress.elapsed() > max_no_progress {
                            let _ = redis_con.srem("dl:inflight", &id);
                            if note_timeout(&mut redis_con) {
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                break Track::Retry(retry_request);
                            } else {
                                tracing::warn!(%id, "Giving up after repeated timeouts");
                                let reject = RejectedTrack::new(
                                    song.clone(),
                                    RejectReason::AbandonedAttemptingSearch,
                                );
                                break Track::Reject(reject);
                            }
                        }
                        continue;
                    }
                }
            };
            Ok(track)
        });
    tracing::info!("EXIT OUT OF DOWNLOAD CLOSED LOOP IMPORTANTE IMPORTANTE");
    println!("EXITTTTTTTTTT");
    let result = download_handle
        .await
        .context("Download thread exiting")?
        .context("Inner")?;
    Ok(result)
}
