use crate::internals::{
    context::context_manager::{
        DownloadedFile, RejectReason, RejectedTrack, RetryRequest, Track, send,
    },
    database::{establish_connection, manager::DatabaseManager, schema::retry_request},
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
    lc.ends_with(".mp3") || lc.ends_with(".flac") || lc.ends_with(".aiff")
}

pub struct DownloadManager {
    client: Arc<Client>,
    root_location: PathBuf,
}

impl DownloadManager {
    pub fn new(client: Arc<Client>, root_location: PathBuf) -> Self {
        DownloadManager {
            client,
            root_location,
        }
    }
    pub async fn run(
        &self,
        track: JudgeSubmission,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
        redis_client: redis::Client,
    ) -> anyhow::Result<()> {
        let client = Arc::clone(&self.client);
        let download_location = self.root_location.clone();
        if is_audio_file(track.query.filename.clone()) {
            let _permit = semaphore.acquire().await.context("acquiring semaphore")?;
            tracing::info!(track.query.filename, "send to download");
            let track = download_track(track, download_location.clone(), client, redis_client)
                .await
                .context("Downloading track")?;
            send(track, &sender).await.context("Sending to finish")?;
        } else {
            let reject = RejectedTrack::new(
                track.clone(),
                RejectReason::NotMusic(track.query.filename.clone()),
            );
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

    let hard_deadline = Duration::from_secs(20 * 60); // 20 min total
    let max_queued = Duration::from_secs(5 * 60); // 5 min en cola
    let max_no_progress = Duration::from_secs(90); // 90s sin avanzar
    let log_every = Duration::from_secs(10); // log cada 10s
    let span = tracing::info_span!("download_thread_inner");
    let download_handle: JoinHandle<anyhow::Result<Track>> =
        tokio::task::spawn_blocking(move || {
            let connection = &mut establish_connection();
            let mut redis_con = redis_client.get_connection().unwrap();
            let track_id = DatabaseManager::get_judge_submission_id(connection, &song)
                .context("Getting js id in download")?;
            let track = span.in_scope(|| {
                loop {
                    let status = rec.recv_timeout(Duration::from_secs(60));
                    match status {
                        Ok(DownloadStatus::Queued) => {
                            let qs = queued_since.get_or_insert(Instant::now());
                            if qs.elapsed() > max_queued {
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                return Track::Retry(retry_request);
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
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                return Track::Retry(retry_request);
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

                            let key = format!("dl:{track_id}:progress");
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
                                .hset_multiple::<String, String, _>(key, &values)
                                .unwrap();
                            // update redis (idealmente también rate-limited)
                            continue;
                        }
                        Ok(DownloadStatus::Completed) => {
                            return Track::File(DownloadedFile {
                                filename: song.query.filename,
                            });
                        }
                        Ok(DownloadStatus::Failed | DownloadStatus::TimedOut) => {
                            tracing::error!(?song, "Error descargando, se salio del loop");
                            return Track::Retry(RetryRequest {
                                request: song.clone(),
                                retry_attempts: 0,
                                failed_download_result: song.query,
                            });
                        }
                        Err(retry_or_tout) => {
                            // si no recibís eventos, tratá esto como “posible stall”
                            if last_progress.elapsed() > max_no_progress {
                                let retry_request = RetryRequest {
                                    request: song.clone(),
                                    failed_download_result: song.clone().query,
                                    retry_attempts: 0,
                                };
                                return Track::Retry(retry_request);
                            }
                            continue;
                        }
                    }
                }
            });
            Ok(track)
        });
    let result = download_handle
        .await
        .context("Download thread exiting")?
        .context("Inner")?;
    Ok(result)
}
