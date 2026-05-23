use crate::internals::{
    context::context_manager::{
        DownloadedFile, RejectReason, RejectedTrack, RetryRequest, Track, send,
    },
    database::manager::DatabaseManager,
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
        redis_pool: crate::internals::context::context_manager::RedisPool,
        db_pool: crate::internals::database::DbPool,
    ) -> anyhow::Result<()> {
        let client = Arc::clone(&self.client);
        let download_location = self.root_location.clone();
        let id = track.track.track_id.clone();
        let is_banned = {
            let mut redis_con = redis_pool.get().context("Redis pool in run")?;
            redis_con.sismember::<_, _>("ban-list", id).unwrap_or(false)
        };
        if is_audio_file(track.query.filename.clone()) && !is_banned {
            let _permit = semaphore.acquire().await.context("acquiring semaphore")?;
            tracing::debug!(track.query.filename, "send to download");
            let track = download_track(
                track,
                download_location.clone(),
                client,
                redis_pool,
                db_pool,
            )
            .await
            .context("Downloading track")?;
            send(track, &sender).await.context("Sending to finish")?;
        } else {
            let reason = if is_banned {
                RejectReason::Banned(track.track.track_id.clone())
            } else {
                RejectReason::NotMusic(track.query.filename.clone())
            };
            let reject = RejectedTrack::new(track.clone(), reason);
            send(Track::Reject(reject), &sender)
                .await
                .context("Rejection sending to chan")?;
            tracing::debug!(
                track.query.filename,
                "Rejected non song & already downloaded file",
            );
        }
        Ok(())
    }
}

#[tracing::instrument(name = "DownloadManager::download_track", skip(song, path, client, redis_pool, db_pool ), fields(
    id = song.track.track_id,
    song_name = song.query.filename,
    user_name = song.query.username,
))]
async fn download_track(
    song: JudgeSubmission,
    path: PathBuf,
    client: Arc<Client>,
    redis_pool: crate::internals::context::context_manager::RedisPool,
    db_pool: crate::internals::database::DbPool,
) -> anyhow::Result<Track> {
    let song_path = PathBuf::from_str(&song.query.filename).context("Can't parse filename")?;
    let path = path.join(song_path.file_name().context("Cannot create file")?);
    let path_str = path.as_path().to_str().context("Non valid path")?;
    let rec = client.download(
        song.query.filename.clone(),
        song.query.username.clone(),
        u64::try_from(song.query.size).context("Download size cannot be converted to u64")?,
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
            let mut conn = db_pool
                .get_timeout(Duration::from_secs(5))
                .context("DB pool in download_track")?;
            let mut redis_con = redis_pool
                .get_timeout(Duration::from_secs(5))
                .context("Redis pool in download_track")?;
            let track_id = DatabaseManager::get_judge_submission_id(&mut conn, &song)
                .context("Getting js id in download")?;
            let key = format!("dl:{track_id}:progress");
            let track = loop {
                if started.elapsed() > hard_deadline {
                    let retry_request = RetryRequest {
                        request: song.clone(),
                        retry_attempts: 0,
                        failed_download_result: song.query.clone(),
                    };
                    break Track::Retry(retry_request);
                }
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
                            break Track::Retry(retry_request);
                        }
                        if last_log.elapsed() > log_every {
                            tracing::debug!("Still queued: {}", song.query.filename);
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
                            break Track::Retry(retry_request);
                        }
                        if last_log.elapsed() > log_every {
                            tracing::debug!(
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
                            .context("Write Redis download progress")?;
                        continue;
                    }
                    Ok(DownloadStatus::Completed) => {
                        redis_con
                            .hset(key.clone(), "completed".to_string(), format!("{}", true))
                            .context("Mark Redis download completed")?;
                        break Track::File(DownloadedFile {
                            filename: song.query.filename,
                            track: song.track,
                        });
                    }
                    Ok(DownloadStatus::Failed | DownloadStatus::TimedOut) => {
                        tracing::error!(?song, "Download failed or timed out");
                        break Track::Retry(RetryRequest {
                            request: song.clone(),
                            retry_attempts: 0,
                            failed_download_result: song.query,
                        });
                    }
                    Err(retry_or_tout) => {
                        tracing::warn!(?retry_or_tout, "Download status receive error");
                        if last_progress.elapsed() > max_no_progress {
                            let retry_request = RetryRequest {
                                request: song.clone(),
                                failed_download_result: song.clone().query,
                                retry_attempts: 0,
                            };
                            break Track::Retry(retry_request);
                        }
                        continue;
                    }
                }
            };
            Ok(track)
        });
    let result = download_handle
        .await
        .context("Download thread exiting")?
        .context("Inner")?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::is_audio_file;

    #[test]
    fn audio_detection_accepts_supported_extensions_case_insensitively() {
        assert!(is_audio_file("song.MP3".to_string()));
        assert!(is_audio_file("song.flac".to_string()));
        assert!(is_audio_file("song.AIFF".to_string()));
        assert!(is_audio_file("song.aac".to_string()));
    }

    #[test]
    fn audio_detection_rejects_unsupported_extensions() {
        assert!(!is_audio_file("song.txt".to_string()));
        assert!(!is_audio_file("song.mp3.exe".to_string()));
    }
}
