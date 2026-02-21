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

const HARD_DEADLINE: Duration = Duration::from_secs(3 * 60);
const MAX_QUEUED: Duration = Duration::from_secs(60);
const MAX_NO_PROGRESS: Duration = Duration::from_secs(20);
const LOG_EVERY: Duration = Duration::from_secs(10);

fn is_audio_file(filename: String) -> bool {
    let lc = filename.to_lowercase();
    lc.ends_with(".mp3") || lc.ends_with(".flac") || lc.ends_with(".aiff") || lc.ends_with(".aac")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadDecision {
    Continue,
    Retry,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadEvent {
    Queued,
    InProgress { bytes_downloaded: u64 },
    Completed,
    Failed,
    TimedOut,
    RecvError,
}

#[derive(Debug, Clone, Copy)]
struct DownloadProgressTracker {
    started: Duration,
    queued_since: Option<Duration>,
    last_progress: Duration,
    last_bytes: u64,
}

impl DownloadProgressTracker {
    fn new(now: Duration) -> Self {
        Self {
            started: now,
            queued_since: None,
            last_progress: now,
            last_bytes: 0,
        }
    }

    fn on_event(&mut self, now: Duration, event: DownloadEvent) -> DownloadDecision {
        if now.saturating_sub(self.started) > HARD_DEADLINE {
            return DownloadDecision::Retry;
        }

        match event {
            DownloadEvent::Queued => {
                let queued_since = self.queued_since.get_or_insert(now);
                if now.saturating_sub(*queued_since) > MAX_QUEUED {
                    DownloadDecision::Retry
                } else {
                    DownloadDecision::Continue
                }
            }
            DownloadEvent::InProgress { bytes_downloaded } => {
                self.queued_since = None;
                if bytes_downloaded > self.last_bytes {
                    self.last_bytes = bytes_downloaded;
                    self.last_progress = now;
                    DownloadDecision::Continue
                } else if now.saturating_sub(self.last_progress) > MAX_NO_PROGRESS {
                    DownloadDecision::Retry
                } else {
                    DownloadDecision::Continue
                }
            }
            DownloadEvent::Completed => DownloadDecision::Completed,
            DownloadEvent::Failed | DownloadEvent::TimedOut => DownloadDecision::Retry,
            DownloadEvent::RecvError => {
                if now.saturating_sub(self.last_progress) > MAX_NO_PROGRESS {
                    DownloadDecision::Retry
                } else {
                    DownloadDecision::Continue
                }
            }
        }
    }
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
        mut redis_client: redis::Client,
    ) -> anyhow::Result<()> {
        let client = Arc::clone(&self.client);
        let download_location = self.root_location.clone();
        let id = format!("{}", track.track.track_id);
        let is_banned = redis_client.sismember("ban-list", id).unwrap();
        if is_audio_file(track.query.filename.clone()) && !is_banned {
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

    let started_at = Instant::now();
    let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
    let mut last_log = Instant::now();
    let download_handle: JoinHandle<anyhow::Result<Track>> =
        tokio::task::spawn_blocking(move || {
            let connection = &mut establish_connection();
            let mut redis_con = redis_client.get_connection().unwrap();
            let track_id = DatabaseManager::get_judge_submission_id(connection, &song)
                .context("Getting js id in download")?;
            let key = format!("dl:{track_id}:progress");
            let track = loop {
                let now = started_at.elapsed();
                let status = rec.recv_timeout(Duration::from_secs(60));
                match status {
                    Ok(DownloadStatus::Queued) => {
                        if matches!(tracker.on_event(now, DownloadEvent::Queued), DownloadDecision::Retry) {
                            let retry_request = RetryRequest {
                                request: song.clone(),
                                failed_download_result: song.clone().query,
                                retry_attempts: 0,
                            };
                            break Track::Retry(retry_request);
                        }
                        if last_log.elapsed() > LOG_EVERY {
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
                        if matches!(
                            tracker.on_event(
                                now,
                                DownloadEvent::InProgress { bytes_downloaded },
                            ),
                            DownloadDecision::Retry
                        ) {
                            let retry_request = RetryRequest {
                                request: song.clone(),
                                failed_download_result: song.clone().query,
                                retry_attempts: 0,
                            };
                            break Track::Retry(retry_request);
                        }
                        if last_log.elapsed() > LOG_EVERY {
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
                        let id = format!("{}", song.track.track_id);
                        redis_con.sadd("ban-list", id).unwrap();
                        if matches!(
                            tracker.on_event(now, DownloadEvent::Completed),
                            DownloadDecision::Completed
                        ) {
                            redis_con
                                .hset(key.clone(), "completed".to_string(), format!("{}", true))
                                .unwrap();
                            break Track::File(DownloadedFile {
                                filename: song.query.filename,
                                track: Some(song.track.clone()),
                            });
                        }
                        continue;
                    }
                    Ok(DownloadStatus::Failed | DownloadStatus::TimedOut) => {
                        let _ = tracker.on_event(now, DownloadEvent::Failed);
                        tracing::error!(?song, "Error descargando, se salio del loop");
                        break Track::Retry(RetryRequest {
                            request: song.clone(),
                            retry_attempts: 0,
                            failed_download_result: song.query,
                        });
                    }
                    Err(retry_or_tout) => {
                        tracing::error!(?retry_or_tout, "Error downloadning");
                        // si no recibís eventos, tratá esto como “posible stall”
                        if matches!(tracker.on_event(now, DownloadEvent::RecvError), DownloadDecision::Retry) {
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
    tracing::info!("EXIT OUT OF DOWNLOAD CLOSED LOOP IMPORTANTE IMPORTANTE");
    println!("EXITTTTTTTTTT");
    let result = download_handle
        .await
        .context("Download thread exiting")?
        .context("Inner")?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn is_audio_file_accepts_common_formats() {
        assert!(is_audio_file("song.mp3".to_string()));
        assert!(is_audio_file("song.FLAC".to_string()));
        assert!(is_audio_file("song.aiff".to_string()));
        assert!(is_audio_file("song.AAC".to_string()));
        assert!(!is_audio_file("song.wav".to_string()));
        assert!(!is_audio_file("song.txt".to_string()));
    }

    #[test]
    fn tracker_retries_on_hard_deadline() {
        let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
        let now = HARD_DEADLINE + Duration::from_secs(1);
        let decision = tracker.on_event(now, DownloadEvent::InProgress { bytes_downloaded: 1 });
        assert_eq!(decision, DownloadDecision::Retry);
    }

    #[test]
    fn tracker_retries_when_queued_too_long() {
        let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
        let decision = tracker.on_event(Duration::from_secs(0), DownloadEvent::Queued);
        assert_eq!(decision, DownloadDecision::Continue);

        let decision = tracker.on_event(MAX_QUEUED + Duration::from_secs(1), DownloadEvent::Queued);
        assert_eq!(decision, DownloadDecision::Retry);
    }

    #[test]
    fn tracker_retries_when_no_progress() {
        let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
        let decision = tracker.on_event(Duration::from_secs(1), DownloadEvent::InProgress {
            bytes_downloaded: 10,
        });
        assert_eq!(decision, DownloadDecision::Continue);

        let decision = tracker.on_event(
            Duration::from_secs(1) + MAX_NO_PROGRESS + Duration::from_secs(1),
            DownloadEvent::InProgress { bytes_downloaded: 10 },
        );
        assert_eq!(decision, DownloadDecision::Retry);
    }

    #[test]
    fn tracker_retries_on_recv_error_after_stall() {
        let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
        let decision = tracker.on_event(Duration::from_secs(1), DownloadEvent::InProgress {
            bytes_downloaded: 10,
        });
        assert_eq!(decision, DownloadDecision::Continue);

        let decision = tracker.on_event(
            Duration::from_secs(1) + MAX_NO_PROGRESS + Duration::from_secs(1),
            DownloadEvent::RecvError,
        );
        assert_eq!(decision, DownloadDecision::Retry);
    }

    #[test]
    fn tracker_allows_completion() {
        let mut tracker = DownloadProgressTracker::new(Duration::ZERO);
        let decision = tracker.on_event(Duration::from_secs(1), DownloadEvent::Completed);
        assert_eq!(decision, DownloadDecision::Completed);
    }
}
