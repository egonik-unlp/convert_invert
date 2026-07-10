use crate::internals::{
    context::context_manager::{
        DownloadedFile, RedisPool, RejectReason, RejectedTrack, RetryRequest, Track, send,
    },
    database::db_pool_snapshot,
    database::manager::DatabaseManager,
    search::search_manager::JudgeSubmission,
    search::search_manager::is_audio_file,
};
use anyhow::Context;
use redis::TypedCommands;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{Semaphore, mpsc::Sender};

/// Talks to the aioslsk engine service over HTTP: start a transfer, poll its status, and mirror
/// progress into Redis. aioslsk connects OUT to reachable uploaders (server-brokered), so this
/// works from behind NAT/CGNAT where the old inbound-only client stalled at 0 bytes.
pub struct DownloadManager {
    http: reqwest::Client,
    base_url: String,
    root_location: PathBuf,
}

#[derive(Serialize)]
struct DownloadRef<'a> {
    username: &'a str,
    filename: &'a str,
}

#[derive(Deserialize)]
struct DownloadStatusBody {
    state: String,
    #[serde(default)]
    bytes: i64,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    speed: f64,
    #[serde(default)]
    reason: Option<String>,
}

impl DownloadManager {
    pub fn new(http: reqwest::Client, base_url: String, root_location: PathBuf) -> Self {
        DownloadManager {
            http,
            base_url,
            root_location,
        }
    }

    pub async fn run(
        &self,
        track: JudgeSubmission,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
        redis_pool: RedisPool,
        db_pool: crate::internals::database::DbPool,
    ) -> anyhow::Result<()> {
        if !is_audio_file(&track.query.filename) {
            let reject = RejectedTrack::new(
                track.clone(),
                RejectReason::NotMusic(track.query.filename.clone()),
            );
            send(Track::Reject(reject), &sender)
                .await
                .context("Rejection sending to chan")?;
            return Ok(());
        }
        let _permit = semaphore.acquire().await.context("acquiring semaphore")?;
        tracing::debug!(track.query.filename, "send to download");
        let outcome = self
            .download_track(track, redis_pool, db_pool)
            .await
            .context("Downloading track")?;
        send(outcome, &sender).await.context("Sending to finish")?;
        Ok(())
    }

    async fn download_track(
        &self,
        song: JudgeSubmission,
        redis_pool: RedisPool,
        db_pool: crate::internals::database::DbPool,
    ) -> anyhow::Result<Track> {
        let username = song.query.username.clone();
        let filename = song.query.filename.clone();
        let total_bytes = u64::try_from(song.query.size).unwrap_or_default();
        let _ = &self.root_location; // downloads land in the engine's shared dir (same volume)

        // Resolve the DB ids for the progress key on a blocking thread (Diesel is sync).
        let (judge_submission_id, track_db_id) = {
            let pool = db_pool.clone();
            let song = song.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<(i32, i32)> {
                let mut conn = pool.get_timeout(Duration::from_secs(15)).map_err(|err| {
                    let snapshot = db_pool_snapshot(&pool);
                    tracing::error!(
                        ?err,
                        db_pool_connections = snapshot.connections,
                        db_pool_idle_connections = snapshot.idle_connections,
                        db_pool_in_use_connections = snapshot.in_use_connections(),
                        "DB pool in download_track"
                    );
                    err
                })?;
                let js = DatabaseManager::get_judge_submission_id(&mut conn, &song)
                    .context("Getting js id in download")?;
                let track_id = DatabaseManager::get_search_item_id(&mut conn, &song.track)
                    .context("Getting track id in download")?;
                Ok((js, track_id))
            })
            .await
            .context("download db lookup thread")??
        };
        let key = format!("dl:{judge_submission_id}:progress");

        push_progress(
            &redis_pool,
            &key,
            progress(
                &song,
                "queued",
                judge_submission_id,
                track_db_id,
                0,
                total_bytes,
                0.0,
                false,
            ),
        )
        .await;

        // Kick off the transfer.
        let start_resp = self
            .http
            .post(format!("{}/download", self.base_url))
            .header("content-type", "application/json")
            .body(
                serde_json::to_string(&DownloadRef {
                    username: &username,
                    filename: &filename,
                })
                .unwrap_or_default(),
            )
            .send()
            .await;
        if let Err(err) = start_resp {
            tracing::warn!(?err, filename, "Failed to start transfer");
            return Ok(retry(&song));
        }

        let hard_deadline = Duration::from_secs(3 * 60);
        let max_queued = Duration::from_secs(90);
        let max_no_progress = Duration::from_secs(45);
        let poll_interval = Duration::from_secs(2);
        let log_every = Duration::from_secs(5);

        let started = Instant::now();
        let mut queued_since: Option<Instant> = Some(Instant::now());
        let mut last_bytes: i64 = 0;
        let mut last_progress = Instant::now();
        let mut last_redis = Instant::now();

        loop {
            tokio::time::sleep(poll_interval).await;

            if started.elapsed() > hard_deadline {
                self.abort(&username, &filename).await;
                push_progress(
                    &redis_pool,
                    &key,
                    progress(
                        &song,
                        "retrying",
                        judge_submission_id,
                        track_db_id,
                        last_bytes.max(0) as u64,
                        total_bytes,
                        0.0,
                        false,
                    ),
                )
                .await;
                tracing::warn!(filename, "Download exceeded hard deadline");
                return Ok(retry(&song));
            }

            let status = match self.status(&username, &filename).await {
                Ok(status) => status,
                Err(err) => {
                    tracing::warn!(?err, filename, "Transfer status poll failed");
                    if last_progress.elapsed() > max_no_progress {
                        self.abort(&username, &filename).await;
                        return Ok(retry(&song));
                    }
                    continue;
                }
            };

            let bytes = status.bytes.max(0);
            let total = if status.size > 0 {
                status.size as u64
            } else {
                total_bytes
            };
            match status.state.as_str() {
                "COMPLETE" => {
                    push_progress(
                        &redis_pool,
                        &key,
                        progress(
                            &song,
                            "completed",
                            judge_submission_id,
                            track_db_id,
                            total,
                            total,
                            0.0,
                            true,
                        ),
                    )
                    .await;
                    tracing::info!(filename, "Downloaded file");
                    return Ok(Track::File(DownloadedFile {
                        filename: song.query.filename,
                        track: song.track,
                    }));
                }
                "FAILED" | "ABORTED" => {
                    tracing::warn!(filename, reason = ?status.reason, "Transfer failed");
                    push_progress(
                        &redis_pool,
                        &key,
                        progress(
                            &song,
                            "retrying",
                            judge_submission_id,
                            track_db_id,
                            bytes as u64,
                            total,
                            0.0,
                            false,
                        ),
                    )
                    .await;
                    return Ok(retry(&song));
                }
                "DOWNLOADING" | "INCOMPLETE" => {
                    queued_since = None;
                    if bytes > last_bytes {
                        last_bytes = bytes;
                        last_progress = Instant::now();
                    } else if last_progress.elapsed() > max_no_progress {
                        self.abort(&username, &filename).await;
                        push_progress(
                            &redis_pool,
                            &key,
                            progress(
                                &song,
                                "retrying",
                                judge_submission_id,
                                track_db_id,
                                bytes as u64,
                                total,
                                0.0,
                                false,
                            ),
                        )
                        .await;
                        tracing::warn!(filename, "Download stalled (no progress)");
                        return Ok(retry(&song));
                    }
                    if last_redis.elapsed() > log_every {
                        push_progress(
                            &redis_pool,
                            &key,
                            progress(
                                &song,
                                "in_progress",
                                judge_submission_id,
                                track_db_id,
                                bytes as u64,
                                total,
                                status.speed,
                                false,
                            ),
                        )
                        .await;
                        last_redis = Instant::now();
                    }
                }
                // QUEUED / INITIALIZING / VIRGIN / UNSET / PAUSED
                _ => {
                    let waited = queued_since.get_or_insert_with(Instant::now);
                    if waited.elapsed() > max_queued {
                        self.abort(&username, &filename).await;
                        push_progress(
                            &redis_pool,
                            &key,
                            progress(
                                &song,
                                "retrying",
                                judge_submission_id,
                                track_db_id,
                                bytes as u64,
                                total,
                                0.0,
                                false,
                            ),
                        )
                        .await;
                        tracing::warn!(filename, "Download stuck queued");
                        return Ok(retry(&song));
                    }
                    if last_redis.elapsed() > log_every {
                        push_progress(
                            &redis_pool,
                            &key,
                            progress(
                                &song,
                                "queued",
                                judge_submission_id,
                                track_db_id,
                                bytes as u64,
                                total,
                                0.0,
                                false,
                            ),
                        )
                        .await;
                        last_redis = Instant::now();
                    }
                }
            }
        }
    }

    async fn status(&self, username: &str, filename: &str) -> anyhow::Result<DownloadStatusBody> {
        let response = self
            .http
            .post(format!("{}/status", self.base_url))
            .header("content-type", "application/json")
            .body(serde_json::to_string(&DownloadRef { username, filename }).unwrap_or_default())
            .send()
            .await
            .context("status request")?;
        let text = response.text().await.context("status body")?;
        serde_json::from_str::<DownloadStatusBody>(&text).context("parse status")
    }

    async fn abort(&self, username: &str, filename: &str) {
        let _ = self
            .http
            .post(format!("{}/abort", self.base_url))
            .header("content-type", "application/json")
            .body(serde_json::to_string(&DownloadRef { username, filename }).unwrap_or_default())
            .send()
            .await;
    }
}

fn retry(song: &JudgeSubmission) -> Track {
    Track::Retry(RetryRequest {
        request: song.clone(),
        retry_attempts: 0,
        failed_download_result: song.query.clone(),
    })
}

struct ProgressUpdate {
    status: &'static str,
    track_db_id: i32,
    judge_submission_id: i32,
    filename: String,
    username: String,
    bytes_downloaded: u64,
    total_bytes: u64,
    speed_bytes_per_sec: f64,
    completed: bool,
}

#[allow(clippy::too_many_arguments)]
fn progress(
    song: &JudgeSubmission,
    status: &'static str,
    judge_submission_id: i32,
    track_db_id: i32,
    bytes_downloaded: u64,
    total_bytes: u64,
    speed_bytes_per_sec: f64,
    completed: bool,
) -> ProgressUpdate {
    ProgressUpdate {
        status,
        track_db_id,
        judge_submission_id,
        filename: song.query.filename.clone(),
        username: song.query.username.clone(),
        bytes_downloaded,
        total_bytes,
        speed_bytes_per_sec,
        completed,
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn push_progress(redis_pool: &RedisPool, key: &str, update: ProgressUpdate) {
    let redis_pool = redis_pool.clone();
    let key = key.to_string();
    let result =
        tokio::task::spawn_blocking(move || write_progress(&redis_pool, &key, update)).await;
    if let Ok(Err(err)) = result {
        tracing::debug!(?err, "Redis progress write failed");
    }
}

fn write_progress(redis_pool: &RedisPool, key: &str, update: ProgressUpdate) -> anyhow::Result<()> {
    let mut redis_con = redis_pool
        .get_timeout(Duration::from_secs(5))
        .context("Redis pool in download progress write")?;
    let values = [
        ("status".to_string(), update.status.to_string()),
        ("track_db_id".to_string(), update.track_db_id.to_string()),
        (
            "judge_submission_id".to_string(),
            update.judge_submission_id.to_string(),
        ),
        ("filename".to_string(), update.filename),
        ("username".to_string(), update.username),
        (
            "bytes_downloaded".to_string(),
            update.bytes_downloaded.to_string(),
        ),
        ("total_bytes".to_string(), update.total_bytes.to_string()),
        (
            "speed_bytes_per_sec".to_string(),
            update.speed_bytes_per_sec.to_string(),
        ),
        ("completed".to_string(), update.completed.to_string()),
        ("updated_at".to_string(), unix_timestamp_secs().to_string()),
    ];
    redis_con
        .hset_multiple::<String, String, _>(key.to_string(), &values)
        .context("Write Redis download progress")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::internals::search::search_manager::is_audio_file;

    #[test]
    fn audio_detection_accepts_supported_extensions_case_insensitively() {
        assert!(is_audio_file("song.MP3"));
        assert!(is_audio_file("song.flac"));
        assert!(is_audio_file("song.AIFF"));
        assert!(is_audio_file("song.aac"));
        assert!(is_audio_file("song.m4a"));
        assert!(is_audio_file("song.opus"));
    }

    #[test]
    fn audio_detection_rejects_unsupported_extensions() {
        assert!(!is_audio_file("song.txt"));
        assert!(!is_audio_file("song.mp3.exe"));
    }
}
