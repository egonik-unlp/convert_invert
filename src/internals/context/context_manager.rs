use crate::internals::database::manager::DatabaseManager;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use soulseek_rs::{Client, ClientSettings};
use std::{
    collections::HashSet, future::Future, panic::AssertUnwindSafe, path::PathBuf, sync::Arc,
};
use tokio::sync::{
    Semaphore,
    mpsc::{self, Sender},
};
use tokio::task::JoinSet;
use tracing::instrument;

use anyhow::Context;

use crate::internals::{
    download::download_manager::DownloadManager,
    judge::{judge_manager::JudgeManager, judges::levenshtein::Levenshtein},
    query::query_manager::QueryManager,
    search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem, SearchManager},
    utils::config::config_manager::Config,
};

/// Metadata for a successfully downloaded file.
#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadedFile {
    pub filename: String,
    pub track: SearchItem,
}

struct ManagedTaskResult {
    pub label: &'static str,
    pub error: Option<String>,
}

struct RunCycleShared<'a> {
    managers: &'a Arc<Managers>,
    sender: &'a Arc<Sender<Track>>,
    state: &'a Arc<tokio::sync::RwLock<HashSet<SearchItem>>>,
    search_semaphore: &'a Arc<Semaphore>,
    download_semaphore: &'a Arc<Semaphore>,
}

/// A request to retry a failed search or download.
#[derive(Debug)]
pub struct RetryRequest {
    pub request: JudgeSubmission,
    pub retry_attempts: u8,
    pub failed_download_result: DownloadableFile,
}

/// The various stages and events in a track's lifecycle.
#[derive(Debug)]
pub enum Track {
    /// A new search query to be performed.
    Query(SearchItem),
    /// A candidate submission found for a track.
    Result(JudgeSubmission),
    /// A candidate that has been judged and is ready for download.
    Downloadable(JudgeSubmission),
    /// A file that has been successfully downloaded.
    File(DownloadedFile),
    /// A request to retry a failed operation.
    Retry(RetryRequest),
    /// A track that has been rejected for a specific reason.
    Reject(RejectedTrack),
}

/// Metadata for a track that was rejected.
#[derive(Debug, Serialize, Deserialize)]
pub struct RejectedTrack {
    track: JudgeSubmission,
    reason: RejectReason,
}

/// Reasons why a track candidate might be rejected.
#[derive(Debug, Serialize, Deserialize)]
pub enum RejectReason {
    /// The track has already been downloaded successfully.
    AlreadyDownloaded,
    /// The candidate's score was below the required threshold.
    LowScore(f32),
    /// The candidate was identified as non-music or invalid.
    NotMusic(String),
    /// The peer providing the file is banned.
    Banned(String),
    /// All search attempts were exhausted without finding a suitable candidate.
    AbandonedAttemptingSearch,
}

impl RejectedTrack {
    pub fn new(track: JudgeSubmission, reason: RejectReason) -> Self {
        Self { track, reason }
    }

    pub fn parts(&self) -> (&JudgeSubmission, &RejectReason) {
        (&self.track, &self.reason)
    }
}

/// Sends a `Track` event to the provided channel.
pub async fn send(message: Track, chan: &Sender<Track>) -> anyhow::Result<()> {
    chan.send(message).await.context("Send to channel")?;
    Ok(())
}

pub type RedisPool = diesel::r2d2::Pool<redis::Client>;

/// Tuning parameters for a worker's execution.
#[derive(Debug, Clone, Copy)]
pub struct WorkerTuning {
    /// Max in-flight search requests against Soulseek. Soulseek is rate-sensitive;
    /// raise carefully.
    pub search_concurrency: usize,
    /// Max in-flight downloads. Keep this below the host's network and file
    /// descriptor budget.
    pub download_concurrency: usize,
    /// Capacity of the work-distribution channel.
    pub queue_capacity: usize,
}

impl WorkerTuning {
    /// Loads tuning parameters from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            search_concurrency: env_usize("SEARCH_CONCURRENCY", 4),
            download_concurrency: env_usize("DOWNLOAD_CONCURRENCY", 7),
            queue_capacity: env_usize("QUEUE_CAPACITY", 20000),
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// The central container for all service managers and shared state.
pub struct Managers {
    /// The shared Soulseek client.
    pub client: Arc<Client>,
    /// Manager for file downloads.
    pub download_manager: DownloadManager,
    /// Manager for Soulseek searches.
    pub search_manager: SearchManager,
    /// Manager for Spotify playlist queries.
    pub query_manager: QueryManager,
    /// Manager for candidate judging.
    pub judge_manager: JudgeManager,
    /// The PostgreSQL connection pool.
    pub db_pool: crate::internals::database::DbPool,
    /// The Redis connection pool.
    pub redis_pool: RedisPool,
}

impl Managers {
    /// Creates a new `Managers` instance with the provided configuration.
    pub fn new(
        score: Option<f32>,
        path: PathBuf,
        config: Config,
        db_pool: crate::internals::database::DbPool,
        redis_pool: RedisPool,
    ) -> Self {
        let client_settings = ClientSettings {
            username: config.user_name,
            password: config.user_password,
            listen_port: config.listen_port,
            ..Default::default()
        };
        let mut client = Client::with_settings(client_settings);
        client.connect();
        let client = Arc::new(client);
        let download_manager = DownloadManager::new(client.clone(), path);
        let search_manager = SearchManager::new(client.clone());
        let judge_threshold =
            score.unwrap_or(crate::internals::judge::judge_manager::JUDGE_THRESHOLD);
        let lev_judge = Levenshtein::new(judge_threshold);
        let judge_manager = JudgeManager::new(Box::new(lev_judge), judge_threshold);
        let query_manager = QueryManager::new_with_timeout(
            config.playlist_id,
            config.client_id,
            config.client_secret,
            config.search_timeout_secs,
        );
        Managers {
            client,
            download_manager,
            search_manager,
            judge_manager,
            query_manager,
            db_pool,
            redis_pool,
        }
    }

    /// Runs a single chunk of tracks through the search-judge-download pipeline.
    ///
    /// This method orchestrates the task lifecycle using a `JoinSet` and an internal
    /// message channel. It ensures that all spawned tasks are completed before returning.
    #[instrument(name = "run-cyle", skip(self, tracks))]
    pub async fn run_cycle(self, tracks: impl IntoIterator<Item = Track>) -> anyhow::Result<()> {
        let managers = Arc::new(self);
        let tuning = WorkerTuning::from_env();
        let mut conn = managers
            .db_pool
            .get()
            .context("Acquiring DB connection from pool")?;
        let mut database_manager = DatabaseManager::new(&mut conn);
        let (sender, mut receiver) = mpsc::channel(tuning.queue_capacity);

        managers.client.login().context("Could not connect")?;

        let sender = Arc::new(sender);
        let state = Arc::new(tokio::sync::RwLock::new(HashSet::new()));
        let search_semaphore = Arc::new(Semaphore::new(tuning.search_concurrency));
        let download_semaphore = Arc::new(Semaphore::new(tuning.download_concurrency));
        tracing::info!(
            search_permits = search_semaphore.available_permits(),
            download_permits = download_semaphore.available_permits(),
            "Started run cycle",
        );
        for track in tracks {
            sender.send(track).await.context("injecting tracks")?;
        }
        let mut tasks = JoinSet::new();
        let mut first_task_error: Option<String> = None;

        while !receiver.is_empty() || !tasks.is_empty() {
            tokio::select! {
                maybe_track = receiver.recv(), if !receiver.is_empty() => {
                    let Some(track) = maybe_track else {
                        break;
                    };
                    tracing::debug!(?track, "Incoming package");
                    if first_task_error.is_some() {
                        tracing::debug!(?track, "Dropping queued work after task failure");
                        continue;
                    }
                    let shared = RunCycleShared {
                        managers: &managers,
                        sender: &sender,
                        state: &state,
                        search_semaphore: &search_semaphore,
                        download_semaphore: &download_semaphore,
                    };
                    process_track(track, shared, &mut database_manager, &mut tasks).await?;
                }
                maybe_result = tasks.join_next(), if !tasks.is_empty() => {
                    let task_result = match maybe_result {
                        Some(Ok(result)) => result,
                        Some(Err(err)) => ManagedTaskResult {
                            label: "unknown",
                            error: Some(format!("managed task join failed: {err}")),
                        },
                        None => continue,
                    };
                    if let Some(error) = task_result.error {
                        let message = format!("{} task failed: {error}", task_result.label);
                        tracing::error!(task_label = task_result.label, %error, "Managed task failed");
                        first_task_error.get_or_insert(message);
                    }
                }
            }
        }
        drop(sender);
        if let Some(error) = first_task_error {
            anyhow::bail!(error);
        }
        tracing::info!("Run cycle finished");
        Ok(())
    }
}

async fn process_track(
    track: Track,
    shared: RunCycleShared<'_>,
    database_manager: &mut DatabaseManager<'_>,
    tasks: &mut JoinSet<ManagedTaskResult>,
) -> anyhow::Result<()> {
    let RunCycleShared {
        managers,
        sender,
        state,
        search_semaphore,
        download_semaphore,
    } = shared;

    database_manager
        .load_item_to_database(&track)
        .context("Load into database")?;
    match track {
        Track::Query(search_item) => {
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            let semaphore = search_semaphore.clone();
            tracing::debug!(?search_item, "Scheduling search");
            spawn_managed(tasks, "search", async move {
                managers
                    .search_manager
                    .run(
                        search_item,
                        3,
                        managers.query_manager_search_timeout(),
                        semaphore,
                        sender,
                    )
                    .await
                    .context("returning track")?;
                Ok(())
            });
        }
        Track::Result(judge_submission) => {
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            spawn_managed(tasks, "judge", async move {
                tracing::debug!(?judge_submission, "Scheduling judge");
                managers
                    .judge_manager
                    .run(judge_submission, sender)
                    .await
                    .context("running judge")
            });
        }
        Track::Downloadable(judge_submission) => {
            if database_manager
                .is_search_item_downloaded(&judge_submission.track)
                .context("Check existing downloaded track")?
            {
                let reject =
                    RejectedTrack::new(judge_submission.clone(), RejectReason::AlreadyDownloaded);
                send(Track::Reject(reject), sender)
                    .await
                    .context("sending already downloaded rejection")?;
                return Ok(());
            }
            let semaphore = download_semaphore.clone();
            let managers = Arc::clone(managers);
            tracing::debug!(?judge_submission, "Scheduling download");
            let judge_sub = judge_submission.clone();
            let sender = Arc::clone(sender);

            let mut state_guard = state.write().await;
            if state_guard.insert(judge_submission.track.clone()) {
                drop(state_guard);

                let managers_clone = Arc::clone(&managers);
                spawn_managed(tasks, "download", async move {
                    managers_clone
                        .download_manager
                        .run(
                            judge_sub,
                            semaphore,
                            sender,
                            managers_clone.redis_pool.clone(),
                            managers_clone.db_pool.clone(),
                        )
                        .await
                        .context("Downloading")?;
                    Ok(())
                });
            } else {
                drop(state_guard);
                let reject =
                    RejectedTrack::new(judge_submission.clone(), RejectReason::AlreadyDownloaded);
                send(Track::Reject(reject), &sender)
                    .await
                    .context("sending rejected_tracks")?;
            }
        }
        Track::File(downloaded_file) => {
            tracing::info!(?downloaded_file, "Downloaded file");
        }
        Track::Retry(mut retry_request) => {
            state.write().await.remove(&retry_request.request.track);
            if retry_request.retry_attempts >= 1 {
                let reject = RejectedTrack::new(
                    retry_request.request,
                    RejectReason::AbandonedAttemptingSearch,
                );
                send(Track::Reject(reject), sender)
                    .await
                    .context("rejecting")?;
                return Ok(());
            }
            retry_request.retry_attempts += 1;
            let managers = Arc::clone(managers);
            let semaphore = search_semaphore.clone();
            let sender = Arc::clone(sender);
            tracing::info!(?retry_request.request, "Retry requested");
            let search_item = retry_request.request.clone();
            spawn_managed(tasks, "retry_search", async move {
                managers
                    .search_manager
                    .run(
                        search_item.track,
                        3,
                        managers.query_manager_search_timeout(),
                        semaphore,
                        sender,
                    )
                    .await
                    .context("returning track")?;
                Ok(())
            });
            tracing::debug!(?retry_request, "Retry queued")
        }
        Track::Reject(rejected_track) => {
            state.write().await.remove(&rejected_track.track.track);
        }
    }
    Ok(())
}

impl Managers {
    fn query_manager_search_timeout(&self) -> u8 {
        self.query_manager.search_timeout_secs
    }
}

fn spawn_managed<F>(tasks: &mut JoinSet<ManagedTaskResult>, label: &'static str, future: F)
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tasks.spawn(async move {
        let result = AssertUnwindSafe(future).catch_unwind().await;
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(format!("{err:?}")),
            Err(_) => Some("task panicked".to_string()),
        };
        ManagedTaskResult { label, error }
    });
}

#[cfg(test)]
mod tests {
    use super::{ManagedTaskResult, spawn_managed};
    use tokio::task::JoinSet;

    async fn next_result(tasks: &mut JoinSet<ManagedTaskResult>) -> ManagedTaskResult {
        tasks
            .join_next()
            .await
            .expect("task result")
            .expect("task joined")
    }

    #[tokio::test]
    async fn managed_task_reports_success_by_label() {
        let mut tasks = JoinSet::new();

        spawn_managed(&mut tasks, "search", async { Ok(()) });

        let result = next_result(&mut tasks).await;
        assert_eq!(result.label, "search");
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn managed_task_captures_errors() {
        let mut tasks = JoinSet::new();

        spawn_managed(&mut tasks, "download", async {
            anyhow::bail!("download failed")
        });

        let result = next_result(&mut tasks).await;
        assert_eq!(result.label, "download");
        assert!(
            result
                .error
                .expect("managed task error")
                .contains("download failed")
        );
    }

    #[tokio::test]
    async fn managed_task_converts_panics_to_errors() {
        let mut tasks = JoinSet::new();

        spawn_managed(&mut tasks, "judge", async {
            panic!("judge panic");
            #[allow(unreachable_code)]
            Ok(())
        });

        let result = next_result(&mut tasks).await;
        assert_eq!(result.label, "judge");
        assert_eq!(result.error.as_deref(), Some("task panicked"));
    }
}
