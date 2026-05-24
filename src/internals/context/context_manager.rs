use crate::internals::database::manager::DatabaseManager;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use soulseek_rs::{Client, ClientSettings};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    future::Future,
    net::TcpListener,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{
    Semaphore,
    mpsc::{self, Sender},
};
use tokio::task::JoinSet;
use tracing::instrument;

use anyhow::Context;

use crate::internals::{
    database::db_pool_snapshot,
    download::download_manager::DownloadManager,
    judge::{judge_manager::JudgeManager, judges::levenshtein::Levenshtein},
    query::query_manager::QueryManager,
    search::search_manager::{
        DownloadableFile, JudgeSubmission, SearchExitReason, SearchItem, SearchManager,
    },
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
    state: &'a Arc<tokio::sync::RwLock<RunState>>,
    search_semaphore: &'a Arc<Semaphore>,
    download_semaphore: &'a Arc<Semaphore>,
}

#[derive(Debug, Default)]
struct RunState {
    in_progress: HashSet<SearchItem>,
    candidate_pools: HashMap<SearchItem, CandidatePool>,
    request_budgets: HashMap<SearchItem, RequestBudget>,
}

#[derive(Debug, Default)]
struct CandidatePool {
    candidates: Vec<JudgeSubmission>,
    failed: HashSet<DownloadableFile>,
    attempts: usize,
    selection_queued: bool,
}

#[derive(Debug, Default)]
struct RequestBudget {
    request_count: usize,
    search_passes: usize,
}

#[derive(Debug, Clone, Copy)]
enum RequestKind {
    Search,
    Download,
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
    /// A relaxed second-pass search query to be performed.
    SearchRetry(SearchItem),
    /// A candidate submission found for a track.
    Result(JudgeSubmission),
    /// A candidate that has been judged and is ready for download.
    Downloadable(JudgeSubmission),
    /// Internal signal to select the best accepted candidate after a short collection window.
    SelectCandidate(SearchItem),
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
    /// Number of accepted candidates retained per track.
    pub max_candidates_per_track: usize,
    /// Max candidate download attempts before falling back to a relaxed search/rejection.
    pub max_download_attempts_per_track: usize,
    /// Seconds to wait for more accepted candidates before the first download attempt.
    pub candidate_collection_secs: u64,
    /// Max original/relaxed search passes per track.
    pub max_search_passes_per_track: usize,
    /// Max total Soulseek search/download requests per track.
    pub max_requests_per_track: usize,
}

impl WorkerTuning {
    /// Loads tuning parameters from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            search_concurrency: env_usize("SEARCH_CONCURRENCY", 1),
            download_concurrency: env_usize("DOWNLOAD_CONCURRENCY", 1),
            queue_capacity: env_usize("QUEUE_CAPACITY", 20000),
            max_candidates_per_track: env_usize("MAX_CANDIDATES_PER_TRACK", 3).max(1),
            max_download_attempts_per_track: env_usize("MAX_DOWNLOAD_ATTEMPTS_PER_TRACK", 2).max(1),
            candidate_collection_secs: env_u64("CANDIDATE_COLLECTION_SECS", 20),
            max_search_passes_per_track: env_usize("MAX_SEARCH_PASSES_PER_TRACK", 2).max(1),
            max_requests_per_track: env_usize("MAX_REQUESTS_PER_TRACK", 8).max(1),
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
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
    pub search_empty_result_cutoff: usize,
}

impl Managers {
    /// Creates a new `Managers` instance with the provided configuration.
    pub fn new(
        score: Option<f32>,
        path: PathBuf,
        config: Config,
        db_pool: crate::internals::database::DbPool,
        redis_pool: RedisPool,
    ) -> anyhow::Result<Self> {
        let listen_port = config.listen_port;
        TcpListener::bind(format!("0.0.0.0:{listen_port}"))
            .with_context(|| format!("listener bind preflight failed on port {listen_port}"))?;
        let client_settings = ClientSettings {
            username: config.user_name,
            password: config.user_password,
            listen_port,
            ..Default::default()
        };
        let search_empty_result_cutoff = config.search_empty_result_cutoff;
        let mut client = Client::with_settings(client_settings);
        catch_unwind(AssertUnwindSafe(|| client.connect())).map_err(|payload| {
            anyhow::anyhow!("listener bind panic: {}", panic_payload(payload))
        })?;
        client.login().context("Could not connect")?;
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
        Ok(Managers {
            client,
            download_manager,
            search_manager,
            judge_manager,
            query_manager,
            db_pool,
            redis_pool,
            search_empty_result_cutoff,
        })
    }

    /// Runs a single chunk of tracks through the search-judge-download pipeline.
    ///
    /// This method orchestrates the task lifecycle using a `JoinSet` and an internal
    /// message channel. It ensures that all spawned tasks are completed before returning.
    #[instrument(name = "run-chunk", skip(self, tracks))]
    pub async fn run_chunk(
        self: &Arc<Self>,
        tracks: impl IntoIterator<Item = Track>,
    ) -> anyhow::Result<()> {
        let managers = Arc::clone(self);
        let tuning = WorkerTuning::from_env();
        let (sender, mut receiver) = mpsc::channel(tuning.queue_capacity);

        let sender = Arc::new(sender);
        let state = Arc::new(tokio::sync::RwLock::new(RunState::default()));
        let search_semaphore = Arc::new(Semaphore::new(tuning.search_concurrency));
        let download_semaphore = Arc::new(Semaphore::new(tuning.download_concurrency));
        let snapshot = db_pool_snapshot(&managers.db_pool);
        tracing::info!(
            search_permits = search_semaphore.available_permits(),
            download_permits = download_semaphore.available_permits(),
            max_candidates_per_track = tuning.max_candidates_per_track,
            max_download_attempts_per_track = tuning.max_download_attempts_per_track,
            candidate_collection_secs = tuning.candidate_collection_secs,
            max_search_passes_per_track = tuning.max_search_passes_per_track,
            max_requests_per_track = tuning.max_requests_per_track,
            db_pool_connections = snapshot.connections,
            db_pool_idle_connections = snapshot.idle_connections,
            db_pool_in_use_connections = snapshot.in_use_connections(),
            "Started run chunk",
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
                    process_track(track, shared, &mut tasks).await?;
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
        let snapshot = db_pool_snapshot(&managers.db_pool);
        tracing::info!(
            db_pool_connections = snapshot.connections,
            db_pool_idle_connections = snapshot.idle_connections,
            db_pool_in_use_connections = snapshot.in_use_connections(),
            "Run chunk finished",
        );
        Ok(())
    }

    pub fn shutdown(self: Arc<Self>) {
        tracing::info!(
            "Managers shutdown requested; soulseek-rs-lib 0.3.0 exposes no public disconnect/logout API",
        );
    }
}

async fn process_track(
    track: Track,
    shared: RunCycleShared<'_>,
    tasks: &mut JoinSet<ManagedTaskResult>,
) -> anyhow::Result<()> {
    let RunCycleShared {
        managers,
        sender,
        state,
        search_semaphore,
        download_semaphore,
    } = shared;

    if !matches!(track, Track::SelectCandidate(_)) {
        let mut conn = managers.db_pool.get().map_err(|err| {
            let snapshot = db_pool_snapshot(&managers.db_pool);
            tracing::error!(
                ?err,
                db_pool_connections = snapshot.connections,
                db_pool_idle_connections = snapshot.idle_connections,
                db_pool_in_use_connections = snapshot.in_use_connections(),
                "DB pool in process_track"
            );
            err
        })?;
        let mut database_manager = DatabaseManager::new(&mut conn);
        database_manager
            .load_item_to_database(&track)
            .context("Load into database")?;
    }
    match track {
        Track::Query(search_item) => {
            let tuning = WorkerTuning::from_env();
            let request_allowed = {
                let mut state_guard = state.write().await;
                spend_request(&mut state_guard, &search_item, &tuning, RequestKind::Search)
            };
            if !request_allowed {
                tracing::info!(
                    track_id = %search_item.track_id,
                    max_search_passes_per_track = tuning.max_search_passes_per_track,
                    max_requests_per_track = tuning.max_requests_per_track,
                    "Request budget exhausted before initial search",
                );
                return Ok(());
            }
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            let semaphore = search_semaphore.clone();
            tracing::debug!(?search_item, "Scheduling search");
            spawn_managed(tasks, "search", async move {
                let outcome = managers
                    .search_manager
                    .run(
                        search_item.clone(),
                        managers.search_empty_result_cutoff(),
                        managers.query_manager_search_timeout(),
                        false,
                        semaphore,
                        Arc::clone(&sender),
                    )
                    .await
                    .context("returning track")?;
                if matches!(outcome.exit_reason, SearchExitReason::NoCandidatesFound) {
                    send(Track::SearchRetry(search_item), &sender)
                        .await
                        .context("queue relaxed search")?;
                }
                Ok(())
            });
        }
        Track::SearchRetry(search_item) => {
            let tuning = WorkerTuning::from_env();
            let request_allowed = {
                let mut state_guard = state.write().await;
                spend_request(&mut state_guard, &search_item, &tuning, RequestKind::Search)
            };
            if !request_allowed {
                tracing::info!(
                    track_id = %search_item.track_id,
                    max_search_passes_per_track = tuning.max_search_passes_per_track,
                    max_requests_per_track = tuning.max_requests_per_track,
                    "Request budget exhausted before relaxed search",
                );
                return Ok(());
            }
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            let semaphore = search_semaphore.clone();
            tracing::debug!(?search_item, "Scheduling relaxed search retry");
            spawn_managed(tasks, "search_retry", async move {
                let outcome = managers
                    .search_manager
                    .run(
                        search_item.clone(),
                        managers.search_empty_result_cutoff(),
                        managers.query_manager_search_timeout(),
                        true,
                        semaphore,
                        sender,
                    )
                    .await
                    .context("returning relaxed track")?;
                if matches!(outcome.exit_reason, SearchExitReason::NoCandidatesFound) {
                    tracing::info!(?search_item, "Relaxed search returned no candidates");
                }
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
            let is_downloaded = {
                let mut conn = managers.db_pool.get().map_err(|err| {
                    let snapshot = db_pool_snapshot(&managers.db_pool);
                    tracing::error!(
                        ?err,
                        db_pool_connections = snapshot.connections,
                        db_pool_idle_connections = snapshot.idle_connections,
                        db_pool_in_use_connections = snapshot.in_use_connections(),
                        "DB pool in downloadable check"
                    );
                    err
                })?;
                let mut database_manager = DatabaseManager::new(&mut conn);
                database_manager
                    .is_search_item_downloaded(&judge_submission.track)
                    .context("Check existing downloaded track")?
            };
            if is_downloaded {
                let reject =
                    RejectedTrack::new(judge_submission.clone(), RejectReason::AlreadyDownloaded);
                send(Track::Reject(reject), sender)
                    .await
                    .context("sending already downloaded rejection")?;
                return Ok(());
            }
            let tuning = WorkerTuning::from_env();
            let mut state_guard = state.write().await;
            let pool = state_guard
                .candidate_pools
                .entry(judge_submission.track.clone())
                .or_default();
            push_candidate(
                pool,
                judge_submission.clone(),
                tuning.max_candidates_per_track,
            );
            tracing::debug!(
                track_id = %judge_submission.track.track_id,
                candidates = pool.candidates.len(),
                "Accepted candidate queued",
            );
            if !pool.selection_queued {
                pool.selection_queued = true;
                let search_item = judge_submission.track.clone();
                let sender = Arc::clone(sender);
                spawn_managed(tasks, "candidate_collection", async move {
                    tokio::time::sleep(std::time::Duration::from_secs(
                        tuning.candidate_collection_secs,
                    ))
                    .await;
                    send(Track::SelectCandidate(search_item), &sender)
                        .await
                        .context("queue candidate selection")
                });
            }
        }
        Track::SelectCandidate(search_item) => {
            let tuning = WorkerTuning::from_env();
            let selection = {
                let mut state_guard = state.write().await;
                select_candidate(&mut state_guard, &search_item, &tuning)
            };
            match selection {
                CandidateSelection::Download(judge_submission) => {
                    schedule_download(
                        tasks,
                        managers,
                        download_semaphore.clone(),
                        Arc::clone(sender),
                        judge_submission,
                        "download",
                    );
                }
                CandidateSelection::RetrySearch => {
                    schedule_relaxed_search(
                        tasks,
                        state,
                        managers,
                        search_semaphore.clone(),
                        Arc::clone(sender),
                        search_item,
                        "retry_search",
                    )
                    .await;
                }
                CandidateSelection::Reject(judge_submission) => {
                    let reject = RejectedTrack::new(
                        judge_submission,
                        RejectReason::AbandonedAttemptingSearch,
                    );
                    send(Track::Reject(reject), sender)
                        .await
                        .context("rejecting exhausted candidates")?;
                }
                CandidateSelection::Wait | CandidateSelection::None => {}
            }
        }
        Track::File(downloaded_file) => {
            state
                .write()
                .await
                .in_progress
                .remove(&downloaded_file.track);
            tracing::info!(?downloaded_file, "Downloaded file");
        }
        Track::Retry(mut retry_request) => {
            let tuning = WorkerTuning::from_env();
            let next_selection = {
                let mut state_guard = state.write().await;
                state_guard.in_progress.remove(&retry_request.request.track);
                if let Some(pool) = state_guard
                    .candidate_pools
                    .get_mut(&retry_request.request.track)
                {
                    pool.failed
                        .insert(retry_request.failed_download_result.clone());
                }
                select_candidate(&mut state_guard, &retry_request.request.track, &tuning)
            };
            match next_selection {
                CandidateSelection::Download(judge_submission) => {
                    tracing::info!(
                        ?retry_request.request,
                        ?judge_submission,
                        "Retrying with alternate candidate",
                    );
                    schedule_download(
                        tasks,
                        managers,
                        download_semaphore.clone(),
                        Arc::clone(sender),
                        judge_submission,
                        "download_retry",
                    );
                    retry_request.retry_attempts += 1;
                    tracing::debug!(?retry_request, "Alternate retry queued");
                    return Ok(());
                }
                CandidateSelection::Reject(judge_submission) => {
                    let reject = RejectedTrack::new(
                        judge_submission,
                        RejectReason::AbandonedAttemptingSearch,
                    );
                    send(Track::Reject(reject), sender)
                        .await
                        .context("rejecting exhausted candidates")?;
                    return Ok(());
                }
                CandidateSelection::Wait => return Ok(()),
                CandidateSelection::RetrySearch | CandidateSelection::None => {}
            }
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
            schedule_relaxed_search(
                tasks,
                state,
                &managers,
                semaphore,
                sender,
                search_item.track,
                "retry_search",
            )
            .await;
            tracing::debug!(?retry_request, "Retry queued")
        }
        Track::Reject(rejected_track) => {
            state
                .write()
                .await
                .in_progress
                .remove(&rejected_track.track.track);
        }
    }
    Ok(())
}

enum CandidateSelection {
    Download(JudgeSubmission),
    RetrySearch,
    Reject(JudgeSubmission),
    Wait,
    None,
}

fn schedule_download(
    tasks: &mut JoinSet<ManagedTaskResult>,
    managers: &Arc<Managers>,
    semaphore: Arc<Semaphore>,
    sender: Arc<Sender<Track>>,
    judge_submission: JudgeSubmission,
    label: &'static str,
) {
    let managers = Arc::clone(managers);
    tracing::debug!(?judge_submission, task_label = label, "Scheduling download");
    spawn_managed(tasks, label, async move {
        managers
            .download_manager
            .run(
                judge_submission,
                semaphore,
                sender,
                managers.redis_pool.clone(),
                managers.db_pool.clone(),
            )
            .await
            .context("Downloading")?;
        Ok(())
    });
}

async fn schedule_relaxed_search(
    tasks: &mut JoinSet<ManagedTaskResult>,
    state: &Arc<tokio::sync::RwLock<RunState>>,
    managers: &Arc<Managers>,
    semaphore: Arc<Semaphore>,
    sender: Arc<Sender<Track>>,
    search_item: SearchItem,
    label: &'static str,
) {
    let tuning = WorkerTuning::from_env();
    let request_allowed = {
        let mut state_guard = state.write().await;
        spend_request(&mut state_guard, &search_item, &tuning, RequestKind::Search)
    };
    if !request_allowed {
        tracing::info!(
            track_id = %search_item.track_id,
            max_search_passes_per_track = tuning.max_search_passes_per_track,
            max_requests_per_track = tuning.max_requests_per_track,
            "Request budget exhausted before relaxed search",
        );
        return;
    }
    let managers = Arc::clone(managers);
    tracing::info!(?search_item, "Scheduling relaxed retry search");
    spawn_managed(tasks, label, async move {
        managers
            .search_manager
            .run(
                search_item,
                managers.search_empty_result_cutoff(),
                managers.query_manager_search_timeout(),
                true,
                semaphore,
                sender,
            )
            .await
            .context("returning track")?;
        Ok(())
    });
}

fn spend_request(
    state: &mut RunState,
    search_item: &SearchItem,
    tuning: &WorkerTuning,
    kind: RequestKind,
) -> bool {
    let budget = state
        .request_budgets
        .entry(search_item.clone())
        .or_default();
    if budget.request_count >= tuning.max_requests_per_track {
        return false;
    }
    if matches!(kind, RequestKind::Search) {
        if budget.search_passes >= tuning.max_search_passes_per_track {
            return false;
        }
        budget.search_passes += 1;
    }
    budget.request_count += 1;
    true
}

fn push_candidate(pool: &mut CandidatePool, candidate: JudgeSubmission, limit: usize) {
    if pool
        .candidates
        .iter()
        .any(|existing| existing.query == candidate.query)
    {
        return;
    }
    pool.candidates.push(candidate);
    pool.candidates.sort_by(compare_candidates);
    pool.candidates.truncate(limit);
}

fn select_candidate(
    state: &mut RunState,
    search_item: &SearchItem,
    tuning: &WorkerTuning,
) -> CandidateSelection {
    if state.in_progress.contains(search_item) {
        return CandidateSelection::Wait;
    }
    let candidate = {
        let Some(pool) = state.candidate_pools.get_mut(search_item) else {
            return CandidateSelection::None;
        };
        pool.selection_queued = false;
        let candidate = pool
            .candidates
            .iter()
            .find(|candidate| !pool.failed.contains(&candidate.query))
            .cloned();
        if let Some(candidate) = candidate {
            if pool.attempts >= tuning.max_download_attempts_per_track {
                return CandidateSelection::Reject(candidate);
            }
            Some(candidate)
        } else {
            if pool.attempts >= tuning.max_download_attempts_per_track {
                return pool
                    .candidates
                    .first()
                    .cloned()
                    .map(CandidateSelection::Reject)
                    .unwrap_or(CandidateSelection::None);
            }
            None
        }
    };
    if let Some(candidate) = candidate {
        if !spend_request(state, search_item, tuning, RequestKind::Download) {
            tracing::info!(
                track_id = %search_item.track_id,
                max_requests_per_track = tuning.max_requests_per_track,
                "Request budget exhausted before download attempt",
            );
            return CandidateSelection::Reject(candidate);
        }
        let pool = state
            .candidate_pools
            .get_mut(search_item)
            .expect("candidate pool exists after selecting candidate");
        pool.attempts += 1;
        state.in_progress.insert(search_item.clone());
        return CandidateSelection::Download(candidate);
    }
    CandidateSelection::RetrySearch
}

fn compare_candidates(left: &JudgeSubmission, right: &JudgeSubmission) -> std::cmp::Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| quality_rank(&right.query.filename).cmp(&quality_rank(&left.query.filename)))
        .then_with(|| right.query.size.cmp(&left.query.size))
}

fn quality_rank(filename: &str) -> u8 {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".flac") {
        4
    } else if lower.ends_with(".wav") || lower.ends_with(".aiff") || lower.ends_with(".aif") {
        3
    } else if lower.ends_with(".mp3") {
        2
    } else if lower.ends_with(".aac") || lower.ends_with(".m4a") || lower.ends_with(".ogg") {
        1
    } else {
        0
    }
}

fn panic_payload(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

impl Managers {
    fn query_manager_search_timeout(&self) -> u8 {
        self.query_manager.search_timeout_secs
    }

    fn search_empty_result_cutoff(&self) -> usize {
        self.search_empty_result_cutoff
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
    use super::{
        CandidatePool, CandidateSelection, ManagedTaskResult, RequestKind, RunState, WorkerTuning,
        push_candidate, select_candidate, spawn_managed, spend_request,
    };
    use crate::internals::search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem};
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

    fn item() -> SearchItem {
        SearchItem::new(
            "spotify-track-id".to_string(),
            "Track".to_string(),
            "Album".to_string(),
            "Artist".to_string(),
        )
    }

    fn submission(filename: &str, score: f32, size: i64) -> JudgeSubmission {
        JudgeSubmission {
            track: item(),
            query: DownloadableFile {
                filename: filename.to_string(),
                username: format!("user-{filename}"),
                size,
            },
            score: Some(score),
        }
    }

    fn tuning(max_attempts: usize) -> WorkerTuning {
        WorkerTuning {
            search_concurrency: 1,
            download_concurrency: 1,
            queue_capacity: 10,
            max_candidates_per_track: 5,
            max_download_attempts_per_track: max_attempts,
            candidate_collection_secs: 0,
            max_search_passes_per_track: 2,
            max_requests_per_track: 8,
        }
    }

    #[test]
    fn candidate_pool_prefers_score_then_quality() {
        let mut pool = CandidatePool::default();

        push_candidate(&mut pool, submission("song.mp3", 0.8, 10), 5);
        push_candidate(&mut pool, submission("song.flac", 0.8, 20), 5);
        push_candidate(&mut pool, submission("lower.flac", 0.7, 30), 5);

        assert_eq!(pool.candidates[0].query.filename, "song.flac");
        assert_eq!(pool.candidates[1].query.filename, "song.mp3");
        assert_eq!(pool.candidates[2].query.filename, "lower.flac");
    }

    #[test]
    fn selection_skips_failed_candidate_before_retrying_search() {
        let search_item = item();
        let first = submission("first.flac", 0.9, 10);
        let second = submission("second.flac", 0.8, 10);
        let mut state = RunState::default();
        let pool = state
            .candidate_pools
            .entry(search_item.clone())
            .or_default();
        push_candidate(pool, first.clone(), 5);
        push_candidate(pool, second.clone(), 5);

        match select_candidate(&mut state, &search_item, &tuning(5)) {
            CandidateSelection::Download(candidate) => assert_eq!(candidate.query, first.query),
            _ => panic!("expected first download"),
        }
        state.in_progress.remove(&search_item);
        state
            .candidate_pools
            .get_mut(&search_item)
            .expect("pool")
            .failed
            .insert(first.query);

        match select_candidate(&mut state, &search_item, &tuning(5)) {
            CandidateSelection::Download(candidate) => assert_eq!(candidate.query, second.query),
            _ => panic!("expected alternate download"),
        }
    }

    #[test]
    fn selection_rejects_after_attempt_limit() {
        let search_item = item();
        let mut state = RunState::default();
        let pool = state
            .candidate_pools
            .entry(search_item.clone())
            .or_default();
        push_candidate(pool, submission("only.flac", 0.9, 10), 5);
        pool.attempts = 1;

        match select_candidate(&mut state, &search_item, &tuning(1)) {
            CandidateSelection::Reject(candidate) => {
                assert_eq!(candidate.query.filename, "only.flac");
            }
            _ => panic!("expected rejection"),
        }
    }

    #[test]
    fn request_budget_caps_search_passes_and_total_requests() {
        let search_item = item();
        let mut state = RunState::default();
        let tuning = WorkerTuning {
            max_search_passes_per_track: 1,
            max_requests_per_track: 2,
            ..tuning(5)
        };

        assert!(spend_request(
            &mut state,
            &search_item,
            &tuning,
            RequestKind::Search
        ));
        assert!(!spend_request(
            &mut state,
            &search_item,
            &tuning,
            RequestKind::Search
        ));
        assert!(spend_request(
            &mut state,
            &search_item,
            &tuning,
            RequestKind::Download
        ));
        assert!(!spend_request(
            &mut state,
            &search_item,
            &tuning,
            RequestKind::Download
        ));
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
