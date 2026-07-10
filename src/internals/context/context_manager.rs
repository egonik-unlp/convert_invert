use crate::internals::database::manager::DatabaseManager;
use futures_util::FutureExt;
use rand::Rng;
use redis::TypedCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadedFile {
    pub filename: String,
    pub track: SearchItem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunEvent {
    SearchQueued(SearchItem),
    SearchRetryQueued(SearchItem),
    CandidateFound(JudgeSubmission),
    CandidateAccepted(JudgeSubmission),
    CandidateSelected(JudgeSubmission),
    FileDownloaded(DownloadedFile),
    RetryQueued {
        request: JudgeSubmission,
        failed: DownloadableFile,
    },
    Rejected {
        track: JudgeSubmission,
        reason: String,
    },
}

struct ManagedTaskResult {
    pub label: &'static str,
    pub error: Option<String>,
}

struct RunCycleShared<'a> {
    managers: &'a Arc<Managers>,
    sender: &'a Arc<Sender<Track>>,
    events: Option<&'a Arc<Sender<RunEvent>>>,
    state: &'a Arc<tokio::sync::RwLock<RunState>>,
    search_semaphore: &'a Arc<Semaphore>,
    download_semaphore: &'a Arc<Semaphore>,
    /// Tuning resolved once at chunk start; avoids re-reading env on every event.
    tuning: WorkerTuning,
}

#[derive(Debug, Default)]
struct RunState {
    in_progress: HashSet<SearchItem>,
    candidate_pools: HashMap<SearchItem, CandidatePool>,
    request_budgets: HashMap<SearchItem, RequestBudget>,
    /// Peer username -> instant at which its download cooldown expires. Peers are skipped
    /// for new attempts while cooling down, so one flaky/hostile peer is not hammered.
    peer_cooldowns: HashMap<String, Instant>,
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
    /// Base backoff (ms) applied with full jitter before retrying a failed download or
    /// search. Paces retries so we do not hammer flaky peers. `0` disables.
    pub retry_backoff_ms: u64,
    /// Base delay (ms) applied with full jitter before issuing each Soulseek search, to
    /// pace outbound requests and reduce ban risk. `0` disables.
    pub search_pacing_ms: u64,
    /// Seconds a peer (Soulseek username) is skipped for new download attempts after one of
    /// its transfers fails/times out, so a single bad peer is not hammered. `0` disables.
    pub peer_cooldown_secs: u64,
    /// Absolute ceiling (secs) for a single download attempt before it is aborted. `0` disables.
    pub download_hard_timeout_secs: u64,
    /// Secs a download may sit queued/initializing (not transferring) before being aborted so it
    /// stops occupying a concurrency slot. Main lever against a pipeline clog. `0` disables.
    pub download_queued_timeout_secs: u64,
    /// Secs an active download may make no byte progress before being aborted. `0` disables.
    pub download_stall_timeout_secs: u64,
}

impl WorkerTuning {
    /// Loads tuning parameters from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            search_concurrency: env_usize("SEARCH_CONCURRENCY", 1),
            download_concurrency: env_usize("DOWNLOAD_CONCURRENCY", 1),
            queue_capacity: env_usize("QUEUE_CAPACITY", 20000),
            max_candidates_per_track: env_usize("MAX_CANDIDATES_PER_TRACK", 8).max(1),
            max_download_attempts_per_track: env_usize("MAX_DOWNLOAD_ATTEMPTS_PER_TRACK", 4).max(1),
            candidate_collection_secs: env_u64("CANDIDATE_COLLECTION_SECS", 20),
            max_search_passes_per_track: env_usize("MAX_SEARCH_PASSES_PER_TRACK", 2).max(1),
            max_requests_per_track: env_usize("MAX_REQUESTS_PER_TRACK", 8).max(1),
            retry_backoff_ms: env_u64("RETRY_BACKOFF_MS", 1000),
            search_pacing_ms: env_u64("SEARCH_PACING_MS", 500),
            peer_cooldown_secs: env_u64("PEER_COOLDOWN_SECS", 120),
            download_hard_timeout_secs: env_u64(
                "DOWNLOAD_HARD_TIMEOUT_SECS",
                crate::internals::download::download_manager::DEFAULT_DOWNLOAD_HARD_TIMEOUT_SECS,
            ),
            download_queued_timeout_secs: env_u64(
                "DOWNLOAD_QUEUED_TIMEOUT_SECS",
                crate::internals::download::download_manager::DEFAULT_DOWNLOAD_QUEUED_TIMEOUT_SECS,
            ),
            download_stall_timeout_secs: env_u64(
                "DOWNLOAD_STALL_TIMEOUT_SECS",
                crate::internals::download::download_manager::DEFAULT_DOWNLOAD_STALL_TIMEOUT_SECS,
            ),
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
        // Soulseek I/O (search + download + share) is delegated to the aioslsk engine service
        // over HTTP. A single login there covers everything, and aioslsk's server-brokered
        // connections make transfers work from behind NAT/CGNAT.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("building HTTP client for the Soulseek engine")?;
        let base_url = config.slsk_url.clone();
        let search_empty_result_cutoff = config.search_empty_result_cutoff;
        let download_timeouts =
            crate::internals::download::download_manager::DownloadTimeouts::from_env();
        let download_manager =
            DownloadManager::new(http.clone(), base_url.clone(), path, download_timeouts);
        let search_manager = SearchManager::new(http.clone(), base_url.clone());
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
        self.run_chunk_with_events(tracks, None).await
    }

    pub async fn run_chunk_with_events(
        self: &Arc<Self>,
        tracks: impl IntoIterator<Item = Track>,
        events: Option<Arc<Sender<RunEvent>>>,
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
                        events: events.as_ref(),
                        state: &state,
                        search_semaphore: &search_semaphore,
                        download_semaphore: &download_semaphore,
                        tuning,
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
        tracing::info!("Managers shutdown; the Soulseek engine runs as a separate service");
    }
}

/// TTL (secs) on the live per-track activity marker in Redis. Short so a dead worker's marker
/// disappears on its own; the API also treats older markers as stale.
pub const ACTIVITY_STALE_SECS: i64 = 45;

fn activity_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Mirror a track's current pipeline stage ("searching" / "judging") into Redis (`act:<track_id>`)
/// so the dashboard can show live activity independent of the paginated library. Best-effort and
/// off the async reactor; downloads are already tracked via `dl:*:progress`.
async fn write_activity(
    redis_pool: &RedisPool,
    track_id: &str,
    stage: &str,
    title: &str,
    artist: &str,
) {
    let redis_pool = redis_pool.clone();
    let track_id = track_id.to_string();
    let stage = stage.to_string();
    let title = title.to_string();
    let artist = artist.to_string();
    let _ = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut con = redis_pool.get_timeout(Duration::from_secs(3))?;
        let key = format!("act:{track_id}");
        let values = [
            ("stage".to_string(), stage),
            ("title".to_string(), title),
            ("artist".to_string(), artist),
            ("track_id".to_string(), track_id.clone()),
            ("updated_at".to_string(), activity_now_secs().to_string()),
        ];
        con.hset_multiple::<String, String, _>(key.clone(), &values)?;
        con.expire(&key, ACTIVITY_STALE_SECS)?;
        Ok(())
    })
    .await;
}

/// Remove a track's activity marker once it reaches a terminal stage (downloaded / rejected).
async fn clear_activity(redis_pool: &RedisPool, track_id: &str) {
    let redis_pool = redis_pool.clone();
    let track_id = track_id.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut con) = redis_pool.get_timeout(Duration::from_secs(3)) {
            let _ = con.del(format!("act:{track_id}"));
        }
    })
    .await;
}

async fn process_track(
    track: Track,
    shared: RunCycleShared<'_>,
    tasks: &mut JoinSet<ManagedTaskResult>,
) -> anyhow::Result<()> {
    let RunCycleShared {
        managers,
        sender,
        events,
        state,
        search_semaphore,
        download_semaphore,
        tuning,
    } = shared;

    emit_run_event(events, event_from_track(&track)).await;

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
            if is_track_downloaded(managers, &search_item)? {
                tracing::info!(
                    track_id = %search_item.track_id,
                    "Skipping search; track already downloaded",
                );
                return Ok(());
            }
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
            write_activity(
                &managers.redis_pool,
                &search_item.track_id,
                "searching",
                &search_item.track,
                &search_item.artist,
            )
            .await;
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            let semaphore = search_semaphore.clone();
            let pacing_ms = tuning.search_pacing_ms;
            tracing::debug!(?search_item, "Scheduling search");
            spawn_managed(tasks, "search", async move {
                pace_request(pacing_ms).await;
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
            if is_track_downloaded(managers, &search_item)? {
                tracing::info!(
                    track_id = %search_item.track_id,
                    "Skipping relaxed search; track already downloaded",
                );
                return Ok(());
            }
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
            write_activity(
                &managers.redis_pool,
                &search_item.track_id,
                "searching",
                &search_item.track,
                &search_item.artist,
            )
            .await;
            let managers = Arc::clone(managers);
            let sender = Arc::clone(sender);
            let semaphore = search_semaphore.clone();
            let pacing_ms = tuning.search_pacing_ms;
            tracing::debug!(?search_item, "Scheduling relaxed search retry");
            spawn_managed(tasks, "search_retry", async move {
                pace_request(pacing_ms).await;
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
            write_activity(
                &managers.redis_pool,
                &judge_submission.track.track_id,
                "judging",
                &judge_submission.track.track,
                &judge_submission.track.artist,
            )
            .await;
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
            let selection = {
                let mut state_guard = state.write().await;
                select_candidate(&mut state_guard, &search_item, &tuning)
            };
            match selection {
                CandidateSelection::Download(judge_submission) => {
                    emit_run_event(
                        events,
                        Some(RunEvent::CandidateSelected(judge_submission.clone())),
                    )
                    .await;
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
                    let _scheduled = schedule_relaxed_search(
                        tasks,
                        state,
                        managers,
                        search_semaphore.clone(),
                        Arc::clone(sender),
                        search_item,
                        "retry_search",
                        tuning,
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
            clear_activity(&managers.redis_pool, &downloaded_file.track.track_id).await;
            tracing::info!(?downloaded_file, "Downloaded file");
        }
        Track::Retry(mut retry_request) => {
            // A2: pace retries with a jittered backoff so flaky peers are not hammered.
            backoff_before_retry(tuning.retry_backoff_ms).await;
            let next_selection = {
                let mut state_guard = state.write().await;
                state_guard.in_progress.remove(&retry_request.request.track);
                // A3: cool the peer whose transfer just failed so we prefer other peers.
                if tuning.peer_cooldown_secs > 0 {
                    let cooled_until =
                        Instant::now() + Duration::from_secs(tuning.peer_cooldown_secs);
                    state_guard.peer_cooldowns.insert(
                        retry_request.failed_download_result.username.clone(),
                        cooled_until,
                    );
                }
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
            let original_request = retry_request.request.clone();
            let scheduled = schedule_relaxed_search(
                tasks,
                state,
                managers,
                search_semaphore.clone(),
                Arc::clone(sender),
                original_request.track.clone(),
                "retry_search",
                tuning,
            )
            .await;
            if !scheduled {
                let reject =
                    RejectedTrack::new(original_request, RejectReason::AbandonedAttemptingSearch);
                send(Track::Reject(reject), sender)
                    .await
                    .context("rejecting exhausted retry search budget")?;
            } else {
                retry_request.retry_attempts += 1;
                tracing::info!(?retry_request.request, "Retry requested");
                tracing::debug!(?retry_request, "Retry queued");
            }
        }
        Track::Reject(rejected_track) => {
            let (track, reason) = rejected_track.parts();
            clear_activity(&managers.redis_pool, &track.track.track_id).await;
            emit_run_event(
                events,
                Some(RunEvent::Rejected {
                    track: track.clone(),
                    reason: format!("{reason:?}"),
                }),
            )
            .await;
            state
                .write()
                .await
                .in_progress
                .remove(&rejected_track.track.track);
        }
    }
    Ok(())
}

async fn emit_run_event(events: Option<&Arc<Sender<RunEvent>>>, event: Option<RunEvent>) {
    let (Some(events), Some(event)) = (events, event) else {
        return;
    };
    let _ = events.send(event).await;
}

fn event_from_track(track: &Track) -> Option<RunEvent> {
    match track {
        Track::Query(item) => Some(RunEvent::SearchQueued(item.clone())),
        Track::SearchRetry(item) => Some(RunEvent::SearchRetryQueued(item.clone())),
        Track::Result(submission) => Some(RunEvent::CandidateFound(submission.clone())),
        Track::Downloadable(submission) => Some(RunEvent::CandidateAccepted(submission.clone())),
        Track::File(file) => Some(RunEvent::FileDownloaded(DownloadedFile {
            filename: file.filename.clone(),
            track: file.track.clone(),
        })),
        Track::Retry(retry) => Some(RunEvent::RetryQueued {
            request: retry.request.clone(),
            failed: retry.failed_download_result.clone(),
        }),
        Track::Reject(_) | Track::SelectCandidate(_) => None,
    }
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

#[allow(clippy::too_many_arguments)]
async fn schedule_relaxed_search(
    tasks: &mut JoinSet<ManagedTaskResult>,
    state: &Arc<tokio::sync::RwLock<RunState>>,
    managers: &Arc<Managers>,
    semaphore: Arc<Semaphore>,
    sender: Arc<Sender<Track>>,
    search_item: SearchItem,
    label: &'static str,
    tuning: WorkerTuning,
) -> bool {
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
        return false;
    }
    let managers = Arc::clone(managers);
    let pacing_ms = tuning.search_pacing_ms;
    tracing::info!(?search_item, "Scheduling relaxed retry search");
    spawn_managed(tasks, label, async move {
        pace_request(pacing_ms).await;
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
    true
}

/// A1: returns true if the track already has a successful download recorded, so callers
/// can skip re-searching Soulseek for it (the biggest source of wasted, ban-prone queries
/// on re-runs and failed-track replays). Reuses [`DatabaseManager::is_search_item_downloaded`].
fn is_track_downloaded(managers: &Arc<Managers>, item: &SearchItem) -> anyhow::Result<bool> {
    let mut conn = managers.db_pool.get().map_err(|err| {
        let snapshot = db_pool_snapshot(&managers.db_pool);
        tracing::error!(
            ?err,
            db_pool_connections = snapshot.connections,
            db_pool_idle_connections = snapshot.idle_connections,
            db_pool_in_use_connections = snapshot.in_use_connections(),
            "DB pool in pre-search downloaded check"
        );
        err
    })?;
    let mut database_manager = DatabaseManager::new(&mut conn);
    database_manager
        .is_search_item_downloaded(item)
        .context("Pre-search downloaded check")
}

/// Returns a full-jitter duration in `[base_ms, 2*base_ms)`. `0` yields zero.
fn jittered(base_ms: u64) -> Duration {
    if base_ms == 0 {
        return Duration::ZERO;
    }
    let extra = rand::rng().random_range(0..base_ms);
    Duration::from_millis(base_ms + extra)
}

/// A2: jittered delay before issuing an outbound Soulseek search, to pace requests.
async fn pace_request(base_ms: u64) {
    let delay = jittered(base_ms);
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

/// A2: jittered backoff before retrying a failed download/search.
async fn backoff_before_retry(base_ms: u64) {
    let delay = jittered(base_ms);
    if !delay.is_zero() {
        tracing::debug!(delay_ms = delay.as_millis() as u64, "Retry backoff");
        tokio::time::sleep(delay).await;
    }
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
    // A3: prune expired peer cooldowns and snapshot the still-cooling peers so we can
    // prefer candidates from other peers without holding a borrow on `state`.
    let now = Instant::now();
    state.peer_cooldowns.retain(|_, until| *until > now);
    let cooled: HashSet<String> = state.peer_cooldowns.keys().cloned().collect();
    let candidate = {
        let Some(pool) = state.candidate_pools.get_mut(search_item) else {
            return CandidateSelection::None;
        };
        pool.selection_queued = false;
        let candidate = pool
            .candidates
            .iter()
            // Prefer a non-failed candidate from a peer that is not cooling down; fall back
            // to any non-failed candidate so a track is never stranded by cooldowns alone.
            .find(|candidate| {
                !pool.failed.contains(&candidate.query)
                    && !cooled.contains(&candidate.query.username)
            })
            .or_else(|| {
                pool.candidates
                    .iter()
                    .find(|candidate| !pool.failed.contains(&candidate.query))
            })
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
    } else if lower.ends_with(".aac")
        || lower.ends_with(".m4a")
        || lower.ends_with(".ogg")
        || lower.ends_with(".opus")
    {
        1
    } else {
        0
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
    use std::time::{Duration, Instant};
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
            relative_mi_score: None,
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
            retry_backoff_ms: 0,
            search_pacing_ms: 0,
            peer_cooldown_secs: 0,
            download_hard_timeout_secs: 180,
            download_queued_timeout_secs: 45,
            download_stall_timeout_secs: 30,
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
    fn selection_prefers_peer_not_in_cooldown() {
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
        // Cool down the top candidate's peer; selection should prefer the other peer.
        state.peer_cooldowns.insert(
            first.query.username.clone(),
            Instant::now() + Duration::from_secs(60),
        );

        match select_candidate(&mut state, &search_item, &tuning(5)) {
            CandidateSelection::Download(candidate) => assert_eq!(candidate.query, second.query),
            _ => panic!("expected the non-cooled peer to be selected"),
        }
    }

    #[test]
    fn selection_falls_back_to_cooled_peer_when_only_option() {
        let search_item = item();
        let only = submission("only.flac", 0.9, 10);
        let mut state = RunState::default();
        let pool = state
            .candidate_pools
            .entry(search_item.clone())
            .or_default();
        push_candidate(pool, only.clone(), 5);
        // Even if the only candidate's peer is cooling down, the track must not be stranded.
        state.peer_cooldowns.insert(
            only.query.username.clone(),
            Instant::now() + Duration::from_secs(60),
        );

        match select_candidate(&mut state, &search_item, &tuning(5)) {
            CandidateSelection::Download(candidate) => assert_eq!(candidate.query, only.query),
            _ => panic!("expected fallback to the only (cooled) candidate"),
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
