use std::collections::{HashMap, HashSet};

use actix_web::{HttpResponse, get, web};
use convert_invert::internals::context::context_manager::RedisPool;
use convert_invert::internals::context::context_manager::WorkerTuning;
use convert_invert::internals::database::DbPool;
use convert_invert::internals::database::schema;
use convert_invert::internals::judge::judge_manager::JUDGE_THRESHOLD;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Float4, Integer, Nullable, Text};
use redis::Commands;
use serde::Serialize;

use crate::errors::{ApiError, ApiResult};
use crate::state::AppState;
use crate::validation::PlaylistQuery;

#[derive(Serialize)]
pub struct HealthResponse {
    pub api: &'static str,
    pub db: &'static str,
    pub tables: HashMap<String, bool>,
    pub redis: &'static str,
    pub jaeger: &'static str,
    #[serde(rename = "accountConflict")]
    pub account_conflict: bool,
    pub warnings: Vec<String>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct StatsResponse {
    #[serde(rename = "totalTracks")]
    pub total_tracks: i64,
    pub pending: i64,
    pub downloading: usize,
    pub completed: i64,
    pub failed: i64,
    #[serde(rename = "globalProgress")]
    pub global_progress: i64,
    #[serde(rename = "remainingTime")]
    pub remaining_time: &'static str,
    #[serde(rename = "tableCounts")]
    pub table_counts: HashMap<String, i64>,
}

#[derive(Serialize)]
pub struct PlaylistSummary {
    pub id: String,
    pub name: String,
    #[serde(rename = "trackCount")]
    pub track_count: i64,
    #[serde(rename = "totalSize")]
    pub total_size: String,
    pub quality: String,
    #[serde(rename = "lastSynced")]
    pub last_synced: String,
    #[serde(rename = "coverArt")]
    pub cover_art: String,
    pub tracks: Vec<TrackResponse>,
    #[serde(rename = "nextCursor", skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<i32>,
}

#[derive(Serialize)]
pub struct TrackResponse {
    pub id: i32,
    pub track_id: String,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub status: String,
    pub progress: i32,
    pub score: Option<f32>,
    #[serde(rename = "relativeMiScore")]
    pub relative_mi_score: Option<f32>,
    #[serde(rename = "candidatesCount")]
    pub candidates_count: i64,
    #[serde(rename = "rejectReason")]
    pub reject_reason: Option<String>,
    #[serde(rename = "downloadStatus", skip_serializing_if = "Option::is_none")]
    pub download_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

#[derive(Serialize)]
pub struct CandidateResponse {
    pub id: i32,
    #[serde(rename = "fileId")]
    pub file_id: i32,
    pub username: String,
    pub filename: String,
    pub score: f32,
    #[serde(rename = "relativeMiScore")]
    pub relative_mi_score: Option<f32>,
}

#[derive(Serialize)]
pub struct TrackReportResponse {
    pub track: TrackReportTrack,
    pub status: TrackReportStatus,
    pub summary: TrackReportSummary,
    pub lifecycle: Vec<TrackLifecycleEvent>,
    pub candidates: Vec<TrackReportCandidate>,
    pub rejections: Vec<TrackReportRejection>,
    pub retries: Vec<TrackReportRetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download: Option<TrackReportDownload>,
    #[serde(rename = "traceAnalysisUnavailable")]
    pub trace_analysis_unavailable: bool,
    #[serde(rename = "traceNote", skip_serializing_if = "Option::is_none")]
    pub trace_note: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct TrackReportTrack {
    pub id: i32,
    #[serde(rename = "trackId")]
    pub track_id: String,
    pub title: String,
    pub artist: String,
    pub album: String,
}

#[derive(Serialize)]
pub struct TrackReportStatus {
    pub stage: String,
    pub progress: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Serialize)]
pub struct TrackReportSummary {
    pub severity: &'static str,
    pub diagnosis: String,
    #[serde(rename = "nextAction")]
    pub next_action: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TrackLifecycleEvent {
    pub kind: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Serialize)]
pub struct TrackReportCandidate {
    pub id: i32,
    #[serde(rename = "fileId")]
    pub file_id: i32,
    pub rank: usize,
    pub username: String,
    pub filename: String,
    pub size: i64,
    pub score: f32,
    #[serde(rename = "relativeMiScore")]
    pub relative_mi_score: Option<f32>,
    pub classification: String,
    pub attempted: bool,
    pub downloaded: bool,
    pub rejected: bool,
}

#[derive(Serialize)]
pub struct TrackReportRejection {
    pub id: i32,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub detail: String,
    #[serde(rename = "candidateId")]
    pub candidate_id: i32,
}

#[derive(Serialize)]
pub struct TrackReportRetry {
    pub id: i32,
    #[serde(rename = "candidateId")]
    pub candidate_id: i32,
    #[serde(rename = "failedFileId")]
    pub failed_file_id: i32,
    #[serde(rename = "retryAttempts")]
    pub retry_attempts: i32,
    pub filename: String,
    pub peer: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct TrackReportDownload {
    pub filename: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<i32>,
    pub completed: bool,
}

#[derive(Serialize)]
pub struct NetworkResponse {
    pub status: &'static str,
    pub user: String,
    pub latency: &'static str,
    pub node: &'static str,
    #[serde(rename = "totalBandwidth")]
    pub total_bandwidth: &'static str,
}

#[derive(Serialize)]
pub struct LogEntry {
    pub id: String,
    pub timestamp: i64,
    pub message: String,
    pub level: String,
}

#[derive(Serialize)]
pub struct ConfigResponse {
    #[serde(rename = "judgeThreshold")]
    pub judge_threshold: f32,
    pub auth: AuthConfig,
    pub tuning: TuningConfig,
}

#[derive(Serialize)]
pub struct AuthConfig {
    pub scheme: &'static str,
    pub header: &'static str,
}

#[derive(Serialize)]
pub struct TuningConfig {
    #[serde(rename = "searchConcurrency")]
    pub search_concurrency: usize,
    #[serde(rename = "downloadConcurrency")]
    pub download_concurrency: usize,
    #[serde(rename = "searchTimeoutSecs")]
    pub search_timeout_secs: u8,
    #[serde(rename = "searchEmptyResultCutoff")]
    pub search_empty_result_cutoff: usize,
    #[serde(rename = "maxCandidatesPerTrack")]
    pub max_candidates_per_track: usize,
    #[serde(rename = "maxDownloadAttemptsPerTrack")]
    pub max_download_attempts_per_track: usize,
    #[serde(rename = "candidateCollectionSecs")]
    pub candidate_collection_secs: u64,
    #[serde(rename = "maxSearchPassesPerTrack")]
    pub max_search_passes_per_track: usize,
    #[serde(rename = "maxRequestsPerTrack")]
    pub max_requests_per_track: usize,
    #[serde(rename = "retryBackoffMs")]
    pub retry_backoff_ms: u64,
    #[serde(rename = "searchPacingMs")]
    pub search_pacing_ms: u64,
    #[serde(rename = "peerCooldownSecs")]
    pub peer_cooldown_secs: u64,
    #[serde(rename = "workerPortRange")]
    pub worker_port_range: String,
    #[serde(rename = "workerAccountMode")]
    pub worker_account_mode: String,
    #[serde(rename = "workerUsername")]
    pub worker_username: String,
    #[serde(rename = "workerUsernamePattern")]
    pub worker_username_pattern: String,
    #[serde(rename = "shareUsername")]
    pub share_username: String,
    #[serde(rename = "accountConflict")]
    pub account_conflict: bool,
    #[serde(rename = "defaultWorkerCount")]
    pub default_worker_count: usize,
    #[serde(rename = "defaultPortBase")]
    pub default_port_base: u16,
    #[serde(rename = "workerPortPublishedRange")]
    pub worker_port_published_range: String,
    #[serde(rename = "defaultRunIdPrefix")]
    pub default_run_id_prefix: String,
    #[serde(rename = "shareMode")]
    pub share_mode: String,
    #[serde(rename = "sharePath")]
    pub share_path: String,
    #[serde(rename = "shareStatus")]
    pub share_status: String,
    #[serde(rename = "configWarnings")]
    pub config_warnings: Vec<String>,
}

#[derive(QueryableByName)]
struct TrackQueryRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    track_id: String,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    artist: String,
    #[diesel(sql_type = Text)]
    album: String,
    #[diesel(sql_type = Nullable<Text>)]
    reject_reason: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    reject_value: Option<String>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    candidates_count: i64,
    #[diesel(sql_type = Nullable<Float4>)]
    max_score: Option<f32>,
    #[diesel(sql_type = Nullable<Float4>)]
    max_relative_mi_score: Option<f32>,
    #[diesel(sql_type = Text)]
    track_status: String,
}

#[derive(QueryableByName)]
struct TrackIdentityRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Text)]
    track_id: String,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    artist: String,
    #[diesel(sql_type = Text)]
    album: String,
}

#[derive(QueryableByName)]
struct CandidateQueryRow {
    #[diesel(sql_type = Integer)]
    submission_id: i32,
    #[diesel(sql_type = Integer)]
    file_id: i32,
    #[diesel(sql_type = Text)]
    username: String,
    #[diesel(sql_type = Text)]
    filename: String,
    #[diesel(sql_type = Nullable<Float4>)]
    score: Option<f32>,
    #[diesel(sql_type = Nullable<Float4>)]
    relative_mi_score: Option<f32>,
}

#[derive(QueryableByName)]
struct TrackReportCandidateRow {
    #[diesel(sql_type = Integer)]
    submission_id: i32,
    #[diesel(sql_type = Integer)]
    file_id: i32,
    #[diesel(sql_type = Text)]
    username: String,
    #[diesel(sql_type = Text)]
    filename: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    size: i64,
    #[diesel(sql_type = Nullable<Float4>)]
    score: Option<f32>,
    #[diesel(sql_type = Nullable<Float4>)]
    relative_mi_score: Option<f32>,
    #[diesel(sql_type = Bool)]
    attempted: bool,
    #[diesel(sql_type = Bool)]
    downloaded: bool,
    #[diesel(sql_type = Bool)]
    rejected: bool,
}

#[derive(QueryableByName)]
struct TrackReportDownloadRow {
    #[diesel(sql_type = Text)]
    filename: String,
}

#[derive(QueryableByName)]
struct TrackReportRejectionRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Integer)]
    candidate_id: i32,
    #[diesel(sql_type = Text)]
    reason: String,
    #[diesel(sql_type = Nullable<Text>)]
    value: Option<String>,
}

#[derive(QueryableByName)]
struct TrackReportRetryRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Integer)]
    candidate_id: i32,
    #[diesel(sql_type = Integer)]
    failed_file_id: i32,
    #[diesel(sql_type = Integer)]
    retry_attempts: i32,
    #[diesel(sql_type = Text)]
    filename: String,
    #[diesel(sql_type = Text)]
    username: String,
}

#[derive(QueryableByName)]
struct ExistsRow {
    #[diesel(sql_type = Bool)]
    exists: bool,
}

#[derive(Default, Clone)]
struct RedisProgress {
    progress: i32,
    finished: bool,
    status: Option<String>,
    filename: Option<String>,
    username: Option<String>,
    track_db_id: Option<i32>,
}

fn redis_progress_from_hash(data: &HashMap<String, String>) -> RedisProgress {
    let finished = data.get("completed").is_some_and(|value| value == "true");
    let downloaded = data
        .get("bytes_downloaded")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or_default();
    let total = data
        .get("total_bytes")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or_default();
    let progress_value = if finished {
        100
    } else if total > 0.0 {
        ((downloaded / total) * 100.0).round() as i32
    } else {
        0
    };
    RedisProgress {
        progress: progress_value.clamp(0, 100),
        finished,
        status: data.get("status").cloned(),
        filename: data.get("filename").cloned(),
        username: data.get("username").cloned(),
        track_db_id: data
            .get("track_db_id")
            .and_then(|value| value.parse::<i32>().ok()),
    }
}

fn redis_progress_map(
    redis_pool: &RedisPool,
    db_pool: &DbPool,
) -> ApiResult<HashMap<i32, RedisProgress>> {
    let mut connection = db_pool.get()?;
    let known_ids = schema::search_items::table
        .select(schema::search_items::id)
        .load::<i32>(&mut connection)?
        .into_iter()
        .collect::<HashSet<_>>();
    let correlations = schema::judge_submissions::table
        .select((
            schema::judge_submissions::id,
            schema::judge_submissions::track,
        ))
        .load::<(i32, i32)>(&mut connection)?
        .into_iter()
        .collect::<HashMap<_, _>>();

    let mut redis_con = redis_pool.get()?;
    // A6: SCAN (non-blocking, cursor-based) instead of KEYS (O(N), blocks the Redis server).
    // This runs on every /stats and /downloads poll, so it must not stall Redis for other
    // clients. `collect` drains the borrowing iterator before the connection is reused below.
    let keys: Vec<String> = redis_con
        .scan_match::<_, String>("dl:*:progress")?
        .collect::<Result<Vec<String>, _>>()?;
    let mut progress = HashMap::new();
    for key in keys {
        let parts = key.split(':').collect::<Vec<_>>();
        let Some(raw_id) = parts.get(1).and_then(|value| value.parse::<i32>().ok()) else {
            continue;
        };
        let value_type: String = redis::cmd("TYPE").arg(&key).query(&mut redis_con)?;
        if value_type != "hash" {
            continue;
        }
        let data: HashMap<String, String> = redis_con.hgetall(&key)?;
        let redis_progress = redis_progress_from_hash(&data);
        let track_id = redis_progress
            .track_db_id
            .or_else(|| correlations.get(&raw_id).copied())
            .or_else(|| known_ids.contains(&raw_id).then_some(raw_id));
        let Some(track_id) = track_id else { continue };

        progress.insert(track_id, redis_progress);
    }
    Ok(progress)
}

/// Counts a known table. Unknown names are a programmer error — return them as
/// such so misconfigured callers fail loudly instead of silently reporting zero.
fn table_count(connection: &mut PgConnection, table: &str) -> Result<i64, ApiError> {
    let count = match table {
        "search_items" => schema::search_items::table.count().get_result(connection),
        "judge_submissions" => schema::judge_submissions::table
            .count()
            .get_result(connection),
        "downloadable_files" => schema::downloadable_files::table
            .count()
            .get_result(connection),
        "downloaded_file" => schema::downloaded_file::table
            .count()
            .get_result(connection),
        "rejected_track" => schema::rejected_track::table.count().get_result(connection),
        other => {
            return Err(ApiError::Internal(format!(
                "table_count called with unknown table '{other}'"
            )));
        }
    };
    count.map_err(ApiError::from)
}

fn playlist_summary(
    track_count: i64,
    tracks: Vec<TrackResponse>,
    next_cursor: Option<i32>,
) -> PlaylistSummary {
    PlaylistSummary {
        id: "all".to_string(),
        name: "Main Library".to_string(),
        track_count,
        total_size: "Unknown".to_string(),
        quality: "Live".to_string(),
        last_synced: "Live".to_string(),
        cover_art: "/favicon.svg".to_string(),
        tracks,
        next_cursor,
    }
}

fn format_reject_reason(reason: Option<String>, value: Option<String>) -> Option<String> {
    reason.map(|reason| {
        let mut formatted = reason.replace('_', " ").to_uppercase();
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            formatted.push_str(": ");
            formatted.push_str(&value);
        }
        formatted
    })
}

fn config_warnings(state: &AppState) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(warning) = state.config.worker_account_warning() {
        warnings.push(warning);
    }
    if state.config.account_conflict() {
        warnings.push(format!(
            "Soulseek account conflict: sharing service is configured as '{}', which overlaps the worker accounts ({}). Use separate worker and share accounts.",
            state.config.share_username,
            state.config.generated_worker_usernames(state.config.worker_count, &state.config.username_prefix).join(", ")
        ));
    }
    if let Some(warning) = state.config.worker_port_capacity_warning() {
        warnings.push(warning);
    }
    warnings
}

fn report_reject_detail(reason: &str, value: Option<&str>) -> String {
    match reason {
        "already_downloaded" => {
            "A matching file was already present in the downloaded set.".to_string()
        }
        "low_score" => value
            .and_then(|value| value.parse::<f32>().ok())
            .map(|score| {
                format!(
                    "Best candidate score was below threshold ({:.0}%).",
                    score * 100.0
                )
            })
            .unwrap_or_else(|| "Candidate score was below the acceptance threshold.".to_string()),
        "not_music" => value
            .filter(|value| !value.is_empty())
            .map(|value| format!("Candidate was rejected as non-music content: {value}"))
            .unwrap_or_else(|| "Candidate was rejected as non-music content.".to_string()),
        "banned" => value
            .filter(|value| !value.is_empty())
            .map(|value| format!("Track or peer is banned: {value}"))
            .unwrap_or_else(|| "Track or peer is banned.".to_string()),
        "abandoned_attempting_search" => {
            "Search attempts were exhausted before a viable candidate was selected.".to_string()
        }
        other => format!("Rejected: {}", other.replace('_', " ")),
    }
}

fn push_event(
    lifecycle: &mut Vec<TrackLifecycleEvent>,
    kind: &str,
    label: &str,
    timestamp: Option<i64>,
    detail: Option<String>,
    source: &str,
) {
    lifecycle.push(TrackLifecycleEvent {
        kind: kind.to_string(),
        label: label.to_string(),
        timestamp,
        detail,
        source: Some(source.to_string()),
    });
}

fn tokenise(value: &str) -> Vec<String> {
    value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|part| part.len() >= 3)
        .map(|part| part.to_lowercase())
        .collect()
}

fn classify_trace_text(text: &str) -> Option<(&'static str, &'static str)> {
    let text = text.to_lowercase();
    if text.contains("reject")
        || text.contains("low_score")
        || text.contains("not_music")
        || text.contains("banned")
    {
        Some(("rejected", "Rejected"))
    } else if text.contains("error")
        || text.contains("failed")
        || text.contains("timed out")
        || text.contains("timeout")
    {
        Some(("error", "Error"))
    } else if text.contains("retry") {
        Some(("retrying", "Retrying"))
    } else if text.contains("completed") || text.contains("downloaded file") {
        Some(("completed", "Completed"))
    } else if text.contains("in_progress")
        || text.contains("downloaded ")
        || text.contains("download progress")
    {
        Some(("download_progress", "Download progress"))
    } else if text.contains("queued") {
        Some(("queued", "Queued"))
    } else if text.contains("select") {
        Some(("selected", "Selected candidate"))
    } else if text.contains("score")
        || text.contains("judge")
        || text.contains("levenshtein")
        || text.contains("relative")
    {
        Some(("scored", "Scored candidate"))
    } else if text.contains("candidate") || text.contains("result") || text.contains("found") {
        Some(("candidate_found", "Candidate found"))
    } else if text.contains("search") {
        Some(("searched", "Searched"))
    } else {
        None
    }
}

fn value_to_match_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        other => other.to_string(),
    }
}

fn trace_matches(text: &str, filters: &[String]) -> Option<String> {
    let lower = text.to_lowercase();
    filters
        .iter()
        .filter(|filter| filter.len() >= 3)
        .find(|filter| lower.contains(filter.as_str()))
        .cloned()
}

fn analyzed_trace_events(
    payload: &serde_json::Value,
    filters: &[String],
) -> Vec<TrackLifecycleEvent> {
    let mut events = Vec::new();
    let Some(traces) = payload.get("data").and_then(|value| value.as_array()) else {
        return events;
    };

    for trace in traces {
        let Some(spans) = trace.get("spans").and_then(|value| value.as_array()) else {
            continue;
        };
        for span in spans {
            let operation = span
                .get("operationName")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let mut span_text = operation.to_string();
            if let Some(tags) = span.get("tags").and_then(|value| value.as_array()) {
                for tag in tags {
                    if let Some(value) = tag.get("value") {
                        span_text.push(' ');
                        span_text.push_str(&value_to_match_text(value));
                    }
                }
            }

            if let Some(matched) = trace_matches(&span_text, filters)
                && let Some((kind, label)) = classify_trace_text(&span_text)
            {
                events.push(TrackLifecycleEvent {
                    kind: kind.to_string(),
                    label: label.to_string(),
                    timestamp: span
                        .get("startTime")
                        .and_then(|value| value.as_i64())
                        .map(|timestamp| timestamp / 1000),
                    detail: Some(format!("Trace matched {matched}.")),
                    source: Some("trace".to_string()),
                });
            }

            if let Some(span_logs) = span.get("logs").and_then(|value| value.as_array()) {
                for log in span_logs {
                    let Some(fields) = log.get("fields").and_then(|value| value.as_array()) else {
                        continue;
                    };
                    let mut log_text = String::new();
                    for field in fields {
                        if let Some(value) = field.get("value") {
                            log_text.push(' ');
                            log_text.push_str(&value_to_match_text(value));
                        }
                    }
                    let Some(matched) = trace_matches(&log_text, filters) else {
                        continue;
                    };
                    let Some((kind, label)) = classify_trace_text(&log_text) else {
                        continue;
                    };
                    events.push(TrackLifecycleEvent {
                        kind: kind.to_string(),
                        label: label.to_string(),
                        timestamp: log
                            .get("timestamp")
                            .and_then(|value| value.as_i64())
                            .map(|timestamp| timestamp / 1000),
                        detail: Some(format!("Trace matched {matched}.")),
                        source: Some("trace".to_string()),
                    });
                }
            }
        }
    }

    events.sort_by_key(|left| left.timestamp);
    events.dedup_by(|left, right| {
        left.kind == right.kind && left.timestamp == right.timestamp && left.detail == right.detail
    });
    events.truncate(20);
    events
}

#[get("/health")]
pub async fn health(state: web::Data<AppState>) -> impl actix_web::Responder {
    let mut response = HealthResponse {
        api: "ONLINE",
        db: "DISCONNECTED",
        tables: HashMap::new(),
        redis: "OFFLINE",
        jaeger: "OFFLINE",
        account_conflict: state.config.account_conflict(),
        warnings: config_warnings(&state),
        error: None,
    };

    match state.db_pool.get() {
        Ok(mut connection) => {
            if diesel::sql_query("SELECT 1")
                .execute(&mut connection)
                .is_ok()
            {
                response.db = "CONNECTED";
                for table in [
                    "search_items",
                    "judge_submissions",
                    "downloadable_files",
                    "downloaded_file",
                    "rejected_track",
                ] {
                    let table_exists = diesel::sql_query(
                        "SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = $1
                        ) AS exists",
                    )
                    .bind::<Text, _>(table)
                    .get_result::<ExistsRow>(&mut connection)
                    .map(|row| row.exists)
                    .unwrap_or(false);
                    response.tables.insert(table.to_string(), table_exists);
                }
            }
        }
        Err(err) => response.error = Some(err.to_string()),
    }

    if state.redis_pool.get().is_ok() {
        response.redis = "CONNECTED";
    }

    let jaeger_url = format!(
        "{}/api/services",
        state.config.jaeger_url.trim_end_matches('/')
    );
    if let Ok(Ok(fetch_response)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), reqwest::get(jaeger_url)).await
        && fetch_response.status().is_success()
    {
        response.jaeger = "ONLINE";
    }

    HttpResponse::Ok().json(response)
}

#[get("/stats")]
pub async fn stats(state: web::Data<AppState>) -> ApiResult<HttpResponse> {
    let mut connection = state.db_pool.get()?;
    let mut table_counts = HashMap::new();
    for table in [
        "search_items",
        "judge_submissions",
        "downloaded_file",
        "rejected_track",
    ] {
        table_counts.insert(table.to_string(), table_count(&mut connection, table)?);
    }
    let total_tracks = table_counts.get("search_items").copied().unwrap_or(0);
    let completed = table_counts.get("downloaded_file").copied().unwrap_or(0);
    let failed = table_counts.get("rejected_track").copied().unwrap_or(0);
    let downloading = redis_progress_map(&state.redis_pool, &state.db_pool)
        .map(|value| {
            value
                .values()
                .filter(|redis_progress| !redis_progress.finished)
                .count()
        })
        .unwrap_or(0);
    let pending = (total_tracks - completed - failed).max(0);
    let global_progress = if total_tracks > 0 {
        ((completed as f64 / total_tracks as f64) * 100.0).round() as i64
    } else {
        0
    };

    Ok(HttpResponse::Ok().json(StatsResponse {
        total_tracks,
        pending,
        downloading,
        completed,
        failed,
        global_progress,
        remaining_time: "Live Sync",
        table_counts,
    }))
}

#[get("/network")]
pub async fn network(state: web::Data<AppState>) -> impl actix_web::Responder {
    HttpResponse::Ok().json(NetworkResponse {
        status: "CONNECTED",
        user: state.config.username_prefix.clone(),
        latency: "0ms",
        node: "Soulseek-Native",
        total_bandwidth: "Live",
    })
}

#[get("/config")]
pub async fn config(state: web::Data<AppState>) -> impl actix_web::Responder {
    let tuning = WorkerTuning::from_env();
    let runtime_config = state.config.clone();
    let port_last = state
        .config
        .port_base
        .saturating_add(state.config.worker_count.saturating_sub(1) as u16);
    let published_port_last = state.config.worker_port_published_last();
    let share_mode = runtime_config.share_mode.clone();
    let share_path = runtime_config.share_path.clone();
    let share_status = match share_mode.as_str() {
        "external" if state.config.account_conflict() => "account_conflict",
        "external" => "external_client_required",
        "disabled" => "disabled",
        _ => "invalid_mode",
    }
    .to_string();
    HttpResponse::Ok().json(ConfigResponse {
        judge_threshold: JUDGE_THRESHOLD,
        auth: AuthConfig {
            scheme: "api_key",
            header: "X-API-Key",
        },
        tuning: TuningConfig {
            search_concurrency: tuning.search_concurrency,
            download_concurrency: tuning.download_concurrency,
            search_timeout_secs: runtime_config.search_timeout_secs,
            search_empty_result_cutoff: runtime_config.search_empty_result_cutoff,
            max_candidates_per_track: tuning.max_candidates_per_track,
            max_download_attempts_per_track: tuning.max_download_attempts_per_track,
            candidate_collection_secs: tuning.candidate_collection_secs,
            max_search_passes_per_track: tuning.max_search_passes_per_track,
            max_requests_per_track: tuning.max_requests_per_track,
            retry_backoff_ms: tuning.retry_backoff_ms,
            search_pacing_ms: tuning.search_pacing_ms,
            peer_cooldown_secs: tuning.peer_cooldown_secs,
            worker_port_range: format!("{}-{port_last}", state.config.port_base),
            worker_account_mode: state.config.worker_account_mode.clone(),
            worker_username: state.config.username_prefix.clone(),
            worker_username_pattern: state.config.worker_username_pattern(),
            share_username: state.config.share_username.clone(),
            account_conflict: state.config.account_conflict(),
            default_worker_count: state.config.worker_count,
            default_port_base: state.config.port_base,
            worker_port_published_range: format!(
                "{}-{published_port_last}",
                state.config.worker_port_published_base
            ),
            default_run_id_prefix: state.config.run_id_prefix.clone(),
            share_mode,
            share_path,
            share_status,
            config_warnings: config_warnings(&state),
        },
    })
}

#[get("/playlists")]
pub async fn playlists(state: web::Data<AppState>) -> ApiResult<HttpResponse> {
    let mut connection = state.db_pool.get()?;
    let count = schema::search_items::table
        .count()
        .get_result::<i64>(&mut connection)
        .unwrap_or(0);
    Ok(HttpResponse::Ok().json(vec![playlist_summary(count, Vec::new(), None)]))
}

#[get("/playlists/{id}")]
pub async fn playlist(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<PlaylistQuery>,
) -> ApiResult<HttpResponse> {
    if path.as_str() != "all" {
        return Err(ApiError::NotFound("Playlist not found"));
    }
    let pagination = query.into_inner().validate()?;
    let mut connection = state.db_pool.get()?;

    let total: i64 = schema::search_items::table
        .count()
        .get_result(&mut connection)
        .unwrap_or(0);

    let cursor_sql = if pagination.cursor.is_some() {
        "AND si.id < $1"
    } else {
        ""
    };
    let limit_param = if pagination.cursor.is_some() {
        "$2"
    } else {
        "$1"
    };

    let sql = format!(
        r#"
        SELECT
            si.id,
            si.track_id,
            si.track AS title,
            si.artist,
            si.album,
            (
                SELECT rt.reason::text
                FROM rejected_track rt
                JOIN judge_submissions js4 ON rt.track = js4.id
                WHERE js4.track = si.id
                ORDER BY rt.id DESC
                LIMIT 1
            ) AS reject_reason,
            (
                SELECT rt.value
                FROM rejected_track rt
                JOIN judge_submissions js4 ON rt.track = js4.id
                WHERE js4.track = si.id
                ORDER BY rt.id DESC
                LIMIT 1
            ) AS reject_value,
            (SELECT COUNT(*) FROM judge_submissions js WHERE js.track = si.id) AS candidates_count,
            (SELECT MAX(js.score) FROM judge_submissions js WHERE js.track = si.id) AS max_score,
            (SELECT MAX(js.relative_mi_score) FROM judge_submissions js WHERE js.track = si.id) AS max_relative_mi_score,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM downloaded_file df
                    WHERE df.filename IN (
                        SELECT dlf.filename FROM downloadable_files dlf
                        JOIN judge_submissions js2 ON dlf.id = js2.query
                        WHERE js2.track = si.id
                    )
                ) THEN 'COMPLETED'
                WHEN EXISTS (
                    SELECT 1 FROM rejected_track rt
                    JOIN judge_submissions js5 ON rt.track = js5.id
                    WHERE js5.track = si.id
                ) THEN 'FAILED'
                WHEN EXISTS (SELECT 1 FROM judge_submissions js3 WHERE js3.track = si.id) THEN 'FILTERING'
                ELSE 'IN_QUEUE'
            END AS track_status
        FROM search_items si
        WHERE 1=1 {cursor_sql}
        ORDER BY si.id DESC
        LIMIT {limit_param}
    "#
    );

    let rows = if let Some(cursor) = pagination.cursor {
        diesel::sql_query(sql)
            .bind::<Integer, _>(cursor)
            .bind::<diesel::sql_types::BigInt, _>(pagination.limit)
            .load::<TrackQueryRow>(&mut connection)?
    } else {
        diesel::sql_query(sql)
            .bind::<diesel::sql_types::BigInt, _>(pagination.limit)
            .load::<TrackQueryRow>(&mut connection)?
    };
    let progress_map = redis_progress_map(&state.redis_pool, &state.db_pool).unwrap_or_default();
    let next_cursor = rows.last().map(|row| row.id).filter(|_| {
        // Only return a cursor if we filled the page; otherwise there's nothing
        // more to fetch.
        rows.len() as i64 >= pagination.limit
    });
    let tracks = rows
        .into_iter()
        .map(|row| {
            let mut track_status = row.track_status;
            let mut progress = if track_status == "COMPLETED" { 100 } else { 0 };
            let mut download_status = None;
            let mut filename = None;
            let mut username = None;
            if track_status != "COMPLETED"
                && let Some(redis_progress) = progress_map.get(&row.id)
            {
                progress = redis_progress.progress;
                download_status = redis_progress.status.clone();
                filename = redis_progress.filename.clone();
                username = redis_progress.username.clone();
                track_status = if redis_progress.finished {
                    "FINALIZING".to_string()
                } else {
                    "DOWNLOADING".to_string()
                };
            }
            TrackResponse {
                id: row.id,
                track_id: row.track_id,
                title: row.title,
                artist: row.artist,
                album: row.album,
                status: track_status,
                progress,
                score: row.max_score,
                relative_mi_score: row.max_relative_mi_score,
                candidates_count: row.candidates_count,
                reject_reason: format_reject_reason(row.reject_reason, row.reject_value),
                download_status,
                filename,
                username,
            }
        })
        .collect::<Vec<_>>();
    Ok(HttpResponse::Ok().json(playlist_summary(total, tracks, next_cursor)))
}

#[get("/tracks/{id}/candidates")]
pub async fn candidates(
    state: web::Data<AppState>,
    path: web::Path<i32>,
) -> ApiResult<HttpResponse> {
    let mut connection = state.db_pool.get()?;
    let rows = diesel::sql_query(
        r#"
            SELECT js.id AS submission_id, dlf.id AS file_id, dlf.username, dlf.filename, js.score, js.relative_mi_score
            FROM judge_submissions js
            JOIN downloadable_files dlf ON js.query = dlf.id
            WHERE js.track = $1
            ORDER BY js.score DESC NULLS LAST
        "#,
    )
    .bind::<Integer, _>(*path)
    .load::<CandidateQueryRow>(&mut connection)?;
    let response = rows
        .into_iter()
        .map(|row| CandidateResponse {
            id: row.submission_id,
            file_id: row.file_id,
            username: row.username,
            filename: row.filename,
            score: row.score.unwrap_or(0.0),
            relative_mi_score: row.relative_mi_score,
        })
        .collect::<Vec<_>>();
    Ok(HttpResponse::Ok().json(response))
}

#[get("/tracks/{id}/report")]
pub async fn track_report(
    state: web::Data<AppState>,
    path: web::Path<i32>,
) -> ApiResult<HttpResponse> {
    let track_db_id = *path;
    let mut connection = state.db_pool.get()?;
    let track = diesel::sql_query(
        r#"
            SELECT id, track_id, track AS title, artist, album
            FROM search_items
            WHERE id = $1
        "#,
    )
    .bind::<Integer, _>(track_db_id)
    .get_result::<TrackIdentityRow>(&mut connection)
    .optional()?
    .ok_or(ApiError::NotFound("Track not found"))?;

    let candidate_rows = diesel::sql_query(
        r#"
            SELECT
                js.id AS submission_id,
                dlf.id AS file_id,
                dlf.username,
                dlf.filename,
                dlf.size,
                js.score,
                js.relative_mi_score,
                EXISTS (
                    SELECT 1 FROM retry_request rr
                    WHERE rr.request = js.id OR rr.failed_download_result = dlf.id
                ) AS attempted,
                EXISTS (
                    SELECT 1 FROM downloaded_file df
                    WHERE df.track = js.track OR df.filename = dlf.filename
                ) AS downloaded,
                EXISTS (
                    SELECT 1 FROM rejected_track rt
                    WHERE rt.track = js.id
                ) AS rejected
            FROM judge_submissions js
            JOIN downloadable_files dlf ON js.query = dlf.id
            WHERE js.track = $1
            ORDER BY js.score DESC NULLS LAST, js.id ASC
        "#,
    )
    .bind::<Integer, _>(track_db_id)
    .load::<TrackReportCandidateRow>(&mut connection)?;

    let downloaded = diesel::sql_query(
        r#"
            SELECT df.filename
            FROM downloaded_file df
            WHERE df.track = $1
               OR df.filename IN (
                    SELECT dlf.filename
                    FROM downloadable_files dlf
                    JOIN judge_submissions js ON js.query = dlf.id
                    WHERE js.track = $1
               )
            ORDER BY df.id DESC
            LIMIT 1
        "#,
    )
    .bind::<Integer, _>(track_db_id)
    .get_result::<TrackReportDownloadRow>(&mut connection)
    .optional()?;

    let rejection_rows = diesel::sql_query(
        r#"
            SELECT rt.id, js.id AS candidate_id, rt.reason::text AS reason, rt.value
            FROM rejected_track rt
            JOIN judge_submissions js ON rt.track = js.id
            WHERE js.track = $1
            ORDER BY rt.id DESC
            LIMIT 10
        "#,
    )
    .bind::<Integer, _>(track_db_id)
    .load::<TrackReportRejectionRow>(&mut connection)?;

    let retry_rows = diesel::sql_query(
        r#"
            SELECT
                rr.id,
                js.id AS candidate_id,
                dlf.id AS failed_file_id,
                rr.retry_attempts,
                dlf.filename,
                dlf.username
            FROM retry_request rr
            JOIN judge_submissions js ON rr.request = js.id
            JOIN downloadable_files dlf ON rr.failed_download_result = dlf.id
            WHERE js.track = $1
            ORDER BY rr.id DESC
            LIMIT 10
        "#,
    )
    .bind::<Integer, _>(track_db_id)
    .load::<TrackReportRetryRow>(&mut connection)?;

    let redis_progress = redis_progress_map(&state.redis_pool, &state.db_pool)
        .ok()
        .and_then(|progress| progress.get(&track_db_id).cloned());

    let accepted_candidate_ids = candidate_rows
        .iter()
        .filter(|row| row.score.unwrap_or(0.0) >= JUDGE_THRESHOLD)
        .map(|row| row.submission_id)
        .collect::<HashSet<_>>();
    let mut lifecycle = Vec::new();
    push_event(
        &mut lifecycle,
        "searched",
        "Track registered",
        None,
        Some(format!("{} by {}", track.title, track.artist)),
        "database",
    );
    if !candidate_rows.is_empty() {
        push_event(
            &mut lifecycle,
            "candidate_found",
            "Candidates found",
            None,
            Some(format!(
                "{} candidate files recorded.",
                candidate_rows.len()
            )),
            "database",
        );
        push_event(
            &mut lifecycle,
            "scored",
            "Candidates scored",
            None,
            Some(
                "Ranked by Levenshtein score; Relative MI is shown as experimental context."
                    .to_string(),
            ),
            "database",
        );
    }
    if let Some(best) = candidate_rows.first()
        && best.score.unwrap_or(0.0) >= JUDGE_THRESHOLD
    {
        push_event(
            &mut lifecycle,
            "selected",
            "Candidate selected",
            None,
            Some(format!("{} from {}", best.filename, best.username)),
            "database",
        );
    }
    if let Some(progress) = &redis_progress {
        push_event(
            &mut lifecycle,
            if progress.finished {
                "completed"
            } else {
                "download_progress"
            },
            if progress.finished {
                "Redis marked completed"
            } else {
                "Active download progress"
            },
            None,
            Some(format!(
                "{}%{}",
                progress.progress,
                progress
                    .status
                    .as_ref()
                    .map(|status| format!(" ({})", status.replace('_', " ")))
                    .unwrap_or_default()
            )),
            "redis",
        );
    }
    for retry in &retry_rows {
        push_event(
            &mut lifecycle,
            "retrying",
            "Retry requested",
            None,
            Some(format!(
                "{} after {} attempt(s).",
                retry.filename, retry.retry_attempts
            )),
            "database",
        );
    }
    for rejection in &rejection_rows {
        push_event(
            &mut lifecycle,
            "rejected",
            "Rejected",
            None,
            Some(report_reject_detail(
                &rejection.reason,
                rejection.value.as_deref(),
            )),
            "database",
        );
    }
    if let Some(downloaded) = &downloaded {
        push_event(
            &mut lifecycle,
            "completed",
            "Downloaded file recorded",
            None,
            Some(downloaded.filename.clone()),
            "database",
        );
    }

    let mut filters = vec![track_db_id.to_string(), track.track_id.to_lowercase()];
    filters.extend(tokenise(&track.title));
    filters.extend(tokenise(&track.artist));
    filters.extend(
        candidate_rows
            .iter()
            .flat_map(|row| {
                [
                    row.submission_id.to_string(),
                    row.file_id.to_string(),
                    row.filename.to_lowercase(),
                    row.username.to_lowercase(),
                ]
            })
            .collect::<Vec<_>>(),
    );
    filters.sort();
    filters.dedup();

    let mut trace_analysis_unavailable = false;
    let mut trace_note = None;
    let traces_url = format!(
        "{}/api/traces?service=convert-invert&limit=80",
        state.config.jaeger_url.trim_end_matches('/')
    );
    match tokio::time::timeout(std::time::Duration::from_secs(2), reqwest::get(traces_url)).await {
        Ok(Ok(response)) if response.status().is_success() => match response.text().await {
            Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(payload) => lifecycle.extend(analyzed_trace_events(&payload, &filters)),
                Err(_) => {
                    trace_analysis_unavailable = true;
                    trace_note = Some(
                        "Trace analysis unavailable; Jaeger returned an unreadable payload."
                            .to_string(),
                    );
                }
            },
            Err(_) => {
                trace_analysis_unavailable = true;
                trace_note = Some(
                    "Trace analysis unavailable; Jaeger response could not be read.".to_string(),
                );
            }
        },
        _ => {
            trace_analysis_unavailable = true;
            trace_note = Some(
                "Trace analysis unavailable; showing database and Redis state only.".to_string(),
            );
        }
    }

    lifecycle.sort_by_key(|left| left.timestamp);
    lifecycle.dedup_by(|left, right| {
        left.kind == right.kind && left.label == right.label && left.detail == right.detail
    });

    let completed = downloaded.is_some();
    let has_rejections = !rejection_rows.is_empty();
    let status = if completed {
        TrackReportStatus {
            stage: "COMPLETED".to_string(),
            progress: 100,
            filename: downloaded.as_ref().map(|row| row.filename.clone()),
            peer: None,
            detail: Some("Downloaded file has been recorded in the database.".to_string()),
        }
    } else if let Some(progress) = &redis_progress {
        TrackReportStatus {
            stage: if progress.finished {
                "FINALIZING"
            } else {
                "DOWNLOADING"
            }
            .to_string(),
            progress: progress.progress,
            filename: progress.filename.clone(),
            peer: progress.username.clone(),
            detail: progress
                .status
                .as_ref()
                .map(|status| status.replace('_', " ")),
        }
    } else if has_rejections {
        TrackReportStatus {
            stage: "FAILED".to_string(),
            progress: 0,
            filename: None,
            peer: None,
            detail: rejection_rows
                .first()
                .map(|row| report_reject_detail(&row.reason, row.value.as_deref())),
        }
    } else if candidate_rows.is_empty() {
        TrackReportStatus {
            stage: "SEARCHING".to_string(),
            progress: 0,
            filename: None,
            peer: None,
            detail: Some("No candidate rows have been recorded yet.".to_string()),
        }
    } else {
        TrackReportStatus {
            stage: "FILTERING".to_string(),
            progress: 0,
            filename: candidate_rows.first().map(|row| row.filename.clone()),
            peer: candidate_rows.first().map(|row| row.username.clone()),
            detail: Some(
                "Candidates are available and awaiting download or rejection.".to_string(),
            ),
        }
    };

    let status_detail = status.detail.as_deref().unwrap_or_default();
    let (severity, diagnosis, next_action) = match status.stage.as_str() {
        "COMPLETED" => (
            "info",
            "Download completed and persisted.".to_string(),
            "No action needed.".to_string(),
        ),
        "DOWNLOADING" | "FINALIZING" if status_detail.eq_ignore_ascii_case("retrying") => (
            "warning",
            "Download stalled and is waiting for a retry.".to_string(),
            "Wait for the worker to select the next candidate or inspect retries.".to_string(),
        ),
        "DOWNLOADING" | "FINALIZING" if status_detail.eq_ignore_ascii_case("queued") => (
            "info",
            "Download is queued with the peer.".to_string(),
            "Wait for the peer to start transferring data.".to_string(),
        ),
        "DOWNLOADING" | "FINALIZING" => (
            "info",
            format!("Download is active at {}%.", status.progress),
            "Wait for completion or retry signal.".to_string(),
        ),
        "FAILED" => (
            "error",
            status
                .detail
                .clone()
                .unwrap_or_else(|| "Track failed during matching or download.".to_string()),
            "Inspect rejected candidates or reprocess the track.".to_string(),
        ),
        "FILTERING" if accepted_candidate_ids.is_empty() => (
            "warning",
            "Candidates exist, but none currently clear the Levenshtein threshold.".to_string(),
            "Review the best-ranked candidate or let the worker retry.".to_string(),
        ),
        "FILTERING" => (
            "info",
            "A candidate meets the configured score threshold.".to_string(),
            "Worker should queue the selected candidate for download.".to_string(),
        ),
        _ => (
            "warning",
            "No candidate metadata has been recorded yet.".to_string(),
            "Wait for search results or start/restart workers.".to_string(),
        ),
    };

    let report_candidates = candidate_rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let score = row.score.unwrap_or(0.0);
            TrackReportCandidate {
                id: row.submission_id,
                file_id: row.file_id,
                rank: index + 1,
                username: row.username,
                filename: row.filename,
                size: row.size,
                score,
                relative_mi_score: row.relative_mi_score,
                classification: if row.downloaded {
                    "downloaded"
                } else if row.rejected {
                    "rejected"
                } else if score >= JUDGE_THRESHOLD {
                    "accepted"
                } else {
                    "below_threshold"
                }
                .to_string(),
                attempted: row.attempted,
                downloaded: row.downloaded,
                rejected: row.rejected,
            }
        })
        .collect::<Vec<_>>();

    let rejections = rejection_rows
        .into_iter()
        .map(|row| TrackReportRejection {
            id: row.id,
            reason: row.reason.replace('_', " "),
            value: row.value.clone(),
            detail: report_reject_detail(&row.reason, row.value.as_deref()),
            candidate_id: row.candidate_id,
        })
        .collect::<Vec<_>>();

    let retries = retry_rows
        .into_iter()
        .map(|row| TrackReportRetry {
            id: row.id,
            candidate_id: row.candidate_id,
            failed_file_id: row.failed_file_id,
            retry_attempts: row.retry_attempts,
            filename: row.filename.clone(),
            peer: row.username.clone(),
            detail: format!(
                "Retry attempt {} after failed download from {}.",
                row.retry_attempts, row.username
            ),
        })
        .collect::<Vec<_>>();

    let download = if let Some(row) = downloaded {
        Some(TrackReportDownload {
            filename: row.filename,
            peer: None,
            status: Some("completed".to_string()),
            progress: Some(100),
            completed: true,
        })
    } else {
        redis_progress.map(|progress| TrackReportDownload {
            filename: progress
                .filename
                .unwrap_or_else(|| "Unknown file".to_string()),
            peer: progress.username,
            status: progress.status,
            progress: Some(progress.progress),
            completed: progress.finished,
        })
    };

    Ok(HttpResponse::Ok().json(TrackReportResponse {
        track: TrackReportTrack {
            id: track.id,
            track_id: track.track_id,
            title: track.title,
            artist: track.artist,
            album: track.album,
        },
        status,
        summary: TrackReportSummary {
            severity,
            diagnosis,
            next_action,
        },
        lifecycle,
        candidates: report_candidates,
        rejections,
        retries,
        download,
        trace_analysis_unavailable,
        trace_note,
    }))
}

#[get("/logs")]
pub async fn logs(state: web::Data<AppState>) -> impl actix_web::Responder {
    let traces_url = format!(
        "{}/api/traces?service=convert-invert&limit=20",
        state.config.jaeger_url.trim_end_matches('/')
    );
    let Ok(Ok(response)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), reqwest::get(traces_url)).await
    else {
        return HttpResponse::Ok().json(Vec::<LogEntry>::new());
    };
    let Ok(body) = response.text().await else {
        return HttpResponse::Ok().json(Vec::<LogEntry>::new());
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&body) else {
        return HttpResponse::Ok().json(Vec::<LogEntry>::new());
    };
    let mut logs = Vec::new();
    if let Some(traces) = payload.get("data").and_then(|value| value.as_array()) {
        for trace in traces {
            let Some(spans) = trace.get("spans").and_then(|value| value.as_array()) else {
                continue;
            };
            for span in spans {
                let span_id = span
                    .get("spanID")
                    .and_then(|value| value.as_str())
                    .unwrap_or("span");
                let operation = span
                    .get("operationName")
                    .and_then(|value| value.as_str())
                    .unwrap_or("operation");
                let timestamp = span
                    .get("startTime")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    / 1000;
                logs.push(LogEntry {
                    id: format!("{span_id}-start"),
                    timestamp,
                    message: format!("[SPAN] {operation} started"),
                    level: "info".to_string(),
                });
                if let Some(span_logs) = span.get("logs").and_then(|value| value.as_array()) {
                    for (idx, log) in span_logs.iter().enumerate() {
                        let message = log
                            .get("fields")
                            .and_then(|value| value.as_array())
                            .and_then(|fields| {
                                fields.iter().find_map(|field| {
                                    let key = field.get("key").and_then(|value| value.as_str())?;
                                    if key == "message" || key == "event" {
                                        field.get("value").map(|value| value.to_string())
                                    } else {
                                        None
                                    }
                                })
                            });
                        if let Some(message) = message {
                            logs.push(LogEntry {
                                id: format!("{span_id}-log-{idx}"),
                                timestamp: log
                                    .get("timestamp")
                                    .and_then(|value| value.as_i64())
                                    .unwrap_or(0)
                                    / 1000,
                                message,
                                level: "debug".to_string(),
                            });
                        }
                    }
                }
            }
        }
    }
    logs.sort_by_key(|right| std::cmp::Reverse(right.timestamp));
    logs.truncate(50);
    HttpResponse::Ok().json(logs)
}

#[derive(Serialize)]
pub struct DownloadedFile {
    pub name: String,
    pub size: u64,
    pub modified: u64,
}

#[get("/downloads")]
pub async fn downloads(state: web::Data<AppState>) -> ApiResult<HttpResponse> {
    let path = &state.config.download_path;
    let mut files = Vec::new();

    if path.exists()
        && path.is_dir()
        && let Ok(entries) = std::fs::read_dir(path)
    {
        for entry in entries.flatten() {
            let file_path = entry.path();
            if file_path.is_file() {
                let name = file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();

                if name.starts_with('.') {
                    continue;
                }

                let metadata = entry.metadata();
                let size = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                let modified = metadata
                    .as_ref()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                files.push(DownloadedFile {
                    name,
                    size,
                    modified,
                });
            }
        }
    }

    files.sort_by_key(|b| std::cmp::Reverse(b.modified));
    Ok(HttpResponse::Ok().json(files))
}

#[cfg(test)]
mod tests {
    use super::{analyzed_trace_events, redis_progress_from_hash};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn redis_progress_maps_queued_hash_as_active_zero_percent() {
        let data = HashMap::from([
            ("status".to_string(), "queued".to_string()),
            ("track_db_id".to_string(), "42".to_string()),
            ("filename".to_string(), "song.flac".to_string()),
            ("username".to_string(), "peer".to_string()),
            ("bytes_downloaded".to_string(), "0".to_string()),
            ("total_bytes".to_string(), "1000".to_string()),
            ("completed".to_string(), "false".to_string()),
        ]);

        let progress = redis_progress_from_hash(&data);

        assert_eq!(progress.progress, 0);
        assert!(!progress.finished);
        assert_eq!(progress.status.as_deref(), Some("queued"));
        assert_eq!(progress.track_db_id, Some(42));
        assert_eq!(progress.filename.as_deref(), Some("song.flac"));
        assert_eq!(progress.username.as_deref(), Some("peer"));
    }

    #[test]
    fn redis_progress_maps_completed_hash_to_done() {
        let data = HashMap::from([
            ("status".to_string(), "completed".to_string()),
            ("bytes_downloaded".to_string(), "100".to_string()),
            ("total_bytes".to_string(), "100".to_string()),
            ("completed".to_string(), "true".to_string()),
        ]);

        let progress = redis_progress_from_hash(&data);

        assert_eq!(progress.progress, 100);
        assert!(progress.finished);
        assert_eq!(progress.status.as_deref(), Some("completed"));
    }

    #[test]
    fn jaeger_analyzer_returns_normalized_events_without_raw_messages() {
        let payload = json!({
            "data": [{
                "spans": [{
                    "spanID": "abc",
                    "operationName": "DownloadManager::download_track",
                    "startTime": 1_700_000_000_000_000i64,
                    "tags": [
                        { "key": "song_name", "value": "Artist - Track.flac" }
                    ],
                    "logs": [{
                        "timestamp": 1_700_000_001_000_000i64,
                        "fields": [
                            { "key": "message", "value": "Downloaded 512 of 1024 at 22 B/s for Artist - Track.flac" }
                        ]
                    }, {
                        "timestamp": 1_700_000_002_000_000i64,
                        "fields": [
                            { "key": "message", "value": "Retry requested for Artist - Track.flac after peer disconnect" }
                        ]
                    }]
                }]
            }]
        });
        let filters = vec!["artist - track.flac".to_string()];

        let events = analyzed_trace_events(&payload, &filters);

        assert!(events.iter().any(|event| event.kind == "download_progress"));
        assert!(events.iter().any(|event| event.kind == "retrying"));
        assert!(
            events
                .iter()
                .filter_map(|event| event.detail.as_deref())
                .all(|detail| !detail.contains("Downloaded 512 of 1024"))
        );
    }
}
