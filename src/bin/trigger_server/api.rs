use std::collections::{HashMap, HashSet};

use actix_web::{HttpResponse, get, web};
use convert_invert::internals::context::context_manager::RedisPool;
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
}

#[derive(Serialize)]
pub struct AuthConfig {
    pub scheme: &'static str,
    pub header: &'static str,
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
    #[diesel(sql_type = Text)]
    track_status: String,
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
    let keys: Vec<String> = redis_con.keys("dl:*:progress")?;
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

#[get("/health")]
pub async fn health(state: web::Data<AppState>) -> impl actix_web::Responder {
    let mut response = HealthResponse {
        api: "ONLINE",
        db: "DISCONNECTED",
        tables: HashMap::new(),
        redis: "OFFLINE",
        jaeger: "OFFLINE",
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
pub async fn network() -> impl actix_web::Responder {
    HttpResponse::Ok().json(NetworkResponse {
        status: "CONNECTED",
        user: std::env::var("USER_NAME").unwrap_or_else(|_| "default".to_string()),
        latency: "0ms",
        node: "Soulseek-Native",
        total_bandwidth: "Live",
    })
}

#[get("/config")]
pub async fn config() -> impl actix_web::Responder {
    HttpResponse::Ok().json(ConfigResponse {
        judge_threshold: JUDGE_THRESHOLD,
        auth: AuthConfig {
            scheme: "api_key",
            header: "X-API-Key",
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
                ELSE 'SEARCHING'
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
            SELECT js.id AS submission_id, dlf.id AS file_id, dlf.username, dlf.filename, js.score
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
        })
        .collect::<Vec<_>>();
    Ok(HttpResponse::Ok().json(response))
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
    logs.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
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

    files.sort_by(|a, b| b.modified.cmp(&a.modified));
    Ok(HttpResponse::Ok().json(files))
}

#[cfg(test)]
mod tests {
    use super::redis_progress_from_hash;
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
}
