use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use convert_invert::internals::{
    context::context_manager::{Managers, Track},
    database::establish_connection,
    query::query_manager::QueryManager,
    search::search_manager::SearchItem,
    utils::config::config_manager::Config,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

#[derive(Clone)]
struct AppConfig {
    bind: String,
    worker_count: usize,
    username_prefix: String,
    port_base: u16,
    run_id_prefix: String,
    download_path: PathBuf,
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(Serialize, Clone)]
struct WorkerInfo {
    id: usize,
    username: String,
    port: u16,
    run_id: String,
    started_at_epoch_secs: u64,
}

struct WorkerHandle {
    info: WorkerInfo,
    handle: JoinHandle<()>,
}

struct AppState {
    workers: Mutex<Vec<WorkerHandle>>,
    config: AppConfig,
    next_worker_id: Mutex<usize>,
    queue_key: Mutex<Option<String>>,
}

#[derive(Deserialize)]
struct StartRequest {
    worker_count: Option<usize>,
    username_prefix: Option<String>,
    port_base: Option<u16>,
    run_id_prefix: Option<String>,
    playlist_id: Option<String>,
    chunk_size: Option<usize>,
    playlist_range_start: Option<usize>,
    playlist_range_end: Option<usize>,
}

#[derive(Deserialize)]
struct StopRequest {
    pids: Option<Vec<u32>>,
}

#[derive(Serialize)]
struct StatusResponse {
    workers: Vec<WorkerInfo>,
    queue_len: usize,
    failed_count: usize,
}

fn chunk_queue_key(playlist_id: &str, chunk_size: usize) -> String {
    format!("dl:chunk_queue:{playlist_id}:{chunk_size}")
}

fn build_chunks(items: &[SearchItem], chunk_size: usize) -> Vec<Vec<SearchItem>> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < items.len() {
        let end = (start + chunk_size).min(items.len());
        chunks.push(items[start..end].to_vec());
        start = end;
    }
    chunks
}

async fn run_worker(
    worker_config: Config,
    playlist_id: String,
    chunk_size: usize,
    download_path: PathBuf,
    redis_client: redis::Client,
    is_leader: bool,
    all_items: Vec<SearchItem>,
) {
    let queue_key = chunk_queue_key(&playlist_id, chunk_size);
    loop {
        let chunk_json: Option<String> = {
            let mut redis_con = match redis_client.get_connection() {
                Ok(con) => con,
                Err(_) => break,
            };
            redis_con.lpop(&queue_key, None).ok()
        };
        let Some(chunk_json) = chunk_json else { break };
        let chunk_items: Vec<SearchItem> = match serde_json::from_str(&chunk_json) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let tracks = chunk_items
            .into_iter()
            .map(Track::Query)
            .collect::<Vec<_>>();
        let managers = Managers::new(
            worker_config.judge_score_levenshtein,
            download_path.clone(),
            worker_config.clone(),
            1.0,
        );
        let connection = &mut establish_connection();
        let _ = managers
            .run_cycle(tracks, connection, redis_client.clone())
            .await;
    }

    if is_leader {
        let failed_ids: Vec<String> = {
            let mut redis_con = match redis_client.get_connection() {
                Ok(con) => con,
                Err(_) => return,
            };
            redis_con.smembers("dl:failed").unwrap_or_default()
        };
        if !failed_ids.is_empty() {
            let failed_items = all_items
                .into_iter()
                .filter(|item| failed_ids.contains(&item.track_id.to_string()))
                .map(Track::Query)
                .collect::<Vec<_>>();
            if !failed_items.is_empty() {
                let managers = Managers::new(
                    worker_config.judge_score_levenshtein,
                    download_path.clone(),
                    worker_config.clone(),
                    2.0,
                );
                let connection = &mut establish_connection();
                let _ = managers
                    .run_cycle(failed_items, connection, redis_client.clone())
                    .await;
            }
        }
    }
}

#[post("/start")]
async fn start_workers(state: web::Data<AppState>, req: web::Json<StartRequest>) -> impl Responder {
    let count = req.worker_count.unwrap_or(state.config.worker_count);
    let username_prefix = req
        .username_prefix
        .clone()
        .unwrap_or_else(|| state.config.username_prefix.clone());
    let port_base = req.port_base.unwrap_or(state.config.port_base);
    let run_id_prefix = req
        .run_id_prefix
        .clone()
        .unwrap_or_else(|| state.config.run_id_prefix.clone());
    let playlist_id = req
        .playlist_id
        .clone()
        .unwrap_or_else(|| "7vdaDB7qkKGbE4abs1iFpQ?si=060b186284b14ad2".to_string());
    let chunk_size = req.chunk_size.unwrap_or(15).max(1);
    let range_start = req.playlist_range_start;
    let range_end = req.playlist_range_end;

    let base_config = match Config::try_from_env() {
        Ok(cfg) => cfg,
        Err(err) => return HttpResponse::InternalServerError().body(err.to_string()),
    };
    let user_password = std::env::var("USER_PASSWORD").unwrap_or_else(|_| "123456".to_string());

    let query_manager = QueryManager::new(
        playlist_id.clone(),
        base_config.client_id.clone(),
        base_config.client_secret.clone(),
    );
    let mut playlist_tracks = match query_manager.fetch_playlist().await {
        Ok(p) => p,
        Err(err) => return HttpResponse::InternalServerError().body(err.to_string()),
    };

    if let (Some(start), Some(end)) = (range_start, range_end) {
        let start = start.min(playlist_tracks.len());
        let end = end.min(playlist_tracks.len());
        if start < end {
            playlist_tracks = playlist_tracks
                .into_iter()
                .skip(start)
                .take(end - start)
                .collect();
        }
    }

    let items = playlist_tracks
        .into_iter()
        .filter_map(|track| match track {
            Track::Query(item) => Some(item),
            _ => None,
        })
        .collect::<Vec<_>>();

    let chunks = build_chunks(&items, chunk_size);
    let queue_key = chunk_queue_key(&playlist_id, chunk_size);
    *state.queue_key.lock().unwrap() = Some(queue_key.clone());
    {
        let mut redis_con = match redis::Client::open("redis://localhost:6379")
            .unwrap()
            .get_connection()
        {
            Ok(con) => con,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Redis connection error: {err}"));
            }
        };
        let _: usize = redis_con.del(queue_key.clone()).unwrap_or(0);
        for chunk in chunks {
            let payload = serde_json::to_string(&chunk).unwrap_or_default();
            let _: usize = redis_con.rpush(&queue_key, payload).unwrap_or(0);
        }
    }

    let mut spawned: Vec<WorkerInfo> = Vec::with_capacity(count);
    let mut guard = state.workers.lock().unwrap();
    let mut id_guard = state.next_worker_id.lock().unwrap();

    for i in 0..count {
        let username = format!("{}{}", username_prefix, i + 1);
        let port = port_base.saturating_add(i as u16);
        let run_id = format!("{}-{}", run_id_prefix, i + 1);
        let started_at_epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let worker_id = *id_guard;
        *id_guard += 1;

        let mut worker_config = base_config.clone();
        worker_config.user_name = username.clone();
        worker_config.user_password = user_password.clone();
        worker_config.listen_port = port as u32;
        worker_config.run_id = run_id.clone();

        let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
        let download_path = state.config.download_path.clone();
        let items_clone = items.clone();
        let playlist_id = playlist_id.clone();
        let is_leader = i == 0;

        let handle = tokio::spawn(run_worker(
            worker_config,
            playlist_id,
            chunk_size,
            download_path,
            redis_client,
            is_leader,
            items_clone,
        ));

        let info = WorkerInfo {
            id: worker_id,
            username,
            port,
            run_id,
            started_at_epoch_secs,
        };
        spawned.push(info.clone());
        guard.push(WorkerHandle { info, handle });
    }

    HttpResponse::Ok().json(spawned)
}

#[post("/stop")]
async fn stop_workers(state: web::Data<AppState>, req: web::Json<StopRequest>) -> impl Responder {
    let mut guard = state.workers.lock().unwrap();
    let target_ids = req.pids.clone();
    let mut stopped: Vec<u32> = Vec::new();
    let mut remaining: Vec<WorkerHandle> = Vec::with_capacity(guard.len());

    for mut entry in guard.drain(..) {
        let should_stop = match &target_ids {
            None => true,
            Some(ids) => ids.contains(&(entry.info.id as u32)),
        };

        if should_stop {
            entry.handle.abort();
            stopped.push(entry.info.id as u32);
        } else {
            remaining.push(entry);
        }
    }

    *guard = remaining;
    HttpResponse::Ok().json(stopped)
}

#[get("/status")]
async fn status(state: web::Data<AppState>) -> impl Responder {
    let mut guard = state.workers.lock().unwrap();
    let mut info: Vec<WorkerInfo> = Vec::with_capacity(guard.len());

    guard.retain_mut(|entry| {
        if entry.handle.is_finished() {
            false
        } else {
            info.push(entry.info.clone());
            true
        }
    });

    let queue_key = state.queue_key.lock().unwrap().clone();
    let (queue_len, failed_count) = if let Some(key) = queue_key {
        let mut redis_con = match redis::Client::open("redis://localhost:6379")
            .unwrap()
            .get_connection()
        {
            Ok(con) => con,
            Err(_) => {
                return HttpResponse::InternalServerError()
                    .body("Redis connection error while reading status");
            }
        };
        let qlen: usize = redis_con.llen(key).unwrap_or(0);
        let failed: usize = redis_con.scard("dl:failed").unwrap_or(0);
        (qlen, failed)
    } else {
        (0, 0)
    };

    HttpResponse::Ok().json(StatusResponse {
        workers: info,
        queue_len,
        failed_count,
    })
}

fn load_config() -> anyhow::Result<AppConfig> {
    let bind = std::env::var("SERVER_BIND").unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    let worker_count = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let username_prefix =
        std::env::var("WORKER_USERNAME_PREFIX").unwrap_or_else(|_| "worker".to_string());
    let port_base = std::env::var("WORKER_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(41000);
    let run_id_prefix =
        std::env::var("WORKER_RUN_ID_PREFIX").unwrap_or_else(|_| "web-trigger".to_string());
    let download_path = std::env::var("DOWNLOAD_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/home/gonik/Music/otra_prueba_g"));
    Ok(AppConfig {
        bind,
        worker_count,
        username_prefix,
        port_base,
        run_id_prefix,
        download_path,
    })
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let connection = &mut establish_connection();
    connection
        .run_pending_migrations(MIGRATIONS)
        .expect("CANT RUN MIGS");
    let config = load_config()?;
    let state = web::Data::new(AppState {
        workers: Mutex::new(Vec::new()),
        config: config.clone(),
        next_worker_id: Mutex::new(1),
        queue_key: Mutex::new(None),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(start_workers)
            .service(stop_workers)
            .service(status)
    })
    .bind(config.bind.clone())?
    .run()
    .await?;

    Ok(())
}
