use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use convert_invert::internals::database::establish_connection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(Clone)]
struct AppConfig {
    bind: String,
    worker_count: usize,
    username_prefix: String,
    port_base: u16,
    worker_bin: PathBuf,
    run_id_prefix: String,
}

#[derive(Serialize, Clone)]
struct WorkerInfo {
    index: usize,
    username: String,
    port: u16,
    pid: u32,
    started_at_epoch_secs: u64,
}

struct AppState {
    workers: Mutex<Vec<(WorkerInfo, Child)>>,
    config: AppConfig,
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
    let playlist_id = req.playlist_id.clone();
    let chunk_size = req.chunk_size;
    let range_start = req.playlist_range_start;
    let range_end = req.playlist_range_end;
    let mut spawned: Vec<WorkerInfo> = Vec::with_capacity(count);

    let mut guard = state.workers.lock().unwrap();
    for i in 0..count {
        let username = format!("{}{}", username_prefix, i + 1);
        let port = port_base.saturating_add(i as u16);
        let run_id = format!("{}-{}", run_id_prefix, i + 1);

        let started_at_epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut cmd = Command::new(&state.config.worker_bin);
        cmd.env("USER_NAME", &username)
            .env("LISTEN_PORT", port.to_string())
            .env("RUN_ID", &run_id)
            .env("PLAYLIST_PARTS", count.to_string())
            .env("PLAYLIST_PART_INDEX", i.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        if let Some(id) = playlist_id.as_ref() {
            cmd.env("PLAYLIST_ID", id);
        }
        if let Some(size) = chunk_size {
            cmd.env("CHUNK_SIZE", size.to_string());
        }
        if let (Some(start), Some(end)) = (range_start, range_end) {
            if start >= end {
                return HttpResponse::BadRequest()
                    .body("playlist_range_start must be < playlist_range_end");
            }
            let span = end - start;
            let sub_start = start + (span * i) / count;
            let sub_end = start + (span * (i + 1)) / count;
            cmd.env("PLAYLIST_RANGE_START", sub_start.to_string())
                .env("PLAYLIST_RANGE_END", sub_end.to_string());
        }

        let child = match cmd.spawn() {
            Ok(child) => child,
            Err(err) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Failed to spawn worker {i}: {err}"));
            }
        };

        let info = WorkerInfo {
            index: i,
            username,
            port,
            pid: child.id(),
            started_at_epoch_secs,
        };

        spawned.push(info.clone());
        guard.push((info, child));
    }

    HttpResponse::Ok().json(spawned)
}

#[derive(Deserialize)]
struct StopRequest {
    pids: Option<Vec<u32>>,
}

#[post("/stop")]
async fn stop_workers(state: web::Data<AppState>, req: web::Json<StopRequest>) -> impl Responder {
    let mut guard = state.workers.lock().unwrap();
    let target_pids = req.pids.clone();
    let mut stopped: Vec<u32> = Vec::new();
    let mut remaining: Vec<(WorkerInfo, Child)> = Vec::with_capacity(guard.len());

    for (info, mut child) in guard.drain(..) {
        let should_stop = match &target_pids {
            None => true,
            Some(pids) => pids.contains(&info.pid),
        };

        if should_stop {
            let _ = child.kill();
            stopped.push(info.pid);
        } else {
            remaining.push((info, child));
        }
    }

    *guard = remaining;
    HttpResponse::Ok().json(stopped)
}

#[get("/status")]
async fn status(state: web::Data<AppState>) -> impl Responder {
    let mut guard = state.workers.lock().unwrap();
    let mut info: Vec<WorkerInfo> = Vec::with_capacity(guard.len());

    guard.retain_mut(|(worker, child)| match child.try_wait() {
        Ok(Some(_status)) => false,
        Ok(None) => {
            info.push(worker.clone());
            true
        }
        Err(_) => {
            info.push(worker.clone());
            true
        }
    });

    HttpResponse::Ok().json(info)
}

fn resolve_worker_bin() -> anyhow::Result<PathBuf> {
    if let Ok(val) = std::env::var("WORKER_BIN") {
        return Ok(PathBuf::from(val));
    }

    if let Ok(current) = std::env::current_exe() {
        if let Some(parent) = current.parent() {
            let sibling = parent.join("convert-invert");
            if sibling.is_file() {
                return Ok(sibling);
            }
        }
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let release_candidate = Path::new(manifest_dir)
        .join("target")
        .join("release")
        .join("convert-invert");
    if release_candidate.is_file() {
        return Ok(release_candidate);
    }
    let candidate = Path::new(manifest_dir)
        .join("target")
        .join("debug")
        .join("convert-invert");
    if candidate.is_file() {
        return Ok(candidate);
    }

    anyhow::bail!("Cannot locate worker binary. Set WORKER_BIN to its path.");
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

    let worker_bin = resolve_worker_bin()?;
    Ok(AppConfig {
        bind,
        worker_count,
        username_prefix,
        port_base,
        worker_bin,
        run_id_prefix,
    })
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let state = web::Data::new(AppState {
        workers: Mutex::new(Vec::new()),
        config: config.clone(),
    });

    let connection = &mut establish_connection();
    connection
        .run_pending_migrations(MIGRATIONS)
        .expect("CANT RUN MIGS");
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
