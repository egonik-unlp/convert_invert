use std::time::Duration;

use actix_cors::Cors;
use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::middleware::{DefaultHeaders, from_fn};
use actix_web::{App, HttpServer, web};
use anyhow::Context;
use convert_invert::internals::context::context_manager::WorkerTuning;
use convert_invert::internals::database::db_pool_max_size_from_env;
use convert_invert::internals::utils::trace;
use convert_invert::internals::worker::worker_manager::WorkerSupervisor;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use tokio::sync::watch;

mod api;
mod config;
mod errors;
mod middleware;
mod state;
mod validation;
mod workers;

use crate::middleware::require_api_key;
use crate::state::AppState;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

fn build_cors(allowed_origins: &[String]) -> Cors {
    let mut cors = Cors::default()
        .allowed_methods(vec!["GET", "POST"])
        .allowed_headers(vec!["X-API-Key", "Content-Type"])
        .max_age(3600);
    for origin in allowed_origins {
        cors = cors.allowed_origin(origin);
    }
    cors
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let mut app_config = config::load()?;
    let run_id = std::env::var("RUN_ID").unwrap_or_else(|_| "api".to_string());
    trace::otel_trace::init_tracing_with_otel("convert-invert-api".to_string(), run_id)
        .context("Tracing")?;
    tracing::info!(
        worker_account_mode = %app_config.worker_account_mode,
        worker_username = %app_config.username_prefix,
        worker_port_base = app_config.port_base,
        download_path = %app_config.download_path.display(),
        share_mode = %app_config.share_mode,
        share_path = %app_config.share_path,
        "Loaded worker/share configuration",
    );

    // A8: `same` account mode logs a single Soulseek account in concurrently when
    // worker_count > 1, which is a common ban/kick trigger (and the sharing sidecar already
    // holds one session with the same credentials). Clamp to a single worker and warn; use
    // real separate accounts (WORKER_ACCOUNT_MODE=suffixed) to run more.
    if app_config.worker_account_mode == "same" && app_config.worker_count > 1 {
        tracing::warn!(
            requested_worker_count = app_config.worker_count,
            "WORKER_ACCOUNT_MODE=same with worker_count > 1 risks a Soulseek ban; clamping \
             worker_count to 1. Set WORKER_ACCOUNT_MODE=suffixed with real separate accounts \
             to run multiple workers.",
        );
        app_config.worker_count = 1;
    }

    let db_pool = convert_invert::internals::database::init_pool()?;
    let tuning = WorkerTuning::from_env();
    let db_pool_max = db_pool_max_size_from_env(18);
    let minimum_pool = tuning.download_concurrency + tuning.search_concurrency + 2;
    if db_pool_max < minimum_pool as u32 {
        tracing::warn!(
            db_pool_max,
            minimum_pool,
            download_concurrency = tuning.download_concurrency,
            search_concurrency = tuning.search_concurrency,
            "DB pool max size is below the recommended concurrency floor",
        );
    }
    {
        let mut connection = db_pool
            .get()
            .map_err(|err| anyhow::anyhow!("Failed to get connection for migration: {err}"))?;
        connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|err| anyhow::anyhow!("Cannot run migrations: {err}"))?;
    }

    let redis_client = redis::Client::open(app_config.redis_url.as_str())?;
    let redis_pool = diesel::r2d2::Pool::builder()
        .max_size(env_u32("REDIS_POOL_MAX_SIZE", 18))
        .connection_timeout(Duration::from_secs(env_u64("REDIS_POOL_TIMEOUT_SECS", 15)))
        .build(redis_client)
        .map_err(|err| anyhow::anyhow!("Failed to create Redis pool: {err}"))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    workers::install_shutdown_handler(shutdown_tx);

    let state = web::Data::new(AppState {
        config: app_config.clone(),
        db_pool: db_pool.clone(),
        redis_pool: redis_pool.clone(),
        shutdown: shutdown_rx.clone(),
        worker_supervisor: WorkerSupervisor::new(
            app_config.download_path.clone(),
            db_pool,
            redis_pool,
            shutdown_rx,
        ),
    });

    // Dashboard polling reads several endpoints every few seconds, so the
    // authenticated API limit needs to allow normal UI use without throttling.
    // Keep write-heavy worker starts on the stricter nested governor below.
    let global_governor = GovernorConfigBuilder::default()
        .milliseconds_per_request(250)
        .burst_size(60)
        .finish()
        .ok_or_else(|| anyhow::anyhow!("Invalid global governor config"))?;
    // 5 req/min per IP on /workers/start: one token every 12s with a burst of 3.
    let workers_governor = GovernorConfigBuilder::default()
        .seconds_per_request(12)
        .burst_size(3)
        .finish()
        .ok_or_else(|| anyhow::anyhow!("Invalid workers governor config"))?;

    let allowed_origins = app_config.allowed_origins.clone();
    let bind = app_config.bind.clone();
    let app_state = state.clone();

    let server = HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(build_cors(&allowed_origins))
            .service(
                web::scope("/api")
                    .wrap(from_fn(require_api_key))
                    .wrap(
                        DefaultHeaders::new()
                            .add(("Cache-Control", "no-store"))
                            .add(("Pragma", "no-cache")),
                    )
                    .wrap(Governor::new(&global_governor))
                    .service(api::health)
                    .service(api::stats)
                    .service(api::network)
                    .service(api::config)
                    .service(api::downloads)
                    .service(api::playlists)
                    .service(api::playlist)
                    .service(api::candidates)
                    .service(api::track_report)
                    .service(api::logs)
                    .service(
                        web::scope("/workers")
                            .service(
                                web::resource("/start")
                                    .wrap(Governor::new(&workers_governor))
                                    .route(web::post().to(workers::start_workers)),
                            )
                            .service(workers::stop_workers)
                            .service(workers::worker_status),
                    ),
            )
    })
    .bind(bind)?
    .shutdown_timeout(30)
    .run();

    let server_handle = server.handle();

    // Spawn a watcher that triggers an Actix graceful stop when the global
    // shutdown signal flips (Actix already handles SIGTERM, but this lets a
    // future programmatic shutdown also drain in-flight requests).
    let mut shutdown_listener = state.shutdown.clone();
    tokio::spawn(async move {
        // Wait until the value becomes true.
        loop {
            if *shutdown_listener.borrow() {
                break;
            }
            if shutdown_listener.changed().await.is_err() {
                return;
            }
        }
        tracing::info!("Initiating graceful Actix shutdown");
        server_handle.stop(true).await;
    });

    server.await?;

    // Give workers a brief window to drain before tearing down telemetry so
    // their final spans are flushed.
    tokio::time::sleep(Duration::from_millis(500)).await;
    trace::otel_trace::shutdown_otel();
    Ok(())
}

fn env_u32(key: &str, default: u32) -> u32 {
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
