use actix_web::{HttpResponse, post, web};
use convert_invert::internals::utils::config::config_manager::Config;
use convert_invert::internals::worker::worker_manager::WorkerStartOptions;
use tokio::sync::watch;

use crate::errors::{ApiError, ApiResult};
use crate::state::AppState;
use crate::validation::{StartRequest, StopRequest};

pub async fn start_workers(
    state: web::Data<AppState>,
    req: web::Json<StartRequest>,
) -> ApiResult<HttpResponse> {
    tracing::info!("Received worker start request");
    let request = req.into_inner().validate(
        state.config.worker_count,
        &state.config.username_prefix,
        state.config.port_base,
        &state.config.run_id_prefix,
        &state.config.worker_account_mode,
    )?;
    tracing::info!(
        worker_count = request.worker_count,
        port_base = request.port_base,
        playlist_id = %request.playlist_id,
        chunk_size = request.chunk_size,
        random_order = request.random_order,
        account_mode = %state.config.worker_account_mode,
        worker_username = %request.username_prefix,
        share_mode = %state.config.share_mode,
        share_username = %state.config.share_username,
        "Validated worker start request",
    );

    let base_config = Config::try_from_env()
        .map_err(|err| ApiError::Internal(format!("Failed to load worker config: {err}")))?;
    if state
        .config
        .account_conflict_for(request.worker_count, &request.username_prefix)
    {
        tracing::warn!(
            worker_username = %request.username_prefix,
            share_username = %state.config.share_username,
            "Rejecting worker start because worker and share accounts conflict",
        );
        return Err(ApiError::BadRequest(format!(
            "SHARE_MODE=external requires different Soulseek accounts for workers ({}) and sharing service ({}). Set WORKER_USERNAME_PREFIX/WORKER_USER_PASSWORD for downloader accounts and SHARE_USER_NAME/SHARE_USER_PASSWORD for sharing.",
            state
                .config
                .generated_worker_usernames(request.worker_count, &request.username_prefix)
                .join(", "),
            state.config.share_username
        )));
    }
    if let Some(warning) = state
        .config
        .worker_port_capacity_warning_for(request.worker_count, request.port_base)
    {
        tracing::warn!(warning = %warning, "Rejecting worker start because configured worker ports exceed published range");
        return Err(ApiError::BadRequest(format!(
            "{warning} Increase WORKER_PORT_COUNT or reduce worker_count."
        )));
    }
    let user_password = state.config.worker_password.clone();

    let spawned = state
        .worker_supervisor
        .start(
            WorkerStartOptions {
                worker_count: request.worker_count,
                username_prefix: request.username_prefix,
                port_base: request.port_base,
                run_id_prefix: request.run_id_prefix,
                account_mode: state.config.worker_account_mode.clone(),
                playlist_id: request.playlist_id,
                chunk_size: request.chunk_size,
                playlist_range: request.playlist_range,
                random_order: request.random_order,
            },
            base_config,
            user_password,
        )
        .await
        .map_err(|err| ApiError::Internal(format!("Failed to start workers: {err}")))?;

    tracing::info!(
        spawned_workers = spawned.len(),
        "Worker start request completed"
    );
    Ok(HttpResponse::Ok().json(spawned))
}

#[post("/stop")]
pub async fn stop_workers(
    state: web::Data<AppState>,
    req: web::Json<StopRequest>,
) -> ApiResult<HttpResponse> {
    let target_ids = req.pids.as_deref();
    let stopped = state
        .worker_supervisor
        .stop(target_ids)
        .map_err(|err| ApiError::Internal(format!("Failed to stop workers: {err}")))?;

    Ok(HttpResponse::Ok().json(stopped))
}

#[actix_web::get("/status")]
pub async fn worker_status(state: web::Data<AppState>) -> ApiResult<HttpResponse> {
    let status = state
        .worker_supervisor
        .status()
        .map_err(|err| ApiError::Internal(format!("Failed to read worker status: {err}")))?;

    Ok(HttpResponse::Ok().json(status))
}

/// Listens for SIGTERM/SIGINT and flips the shutdown signal so workers can
/// drain at chunk boundaries. Actix's own SIGTERM handler still drives the
/// HTTP shutdown_timeout; this is for the worker tasks that live alongside it.
pub fn install_shutdown_handler(tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut term = match signal(SignalKind::terminate()) {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(?err, "Failed to install SIGTERM handler");
                    return;
                }
            };
            let mut int = match signal(SignalKind::interrupt()) {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(?err, "Failed to install SIGINT handler");
                    return;
                }
            };
            tokio::select! {
                _ = term.recv() => tracing::info!("Received SIGTERM"),
                _ = int.recv() => tracing::info!("Received SIGINT"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Received Ctrl-C");
        }
        let _ = tx.send(true);
    });
}
