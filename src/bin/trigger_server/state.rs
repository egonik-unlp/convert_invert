use convert_invert::internals::context::context_manager::RedisPool;
use convert_invert::internals::database::DbPool;
use convert_invert::internals::worker::worker_manager::WorkerSupervisor;
use tokio::sync::watch;

use crate::config::AppConfig;

pub struct AppState {
    pub config: AppConfig,
    pub db_pool: DbPool,
    pub redis_pool: RedisPool,
    pub shutdown: watch::Receiver<bool>,
    pub worker_supervisor: WorkerSupervisor,
}
