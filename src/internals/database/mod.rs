use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use dotenvy::dotenv;
use std::{env, time::Duration};

pub mod manager;
pub mod model;
pub mod schema;

pub type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

#[derive(Debug, Clone, Copy)]
pub struct DbPoolSnapshot {
    pub connections: u32,
    pub idle_connections: u32,
}

impl DbPoolSnapshot {
    pub fn in_use_connections(self) -> u32 {
        self.connections.saturating_sub(self.idle_connections)
    }
}

pub fn db_pool_snapshot(pool: &DbPool) -> DbPoolSnapshot {
    let state = pool.state();
    DbPoolSnapshot {
        connections: state.connections,
        idle_connections: state.idle_connections,
    }
}

pub fn db_pool_max_size_from_env(default: u32) -> u32 {
    env::var("DB_POOL_MAX_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub fn db_pool_timeout_from_env(default_secs: u64) -> Duration {
    let secs = env::var("DB_POOL_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

pub fn init_pool() -> anyhow::Result<DbPool> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL")?;
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(db_pool_max_size_from_env(18))
        .connection_timeout(db_pool_timeout_from_env(15))
        .build(manager)
        .map_err(Into::into)
}

pub fn establish_connection() -> anyhow::Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")?;
    PgConnection::establish(&database_url).map_err(Into::into)
}
