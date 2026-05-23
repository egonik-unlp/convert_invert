use anyhow::Context;
use diesel_migrations::{MigrationHarness, embed_migrations};
use itertools::Itertools;
use std::path::PathBuf;
use tracing::instrument;

use convert_invert::internals::database::init_pool;
use convert_invert::internals::{
    context::context_manager::Managers,
    query::query_manager::QueryManager,
    utils::{config::config_manager::Config, trace},
};

use diesel_migrations::EmbeddedMigrations;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[instrument(name = "main-span")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = Config::try_from_env().context("Cannot read env vars for config")?;
    let attempt_num: usize = match std::env::args().nth(1) {
        Some(value) => value.parse().context("Parse attempt number")?,
        None => 1usize,
    };
    config.run_id = format!("{}_attempt_{}", config.run_id, attempt_num);

    trace::otel_trace::init_tracing_with_otel("convert-invert".to_string(), config.run_id.clone())
        .context("Tracing")?;

    let db_pool = init_pool().context("Initialize database pool")?;
    {
        let mut connection = db_pool.get().context("Initial migration connection")?;
        connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|err| anyhow::anyhow!("Cannot run migrations: {err}"))?;
    }

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url).context("Create Redis client")?;
    let redis_pool = diesel::r2d2::Pool::builder()
        .build(redis_client)
        .context("Create Redis pool")?;

    let download_path = std::env::var("DOWNLOAD_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./downloads"));
    tokio::fs::create_dir_all(&download_path)
        .await
        .with_context(|| format!("Create download directory {}", download_path.display()))?;

    let playlist = QueryManager::new_with_timeout(
        config.playlist_id.clone(),
        config.client_id.clone(),
        config.client_secret.clone(),
        config.search_timeout_secs,
    )
    .fetch_playlist()
    .await
    .context("Fetch playlist")?;
    let mut count = 0;
    for chunk in &playlist.into_iter().chunks(15) {
        count += 1;
        let managers = Managers::new(
            config.judge_score_levenshtein,
            download_path.clone(),
            config.clone(),
            db_pool.clone(),
            redis_pool.clone(),
        );
        managers
            .run_cycle(chunk)
            .await
            .with_context(|| format!("Run cycle {count}"))?;
        tracing::info!(cycle_n = count, "Done with cycle");
    }

    trace::otel_trace::shutdown_otel();

    Ok(())
}
