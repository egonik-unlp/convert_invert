use anyhow::Context;
use diesel_migrations::{MigrationHarness, embed_migrations};
use itertools::Itertools;
use std::{path::PathBuf, str::FromStr};
use tracing::instrument;

use convert_invert::internals::database::init_pool;
use convert_invert::internals::{
    context::context_manager::Managers,
    utils::{config::config_manager::Config, trace},
};

use diesel_migrations::EmbeddedMigrations;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[instrument(name = "main-span")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_pool = init_pool();
    {
        let mut connection = db_pool.get().context("Initial migration connection")?;
        connection
            .run_pending_migrations(MIGRATIONS)
            .expect("CANT RUN MIGS");
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let redis_pool = diesel::r2d2::Pool::builder()
        .build(redis_client)
        .expect("Failed to create Redis pool");

    let mut config = Config::try_from_env().context("Cannot read env vars for config")?;
    let attempt_num: usize = match std::env::args().nth(1) {
        Some(value) => value.parse().unwrap(),
        None => 1usize,
    };
    config.run_id = format!("{}_attempt_{}", config.run_id, attempt_num);

    trace::otel_trace::init_tracing_with_otel("convert-invert".to_string(), config.run_id.clone())
        .context("Tracing")?;

    let download_path =
        PathBuf::from_str("/home/gonik/Music/otra_prueba_g").context("Acquiring download dir")?;

    let managers = Managers::new(
        config.judge_score_levenshtein,
        download_path.clone(),
        config.clone(),
        db_pool.clone(),
        redis_pool.clone(),
    );

    let playlist = managers.get_playlist().await;
    let mut count = 0;
    for chunk in &playlist
        .into_iter()
        // .skip(66)
        .chunks(15)
    {
        count += 1;
        let managers = Managers::new(
            config.judge_score_levenshtein,
            download_path.clone(),
            config.clone(),
            db_pool.clone(),
            redis_pool.clone(),
        );
        managers.run_cycle(chunk).await.unwrap();
        tracing::info!(cycle_n = count, "\n\nDone with cycle\n\n");
        println!("CHUNKERO DUOS {count}")
    }

    println!("Outer");
    trace::otel_trace::shutdown_otel();

    Ok(())
}
