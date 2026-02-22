use anyhow::Context;
use diesel_migrations::{MigrationHarness, embed_migrations};
use itertools::Itertools;
use std::{path::PathBuf, str::FromStr};
use tracing::instrument;

use convert_invert::internals::database::establish_connection;
use convert_invert::internals::{
    context::context_manager::Managers,
    utils::{config::config_manager::Config, trace},
};

use diesel_migrations::EmbeddedMigrations;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

fn apply_playlist_partition(mut playlist: Vec<convert_invert::internals::context::context_manager::Track>) -> Vec<convert_invert::internals::context::context_manager::Track> {
    use std::env;

    let range_start = env::var("PLAYLIST_RANGE_START").ok().and_then(|v| v.parse::<usize>().ok());
    let range_end = env::var("PLAYLIST_RANGE_END").ok().and_then(|v| v.parse::<usize>().ok());
    if let (Some(start), Some(end)) = (range_start, range_end) {
        let len = playlist.len();
        let start = start.min(len);
        let end = end.min(len);
        if start < end {
            tracing::info!(start, end, len, "Applying explicit playlist range");
            return playlist.into_iter().skip(start).take(end - start).collect();
        }
    }

    let parts = env::var("PLAYLIST_PARTS").ok().and_then(|v| v.parse::<usize>().ok());
    let index = env::var("PLAYLIST_PART_INDEX").ok().and_then(|v| v.parse::<usize>().ok());
    if let (Some(parts), Some(index)) = (parts, index) {
        if parts > 0 && index < parts {
            let len = playlist.len();
            let start = len * index / parts;
            let end = len * (index + 1) / parts;
            tracing::info!(parts, index, start, end, len, "Applying playlist partition");
            playlist = playlist.into_iter().skip(start).take(end - start).collect();
        }
    }

    playlist
}

#[instrument(name = "main-span")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection = &mut establish_connection();
    connection
        .run_pending_migrations(MIGRATIONS)
        .expect("CANT RUN MIGS");
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
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
    let playlist_id = "7vdaDB7qkKGbE4abs1iFpQ?si=060b186284b14ad2";

    let managers = Managers::new(
        config.judge_score_levenshtein,
        download_path.clone(),
        config.clone(),
        playlist_id,
    );

    let playlist = apply_playlist_partition(managers.get_playlist().await);
    let mut count = 0;
    for chunk in &playlist.into_iter().chunks(15) {
        count += 1;
        let managers = Managers::new(
            config.judge_score_levenshtein,
            download_path.clone(),
            config.clone(),
            playlist_id,
        );
        managers
            .run_cycle(chunk, connection, redis_client.clone())
            .await
            .unwrap();
        tracing::info!(cycle_n = count, "\n\nDone with cycle\n\n");
        println!("CHUNKERO DUOS {count}")
    }

    println!("Outer");
    trace::otel_trace::shutdown_otel();

    Ok(())
}
