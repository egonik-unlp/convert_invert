use anyhow::Context;
use diesel_migrations::{MigrationHarness, embed_migrations};
use std::{path::PathBuf, str::FromStr};
use tracing::instrument;
use redis::Commands;

use convert_invert::internals::database::establish_connection;
use convert_invert::internals::{
    context::context_manager::Managers,
    utils::{config::config_manager::Config, trace},
};

use diesel_migrations::EmbeddedMigrations;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

fn chunk_queue_keys(playlist_id: &str, chunk_size: usize) -> (String, String) {
    let key = format!("dl:chunk_queue:{playlist_id}:{chunk_size}");
    let init_key = format!("dl:chunk_queue_init:{playlist_id}:{chunk_size}");
    (key, init_key)
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
    let playlist_id = std::env::var("PLAYLIST_ID")
        .unwrap_or_else(|_| "7vdaDB7qkKGbE4abs1iFpQ?si=060b186284b14ad2".to_string());
    let chunk_size: usize = std::env::var("CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);

    let managers = Managers::new(
        config.judge_score_levenshtein,
        download_path.clone(),
        config.clone(),
        &playlist_id,
        1.0,
    );

    let full_playlist = managers.get_playlist().await;
    let search_items = full_playlist
        .into_iter()
        .filter_map(|track| match track {
            convert_invert::internals::context::context_manager::Track::Query(item) => {
                Some(item)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    let (queue_key, init_key) = chunk_queue_keys(&playlist_id, chunk_size);
    {
        let mut redis_con = redis_client.get_connection().unwrap();
        let initialized: usize = redis_con.set_nx(init_key.clone(), true).unwrap_or(0);
        if initialized == 1 {
            let _: usize = redis_con.del(queue_key.clone()).unwrap_or(0);
            let len = search_items.len();
            let mut start = 0usize;
            while start < len {
                let end = (start + chunk_size).min(len);
                let _: usize = redis_con
                    .rpush(&queue_key, format!("{start}:{end}"))
                    .unwrap_or(0);
                start = end;
            }
        }
    }

    let mut count = 0;
    loop {
        let range: Option<String> = {
            let mut redis_con = redis_client.get_connection().unwrap();
            redis_con.lpop(&queue_key, None).ok()
        };
        let Some(range) = range else { break };
        let Some((start, end)) = range.split_once(':') else { continue };
        let start: usize = start.parse().unwrap_or(0);
        let end: usize = end.parse().unwrap_or(start);
        let end = end.min(search_items.len());
        if start >= end {
            continue;
        }
        let chunk = search_items[start..end]
            .iter()
            .cloned()
            .map(convert_invert::internals::context::context_manager::Track::Query)
            .collect::<Vec<_>>();
        count += 1;
        let managers = Managers::new(
            config.judge_score_levenshtein,
            download_path.clone(),
            config.clone(),
            &playlist_id,
            1.0,
        );
        managers
            .run_cycle(chunk, connection, redis_client.clone())
            .await
            .unwrap();
        tracing::info!(cycle_n = count, "\n\nDone with cycle\n\n");
        println!("CHUNKERO DUOS {count}");
    }

    let failed_ids: Vec<String> = {
        let mut redis_con = redis_client.get_connection().unwrap();
        redis_con.smembers("dl:failed").unwrap_or_default()
    };
    if !failed_ids.is_empty() {
        let failed_items = search_items
            .into_iter()
            .filter(|item| failed_ids.contains(&item.track_id.to_string()))
            .map(convert_invert::internals::context::context_manager::Track::Query)
            .collect::<Vec<_>>();
        if !failed_items.is_empty() {
            tracing::info!(
                failed = failed_items.len(),
                "Retrying failed tracks with longer timeouts"
            );
            let managers = Managers::new(
                config.judge_score_levenshtein,
                download_path.clone(),
                config.clone(),
                &playlist_id,
                2.0,
            );
            managers
                .run_cycle(failed_items, connection, redis_client.clone())
                .await
                .unwrap();
        }
    }

    println!("Outer");
    trace::otel_trace::shutdown_otel();

    Ok(())
}
