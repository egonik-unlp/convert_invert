use actix_web::{Error, HttpResponse, Responder, get};
use anyhow::Context;
use diesel::deserialize::Result;
use diesel_migrations::{MigrationHarness, embed_migrations};
use itertools::Itertools;
use rand::Rng;
use std::io;
use std::{path::PathBuf, str::FromStr};
use tracing::instrument;

use convert_invert::internals::database::establish_connection;
use convert_invert::internals::{
    context::context_manager::Managers,
    utils::{config::config_manager::Config, trace},
};

use diesel_migrations::EmbeddedMigrations;
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[instrument(name = "main-span")]
#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    Ok(())
}

#[get("/launch")]
async fn launch_playlist_task() -> actix_web::Result<HttpResponse, actix_web::Error> {
    let connection = &mut establish_connection();
    connection
        .run_pending_migrations(MIGRATIONS)
        .expect("CANT RUN MIGS");
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut config = Config::try_from_env().unwrap();
    let attempt_num: usize = match std::env::args().nth(1) {
        Some(value) => value.parse().unwrap(),
        None => 1usize,
    };
    config.run_id = format!("{}_attempt_{}", config.run_id, attempt_num);

    trace::otel_trace::init_tracing_with_otel("convert-invert".to_string(), config.run_id.clone())
        .unwrap();

    let download_path = PathBuf::from_str("/home/gonik/Music/otra_prueba_g")
        .context("Acquiring download dir")
        .unwrap();

    let managers = Managers::new(
        config.judge_score_levenshtein,
        download_path.clone(),
        config.clone(),
    );

    let playlist = managers.get_playlist().await;
    let mut count = 0;
    for chunk in &playlist
        .into_iter()
        // .skip(66)
        .chunks(15)
    {
        count += 1;
        let (sender, receiver) = tokio::sync::mpsc::channel(20000);
        let managers = Managers::new(
            config.judge_score_levenshtein,
            download_path.clone(),
            config.clone(),
        );
        let sender = Managers::inject_tracks(chunk, sender).await.unwrap();
        managers
            .run_cycle(sender, receiver, connection, redis_client.clone())
            .await
            .unwrap();
        tracing::info!(cycle_n = count, "\n\nDone with cycle\n\n");
        println!("CHUNKERO DUOS {count}")
    }
    trace::otel_trace::shutdown_otel();
    Ok(HttpResponse::Ok().body("Done"))
}
