use std::env;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Default, Clone)]
pub struct Config {
    pub run_id: String,
    pub log_level: EnvFilter,
    pub user_name: String,
    pub user_password: String,
    pub judge_score_levenshtein: Option<f32>,
    pub judge_score_llm: Option<f32>,
    pub listen_port: u32,
    pub search_timeout_secs: u8,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

impl Config {
    pub fn try_from_env() -> anyhow::Result<Self> {
        dotenvy::dotenv().context("Getting env vars")?;
        let run_id = env::var("RUN_ID").unwrap_or("default_run_name".to_string());
        let log_level: EnvFilter = env::var("LOG_LEVEL").unwrap_or("debug".to_string()).into();
        let user_name = env::var("USER_NAME").unwrap_or("default".to_string());
        let user_password = env::var("USER_PASSWORD").unwrap_or("123456".to_string());
        let client_id = env::var("CLIENT_ID").ok();
        let client_secret = env::var("CLIENT_SECRET").ok();
        let judge_score_levenshtein: Option<f32> = {
            let val = env::var("JUDGE_SCORE_LEVENSHTEIN").ok();
            val.map(|v| v.parse().expect("Cannot parse judge score levenshtein"))
        };
        let judge_score_llm: Option<f32> = {
            let val = env::var("JUDGE_SCORE_LLM").ok();
            val.map(|v| v.parse().expect("Cannot parse judge score llm"))
        };
        let listen_port: u32 = {
            let val = env::var("LISTEN_PORT").unwrap_or("3124".to_string());
            val.parse().context("cannot parse val")?
        };

        let search_timeout_secs: u8 = {
            let val = env::var("SEARCH_TIMEOUT_SECS").unwrap_or("10".to_string());
            val.parse().context("cannot parse val")?
        };
        Ok(Config {
            run_id,
            log_level,
            user_name,
            user_password,
            judge_score_levenshtein,
            judge_score_llm,
            listen_port,
            search_timeout_secs,
            client_id,
            client_secret,
        })
    }

    //TODO: Find a way
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_level: EnvFilter,
        user_name: String,
        user_password: String,
        judge_score_levenshtein: Option<f32>,
        judge_score_llm: Option<f32>,
        listen_port: u32,
        search_timeout_secs: u8,
        run_id: String,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        Config {
            run_id,
            log_level,
            user_name,
            user_password,
            judge_score_levenshtein,
            judge_score_llm,
            listen_port,
            search_timeout_secs,
            client_id,
            client_secret,
        }
    }
}
