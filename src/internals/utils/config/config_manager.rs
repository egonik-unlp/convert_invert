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
    pub search_empty_result_cutoff: usize,
    pub playlist_id: String,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub share_mode: String,
    pub share_path: String,
    /// Base URL of the aioslsk engine service that performs search + download + share.
    pub slsk_url: String,
}

impl Config {
    pub fn try_from_env() -> anyhow::Result<Self> {
        load_dotenv_files();
        let run_id = env::var("RUN_ID").unwrap_or_else(|_| "default_run".to_string());
        let log_level: EnvFilter = env::var("LOG_LEVEL").unwrap_or("debug".to_string()).into();
        let user_name = env::var("USER_NAME").unwrap_or("default".to_string());
        let user_password = env::var("USER_PASSWORD").unwrap_or_default();
        let client_id = env::var("CLIENT_ID").ok();
        let client_secret = env::var("CLIENT_SECRET").ok();
        let playlist_id = env::var("PLAYLIST_ID")
            .unwrap_or_else(|_| "4RNxYgx8c1WuDV7MItXel2?si=e5b2ceac9697423f".to_string());
        let judge_score_levenshtein: Option<f32> = {
            let val = env::var("JUDGE_SCORE_LEVENSHTEIN").ok();
            val.map(|v| v.parse().context("Cannot parse JUDGE_SCORE_LEVENSHTEIN"))
                .transpose()?
        };
        let judge_score_llm: Option<f32> = {
            let val = env::var("JUDGE_SCORE_LLM").ok();
            val.map(|v| v.parse().context("Cannot parse JUDGE_SCORE_LLM"))
                .transpose()?
        };
        let listen_port: u32 = {
            let val = env::var("LISTEN_PORT").unwrap_or_else(|_| "41000".to_string());
            val.parse().context("Cannot parse LISTEN_PORT")?
        };

        let search_timeout_secs: u8 = {
            let val = env::var("SEARCH_TIMEOUT_SECS").unwrap_or("20".to_string());
            val.parse().context("Cannot parse SEARCH_TIMEOUT_SECS")?
        };
        let search_empty_result_cutoff: usize = {
            let val = env::var("SEARCH_EMPTY_RESULT_CUTOFF").unwrap_or("8".to_string());
            val.parse()
                .context("Cannot parse SEARCH_EMPTY_RESULT_CUTOFF")?
        };
        let share_mode = env::var("SHARE_MODE").unwrap_or_else(|_| "disabled".to_string());
        let share_path = env::var("SHARE_PATH").unwrap_or_else(|_| "/downloads".to_string());
        let slsk_url = env::var("SLSK_URL").unwrap_or_else(|_| "http://sharing:8080".to_string());

        Ok(Config {
            run_id,
            log_level,
            user_name,
            user_password,
            judge_score_levenshtein,
            judge_score_llm,
            listen_port,
            search_timeout_secs,
            search_empty_result_cutoff,
            playlist_id,
            client_id,
            client_secret,
            share_mode,
            share_path,
            slsk_url,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_level: EnvFilter,
        user_name: String,
        user_password: String,
        judge_score_levenshtein: Option<f32>,
        judge_score_llm: Option<f32>,
        listen_port: u32,
        search_timeout_secs: u8,
        search_empty_result_cutoff: usize,
        run_id: String,
        playlist_id: String,
        client_id: Option<String>,
        client_secret: Option<String>,
        share_mode: String,
        share_path: String,
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
            search_empty_result_cutoff,
            playlist_id,
            client_id,
            client_secret,
            share_mode,
            share_path,
            slsk_url: "http://sharing:8080".to_string(),
        }
    }
}

fn load_dotenv_files() {
    dotenvy::dotenv().ok();
    dotenvy::from_path("../.env").ok();
}
