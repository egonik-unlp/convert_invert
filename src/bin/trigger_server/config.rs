use std::path::PathBuf;

#[derive(Clone)]
pub struct AppConfig {
    pub bind: String,
    pub worker_count: usize,
    pub username_prefix: String,
    pub worker_account_mode: String,
    pub port_base: u16,
    pub run_id_prefix: String,
    pub download_path: PathBuf,
    pub redis_url: String,
    pub jaeger_url: String,
    pub api_key: String,
    pub allowed_origins: Vec<String>,
    pub share_mode: String,
    pub share_path: String,
    pub search_timeout_secs: u8,
    pub search_empty_result_cutoff: usize,
}

pub fn load() -> anyhow::Result<AppConfig> {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("API_KEY").map_err(|_| {
        anyhow::anyhow!("API_KEY env var is required — generate one with `openssl rand -hex 32`")
    })?;
    if api_key.len() < 16 {
        anyhow::bail!("API_KEY must be at least 16 characters");
    }

    let allowed_origins: Vec<String> = std::env::var("ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:5173".to_string())
        .split(',')
        .map(|origin| origin.trim().to_string())
        .filter(|origin| !origin.is_empty())
        .collect();

    let bind = std::env::var("SERVER_BIND").unwrap_or_else(|_| "127.0.0.1:3124".to_string());
    let worker_count = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let worker_account_mode =
        std::env::var("WORKER_ACCOUNT_MODE").unwrap_or_else(|_| "same".to_string());
    let username_prefix = if worker_account_mode == "same" {
        std::env::var("USER_NAME").unwrap_or_else(|_| "default".to_string())
    } else {
        std::env::var("WORKER_USERNAME_PREFIX").unwrap_or_else(|_| "worker".to_string())
    };
    let port_base = std::env::var("WORKER_PORT_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(41000);
    let run_id_prefix =
        std::env::var("WORKER_RUN_ID_PREFIX").unwrap_or_else(|_| "web-trigger".to_string());
    let download_path = std::env::var("DOWNLOAD_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./downloads"));
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let jaeger_url =
        std::env::var("JAEGER_URL").unwrap_or_else(|_| "http://localhost:16686".to_string());
    let share_mode = std::env::var("SHARE_MODE").unwrap_or_else(|_| "disabled".to_string());
    let share_path = std::env::var("SHARE_PATH").unwrap_or_else(|_| "/downloads".to_string());
    let search_timeout_secs = std::env::var("SEARCH_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let search_empty_result_cutoff = std::env::var("SEARCH_EMPTY_RESULT_CUTOFF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    Ok(AppConfig {
        bind,
        worker_count,
        username_prefix,
        worker_account_mode,
        port_base,
        run_id_prefix,
        download_path,
        redis_url,
        jaeger_url,
        api_key,
        allowed_origins,
        share_mode,
        share_path,
        search_timeout_secs,
        search_empty_result_cutoff,
    })
}
