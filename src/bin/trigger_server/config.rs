use std::path::PathBuf;

#[derive(Clone)]
pub struct AppConfig {
    pub bind: String,
    pub worker_count: usize,
    pub username_prefix: String,
    pub worker_password: String,
    pub worker_account_mode: String,
    pub port_base: u16,
    pub worker_port_count: usize,
    pub worker_port_published_base: u16,
    pub run_id_prefix: String,
    pub download_path: PathBuf,
    pub redis_url: String,
    pub jaeger_url: String,
    pub api_key: String,
    pub allowed_origins: Vec<String>,
    pub share_mode: String,
    pub share_path: String,
    pub share_username: String,
    pub search_timeout_secs: u8,
    pub search_empty_result_cutoff: usize,
}

impl AppConfig {
    pub fn account_conflict(&self) -> bool {
        self.account_conflict_for(self.worker_count, &self.username_prefix)
    }

    pub fn account_conflict_for(&self, worker_count: usize, username_prefix: &str) -> bool {
        if self.share_mode != "external" || self.share_username.is_empty() {
            return false;
        }
        self.generated_worker_usernames(worker_count, username_prefix)
            .iter()
            .any(|worker_username| worker_username == &self.share_username)
    }

    pub fn generated_worker_usernames(
        &self,
        worker_count: usize,
        username_prefix: &str,
    ) -> Vec<String> {
        if self.worker_account_mode == "same" {
            return if username_prefix.is_empty() {
                Vec::new()
            } else {
                vec![username_prefix.to_string()]
            };
        }

        (1..=worker_count)
            .map(|worker_number| format!("{username_prefix}{worker_number}"))
            .collect()
    }

    pub fn worker_username_pattern(&self) -> String {
        if self.worker_account_mode == "same" {
            self.username_prefix.clone()
        } else {
            format!(
                "{}1..{}{}",
                self.username_prefix, self.username_prefix, self.worker_count
            )
        }
    }

    pub fn worker_account_warning(&self) -> Option<String> {
        if self.worker_account_mode == "same" || !self.username_prefix.trim().is_empty() {
            return None;
        }
        Some("WORKER_USERNAME_PREFIX is required when WORKER_ACCOUNT_MODE is not same.".to_string())
    }

    pub fn worker_port_published_last(&self) -> u16 {
        self.worker_port_published_base
            .saturating_add(self.worker_port_count.saturating_sub(1) as u16)
    }

    pub fn worker_port_capacity_warning(&self) -> Option<String> {
        self.worker_port_capacity_warning_for(self.worker_count, self.port_base)
    }

    pub fn worker_port_capacity_warning_for(
        &self,
        worker_count: usize,
        port_base: u16,
    ) -> Option<String> {
        let configured_last = port_base.saturating_add(worker_count.saturating_sub(1) as u16);
        let published_last = self.worker_port_published_last();
        (port_base < self.worker_port_published_base || configured_last > published_last).then(|| {
            format!(
                "Requested workers need ports {port_base}-{configured_last}, but the published worker range is {}-{published_last}.",
                self.worker_port_published_base
            )
        })
    }
}

pub fn load() -> anyhow::Result<AppConfig> {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("API_KEY").map_err(|_| {
        anyhow::anyhow!("API_KEY env var is required — generate one with `openssl rand -hex 32`")
    })?;
    if api_key.len() < 16 {
        anyhow::bail!("API_KEY must be at least 16 characters");
    }

    // Default covers both the docker-compose frontend (:5173) and the native Vite dev
    // server (:3000). The blank-base-URL same-origin proxy path needs no CORS at all;
    // these origins only matter for split-origin (cross-origin) setups.
    let allowed_origins: Vec<String> = std::env::var("ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:5173,http://localhost:3000".to_string())
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
    let legacy_user_name = std::env::var("USER_NAME").unwrap_or_else(|_| "default".to_string());
    let legacy_user_password = std::env::var("USER_PASSWORD").unwrap_or_default();
    let username_prefix = if worker_account_mode == "same" {
        std::env::var("WORKER_USER_NAME").unwrap_or_else(|_| legacy_user_name.clone())
    } else {
        std::env::var("WORKER_USERNAME_PREFIX").unwrap_or_else(|_| "worker".to_string())
    };
    let worker_password =
        std::env::var("WORKER_USER_PASSWORD").unwrap_or_else(|_| legacy_user_password.clone());
    let port_base = std::env::var("WORKER_PORT_BASE")
        .or_else(|_| std::env::var("LISTEN_PORT"))
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(41000);
    let worker_port_count = std::env::var("WORKER_PORT_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    let worker_port_published_base = std::env::var("WORKER_PORT_PUBLISHED_BASE")
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
    let share_username = std::env::var("SHARE_USER_NAME").unwrap_or(legacy_user_name);
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
        worker_password,
        worker_account_mode,
        port_base,
        worker_port_count,
        worker_port_published_base,
        run_id_prefix,
        download_path,
        redis_url,
        jaeger_url,
        api_key,
        allowed_origins,
        share_mode,
        share_path,
        share_username,
        search_timeout_secs,
        search_empty_result_cutoff,
    })
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use std::path::PathBuf;

    fn config(worker: &str, share: &str, share_mode: &str) -> AppConfig {
        AppConfig {
            bind: "127.0.0.1:3124".to_string(),
            worker_count: 1,
            username_prefix: worker.to_string(),
            worker_password: "secret".to_string(),
            worker_account_mode: "same".to_string(),
            port_base: 41000,
            worker_port_count: 32,
            worker_port_published_base: 41000,
            run_id_prefix: "run".to_string(),
            download_path: PathBuf::from("/downloads"),
            redis_url: "redis://redis:6379".to_string(),
            jaeger_url: "http://jaeger:16686".to_string(),
            api_key: "0123456789abcdef".to_string(),
            allowed_origins: vec!["http://localhost:5173".to_string()],
            share_mode: share_mode.to_string(),
            share_path: "/downloads".to_string(),
            share_username: share.to_string(),
            search_timeout_secs: 20,
            search_empty_result_cutoff: 8,
        }
    }

    #[test]
    fn detects_external_share_account_conflict() {
        assert!(config("same-user", "same-user", "external").account_conflict());
        assert!(!config("worker", "share", "external").account_conflict());
        assert!(!config("same-user", "same-user", "disabled").account_conflict());
    }

    #[test]
    fn detects_external_share_account_conflict_for_suffixed_workers() {
        let mut config = config("worker", "worker3", "external");
        config.worker_account_mode = "suffixed".to_string();
        config.worker_count = 4;

        assert!(config.account_conflict());
        assert!(!config.account_conflict_for(2, "worker"));
    }

    #[test]
    fn reports_worker_username_pattern() {
        let mut config = config("worker", "share", "external");
        assert_eq!(config.worker_username_pattern(), "worker");

        config.worker_account_mode = "suffixed".to_string();
        config.worker_count = 4;
        assert_eq!(config.worker_username_pattern(), "worker1..worker4");
    }

    #[test]
    fn warns_when_worker_count_exceeds_published_port_capacity() {
        let mut config = config("worker", "share", "external");
        config.worker_count = 33;
        config.worker_port_count = 32;

        assert!(config.worker_port_capacity_warning().is_some());

        config.worker_count = 32;
        assert!(config.worker_port_capacity_warning().is_none());
    }

    #[test]
    fn warns_when_requested_worker_ports_are_outside_published_range() {
        let config = config("worker", "share", "external");

        assert!(config.worker_port_capacity_warning_for(1, 40999).is_some());
        assert!(config.worker_port_capacity_warning_for(33, 41000).is_some());
        assert!(config.worker_port_capacity_warning_for(32, 41000).is_none());
    }
}
