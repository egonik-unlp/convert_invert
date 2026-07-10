use serde::Deserialize;

use crate::errors::ApiError;

/// Bounds derived from operational concerns: workers consume Soulseek listen ports
/// and a slot in tokio's runtime, so 32 is a generous-but-bounded ceiling. Ports
/// below 10000 risk colliding with ephemeral system ports.
const MAX_WORKER_COUNT: usize = 32;
const MIN_PORT_BASE: u16 = 10_000;
const MAX_PORT_BASE: u16 = 65_000;
const MAX_CHUNK_SIZE: usize = 1000;

#[derive(Deserialize)]
pub struct StartRequest {
    pub worker_count: Option<usize>,
    pub username_prefix: Option<String>,
    pub port_base: Option<u16>,
    pub run_id_prefix: Option<String>,
    pub playlist_id: Option<String>,
    pub chunk_size: Option<usize>,
    pub playlist_range_start: Option<usize>,
    pub playlist_range_end: Option<usize>,
    pub random_order: Option<bool>,
}

pub struct StartRequestValidated {
    pub worker_count: usize,
    pub username_prefix: String,
    pub port_base: u16,
    pub run_id_prefix: String,
    pub playlist_id: String,
    pub chunk_size: usize,
    pub playlist_range: Option<(usize, usize)>,
    pub random_order: bool,
}

impl StartRequest {
    pub fn validate(
        self,
        default_worker_count: usize,
        default_username_prefix: &str,
        default_port_base: u16,
        default_run_id_prefix: &str,
        worker_account_mode: &str,
    ) -> Result<StartRequestValidated, ApiError> {
        let worker_count = self.worker_count.unwrap_or(default_worker_count);
        if worker_count == 0 || worker_count > MAX_WORKER_COUNT {
            return Err(ApiError::BadRequest(format!(
                "worker_count must be 1..={MAX_WORKER_COUNT}"
            )));
        }
        // `same` mode no longer forces a single worker: all Soulseek I/O is delegated to the one
        // aioslsk engine (a single login), so N workers are just parallel orchestration loops
        // sharing it — the downloader username is vestigial and identical across them.

        let port_base = self.port_base.unwrap_or(default_port_base);
        if !(MIN_PORT_BASE..=MAX_PORT_BASE).contains(&port_base) {
            return Err(ApiError::BadRequest(format!(
                "port_base must be {MIN_PORT_BASE}..={MAX_PORT_BASE}"
            )));
        }
        // Last worker's port = port_base + (worker_count-1); make sure it fits.
        if (port_base as usize) + worker_count > MAX_PORT_BASE as usize {
            return Err(ApiError::BadRequest(format!(
                "port_base + worker_count exceeds {MAX_PORT_BASE}"
            )));
        }

        let chunk_size = self.chunk_size.unwrap_or(15).max(1);
        if chunk_size > MAX_CHUNK_SIZE {
            return Err(ApiError::BadRequest(format!(
                "chunk_size must be 1..={MAX_CHUNK_SIZE}"
            )));
        }

        let playlist_id = self.playlist_id.ok_or_else(|| {
            ApiError::BadRequest(
                "playlist_id is required — pass a Spotify playlist ID in the request body".into(),
            )
        })?;
        if playlist_id.trim().is_empty() {
            return Err(ApiError::BadRequest("playlist_id is empty".into()));
        }

        let playlist_range = match (self.playlist_range_start, self.playlist_range_end) {
            (Some(start), Some(end)) if start < end => Some((start, end)),
            (Some(_), Some(_)) => {
                return Err(ApiError::BadRequest(
                    "playlist_range_start must be < playlist_range_end".into(),
                ));
            }
            _ => None,
        };

        let username_prefix = if worker_account_mode == "same" {
            default_username_prefix.to_string()
        } else {
            self.username_prefix
                .filter(|prefix| !prefix.trim().is_empty())
                .unwrap_or_else(|| default_username_prefix.to_string())
        };
        if worker_account_mode != "same" && username_prefix.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "WORKER_USERNAME_PREFIX is required when WORKER_ACCOUNT_MODE is not same".into(),
            ));
        }
        let run_id_prefix = self
            .run_id_prefix
            .filter(|prefix| !prefix.is_empty())
            .unwrap_or_else(|| default_run_id_prefix.to_string());

        Ok(StartRequestValidated {
            worker_count,
            username_prefix,
            port_base,
            run_id_prefix,
            playlist_id,
            chunk_size,
            playlist_range,
            random_order: self.random_order.unwrap_or(false),
        })
    }
}

#[derive(Deserialize)]
pub struct StopRequest {
    pub pids: Option<Vec<u32>>,
}

#[derive(Deserialize)]
pub struct PlaylistQuery {
    pub limit: Option<usize>,
    pub cursor: Option<i32>,
}

pub struct PlaylistQueryValidated {
    pub limit: i64,
    pub cursor: Option<i32>,
}

impl PlaylistQuery {
    pub fn validate(self) -> Result<PlaylistQueryValidated, ApiError> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(ApiError::BadRequest("limit must be 1..=200".into()));
        }
        Ok(PlaylistQueryValidated {
            limit: limit as i64,
            cursor: self.cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StartRequest;
    use crate::errors::ApiError;

    fn start_request(worker_count: Option<usize>, username_prefix: Option<&str>) -> StartRequest {
        StartRequest {
            worker_count,
            username_prefix: username_prefix.map(str::to_string),
            port_base: None,
            run_id_prefix: None,
            playlist_id: Some("playlist".to_string()),
            chunk_size: None,
            playlist_range_start: None,
            playlist_range_end: None,
            random_order: None,
        }
    }

    #[test]
    fn same_account_mode_accepts_one_worker_with_configured_username() {
        let result = start_request(Some(1), None)
            .validate(1, "real-user", 41000, "run", "same")
            .expect("valid same-account request");

        assert_eq!(result.worker_count, 1);
        assert_eq!(result.username_prefix, "real-user");
    }

    #[test]
    fn same_account_mode_accepts_multiple_workers() {
        // The aioslsk engine owns the single login, so N same-mode workers are safe (they are
        // parallel orchestration loops sharing it); the downloader username stays identical.
        let result = start_request(Some(2), None)
            .validate(1, "real-user", 41000, "run", "same")
            .expect("same-account mode should now accept multiple workers");

        assert_eq!(result.worker_count, 2);
        assert_eq!(result.username_prefix, "real-user");
    }

    #[test]
    fn same_account_mode_ignores_username_override() {
        let result =
            start_request(Some(1), Some("other")).validate(1, "real-user", 41000, "run", "same");

        let validated = result.expect("same-account mode should ignore username override");
        assert_eq!(validated.username_prefix, "real-user");
    }

    #[test]
    fn random_order_defaults_to_false() {
        let result = start_request(Some(1), None)
            .validate(1, "real-user", 41000, "run", "same")
            .expect("valid request");

        assert!(!result.random_order);
    }

    #[test]
    fn random_order_can_be_enabled() {
        let mut request = start_request(Some(1), None);
        request.random_order = Some(true);
        let result = request
            .validate(1, "real-user", 41000, "run", "same")
            .expect("valid request");

        assert!(result.random_order);
    }

    #[test]
    fn suffixed_account_mode_accepts_multiple_workers() {
        let result = start_request(Some(4), None)
            .validate(4, "worker", 41000, "run", "suffixed")
            .expect("valid suffixed-account request");

        assert_eq!(result.worker_count, 4);
        assert_eq!(result.username_prefix, "worker");
    }

    #[test]
    fn suffixed_account_mode_requires_username_prefix() {
        let result = start_request(Some(4), None).validate(4, "", 41000, "run", "suffixed");

        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message.contains("WORKER_USERNAME_PREFIX"))
        );
    }
}
