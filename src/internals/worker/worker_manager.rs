use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use rand::seq::SliceRandom;
use redis::Commands;
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::internals::context::context_manager::{Managers, RedisPool, Track, WorkerTuning};
use crate::internals::database::DbPool;
use crate::internals::database::manager::DatabaseManager;
use crate::internals::query::query_manager::QueryManager;
use crate::internals::search::search_manager::SearchItem;
use crate::internals::utils::config::config_manager::Config;

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct WorkerInfo {
    pub id: usize,
    pub username: String,
    pub port: u16,
    pub run_id: String,
    pub started_at_epoch_secs: u64,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct WorkerStatus {
    pub workers: Vec<WorkerInfo>,
    pub queue_len: usize,
    pub failed_count: usize,
}

#[derive(Clone, Debug)]
pub struct WorkerStartOptions {
    pub worker_count: usize,
    pub username_prefix: String,
    pub port_base: u16,
    pub run_id_prefix: String,
    pub account_mode: String,
    pub playlist_id: String,
    pub chunk_size: usize,
    pub playlist_range: Option<(usize, usize)>,
    pub random_order: bool,
}

pub struct WorkerSupervisor {
    workers: Mutex<Vec<WorkerHandle>>,
    next_worker_id: AtomicUsize,
    queue_key: Mutex<Option<String>>,
    download_path: PathBuf,
    db_pool: DbPool,
    redis_pool: RedisPool,
    shutdown: watch::Receiver<bool>,
}

struct WorkerHandle {
    info: WorkerInfo,
    handle: JoinHandle<()>,
}

struct WorkerRunConfig {
    worker_config: Config,
    playlist_id: String,
    chunk_size: usize,
    download_path: PathBuf,
    /// Sanitized playlist name; completed files land in `download_path/<download_subdir>` so
    /// each playlist gets its own folder. Empty means "download straight into `download_path`".
    download_subdir: String,
    redis_pool: RedisPool,
    db_pool: DbPool,
    is_leader: bool,
    all_items: Vec<SearchItem>,
    shutdown: watch::Receiver<bool>,
}

impl WorkerSupervisor {
    pub fn new(
        download_path: PathBuf,
        db_pool: DbPool,
        redis_pool: RedisPool,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            workers: Mutex::new(Vec::new()),
            next_worker_id: AtomicUsize::new(1),
            queue_key: Mutex::new(None),
            download_path,
            db_pool,
            redis_pool,
            shutdown,
        }
    }

    pub async fn start(
        &self,
        options: WorkerStartOptions,
        base_config: Config,
        user_password: String,
    ) -> anyhow::Result<Vec<WorkerInfo>> {
        let query_manager = QueryManager::new_with_timeout(
            options.playlist_id.clone(),
            base_config.client_id.clone(),
            base_config.client_secret.clone(),
            base_config.search_timeout_secs,
        );
        let (playlist_name, playlist_tracks) = query_manager
            .fetch_playlist_with_name()
            .await
            .context("Fetch worker playlist")?;
        let download_subdir = sanitize_folder_name(&playlist_name)
            .unwrap_or_else(|| sanitize_folder_name(&options.playlist_id).unwrap_or_default());
        let playlist_tracks = apply_playlist_range(playlist_tracks, options.playlist_range);
        let mut items = playlist_tracks
            .into_iter()
            .filter_map(|track| match track {
                Track::Query(item) => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        if options.random_order {
            shuffle_items(&mut items);
        }

        self.persist_search_items(&items)
            .context("Persist fetched playlist tracks")?;

        self.replace_queue(&options.playlist_id, options.chunk_size, &items)
            .context("Build worker queue")?;

        let tuning = WorkerTuning::from_env();
        let last_port = options
            .port_base
            .saturating_add(options.worker_count.saturating_sub(1) as u16);
        tracing::info!(
            worker_count = options.worker_count,
            chunk_size = options.chunk_size,
            port_base = options.port_base,
            port_last = last_port,
            search_concurrency = tuning.search_concurrency,
            download_concurrency = tuning.download_concurrency,
            max_candidates_per_track = tuning.max_candidates_per_track,
            max_download_attempts_per_track = tuning.max_download_attempts_per_track,
            candidate_collection_secs = tuning.candidate_collection_secs,
            max_search_passes_per_track = tuning.max_search_passes_per_track,
            max_requests_per_track = tuning.max_requests_per_track,
            playlist_id = %options.playlist_id,
            playlist = %playlist_name,
            download_subdir = %download_subdir,
            random_order = options.random_order,
            account_mode = %options.account_mode,
            username = %options.username_prefix,
            "Starting workers",
        );

        let mut spawned = Vec::with_capacity(options.worker_count);
        let mut guard = self
            .workers
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lock poisoned"))?;

        for index in 0..options.worker_count {
            let info = worker_info(
                self.next_worker_id.fetch_add(1, Ordering::Relaxed),
                &options.username_prefix,
                &options.account_mode,
                options.port_base,
                &options.run_id_prefix,
                index,
            );

            let mut worker_config = base_config.clone();
            worker_config.user_name = info.username.clone();
            worker_config.user_password = user_password.clone();
            worker_config.listen_port = u32::from(info.port);
            worker_config.run_id = info.run_id.clone();

            let handle = tokio::spawn(run_worker(WorkerRunConfig {
                worker_config,
                playlist_id: options.playlist_id.clone(),
                chunk_size: options.chunk_size,
                download_path: self.download_path.clone(),
                download_subdir: download_subdir.clone(),
                redis_pool: self.redis_pool.clone(),
                db_pool: self.db_pool.clone(),
                is_leader: index == 0,
                all_items: items.clone(),
                shutdown: self.shutdown.clone(),
            }));

            spawned.push(info.clone());
            guard.push(WorkerHandle { info, handle });
        }

        Ok(spawned)
    }

    pub fn stop(&self, target_ids: Option<&[u32]>) -> anyhow::Result<Vec<u32>> {
        let mut guard = self
            .workers
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lock poisoned"))?;
        let mut stopped = Vec::new();
        let mut remaining = Vec::with_capacity(guard.len());

        for entry in guard.drain(..) {
            let should_stop = match target_ids {
                None => true,
                Some(ids) => ids.contains(&(entry.info.id as u32)),
            };

            if should_stop {
                entry.handle.abort();
                stopped.push(entry.info.id as u32);
            } else {
                remaining.push(entry);
            }
        }

        *guard = remaining;
        Ok(stopped)
    }

    pub fn status(&self) -> anyhow::Result<WorkerStatus> {
        let mut guard = self
            .workers
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lock poisoned"))?;
        let mut workers = Vec::with_capacity(guard.len());

        guard.retain_mut(|entry| {
            if entry.handle.is_finished() {
                false
            } else {
                workers.push(entry.info.clone());
                true
            }
        });

        let queue_key = self
            .queue_key
            .lock()
            .map_err(|_| anyhow::anyhow!("queue key lock poisoned"))?
            .clone();
        let (queue_len, failed_count) = if let Some(key) = queue_key {
            let mut redis_con = self.redis_pool.get().context("Acquire Redis connection")?;
            let queue_len = redis_con.llen(key).unwrap_or(0);
            let failed_count = redis_con.scard("dl:failed").unwrap_or(0);
            (queue_len, failed_count)
        } else {
            (0, 0)
        };

        Ok(WorkerStatus {
            workers,
            queue_len,
            failed_count,
        })
    }

    fn persist_search_items(&self, items: &[SearchItem]) -> anyhow::Result<()> {
        let mut conn = self.db_pool.get().context("Acquire DB connection")?;
        let mut database_manager = DatabaseManager::new(&mut conn);
        for item in items {
            database_manager
                .load_item_to_database(&Track::Query(item.clone()))
                .with_context(|| format!("Persist search item {}", item.track_id))?;
        }
        tracing::info!(
            track_count = items.len(),
            "Persisted fetched playlist tracks"
        );
        Ok(())
    }

    fn replace_queue(
        &self,
        playlist_id: &str,
        chunk_size: usize,
        items: &[SearchItem],
    ) -> anyhow::Result<()> {
        let queue_key = chunk_queue_key(playlist_id, chunk_size);
        self.queue_key
            .lock()
            .map_err(|_| anyhow::anyhow!("queue key lock poisoned"))?
            .replace(queue_key.clone());

        let chunks = build_chunks(items, chunk_size);
        let mut redis_con = self.redis_pool.get().context("Acquire Redis connection")?;
        let _: usize = redis_con.del(queue_key.clone()).unwrap_or(0);
        for chunk in chunks {
            let payload = serde_json::to_string(&chunk).context("Serialise worker chunk")?;
            let _: usize = redis_con.rpush(&queue_key, payload).unwrap_or(0);
        }
        Ok(())
    }
}

pub fn chunk_queue_key(playlist_id: &str, chunk_size: usize) -> String {
    format!("dl:chunk_queue:{playlist_id}:{chunk_size}")
}

/// Turn a Spotify playlist name into a safe single-segment folder name: drop path separators
/// and characters that are illegal on common filesystems, collapse whitespace, and cap the
/// length. Returns `None` if nothing usable remains (caller falls back to the playlist id).
pub fn sanitize_folder_name(name: &str) -> Option<String> {
    let cleaned: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => ' ',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect();
    // Collapse runs of whitespace into single spaces and trim.
    let collapsed = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    // Avoid leading/trailing dots (hidden files / Windows quirks) and cap the length.
    let trimmed = collapsed.trim_matches('.').trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(
        trimmed
            .chars()
            .take(120)
            .collect::<String>()
            .trim()
            .to_string(),
    )
}

pub fn build_chunks(items: &[SearchItem], chunk_size: usize) -> Vec<Vec<SearchItem>> {
    let chunk_size = chunk_size.max(1);
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < items.len() {
        let end = (start + chunk_size).min(items.len());
        chunks.push(items[start..end].to_vec());
        start = end;
    }
    chunks
}

pub fn shuffle_items(items: &mut [SearchItem]) {
    let mut rng = rand::rng();
    items.shuffle(&mut rng);
}

pub fn apply_playlist_range(
    playlist_tracks: Vec<Track>,
    playlist_range: Option<(usize, usize)>,
) -> Vec<Track> {
    let Some((start, end)) = playlist_range else {
        return playlist_tracks;
    };
    let start = start.min(playlist_tracks.len());
    let end = end.min(playlist_tracks.len());
    if start >= end {
        return playlist_tracks;
    }
    playlist_tracks
        .into_iter()
        .skip(start)
        .take(end - start)
        .collect()
}

fn worker_info(
    worker_id: usize,
    username_prefix: &str,
    account_mode: &str,
    port_base: u16,
    run_id_prefix: &str,
    index: usize,
) -> WorkerInfo {
    let worker_number = index + 1;
    let username = if account_mode == "same" {
        username_prefix.to_string()
    } else {
        format!("{username_prefix}{worker_number}")
    };
    WorkerInfo {
        id: worker_id,
        username,
        port: port_base.saturating_add(index as u16),
        run_id: format!("{run_id_prefix}-{worker_number}"),
        started_at_epoch_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    }
}

async fn run_worker(config: WorkerRunConfig) {
    let WorkerRunConfig {
        worker_config,
        playlist_id,
        chunk_size,
        download_path,
        download_subdir,
        redis_pool,
        db_pool,
        is_leader,
        all_items,
        mut shutdown,
    } = config;

    // Completed files are organised into a per-playlist subfolder of the shared download dir.
    let run_download_path = if download_subdir.is_empty() {
        download_path.clone()
    } else {
        download_path.join(&download_subdir)
    };

    let queue_key = chunk_queue_key(&playlist_id, chunk_size);
    let managers = match Managers::new(
        worker_config.judge_score_levenshtein,
        run_download_path.clone(),
        worker_config.clone(),
        db_pool.clone(),
        redis_pool.clone(),
    ) {
        Ok(managers) => Arc::new(managers),
        Err(err) => {
            tracing::error!(?err, run_id = %worker_config.run_id, "Worker failed to start managers");
            return;
        }
    };

    loop {
        if *shutdown.borrow() {
            tracing::info!(run_id = %worker_config.run_id, "Worker exiting due to shutdown");
            managers.shutdown();
            return;
        }

        let chunk_json: Option<String> = {
            let mut redis_con = match redis_pool.get() {
                Ok(con) => con,
                Err(err) => {
                    tracing::error!(?err, "Worker failed to acquire Redis connection; exiting");
                    return;
                }
            };
            redis_con.lpop(&queue_key, None).ok()
        };
        let Some(chunk_json) = chunk_json else {
            break;
        };

        let chunk_items: Vec<SearchItem> = match serde_json::from_str(&chunk_json) {
            Ok(value) => value,
            Err(err) => {
                let truncated = chunk_json.chars().take(500).collect::<String>();
                tracing::error!(
                    %err,
                    payload = %truncated,
                    run_id = %worker_config.run_id,
                    "Worker skipped malformed chunk payload",
                );
                continue;
            }
        };

        let tracks = chunk_items
            .into_iter()
            .map(Track::Query)
            .collect::<Vec<_>>();

        tokio::select! {
            result = managers.run_chunk(tracks) => {
                if let Err(err) = result {
                    tracing::error!(
                        ?err,
                        run_id = %worker_config.run_id,
                        username = %worker_config.user_name,
                        port = worker_config.listen_port,
                        chunk_size,
                        "run_chunk failed",
                    );
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(run_id = %worker_config.run_id, "Worker shutdown mid-cycle");
                    managers.shutdown();
                    return;
                }
            }
        }
    }

    if is_leader && !*shutdown.borrow() {
        let failed_ids: Vec<String> = {
            let mut redis_con = match redis_pool.get() {
                Ok(con) => con,
                Err(_) => return,
            };
            redis_con.smembers("dl:failed").unwrap_or_default()
        };
        if !failed_ids.is_empty() {
            let failed_items = all_items
                .into_iter()
                .filter(|item| failed_ids.contains(&item.track_id))
                .map(Track::Query)
                .collect::<Vec<_>>();
            if !failed_items.is_empty()
                && let Err(err) = managers.run_chunk(failed_items).await
            {
                tracing::error!(
                    ?err,
                    run_id = %worker_config.run_id,
                    username = %worker_config.user_name,
                    port = worker_config.listen_port,
                    chunk_size,
                    "failed-track replay run_chunk failed",
                );
            }
        }
    }
    managers.shutdown();
}

#[cfg(test)]
mod tests {
    use super::{
        apply_playlist_range, build_chunks, chunk_queue_key, sanitize_folder_name, worker_info,
    };
    use crate::internals::context::context_manager::Track;
    use crate::internals::search::search_manager::SearchItem;

    fn item(id: &str) -> SearchItem {
        SearchItem::new(
            id.to_string(),
            format!("track-{id}"),
            "album".to_string(),
            "artist".to_string(),
        )
    }

    #[test]
    fn queue_key_includes_playlist_and_chunk_size() {
        assert_eq!(
            chunk_queue_key("playlist", 15),
            "dl:chunk_queue:playlist:15"
        );
    }

    #[test]
    fn chunks_items_by_requested_size() {
        let items = vec![item("1"), item("2"), item("3"), item("4"), item("5")];
        let chunks = build_chunks(&items, 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 2);
        assert_eq!(chunks[1].len(), 2);
        assert_eq!(chunks[2].len(), 1);
    }

    #[test]
    fn chunk_size_zero_is_treated_as_one() {
        let items = vec![item("1"), item("2")];
        let chunks = build_chunks(&items, 0);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn range_is_applied_before_chunking() {
        let tracks = vec![item("1"), item("2"), item("3"), item("4")]
            .into_iter()
            .map(Track::Query)
            .collect::<Vec<_>>();
        let ranged = apply_playlist_range(tracks, Some((1, 3)));
        let items = ranged
            .into_iter()
            .filter_map(|track| match track {
                Track::Query(item) => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>();
        let chunks = build_chunks(&items, 1);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0][0].track_id, "2");
        assert_eq!(chunks[1][0].track_id, "3");
    }

    #[test]
    fn applies_playlist_range_when_valid_after_clamping() {
        let tracks = vec![item("1"), item("2"), item("3")]
            .into_iter()
            .map(Track::Query)
            .collect::<Vec<_>>();
        let ranged = apply_playlist_range(tracks, Some((1, 10)));
        assert_eq!(ranged.len(), 2);
    }

    #[test]
    fn worker_info_uses_one_based_worker_suffixes() {
        let info = worker_info(7, "worker", "multi", 41000, "run", 2);
        assert_eq!(info.id, 7);
        assert_eq!(info.username, "worker3");
        assert_eq!(info.port, 41002);
        assert_eq!(info.run_id, "run-3");
    }

    #[test]
    fn worker_info_uses_exact_username_in_same_account_mode() {
        let info = worker_info(7, "real-user", "same", 41000, "run", 0);
        assert_eq!(info.id, 7);
        assert_eq!(info.username, "real-user");
        assert_eq!(info.port, 41000);
        assert_eq!(info.run_id, "run-1");
    }

    #[test]
    fn sanitize_folder_name_keeps_readable_names() {
        assert_eq!(
            sanitize_folder_name("My Chill Mix").as_deref(),
            Some("My Chill Mix")
        );
    }

    #[test]
    fn sanitize_folder_name_strips_path_and_illegal_chars() {
        assert_eq!(
            sanitize_folder_name("Rock/Metal: the *best*?").as_deref(),
            Some("Rock Metal the best"),
        );
        assert_eq!(
            sanitize_folder_name("a\\b\"c<d>e|f").as_deref(),
            Some("a b c d e f"),
        );
    }

    #[test]
    fn sanitize_folder_name_collapses_whitespace_and_trims_dots() {
        assert_eq!(
            sanitize_folder_name("  ..spaced   out..  ").as_deref(),
            Some("spaced out"),
        );
    }

    #[test]
    fn sanitize_folder_name_returns_none_when_nothing_usable() {
        assert_eq!(sanitize_folder_name("   "), None);
        assert_eq!(sanitize_folder_name("///:::"), None);
        assert_eq!(sanitize_folder_name(""), None);
    }
}
