use crate::internals::{
    context::context_manager::{Track, send},
    parsing::deserialize::Playlist,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use soulseek_rs::SearchResult;
use std::{
    collections::HashSet,
    fmt::Display,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc::Sender},
    time::sleep,
};
use tracing::{Instrument, info_span, instrument};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchExitReason {
    NoCandidatesFound,
    EmptyAfterPeerErrors,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutcome {
    pub exit_reason: SearchExitReason,
    pub candidates_sent: usize,
}

fn downloadable_size(size: u64) -> Option<i64> {
    i64::try_from(size).ok()
}

#[derive(Debug, Deserialize, Serialize, Clone, Hash, PartialEq, Eq)]
pub struct SearchItem {
    pub track_id: String,
    pub track: String,
    pub album: String,
    pub artist: String,
}
impl SearchItem {
    pub fn new(track_id: String, track: String, album: String, artist: String) -> Self {
        SearchItem {
            track_id,
            track,
            album,
            artist,
        }
    }

    pub fn from_metadata(track: String, album: String, artist: String) -> Self {
        let track_id = format!("metadata:{track}:{artist}:{album}");
        Self::new(track_id, track, album, artist)
    }
}
impl From<Playlist> for Vec<SearchItem> {
    fn from(value: Playlist) -> Vec<SearchItem> {
        value
            .tracks
            .and_then(|tracks| tracks.items)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|tr| {
                let track = tr.track?;
                let track_name = track.name?;
                let album_name = track.album?.name?;
                let artist_name = track.artists?.first()?.name.clone()?;
                Some(SearchItem::from_metadata(
                    track_name,
                    album_name,
                    artist_name,
                ))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DownloadableFile {
    pub filename: String,
    pub username: String,
    pub size: i64,
}
impl Display for SearchItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {} - {}", self.track, self.artist, self.album)
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JudgeSubmission {
    pub track: SearchItem,
    pub query: DownloadableFile,
    pub score: Option<f32>,
}
impl PartialEq for JudgeSubmission {
    fn eq(&self, other: &Self) -> bool {
        self.track.eq(&other.track) && self.query.eq(&other.query)
    }
}

pub struct SearchManager {
    pub client: Arc<soulseek_rs::Client>,
}

impl SearchManager {
    pub fn new(client: Arc<soulseek_rs::Client>) -> Self {
        SearchManager { client }
    }
    fn build_submissions(track: SearchItem, result: SearchResult) -> Vec<JudgeSubmission> {
        result
            .files
            .into_iter()
            .filter_map(|f| {
                let size = match downloadable_size(f.size) {
                    Some(size) => size,
                    None => {
                        tracing::warn!(
                            filename = f.name,
                            size = f.size,
                            "Skipping search result with size too large for storage"
                        );
                        return None;
                    }
                };
                Some(JudgeSubmission {
                    query: DownloadableFile {
                        filename: f.name,
                        size,
                        username: f.username,
                    },
                    track: track.clone(),
                    score: None,
                })
            })
            .collect()
    }
    pub async fn run(
        &self,
        track: SearchItem,
        count_cutoff: usize,
        timeout_secs: u8,
        relaxed_query: bool,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
    ) -> anyhow::Result<SearchOutcome> {
        let client = self.client.clone();
        let _permit = semaphore.acquire().await.context("Getting permit")?;
        let outcome = track_search_task(
            client,
            track,
            count_cutoff,
            timeout_secs,
            relaxed_query,
            sender,
        )
        .await
        .context("TST")?;
        Ok(outcome)
    }
}

#[instrument(
    name = "track_search_task",
    skip(client ),
    fields(
        id = data.track_id,
        query = ?data.track,
    )
)]
pub async fn track_search_task(
    client: Arc<soulseek_rs::Client>,
    data: SearchItem,
    count_cutoff: usize,
    timeout_secs: u8,
    relaxed_query: bool,
    sender: Arc<Sender<Track>>,
) -> anyhow::Result<SearchOutcome> {
    let query_string = if relaxed_query {
        data.track.clone()
    } else {
        format!("{} - {}", data.track.as_str(), data.artist)
    };
    let search_timeout = Duration::from_secs(u64::from(timeout_secs.max(1)));
    let cancel = Arc::new(AtomicBool::new(false));
    let span = info_span!("track_blocking");
    let search_thread = {
        let search_client = client.clone();
        let query_string_search = query_string.clone();
        let cancel_search = cancel.clone();
        tokio::task::spawn_blocking(move || {
            search_client.search_with_cancel(
                query_string_search.as_str(),
                search_timeout,
                Some(cancel_search),
            )
        })
    }
    .instrument(span);
    let mut previous_submissions = HashSet::new();
    let mut count = 0;
    let mut total_files_found = 0;
    let mut candidates_sent = 0usize;
    'main: loop {
        sleep(search_timeout).await;
        let results = client.get_search_results(&query_string);
        let current_total_files: usize = results.iter().map(|res| res.files.len()).sum();

        if current_total_files > total_files_found {
            for result in results {
                let submissions = SearchManager::build_submissions(data.clone(), result);
                for submission in submissions {
                    let key = (
                        submission.query.filename.clone(),
                        submission.query.username.clone(),
                    );
                    if !previous_submissions.contains(&key) {
                        send(Track::Result(submission.clone()), &sender)
                            .await
                            .context("Sending result")?;
                        previous_submissions.insert(key);
                        candidates_sent += 1;
                    }
                }
            }
            total_files_found = current_total_files;
            count = 0;
        } else {
            count += 1;
        }
        if count > count_cutoff {
            tracing::info!(
                times = count_cutoff,
                query_string = query_string,
                relaxed_query,
                exit_reason = if candidates_sent == 0 {
                    "NoCandidatesFound"
                } else {
                    "EmptyAfterPeerErrors"
                },
                "Exited because consecutive empty results",
            );
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            break 'main;
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    let search_result = search_thread
        .await
        .context("Search thread join failed")?
        .context("Inner search thread issue");
    if let Err(err) = search_result {
        tracing::warn!(
            ?err,
            query_string,
            relaxed_query,
            "Search task cancelled or failed"
        );
        return Ok(SearchOutcome {
            exit_reason: SearchExitReason::Cancelled,
            candidates_sent,
        });
    }
    Ok(SearchOutcome {
        exit_reason: if candidates_sent == 0 {
            SearchExitReason::NoCandidatesFound
        } else {
            SearchExitReason::EmptyAfterPeerErrors
        },
        candidates_sent,
    })
}

#[cfg(test)]
mod tests {
    use super::{SearchItem, downloadable_size};

    #[test]
    fn metadata_fallback_id_is_deterministic() {
        let first = SearchItem::from_metadata(
            "Track".to_string(),
            "Album".to_string(),
            "Artist".to_string(),
        );
        let second = SearchItem::from_metadata(
            "Track".to_string(),
            "Album".to_string(),
            "Artist".to_string(),
        );

        assert_eq!(first.track_id, second.track_id);
        assert_eq!(first.track_id, "metadata:Track:Artist:Album");
    }

    #[test]
    fn downloadable_size_rejects_values_that_do_not_fit_database_type() {
        assert_eq!(downloadable_size(i64::MAX as u64), Some(i64::MAX));
        assert_eq!(downloadable_size(i64::MAX as u64 + 1), None);
    }
}
