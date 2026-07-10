use crate::internals::{
    context::context_manager::{Track, send},
    parsing::deserialize::Playlist,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};
use tokio::sync::{Semaphore, mpsc::Sender};
use tracing::instrument;

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

pub fn is_audio_file(filename: &str) -> bool {
    let lower = filename.to_ascii_lowercase();
    [
        ".mp3", ".flac", ".aiff", ".aif", ".aac", ".m4a", ".ogg", ".opus", ".wav",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
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
    pub relative_mi_score: Option<f32>,
}
impl PartialEq for JudgeSubmission {
    fn eq(&self, other: &Self) -> bool {
        self.track.eq(&other.track) && self.query.eq(&other.query)
    }
}

#[derive(Serialize)]
struct SearchRequestBody<'a> {
    query: &'a str,
    timeout: u8,
    max_results: u32,
}

#[derive(Deserialize)]
struct SearchResponseBody {
    results: Vec<SlskCandidate>,
}

#[derive(Deserialize)]
struct SlskCandidate {
    username: String,
    filename: String,
    size: u64,
}

/// Searches Soulseek by delegating to the aioslsk engine service over HTTP. The service
/// collects results for `timeout` seconds and returns audio candidates; we forward each as a
/// `Track::Result` for judging. One login (in the service) covers search + download + share,
/// and aioslsk's server-brokered connections make transfers work from behind NAT/CGNAT.
pub struct SearchManager {
    http: reqwest::Client,
    base_url: String,
}

impl SearchManager {
    pub fn new(http: reqwest::Client, base_url: String) -> Self {
        SearchManager { http, base_url }
    }

    #[instrument(
        name = "track_search_task",
        skip(self, semaphore, sender),
        fields(id = %track.track_id, query = %track.track),
    )]
    pub async fn run(
        &self,
        track: SearchItem,
        count_cutoff: usize,
        timeout_secs: u8,
        relaxed_query: bool,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
    ) -> anyhow::Result<SearchOutcome> {
        let _ = count_cutoff; // the engine service owns the collection window now
        let _permit = semaphore.acquire().await.context("Getting permit")?;

        let query_string = if relaxed_query {
            track.track.clone()
        } else {
            format!("{} - {}", track.track.as_str(), track.artist)
        };

        let body = serde_json::to_string(&SearchRequestBody {
            query: &query_string,
            timeout: timeout_secs.max(1),
            max_results: 200,
        })
        .context("serialize search request")?;

        let response = self
            .http
            .post(format!("{}/search", self.base_url))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .context("search request to engine")?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            tracing::warn!(%status, %query_string, relaxed_query, detail = %text, "Engine search failed");
            return Ok(SearchOutcome {
                exit_reason: SearchExitReason::Cancelled,
                candidates_sent: 0,
            });
        }
        let text = response.text().await.context("read search body")?;
        let parsed: SearchResponseBody =
            serde_json::from_str(&text).context("parse search body")?;

        let mut candidates_sent = 0usize;
        for candidate in parsed.results {
            if !is_audio_file(&candidate.filename) {
                continue;
            }
            let Some(size) = downloadable_size(candidate.size) else {
                continue;
            };
            let submission = JudgeSubmission {
                query: DownloadableFile {
                    filename: candidate.filename,
                    username: candidate.username,
                    size,
                },
                track: track.clone(),
                score: None,
                relative_mi_score: None,
            };
            send(Track::Result(submission), &sender)
                .await
                .context("Sending result")?;
            candidates_sent += 1;
        }

        if candidates_sent == 0 {
            tracing::info!(
                query_string,
                relaxed_query,
                exit_reason = "NoCandidatesFound",
                "Exited because consecutive empty results",
            );
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
}

#[cfg(test)]
mod tests {
    use super::{SearchItem, downloadable_size, is_audio_file};

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

    #[test]
    fn audio_detection_accepts_common_music_extensions() {
        assert!(is_audio_file("song.MP3"));
        assert!(is_audio_file("song.flac"));
        assert!(is_audio_file("song.m4a"));
        assert!(is_audio_file("song.opus"));
        assert!(is_audio_file("song.ogg"));
        assert!(is_audio_file("song.wav"));
        assert!(is_audio_file("song.aif"));
    }

    #[test]
    fn audio_detection_rejects_non_audio_files() {
        assert!(!is_audio_file("folder.jpg"));
        assert!(!is_audio_file("playlist.m3u8"));
        assert!(!is_audio_file("song.flac.txt"));
    }
}
