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
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc::Sender},
    time::sleep,
};
use tracing::{Instrument, info_span, instrument};

const TIMES_WITH_NO_NEW_FILES: usize = 3;

#[derive(Debug, Deserialize, Serialize, Clone, Hash, PartialEq, Eq)]
pub struct SearchItem {
    pub track_id: u64,
    pub track: String,
    pub album: String,
    pub artist: String,
}
impl SearchItem {
    pub fn new(track: String, album: String, artist: String) -> Self {
        let track_id = {
            let mut s = DefaultHasher::new();
            track.hash(&mut s);
            album.hash(&mut s);
            artist.hash(&mut s);
            s.finish()
        };
        SearchItem {
            track_id,
            track,
            album,
            artist,
        }
    }
}
impl From<Playlist> for Vec<SearchItem> {
    fn from(value: Playlist) -> Vec<SearchItem> {
        let tracks = value.tracks.unwrap().items.unwrap();
        tracks
            .into_iter()
            .map(|tr| {
                let track = tr.clone().track.unwrap().name.unwrap();
                let artist = tr
                    .track
                    .clone()
                    .unwrap()
                    .artists
                    .unwrap()
                    .first()
                    .unwrap()
                    .name
                    .clone()
                    .unwrap()
                    .clone();
                let album = tr.track.unwrap().album.unwrap().name.unwrap();
                SearchItem::new(track, album, artist)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DownloadableFile {
    pub filename: String,
    pub username: String,
    pub size: i32,
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
    pub handles: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl SearchManager {
    pub fn new(client: Arc<soulseek_rs::Client>) -> Self {
        SearchManager {
            client,
            handles: vec![],
        }
    }
    fn build_submissions(track: SearchItem, result: SearchResult) -> Vec<JudgeSubmission> {
        result
            .files
            .into_iter()
            .map(|f| JudgeSubmission {
                query: DownloadableFile {
                    filename: f.name,
                    size: f.size as i32,
                    username: f.username,
                },
                track: track.clone(),
                score: None,
            })
            .collect()
    }
    pub async fn run(
        &self,
        track: SearchItem,
        count_cutoff: usize,
        semaphore: Arc<Semaphore>,
        sender: Arc<Sender<Track>>,
    ) -> anyhow::Result<()> {
        let client = self.client.clone();
        let _permit = semaphore.acquire().await.context("Getting permit")?;
        track_search_task(client, track, count_cutoff, sender)
            .await
            .context("TST")?;
        Ok(())
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
    sender: Arc<Sender<Track>>,
) -> anyhow::Result<()> {
    let query_string = format!("{} - {}", data.track.as_str(), data.artist);
    let cancel = Arc::new(AtomicBool::new(false));
    let span = info_span!("track_blocking");
    let search_thread = {
        let search_client = client.clone();
        let query_string_search = query_string.clone();
        let cancel_search = cancel.clone();
        tokio::task::spawn_blocking(move || {
            search_client.search_with_cancel(
                query_string_search.as_str(),
                Duration::from_secs(30), // Reduced duration for faster exit
                Some(cancel_search),
            )
        })
    }
    .instrument(span);
    let mut previous_submissions = HashSet::new();
    let mut count = 0;
    let mut total_files_found = 0;
    'main: loop {
        sleep(Duration::from_secs(10)).await;
        let results = client.get_search_results(&query_string);
        let results_count: usize = results.iter().map(|res| res.files.len()).sum();
        if !results.is_empty() && !total_files_found.eq(&results_count) {
            total_files_found += results_count - total_files_found;
            for result in results {
                let submisssions = SearchManager::build_submissions(data.clone(), result);
                for submission in submisssions {
                    if !previous_submissions.contains(&(
                        submission.query.filename.clone(),
                        submission.query.username.clone(),
                    )) {
                        send(Track::Result(submission.clone()), &sender)
                            .await
                            .context("Sending result")?;
                        previous_submissions
                            .insert((submission.query.filename, submission.query.username));
                    }
                }
            }
            count = 0;
        } else {
            count += 1;
        }
        if count > count_cutoff {
            tracing::info!(
                times = TIMES_WITH_NO_NEW_FILES,
                query_string = query_string,
                "Exited because consecutive empty results",
            );
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            break 'main;
        }
    }
    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    search_thread
        .await
        .unwrap()
        .context("Inner search thread issue")?;
    Ok(())
}
