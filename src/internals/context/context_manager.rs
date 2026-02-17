use crate::internals::database::manager::DatabaseManager;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use soulseek_rs::{Client, ClientSettings};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{
        RwLock, Semaphore,
        mpsc::{self, Receiver, Sender},
    },
    task::{JoinHandle, JoinSet},
};
use tracing::{Instrument, info_span, instrument};

use anyhow::Context;

use crate::internals::{
    download::download_manager::DownloadManager,
    judge::{judge_manager::JudgeManager, judges::levenshtein::Levenshtein},
    query::query_manager::QueryManager,
    search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem, SearchManager},
    utils::config::config_manager::Config,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadedFile {
    pub filename: String,
}

#[derive(Debug)]
pub struct RetryRequest {
    pub request: JudgeSubmission,
    pub retry_attempts: u8,
    pub failed_download_result: DownloadableFile,
}

#[derive(Debug)]
pub enum Track {
    Query(SearchItem),
    Result(JudgeSubmission),
    Downloadable(JudgeSubmission),
    File(DownloadedFile),
    Retry(RetryRequest),
    Reject(RejectedTrack),
    NoMoreTracks,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RejectedTrack {
    track: JudgeSubmission,
    reason: RejectReason,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RejectReason {
    AlreadyDownloaded,
    LowScore(f32),
    NotMusic(String),
    AbandonedAttemptingSearch,
}

impl RejectedTrack {
    pub fn new(track: JudgeSubmission, reason: RejectReason) -> Self {
        Self { track, reason }
    }

    pub fn parts(&self) -> (&JudgeSubmission, &RejectReason) {
        (&self.track, &self.reason)
    }
}

pub trait Manager {
    fn run(self) -> anyhow::Result<()>;
}
pub async fn send(message: Track, chan: &Sender<Track>) -> anyhow::Result<()> {
    chan.send(message).await.context("Send to channel")?;
    Ok(())
}

pub struct Managers {
    pub client: Arc<Client>,
    pub download_manager: DownloadManager,
    pub search_manager: SearchManager,
    pub query_manager: QueryManager,
    pub judge_manager: JudgeManager,
}

#[derive(Debug)]
pub struct RunTools {
    pub download_semaphore: Semaphore,
    pub search_semaphore: Semaphore,
    pub successful_downloads: Vec<Track>,
    pub rejected_tracks: Vec<Track>,
    pub handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl RunTools {
    pub fn new(search_limit: usize, download_limit: usize) -> Self {
        let search_semaphore = Semaphore::new(search_limit);
        let download_semaphore = Semaphore::new(download_limit);
        let successful_downloads = vec![];
        let rejected_tracks = vec![];
        let handles = vec![];
        Self {
            search_semaphore,
            download_semaphore,
            successful_downloads,
            rejected_tracks,
            handles,
        }
    }
}

pub enum QueuePriority {
    NormalRun(JoinHandle<anyhow::Result<()>>),
    RetryRun(JoinHandle<anyhow::Result<()>>),
    // Terminate,
}

impl Managers {
    pub fn new(score: Option<f32>, path: PathBuf, config: Config) -> Self {
        let client_settings = ClientSettings {
            username: config.user_name,
            password: config.user_password,
            listen_port: config.listen_port,
            ..Default::default()
        };
        let mut client = Client::with_settings(client_settings);
        client.connect();
        let client = Arc::new(client);
        let download_manager = DownloadManager::new(client.clone(), path);
        let search_manager = SearchManager::new(client.clone());
        let lev_judge = Levenshtein::new(score.unwrap_or(0.75));
        let judge_manager = JudgeManager::new(Box::new(lev_judge));
        let query_manager = QueryManager::new(
            "1B3Q6EB9Pjb57jKywHJPfq?si=2f36139519544813",
            config.client_id,
            config.client_secret,
        );
        Managers {
            client,
            download_manager,
            search_manager,
            judge_manager,
            query_manager,
        }
    }
    pub async fn get_playlist(&self) -> Vec<Track> {
        self.query_manager.clone().fetch_playlist().await.unwrap()
    }
    pub async fn inject_tracks(
        track_chunk: impl IntoIterator<Item = Track>,
        sender: Sender<Track>,
    ) -> anyhow::Result<Sender<Track>> {
        for track in track_chunk {
            send(track, &sender).await.unwrap();
        }
        send(Track::NoMoreTracks, &sender).await.unwrap();
        Ok(sender)
    }

    #[instrument(
        name = "run-cyle",
        skip(self, sender, receiver, connection, redis_client)
    )]
    pub async fn run_cycle(
        self,
        sender: Sender<Track>,
        mut receiver: Receiver<Track>,
        connection: &mut PgConnection,
        redis_client: redis::Client,
    ) -> anyhow::Result<()> {
        let managers = Arc::new(self);
        let mut database_manager = DatabaseManager::new(connection);

        managers.client.login().context("Could not connect")?;
        let sender = Arc::new(sender);
        let storage = Vec::new();
        let state = Arc::new(RwLock::new(storage));
        let (task_sender, task_receiver) = mpsc::channel(300);
        let search_semaphore = Arc::new(Semaphore::new(4));
        let download_semaphore = Arc::new(Semaphore::new(7));
        let manager_span = info_span!("context-span");
        let task_manager: JoinHandle<anyhow::Result<()>> = tokio::spawn(
            async move {
                await_pending_tasks(task_receiver)
                    .await
                    .context("Awaiting tasks")?;
                Ok(())
            }
            .instrument(manager_span),
        );
        loop {
            tokio::select! {
                maybe_msg = receiver.recv() => {
                    match maybe_msg {
                        None => {
                            println!("I do not expect to get here, ever");
                            break;
                        }
                        Some(track) => {
                            tracing::info!(?track, "Incoming package");
                            let task_queue = task_sender.clone();
                            database_manager
                                .load_item_to_database(&track)
                                .context("Load into database")?;
                            match track {
                                Track::Query(search_item) => {
                                    let managers = Arc::clone(&managers);
                                    let sender = Arc::clone(&sender);
                                    let semaphore = search_semaphore.clone();
                                    tracing::info!(?search_item, "Enter search_item");
                                    let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                                        managers
                                            .search_manager
                                            .run(search_item, 0, semaphore, sender)
                                            .await
                                            .context("returning track")?;
                                        Ok(())
                                    });
                                    tokio::time::sleep(Duration::from_secs(3)).await;
                                    task_queue
                                        .send(QueuePriority::NormalRun(handle))
                                        .await
                                        .context("Submitting task to queue")?;
                                }
                                Track::Result(judge_submission) => {
                                    let managers = Arc::clone(&managers);
                                    let sender = Arc::clone(&sender);
                                    let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                                        tracing::info!(?judge_submission, "Enter result");
                                        managers
                                            .judge_manager
                                            .run(judge_submission, sender)
                                            .await
                                            .context("Returning judge_submission")?;
                                        Ok(())
                                    });
                                    handle.await.context("handle-revisar")?.context("inner")?;
                                }
                                Track::Downloadable(judge_submission) => {
                                    let semaphore = download_semaphore.clone();
                                    let managers = Arc::clone(&managers);
                                    let sender = Arc::clone(&sender);
                                    tracing::info!(?judge_submission, "Enter downloadable");
                                    let judge_sub = judge_submission.clone();
                                    if !state.read().await.contains(&judge_submission.track) {
                                        let redis_client = redis_client.clone();
                                        let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                                            managers
                                                .download_manager
                                                .run(judge_sub, semaphore, sender, redis_client)
                                                .await
                                                .context("Downloading")?;
                                            Ok(())
                                        });
                                        task_queue
                                            .send(QueuePriority::NormalRun(handle))
                                            .await
                                            .context("Submitting task to queue")?;
                                    } else {
                                        let reject = RejectedTrack::new(
                                            judge_submission.clone(),
                                            RejectReason::AlreadyDownloaded,
                                        );
                                        send(Track::Reject(reject), &sender)
                                            .await
                                            .context("sending rejected_tracks")?;
                                    }
                                    let mut write = state.write().await;
                                    write.push(judge_submission.track);
                                }
                                Track::File(downloaded_file) => {
                                    tracing::info!(?downloaded_file, "Downloaded file");
                                }
                                Track::Retry(mut retry_request) => {
                                    if retry_request.retry_attempts >= 1 {
                                        let reject = RejectedTrack::new(
                                            retry_request.request,
                                            RejectReason::AbandonedAttemptingSearch,
                                        );
                                        send(Track::Reject(reject), &sender)
                                            .await
                                            .context("rejecting")?;
                                        continue;
                                    }
                                    retry_request.retry_attempts += 1;
                                    let managers = Arc::clone(&managers);
                                    let semaphore = search_semaphore.clone();
                                    let sender = Arc::clone(&sender);
                                    tracing::info!(?retry_request.request, "Retry zone");
                                    let search_item = retry_request.request.clone();
                                    let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                                        managers
                                            .search_manager
                                            .run(search_item.track, 1, semaphore, sender)
                                            .await
                                            .context("returning track")?;
                                        Ok(())
                                    });
                                    task_queue
                                        .send(QueuePriority::RetryRun(handle))
                                        .await
                                        .context("Submitting task to queue")?;
                                    tracing::info!(?retry_request, "Retry requestedfile")
                                }
                                Track::Reject(_rejected_track) => {}
                                Track::NoMoreTracks => {
                                    println!("No more tracks signal received");
                                }
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(3 * 60)) => {
                    break;
                }
            }
        }
        // task_sender
        //     .send(QueuePriority::Terminate)
        //     .await
        //     .context("sending shutdown signal")?;
        drop(task_sender);
        drop(sender);
        task_manager
            .await
            .context("Awaiting task manager shutdown")?
            .context("Inner")?;
        Ok(())
    }
}

#[instrument(name = "task manager", skip(receiver))]
pub async fn await_pending_tasks(mut receiver: Receiver<QueuePriority>) -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    let mut retries_queue = vec![];
    while let Some(msg) = receiver.recv().await {
        match msg {
            QueuePriority::NormalRun(join_handle) => {
                set.spawn(async move { join_handle.await.context("Awaiting handle")? });
            }
            QueuePriority::RetryRun(join_handle) => retries_queue.push(join_handle),
            // QueuePriority::Terminate => break,
        }
    }

    while let Some(res) = set.join_next().await {
        res.context("Failed returning from task")?
            .context("inner")?;
    }
    println!("Transition between regular and retries");
    for task in retries_queue {
        task.await.context("Awaiting retry")?.context("inner")?;
    }
    Ok(())
}
