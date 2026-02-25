use crate::internals::database::manager::DatabaseManager;
use diesel::prelude::*;
use redis::Commands;
use serde::{Deserialize, Serialize};
use soulseek_rs::{Client, ClientSettings};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{
        Semaphore,
        mpsc::{self, Receiver, Sender},
    },
    task::{JoinHandle, JoinSet},
    time::Instant,
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

const MAX_RETRY_ATTEMPTS: u8 = 3;

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
pub enum QueuePriority {
    NormalRun(JoinHandle<anyhow::Result<()>>),
    RetryRun(JoinHandle<anyhow::Result<()>>),
    Terminate,
}

impl Managers {
    pub fn new(score: Option<f32>, path: PathBuf, config: Config, timeout_multiplier: f64) -> Self {
        let client_settings = ClientSettings {
            username: config.user_name,
            password: config.user_password,
            listen_port: config.listen_port,
            ..Default::default()
        };
        let mut client = Client::with_settings(client_settings);
        client.connect();
        let client = Arc::new(client);
        let download_manager = DownloadManager::new(client.clone(), path, timeout_multiplier);
        let search_manager = SearchManager::new(client.clone());
        let lev_judge = Levenshtein::new(score.unwrap_or(0.75));
        let judge_manager = JudgeManager::new(Box::new(lev_judge));
        let query_manager = QueryManager::new(
            "7vdaDB7qkKGbE4abs1iFpQ?si=060b186284b14ad2",
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

    #[instrument(name = "run-cyle", skip(self, tracks, connection, redis_client))]
    pub async fn run_cycle(
        self,
        tracks: impl IntoIterator<Item = Track>,
        connection: &mut PgConnection,
        mut redis_client: redis::Client,
    ) -> anyhow::Result<()> {
        let managers = Arc::new(self);
        let mut database_manager = DatabaseManager::new(connection);
        let (sender, mut receiver) = mpsc::channel(20000);

        managers.client.login().context("Could not connect")?;

        let sender = Arc::new(sender);
        let (task_sender, task_receiver) = mpsc::channel(300);
        let search_semaphore = Arc::new(Semaphore::new(4));
        let download_semaphore = Arc::new(Semaphore::new(7));

        println!(
            "\n\n\n\nStarted up new cycle\n\n\n\nAvailable Permits:\nSearch semaphore: {}\nDownload semaphore: {}",
            search_semaphore.available_permits(),
            download_semaphore.available_permits()
        );
        let manager_span = info_span!("context-span");
        redis_client.set::<_, _, ()>("shutdown", false).unwrap();
        let spawned_redis = redis_client.clone();
        let task_manager: JoinHandle<anyhow::Result<()>> = tokio::spawn(
            async move {
                await_pending_tasks(task_receiver, spawned_redis)
                    .await
                    .context("Awaiting tasks")?;
                Ok(())
            }
            .instrument(manager_span),
        );
        let mut task_manager = Some(task_manager);
        for track in tracks {
            sender.send(track).await.context("injecting tracks")?;
        }
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
                                    tracing::info!(?judge_submission, "Enter downloadable");
                                    let judge_sub = judge_submission.clone();
                                    let sender = Arc::clone(&sender);
                                    let redis_client = redis_client.clone();
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
                                }
                                Track::File(downloaded_file) => {
                                    tracing::info!(?downloaded_file, "Downloaded file");
                                }
                                Track::Retry(mut retry_request) => {
                                    if retry_request.retry_attempts >= MAX_RETRY_ATTEMPTS {
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
                                            .run(search_item.track, 0, semaphore, sender)
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
                _ = tokio::time::sleep(Duration::from_secs(15 * 60)) => {
                    let sender = task_sender.clone();
                    sender.send(QueuePriority::Terminate).await.context("SE ROMPIO EL CHAN CHAN")?;
                    if let Some(handle) = task_manager.take() {
                        handle
                            .await
                            .context("Awaiting task manager shutdown")?
                            .context("Inner")?;
                    }
                    println!("BREAK LOOOP EL MAS IMPORTANTE");
                    break;
                }
            }
        }
        drop(sender);
        drop(task_sender);
        if let Some(handle) = task_manager {
            handle
                .await
                .context("Awaiting task manager shutdown")?
                .context("Inner")?;
        }
        println!("END OF FUNCTION");
        Ok(())
    }
}

#[instrument(name = "task manager", skip(receiver))]
pub async fn await_pending_tasks(
    mut receiver: Receiver<QueuePriority>,
    mut redis_client: redis::Client,
) -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    let mut retries_queue = vec![];
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            maybe_msg = receiver.recv() => {
                match maybe_msg {
                    None => break,
                    Some(msg) => match msg {
                        QueuePriority::NormalRun(join_handle) => {
                            set.spawn(async move { join_handle.await.context("Awaiting handle")? });
                        }
                        QueuePriority::RetryRun(join_handle) => retries_queue.push(join_handle),
                        QueuePriority::Terminate => break,
                    },
                }
            }
            _ = interval.tick() => {
                if let Ok(mut redis_con) = redis_client.get_connection() {
                    let inflight: usize = redis_con.scard("dl:inflight").unwrap_or(0);
                    let completed: usize = redis_con.scard("dl:completed").unwrap_or(0);
                    tracing::info!(
                        inflight,
                        completed,
                        "Download state snapshot"
                    );
                }
            }
        }
    }

    while let Some(res) = set.join_next().await {
        res.context("Failed returning from task")?
            .context("inner")?;
    }
    println!("Transition between regular and retries");
    println!("Length of await queue = {}", retries_queue.len());
    for task in retries_queue {
        println!("Started awaiting");
        let start = Instant::now();
        task.await.context("Awaiting retry")?.context("inner")?;
        let span = (Instant::now() - start).as_secs_f32();
        println!("AWAITED ONE. Took {span}s");
    }
    redis_client
        .set::<&'static str, bool, ()>("shutdown", true)
        .context("writing to redis")?;
    Ok(())
}
