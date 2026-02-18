use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::internals::{
    context::context_manager::{RejectReason, RejectedTrack, Track, send},
    search::search_manager::JudgeSubmission,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ResponseFormat {
    pub score: Option<f32>,
    pub query_song: Option<String>,
    pub filename: Option<String>,
}

#[async_trait]
pub trait Judge: Send + Sync {
    async fn judge(&self, submission: JudgeSubmission) -> anyhow::Result<bool>;
    async fn judge_score(&self, submission: JudgeSubmission) -> anyhow::Result<f32>;
    async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<Vec<f32>>;
}

pub struct JudgeManager {
    pub method: Box<dyn Judge>,
}
impl JudgeManager {
    pub fn new(method: Box<dyn Judge>) -> JudgeManager {
        JudgeManager { method }
    }
    pub async fn run(
        &self,
        track: JudgeSubmission,
        sender: Arc<Sender<Track>>,
    ) -> anyhow::Result<()> {
        tracing::info!("received in judge manager = {:?}", track);
        let mut inner_track = track.clone();
        let response = self
            .method
            .judge_score(track.clone())
            .await
            .context("awaiting judge response")?;
        inner_track.score = Some(response);
        if response > 0.75 {
            send(Track::Downloadable(inner_track), &sender)
                .await
                .context("sending judgement")?;
        } else {
            let reject = RejectedTrack::new(inner_track, RejectReason::LowScore(response));
            send(Track::Reject(reject), &sender)
                .await
                .context("sending reject")?;
        }
        Ok(())
    }
}
