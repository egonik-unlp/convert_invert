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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internals::search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem};
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct FixedJudge {
        score: f32,
    }

    #[async_trait]
    impl Judge for FixedJudge {
        async fn judge(&self, _submission: JudgeSubmission) -> anyhow::Result<bool> {
            Ok(self.score > 0.75)
        }

        async fn judge_score(&self, _submission: JudgeSubmission) -> anyhow::Result<f32> {
            Ok(self.score)
        }

        async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<Vec<f32>> {
            Ok(vec![self.score; submissions.len()])
        }
    }

    fn sample_submission() -> JudgeSubmission {
        JudgeSubmission {
            track: SearchItem::new("Track".to_string(), "Album".to_string(), "Artist".to_string()),
            query: DownloadableFile {
                filename: "track.mp3".to_string(),
                username: "user".to_string(),
                size: 123,
            },
            score: None,
        }
    }

    #[tokio::test]
    async fn judge_manager_sends_downloadable_on_high_score() {
        let manager = JudgeManager::new(Box::new(FixedJudge { score: 0.9 }));
        let (sender, mut receiver) = mpsc::channel(4);
        let submission = sample_submission();

        manager
            .run(submission.clone(), Arc::new(sender))
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("expected track");
        match msg {
            Track::Downloadable(out) => {
                assert_eq!(out.track, submission.track);
                assert_eq!(out.query, submission.query);
                assert_eq!(out.score, Some(0.9));
            }
            other => panic!("expected Downloadable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn judge_manager_sends_reject_on_low_score() {
        let manager = JudgeManager::new(Box::new(FixedJudge { score: 0.4 }));
        let (sender, mut receiver) = mpsc::channel(4);
        let submission = sample_submission();

        manager
            .run(submission.clone(), Arc::new(sender))
            .await
            .unwrap();

        let msg = receiver.recv().await.expect("expected track");
        match msg {
            Track::Reject(rejected) => {
                let (track, reason) = rejected.parts();
                assert_eq!(track, &submission);
                match reason {
                    RejectReason::LowScore(score) => assert!((*score - 0.4).abs() < f32::EPSILON),
                    _ => panic!("expected LowScore reject reason"),
                }
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }
}
