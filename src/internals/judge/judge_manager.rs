use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::internals::{
    context::context_manager::{RejectReason, RejectedTrack, Track, send},
    search::search_manager::JudgeSubmission,
};

/// Single source of truth for the judge's accept/reject cutoff. The HTTP API
/// surfaces this value at `/api/config` so the frontend can render the same
/// threshold the backend actually applies — previously the two drifted (0.75
/// in code, 0.85 in the UI).
pub const JUDGE_THRESHOLD: f32 = 0.75;

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
    pub threshold: f32,
}
impl JudgeManager {
    pub fn new(method: Box<dyn Judge>, threshold: f32) -> JudgeManager {
        JudgeManager { method, threshold }
    }
    pub async fn run(
        &self,
        track: JudgeSubmission,
        sender: Arc<Sender<Track>>,
    ) -> anyhow::Result<()> {
        tracing::debug!(?track, "Received judge submission");
        let mut inner_track = track.clone();
        let response = self
            .method
            .judge_score(track.clone())
            .await
            .context("awaiting judge response")?;
        inner_track.score = Some(response);
        if response > self.threshold {
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
    use super::{Judge, JudgeManager};
    use crate::internals::{
        context::context_manager::{RejectReason, Track},
        search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem},
    };
    use async_trait::async_trait;
    use std::sync::Arc;

    struct FixedJudge {
        score: f32,
    }

    #[async_trait]
    impl Judge for FixedJudge {
        async fn judge(&self, _submission: JudgeSubmission) -> anyhow::Result<bool> {
            Ok(self.score > 0.0)
        }

        async fn judge_score(&self, _submission: JudgeSubmission) -> anyhow::Result<f32> {
            Ok(self.score)
        }

        async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<Vec<f32>> {
            Ok(vec![self.score; submissions.len()])
        }
    }

    fn submission() -> JudgeSubmission {
        JudgeSubmission {
            track: SearchItem::new(
                "spotify-track-id".to_string(),
                "Track".to_string(),
                "Album".to_string(),
                "Artist".to_string(),
            ),
            query: DownloadableFile {
                filename: "Artist - Track.flac".to_string(),
                username: "user".to_string(),
                size: 1024,
            },
            score: None,
        }
    }

    #[tokio::test]
    async fn manager_uses_configured_threshold_for_acceptance() {
        let manager = JudgeManager::new(Box::new(FixedJudge { score: 0.8 }), 0.75);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        manager
            .run(submission(), Arc::new(sender))
            .await
            .expect("judge run");

        match receiver.recv().await.expect("message") {
            Track::Downloadable(track) => assert_eq!(track.score, Some(0.8)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn manager_rejects_scores_at_or_below_threshold() {
        let manager = JudgeManager::new(Box::new(FixedJudge { score: 0.75 }), 0.75);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);

        manager
            .run(submission(), Arc::new(sender))
            .await
            .expect("judge run");

        match receiver.recv().await.expect("message") {
            Track::Reject(track) => {
                let (_, reason) = track.parts();
                assert!(matches!(reason, RejectReason::LowScore(score) if *score == 0.75));
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}
