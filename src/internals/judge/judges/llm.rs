use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use reqwest::Url;
use tracing::instrument;

use crate::internals::{
    judge::judge_manager::{Judge, ResponseFormat},
    search::search_manager::JudgeSubmission,
};

#[derive(Clone)]
pub struct LocalLLM {
    pub address: String,
    pub port: i32,
    pub score_cutoff: f32,
}
impl LocalLLM {
    pub fn new(address: String, port: i32, score_cutoff: f32) -> Self {
        LocalLLM {
            address,
            port,
            score_cutoff,
        }
    }
    #[instrument(name = "LocalLLM::get_score", skip(self, submission))]
    async fn get_score(&self, submission: JudgeSubmission) -> anyhow::Result<ResponseFormat> {
        let url = Url::parse(&format!("{}:{}/score", self.address, self.port)).unwrap();
        let client = reqwest::Client::new();
        let str_val = serde_json::to_string(&submission).context("Parsing own track")?;
        let res = client
            .post(url)
            .header("Content-Type", "application/json")
            .body(str_val)
            .send()
            .await
            .context("Response from llm server")?;
        let text = res.text().await.context("Error reading response")?;
        let response: ResponseFormat = serde_json::from_str(&text).context("parsing response")?;
        Ok(response)
    }
    #[instrument(name = "LocalLLM::get_score", skip(self, submission))]
    async fn get_score_vec(
        &self,
        submission: Vec<JudgeSubmission>,
    ) -> anyhow::Result<Vec<ResponseFormat>> {
        let url = Url::parse(&format!("{}:{}/score", self.address, self.port)).unwrap();
        let client = reqwest::Client::new();
        let str_val = serde_json::to_string(&submission).context("Parsing own track")?;
        let res = client
            .post(url)
            .header("Content-Type", "application/json")
            .body(str_val)
            .send()
            .await
            .context("Response from llm server")?;
        let text = res.text().await.context("Error reading response")?;
        let response: Vec<ResponseFormat> =
            serde_json::from_str(&text).context("parsing response")?;
        Ok(response)
    }
}

#[async_trait]
impl Judge for LocalLLM {
    #[instrument(name = "LocalLLM::judge", skip(self), fields(username = submission.query.username , query_song = submission.track.track, file_q = submission.query.filename))]
    async fn judge(&self, submission: JudgeSubmission) -> anyhow::Result<bool> {
        let response = self.get_score(submission).await.context("Getting score")?;
        tracing::info!("{:?}", response);
        match response.score {
            x if x.unwrap_or_default() > self.score_cutoff => Ok(true),
            _ => Ok(false),
        }
    }

    #[instrument(name = "LocalLLM::judge", skip(self), fields(username = submission.query.username , query_song = submission.track.track, file_q = submission.query.filename))]
    async fn judge_score(&self, submission: JudgeSubmission) -> anyhow::Result<f32> {
        let response = self.get_score(submission).await.context("Getting score")?;
        Ok(response.score.unwrap_or_default())
    }
    #[instrument(name = "LocalLLM::judge_block", skip(self))]
    async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<f32> {
        let response = self
            .get_score_vec(submissions)
            .await
            .context("Getting score")?;
        let response = response
            .into_iter()
            .map(|val| val.score.unwrap_or_default())
            .sorted_by(|val, other| ((val * 1000.0) as usize).cmp(&((other * 1000.0) as usize)))
            .rev()
            .nth(0)
            .unwrap();
        Ok(response)
    }
}
