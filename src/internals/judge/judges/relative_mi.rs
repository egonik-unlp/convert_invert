use async_trait::async_trait;
use std::collections::HashMap;
use tracing::instrument;

use crate::internals::{judge::judge_manager::Judge, search::search_manager::JudgeSubmission};

#[derive(Clone, Default)]
pub struct RelativeMi;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Title,
    Artist,
    Album,
    Filename,
}

#[derive(Debug, Clone)]
struct WeightedToken {
    token: String,
    weight: f32,
}

impl RelativeMi {
    pub fn new() -> Self {
        Self
    }
}

fn basename_without_extension(filename: &str) -> &str {
    let basename = filename
        .rsplit(['\\', '/'])
        .next()
        .unwrap_or(filename)
        .trim();
    basename.rsplit_once('.').map_or(basename, |(stem, _)| stem)
}

fn normalize_tokens(input: &str) -> Vec<String> {
    let mut output = String::with_capacity(input.len());
    let mut bracket_depth = 0usize;
    for character in input.chars() {
        match character {
            '(' | '[' | '{' => {
                bracket_depth += 1;
                output.push(' ');
            }
            ')' | ']' | '}' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                output.push(' ');
            }
            _ if bracket_depth > 0 => {}
            _ if character.is_alphanumeric() => {
                output.extend(character.to_lowercase());
            }
            _ => output.push(' '),
        }
    }
    output
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

fn token_weight(token: &str, field: Field) -> f32 {
    let generic = [
        "original",
        "mix",
        "edit",
        "remix",
        "version",
        "radio",
        "club",
        "extended",
        "feat",
        "featuring",
        "remaster",
        "remastered",
        "web",
        "vinyl",
        "cd",
        "flac",
        "mp3",
        "m4a",
        "wav",
        "ogg",
        "opus",
        "lossless",
        "320",
        "24bit",
        "96khz",
    ];
    if generic.contains(&token) {
        return 0.0;
    }
    if token.len() <= 2 && token.chars().all(|character| character.is_ascii_digit()) {
        return 0.0;
    }
    match field {
        Field::Title => 1.0,
        Field::Artist => 1.0,
        Field::Album => 0.35,
        Field::Filename => 1.0,
    }
}

fn weighted_tokens(input: &str, field: Field) -> Vec<WeightedToken> {
    normalize_tokens(input)
        .into_iter()
        .filter_map(|token| {
            let weight = token_weight(&token, field);
            (weight > 0.0).then_some(WeightedToken { token, weight })
        })
        .collect()
}

fn merge_weighted_tokens(
    groups: impl IntoIterator<Item = Vec<WeightedToken>>,
) -> HashMap<String, f32> {
    let mut merged = HashMap::new();
    for group in groups {
        for token in group {
            merged
                .entry(token.token)
                .and_modify(|weight: &mut f32| *weight = weight.max(token.weight))
                .or_insert(token.weight);
        }
    }
    merged
}

fn weighted_sum(tokens: &HashMap<String, f32>) -> f32 {
    tokens.values().sum()
}

fn extension_quality(filename: &str) -> f32 {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".flac")
        || lower.ends_with(".wav")
        || lower.ends_with(".aiff")
        || lower.ends_with(".aif")
    {
        1.0
    } else if lower.ends_with(".mp3") || lower.ends_with(".m4a") || lower.ends_with(".aac") {
        0.75
    } else if lower.ends_with(".ogg") || lower.ends_with(".opus") {
        0.6
    } else {
        0.0
    }
}

pub fn score_submission(submission: &JudgeSubmission) -> f32 {
    let spotify_tokens = merge_weighted_tokens([
        weighted_tokens(&submission.track.track, Field::Title),
        weighted_tokens(&submission.track.artist, Field::Artist),
        weighted_tokens(&submission.track.album, Field::Album),
    ]);
    let filename_tokens = merge_weighted_tokens([weighted_tokens(
        basename_without_extension(&submission.query.filename),
        Field::Filename,
    )]);

    let spotify_total = weighted_sum(&spotify_tokens);
    let filename_total = weighted_sum(&filename_tokens);
    if spotify_total <= f32::EPSILON || filename_total <= f32::EPSILON {
        return 0.0;
    }

    let shared_info: f32 = spotify_tokens
        .iter()
        .filter_map(|(token, spotify_weight)| {
            filename_tokens
                .get(token)
                .map(|filename_weight| spotify_weight.min(*filename_weight))
        })
        .sum();
    let coverage = shared_info / spotify_total;
    let purity = shared_info / filename_total;
    let quality = extension_quality(&submission.query.filename);

    ((0.70 * coverage) + (0.20 * purity) + (0.10 * quality)).clamp(0.0, 1.0)
}

#[async_trait]
impl Judge for RelativeMi {
    async fn judge(&self, submission: JudgeSubmission) -> anyhow::Result<bool> {
        Ok(self.judge_score(submission).await? > 0.0)
    }

    #[instrument(name = "RelativeMi::judge_score", skip(self, submission), fields(id=submission.track.track_id, username = submission.query.username, query_song = submission.track.track, file_q = submission.query.filename))]
    async fn judge_score(&self, submission: JudgeSubmission) -> anyhow::Result<f32> {
        let score = score_submission(&submission);
        tracing::debug!(score, "Relative MI score");
        Ok(score)
    }

    async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<Vec<f32>> {
        Ok(submissions.iter().map(score_submission).collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use super::score_submission;
    use crate::internals::search::search_manager::{DownloadableFile, JudgeSubmission, SearchItem};

    fn submission(track: &str, artist: &str, album: &str, filename: &str) -> JudgeSubmission {
        JudgeSubmission {
            track: SearchItem::new(
                "spotify-track-id".to_string(),
                track.to_string(),
                album.to_string(),
                artist.to_string(),
            ),
            query: DownloadableFile {
                filename: filename.to_string(),
                username: "user".to_string(),
                size: 1024,
            },
            score: None,
            relative_mi_score: None,
        }
    }

    #[test]
    fn exact_title_artist_filename_scores_high() {
        let score = score_submission(&submission(
            "Looking at Your Pager",
            "KH",
            "Looking at Your Pager",
            "KH - Looking at Your Pager.flac",
        ));

        assert!(score > 0.85, "score was {score}");
    }

    #[test]
    fn noisy_correct_filename_keeps_coverage_but_loses_purity() {
        let clean = score_submission(&submission(
            "Looking at Your Pager",
            "KH",
            "Looking at Your Pager",
            "KH - Looking at Your Pager.flac",
        ));
        let noisy = score_submission(&submission(
            "Looking at Your Pager",
            "KH",
            "Looking at Your Pager",
            "VA - Best Ibiza Techno Club Remix Extended Pack 2022 - Looking at Your Pager.flac",
        ));

        assert!(noisy > 0.55, "noisy score was {noisy}");
        assert!(noisy < clean, "clean={clean} noisy={noisy}");
    }

    #[test]
    fn unrelated_filename_scores_low() {
        let score = score_submission(&submission(
            "Looking at Your Pager",
            "KH",
            "Looking at Your Pager",
            "Massive Attack - Unfinished Sympathy.flac",
        ));

        assert!(score < 0.35, "score was {score}");
    }

    #[test]
    fn generic_only_overlap_does_not_inflate_score() {
        let score = score_submission(&submission(
            "Original Mix",
            "Artist",
            "Album",
            "Club Extended Original Mix.flac",
        ));

        assert!(score < 0.35, "score was {score}");
    }
}
