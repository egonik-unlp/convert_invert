use async_trait::async_trait;
use tracing::instrument;

use crate::internals::{judge::judge_manager::Judge, search::search_manager::JudgeSubmission};

#[derive(Clone)]
pub struct Levenshtein {
    pub score_cutoff: f32,
}
impl Levenshtein {
    pub fn new(score_cutoff: f32) -> Self {
        Levenshtein { score_cutoff }
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

fn normalize_text(input: &str) -> String {
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

    let noise = [
        "original",
        "remaster",
        "remastered",
        "mix",
        "master",
        "version",
        "edit",
        "radio",
        "web",
        "vinyl",
        "flac",
        "mp3",
        "lossless",
        "320",
        "24bit",
        "96khz",
    ];

    output
        .split_whitespace()
        .filter(|token| {
            !(noise.contains(token)
                || token.len() <= 2 && token.chars().all(|character| character.is_ascii_digit()))
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn significant_tokens(input: &str) -> Vec<String> {
    normalize_text(input)
        .split_whitespace()
        .filter(|token| token.len() > 1 || token.chars().all(|character| character.is_numeric()))
        .map(str::to_string)
        .collect()
}

fn token_coverage_score(filename: &str, track: &str, artist: &str) -> f32 {
    let filename_tokens = significant_tokens(filename);
    if filename_tokens.is_empty() {
        return 0.0;
    }
    let filename_token_set = filename_tokens
        .iter()
        .collect::<std::collections::HashSet<_>>();
    let track_tokens = significant_tokens(track);
    if track_tokens.is_empty() {
        return 0.0;
    }
    let track_matches = track_tokens
        .iter()
        .filter(|token| filename_token_set.contains(token))
        .count();
    let track_coverage = track_matches as f32 / track_tokens.len() as f32;
    if track_coverage < 1.0 {
        return track_coverage * 0.74;
    }

    let artist_tokens = significant_tokens(artist);
    if artist_tokens.is_empty() {
        return 0.82;
    }
    let artist_matches = artist_tokens
        .iter()
        .filter(|token| filename_token_set.contains(token))
        .count();
    let artist_coverage = artist_matches as f32 / artist_tokens.len() as f32;
    if artist_coverage > 0.0 {
        0.90_f32.max(0.82 + (artist_coverage * 0.12))
    } else {
        0.82
    }
}

fn normalized_score(submission: &JudgeSubmission) -> f32 {
    let filename = normalize_text(basename_without_extension(&submission.query.filename));
    let track = normalize_text(&submission.track.track);
    let artist = normalize_text(&submission.track.artist);
    let album = normalize_text(&submission.track.album);
    let targets = [
        format!("{artist} {track}"),
        format!("{track} {artist}"),
        track.clone(),
        format!("{track} {album}"),
    ];
    let exact_score = targets
        .iter()
        .filter(|target| !target.trim().is_empty())
        .map(|target| {
            if filename == *target {
                1.0
            } else if filename.contains(target) || target.contains(&filename) {
                0.9
            } else {
                0.0
            }
        })
        .fold(0.0_f32, f32::max);
    exact_score.max(token_coverage_score(
        &filename,
        &submission.track.track,
        &submission.track.artist,
    ))
}

#[async_trait]
impl Judge for Levenshtein {
    #[instrument(name = "Levenshtein::judge", skip(self, submission), fields(id=submission.track.track_id,username = submission.query.username , query_song = submission.track.track, file_q = submission.query.filename))]
    async fn judge(&self, submission: JudgeSubmission) -> anyhow::Result<bool> {
        let distance_val = normalized_score(&submission);
        tracing::debug!(score = distance_val, "Levenshtein score");
        let val = distance_val > self.score_cutoff;
        Ok(val)
    }
    #[instrument(name = "Levenshtein::judge_score", skip(self,submission), fields(id=submission.track.track_id,username = submission.query.username , query_song = submission.track.track, file_q = submission.query.filename))]
    async fn judge_score(&self, submission: JudgeSubmission) -> anyhow::Result<f32> {
        let distance_val = normalized_score(&submission);
        tracing::debug!(score = distance_val, "Levenshtein score");
        Ok(distance_val)
    }
    #[instrument(name = "Levenshtein::judge_block", skip(self))]
    async fn judge_block(&self, submissions: Vec<JudgeSubmission>) -> anyhow::Result<Vec<f32>> {
        let results: Vec<_> = submissions
            .into_iter()
            .map(|submission| normalized_score(&submission))
            .collect();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::normalized_score;
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
    fn accepts_album_path_when_track_and_artist_are_present() {
        let score = normalized_score(&submission(
            "Only Human",
            "KH",
            "Only Human",
            "Music\\Electronic\\Kieran Hebden\\2019 KH - Only Human\\01 - Only Human.flac",
        ));

        assert!(score > 0.75, "score was {score}");
    }

    #[test]
    fn accepts_track_numbered_album_file_when_title_is_present() {
        let score = normalized_score(&submission(
            "Electric Fish",
            "Ana Frango Eletrico",
            "Me Chama De Gato Que Eu Sou Sua",
            "# Music Lossless\\Ana Frango Elétrico\\Me Chama De Gato Que Eu Sou Sua\\01 Electric Fish.m4a",
        ));

        assert!(score > 0.75, "score was {score}");
    }

    #[test]
    fn rejects_unrelated_filenames() {
        let score = normalized_score(&submission(
            "Only Human",
            "KH",
            "Only Human",
            "Massive Attack - Unfinished Sympathy.flac",
        ));

        assert!(score < 0.75, "score was {score}");
    }
}
