use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
};

use anyhow::Context;
use serde_json::Value;

#[derive(Default)]
struct LogStats {
    total_lines: usize,
    parsed_json_lines: usize,
    warning_lines: usize,
    error_lines: usize,
    channel_closed_completions: usize,
    retries: usize,
    empty_result_exits: usize,
    download_timeouts: usize,
    connection_timeouts: usize,
    connection_refused: usize,
    no_route_to_host: usize,
    peer_disconnects: usize,
    searched_track_ids: BTreeSet<String>,
    downloaded_by_track_id: BTreeMap<String, usize>,
}

impl LogStats {
    fn record(&mut self, line: &str) {
        self.total_lines += 1;
        let Some(payload) = json_payload(line) else {
            self.record_text(line);
            return;
        };
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            self.record_text(line);
            return;
        };
        self.parsed_json_lines += 1;

        let level = value
            .get("level")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if level == "WARN" {
            self.warning_lines += 1;
        } else if level == "ERROR" {
            self.error_lines += 1;
        }

        let message = value
            .pointer("/fields/message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.record_text(message);
        if message == "Failed to report task completion"
            && value
                .pointer("/fields/err")
                .and_then(Value::as_str)
                .is_some_and(|err| err.contains("channel closed"))
        {
            self.channel_closed_completions += 1;
        }

        if let Some(track_id) = value.pointer("/span/id").and_then(Value::as_str)
            && value.pointer("/span/name").and_then(Value::as_str) == Some("track_search_task")
            && message == "enter"
        {
            self.searched_track_ids.insert(track_id.to_string());
        }

        if message == "Downloaded file"
            && let Some(downloaded) = value
                .pointer("/fields/downloaded_file")
                .and_then(Value::as_str)
            && let Some(track_id) = extract_after(downloaded, "track_id: \"")
        {
            *self.downloaded_by_track_id.entry(track_id).or_default() += 1;
        }
    }

    fn record_text(&mut self, text: &str) {
        if text.contains("Failed to report task completion") && text.contains("channel closed") {
            self.channel_closed_completions += 1;
        }
        if text.contains("Retry requested") {
            self.retries += 1;
        }
        if text.contains("Exited because consecutive empty results") {
            self.empty_result_exits += 1;
        }
        if text.contains("Download status receive error") || text.contains("TimedOut") {
            self.download_timeouts += 1;
        }
        if text.contains("connection timed out") {
            self.connection_timeouts += 1;
        }
        if text.contains("Connection refused") || text.contains("connection refused") {
            self.connection_refused += 1;
        }
        if text.contains("No route to host") || text.contains("no route to host") {
            self.no_route_to_host += 1;
        }
        if text.contains("disconnected with error") {
            self.peer_disconnects += 1;
        }
    }

    fn print(&self) {
        let downloaded_events: usize = self.downloaded_by_track_id.values().sum();
        let duplicated_downloads = self
            .downloaded_by_track_id
            .iter()
            .filter(|(_, count)| **count > 1)
            .collect::<Vec<_>>();

        println!("run log summary");
        println!("  total_lines: {}", self.total_lines);
        println!("  parsed_json_lines: {}", self.parsed_json_lines);
        println!("  warning_lines: {}", self.warning_lines);
        println!("  error_lines: {}", self.error_lines);
        println!(
            "  task_completion_channel_closed: {}",
            self.channel_closed_completions
        );
        println!(
            "  unique_searched_tracks: {}",
            self.searched_track_ids.len()
        );
        println!("  downloaded_file_events: {downloaded_events}");
        println!(
            "  unique_downloaded_tracks: {}",
            self.downloaded_by_track_id.len()
        );
        println!("  retry_requests: {}", self.retries);
        println!("  empty_result_exits: {}", self.empty_result_exits);
        println!("  download_timeout_warnings: {}", self.download_timeouts);
        println!("  peer_disconnects: {}", self.peer_disconnects);
        println!("  connection_timed_out: {}", self.connection_timeouts);
        println!("  connection_refused: {}", self.connection_refused);
        println!("  no_route_to_host: {}", self.no_route_to_host);
        println!(
            "  duplicate_downloaded_tracks: {}",
            duplicated_downloads.len()
        );

        if !duplicated_downloads.is_empty() {
            println!();
            println!("duplicate downloads by track_id");
            for (track_id, count) in duplicated_downloads {
                println!("  {track_id}: {count}");
            }
        }
    }
}

fn json_payload(line: &str) -> Option<&str> {
    line.find('{').map(|start| &line[start..])
}

fn extract_after(value: &str, marker: &str) -> Option<String> {
    let start = value.find(marker)? + marker.len();
    let rest = &value[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn main() -> anyhow::Result<()> {
    let path = env::args()
        .nth(1)
        .context("usage: cargo run --bin analyze_run_log -- <log-file>")?;
    let contents = fs::read_to_string(&path).with_context(|| format!("read {path}"))?;
    let mut stats = LogStats::default();
    for line in contents.lines() {
        stats.record(line);
    }
    stats.print();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::LogStats;

    #[test]
    fn parses_key_reliability_signals_from_json_logs() {
        let mut stats = LogStats::default();

        stats.record(
            r#"api-1 | {"level":"ERROR","fields":{"message":"Failed to report task completion","err":"Send to channel\n\nCaused by:\n    channel closed","label":"search"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"enter"},"span":{"name":"track_search_task","id":"track-1"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"Downloaded file","downloaded_file":"DownloadedFile { filename: \"a.mp3\", track: SearchItem { track_id: \"track-1\", track: \"Song\", album: \"Album\", artist: \"Artist\" } }"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"Downloaded file","downloaded_file":"DownloadedFile { filename: \"b.mp3\", track: SearchItem { track_id: \"track-1\", track: \"Song\", album: \"Album\", artist: \"Artist\" } }"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"Exited because consecutive empty results"}}"#,
        );

        assert_eq!(stats.total_lines, 5);
        assert_eq!(stats.parsed_json_lines, 5);
        assert_eq!(stats.error_lines, 1);
        assert_eq!(stats.channel_closed_completions, 1);
        assert_eq!(stats.searched_track_ids.len(), 1);
        assert_eq!(stats.downloaded_by_track_id.get("track-1"), Some(&2));
        assert_eq!(stats.empty_result_exits, 1);
    }

    #[test]
    fn falls_back_to_text_matching_for_non_json_lines() {
        let mut stats = LogStats::default();

        stats.record("Peer abc disconnected with error: connection timed out");
        stats.record("Peer def disconnected with error: Connection refused");
        stats.record("Peer ghi disconnected with error: No route to host");
        stats.record("Download status receive error: Timeout");
        stats.record("Retry requested");

        assert_eq!(stats.total_lines, 5);
        assert_eq!(stats.parsed_json_lines, 0);
        assert_eq!(stats.peer_disconnects, 3);
        assert_eq!(stats.connection_timeouts, 1);
        assert_eq!(stats.connection_refused, 1);
        assert_eq!(stats.no_route_to_host, 1);
        assert_eq!(stats.download_timeouts, 1);
        assert_eq!(stats.retries, 1);
    }
}
