use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Default)]
struct LogStats {
    total_lines: usize,
    parsed_json_lines: usize,
    warning_lines: usize,
    error_lines: usize,
    channel_closed_completions: usize,
    listener_bind_panics: usize,
    db_pool_timeouts: usize,
    duplicate_track_insert_errors: usize,
    retries: usize,
    budget_exhausted: usize,
    already_downloaded_skips: usize,
    retry_backoffs: usize,
    search_passes: usize,
    download_attempts: usize,
    empty_result_exits: usize,
    download_timeouts: usize,
    connection_timeouts: usize,
    connection_refused: usize,
    no_route_to_host: usize,
    peer_disconnects: usize,
    first_timestamp: Option<DateTime<Utc>>,
    last_timestamp: Option<DateTime<Utc>>,
    run_cycle_runtimes: Vec<Duration>,
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

        if let Some(timestamp) = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_timestamp)
        {
            self.first_timestamp = Some(
                self.first_timestamp
                    .map_or(timestamp, |current| current.min(timestamp)),
            );
            self.last_timestamp = Some(
                self.last_timestamp
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
        }

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
        if let Some(error) = value.pointer("/fields/error").and_then(Value::as_str) {
            self.record_text(error);
        }
        if let Some(err) = value.pointer("/fields/err").and_then(Value::as_str) {
            self.record_text(err);
        }
        if message == "close"
            && value
                .pointer("/target")
                .and_then(Value::as_str)
                .is_some_and(|target| target.ends_with("context::context_manager"))
            && value
                .pointer("/span/name")
                .and_then(Value::as_str)
                .is_some_and(|name| name == "run-chunk" || name == "run-cyle")
            && let (Some(busy), Some(idle)) = (
                value
                    .pointer("/fields/time.busy")
                    .and_then(Value::as_str)
                    .and_then(parse_tracing_duration),
                value
                    .pointer("/fields/time.idle")
                    .and_then(Value::as_str)
                    .and_then(parse_tracing_duration),
            )
        {
            self.run_cycle_runtimes.push(busy + idle);
        }
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
        if (text.contains("Failed to bind listener to port")
            || text.contains("listener bind panic")
            || text.contains("AddrInUse"))
            && (text.contains("panicked") || text.contains("panic") || text.contains("AddrInUse"))
        {
            self.listener_bind_panics += 1;
        }
        if text.contains("DB pool") && text.contains("timed out waiting for connection") {
            self.db_pool_timeouts += 1;
        }
        if text.contains("duplicate key value violates unique constraint")
            && text.contains("downloaded_file_track_uidx")
        {
            self.duplicate_track_insert_errors += 1;
        }
        if text.contains("Retry requested") {
            self.retries += 1;
        }
        if text.contains("Request budget exhausted") {
            self.budget_exhausted += 1;
        }
        // A1: searches skipped because the track was already downloaded (pure wasted-query
        // savings). A2: jittered backoffs applied before a retry (only visible at debug level).
        if text.contains("track already downloaded") {
            self.already_downloaded_skips += 1;
        }
        if text.contains("Retry backoff") {
            self.retry_backoffs += 1;
        }
        if text.contains("Scheduling search") || text.contains("Scheduling relaxed retry search") {
            self.search_passes += 1;
        }
        if text.contains("Scheduling download") {
            self.download_attempts += 1;
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
        if let Some(runtime) = self.log_runtime() {
            println!("  log_runtime: {}", format_duration(runtime));
        }
        if !self.run_cycle_runtimes.is_empty() {
            println!("  run_cycle_count: {}", self.run_cycle_runtimes.len());
            println!(
                "  run_cycle_runtime_total: {}",
                format_duration(duration_sum(&self.run_cycle_runtimes))
            );
            println!(
                "  run_cycle_runtime_avg: {}",
                format_duration(duration_average(&self.run_cycle_runtimes))
            );
            println!(
                "  run_cycle_runtime_min: {}",
                format_duration(*self.run_cycle_runtimes.iter().min().unwrap())
            );
            println!(
                "  run_cycle_runtime_max: {}",
                format_duration(*self.run_cycle_runtimes.iter().max().unwrap())
            );
        }
        println!("  warning_lines: {}", self.warning_lines);
        println!("  error_lines: {}", self.error_lines);
        println!(
            "  task_completion_channel_closed: {}",
            self.channel_closed_completions
        );
        println!("  listener_bind_panics: {}", self.listener_bind_panics);
        println!("  db_pool_timeouts: {}", self.db_pool_timeouts);
        println!(
            "  duplicate_track_insert_errors: {}",
            self.duplicate_track_insert_errors
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
        println!("  budget_exhausted: {}", self.budget_exhausted);
        println!(
            "  already_downloaded_skips: {}",
            self.already_downloaded_skips
        );
        println!("  retry_backoffs: {}", self.retry_backoffs);
        println!("  search_passes: {}", self.search_passes);
        println!("  download_attempts: {}", self.download_attempts);
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

    fn log_runtime(&self) -> Option<Duration> {
        let first = self.first_timestamp?;
        let last = self.last_timestamp?;
        last.signed_duration_since(first).to_std().ok()
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

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn parse_tracing_duration(value: &str) -> Option<Duration> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ns") {
        (number, 1.0 / 1_000_000_000.0)
    } else if let Some(number) = value
        .strip_suffix("µs")
        .or_else(|| value.strip_suffix("us"))
    {
        (number, 1.0 / 1_000_000.0)
    } else if let Some(number) = value.strip_suffix("ms") {
        (number, 1.0 / 1_000.0)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60.0)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600.0)
    } else {
        return None;
    };

    let seconds = number.parse::<f64>().ok()? * multiplier;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(seconds))
}

fn duration_sum(values: &[Duration]) -> Duration {
    values.iter().copied().sum()
}

fn duration_average(values: &[Duration]) -> Duration {
    duration_sum(values) / values.len() as u32
}

fn format_duration(duration: Duration) -> String {
    let total_millis = duration.as_millis();
    let millis = total_millis % 1_000;
    let total_seconds = total_millis / 1_000;
    let seconds = total_seconds % 60;
    let total_minutes = total_seconds / 60;
    let minutes = total_minutes % 60;
    let hours = total_minutes / 60;

    if hours > 0 {
        format!("{hours}h {minutes}m {seconds}.{millis:03}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}.{millis:03}s")
    } else {
        format!("{seconds}.{millis:03}s")
    }
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
    use std::time::Duration;

    use super::{LogStats, format_duration, parse_tracing_duration};

    #[test]
    fn parses_key_reliability_signals_from_json_logs() {
        let mut stats = LogStats::default();

        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"ERROR","fields":{"message":"Failed to report task completion","err":"Send to channel\n\nCaused by:\n    channel closed","label":"search"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"INFO","fields":{"message":"enter"},"span":{"name":"track_search_task","id":"track-1"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"INFO","fields":{"message":"Downloaded file","downloaded_file":"DownloadedFile { filename: \"a.mp3\", track: SearchItem { track_id: \"track-1\", track: \"Song\", album: \"Album\", artist: \"Artist\" } }"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"INFO","fields":{"message":"Downloaded file","downloaded_file":"DownloadedFile { filename: \"b.mp3\", track: SearchItem { track_id: \"track-1\", track: \"Song\", album: \"Album\", artist: \"Artist\" } }"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"INFO","fields":{"message":"Exited because consecutive empty results"}}"#,
        );

        assert_eq!(stats.total_lines, 5);
        assert_eq!(stats.parsed_json_lines, 5);
        assert_eq!(stats.log_runtime(), Some(Duration::from_millis(0)));
        assert_eq!(stats.error_lines, 1);
        assert_eq!(stats.channel_closed_completions, 1);
        assert_eq!(stats.listener_bind_panics, 0);
        assert_eq!(stats.db_pool_timeouts, 0);
        assert_eq!(stats.duplicate_track_insert_errors, 0);
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

    #[test]
    fn counts_anti_ban_signals() {
        let mut stats = LogStats::default();

        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"Skipping search; track already downloaded","track_id":"t1"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"INFO","fields":{"message":"Skipping relaxed search; track already downloaded","track_id":"t2"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"DEBUG","fields":{"message":"Retry backoff","delay_ms":1200}}"#,
        );

        assert_eq!(stats.already_downloaded_skips, 2);
        assert_eq!(stats.retry_backoffs, 1);
    }

    #[test]
    fn counts_stage_two_reliability_signals_from_text() {
        let mut stats = LogStats::default();

        stats.record(
            "thread '<unnamed>' panicked at 'Failed to bind listener to port: Os { code: 98, kind: AddrInUse }'",
        );
        stats.record("DB pool in download_track: timed out waiting for connection");
        stats.record(
            "duplicate key value violates unique constraint \"downloaded_file_track_uidx\"",
        );

        assert_eq!(stats.listener_bind_panics, 1);
        assert_eq!(stats.db_pool_timeouts, 1);
        assert_eq!(stats.duplicate_track_insert_errors, 1);
    }

    #[test]
    fn counts_stage_two_reliability_signals_from_json_messages() {
        let mut stats = LogStats::default();

        stats.record(
            r#"api-1 | {"level":"ERROR","fields":{"message":"listener bind panic: Failed to bind listener to port: Os { code: 98, kind: AddrInUse }"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"ERROR","fields":{"message":"DB pool in download_track: timed out waiting for connection"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"ERROR","fields":{"message":"Managed task failed","error":"Downloading\n\nCaused by:\n    0: Downloading track\n    1: Inner\n    2: DB pool in download_track\n    3: timed out waiting for connection"}}"#,
        );
        stats.record(
            r#"api-1 | {"level":"ERROR","fields":{"message":"duplicate key value violates unique constraint \"downloaded_file_track_uidx\""}}"#,
        );

        assert_eq!(stats.listener_bind_panics, 1);
        assert_eq!(stats.db_pool_timeouts, 2);
        assert_eq!(stats.duplicate_track_insert_errors, 1);
    }

    #[test]
    fn records_log_and_run_cycle_runtimes() {
        let mut stats = LogStats::default();

        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:47:45.659709Z","level":"INFO","fields":{"message":"Started run cycle"},"target":"convert_invert::internals::context::context_manager","span":{"name":"run-cyle"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:51:56.260709Z","level":"INFO","fields":{"message":"close","time.busy":"601ms","time.idle":"250s"},"target":"convert_invert::internals::context::context_manager","span":{"name":"run-cyle"}}"#,
        );
        stats.record(
            r#"api-1 | {"timestamp":"2026-05-23T17:52:36.260709Z","level":"INFO","fields":{"message":"close","time.busy":"1.02s","time.idle":"40.0s"},"target":"convert_invert::internals::context::context_manager","span":{"name":"run-chunk"}}"#,
        );

        assert_eq!(
            stats.log_runtime(),
            Some(Duration::from_secs(290) + Duration::from_millis(601))
        );
        assert_eq!(stats.run_cycle_runtimes.len(), 2);
        assert_eq!(
            stats.run_cycle_runtimes[0],
            Duration::from_secs(250) + Duration::from_millis(601)
        );
        assert_eq!(
            stats.run_cycle_runtimes[1],
            Duration::from_secs(41) + Duration::from_millis(20)
        );
    }

    #[test]
    fn parses_and_formats_tracing_durations() {
        assert_eq!(parse_tracing_duration("2s"), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_tracing_duration("1.5ms"),
            Some(Duration::from_micros(1_500))
        );
        assert_eq!(
            parse_tracing_duration("42µs"),
            Some(Duration::from_micros(42))
        );
        assert_eq!(format_duration(Duration::from_millis(61_234)), "1m 1.234s");
    }
}
