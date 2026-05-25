//! Standalone runner for the convert-invert engine.
//!
//! This binary provides a CLI interface to run the Spotify-to-Soulseek process
//! for a playlist or one Spotify track using the core library's managed cycle.

use anyhow::{Context, bail};
use crossterm::{
    event::{self, Event as CrosstermEvent, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use itertools::Itertools;
use rand::seq::SliceRandom;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, Paragraph, Wrap},
};
use std::{
    io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::instrument;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use convert_invert::internals::database::{db_pool_max_size_from_env, init_pool};
use convert_invert::internals::{
    context::context_manager::{Managers, RunEvent, Track, WorkerTuning},
    query::query_manager::QueryManager,
    search::search_manager::{JudgeSubmission, SearchItem},
    utils::{config::config_manager::Config, trace},
};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

enum Command {
    Playlist {
        attempt: usize,
        playlist_id: Option<String>,
        random_order: bool,
    },
    Track {
        attempt: usize,
        spotify_url: String,
    },
}

struct RuntimeContext {
    config: Config,
    db_pool: convert_invert::internals::database::DbPool,
    redis_pool: convert_invert::internals::context::context_manager::RedisPool,
    download_path: PathBuf,
}

#[instrument(name = "main-span")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let command = parse_command(std::env::args().skip(1).collect())?;
    match command {
        Command::Playlist {
            attempt,
            playlist_id,
            random_order,
        } => run_playlist(attempt, playlist_id, random_order).await,
        Command::Track {
            attempt,
            spotify_url,
        } => run_track(attempt, spotify_url).await,
    }
}

async fn run_playlist(
    attempt: usize,
    playlist_id: Option<String>,
    random_order: bool,
) -> anyhow::Result<()> {
    let mut config = Config::try_from_env().context("Cannot read env vars for config")?;
    if let Some(playlist_id) = playlist_id {
        config.playlist_id = playlist_id;
    }
    config.run_id = format!("{}_attempt_{}", config.run_id, attempt);
    trace::otel_trace::init_tracing_with_otel("convert-invert".to_string(), config.run_id.clone())
        .context("Tracing")?;

    let context = setup_runtime(config).await?;
    let mut playlist = QueryManager::new_with_timeout(
        context.config.playlist_id.clone(),
        context.config.client_id.clone(),
        context.config.client_secret.clone(),
        context.config.search_timeout_secs,
    )
    .fetch_playlist()
    .await
    .context("Fetch playlist")?;
    if random_order {
        let mut rng = rand::rng();
        playlist.shuffle(&mut rng);
        tracing::info!(track_count = playlist.len(), "Randomized playlist order");
    }

    let managers = Arc::new(
        Managers::new(
            context.config.judge_score_levenshtein,
            context.download_path,
            context.config.clone(),
            context.db_pool,
            context.redis_pool,
        )
        .context("Start managers")?,
    );

    let mut count = 0;
    for chunk in &playlist.into_iter().chunks(15) {
        count += 1;
        managers
            .run_chunk(chunk)
            .await
            .with_context(|| format!("Run cycle {count}"))?;
        tracing::info!(cycle_n = count, "Done with cycle");
    }
    managers.shutdown();
    trace::otel_trace::shutdown_otel();
    Ok(())
}

async fn run_track(attempt: usize, spotify_url: String) -> anyhow::Result<()> {
    init_silent_tracing();
    let mut config = Config::try_from_env().context("Cannot read env vars for config")?;
    config.run_id = format!("{}_track_attempt_{}", config.run_id, attempt);
    let track = QueryManager::new_with_timeout(
        spotify_url,
        config.client_id.clone(),
        config.client_secret.clone(),
        config.search_timeout_secs,
    )
    .fetch_track()
    .await
    .context("Fetch Spotify track")?;
    let track_item = match &track {
        Track::Query(item) => item.clone(),
        _ => bail!("Spotify track did not produce a query item"),
    };

    let context = setup_runtime(config).await?;
    let managers = Arc::new(
        Managers::new(
            context.config.judge_score_levenshtein,
            context.download_path,
            context.config,
            context.db_pool,
            context.redis_pool,
        )
        .context("Start managers")?,
    );
    let (event_tx, event_rx) = mpsc::channel(256);
    let managers_for_run = Arc::clone(&managers);
    let run = tokio::spawn(async move {
        managers_for_run
            .run_chunk_with_events(vec![track], Some(Arc::new(event_tx)))
            .await
    });

    let ui_result = run_single_track_tui(track_item, event_rx, run).await;
    managers.shutdown();
    ui_result
}

async fn setup_runtime(config: Config) -> anyhow::Result<RuntimeContext> {
    let db_pool = init_pool().context("Initialize database pool")?;
    let tuning = WorkerTuning::from_env();
    let db_pool_max = db_pool_max_size_from_env(18);
    let minimum_pool = tuning.download_concurrency + tuning.search_concurrency + 2;
    if db_pool_max < minimum_pool as u32 {
        tracing::warn!(
            db_pool_max,
            minimum_pool,
            download_concurrency = tuning.download_concurrency,
            search_concurrency = tuning.search_concurrency,
            "DB pool max size is below the recommended concurrency floor",
        );
    }
    {
        let mut connection = db_pool.get().context("Initial migration connection")?;
        connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|err| anyhow::anyhow!("Cannot run migrations: {err}"))?;
    }

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url).context("Create Redis client")?;
    let redis_pool = diesel::r2d2::Pool::builder()
        .max_size(env_u32("REDIS_POOL_MAX_SIZE", 18))
        .connection_timeout(Duration::from_secs(env_u64("REDIS_POOL_TIMEOUT_SECS", 15)))
        .build(redis_client)
        .context("Create Redis pool")?;

    let download_path = std::env::var("DOWNLOAD_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./downloads"));
    tokio::fs::create_dir_all(&download_path)
        .await
        .with_context(|| format!("Create download directory {}", download_path.display()))?;

    Ok(RuntimeContext {
        config,
        db_pool,
        redis_pool,
        download_path,
    })
}

struct TuiState {
    track: SearchItem,
    started: Instant,
    status: String,
    candidates_found: usize,
    candidates_accepted: usize,
    retries: usize,
    selected: Option<JudgeSubmission>,
    downloaded_file: Option<String>,
    rejected: Option<String>,
    finished_without_download: bool,
    recent: Vec<String>,
}

impl TuiState {
    fn new(track: SearchItem) -> Self {
        Self {
            track,
            started: Instant::now(),
            status: "Preparing".to_string(),
            candidates_found: 0,
            candidates_accepted: 0,
            retries: 0,
            selected: None,
            downloaded_file: None,
            rejected: None,
            finished_without_download: false,
            recent: Vec::new(),
        }
    }

    fn push_recent(&mut self, line: impl Into<String>) {
        self.recent.push(line.into());
        if self.recent.len() > 8 {
            self.recent.remove(0);
        }
    }

    fn apply(&mut self, event: RunEvent) {
        match event {
            RunEvent::SearchQueued(item) => {
                self.status = "Searching Soulseek".to_string();
                self.push_recent(format!("Search queued: {} - {}", item.track, item.artist));
            }
            RunEvent::SearchRetryQueued(_) => {
                self.status = "Retrying with relaxed search".to_string();
                self.push_recent("Relaxed search queued");
            }
            RunEvent::CandidateFound(candidate) => {
                self.candidates_found += 1;
                self.push_recent(format!(
                    "Candidate: {} ({})",
                    candidate.query.filename, candidate.query.username
                ));
            }
            RunEvent::CandidateAccepted(candidate) => {
                self.candidates_accepted += 1;
                self.status = "Collecting accepted candidates".to_string();
                self.push_recent(format!(
                    "Accepted: {} score={}",
                    candidate.query.filename,
                    format_score(candidate.score)
                ));
            }
            RunEvent::CandidateSelected(candidate) => {
                self.status = "Downloading selected candidate".to_string();
                self.selected = Some(candidate.clone());
                self.push_recent(format!(
                    "Selected: {} ({})",
                    candidate.query.filename, candidate.query.username
                ));
            }
            RunEvent::FileDownloaded(file) => {
                self.status = "Completed".to_string();
                self.downloaded_file = Some(file.filename.clone());
                self.push_recent(format!("Downloaded: {}", file.filename));
            }
            RunEvent::RetryQueued { failed, .. } => {
                self.retries += 1;
                self.status = "Retrying download".to_string();
                self.push_recent(format!("Retry queued after failure: {}", failed.filename));
            }
            RunEvent::Rejected { reason, .. } => {
                self.status = "Rejected".to_string();
                self.rejected = Some(reason.clone());
                self.push_recent(format!("Rejected: {reason}"));
            }
        }
    }

    fn finished(&self) -> bool {
        self.downloaded_file.is_some() || self.rejected.is_some() || self.finished_without_download
    }
}

async fn run_single_track_tui(
    track: SearchItem,
    mut event_rx: mpsc::Receiver<RunEvent>,
    run: tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    enable_raw_mode().context("Enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("Enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("Create terminal")?;
    let mut state = TuiState::new(track);
    let mut run_result: Option<anyhow::Result<()>> = None;
    let mut run = Some(run);
    let mut cancelled = false;

    loop {
        while let Ok(event) = event_rx.try_recv() {
            state.apply(event);
        }
        terminal.draw(|frame| draw_track_ui(frame, &state))?;

        if event::poll(Duration::from_millis(100)).context("Read terminal event")?
            && matches!(
                event::read().context("Read key")?,
                CrosstermEvent::Key(key) if key.code == KeyCode::Char('q')
            )
        {
            if let Some(handle) = &run {
                handle.abort();
            }
            cancelled = true;
            break;
        }

        if run_result.is_none() && run.as_ref().is_some_and(|handle| handle.is_finished()) {
            let handle = run.take().expect("checked join handle presence");
            run_result = Some(handle.await.context("Single-track task join failed")?);
        }
        if state.finished() && run_result.is_some() {
            tokio::time::sleep(Duration::from_millis(900)).await;
            break;
        }
        if run_result.as_ref().is_some_and(Result::is_ok) && !state.finished() {
            state.status = "Finished without download".to_string();
            state.finished_without_download = true;
            state.push_recent("Pipeline finished without a downloaded file");
            continue;
        }
        if let Some(Err(_)) = &run_result {
            state.status = "Failed".to_string();
            break;
        }
    }

    disable_raw_mode().context("Disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen).context("Leave alternate screen")?;
    terminal.show_cursor().context("Show cursor")?;

    if cancelled {
        bail!("Single-track download cancelled");
    }
    if let Some(result) = run_result {
        result?;
    }
    if let Some(file) = state.downloaded_file {
        println!(
            "Downloaded {} - {}: {}",
            state.track.artist, state.track.track, file
        );
    } else if let Some(reason) = state.rejected {
        println!(
            "Track was not downloaded: {} - {} ({reason})",
            state.track.artist, state.track.track
        );
    } else if state.finished_without_download {
        println!(
            "Track was not downloaded: {} - {} (no downloadable candidate completed)",
            state.track.artist, state.track.track
        );
    }
    Ok(())
}

fn draw_track_ui(frame: &mut ratatui::Frame<'_>, state: &TuiState) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),
            Constraint::Length(5),
            Constraint::Min(8),
        ])
        .split(area);

    let title = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Track: ", Style::default().fg(Color::Gray)),
            Span::styled(
                format!("{} - {}", state.track.artist, state.track.track),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!("Album: {}", state.track.album)),
        Line::from(format!("Spotify ID: {}", state.track.track_id)),
        Line::from(format!("Elapsed: {}s", state.started.elapsed().as_secs())),
    ])
    .block(
        Block::default()
            .title("Single Track Download")
            .borders(Borders::ALL),
    )
    .wrap(Wrap { trim: true });
    frame.render_widget(title, chunks[0]);

    let progress = if state.downloaded_file.is_some() {
        100
    } else if state.selected.is_some() {
        75
    } else if state.candidates_accepted > 0 {
        55
    } else if state.candidates_found > 0 {
        35
    } else {
        15
    };
    let gauge = Gauge::default()
        .block(
            Block::default()
                .title(state.status.as_str())
                .borders(Borders::ALL),
        )
        .gauge_style(Style::default().fg(Color::Cyan))
        .percent(progress);
    frame.render_widget(gauge, chunks[1]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(chunks[2]);

    let selected = state
        .selected
        .as_ref()
        .map(|candidate| {
            format!(
                "{}\nPeer: {}\nScore: {}\nSize: {} bytes",
                candidate.query.filename,
                candidate.query.username,
                format_score(candidate.score),
                candidate.query.size
            )
        })
        .unwrap_or_else(|| "No candidate selected yet".to_string());
    let stats = Paragraph::new(format!(
        "Candidates found: {}\nAccepted: {}\nRetries: {}\n\n{}",
        state.candidates_found, state.candidates_accepted, state.retries, selected
    ))
    .block(Block::default().title("Download").borders(Borders::ALL))
    .wrap(Wrap { trim: true });
    frame.render_widget(stats, body[0]);

    let items = state
        .recent
        .iter()
        .rev()
        .map(|line| ListItem::new(line.clone()))
        .collect::<Vec<_>>();
    let recent = List::new(items).block(Block::default().title("Activity").borders(Borders::ALL));
    frame.render_widget(recent, body[1]);
}

fn format_score(score: Option<f32>) -> String {
    score
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn parse_command(args: Vec<String>) -> anyhow::Result<Command> {
    if args
        .first()
        .is_some_and(|arg| arg == "-h" || arg == "--help")
    {
        bail!(usage());
    }
    if args.is_empty() {
        return Ok(Command::Playlist {
            attempt: 1,
            playlist_id: None,
            random_order: false,
        });
    }
    if args.len() == 1
        && let Ok(attempt) = args[0].parse::<usize>()
    {
        return Ok(Command::Playlist {
            attempt,
            playlist_id: None,
            random_order: false,
        });
    }
    match args[0].as_str() {
        "playlist" => parse_playlist_args(&args[1..]),
        "track" => parse_track_args(&args[1..]),
        other => bail!("Unknown command '{other}'\n{}", usage()),
    }
}

fn parse_playlist_args(args: &[String]) -> anyhow::Result<Command> {
    let mut attempt = 1usize;
    let mut playlist_id = None;
    let mut random_order = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--attempt" => {
                i += 1;
                attempt = args
                    .get(i)
                    .context("--attempt requires a number")?
                    .parse()
                    .context("Parse --attempt")?;
            }
            "--playlist-id" => {
                i += 1;
                playlist_id = Some(
                    args.get(i)
                        .context("--playlist-id requires a value")?
                        .clone(),
                );
            }
            "--random-order" => random_order = true,
            other => bail!("Unknown playlist argument '{other}'\n{}", usage()),
        }
        i += 1;
    }
    Ok(Command::Playlist {
        attempt,
        playlist_id,
        random_order,
    })
}

fn parse_track_args(args: &[String]) -> anyhow::Result<Command> {
    let mut attempt = 1usize;
    let mut spotify_url = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--attempt" => {
                i += 1;
                attempt = args
                    .get(i)
                    .context("--attempt requires a number")?
                    .parse()
                    .context("Parse --attempt")?;
            }
            value if value.starts_with("--") => {
                bail!("Unknown track argument '{value}'\n{}", usage())
            }
            value => spotify_url = Some(value.to_string()),
        }
        i += 1;
    }
    Ok(Command::Track {
        attempt,
        spotify_url: spotify_url.context("track requires a Spotify track URL, URI, or ID")?,
    })
}

fn usage() -> &'static str {
    "Usage:\n  convert-invert [attempt]\n  convert-invert playlist [--attempt N] [--playlist-id ID_OR_URL] [--random-order]\n  convert-invert track <spotify-track-url> [--attempt N]"
}

fn init_silent_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::registry().with(env_filter).try_init();
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
