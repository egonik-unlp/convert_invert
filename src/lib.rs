//! # convert-invert
//!
//! A Spotify-playlist-to-Soulseek bridge: matches Spotify tracks against the Soulseek
//! network, scores candidates with a Levenshtein-based judge, and downloads the best
//! match.
//!
//! This crate provides the core logic for searching, judging, and downloading tracks,
//! orchestrated by a managed run cycle.
//!
//! ## Core Components
//!
//! - **Managers**: The system is divided into specialized managers for search, judging,
//!   downloads, database interaction, and playlist querying.
//! - **Run Cycle**: A central orchestration loop that manages task lifecycle using `tokio::task::JoinSet`.
//! - **Persistence**: Uses PostgreSQL (via Diesel) for authoritative state and Redis for
//!   real-time progress and task queuing.

pub mod internals;
