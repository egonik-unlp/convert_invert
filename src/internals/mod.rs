//! Internal domain modules for the convert-invert engine.
//!
//! This module contains the core logic for the system, organized into specialized
//! submodules for different domains.

/// Core orchestration and task lifecycle management.
pub mod context;
/// Database persistence using Diesel and PostgreSQL.
pub mod database;
/// Soulseek file download management.
pub mod download;
/// Scoring and candidate evaluation strategies.
pub mod judge;
/// Parsing and deserialization helpers.
pub mod parsing;
/// Spotify playlist and track querying.
pub mod query;
/// Soulseek network search management.
pub mod search;
/// Configuration and tracing utilities.
pub mod utils;
/// Worker supervisor and chunk processing.
pub mod worker;
