use std::io::Write;

use diesel::{
    AsExpression, Associations, FromSqlRow, Identifiable, Insertable, Queryable, Selectable,
    deserialize::{self, FromSql},
    pg::{Pg, PgValue},
    serialize::{self, IsNull, Output, ToSql},
};
use serde::{Deserialize, Serialize};

use crate::internals::{
    context::context_manager::{
        DownloadedFile, RejectReason as RuntimeRejectReason, RejectedTrack, RetryRequest,
    },
    database::schema::{self, sql_types},
    search::search_manager::{
        DownloadableFile as RuntimeDownloadableFile, JudgeSubmission as RuntimeJudgeSubmission,
        SearchItem as RuntimeSearchItem,
    },
};

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = schema::search_items)]
pub struct SearchItemRow {
    pub id: i32,
    pub track_id: i32,
    pub track: String,
    pub artist: String,
    pub album: String,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::search_items)]
pub struct NewSearchItemRow {
    pub track_id: i32,
    pub track: String,
    pub artist: String,
    pub album: String,
}

impl From<&RuntimeSearchItem> for NewSearchItemRow {
    fn from(value: &RuntimeSearchItem) -> Self {
        Self {
            track_id: value.track_id,
            track: value.track.clone(),
            artist: value.artist.clone(),
            album: value.album.clone(),
        }
    }
}

impl From<RuntimeSearchItem> for NewSearchItemRow {
    fn from(value: RuntimeSearchItem) -> Self {
        Self::from(&value)
    }
}

impl From<SearchItemRow> for RuntimeSearchItem {
    fn from(value: SearchItemRow) -> Self {
        Self {
            track_id: value.track_id,
            track: value.track,
            artist: value.artist,
            album: value.album,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = schema::downloadable_files)]
pub struct DownloadableFileRow {
    pub id: i32,
    pub filename: String,
    pub username: String,
    pub size: i32,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::downloadable_files)]
pub struct NewDownloadableFileRow {
    pub filename: String,
    pub username: String,
    pub size: i32,
}

impl From<&RuntimeDownloadableFile> for NewDownloadableFileRow {
    fn from(value: &RuntimeDownloadableFile) -> Self {
        Self {
            filename: value.filename.clone(),
            username: value.username.clone(),
            size: value.size,
        }
    }
}

impl From<RuntimeDownloadableFile> for NewDownloadableFileRow {
    fn from(value: RuntimeDownloadableFile) -> Self {
        Self::from(&value)
    }
}

impl From<DownloadableFileRow> for RuntimeDownloadableFile {
    fn from(value: DownloadableFileRow) -> Self {
        Self {
            filename: value.filename,
            username: value.username,
            size: value.size,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = schema::judge_submissions)]
#[diesel(belongs_to(SearchItemRow, foreign_key = track))]
#[diesel(belongs_to(DownloadableFileRow, foreign_key = query))]
pub struct JudgeSubmissionRow {
    pub id: i32,
    pub track: i32,
    pub query: i32,
    pub score: Option<f32>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::judge_submissions)]
pub struct NewJudgeSubmissionRow {
    pub track: i32,
    pub query: i32,
    pub score: Option<f32>,
}

#[derive(Debug, Clone)]
pub struct JudgeSubmissionJoined {
    pub row: JudgeSubmissionRow,
    pub track: SearchItemRow,
    pub query: DownloadableFileRow,
}

impl From<JudgeSubmissionJoined> for RuntimeJudgeSubmission {
    fn from(value: JudgeSubmissionJoined) -> Self {
        Self {
            track: value.track.into(),
            query: value.query.into(),
            score: None,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = schema::downloaded_file)]
pub struct DownloadedFileRow {
    pub id: i32,
    pub filename: String,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::downloaded_file)]
pub struct NewDownloadedFileRow {
    pub filename: String,
}

impl From<&DownloadedFile> for NewDownloadedFileRow {
    fn from(value: &DownloadedFile) -> Self {
        Self {
            filename: value.filename.clone(),
        }
    }
}

impl From<DownloadedFile> for NewDownloadedFileRow {
    fn from(value: DownloadedFile) -> Self {
        Self::from(&value)
    }
}

impl From<DownloadedFileRow> for DownloadedFile {
    fn from(value: DownloadedFileRow) -> Self {
        Self {
            filename: value.filename,
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = schema::retry_request)]
#[diesel(belongs_to(JudgeSubmissionRow, foreign_key = request))]
#[diesel(belongs_to(DownloadableFileRow, foreign_key = failed_download_result))]
pub struct RetryRequestRow {
    pub id: i32,
    pub request: i32,
    pub retry_attempts: i32,
    pub failed_download_result: i32,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::retry_request)]
pub struct NewRetryRequestRow {
    pub request: i32,
    pub retry_attempts: i32,
    pub failed_download_result: i32,
}

#[derive(Debug, Clone)]
pub struct RetryRequestJoined {
    pub row: RetryRequestRow,
    pub request: JudgeSubmissionJoined,
    pub failed_download_result: DownloadableFileRow,
}

impl From<RetryRequestJoined> for RetryRequest {
    fn from(value: RetryRequestJoined) -> Self {
        Self {
            request: value.request.into(),
            retry_attempts: value.row.retry_attempts as u8,
            failed_download_result: value.failed_download_result.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::RejectReason)]
pub enum RejectReasonRow {
    AlreadyDownloaded,
    LowScore,
    NotMusic,
    AbandonedAttemptingSearch,
}

impl From<&RuntimeRejectReason> for RejectReasonRow {
    fn from(value: &RuntimeRejectReason) -> Self {
        match value {
            RuntimeRejectReason::AlreadyDownloaded => Self::AlreadyDownloaded,
            RuntimeRejectReason::LowScore(_) => Self::LowScore,
            RuntimeRejectReason::NotMusic(_) => Self::NotMusic,
            RuntimeRejectReason::AbandonedAttemptingSearch => Self::AbandonedAttemptingSearch,
        }
    }
}

impl From<RuntimeRejectReason> for RejectReasonRow {
    fn from(value: RuntimeRejectReason) -> Self {
        Self::from(&value)
    }
}

impl From<RejectReasonRow> for RuntimeRejectReason {
    fn from(value: RejectReasonRow) -> Self {
        match value {
            RejectReasonRow::AlreadyDownloaded => RuntimeRejectReason::AlreadyDownloaded,
            // Database enum intentionally drops payload details.
            RejectReasonRow::LowScore => RuntimeRejectReason::LowScore(0.0),
            RejectReasonRow::NotMusic => RuntimeRejectReason::NotMusic(String::new()),
            RejectReasonRow::AbandonedAttemptingSearch => {
                RuntimeRejectReason::AbandonedAttemptingSearch
            }
        }
    }
}

impl ToSql<sql_types::RejectReason, Pg> for RejectReasonRow {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let value = match self {
            RejectReasonRow::AlreadyDownloaded => b"already_downloaded".as_slice(),
            RejectReasonRow::LowScore => b"low_score".as_slice(),
            RejectReasonRow::NotMusic => b"not_music".as_slice(),
            RejectReasonRow::AbandonedAttemptingSearch => b"abandoned_attempting_search".as_slice(),
        };
        out.write_all(value)?;
        Ok(IsNull::No)
    }
}

impl FromSql<sql_types::RejectReason, Pg> for RejectReasonRow {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"already_downloaded" => Ok(Self::AlreadyDownloaded),
            b"low_score" => Ok(Self::LowScore),
            b"not_music" => Ok(Self::NotMusic),
            b"abandoned_attempting_search" => Ok(Self::AbandonedAttemptingSearch),
            unknown => Err(format!(
                "Unrecognized reject_reason value: {}",
                String::from_utf8_lossy(unknown)
            )
            .into()),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = schema::rejected_track)]
#[diesel(belongs_to(JudgeSubmissionRow, foreign_key = track))]
pub struct RejectedTrackRow {
    pub id: i32,
    pub track: i32,
    pub reason: RejectReasonRow,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = schema::rejected_track)]
pub struct NewRejectedTrackRow {
    pub track: i32,
    pub reason: RejectReasonRow,
    pub value: Option<String>,
}

impl NewRejectedTrackRow {
    pub fn from_runtime(track_id: i32, value: &RejectedTrack) -> Self {
        let (_, reason) = value.parts();
        Self {
            track: track_id,
            reason: reason.into(),
            value: match reason {
                RuntimeRejectReason::AlreadyDownloaded
                | RuntimeRejectReason::AbandonedAttemptingSearch => None,
                RuntimeRejectReason::LowScore(score) => Some(format!("{score}")),
                RuntimeRejectReason::NotMusic(filename) => Some(filename.to_owned()),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct RejectedTrackJoined {
    pub row: RejectedTrackRow,
    pub track: JudgeSubmissionJoined,
}

impl From<RejectedTrackJoined> for RejectedTrack {
    fn from(value: RejectedTrackJoined) -> Self {
        RejectedTrack::new(value.track.into(), value.row.reason.into())
    }
}
