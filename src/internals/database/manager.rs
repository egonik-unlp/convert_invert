use anyhow::Context;
use diesel::{
    PgConnection, dsl::insert_into, prelude::*, sql_types::Integer, sql_types::Nullable,
    sql_types::Text,
};

use crate::internals::context::context_manager::{RejectedTrack, RetryRequest, Track};
use crate::internals::database::{model, schema};
use crate::internals::search::search_manager::{
    DownloadableFile as RuntimeDownloadableFile, JudgeSubmission as RuntimeJudgeSubmission,
    SearchItem as RuntimeSearchItem,
};

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

fn reject_reason_name(reason: model::RejectReasonRow) -> &'static str {
    match reason {
        model::RejectReasonRow::AlreadyDownloaded => "already_downloaded",
        model::RejectReasonRow::LowScore => "low_score",
        model::RejectReasonRow::NotMusic => "not_music",
        model::RejectReasonRow::Banned => "banned",
        model::RejectReasonRow::AbandonedAttemptingSearch => "abandoned_attempting_search",
    }
}

pub struct DatabaseManager<'a> {
    pub connection: &'a mut PgConnection,
}

impl<'a> DatabaseManager<'a> {
    pub fn new(connection: &'a mut PgConnection) -> Self {
        Self { connection }
    }

    fn insert_search_item(
        connection: &mut PgConnection,
        search_item: &RuntimeSearchItem,
    ) -> anyhow::Result<i32> {
        let value = model::NewSearchItemRow::from(search_item);
        insert_into(schema::search_items::table)
            .values(&value)
            .on_conflict(schema::search_items::track_id)
            .do_nothing()
            .execute(connection)
            .context("Insert search item")?;

        let inserted_id = Self::get_search_item_id(connection, search_item)
            .context("Fetch inserted or existing search item")?;
        Ok(inserted_id)
    }

    fn insert_downloadable_file(
        connection: &mut PgConnection,
        downloadable_file: &RuntimeDownloadableFile,
    ) -> anyhow::Result<i32> {
        let value = model::NewDownloadableFileRow::from(downloadable_file);
        insert_into(schema::downloadable_files::table)
            .values(&value)
            .on_conflict((
                schema::downloadable_files::filename,
                schema::downloadable_files::username,
                schema::downloadable_files::size,
            ))
            .do_nothing()
            .execute(connection)
            .context("Insert downloadable file")?;

        let inserted_id = Self::get_downloadable_file_id(connection, downloadable_file)
            .context("Fetch inserted or existing downloadable file")?;
        Ok(inserted_id)
    }

    fn get_downloadable_file_id(
        connection: &mut PgConnection,
        downloadable_file: &RuntimeDownloadableFile,
    ) -> anyhow::Result<i32> {
        use schema::downloadable_files::dsl as df;
        let downloadable_id = schema::downloadable_files::table
            .filter(df::filename.eq(&downloadable_file.filename))
            .filter(df::username.eq(&downloadable_file.username))
            .filter(df::size.eq(downloadable_file.size))
            .select(df::id)
            .get_result(connection)
            .context("Fetch downloadable file id")?;
        Ok(downloadable_id)
    }

    fn upsert_downloaded_file(
        connection: &mut PgConnection,
        downloaded_file: &crate::internals::context::context_manager::DownloadedFile,
    ) -> anyhow::Result<()> {
        use schema::downloaded_file::dsl as dl;
        let track_id = Some(
            Self::get_search_item_id(connection, &downloaded_file.track)
                .context("Fetch downloaded file track id")?,
        );
        let value = model::NewDownloadedFileRow::from_runtime(downloaded_file, track_id);
        insert_into(schema::downloaded_file::table)
            .values(&value)
            .on_conflict(dl::filename)
            .do_update()
            .set(dl::track.eq(value.track))
            .execute(connection)
            .context("Insert downloaded file")?;
        Ok(())
    }

    pub fn is_search_item_downloaded(
        &mut self,
        search_item: &RuntimeSearchItem,
    ) -> anyhow::Result<bool> {
        use schema::downloaded_file::dsl as dl;
        let search_id = match Self::get_search_item_id(self.connection, search_item) {
            Ok(search_id) => search_id,
            Err(_) => return Ok(false),
        };
        let count: i64 = schema::downloaded_file::table
            .filter(dl::track.eq(search_id))
            .count()
            .get_result(self.connection)
            .context("Check downloaded track")?;
        Ok(count > 0)
    }

    fn existing_rejected_track_id(
        connection: &mut PgConnection,
        track_id: i32,
        value: &model::NewRejectedTrackRow,
    ) -> anyhow::Result<Option<i32>> {
        let existing = diesel::sql_query(
            "SELECT id
             FROM rejected_track
             WHERE track = $1
               AND reason = $2::reject_reason
               AND value IS NOT DISTINCT FROM $3
             LIMIT 1",
        )
        .bind::<Integer, _>(track_id)
        .bind::<Text, _>(reject_reason_name(value.reason))
        .bind::<Nullable<Text>, _>(value.value.as_deref())
        .get_result::<IdRow>(connection)
        .optional()
        .map(|row| row.map(|row| row.id))
        .context("Fetch existing rejected track")?;
        Ok(existing)
    }

    fn existing_retry_request_id(
        connection: &mut PgConnection,
        value: &model::NewRetryRequestRow,
    ) -> anyhow::Result<Option<i32>> {
        use schema::retry_request::dsl as rr;
        let existing = schema::retry_request::table
            .filter(rr::request.eq(value.request))
            .filter(rr::retry_attempts.eq(value.retry_attempts))
            .filter(rr::failed_download_result.eq(value.failed_download_result))
            .select(rr::id)
            .first::<i32>(connection)
            .optional()
            .context("Fetch existing retry request")?;
        Ok(existing)
    }

    fn update_jugde_submission_score(
        connection: &mut PgConnection,
        judge_submission: &RuntimeJudgeSubmission,
    ) -> anyhow::Result<()> {
        let judge_submission_id = Self::get_judge_submission_id(connection, judge_submission)?;
        diesel::update(
            schema::judge_submissions::table
                .filter(schema::judge_submissions::id.eq(&judge_submission_id)),
        )
        .set(schema::judge_submissions::score.eq(judge_submission.score))
        .execute(connection)
        .context("update and set score")?;
        Ok(())
    }
    fn insert_judge_submission(
        connection: &mut PgConnection,
        judge_submission: &RuntimeJudgeSubmission,
    ) -> anyhow::Result<i32> {
        use schema::judge_submissions::dsl as js;
        let track_id = Self::get_search_item_id(connection, &judge_submission.track)
            .with_context(|| format!("fetching track_id={:?}", judge_submission.track))?;
        let query_id = Self::insert_downloadable_file(connection, &judge_submission.query)
            .with_context(|| format!("fetching track_id={:?}", judge_submission.query))?;

        let value = model::NewJudgeSubmissionRow {
            track: track_id,
            query: query_id,
            score: None,
        };
        insert_into(schema::judge_submissions::table)
            .values(&value)
            .on_conflict((js::track, js::query))
            .do_nothing()
            .execute(connection)
            .context("Insert judge submission")?;
        let inserted_id = schema::judge_submissions::table
            .filter(js::track.eq(track_id))
            .filter(js::query.eq(query_id))
            .select(js::id)
            .get_result(connection)
            .context("Fetch inserted or existing judge submission")?;
        Ok(inserted_id)
    }
    pub fn get_judge_submission_id(
        connection: &mut PgConnection,
        judge_submission: &RuntimeJudgeSubmission,
    ) -> anyhow::Result<i32> {
        use schema::downloadable_files::dsl as df;
        use schema::judge_submissions::dsl as js;
        let query_id: i32 = schema::downloadable_files::table
            .filter(df::filename.eq(&judge_submission.query.filename))
            .filter(df::username.eq(&judge_submission.query.username))
            .filter(df::size.eq(judge_submission.query.size))
            .select(df::id)
            .get_result(connection)
            .context("Getting download id in js")?;
        let search_id = Self::get_search_item_id(connection, &judge_submission.track)?;

        let judge_id = schema::judge_submissions::table
            .filter(js::track.eq(search_id))
            .filter(js::query.eq(query_id))
            .select(js::id)
            .get_result(connection)
            .with_context(|| {
                format!(
                    "Fetch judge submission id for track_id={} query_id={}",
                    search_id, query_id
                )
            })?;
        Ok(judge_id)
    }

    fn insert_retry_request(
        connection: &mut PgConnection,
        retry_request: &RetryRequest,
    ) -> anyhow::Result<()> {
        let request_id = Self::get_judge_submission_id(connection, &retry_request.request)?;
        let failed_download_result =
            Self::get_downloadable_file_id(connection, &retry_request.failed_download_result)?;
        let value = model::NewRetryRequestRow {
            request: request_id,
            retry_attempts: i32::from(retry_request.retry_attempts),
            failed_download_result,
        };
        if Self::existing_retry_request_id(connection, &value)?.is_none() {
            insert_into(schema::retry_request::table)
                .values(&value)
                .execute(connection)
                .context("Insert retry request")?;
        }
        Ok(())
    }

    fn insert_rejected_track(
        connection: &mut PgConnection,
        rejected_track: &RejectedTrack,
    ) -> anyhow::Result<()> {
        let (judge_submission, _) = rejected_track.parts();
        let track_id = Self::get_judge_submission_id(connection, judge_submission)?;
        let value = model::NewRejectedTrackRow::from_runtime(track_id, rejected_track);
        if Self::existing_rejected_track_id(connection, track_id, &value)?.is_none() {
            insert_into(schema::rejected_track::table)
                .values(&value)
                .execute(connection)
                .context("Insert rejected track")?;
        }
        Ok(())
    }
    pub fn get_search_item_id(
        connection: &mut PgConnection,
        search_item: &RuntimeSearchItem,
    ) -> anyhow::Result<i32> {
        use schema::search_items::dsl as sl;
        let search_id = schema::search_items::table
            .filter(sl::track_id.eq(&search_item.track_id))
            .select(schema::search_items::id)
            .get_result(connection)
            .context("database fetch search_id in get seatch id func")?;
        Ok(search_id)
    }

    pub fn load_item_to_database(&mut self, item: &Track) -> anyhow::Result<()> {
        self.connection
            .transaction::<_, anyhow::Error, _>(|connection| {
                match item {
                    Track::Query(search_item) => {
                        Self::insert_search_item(connection, search_item)?;
                    }
                    Track::Result(judge_submission) => {
                        Self::insert_judge_submission(connection, judge_submission)?;
                    }
                    Track::Downloadable(judge_submission) => {
                        Self::update_jugde_submission_score(connection, judge_submission)?;
                    }
                    Track::File(downloaded_file) => {
                        Self::upsert_downloaded_file(connection, downloaded_file)?;
                    }
                    Track::Retry(retry_request) => {
                        Self::insert_retry_request(connection, retry_request)?;
                    }
                    Track::Reject(rejected_track) => {
                        Self::insert_rejected_track(connection, rejected_track)?;
                    }
                }
                Ok(())
            })
            .context("Persist track into database")?;
        Ok(())
    }
}
