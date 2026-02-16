use anyhow::{Context, Ok};
use diesel::{PgConnection, dsl::insert_into, prelude::*};

use crate::internals::context::context_manager::{RejectedTrack, RetryRequest, Track};
use crate::internals::database::{model, schema};
use crate::internals::search::search_manager::{
    DownloadableFile as RuntimeDownloadableFile, JudgeSubmission as RuntimeJudgeSubmission,
    SearchItem as RuntimeSearchItem,
};
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
        let inserted_id = insert_into(schema::search_items::table)
            .values(&value)
            .returning(schema::search_items::id)
            .get_result(connection)
            .context("Insert search item")?;
        Ok(inserted_id)
    }

    fn insert_downloadable_file(
        connection: &mut PgConnection,
        downloadable_file: &RuntimeDownloadableFile,
    ) -> anyhow::Result<i32> {
        let value = model::NewDownloadableFileRow::from(downloadable_file);
        let inserted_id = insert_into(schema::downloadable_files::table)
            .values(&value)
            .returning(schema::downloadable_files::id)
            .get_result(connection)
            .context("Insert downloadable file")?;
        Ok(inserted_id)
    }
    fn get_downloadable_file_id(
        connection: &mut PgConnection,
        downloadable_file: &RuntimeDownloadableFile,
    ) -> anyhow::Result<i32> {
        use schema::downloadable_files::dsl as df;
        let downloadable_id = schema::downloadable_files::table
            .filter(df::filename.eq(&downloadable_file.filename))
            // .filter(df::username.eq(&downloadable_file.username))
            // .filter(df::size.eq(&downloadable_file.size))
            .select(df::id)
            .get_result(connection)
            .context("fetch download id from database at get down file id")?;
        Ok(downloadable_id)
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
        let inserted_id = insert_into(schema::judge_submissions::table)
            .values(&value)
            .returning(js::id)
            .get_result(connection)
            .context("Insert judge submission")?;
        Ok(inserted_id)
    }
    // BUG: ERROR HERE
    pub fn get_judge_submission_id(
        connection: &mut PgConnection,
        judge_submission: &RuntimeJudgeSubmission,
    ) -> anyhow::Result<i32> {
        use schema::downloadable_files::dsl as df;
        use schema::judge_submissions::dsl as js;
        let track_id: i32 = schema::downloadable_files::table
            .filter(df::filename.eq(&judge_submission.query.filename))
            .filter(df::username.eq(&judge_submission.query.username))
            .filter(df::size.eq(judge_submission.query.size))
            .select(df::id)
            .get_result(connection)
            .context("Getting download id in js")?;

        let judge_id = schema::judge_submissions::table
            .filter(js::query.eq(track_id))
            .select(js::id)
            .get_result(connection)
            .with_context(|| {
                format!(
                    "fetch judge id from db JSGET, track:\n{:?} track_id:\n{:?}",
                    judge_submission, track_id
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
        insert_into(schema::retry_request::table)
            .values(&value)
            .execute(connection)
            .context("Insert retry request")?;
        Ok(())
    }

    fn insert_rejected_track(
        connection: &mut PgConnection,
        rejected_track: &RejectedTrack,
    ) -> anyhow::Result<()> {
        let (judge_submission, _) = rejected_track.parts();
        let track_id = Self::get_judge_submission_id(connection, judge_submission)?;
        let value = model::NewRejectedTrackRow::from_runtime(track_id, rejected_track);
        insert_into(schema::rejected_track::table)
            .values(&value)
            .execute(connection)
            .context("Insert rejected track")?;
        Ok(())
    }
    fn get_search_item_id(
        connection: &mut PgConnection,
        search_item: &RuntimeSearchItem,
    ) -> anyhow::Result<i32> {
        use schema::search_items::dsl as sl;
        let search_id = schema::search_items::table
            .filter(sl::track_id.eq(search_item.track_id))
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
                        let value = model::NewDownloadedFileRow::from(downloaded_file);
                        insert_into(schema::downloaded_file::table)
                            .values(value)
                            .execute(connection)
                            .context("Insert downloaded file")?;
                    }
                    Track::Retry(retry_request) => {
                        Self::insert_retry_request(connection, retry_request)?;
                    }
                    Track::Reject(rejected_track) => {
                        Self::insert_rejected_track(connection, rejected_track)?;
                    }
                    Track::NoMoreTracks => (),
                }
                Ok(())
            })
            //BUG: Explodes here
            .context("Persist track into database")?;
        Ok(())
    }
}
