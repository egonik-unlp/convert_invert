// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "reject_reason"))]
    pub struct RejectReason;
}

diesel::table! {
    downloadable_files (id) {
        id -> Int4,
        filename -> Varchar,
        username -> Varchar,
        size -> Int8,
    }
}

diesel::table! {
    downloaded_file (id) {
        id -> Int4,
        filename -> Varchar,
        track -> Nullable<Int4>,
    }
}

diesel::table! {
    judge_submissions (id) {
        id -> Int4,
        track -> Int4,
        query -> Int4,
        score -> Nullable<Float4>,
        relative_mi_score -> Nullable<Float4>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::RejectReason;

    rejected_track (id) {
        id -> Int4,
        track -> Int4,
        reason -> RejectReason,
        value -> Nullable<Varchar>,
    }
}

diesel::table! {
    retry_request (id) {
        id -> Int4,
        request -> Int4,
        retry_attempts -> Int4,
        failed_download_result -> Int4,
    }
}

diesel::table! {
    search_items (id) {
        id -> Int4,
        track_id -> Varchar,
        track -> Varchar,
        artist -> Varchar,
        album -> Varchar,
        playlist_id -> Nullable<Varchar>,
        playlist_name -> Nullable<Varchar>,
    }
}

diesel::joinable!(judge_submissions -> downloadable_files (query));
diesel::joinable!(judge_submissions -> search_items (track));
diesel::joinable!(downloaded_file -> search_items (track));
diesel::joinable!(rejected_track -> judge_submissions (track));
diesel::joinable!(retry_request -> downloadable_files (failed_download_result));
diesel::joinable!(retry_request -> judge_submissions (request));

diesel::allow_tables_to_appear_in_same_query!(
    downloadable_files,
    downloaded_file,
    judge_submissions,
    rejected_track,
    retry_request,
    search_items,
);
