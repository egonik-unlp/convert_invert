DROP INDEX IF EXISTS downloaded_file_track_idx;

ALTER TABLE downloaded_file
  DROP COLUMN IF EXISTS track;

ALTER TABLE downloadable_files
  ALTER COLUMN size TYPE INTEGER;

ALTER TABLE search_items
  ALTER COLUMN track_id TYPE BIGINT USING track_id::bigint;

-- PostgreSQL cannot drop enum values directly. The 'banned' reject_reason value
-- is intentionally left in place on rollback.
