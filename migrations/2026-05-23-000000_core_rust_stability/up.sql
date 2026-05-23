ALTER TABLE search_items
  ALTER COLUMN track_id TYPE VARCHAR USING track_id::text;

ALTER TABLE downloadable_files
  ALTER COLUMN size TYPE BIGINT;

ALTER TYPE reject_reason ADD VALUE IF NOT EXISTS 'banned';

ALTER TABLE downloaded_file
  ADD COLUMN IF NOT EXISTS track INTEGER REFERENCES search_items(id);

CREATE INDEX IF NOT EXISTS downloaded_file_track_idx
  ON downloaded_file(track);
