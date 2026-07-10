DROP INDEX IF EXISTS idx_search_items_playlist_id;
ALTER TABLE search_items
  DROP COLUMN IF EXISTS playlist_name,
  DROP COLUMN IF EXISTS playlist_id;
