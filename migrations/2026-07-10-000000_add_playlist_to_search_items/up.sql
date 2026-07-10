-- Tag each track with the playlist that most recently included it, so the library can be
-- scoped/filtered per playlist instead of showing every run's tracks in one pile.
ALTER TABLE search_items
  ADD COLUMN IF NOT EXISTS playlist_id VARCHAR,
  ADD COLUMN IF NOT EXISTS playlist_name VARCHAR;

CREATE INDEX IF NOT EXISTS idx_search_items_playlist_id ON search_items (playlist_id);
