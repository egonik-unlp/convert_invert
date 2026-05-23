DELETE FROM downloaded_file df
USING downloaded_file keep
WHERE df.track = keep.track
  AND df.track IS NOT NULL
  AND df.id > keep.id;

CREATE UNIQUE INDEX IF NOT EXISTS downloaded_file_track_uidx
  ON downloaded_file(track)
  WHERE track IS NOT NULL;
