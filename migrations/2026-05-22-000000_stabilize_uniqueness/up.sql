UPDATE judge_submissions js
SET track = canonical.keep_id
FROM (
  SELECT track_id, MIN(id) AS keep_id
  FROM search_items
  GROUP BY track_id
) canonical
JOIN search_items duplicate
  ON duplicate.track_id = canonical.track_id
WHERE js.track = duplicate.id
  AND duplicate.id <> canonical.keep_id;

DELETE FROM search_items si
USING search_items keep
WHERE si.track_id = keep.track_id
  AND si.id > keep.id;

UPDATE judge_submissions js
SET query = canonical.keep_id
FROM (
  SELECT filename, username, size, MIN(id) AS keep_id
  FROM downloadable_files
  GROUP BY filename, username, size
) canonical
JOIN downloadable_files duplicate
  ON duplicate.filename = canonical.filename
 AND duplicate.username = canonical.username
 AND duplicate.size = canonical.size
WHERE js.query = duplicate.id
  AND duplicate.id <> canonical.keep_id;

UPDATE retry_request rr
SET failed_download_result = canonical.keep_id
FROM (
  SELECT filename, username, size, MIN(id) AS keep_id
  FROM downloadable_files
  GROUP BY filename, username, size
) canonical
JOIN downloadable_files duplicate
  ON duplicate.filename = canonical.filename
 AND duplicate.username = canonical.username
 AND duplicate.size = canonical.size
WHERE rr.failed_download_result = duplicate.id
  AND duplicate.id <> canonical.keep_id;

DELETE FROM downloadable_files df
USING downloadable_files keep
WHERE df.filename = keep.filename
  AND df.username = keep.username
  AND df.size = keep.size
  AND df.id > keep.id;

UPDATE retry_request rr
SET request = canonical.keep_id
FROM (
  SELECT track, query, MIN(id) AS keep_id
  FROM judge_submissions
  GROUP BY track, query
) canonical
JOIN judge_submissions duplicate
  ON duplicate.track = canonical.track
 AND duplicate.query = canonical.query
WHERE rr.request = duplicate.id
  AND duplicate.id <> canonical.keep_id;

UPDATE rejected_track rt
SET track = canonical.keep_id
FROM (
  SELECT track, query, MIN(id) AS keep_id
  FROM judge_submissions
  GROUP BY track, query
) canonical
JOIN judge_submissions duplicate
  ON duplicate.track = canonical.track
 AND duplicate.query = canonical.query
WHERE rt.track = duplicate.id
  AND duplicate.id <> canonical.keep_id;

DELETE FROM judge_submissions js
USING judge_submissions keep
WHERE js.track = keep.track
  AND js.query = keep.query
  AND js.id > keep.id;

DELETE FROM downloaded_file df
USING downloaded_file keep
WHERE df.filename = keep.filename
  AND df.id > keep.id;

DELETE FROM retry_request rr
USING retry_request keep
WHERE rr.request = keep.request
  AND rr.retry_attempts = keep.retry_attempts
  AND rr.failed_download_result = keep.failed_download_result
  AND rr.id > keep.id;

DELETE FROM rejected_track rt
USING rejected_track keep
WHERE rt.track = keep.track
  AND rt.reason = keep.reason
  AND rt.value IS NOT DISTINCT FROM keep.value
  AND rt.id > keep.id;

CREATE UNIQUE INDEX IF NOT EXISTS search_items_track_id_uidx
  ON search_items (track_id);

CREATE UNIQUE INDEX IF NOT EXISTS downloadable_files_identity_uidx
  ON downloadable_files (filename, username, size);

CREATE UNIQUE INDEX IF NOT EXISTS judge_submissions_track_query_uidx
  ON judge_submissions (track, query);

CREATE UNIQUE INDEX IF NOT EXISTS downloaded_file_filename_uidx
  ON downloaded_file (filename);

CREATE UNIQUE INDEX IF NOT EXISTS retry_request_identity_uidx
  ON retry_request (request, retry_attempts, failed_download_result);

CREATE UNIQUE INDEX IF NOT EXISTS rejected_track_identity_uidx
  ON rejected_track (track, reason, COALESCE(value, ''));
