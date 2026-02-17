create type reject_reason as ENUM ('already_downloaded','low_score', 'not_music', 'abandoned_attempting_search');

CREATE TABLE IF NOT EXISTS rejected_track (
  id serial not null primary key,
  track int references judge_submissions(id) not null,
  reason reject_reason not null,
  value varchar
)
