CREATE TABLE IF NOT EXISTS retry_request (
  id serial not null primary key,
request int references judge_submissions(id) not null,
retry_attempts integer not null,
failed_download_result int references  downloadable_files(id) not null
)
