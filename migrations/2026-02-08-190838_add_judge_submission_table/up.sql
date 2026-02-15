-- Your SQL goes here
--
--
CREATE TABLE IF NOT EXISTS judge_submissions (
  id serial not null primary key,
    track int references search_items(id) not null,
    query int references downloadable_files(id) not null,
    score real
)
