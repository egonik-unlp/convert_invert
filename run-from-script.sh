#! /usr/bin/env zsh

docker compose down -v
docker compose up -d
diesel database setup
diesel database reset 
cargo run --release
