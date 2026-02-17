#! /usr/bin/env zsh

docker compose down -v
docker compose up -d
diesel setup
cargo run --release
