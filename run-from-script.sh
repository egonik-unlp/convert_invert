#! /usr/bin/env zsh

docker compose down -v
docker compose up -d
sleep 3
diesel setup
cargo run --release | tee logs-script
