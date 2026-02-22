#! /usr/bin/env zsh

docker compose down -v
docker compose up -d
sleep 3
diesel setup
cargo build --release --bin convert-invert
SERVER_BIND=127.0.0.1:8081 WORKER_COUNT=4 WORKER_BIN=target/release/convert-invert cargo run --release --bin trigger_server
# cargo run --release | tee logs-script
