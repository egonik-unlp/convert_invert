#! /usr/bin/env zsh

docker compose down -v
docker compose up -d
sleep 3
diesel setup
SERVER_BIND=127.0.0.1:8081 WORKER_COUNT=4 cargo run --release --bin trigger_server
# cargo run --release | tee logs-script
