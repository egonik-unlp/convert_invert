# Convert Invert

Convert Invert searches a Spotify playlist and downloads matching tracks from Soulseek. It supports multi-worker runs and a companion web server to trigger downloads on demand.

## Quick Start

1. Start dependencies:

```bash
docker compose up -d
```

2. Run the main downloader:

```bash
cargo run --release
```

## Environment Variables

These are required for normal operation:

- `USER_NAME` / `USER_PASSWORD` (Soulseek)
- `CLIENT_ID` / `CLIENT_SECRET` (Spotify)
- `RUN_ID` (log/run grouping label)
- `LISTEN_PORT` (Soulseek client port)
- `SEARCH_TIMEOUT_SECS`
- `LOG_LEVEL`

## Playlist Partitioning

The downloader can process only a portion of the playlist. This is how the web server distributes work across multiple workers.

Partitioning is handled automatically by the trigger server. The downloader still accepts:

- `PLAYLIST_PARTS` / `PLAYLIST_PART_INDEX`
- `PLAYLIST_RANGE_START` / `PLAYLIST_RANGE_END`

but you do not need to set these manually when using the server.

## Trigger Server (Web API)

The `trigger_server` binary runs an HTTP server that spawns multiple downloader processes.

Build:

```bash
cargo build --release --bin trigger_server
```

Run:

```bash
SERVER_BIND=127.0.0.1:8081 WORKER_COUNT=4 cargo run --release --bin trigger_server
```

### Endpoints

`POST /start`  
Spawn workers and start downloading. Request body is optional.

Fields:
- `worker_count` (default: `WORKER_COUNT` or 4)
- `username_prefix` (default: `WORKER_USERNAME_PREFIX` or `worker`)
- `port_base` (default: `WORKER_PORT_BASE` or `41000`)
- `run_id_prefix` (default: `WORKER_RUN_ID_PREFIX` or `web-trigger`)
- `playlist_range_start` (optional)
- `playlist_range_end` (optional)

Example:

```bash
curl -X POST http://127.0.0.1:8081/start \
  -H 'Content-Type: application/json' \
  -d '{"worker_count":4,"username_prefix":"dl-","port_base":42000,"run_id_prefix":"batch-a"}'
```

`GET /status`  
Returns the list of currently running workers plus queue depth and failed count.

`POST /stop`  
Stops workers. If `pids` is omitted, stops all workers.

Example:

```bash
curl -X POST http://127.0.0.1:8081/stop \
  -H 'Content-Type: application/json' \
  -d '{"pids":[12345,12346]}'
```

### Server Environment Variables

- `SERVER_BIND` (default `127.0.0.1:8081`)
- `WORKER_COUNT` (default `4`)
- `WORKER_USERNAME_PREFIX` (default `worker`)
- `WORKER_PORT_BASE` (default `41000`)
- `WORKER_RUN_ID_PREFIX` (default `web-trigger`)
- `WORKER_BIN` (optional explicit path to the downloader binary)

### Notes

- The server now runs workers **in-process** (tokio tasks). It **fetches the playlist once**,
  builds a Redis-backed chunk queue, and workers pop chunks from it.
- When the queue is empty, failed tracks are retried with longer timeouts.
