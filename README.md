# Convert Invert

Rust synchronization engine and dashboard API.

## Run With Docker

From the repository root:

```bash
docker compose up --build
```

The API listens on `http://localhost:3124/api`, and the frontend is served through the root Compose stack.

## Required Configuration

Set these in the root `.env` or shell environment as needed:

```bash
CLIENT_ID=
CLIENT_SECRET=
USER_NAME=
USER_PASSWORD=
```

The Compose defaults provide Postgres, Redis, Jaeger, and `/downloads` wiring for local development.
The default worker account mode is `same`: run one worker using `USER_NAME`, and
let the Compose sharing sidecar log in with the same Soulseek credentials while
sharing the downloaded files.

## Analyze Worker Logs

To compare a run against the reliability issues tracked in `PLAN-OVERHAUL.md`, run:

```bash
cargo run --bin analyze_run_log -- ../worker-docker-logs.log
```

The analyzer reports task-completion channel closures, searched/downloaded track counts, retries, empty-result exits, peer failures, and duplicate successful downloads by `track_id`.
