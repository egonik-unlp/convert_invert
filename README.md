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
WORKER_USER_NAME=
WORKER_USER_PASSWORD=
WORKER_USERNAME_PREFIX=
SHARE_USER_NAME=
SHARE_USER_PASSWORD=
```

The Compose defaults provide Postgres, Redis, Jaeger, and `/downloads` wiring for local development.
The default multi-worker account mode is `suffixed`: with `WORKER_COUNT=4` and
`WORKER_USERNAME_PREFIX=worker`, workers log in as `worker1` through `worker4`
using `WORKER_USER_PASSWORD`. When the Compose sharing sidecar is enabled with
`SHARE_MODE=external`, it should log in with a distinct `SHARE_USER_NAME` while
sharing the downloaded files. Use `WORKER_ACCOUNT_MODE=same` only for
`WORKER_COUNT=1`.
Worker listen ports must fit the published host range. The root Compose stack
publishes `41000-41031` for workers and `41032-41033` for sharing by default;
if `WORKER_PORT_BASE`, `WORKER_PORT_COUNT`, or `WORKER_COUNT` changes, update
the Compose port range and firewall/router forwarding to match.

## Analyze Worker Logs

To compare a run against the reliability issues tracked in `PLAN-OVERHAUL.md`, run:

```bash
cargo run --bin analyze_run_log -- ../worker-docker-logs.log
```

The analyzer reports task-completion channel closures, searched/downloaded track counts, retries, empty-result exits, peer failures, and duplicate successful downloads by `track_id`.
