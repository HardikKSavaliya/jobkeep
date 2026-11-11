# JobKeep

Background jobs, scheduling and recurring tasks for Rust, in the spirit of Hangfire.

- **Jobs are plain async functions.** `#[job]` turns one into a serializable payload struct.
- **Runs on Postgres in production** and in memory for tests. Any number of servers can share one database.
- **Retries** with exponential backoff and jitter. A panic is caught and treated as a retryable error.
- **Scheduled, delayed, recurring (cron) and continuation jobs**, plus deduplication keys.
- **At-least-once delivery with leases.** Jobs whose worker crashed are picked up again.
- **Graceful shutdown.** Running jobs are cancelled cooperatively, and unfinished ones go back to their queue without using up an attempt.
- **Web dashboard and CLI** to inspect, requeue, delete and purge jobs.

## Quick start

```rust
use jobkeep_client::Client;
use jobkeep_core::{job, recurring, JobContext, JobError};
use jobkeep_postgres::PgStorage;
use jobkeep_server::Server;

#[job(queue = "emails", max_attempts = 5)]
async fn send_welcome_email(ctx: &JobContext, to: String) -> anyhow::Result<()> {
    // `?` on an anyhow error makes the job retry with backoff.
    smtp_send(&to).await?;
    Ok(())
}

#[recurring("0 0 3 * * *")] // 03:00 UTC daily: sec min hour day month weekday
async fn nightly_cleanup(ctx: &JobContext) -> Result<(), JobError> {
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = PgStorage::connect(&std::env::var("DATABASE_URL")?).await?;
    storage.migrate().await?;

    let client = Client::new(storage.clone());
    client.enqueue(&SendWelcomeEmail { to: "ada@example.com".into() }).await?;

    Server::builder(storage)
        .register::<SendWelcomeEmail>()
        .recurring::<NightlyCleanup>()
        .queues(["default", "emails"])
        .concurrency(20)
        .build()
        .run_until_ctrl_c()
        .await;
    Ok(())
}
```

## Examples

You need Rust (stable) for both examples, and Docker for the demo app. Run every command from the repository root.

```sh
git clone https://github.com/hardiksavaliya/jobkeep.git
cd jobkeep
```

### Demo app: add jobs and review them in the dashboard

[`examples/demo`](examples/demo) runs a worker, a page with buttons that add sample jobs, and the dashboard, all in one process, on Postgres in Docker.

```sh
# 1. Start Postgres (port 55433, database jobkeep_demo)
docker compose -f examples/demo/docker-compose.yml up -d

# 2. Run the demo. It creates the jobs table on startup.
cargo run -p jobkeep-demo
```

Open:

- <http://127.0.0.1:8080> to add jobs: orders with receipt continuations, flaky emails that retry, scheduled emails, a slow report you can cancel, a permanently failing import, a deduplicated import, or a batch of 20
- <http://127.0.0.1:8080/jobs> to review them in the dashboard: counts by state, filters, job details, requeue and delete

Stop it with Ctrl-C. Running jobs are cancelled and go back to their queue. When you're finished:

```sh
docker compose -f examples/demo/docker-compose.yml down      # add -v to delete the data too
```

Set `DATABASE_URL` to use a different database, or `BIND` to change the address. The [demo README](examples/demo/README.md) explains what each button shows.

### Quickstart: no database needed

```sh
cargo run -p jobkeep-server --example quickstart
```

This runs a retry, a continuation, a scheduled job and a recurring job on the in-memory backend for a few seconds, prints what happened, and exits. The code is in [`crates/jobkeep-server/examples/quickstart.rs`](crates/jobkeep-server/examples/quickstart.rs).

## Crates

| Crate | What it is |
|---|---|
| `jobkeep-core` | `Job` trait, `State` machine, `Storage` trait, `Registry`. The `macros` feature re-exports `#[job]`/`#[recurring]`; the `testing` feature has the backend conformance suite. |
| `jobkeep-macros` | `#[job]` and `#[recurring]` |
| `jobkeep-client` | `Client`: enqueue, schedule, continuations, requeue, delete, list, stats, purge |
| `jobkeep-server` | `Server`: workers, retries, heartbeats, maintenance, recurring scheduler, graceful shutdown |
| `jobkeep-postgres` | `PgStorage`: Postgres backend using `FOR UPDATE SKIP LOCKED` |
| `jobkeep-memory` | `MemoryStorage`: in-memory backend for tests, with a controllable clock |
| `jobkeep-dashboard` | axum router serving a web UI and JSON API |
| `jobkeep-cli` | `jobkeep` binary for operating a Postgres job store |

## Concepts

### Job lifecycle

```text
  Scheduled ──┐            ┌──────────── retry with backoff ──────────┐
              ▼            ▼                                          │
  Awaiting ─► Enqueued ─► Processing ─► Succeeded                     │
              ▲   ▲        │  │  └────► Failed                        │
              │   │        │  └───────────────────────────────────────┘
              │   └────────┘  worker lost / shutdown: requeue
              └── manual requeue from Succeeded, Failed or Deleted
  Every non-deleted state can move to Deleted.
```

Backends check every transition against this machine (`State::can_transition_to`).

### Errors and retries

- `JobError::Retryable` (what `?` on an `anyhow::Error` produces) retries according to the server's `RetryPolicy` until `max_attempts` runs out. The default is 2s, 4s, 8s, … capped at 1h, with jitter.
- `JobError::Permanent` moves the job straight to `Failed`.
- A panic counts as a retryable error.
- A payload that no longer deserializes, or a job type the server doesn't know, fails permanently.

### Delivery guarantees

Delivery is **at least once**. A worker holds a lease on each running job and renews it every third of the lease (30s by default). If the worker dies, the lease expires and maintenance puts the job back in its queue. A job on its last attempt is marked failed instead, so a job that keeps crashing its worker can't loop forever. A job can therefore run more than once, so make it idempotent.

Deleting a running job, or a lost lease, fires `ctx.cancellation`. Long-running jobs should watch it and return `JobError::Cancelled`. A job that ignores it is aborted after a grace period.

All "now" decisions (is it due, has the lease expired) use the backend's clock. Servers with slightly different clocks still agree.

### Scheduling, continuations, deduplication

```rust
client.schedule_in(&Reminder { user_id }, TimeDelta::hours(24)).await?;
let charge = client.enqueue(&ChargeCard { order_id }).await?;
client.enqueue_after(charge, &SendReceipt { order_id }).await?;    // runs once charge succeeds
client.enqueue_unique(format!("import:{date}"), &Import { date }).await?; // at most one per key
```

A continuation waits in `Awaiting` until its parent succeeds. If the parent fails or is deleted, the continuation and its own continuations are deleted.

Each recurring tick is enqueued with the key `recurring:{id}:{timestamp}`. Every server can run the scheduler and each tick still runs once. Ticks missed while no server was running are not caught up.

### Shared services

```rust
Server::builder(storage).extension(http_client.clone()) // ...
// in a job:
let http = ctx.extensions.get::<reqwest::Client>().unwrap();
```

## Dashboard

### Run it from the CLI

```sh
# 1. Start a Postgres (skip this if you already have one)
docker run -d --name jobkeep-pg -p 55432:5432 \
  -e POSTGRES_USER=jobkeep -e POSTGRES_PASSWORD=jobkeep -e POSTGRES_DB=jobkeep postgres:17-alpine
# Later runs: docker start jobkeep-pg

# 2. Point the CLI at the database
export DATABASE_URL=postgres://jobkeep:jobkeep@localhost:55432/jobkeep

# 3. Create the jobs table (safe to run more than once)
cargo run -p jobkeep-cli -- migrate

# 4. Start the dashboard
cargo run -p jobkeep-cli -- dashboard
```

Open <http://127.0.0.1:8080> and stop it with Ctrl-C. Use `--bind 127.0.0.1:9000` to change the address, or `--database-url postgres://...` instead of `DATABASE_URL`. After `cargo build -p jobkeep-cli`, you can run `target/debug/jobkeep dashboard` directly.

The dashboard shows jobs stored in that database. It is empty until an app using `PgStorage` with the same `DATABASE_URL` enqueues some. The in-memory quickstart example and the test suite don't write there.

### Embed it in your app

```rust
let app = axum::Router::new().nest("/jobs", jobkeep_dashboard::router(storage.clone()));
```

The dashboard shows job counts by state, a filterable job list, job details, and requeue/delete buttons. It has **no authentication**, so mount it behind your own auth layer. Mutating requests need an `x-jobkeep-action` header, which other origins can't send.

## CLI

```text
export DATABASE_URL=postgres://localhost/app
jobkeep migrate                  # create/upgrade the jobkeep_jobs table
jobkeep stats
jobkeep list --state failed --queue emails --limit 20
jobkeep show <id>
jobkeep requeue <id>
jobkeep delete <id>
jobkeep purge --older-than 7d    # succeeded + deleted jobs only
jobkeep dashboard --bind 127.0.0.1:8080
```

The server also purges succeeded and deleted jobs older than its `retention` (7 days by default).

## Development

```sh
cargo test --workspace --all-features
```

The Postgres tests (the shared conformance suite and full-lifecycle tests) need a database. Without one they print a note and are skipped locally. On CI a missing database is an error.

```sh
docker run -d --name jobkeep-pg -p 55432:5432 \
  -e POSTGRES_USER=jobkeep -e POSTGRES_PASSWORD=jobkeep -e POSTGRES_DB=jobkeep postgres:17-alpine
export JOBKEEP_TEST_DATABASE_URL=postgres://jobkeep:jobkeep@localhost:55432/jobkeep
cargo test --workspace --all-features
```

Each test creates its own schema, so tests run in parallel without interfering. Use a throwaway database: the schemas are not dropped afterwards.

A new backend proves itself by passing the conformance suite:

```rust
jobkeep_core::storage_conformance_tests!(MyStorage::new());
```

## Not yet

- Instant wake-up on enqueue (Postgres `LISTEN/NOTIFY`). Workers poll every `poll_interval`, 1s by default.
- Metrics export (Prometheus/OpenTelemetry). Tracing spans and events are in place.
- Per-job timeouts, rate limits, and batches.
- Publishing to crates.io.
