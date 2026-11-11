# JobKeep demo

A small app that shows JobKeep end to end. Add sample jobs from a web page, and a worker in the same process runs them while you watch in the dashboard.

## Run it

From the repository root:

```sh
# 1. Start Postgres (port 55433, database jobkeep_demo)
docker compose -f examples/demo/docker-compose.yml up -d

# 2. Run the demo. It creates the jobs table on startup.
cargo run -p jobkeep-demo
```

Then open:

- **http://127.0.0.1:8080**: buttons that add jobs
- **http://127.0.0.1:8080/jobs**: the dashboard, to review them

Stop it with Ctrl-C. Running jobs are cancelled and go back to their queue, then pick up again on the next start.

| Setting | Default |
|---|---|
| `DATABASE_URL` | `postgres://jobkeep:jobkeep@localhost:55433/jobkeep_demo` |
| `BIND` | `127.0.0.1:8080` |
| `RUST_LOG` | `info,sqlx=warn` |

When you're done, `docker compose -f examples/demo/docker-compose.yml down` stops the database. Add `-v` to delete its data too.

## What to try

| Button | What you'll see in the dashboard |
|---|---|
| Place an order | `process_order` runs and sometimes times out and retries. Its receipt email waits in **awaiting**, then runs once the charge succeeds. |
| Send an email | Fails about a third of the time, so watch `attempt` go up and `last_error` fill in before it **succeeds**. |
| Schedule an email in 30s | Sits in **scheduled**, then runs when it's due. |
| Generate a slow report | **processing** for 15s. Delete it from the dashboard and it is cancelled at its next heartbeat. |
| Import an empty CSV | Goes straight to **failed** with no retries (a permanent error). Click **Requeue** in its details to run it again. |
| Unique import | Click it twice. The dedupe key means only one job is stored. |
| Add 20 mixed jobs | Fills the dashboard so you can try the state cards, filters and paging. |

A recurring `cleanup_sessions` job also runs every 30 seconds.

You can also inspect the same database with the CLI:

```sh
export DATABASE_URL=postgres://jobkeep:jobkeep@localhost:55433/jobkeep_demo
cargo run -p jobkeep-cli -- stats
cargo run -p jobkeep-cli -- list --state failed
```

## Code

- [`src/jobs.rs`](src/jobs.rs): the jobs, written with `#[job]` and `#[recurring]`
- [`src/main.rs`](src/main.rs): connects to Postgres, starts the server, and serves the page and dashboard with axum
- `cargo test -p jobkeep-demo` checks every button on the in-memory backend, with no database needed
