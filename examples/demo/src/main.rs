//! JobKeep demo: one process that runs a job server, a page with buttons
//! for adding sample jobs, and the dashboard for reviewing them.
//!
//! ```sh
//! docker compose -f examples/demo/docker-compose.yml up -d
//! cargo run -p jobkeep-demo
//! # open http://127.0.0.1:8080
//! ```
//!
//! Settings come from the environment:
//! - `DATABASE_URL` (default: the docker-compose database)
//! - `BIND` (default: `127.0.0.1:8080`)

mod jobs;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use chrono::TimeDelta;
use jobkeep_client::Client;
use jobkeep_core::Storage;
use jobkeep_postgres::PgStorage;
use jobkeep_server::Server;

use jobs::{CleanupSessions, GenerateReport, ImportCsv, ProcessOrder, SendEmail};

const DEFAULT_DATABASE_URL: &str = "postgres://jobkeep:jobkeep@localhost:55433/jobkeep_demo";
const HOME_HTML: &str = include_str!("home.html");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());
    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse()
        .context("BIND must be an address like 127.0.0.1:8080")?;

    let storage = PgStorage::connect(&database_url).await.with_context(|| {
        format!(
            "cannot connect to {database_url}; start the database with \
             `docker compose -f examples/demo/docker-compose.yml up -d` or set DATABASE_URL"
        )
    })?;
    storage.migrate().await.context("running migrations")?;

    // The worker: runs every job type on these queues.
    let server = Server::builder(storage.clone())
        .register::<SendEmail>()
        .register::<ProcessOrder>()
        .register::<GenerateReport>()
        .register::<ImportCsv>()
        .recurring::<CleanupSessions>()
        .queues(["default", "emails", "payments", "maintenance"])
        .concurrency(5)
        .retry(jobkeep_server::RetryPolicy::exponential(
            TimeDelta::seconds(2),
            TimeDelta::seconds(20),
        ))
        .shutdown_timeout(Duration::from_secs(10))
        .spawn();

    let app = app(Client::new(storage.clone()), storage);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    println!();
    println!("  JobKeep demo is running");
    println!("  add jobs:   http://{bind}/");
    println!("  dashboard:  http://{bind}/jobs");
    println!("  stop:       Ctrl-C");
    println!();

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;

    println!("shutting down; unfinished jobs go back to their queue");
    server.shutdown().await;
    Ok(())
}

/// The web app: the "add jobs" page at `/` and the dashboard at `/jobs`.
fn app<S: Storage>(client: Client<S>, storage: S) -> Router {
    Router::new()
        .route("/", get(|| async { Html(HOME_HTML) }))
        .route("/add/{kind}", post(add::<S>))
        .with_state(client)
        .nest("/jobs", jobkeep_dashboard::router(storage))
}

/// Next order number, so orders are easy to tell apart in the dashboard.
fn next_order_id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(1000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Enqueues the sample job(s) named by `kind`, then returns to the page.
async fn add<S: Storage>(State(client): State<Client<S>>, Path(kind): Path<String>) -> Response {
    match enqueue(&client, &kind).await {
        Ok(Some(n)) => Redirect::to(&format!("/?added={kind}&n={n}")).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, format!("unknown job kind {kind:?}")).into_response(),
        Err(e) => {
            tracing::error!("enqueueing {kind} failed: {e:#}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("enqueue failed: {e}"),
            )
                .into_response()
        }
    }
}

/// Returns how many jobs were enqueued, or `None` for an unknown kind.
async fn enqueue<S: Storage>(client: &Client<S>, kind: &str) -> anyhow::Result<Option<usize>> {
    let n = match kind {
        "order" => {
            let order_id = next_order_id();
            let charge = client
                .enqueue(&ProcessOrder {
                    order_id,
                    amount_cents: 4_999,
                })
                .await?;
            client
                .enqueue_after(
                    charge,
                    &SendEmail {
                        to: "customer@example.com".into(),
                        subject: format!("Receipt for order #{order_id}"),
                    },
                )
                .await?;
            2
        }
        "email" => {
            client
                .enqueue(&SendEmail {
                    to: "ada@example.com".into(),
                    subject: "Welcome to JobKeep".into(),
                })
                .await?;
            1
        }
        "scheduled" => {
            client
                .schedule_in(
                    &SendEmail {
                        to: "ada@example.com".into(),
                        subject: "Your 30-second reminder".into(),
                    },
                    TimeDelta::seconds(30),
                )
                .await?;
            1
        }
        "report" => {
            client
                .enqueue(&GenerateReport {
                    name: "Monthly revenue".into(),
                })
                .await?;
            1
        }
        "failing" => {
            client
                .enqueue(&ImportCsv {
                    file: "customers-empty.csv".into(),
                    rows: 0,
                })
                .await?;
            1
        }
        "unique" => {
            // Only the first click stores a job; later clicks find it.
            client
                .enqueue_unique(
                    "import:customers.csv",
                    &ImportCsv {
                        file: "customers.csv".into(),
                        rows: 1_200,
                    },
                )
                .await?;
            1
        }
        "batch" => {
            for i in 0..20u32 {
                match i % 4 {
                    0 => {
                        client
                            .enqueue(&ProcessOrder {
                                order_id: next_order_id(),
                                amount_cents: 1_000 + u64::from(i) * 100,
                            })
                            .await?;
                    }
                    1 => {
                        client
                            .enqueue(&SendEmail {
                                to: format!("user{i}@example.com"),
                                subject: "Weekly digest".into(),
                            })
                            .await?;
                    }
                    2 => {
                        client
                            .enqueue(&ImportCsv {
                                file: format!("batch-{i}.csv"),
                                rows: i * 10,
                            })
                            .await?;
                    }
                    _ => {
                        client
                            .schedule_in(
                                &SendEmail {
                                    to: format!("user{i}@example.com"),
                                    subject: "Follow-up".into(),
                                },
                                TimeDelta::seconds(i64::from(i)),
                            )
                            .await?;
                    }
                }
            }
            20
        }
        _ => return Ok(None),
    };
    Ok(Some(n))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use jobkeep_core::{JobFilter, State as JobState};
    use jobkeep_memory::MemoryStorage;
    use tower::ServiceExt;

    use super::*;

    fn demo() -> (Router, MemoryStorage) {
        let storage = MemoryStorage::new();
        (app(Client::new(storage.clone()), storage.clone()), storage)
    }

    async fn post(app: &Router, uri: &str) -> StatusCode {
        let req = Request::post(uri).body(Body::empty()).unwrap();
        app.clone().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn serves_home_and_dashboard() {
        let (app, _) = demo();
        for uri in ["/", "/jobs", "/jobs/api/stats"] {
            let res = app
                .clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK, "{uri}");
        }
    }

    #[tokio::test]
    async fn every_button_enqueues_jobs() {
        let (app, storage) = demo();
        for kind in [
            "order",
            "email",
            "scheduled",
            "report",
            "failing",
            "unique",
            "batch",
        ] {
            assert_eq!(
                post(&app, &format!("/add/{kind}")).await,
                StatusCode::SEE_OTHER,
                "{kind}"
            );
        }
        // order (2) + email + scheduled + report + failing + unique + batch (20)
        assert_eq!(storage.len(), 27);

        let receipts = storage
            .list(&JobFilter::new().state(JobState::Awaiting))
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1, "the receipt waits for its order");
        assert_eq!(receipts[0].job_type, "send_email");
    }

    #[tokio::test]
    async fn unique_import_is_stored_once() {
        let (app, storage) = demo();
        post(&app, "/add/unique").await;
        post(&app, "/add/unique").await;
        assert_eq!(storage.len(), 1);
    }

    #[tokio::test]
    async fn unknown_kind_is_404() {
        let (app, storage) = demo();
        assert_eq!(post(&app, "/add/nope").await, StatusCode::NOT_FOUND);
        assert!(storage.is_empty());
    }
}
