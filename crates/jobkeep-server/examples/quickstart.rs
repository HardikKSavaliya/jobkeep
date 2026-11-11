//! JobKeep end to end on the in-memory backend: define jobs with the
//! macros, enqueue them with the client, and run them with a server.
//!
//! Run with `cargo run -p jobkeep-server --example quickstart`.
//! Swap `MemoryStorage` for `jobkeep_postgres::PgStorage` to persist jobs.

use std::time::Duration;

use chrono::TimeDelta;
use jobkeep_client::Client;
use jobkeep_core::{JobContext, JobError, State, job, recurring};
use jobkeep_memory::MemoryStorage;
use jobkeep_server::{RetryPolicy, Server};

/// Fails on its first attempt to show retries.
#[job(queue = "emails", max_attempts = 3)]
async fn send_welcome_email(ctx: &JobContext, to: String) -> anyhow::Result<()> {
    anyhow::ensure!(ctx.attempt > 1, "smtp server busy");
    println!("  sent welcome email to {to} (attempt {})", ctx.attempt);
    Ok(())
}

#[job]
async fn track_signup(user_id: u64) -> Result<(), JobError> {
    println!("  recorded signup of user {user_id}");
    Ok(())
}

#[recurring("*/2 * * * * *")]
async fn heartbeat() -> Result<(), JobError> {
    println!("  recurring heartbeat");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("jobkeep_server=warn")
        .init();

    let storage = MemoryStorage::new();
    let client = Client::new(storage.clone());
    let server = Server::builder(storage)
        .register::<SendWelcomeEmail>()
        .register::<TrackSignup>()
        .recurring::<Heartbeat>()
        .queues(["default", "emails"])
        .poll_interval(Duration::from_millis(50))
        .retry(RetryPolicy::fixed(TimeDelta::milliseconds(200)))
        .spawn();

    println!("enqueueing jobs");
    let email = client
        .enqueue(&SendWelcomeEmail {
            to: "ada@example.com".into(),
        })
        .await?;
    // Runs only after the email succeeds.
    client
        .enqueue_after(email, &TrackSignup { user_id: 42 })
        .await?;
    client
        .schedule_in(&TrackSignup { user_id: 7 }, TimeDelta::seconds(1))
        .await?;

    tokio::time::sleep(Duration::from_secs(3)).await;
    server.shutdown().await;

    let stats = client.stats().await?;
    println!("done:");
    for state in State::ALL {
        let n = stats.count(state);
        if n > 0 {
            println!("  {state:<10} {n}");
        }
    }
    Ok(())
}
