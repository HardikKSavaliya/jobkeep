//! The demo's jobs. Each one shows a different part of JobKeep; the delays
//! and random failures are there so you can watch jobs move through states
//! in the dashboard.

use std::time::Duration;

use jobkeep_core::{JobContext, JobError, job, recurring};

/// Returns true with probability `percent / 100`.
fn chance(percent: u64) -> bool {
    uuid::Uuid::new_v4().as_u64_pair().0 % 100 < percent
}

/// Sleeps, but stops early (with `Cancelled`) if the job is cancelled:
/// deleted from the dashboard, or the server is shutting down.
async fn work(ctx: &JobContext, duration: Duration) -> Result<(), JobError> {
    tokio::select! {
        () = tokio::time::sleep(duration) => Ok(()),
        () = ctx.cancellation.cancelled() => Err(JobError::Cancelled),
    }
}

/// Fails about a third of the time with a retryable error, so most emails
/// show a retry or two in the dashboard before succeeding.
#[job(queue = "emails", max_attempts = 4)]
pub async fn send_email(ctx: &JobContext, to: String, subject: String) -> anyhow::Result<()> {
    work(ctx, Duration::from_millis(400)).await?;
    anyhow::ensure!(!chance(35), "SMTP server busy, try again later");
    tracing::info!(%to, %subject, attempt = ctx.attempt, "email sent");
    Ok(())
}

/// Charges an order. Enqueued with a receipt email as its continuation, so
/// the receipt waits in "awaiting" until the charge succeeds.
#[job(queue = "payments", max_attempts = 3)]
pub async fn process_order(
    ctx: &JobContext,
    order_id: u32,
    amount_cents: u64,
) -> anyhow::Result<()> {
    work(ctx, Duration::from_millis(800)).await?;
    anyhow::ensure!(ctx.attempt > 1 || !chance(30), "payment gateway timeout");
    tracing::info!(order_id, amount_cents, "order charged");
    Ok(())
}

/// Runs for a while. Delete it from the dashboard while it's processing and
/// it stops at its next heartbeat.
#[job]
pub async fn generate_report(ctx: &JobContext, name: String) -> Result<(), JobError> {
    work(ctx, Duration::from_secs(15)).await?;
    tracing::info!(%name, "report generated");
    Ok(())
}

/// A permanent failure: goes straight to "failed" with no retries. Requeue it
/// from the dashboard to see it run (and fail) again.
#[job]
pub async fn import_csv(ctx: &JobContext, file: String, rows: u32) -> Result<(), JobError> {
    work(ctx, Duration::from_millis(300)).await?;
    if rows == 0 {
        return Err(JobError::permanent(format!("{file} has no rows")));
    }
    tracing::info!(%file, rows, "import finished");
    Ok(())
}

/// Runs every 30 seconds on whichever server notices the tick first.
#[recurring("*/30 * * * * *", queue = "maintenance")]
pub async fn cleanup_sessions(ctx: &JobContext) -> Result<(), JobError> {
    work(ctx, Duration::from_millis(200)).await?;
    tracing::info!("expired sessions cleaned up");
    Ok(())
}
