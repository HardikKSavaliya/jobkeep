//! Scratch check of the `Job` API: a hand-written job (no macro), a JSON
//! round trip, and running it by name through the `Registry`.
//!
//! Run with `cargo run -p jobkeep-core --example hello`.

use anyhow::Context;
use jobkeep_core::{CancellationToken, Job, JobContext, JobError, JobId, NewJob, Queue, Registry};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct SendEmail {
    to: String,
    subject: String,
}

impl Job for SendEmail {
    const NAME: &'static str = "send_email";

    fn queue() -> Queue {
        Queue::from("emails")
    }

    fn max_attempts() -> u32 {
        3
    }

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        if ctx.is_cancelled() {
            return Err(JobError::Cancelled);
        }
        if !self.to.contains('@') {
            return Err(JobError::permanent(format!("invalid address: {}", self.to)));
        }
        // A real job would talk to an SMTP server here; `?` on an anyhow
        // error turns any failure into a retryable one.
        deliver(self).context("delivering email")?;
        println!(
            "[job {} attempt {}] sent {:?} to {}",
            ctx.job_id, ctx.attempt, self.subject, self.to
        );
        Ok(())
    }
}

fn deliver(_email: &SendEmail) -> std::io::Result<()> {
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let job = SendEmail {
        to: "ada@example.com".into(),
        subject: "Hello from JobKeep".into(),
    };

    // What the client would hand to storage.
    let new_job = NewJob::from_job(&job)?;
    println!(
        "stored: type={} queue={} max_attempts={} payload={}",
        new_job.job_type, new_job.queue, new_job.max_attempts, new_job.payload
    );

    // What a worker does after fetching the row: the registry looks up the
    // type by name, deserializes the payload and runs it.
    let mut registry = Registry::new();
    registry.register::<SendEmail>()?;

    let ctx = || JobContext::new(JobId::new(), 1, CancellationToken::new());
    registry
        .run(&new_job.job_type, new_job.payload.clone(), ctx())
        .await?;

    let unknown = registry.run("resize_image", new_job.payload, ctx()).await;
    println!("unknown type -> {}", unknown.unwrap_err());

    // Direct calls, for the failure paths.
    let ctx = ctx();

    // Failure paths.
    let bad = SendEmail {
        to: "nobody".into(),
        subject: "oops".into(),
    };
    match bad.run(&ctx).await {
        Err(e) => println!("bad address -> {e} (retryable: {})", e.is_retryable()),
        Ok(()) => unreachable!(),
    }

    ctx.cancellation.cancel();
    match job.run(&ctx).await {
        Err(e) => println!("after cancel -> {e}"),
        Ok(()) => unreachable!(),
    }

    Ok(())
}
