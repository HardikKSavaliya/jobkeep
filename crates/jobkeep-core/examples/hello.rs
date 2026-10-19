//! Scratch check of the `Job` API: a hand-written job (no macro), a JSON
//! round trip, and a direct call to `run`.
//!
//! Run with `cargo run -p jobkeep-core --example hello`.

use anyhow::Context;
use jobkeep_core::{CancellationToken, Job, JobContext, JobError, JobId, NewJob, Queue};
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

    // What a worker would do after fetching the row: look up the type by
    // name, deserialize the payload and run it.
    assert_eq!(new_job.job_type, SendEmail::NAME);
    let fetched: SendEmail = serde_json::from_value(new_job.payload)?;

    let ctx = JobContext::new(JobId::new(), 1, CancellationToken::new());
    fetched.run(&ctx).await?;

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
    match fetched.run(&ctx).await {
        Err(e) => println!("after cancel -> {e}"),
        Ok(()) => unreachable!(),
    }

    Ok(())
}
