//! `#[job]` and `#[recurring]` from the user's side.

use jobkeep_core::{
    CancellationToken, Job, JobContext, JobError, JobId, NewJob, Recurring, Registry, job,
    recurring,
};

/// Delivers an email.
#[job(queue = "emails", max_attempts = 3)]
pub async fn send_email(ctx: &JobContext, to: String, cc: Vec<String>) -> Result<(), JobError> {
    if ctx.is_cancelled() {
        return Err(JobError::Cancelled);
    }
    if to.is_empty() {
        return Err(JobError::permanent("no recipient"));
    }
    let _ = cc;
    Ok(())
}

#[job(name = "billing.charge")]
async fn charge(cents: u64) -> anyhow::Result<()> {
    anyhow::ensure!(cents > 0, "nothing to charge");
    Ok(())
}

#[recurring("0 0 3 * * *", queue = "maintenance")]
async fn nightly_cleanup(_ctx: &JobContext) -> Result<(), JobError> {
    Ok(())
}

fn ctx() -> JobContext {
    JobContext::new(JobId::new(), 1, CancellationToken::new())
}

#[test]
fn generates_job_impl_and_payload() {
    assert_eq!(SendEmail::NAME, "send_email");
    assert_eq!(SendEmail::queue().as_str(), "emails");
    assert_eq!(SendEmail::max_attempts(), 3);
    assert_eq!(Charge::NAME, "billing.charge");
    assert_eq!(Charge::queue().as_str(), "default");
    assert_eq!(Charge::max_attempts(), 10);

    let job = NewJob::from_job(&SendEmail {
        to: "ada@example.com".into(),
        cc: vec!["bob@example.com".into()],
    })
    .unwrap();
    assert_eq!(job.job_type, "send_email");
    assert_eq!(job.queue.as_str(), "emails");
    assert_eq!(
        job.payload,
        serde_json::json!({ "to": "ada@example.com", "cc": ["bob@example.com"] })
    );
}

#[tokio::test]
async fn generated_jobs_run_the_function() {
    let ok = SendEmail {
        to: "ada@example.com".into(),
        cc: vec![],
    };
    assert!(ok.run(&ctx()).await.is_ok());

    let bad = SendEmail {
        to: String::new(),
        cc: vec![],
    };
    assert!(matches!(bad.run(&ctx()).await, Err(JobError::Permanent(_))));

    // anyhow errors become retryable.
    let err = Charge { cents: 0 }.run(&ctx()).await.unwrap_err();
    assert!(err.is_retryable());
    assert_eq!(err.to_string(), "retryable error: nothing to charge");

    // The function is still callable directly.
    assert!(charge(5).await.is_ok());
}

#[tokio::test]
async fn generated_jobs_register_and_run_by_name() {
    let mut registry = Registry::new();
    registry
        .register::<SendEmail>()
        .unwrap()
        .register::<Charge>()
        .unwrap()
        .register::<NightlyCleanup>()
        .unwrap();

    let stored = NewJob::from_job(&Charge { cents: 250 }).unwrap();
    registry
        .run(&stored.job_type, stored.payload, ctx())
        .await
        .unwrap();
}

#[test]
fn recurring_sets_schedule() {
    assert_eq!(NightlyCleanup::SCHEDULE, "0 0 3 * * *");
    assert_eq!(NightlyCleanup::NAME, "nightly_cleanup");
    assert_eq!(NightlyCleanup::queue().as_str(), "maintenance");
    let payload = NewJob::from_job(&NightlyCleanup::default())
        .unwrap()
        .payload;
    assert_eq!(payload, serde_json::json!({}));
}
