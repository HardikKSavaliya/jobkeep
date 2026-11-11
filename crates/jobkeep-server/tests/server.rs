//! End-to-end server behaviour on the memory backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use jobkeep_client::Client;
use jobkeep_core::{Job, JobContext, JobError, JobId, NewJob, Recurring, State, Storage};
use jobkeep_memory::MemoryStorage;
use jobkeep_server::{RetryPolicy, Server, ServerBuilder};
use serde::{Deserialize, Serialize};

/// Shared counters jobs report into, passed as an extension.
#[derive(Default)]
struct Probe {
    runs: AtomicU32,
    running: AtomicU32,
    max_running: AtomicU32,
    saw_cancel: AtomicBool,
}

fn probe(ctx: &JobContext) -> &Probe {
    ctx.extensions
        .get::<Arc<Probe>>()
        .expect("probe registered")
}

#[derive(Serialize, Deserialize)]
struct Succeed;

impl Job for Succeed {
    const NAME: &'static str = "succeed";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Fails with a retryable error until attempt `succeed_on`.
#[derive(Serialize, Deserialize)]
struct Flaky {
    succeed_on: u32,
}

impl Job for Flaky {
    const NAME: &'static str = "flaky";

    fn max_attempts() -> u32 {
        3
    }

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).runs.fetch_add(1, Ordering::SeqCst);
        if ctx.attempt < self.succeed_on {
            Err(anyhow::anyhow!("attempt {} failed", ctx.attempt))?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Permanent;

impl Job for Permanent {
    const NAME: &'static str = "permanent";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).runs.fetch_add(1, Ordering::SeqCst);
        Err(JobError::permanent("bad input"))
    }
}

#[derive(Serialize, Deserialize)]
struct Panics;

impl Job for Panics {
    const NAME: &'static str = "panics";

    fn max_attempts() -> u32 {
        2
    }

    async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
        panic!("kaboom");
    }
}

/// Sleeps, tracking how many run at once.
#[derive(Serialize, Deserialize)]
struct Slow {
    millis: u64,
}

impl Job for Slow {
    const NAME: &'static str = "slow";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        let p = probe(ctx);
        let now = p.running.fetch_add(1, Ordering::SeqCst) + 1;
        p.max_running.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        p.running.fetch_sub(1, Ordering::SeqCst);
        p.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Runs until cancelled, then reports `Cancelled`.
#[derive(Serialize, Deserialize)]
struct UntilCancelled;

impl Job for UntilCancelled {
    const NAME: &'static str = "until_cancelled";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).running.fetch_add(1, Ordering::SeqCst);
        ctx.cancellation.cancelled().await;
        probe(ctx).saw_cancel.store(true, Ordering::SeqCst);
        Err(JobError::Cancelled)
    }
}

/// Ignores cancellation entirely.
#[derive(Serialize, Deserialize)]
struct Stuck;

impl Job for Stuck {
    const NAME: &'static str = "stuck";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).running.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
struct EverySecond;

impl Job for EverySecond {
    const NAME: &'static str = "every_second";

    async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
        probe(ctx).runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Recurring for EverySecond {
    const SCHEDULE: &'static str = "* * * * * *";
}

struct Harness {
    storage: MemoryStorage,
    client: Client<MemoryStorage>,
    probe: Arc<Probe>,
}

impl Harness {
    fn new() -> Self {
        let storage = MemoryStorage::new();
        Self {
            client: Client::new(storage.clone()),
            storage,
            probe: Arc::default(),
        }
    }

    /// A fast-ticking server with every test job registered.
    fn server(&self) -> ServerBuilder<MemoryStorage> {
        Server::builder(self.storage.clone())
            .register::<Succeed>()
            .register::<Flaky>()
            .register::<Permanent>()
            .register::<Panics>()
            .register::<Slow>()
            .register::<UntilCancelled>()
            .register::<Stuck>()
            .extension(Arc::clone(&self.probe))
            .poll_interval(Duration::from_millis(5))
            .maintenance_interval(Duration::from_millis(5))
            .retry(RetryPolicy::fixed(TimeDelta::milliseconds(1)))
            .lease(TimeDelta::milliseconds(300))
    }

    async fn state(&self, id: JobId) -> State {
        self.storage.get(id).await.unwrap().unwrap().state
    }

    async fn wait_for(&self, id: JobId, state: State) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let now = self.state(id).await;
            if now == state {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "job {id} stuck in {now}, expected {state}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn wait_until(&self, what: &str, f: impl Fn(&Probe) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !f(&self.probe) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_job_to_success() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&Succeed).await.unwrap();

    h.wait_for(id, State::Succeeded).await;
    assert_eq!(h.probe.runs.load(Ordering::SeqCst), 1);
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 1);
    assert_eq!(rec.worker, None);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_until_success() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&Flaky { succeed_on: 3 }).await.unwrap();

    h.wait_for(id, State::Succeeded).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 3);
    assert_eq!(
        rec.last_error.as_deref(),
        Some("retryable error: attempt 2 failed")
    );
    assert_eq!(h.probe.runs.load(Ordering::SeqCst), 3);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fails_after_last_attempt() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&Flaky { succeed_on: 99 }).await.unwrap();

    h.wait_for(id, State::Failed).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 3);
    assert_eq!(
        rec.last_error.as_deref(),
        Some("retryable error: attempt 3 failed")
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn permanent_error_is_not_retried() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&Permanent).await.unwrap();

    h.wait_for(id, State::Failed).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 1);
    assert_eq!(
        rec.last_error.as_deref(),
        Some("permanent error: bad input")
    );
    assert_eq!(h.probe.runs.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn panic_is_caught_and_retried() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&Panics).await.unwrap();

    h.wait_for(id, State::Failed).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 2);
    assert_eq!(rec.last_error.as_deref(), Some("job panicked: kaboom"));

    // The server survived and keeps working.
    let ok = h.client.enqueue(&Succeed).await.unwrap();
    h.wait_for(ok, State::Succeeded).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_job_type_fails() {
    let h = Harness::new();
    let server = h.server().spawn();
    let mut job = NewJob::from_job(&Succeed).unwrap();
    job.job_type = "not_registered".into();
    let id = h.client.enqueue_new(job).await.unwrap();

    h.wait_for(id, State::Failed).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert!(rec.last_error.unwrap().contains("no handler registered"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn respects_concurrency_limit() {
    let h = Harness::new();
    let server = h.server().concurrency(2).spawn();
    let mut ids = Vec::new();
    for _ in 0..6 {
        ids.push(h.client.enqueue(&Slow { millis: 40 }).await.unwrap());
    }
    for id in ids {
        h.wait_for(id, State::Succeeded).await;
    }
    assert_eq!(h.probe.max_running.load(Ordering::SeqCst), 2);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn only_fetches_its_queues() {
    let h = Harness::new();
    let server = h.server().queues(["emails"]).spawn();
    let other = h.client.enqueue(&Succeed).await.unwrap();
    let mine = h
        .client
        .enqueue_new(NewJob::from_job(&Succeed).unwrap().with_queue("emails"))
        .await
        .unwrap();

    h.wait_for(mine, State::Succeeded).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.state(other).await, State::Enqueued);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn scheduled_job_runs_when_due() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h
        .client
        .schedule_in(&Succeed, TimeDelta::milliseconds(150))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.state(id).await, State::Scheduled);
    h.wait_for(id, State::Succeeded).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert!(rec.updated_at >= rec.run_at);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn continuation_runs_after_parent() {
    let h = Harness::new();
    let parent = h.client.enqueue(&Flaky { succeed_on: 2 }).await.unwrap();
    let child = h.client.enqueue_after(parent, &Succeed).await.unwrap();
    let doomed_parent = h.client.enqueue(&Permanent).await.unwrap();
    let doomed = h
        .client
        .enqueue_after(doomed_parent, &Succeed)
        .await
        .unwrap();

    let server = h.server().spawn();
    h.wait_for(child, State::Succeeded).await;
    h.wait_for(doomed, State::Deleted).await;
    let parent = h.storage.get(parent).await.unwrap().unwrap();
    let child = h.storage.get(child).await.unwrap().unwrap();
    assert!(child.updated_at >= parent.updated_at);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_requeues_cooperative_job() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&UntilCancelled).await.unwrap();
    h.wait_until("job to start", |p| p.running.load(Ordering::SeqCst) == 1)
        .await;

    server.shutdown().await;
    assert!(h.probe.saw_cancel.load(Ordering::SeqCst));
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Enqueued);
    assert_eq!(rec.attempt, 0, "a shutdown does not use up an attempt");
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_timeout_aborts_stuck_job() {
    let h = Harness::new();
    let server = h
        .server()
        .shutdown_timeout(Duration::from_millis(50))
        .spawn();
    let id = h.client.enqueue(&Stuck).await.unwrap();
    h.wait_until("job to start", |p| p.running.load(Ordering::SeqCst) == 1)
        .await;

    let started = tokio::time::Instant::now();
    server.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(h.state(id).await, State::Enqueued);
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_running_job_cancels_it() {
    let h = Harness::new();
    let server = h.server().spawn();
    let id = h.client.enqueue(&UntilCancelled).await.unwrap();
    h.wait_until("job to start", |p| p.running.load(Ordering::SeqCst) == 1)
        .await;

    h.client.delete(id).await.unwrap();
    // Noticed at the next heartbeat (every lease / 3 = 100ms).
    h.wait_until("job to be cancelled", |p| {
        p.saw_cancel.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(h.state(id).await, State::Deleted);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeats_keep_long_job_alive() {
    let h = Harness::new();
    let server = h.server().spawn();
    // Runs for over three times the 300ms lease.
    let id = h.client.enqueue(&Slow { millis: 1_000 }).await.unwrap();

    h.wait_for(id, State::Succeeded).await;
    let rec = h.storage.get(id).await.unwrap().unwrap();
    assert_eq!(rec.attempt, 1, "the lease never expired");
    assert_eq!(h.probe.runs.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crashed_worker_job_is_taken_back() {
    let h = Harness::new();
    let id = h.client.enqueue(&Succeed).await.unwrap();
    // A worker that claimed the job and died without a heartbeat.
    let dead = jobkeep_core::WorkerId::new();
    h.storage
        .fetch(&[Default::default()], dead, TimeDelta::milliseconds(-1))
        .await
        .unwrap()
        .unwrap();

    let server = h.server().spawn();
    h.wait_for(id, State::Succeeded).await;
    assert_eq!(h.storage.get(id).await.unwrap().unwrap().attempt, 2);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recurring_job_runs_once_per_tick_across_servers() {
    let h = Harness::new();
    let a = h.server().recurring::<EverySecond>().spawn();
    let b = h.server().recurring::<EverySecond>().spawn();

    h.wait_until("two recurring runs", |p| p.runs.load(Ordering::SeqCst) >= 2)
        .await;
    a.shutdown().await;
    b.shutdown().await;

    let jobs = h
        .storage
        .list(&jobkeep_core::JobFilter::new().job_type("every_second"))
        .await
        .unwrap();
    let mut keys: Vec<_> = jobs.iter().map(|j| j.unique_key.clone().unwrap()).collect();
    let total = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), total, "a tick was enqueued twice");
    assert!(total >= 2);
    assert!(jobs.iter().all(|j| j.run_at <= Utc::now()));
}

#[tokio::test(flavor = "multi_thread")]
async fn purges_old_finished_jobs() {
    let h = Harness::new();
    let id = h.client.enqueue(&Succeed).await.unwrap();
    let server = h
        .server()
        .retention(Some(TimeDelta::zero()))
        .purge_interval(Duration::from_millis(5))
        .spawn();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while h.storage.get(id).await.unwrap().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job was never purged"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(h.probe.runs.load(Ordering::SeqCst), 1);
    server.shutdown().await;
}

#[test]
#[should_panic(expected = "already registered")]
fn duplicate_registration_panics() {
    let _ = Server::builder(MemoryStorage::new())
        .register::<Succeed>()
        .register::<Succeed>();
}

#[test]
fn bad_schedule_is_an_error() {
    let err = Server::builder(MemoryStorage::new())
        .recurring_job("nightly", "not a cron", Succeed)
        .unwrap_err();
    assert!(err.to_string().contains("nightly"), "{err}");
}
