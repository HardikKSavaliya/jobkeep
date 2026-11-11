//! Full lifecycle on a real Postgres: jobs written with the macros, enqueued
//! through the client, run by two servers sharing one database, and managed
//! through the dashboard API.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::TimeDelta;
use jobkeep_client::Client;
use jobkeep_core::{
    JobContext, JobError, JobFilter, JobId, Queue, State, Storage, WorkerId, job, recurring,
};
use jobkeep_postgres::PgStorage;
use jobkeep_server::{RetryPolicy, Server, ServerBuilder, ServerHandle};
use tower::ServiceExt;

/// Counters the jobs report into, shared through an extension.
#[derive(Default)]
struct Probe {
    charges: AtomicU32,
    receipts: AtomicU32,
    reports: AtomicU32,
    ticks: AtomicU32,
    waiting: AtomicU32,
}

fn probe(ctx: &JobContext) -> &Probe {
    ctx.extensions
        .get::<Arc<Probe>>()
        .expect("probe registered")
}

/// Fails its first attempt with a retryable error, then succeeds.
#[job(queue = "payments", max_attempts = 3)]
async fn charge_card(ctx: &JobContext, order_id: u32, cents: u64) -> anyhow::Result<()> {
    probe(ctx).charges.fetch_add(1, Ordering::SeqCst);
    anyhow::ensure!(ctx.attempt > 1, "card network timeout for order {order_id}");
    anyhow::ensure!(cents > 0, "nothing to charge");
    Ok(())
}

#[job(queue = "emails")]
async fn send_receipt(ctx: &JobContext, order_id: u32) -> Result<(), JobError> {
    let _ = order_id;
    probe(ctx).receipts.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[job]
async fn import_batch(rows: i64) -> Result<(), JobError> {
    if rows < 0 {
        return Err(JobError::permanent(format!("negative row count {rows}")));
    }
    Ok(())
}

/// Runs for longer than the 1s lease, so it only survives if heartbeats work.
#[job]
async fn build_report(ctx: &JobContext) -> Result<(), JobError> {
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    probe(ctx).reports.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[job]
async fn wait_for_shutdown(ctx: &JobContext) -> Result<(), JobError> {
    probe(ctx).waiting.fetch_add(1, Ordering::SeqCst);
    ctx.cancellation.cancelled().await;
    Err(JobError::Cancelled)
}

#[recurring("* * * * * *")]
async fn tick(ctx: &JobContext) -> Result<(), JobError> {
    probe(ctx).ticks.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn server(storage: &PgStorage, probe: &Arc<Probe>) -> ServerBuilder<PgStorage> {
    Server::builder(storage.clone())
        .register::<ChargeCard>()
        .register::<SendReceipt>()
        .register::<ImportBatch>()
        .register::<BuildReport>()
        .register::<WaitForShutdown>()
        .recurring::<Tick>()
        .extension(Arc::clone(probe))
        .queues(["default", "payments", "emails"])
        .concurrency(4)
        .lease(TimeDelta::seconds(1))
        .poll_interval(Duration::from_millis(20))
        .maintenance_interval(Duration::from_millis(20))
        .retry(RetryPolicy::fixed(TimeDelta::milliseconds(10)))
}

async fn wait_for(storage: &PgStorage, id: JobId, state: State) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let now = storage.get(id).await.unwrap().expect("job exists").state;
        if now == state {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {id} stuck in {now}, expected {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn api(app: &axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_cycle() {
    let Some(storage) = common::storage().await else {
        return;
    };
    let client = Client::new(storage.clone());
    let probe = Arc::new(Probe::default());

    // A worker that claimed a job and died before the servers started.
    let orphan = client.enqueue(&ImportBatch { rows: 1 }).await.unwrap();
    storage
        .fetch(
            &[Queue::default()],
            WorkerId::new(),
            TimeDelta::milliseconds(-1),
        )
        .await
        .unwrap()
        .expect("orphan claimed");

    // Two servers share the database.
    let a = server(&storage, &probe).spawn();
    let b = server(&storage, &probe).spawn();

    // 20 orders, each charge followed by a receipt continuation.
    const ORDERS: u32 = 20;
    let mut charges = Vec::new();
    let mut receipts = Vec::new();
    for order_id in 0..ORDERS {
        let charge = client
            .enqueue(&ChargeCard {
                order_id,
                cents: 1_000 + u64::from(order_id),
            })
            .await
            .unwrap();
        let receipt = client
            .enqueue_after(charge, &SendReceipt { order_id })
            .await
            .unwrap();
        charges.push(charge);
        receipts.push(receipt);
    }

    // A permanent failure takes its continuation with it.
    let bad_import = client.enqueue(&ImportBatch { rows: -5 }).await.unwrap();
    let after_bad = client
        .enqueue_after(bad_import, &SendReceipt { order_id: 999 })
        .await
        .unwrap();

    // Scheduled, deduplicated, and long-running jobs.
    let scheduled = client
        .schedule_in(&ImportBatch { rows: 10 }, TimeDelta::milliseconds(500))
        .await
        .unwrap();
    let unique = client
        .enqueue_unique("import:2026-11-08", &ImportBatch { rows: 3 })
        .await
        .unwrap();
    let same = client
        .enqueue_unique("import:2026-11-08", &ImportBatch { rows: 3 })
        .await
        .unwrap();
    assert_eq!(unique, same);
    let report = client.enqueue(&BuildReport {}).await.unwrap();

    for (&charge, &receipt) in charges.iter().zip(&receipts) {
        wait_for(&storage, charge, State::Succeeded).await;
        wait_for(&storage, receipt, State::Succeeded).await;
    }
    wait_for(&storage, bad_import, State::Failed).await;
    wait_for(&storage, after_bad, State::Deleted).await;
    wait_for(&storage, scheduled, State::Succeeded).await;
    wait_for(&storage, unique, State::Succeeded).await;
    wait_for(&storage, orphan, State::Succeeded).await;
    wait_for(&storage, report, State::Succeeded).await;
    wait_until("two recurring ticks", || {
        probe.ticks.load(Ordering::SeqCst) >= 2
    })
    .await;

    // Every charge failed once and succeeded on its retry, exactly once each.
    for &charge in &charges {
        let job = storage.get(charge).await.unwrap().unwrap();
        assert_eq!(job.attempt, 2, "{charge}");
        assert!(job.last_error.unwrap().contains("card network timeout"));
    }
    assert_eq!(probe.charges.load(Ordering::SeqCst), 2 * ORDERS);
    assert_eq!(probe.receipts.load(Ordering::SeqCst), ORDERS);
    // The report outlived its lease three times over without being re-run.
    assert_eq!(probe.reports.load(Ordering::SeqCst), 1);
    assert_eq!(storage.get(report).await.unwrap().unwrap().attempt, 1);
    // The orphan was taken back from the dead worker.
    let orphan = storage.get(orphan).await.unwrap().unwrap();
    assert_eq!(orphan.attempt, 2);
    let bad = storage.get(bad_import).await.unwrap().unwrap();
    assert_eq!(bad.attempt, 1, "permanent errors are not retried");
    assert_eq!(
        bad.last_error.as_deref(),
        Some("permanent error: negative row count -5")
    );

    // Both servers saw every tick, but each tick was stored once.
    let ticks = client
        .list(&JobFilter::new().job_type("tick").limit(500))
        .await
        .unwrap();
    let mut keys: Vec<_> = ticks
        .iter()
        .map(|t| t.unique_key.clone().unwrap())
        .collect();
    let total = keys.len();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), total, "a recurring tick ran twice");

    // The dashboard sees it all, and can requeue the failed import.
    let app = jobkeep_dashboard::router(storage.clone());
    let (status, stats) = api(
        &app,
        Request::get("/api/stats").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats["counts"]["failed"], 1);
    assert_eq!(stats["counts"]["deleted"], 1);
    assert!(stats["counts"]["succeeded"].as_u64().unwrap() >= u64::from(2 * ORDERS) + 5);

    let (_, failed) = api(
        &app,
        Request::get("/api/jobs?state=failed")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(failed[0]["id"], bad_import.to_string());

    let (status, _) = api(
        &app,
        Request::post(format!("/api/jobs/{bad_import}/requeue"))
            .header(jobkeep_dashboard::ACTION_HEADER, "requeue")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // It runs again from attempt 1 and fails the same way.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let job = storage.get(bad_import).await.unwrap().unwrap();
        if job.state == State::Failed && job.attempt == 1 && job.updated_at > bad.updated_at {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "requeued job did not rerun"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    a.shutdown().await;
    b.shutdown().await;

    // Purging keeps failed jobs and drops finished ones.
    let purged = client
        .purge(chrono::Utc::now() + TimeDelta::seconds(5))
        .await
        .unwrap();
    assert!(purged >= u64::from(2 * ORDERS) + 5);
    let stats = client.stats().await.unwrap();
    assert_eq!(stats.count(State::Succeeded), 0);
    assert_eq!(stats.count(State::Deleted), 0);
    assert_eq!(stats.count(State::Failed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_and_deletion_on_postgres() {
    let Some(storage) = common::storage().await else {
        return;
    };
    let client = Client::new(storage.clone());
    let probe = Arc::new(Probe::default());

    let server: ServerHandle = server(&storage, &probe).spawn();
    let deleted = client.enqueue(&WaitForShutdown {}).await.unwrap();
    let interrupted = client.enqueue(&WaitForShutdown {}).await.unwrap();
    wait_until("both jobs to start", || {
        probe.waiting.load(Ordering::SeqCst) == 2
    })
    .await;
    wait_for(&storage, deleted, State::Processing).await;
    wait_for(&storage, interrupted, State::Processing).await;

    // Deleting a running job cancels it at the next heartbeat; the other
    // keeps running.
    client.delete(deleted).await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        storage.get(deleted).await.unwrap().unwrap().state,
        State::Deleted
    );
    assert_eq!(
        storage.get(interrupted).await.unwrap().unwrap().state,
        State::Processing
    );

    // Shutdown hands the running job back without using up an attempt.
    server.shutdown().await;
    let job = storage.get(interrupted).await.unwrap().unwrap();
    assert_eq!(job.state, State::Enqueued);
    assert_eq!(job.attempt, 0);
    assert_eq!(job.worker, None);
}
