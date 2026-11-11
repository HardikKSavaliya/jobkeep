//! A conformance suite every [`Storage`] backend must pass.
//!
//! Enable the `testing` feature in a backend's dev-dependencies and expand
//! [`storage_conformance_tests!`](crate::storage_conformance_tests) in a test
//! module, giving it an expression that builds a fresh, empty backend:
//!
//! ```ignore
//! mod conformance {
//!     use super::*;
//!     jobkeep_core::storage_conformance_tests!(MemoryStorage::new());
//! }
//! ```
//!
//! The expression is evaluated inside an `async fn`, so it may `.await`
//! (e.g. to create a fresh database). The expanding crate needs `tokio` as a
//! dev-dependency with the `macros` and `rt-multi-thread` features.
//!
//! The suite assumes the backend's clock tracks real time (it builds jobs
//! from `Utc::now()`), so run it against a system clock, not a frozen test
//! clock.
//!
//! No test sleeps: "due" and "expired" are produced with times in the past
//! and negative leases, so the suite is not timing-sensitive against a real
//! database.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{TimeDelta, Utc};

use crate::{
    JobFilter, JobId, NewJob, Outcome, Priority, Queue, State, Storage, StorageError, WorkerId,
    parent_failed_error,
};

/// Expands to one `#[tokio::test]` per conformance check, each run against
/// a backend built by `$make`. See the [module docs](crate::testing).
#[macro_export]
macro_rules! storage_conformance_tests {
    ($make:expr) => {
        $crate::__storage_conformance_tests!(
            $make;
            enqueue_then_get,
            future_run_at_is_scheduled,
            fetch_claims_job,
            fetch_respects_queues,
            fetch_orders_by_priority_then_run_at,
            complete_succeeded,
            complete_failed_records_error,
            retry_is_scheduled_until_promoted,
            requeue_outcome_keeps_attempt,
            other_worker_cannot_touch_lease,
            heartbeat_extends_lease,
            expired_lease_is_requeued,
            expired_lease_on_last_attempt_fails,
            delete_rules,
            delete_running_job_revokes_lease,
            manual_requeue_rules,
            missing_jobs,
            concurrent_fetch_never_double_claims,
            unique_key_dedupes,
            continuation_runs_after_parent_succeeds,
            continuations_deleted_when_parent_fails,
            continuations_deleted_with_parent,
            continuations_deleted_when_lease_runs_out,
            continuation_of_finished_parent,
            continuation_of_missing_parent,
            stats_count_by_state,
            list_filters_newest_first,
            purge_removes_old_finished_jobs,
        );
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __storage_conformance_tests {
    ($make:expr; $($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $name() {
                $crate::testing::$name($make).await;
            }
        )*
    };
}

fn lease() -> TimeDelta {
    TimeDelta::seconds(30)
}

/// A lease that is already over when granted.
fn expired_lease() -> TimeDelta {
    TimeDelta::seconds(-1)
}

fn new_job(queue: &str) -> NewJob {
    NewJob {
        job_type: "test".into(),
        payload: serde_json::json!({ "queue": queue }),
        queue: Queue::from(queue),
        priority: Priority::NORMAL,
        run_at: None,
        max_attempts: 3,
        parent: None,
        unique_key: None,
    }
}

fn default_queue() -> [Queue; 1] {
    [Queue::default()]
}

async fn fetch_one<S: Storage>(s: &S, worker: WorkerId) -> crate::FetchedJob {
    s.fetch(&default_queue(), worker, lease())
        .await
        .unwrap()
        .expect("a job should be ready")
}

async fn state_of<S: Storage>(s: &S, id: JobId) -> State {
    s.get(id).await.unwrap().expect("job exists").state
}

pub async fn enqueue_then_get<S: Storage>(s: S) {
    let job = new_job("default").with_priority(Priority::HIGH);
    let id = s.enqueue(job.clone()).await.unwrap();

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.id, id);
    assert_eq!(rec.job_type, job.job_type);
    assert_eq!(rec.payload, job.payload);
    assert_eq!(rec.queue, job.queue);
    assert_eq!(rec.priority, Priority::HIGH);
    assert_eq!(rec.state, State::Enqueued);
    assert_eq!(rec.attempt, 0);
    assert_eq!(rec.max_attempts, 3);
    assert_eq!(rec.last_error, None);
    assert_eq!(rec.worker, None);
    assert_eq!(rec.lease_expires_at, None);
}

pub async fn future_run_at_is_scheduled<S: Storage>(s: S) {
    let job = new_job("default").with_run_at(Utc::now() + TimeDelta::hours(1));
    let id = s.enqueue(job).await.unwrap();

    assert_eq!(state_of(&s, id).await, State::Scheduled);
    assert_eq!(s.promote_due().await.unwrap(), 0);
    let fetched = s.fetch(&default_queue(), WorkerId::new(), lease()).await;
    assert!(fetched.unwrap().is_none());
}

pub async fn fetch_claims_job<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();

    let before = Utc::now();
    let job = fetch_one(&s, worker).await;
    assert_eq!(job.id, id);
    assert_eq!(job.attempt, 1);
    assert_eq!(job.max_attempts, 3);
    assert!(job.lease_expires_at > before);

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Processing);
    assert_eq!(rec.attempt, 1);
    assert_eq!(rec.worker, Some(worker));
    assert!(rec.lease_expires_at.is_some());

    // Claimed jobs are not handed out again.
    let again = s.fetch(&default_queue(), WorkerId::new(), lease()).await;
    assert!(again.unwrap().is_none());
}

pub async fn fetch_respects_queues<S: Storage>(s: S) {
    let emails = s.enqueue(new_job("emails")).await.unwrap();
    let reports = s.enqueue(new_job("reports")).await.unwrap();
    let worker = WorkerId::new();

    let none = s.fetch(&default_queue(), worker, lease()).await.unwrap();
    assert!(none.is_none());

    let got = s.fetch(&[Queue::from("reports")], worker, lease()).await;
    assert_eq!(got.unwrap().unwrap().id, reports);

    let both = [Queue::from("reports"), Queue::from("emails")];
    let got = s.fetch(&both, worker, lease()).await;
    assert_eq!(got.unwrap().unwrap().id, emails);
}

pub async fn fetch_orders_by_priority_then_run_at<S: Storage>(s: S) {
    let now = Utc::now();
    let old_low = new_job("default")
        .with_priority(Priority::LOW)
        .with_run_at(now - TimeDelta::minutes(10));
    let newer_normal = new_job("default").with_run_at(now - TimeDelta::seconds(5));
    let older_normal = new_job("default").with_run_at(now - TimeDelta::seconds(10));
    let high = new_job("default").with_priority(Priority::HIGH);

    let old_low = s.enqueue(old_low).await.unwrap();
    let newer_normal = s.enqueue(newer_normal).await.unwrap();
    let older_normal = s.enqueue(older_normal).await.unwrap();
    let high = s.enqueue(high).await.unwrap();

    let worker = WorkerId::new();
    let mut order = Vec::new();
    for _ in 0..4 {
        order.push(fetch_one(&s, worker).await.id);
    }
    assert_eq!(order, [high, older_normal, newer_normal, old_low]);
}

pub async fn complete_succeeded<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;

    s.complete(id, worker, Outcome::Succeeded).await.unwrap();

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Succeeded);
    assert_eq!(rec.worker, None);
    assert_eq!(rec.lease_expires_at, None);

    // The lease is released, so a second report is rejected.
    let err = s
        .complete(id, worker, Outcome::Succeeded)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::LeaseLost(got) if got == id),
        "{err}"
    );
}

pub async fn complete_failed_records_error<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;

    let outcome = Outcome::Failed {
        error: "bad input".into(),
    };
    s.complete(id, worker, outcome).await.unwrap();

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Failed);
    assert_eq!(rec.last_error.as_deref(), Some("bad input"));
    assert_eq!(rec.worker, None);
}

pub async fn retry_is_scheduled_until_promoted<S: Storage>(s: S) {
    let due = s.enqueue(new_job("default")).await.unwrap();
    let later = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;
    fetch_one(&s, worker).await;

    let retry = |at| Outcome::Retry {
        at,
        error: "timeout".into(),
    };
    s.complete(due, worker, retry(Utc::now() - TimeDelta::seconds(1)))
        .await
        .unwrap();
    s.complete(later, worker, retry(Utc::now() + TimeDelta::hours(1)))
        .await
        .unwrap();

    for id in [due, later] {
        let rec = s.get(id).await.unwrap().unwrap();
        assert_eq!(rec.state, State::Scheduled);
        assert_eq!(rec.last_error.as_deref(), Some("timeout"));
        assert_eq!(rec.worker, None);
    }

    assert_eq!(s.promote_due().await.unwrap(), 1);
    assert_eq!(state_of(&s, due).await, State::Enqueued);
    assert_eq!(state_of(&s, later).await, State::Scheduled);

    let job = fetch_one(&s, worker).await;
    assert_eq!(job.id, due);
    assert_eq!(job.attempt, 2);
}

pub async fn requeue_outcome_keeps_attempt<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    assert_eq!(fetch_one(&s, worker).await.attempt, 1);

    s.complete(id, worker, Outcome::Requeue).await.unwrap();
    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Enqueued);
    assert_eq!(rec.attempt, 0);
    assert_eq!(rec.worker, None);

    assert_eq!(fetch_one(&s, worker).await.attempt, 1);
}

pub async fn other_worker_cannot_touch_lease<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    fetch_one(&s, WorkerId::new()).await;
    let intruder = WorkerId::new();

    let err = s.heartbeat(id, intruder, lease()).await.unwrap_err();
    assert!(matches!(err, StorageError::LeaseLost(_)), "{err}");
    let err = s
        .complete(id, intruder, Outcome::Succeeded)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::LeaseLost(_)), "{err}");

    assert_eq!(state_of(&s, id).await, State::Processing);
}

pub async fn heartbeat_extends_lease<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    s.fetch(&default_queue(), worker, expired_lease())
        .await
        .unwrap()
        .unwrap();

    // Not yet reclaimed, so the worker still holds it and may renew.
    let expires = s.heartbeat(id, worker, lease()).await.unwrap();
    assert!(expires > Utc::now());

    assert_eq!(s.requeue_expired().await.unwrap(), 0);
    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Processing);
    assert_eq!(rec.lease_expires_at, Some(expires));
}

pub async fn expired_lease_is_requeued<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let healthy = s.enqueue(new_job("default")).await.unwrap();
    let lost = WorkerId::new();
    s.fetch(&default_queue(), lost, expired_lease())
        .await
        .unwrap()
        .unwrap();
    fetch_one(&s, WorkerId::new()).await;

    assert_eq!(s.requeue_expired().await.unwrap(), 1);

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Enqueued);
    assert_eq!(rec.attempt, 1);
    assert_eq!(rec.worker, None);
    assert!(rec.last_error.is_some());
    assert_eq!(state_of(&s, healthy).await, State::Processing);

    // The old worker finds out on its next call.
    let err = s.heartbeat(id, lost, lease()).await.unwrap_err();
    assert!(matches!(err, StorageError::LeaseLost(_)), "{err}");

    assert_eq!(fetch_one(&s, WorkerId::new()).await.attempt, 2);
}

pub async fn expired_lease_on_last_attempt_fails<S: Storage>(s: S) {
    let id = s
        .enqueue(new_job("default").with_max_attempts(1))
        .await
        .unwrap();
    s.fetch(&default_queue(), WorkerId::new(), expired_lease())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(s.requeue_expired().await.unwrap(), 1);
    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Failed);
    assert!(rec.last_error.is_some());
}

pub async fn delete_rules<S: Storage>(s: S) {
    let scheduled = new_job("default").with_run_at(Utc::now() + TimeDelta::hours(1));
    let scheduled = s.enqueue(scheduled).await.unwrap();
    let enqueued = s.enqueue(new_job("default")).await.unwrap();

    s.delete(scheduled).await.unwrap();
    s.delete(enqueued).await.unwrap();
    assert_eq!(state_of(&s, scheduled).await, State::Deleted);
    assert_eq!(state_of(&s, enqueued).await, State::Deleted);

    // Deleted jobs are not promoted or fetched.
    assert_eq!(s.promote_due().await.unwrap(), 0);
    let none = s.fetch(&default_queue(), WorkerId::new(), lease()).await;
    assert!(none.unwrap().is_none());

    let err = s.delete(enqueued).await.unwrap_err();
    assert!(
        matches!(
            err,
            StorageError::InvalidTransition {
                from: State::Deleted,
                to: State::Deleted,
                ..
            }
        ),
        "{err}"
    );
}

pub async fn delete_running_job_revokes_lease<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;

    s.delete(id).await.unwrap();
    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Deleted);
    assert_eq!(rec.worker, None);

    let err = s.heartbeat(id, worker, lease()).await.unwrap_err();
    assert!(matches!(err, StorageError::LeaseLost(_)), "{err}");
    let err = s
        .complete(id, worker, Outcome::Succeeded)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::LeaseLost(_)), "{err}");
}

pub async fn manual_requeue_rules<S: Storage>(s: S) {
    let id = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();

    // Not allowed while it is still waiting to run, or running.
    let err = s.requeue(id).await.unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidTransition { .. }),
        "{err}"
    );
    fetch_one(&s, worker).await;
    let err = s.requeue(id).await.unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidTransition { .. }),
        "{err}"
    );

    let failed = Outcome::Failed {
        error: "boom".into(),
    };
    s.complete(id, worker, failed).await.unwrap();
    s.requeue(id).await.unwrap();

    let rec = s.get(id).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Enqueued);
    assert_eq!(rec.attempt, 0);
    assert_eq!(rec.last_error, None);
    assert_eq!(fetch_one(&s, worker).await.attempt, 1);

    // Also from Succeeded and Deleted.
    s.complete(id, worker, Outcome::Succeeded).await.unwrap();
    s.requeue(id).await.unwrap();
    s.delete(id).await.unwrap();
    s.requeue(id).await.unwrap();
    assert_eq!(state_of(&s, id).await, State::Enqueued);
}

pub async fn missing_jobs<S: Storage>(s: S) {
    let id = JobId::new();
    let worker = WorkerId::new();

    assert!(s.get(id).await.unwrap().is_none());
    let not_found =
        |r: Result<_, StorageError>| matches!(r, Err(StorageError::NotFound(got)) if got == id);
    assert!(not_found(s.delete(id).await));
    assert!(not_found(s.requeue(id).await));
    assert!(not_found(s.complete(id, worker, Outcome::Succeeded).await));
    assert!(not_found(
        s.heartbeat(id, worker, lease()).await.map(|_| ())
    ));
}

pub async fn concurrent_fetch_never_double_claims<S: Storage>(s: S) {
    const JOBS: usize = 200;
    const WORKERS: usize = 8;

    let s = Arc::new(s);
    let mut expected = HashSet::new();
    for _ in 0..JOBS {
        expected.insert(s.enqueue(new_job("default")).await.unwrap());
    }

    let mut tasks = Vec::new();
    for _ in 0..WORKERS {
        let s = Arc::clone(&s);
        tasks.push(tokio::spawn(async move {
            let worker = WorkerId::new();
            let mut got = Vec::new();
            while let Some(job) = s.fetch(&default_queue(), worker, lease()).await.unwrap() {
                got.push(job.id);
            }
            got
        }));
    }

    let mut seen = HashSet::new();
    for task in tasks {
        for id in task.await.unwrap() {
            assert!(seen.insert(id), "job {id} was fetched twice");
        }
    }
    assert_eq!(seen, expected);
}

pub async fn unique_key_dedupes<S: Storage>(s: S) {
    let first = s
        .enqueue(new_job("default").with_unique_key("nightly:2026-11-08"))
        .await
        .unwrap();
    let second = s
        .enqueue(new_job("default").with_unique_key("nightly:2026-11-08"))
        .await
        .unwrap();
    let other = s
        .enqueue(new_job("default").with_unique_key("nightly:2026-11-09"))
        .await
        .unwrap();
    // Jobs without a key never collide.
    s.enqueue(new_job("default")).await.unwrap();
    s.enqueue(new_job("default")).await.unwrap();

    assert_eq!(first, second);
    assert_ne!(first, other);
    let rec = s.get(first).await.unwrap().unwrap();
    assert_eq!(rec.unique_key.as_deref(), Some("nightly:2026-11-08"));
    assert_eq!(s.stats().await.unwrap().total(), 4);
}

pub async fn continuation_runs_after_parent_succeeds<S: Storage>(s: S) {
    let parent = s.enqueue(new_job("default")).await.unwrap();
    let child = s
        .enqueue(new_job("default").with_parent(parent))
        .await
        .unwrap();
    let grandchild = s
        .enqueue(new_job("default").with_parent(child))
        .await
        .unwrap();

    let rec = s.get(child).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Awaiting);
    assert_eq!(rec.parent, Some(parent));

    let worker = WorkerId::new();
    assert_eq!(fetch_one(&s, worker).await.id, parent);
    let none = s.fetch(&default_queue(), worker, lease()).await.unwrap();
    assert!(none.is_none(), "continuations wait for their parent");

    s.complete(parent, worker, Outcome::Succeeded)
        .await
        .unwrap();
    assert_eq!(state_of(&s, child).await, State::Enqueued);
    assert_eq!(state_of(&s, grandchild).await, State::Awaiting);

    assert_eq!(fetch_one(&s, worker).await.id, child);
    s.complete(child, worker, Outcome::Succeeded).await.unwrap();
    assert_eq!(state_of(&s, grandchild).await, State::Enqueued);
}

pub async fn continuations_deleted_when_parent_fails<S: Storage>(s: S) {
    let parent = s.enqueue(new_job("default")).await.unwrap();
    let child = s
        .enqueue(new_job("default").with_parent(parent))
        .await
        .unwrap();
    let grandchild = s
        .enqueue(new_job("default").with_parent(child))
        .await
        .unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;

    // A retry is not the end: continuations keep waiting.
    let retry = Outcome::Retry {
        at: Utc::now() - TimeDelta::seconds(1),
        error: "flaky".into(),
    };
    s.complete(parent, worker, retry).await.unwrap();
    assert_eq!(state_of(&s, child).await, State::Awaiting);
    s.promote_due().await.unwrap();
    fetch_one(&s, worker).await;

    let failed = Outcome::Failed {
        error: "boom".into(),
    };
    s.complete(parent, worker, failed).await.unwrap();
    for id in [child, grandchild] {
        let rec = s.get(id).await.unwrap().unwrap();
        assert_eq!(rec.state, State::Deleted, "{id}");
        assert!(rec.last_error.is_some());
    }
    let rec = s.get(child).await.unwrap().unwrap();
    assert_eq!(rec.last_error, Some(parent_failed_error(parent)));
}

pub async fn continuations_deleted_with_parent<S: Storage>(s: S) {
    let parent = s.enqueue(new_job("default")).await.unwrap();
    let child = s
        .enqueue(new_job("default").with_parent(parent))
        .await
        .unwrap();
    s.delete(parent).await.unwrap();
    assert_eq!(state_of(&s, child).await, State::Deleted);
}

pub async fn continuations_deleted_when_lease_runs_out<S: Storage>(s: S) {
    let parent = s
        .enqueue(new_job("default").with_max_attempts(1))
        .await
        .unwrap();
    let child = s
        .enqueue(new_job("default").with_parent(parent))
        .await
        .unwrap();
    s.fetch(&default_queue(), WorkerId::new(), expired_lease())
        .await
        .unwrap()
        .unwrap();
    s.requeue_expired().await.unwrap();
    assert_eq!(state_of(&s, parent).await, State::Failed);
    assert_eq!(state_of(&s, child).await, State::Deleted);
}

pub async fn continuation_of_finished_parent<S: Storage>(s: S) {
    let ok = s.enqueue(new_job("default")).await.unwrap();
    let bad = s.enqueue(new_job("default")).await.unwrap();
    let worker = WorkerId::new();
    fetch_one(&s, worker).await;
    fetch_one(&s, worker).await;
    s.complete(ok, worker, Outcome::Succeeded).await.unwrap();
    let failed = Outcome::Failed {
        error: "boom".into(),
    };
    s.complete(bad, worker, failed).await.unwrap();

    let after_ok = s.enqueue(new_job("default").with_parent(ok)).await.unwrap();
    let after_ok_later = s
        .enqueue(
            new_job("default")
                .with_parent(ok)
                .with_run_at(Utc::now() + TimeDelta::hours(1)),
        )
        .await
        .unwrap();
    let after_bad = s
        .enqueue(new_job("default").with_parent(bad))
        .await
        .unwrap();

    assert_eq!(state_of(&s, after_ok).await, State::Enqueued);
    assert_eq!(state_of(&s, after_ok_later).await, State::Scheduled);
    let rec = s.get(after_bad).await.unwrap().unwrap();
    assert_eq!(rec.state, State::Deleted);
    assert_eq!(rec.last_error, Some(parent_failed_error(bad)));
}

pub async fn continuation_of_missing_parent<S: Storage>(s: S) {
    let missing = JobId::new();
    let err = s
        .enqueue(new_job("default").with_parent(missing))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::NotFound(id) if id == missing),
        "{err}"
    );
    assert_eq!(s.stats().await.unwrap().total(), 0);
}

pub async fn stats_count_by_state<S: Storage>(s: S) {
    assert_eq!(s.stats().await.unwrap().total(), 0);

    for _ in 0..3 {
        s.enqueue(new_job("default")).await.unwrap();
    }
    s.enqueue(new_job("default").with_run_at(Utc::now() + TimeDelta::hours(1)))
        .await
        .unwrap();
    let worker = WorkerId::new();
    let done = fetch_one(&s, worker).await.id;
    s.complete(done, worker, Outcome::Succeeded).await.unwrap();
    fetch_one(&s, worker).await;

    let stats = s.stats().await.unwrap();
    assert_eq!(stats.count(State::Enqueued), 1);
    assert_eq!(stats.count(State::Scheduled), 1);
    assert_eq!(stats.count(State::Processing), 1);
    assert_eq!(stats.count(State::Succeeded), 1);
    assert_eq!(stats.count(State::Failed), 0);
    assert_eq!(stats.total(), 4);
}

pub async fn list_filters_newest_first<S: Storage>(s: S) {
    let mut emails = Vec::new();
    for _ in 0..5 {
        emails.push(s.enqueue(new_job("emails")).await.unwrap());
    }
    let mut other = new_job("reports");
    other.job_type = "report".into();
    let report = s.enqueue(other).await.unwrap();
    let worker = WorkerId::new();
    let running = s
        .fetch(&[Queue::from("emails")], worker, lease())
        .await
        .unwrap()
        .unwrap()
        .id;
    assert_eq!(running, emails[0]);

    let ids = |jobs: Vec<crate::JobRecord>| jobs.into_iter().map(|j| j.id).collect::<Vec<_>>();

    let all = ids(s.list(&JobFilter::new()).await.unwrap());
    let mut expected = emails.clone();
    expected.push(report);
    expected.reverse();
    assert_eq!(all, expected);

    let by_queue = ids(s.list(&JobFilter::new().queue("reports")).await.unwrap());
    assert_eq!(by_queue, [report]);
    let by_type = ids(s.list(&JobFilter::new().job_type("report")).await.unwrap());
    assert_eq!(by_type, [report]);
    let by_state = ids(s
        .list(&JobFilter::new().state(State::Processing))
        .await
        .unwrap());
    assert_eq!(by_state, [running]);

    let page = JobFilter::new().queue("emails").limit(2).offset(1);
    let page = ids(s.list(&page).await.unwrap());
    assert_eq!(page, [emails[3], emails[2]]);
}

pub async fn purge_removes_old_finished_jobs<S: Storage>(s: S) {
    let worker = WorkerId::new();
    let finish = async |outcome: Option<Outcome>| {
        let id = s.enqueue(new_job("default")).await.unwrap();
        if let Some(outcome) = outcome {
            fetch_one(&s, worker).await;
            s.complete(id, worker, outcome).await.unwrap();
        }
        id
    };
    let succeeded = finish(Some(Outcome::Succeeded)).await;
    let failed = finish(Some(Outcome::Failed {
        error: "boom".into(),
    }))
    .await;
    let enqueued = finish(None).await;
    let deleted = finish(None).await;
    s.delete(deleted).await.unwrap();

    // Nothing is old enough yet.
    let an_hour_ago = Utc::now() - TimeDelta::hours(1);
    assert_eq!(s.purge(an_hour_ago).await.unwrap(), 0);

    assert_eq!(
        s.purge(Utc::now() + TimeDelta::seconds(1)).await.unwrap(),
        2
    );
    assert!(s.get(succeeded).await.unwrap().is_none());
    assert!(s.get(deleted).await.unwrap().is_none());
    assert_eq!(state_of(&s, failed).await, State::Failed);
    assert_eq!(state_of(&s, enqueued).await, State::Enqueued);
}
