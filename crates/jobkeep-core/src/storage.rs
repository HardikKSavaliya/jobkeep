//! The [`Storage`] trait that backends implement, and the records it
//! exchanges with the server and client.
//!
//! Delivery is at-least-once. [`Storage::fetch`] hands a job to a worker
//! under a time-limited lease. The worker extends the lease with
//! [`Storage::heartbeat`] while the job runs and releases it with
//! [`Storage::complete`]. If the worker dies, the lease expires and
//! [`Storage::requeue_expired`] makes the job available again, so a job can
//! run more than once and should be idempotent.
//!
//! All time-dependent decisions (is a job due, has a lease expired) use the
//! backend's clock, so several servers sharing one database agree on them.
//!
//! A job created with [`NewJob::parent`] is a *continuation*: it waits in
//! [`State::Awaiting`] until its parent succeeds, then moves to
//! [`State::Enqueued`]. If the parent fails or is deleted instead, the
//! continuation, and any continuations of it, move to [`State::Deleted`].

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::{JobId, NewJob, Priority, Queue, State, WorkerId};

/// Errors returned by a [`Storage`] backend.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// No job with this id exists.
    #[error("job {0} not found")]
    NotFound(JobId),

    /// The requested change is not allowed by the [`State`] machine.
    #[error("job {id} cannot move from {from} to {to}")]
    InvalidTransition { id: JobId, from: State, to: State },

    /// The worker no longer holds the job's lease: it expired and the job
    /// was requeued, or the job was deleted while running.
    #[error("worker no longer holds the lease on job {0}")]
    LeaseLost(JobId),

    /// The backend itself failed (connection lost, query error, ...).
    #[error("storage backend error: {0:#}")]
    Backend(#[from] anyhow::Error),
}

/// Checks a state change against the [`State`] machine.
///
/// Backends call this before every transition, so no backend can move a job
/// along an edge the state diagram does not allow.
pub fn ensure_transition(id: JobId, from: State, to: State) -> Result<(), StorageError> {
    if from.can_transition_to(to) {
        Ok(())
    } else {
        Err(StorageError::InvalidTransition { id, from, to })
    }
}

/// A job handed to a worker by [`Storage::fetch`]. Its state is now
/// [`State::Processing`] and the worker holds its lease.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedJob {
    pub id: JobId,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub queue: Queue,
    pub priority: Priority,
    /// The attempt about to run, starting at 1.
    pub attempt: u32,
    pub max_attempts: u32,
    /// When the lease runs out unless renewed with [`Storage::heartbeat`].
    pub lease_expires_at: DateTime<Utc>,
}

/// How a run ended, reported with [`Storage::complete`].
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The job finished successfully.
    Succeeded,
    /// The job failed for good: a permanent error, or attempts exhausted.
    Failed { error: String },
    /// The job failed and should run again at `at`.
    Retry { at: DateTime<Utc>, error: String },
    /// The worker gave the job back without running it to completion,
    /// typically on shutdown. This does not count as an attempt: the next
    /// fetch reports the same attempt number again.
    Requeue,
}

impl Outcome {
    /// The state a job in [`State::Processing`] moves to.
    pub fn target_state(&self) -> State {
        match self {
            Outcome::Succeeded => State::Succeeded,
            Outcome::Failed { .. } => State::Failed,
            Outcome::Retry { .. } => State::Scheduled,
            Outcome::Requeue => State::Enqueued,
        }
    }

    /// The error message to record on the job, if any.
    pub fn error(&self) -> Option<&str> {
        match self {
            Outcome::Failed { error } | Outcome::Retry { error, .. } => Some(error),
            Outcome::Succeeded | Outcome::Requeue => None,
        }
    }
}

/// Error recorded on a continuation deleted because its parent did not
/// succeed.
pub fn parent_failed_error(parent: JobId) -> String {
    format!("parent job {parent} did not succeed")
}

/// A stored job as seen by the client and dashboard.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub queue: Queue,
    pub priority: Priority,
    pub state: State,
    /// Number of attempts started so far.
    pub attempt: u32,
    pub max_attempts: u32,
    /// When the job is (or was) due to run.
    pub run_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Error from the most recent failed attempt.
    pub last_error: Option<String>,
    /// The worker holding the lease, while [`State::Processing`].
    pub worker: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// The job this one continues, if it is a continuation.
    pub parent: Option<JobId>,
    /// Deduplication key given at enqueue time.
    pub unique_key: Option<String>,
}

// Hand-written so a large payload doesn't flood logs.
impl fmt::Debug for JobRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobRecord")
            .field("id", &self.id)
            .field("job_type", &self.job_type)
            .field("queue", &self.queue)
            .field("state", &self.state)
            .field("attempt", &self.attempt)
            .field("max_attempts", &self.max_attempts)
            .field("run_at", &self.run_at)
            .field("last_error", &self.last_error)
            .field("worker", &self.worker)
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

/// Number of jobs in each state, from [`Storage::stats`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub counts: HashMap<State, u64>,
}

impl Stats {
    /// Number of jobs in `state`.
    pub fn count(&self, state: State) -> u64 {
        self.counts.get(&state).copied().unwrap_or(0)
    }

    /// Number of jobs in any state.
    pub fn total(&self) -> u64 {
        self.counts.values().sum()
    }
}

/// Which jobs [`Storage::list`] returns. Unset filters match everything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JobFilter {
    pub state: Option<State>,
    pub queue: Option<Queue>,
    pub job_type: Option<String>,
    pub limit: u32,
    pub offset: u32,
}

impl JobFilter {
    /// Largest page [`Storage::list`] is expected to return.
    pub const MAX_LIMIT: u32 = 500;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(mut self, state: State) -> Self {
        self.state = Some(state);
        self
    }

    pub fn queue(mut self, queue: impl Into<Queue>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn job_type(mut self, job_type: impl Into<String>) -> Self {
        self.job_type = Some(job_type.into());
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// The limit to apply, clamped to `1..=MAX_LIMIT`.
    pub fn effective_limit(&self) -> u32 {
        self.limit.clamp(1, Self::MAX_LIMIT)
    }

    /// Returns whether `job` matches the state, queue and type filters.
    pub fn matches(&self, job: &JobRecord) -> bool {
        self.state.is_none_or(|s| s == job.state)
            && self.queue.as_ref().is_none_or(|q| *q == job.queue)
            && self.job_type.as_ref().is_none_or(|t| *t == job.job_type)
    }
}

impl Default for JobFilter {
    fn default() -> Self {
        Self {
            state: None,
            queue: None,
            job_type: None,
            limit: 50,
            offset: 0,
        }
    }
}

/// A place jobs are kept: in memory, Postgres, ...
///
/// Every method is atomic with respect to the others, including across
/// processes sharing the same backend. Every state change must be one the
/// [`State`] machine allows; [`ensure_transition`] checks a single change.
pub trait Storage: Send + Sync + 'static {
    /// Stores a new job and returns its id.
    ///
    /// The job starts in [`State::Enqueued`] if `run_at` is not in the
    /// future, and in [`State::Scheduled`] otherwise.
    ///
    /// With a [`NewJob::parent`], the job is a continuation and starts in
    /// [`State::Awaiting`]; if the parent has already succeeded it starts as
    /// above, and if the parent already failed or was deleted it starts in
    /// [`State::Deleted`]. Fails with [`StorageError::NotFound`] if the
    /// parent does not exist.
    ///
    /// With a [`NewJob::unique_key`] already used by a stored job, nothing is
    /// stored and the existing job's id is returned. Keys stay taken until
    /// the job holding them is purged.
    fn enqueue(&self, job: NewJob) -> impl Future<Output = Result<JobId, StorageError>> + Send;

    /// Claims the next ready job on any of `queues` for `worker`, or returns
    /// `None` if there is none.
    ///
    /// Jobs are taken highest [`Priority`] first, then oldest `run_at`.
    /// The job moves to [`State::Processing`], its attempt count goes up by
    /// one, and `worker` holds its lease for `lease`.
    fn fetch(
        &self,
        queues: &[Queue],
        worker: WorkerId,
        lease: TimeDelta,
    ) -> impl Future<Output = Result<Option<FetchedJob>, StorageError>> + Send;

    /// Extends `worker`'s lease on a running job to `lease` from now and
    /// returns the new expiry.
    ///
    /// Fails with [`StorageError::LeaseLost`] if `worker` no longer holds the
    /// lease; the worker should then stop the job.
    fn heartbeat(
        &self,
        id: JobId,
        worker: WorkerId,
        lease: TimeDelta,
    ) -> impl Future<Output = Result<DateTime<Utc>, StorageError>> + Send;

    /// Records how a run ended and releases the lease.
    ///
    /// Continuations of the job are released on [`Outcome::Succeeded`] and
    /// deleted on [`Outcome::Failed`].
    ///
    /// Fails with [`StorageError::LeaseLost`] if `worker` no longer holds the
    /// lease; the outcome is then discarded.
    fn complete(
        &self,
        id: JobId,
        worker: WorkerId,
        outcome: Outcome,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Moves every [`State::Scheduled`] job whose `run_at` has arrived to
    /// [`State::Enqueued`]. Returns how many were moved.
    fn promote_due(&self) -> impl Future<Output = Result<u64, StorageError>> + Send;

    /// Takes back every [`State::Processing`] job whose lease has expired.
    /// Returns how many were taken back.
    ///
    /// A job with attempts left goes back to [`State::Enqueued`]. A job that
    /// has used all its attempts moves to [`State::Failed`] instead (and its
    /// continuations are deleted), so a job that keeps crashing its worker
    /// cannot loop forever.
    fn requeue_expired(&self) -> impl Future<Output = Result<u64, StorageError>> + Send;

    /// Returns the job with this id, if it exists.
    fn get(
        &self,
        id: JobId,
    ) -> impl Future<Output = Result<Option<JobRecord>, StorageError>> + Send;

    /// Moves a job to [`State::Deleted`]. A running job loses its lease, and
    /// continuations still awaiting it are deleted too.
    fn delete(&self, id: JobId) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Manually puts a [`State::Succeeded`], [`State::Failed`] or
    /// [`State::Deleted`] job back to [`State::Enqueued`], with its attempt
    /// count reset to zero.
    fn requeue(&self, id: JobId) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Counts jobs by state.
    fn stats(&self) -> impl Future<Output = Result<Stats, StorageError>> + Send;

    /// Lists jobs matching `filter`, newest first.
    fn list(
        &self,
        filter: &JobFilter,
    ) -> impl Future<Output = Result<Vec<JobRecord>, StorageError>> + Send;

    /// Permanently removes [`State::Succeeded`] and [`State::Deleted`] jobs
    /// last updated before `older_than`. Returns how many were removed.
    /// Failed jobs are kept until requeued or deleted.
    fn purge(
        &self,
        older_than: DateTime<Utc>,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send;
}

/// Shares one backend between owners, e.g. a client and a server.
impl<S: Storage> Storage for Arc<S> {
    fn enqueue(&self, job: NewJob) -> impl Future<Output = Result<JobId, StorageError>> + Send {
        (**self).enqueue(job)
    }

    fn fetch(
        &self,
        queues: &[Queue],
        worker: WorkerId,
        lease: TimeDelta,
    ) -> impl Future<Output = Result<Option<FetchedJob>, StorageError>> + Send {
        (**self).fetch(queues, worker, lease)
    }

    fn heartbeat(
        &self,
        id: JobId,
        worker: WorkerId,
        lease: TimeDelta,
    ) -> impl Future<Output = Result<DateTime<Utc>, StorageError>> + Send {
        (**self).heartbeat(id, worker, lease)
    }

    fn complete(
        &self,
        id: JobId,
        worker: WorkerId,
        outcome: Outcome,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        (**self).complete(id, worker, outcome)
    }

    fn promote_due(&self) -> impl Future<Output = Result<u64, StorageError>> + Send {
        (**self).promote_due()
    }

    fn requeue_expired(&self) -> impl Future<Output = Result<u64, StorageError>> + Send {
        (**self).requeue_expired()
    }

    fn get(
        &self,
        id: JobId,
    ) -> impl Future<Output = Result<Option<JobRecord>, StorageError>> + Send {
        (**self).get(id)
    }

    fn delete(&self, id: JobId) -> impl Future<Output = Result<(), StorageError>> + Send {
        (**self).delete(id)
    }

    fn requeue(&self, id: JobId) -> impl Future<Output = Result<(), StorageError>> + Send {
        (**self).requeue(id)
    }

    fn stats(&self) -> impl Future<Output = Result<Stats, StorageError>> + Send {
        (**self).stats()
    }

    fn list(
        &self,
        filter: &JobFilter,
    ) -> impl Future<Output = Result<Vec<JobRecord>, StorageError>> + Send {
        (**self).list(filter)
    }

    fn purge(
        &self,
        older_than: DateTime<Utc>,
    ) -> impl Future<Output = Result<u64, StorageError>> + Send {
        (**self).purge(older_than)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_transition_follows_state_machine() {
        let id = JobId::new();
        assert!(ensure_transition(id, State::Enqueued, State::Processing).is_ok());

        let err = ensure_transition(id, State::Succeeded, State::Processing).unwrap_err();
        assert!(matches!(
            err,
            StorageError::InvalidTransition {
                from: State::Succeeded,
                to: State::Processing,
                ..
            }
        ));
        assert_eq!(
            err.to_string(),
            format!("job {id} cannot move from succeeded to processing")
        );
    }

    #[test]
    fn every_outcome_is_a_legal_move_from_processing() {
        let outcomes = [
            Outcome::Succeeded,
            Outcome::Failed { error: "x".into() },
            Outcome::Retry {
                at: Utc::now(),
                error: "x".into(),
            },
            Outcome::Requeue,
        ];
        for outcome in outcomes {
            let to = outcome.target_state();
            assert!(State::Processing.can_transition_to(to), "{outcome:?}");
        }
    }

    #[test]
    fn filter_limits_are_clamped() {
        assert_eq!(JobFilter::new().effective_limit(), 50);
        assert_eq!(JobFilter::new().limit(0).effective_limit(), 1);
        assert_eq!(JobFilter::new().limit(10_000).effective_limit(), 500);
    }

    #[test]
    fn stats_count_missing_states_as_zero() {
        let mut stats = Stats::default();
        stats.counts.insert(State::Failed, 2);
        stats.counts.insert(State::Enqueued, 3);
        assert_eq!(stats.count(State::Failed), 2);
        assert_eq!(stats.count(State::Deleted), 0);
        assert_eq!(stats.total(), 5);
    }

    #[test]
    fn outcome_errors() {
        assert_eq!(Outcome::Succeeded.error(), None);
        assert_eq!(Outcome::Requeue.error(), None);
        assert_eq!(
            Outcome::Failed {
                error: "boom".into()
            }
            .error(),
            Some("boom")
        );
    }

    /// A backend that stores nothing, to check the trait can be implemented
    /// with `async fn` and that its futures can be spawned from generic code.
    struct Null;

    impl Storage for Null {
        async fn enqueue(&self, _job: NewJob) -> Result<JobId, StorageError> {
            Ok(JobId::new())
        }
        async fn fetch(
            &self,
            _queues: &[Queue],
            _worker: WorkerId,
            _lease: TimeDelta,
        ) -> Result<Option<FetchedJob>, StorageError> {
            Ok(None)
        }
        async fn heartbeat(
            &self,
            id: JobId,
            _worker: WorkerId,
            _lease: TimeDelta,
        ) -> Result<DateTime<Utc>, StorageError> {
            Err(StorageError::LeaseLost(id))
        }
        async fn complete(
            &self,
            id: JobId,
            _worker: WorkerId,
            _outcome: Outcome,
        ) -> Result<(), StorageError> {
            Err(StorageError::LeaseLost(id))
        }
        async fn promote_due(&self) -> Result<u64, StorageError> {
            Ok(0)
        }
        async fn requeue_expired(&self) -> Result<u64, StorageError> {
            Ok(0)
        }
        async fn get(&self, _id: JobId) -> Result<Option<JobRecord>, StorageError> {
            Ok(None)
        }
        async fn delete(&self, id: JobId) -> Result<(), StorageError> {
            Err(StorageError::NotFound(id))
        }
        async fn requeue(&self, id: JobId) -> Result<(), StorageError> {
            Err(StorageError::NotFound(id))
        }
        async fn stats(&self) -> Result<Stats, StorageError> {
            Ok(Stats::default())
        }
        async fn list(&self, _filter: &JobFilter) -> Result<Vec<JobRecord>, StorageError> {
            Ok(Vec::new())
        }
        async fn purge(&self, _older_than: DateTime<Utc>) -> Result<u64, StorageError> {
            Ok(0)
        }
    }

    fn assert_send<T: Send>(_: T) {}

    fn futures_are_send<S: Storage>(storage: &S) {
        let queues = [Queue::default()];
        assert_send(storage.fetch(&queues, WorkerId::new(), TimeDelta::seconds(30)));
        assert_send(storage.promote_due());
    }

    #[tokio::test]
    async fn trait_is_implementable() {
        let storage = Null;
        futures_are_send(&storage);

        let id = JobId::new();
        assert!(
            storage
                .fetch(&[Queue::default()], WorkerId::new(), TimeDelta::seconds(30))
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            storage.delete(id).await,
            Err(StorageError::NotFound(got)) if got == id
        ));
    }
}
