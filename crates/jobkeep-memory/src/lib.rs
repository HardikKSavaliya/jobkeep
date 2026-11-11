//! In-memory [`Storage`] backend for JobKeep.
//!
//! Jobs live in a `HashMap` behind one mutex, so every operation is atomic.
//! Nothing survives a restart, and [`Storage::fetch`] scans all jobs, so this
//! backend is meant for tests and local development, not production.
//!
//! For tests that need to control time, build it with
//! [`MemoryStorage::with_manual_clock`].

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, TimeDelta, Utc};
use jobkeep_core::{
    FetchedJob, JobFilter, JobId, JobRecord, NewJob, Outcome, Queue, State, Stats, Storage,
    StorageError, WorkerId, ensure_transition, parent_failed_error,
};

/// Error recorded on a job whose worker stopped renewing its lease.
const LEASE_EXPIRED: &str = "worker lost: lease expired";

/// An in-memory job store. Cloning shares the same jobs.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    jobs: Arc<Mutex<HashMap<JobId, JobRecord>>>,
    clock: Clock,
}

impl MemoryStorage {
    /// Creates an empty store that uses the system clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty store whose clock starts at `start` and only moves
    /// when told to.
    pub fn with_manual_clock(start: DateTime<Utc>) -> (Self, ManualClock) {
        let clock = ManualClock(Arc::new(Mutex::new(start)));
        let storage = Self {
            jobs: Arc::default(),
            clock: Clock::Manual(clock.clone()),
        };
        (storage, clock)
    }

    /// Number of stored jobs, in any state.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Ids of all jobs currently in `state`, in no particular order.
    pub fn ids_in_state(&self, state: State) -> Vec<JobId> {
        self.lock()
            .values()
            .filter(|job| job.state == state)
            .map(|job| job.id)
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<JobId, JobRecord>> {
        // A panic while holding the lock can't leave a job half-updated
        // (every update is a few field writes after the checks), so the
        // data is still usable.
        self.jobs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("jobs", &self.len())
            .field("clock", &self.clock)
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
enum Clock {
    #[default]
    System,
    Manual(ManualClock),
}

impl Clock {
    fn now(&self) -> DateTime<Utc> {
        match self {
            Clock::System => Utc::now(),
            Clock::Manual(clock) => clock.now(),
        }
    }
}

/// A clock for [`MemoryStorage`] that only moves when told to.
#[derive(Clone, Debug)]
pub struct ManualClock(Arc<Mutex<DateTime<Utc>>>);

impl ManualClock {
    pub fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = now;
    }

    pub fn advance(&self, by: TimeDelta) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) += by;
    }
}

/// Moves `job` to `to`, checking the state machine first.
fn transition(job: &mut JobRecord, to: State, now: DateTime<Utc>) -> Result<(), StorageError> {
    ensure_transition(job.id, job.state, to)?;
    job.state = to;
    job.updated_at = now;
    if to != State::Processing {
        job.worker = None;
        job.lease_expires_at = None;
    }
    Ok(())
}

/// Returns the job if `worker` holds its lease.
///
/// A lease is held until the job leaves `Processing`, even past its expiry
/// time: nobody else can have it until `requeue_expired` takes it back.
fn leased(
    jobs: &mut HashMap<JobId, JobRecord>,
    id: JobId,
    worker: WorkerId,
) -> Result<&mut JobRecord, StorageError> {
    let job = jobs.get_mut(&id).ok_or(StorageError::NotFound(id))?;
    if job.state == State::Processing && job.worker == Some(worker) {
        Ok(job)
    } else {
        Err(StorageError::LeaseLost(id))
    }
}

/// Moves continuations awaiting `parent` to `Enqueued`.
fn release_children(jobs: &mut HashMap<JobId, JobRecord>, parent: JobId, now: DateTime<Utc>) {
    for job in jobs.values_mut() {
        if job.parent == Some(parent) && job.state == State::Awaiting {
            transition(job, State::Enqueued, now).expect("awaiting -> enqueued is allowed");
            job.run_at = now;
        }
    }
}

/// Deletes continuations awaiting `parent`, and theirs, recursively.
fn delete_children(jobs: &mut HashMap<JobId, JobRecord>, parent: JobId, now: DateTime<Utc>) {
    let mut parents = vec![parent];
    while let Some(parent) = parents.pop() {
        for job in jobs.values_mut() {
            if job.parent == Some(parent) && job.state == State::Awaiting {
                transition(job, State::Deleted, now).expect("awaiting -> deleted is allowed");
                job.last_error = Some(parent_failed_error(parent));
                parents.push(job.id);
            }
        }
    }
}

impl Storage for MemoryStorage {
    async fn enqueue(&self, job: NewJob) -> Result<JobId, StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();

        if let Some(key) = &job.unique_key
            && let Some(existing) = jobs.values().find(|j| j.unique_key.as_ref() == Some(key))
        {
            return Ok(existing.id);
        }

        let run_at = job.run_at.unwrap_or(now);
        let mut last_error = None;
        let state = match job.parent {
            Some(parent) => {
                let parent_state = jobs
                    .get(&parent)
                    .ok_or(StorageError::NotFound(parent))?
                    .state;
                match parent_state {
                    State::Succeeded if run_at > now => State::Scheduled,
                    State::Succeeded => State::Enqueued,
                    State::Failed | State::Deleted => {
                        last_error = Some(parent_failed_error(parent));
                        State::Deleted
                    }
                    _ => State::Awaiting,
                }
            }
            None if run_at > now => State::Scheduled,
            None => State::Enqueued,
        };

        let id = JobId::new();
        let record = JobRecord {
            id,
            job_type: job.job_type,
            payload: job.payload,
            queue: job.queue,
            priority: job.priority,
            state,
            attempt: 0,
            max_attempts: job.max_attempts,
            run_at,
            created_at: now,
            updated_at: now,
            last_error,
            worker: None,
            lease_expires_at: None,
            parent: job.parent,
            unique_key: job.unique_key,
        };
        jobs.insert(id, record);
        Ok(id)
    }

    async fn fetch(
        &self,
        queues: &[Queue],
        worker: WorkerId,
        lease: TimeDelta,
    ) -> Result<Option<FetchedJob>, StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();

        // Highest priority first, then oldest run_at; the id breaks ties so
        // the order is deterministic.
        let next = jobs
            .values()
            .filter(|job| job.state == State::Enqueued && queues.contains(&job.queue))
            .min_by_key(|job| (std::cmp::Reverse(job.priority), job.run_at, job.id))
            .map(|job| job.id);
        let Some(id) = next else {
            return Ok(None);
        };

        let job = jobs.get_mut(&id).expect("id came from the map");
        transition(job, State::Processing, now)?;
        let lease_expires_at = now + lease;
        job.attempt += 1;
        job.worker = Some(worker);
        job.lease_expires_at = Some(lease_expires_at);

        Ok(Some(FetchedJob {
            id,
            job_type: job.job_type.clone(),
            payload: job.payload.clone(),
            queue: job.queue.clone(),
            priority: job.priority,
            attempt: job.attempt,
            max_attempts: job.max_attempts,
            lease_expires_at,
        }))
    }

    async fn heartbeat(
        &self,
        id: JobId,
        worker: WorkerId,
        lease: TimeDelta,
    ) -> Result<DateTime<Utc>, StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();
        let job = leased(&mut jobs, id, worker)?;
        let expires = now + lease;
        job.lease_expires_at = Some(expires);
        job.updated_at = now;
        Ok(expires)
    }

    async fn complete(
        &self,
        id: JobId,
        worker: WorkerId,
        outcome: Outcome,
    ) -> Result<(), StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();
        let job = leased(&mut jobs, id, worker)?;
        transition(job, outcome.target_state(), now)?;
        if let Some(error) = outcome.error() {
            job.last_error = Some(error.to_owned());
        }
        match outcome {
            Outcome::Retry { at, .. } => job.run_at = at,
            Outcome::Requeue => job.attempt = job.attempt.saturating_sub(1),
            Outcome::Succeeded => release_children(&mut jobs, id, now),
            Outcome::Failed { .. } => delete_children(&mut jobs, id, now),
        }
        Ok(())
    }

    async fn promote_due(&self) -> Result<u64, StorageError> {
        let now = self.clock.now();
        let mut promoted = 0;
        for job in self.lock().values_mut() {
            if job.state == State::Scheduled && job.run_at <= now {
                transition(job, State::Enqueued, now)?;
                promoted += 1;
            }
        }
        Ok(promoted)
    }

    async fn requeue_expired(&self) -> Result<u64, StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();
        let mut failed = Vec::new();
        let mut reclaimed = 0;
        for job in jobs.values_mut() {
            let expired =
                job.state == State::Processing && job.lease_expires_at.is_some_and(|at| at <= now);
            if !expired {
                continue;
            }
            let to = if job.attempt >= job.max_attempts {
                State::Failed
            } else {
                State::Enqueued
            };
            transition(job, to, now)?;
            job.last_error = Some(LEASE_EXPIRED.to_owned());
            if to == State::Failed {
                failed.push(job.id);
            }
            reclaimed += 1;
        }
        for id in failed {
            delete_children(&mut jobs, id, now);
        }
        Ok(reclaimed)
    }

    async fn get(&self, id: JobId) -> Result<Option<JobRecord>, StorageError> {
        Ok(self.lock().get(&id).cloned())
    }

    async fn delete(&self, id: JobId) -> Result<(), StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();
        let job = jobs.get_mut(&id).ok_or(StorageError::NotFound(id))?;
        transition(job, State::Deleted, now)?;
        delete_children(&mut jobs, id, now);
        Ok(())
    }

    async fn requeue(&self, id: JobId) -> Result<(), StorageError> {
        let now = self.clock.now();
        let mut jobs = self.lock();
        let job = jobs.get_mut(&id).ok_or(StorageError::NotFound(id))?;
        // The state machine also lets Scheduled, Awaiting and Processing
        // reach Enqueued, but those are the server's moves, not a user's.
        if !matches!(job.state, State::Succeeded | State::Failed | State::Deleted) {
            return Err(StorageError::InvalidTransition {
                id,
                from: job.state,
                to: State::Enqueued,
            });
        }
        transition(job, State::Enqueued, now)?;
        job.attempt = 0;
        job.last_error = None;
        job.run_at = now;
        Ok(())
    }

    async fn stats(&self) -> Result<Stats, StorageError> {
        let mut stats = Stats::default();
        for job in self.lock().values() {
            *stats.counts.entry(job.state).or_default() += 1;
        }
        Ok(stats)
    }

    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobRecord>, StorageError> {
        let jobs = self.lock();
        let mut matching: Vec<_> = jobs.values().filter(|job| filter.matches(job)).collect();
        // Ids are UUIDv7, so descending id is newest first.
        matching.sort_unstable_by_key(|job| std::cmp::Reverse(job.id));
        Ok(matching
            .into_iter()
            .skip(filter.offset as usize)
            .take(filter.effective_limit() as usize)
            .cloned()
            .collect())
    }

    async fn purge(&self, older_than: DateTime<Utc>) -> Result<u64, StorageError> {
        let mut jobs = self.lock();
        let before = jobs.len();
        jobs.retain(|_, job| {
            !(matches!(job.state, State::Succeeded | State::Deleted) && job.updated_at < older_than)
        });
        Ok((before - jobs.len()) as u64)
    }
}

#[cfg(test)]
mod tests {
    use jobkeep_core::{Job, JobContext, JobError, Registry};
    use serde::{Deserialize, Serialize};

    use super::*;

    mod conformance {
        use super::*;
        jobkeep_core::storage_conformance_tests!(MemoryStorage::new());
    }

    fn lease() -> TimeDelta {
        TimeDelta::seconds(30)
    }

    fn job_at(run_at: DateTime<Utc>) -> NewJob {
        let run_at = Some(run_at);
        NewJob {
            job_type: "test".into(),
            payload: serde_json::Value::Null,
            queue: Queue::default(),
            priority: Default::default(),
            run_at,
            max_attempts: 3,
            parent: None,
            unique_key: None,
        }
    }

    #[tokio::test]
    async fn manual_clock_drives_scheduling() {
        let start = Utc::now();
        let (s, clock) = MemoryStorage::with_manual_clock(start);
        let id = s
            .enqueue(job_at(start + TimeDelta::minutes(5)))
            .await
            .unwrap();

        clock.advance(TimeDelta::minutes(4));
        assert_eq!(s.promote_due().await.unwrap(), 0);

        clock.advance(TimeDelta::minutes(1));
        assert_eq!(s.promote_due().await.unwrap(), 1);
        assert_eq!(s.ids_in_state(State::Enqueued), [id]);
    }

    #[tokio::test]
    async fn manual_clock_drives_lease_expiry() {
        let start = Utc::now();
        let (s, clock) = MemoryStorage::with_manual_clock(start);
        let id = s.enqueue(job_at(start)).await.unwrap();
        let worker = WorkerId::new();

        let job = s
            .fetch(&[Queue::default()], worker, lease())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.lease_expires_at, start + lease());

        clock.advance(TimeDelta::seconds(20));
        let renewed = s.heartbeat(id, worker, lease()).await.unwrap();
        assert_eq!(renewed, start + TimeDelta::seconds(50));

        clock.set(start + TimeDelta::seconds(49));
        assert_eq!(s.requeue_expired().await.unwrap(), 0);
        clock.set(start + TimeDelta::seconds(50));
        assert_eq!(s.requeue_expired().await.unwrap(), 1);

        let rec = s.get(id).await.unwrap().unwrap();
        assert_eq!(rec.state, State::Enqueued);
        assert_eq!(rec.last_error.as_deref(), Some(LEASE_EXPIRED));
        assert_eq!(rec.updated_at, start + TimeDelta::seconds(50));
    }

    #[tokio::test]
    async fn clones_share_jobs() {
        let a = MemoryStorage::new();
        let b = a.clone();
        let id = a.enqueue(job_at(Utc::now())).await.unwrap();
        assert!(b.get(id).await.unwrap().is_some());
        assert_eq!(b.len(), 1);
    }

    #[derive(Serialize, Deserialize)]
    struct Greet {
        name: String,
    }

    impl Job for Greet {
        const NAME: &'static str = "greet";

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            if self.name.is_empty() {
                return Err(JobError::permanent("no name"));
            }
            Ok(())
        }
    }

    /// The loop a server will run, by hand: enqueue, fetch, run through
    /// the registry, report the outcome.
    #[tokio::test]
    async fn end_to_end_with_registry() {
        let s = MemoryStorage::new();
        let mut registry = Registry::new();
        registry.register::<Greet>().unwrap();

        let good = s
            .enqueue(NewJob::from_job(&Greet { name: "Ada".into() }).unwrap())
            .await
            .unwrap();
        let bad = s
            .enqueue(
                NewJob::from_job(&Greet {
                    name: String::new(),
                })
                .unwrap(),
            )
            .await
            .unwrap();

        let worker = WorkerId::new();
        while let Some(job) = s.fetch(&[Queue::default()], worker, lease()).await.unwrap() {
            let ctx = JobContext::new(job.id, job.attempt, Default::default());
            let outcome = match registry.run(&job.job_type, job.payload, ctx).await {
                Ok(()) => Outcome::Succeeded,
                Err(e) => Outcome::Failed {
                    error: e.to_string(),
                },
            };
            s.complete(job.id, worker, outcome).await.unwrap();
        }

        assert_eq!(s.ids_in_state(State::Succeeded), [good]);
        let rec = s.get(bad).await.unwrap().unwrap();
        assert_eq!(rec.state, State::Failed);
        assert_eq!(rec.last_error.as_deref(), Some("permanent error: no name"));
    }
}
