//! The JobKeep client: enqueue, schedule and manage jobs from application
//! code.
//!
//! ```ignore
//! let client = Client::new(storage);
//! let id = client.enqueue(&SendEmail { to: "ada@example.com".into() }).await?;
//! client.schedule_in(&Reminder { user_id }, TimeDelta::hours(24)).await?;
//! client.enqueue_after(id, &LogDelivery { email_id: id }).await?;
//! ```

use std::sync::Arc;

use chrono::{DateTime, TimeDelta, Utc};
use jobkeep_core::{Job, JobFilter, JobId, JobRecord, NewJob, Stats, Storage, StorageError};

/// Errors from [`Client`] calls.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The job's payload could not be serialized to JSON.
    #[error("cannot serialize job payload: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// A handle for enqueueing and managing jobs. Cheap to clone.
#[derive(Debug)]
pub struct Client<S> {
    storage: Arc<S>,
}

impl<S> Clone for Client<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}

impl<S: Storage> Client<S> {
    /// Creates a client on `storage`. To share a backend with a server,
    /// pass each a clone (backends share their pool or memory when cloned)
    /// or an `Arc`.
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
        }
    }

    /// The backend this client writes to.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Enqueues `job` to run as soon as a worker is free.
    pub async fn enqueue<J: Job>(&self, job: &J) -> Result<JobId, ClientError> {
        self.enqueue_new(NewJob::from_job(job)?).await
    }

    /// Enqueues `job` to run at `at`.
    pub async fn schedule<J: Job>(&self, job: &J, at: DateTime<Utc>) -> Result<JobId, ClientError> {
        self.enqueue_new(NewJob::from_job(job)?.with_run_at(at))
            .await
    }

    /// Enqueues `job` to run after `delay`.
    pub async fn schedule_in<J: Job>(
        &self,
        job: &J,
        delay: TimeDelta,
    ) -> Result<JobId, ClientError> {
        self.schedule(job, Utc::now() + delay).await
    }

    /// Enqueues `job` as a continuation of `parent`: it runs once `parent`
    /// succeeds, and is deleted if `parent` fails or is deleted.
    pub async fn enqueue_after<J: Job>(
        &self,
        parent: JobId,
        job: &J,
    ) -> Result<JobId, ClientError> {
        self.enqueue_new(NewJob::from_job(job)?.with_parent(parent))
            .await
    }

    /// Enqueues `job` unless a job with the same `key` already exists, in
    /// which case that job's id is returned.
    pub async fn enqueue_unique<J: Job>(
        &self,
        key: impl Into<String>,
        job: &J,
    ) -> Result<JobId, ClientError> {
        self.enqueue_new(NewJob::from_job(job)?.with_unique_key(key))
            .await
    }

    /// Enqueues a fully specified job record, for full control over queue,
    /// priority, attempts and so on.
    pub async fn enqueue_new(&self, job: NewJob) -> Result<JobId, ClientError> {
        let job_type = job.job_type.clone();
        let id = self.storage.enqueue(job).await?;
        tracing::debug!(%id, %job_type, "enqueued job");
        Ok(id)
    }

    pub async fn get(&self, id: JobId) -> Result<Option<JobRecord>, ClientError> {
        Ok(self.storage.get(id).await?)
    }

    /// Deletes a job. A running job is cancelled at its next heartbeat.
    pub async fn delete(&self, id: JobId) -> Result<(), ClientError> {
        Ok(self.storage.delete(id).await?)
    }

    /// Runs a succeeded, failed or deleted job again.
    pub async fn requeue(&self, id: JobId) -> Result<(), ClientError> {
        Ok(self.storage.requeue(id).await?)
    }

    pub async fn stats(&self) -> Result<Stats, ClientError> {
        Ok(self.storage.stats().await?)
    }

    pub async fn list(&self, filter: &JobFilter) -> Result<Vec<JobRecord>, ClientError> {
        Ok(self.storage.list(filter).await?)
    }

    /// Permanently removes succeeded and deleted jobs last updated before
    /// `older_than`. Returns how many were removed.
    pub async fn purge(&self, older_than: DateTime<Utc>) -> Result<u64, ClientError> {
        Ok(self.storage.purge(older_than).await?)
    }
}

#[cfg(test)]
mod tests {
    use jobkeep_core::{JobContext, JobError, Priority, Queue, State};
    use jobkeep_memory::MemoryStorage;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Ping {
        n: u32,
    }

    impl Job for Ping {
        const NAME: &'static str = "ping";

        fn queue() -> Queue {
            Queue::from("pings")
        }

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn client() -> Client<MemoryStorage> {
        Client::new(MemoryStorage::new())
    }

    #[tokio::test]
    async fn enqueue_stores_typed_job() {
        let client = client();
        let id = client.enqueue(&Ping { n: 7 }).await.unwrap();
        let rec = client.get(id).await.unwrap().unwrap();
        assert_eq!(rec.job_type, "ping");
        assert_eq!(rec.queue.as_str(), "pings");
        assert_eq!(rec.state, State::Enqueued);
        let job: Ping = serde_json::from_value(rec.payload).unwrap();
        assert_eq!(job, Ping { n: 7 });
    }

    #[tokio::test]
    async fn schedule_and_continuations() {
        let client = client();
        let later = client
            .schedule_in(&Ping { n: 1 }, TimeDelta::hours(1))
            .await
            .unwrap();
        let after = client.enqueue_after(later, &Ping { n: 2 }).await.unwrap();

        assert_eq!(
            client.get(later).await.unwrap().unwrap().state,
            State::Scheduled
        );
        let rec = client.get(after).await.unwrap().unwrap();
        assert_eq!(rec.state, State::Awaiting);
        assert_eq!(rec.parent, Some(later));

        client.delete(later).await.unwrap();
        assert_eq!(
            client.get(after).await.unwrap().unwrap().state,
            State::Deleted
        );
        client.requeue(after).await.unwrap();
        assert_eq!(client.stats().await.unwrap().count(State::Enqueued), 1);
    }

    #[tokio::test]
    async fn unique_and_custom_jobs() {
        let client = client();
        let a = client.enqueue_unique("k", &Ping { n: 1 }).await.unwrap();
        let b = client.enqueue_unique("k", &Ping { n: 2 }).await.unwrap();
        assert_eq!(a, b);

        let custom = NewJob::from_job(&Ping { n: 3 })
            .unwrap()
            .with_priority(Priority::CRITICAL)
            .with_queue("urgent");
        let id = client.enqueue_new(custom).await.unwrap();
        let listed = client
            .list(&JobFilter::new().queue("urgent"))
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].priority, Priority::CRITICAL);
    }

    #[tokio::test]
    async fn missing_parent_is_a_storage_error() {
        let err = client()
            .enqueue_after(JobId::new(), &Ping { n: 1 })
            .await
            .unwrap_err();
        assert!(
            matches!(err, ClientError::Storage(StorageError::NotFound(_))),
            "{err}"
        );
    }
}
