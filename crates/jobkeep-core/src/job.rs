//! The [`Job`] trait, the context a job runs with, and the storage-facing
//! [`NewJob`] record.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::{JobError, JobId, Priority, Queue};

/// A unit of background work.
///
/// The struct itself is the payload: it is serialized to JSON on enqueue and
/// deserialized again by the worker that runs it.
pub trait Job: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable name used to find the handler for a stored job.
    /// Changing it orphans jobs already in storage.
    const NAME: &'static str;

    /// Queue the job is placed on.
    fn queue() -> Queue {
        Queue::default()
    }

    /// Total number of runs (first attempt plus retries) before the job is
    /// marked `Failed`.
    fn max_attempts() -> u32 {
        10
    }

    /// Runs the job.
    fn run(&self, ctx: &JobContext) -> impl Future<Output = Result<(), JobError>> + Send;
}

/// Information and services available to a running job.
#[derive(Debug)]
pub struct JobContext {
    /// Id of the job being run.
    pub job_id: JobId,
    /// Which attempt this is, starting at 1.
    pub attempt: u32,
    /// Fires when the job is deleted or the server is shutting down.
    /// Long-running jobs should check it and return [`JobError::Cancelled`].
    pub cancellation: CancellationToken,
    /// Shared services (database pools, HTTP clients, ...) registered on the server.
    pub extensions: Extensions,
}

impl JobContext {
    pub fn new(job_id: JobId, attempt: u32, cancellation: CancellationToken) -> Self {
        Self {
            job_id,
            attempt,
            cancellation,
            extensions: Extensions::default(),
        }
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

/// A type-keyed map of shared values.
///
/// Stub for now: it will be populated by the server when dependency
/// injection lands.
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a value, returning the previous value of the same type.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|old| old.downcast().ok().map(|b| *b))
    }

    /// Returns a reference to the value of type `T`, if present.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.map.get(&TypeId::of::<T>())?.downcast_ref()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish_non_exhaustive()
    }
}

/// A job ready to be written to storage. Type-erased: the payload is JSON
/// and the job type is identified by [`Job::NAME`].
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    pub job_type: String,
    pub payload: serde_json::Value,
    pub queue: Queue,
    pub priority: Priority,
    /// When the job should first run. A time in the past means "now".
    pub run_at: DateTime<Utc>,
    pub max_attempts: u32,
}

impl NewJob {
    /// Builds a record from a typed job, using the job's queue and
    /// attempt defaults, normal priority, and `run_at = now`.
    pub fn from_job<J: Job>(job: &J) -> serde_json::Result<Self> {
        Ok(Self {
            job_type: J::NAME.to_owned(),
            payload: serde_json::to_value(job)?,
            queue: J::queue(),
            priority: Priority::default(),
            run_at: Utc::now(),
            max_attempts: J::max_attempts(),
        })
    }

    pub fn with_queue(mut self, queue: impl Into<Queue>) -> Self {
        self.queue = queue.into();
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_run_at(mut self, run_at: DateTime<Utc>) -> Self {
        self.run_at = run_at;
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct SendEmail {
        to: String,
    }

    impl Job for SendEmail {
        const NAME: &'static str = "send_email";

        fn queue() -> Queue {
            Queue::from("emails")
        }

        async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
            if ctx.is_cancelled() {
                return Err(JobError::Cancelled);
            }
            if self.to.is_empty() {
                return Err(JobError::permanent("no recipient"));
            }
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Noop;

    impl Job for Noop {
        const NAME: &'static str = "noop";

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn ctx() -> JobContext {
        JobContext::new(JobId::new(), 1, CancellationToken::new())
    }

    #[tokio::test]
    async fn runs_job_and_honours_cancellation() {
        let job = SendEmail {
            to: "a@example.com".into(),
        };
        let ctx = ctx();
        assert!(job.run(&ctx).await.is_ok());

        ctx.cancellation.cancel();
        assert!(matches!(job.run(&ctx).await, Err(JobError::Cancelled)));
    }

    #[tokio::test]
    async fn permanent_errors_surface() {
        let job = SendEmail { to: String::new() };
        assert!(matches!(job.run(&ctx()).await, Err(JobError::Permanent(_))));
    }

    #[test]
    fn run_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let job = Noop;
        let ctx = ctx();
        assert_send(job.run(&ctx));
    }

    #[test]
    fn new_job_uses_trait_defaults_and_overrides() {
        let job = NewJob::from_job(&SendEmail {
            to: "a@example.com".into(),
        })
        .unwrap();
        assert_eq!(job.job_type, "send_email");
        assert_eq!(job.queue.as_str(), "emails");
        assert_eq!(job.priority, Priority::NORMAL);
        assert_eq!(job.max_attempts, 10);
        assert_eq!(
            serde_json::from_value::<SendEmail>(job.payload.clone()).unwrap(),
            SendEmail {
                to: "a@example.com".into()
            }
        );

        let noop = NewJob::from_job(&Noop)
            .unwrap()
            .with_priority(Priority::HIGH)
            .with_max_attempts(1);
        assert_eq!(noop.queue, Queue::default());
        assert_eq!(noop.priority, Priority::HIGH);
        assert_eq!(noop.max_attempts, 1);
    }

    #[test]
    fn extensions_are_type_keyed() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());
        assert_eq!(ext.insert(5u32), None);
        assert_eq!(ext.insert(7u32), Some(5));
        ext.insert(String::from("pool"));
        assert_eq!(ext.get::<u32>(), Some(&7));
        assert_eq!(ext.get::<String>().map(String::as_str), Some("pool"));
        assert_eq!(ext.get::<i64>(), None);
        assert_eq!(ext.len(), 2);
    }
}
