//! The [`Registry`]: maps a stored job's type name back to code that can
//! run it.
//!
//! [`Job`] is not dyn-compatible (its `run` returns `impl Future`), so the
//! registry stores one type-erased handler per job type instead. A handler
//! takes the JSON payload and an owned [`JobContext`], deserializes the
//! payload into the job struct and runs it.

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::{Job, JobContext, JobError};

/// A boxed, sendable future, as returned by [`Registry::run`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type Handler = Arc<
    dyn Fn(serde_json::Value, JobContext) -> BoxFuture<'static, Result<(), JobError>> + Send + Sync,
>;

/// Errors from building a [`Registry`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// Two job types share the same [`Job::NAME`].
    #[error("a job named {0:?} is already registered")]
    Duplicate(&'static str),
}

/// The set of job types a server knows how to run.
///
/// Cloning is cheap: handlers are shared.
#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<&'static str, Handler>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers job type `J` under [`Job::NAME`].
    ///
    /// Fails if another job type already uses that name.
    pub fn register<J: Job>(&mut self) -> Result<&mut Self, RegistryError> {
        if self.handlers.contains_key(J::NAME) {
            return Err(RegistryError::Duplicate(J::NAME));
        }
        let handler: Handler = Arc::new(|payload, ctx| {
            Box::pin(async move {
                // A payload that doesn't match the struct won't match on a
                // retry either.
                let job: J = serde_json::from_value(payload).map_err(|e| {
                    JobError::permanent(format!("invalid payload for job {:?}: {e}", J::NAME))
                })?;
                job.run(&ctx).await
            })
        });
        self.handlers.insert(J::NAME, handler);
        Ok(self)
    }

    /// Returns whether a handler is registered for `job_type`.
    pub fn contains(&self, job_type: &str) -> bool {
        self.handlers.contains_key(job_type)
    }

    /// Names of all registered job types, in no particular order.
    pub fn job_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.handlers.keys().copied()
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Runs a stored job: finds the handler for `job_type`, deserializes
    /// `payload` and calls [`Job::run`].
    ///
    /// Resolves to [`JobError::Permanent`] if no handler is registered or the
    /// payload does not deserialize. Callers that want to treat an unknown
    /// type differently (say, leave it for another server) should check
    /// [`Registry::contains`] first.
    pub fn run(
        &self,
        job_type: &str,
        payload: serde_json::Value,
        ctx: JobContext,
    ) -> BoxFuture<'static, Result<(), JobError>> {
        match self.handlers.get(job_type) {
            Some(handler) => handler(payload, ctx),
            None => {
                let err =
                    JobError::permanent(format!("no handler registered for job {job_type:?}"));
                Box::pin(async move { Err(err) })
            }
        }
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<_> = self.job_types().collect();
        names.sort_unstable();
        f.debug_struct("Registry")
            .field("job_types", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{CancellationToken, Extensions, JobId, NewJob};

    #[derive(Serialize, Deserialize)]
    struct Add {
        n: u32,
    }

    /// Shared counter the `Add` job adds to, reached through extensions.
    #[derive(Default)]
    struct Total(AtomicU32);

    impl Job for Add {
        const NAME: &'static str = "add";

        async fn run(&self, ctx: &JobContext) -> Result<(), JobError> {
            let total = ctx
                .extensions
                .get::<Total>()
                .ok_or_else(|| JobError::permanent("Total not registered"))?;
            total.0.fetch_add(self.n, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Fail;

    impl Job for Fail {
        const NAME: &'static str = "fail";

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            Err(JobError::permanent("always fails"))
        }
    }

    /// Different type, same name as `Add`.
    #[derive(Serialize, Deserialize)]
    struct AddImpostor;

    impl Job for AddImpostor {
        const NAME: &'static str = "add";

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn registry() -> Registry {
        let mut r = Registry::new();
        r.register::<Add>().unwrap().register::<Fail>().unwrap();
        r
    }

    fn ctx(extensions: Arc<Extensions>) -> JobContext {
        JobContext::new(JobId::new(), 1, CancellationToken::new()).with_extensions(extensions)
    }

    #[tokio::test]
    async fn runs_stored_job_by_name() {
        let mut ext = Extensions::new();
        ext.insert(Total::default());
        let ext = Arc::new(ext);

        let registry = registry();
        let stored = NewJob::from_job(&Add { n: 3 }).unwrap();
        registry
            .run(&stored.job_type, stored.payload.clone(), ctx(ext.clone()))
            .await
            .unwrap();
        registry
            .run(&stored.job_type, stored.payload, ctx(ext.clone()))
            .await
            .unwrap();

        assert_eq!(ext.get::<Total>().unwrap().0.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn job_errors_pass_through() {
        let err = registry()
            .run("fail", serde_json::json!(null), ctx(Default::default()))
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "permanent error: always fails");
    }

    #[tokio::test]
    async fn bad_payload_is_permanent() {
        let err = registry()
            .run(
                "add",
                serde_json::json!({ "n": "three" }),
                ctx(Default::default()),
            )
            .await
            .unwrap_err();
        assert!(!err.is_retryable());
        assert!(
            err.to_string().contains("invalid payload for job \"add\""),
            "{err}"
        );
    }

    #[tokio::test]
    async fn unknown_type_is_permanent() {
        let registry = registry();
        assert!(!registry.contains("nope"));
        let err = registry
            .run("nope", serde_json::json!({}), ctx(Default::default()))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "permanent error: no handler registered for job \"nope\""
        );
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let mut registry = registry();
        assert_eq!(
            registry.register::<AddImpostor>().unwrap_err(),
            RegistryError::Duplicate("add")
        );
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn lists_job_types() {
        let registry = registry();
        let mut names: Vec<_> = registry.job_types().collect();
        names.sort_unstable();
        assert_eq!(names, ["add", "fail"]);
        assert!(registry.contains("add"));
        assert_eq!(
            format!("{registry:?}"),
            r#"Registry { job_types: ["add", "fail"] }"#
        );
    }

    #[test]
    fn run_future_can_be_spawned() {
        fn assert_static_send<T: Send + 'static>(_: T) {}
        let fut = registry().run(
            "add",
            serde_json::json!({ "n": 1 }),
            ctx(Default::default()),
        );
        assert_static_send(fut);
    }
}
