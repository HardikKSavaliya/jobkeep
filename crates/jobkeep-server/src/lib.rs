//! The JobKeep server: fetches jobs from a [`Storage`] backend and runs them.
//!
//! ```ignore
//! let server = Server::builder(storage)
//!     .register::<SendEmail>()
//!     .recurring::<NightlyReport>()
//!     .extension(http_client)
//!     .queues(["default", "emails"])
//!     .concurrency(20)
//!     .build();
//! server.run_until_ctrl_c().await;
//! ```
//!
//! A running server does three things:
//!
//! - **Workers** fetch jobs from its queues, at most `concurrency` at once,
//!   and run them. While a job runs, the server renews its lease; if the
//!   lease is lost (the job was deleted, or the server stalled long enough
//!   for another to take it back) the job is cancelled. Failed jobs are
//!   retried according to the [`RetryPolicy`] until they run out of
//!   attempts.
//! - **Maintenance** moves due scheduled jobs to their queue, takes back jobs
//!   whose worker died, enqueues recurring jobs, and purges old finished
//!   jobs.
//! - **Shutdown** stops fetching, cancels running jobs' tokens, waits up to
//!   `shutdown_timeout` for them, then aborts the rest. Jobs that did not
//!   finish go back to their queue without using up an attempt.
//!
//! Several servers can share one backend. Every server sharing a queue must
//! register the same job types: a job whose type a server does not know
//! fails permanently.

mod recurring;
mod retry;

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use jobkeep_core::{
    Extensions, FetchedJob, Job, JobContext, JobError, Outcome, Queue, Recurring, Registry,
    Storage, StorageError, WorkerId,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, debug, error, error_span, info, warn};

pub use recurring::InvalidSchedule;
pub use retry::RetryPolicy;

use recurring::{RecurringJob, RecurringScheduler};

/// Tuning knobs for a [`Server`]. Set through [`ServerBuilder`].
#[derive(Debug, Clone)]
pub struct Config {
    /// Queues to fetch from. Default: `["default"]`.
    pub queues: Vec<Queue>,
    /// Most jobs run at once. Default: 10.
    pub concurrency: usize,
    /// How long a fetched job is reserved for this server without a
    /// heartbeat. Heartbeats are sent every third of it. Default: 30s.
    pub lease: TimeDelta,
    /// How long to wait before polling again when the queues are empty.
    /// Default: 1s.
    pub poll_interval: Duration,
    /// How often maintenance runs. Default: 1s.
    pub maintenance_interval: Duration,
    /// How long running jobs get to finish on shutdown. Default: 30s.
    pub shutdown_timeout: Duration,
    /// Delay between attempts of a failing job.
    pub retry: RetryPolicy,
    /// Succeeded and deleted jobs older than this are purged. `None` keeps
    /// them forever. Default: 7 days.
    pub retention: Option<TimeDelta>,
    /// How often to purge when `retention` is set. Default: 60s.
    pub purge_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queues: vec![Queue::default()],
            concurrency: 10,
            lease: TimeDelta::seconds(30),
            poll_interval: Duration::from_secs(1),
            maintenance_interval: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(30),
            retry: RetryPolicy::default(),
            retention: Some(TimeDelta::days(7)),
            purge_interval: Duration::from_secs(60),
        }
    }
}

/// Builds a [`Server`]. Created with [`Server::builder`].
pub struct ServerBuilder<S> {
    storage: Arc<S>,
    registry: Registry,
    extensions: Extensions,
    recurring: Vec<RecurringJob>,
    config: Config,
}

impl<S: Storage> ServerBuilder<S> {
    /// Registers job type `J` so this server can run it.
    ///
    /// # Panics
    ///
    /// If another job type with the same [`Job::NAME`] is registered.
    pub fn register<J: Job>(mut self) -> Self {
        if let Err(e) = self.registry.register::<J>() {
            panic!("{e}");
        }
        self
    }

    /// Registers a recurring job type: runs it on [`Recurring::SCHEDULE`],
    /// with [`Job::NAME`] as the recurring id.
    ///
    /// # Panics
    ///
    /// If the schedule does not parse, or the name is already taken.
    pub fn recurring<J: Recurring>(self) -> Self {
        match self.recurring_job(J::NAME, J::SCHEDULE, J::default()) {
            Ok(builder) => builder,
            Err(e) => panic!("{e}"),
        }
    }

    /// Runs `job` on a cron `schedule` (with a leading seconds field, in
    /// UTC). `id` names this schedule; it must be unique and stable, since
    /// it is how servers sharing a backend agree on each run.
    ///
    /// Registers `J` too, if it isn't already.
    pub fn recurring_job<J: Job>(
        mut self,
        id: impl Into<String>,
        schedule: &str,
        job: J,
    ) -> Result<Self, InvalidSchedule> {
        let job = RecurringJob::new(id.into(), schedule, &job)?;
        if self.recurring.iter().any(|r| r.id == job.id) {
            panic!("a recurring job with id {:?} is already registered", job.id);
        }
        if !self.registry.contains(J::NAME) {
            self = self.register::<J>();
        }
        self.recurring.push(job);
        Ok(self)
    }

    /// Replaces the registry with one built elsewhere.
    pub fn registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// Makes `value` available to jobs through
    /// [`JobContext::extensions`](jobkeep_core::JobContext).
    pub fn extension<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.extensions.insert(value);
        self
    }

    pub fn queues<Q: Into<Queue>>(mut self, queues: impl IntoIterator<Item = Q>) -> Self {
        self.config.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.config.concurrency = concurrency;
        self
    }

    pub fn lease(mut self, lease: TimeDelta) -> Self {
        self.config.lease = lease;
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    pub fn maintenance_interval(mut self, interval: Duration) -> Self {
        self.config.maintenance_interval = interval;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.config.retry = retry;
        self
    }

    pub fn retention(mut self, retention: Option<TimeDelta>) -> Self {
        self.config.retention = retention;
        self
    }

    pub fn purge_interval(mut self, interval: Duration) -> Self {
        self.config.purge_interval = interval;
        self
    }

    /// Finishes building.
    ///
    /// # Panics
    ///
    /// If the configuration is unusable: no queues, zero concurrency, or a
    /// lease shorter than 30ms.
    pub fn build(self) -> Server<S> {
        let config = self.config;
        assert!(!config.queues.is_empty(), "server needs at least one queue");
        assert!(config.concurrency > 0, "concurrency must be at least 1");
        assert!(
            config.lease >= TimeDelta::milliseconds(30),
            "lease must be at least 30ms"
        );
        let heartbeat_interval = (config.lease / 3).to_std().expect("lease is positive");

        Server {
            inner: Arc::new(Inner {
                storage: self.storage,
                registry: self.registry,
                extensions: Arc::new(self.extensions),
                recurring: self.recurring,
                worker_id: WorkerId::new(),
                heartbeat_interval,
                config,
            }),
        }
    }
}

impl<S: Storage> ServerBuilder<S> {
    /// Builds the server and runs it in the background; shorthand for
    /// `build().spawn()`.
    pub fn spawn(self) -> ServerHandle {
        self.build().spawn()
    }
}

impl<S> std::fmt::Debug for ServerBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("registry", &self.registry)
            .field("recurring", &self.recurring.len())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Runs jobs. See the [crate docs](crate).
pub struct Server<S> {
    inner: Arc<Inner<S>>,
}

struct Inner<S> {
    storage: Arc<S>,
    registry: Registry,
    extensions: Arc<Extensions>,
    recurring: Vec<RecurringJob>,
    worker_id: WorkerId,
    heartbeat_interval: Duration,
    config: Config,
}

impl<S> std::fmt::Debug for Server<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("worker_id", &self.inner.worker_id)
            .field("registry", &self.inner.registry)
            .field("recurring", &self.inner.recurring.len())
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl<S: Storage> Server<S> {
    /// Starts building a server on `storage`. To share a backend with a
    /// client, pass each a clone (backends share their pool or memory when
    /// cloned) or an `Arc`.
    pub fn builder(storage: S) -> ServerBuilder<S> {
        ServerBuilder {
            storage: Arc::new(storage),
            registry: Registry::new(),
            extensions: Extensions::new(),
            recurring: Vec::new(),
            config: Config::default(),
        }
    }

    /// The id this server uses to hold leases.
    pub fn worker_id(&self) -> WorkerId {
        self.inner.worker_id
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Runs until `shutdown` is cancelled, then shuts down gracefully.
    pub async fn run(self, shutdown: CancellationToken) {
        let inner = self.inner;
        info!(
            worker = %inner.worker_id,
            queues = ?inner.config.queues,
            concurrency = inner.config.concurrency,
            job_types = ?inner.registry,
            "jobkeep server started"
        );

        let tracker = TaskTracker::new();
        let hard_stop = CancellationToken::new();
        let maintenance = tokio::spawn(maintenance_loop(Arc::clone(&inner), shutdown.clone()));

        fetch_loop(&inner, &shutdown, &hard_stop, &tracker).await;

        tracker.close();
        if !tracker.is_empty() {
            info!(
                running = tracker.len(),
                "waiting for running jobs to finish"
            );
        }
        if tokio::time::timeout(inner.config.shutdown_timeout, tracker.wait())
            .await
            .is_err()
        {
            warn!(
                running = tracker.len(),
                "shutdown timeout reached; aborting running jobs"
            );
            hard_stop.cancel();
            tracker.wait().await;
        }
        if let Err(e) = maintenance.await {
            error!("maintenance task failed: {e}");
        }
        info!(worker = %inner.worker_id, "jobkeep server stopped");
    }

    /// Runs until Ctrl-C (or SIGTERM on Unix), then shuts down gracefully.
    pub async fn run_until_ctrl_c(self) {
        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            wait_for_signal().await;
            info!("shutdown signal received");
            signal.cancel();
        });
        self.run(shutdown).await;
    }

    /// Runs in a background task. Stop it with [`ServerHandle::shutdown`].
    pub fn spawn(self) -> ServerHandle {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(self.run(shutdown.clone()));
        ServerHandle { shutdown, task }
    }
}

/// A server running in the background, from [`Server::spawn`].
#[derive(Debug)]
pub struct ServerHandle {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl ServerHandle {
    /// Shuts the server down gracefully and waits until it has stopped.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        if let Err(e) = self.task.await {
            error!("server task failed: {e}");
        }
    }
}

async fn wait_for_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("cannot listen for Ctrl-C: {e}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                error!("cannot listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

async fn fetch_loop<S: Storage>(
    inner: &Arc<Inner<S>>,
    shutdown: &CancellationToken,
    hard_stop: &CancellationToken,
    tracker: &TaskTracker,
) {
    let slots = Arc::new(Semaphore::new(inner.config.concurrency));
    loop {
        let permit = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            permit = Arc::clone(&slots).acquire_owned() => permit.expect("semaphore is never closed"),
        };

        let fetched = inner
            .storage
            .fetch(&inner.config.queues, inner.worker_id, inner.config.lease)
            .await;
        match fetched {
            Ok(Some(job)) if shutdown.is_cancelled() => {
                // Fetched just as shutdown began: give it straight back.
                complete(inner, &job, Outcome::Requeue).await;
                return;
            }
            Ok(Some(job)) => {
                tracker.spawn(run_job(
                    Arc::clone(inner),
                    job,
                    permit,
                    shutdown.child_token(),
                    hard_stop.clone(),
                ));
            }
            Ok(None) => {
                drop(permit);
                sleep_unless_cancelled(inner.config.poll_interval, shutdown).await;
            }
            Err(e) => {
                drop(permit);
                error!("fetching jobs failed: {e}");
                sleep_unless_cancelled(inner.config.poll_interval, shutdown).await;
            }
        }
    }
}

async fn sleep_unless_cancelled(duration: Duration, token: &CancellationToken) {
    tokio::select! {
        () = token.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

/// How a job's task ended, before it is turned into an [`Outcome`].
enum RunEnd {
    Finished(Result<Result<(), JobError>, tokio::task::JoinError>),
    /// Aborted at the end of the shutdown timeout.
    Aborted,
    /// The lease was lost; someone else owns the job now.
    LeaseLost,
}

async fn run_job<S: Storage>(
    inner: Arc<Inner<S>>,
    job: FetchedJob,
    _permit: OwnedSemaphorePermit,
    token: CancellationToken,
    hard_stop: CancellationToken,
) {
    // At ERROR level so the job's fields show on its log lines even when
    // only warnings and errors are enabled.
    let span = error_span!("job", id = %job.id, job_type = %job.job_type, attempt = job.attempt);
    async move {
        debug!("job started");
        let started = Instant::now();
        let ctx = JobContext::new(job.id, job.attempt, token.clone())
            .with_extensions(Arc::clone(&inner.extensions));
        let run = inner.registry.run(&job.job_type, job.payload.clone(), ctx);
        // Spawned so a panic in the job is caught as a JoinError.
        let mut task = tokio::spawn(run.in_current_span());

        let mut heartbeat = tokio::time::interval_at(
            Instant::now() + inner.heartbeat_interval,
            inner.heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let end = loop {
            tokio::select! {
                result = &mut task => break RunEnd::Finished(result),
                () = hard_stop.cancelled() => {
                    task.abort();
                    break RunEnd::Aborted;
                }
                _ = heartbeat.tick() => {
                    match inner.storage.heartbeat(job.id, inner.worker_id, inner.config.lease).await {
                        Ok(_) => {}
                        Err(StorageError::LeaseLost(_) | StorageError::NotFound(_)) => {
                            break RunEnd::LeaseLost;
                        }
                        Err(e) => warn!("heartbeat failed: {e}"),
                    }
                }
            }
        };

        let elapsed_ms = started.elapsed().as_millis() as u64;
        let outcome = match end {
            RunEnd::LeaseLost => {
                // Someone else owns the job now, so its result is not ours to
                // save. Ask it to stop, and abort it if it doesn't.
                warn!(elapsed_ms, "lease lost; cancelling job");
                token.cancel();
                let grace = inner.config.lease.to_std().expect("lease is positive");
                tokio::select! {
                    _ = &mut task => {}
                    () = tokio::time::sleep(grace) => task.abort(),
                    () = hard_stop.cancelled() => task.abort(),
                }
                return;
            }
            RunEnd::Aborted => {
                warn!(elapsed_ms, "job aborted at shutdown; requeueing");
                Outcome::Requeue
            }
            RunEnd::Finished(result) => outcome_of(&inner, &job, &token, result),
        };
        match &outcome {
            Outcome::Succeeded => info!(elapsed_ms, "job succeeded"),
            Outcome::Retry { at, error } => warn!(elapsed_ms, retry_at = %at, "job failed, will retry: {error}"),
            Outcome::Failed { error } => error!(elapsed_ms, "job failed: {error}"),
            Outcome::Requeue => info!(elapsed_ms, "job stopped for shutdown; requeueing"),
        }
        complete(&inner, &job, outcome).await;
    }
    .instrument(span)
    .await
}

fn outcome_of<S>(
    inner: &Inner<S>,
    job: &FetchedJob,
    token: &CancellationToken,
    result: Result<Result<(), JobError>, tokio::task::JoinError>,
) -> Outcome {
    let retry_or_fail = |error: String| {
        if job.attempt >= job.max_attempts {
            Outcome::Failed { error }
        } else {
            Outcome::Retry {
                at: Utc::now() + inner.config.retry.delay(job.attempt),
                error,
            }
        }
    };

    match result {
        Ok(Ok(())) => Outcome::Succeeded,
        Ok(Err(JobError::Cancelled)) if token.is_cancelled() => Outcome::Requeue,
        Ok(Err(e @ JobError::Cancelled)) => Outcome::Failed {
            error: format!("{e}, but no cancellation was requested"),
        },
        Ok(Err(e @ JobError::Permanent(_))) => Outcome::Failed {
            error: e.to_string(),
        },
        Ok(Err(e @ JobError::Retryable(_))) => retry_or_fail(e.to_string()),
        Err(e) if e.is_panic() => retry_or_fail(format!("job panicked: {}", panic_message(e))),
        // Only `RunEnd::Aborted` aborts the task, and that never gets here.
        Err(_) => Outcome::Requeue,
    }
}

fn panic_message(e: tokio::task::JoinError) -> String {
    let payload = e.into_panic();
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn complete<S: Storage>(inner: &Inner<S>, job: &FetchedJob, outcome: Outcome) {
    match inner
        .storage
        .complete(job.id, inner.worker_id, outcome)
        .await
    {
        Ok(()) => {}
        Err(StorageError::LeaseLost(_)) => {
            warn!(id = %job.id, "lease lost before the result was saved")
        }
        // The lease will expire and the job will be taken back.
        Err(e) => error!(id = %job.id, "saving job result failed: {e}"),
    }
}

async fn maintenance_loop<S: Storage>(inner: Arc<Inner<S>>, shutdown: CancellationToken) {
    let mut scheduler = RecurringScheduler::new(&inner.recurring, Utc::now());
    let mut last_purge: Option<Instant> = None;
    let mut tick = tokio::time::interval(inner.config.maintenance_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        let storage = &*inner.storage;

        match storage.requeue_expired().await {
            Ok(0) => {}
            Ok(n) => warn!(count = n, "took back jobs whose worker stopped responding"),
            Err(e) => error!("taking back expired jobs failed: {e}"),
        }
        scheduler.enqueue_due(storage, Utc::now()).await;
        match storage.promote_due().await {
            Ok(0) => {}
            Ok(n) => debug!(count = n, "enqueued due scheduled jobs"),
            Err(e) => error!("enqueueing scheduled jobs failed: {e}"),
        }

        if let Some(retention) = inner.config.retention
            && last_purge.is_none_or(|at| at.elapsed() >= inner.config.purge_interval)
        {
            last_purge = Some(Instant::now());
            match storage.purge(Utc::now() - retention).await {
                Ok(0) => {}
                Ok(n) => info!(count = n, "purged old jobs"),
                Err(e) => error!("purging old jobs failed: {e}"),
            }
        }
    }
}
