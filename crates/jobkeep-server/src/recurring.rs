//! Recurring jobs: enqueue a job every time its cron schedule fires.
//!
//! Each run is enqueued with the unique key `recurring:{id}:{timestamp}`, so
//! when several servers share a backend and all notice the same tick, only
//! one job is stored. Ticks missed while no server was running are not
//! caught up; if several ticks are missed while running (a long stall),
//! only the latest one is enqueued.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use cron::Schedule;
use jobkeep_core::{Job, NewJob, Storage};
use tracing::{debug, error, warn};

/// A cron schedule that does not parse.
#[derive(Debug, thiserror::Error)]
#[error("invalid schedule {schedule:?} for recurring job {id:?}: {source}")]
pub struct InvalidSchedule {
    pub id: String,
    pub schedule: String,
    #[source]
    pub source: cron::error::Error,
}

pub(crate) struct RecurringJob {
    pub(crate) id: String,
    schedule: Schedule,
    template: NewJob,
}

impl RecurringJob {
    pub(crate) fn new<J: Job>(
        id: String,
        schedule: &str,
        job: &J,
    ) -> Result<Self, InvalidSchedule> {
        let parsed = Schedule::from_str(schedule).map_err(|source| InvalidSchedule {
            id: id.clone(),
            schedule: schedule.to_owned(),
            source,
        })?;
        let template = NewJob::from_job(job).expect("job payload must serialize to JSON");
        Ok(Self {
            id,
            schedule: parsed,
            template,
        })
    }
}

/// Tracks the next fire time of each recurring job.
pub(crate) struct RecurringScheduler<'a> {
    entries: Vec<(&'a RecurringJob, Option<DateTime<Utc>>)>,
}

impl<'a> RecurringScheduler<'a> {
    pub(crate) fn new(jobs: &'a [RecurringJob], now: DateTime<Utc>) -> Self {
        let entries = jobs
            .iter()
            .map(|job| (job, job.schedule.after(&now).next()))
            .collect();
        Self { entries }
    }

    /// Enqueues every recurring job whose next tick is at or before `now`.
    pub(crate) async fn enqueue_due<S: Storage>(&mut self, storage: &S, now: DateTime<Utc>) {
        for (job, next) in &mut self.entries {
            let Some(due) = *next else { continue };
            if due > now {
                continue;
            }

            // Skip to the latest tick that has passed.
            let mut latest = due;
            let mut skipped = 0;
            let mut upcoming = job.schedule.after(&due);
            let following = loop {
                match upcoming.next() {
                    Some(t) if t <= now => {
                        latest = t;
                        skipped += 1;
                    }
                    other => break other,
                }
            };
            if skipped > 0 {
                warn!(id = %job.id, skipped, "recurring job missed ticks; running the latest only");
            }

            let new_job = job
                .template
                .clone()
                .with_run_at(latest)
                .with_unique_key(format!("recurring:{}:{}", job.id, latest.timestamp()));
            match storage.enqueue(new_job).await {
                Ok(id) => {
                    debug!(recurring = %job.id, %id, tick = %latest, "enqueued recurring job");
                    *next = following;
                }
                // Keep `next` so the tick is retried on the next pass.
                Err(e) => error!(recurring = %job.id, "enqueueing recurring job failed: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use jobkeep_core::{JobContext, JobError, State};
    use jobkeep_memory::MemoryStorage;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Default, Serialize, Deserialize)]
    struct Tick;

    impl Job for Tick {
        const NAME: &'static str = "tick";

        async fn run(&self, _ctx: &JobContext) -> Result<(), JobError> {
            Ok(())
        }
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn rejects_bad_schedule() {
        let err = RecurringJob::new("tick".into(), "every tuesday", &Tick)
            .err()
            .unwrap();
        assert!(err.to_string().contains("\"every tuesday\""), "{err}");
    }

    #[tokio::test]
    async fn enqueues_each_tick_once() {
        let jobs = [RecurringJob::new("tick".into(), "0 * * * * *", &Tick).unwrap()];
        let storage = MemoryStorage::new();
        let start = at("2026-11-08T10:00:30Z");
        let mut scheduler = RecurringScheduler::new(&jobs, start);

        scheduler.enqueue_due(&storage, start).await;
        assert!(storage.is_empty());

        scheduler
            .enqueue_due(&storage, at("2026-11-08T10:01:00Z"))
            .await;
        scheduler
            .enqueue_due(&storage, at("2026-11-08T10:01:20Z"))
            .await;
        assert_eq!(storage.len(), 1);

        scheduler
            .enqueue_due(&storage, at("2026-11-08T10:02:05Z"))
            .await;
        assert_eq!(storage.len(), 2);

        let jobs = storage.list(&Default::default()).await.unwrap();
        let mut keys: Vec<_> = jobs.iter().filter_map(|j| j.unique_key.clone()).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                format!("recurring:tick:{}", at("2026-11-08T10:01:00Z").timestamp()),
                format!("recurring:tick:{}", at("2026-11-08T10:02:00Z").timestamp()),
            ]
        );
        assert!(
            jobs.iter()
                .all(|j| j.state == State::Enqueued && j.job_type == "tick")
        );
    }

    #[tokio::test]
    async fn missed_ticks_collapse_to_latest() {
        let jobs = [RecurringJob::new("tick".into(), "0 * * * * *", &Tick).unwrap()];
        let storage = MemoryStorage::new();
        let start = at("2026-11-08T10:00:30Z");
        let mut scheduler = RecurringScheduler::new(&jobs, start);

        scheduler
            .enqueue_due(&storage, start + TimeDelta::minutes(10))
            .await;
        let jobs = storage.list(&Default::default()).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].run_at, at("2026-11-08T10:10:00Z"));
    }

    #[tokio::test]
    async fn two_schedulers_share_ticks() {
        let jobs = [RecurringJob::new("tick".into(), "0 * * * * *", &Tick).unwrap()];
        let storage = MemoryStorage::new();
        let start = at("2026-11-08T10:00:30Z");
        let mut a = RecurringScheduler::new(&jobs, start);
        let mut b = RecurringScheduler::new(&jobs, start + TimeDelta::seconds(5));

        let now = at("2026-11-08T10:01:01Z");
        a.enqueue_due(&storage, now).await;
        b.enqueue_due(&storage, now).await;
        assert_eq!(storage.len(), 1);
    }
}
