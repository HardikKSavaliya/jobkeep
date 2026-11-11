//! PostgreSQL [`Storage`] backend for JobKeep.
//!
//! ```ignore
//! let storage = PgStorage::connect("postgres://localhost/app").await?;
//! storage.migrate().await?;
//! ```
//!
//! Jobs live in one table, `jobkeep_jobs`, created by [`PgStorage::migrate`]
//! in the first schema on the connection's `search_path`. Workers claim jobs
//! with `FOR UPDATE SKIP LOCKED`, so any number of servers can share the
//! table without handing out a job twice. All times come from the database
//! clock (`now()`).

use std::str::FromStr;

use chrono::{DateTime, TimeDelta, Utc};
use jobkeep_core::{
    FetchedJob, JobFilter, JobId, JobRecord, NewJob, Outcome, Priority, Queue, State, Stats,
    Storage, StorageError, WorkerId, ensure_transition, parent_failed_error,
};
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::{PgPool, PgRow};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

/// The schema migrations, for running them with your own migration setup.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Error recorded on a job whose worker stopped renewing its lease.
const LEASE_EXPIRED: &str = "worker lost: lease expired";

/// A job store in PostgreSQL. Cloning shares the connection pool.
#[derive(Debug, Clone)]
pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connects a new pool to `url`.
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        Ok(Self::new(PgPool::connect(url).await?))
    }

    /// Creates or upgrades the job table.
    pub async fn migrate(&self) -> Result<(), MigrateError> {
        MIGRATOR.run(&self.pool).await
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

fn db(e: sqlx::Error) -> StorageError {
    StorageError::Backend(anyhow::Error::new(e))
}

fn corrupt(msg: String) -> StorageError {
    StorageError::Backend(anyhow::anyhow!("corrupt row in jobkeep_jobs: {msg}"))
}

fn micros(d: TimeDelta) -> i64 {
    // Out-of-range leases would overflow the timestamp; a century is plenty.
    d.num_microseconds()
        .unwrap_or(i64::MAX)
        .clamp(-3_155_760_000_000_000, 3_155_760_000_000_000)
}

fn to_i32(n: u32) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

fn to_u32(n: i32) -> u32 {
    u32::try_from(n).unwrap_or(0)
}

fn parse_state(s: &str) -> Result<State, StorageError> {
    State::from_str(s).map_err(|e| corrupt(e.to_string()))
}

fn record(row: &PgRow) -> Result<JobRecord, StorageError> {
    let get_err = |e: sqlx::Error| corrupt(e.to_string());
    Ok(JobRecord {
        id: JobId::from_uuid(row.try_get("id").map_err(get_err)?),
        job_type: row.try_get("job_type").map_err(get_err)?,
        payload: row.try_get("payload").map_err(get_err)?,
        queue: Queue::new(row.try_get::<String, _>("queue").map_err(get_err)?),
        priority: Priority(row.try_get("priority").map_err(get_err)?),
        state: parse_state(row.try_get("state").map_err(get_err)?)?,
        attempt: to_u32(row.try_get("attempt").map_err(get_err)?),
        max_attempts: to_u32(row.try_get("max_attempts").map_err(get_err)?),
        run_at: row.try_get("run_at").map_err(get_err)?,
        created_at: row.try_get("created_at").map_err(get_err)?,
        updated_at: row.try_get("updated_at").map_err(get_err)?,
        last_error: row.try_get("last_error").map_err(get_err)?,
        worker: row
            .try_get::<Option<Uuid>, _>("worker")
            .map_err(get_err)?
            .map(WorkerId::from_uuid),
        lease_expires_at: row.try_get("lease_expires_at").map_err(get_err)?,
        parent: row
            .try_get::<Option<Uuid>, _>("parent_id")
            .map_err(get_err)?
            .map(JobId::from_uuid),
        unique_key: row.try_get("unique_key").map_err(get_err)?,
    })
}

/// Locks a job row and returns its state, or `NotFound`.
async fn lock_state(
    tx: &mut Transaction<'_, Postgres>,
    id: JobId,
) -> Result<(State, Option<WorkerId>), StorageError> {
    let row = sqlx::query("SELECT state, worker FROM jobkeep_jobs WHERE id = $1 FOR UPDATE")
        .bind(id.as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .ok_or(StorageError::NotFound(id))?;
    let state = parse_state(row.try_get("state").map_err(db)?)?;
    let worker = row
        .try_get::<Option<Uuid>, _>("worker")
        .map_err(db)?
        .map(WorkerId::from_uuid);
    Ok((state, worker))
}

/// Moves continuations awaiting `parent` to `enqueued`.
async fn release_children(
    tx: &mut Transaction<'_, Postgres>,
    parent: JobId,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE jobkeep_jobs SET state = 'enqueued', run_at = now(), updated_at = now() \
         WHERE parent_id = $1 AND state = 'awaiting'",
    )
    .bind(parent.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

/// Deletes continuations awaiting any of `parents`, and theirs, recursively.
async fn delete_children(
    tx: &mut Transaction<'_, Postgres>,
    parents: &[Uuid],
) -> Result<(), StorageError> {
    if parents.is_empty() {
        return Ok(());
    }
    // The message matches `jobkeep_core::parent_failed_error`; `uuid::text`
    // is the same hyphenated form as `Uuid`'s Display.
    debug_assert_eq!(
        parent_failed_error(JobId::from_uuid(Uuid::nil())),
        format!("parent job {} did not succeed", Uuid::nil())
    );
    sqlx::query(
        "WITH RECURSIVE doomed AS ( \
             SELECT id FROM jobkeep_jobs WHERE parent_id = ANY($1) AND state = 'awaiting' \
             UNION \
             SELECT j.id FROM jobkeep_jobs j JOIN doomed d ON j.parent_id = d.id \
             WHERE j.state = 'awaiting' \
         ) \
         UPDATE jobkeep_jobs \
         SET state = 'deleted', updated_at = now(), \
             last_error = 'parent job ' || parent_id::text || ' did not succeed' \
         WHERE id IN (SELECT id FROM doomed)",
    )
    .bind(parents)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

impl Storage for PgStorage {
    async fn enqueue(&self, job: NewJob) -> Result<JobId, StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;

        if let Some(key) = &job.unique_key {
            let existing: Option<Uuid> =
                sqlx::query_scalar("SELECT id FROM jobkeep_jobs WHERE unique_key = $1")
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            if let Some(id) = existing {
                return Ok(JobId::from_uuid(id));
            }
        }

        // `now()` is fixed for the whole transaction.
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;

        let run_at = job.run_at.unwrap_or(now);
        let mut last_error = None;
        let state = match job.parent {
            Some(parent) => {
                // FOR SHARE: the parent cannot finish between this check and
                // our insert, or the continuation would wait forever.
                let parent_state: String =
                    sqlx::query_scalar("SELECT state FROM jobkeep_jobs WHERE id = $1 FOR SHARE")
                        .bind(parent.as_uuid())
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(db)?
                        .ok_or(StorageError::NotFound(parent))?;
                match parse_state(&parent_state)? {
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
        let inserted: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO jobkeep_jobs \
                 (id, job_type, payload, queue, priority, state, max_attempts, run_at, \
                  created_at, updated_at, last_error, parent_id, unique_key) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11, $12) \
             ON CONFLICT (unique_key) DO NOTHING \
             RETURNING id",
        )
        .bind(id.as_uuid())
        .bind(&job.job_type)
        .bind(&job.payload)
        .bind(job.queue.as_str())
        .bind(job.priority.0)
        .bind(state.as_str())
        .bind(to_i32(job.max_attempts))
        .bind(run_at)
        .bind(now)
        .bind(last_error)
        .bind(job.parent.map(|p| *p.as_uuid()))
        .bind(&job.unique_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        let id = match inserted {
            Some(id) => JobId::from_uuid(id),
            // Another transaction stored the same key after our check.
            None => {
                let id: Uuid =
                    sqlx::query_scalar("SELECT id FROM jobkeep_jobs WHERE unique_key = $1")
                        .bind(&job.unique_key)
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(db)?;
                JobId::from_uuid(id)
            }
        };
        tx.commit().await.map_err(db)?;
        Ok(id)
    }

    async fn fetch(
        &self,
        queues: &[Queue],
        worker: WorkerId,
        lease: TimeDelta,
    ) -> Result<Option<FetchedJob>, StorageError> {
        let queues: Vec<&str> = queues.iter().map(Queue::as_str).collect();
        let row = sqlx::query(
            "UPDATE jobkeep_jobs \
             SET state = 'processing', attempt = attempt + 1, worker = $2, \
                 lease_expires_at = now() + $3 * interval '1 microsecond', updated_at = now() \
             WHERE id = ( \
                 SELECT id FROM jobkeep_jobs \
                 WHERE state = 'enqueued' AND queue = ANY($1) \
                 ORDER BY priority DESC, run_at, id \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             RETURNING id, job_type, payload, queue, priority, attempt, max_attempts, \
                       lease_expires_at",
        )
        .bind(&queues)
        .bind(worker.as_uuid())
        .bind(micros(lease))
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        let Some(row) = row else { return Ok(None) };
        let get_err = |e: sqlx::Error| corrupt(e.to_string());
        Ok(Some(FetchedJob {
            id: JobId::from_uuid(row.try_get("id").map_err(get_err)?),
            job_type: row.try_get("job_type").map_err(get_err)?,
            payload: row.try_get("payload").map_err(get_err)?,
            queue: Queue::new(row.try_get::<String, _>("queue").map_err(get_err)?),
            priority: Priority(row.try_get("priority").map_err(get_err)?),
            attempt: to_u32(row.try_get("attempt").map_err(get_err)?),
            max_attempts: to_u32(row.try_get("max_attempts").map_err(get_err)?),
            lease_expires_at: row.try_get("lease_expires_at").map_err(get_err)?,
        }))
    }

    async fn heartbeat(
        &self,
        id: JobId,
        worker: WorkerId,
        lease: TimeDelta,
    ) -> Result<DateTime<Utc>, StorageError> {
        let expires: Option<DateTime<Utc>> = sqlx::query_scalar(
            "UPDATE jobkeep_jobs \
             SET lease_expires_at = now() + $3 * interval '1 microsecond', updated_at = now() \
             WHERE id = $1 AND state = 'processing' AND worker = $2 \
             RETURNING lease_expires_at",
        )
        .bind(id.as_uuid())
        .bind(worker.as_uuid())
        .bind(micros(lease))
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        if let Some(expires) = expires {
            return Ok(expires);
        }

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobkeep_jobs WHERE id = $1)")
                .bind(id.as_uuid())
                .fetch_one(&self.pool)
                .await
                .map_err(db)?;
        Err(if exists {
            StorageError::LeaseLost(id)
        } else {
            StorageError::NotFound(id)
        })
    }

    async fn complete(
        &self,
        id: JobId,
        worker: WorkerId,
        outcome: Outcome,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let (state, holder) = lock_state(&mut tx, id).await?;
        if state != State::Processing || holder != Some(worker) {
            return Err(StorageError::LeaseLost(id));
        }
        let to = outcome.target_state();
        ensure_transition(id, state, to)?;

        let (run_at, give_back_attempt) = match &outcome {
            Outcome::Retry { at, .. } => (Some(*at), 0),
            Outcome::Requeue => (None, 1),
            Outcome::Succeeded | Outcome::Failed { .. } => (None, 0),
        };
        sqlx::query(
            "UPDATE jobkeep_jobs \
             SET state = $2, last_error = COALESCE($3, last_error), run_at = COALESCE($4, run_at), \
                 attempt = GREATEST(attempt - $5, 0), worker = NULL, lease_expires_at = NULL, \
                 updated_at = now() \
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .bind(to.as_str())
        .bind(outcome.error())
        .bind(run_at)
        .bind(give_back_attempt)
        .execute(&mut *tx)
        .await
        .map_err(db)?;

        match outcome {
            Outcome::Succeeded => release_children(&mut tx, id).await?,
            Outcome::Failed { .. } => delete_children(&mut tx, &[*id.as_uuid()]).await?,
            Outcome::Retry { .. } | Outcome::Requeue => {}
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn promote_due(&self) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE jobkeep_jobs SET state = 'enqueued', updated_at = now() \
             WHERE state = 'scheduled' AND run_at <= now()",
        )
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(result.rows_affected())
    }

    async fn requeue_expired(&self) -> Result<u64, StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let rows = sqlx::query(
            "UPDATE jobkeep_jobs \
             SET state = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'enqueued' END, \
                 last_error = $1, worker = NULL, lease_expires_at = NULL, updated_at = now() \
             WHERE state = 'processing' AND lease_expires_at <= now() \
             RETURNING id, state",
        )
        .bind(LEASE_EXPIRED)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;

        let mut failed = Vec::new();
        for row in &rows {
            if row.try_get::<&str, _>("state").map_err(db)? == State::Failed.as_str() {
                failed.push(row.try_get::<Uuid, _>("id").map_err(db)?);
            }
        }
        delete_children(&mut tx, &failed).await?;
        tx.commit().await.map_err(db)?;
        Ok(rows.len() as u64)
    }

    async fn get(&self, id: JobId) -> Result<Option<JobRecord>, StorageError> {
        sqlx::query("SELECT * FROM jobkeep_jobs WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .as_ref()
            .map(record)
            .transpose()
    }

    async fn delete(&self, id: JobId) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let (state, _) = lock_state(&mut tx, id).await?;
        ensure_transition(id, state, State::Deleted)?;
        sqlx::query(
            "UPDATE jobkeep_jobs \
             SET state = 'deleted', worker = NULL, lease_expires_at = NULL, updated_at = now() \
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        delete_children(&mut tx, &[*id.as_uuid()]).await?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn requeue(&self, id: JobId) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let (state, _) = lock_state(&mut tx, id).await?;
        // The state machine also lets Scheduled, Awaiting and Processing
        // reach Enqueued, but those are the server's moves, not a user's.
        if !matches!(state, State::Succeeded | State::Failed | State::Deleted) {
            return Err(StorageError::InvalidTransition {
                id,
                from: state,
                to: State::Enqueued,
            });
        }
        ensure_transition(id, state, State::Enqueued)?;
        sqlx::query(
            "UPDATE jobkeep_jobs \
             SET state = 'enqueued', attempt = 0, last_error = NULL, run_at = now(), \
                 updated_at = now() \
             WHERE id = $1",
        )
        .bind(id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    async fn stats(&self) -> Result<Stats, StorageError> {
        let rows = sqlx::query("SELECT state, count(*) AS n FROM jobkeep_jobs GROUP BY state")
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;
        let mut stats = Stats::default();
        for row in rows {
            let state = parse_state(row.try_get("state").map_err(db)?)?;
            let n: i64 = row.try_get("n").map_err(db)?;
            stats.counts.insert(state, n.max(0) as u64);
        }
        Ok(stats)
    }

    async fn list(&self, filter: &JobFilter) -> Result<Vec<JobRecord>, StorageError> {
        // Ids are UUIDv7 and Postgres compares UUIDs bytewise, so descending
        // id is newest first.
        let rows = sqlx::query(
            "SELECT * FROM jobkeep_jobs \
             WHERE ($1::text IS NULL OR state = $1) \
               AND ($2::text IS NULL OR queue = $2) \
               AND ($3::text IS NULL OR job_type = $3) \
             ORDER BY id DESC \
             LIMIT $4 OFFSET $5",
        )
        .bind(filter.state.map(|s| s.as_str()))
        .bind(filter.queue.as_ref().map(Queue::as_str))
        .bind(filter.job_type.as_deref())
        .bind(i64::from(filter.effective_limit()))
        .bind(i64::from(filter.offset))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.iter().map(record).collect()
    }

    async fn purge(&self, older_than: DateTime<Utc>) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM jobkeep_jobs \
             WHERE state IN ('succeeded', 'deleted') AND updated_at < $1",
        )
        .bind(older_than)
        .execute(&self.pool)
        .await
        .map_err(db)?;
        Ok(result.rows_affected())
    }
}
