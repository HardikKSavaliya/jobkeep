-- JobKeep job table. Tables are unqualified, so they land in the first
-- schema on the connection's search_path.

CREATE TABLE jobkeep_jobs (
    id               UUID        PRIMARY KEY,
    job_type         TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    queue            TEXT        NOT NULL,
    priority         SMALLINT    NOT NULL,
    state            TEXT        NOT NULL CHECK (state IN (
                         'enqueued', 'scheduled', 'awaiting', 'processing',
                         'succeeded', 'failed', 'deleted')),
    attempt          INTEGER     NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts     INTEGER     NOT NULL CHECK (max_attempts >= 0),
    run_at           TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    last_error       TEXT,
    worker           UUID,
    lease_expires_at TIMESTAMPTZ,
    -- Purging a finished parent must not purge or block its continuations.
    parent_id        UUID        REFERENCES jobkeep_jobs (id) ON DELETE SET NULL,
    unique_key       TEXT        UNIQUE
);

-- fetch: next ready job per queue, highest priority then oldest.
CREATE INDEX jobkeep_jobs_ready ON jobkeep_jobs (queue, priority DESC, run_at, id)
    WHERE state = 'enqueued';
-- promote_due
CREATE INDEX jobkeep_jobs_scheduled ON jobkeep_jobs (run_at)
    WHERE state = 'scheduled';
-- requeue_expired
CREATE INDEX jobkeep_jobs_leases ON jobkeep_jobs (lease_expires_at)
    WHERE state = 'processing';
-- releasing and deleting continuations
CREATE INDEX jobkeep_jobs_parent ON jobkeep_jobs (parent_id)
    WHERE parent_id IS NOT NULL;
-- stats, filtered lists and purge
CREATE INDEX jobkeep_jobs_state_updated ON jobkeep_jobs (state, updated_at);
