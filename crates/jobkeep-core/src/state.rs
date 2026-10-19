//! The job lifecycle state machine.
//!
//! ```text
//!   Scheduled ──┐            ┌──────────── retry with backoff ─────────────┐
//!               ▼            ▼                                             │
//!   Awaiting ─► Enqueued ─► Processing ─► Succeeded                        │
//!               ▲   ▲        │  │  └────► Failed (attempts exhausted /     │
//!               │   │        │  │                 permanent error)         │
//!               │   │        │  └──────────────────────────────────────────┘
//!               │   └────────┘  worker lost / shutdown: requeue
//!               │
//!               └── manual requeue from Succeeded, Failed or Deleted
//!
//!   Every non-deleted state can move to Deleted.
//! ```

use std::fmt;

use serde::{Deserialize, Serialize};

/// Lifecycle state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Ready to be picked up by a worker.
    Enqueued,
    /// Waiting for its `run_at` time; enqueued once it arrives.
    Scheduled,
    /// Waiting for a parent job to finish (continuations).
    Awaiting,
    /// Currently being run by a worker.
    Processing,
    /// Finished successfully.
    Succeeded,
    /// Failed permanently or ran out of attempts.
    Failed,
    /// Removed by a user or cancelled; kept for history until expired.
    Deleted,
}

impl State {
    /// All states, in declaration order.
    pub const ALL: [State; 7] = [
        State::Enqueued,
        State::Scheduled,
        State::Awaiting,
        State::Processing,
        State::Succeeded,
        State::Failed,
        State::Deleted,
    ];

    /// Returns whether a job in `self` may move to `next`.
    ///
    /// Self-transitions are never allowed.
    pub fn can_transition_to(&self, next: State) -> bool {
        use State::*;

        matches!(
            (*self, next),
            // Time arrived / parent finished.
            (Scheduled | Awaiting, Enqueued)
            // A worker fetched it.
            | (Enqueued, Processing)
            // Outcome of a run: success, permanent failure, retry with
            // backoff, or requeue when the worker died or shut down.
            | (Processing, Succeeded | Failed | Scheduled | Enqueued)
            // Manual requeue from the dashboard or client.
            | (Succeeded | Failed | Deleted, Enqueued)
            // Anything not already deleted can be deleted.
            | (Enqueued | Scheduled | Awaiting | Processing | Succeeded | Failed, Deleted)
        )
    }

    /// Returns whether the job is done and only kept around for history.
    pub fn is_final(&self) -> bool {
        matches!(self, State::Succeeded | State::Deleted)
    }

    /// Stable lowercase name, matching the serde representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Enqueued => "enqueued",
            State::Scheduled => "scheduled",
            State::Awaiting => "awaiting",
            State::Processing => "processing",
            State::Succeeded => "succeeded",
            State::Failed => "failed",
            State::Deleted => "deleted",
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::State::{self, *};

    /// The full transition table, written out so a change to the diagram
    /// has to be made deliberately in two places.
    const ALLOWED: &[(State, State)] = &[
        (Scheduled, Enqueued),
        (Scheduled, Deleted),
        (Awaiting, Enqueued),
        (Awaiting, Deleted),
        (Enqueued, Processing),
        (Enqueued, Deleted),
        (Processing, Succeeded),
        (Processing, Failed),
        (Processing, Scheduled),
        (Processing, Enqueued),
        (Processing, Deleted),
        (Succeeded, Enqueued),
        (Succeeded, Deleted),
        (Failed, Enqueued),
        (Failed, Deleted),
        (Deleted, Enqueued),
    ];

    #[test]
    fn transition_table_matches_diagram() {
        for from in State::ALL {
            for to in State::ALL {
                let expected = ALLOWED.contains(&(from, to));
                assert_eq!(
                    from.can_transition_to(to),
                    expected,
                    "{from} -> {to} should be {}",
                    if expected { "allowed" } else { "rejected" }
                );
            }
        }
    }

    #[test]
    fn no_self_transitions() {
        for s in State::ALL {
            assert!(!s.can_transition_to(s), "{s} -> {s}");
        }
    }

    #[test]
    fn serde_name_matches_as_str() {
        for s in State::ALL {
            assert_eq!(
                serde_json::to_string(&s).unwrap(),
                format!("\"{}\"", s.as_str())
            );
        }
    }
}
