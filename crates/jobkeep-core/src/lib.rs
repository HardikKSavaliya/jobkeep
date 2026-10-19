//! Core types and traits for JobKeep.
//!
//! This crate holds the vocabulary shared by every other JobKeep crate:
//! job identity ([`JobId`], [`Queue`], [`Priority`]), the job lifecycle
//! ([`State`]), the user-facing [`Job`] trait with its [`JobContext`] and
//! [`JobError`], and the storage-facing [`NewJob`] record.

mod error;
mod job;
mod state;
mod types;

pub use error::JobError;
pub use job::{Extensions, Job, JobContext, NewJob};
pub use state::State;
pub use types::{JobId, Priority, Queue};

#[cfg(feature = "macros")]
pub use jobkeep_macros::{job, recurring};

/// Re-exported so users of [`JobContext`] don't need a direct `tokio-util` dependency.
pub use tokio_util::sync::CancellationToken;
