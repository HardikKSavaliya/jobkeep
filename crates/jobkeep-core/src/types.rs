//! Identity types: [`JobId`], [`Queue`] and [`Priority`].

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier of a job.
///
/// New ids are UUIDv7, so they sort by creation time, which keeps
/// B-tree indexes in storage backends compact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Generates a new, time-ordered job id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID, e.g. one loaded from storage.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for JobId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<JobId> for Uuid {
    fn from(id: JobId) -> Self {
        id.0
    }
}

/// Name of the queue a job is placed on. Workers subscribe to one or more queues.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Queue(String);

impl Queue {
    /// Name of the queue used when none is specified.
    pub const DEFAULT: &'static str = "default";

    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Queue {
    fn default() -> Self {
        Self::new(Self::DEFAULT)
    }
}

impl fmt::Display for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Queue {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for Queue {
    fn from(name: String) -> Self {
        Self(name)
    }
}

/// Fetch priority within a queue. Higher values are fetched first.
///
/// Stored as an `i16` so it maps directly onto a Postgres `SMALLINT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(pub i16);

impl Priority {
    pub const LOW: Self = Self(-10);
    pub const NORMAL: Self = Self(0);
    pub const HIGH: Self = Self(10);
    pub const CRITICAL: Self = Self(20);
}

impl Default for Priority {
    fn default() -> Self {
        Self::NORMAL
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_id_round_trips_through_string_and_json() {
        let id = JobId::new();
        assert_eq!(id.to_string().parse::<JobId>().unwrap(), id);

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(serde_json::from_str::<JobId>(&json).unwrap(), id);
    }

    #[test]
    fn job_ids_are_time_ordered() {
        let a = JobId::new();
        let b = JobId::new();
        assert!(a < b);
    }

    #[test]
    fn queue_defaults_to_default() {
        assert_eq!(Queue::default().as_str(), "default");
        assert_eq!(
            serde_json::to_string(&Queue::from("emails")).unwrap(),
            "\"emails\""
        );
    }

    #[test]
    fn priority_orders_high_before_low() {
        assert_eq!(Priority::default(), Priority::NORMAL);
        assert!(Priority::CRITICAL > Priority::HIGH);
        assert!(Priority::HIGH > Priority::NORMAL);
        assert!(Priority::NORMAL > Priority::LOW);
    }
}
