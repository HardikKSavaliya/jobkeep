//! Errors returned from [`Job::run`](crate::Job::run).

/// The outcome of a failed job run. Tells the server what to do next.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// A transient failure. The job is retried with backoff until it runs
    /// out of attempts.
    ///
    /// `?` on an [`anyhow::Error`] produces this variant.
    #[error("retryable error: {0:#}")]
    Retryable(#[from] anyhow::Error),

    /// The job can never succeed. It moves straight to `Failed`.
    #[error("permanent error: {0}")]
    Permanent(String),

    /// The job stopped because its cancellation token fired.
    #[error("job was cancelled")]
    Cancelled,
}

impl JobError {
    /// Builds a [`JobError::Retryable`] from any error type.
    pub fn retryable<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Retryable(anyhow::Error::new(err))
    }

    /// Builds a [`JobError::Permanent`] from a message.
    pub fn permanent(msg: impl Into<String>) -> Self {
        Self::Permanent(msg.into())
    }

    /// Returns whether the server should schedule another attempt.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    fn fails() -> Result<(), JobError> {
        Err(std::io::Error::other("connection reset")).context("calling smtp")?;
        Ok(())
    }

    #[test]
    fn question_mark_on_anyhow_is_retryable() {
        let err = fails().unwrap_err();
        assert!(err.is_retryable());
        assert_eq!(
            err.to_string(),
            "retryable error: calling smtp: connection reset"
        );
    }

    #[test]
    fn permanent_and_cancelled_are_not_retryable() {
        assert!(!JobError::permanent("bad input").is_retryable());
        assert!(!JobError::Cancelled.is_retryable());
    }
}
