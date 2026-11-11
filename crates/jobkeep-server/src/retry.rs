//! How long to wait before retrying a failed job.

use chrono::TimeDelta;

/// Delay before each retry: exponential backoff, capped, with optional
/// jitter so jobs that failed together don't all retry together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delay before the first retry.
    pub base: TimeDelta,
    /// Longest delay between attempts.
    pub max: TimeDelta,
    /// Randomize each delay to between half and all of its nominal value.
    pub jitter: bool,
}

impl RetryPolicy {
    /// `base`, `2 * base`, `4 * base`, ... up to `max`, with jitter.
    pub fn exponential(base: TimeDelta, max: TimeDelta) -> Self {
        Self {
            base,
            max,
            jitter: true,
        }
    }

    /// The same delay before every retry, without jitter.
    pub fn fixed(delay: TimeDelta) -> Self {
        Self {
            base: delay,
            max: delay,
            jitter: false,
        }
    }

    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Nominal delay after failed attempt number `attempt` (starting at 1),
    /// before jitter.
    pub fn nominal_delay(&self, attempt: u32) -> TimeDelta {
        let doublings = attempt.saturating_sub(1).min(30);
        self.base
            .checked_mul(1 << doublings)
            .unwrap_or(self.max)
            .min(self.max)
    }

    /// Delay after failed attempt number `attempt` (starting at 1).
    pub fn delay(&self, attempt: u32) -> TimeDelta {
        let nominal = self.nominal_delay(attempt);
        if !self.jitter {
            return nominal;
        }
        let micros = nominal.num_microseconds().unwrap_or(i64::MAX);
        let half = micros / 2;
        let spread = (micros - half).max(1) as u64;
        TimeDelta::microseconds(half + (random_u64() % spread) as i64)
    }
}

impl Default for RetryPolicy {
    /// 2s, 4s, 8s, ... capped at one hour, with jitter.
    fn default() -> Self {
        Self::exponential(TimeDelta::seconds(2), TimeDelta::hours(1))
    }
}

fn random_u64() -> u64 {
    // Jitter needs spread, not cryptographic quality; v4 UUIDs are random
    // and already a dependency.
    uuid::Uuid::new_v4().as_u64_pair().0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_doubles_up_to_max() {
        let p = RetryPolicy::exponential(TimeDelta::seconds(2), TimeDelta::seconds(30));
        let secs: Vec<_> = (1..=6).map(|a| p.nominal_delay(a).num_seconds()).collect();
        assert_eq!(secs, [2, 4, 8, 16, 30, 30]);
        assert_eq!(p.nominal_delay(1_000), TimeDelta::seconds(30));
    }

    #[test]
    fn jitter_stays_within_half_and_full() {
        let p = RetryPolicy::exponential(TimeDelta::seconds(10), TimeDelta::hours(1));
        for _ in 0..1_000 {
            let d = p.delay(1);
            assert!(
                d >= TimeDelta::seconds(5) && d <= TimeDelta::seconds(10),
                "{d}"
            );
        }
    }

    #[test]
    fn fixed_has_no_jitter() {
        let p = RetryPolicy::fixed(TimeDelta::milliseconds(250));
        assert_eq!(p.delay(1), TimeDelta::milliseconds(250));
        assert_eq!(p.delay(9), TimeDelta::milliseconds(250));
    }
}
