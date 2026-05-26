//! Token budget for a federated query.

/// How many records a single membrane query is allowed to surface.
///
/// `max_remote_records` caps the total returned from peer tenants;
/// `max_total_records` caps the merged set (local + remote) returned
/// to the caller.
#[derive(Copy, Clone, Debug)]
pub struct TokenBudget {
    /// Upper bound on records returned by peer fan-out.
    pub max_remote_records: u32,
    /// Upper bound on records returned to the caller after merge.
    pub max_total_records: u32,
}

impl TokenBudget {
    /// Symmetric budget: both caps set to `n`.
    pub fn flat(n: u32) -> Self {
        Self {
            max_remote_records: n,
            max_total_records: n,
        }
    }

    /// Asymmetric: tighter remote cap (saves bandwidth), looser total cap.
    pub fn split(remote: u32, total: u32) -> Self {
        Self {
            max_remote_records: remote,
            max_total_records: total,
        }
    }
}

impl Default for TokenBudget {
    /// Sensible default for one-user/few-agents scenarios: 32 remote / 64 total.
    fn default() -> Self {
        Self {
            max_remote_records: 32,
            max_total_records: 64,
        }
    }
}
