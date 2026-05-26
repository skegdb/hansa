//! Proportional allocation of a remote budget across scored peers.

use crate::MemberRecord;

/// Hard cap on how many peers a single membrane query fans out to.
/// Saga-scored top-N is the relevant subset; larger fan-outs cost
/// latency without much recall gain.
pub const DEFAULT_MAX_PEERS: usize = 8;

/// One peer's share of the remote budget.
#[derive(Debug, Clone)]
pub struct PeerAllocation {
    /// The peer to query.
    pub member: MemberRecord,
    /// How many records this peer may return.
    pub budget: u32,
}

/// Allocate `total` records across `scored` peers, proportionally to
/// their saga scores, with `min_per_peer` floor and an overall fan-out
/// cap of `max_peers`.
///
/// Peers with scores below `min_similarity` are dropped *before*
/// allocation - i.e. they get no budget at all. If after filtering no
/// peer remains, returns an empty allocation.
///
/// Scores must already be in descending order. Caller is responsible
/// for sorting + filtering.
pub fn proportional_allocation(
    scored: Vec<(MemberRecord, f32)>,
    total: u32,
    min_per_peer: u32,
    max_peers: usize,
) -> Vec<PeerAllocation> {
    if scored.is_empty() || total == 0 || max_peers == 0 {
        return Vec::new();
    }

    let trimmed: Vec<(MemberRecord, f32)> = scored.into_iter().take(max_peers).collect();

    // Shift scores so the lowest is at zero, then add an epsilon to
    // avoid the degenerate "all scores equal" case making sum=0.
    let min_score = trimmed
        .iter()
        .map(|(_, s)| *s)
        .fold(f32::INFINITY, f32::min);
    let shifted: Vec<f32> = trimmed
        .iter()
        .map(|(_, s)| (*s - min_score) + 1e-3)
        .collect();
    let sum: f64 = shifted.iter().map(|&s| s as f64).sum();

    // First pass: floor + proportional share.
    let mut alloc: Vec<u32> = shifted
        .iter()
        .map(|&s| {
            let share = ((s as f64 / sum) * total as f64).floor() as u32;
            share.max(min_per_peer)
        })
        .collect();

    // If we over-spent (min floor pushed totals above `total`), trim from
    // the lowest-scored peer downward.
    let mut spent: u32 = alloc.iter().sum();
    while spent > total && !alloc.is_empty() {
        // Trim from lowest-scored allocations (end of vec).
        if let Some(slot) = alloc.iter_mut().rev().find(|v| **v > min_per_peer) {
            *slot -= 1;
            spent -= 1;
        } else {
            // Every slot is at min_per_peer - drop the last peer entirely.
            let drop_idx = alloc.len() - 1;
            spent -= alloc[drop_idx];
            alloc.remove(drop_idx);
        }
    }

    // If we under-spent, distribute the remainder to top peers (front of vec).
    let mut idx = 0usize;
    while spent < total && !alloc.is_empty() {
        let n = alloc.len();
        alloc[idx % n] += 1;
        spent += 1;
        idx += 1;
    }

    let n = alloc.len();
    trimmed
        .into_iter()
        .take(n)
        .zip(alloc)
        .map(|((member, _), budget)| PeerAllocation { member, budget })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use skeg_rigging::TenantId;
    use skeg_rigging_net::TenantLocation;
    use std::path::PathBuf;

    fn peer(seed: u8) -> MemberRecord {
        MemberRecord {
            tenant_id: TenantId::from_bytes([seed; 16]),
            tenant_location: TenantLocation::Path {
                path: PathBuf::from(format!("/t/{seed}")),
            },
            embedding_dim: 4,
            joined_at: 0,
        }
    }

    #[test]
    fn empty_input_yields_empty() {
        let r = proportional_allocation(vec![], 10, 1, 8);
        assert!(r.is_empty());
    }

    #[test]
    fn allocation_sums_to_total() {
        let scored = vec![(peer(1), 0.9), (peer(2), 0.5), (peer(3), 0.1)];
        let r = proportional_allocation(scored, 30, 1, 8);
        let total: u32 = r.iter().map(|a| a.budget).sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn top_peer_gets_largest_share() {
        let scored = vec![(peer(1), 0.9), (peer(2), 0.1)];
        let r = proportional_allocation(scored, 20, 1, 8);
        assert!(r[0].budget > r[1].budget, "got {:?}", r);
    }

    #[test]
    fn max_peers_cap_is_enforced() {
        let scored: Vec<_> = (1u8..=12).map(|s| (peer(s), 1.0 - s as f32 * 0.01)).collect();
        let r = proportional_allocation(scored, 40, 1, 4);
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn min_per_peer_is_honoured_unless_budget_too_small() {
        let scored = vec![(peer(1), 1.0), (peer(2), 1.0), (peer(3), 1.0)];
        let r = proportional_allocation(scored, 6, 2, 8);
        for a in &r {
            assert!(a.budget >= 2, "below min: {a:?}");
        }
    }

    #[test]
    fn zero_total_returns_empty() {
        let scored = vec![(peer(1), 1.0)];
        let r = proportional_allocation(scored, 0, 1, 8);
        assert!(r.is_empty());
    }
}
