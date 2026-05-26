//! Membrane: the federated query path.
//!
//! A membrane query is the mechanism by which one hansa member queries
//! across its peers under an explicit token budget. The pipeline:
//!
//! 1. Query the local tenant in full visibility (no filter).
//! 2. Score each peer's saga against the query embedding.
//! 3. Allocate the remote budget proportionally to peer scores, with a
//!    per-peer minimum and a cap on fan-out.
//! 4. Open each selected peer's tenant read-only via an injected
//!    `PeerOpener` closure and run a filtered query (only `shareable`
//!    records).
//! 5. Merge local + remote hits, sort by similarity, truncate to the
//!    total budget.
//!
//! A peer that fails to open, lock, or query (offline, corrupted) is
//! logged and skipped - the query never aborts because of one bad peer.
//! Local hits are always included.

mod allocation;
mod budget;
mod query;

pub use allocation::{DEFAULT_MAX_PEERS, PeerAllocation, proportional_allocation};
pub use budget::TokenBudget;
pub use query::{HitOrigin, MembraneHit, MembraneQuery, MembraneStats, PeerOpener};
