#![deny(unsafe_code)]
#![warn(missing_docs)]

//! `hansa` - federation primitive for skeg.
//!
//! A *hansa* is a trust group of agents that hold the same
//! [`HansaKey`]. Members keep their memories private by default and opt
//! into sharing per record via a `shareable` flag. A query made through
//! a member's [`Hansa`] handle can fan out, under an explicit token
//! budget, to peers whose [`Saga`] (digest) suggests they hold relevant
//! records.
//!
//! v0.1 is filesystem-local: peers are discovered through a
//! [`FileRegistry`] under `~/.hansa/<hansa-id>/` and queried by opening
//! their vaults read-only via [`skeg_rigging::ReadOnlyView`].
//! Cross-machine federation is a v0.3 concern.
//!
//! ## What v0.1 includes
//!
//! - [`HansaKey`], [`HansaId`], and three [`Keystore`] implementations
//!   (env, file, in-memory).
//! - [`FileRegistry`] (append-only `members.log` + advisory-lock
//!   compaction).
//! - [`Saga`]: condensed memory digest (k-means centroids + tag
//!   aggregate), persisted via `skeg-hull`'s SagaV1 format.
//! - The membrane query path (forthcoming in the same release line).
//!
//! ## Trust model
//!
//! Anyone with the [`HansaKey`] is a fully trusted equal. Revocation in
//! v0.1 means rotating the key and redistributing it to the remaining
//! members. v0.2 introduces a skipper keypair and selective revocation.

pub mod background;
pub mod cache;
pub mod context;
pub mod error;
pub mod manifest;
pub mod hansa;
pub mod hybrid_registry;
pub mod key;
pub mod keystore;
pub mod member;
pub mod membrane;
pub mod registry;
pub mod saga;

pub use background::{BackgroundRefreshConfig, RefreshHandle};
pub use cache::BundleCache;
pub use context::{
    CharCountTokenizer, ContextBuilder, ContextBundle, ContextItem, Ranker, SimilarityRanker,
    TokenDensityRanker, Tokenizer,
};
pub use error::HansaError;
pub use hansa::{Hansa, HansaConfig};
pub use hybrid_registry::HybridRegistry;
pub use key::{HansaId, HansaKey};
pub use keystore::{EnvKeystore, FileKeystore, Keystore, MemoryKeystore};
pub use manifest::{ManifestStore, PeerManifest};
pub use member::MemberRecord;
pub use membrane::{HitOrigin, MembraneHit, MembraneStats, PeerOpener, TokenBudget};
pub use registry::{FileRegistry, Registry};
pub use saga::Saga;

/// Crate result alias.
pub type Result<T> = std::result::Result<T, HansaError>;

/// Re-exports for typical consumption.
pub mod prelude {
    pub use crate::{
        BackgroundRefreshConfig, BundleCache, CharCountTokenizer, ContextBuilder, ContextBundle,
        ContextItem, EnvKeystore, FileKeystore, FileRegistry, Hansa, HansaConfig, HansaError,
        HansaId, HansaKey, HitOrigin, Keystore, ManifestStore, MemberRecord, MembraneHit,
        MembraneStats, MemoryKeystore, PeerManifest, PeerOpener, Ranker, RefreshHandle, Registry,
        Result, Saga, SimilarityRanker, TokenBudget, TokenDensityRanker, Tokenizer,
    };
}
