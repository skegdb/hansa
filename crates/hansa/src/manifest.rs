//! Peer manifests (F.5).
//!
//! In a Hanseatic league a *manifest* is the cargo record a captain
//! keeps of who shipped what, on time or late, helpful or not. The
//! membrane keeps the same kind of book on its peers: every time a
//! query fan-out returns hits from a peer, we mark how many of those
//! hits the caller flagged as actually useful. Future queries bias
//! the per-peer budget proportionally - the league favours peers
//! that have delivered.
//!
//! ## On-disk layout
//!
//! Manifests live next to the saga store:
//!
//! ```text
//! ~/.hansa/<hansa_id>/
//!   members.log
//!   members.snap
//!   sagas/<peer_id>.saga
//!   manifests/<peer_id>.manifest        # this module
//! ```
//!
//! One file per peer that this hansa has observed. Atomic write via
//! temp+rename, matching `members.snap`. JSON encoding so an operator
//! can inspect a manifest with `cat`.
//!
//! ## Why per-peer and not per-cluster
//!
//! v0.1 keeps a single usefulness counter per peer rather than the
//! per-cluster breakdown described in the design doc. Per-cluster
//! requires the membrane to surface the winning centroid id at score
//! time; that's a bigger surface change and is parked for v0.2.
//! Per-peer is enough to bias routing for the common case ("peer
//! Alice has been useful 80% of the time → bias her budget up").
//!
//! ## Decay
//!
//! A peer that was helpful a year ago shouldn't dominate routing
//! today. Each manifest carries `last_useful_at` (unix seconds);
//! [`PeerManifest::usefulness_factor`] applies exponential decay
//! with a 24-hour half-life. The bias multiplier is capped at
//! `+50%` so a strong manifest can't outweigh a clearly higher saga
//! score.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use skeg_rigging::TenantId;

use crate::Result;

/// Per-peer record of past usefulness.
///
/// `useful_hits` and `total_hits` are cumulative since this hansa
/// first observed the peer. `last_useful_at` is the unix-seconds
/// timestamp of the most recent [`crate::Hansa::record_useful_hits`]
/// call that bumped `useful_hits`.
///
/// New manifests start zeroed; missing files on disk are treated as
/// neutral (no bias) rather than as errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerManifest {
    /// Raw bytes of the peer tenant id. `TenantId` itself doesn't
    /// derive serde, so we round-trip the 16 bytes and reconstruct
    /// the typed id via [`Self::peer_tenant_id`].
    pub peer_id_bytes: [u8; 16],
    /// Number of hits this peer returned that the caller marked as
    /// useful via [`crate::Hansa::record_useful_hits`].
    pub useful_hits: u64,
    /// Total number of hits this peer has ever returned to this
    /// hansa, across all queries. Updated by the membrane after
    /// every fan-out.
    pub total_hits: u64,
    /// Unix seconds of the most recent `useful_hits` bump. Zero
    /// when the peer has never been marked useful.
    pub last_useful_at: u64,
}

impl PeerManifest {
    /// Half-life of the recency decay, in seconds (24 hours).
    pub const RECENCY_HALF_LIFE_S: f32 = 86_400.0;

    /// Cap on the *positive* bias multiplier: `1.0 + 0.5 = 1.5`. A
    /// perfect manifest tops out at +50% of the base saga score.
    pub const BIAS_CAP: f32 = 0.5;

    /// Trial period (F.4): a peer that has returned this many results
    /// without any being marked useful is considered to be in a cold
    /// streak. Below this threshold the factor stays neutral so a
    /// fresh peer always gets a chance.
    pub const TRIAL_RETURNS: u64 = 20;

    /// Cap on the *negative* bias multiplier: the factor for an
    /// asymptotically cold peer floors at `1.0 - 0.8 = 0.2`. A cold
    /// peer is not banned outright (F.4 wants discouragement, not
    /// blacklist); the saga score has to be that much higher for
    /// the membrane to still pick the peer.
    pub const NEGATIVE_BIAS_CAP: f32 = 0.8;

    /// Fresh, neutral manifest. Returned when no file exists on disk.
    pub fn empty(peer_tenant_id: TenantId) -> Self {
        Self {
            peer_id_bytes: *peer_tenant_id.as_bytes(),
            useful_hits: 0,
            total_hits: 0,
            last_useful_at: 0,
        }
    }

    /// The peer tenant id reconstructed from the stored bytes.
    pub fn peer_tenant_id(&self) -> TenantId {
        TenantId::from_bytes(self.peer_id_bytes)
    }

    /// Multiplier applied to the peer's saga score during membrane
    /// fan-out budgeting.
    ///
    /// Three regimes:
    ///
    /// - **Unknown** (`total_hits == 0`) - returns `1.0`. We've
    ///   never queried the peer; no signal either way.
    /// - **Trial** (`useful_hits == 0` and `total_hits < TRIAL_RETURNS`):
    ///   returns `1.0`. Give every new peer the same `TRIAL_RETURNS`
    ///   queries' worth of cooperation before any judgement.
    /// - **Cold** (F.4: `useful_hits == 0` and
    ///   `total_hits >= TRIAL_RETURNS`) - returns `< 1.0`. Each
    ///   additional dud return past the trial period drags the
    ///   factor toward `1.0 - NEGATIVE_BIAS_CAP`. The peer is not
    ///   banned outright; a high saga score can still surface it.
    /// - **Useful** (F.5: `useful_hits > 0`) - returns
    ///   `1.0 + min(BIAS_CAP, useful_ratio * recency)`. Capped at
    ///   `1.0 + BIAS_CAP`.
    pub fn usefulness_factor(&self, now_unix_s: u64) -> f32 {
        if self.total_hits == 0 {
            return 1.0;
        }
        if self.useful_hits == 0 {
            if self.total_hits < Self::TRIAL_RETURNS {
                return 1.0;
            }
            // Cold path (F.4): asymptote toward 1.0 - NEGATIVE_BIAS_CAP.
            // After one full TRIAL_RETURNS of additional duds (so total =
            // 2 * TRIAL_RETURNS) the pull is saturated.
            let excess = (self.total_hits - Self::TRIAL_RETURNS) as f32;
            let pull = (excess / Self::TRIAL_RETURNS as f32).min(1.0);
            return 1.0 - Self::NEGATIVE_BIAS_CAP * pull;
        }
        // Useful path (F.5).
        let ratio = self.useful_hits as f32 / self.total_hits as f32;
        let age_s = now_unix_s.saturating_sub(self.last_useful_at) as f32;
        let recency = (-age_s / Self::RECENCY_HALF_LIFE_S).exp();
        let bias = (ratio * recency).min(Self::BIAS_CAP);
        1.0 + bias
    }

    /// True when this peer is in F.4's cold streak (zero useful hits
    /// past the trial period). Useful for diagnostics; the membrane
    /// itself only inspects [`Self::usefulness_factor`].
    pub fn is_cold(&self) -> bool {
        self.useful_hits == 0 && self.total_hits >= Self::TRIAL_RETURNS
    }
}

/// On-disk store of [`PeerManifest`]s. One file per peer under
/// `manifest_dir/`. The store is stateless - it reads and writes
/// files on demand and does not cache.
pub struct ManifestStore {
    dir: PathBuf,
}

impl ManifestStore {
    /// Bind the store to `dir`. The directory is created lazily on
    /// first write; reads from a non-existent dir return empty
    /// manifests rather than errors.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Manifest dir.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// File path for a peer's manifest.
    pub fn manifest_path(&self, peer: TenantId) -> PathBuf {
        let mut name = String::with_capacity(32);
        for b in peer.as_bytes() {
            name.push_str(&format!("{b:02x}"));
        }
        name.push_str(".manifest");
        self.dir.join(name)
    }

    /// Read a peer's manifest. Returns a neutral [`PeerManifest::empty`]
    /// when the file does not exist or is unreadable - manifests are
    /// best effort.
    pub fn read(&self, peer: TenantId) -> PeerManifest {
        let path = self.manifest_path(peer);
        let Ok(bytes) = std::fs::read(&path) else {
            return PeerManifest::empty(peer);
        };
        serde_json::from_slice::<PeerManifest>(&bytes).unwrap_or_else(|_| PeerManifest::empty(peer))
    }

    /// Atomically write a manifest (temp + rename). Creates the
    /// manifest dir if missing.
    pub fn write(&self, manifest: &PeerManifest) -> Result<()> {
        std::fs::create_dir_all(&self.dir).map_err(crate::HansaError::from)?;
        let bytes = serde_json::to_vec(manifest).map_err(|e| {
            crate::HansaError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("manifest serialise: {e}"),
            ))
        })?;
        let path = self.manifest_path(manifest.peer_tenant_id());
        let tmp = path.with_extension("manifest.tmp");
        std::fs::write(&tmp, &bytes).map_err(crate::HansaError::from)?;
        std::fs::rename(&tmp, &path).map_err(crate::HansaError::from)?;
        Ok(())
    }

    /// Increment `total_hits` by `delta` for the given peer. Reads,
    /// bumps, atomic-writes. Best effort: serialisation errors are
    /// swallowed (logged via stderr) so manifest drift never aborts
    /// a query.
    pub fn bump_total(&self, peer: TenantId, delta: u64) {
        let mut m = self.read(peer);
        m.total_hits = m.total_hits.saturating_add(delta);
        if let Err(e) = self.write(&m) {
            eprintln!("hansa: manifest bump_total({peer}) failed: {e}");
        }
    }

    /// Increment `useful_hits` by `delta` and refresh
    /// `last_useful_at`. Best effort like [`Self::bump_total`].
    pub fn bump_useful(&self, peer: TenantId, delta: u64) {
        let mut m = self.read(peer);
        m.useful_hits = m.useful_hits.saturating_add(delta);
        m.last_useful_at = unix_seconds();
        if let Err(e) = self.write(&m) {
            eprintln!("hansa: manifest bump_useful({peer}) failed: {e}");
        }
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(b: u8) -> TenantId {
        TenantId::from_bytes([b; 16])
    }

    #[test]
    fn empty_manifest_factor_is_neutral() {
        let m = PeerManifest::empty(tid(0x11));
        assert_eq!(m.usefulness_factor(unix_seconds()), 1.0);
    }

    #[test]
    fn factor_grows_with_useful_ratio_and_caps_at_bias_cap() {
        let mut m = PeerManifest::empty(tid(0x12));
        m.total_hits = 10;
        m.useful_hits = 10; // 100% useful
        m.last_useful_at = unix_seconds();
        let factor = m.usefulness_factor(unix_seconds());
        assert!((factor - 1.5).abs() < 1e-3, "got {factor}");
    }

    #[test]
    fn factor_decays_with_age() {
        let mut m = PeerManifest::empty(tid(0x13));
        m.total_hits = 10;
        m.useful_hits = 10;
        m.last_useful_at = unix_seconds() - PeerManifest::RECENCY_HALF_LIFE_S as u64;
        // One half-life of age → recency factor = exp(-1) ≈ 0.368,
        // so bias = 1.0 × 0.368 = 0.368 → factor ≈ 1.368.
        let factor = m.usefulness_factor(unix_seconds());
        assert!(factor > 1.30 && factor < 1.40, "got {factor}");
    }

    #[test]
    fn trial_period_keeps_factor_neutral_for_unused_peer() {
        // Below TRIAL_RETURNS the peer is in trial; factor stays 1.0
        // even when nothing has been marked useful yet.
        let mut m = PeerManifest::empty(tid(0x14));
        m.total_hits = PeerManifest::TRIAL_RETURNS - 1;
        m.useful_hits = 0;
        let factor = m.usefulness_factor(unix_seconds());
        assert_eq!(factor, 1.0);
        assert!(!m.is_cold());
    }

    #[test]
    fn cold_peer_factor_drops_below_one_past_trial_period() {
        // Halfway through the cold-streak window: total = 1.5 * trial.
        let mut m = PeerManifest::empty(tid(0x15));
        m.total_hits = PeerManifest::TRIAL_RETURNS + PeerManifest::TRIAL_RETURNS / 2;
        m.useful_hits = 0;
        let factor = m.usefulness_factor(unix_seconds());
        // pull = 0.5, factor = 1 - 0.8 * 0.5 = 0.6
        assert!((factor - 0.6).abs() < 1e-3, "got {factor}");
        assert!(m.is_cold());
    }

    #[test]
    fn cold_peer_factor_floors_at_negative_cap() {
        // Saturated cold streak: 3x trial.
        let mut m = PeerManifest::empty(tid(0x16));
        m.total_hits = PeerManifest::TRIAL_RETURNS * 3;
        m.useful_hits = 0;
        let factor = m.usefulness_factor(unix_seconds());
        // pull = clamped to 1.0, factor = 1 - 0.8 = 0.2
        assert!((factor - 0.2).abs() < 1e-3, "got {factor}");
    }

    #[test]
    fn one_useful_hit_exits_cold_streak() {
        // Even a single useful hit pulls the peer back into the
        // positive bias path.
        let mut m = PeerManifest::empty(tid(0x17));
        m.total_hits = PeerManifest::TRIAL_RETURNS * 3;
        m.useful_hits = 1;
        m.last_useful_at = unix_seconds();
        let factor = m.usefulness_factor(unix_seconds());
        assert!(factor > 1.0, "got {factor}");
        assert!(!m.is_cold());
    }

    #[test]
    fn store_read_missing_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path());
        let m = store.read(tid(0x21));
        assert_eq!(m, PeerManifest::empty(tid(0x21)));
    }

    #[test]
    fn store_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path());
        let m = PeerManifest {
            peer_id_bytes: *tid(0x22).as_bytes(),
            useful_hits: 42,
            total_hits: 50,
            last_useful_at: 1_700_000_000,
        };
        store.write(&m).unwrap();
        let loaded = store.read(tid(0x22));
        assert_eq!(loaded, m);
    }

    #[test]
    fn store_bump_useful_increments_and_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path());
        let p = tid(0x23);
        store.bump_total(p, 10);
        store.bump_useful(p, 3);
        let m = store.read(p);
        assert_eq!(m.total_hits, 10);
        assert_eq!(m.useful_hits, 3);
        assert!(m.last_useful_at > 0);
    }

    #[test]
    fn store_corrupted_file_is_treated_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path());
        std::fs::create_dir_all(dir.path()).unwrap();
        std::fs::write(store.manifest_path(tid(0x24)), b"{garbage json").unwrap();
        let m = store.read(tid(0x24));
        assert_eq!(m, PeerManifest::empty(tid(0x24)));
    }
}
