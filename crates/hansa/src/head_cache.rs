//! Anti-rollback head cache.
//!
//! A member that has already seen a chain remembers its last verified
//! head — the `(seq, hash)` of the final link — in a small file kept
//! *outside* the shared registry directory. On the next open it refuses
//! a chain whose head has regressed below what it cached, which catches
//! an attacker (or a stale mirror) presenting a truncated chain that
//! drops a later `Revoke`.
//!
//! This protects *returning* members only. A first-time joiner has no
//! cached head to compare against; closing that gap needs an external
//! witness (a network registry), which is later work. Point
//! [`crate::HansaConfig::head_cache_dir`] at a path the registry writer
//! does not control (e.g. `~/.cache/hansa`) for the guarantee to hold.

use std::path::{Path, PathBuf};

use crate::{HansaId, HansaError, Result};

/// Per-machine store of last-verified chain heads.
pub struct HeadCache {
    dir: PathBuf,
}

impl HeadCache {
    /// Cache rooted at `dir`. The directory is created on first write.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn path(&self, hansa: HansaId) -> PathBuf {
        self.dir.join(format!("{}.head", hansa.as_hex()))
    }

    /// Last verified `(seq, hash)` for `hansa`, if any is cached.
    pub fn load(&self, hansa: HansaId) -> Result<Option<(u64, [u8; 32])>> {
        let path = self.path(hansa);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)?;
        let mut parts = text.split_whitespace();
        let mut parse = || -> Option<(u64, [u8; 32])> {
            let seq: u64 = parts.next()?.parse().ok()?;
            let hex = parts.next()?;
            if hex.len() != 64 {
                return None;
            }
            let mut hash = [0u8; 32];
            for (i, b) in hash.iter_mut().enumerate() {
                *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
            }
            Some((seq, hash))
        };
        parse()
            .map(Some)
            .ok_or_else(|| HansaError::RegistryMalformed(format!("head cache {}", path.display())))
    }

    /// Record `(seq, hash)` as the latest verified head for `hansa`.
    pub fn store(&self, hansa: HansaId, seq: u64, hash: [u8; 32]) -> Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let mut hex = String::with_capacity(64);
        for b in hash {
            hex.push_str(&format!("{b:02x}"));
        }
        let path = self.path(hansa);
        let tmp = path.with_extension("head.tmp");
        std::fs::write(&tmp, format!("{seq} {hex}\n"))?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Check a freshly-replayed head against the cache, then record it.
    /// Errors with [`HansaError::ChainRegressed`] when the head moved
    /// backwards (lower seq) or forked (same seq, different hash).
    pub fn check_and_record(&self, hansa: HansaId, seq: u64, hash: [u8; 32]) -> Result<()> {
        if let Some((cseq, chash)) = self.load(hansa)?
            && (seq < cseq || (seq == cseq && hash != chash))
        {
            return Err(HansaError::ChainRegressed);
        }
        self.store(hansa, seq, hash)
    }
}

impl AsRef<Path> for HeadCache {
    fn as_ref(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sign::Skipper;

    fn id() -> HansaId {
        HansaId::from_skipper(&Skipper::from_seed([1; 32]).public())
    }

    #[test]
    fn forward_progress_is_recorded_and_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HeadCache::new(dir.path());
        let h = id();
        cache.check_and_record(h, 3, [1; 32]).unwrap();
        assert_eq!(cache.load(h).unwrap(), Some((3, [1; 32])));
        // A later head is fine.
        cache.check_and_record(h, 5, [2; 32]).unwrap();
        assert_eq!(cache.load(h).unwrap(), Some((5, [2; 32])));
    }

    #[test]
    fn regressed_seq_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HeadCache::new(dir.path());
        let h = id();
        cache.check_and_record(h, 5, [2; 32]).unwrap();
        assert!(matches!(
            cache.check_and_record(h, 3, [9; 32]),
            Err(HansaError::ChainRegressed)
        ));
    }

    #[test]
    fn fork_at_same_seq_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cache = HeadCache::new(dir.path());
        let h = id();
        cache.check_and_record(h, 5, [2; 32]).unwrap();
        assert!(matches!(
            cache.check_and_record(h, 5, [7; 32]),
            Err(HansaError::ChainRegressed)
        ));
    }
}
