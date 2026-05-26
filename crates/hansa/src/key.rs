//! `HansaKey` and `HansaId`.
//!
//! A `HansaKey` is a 32-byte symmetric secret that identifies membership
//! in a trust group. Holding it is the entire authorisation mechanism in
//! v0.1. The key zeroises on drop.
//!
//! A `HansaId` is a public, derivable identifier - `blake3(key ||
//! "hansa-id-v1")` - used as the directory name in the registry and in
//! logs. Knowing the id does not grant access; only the key does.

use std::fmt;

use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::HansaError;

const HANSA_ID_DOMAIN: &[u8] = b"hansa-id-v1";

/// 32-byte symmetric secret that defines membership in a hansa.
///
/// The internal bytes are not exposed; serialisation and storage go
/// through the [`Keystore`](crate::keystore::Keystore) trait.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct HansaKey {
    bytes: [u8; 32],
}

impl HansaKey {
    /// Length of a HansaKey in bytes.
    pub const LEN: usize = 32;

    /// Generate a fresh HansaKey from OS randomness.
    pub fn generate() -> Self {
        let bytes: [u8; 32] = rand::random();
        Self { bytes }
    }

    /// Derive a HansaKey from a passphrase and a caller-chosen salt.
    /// Uses BLAKE3 in KDF mode; the salt is incorporated as the context.
    pub fn from_passphrase(passphrase: &str, salt: &[u8]) -> Self {
        // BLAKE3 derive_key takes a context string; we keep the salt out
        // of it (the context must be a known constant) and feed it into
        // the keyed hash instead.
        let mut hasher = blake3::Hasher::new_derive_key("hansa.key.v1");
        hasher.update(salt);
        hasher.update(passphrase.as_bytes());
        let mut bytes = [0u8; 32];
        let mut reader = hasher.finalize_xof();
        reader.fill(&mut bytes);
        Self { bytes }
    }

    /// Construct directly from 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Construct from a byte slice. Errors if the length is not 32.
    pub fn from_slice(slice: &[u8]) -> Result<Self, HansaError> {
        if slice.len() != Self::LEN {
            return Err(HansaError::InvalidKeyLength(slice.len()));
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(slice);
        Ok(Self { bytes })
    }

    /// Derive the public [`HansaId`] for this key.
    pub fn hansa_id(&self) -> HansaId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.bytes);
        hasher.update(HANSA_ID_DOMAIN);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(digest.as_bytes());
        HansaId(out)
    }

    /// Internal: lend out the raw bytes for a [`Keystore`] backend.
    /// Intentionally crate-private.
    pub(crate) fn raw(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for HansaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the key. Show only its derived id.
        write!(f, "HansaKey(id={})", self.hansa_id())
    }
}

impl PartialEq for HansaKey {
    fn eq(&self, other: &Self) -> bool {
        // Constant-time comparison would be nicer, but key equality is
        // not on a hot path; the v0.1 trust model already assumes
        // everyone holding the key is trusted.
        self.bytes == other.bytes
    }
}
impl Eq for HansaKey {}

/// Public, non-secret identifier of a hansa.
///
/// `HansaId = blake3(key || "hansa-id-v1")`. Used as the directory name
/// under `~/.hansa/` and in event logs. Holding only the id grants no
/// access.
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct HansaId(pub(crate) [u8; 32]);

impl HansaId {
    /// 32 bytes.
    pub const LEN: usize = 32;

    /// Hex representation (64 chars).
    pub fn as_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    /// Raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for HansaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_hex())
    }
}

impl fmt::Debug for HansaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HansaId({})", self.as_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_id() {
        let k = HansaKey::from_bytes([7; 32]);
        assert_eq!(k.hansa_id(), k.hansa_id());
        let k2 = HansaKey::from_bytes([7; 32]);
        assert_eq!(k.hansa_id(), k2.hansa_id());
    }

    #[test]
    fn different_keys_different_ids() {
        let a = HansaKey::from_bytes([1; 32]);
        let b = HansaKey::from_bytes([2; 32]);
        assert_ne!(a.hansa_id(), b.hansa_id());
    }

    #[test]
    fn generate_random_keys_differ() {
        let a = HansaKey::generate();
        let b = HansaKey::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn passphrase_is_deterministic_under_same_salt() {
        let a = HansaKey::from_passphrase("hunter2", b"salt-1");
        let b = HansaKey::from_passphrase("hunter2", b"salt-1");
        assert_eq!(a, b);
        let c = HansaKey::from_passphrase("hunter2", b"salt-2");
        assert_ne!(a, c);
    }

    #[test]
    fn from_slice_rejects_wrong_length() {
        let err = HansaKey::from_slice(&[0u8; 16]).unwrap_err();
        assert!(matches!(err, HansaError::InvalidKeyLength(16)));
    }

    #[test]
    fn hex_is_64_chars() {
        let id = HansaKey::from_bytes([0xab; 32]).hansa_id();
        assert_eq!(id.as_hex().len(), 64);
    }

    #[test]
    fn debug_does_not_leak_bytes() {
        let k = HansaKey::from_bytes([0xab; 32]);
        let s = format!("{k:?}");
        assert!(s.starts_with("HansaKey(id="));
        // The raw byte 0xab repeated would appear as "abab..."; the id is
        // blake3(...) which won't be the same all-ab string, but just to
        // be sure the literal raw key bytes don't appear:
        assert!(!s.contains("ababababababababababababababababab"));
    }
}
