//! Pluggable storage for [`HansaKey`].
//!
//! Three implementations ship in v0.1:
//!
//! - [`EnvKeystore`] reads from `HANSA_KEY_<SLOT>` environment variables
//!   (hex-encoded 32-byte keys). Convenient for spawning agents under
//!   process supervisors.
//! - [`FileKeystore`] holds keys in a passphrase-encrypted file. v0.1
//!   ships a simpler variant: a file of raw hex keys, one per slot,
//!   protected only by filesystem permissions. Passphrase encryption is
//!   v0.2.
//! - [`MemoryKeystore`] keeps keys in-process; intended for tests.
//!
//! Future implementations like `KeychainKeystore` (macOS) live in
//! separate crates behind feature flags and are out of scope for v0.1.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::{HansaError, HansaKey, Result};

/// Pluggable storage for [`HansaKey`]s, addressed by a string slot.
pub trait Keystore: Send + Sync {
    /// Load the key in the named slot.
    fn load(&self, slot: &str) -> Result<HansaKey>;
    /// Store `key` under `slot`, overwriting any existing entry.
    fn store(&self, slot: &str, key: &HansaKey) -> Result<()>;
    /// Remove the key in `slot`, if any. Returns `Ok(())` when no key
    /// exists, to keep removal idempotent.
    fn remove(&self, slot: &str) -> Result<()>;
}

// ============ EnvKeystore ============

/// Reads keys from `HANSA_KEY_<SLOT>` environment variables.
///
/// `slot` is upcased and inserted into the variable name. The value is
/// hex-decoded into 32 bytes. Writing and removing are no-ops (the
/// environment is shared with the host process; mutating it from inside
/// a library is hostile).
pub struct EnvKeystore;

impl EnvKeystore {
    /// Construct a fresh handle. The struct itself holds no state.
    pub fn new() -> Self {
        Self
    }

    fn env_name(slot: &str) -> String {
        let mut s = String::from("HANSA_KEY_");
        s.push_str(&slot.to_ascii_uppercase());
        s
    }
}

impl Default for EnvKeystore {
    fn default() -> Self {
        Self::new()
    }
}

impl Keystore for EnvKeystore {
    fn load(&self, slot: &str) -> Result<HansaKey> {
        let name = Self::env_name(slot);
        let value =
            std::env::var(&name).map_err(|_| HansaError::KeyNotFound(slot.to_owned()))?;
        let bytes = decode_hex(value.trim())?;
        HansaKey::from_slice(&bytes)
    }

    fn store(&self, _slot: &str, _key: &HansaKey) -> Result<()> {
        Err(HansaError::Invariant(
            "EnvKeystore is read-only at runtime".into(),
        ))
    }

    fn remove(&self, _slot: &str) -> Result<()> {
        Err(HansaError::Invariant(
            "EnvKeystore is read-only at runtime".into(),
        ))
    }
}

// ============ FileKeystore ============

/// Plain hex-encoded keystore on disk.
///
/// Each file holds one key (32 bytes, hex-encoded ASCII, no
/// terminator). The path is `<root>/<slot>.key`. v0.1 relies on
/// filesystem permissions for confidentiality. v0.2 will add
/// passphrase-derived encryption.
pub struct FileKeystore {
    /// Directory that contains key files.
    pub root: PathBuf,
}

impl FileKeystore {
    /// Construct against a root directory. The directory is created on
    /// first store if missing.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, slot: &str) -> PathBuf {
        let mut p = self.root.clone();
        p.push(format!("{slot}.key"));
        p
    }
}

impl Keystore for FileKeystore {
    fn load(&self, slot: &str) -> Result<HansaKey> {
        let path = self.path_for(slot);
        let text = std::fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                HansaError::KeyNotFound(slot.to_owned())
            } else {
                HansaError::Io(e)
            }
        })?;
        let bytes = decode_hex(text.trim())?;
        HansaKey::from_slice(&bytes)
    }

    fn store(&self, slot: &str, key: &HansaKey) -> Result<()> {
        std::fs::create_dir_all(&self.root)?;
        let path = self.path_for(slot);
        let hex = encode_hex(key.raw());
        write_with_perms(&path, hex.as_bytes())?;
        Ok(())
    }

    fn remove(&self, slot: &str) -> Result<()> {
        match std::fs::remove_file(self.path_for(slot)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(HansaError::Io(e)),
        }
    }
}

#[cfg(unix)]
fn write_with_perms(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn write_with_perms(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

// ============ MemoryKeystore ============

/// In-process keystore. Resets when the handle drops. Intended for
/// tests and ephemeral demos.
#[derive(Clone, Default)]
pub struct MemoryKeystore {
    inner: Arc<RwLock<HashMap<String, HansaKey>>>,
}

impl MemoryKeystore {
    /// Fresh empty keystore.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Keystore for MemoryKeystore {
    fn load(&self, slot: &str) -> Result<HansaKey> {
        self.inner
            .read()
            .get(slot)
            .cloned()
            .ok_or_else(|| HansaError::KeyNotFound(slot.to_owned()))
    }

    fn store(&self, slot: &str, key: &HansaKey) -> Result<()> {
        self.inner.write().insert(slot.to_owned(), key.clone());
        Ok(())
    }

    fn remove(&self, slot: &str) -> Result<()> {
        self.inner.write().remove(slot);
        Ok(())
    }
}

// ============ helpers ============

fn decode_hex(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(HansaError::Invariant(format!(
            "hex string has odd length: {}",
            input.len()
        )));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(HansaError::Invariant(format!(
            "invalid hex byte: {b:#x}"
        ))),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_keystore_roundtrip() {
        let ks = MemoryKeystore::new();
        let k = HansaKey::from_bytes([0x42; 32]);
        ks.store("test", &k).unwrap();
        let loaded = ks.load("test").unwrap();
        assert_eq!(loaded, k);
    }

    #[test]
    fn memory_keystore_remove_idempotent() {
        let ks = MemoryKeystore::new();
        ks.remove("nope").unwrap();
        let k = HansaKey::from_bytes([1; 32]);
        ks.store("s", &k).unwrap();
        ks.remove("s").unwrap();
        let err = ks.load("s").unwrap_err();
        assert!(matches!(err, HansaError::KeyNotFound(_)));
    }

    #[test]
    fn file_keystore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let ks = FileKeystore::new(dir.path());
        let k = HansaKey::from_bytes([0xaa; 32]);
        ks.store("alpha", &k).unwrap();
        let loaded = ks.load("alpha").unwrap();
        assert_eq!(loaded, k);
    }

    #[test]
    fn file_keystore_missing_slot() {
        let dir = tempfile::tempdir().unwrap();
        let ks = FileKeystore::new(dir.path());
        let err = ks.load("nope").unwrap_err();
        assert!(matches!(err, HansaError::KeyNotFound(_)));
    }

    #[test]
    fn hex_decode_rejects_invalid() {
        assert!(decode_hex("z0").is_err());
        assert!(decode_hex("a").is_err()); // odd length
        assert!(decode_hex("00").is_ok());
    }

    #[test]
    fn hex_roundtrip() {
        let bytes = [0xaa, 0xbb, 0xcc, 0xdd];
        let s = encode_hex(&bytes);
        assert_eq!(s, "aabbccdd");
        assert_eq!(decode_hex(&s).unwrap(), bytes);
    }

    #[test]
    #[allow(unsafe_code)]
    fn env_keystore_reads_var() {
        let hex = "aa".repeat(32);
        let slot = "test_env_slot_unique";
        let name = format!("HANSA_KEY_{}", slot.to_uppercase());
        // SAFETY: set_var/remove_var are unsafe on edition 2024 because
        // they mutate process-global state without synchronisation. This
        // test is single-threaded and cleans up after itself.
        unsafe { std::env::set_var(&name, &hex) };
        let ks = EnvKeystore::new();
        let key = ks.load(slot).unwrap();
        assert_eq!(key.raw(), &[0xaa; 32]);
        unsafe { std::env::remove_var(&name) };
    }
}
