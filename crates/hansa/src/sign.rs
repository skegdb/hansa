//! Signing primitives for the M3 trust model.
//!
//! The *skipper* is a hansa's signing authority: an ed25519 keypair
//! generated at founding. Members verify the skipper's signatures to
//! decide who is a real member, so authority no longer rests on the
//! shared-writable filesystem (see `private/m3-security.md`).
//!
//! Three rules this module enforces, all from the design doc:
//!
//! - **`verify_strict` always.** Plain ed25519 `verify` permits weak-key
//!   forgery; `verify_strict` rejects small-order public keys. A
//!   [`SkipperPub`] additionally refuses to construct from a weak key.
//! - **Domain separation.** Every signature is over `DOMAIN || msg`, so
//!   a signature for one object type can never be replayed as another.
//!   Distinct signed objects use distinct domain tags.
//! - **Canonical bytes, never JSON.** Callers build the signed message
//!   with [`canonical::Writer`] (length-prefixed, deterministic). JSON
//!   stays a transport encoding only; we never sign emitted JSON.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use zeroize::Zeroize;

use crate::{HansaError, Result};

/// Domain tag for the genesis record signature.
pub const DOMAIN_GENESIS: &[u8] = b"hansa.genesis.v2\0";
/// Domain tag for `members.log` link signatures (Admit / Revoke /
/// Checkpoint bodies are disambiguated by an inner body tag inside the
/// canonical message, not by the domain).
pub const DOMAIN_LINK: &[u8] = b"hansa.link.v2\0";

/// A hansa's signing authority. Holds the ed25519 secret; zeroized on
/// drop by `ed25519-dalek`'s `zeroize` feature.
pub struct Skipper {
    signing: SigningKey,
}

impl Skipper {
    /// Mint a fresh skipper keypair from the OS RNG.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let signing = SigningKey::from_bytes(&seed);
        seed.zeroize();
        Self { signing }
    }

    /// Reconstruct a skipper from a 32-byte secret seed (e.g. loaded
    /// from a keystore).
    pub fn from_seed(mut seed: [u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(&seed);
        seed.zeroize();
        Self { signing }
    }

    /// Derive a skipper deterministically from a [`crate::HansaKey`].
    ///
    /// Convenience for the local single-user model: the key-holder is
    /// the skipper, so the one shared secret grants both reading and
    /// membership control. This deliberately trades away the
    /// key/authority separation — anyone holding the key can sign — for
    /// zero extra key distribution, which suits one user with several
    /// agents on one machine. For true asymmetric trust (the key lets
    /// you read but not change membership), generate an independent
    /// [`Skipper::generate`] and distribute its secret separately.
    pub fn from_hansa_key(key: &crate::HansaKey) -> Self {
        let mut h = blake3::Hasher::new_derive_key("hansa.skipper.from-key.v1");
        h.update(key.raw());
        let mut seed = [0u8; 32];
        seed.copy_from_slice(h.finalize().as_bytes());
        Self::from_seed(seed)
    }

    /// The 32-byte secret seed, for storage in a keystore. Handle with
    /// the same care as a [`crate::HansaKey`]; zeroize the copy when
    /// done.
    pub fn to_seed(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    /// This skipper's public verifying key.
    pub fn public(&self) -> SkipperPub {
        SkipperPub(self.signing.verifying_key())
    }

    /// Sign `msg` under `domain`. The signed bytes are `domain || msg`,
    /// so the same `msg` under a different domain yields a signature
    /// that will not verify.
    pub fn sign(&self, domain: &[u8], msg: &[u8]) -> Sig {
        let buf = framed(domain, msg);
        Sig(self.signing.sign(&buf).to_bytes())
    }
}

impl std::fmt::Debug for Skipper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the secret.
        write!(f, "Skipper(pub={})", self.public())
    }
}

/// A skipper's public verifying key. Guaranteed non-weak: construction
/// rejects small-order points.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SkipperPub(VerifyingKey);

impl SkipperPub {
    /// Parse from 32 raw bytes. Rejects malformed encodings
    /// ([`HansaError::MalformedCrypto`]) and weak / small-order keys
    /// ([`HansaError::WeakKey`]).
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self> {
        let vk = VerifyingKey::from_bytes(&bytes)
            .map_err(|e| HansaError::MalformedCrypto(e.to_string()))?;
        if vk.is_weak() {
            return Err(HansaError::WeakKey);
        }
        Ok(Self(vk))
    }

    /// The 32-byte compressed public key.
    pub fn as_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// Verify `sig` over `domain || msg` with `verify_strict`. Returns
    /// [`HansaError::BadSignature`] on any failure.
    pub fn verify(&self, domain: &[u8], msg: &[u8], sig: &Sig) -> Result<()> {
        let signature = Signature::from_bytes(&sig.0);
        let buf = framed(domain, msg);
        self.0
            .verify_strict(&buf, &signature)
            .map_err(|_| HansaError::BadSignature)
    }
}

impl std::fmt::Display for SkipperPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex(&self.as_bytes()))
    }
}

impl std::fmt::Debug for SkipperPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SkipperPub({self})")
    }
}

/// A detached ed25519 signature (64 bytes).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Sig([u8; 64]);

impl Sig {
    /// The 64 raw signature bytes.
    pub fn as_bytes(&self) -> [u8; 64] {
        self.0
    }

    /// Reconstruct from 64 raw bytes.
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Debug for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sig({})", hex(&self.0))
    }
}

// Both wire types serialize as hex strings: the canonical signing bytes
// are produced by `canonical::Writer`, never by serde, so this encoding
// is for transport (members.log JSON) only.
impl serde::Serialize for SkipperPub {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&hex(&self.as_bytes()))
    }
}

impl<'de> serde::Deserialize<'de> for SkipperPub {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let bytes: [u8; 32] = unhex(&s).map_err(serde::de::Error::custom)?;
        SkipperPub::from_bytes(bytes).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for Sig {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&hex(&self.0))
    }
}

impl<'de> serde::Deserialize<'de> for Sig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Sig(unhex(&s).map_err(serde::de::Error::custom)?))
    }
}

/// Prepend the domain tag to the message. Single source of domain
/// separation so [`Skipper::sign`] and [`SkipperPub::verify`] agree.
fn framed(domain: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(domain.len() + msg.len());
    buf.extend_from_slice(domain);
    buf.extend_from_slice(msg);
    buf
}

/// Deterministic, length-prefixed encoding for signable structures.
///
/// Fixed-width integers are little-endian; variable-length byte strings
/// are prefixed with a `u32` length. The result is stable across runs
/// and platforms, so two encodings of equal data are byte-identical and
/// a signature over them is reproducible.
pub mod canonical {
    /// Builder for canonical signing bytes. Does **not** carry the
    /// domain tag — that is applied by [`super::Skipper::sign`].
    #[derive(Default)]
    pub struct Writer(Vec<u8>);

    impl Writer {
        /// Empty writer.
        pub fn new() -> Self {
            Self(Vec::new())
        }
        /// Append one byte (e.g. a body discriminant tag).
        pub fn u8(mut self, x: u8) -> Self {
            self.0.push(x);
            self
        }
        /// Append a little-endian `u32`.
        pub fn u32(mut self, x: u32) -> Self {
            self.0.extend_from_slice(&x.to_le_bytes());
            self
        }
        /// Append a little-endian `u64`.
        pub fn u64(mut self, x: u64) -> Self {
            self.0.extend_from_slice(&x.to_le_bytes());
            self
        }
        /// Append a little-endian `i64`.
        pub fn i64(mut self, x: i64) -> Self {
            self.0.extend_from_slice(&x.to_le_bytes());
            self
        }
        /// Append a fixed-width byte block with no length prefix (use
        /// for fixed-size fields like 16/32/64-byte ids and hashes).
        pub fn fixed(mut self, b: &[u8]) -> Self {
            self.0.extend_from_slice(b);
            self
        }
        /// Append a variable-length byte string, length-prefixed.
        pub fn bytes(mut self, b: &[u8]) -> Self {
            self.0.extend_from_slice(&(b.len() as u32).to_le_bytes());
            self.0.extend_from_slice(b);
            self
        }
        /// Append a variable-length string, length-prefixed.
        pub fn str(self, s: &str) -> Self {
            self.bytes(s.as_bytes())
        }
        /// Finish, yielding the canonical bytes.
        pub fn finish(self) -> Vec<u8> {
            self.0
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn unhex<const N: usize>(s: &str) -> std::result::Result<[u8; N], String> {
    if s.len() != N * 2 {
        return Err(format!("expected {} hex chars, got {}", N * 2, s.len()));
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

// `RngCore::fill_bytes` lives behind this trait import.
use rand::RngCore;

#[cfg(test)]
mod tests {
    use super::*;

    fn msg() -> Vec<u8> {
        canonical::Writer::new()
            .u64(7)
            .fixed(&[0xab; 32])
            .str("hello")
            .finish()
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let sk = Skipper::generate();
        let pk = sk.public();
        let m = msg();
        let sig = sk.sign(DOMAIN_LINK, &m);
        assert!(pk.verify(DOMAIN_LINK, &m, &sig).is_ok());
    }

    #[test]
    fn tampered_message_fails() {
        let sk = Skipper::generate();
        let pk = sk.public();
        let sig = sk.sign(DOMAIN_LINK, &msg());
        let mut bad = msg();
        bad[0] ^= 0x01;
        assert!(matches!(
            pk.verify(DOMAIN_LINK, &bad, &sig),
            Err(HansaError::BadSignature)
        ));
    }

    #[test]
    fn wrong_domain_fails() {
        let sk = Skipper::generate();
        let pk = sk.public();
        let m = msg();
        let sig = sk.sign(DOMAIN_GENESIS, &m);
        // Same message, different domain tag → must not verify.
        assert!(matches!(
            pk.verify(DOMAIN_LINK, &m, &sig),
            Err(HansaError::BadSignature)
        ));
    }

    #[test]
    fn wrong_key_fails() {
        let sk = Skipper::generate();
        let other = Skipper::generate();
        let m = msg();
        let sig = sk.sign(DOMAIN_LINK, &m);
        assert!(other.public().verify(DOMAIN_LINK, &m, &sig).is_err());
    }

    #[test]
    fn weak_key_rejected() {
        // The ed25519 identity element (y = 1) is a small-order point.
        let mut identity = [0u8; 32];
        identity[0] = 1;
        assert!(matches!(
            SkipperPub::from_bytes(identity),
            Err(HansaError::WeakKey)
        ));
    }

    #[test]
    fn pub_bytes_roundtrip() {
        let pk = Skipper::generate().public();
        let back = SkipperPub::from_bytes(pk.as_bytes()).unwrap();
        assert_eq!(pk, back);
    }

    #[test]
    fn seed_roundtrip_preserves_identity() {
        let sk = Skipper::generate();
        let pub1 = sk.public();
        let sk2 = Skipper::from_seed(sk.to_seed());
        assert_eq!(pub1, sk2.public());
    }

    #[test]
    fn canonical_is_stable_and_unambiguous() {
        // Same data → identical bytes.
        let a = canonical::Writer::new().u64(1).str("ab").finish();
        let b = canonical::Writer::new().u64(1).str("ab").finish();
        assert_eq!(a, b);
        // Length-prefixing prevents field-boundary ambiguity:
        // ("a","bc") must not encode the same as ("ab","c").
        let x = canonical::Writer::new().str("a").str("bc").finish();
        let y = canonical::Writer::new().str("ab").str("c").finish();
        assert_ne!(x, y);
    }

    #[test]
    fn sig_and_pub_serde_hex_roundtrip() {
        let sk = Skipper::generate();
        let pk = sk.public();
        let sig = sk.sign(DOMAIN_LINK, &msg());
        let pk_json = serde_json::to_string(&pk).unwrap();
        let sig_json = serde_json::to_string(&sig).unwrap();
        assert_eq!(serde_json::from_str::<SkipperPub>(&pk_json).unwrap(), pk);
        assert_eq!(serde_json::from_str::<Sig>(&sig_json).unwrap(), sig);
    }
}
