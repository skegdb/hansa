//! The genesis record: a hansa's root of trust.
//!
//! Founding a hansa mints a [`crate::Skipper`] and writes a genesis
//! record, self-signed by that skipper, as the seq-0 entry of the
//! members log. The genesis pins three things for the whole hansa: the
//! skipper public key (the signing authority), the embedding dimension,
//! and — via [`crate::HansaId::from_skipper`] — the identity itself.
//!
//! A joiner that was told a `HansaId` out-of-band recomputes the id from
//! the genesis `skipper_pub` and refuses to join if it does not match.
//! That is the pin: knowing which hansa to join *is* pinning its skipper
//! (see `private/m3-security.md` §5.2, decision D1).

use serde::{Deserialize, Serialize};

use crate::sign::{DOMAIN_GENESIS, Sig, Skipper, SkipperPub, canonical};
use crate::{HansaError, HansaId, Result};

/// Current genesis format version.
pub const GENESIS_V: u8 = 2;

/// Root-of-trust record for a hansa. Carried as the first log link and
/// authenticated by `sig` under [`DOMAIN_GENESIS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Genesis {
    /// Format version (`GENESIS_V`).
    pub v: u8,
    /// The hansa id. Must equal `HansaId::from_skipper(skipper_pub)`.
    pub hansa_id: HansaId,
    /// The skipper's public verifying key: the signing authority.
    pub skipper_pub: SkipperPub,
    /// Embedding dimension pinned for every member of this hansa.
    pub embedding_dim: u32,
    /// Unix seconds at founding.
    pub created_at: i64,
    /// Reserved hook for D4 (in-band skipper rotation). When false, the
    /// skipper key is fixed for the hansa's life and rotation means
    /// founding a new hansa. Not acted on in M3; recorded so the format
    /// need not change when rotation lands.
    pub rotation_allowed: bool,
}

impl Genesis {
    /// Build and sign a genesis for `skipper`. The id is derived from
    /// the skipper public key, so the returned `hansa_id` already
    /// commits to the skipper.
    pub fn found(
        skipper: &Skipper,
        embedding_dim: u32,
        created_at: i64,
        rotation_allowed: bool,
    ) -> (Self, Sig) {
        let skipper_pub = skipper.public();
        let g = Genesis {
            v: GENESIS_V,
            hansa_id: HansaId::from_skipper(&skipper_pub),
            skipper_pub,
            embedding_dim,
            created_at,
            rotation_allowed,
        };
        let sig = skipper.sign(DOMAIN_GENESIS, &g.canonical());
        (g, sig)
    }

    /// Canonical signing bytes (no domain tag; applied at sign time).
    pub fn canonical(&self) -> Vec<u8> {
        canonical::Writer::new()
            .u8(self.v)
            .fixed(self.hansa_id.as_bytes())
            .fixed(&self.skipper_pub.as_bytes())
            .u32(self.embedding_dim)
            .i64(self.created_at)
            .u8(self.rotation_allowed as u8)
            .finish()
    }

    /// Full verification of a received genesis:
    ///
    /// 1. version is recognised,
    /// 2. the id commits to the skipper ([`HansaError::IdMismatch`]),
    /// 3. the signature verifies under the embedded skipper key
    ///    ([`HansaError::BadSignature`]).
    ///
    /// Step 2 before step 3 matters: it proves the skipper key is the
    /// one the joiner pinned via the HansaId, not just *some* key that
    /// signed the record.
    pub fn verify(&self, sig: &Sig) -> Result<()> {
        if self.v != GENESIS_V {
            return Err(HansaError::MalformedCrypto(format!(
                "unsupported genesis version {}",
                self.v
            )));
        }
        if HansaId::from_skipper(&self.skipper_pub) != self.hansa_id {
            return Err(HansaError::IdMismatch);
        }
        self.skipper_pub
            .verify(DOMAIN_GENESIS, &self.canonical(), sig)
    }

    /// Verify against the `HansaId` a joiner was independently given.
    /// Equivalent to [`Self::verify`] plus an equality check that the
    /// genesis is for the hansa the caller meant to join.
    pub fn verify_for(&self, expected: HansaId, sig: &Sig) -> Result<()> {
        if self.hansa_id != expected {
            return Err(HansaError::IdMismatch);
        }
        self.verify(sig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn found_then_verify_roundtrips() {
        let sk = Skipper::generate();
        let (g, sig) = Genesis::found(&sk, 768, 1_700_000_000, false);
        assert_eq!(g.hansa_id, HansaId::from_skipper(&sk.public()));
        assert!(g.verify(&sig).is_ok());
    }

    #[test]
    fn id_commits_to_skipper() {
        let sk = Skipper::generate();
        let (mut g, sig) = Genesis::found(&sk, 4, 1, false);
        // Swap the id to a different skipper's — binding must fail.
        let other = Skipper::generate();
        g.hansa_id = HansaId::from_skipper(&other.public());
        assert!(matches!(g.verify(&sig), Err(HansaError::IdMismatch)));
    }

    #[test]
    fn swapped_skipper_pub_breaks_binding() {
        let sk = Skipper::generate();
        let (mut g, sig) = Genesis::found(&sk, 4, 1, false);
        // Keep the id, swap the embedded key: id no longer commits to it.
        g.skipper_pub = Skipper::generate().public();
        assert!(matches!(g.verify(&sig), Err(HansaError::IdMismatch)));
    }

    #[test]
    fn tampered_dim_fails_signature() {
        let sk = Skipper::generate();
        let (mut g, sig) = Genesis::found(&sk, 4, 1, false);
        g.embedding_dim = 8; // id still binds (same skipper), but sig won't.
        assert!(matches!(g.verify(&sig), Err(HansaError::BadSignature)));
    }

    #[test]
    fn verify_for_rejects_wrong_expected_id() {
        let sk = Skipper::generate();
        let (g, sig) = Genesis::found(&sk, 4, 1, false);
        let wrong = HansaId::from_skipper(&Skipper::generate().public());
        assert!(matches!(
            g.verify_for(wrong, &sig),
            Err(HansaError::IdMismatch)
        ));
        assert!(g.verify_for(g.hansa_id, &sig).is_ok());
    }

    #[test]
    fn json_transport_roundtrip() {
        let sk = Skipper::generate();
        let (g, _sig) = Genesis::found(&sk, 768, 42, true);
        let s = serde_json::to_string(&g).unwrap();
        let back: Genesis = serde_json::from_str(&s).unwrap();
        assert_eq!(g, back);
    }
}
