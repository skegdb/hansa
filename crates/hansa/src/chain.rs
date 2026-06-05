//! The signed, hash-chained members log.
//!
//! Each entry is a [`Link`] carrying its sequence number, the blake3
//! hash of the previous link, a body (genesis / admit / revoke), the
//! key that signed it, and the signature. Replay folds the chain into
//! the active member set, but only after four checks per link:
//!
//! 1. `seq` is exactly previous + 1,
//! 2. `prev` is the hash of the previous accepted link,
//! 3. the signer is the hansa's skipper (the genesis key),
//! 4. the signature verifies (`verify_strict`).
//!
//! Any failure aborts replay loudly — a broken chain is a security
//! event, not something to skip past. This moves trust off the
//! shared-writable filesystem and into the signatures, so a member who
//! can write the log still cannot forge or evict membership.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use skeg_rigging::TenantId;

use crate::genesis::Genesis;
use crate::member::MemberRecord;
use crate::sign::{DOMAIN_LINK, Sig, Skipper, SkipperPub, canonical};
use crate::{HansaError, HansaId, Result};

/// A signed statement in the members log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Body {
    /// Root of trust; only valid at seq 0.
    Genesis(Genesis),
    /// The skipper admits a member.
    Admit {
        /// The member being admitted.
        member: MemberRecord,
        /// The member's own signing key, reserved for cross-tenant
        /// provenance later. `None` in the current trust model.
        #[serde(default)]
        member_pub: Option<[u8; 32]>,
    },
    /// The skipper revokes a member by tenant id.
    Revoke {
        /// Tenant being revoked.
        #[serde(with = "crate::member::tenant_id_hex")]
        tenant_id: TenantId,
        /// Unix seconds of the revocation.
        at: i64,
        /// Optional human note.
        #[serde(default)]
        reason: Option<String>,
    },
}

impl Body {
    fn write_canonical(&self, w: canonical::Writer) -> canonical::Writer {
        match self {
            Body::Genesis(g) => w.u8(0).fixed(&g.canonical()),
            Body::Admit { member, member_pub } => {
                let w = w.u8(1).bytes(&member.canonical());
                match member_pub {
                    None => w.u8(0),
                    Some(pk) => w.u8(1).fixed(pk),
                }
            }
            Body::Revoke {
                tenant_id,
                at,
                reason,
            } => {
                let w = w.u8(2).fixed(&tenant_id.0).i64(*at);
                match reason {
                    None => w.u8(0),
                    Some(r) => w.u8(1).str(r),
                }
            }
        }
    }
}

/// One entry of the chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    /// Position in the chain; 0 is the genesis.
    pub seq: u64,
    /// blake3 of the previous link's canonical bytes; zeros at seq 0.
    #[serde(with = "hash_hex")]
    pub prev: [u8; 32],
    /// What this link states.
    pub body: Body,
    /// Public key that signed this link.
    pub signed_by: SkipperPub,
    /// Signature over the link's canonical bytes.
    pub sig: Sig,
}

impl Link {
    /// The genesis link (seq 0). `sig` is the genesis signature produced
    /// by [`Genesis::found`] (under the genesis domain), not a link
    /// signature.
    pub fn genesis(g: Genesis, sig: Sig) -> Self {
        let signed_by = g.skipper_pub;
        Link {
            seq: 0,
            prev: [0u8; 32],
            body: Body::Genesis(g),
            signed_by,
            sig,
        }
    }

    /// Build and sign an admit/revoke link with the skipper.
    pub fn signed(skipper: &Skipper, seq: u64, prev: [u8; 32], body: Body) -> Self {
        let mut link = Link {
            seq,
            prev,
            body,
            signed_by: skipper.public(),
            sig: Sig::from_bytes([0u8; 64]),
        };
        link.sig = skipper.sign(DOMAIN_LINK, &link.canonical());
        link
    }

    /// Canonical signing bytes: everything but the signature itself.
    pub fn canonical(&self) -> Vec<u8> {
        let w = canonical::Writer::new().u64(self.seq).fixed(&self.prev);
        self.body
            .write_canonical(w)
            .fixed(&self.signed_by.as_bytes())
            .finish()
    }

    /// Hash that the *next* link must carry as its `prev`.
    pub fn hash(&self) -> [u8; 32] {
        *blake3::hash(&self.canonical()).as_bytes()
    }
}

/// Result of a successful replay.
#[derive(Debug, Clone)]
pub struct ReplayOutcome {
    /// The verified genesis.
    pub genesis: Genesis,
    /// Active members, sorted by tenant id.
    pub active: Vec<MemberRecord>,
    /// Sequence number of the last link.
    pub head_seq: u64,
    /// Hash of the last link (the current chain head).
    pub head_hash: [u8; 32],
}

/// Replay and verify a chain into its active member set.
///
/// `expected_id`, when given, additionally pins the genesis to the id
/// the caller meant to join (the out-of-band fingerprint check).
pub fn replay(links: &[Link], expected_id: Option<HansaId>) -> Result<ReplayOutcome> {
    let first = links.first().ok_or(HansaError::ChainBroken { seq: 0 })?;
    let Body::Genesis(g) = &first.body else {
        return Err(HansaError::ChainBroken { seq: 0 });
    };
    if first.seq != 0 || first.prev != [0u8; 32] || first.signed_by != g.skipper_pub {
        return Err(HansaError::ChainBroken { seq: 0 });
    }
    // Genesis is self-signed under its own domain; this also checks the
    // id<->skipper binding (and the expected id, if supplied).
    match expected_id {
        Some(id) => g.verify_for(id, &first.sig)?,
        None => g.verify(&first.sig)?,
    }

    let skipper = g.skipper_pub;
    let mut active: HashMap<TenantId, MemberRecord> = HashMap::new();
    let mut prev_hash = first.hash();
    let mut prev_seq = 0u64;

    for link in &links[1..] {
        if link.seq != prev_seq + 1 || link.prev != prev_hash {
            return Err(HansaError::ChainBroken { seq: link.seq });
        }
        if link.signed_by != skipper {
            return Err(HansaError::Unauthorized);
        }
        link.signed_by
            .verify(DOMAIN_LINK, &link.canonical(), &link.sig)
            .map_err(|_| HansaError::ChainBroken { seq: link.seq })?;

        match &link.body {
            Body::Genesis(_) => return Err(HansaError::ChainBroken { seq: link.seq }),
            Body::Admit { member, .. } => {
                active.insert(member.tenant_id, member.clone());
            }
            Body::Revoke { tenant_id, .. } => {
                active.remove(tenant_id);
            }
        }
        prev_hash = link.hash();
        prev_seq = link.seq;
    }

    let mut active: Vec<MemberRecord> = active.into_values().collect();
    active.sort_by(|a, b| a.tenant_id.0.cmp(&b.tenant_id.0));
    Ok(ReplayOutcome {
        genesis: g.clone(),
        active,
        head_seq: prev_seq,
        head_hash: prev_hash,
    })
}

mod hash_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(h: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        let mut buf = String::with_capacity(64);
        for b in h {
            buf.push_str(&format!("{b:02x}"));
        }
        s.serialize_str(&buf)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 64 {
            return Err(serde::de::Error::custom("expected 64-char hex hash"));
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(serde::de::Error::custom)?;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skeg_rigging_net::TenantLocation;
    use std::path::PathBuf;

    fn member(seed: u8) -> MemberRecord {
        MemberRecord {
            tenant_id: TenantId::from_bytes([seed; 16]),
            tenant_location: TenantLocation::Path {
                path: PathBuf::from(format!("/t/{seed}")),
            },
            embedding_dim: 8,
            joined_at: 1_700_000_000 + seed as i64,
        }
    }

    fn admit(skipper: &Skipper, seq: u64, prev: [u8; 32], seed: u8) -> Link {
        Link::signed(
            skipper,
            seq,
            prev,
            Body::Admit {
                member: member(seed),
                member_pub: None,
            },
        )
    }

    /// Genesis + a run of admits, chained correctly.
    fn build_chain(skipper: &Skipper, seeds: &[u8]) -> Vec<Link> {
        let (g, sig) = Genesis::found(skipper, 8, 1, false);
        let mut links = vec![Link::genesis(g, sig)];
        for &s in seeds {
            let prev = links.last().unwrap().hash();
            let seq = links.len() as u64;
            links.push(admit(skipper, seq, prev, s));
        }
        links
    }

    #[test]
    fn replay_accepts_valid_chain() {
        let sk = Skipper::generate();
        let links = build_chain(&sk, &[1, 2, 3]);
        let out = replay(&links, None).unwrap();
        assert_eq!(out.active.len(), 3);
        assert_eq!(out.head_seq, 3);
    }

    #[test]
    fn revoke_removes_member() {
        let sk = Skipper::generate();
        let mut links = build_chain(&sk, &[1, 2]);
        let prev = links.last().unwrap().hash();
        links.push(Link::signed(
            &sk,
            3,
            prev,
            Body::Revoke {
                tenant_id: TenantId::from_bytes([1; 16]),
                at: 2,
                reason: Some("left".into()),
            },
        ));
        let out = replay(&links, None).unwrap();
        assert_eq!(out.active.len(), 1);
        assert_eq!(out.active[0].tenant_id, TenantId::from_bytes([2; 16]));
    }

    #[test]
    fn forged_admit_without_skipper_sig_rejected() {
        let sk = Skipper::generate();
        let mut links = build_chain(&sk, &[1]);
        // An impostor with its own keypair appends a correctly-chained
        // admit. Signature is valid for the impostor, but the signer is
        // not the genesis skipper.
        let impostor = Skipper::generate();
        let prev = links.last().unwrap().hash();
        links.push(admit(&impostor, 2, prev, 9));
        assert!(matches!(
            replay(&links, None),
            Err(HansaError::Unauthorized)
        ));
    }

    #[test]
    fn reordered_links_rejected() {
        let sk = Skipper::generate();
        let mut links = build_chain(&sk, &[1, 2]);
        links.swap(1, 2); // breaks seq + prev
        assert!(matches!(
            replay(&links, None),
            Err(HansaError::ChainBroken { .. })
        ));
    }

    #[test]
    fn rewritten_body_breaks_chain() {
        let sk = Skipper::generate();
        let mut links = build_chain(&sk, &[1, 2]);
        // Tamper link 1's member after the fact: its signature no longer
        // covers the body, and its hash no longer matches link 2's prev.
        if let Body::Admit { member, .. } = &mut links[1].body {
            member.embedding_dim = 999;
        }
        assert!(matches!(
            replay(&links, None),
            Err(HansaError::ChainBroken { .. })
        ));
    }

    #[test]
    fn truncated_genesis_only_is_empty() {
        let sk = Skipper::generate();
        let links = build_chain(&sk, &[]);
        let out = replay(&links, None).unwrap();
        assert!(out.active.is_empty());
        assert_eq!(out.head_seq, 0);
    }

    #[test]
    fn empty_chain_rejected() {
        assert!(matches!(
            replay(&[], None),
            Err(HansaError::ChainBroken { seq: 0 })
        ));
    }

    #[test]
    fn expected_id_mismatch_rejected() {
        let sk = Skipper::generate();
        let links = build_chain(&sk, &[1]);
        let wrong = HansaId::from_skipper(&Skipper::generate().public());
        assert!(matches!(
            replay(&links, Some(wrong)),
            Err(HansaError::IdMismatch)
        ));
        let right = HansaId::from_skipper(&sk.public());
        assert!(replay(&links, Some(right)).is_ok());
    }

    #[test]
    fn link_json_roundtrip() {
        let sk = Skipper::generate();
        let links = build_chain(&sk, &[1]);
        let s = serde_json::to_string(&links).unwrap();
        let back: Vec<Link> = serde_json::from_str(&s).unwrap();
        let out = replay(&back, None).unwrap();
        assert_eq!(out.active.len(), 1);
    }
}
