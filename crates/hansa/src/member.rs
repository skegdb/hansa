//! Member record stored in the registry.

use serde::{Deserialize, Serialize};
use skeg_rigging::TenantId;
use skeg_rigging_net::TenantLocation;

/// A single member of a hansa: tenant id, where its tenant lives
/// (filesystem path *or* network URL), and the embedding dim it serves.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberRecord {
    /// Stable id of the member's tenant.
    #[serde(with = "tenant_id_hex")]
    pub tenant_id: TenantId,
    /// Where the tenant lives. Cross-transport: filesystem, RESP3,
    /// or HTTP. The membrane's `PeerOpener` dispatches on this.
    pub tenant_location: TenantLocation,
    /// Embedding dimension. All members of one hansa must agree.
    pub embedding_dim: u32,
    /// Unix seconds when the member joined.
    pub joined_at: i64,
}

mod tenant_id_hex {
    use serde::{Deserialize, Deserializer, Serializer};
    use skeg_rigging::TenantId;

    pub fn serialize<S: Serializer>(id: &TenantId, s: S) -> Result<S::Ok, S::Error> {
        let mut buf = String::with_capacity(32);
        for b in id.0 {
            buf.push_str(&format!("{b:02x}"));
        }
        s.serialize_str(&buf)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<TenantId, D::Error> {
        let s: String = Deserialize::deserialize(d)?;
        if s.len() != 32 {
            return Err(serde::de::Error::custom(format!(
                "expected 32-char hex tenant id, got {}",
                s.len()
            )));
        }
        let mut out = [0u8; 16];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(TenantId(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn json_roundtrip_path_location() {
        let rec = MemberRecord {
            tenant_id: TenantId::from_bytes([0xab; 16]),
            tenant_location: TenantLocation::Path {
                path: PathBuf::from("/tmp/t"),
            },
            embedding_dim: 768,
            joined_at: 1_700_000_000,
        };
        let s = serde_json::to_string(&rec).unwrap();
        let back: MemberRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(rec, back);
        assert!(s.contains(&"ab".repeat(16)));
        assert!(s.contains("\"kind\":\"path\""));
    }

    #[test]
    fn json_roundtrip_resp3_location() {
        let rec = MemberRecord {
            tenant_id: TenantId::from_bytes([0x11; 16]),
            tenant_location: TenantLocation::Resp3 {
                endpoint: "host:6379".into(),
                auth: Some(("alice".into(), "secret".into())),
            },
            embedding_dim: 4,
            joined_at: 1,
        };
        let s = serde_json::to_string(&rec).unwrap();
        let back: MemberRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(rec, back);
        assert!(s.contains("\"kind\":\"resp3\""));
    }
}
