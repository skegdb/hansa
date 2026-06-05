//! Migrate a v1 (unsigned) hansa to a v2 (skipper-signed) one.
//!
//! v1 and v2 are different trust models with different id namespaces, so
//! there is no in-place upgrade. Run by a holder of the v1 symmetric
//! key: read the active members from the old unsigned `members.log`,
//! found a fresh v2 hansa with a skipper, and re-admit each member with
//! a signed link. The new (v2) [`HansaId`] is returned for the operator
//! to redistribute.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};
use skeg_rigging::TenantId;

use crate::chain::Body;
use crate::member::MemberRecord;
use crate::sign::Skipper;
use crate::{HansaId, HansaKey, Registry, Result};

/// The v1 members-log event format, kept here only so migration can read
/// old logs. The live path no longer writes this.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum V1Event {
    Join(MemberRecord),
    Leave {
        #[serde(with = "crate::member::tenant_id_hex")]
        tenant_id: TenantId,
    },
}

/// Replay a v1 (unsigned) members log + snapshot into its active set.
///
/// `v1_root` is the registry root that held the v1 hansa; the log lives
/// at `<v1_root>/<v1_id>/members.{snap,log}`. Errors if the files are
/// not in the v1 format (e.g. already migrated to signed links).
pub fn read_v1_members(v1_root: &Path, v1_id: HansaId) -> Result<Vec<MemberRecord>> {
    let dir = v1_root.join(v1_id.as_hex());
    let mut active: HashMap<TenantId, MemberRecord> = HashMap::new();

    let snap = dir.join("members.snap");
    if snap.exists() {
        let bytes = std::fs::read(&snap)?;
        if !bytes.is_empty() {
            for m in serde_json::from_slice::<Vec<MemberRecord>>(&bytes)? {
                active.insert(m.tenant_id, m);
            }
        }
    }

    let log = dir.join("members.log");
    if log.exists() {
        for line in BufReader::new(std::fs::File::open(&log)?).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<V1Event>(&line)? {
                V1Event::Join(m) => {
                    active.insert(m.tenant_id, m);
                }
                V1Event::Leave { tenant_id } => {
                    active.remove(&tenant_id);
                }
            }
        }
    }

    let mut out: Vec<MemberRecord> = active.into_values().collect();
    out.sort_by(|a, b| a.tenant_id.0.cmp(&b.tenant_id.0));
    Ok(out)
}

/// Migrate the v1 hansa identified by `key` into a fresh v2 hansa under
/// `skipper`. Founds the v2 chain in `registry` and re-admits every
/// active v1 member. Returns the new (v2) [`HansaId`].
///
/// `embedding_dim` pins the v2 genesis; pass the dimension the members
/// already share.
pub fn migrate_v1(
    registry: &dyn Registry,
    v1_root: &Path,
    key: &HansaKey,
    skipper: &Skipper,
    embedding_dim: u32,
    now: i64,
) -> Result<HansaId> {
    let v1_id = key.hansa_id();
    let members = read_v1_members(v1_root, v1_id)?;

    let v2_id = HansaId::from_skipper(&skipper.public());
    registry.found(v2_id, skipper, embedding_dim, now)?;
    for member in members {
        registry.append_next(
            v2_id,
            skipper,
            Body::Admit {
                member,
                member_pub: None,
            },
        )?;
    }
    Ok(v2_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileRegistry;
    use skeg_rigging_net::TenantLocation;
    use std::path::PathBuf;

    fn mk_member(seed: u8) -> MemberRecord {
        MemberRecord {
            tenant_id: TenantId::from_bytes([seed; 16]),
            tenant_location: TenantLocation::Path {
                path: PathBuf::from(format!("/t/{seed}")),
            },
            embedding_dim: 8,
            joined_at: 1,
        }
    }

    /// Hand-write a v1 log (the format the current code no longer emits).
    fn write_v1_log(root: &Path, id: HansaId, events: &[V1Event]) {
        let dir = root.join(id.as_hex());
        std::fs::create_dir_all(&dir).unwrap();
        let mut buf = String::new();
        for e in events {
            buf.push_str(&serde_json::to_string(e).unwrap());
            buf.push('\n');
        }
        std::fs::write(dir.join("members.log"), buf).unwrap();
    }

    #[test]
    fn migrates_active_members_into_a_signed_chain() {
        let dir = tempfile::tempdir().unwrap();
        let key = HansaKey::from_bytes([1; 32]);
        let v1_id = key.hansa_id();

        // v1 log: join 1, join 2, join 3, leave 2 -> active {1, 3}.
        write_v1_log(
            dir.path(),
            v1_id,
            &[
                V1Event::Join(mk_member(1)),
                V1Event::Join(mk_member(2)),
                V1Event::Join(mk_member(3)),
                V1Event::Leave {
                    tenant_id: TenantId::from_bytes([2; 16]),
                },
            ],
        );

        let reg = FileRegistry::new(dir.path());
        let skipper = Skipper::from_seed([9; 32]);
        let v2_id = migrate_v1(&reg, dir.path(), &key, &skipper, 8, 1).unwrap();

        assert_ne!(v2_id, v1_id, "v2 id must differ from v1");
        let members: Vec<u8> = reg
            .members(v2_id)
            .unwrap()
            .iter()
            .map(|m| m.tenant_id.0[0])
            .collect();
        assert_eq!(members, vec![1, 3]);
    }

    #[test]
    fn refuses_a_signed_log_as_v1() {
        // A v2 (signed) log must not be read as v1.
        let dir = tempfile::tempdir().unwrap();
        let reg = FileRegistry::new(dir.path());
        let skipper = Skipper::from_seed([5; 32]);
        let id = HansaId::from_skipper(&skipper.public());
        reg.found(id, &skipper, 8, 1).unwrap();
        // Reading the signed log as v1 events fails.
        assert!(read_v1_members(dir.path(), id).is_err());
    }
}
