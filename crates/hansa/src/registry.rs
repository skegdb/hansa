//! Hansa registry: how members discover each other.
//!
//! v0.1 ships [`FileRegistry`], a filesystem-local registry suitable for
//! one user with several agent processes on one machine. It is an
//! append-only newline-delimited JSON log (`members.log`) plus an
//! occasional snapshot (`members.snap`). Compaction holds an advisory
//! lock so concurrent writers do not corrupt the file.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use skeg_rigging::TenantId;

use crate::{HansaError, HansaId, MemberRecord, Result};

/// Threshold for triggering compaction: total log size in bytes.
const COMPACTION_LOG_BYTES: u64 = 10 * 1024 * 1024;
/// Threshold for triggering compaction: leaves as a fraction of joins.
const COMPACTION_LEAVE_RATIO: f32 = 0.25;

/// Mechanism by which members of a hansa discover each other.
pub trait Registry: Send + Sync {
    /// Add a member to the named hansa. Idempotent: re-joining with the
    /// same `tenant_id` updates the record but does not duplicate it.
    fn join(&self, hansa: HansaId, member: MemberRecord) -> Result<()>;

    /// Remove a member from the named hansa. Idempotent.
    fn leave(&self, hansa: HansaId, tenant: TenantId) -> Result<()>;

    /// List the currently-active members of `hansa`.
    fn members(&self, hansa: HansaId) -> Result<Vec<MemberRecord>>;
}

/// Filesystem-local registry. Default root is `~/.hansa/`.
pub struct FileRegistry {
    /// Root directory; each hansa gets a subdirectory keyed by its id.
    pub root: PathBuf,
}

impl FileRegistry {
    /// Construct against an explicit root directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Default root: `$HOME/.hansa`. Falls back to `./.hansa` if `HOME`
    /// is unset.
    pub fn default_root() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self::new(home.join(".hansa"))
    }

    /// Path to a hansa's directory under [`Self::root`].
    pub fn hansa_dir(&self, hansa: HansaId) -> PathBuf {
        self.root.join(hansa.as_hex())
    }

    fn log_path(&self, hansa: HansaId) -> PathBuf {
        self.hansa_dir(hansa).join("members.log")
    }

    fn snap_path(&self, hansa: HansaId) -> PathBuf {
        self.hansa_dir(hansa).join("members.snap")
    }

    fn lock_path(&self, hansa: HansaId) -> PathBuf {
        self.hansa_dir(hansa).join("lock")
    }

    fn ensure_dir(&self, hansa: HansaId) -> Result<()> {
        std::fs::create_dir_all(self.hansa_dir(hansa))?;
        Ok(())
    }

    fn snapshot_members(&self, hansa: HansaId) -> Result<HashMap<TenantId, MemberRecord>> {
        let mut active: HashMap<TenantId, MemberRecord> = HashMap::new();

        // 1. Load snapshot, if any.
        let snap = self.snap_path(hansa);
        if snap.exists() {
            let bytes = std::fs::read(&snap)?;
            if !bytes.is_empty() {
                let entries: Vec<MemberRecord> = serde_json::from_slice(&bytes)?;
                for m in entries {
                    active.insert(m.tenant_id, m);
                }
            }
        }

        // 2. Replay log, if any.
        let log = self.log_path(hansa);
        if log.exists() {
            let f = std::fs::File::open(&log)?;
            let reader = BufReader::new(f);
            for (lineno, line) in reader.lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let event: Event = serde_json::from_str(&line).map_err(|e| {
                    HansaError::RegistryMalformed(format!(
                        "{}:{}: {e}",
                        log.display(),
                        lineno + 1
                    ))
                })?;
                event.apply(&mut active);
            }
        }

        Ok(active)
    }

    fn append_event(&self, hansa: HansaId, event: &Event) -> Result<()> {
        self.ensure_dir(hansa)?;
        let path = self.log_path(hansa);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut buf = serde_json::to_string(event)?;
        buf.push('\n');
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    /// Force a snapshot rewrite: serialise the currently-active members
    /// into `members.snap` (atomic via temp+rename) and truncate the
    /// log. Acquires the advisory lock for the duration.
    pub fn compact(&self, hansa: HansaId) -> Result<()> {
        self.ensure_dir(hansa)?;
        let lock_path = self.lock_path(hansa);
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false) // 0-byte lock file; explicit for clippy
            .open(&lock_path)?;
        lock_file.lock_exclusive()?;

        let result = (|| -> Result<()> {
            let active = self.snapshot_members(hansa)?;
            let mut list: Vec<MemberRecord> = active.into_values().collect();
            list.sort_by(|a, b| a.tenant_id.0.cmp(&b.tenant_id.0));
            let bytes = serde_json::to_vec(&list)?;
            let snap_path = self.snap_path(hansa);
            let tmp_path = snap_path.with_extension("snap.tmp");
            std::fs::write(&tmp_path, &bytes)?;
            std::fs::rename(&tmp_path, &snap_path)?;
            // Truncate the log.
            let log = self.log_path(hansa);
            std::fs::File::create(&log)?;
            Ok(())
        })();

        FileExt::unlock(&lock_file)?;
        result
    }

    fn maybe_compact(&self, hansa: HansaId) -> Result<()> {
        let log_size = std::fs::metadata(self.log_path(hansa))
            .map(|m| m.len())
            .unwrap_or(0);
        let mut joins = 0u64;
        let mut leaves = 0u64;
        if let Ok(f) = std::fs::File::open(self.log_path(hansa)) {
            for line in BufReader::new(f).lines().map_while(std::result::Result::ok) {
                if line.contains("\"event\":\"join\"") {
                    joins += 1;
                } else if line.contains("\"event\":\"leave\"") {
                    leaves += 1;
                }
            }
        }
        let leave_ratio = if joins == 0 {
            0.0
        } else {
            leaves as f32 / joins as f32
        };
        if log_size >= COMPACTION_LOG_BYTES || leave_ratio >= COMPACTION_LEAVE_RATIO {
            self.compact(hansa)?;
        }
        Ok(())
    }
}

impl Registry for FileRegistry {
    fn join(&self, hansa: HansaId, member: MemberRecord) -> Result<()> {
        // Enforce dim consistency across members.
        let existing = self.snapshot_members(hansa)?;
        if let Some(any) = existing.values().next()
            && any.embedding_dim != member.embedding_dim
        {
            return Err(HansaError::DimMismatch {
                existing: any.embedding_dim,
                joining: member.embedding_dim,
            });
        }
        self.append_event(hansa, &Event::Join(member))?;
        self.maybe_compact(hansa)
    }

    fn leave(&self, hansa: HansaId, tenant: TenantId) -> Result<()> {
        self.append_event(hansa, &Event::Leave { tenant_id: tenant })?;
        self.maybe_compact(hansa)
    }

    fn members(&self, hansa: HansaId) -> Result<Vec<MemberRecord>> {
        let active = self.snapshot_members(hansa)?;
        let mut list: Vec<MemberRecord> = active.into_values().collect();
        list.sort_by(|a, b| a.tenant_id.0.cmp(&b.tenant_id.0));
        Ok(list)
    }
}

// ============ event encoding ============

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "lowercase")]
enum Event {
    Join(MemberRecord),
    Leave {
        #[serde(with = "tenant_id_hex")]
        tenant_id: TenantId,
    },
}

impl Event {
    fn apply(&self, active: &mut HashMap<TenantId, MemberRecord>) {
        match self {
            Event::Join(m) => {
                active.insert(m.tenant_id, m.clone());
            }
            Event::Leave { tenant_id } => {
                active.remove(tenant_id);
            }
        }
    }
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
