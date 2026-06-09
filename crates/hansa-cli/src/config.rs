//! Persistent CLI state under `~/.hansa/`.
//!
//! `cli.toml` can hold several local agents (members) and remembers which
//! one is active. Adding an agent appends to the list; removing one drops
//! it. The trust-group secret itself is never stored here; it is derived
//! from a passphrase and kept by the [`hansa::FileKeystore`].

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// One local agent: a member of one hansa, backed by one vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    /// Human name of the hansa (keystore slot and key salt).
    pub hansa: String,
    /// Friendly name for this agent.
    pub tenant: String,
    /// 16-byte tenant id, hex-encoded.
    pub tenant_id_hex: String,
    /// Embedding endpoint (Ollama-compatible `/api/embed`).
    pub embed_url: String,
    /// Embedding model name.
    pub embed_model: String,
    /// Embedding dimension (probed once at init, fixed for the vault).
    pub dim: u32,
    /// Monotonic record id allocator for this agent's vault.
    pub next_record_id: u64,
}

impl Member {
    /// 16-byte tenant id decoded from hex.
    pub fn tenant_id_bytes(&self) -> Result<[u8; 16]> {
        let raw = decode_hex(&self.tenant_id_hex)?;
        raw.as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("tenant_id must be 16 bytes"))
    }

    /// Hand out the next record id and advance the counter.
    pub fn alloc_record_id(&mut self) -> u64 {
        let id = self.next_record_id;
        self.next_record_id += 1;
        id
    }
}

/// On-disk CLI configuration: a roster of local agents plus the active one.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// `"<hansa>/<tenant>"` of the active agent.
    #[serde(default)]
    pub active: String,
    /// All agents this machine runs.
    #[serde(default)]
    pub members: Vec<Member>,
}

impl Config {
    /// `~/.hansa`, the root for registry, keys, vaults and this config.
    pub fn root() -> PathBuf {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".hansa")
    }

    /// Path of the config file.
    pub fn path() -> PathBuf {
        Self::root().join("cli.toml")
    }

    /// Load the saved config, or a fresh empty one if none exists.
    pub fn load() -> Result<Config> {
        let p = Self::path();
        if !p.exists() {
            return Ok(Config::default());
        }
        let text = std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
        toml::from_str(&text).with_context(|| format!("parse {}", p.display()))
    }

    /// Persist the config (creating `~/.hansa` if needed).
    pub fn save(&self) -> Result<()> {
        let root = Self::root();
        std::fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        let text = toml::to_string_pretty(self).context("serialize config")?;
        std::fs::write(Self::path(), text).context("write cli.toml")?;
        Ok(())
    }

    /// Composite key for a member.
    pub fn key_of(hansa: &str, tenant: &str) -> String {
        format!("{hansa}/{tenant}")
    }

    /// Index of the active member, or a hint to run `hansa init`.
    pub fn active_index(&self) -> Result<usize> {
        if self.members.is_empty() {
            anyhow::bail!("no agents yet, run `hansa init` first");
        }
        self.members
            .iter()
            .position(|m| Self::key_of(&m.hansa, &m.tenant) == self.active)
            .context("active agent missing; run `hansa use <tenant>` to pick one")
    }

    /// The active member (clone).
    pub fn active_member(&self) -> Result<Member> {
        Ok(self.members[self.active_index()?].clone())
    }

    /// Find a member by (hansa, tenant).
    pub fn find(&self, hansa: &str, tenant: &str) -> Option<usize> {
        self.members
            .iter()
            .position(|m| m.hansa == hansa && m.tenant == tenant)
    }

    /// Find members by tenant name across all hansas.
    pub fn find_by_tenant(&self, tenant: &str) -> Vec<usize> {
        self.members
            .iter()
            .enumerate()
            .filter(|(_, m)| m.tenant == tenant)
            .map(|(i, _)| i)
            .collect()
    }

    /// Insert or replace a member and make it active.
    pub fn upsert_active(&mut self, m: Member) {
        self.active = Self::key_of(&m.hansa, &m.tenant);
        match self.find(&m.hansa, &m.tenant) {
            Some(i) => self.members[i] = m,
            None => self.members.push(m),
        }
    }

    /// Remove a member; if it was active, clear the active pointer.
    pub fn remove(&mut self, hansa: &str, tenant: &str) -> bool {
        let key = Self::key_of(hansa, tenant);
        let before = self.members.len();
        self.members.retain(|m| Self::key_of(&m.hansa, &m.tenant) != key);
        if self.active == key {
            self.active = self
                .members
                .first()
                .map(|m| Self::key_of(&m.hansa, &m.tenant))
                .unwrap_or_default();
        }
        self.members.len() != before
    }
}

/// Lowercase hex of bytes.
pub fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Decode a hex string to bytes.
pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        anyhow::bail!("hex string has odd length");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).context("invalid hex digit"))
        .collect()
}
