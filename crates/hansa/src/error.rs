//! Top-level hansa error.

/// Errors surfaced by the hansa crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HansaError {
    /// I/O failure on a hansa-managed file (registry, saga, lock).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Hull format error (saga read/write).
    #[error("hull error: {0}")]
    Hull(#[from] skeg_hull::Error),
    /// Rigging error (vault open or query).
    #[error("rigging open error: {0}")]
    Open(#[from] skeg_rigging::OpenError),
    /// Rigging query error.
    #[error("rigging query error: {0}")]
    Query(#[from] skeg_rigging::QueryError),
    /// JSON encoding / decoding error (members.log entries).
    #[error("members.log JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Keystore did not have the requested key.
    #[error("keystore: key not found for slot '{0}'")]
    KeyNotFound(String),
    /// Caller supplied a key of the wrong length (HansaKey is 32 bytes).
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),
    /// A registry operation tried to join a hansa whose existing
    /// members use a different embedding dimension.
    #[error("embedding dim mismatch in registry: existing {existing}, joining {joining}")]
    DimMismatch {
        /// Dim already established by other members.
        existing: u32,
        /// Dim the would-be member declared.
        joining: u32,
    },
    /// Malformed registry data (truncated record, invalid JSON line,
    /// etc.).
    #[error("registry malformed: {0}")]
    RegistryMalformed(String),
    /// Catch-all for invariant violations.
    #[error("invariant: {0}")]
    Invariant(String),
}
