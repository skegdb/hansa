//! Hand-curated knowledge for the three agents in this demo.
//!
//! Each agent stores **`Note`s**: a piece of text plus a small
//! concept-axis vector standing in for what a real embedding model
//! would produce. Eight axes are enough to make sharing across agents
//! visually meaningful.

pub const DIM: u32 = 8;

/// Names for the 8 axes used in this demo. Real embeddings would not
/// have human-readable axes; we use them here so the reader can follow
/// why a record matched.
pub const CONCEPT_NAMES: [&str; 8] = [
    "rust",         // axis 0
    "python",       // axis 1
    "crypto",       // axis 2
    "vectors",      // axis 3
    "security",     // axis 4
    "federation",   // axis 5
    "architecture", // axis 6
    "performance",  // axis 7
];

/// One note in an agent's memory.
#[derive(Debug, Clone)]
pub struct Note {
    /// Stable id inside this agent's memory.
    pub id: u64,
    /// Human-readable text. Stored as the record payload so the demo
    /// can print it back from query hits.
    pub text: &'static str,
    /// Concept-axis activation. Length must equal [`DIM`].
    pub concept: [f32; 8],
    /// Tags driving the saga's tag aggregate.
    pub tags: &'static [&'static str],
    /// Whether peers in the hansa may see this note.
    pub shareable: bool,
}

/// Agent A - *work* notes: meetings, decisions, design reviews. Two
/// items are private (a 1:1 and a personal reminder) and never reach
/// other agents.
pub fn work_notes() -> Vec<Note> {
    let v = |a: usize, w: f32| {
        let mut x = [0.0f32; 8];
        x[a] = w;
        x
    };
    let mix = |pairs: &[(usize, f32)]| {
        let mut x = [0.0f32; 8];
        for (a, w) in pairs {
            x[*a] = *w;
        }
        x
    };
    vec![
        Note {
            id: 1,
            text: "2026-03-12 design meeting: agreed to derive HansaKey via BLAKE3 KDF mode, salt-as-context",
            concept: mix(&[(2, 1.0), (4, 0.4), (6, 0.6)]),
            tags: &["meeting", "decision", "crypto"],
            shareable: true,
        },
        Note {
            id: 2,
            text: "Decision: vectors stay f32 in v0.1, quantization is M2 work",
            concept: mix(&[(3, 1.0), (6, 0.5), (7, 0.5)]),
            tags: &["decision", "vectors"],
            shareable: true,
        },
        Note {
            id: 3,
            text: "Design review 2026-03-20: federation needs explicit token budget per query",
            concept: mix(&[(5, 1.0), (6, 0.7), (3, 0.3)]),
            tags: &["design", "federation"],
            shareable: true,
        },
        Note {
            id: 4,
            text: "Skipper keypair (ed25519) deferred to v0.2; document trust limits in README",
            concept: mix(&[(2, 0.6), (4, 0.8), (5, 0.5)]),
            tags: &["decision", "security"],
            shareable: true,
        },
        Note {
            id: 5,
            text: "1:1 with @alice - career conversation, NOT for the team",
            concept: v(6, 0.4),
            tags: &["meeting", "1on1", "private"],
            shareable: false,
        },
        Note {
            id: 6,
            text: "Personal: dentist Friday 14:00",
            concept: [0.0; 8],
            tags: &["personal"],
            shareable: false,
        },
        Note {
            id: 7,
            text: "Roadmap M3 covers Lifecycle traits in rigging; M4 adds events",
            concept: mix(&[(5, 0.6), (6, 0.8)]),
            tags: &["roadmap", "design"],
            shareable: true,
        },
    ]
}

/// Agent B - *research* notes: papers, blog posts, talks. All shareable
/// except a personal book-club note.
pub fn research_notes() -> Vec<Note> {
    let mix = |pairs: &[(usize, f32)]| {
        let mut x = [0.0f32; 8];
        for (a, w) in pairs {
            x[*a] = *w;
        }
        x
    };
    vec![
        Note {
            id: 1,
            text: "BLAKE3 paper: tree hashing for parallel and verifiable hashing",
            concept: mix(&[(2, 1.0), (7, 0.7)]),
            tags: &["paper", "crypto"],
            shareable: true,
        },
        Note {
            id: 2,
            text: "DiskANN / Vamana paper notes - disk-resident graph, R/L tuning",
            concept: mix(&[(3, 1.0), (6, 0.5), (7, 0.6)]),
            tags: &["paper", "vectors"],
            shareable: true,
        },
        Note {
            id: 3,
            text: "antirez blog: 'Anatomy of a vector DB' - RAM-frugal local inference",
            concept: mix(&[(3, 1.0), (5, 0.3), (6, 0.5), (7, 0.7)]),
            tags: &["blog", "vectors"],
            shareable: true,
        },
        Note {
            id: 4,
            text: "Salt-and-pepper chain crypto post - useful framing for key rotation",
            concept: mix(&[(2, 0.9), (4, 0.6)]),
            tags: &["blog", "crypto"],
            shareable: true,
        },
        Note {
            id: 5,
            text: "Personal: book club notes on Borges' 'Library of Babel'",
            concept: [0.0; 8],
            tags: &["personal"],
            shareable: false,
        },
        Note {
            id: 6,
            text: "Hanseatic League - autonomous trade alliance as a model for federation",
            concept: mix(&[(5, 0.9), (6, 0.4)]),
            tags: &["blog", "federation"],
            shareable: true,
        },
    ]
}

/// Agent C - *code* notes: idiomatic patterns and snippets. The Python
/// snippet is non-shareable because the team is Rust-only and exposing
/// Python idioms is noise to peers.
pub fn code_notes() -> Vec<Note> {
    let mix = |pairs: &[(usize, f32)]| {
        let mut x = [0.0f32; 8];
        for (a, w) in pairs {
            x[*a] = *w;
        }
        x
    };
    vec![
        Note {
            id: 1,
            text: "Pattern: Arc<dyn Trait> + injected closure for adapter-agnostic plugins",
            concept: mix(&[(0, 1.0), (6, 0.7)]),
            tags: &["pattern", "rust"],
            shareable: true,
        },
        Note {
            id: 2,
            text: "Snippet: blake3::Hasher::new_derive_key(\"hansa.key.v1\") with salted KDF",
            concept: mix(&[(0, 0.6), (2, 1.0), (4, 0.3)]),
            tags: &["snippet", "rust", "crypto"],
            shareable: true,
        },
        Note {
            id: 3,
            text: "Snippet: rayon par_iter fan-out across peers, collect to Vec<MembraneHit>",
            concept: mix(&[(0, 1.0), (5, 0.5), (7, 0.9)]),
            tags: &["snippet", "rust", "perf", "federation"],
            shareable: true,
        },
        Note {
            id: 4,
            text: "Pattern: write-to-temp + rename + fsync(dir) - atomic_write() in skeg-hull",
            concept: mix(&[(0, 0.8), (6, 0.6), (7, 0.4)]),
            tags: &["pattern", "rust", "storage"],
            shareable: true,
        },
        Note {
            id: 5,
            text: "Personal: my dotfiles repo at git@gist:dotfiles",
            concept: [0.0; 8],
            tags: &["personal"],
            shareable: false,
        },
        Note {
            id: 6,
            text: "Python: typing.Protocol vs abc.ABC tradeoffs (NOT used in our Rust codebase)",
            concept: mix(&[(1, 1.0), (6, 0.5)]),
            tags: &["snippet", "python"],
            shareable: false,
        },
    ]
}

/// Pretty-print a concept vector as `axis(weight)` pairs, dropping zeros.
pub fn render_concepts(v: &[f32]) -> String {
    let mut parts = Vec::new();
    for (i, w) in v.iter().enumerate() {
        if *w > 0.0 {
            parts.push(format!("{}({:.2})", CONCEPT_NAMES[i], w));
        }
    }
    if parts.is_empty() {
        "(none)".into()
    } else {
        parts.join(" ")
    }
}
