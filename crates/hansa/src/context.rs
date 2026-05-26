//! Context assembly: turn membrane hits into an LLM-ready bundle.
//!
//! A membrane query returns [`MembraneHit`]s sorted by similarity. To
//! feed a language model, applications then have to:
//!
//! 1. Drop hits below a quality threshold.
//! 2. De-duplicate near-identical records that several peers might
//!    have copied to each other's memory.
//! 3. Truncate to fit the model's context window (in *tokens*, not
//!    record count).
//! 4. Render with provenance markers so the model - or a downstream
//!    re-ranker - can tell what came from whom.
//!
//! [`ContextBuilder`] performs the same four steps in one place. The
//! tokenizer is injected so hansa stays model-agnostic; a coarse
//! [`CharCountTokenizer`] ships as the default fallback.
//!
//! ```rust,ignore
//! let hits = hansa.query(&embedding)?.execute()?;
//! let bundle = ContextBuilder::from_hits(hits)
//!     .min_similarity(0.3)
//!     .token_budget(2048)
//!     .dedup(true)
//!     .build();
//! println!("{}", bundle.render_markdown());
//! ```

use std::sync::Arc;

use crate::membrane::{HitOrigin, MembraneHit};
use skeg_rigging::{RecordId, TenantId};

/// One item that survived the filter and made it into a [`ContextBundle`].
#[derive(Debug, Clone)]
pub struct ContextItem {
    /// Where the underlying record came from.
    pub origin: HitOrigin,
    /// Record identifier in the producing tenant.
    pub record_id: RecordId,
    /// Similarity score reported by the producing tenant.
    pub similarity: f32,
    /// UTF-8 lossy decoding of the payload bytes.
    pub content: String,
    /// Tokens consumed by `content` according to the chosen tokenizer.
    pub tokens: usize,
    /// 32-byte BLAKE3 of a normalised form of `content`. Stable enough
    /// to deduplicate copy-pasted snippets across peers.
    pub dedup_key: [u8; 32],
    /// Vector for the underlying record, propagated from
    /// [`MembraneHit::embedding`]. Available for local + filesystem peers;
    /// `None` for RESP3 / HTTP peers whose transports don't surface the
    /// raw embedding. Semantic dedup runs only when both compared items
    /// carry a vector.
    pub embedding: Option<Vec<f32>>,
    /// Every source that contributed to this item's dedup group. Starts
    /// as `vec![origin]`; each time a byte/sentence/semantic-dedup
    /// dropped duplicate would have appeared, its origin is appended
    /// here instead. Renderers use this to emit one corroboration
    /// marker (`+ peer-XX, peer-YY`) rather than dropping the signal.
    pub sources: Vec<HitOrigin>,
}

impl ContextItem {
    /// Short label for the principal source. Stable since v0.1; renders
    /// just `self.origin` even when [`Self::sources`] holds more
    /// entries. Use [`Self::sources_label`] when the corroboration
    /// list is wanted.
    pub fn origin_label(&self) -> String {
        origin_label(self.origin)
    }

    /// Comma-joined label of every source in [`Self::sources`]. With a
    /// single source this is identical to [`Self::origin_label`];
    /// with N sources the principal is rendered as
    /// `<principal> + peer-XX, peer-YY` (N - 1 trailing markers).
    pub fn sources_label(&self) -> String {
        if self.sources.len() <= 1 {
            return self.origin_label();
        }
        let mut out = origin_label(self.sources[0]);
        out.push_str(" + ");
        let rest: Vec<String> = self.sources[1..].iter().copied().map(origin_label).collect();
        out.push_str(&rest.join(", "));
        out
    }
}

fn origin_label(o: HitOrigin) -> String {
    match o {
        HitOrigin::Local => "LOCAL".to_string(),
        HitOrigin::Remote { tenant_id } => format!("peer-{}", short_id(tenant_id)),
    }
}

/// Outcome of a [`ContextBuilder::build`] call.
#[derive(Debug, Clone, Default)]
pub struct ContextBundle {
    /// Items that survived all filters, ordered by descending
    /// similarity.
    pub items: Vec<ContextItem>,
    /// Sum of `tokens` across kept items.
    pub total_tokens: usize,
    /// Hits dropped because a near-identical earlier hit already won.
    pub dropped_duplicates: usize,
    /// Hits dropped because similarity was below the threshold.
    pub dropped_below_threshold: usize,
    /// Hits dropped because adding them would have busted the budget.
    pub dropped_over_budget: usize,
    /// Individual sentences removed from items because a byte-identical
    /// sentence had already appeared in an earlier kept item. Counted
    /// only when [`ContextBuilder::sentence_dedup`] is on.
    pub dropped_redundant_sentences: usize,
    /// Hits dropped because their embedding was within cosine threshold
    /// of an earlier kept item (paraphrase dedup). Counted only when
    /// [`ContextBuilder::semantic_dedup`] is on.
    pub dropped_semantic_duplicates: usize,
}

/// Token-counting strategy. The builder calls this for every candidate.
///
/// hansa does not pin a specific tokenizer because vocabularies differ
/// across models. Real applications should pass a model-specific impl
/// (e.g. wrapping `tiktoken-rs` for GPT-style models). For tests and
/// demos, [`CharCountTokenizer`] is a coarse but fast fallback.
pub trait Tokenizer: Send + Sync {
    /// Count tokens in `text`.
    fn count(&self, text: &str) -> usize;
}

/// Coarse default: roughly one token per four characters. Within ~30%
/// of real BPE counts for English prose; useful for tests, not for
/// production prompts.
pub struct CharCountTokenizer;

/// Final-ordering strategy applied to bundle items right before the
/// budget cut. Different callers want different trade-offs:
///
/// - [`SimilarityRanker`]: pure ordering by `item.similarity`. Default.
///   Faithful to the membrane's ranking - what arrived first stays first.
/// - [`TokenDensityRanker`]: `similarity / log2(1 + tokens)`. Penalises
///   long items, favouring crisp facts. Strictly better when the budget
///   is tight and items vary in length.
pub trait Ranker: Send + Sync {
    /// Higher score = ranked earlier. Items are sorted descending.
    fn score(&self, item: &ContextItem) -> f32;
}

/// Default ranker: returns the hit's similarity verbatim.
pub struct SimilarityRanker;
impl Ranker for SimilarityRanker {
    fn score(&self, item: &ContextItem) -> f32 {
        item.similarity
    }
}

/// Density-aware ranker: `similarity / log2(1 + tokens)`. A 50-token
/// fact at sim=0.7 beats a 500-token ramble at sim=0.85 - the cost
/// per unit similarity is lower. The log dampens the penalty so short
/// items don't dominate spuriously when they happen to score high.
pub struct TokenDensityRanker;
impl Ranker for TokenDensityRanker {
    fn score(&self, item: &ContextItem) -> f32 {
        let denom = ((1 + item.tokens) as f32).log2().max(1.0);
        item.similarity / denom
    }
}

impl Tokenizer for CharCountTokenizer {
    fn count(&self, text: &str) -> usize {
        // Saturating + ceiling-rounded so even a 1-char content costs 1
        // token. Matches user intuition that "non-empty content has a
        // cost".
        let chars = text.chars().count();
        chars.div_ceil(4).max(if chars == 0 { 0 } else { 1 })
    }
}

/// Builder over [`MembraneHit`]s producing a [`ContextBundle`].
pub struct ContextBuilder {
    hits: Vec<MembraneHit>,
    min_similarity: f32,
    token_budget: Option<usize>,
    record_budget: Option<usize>,
    dedup: bool,
    sentence_dedup: bool,
    /// Cosine threshold for semantic dedup, or `None` when off.
    semantic_dedup: Option<f32>,
    tokenizer: Arc<dyn Tokenizer>,
    ranker: Arc<dyn Ranker>,
}

impl ContextBuilder {
    /// Start a builder from a membrane query result.
    pub fn from_hits(hits: Vec<MembraneHit>) -> Self {
        Self {
            hits,
            min_similarity: f32::NEG_INFINITY,
            token_budget: None,
            record_budget: None,
            dedup: true,
            sentence_dedup: false,
            semantic_dedup: None,
            tokenizer: Arc::new(CharCountTokenizer),
            ranker: Arc::new(SimilarityRanker),
        }
    }

    /// Drop hits whose similarity is below `threshold`.
    pub fn min_similarity(mut self, threshold: f32) -> Self {
        self.min_similarity = threshold;
        self
    }

    /// Truncate the bundle so its `total_tokens` stays at or below
    /// `n`. Items are consumed in similarity order; first one that
    /// overflows is dropped, and the loop stops (no greedy continue,
    /// to preserve order).
    pub fn token_budget(mut self, n: usize) -> Self {
        self.token_budget = Some(n);
        self
    }

    /// Cap the number of kept items.
    pub fn record_budget(mut self, n: usize) -> Self {
        self.record_budget = Some(n);
        self
    }

    /// Toggle BLAKE3-based deduplication on whole payloads. On by
    /// default. Catches "the same record copied across peers".
    pub fn dedup(mut self, on: bool) -> Self {
        self.dedup = on;
        self
    }

    /// Toggle **sentence-level** deduplication: when on, after a record
    /// makes it past the byte-level dedup, its body is split into
    /// sentences and any sentence already seen in an earlier kept item
    /// is dropped from this one. Off by default because it changes the
    /// shape of the rendered output (some items become shorter); turn
    /// it on for RAG-style corpora where chunks of the same document
    /// share boilerplate.
    ///
    /// Lossless on information: every unique sentence still appears
    /// exactly once with its first observed provenance. Counted in
    /// `ContextBundle::dropped_redundant_sentences`.
    pub fn sentence_dedup(mut self, on: bool) -> Self {
        self.sentence_dedup = on;
        self
    }

    /// Enable **semantic** deduplication at cosine `threshold` in
    /// `[0.0, 1.0]`. Off by default. When on, after a hit passes
    /// byte-level and sentence-level dedup, the builder compares its
    /// embedding to every earlier kept hit that also carries an
    /// embedding; if any pairwise cosine is `>= threshold`, the new hit
    /// is dropped (the earlier one wins because it had a higher
    /// upstream similarity).
    ///
    /// Tradeoff: catches paraphrases that byte/sentence dedup misses,
    /// but costs `O(kept²·dim)` per build. The default off lets short
    /// pipelines stay cheap; turn it on (typical threshold 0.95) for
    /// RAG corpora where peers paraphrase the same fact.
    ///
    /// Items without an embedding (RESP3 / HTTP peers) are never
    /// dropped by semantic dedup - they are also never compared.
    pub fn semantic_dedup(mut self, threshold: f32) -> Self {
        self.semantic_dedup = Some(threshold);
        self
    }

    /// Replace the [`Ranker`] used to order items right before the
    /// budget cut. Defaults to [`SimilarityRanker`] (preserves
    /// membrane order); pass [`TokenDensityRanker`] for density-aware
    /// ordering that prefers short, high-similarity items.
    pub fn ranker(mut self, r: Arc<dyn Ranker>) -> Self {
        self.ranker = r;
        self
    }

    /// Replace the tokenizer.
    pub fn tokenizer(mut self, t: Arc<dyn Tokenizer>) -> Self {
        self.tokenizer = t;
        self
    }

    /// Run the pipeline. Idempotent: builders are consumed.
    pub fn build(self) -> ContextBundle {
        let mut bundle = ContextBundle::default();
        // Byte-dedup winners: `(normalised_key, eligible_index)` so a
        // dropped near-identical hit can attribute its origin to the
        // item that won.
        let mut seen_record: Vec<([u8; 32], usize)> = Vec::with_capacity(self.hits.len());
        // For sentence-level dedup: each sentence's normalised BLAKE3.
        // Tens of thousands of these are cheap (32 bytes each).
        let mut seen_sentence: Vec<[u8; 32]> = Vec::new();
        // Semantic-dedup winners: `(unit_vec, eligible_index)` for the
        // same attribution reason as `seen_record`.
        let mut seen_unit_vecs: Vec<(Vec<f32>, usize)> = Vec::new();

        // Phase 1: filter (similarity threshold, record dedup, sentence
        // dedup, semantic dedup). Collect every item that could
        // plausibly go into the final bundle. Budget is applied later,
        // after ranking.
        let mut eligible: Vec<ContextItem> = Vec::with_capacity(self.hits.len());
        for hit in self.hits {
            if hit.similarity < self.min_similarity {
                bundle.dropped_below_threshold += 1;
                continue;
            }
            let raw_content = String::from_utf8_lossy(&hit.payload).into_owned();
            let key = blake3_normalised(&raw_content);
            if self.dedup && let Some(&(_, winner_idx)) =
                seen_record.iter().find(|(k, _)| k == &key)
            {
                bundle.dropped_duplicates += 1;
                attribute_source(&mut eligible[winner_idx].sources, hit.origin);
                continue;
            }
            let (content, dropped_sentences) = if self.sentence_dedup {
                dedup_sentences(&raw_content, &mut seen_sentence)
            } else {
                (raw_content, 0)
            };
            if self.sentence_dedup && content.trim().is_empty() {
                bundle.dropped_redundant_sentences += dropped_sentences;
                continue;
            }
            // Semantic dedup: cosine of unit-normalised embeddings vs
            // every earlier kept item that also had one. Quadratic in
            // kept-count; gated behind opt-in.
            let unit_for_this = if self.semantic_dedup.is_some() {
                hit.embedding.as_deref().and_then(unit_normalise)
            } else {
                None
            };
            if let Some(threshold) = self.semantic_dedup
                && let Some(ref unit) = unit_for_this
            {
                let near_dup = seen_unit_vecs
                    .iter()
                    .find(|(other, _)| dot(unit, other) >= threshold)
                    .map(|(_, idx)| *idx);
                if let Some(winner_idx) = near_dup {
                    bundle.dropped_semantic_duplicates += 1;
                    attribute_source(&mut eligible[winner_idx].sources, hit.origin);
                    continue;
                }
            }
            let tokens = self.tokenizer.count(&content);
            let new_idx = eligible.len();
            seen_record.push((key, new_idx));
            if let Some(unit) = unit_for_this {
                seen_unit_vecs.push((unit, new_idx));
            }
            bundle.dropped_redundant_sentences += dropped_sentences;
            eligible.push(ContextItem {
                origin: hit.origin,
                record_id: hit.record_id,
                similarity: hit.similarity,
                content,
                tokens,
                dedup_key: key,
                embedding: hit.embedding,
                sources: vec![hit.origin],
            });
        }

        // Phase 2: rank. Sort by `Ranker::score` descending. Ties keep
        // their relative order (Rust's sort is stable).
        let ranker = self.ranker.clone();
        eligible.sort_by(|a, b| {
            ranker
                .score(b)
                .partial_cmp(&ranker.score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Phase 3: budget. Walk ranked items, keep until either the
        // token or record budget would overflow.
        for item in eligible {
            if let Some(budget) = self.token_budget
                && bundle.total_tokens + item.tokens > budget
            {
                bundle.dropped_over_budget += 1;
                continue;
            }
            if let Some(cap) = self.record_budget
                && bundle.items.len() >= cap
            {
                bundle.dropped_over_budget += 1;
                continue;
            }
            bundle.total_tokens += item.tokens;
            bundle.items.push(item);
        }
        bundle
    }
}

/// Split `content` into sentences, drop those whose normalised BLAKE3
/// already appears in `seen`, push the surviving keys into `seen`.
/// Returns the rebuilt content (kept sentences joined with `. `) plus
/// the count of dropped sentences.
fn dedup_sentences(content: &str, seen: &mut Vec<[u8; 32]>) -> (String, usize) {
    let sentences = split_sentences(content);
    let mut kept_parts: Vec<&str> = Vec::with_capacity(sentences.len());
    let mut dropped = 0usize;
    for s in sentences {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            continue;
        }
        let key = blake3_normalised(trimmed);
        if seen.contains(&key) {
            dropped += 1;
            continue;
        }
        seen.push(key);
        kept_parts.push(trimmed);
    }
    // Rebuild with `. ` between kept sentences. Cheap and avoids
    // re-emitting punctuation we may have eaten during split.
    let rebuilt = kept_parts.join(". ");
    let rebuilt = if rebuilt.is_empty() {
        String::new()
    } else if rebuilt.ends_with(['.', '!', '?']) {
        rebuilt
    } else {
        format!("{rebuilt}.")
    };
    (rebuilt, dropped)
}

/// Very small sentence splitter: cuts on `. `, `! `, `? `, `.\n`,
/// `!\n`, `?\n`. Misses abbreviations ("Dr. Smith" splits in two);
/// good enough for prose and dramatically less code than a real
/// segmenter. Real callers needing strict segmentation can split
/// upstream and rely on whole-payload dedup instead.
fn split_sentences(content: &str) -> Vec<&str> {
    let bytes = content.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if (b == b'.' || b == b'!' || b == b'?')
            && i + 1 < bytes.len()
            && (bytes[i + 1] == b' ' || bytes[i + 1] == b'\n')
        {
            // Include the punctuation in the current sentence.
            out.push(&content[start..=i]);
            start = i + 1;
            i += 1;
        }
        i += 1;
    }
    if start < bytes.len() {
        out.push(&content[start..]);
    }
    out
}

impl ContextBundle {
    /// Number of items kept.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True when no item was kept.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Items that came from the caller's own tenant.
    pub fn local_items(&self) -> impl Iterator<Item = &ContextItem> {
        self.items
            .iter()
            .filter(|i| matches!(i.origin, HitOrigin::Local))
    }

    /// Items that came from peers.
    pub fn remote_items(&self) -> impl Iterator<Item = &ContextItem> {
        self.items
            .iter()
            .filter(|i| matches!(i.origin, HitOrigin::Remote { .. }))
    }

    /// Render as markdown with H3 headers per provenance group and
    /// quoted content blocks. Suitable for direct injection into an
    /// LLM prompt.
    ///
    /// When an item carries more than one source (other peers shipped
    /// a near-duplicate that the dedup pass collapsed), the header
    /// lists them after a `+` separator and appends an N-sources
    /// counter so the model sees the corroboration signal without
    /// paying for full repeated quotes.
    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        for item in &self.items {
            let sources_part = if item.sources.len() > 1 {
                format!(", {} sources", item.sources.len())
            } else {
                String::new()
            };
            out.push_str(&format!(
                "### {} (sim={:.3}, ~{} tokens{sources_part})\n\n",
                item.sources_label(),
                item.similarity,
                item.tokens
            ));
            for line in item.content.lines() {
                out.push_str("> ");
                out.push_str(line);
                out.push('\n');
            }
            out.push('\n');
        }
        out
    }

    /// Render in the most token-efficient default form: one item per
    /// line, prefixed with a single character marking the origin
    /// (`L` for the caller's own tenant, a hex digit for each distinct
    /// peer). No similarity scores, no token counts, no quote prefixes.
    ///
    /// This is the recommended renderer when the bundle is fed
    /// directly to an LLM as part of a prompt - the LLM does not need
    /// the debug metadata, and skipping it saves the ~10 tokens per
    /// item that markdown costs. Use [`Self::render_markdown`] when
    /// you (a human) want to inspect the bundle, and
    /// [`Self::render_with`] when you need a custom format.
    pub fn render_compact(&self) -> String {
        let mut out = String::new();
        for item in &self.items {
            let mark = match item.origin {
                HitOrigin::Local => 'L',
                HitOrigin::Remote { tenant_id } => {
                    // High nibble of the first id byte - gives 16 stable
                    // labels (`0`..`f`). Fleets with more than 16 peers
                    // sharing the same hansa will alias; for those
                    // cases the caller should use `render_with(...)`
                    // and supply a per-peer index of their own.
                    let b = tenant_id.0[0];
                    std::char::from_digit((b >> 4) as u32, 16).unwrap_or('?')
                }
            };
            out.push('[');
            out.push(mark);
            out.push(']');
            out.push(' ');
            out.push_str(&item.content);
            out.push('\n');
        }
        out
    }

    /// Render in TOON (Token-Oriented Object Notation) form: a
    /// header row plus one CSV-style row per item under an
    /// `items[N]:` group label. Designed for LLMs that want
    /// structured fields (`src`, `sim`, `text`) instead of free
    /// prose. Costs more than [`Self::render_compact`] but ~30-50%
    /// less than JSON for the same structured data.
    ///
    /// Output shape:
    ///
    /// ```text
    /// items[3]:
    ///   src,sim,text
    ///   L,0.95,"local note"
    ///   2,0.80,"peer note one"
    ///   3,0.70,"peer note two"
    /// ```
    ///
    /// CSV quoting rules: any field containing `,`, `"`, or a newline
    /// is wrapped in double-quotes and internal quotes are doubled.
    pub fn render_toon(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("items[{}]:\n", self.items.len()));
        out.push_str("  src,sim,text\n");
        for item in &self.items {
            let src = match item.origin {
                HitOrigin::Local => "L".to_string(),
                HitOrigin::Remote { tenant_id } => {
                    let b = tenant_id.0[0];
                    std::char::from_digit((b >> 4) as u32, 16)
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "?".into())
                }
            };
            out.push_str("  ");
            out.push_str(&src);
            out.push(',');
            // Two decimal places: enough for ranking, less token-noisy
            // than four.
            out.push_str(&format!("{:.2}", item.similarity));
            out.push(',');
            out.push_str(&csv_field(&item.content));
            out.push('\n');
        }
        out
    }

    /// Render as plain text. Items separated by a blank line; each item
    /// prefixed with `[origin]`.
    pub fn render_plain(&self) -> String {
        let mut out = String::new();
        for item in &self.items {
            out.push_str(&format!("[{}] ", item.origin_label()));
            out.push_str(&item.content);
            out.push_str("\n\n");
        }
        out
    }

    /// Custom rendering. The closure is called once per item and its
    /// outputs are joined verbatim.
    pub fn render_with<F: Fn(&ContextItem) -> String>(&self, fmt: F) -> String {
        let mut out = String::new();
        for item in &self.items {
            out.push_str(&fmt(item));
        }
        out
    }
}

/// Append `origin` to `sources` unless an equivalent entry already
/// exists. The same peer can hit the same item twice (byte-dedup and
/// later semantic-dedup of the same paraphrase); we want the rendered
/// list de-duplicated so the model sees each source once.
fn attribute_source(sources: &mut Vec<HitOrigin>, origin: HitOrigin) {
    if !sources.contains(&origin) {
        sources.push(origin);
    }
}

/// Unit-normalise an embedding. Returns `None` for the zero vector
/// (no meaningful direction) so semantic dedup ignores it.
fn unit_normalise(v: &[f32]) -> Option<Vec<f32>> {
    let mut norm = 0.0f32;
    for x in v {
        norm += x * x;
    }
    let norm = norm.sqrt();
    if !norm.is_finite() || norm == 0.0 {
        return None;
    }
    Some(v.iter().map(|x| x / norm).collect())
}

/// Dot product of two equal-length unit vectors == cosine similarity.
/// Mismatched lengths short-circuit to a value that will not trigger
/// dedup (returns `f32::NEG_INFINITY`), so a peer running at a different
/// embedding dim cannot accidentally collapse a local hit.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::NEG_INFINITY;
    }
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn blake3_normalised(content: &str) -> [u8; 32] {
    // Lowercase + collapse whitespace runs to one space + trim. Stable
    // enough to catch "the SAME paragraph copied across peers with
    // slight whitespace differences" without being so aggressive that
    // legitimately distinct quotes collide.
    let mut prev_space = false;
    let mut normalised = String::with_capacity(content.len());
    for c in content.chars() {
        if c.is_whitespace() {
            if !prev_space && !normalised.is_empty() {
                normalised.push(' ');
            }
            prev_space = true;
        } else {
            normalised.extend(c.to_lowercase());
            prev_space = false;
        }
    }
    let trimmed = normalised.trim_end();
    *blake3::hash(trimmed.as_bytes()).as_bytes()
}

/// CSV-style quoting for TOON. Wraps in `"..."` and doubles internal
/// quotes when the field contains a comma, a quote, or a newline.
fn csv_field(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}

fn short_id(id: TenantId) -> String {
    // First 4 hex chars are plenty to disambiguate peers visually.
    let b = id.as_bytes();
    format!("{:02x}{:02x}", b[0], b[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn hit(origin: HitOrigin, id: u64, sim: f32, content: &str) -> MembraneHit {
        MembraneHit {
            record_id: RecordId(id),
            similarity: sim,
            origin,
            payload: Bytes::from(content.as_bytes().to_vec()),
            embedding: None,
        }
    }

    fn hit_with_vec(
        origin: HitOrigin,
        id: u64,
        sim: f32,
        content: &str,
        v: Vec<f32>,
    ) -> MembraneHit {
        MembraneHit {
            record_id: RecordId(id),
            similarity: sim,
            origin,
            payload: Bytes::from(content.as_bytes().to_vec()),
            embedding: Some(v),
        }
    }

    #[test]
    fn char_count_tokenizer_basic() {
        let t = CharCountTokenizer;
        assert_eq!(t.count(""), 0);
        assert_eq!(t.count("a"), 1);
        assert_eq!(t.count("abcd"), 1);
        assert_eq!(t.count("abcde"), 2);
        assert_eq!(t.count("hello world"), 3);
    }

    #[test]
    fn min_similarity_drops_low_hits() {
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.9, "kept high"),
            hit(HitOrigin::Local, 2, 0.4, "kept mid"),
            hit(HitOrigin::Local, 3, 0.1, "dropped low"),
        ];
        let b = ContextBuilder::from_hits(hits)
            .min_similarity(0.3)
            .build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_below_threshold, 1);
    }

    #[test]
    fn dedup_removes_near_identical_content() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([2; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.9, "BLAKE3 derive_key with salt"),
            hit(peer, 2, 0.8, "blake3 derive_key with salt"), // dup after lowercase
            hit(peer, 3, 0.7, "completely different content"),
        ];
        let b = ContextBuilder::from_hits(hits).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_duplicates, 1);
    }

    #[test]
    fn token_budget_caps_total() {
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.9, "short"),    // 2 toks
            hit(HitOrigin::Local, 2, 0.8, "also short"), // 3 toks
            hit(HitOrigin::Local, 3, 0.7, "this one is longer and would overflow"), // ~10
        ];
        let b = ContextBuilder::from_hits(hits).token_budget(6).build();
        assert!(b.total_tokens <= 6, "got {} tokens", b.total_tokens);
        assert_eq!(b.dropped_over_budget, 1);
    }

    #[test]
    fn record_budget_caps_count() {
        let hits = (0..10)
            .map(|i| hit(HitOrigin::Local, i, 0.9, "x"))
            .collect();
        // dedup is on by default - identical "x" content makes all but
        // the first dups. Turn dedup off so record_budget is the
        // effective cap.
        let b = ContextBuilder::from_hits(hits)
            .dedup(false)
            .record_budget(3)
            .build();
        assert_eq!(b.items.len(), 3);
    }

    #[test]
    fn render_markdown_marks_provenance() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x42; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.95, "local note"),
            hit(peer, 2, 0.80, "peer note"),
        ];
        let md = ContextBuilder::from_hits(hits).build().render_markdown();
        assert!(md.contains("### LOCAL"), "missing local marker: {md}");
        assert!(md.contains("### peer-4242"), "missing peer marker: {md}");
        assert!(md.contains("> local note"));
        assert!(md.contains("> peer note"));
    }

    #[test]
    fn sentence_dedup_drops_common_phrases_across_items() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x20; 16]),
        };
        let hits = vec![
            hit(
                HitOrigin::Local,
                1,
                0.9,
                "BLAKE3 is a fast hash. We use it for KDF derivation.",
            ),
            hit(
                peer,
                2,
                0.8,
                "BLAKE3 is a fast hash. It supports tree hashing for parallelism.",
            ),
        ];
        let with = ContextBuilder::from_hits(hits.clone())
            .dedup(true)
            .sentence_dedup(true)
            .build();
        let without = ContextBuilder::from_hits(hits)
            .dedup(true)
            .sentence_dedup(false)
            .build();

        // Item 2 should have lost the shared first sentence.
        assert_eq!(with.dropped_redundant_sentences, 1);
        assert!(with.total_tokens < without.total_tokens);
        // Item 2 must still be present (it had a unique sentence).
        assert_eq!(with.items.len(), 2);
        // The kept second item starts with the unique sentence.
        let second = &with.items[1].content;
        assert!(
            !second.contains("BLAKE3 is a fast hash"),
            "shared sentence leaked: {second}"
        );
        assert!(second.contains("tree hashing"));
    }

    #[test]
    fn sentence_dedup_drops_fully_redundant_items() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x30; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.9, "Alpha. Beta. Gamma."),
            // Same three sentences, different id - exact byte-dedup
            // wouldn't match because we'll permute id, but here it's
            // identical so byte-dedup catches it. Test instead with
            // reordered sentences.
            hit(peer, 2, 0.8, "Gamma. Beta. Alpha."),
        ];
        let with = ContextBuilder::from_hits(hits)
            .dedup(false) // disable byte-dedup so the test focuses on sentence-dedup
            .sentence_dedup(true)
            .build();
        assert_eq!(with.dropped_redundant_sentences, 3);
        assert_eq!(with.items.len(), 1, "second item should have collapsed");
    }

    #[test]
    fn render_toon_emits_csv_with_header_and_quotes_when_needed() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x20; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.95, "simple text"),
            hit(peer, 2, 0.80, "text, with comma"),
            hit(HitOrigin::Local, 3, 0.70, "has \"quotes\" inside"),
        ];
        let toon = ContextBuilder::from_hits(hits).build().render_toon();
        assert!(toon.starts_with("items[3]:\n"));
        assert!(toon.contains("src,sim,text"));
        assert!(toon.contains("L,0.95,simple text"));
        // Comma triggers quoting.
        assert!(toon.contains("2,0.80,\"text, with comma\""));
        // Quotes are doubled inside a quoted field.
        assert!(toon.contains("L,0.70,\"has \"\"quotes\"\" inside\""));
    }

    #[test]
    fn render_compact_is_short_and_marks_origin() {
        let peer1 = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x20; 16]),
        };
        let peer2 = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x30; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.95, "local note"),
            hit(peer1, 2, 0.80, "peer one note"),
            hit(peer2, 3, 0.70, "peer two note"),
        ];
        let bundle = ContextBuilder::from_hits(hits).build();
        let compact = bundle.render_compact();
        // 4-byte per-item overhead: `[X] \n`.
        assert!(compact.contains("[L] local note"));
        // First hex digit of 0x20 = 2, 0x30 = 3.
        assert!(compact.contains("[2] peer one note"));
        assert!(compact.contains("[3] peer two note"));
        // No markdown headers, no debug metadata.
        assert!(!compact.contains("###"));
        assert!(!compact.contains("sim="));
        assert!(!compact.contains("> "));
    }

    #[test]
    fn empty_hits_yields_empty_bundle() {
        let b = ContextBuilder::from_hits(Vec::new()).build();
        assert!(b.is_empty());
        assert_eq!(b.total_tokens, 0);
    }

    #[test]
    fn byte_dedup_collapses_sources_into_winner() {
        let peer_a = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0xa1; 16]),
        };
        let peer_b = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0xb2; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.95, "Shared fact about X."),
            hit(peer_a, 2, 0.85, "shared fact about x."), // byte-dedup match
            hit(peer_b, 3, 0.80, "Shared fact about X."), // byte-dedup match
            hit(HitOrigin::Local, 4, 0.70, "Unique fact."),
        ];
        let b = ContextBuilder::from_hits(hits).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_duplicates, 2);
        // Winner is the first hit (Local); the two collapsed peers
        // appear as additional sources.
        let winner = b
            .items
            .iter()
            .find(|i| matches!(i.origin, HitOrigin::Local) && i.content.starts_with("Shared"))
            .expect("winner should be local");
        assert_eq!(winner.sources.len(), 3);
        assert_eq!(winner.sources[0], HitOrigin::Local);
        assert!(winner.sources.contains(&peer_a));
        assert!(winner.sources.contains(&peer_b));
    }

    #[test]
    fn semantic_dedup_collapses_sources_into_winner() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0xcc; 16]),
        };
        let hits = vec![
            hit_with_vec(
                HitOrigin::Local,
                1,
                0.9,
                "Local prose.",
                vec![1.0, 0.0, 0.0],
            ),
            hit_with_vec(
                peer,
                2,
                0.85,
                "Peer paraphrase.",
                vec![0.999, 0.001, 0.0],
            ),
        ];
        let b = ContextBuilder::from_hits(hits).semantic_dedup(0.95).build();
        assert_eq!(b.items.len(), 1);
        assert_eq!(b.dropped_semantic_duplicates, 1);
        let winner = &b.items[0];
        assert_eq!(winner.sources.len(), 2);
        assert_eq!(winner.sources[0], HitOrigin::Local);
        assert_eq!(winner.sources[1], peer);
    }

    #[test]
    fn render_markdown_lists_collapsed_sources() {
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x42; 16]),
        };
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.95, "Shared fact."),
            hit(peer, 2, 0.85, "Shared fact."),
        ];
        let md = ContextBuilder::from_hits(hits).build().render_markdown();
        assert!(md.contains("LOCAL + peer-4242"), "expected merged header: {md}");
        assert!(md.contains("2 sources"), "expected source count: {md}");
    }

    #[test]
    fn single_source_render_is_unchanged() {
        // Backwards-compat: items with one source must not gain a
        // trailing `, N sources` or a `+` separator.
        let hits = vec![hit(HitOrigin::Local, 1, 0.9, "Only one source here.")];
        let md = ContextBuilder::from_hits(hits).build().render_markdown();
        assert!(md.contains("### LOCAL"));
        assert!(!md.contains(" + "));
        assert!(!md.contains("sources"));
    }

    #[test]
    fn semantic_dedup_drops_near_duplicate_embeddings() {
        // Two hits with byte-different payloads but ~identical
        // direction - exactly the paraphrase case byte/sentence dedup
        // misses.
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x40; 16]),
        };
        let hits = vec![
            hit_with_vec(
                HitOrigin::Local,
                1,
                0.9,
                "Vectors close in space.",
                vec![1.0, 0.0, 0.0],
            ),
            hit_with_vec(
                peer,
                2,
                0.85,
                "Different prose, same direction.",
                vec![0.999, 0.001, 0.001],
            ),
            // Third item is orthogonal - must survive.
            hit_with_vec(
                HitOrigin::Local,
                3,
                0.7,
                "Orthogonal content.",
                vec![0.0, 1.0, 0.0],
            ),
        ];
        let b = ContextBuilder::from_hits(hits).semantic_dedup(0.95).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_semantic_duplicates, 1);
    }

    #[test]
    fn semantic_dedup_off_by_default() {
        // Same paraphrase pair as above but no opt-in: both survive.
        let hits = vec![
            hit_with_vec(
                HitOrigin::Local,
                1,
                0.9,
                "First.",
                vec![1.0, 0.0, 0.0],
            ),
            hit_with_vec(
                HitOrigin::Local,
                2,
                0.85,
                "Second.",
                vec![0.999, 0.001, 0.001],
            ),
        ];
        let b = ContextBuilder::from_hits(hits).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_semantic_duplicates, 0);
    }

    #[test]
    fn semantic_dedup_ignores_hits_without_embedding() {
        // Two paraphrases but the second lacks an embedding (e.g.
        // RESP3 peer). Semantic dedup must not drop it.
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x50; 16]),
        };
        let hits = vec![
            hit_with_vec(
                HitOrigin::Local,
                1,
                0.9,
                "Local with vec.",
                vec![1.0, 0.0, 0.0],
            ),
            hit(peer, 2, 0.85, "Remote without vec."),
        ];
        let b = ContextBuilder::from_hits(hits).semantic_dedup(0.5).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_semantic_duplicates, 0);
    }

    #[test]
    fn semantic_dedup_handles_mismatched_dim() {
        // A peer running at a different dim must not collapse local
        // hits. Pairwise cosine returns -inf, so the threshold check
        // never fires.
        let peer = HitOrigin::Remote {
            tenant_id: TenantId::from_bytes([0x60; 16]),
        };
        let hits = vec![
            hit_with_vec(
                HitOrigin::Local,
                1,
                0.9,
                "Local dim 3.",
                vec![1.0, 0.0, 0.0],
            ),
            hit_with_vec(peer, 2, 0.85, "Peer dim 4.", vec![1.0, 0.0, 0.0, 0.0]),
        ];
        let b = ContextBuilder::from_hits(hits).semantic_dedup(0.9).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_semantic_duplicates, 0);
    }

    #[test]
    fn dedup_off_keeps_duplicates() {
        let hits = vec![
            hit(HitOrigin::Local, 1, 0.9, "same content"),
            hit(HitOrigin::Local, 2, 0.8, "same content"),
        ];
        let b = ContextBuilder::from_hits(hits).dedup(false).build();
        assert_eq!(b.items.len(), 2);
        assert_eq!(b.dropped_duplicates, 0);
    }
}
