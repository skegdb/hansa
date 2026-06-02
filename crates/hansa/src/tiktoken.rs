//! Real BPE tokenizer via `tiktoken-rs`.
//!
//! Behind the `tiktoken` feature flag. Use this when the context
//! bundle budget is a real OpenAI prompt cap and you cannot afford
//! the ±30% drift of [`crate::CharCountTokenizer`].
//!
//! ## Why a separate type
//!
//! `tiktoken-rs` embeds the BPE merge tables for each encoding family
//! (~6 MB total for the common four: `cl100k_base`, `o200k_base`,
//! `p50k_base`, `r50k_base`). Anyone building hansa for a non-OpenAI
//! pipeline shouldn't pay that cost. The base crate stays slim;
//! `tiktoken-rs` only enters the dep graph when this feature is on.
//!
//! ## Picking an encoding
//!
//! | model family                    | encoding      | constructor               |
//! | ------------------------------- | ------------- | ------------------------- |
//! | GPT-4o, GPT-4o-mini             | `o200k_base`  | [`TiktokenTokenizer::gpt4o`] |
//! | GPT-4, GPT-3.5-turbo            | `cl100k_base` | [`TiktokenTokenizer::gpt4`]  |
//! | Codex, davinci-002              | `p50k_base`   | [`TiktokenTokenizer::codex`] |
//! | text-davinci-003                | `p50k_base`   | [`TiktokenTokenizer::codex`] |
//!
//! When in doubt: `gpt4o` for new builds, `gpt4` for legacy GPT-3.5
//! / GPT-4 prompts.

use std::sync::Arc;

use tiktoken_rs::{CoreBPE, cl100k_base, o200k_base, p50k_base};

use crate::context::Tokenizer;

/// Exact-count tokenizer backed by an OpenAI BPE encoding.
///
/// Cheap to clone - the underlying `CoreBPE` is wrapped in `Arc`.
/// Encoding is allocation-free per `count()` call modulo the
/// transient token vector that `tiktoken-rs` returns; for hot paths
/// it's still ~10-50 us per kilobyte of text on M-series.
#[derive(Clone)]
pub struct TiktokenTokenizer {
    bpe: Arc<CoreBPE>,
}

impl TiktokenTokenizer {
    /// Wrap an arbitrary `CoreBPE`. Use the named constructors below
    /// unless you've loaded a custom encoding.
    pub fn from_bpe(bpe: CoreBPE) -> Self {
        Self { bpe: Arc::new(bpe) }
    }

    /// GPT-4o / GPT-4o-mini encoding (`o200k_base`).
    pub fn gpt4o() -> Self {
        Self::from_bpe(o200k_base().expect("o200k_base BPE table"))
    }

    /// GPT-4 / GPT-3.5-turbo encoding (`cl100k_base`).
    pub fn gpt4() -> Self {
        Self::from_bpe(cl100k_base().expect("cl100k_base BPE table"))
    }

    /// Legacy Codex / text-davinci-003 encoding (`p50k_base`).
    pub fn codex() -> Self {
        Self::from_bpe(p50k_base().expect("p50k_base BPE table"))
    }
}

impl Tokenizer for TiktokenTokenizer {
    fn count(&self, text: &str) -> usize {
        if text.is_empty() {
            return 0;
        }
        // `encode_with_special_tokens` is the right call here: the
        // caller is asking "how many tokens does this prompt cost?",
        // and special tokens (when present) cost their own slot.
        self.bpe.encode_with_special_tokens(text).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_is_zero_tokens() {
        let t = TiktokenTokenizer::gpt4o();
        assert_eq!(t.count(""), 0);
    }

    #[test]
    fn english_hello_world_is_predictable() {
        // "Hello, world!" is a canonical example that tokenizes to
        // a stable handful of tokens across encodings. We assert a
        // ballpark (1..6) rather than a magic number so a future
        // upstream BPE table tweak doesn't break the test.
        let t = TiktokenTokenizer::gpt4o();
        let n = t.count("Hello, world!");
        assert!((1..=6).contains(&n), "got {n}");
    }

    #[test]
    fn long_prose_is_close_to_chars_div_four() {
        // For ~English prose around the kilobyte mark the rule of
        // thumb is roughly accurate. We assert tiktoken's count lies
        // within ±50% of chars/4, which is a generous band that
        // catches "tokenizer returned 0" or "tokenizer returned 10x".
        let text =
            "the quick brown fox jumps over the lazy dog. ".repeat(50);
        let t = TiktokenTokenizer::gpt4o();
        let n = t.count(&text);
        let approx = text.chars().count() / 4;
        let lo = approx / 2;
        let hi = approx * 3 / 2;
        assert!(
            n >= lo && n <= hi,
            "tiktoken count {n} outside [{lo}, {hi}] for chars/4={approx}",
        );
    }

    #[test]
    fn cjk_chars_cost_more_than_chars_div_four() {
        // Each CJK character is typically 2-3 tokens under
        // `o200k_base`; chars/4 underestimates badly. This is the
        // headline failure mode that pushed us to ship a real BPE.
        let text = "日本語".repeat(100); // 300 CJK chars
        let t = TiktokenTokenizer::gpt4o();
        let n = t.count(&text);
        let chars_div_4 = text.chars().count() / 4; // = 75
        assert!(
            n > chars_div_4 * 2,
            "CJK text: tiktoken={n} chars/4={chars_div_4}; expected \
             real BPE >> heuristic"
        );
    }

    #[test]
    fn three_encodings_all_produce_counts() {
        // Encodings often agree on common ASCII (BPE is mostly
        // greedy on familiar tokens). Don't insist on disagreement;
        // just confirm each constructor returns something sensible.
        let text = "function compute(x) { return x * x + 1; }";
        let g4o = TiktokenTokenizer::gpt4o().count(text);
        let g4 = TiktokenTokenizer::gpt4().count(text);
        let codex = TiktokenTokenizer::codex().count(text);
        assert!(g4o > 0);
        assert!(g4 > 0);
        assert!(codex > 0);
        // Sanity: should be at most ~text.len() (one token per byte
        // is the worst case for any reasonable BPE).
        assert!(g4o <= text.len());
        assert!(g4 <= text.len());
        assert!(codex <= text.len());
    }

    #[test]
    fn encodings_diverge_on_unicode() {
        // CJK is where o200k_base (newer, bigger vocab) wins vs
        // older encodings. We expect gpt4o to produce strictly
        // fewer tokens than codex (which has no CJK merges).
        let text = "日本語の文書をトークン化する";
        let g4o = TiktokenTokenizer::gpt4o().count(text);
        let codex = TiktokenTokenizer::codex().count(text);
        assert!(
            g4o < codex,
            "gpt4o={g4o} should beat codex={codex} on CJK"
        );
    }

    #[test]
    fn tokenizer_is_clone_and_send() {
        let t = TiktokenTokenizer::gpt4o();
        let t2 = t.clone();
        // `Send` so we can move into a `rayon` task or a `tokio` task.
        std::thread::spawn(move || {
            let _ = t2.count("hello");
        })
        .join()
        .unwrap();
        // Original still works after clone.
        let _ = t.count("hello");
    }
}
