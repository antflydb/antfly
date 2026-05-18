# REGEX

Last updated: 2026-04-11

## Goal

Speed up the local `vellum`/`regex` stack with portable Zig SIMD where it materially helps, while keeping the current FST integration and full cross-platform support.

## Current State

- `lib/regex/src/automaton.zig` compiles regexes to a Thompson NFA and lazily determinizes them while servicing `vellum.Automaton`.
- `pkg/antfly/src/search/query.zig` already uses that automaton to prune `vellum` FST traversal for regexp queries.
- Several plain haystack paths still do substring matching by restarting the automaton at every byte offset:
  - `pkg/antfly/src/search/pattern_filter.zig`
  - `pkg/antfly/src/storage/db/query/graph_exec.zig`
  - `pkg/antfly/src/storage/db/aggregations.zig`
- `lib/regex/src/mod.zig` also has a separate AST-based matcher used by JSON Schema and similar validation paths.

## What SIMD Should Target First

The first high-return target is the plain haystack scan path, not `vellum` trie traversal.

- FST traversal is branchy pointer-chasing and tends to benefit more from better automaton state caching and byte-class reduction than from lane-wise SIMD.
- Plain haystack scanning is contiguous byte processing and is a natural fit for SIMD prefilters.

## Plan

1. Centralize the compiled-regex substring matcher so every non-FST call site goes through one implementation.
2. Add literal extraction for required prefixes/literals from compiled regexes.
3. Add a portable SIMD candidate finder in Zig:
   - single literal / substring path first
   - small literal set path next, Teddy-style if it still looks worthwhile after measurement
4. Verify candidates with the existing automaton so correctness stays simple.
5. Improve automaton internals for the `vellum` path:
   - replace linear DFA state lookup
   - add byte-equivalence classes
   - precompute epsilon closures where profitable

## Notes

- Zig gives us a viable cross-platform SIMD route via vectors and target-feature-aware implementations; we do not need to pull in an x86-only dependency just to get the first gains.
- Hyperscan-style ideas are still useful as design input, especially literal prefilters and Teddy-like multi-literal screening, but we can reimplement the parts that fit this codebase.
- The compiled automaton path currently treats `^` and `$` as implicit for FST matching. For plain substring matching, that needs explicit wrapper logic so anchored patterns remain correct.

## Completed

- 2026-04-11: centralized compiled-regex substring matching in `lib/regex` with explicit anchor handling for `^` and `$`, and switched duplicated helper call sites to use it.
- 2026-04-11: added conservative required-prefix extraction to compiled regexes and routed substring matching through candidate-based verification, creating the prefilter hook where SIMD search can land next.
- 2026-04-11: replaced the scalar first-byte prefix scan with a portable Zig vector prefilter for candidate discovery before full regex verification.
- 2026-04-11: widened prefix extraction from a single literal to small literal sets for simple alternations, so patterns like `foo|bar` and `(foo|bar)baz` can prefilter on multiple required candidates.
- 2026-04-11: generalized the small-set first-byte prefilter so extracted multi-prefix scans search once for the whole deduplicated starting-byte set before exact prefix verification.
- 2026-04-11: strengthened candidate screening with a cheap secondary-byte check before full prefix equality, reducing false positives when many prefixes share the same first byte.
- 2026-04-11: replaced the lazy DFA cache's linear state-set lookup with a hashed index, which is the first direct `vellum`-side automaton optimization.
- 2026-04-11: added byte-equivalence classes and per-DFA-state transition caching on those classes, so `vellum` traversal now reuses transitions across bytes with identical NFA behavior.
- 2026-04-11: precomputed single-state epsilon closures and changed subset-closure building to union cached closures instead of re-walking epsilon edges on every DFA step.
- 2026-04-11: added a dedicated `regex-bench` target to measure haystack candidate filtering and `vellum` automaton traversal, so the next optimization step can be chosen from local numbers instead of guesses.
- 2026-04-11: moved prefix scan metadata construction out of the hot path by precomputing deduplicated first-byte sets and per-prefix secondary-check offsets at compile time.
