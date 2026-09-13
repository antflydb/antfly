# REGEX

`lib/regex` is Antfly's regex engine. It backs `vellum` FST-based regexp
queries and the AST-based matcher used by JSON Schema and similar validation
paths, with a portable Zig SIMD prefilter for plain (non-FST) haystack
scanning.

## Components

- `src/automaton.zig`: compiles a regex to a Thompson NFA, then lazily
  determinizes it (on-the-fly powerset/subset construction) into a DFA used to
  implement the `vellum.Automaton` interface. `pkg/antfly/src/search/query.zig`
  uses this automaton to prune `vellum` FST traversal for regexp queries.
  - Bytes are grouped into equivalence classes so the DFA transition table is
    indexed by class rather than by raw byte, and per-state transitions are
    cached in a hashed DFA-state cache (keyed by NFA state set) instead of a
    linear scan.
  - Single-state epsilon closures are precomputed once and unioned to build
    subset closures, instead of re-walking epsilon edges on every DFA step.
  - Required-prefix literals are extracted from the compiled pattern (up to 8
    literals, 32 bytes each) for FST-side pruning and for the plain-haystack
    prefilter below.
- `src/mod.zig`: the AST-based matcher (`PreparedPattern`) used by non-FST
  callers such as `pkg/antfly/src/search/pattern_filter.zig`,
  `pkg/antfly/src/storage/db/query/graph_exec.zig`, and
  `pkg/antfly/src/storage/db/aggregations.zig`. It parses the pattern into a
  small AST, then matches by walking that AST against the input (NFA
  simulation, not backtracking).
  - Compiled-regex substring matching is centralized here so every non-FST
    call site goes through one implementation, with explicit handling for `^`
    and `$` anchors (anchors are otherwise implicit for FST matching).
  - A portable Zig `@Vector`-based prefilter scans for required literal
    prefixes (single literal, or a small deduplicated first-byte set for
    simple alternations like `foo|bar`) before falling back to full regex
    verification, with a cheap secondary-byte check to reduce false-candidate
    verification when multiple prefixes share a first byte. Prefilter metadata
    (deduplicated first-byte sets, per-prefix secondary-check offsets) is
    precomputed once at compile time rather than on every scan.

## Supported Syntax

- literals and `.` (any byte)
- concatenation and `|` alternation
- `()` grouping (no captures)
- character classes: `[abc]`, ranges `[a-z]`, negation `[^abc]`
- quantifiers: `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`
- `\` escapes the next character (no `\d`/`\w`/`\s` shorthand classes)
- `^` / `$` anchors

Regexes operate on raw bytes; there is no separate Unicode code-point mode.

## Limits

- `automaton.zig`: at most 256 live NFA states per compiled pattern
  (`max_nfa_states`).
- `mod.zig`: `PreparedPattern` parsing bounds nesting depth at 128 and the
  total AST node count at `4 * max_states` (4096 states), returning
  `error.InvalidRegex` if a pattern would exceed either bound.

## Matching Strategy

- FST traversal (`automaton.zig`) never backtracks: the compiled automaton
  exposes DFA-shaped `step`/`isMatch` behavior to `vellum`, with byte-class
  transitions and a hashed state cache keeping per-step cost low even though
  states are computed lazily.
- Plain haystack scanning (`mod.zig`) is candidate-driven: the SIMD prefilter
  finds candidate offsets for required prefixes/literals, and every candidate
  is verified against the compiled automaton so correctness does not depend on
  the prefilter being exact.

## Benchmarking

`regex-bench` (`bench/regex_bench.zig`) measures haystack candidate filtering
and `vellum` automaton traversal so further optimization work can be judged
from local numbers instead of guesses.

## Open Work

- FST traversal itself (walking the lazily-built DFA over the trie) is still
  branchy pointer-chasing; further gains there are expected to come from
  automaton-side work (already-landed byte classes and closure caching, plus
  any future state-layout changes) rather than from SIMD.
- Small-literal-set prefiltering is capped at 8 literals / 32 bytes per
  literal; patterns whose required-prefix analysis exceeds that bound fall
  back to unfiltered automaton verification.
