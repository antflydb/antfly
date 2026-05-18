# JSON Plan

This file is the execution plan for `lib/json`.

Use it to answer:

- what “transparent” JSON compatibility means in this repo
- which `std.json` entry points can realistically get a SIMD backend first
- how we stage the work without breaking existing callers

## Goal

Build a local `lib/json` package that is source-compatible with the common
`std.json` API shape while leaving room for a future SIMD-backed parser for
full-buffer parsing.

The intended caller experience is:

- switch imports from `std.json` to `@import("antfly-json")`
- keep `parseFromSlice`, `parseFromSliceLeaky`, `Value`, `Stringify`, and the
  common parse/stringify helpers unchanged
- get automatic backend selection later without changing call sites

## Non-Goals

The first tranche should not pretend to solve the whole JSON surface:

- do not replace `std.json.Scanner` or `std.json.Reader` with a fake SIMD story
- do not rewrite stringification; keep `std.json.Stringify`
- do not change parsing semantics for existing callers in phase 0
- do not require C++ or an external `simdjson` checkout just to build the repo

## Compatibility Boundary

The compatibility target is:

- high-level full-buffer parsing:
  - `parseFromSlice`
  - `parseFromSliceLeaky`
- common exported types and helpers:
  - `Value`
  - `ObjectMap`
  - `Array`
  - `Parsed`
  - `ParseOptions`
  - `Stringify`
  - `fmt`

The intentionally non-transparent area is:

- low-level token streaming through `Scanner` and `Reader`
- exact internal diagnostics and token lifetime behavior for a future SIMD path

Those low-level APIs stay re-exported for compatibility, but phase 0 and phase 1
will continue routing them through `std.json`.

## Current Phase

Phase 0 is what should land now:

1. Add `lib/json/src/mod.zig`.
2. Re-export the `std.json` surface needed by existing callers.
3. Wrap `parseFromSlice` and `parseFromSliceLeaky` behind backend selection.
4. Keep the selected backend as `stdlib` for now.
5. Expose backend-selection metadata so future tests can assert dispatch logic.
6. Add standalone `lib-json-test` coverage in `build.zig`.

That gives us:

- a real package boundary
- a transparent import target for new code
- a dispatch seam for future SIMD work
- zero behavior drift today

Phase 0.5 is the current backend checkpoint after that:

- a real SIMD stage-1 structural scanner exists under `lib/json`
- stage 1 now masks punctuation inside strings and records quote delimiters separately
- explicit `.simd` requests use a partial SIMD backend
- that backend now includes a real `std.json.Value` parser with escape and unicode decoding
- both custom parsers now use the stage-1 quote index for direct string extraction when
  the current string segment has no escapes
- typed struct parsing now matches plain field names directly from the input slice without
  allocating temporary key strings
- typed unknown-field skipping now validates and skips nested strings without allocating
  discarded string values
- native typed parsing now honors `allocate = .alloc_if_needed` for plain `[]const u8`
  string slices and still allocates when escapes force decoding
- custom `jsonParse` types can now stay inside the typed `.simd` path via subtree fallback
  instead of forcing the whole containing parse back through `Value`
- typed `.simd` parsing now has a native parser for a bounded but useful subset:
  scalars, enums, tagged unions, optionals, arrays, vectors, slices, pointers, tuple
  structs, and non-tuple structs, with custom `jsonParse` handled via subtree fallback
- everything else still falls back to `std.json` parsing
- a dedicated `json-bench` executable now exists under `lib/json/bench/json_bench.zig`
  so we can compare `std.json`, `antfly-json` auto-selection, and explicit `.simd`
  on representative typed and `Value` payloads
- escaped-string decoding now reserves bounded output capacity up front and
  avoids extra direct-path scans, which improves the escape-heavy typed benchmark
- subtree-local stdlib fallback for custom `jsonParse` types now captures the
  current value slice from the structural index instead of walking it recursively
- auto typed selection now stays on `stdlib` for types that contain custom
  `jsonParse` subtrees, because the benchmarked partial SIMD path is slower there
- stage 1 now also records whether the input contains any raw control bytes
  inside strings, which lets plain-string fast paths skip repeated per-string
  control-byte rescans when the whole input is already known clean

That means we now have an actual parser component to build on, but not a
standalone SIMD DOM/on-demand parser yet.

## Benchmark Snapshot

The first `zig build json-bench` run on local `aarch64-macos` shows:

- tiny typed payloads should stay on `stdlib`; explicit `.simd` is slower there
- medium typed payloads already benefit from the custom backend, though the
  win is still modest and workload-sensitive
- escaped-string-heavy payloads have improved from near parity to a modest win
  on the auto backend, but string decoding is still an obvious remaining bottleneck
- typed payloads that contain custom `jsonParse` subtrees should stay on stdlib
  in auto mode; explicit `.simd` remains available but is slower there
- ignored-unknown typed payloads regress on explicit `.simd` for both plain and
  escape-heavy inputs, which points more at composite traversal/skip overhead
  than at escape decoding alone
- a first live `skip_tape` integration for ignored unknown composite subtrees
  was benchmarked and rejected; building whole-input pairing metadata on demand
  cost more than the current recursive skip on the tested payloads, so the tape
  remains isolated as groundwork rather than an active parser path
- a second subtree-local validator path that reused the current structural index
  was also benchmarked and rejected; validating the current unknown composite
  subtree before skipping it still duplicated enough work to regress the same
  skip-heavy benchmarks
- large `Value` payloads show the clearest upside so far at roughly `1.3x`
- the stage-1 string-control flag improves plain-string workloads without
  changing behavior, but it does not materially fix the ignored-unknown
  regression, so skip/traversal is still the next real target

That is enough signal to justify the current architecture. The next parser work
should be driven by benchmark deltas rather than only by API coverage.

## Backend Contract

Backend selection should stay explicit and testable.

The public contract should include:

- `PreferredBackend = .auto | .stdlib | .simd`
- `Backend = .stdlib | .simd`
- `BackendConfig`
- `BackendSelection`

Rules:

- `parseFromSlice*` uses backend selection
- `parseFromTokenSource*` stays a straight pass-through to `std.json`
- small inputs should continue using the scalar path even after SIMD exists
- unsupported targets always fall back to `stdlib`
- explicit `.simd` and large-slice `.auto` now use the partial SIMD backend
- when the custom backend cannot yet cover a case, it falls back internally to
  `std.json` rather than changing the external API

## Phase 1

Add an internal SIMD parser implementation for complete input slices only.

Requirements:

- no source-level change for high-level callers
- preserve `Parsed(T)` ownership semantics
- preserve `ParseOptions.allocate` and `max_value_len` behavior where possible
- fall back to `stdlib` when the SIMD path cannot preserve semantics

The first realistic target should be:

- `parseFromSlice(Value, ...)`
- `parseFromSlice` for plain structs, arrays, strings, booleans, integers, and
  floats

The first version does not need to handle:

- streaming token APIs
- every corner of number formatting diagnostics
- every exact error path parity case before the fast path is stable

## Validation

Each phase should keep these checks in place:

- standalone unit tests for backend selection
- parse compatibility tests against representative typed structs
- `Value` parse compatibility tests
- stringify compatibility tests for callers importing `antfly-json`
- benchmark coverage later for:
  - tiny payloads
  - medium API payloads
  - large document arrays

## Immediate Execution Order

1. Land the compatibility package and tests.
2. Migrate opt-in call sites that want a local JSON facade.
3. Add microbenchmarks and payload fixtures that represent real request bodies.
4. Implement a slice-only SIMD backend behind the existing wrapper.
5. Expand compatibility coverage before switching any hot-path call sites.

## Risks

- “transparent” is only honest for high-level full-buffer parsing
- future SIMD parsing may not match every `std.json` diagnostic exactly
- tiny payloads may regress if backend selection is too eager
- cross-target support will need explicit fallback behavior
