# JSON Library

`lib/json` is a local package that is source-compatible with the common
`std.json` API shape, with a dispatch seam for an in-progress SIMD-backed
parser for full-buffer parsing.

Use this file to answer:

- what "transparent" JSON compatibility means in this repo
- which `std.json` entry points have a SIMD backend today
- how the backend is staged so existing callers do not see behavior drift

## Goal

Callers switch imports from `std.json` to `@import("antfly-json")` and keep
`parseFromSlice`, `parseFromSliceLeaky`, `Value`, `Stringify`, and the common
parse/stringify helpers unchanged. Backend selection (`stdlib` vs `simd`)
happens without changing call sites.

## Non-Goals

- `lib/json` does not replace `std.json.Scanner` or `std.json.Reader` with a
  SIMD implementation.
- Stringification stays on `std.json.Stringify`.
- The custom backend does not change parsing semantics for existing callers;
  where it cannot yet cover a case, it falls back to `std.json` internally
  rather than changing the external API.
- It does not require C++ or an external `simdjson` checkout to build the
  repo.

## Compatibility Boundary

The compatibility target is:

- high-level full-buffer parsing: `parseFromSlice`, `parseFromSliceLeaky`
- common exported types and helpers: `Value`, `ObjectMap`, `Array`, `Parsed`,
  `ParseOptions`, `Stringify`, `fmt`

The intentionally non-transparent area is low-level token streaming through
`Scanner` and `Reader`, and exact internal diagnostics/token-lifetime behavior
for the SIMD path. Those low-level APIs stay re-exported for compatibility but
continue to route through `std.json`.

## Current Backend

`lib/json/src/mod.zig` re-exports the `std.json` surface needed by existing
callers and wraps `parseFromSlice`/`parseFromSliceLeaky` behind backend
selection, with backend-selection metadata exposed so tests can assert
dispatch logic. A standalone `lib-json-test` step in `build.zig` covers this.

A real SIMD stage-1 structural scanner exists under `lib/json`:

- stage 1 masks punctuation inside strings, records quote delimiters
  separately, and records whether the input contains any raw control bytes
  inside strings (so plain-string fast paths can skip repeated per-string
  control-byte rescans when the whole input is already known clean)
- explicit `.simd` requests use a partial SIMD backend built on top of that
  scanner
- that backend includes a `std.json.Value` parser with escape and Unicode
  decoding; both custom parsers use the stage-1 quote index for direct string
  extraction when the current string segment has no escapes
- typed struct parsing matches plain field names directly from the input
  slice without allocating temporary key strings, and typed unknown-field
  skipping validates and skips nested strings without allocating discarded
  string values
- native typed parsing honors `allocate = .alloc_if_needed` for plain
  `[]const u8` string slices and still allocates when escapes force decoding
- typed `.simd` parsing has a native parser for a bounded but useful subset:
  scalars, enums, tagged unions, optionals, arrays, vectors, slices, pointers,
  tuple structs, and non-tuple structs; custom `jsonParse` types stay inside
  the typed `.simd` path via subtree fallback instead of forcing the whole
  containing parse back through `Value`
- escaped-string decoding reserves bounded output capacity up front and
  avoids extra direct-path scans
- everything the native parser does not cover falls back to `std.json`
  parsing

That gives `lib/json` an actual parser component to build on, but not yet a
standalone SIMD DOM/on-demand parser (that is [Open work](#open-work)).

A dedicated `json-bench` executable (`lib/json/bench/json_bench.zig`) compares
`std.json`, `antfly-json` auto-selection, and explicit `.simd` on
representative typed and `Value` payloads; see
[Benchmark Findings](#benchmark-findings) below.

## Backend Contract

Backend selection is explicit and testable. The public contract includes:

- `PreferredBackend = .auto | .stdlib | .simd`
- `Backend = .stdlib | .simd`
- `BackendConfig`
- `BackendSelection`

Rules:

- `parseFromSlice*` uses backend selection
- `parseFromTokenSource*` stays a straight pass-through to `std.json`
- small inputs continue using the scalar path even where SIMD exists
- unsupported targets always fall back to `stdlib`
- explicit `.simd` and large-slice `.auto` use the partial SIMD backend
- auto typed selection stays on `stdlib` for types that contain custom
  `jsonParse` subtrees, because the benchmarked partial SIMD path is slower
  there
- when the custom backend cannot yet cover a case, it falls back internally to
  `std.json` rather than changing the external API

## Validation

Each backend change keeps these checks in place:

- standalone unit tests for backend selection
- parse compatibility tests against representative typed structs
- `Value` parse compatibility tests
- stringify compatibility tests for callers importing `antfly-json`
- benchmark coverage for tiny payloads, medium API payloads, and large
  document arrays

## Open work

- Add an internal SIMD parser implementation for complete input slices only
  (the current native parser does not yet stand fully alone as a SIMD
  DOM/on-demand parser). Requirements once this lands: no source-level change
  for high-level callers; preserve `Parsed(T)` ownership semantics; preserve
  `ParseOptions.allocate` and `max_value_len` behavior where possible; fall
  back to `stdlib` when the SIMD path cannot preserve semantics. First
  realistic target: `parseFromSlice(Value, ...)` and `parseFromSlice` for
  plain structs, arrays, strings, booleans, integers, and floats. Streaming
  token APIs, every corner of number-formatting diagnostics, and full error-path
  parity are explicitly not required for the first version.
- Migrate opt-in call sites that want a local JSON facade onto
  `antfly-json`.
- Add microbenchmarks and payload fixtures that represent real request
  bodies, beyond the current representative `json-bench` payloads.
- Expand compatibility coverage before switching any hot-path call sites to
  the SIMD backend by default.
- String decoding is still the most obvious remaining bottleneck for
  escaped-string-heavy payloads (see Benchmark Findings).
- A first `skip_tape` integration for ignored unknown composite subtrees was
  benchmarked and rejected: building whole-input pairing metadata on demand
  cost more than the current recursive skip on the tested payloads. The tape
  remains isolated as groundwork, not an active parser path. A second
  subtree-local validator approach (reusing the current structural index) was
  also benchmarked and rejected for the same reason. Skip/traversal overhead
  for ignored-unknown typed payloads is still the next real target;
  regressions on explicit `.simd` for both plain and escape-heavy
  ignored-unknown inputs point at composite traversal/skip cost rather than
  escape decoding alone.

## Risks

- "Transparent" is only honest for high-level full-buffer parsing.
- Future SIMD parsing may not match every `std.json` diagnostic exactly.
- Tiny payloads may regress if backend selection becomes too eager.
- Cross-target support needs explicit fallback behavior (already handled by
  falling back to `stdlib` on unsupported targets).

## Benchmark Findings

The first `zig build json-bench && ./zig-out/bin/json_bench` run on local
`aarch64-macos` showed:

- tiny typed payloads should stay on `stdlib`; explicit `.simd` is slower
  there
- medium typed payloads already benefit from the custom backend, though the
  win is still modest and workload-sensitive
- escaped-string-heavy payloads improved from near parity to a modest win on
  the auto backend, but string decoding is still an obvious remaining
  bottleneck
- typed payloads that contain custom `jsonParse` subtrees should stay on
  stdlib in auto mode; explicit `.simd` remains available but is slower there
- ignored-unknown typed payloads regress on explicit `.simd` for both plain
  and escape-heavy inputs, pointing more at composite traversal/skip overhead
  than at escape decoding alone
- large `Value` payloads show the clearest upside so far, at roughly `1.3x`
- the stage-1 string-control flag improves plain-string workloads without
  changing behavior, but does not materially fix the ignored-unknown
  regression

That is enough signal to justify the current architecture; further parser work
should be driven by benchmark deltas rather than only by API coverage.
