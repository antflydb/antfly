# Retained media session and measurement implementation

This is the first implementation slice of
[the production cost plan](../production-cost-plan.md), on the existing
`build/compile-cost-improvements` worktree. It does not complete the production
PDF/image migration or qualify an overall product size/build improvement.

## Changes

The private media ABI is version 3. Its new opaque PDF session keeps the Reader,
parse caches, allocator adapter and deadline at stable provider-owned addresses.
Input PDF bytes and the allocator callback context must outlive session close;
the API does not copy the entire source. Cancellation contexts are borrowed for
one synchronous operation and cleared before it returns. The client retains a
stable allocator callback context until close.

The render request accepts a synchronous output allocator. The new client
charges the retained PNG directly to that allocator and returns it without a
second full-output copy. The current allocation bridge exposes allocate/free,
with resize/remap fallback; runtime peak-allocation behavior still needs
qualification before migrating callers that can resize buffers in place. The
existing one-shot operation still returns a
provider-owned buffer, preserves its operation identity and delegates to the
same session implementation. No external/public CAPI exports were added.

Malformed-input diagnostics retain the existing shared failure-envelope
semantics. In particular, `InvalidPdfHeader` remains an undeclared diagnostic
with `internal`/`StorageKernelFailure`, retaining its exact name and provenance.
The new client's `initWithFailure` exposes that envelope. A complete migration
must qualify additional domain error mappings before replacing direct callers;
this prototype does not silently change existing error classification.

The media constructor now explicitly declares its root storage-independent.
Previously, final-link discovery inferred storage consumption from its embedded
source path and attached the whole storage engine and inference dependencies to
media-only executables. The declaration follows cloned module graphs. An
accidental unresolved storage call now fails to link. No scheduler reservations
were reduced.

## Verification

Native ARM64 macOS 27.0.1, Zig 0.17.0, baseline CPU, Metal/CUDA/system BLAS off:

- Four source-free compiled PDF session tests passed in Fast and Debug, with no
  skips or leaks: repeated output after the open frame returns, malformed/partial
  initialization, allocation failure, ABI rejection, idempotent null close,
  cancellation and successful reuse afterward, and output allocation failure.
- Nine existing compiled enrichment boundary tests passed in Fast and Debug,
  including extraction regions, existing single-page PNG output, callback errors,
  allocator admission, decode limits and failure identities.
- The production storage compilation checker and its 32 tests passed.
- The measurement map and compiler report parsers passed 18 tests; the existing
  memory profiler/summarizer passed 10 with process-inspection permission.
- Zig formatting, Python lint, workflow validation and license checks passed.

A focused instrumented Fast run completed 18 build steps. Media owner emission
was approximately 23 seconds in the initial screening; a subsequent sampled run
recorded a largest compiler RSS of 914 MiB. These are local screening observations,
not paired performance measurements or justification for lower memory claims.
The original mistaken test graph was stopped after it had unnecessarily started
the storage compiler; its incomplete run is not a timing baseline.

The cache experiment copies mutable source files into a private overlay inside
the worktree cache and removes it afterward. It exercises the actual media
archive and opaque caller, and rejects storage/inference owners in the graph:

| Change | Media owner | PDF caller object | End-to-end seconds |
| --- | --- | --- | ---: |
| Initial | Built | Built | 37.1 |
| None | Cached | Cached | 8.5 |
| Provider implementation | Built | Cached | 35.5 |
| Client implementation | Cached | Built | 12.1 |
| Contract | Built | Built | 32.9 |

Times include configuration, final linking and tests; they are not codegen-only
measurements or an order-of-magnitude product result. Run
`python3 zig/tools/check_media_owner_cache.py --report <report.json>` to repeat.

## Measurement tooling

`report_zig_link_map.py` reads exact live/dead linker atoms, object provenance,
section coverage, aliases/overlaps and repeated-name candidates. It checks the
installed binary SHA256 against the output named in the map and verifies Mach-O
section addresses/sizes. Section flags identify zero-fill runtime reservations;
these are not counted as file-size savings. Invalid UTF-8 and literal control
characters in symbol names are accepted without splitting map records. Keep the
raw map alongside its JSON/hash; decoded names alone are not an audit record.

Example from the worktree root:

```sh
python3 scripts/report_zig_link_map.py path/to/libantfly.map \
  --binary zig/zig-out/lib/libantfly.dylib --map-root zig --output link-map.json
```

`collect_zig_time_report.py` captures and parses the installed compiler's binary
WebSocket messages, with hashes pinned for both `std/Build/abi.zig` and the
compiler's payload reader. The latter is necessary: the ABI comment omits the
actual per-declaration `sema_count` field. The collector connects after the build
summary because this compiler announces its socket before configuring its graph;
an earlier connection can panic in the compiler. Raw frames and logs are kept.

These measurement commands require Python 3.11 or newer (native screening used
Python 3.13). Run from `zig/`, for example:

```sh
python3 ../scripts/collect_zig_time_report.py --output ../media-timings.json -- \
  python3 tools/run_bounded_zig_build.py -- build runtime-unit-enrichment_compute \
  -Doptimize=fast -Dstrip=true -Dcpu=baseline -Dmetal=false -Dcuda=false \
  -Dsystem-blas=false
```

It forces rebuilding and belongs in a separate diagnostic pass. The optional CI
pass runs after primary product hashes/attribution have been recorded. The CI
`products` selection builds `antfly capi runtime-archives` together, with selectable
x86 GNU/ARM64 musl CPU profiles and actual compile-time stripping.

One real media report retained 5,756 timed declarations, 1,593 generic instances,
1,392 inline calls, 167 imported files and 1,686 reachable files. Its reported
LLVM emission interval was 27.18 seconds; declaration processing was 0.576 seconds.
This owner is dominated by the final backend stage in this diagnostic sample.
Per-declaration `codegen_ns` describes Zig's declaration code generation, not
per-function LLVM optimization cost. Do not generalize this result to the much
larger storage/API/inference owners before collecting their reports.

Linux memory snapshots retain cgroup limits/current/peak, swap and memory events
before and after profiling. They identify their observed scope and explicitly
warn that cumulative/shared cgroup peaks are not isolated compiler peaks. Raw
per-process and same-poll aggregate measurements remain separate.

Receipts are ignored under `.benchmark-results/compile-costs/`: `pdf-session-v3.*`,
`media-owner-regressions.log`, `media-owner-debug.log`, `media-owner-cache*.json/log`,
`pdf-consumer-time-report.*`, `media-owner-time-report.*`, and
`libantfly-link-map.json`. The native product regression below does not qualify a performance gain.
P0 server attribution and controlled production baseline, P1b/P1c, and
P2–P7 retain the plan's original exit criteria.

## Combined native product regression

The frozen production source at `f5c0c79fe8` completed the native CPU Fast/stripped
combined build and selected integration checks: **133/133 build steps succeeded**.
This included 57 storage-owner tests and three opaque-query-owner tests without
skips/leaks, CAPI smoke checks, linked-inference ABI integration, CLI checks and
verification of exactly 134 public CAPI exports with no internal exports.
The HTTP query/deadline regression selection also passed all 10 tests.

| Installed artifact | Bytes | Change from `9b3fda9f22` |
| --- | ---: | ---: |
| `antfly` | 229,621,752 | 0 |
| `libantfly.dylib` | 80,026,400 | 0 |
| Storage archive | 64,326,696 | +24 |
| Inference archive | 47,953,232 | 0; identical SHA256 |

Server SHA256:
`5625169022d2962c5acf8c30fcead5af7d5b539c7476d21d32f08270f83e22c2`.
C-library SHA256:
`71c40b8e86ba8c7c9ee6eb3c2ae4af2536ca29d30d31ae40612fa7e6dfaaaa60`.
The earlier pre-query-owner server cost remains +3,981,856 bytes.

The new verified linker map reports 66,600,616 text bytes, four bytes above the
baseline despite unchanged page-aligned file size. The new session exports and
the existing one-shot render export are dead-stripped from this library. The
three retained PDF image decoders and the nominal 3,848,196-byte repeated-name
candidate total remain: this prototype has **not** removed production PDF/image
duplication. Full caller migration remains necessary.

This regression run took 2,856 seconds, with sampled individual and same-poll
compiler RSS peaks both about 8.89 GiB. It was not a controlled paired timing or
memory qualification: other work ran on the host, and earlier cached outputs
were absent. Rebuilding the inference archive produced identical bytes; this
observation alone does not diagnose cache-key invalidation. Memory reservations
remain unchanged.

A subsequent correctly rooted warm check passed 110/110 steps, with the physical
storage, inference, API and media owners cached and both media suites passing.
An initial warm invocation from the repository root used incorrect fixture paths
and different cache arguments; only that invocation was stopped. Its failed log
is retained separately and is not counted as a regression or timing measurement.
The new PDF suite is now included in the existing storage/integration aggregates,
as well as its explicit `opaque-pdf-session-test` entry point.

Receipts: `media-session-products.*`, `media-session-products-artifacts.json`,
`media-session-libantfly.map`, `media-session-libantfly-link-map.json`,
`media-session-final-warm-corrected.log`, and `media-session-http.log`.
The raw map and its cached dylib were SHA256 matched before cache cleanup.
