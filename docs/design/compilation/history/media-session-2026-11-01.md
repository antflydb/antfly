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
second full-output copy. The existing one-shot operation still returns a
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

Run it from `zig/`, for example:

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
`libantfly-link-map.json`. No new production binaries have been qualified by this
slice. P0 server attribution and controlled production baseline, P1b/P1c, and
P2–P7 retain the plan's original exit criteria.
