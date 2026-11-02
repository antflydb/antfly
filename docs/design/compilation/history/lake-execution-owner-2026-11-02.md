# Lake execution ownership prototype

The opt-in `-Dshared-lake-execution=true` graph moves complete native lake
archive build/rebuild attempts into the existing serverless runtime archive.
The API retains authorization, attempt admission, lease renewal, checkpoint
metadata CAS, retry policy and the final publication CAS. Physical uploads never
publish a candidate by themselves. This is a server build change; libantfly does
not acquire a serverless runtime dependency.

## Operation boundary

`api/lake_execution_build_impl.zig` contains the physical implementation extracted
from `lake_index_publication.zig`: source coverage verification, row identity,
navigation, row scans, parallel text/ordered/dense/sparse/algebraic builders,
private artifacts, reuse/recovery, and cleanup. The original source-fixture path
continues to execute that same implementation.

The native client sends one synchronous operation with table/catalog values,
source-opening options, artifact store identity/location, embedding policy,
work limits, and coarse control callbacks. The serverless provider opens its own
`Store` and `ServingSource`, validates their admitted identities, constructs its
own embedding runtime, runs the attempt, and destroys those resources before
returning owned candidate catalog bytes. No scanner, artifact-store vtable,
row-source provider or per-row callback crosses compilation owners.

Only catalog control crosses back to the API: liveness checks, clock reads,
lease snapshots, checkpoint saves and checkpoint discards. Callback failures use
names and origin-scoped opaque tokens, preserving caller-only errors without
interpreting foreign Zig error integers. A separate bounded lake error vocabulary
covers operation-owned errors absent from the inference owner's error table.
Artifact-read failure provenance remains distinct from source failures. Ambiguous
checkpoint discards are returned once; they are never replayed by the provider.

The executor crosses through the existing checked `IoBorrow` adapter. Secret
store and Antfly provider capabilities retain their existing checked callback
boundaries. Foreign HTTP/credential/cache objects are not dereferenced: the
provider constructs local pools, and borrows the process quota registry so a
build cannot multiply outbound provider capacity. Sources remain read-only;
artifact write authority still comes from independent artifact configuration.

Working-set admission is owned by the provider. Callback allocations from that
quota preserve the stable `LakeSidecarBuildLimitExceeded` outcome, including
when an allocation failure first occurs in an API-owned lease callback. Output
catalog bytes are copied into the caller's allocator before the local quota is
released.

## Build graph and ABI findings

The publication facade and coordinator use the named `lake_execution_backend`
shim. It resolves a namespace issued by the actual root Module, preserving the
single-file ownership required by Zig. Source/provider roots expose the raw
implementation. Graph-selected native API/distributed roots expose the native
client and contain no literal physical implementation import. The serverless
root exports the operation only when the opt-in graph requests it.

A computed `@import` path is unsupported by the pinned compiler. An inactive
literal implementation import would also remain a cache input; merely moving a
function behind a compile-time branch is insufficient.

The independent-archive fixture found another ABI issue: the anonymous enum in
`StoreLocator.protocol` had analysis-order-dependent type names. Naming the
unchanged representation `StoreLocator.Protocol`, and guarding its tags, makes
its native contract stable across compilation owners. The endpoint/getter is v2
and checks its explicit layout count before dereferencing the layout array.
Supplemental checks cover cancellation and progress callbacks, their by-value
payloads, and callback outcome wrappers. Stale-count tests use an invalid layout
pointer to verify this ordering. No guard was weakened.

Source fixtures also need one complete source media graph. With shared image/PDF
product flags enabled, their inference/readers dependencies can otherwise carry
a PDF facade while the fixture imports raw PDF, giving `pdf_syntax` two owners.
The test builder restores those transitive substitutions with one graph
rewriter; it does not alter production media selection.

## Validation and limits

Routine work uses Debug builds. Focused source publication and source
coordinator regressions passed, including artifact recovery, ambiguous metadata
outcomes, publication reuse and quota behavior. The dedicated operation fixture
covers all five writer families, checkpoint reopen, derived-directory loss,
ambiguous discard, cancellation, lease expiry, rejected checkpoints, store/source
identity changes, resource limits, contract rejection and error-name transport.
It checks generation/token/signature and logical declarations across retries,
then reads candidate artifacts with checksum verification. Dense checkpoints may
be repacked, so physical directory identity is not the idempotency key.

Final focused Debug checks passed with both product flags enabled: **6 source
operation tests and 6 independently linked operation tests**, zero failures or
leaks (80/80 build steps). This includes a regression that product flags cannot
select native execution inside a source fixture. The source coordinator's
existing recovery/fencing regression also passed against the final source
backend with both product flags (57/57 build steps, zero failures or leaks).

The independently compiled fixture is the important boundary check:

```sh
zig build lake-execution-abi-integration-test lake-integration-test \
  -Doptimize=debug -Dtest-filter="external lake execution" \
  -Dshared-image-operations=true -Dshared-lake-execution=true \
  -Dmetal=false -Dcuda=false -Dsystem-blas=false
```

The cache/symbol receipt tool freezes authored Zig sources and build construction
inside a temporary overlay under the worktree's `.zig-cache`, then checks actual
production API/serverless archives across warm, physical-code, client-code and
contract edits:

```sh
python3 tools/check_lake_execution_cache.py --report <report.json>
```

The final production matrix passed **5/5 phases**. It copied 4,601 authored
Zig/construction files into the private overlay and retained logs and source
hashes in `.benchmark-results/compile-costs/lake-execution-owner-cache.json`.
The final provider, client, contract, physical implementation, coordinator and
publication facade hashes match the working-tree sources. Temporary source and
build-cache overlays were removed after completion.

| Phase | API archive | Serverless archive | Elapsed |
| --- | --- | --- | ---: |
| Initial | Built | Built | 328.47 s |
| Warm | Cached | Cached | 14.10 s |
| Reachable physical-code edit | Cached, byte-identical | Rebuilt | 154.15 s |
| Reachable client-code edit | Rebuilt | Cached, byte-identical | 235.01 s |
| Contract edit | Rebuilt | Rebuilt | 403.67 s |

The Debug API archive contains **zero migrated builder symbols**; serverless
contains **55** (56 after the reachable helper probe). The selector covers the
complete extracted implementation/publication-builder namespaces and native
text/sparse/dense/ordered/aggregate build entry points, rather than treating all
reader/helper symbols as writers. Initial Debug archive sizes were 300,727,288
bytes for API and 156,575,360 for serverless; these include debug information and
are not final executable text sizes.

The matrix used `-j1`, baseline CPU and both product flags. Other work ran on the
same machine. The elapsed times are reproducible ownership receipts, not a
paired cold-build benchmark, normal-runner critical path or release speedup.

These checks establish ownership and compiler-cache behavior, not a release
binary saving or cold-build speedup. Before enabling the option by default,
measure both owners and aggregate build work on the normal runner, and exercise
cloud/catalog pinned-snapshot paths with production providers. The prototype deliberately
reopens physical clients instead of borrowing the API read cache; cross-build
HTTP/credential cache reuse remains an explicit performance tradeoff.

Pinned lake query execution still belongs to the API. Recent-vector publication
also calls some shared dense writer helpers, so those helpers may remain there
even after archive build/rebuild entry points migrate. Moving complete pinned
queries and complete recent-vector operations are separate next boundaries;
this work does not claim that every lake-related symbol has left the API.
