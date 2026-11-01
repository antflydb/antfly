# Operation ownership and borrowed batch experiments, 2026-11-01

## Provenance and scope

Implementation: `3051dca22ce45970965cefe04d1e1e76c72e6359`, on top of
`ba9b06caf4`. Builds ran from the corresponding source before committing, so
embedded build metadata names the parent revision. This is local experimental
evidence, not a new production release baseline.

| Field | Profile |
|---|---|
| Host | macOS 27.0.1, arm64; other builds were active; CPU model and available RAM were not recorded |
| Compiler | Zig 0.17.0, stock native build runner through `run_bounded_zig_build.py` |
| Target | Native ARM64/macOS, native CPU; Metal/CUDA disabled |
| Optimization | Debug for integration, cache checks and transport timings; `fast`, stripped, for optimized owner probes |
| Cache | Worktree `zig/.zig-cache`; cache-isolation checks used disposable source overlays and local caches under `zig/.zig-cache/architecture-probe-overlays`, removed afterward; their shared global cache stayed under `zig/.zig-cache/global` |
| Product layout | Existing seven runtime units and shared storage/inference owners remain; two opt-in probe archives are separate from product composition |
| Measurements | Zig step time/rounded MaxRSS, artifact cache states, and paired operation timings; LLVM phase/declaration counts and emitted overlap were not collected |
| Artifact qualification | No final executable/CAPI size comparison, cold release comparison, or Linux normal-runner qualification |

## Experiments and decisions

| Hypothesis | Implementation | Decision |
|---|---|---|
| Test consumers should reuse compiled implementations | Probe tests compile to independent test objects, then link provider archives through the existing `linked_tests` construction | Keep the opt-in fixtures and cache contract checks; do not consolidate large physical suites based on this result |
| Mutation preparation should expose request-only plans | `transform_read_plan.Plan` selects durable bases without DB/schema/executor dependencies; the existing concrete snapshot reader executes the plan | Keep the bounded extraction; it is not a complete mutation effect IR |
| Physical index operations can have independent ownership | An archive roots the exact production `Index.create`/`destroy` lifecycle, with opaque handles and provider-owned allocation | Keep as a lifecycle probe; queries, mutations, backend binding and transaction ownership have not moved |
| Internal batch transport can borrow document buffers | Private ABI v85 adds ordinary document writes/deletes with borrowed bytes, exact failure envelopes/stages, existing DB commit semantics and provider-owned responses | Keep additive and experimental; existing call sites continue using JSON |
| Tensor operations can be compiled independently | An archive roots existing RoPE/QKV-RoPE and GEGLU forward/backward implementations; clients pass complete tensors through C-layout descriptors | Keep as a boundary/correctness probe; this small kernel family does not establish a material inference critical-path improvement |

The physical algebraic index now delegates configuration validation to the
existing pure `index_config.validateConfig`, removing a second 153-line
implementation. Public validation behavior is covered by the existing config
regressions. The pure transform planner owns its containers and borrows request
keys; write/delete bases and first-transform order are preserved, including
allocation-failure cleanup.

### A composition problem exposed by the index probe

The first index probe silently linked the complete storage engine, inference
core and enrichment dependency chain. The final-link heuristic treated any
embedded module declaring `storage_engine_options` as a shared-engine client.

`source_owner` now respects the same explicit implementation-owner selection as
`engine_selection.zig`. The index probe declares implementation ownership and
links successfully without those product archives. The cache checker rejects
any probe graph that includes them. Normal shared-engine clients still link
their providers, as exercised by the 57 compiled storage-owner regressions.

This proves independent lifecycle compilation, not a safe split of physical
query execution. No backend or per-posting callback boundary was introduced.

## Cache isolation

Both real operation families passed the following sequence. Mutations occur
only in isolated source overlays, never in the checkout or dependency sources.

| Change | Provider archive | Consumer test object |
|---|---|---|
| Initial build | Built | Built |
| No change | Cached | Cached |
| Consumer test edit | Cached | Rebuilt |
| Production implementation source edit | Rebuilt | Cached |
| ABI contract edit | Rebuilt | Rebuilt |

The optimized native probes reported:

| Compilation | Zig step time | Reported MaxRSS |
|---|---:|---:|
| Index lifecycle provider | 8 s | 572M |
| Index client object | 2 s | 186M |
| Fused CPU provider | 266 ms | 115M |
| Fused CPU client object | 2 s | 193M |

These are isolated unit observations with warm dependencies. They do not
measure time saved by moving an implementation out of an existing product
owner. Additional archives can duplicate support code; emitted overlap and
final-image size remain acceptance gates before enabling a production split.

## Borrowed document transport

The operation validates all descriptor shapes and JSON documents before
mutation. Document number spellings are retained. It does not borrow buffers
beyond the synchronous call, bypass durable commit, or accept advanced private
commands through an ordinary-document request. Failure metadata distinguishes
validation, commit and response stages; a commit-stage failure alone does not
establish whether the mutation committed. The exact semantic error still
controls that decision.

Three Debug trials each ran ten paired rounds at each batch size, alternating
transport order. Every call updates existing rows, starting from an equivalent
state. JSON owner time excludes consumer encoding; borrowed owner time includes
descriptor construction. Responses remain small provider-owned JSON buffers.

| Documents | JSON median ms/call | Borrowed median ms/call |
|---|---:|---:|
| 1 | 1.3222 | 1.0028 |
| 100 | 12.6057 | 12.6061 |
| 1,000 | 122.2709 | 116.3664 |

The 1,000-document median is about 4.8% lower, but JSON trials ranged from
110.654 to 152.9622 ms and borrowed trials from 113.0295 to 134.6779 ms.
The difference is smaller than observed run-to-run variation. The first
single-document borrowed trial was slower than JSON. There is no established
bulk throughput win or basis for changing default transports from these runs.
Borrowed descriptors total 32,000 bytes at 1,000 documents, but the roughly
500 KB of document payload still exists in caller-owned memory.

## Validation and reproduction

- Debug and stripped ReleaseFast operation probes: two planner, two index and
  three tensor client tests passed in each profile.
- All 57 compiled storage-owner regressions passed, including the new borrowed
  path's precision, buffer lifetime, atomic rejection, ABI and failure checks.
- Three transform preparation/budget/transaction integration regressions passed.
- Pure index-config root: 23 tests; physical config selection: three tests.
- Failure registry: ten tests; released status identifiers: one test.
- Runtime/codegen/API-kernel import boundaries, control catalog, Zig formatting,
  workflow lint and source license checks passed.

From `zig/`:

```sh
zig build architecture-probe-test -Doptimize=debug -Dmetal=false -Dcuda=false
zig build architecture-probe-test -Doptimize=fast -Dstrip=true -Dmetal=false -Dcuda=false
python3 tools/check_architecture_probe_cache.py --family index --report ../.benchmark-results/compile-costs/index-cache.json
python3 tools/check_architecture_probe_cache.py --family inference --report ../.benchmark-results/compile-costs/inference-cache.json
zig build storage-boundary-bench -Doptimize=debug -Dmetal=false -Dcuda=false
./zig-out/bin/storage_boundary_bench zig-out/new-empty-benchmark-directory
```

The opt-in profiling workflow accepts `architecture-probe-test` and retains
both cache-check reports. It was prepared and linted, not dispatched. Local raw
logs/reports are under `.benchmark-results/compile-costs` in the worktree.

## Next cuts

Before enabling an index archive, root one complete physical query or mutation
family, including snapshot/transaction ownership and result release. Keep its
inner scans local and compare aggregate emission and final product size.
The current lifecycle probe is a prerequisite, not that implementation.

For inference, select a measured expensive family such as quantized matrix or
expert execution, then preserve admission, cancellation and backend semantics.
The small fused-operation probe is insufficient justification by itself.

For mutation planning, extract another complete effect family behind concrete
inputs and explicit prerequisites, retaining retries and committed outcomes.
For transport, profile ReleaseFast with allocation and CPU attribution before
considering typed query results or migrating callback-bearing call sites.
