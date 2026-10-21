# Antfly Zig compilation experiment ledger

> Summary of a verbatim log. Full text: [docs/design/compilation/history/experiment-ledger-2026-08-to-10.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/compilation/history/experiment-ledger-2026-08-to-10.md). Originally relocated verbatim from `zig/COMPILATION_EXPERIMENTS.md` (lines 1–5187 at commit 3ce5aff4ea) on 2026-10-12; durable decisions were folded into [COMPILATION.md](../../../../zig/COMPILATION.md), which is the authoritative description of the current build.

The investigation (August–October 2026) started from an ARM64 Linux musl
`ReleaseFast` release build ending in `std::bad_alloc`. The goal was a reliable,
fast cold build of the one static `antfly` executable on the normal
cost-efficient CI runner, without serializing the build, adding memory, or
weakening behavior. It produced a source-selected, multi-unit topology with one
compiled physical-storage owner, a lossless cross-unit failure-identity ABI, and
graph gates. Reliability was achieved on the normal runner; the storage and
inference units still exceeded the 380-second per-unit gate at the end of the log.

| Era | Phases | Durable conclusion |
|---|---|---|
| Failure diagnosis and graph mapping | Original diagnostics through first clean-cache profile | Not demonstrated to be ordinary cgroup OOM; LLVM emission and repeated implementation codegen dominate |
| Kernel and owner-ABI scaffolding | 1–3b | Source facades and narrow call wrappers are insufficient without physical ownership |
| Complete storage ownership migration | 4a–4o | Coarse opaque ownership can remove physical storage from control units while preserving behavior and exact failures |
| Query and compute isolation | 4p–4u | Local query belongs with storage; enrichment remains a separate compute island |
| Failure identity and inference boundary | 4v–4x | Status, operation state, callback failure, and exact semantic identity stay separate across every compiled boundary |
| Post-main source selection and product composition | 4y–4ac | Accepted candidate has six compiled units; top-level inference command splits are counterproductive |

## Goal and constraints (as recorded)
- One statically linked `antfly` executable; standalone always embeds inference; the C API reuses the same compiled storage and carries no server/runtime code; `ReleaseFast` (not `ReleaseSmall`); normal build concurrency (no `-j1`); LSM is production, LMDB only for tests/fixtures/conversion/legacy.
- Runtime boundaries must be coarse — no call per record, posting, LMDB operation, or vector candidate. Allocation, cancellation, deadlines, and error translation are explicit at every compiled ABI.
- Exit criteria: repeated reliable cold builds on the normal runner; storage/local query compiled once and reused by executable and C API; distributed critical path at most 380 s (preferably 350 s); no LMDB in production artifacts; no runner-cost increase. Enabling by default, merging, or raising CI cost needs explicit approval.
- Larger runner, swap, and `-j1` are diagnostics only, never the architecture.
- Goal loop decision vocabulary: **keep** (sound and removes compiler work now, or a bounded prerequisite to a named next cut), **revise** (right direction but legacy roots remain or the change is host variance), **revert** (duplicates work, grows the wrong artifact, weakens behavior).

## Original failure diagnosis
- cgroup OOM counters stayed zero; observed cgroup peak was about 18–20 GB; the extracted direct `zig build-exe` succeeded at about 15.2 GB RSS, as did an `strace` replay. The failing path used the build-runner/compiler protocol (`--listen=-`), the successful replay did not. Conclusion: likely a Zig build-runner/compiler-server or LLVM pressure issue; the architecture work does not depend on proving it.
- 64 GB runner and swap were rejected as the primary fix for that reason.
- Profiles consistently attributed about 97–98% of compiler wall time to LLVM emission, so reuse requires a *compiled artifact boundary*; Zig's lazy source imports mean a module or facade is not a separately compiled library.
- Early five-report graph: 2,147 repository-file instances, 1,204 unique, 943 duplicates; 163 duplicated storage files (~382,000 duplicated lines). Storage dominated; HTTPX was too small for its own ABI; LMDB should drop out of production rather than get a library.

## Experiment log

Rows are not comparable unless host, target, cache state, and report set match.

| Experiment | Result | Decision |
|---|---:|---|
| Direct replay without build-runner protocol | Succeeded at about 15.2 GB RSS | Not demonstrated to be ordinary OOM |
| Serial compilation | Avoids pressure but lengthens build | Rejected |
| CLI-only source facade | Implementation leaked back via internal imports | Rejected |
| Data alone / data + metadata / data + serverless | 371.6 s / 376.9 s / 387.1 s | Strongly favors co-generation |
| Restore-staging boundary prototype | Client 144.3 s → 38.8 s | Coarse opaque bridges can pay off |
| API + distributed co-generation | About 420 s | Rejected |
| Function/data section GC | `libantfly.so` 28.3 MB back to 16.55 MB | Kept |
| Production LSM-only feature set | No `mdb_*`/LMDB symbols in storage archive | Kept |
| Opt-in build-only storage kernel (Phase 1) | Distributed 425.652 s → 415.352 s; duplicates 393 → 756 | Scaffolding only |
| One-call `DB.search` probe (2a) | 401.086 s, declarations 41,984 → 41,981 | Rejected |
| Combined provisioned + hosted read vtable (2b/2c) | 367.185 s, 40,585 declarations | Go for local-query island; revert raw prototype |
| Post-serverless compile-once control | 358.589 s / 352.900 s; duplicates 873 → 482; executable 72.121 → 60.804 MB | Kept, later superseded by 9–11 m runner results |
| Post-main normal-runner controls | App/storage 9–11 m at 10 GB MaxRSS, zero swap | Reliable; misses 380 s |
| CLI + metadata control-only probe | 295.647 s | Duplicates too much storage alone |
| Remote-only metadata control split | Duplicates 438 → 965; executable 59.271 → 76.892 MB | Rejected |
| API/serverless + CLI/metadata coalescing | 409.246 s; executable 61.224 → 72.464 MB | Rejected |
| Application/storage without remote CLI | 340.130 s | Kept |
| Serverless local runtime in app/storage | App 361.270 s; API 125.604 s; executable → 59.271 MB | Kept |
| Canonical storage-contract/import cut | Declarations 17,332 → 17,334 | Rejected: lazy file removal ≠ emitted-code removal |
| Isolated local HA runtime | Saved 5.344 s, new 30.525 s unit | Rejected |
| Control-only provisioned write vtable | 366.523 s; only 703 KB less object data | Rejected |
| Atomic control-only physical-source cut (4l) | Full cold archive 509 s → 458 s; executable 71.79 → 65.27 MB | Kept behind experiment |
| First normal-runner physical-source cut | Storage 11 m, distributed 8 m; archive 16:25.14 | Reliable, both miss 380 s |
| Atomic local-query cut, normal runner (4t) | Storage 590.707 s, distributed 482.636 s; 19:23.59 | Boundary kept opt-in; schedule rejected |
| Co-generated storage/local query (4u) | Local 313.334 s; runner chain 664.411 → 599.665 s | Kept opt-in |
| HA seed source-selection repair (4y) | Distributed 348.370 → 267.599 s | Kept |
| Serverless product unit (4aa) | Local storage 265.177 s; runner storage 556.232 s | Kept |
| Independent API unit (4ab) | Runner archive 16:14.57; API 244.981 s, distributed 297.394 s | Kept |
| Top-level inference splits (4ac) | +151.284 s / +127.444 s compiler work | Rejected |

## Decisions (accepted increments)
- **Runtime-specific roots and section GC** — production runtimes no longer import the public/test root; function/data sections keep the C API small when it links a shared PIC archive. Linked production defaults to `-Dproduction-lsm-only=true`.
- **Serverless API-protocol placement** (pre-owner era) — serverless moved into the API island: distributed 397.836 s → 361.564 s with the executable shrinking (72.290 → 72.121 MB). Kept.
- **Compile-once control, then reopened** — removing the separate storage unit gave 352.900–358.589 s locally and cut duplicates 44.8%, so the separate-kernel migration was paused; two normal-runner runs (`31281549097`, `31283050205`) then took 9–11 m for the combined unit, reopening the architecture. The API scheduling claim was corrected from 5 GiB to 7 GiB after a 6.32 GB peak caused Zig to discard a completed step.
- **Remote-CLI cut** — local HA moved into application/storage so the CLI unit (38.029 s, 53 files) contains no storage files. Deterministic scheduling: explicit compile dependencies are required because Zig 0.16 randomizes dependency traversal; claims API 7 GiB, application/storage 11 GiB, inference 8 GiB, CLI 3 GiB within a 20 GB budget plus 4 GB runner reserve. Normal-runner run `31287672283` passed (15:17.77).
- **Serverless local runtime into application/storage** — API 206.191 s → 125.604 s for +21.140 s on the application unit; normal-runner run `31288827248` cut the archive to 13:40.16. Kept.
- **Process-scoped opaque owner context** (ABI 10) — one context owns the resource manager and decoded-index caches; destruction returns `busy` until owners drain. `DataServer` owns one heap-stable `ProvisionedKernelOwnerSource`; owner creation is also attachment, which fixed a warmup race that produced `GenerationTransitionActive`.
- **Ownership families moved behind the owner (Phases 4a–4k)**, each as tested ABI preparation with no compiler claim until the atomic switch: catalog reconfiguration (ABI 11), bounded `reconcile` (12), owner-fenced table retirement, bulk-ingest lifecycle (13, retry-safe table ledger), transactions and recovery (14), artifact operations (15), local backup (16), restore prepare/promote/reconcile/repair (17), maintenance quanta (18), opaque data-Raft apply store (20) and projection/split/merge (21–22).
- **Restore/snapshot ordering** — `prepare -> promote -> publish -> commit`; control publishes the authoritative table definition between promote and physical publish, and failures before physical publication leave the live generation unchanged.
- **Atomic control-only source selection (4l)** — `storage_source_options.control_only` is a per-compiler-unit property; the distributed unit selects contract-only roots and cannot fall back to physical DB caches. Focused control probe: 341.1 s → 236 s, emitted text 21,747,232 → 16,534,856 B. Storage scheduling claim corrected to 10.25 GiB after a 10,047,467,520-byte peak; distributed reduced from 11 to 9 GiB.
- **Per-source sections stay enabled** so CI rejects source-only relocations using emitted overlap rather than lexical reachability.
- **Aggregation folding in the kernel (4m)** — one call with borrowed hit descriptors; storage/distributed duplicate text 3,237,340 → 2,281,040 B (-29.5%).
- **Lossless failure registry (ABI 28 onward)** — `runtime_failure_identity.zig` is the single bidirectional status registry with uniqueness, round-trip, and exhaustiveness tests; only unregistered provider errors become `internal` / `StorageKernelFailure`. New operations must register their error vocabulary and test a real provider/client failure round trip first.
- **Metadata apply ownership (4n)** and **opaque physical WAL (4o, ABI 29)** — WAL iteration pages bounded at 512 entries and 4 MiB; append returns the assigned operation's `lsn + 1`; duplicate text 2,309,244 → 1,854,020 B; native vs opaque WAL benchmark +1.2% / +1.0% (within variance).
- **`FailureIdentity` envelope (4r, ABI 30–32)** — stable status, originating boundary, version, operation stage, bounded error name, truncation bit, full-name hash. Valid inner envelopes are forwarded unchanged; malformed ones become `invalid_boundary_failure_identity`. `dense_repair_backpressure` got its own status instead of the client guessing from generic `busy`. Callback errors go through a consumer-owned `CallbackErrorRelay` and are rethrown exactly.
- **Document/media compute island (4s)** — 18.428 s enrichment unit; results capped at 64 units or 4 MiB per batch. Kept, though the normal-runner schedule (run `31530408426`, 17:52.22, cgroup peak 15.51 GiB) was rejected as a performance solution.
- **Atomic local-query/preflight migration (4t, ABI 33)** — moving graph, text statistics, algebraic partials, and preflight together (each a real root of `DB.search`/`searchComposed`) made storage emit zero bytes for `storage.db.query.search_exec`; storage 307.033 s → 278.304 s.
- **Co-generated storage + local query (4u)** — same hidden `antfly_local_query_*` ABI emitted in the storage archive; runner run `31550210130` reduced the matched chain by 64.746 s and `libantfly.so` returned to 18,460,688 B.
- **Shared failure ABI and inference lifecycle (4v, ABI 34)** — `runtime_failure_abi.zig` is dependency-neutral; a boundary returning only a boolean, exit code, broad class, or provenance-less status is incomplete.
- **Unstable sort in aggregations (4w)** — 27 of 28 stable sorts had unique keys or deterministic tie-breakers and became `std.mem.sortUnstable`; the percentile sort stayed stable because NaN values compare equivalent. Storage/query 327.342 s → 302.038 s; PDQ ran at 0.767× stable-sort duration on 100,000-bucket rounds.
- **`AntflyProvider` replaced by a C-compatible inference ABI (4x, ABI 35–37)** — one call per complete embed/sparse/rerank/generation/media/model-list operation; provider-owned results with explicit destroy. Kept for correctness; as a compile experiment it missed the 30–45 s materiality threshold.
- **Darwin public-library budget** raised from 60 MiB to 80 MiB (83,886,080 bytes) with explicit user approval on 2026-10-11.

## Rejected approaches
- **Narrow call boundaries without ownership** — one-call `DB.search` (only 3 declarations moved; executable +5.3%); raw read-vtable prototype (arbitrary Zig error-set numbers differed across units — `HAReadRequiresPrimary` arrived as an unrelated error — and the executable grew 24.0%).
- **Stateless stable-query ABI (2d)** — failed with `GenerationTransitionActive` because the storage archive cannot open a second DB for a live generation; do not work around it by closing the writer, weakening generation locking, or mapping to retry.
- **Adding owner operation families one at a time (Phase 3 candidate)** — behaviorally sound, but the distributed graph stayed at 724 files; reverted including Phase 2p attachment. Wall-time changes of 3–7% across every unit were host variance.
- **Lite command dispatch into storage** — distributed -5.170 s, storage +7.968 s; reverted.
- **Standalone + storage codegen island** — distributed 382.842 s but duplicates 780 → 985 and executable 72.290 → 79.008 MB.
- **Separate storage kernel alongside legacy sources** — duplicates nearly the entire storage graph and grows the executable (e.g. +18.8% in the write-vtable probe).
- **Exact-sort owner callback** — three API captures +2.565 to +3.261 s; physical files stayed in the graph.
- **API wire-root / durable-persistence boundary and canonical contract extraction** — removed 42k–62k lexical lines but declarations, generic counts, and object bytes were unchanged. Lexical line or `all_files` counts are not codegen evidence without an object delta.
- **API contract-import cleanup** — API objects byte-for-byte identical; reverted with the temporary profiling target.
- **Isolated local HA** — 5.344 s saved for a 30.525 s unit duplicating 75 files.
- **Remote-only metadata split** — saved 35.029 s in isolation but executable +29.7% and storage duplicate instances 52 → 189; do not revive until metadata consumes storage through the opaque kernel.
- **Runtime-status/startup-catch-up slice and local-read fallback pruning** — distributed graph identical (720 files); storage +13 s.
- **Role placement after the physical-cache cut** — moving standalone, serverless, and metadata into storage just moved the critical unit (376.890 s) and grew the executable 6.8%.
- **Standalone/serverless root relocation (Debug probe)** — distributed -124 files, storage +273 files.
- **Composition relocation after 4l** — standalone + Lite alone emitted 8,191,824 B duplicate text; serverless is itself local-query code and belongs with the kernel.
- **Control-only native-storage-pool selector** — duplicate text changed only 2,281,040 → 2,280,284 B.
- **Three separate physical product libraries (4p)** — 167 repeated modules with 8,949,496 B duplicate text.
- **Enrichment runtime wrapped one-for-one** — 105 direct store/scan/catalog interactions would make a chatty backend-shaped ABI; the decoder-only 24-second adapter is also not a candidate.
- **Broadened local-query provider (4t)** — moved exports that were not the retaining roots; local query +25.386 s and C API grew. A separate typed query vtable and removing `Engine.search` both proved the vtable was already lazy.
- **Synthetic hand-written providers (4z)** — failed to compile; the exact production `storageOwnerBatchJson` root compiled in 172.640 s. Future cuts must root the exact production operation first.
- **Serverless co-generated with distributed (4aa)** — improved the critical path only 16.280 s.
- **API/standalone/runtime three-way split (4ab)** — repeated 4.29 MB of named text; standalone owns `DataServer`, local metadata, and startup, so it is not thin composition.
- **Top-level inference splits (4ac)** — command + host would need 401.560 s of compiler work vs 250.276 s co-generated.

## Phase 4y: post-main HA seed source-selection repair
Merging main pulled HA seed activation and startup validation into distributed
control: distributed grew to 348.370 s, 860 imported files, and 39,973
declarations, co-emitting 6.47 MB with storage including physical
`storage.db.db`, `index_manager`, and enrichment runtime. The repair keeps
portable seed topology (`seed_topology.zig`, `seed_activation_contract.zig`) in
control and moves activation, materialization, live-DB validation, and pruning
behind the storage owner via a versioned JSON request. ABI 39 registers every
expected HA activation/materialization failure; a compiler-graph gate rejects
distributed analysis of the physical seed modules.

| Unit | Post-main control | HA owner repair |
|---|---:|---:|
| Storage + local query | 310.367 s | 321.719 s |
| Distributed/API control | 348.370 s | 267.599 s |
| Inference | 236.780 s | 251.509 s |

Distributed fell to 756 files and 31,814 declarations; co-emission fell to a
2.57 MB lower bound; the executable shrank 71,860,000 → 65,522,864 bytes.
Normal-runner run `31623309087` (commit `ab2b0e6b7`): storage 637.544 s,
distributed 523.228 s, inference 524.793 s, archive 19:22.35, zero swap — kept
as the ownership baseline, but all three exceeded 380 s. The same work fixed
four HTTP tests still using `db_mod.testing` and nine storage files missing
from shard discovery.

## 2026-09-12: Cold x86_64 Linux release scheduling and boundary runtime cost
Appended from `COMPILATION.md` on 2026-09-16 per the
[experiment-record policy](../../../../zig/COMPILATION.md#experiment-record-policy).
Two fresh-cache builds of source `83cd076bf` on one isolated pod (AMD EPYC
7B13, 7.5 CPU request, 24 GiB limit, 22 GiB Zig budget, Zig 0.16.0, `-j8`)
changed only runtime compilation reservations:

| Measurement | Conservative reservations | Measured reservations |
| --- | ---: | ---: |
| Elapsed build time | 72m44s | 27m01s |
| Process-tree CPU time | 87m52s | 78m15s |
| Sampled peak process-tree RSS | 5.93 GiB | 13.08 GiB |
| Storage / inference compilation begins | 29m26s / 56m28s | 45s / 45s |

Both produced the same executable SHA-256; the 62.9% elapsed reduction is one
paired measurement on a shared node, explained by storage and inference
overlapping instead of being serialized by a combined 36 GiB claim. New claims
(native x86_64 Linux, baseline GNU, stripped ReleaseFast, CPU inference only):
storage 8 GiB, inference 8 GiB, distributed 5 GiB, API 5 GiB, serverless 4 GiB,
CLI 2 GiB, enrichment 2 GiB.

Boundary runtime cost (`storage_boundary_bench`, local Debug, ~500-byte
documents): a 1,000-document batch spent 8.9 ms encoding/parsing vs 46.7 ms in
the owner batch; a 1,000-hit query took 20.9 ms in the owner plus 11.2 ms
consumer decoding. JSON transport is a material remaining cost; any future
compact transport must keep complete-operation calls, provider-owned results,
validation, and exact error/cancellation semantics.

## Key measurements

Historical pre-serverless baseline (2026-08-07, local cross-compile, fresh cache):
API kernel 142.809 s, distributed/storage 425.652 s (97.9% LLVM), inference
242.082 s; executable 57,640,592 bytes; `libantfly.so` 16,554,944 bytes.

Normal-runner (`arc-antfly-publish`, ARM64 musl `ReleaseFast`, cold) milestones:

| Checkpoint | Run | Critical units | Archive wall time |
|---|---|---|---:|
| Compile-once control | `31281549097` / `31283050205` | app/storage 9 m / 11 m | 12:28.74 / 14:45.44 |
| Remote-CLI cut | `31287672283` | app/storage 11 m | 15:17.77 |
| Serverless in app/storage | `31288827248` | app/storage 12 m | 13:40.16 |
| Physical-source cut (4l) | `31437306380` | storage 11 m, distributed 8 m | 16:25.14 |
| WAL ownership (4o) | `31518438580` | storage 530.153 s, distributed 394.388 s | 16 m 34 s |
| Compute island (4s) | `31530408426` | storage 615.200 s | 17:52.22 |
| Local-query island (4t) | `31547528877` | storage + query chain 717.965 s | 19:23.59 |
| Co-generated query (4u) | `31550210130` | storage/query 599.665 s | 17:21.73 |
| HA seed repair (4y) | `31623309087` | storage 637.544 s | 19:22.35 |
| Serverless product unit (4aa) | `31636098096` | storage 556.232 s, distributed 536.737 s | 18:44.76 |
| Independent API unit (4ab) | `31643584514` | storage 482.880 s, inference 444.092 s | 16:14.57 |

Emitted-module attribution before the atomic cut found 172 modules emitted twice
(8,650,546 duplicate allocatable bytes), led by `storage.db.db` (1,880,488 B),
`storage.db.algebraic.index` (977,408 B), and `storage.db.catalog.index_manager`
(785,596 B). Refined section-name analysis in 4x showed the module lower bound
overstates duplication (1,947,510 B lower bound vs 1,529,268 B of truly repeated
sections).

2026-10-11 native Darwin cold release archive (commit `773daba39a`, ARM64 macOS,
14 CPUs, Zig 0.17.0, shared host): 73/73 steps in 3,136.06 s (about 1.03 cores
average), largest descendant RSS 9,448,030,208 bytes; executable 174,115,224 bytes,
public library 69,178,784 bytes (65.97 MiB, over the then-60 MiB budget). The API
stack sample attributed 7,119 of 7,202 main-thread samples to LLVM's function pass
manager. Linker-map attribution: 34,481,724 text bytes in the public C API object,
19,356,368 in native inference; two `batchInternalPrepared` specializations alone
occupied 1,925,824 and 1,825,464 bytes. Results: keep admission/diagnostic fixes
and linker maps (`libantfly.map`), claim no speedup; linking the server storage
archive into the Apache public library is not a valid shortcut.

## Defects found along the way
- Internal batch encoder dropped explicit `timestamp_ns` and rejected graph mutations; graph-expansion decoder required optional null fields.
- Preflight treated every non-null `full_text` query as needing a primary text index; unbound `match_all`/`match_none` sentinels are now valid with no text index.
- Experimental Raft apply passed `_transaction` envelopes to `DB.batchReplicatedApply`, silently leaving transaction phases unapplied.
- LSM-only roles tried to open the LMDB restore-job registry at startup.
- Repair-result decoder dropped `controls_applied`.
- `capi_root.zig` did not export `platform_sync`; failed C entry points must destroy partially initialized owners explicitly rather than rely on `errdefer` in a `c_int` function.
- A stale warmup test expectation on `origin/main`; four HTTP tests and nine storage files missed by shard discovery (4y).
- CI collector failed because the ARC image lacked `node` on `PATH` (run `31448346954`, not a measurement).

## Open items recorded in the log (later resolution not recorded here)
- Storage/local query and inference remained above the 380-second normal-runner gate; the next suggested experiment was a coarse independently owned local-index subsystem inside the physical owner, and for inference moving a complete heavyweight operation family behind a compiled engine ABI.
- (Phase 2m) The experimental Raft batch carried full schema/index JSON in every entry; envelope/log amplification had to be measured and replaced by a durable versioned descriptor reference before default enablement.
- (Phase 4h) Making provider, secret, and remote-content capabilities available to every resident and candidate owner was recorded as required before the atomic source switch was complete for managed enrichments.
- Next measured duplicates after 4x: `api.query_contract`, `schema.table_schema_impl`, `foreign.postgres_libpq`, `common.config`, `api.table_reads`.
