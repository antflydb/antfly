# Unified inference completion campaign

This is the finite closeout checklist for issue #1166 and the combined Tile/native
program PR. Earlier outcome registers retain their original baselines and failed
experiments. This campaign counts new implementation separately from upstream
main changes and from transfers into generated ownership.

Baseline: `5ab37baa64210bf73dd5cf62f05217636c177380`.
Main integrated at `97945ad58e`, merge `d1509cf0a356aa0fb734a3a151546768255a2458`.
The main integration changes loading/error/streaming behavior, not qualified
native numerical programs.

## Completion gates

- [x] Integrate current main without dropping upstream behavior.
- [x] Produce one consumed ownership inventory from existing generator/catalog
      metadata, with explicit canonical-native and compatibility classifications.
- [x] Connect remaining duplicated row-program schedule facts to native emission
      and actual launch consumers through one typed family contract.
- [x] Finish the profitable family migrations identified by that inventory;
      retain independently justified precision/register/scratch schedules.
- [x] Retire proven replaced/dead paths and preserve qualification executables.
- [x] Enforce operation-based ownership names and complete source identities.
- [x] Reconcile original issue acceptance criteria to implementation and tests.
- [x] Run final host, CPU/Metal/CUDA correctness, lifetime/failure and source/image
      qualification after integration and implementation.
- [x] Run the representative CPU/Metal/Glowworm model timing matrix with unchanged
      numerical and performance budgets and retained AA/AB/BA controls.
- [x] Produce matching-baseline source/object/image accounting and consolidate
      the final qualification report and PR description.
- [x] Resolve the docs/design exception's large-code-PR classification without
      granting automatic approval or weakening comparison validation.
- [ ] Obtain required human review/full CI on the final pushed SHA. The trusted
      base workflow cannot consume its own unmerged policy changes; the code PR
      always requires human review. Physical L4/A100 runs remain explicitly
      deferred under the user's available-host decision.

Unique native primitives do not need generator wrappers solely to improve a
coverage number. Existing typed generators, native templates, catalogs and
canonical numerical leaves remain their owners. A source reduction charges all
replacement owners and metadata. A binary reduction requires separate native
image evidence.

## Implementation closeout

The consumed inventory is exported by `quant-kernel-codegen --inventory` from
both real registries. `check-kernel-ownership` runs both freshness checks and
verifies the records against shipped source; CI runs it and its negative
controls. It covers 548 Tile regions, 129 quant regions, 75 generated files,
17 standalone CUDA image/producer identities, and 58 canonical surrounds/local
include leaves. The verifier script itself is a declared build input, so edits
invalidate its cached check. The report is not a second runtime catalog.

Q4/Q8 row schedules now supply native extents, ABI family, kernel identity,
launch grids/threads and static shared bytes from one std-only typed owner.
Three repeated linear argument packs/launches share a forced-inline adapter;
QKV admission and row dispatch consume the same qualified profile. Native
programs remain unchanged. The named two-row schedule retains its separate
32-register implementation; the 40-register array alternative remains rejected.

| Remaining clone/audit group | Long-term owner and disposition |
|---|---|
| Metal flash prefill / head-dim-512 | Existing generated attention owner and shared phases; precision/staging/publication schedules remain static specializations. |
| Q4/Q8 linear rows and QKV rows | Shared numerical phase plus the connected typed row contract; preserve named operands and independent plane mapping. |
| Q6_K argmax K2560/K3840 | One CUDA renderer emits the qualified K specializations; generated body similarity is not duplicate maintained mathematics. |
| Q4/Q8 projection pairs | Shared DP4A/warp phases and projection template; retain GGML/legacy packed representation, epilogue and warp/resource distinctions. |
| Q4/Q5 encode families | Existing codec-aware generated owners retain different scale/min/high-bit reconstruction; common traversal/stores are already shared. |
| Native surrounding helpers and includes | Canonical numerical leaves remain authored implementation. Include/template/macro consumers and computed lookups prevent lexical deletion. |
| Retired paths | BP removes 88 unused functions and unused ownership/cache paths; BK retires the exact unreachable/duplicate exports; BT removes the unused generated decoder. Qualification executables and live compatibility exports remain. |

The former BERT prefill source owner is named for its actual F32/head-dim-64
operation. Historical exported symbols remain compatibility adapters. Original
issue acceptance items are mapped individually in
[the acceptance map](unified-execution-acceptance.md), including retained
implementation-directory boundaries and explicitly unsupported signatures.

The docs/design approval exception now classifies a change set at/above the
300-file comparison bound as requiring human review before attempting the
bounded comparison. It does not autoapprove a code PR or relax immutable tree,
SHA, reviewer or file-policy checks. All 23 workflow tests pass.

Closeout validation also corrected an existing lifetime test's sub-ULP absolute
exponential oracle: it now compares against independent F64 values with a two-F32-
epsilon relative bound. The optimized macOS CPU compiler reservation reflects
its measured 11,435,048,960-byte peak with 10% rounded headroom. These are test/
scheduler corrections, not relaxed production numerical or performance budgets.
The generated error-name table includes the main integration's missing names
and the new offline guard errors.

## Final qualification and accounting

| Closeout scope | Result |
|---|---|
| CPU contracts/ownership, optimized native build | 130 passed; 17 expected disabled-backend skips. |
| Physical CPU/Metal preparation, views, transfers, integer storage/cache eviction, BERT gradients; Metal debug layer | 20 passed; 2 CUDA-only skips. |
| Physical GB10 CUDA launch ABI/residency/prepared storage and row-tail/canary/captured-input checks | 71 passed; 1 optional EmbeddingGemma2 fixture skip. The new row-schedule replay test executes and passes. |
| Tile lowering/ownership/schedule/inventory tests | 22 passed. |
| Inventory verifier negative controls | 5 passed. |
| Docs/design approval policy | 23 passed. |
| Final canonical CUDA portable PTX, seven-target fatbin, SM89 cubin | All three rebuild byte-identically with pinned NVCC 13.2.86. |

The final model matrix runs **1,008 fresh processes**, **63 case comparisons**
and **126 timing gates**: seven CPU, eight Metal and six CUDA workloads, each
with balanced sixteen-arm AA, AB and reversed BA controls. Every output is finite
and byte-identical to its matching frozen baseline. Median and p95 budgets remain
1.05 and 1.10. The worst observed ratios are **1.018495 median** and **1.048172
p95**. CUDA uses the authorized shared-GPU comparison and qualified CUDA 13.0
cuBLAS library, with GPU-state snapshots around each matrix.

CPU uses the frozen BP baseline; Metal uses the corrected BM baseline so a broken
historical multi-row decoder cannot qualify a candidate. The final metadata-only
prefill owner-label correction is recorded separately from the immutable benchmark
binaries. Its numerical CPU/Metal sources and CUDA host launch inputs match the
qualified snapshots; CUDA changes only generator comment labels and the final
three-image rebuild confirms identical images.

| Matrix | Cases | Worst median ratio | Worst p95 ratio |
|---|---:|---:|---:|
| CPU AA | 7 | 1.000814 | 1.005799 |
| CPU AB | 7 | 1.004675 | 1.006138 |
| CPU BA | 7 | 1.018495 | 1.040269 |
| Metal AA | 8 | 1.003453 | 1.008044 |
| Metal AB | 8 | 1.006946 | 1.048172 |
| Metal BA | 8 | 1.016907 | 1.030188 |
| CUDA AA | 6 | 1.000115 | 1.000525 |
| CUDA AB | 6 | 1.006732 | 1.024207 |
| CUDA BA | 6 | 1.005751 | 1.006381 |

Against pinned main `97945ad58e`, counted authored production removes **14,695
lines / 1,002,639 bytes**, including all replacement generator and shared-owner
costs. The closeout itself adds **241 authored production lines / 14,058 bytes**
for contracts, inventories and guards. Generated output, tests, qualification
tooling and documentation remain separate budgets. No relocation savings are
claimed.

| Compiled artifact, matching baseline/toolchain | Main bytes | Candidate bytes |
|---|---:|---:|
| Metal Objective-C object, Clang `-O3 -fobjc-arc` | 2,936,544 | 3,794,080 |
| Canonical CUDA fatbin | 7,942,584 | 15,000,488 |
| Portable CUDA PTX | 11,729,203 | 11,679,367 |
| SM89 cubin | 6,684,096 | 6,641,472 |

The Objective-C object grows; it includes native source and host bindings, not a
count of maintained mathematical definitions. Main already uses size compression;
the branch retains qualified fast compression for startup behavior. Its fatbin is
larger than main. The historical BK 74.3% reduction is against the explicitly
pinned 58.4 MB uncompressed baseline, and does not describe this current-main
comparison. Closeout changes zero checked-in device-image bytes.

Receipts are retained under `.benchmark-assets/1166/completion/`: model matrix
receipts and immutable executables/configs, `model-qualification-summary.json`,
`canonical-final/artifact-check.json`, `final-native-preservation.json`,
`post-qualification-owner-label.json`, inventory/cache checks, object input/command
identities, and source accounting against both the merge baseline and pinned main.
Failed pre-fix host tests, the sandbox-stalled Metal run, initial cache run made
while inputs were changing, and the corrected reruns remain available.

The final enabled-backend cache identity scope passes after the F32 owner-label
correction (227.631 seconds); the consumed inventory/freshness build and all 22
Tile tests pass on the final source. Human review/full CI is the remaining merge
gate, and physical L4/A100 timings retain their explicit available-host deferral.
