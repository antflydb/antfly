# Z: native kernel families and stored expansion removal

Baseline: `0911b96caafa64e9617679b29e8d62f79656aaa5`, including main through
`9ace47d36392186d9130174eec375b5907d01ef8`. Earlier campaigns and upstream
changes receive no new deletion credit.

## Implementation

The CPU Q8_K wide dot family now has one native Zig comptime owner for nine
specializations: MR1/MR2/MR4 with eight separate blocks, panel8 or panel16.
The existing integer dot leaves, chunk traversal and post-dot scaling remain
explicit. Generated regions publish the existing return types through thin
wrappers. The old wide-body text emitter is removed. Four-column and fused
schedules remain distinct.

Metal's four BF16 vector affine exports share one native MSL template,
`termite_bf16_affine_vec4<Rows>`. The SIMD reduction, BF16 conversion, bias
precision and guards retain their original order. Six paged GQA stage/reducer
exports share two native always-inline numerical functions. Ordinary, OCR and
indirect batched entry points retain their own guards, pointer acquisition,
launch schedules and scratch contracts. The fifteen tiled affine exports
already shared a native body; this campaign claims no new reduction for them.

CUDA's eighteen F32 decode exports instantiate three native C++ algorithms:
serial attention, split stage and split reduction. The native template replaces
both the old numerical string builders and per-head serial implementation
owners. Specialization remains at compile time. Flash prefill and split-K
online retain their separate native owners because their schedules and
precision differ.

Twelve checked-in standalone CUDA attention files now include their canonical
native owner instead of storing a complete copy per geometry. Runtime/JIT
source stays complete and self-contained; artifact fingerprints still cover
that complete semantic source. The qualification identity now covers all three
native template inputs, including the two previously omitted owners. Mutating
each template independently invalidates qualification without invalidating the
ordinary baseline program. The closure contains 19 Metal qualification inputs,
68 CUDA qualification inputs and eight CUDA dispatch inputs.

## Source accounting

The ledger charges every replacement helper, native header and build contract.
Inline tests and benchmark tooling are reported separately. Generated PTX and
binary images are excluded from source-text totals.

| Source category | Net bytes removed |
| --- | ---: |
| Maintained production | 15,830 |
| Stored generated production | 359,395 |
| **Physical production text** | **375,225** |
| Inline tests (growth) | -6,339 |
| Test and benchmark tooling (growth) | -3,040 |

Within maintained production, Metal shader source removes 11,641 bytes and
retired generator owners remove 22,706 bytes. The new CUDA native owner costs
14,532 bytes, the CPU native owner costs 3,513 bytes and shared/build contracts
cost 472 bytes. These are components of the net, not additional credits.
The generated-source removal includes the compact standalone files, main CUDA
bundle and CPU thin wrappers. The expanded main MSL program also shrinks from
988,879 to 972,507 bytes (16,372 bytes); that expanded representation is not
counted again in the ledger. This is a source-maintenance and repository-text
result; it is not an executable-size reduction or completion of the separate
20% CUDA/Metal maintenance-source targets.

## Qualification

Evidence is retained under `.benchmark-assets/1166/z-campaign/`, including
failed prototypes, raw timing arms, manifests, compiler output and source/image
hashes. Apple device measurements use the local M4 Max. Independent correctness
references accompany native code comparisons.

- Source/codegen suite: 149 tests passed. Metal compiler checks: 54 passed using
  the real runtime compiler; this does not claim offline AIR compilation.
  Metal runtime suite: 154 cases passed, including the precision hook.
- CPU: all nine Q8_K wrapper paths pass an independent F64 oracle with signed
  inputs and negative scales. Three fixed 32-arm AA/AB/BA cohorts pass the
  original 5% median / 10% p95 gates. Worst median ratio is 1.014706; worst p95
  ratio is 1.058824. Eight measured native bodies are instruction-identical;
  MR1 panel16 changes from 1,275 to 1,306 instructions and passes those timing
  gates. No instruction-identity claim is made for that body.
- Metal: 33 affine and 56 GQA cases pass all three fixed AA/AB/BA cohorts,
  totaling 267 direct comparisons. Cases cover vector row/column tails,
  HD256/512, grouped heads, local/global windows, page and split boundaries,
  ring pages with NaN padding, and mixed indirect batches of two/four records.
  F64 affine, split stage and nontrivial reducer references pass. Output,
  guards, pipeline thread capacity and static threadgroup-memory gates pass.
  The largest candidate median ratios are 1.017136 for affine and 1.005748 for
  GQA, within the unchanged 1.05 gate. Native token substitution proves that
  all six GQA exports preserve their numerical bodies.
- CUDA compiler: all three shipped images were regenerated with pinned CUDA
  13.2 on native Linux ARM. Each of seven targets (SM75/80/89/90/100/110/120)
  contains 454 kernels; 448 have identical disassembly. Only six split-stage
  kernels change, each exchanging two independent, equivalent integer counter
  updates. Floating instructions retain their positions; instruction counts,
  registers, stack, shared, local and constant resources do not increase.
  SM100 assigns the two integer updates differently between IADD3 and VIADD;
  this is explicitly not an instruction-identity or device-latency claim.
  Native source substitution also compares 359 raw global definitions,
  including all eighteen new template instantiations. Compiler/tool tests:
  24 passed. Task-owned compiler VMs are removed after use.
- Product compilation passes with Metal and CUDA enabled together. WASM32,
  WASM64 and x86 v2/v3/v4 compile checks pass. The x86 test binaries explicitly
  instantiate all nine Q8_K oracle paths; these are compile checks, not native
  execution claims. All three template-mutation cache regressions pass.

Initial failures are retained: stale source-shape assertions after moving
bodies, an obsolete scratch-occurrence assertion that included new OCR uses,
and one runtime rerun missing the runtime-compiler bridge in PATH. Assertions
were scoped to the actual ownership/dispatch contracts, and the final suites
above pass. No numerical or performance gate was relaxed.

## Composed execution

Both frozen executables pass all fifteen CPU/Metal model comparisons: thirteen
MiniLM/reranker cases plus two tiny-Llama decoder cases. Outputs are
byte-identical and route checks pass. The largest candidate median ratio is
1.004399; the largest p95 ratio is 1.008433, both on the native F16 reranker.
The original 5% median / 10% p95 gates remain unchanged. The decoder protocol
was declared before timing with 100 warmups and 201 measurements per arm;
all cases retain their fixed 32-arm cohorts and no automatic timing retries.

All 42 controls pass: five primitives, three extended-warmup primitives, all
31 short normalization widths, and three host-encoding controls. Their route
checks pass. The independent decoder reference passes all five steps for both
CPU and Metal at the existing 0.002 tolerance; maximum errors are 0.000668 and
0.000759 respectively.

All seven protected native groups retain instructions, resources and required
alignment. The three GEMM/attention leaves retain 655 / 553 / 619 instructions
and 64-byte alignment. Four projection bodies retain 1,215 / 1,829 / 18,758 /
35,383 instructions. Changed constant addresses resolve to identical bytes.
These protected groups are separate from the nine Q8_K measurement bodies
whose one non-identical specialization is disclosed above.

All five 120-second ownership soaks pass: native/Metal encoders, native Q4_K
reranker and native/Metal decoders. Outputs remain finite and tracked host
ownership returns to zero at teardown. These run concurrently as stress tests;
their timings are not used for latency admission.

All twelve compact standalone CUDA attention sources compile with NVCC 13.2
for SM89 and produce normalized PTX identical to their fully reconstructed
sources. This independently verifies the relative include closure. Both
compiler-only VMs used for final image and standalone-source checks are removed;
the original Podman machine and default connection are preserved.

Five expected negative controls reject a deliberately wrong numerical reference
and invalid pointer-patch target, alignment, owner extent and pointee extent.
The preprocessed Metal host operations match the baseline exactly except for
optional diagnostic source-line tags, and the separate training shader program
is byte-identical. Zig formatting and whitespace checks pass.

## CUDA device qualification remains queued

The final source snapshot has a separate Glowworm worktree,
`/home/ajroetker/antfly/.worktrees/1166-z-validation`, with its own build caches
and results. It imports and verifies the complete Zig source/image manifest,
independently pins helper inputs and preserves immutable baseline fixtures. A
private read-only cuDNN closure supports the required OCR tests.

Its controller waits for the existing X and OCR qualification queues and other
GPU work, observes a quiet window, then runs the selected unit/ownership/kernel
suite, GLiNER2/GLiNER25, fresh product/codegen/model builds, all 21 direct CUDA
profiles (558 cases), four composed model cases and two soaks. If other work
resumes, it stops only its own process group and retains unqualified partial
results before waiting again. Build logs also reject Zig memory-limit errors
that return a zero process status.

This is queued validation, not a CUDA device or latency pass. The seven-target
compiler/resource audit above is complete; GPU correctness and performance
admission for this snapshot still require that isolated run. The individual
CUDA/Metal 20% maintained-source targets remain open.

Frozen implementation is `9a9054d6f849f5b92cc9e1f55861040b2ccb0ae6`.
The 8,009-input source/image manifest is
`26b68b43cd98c7b3eebd7796d7472abbd2498246e77c34e5019a88b88e91bb7e`.
Glowworm's matching synthetic import is
`05c66030fd71094e60456b2e6ed004cdb797348a`, with 37 pinned helper inputs.
The controller was verified alive and waiting behind the earlier queues and
active GPU work. Raw receipts, image hashes and timing arms remain under the
Z evidence directory; later documentation-only commits do not change the
qualified source/image closure.
