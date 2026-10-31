# AZ: static row and chronological-attention owners

Baseline: `3bc9609613`. Matching model operations now use generic backend owners;
native exports, model admission, fusion boundaries and tuned schedules remain.
There is no runtime program interpreter.

CUDA's `row_program.cuh` owns warp/block RMS traversal, shared-tree LayerNorm
and interleaved tree statistics. EmbeddingGemma RMS, scaled RMS,
residual/next-norm and packed heads use that owner. Generic F32 RMS, Gemma
triple-output/parallel-FFN reductions and LightOn vision LayerNorm share their
appropriate tree policy. Training's different four-lane accumulation stays
separate. Packed and unpacked BF16 GELU products share arithmetic, including
rounding GELU to BF16 **before** multiplying the other projection.

The larger consolidation is `cuda_gqa_score_prework_f32.cuh`. The renderer
instantiates score prework and one chronological-softmax consumer for HD256/512,
serial/tiled output ownership and the existing scratch capacities. Device scalar
reads, paged KV bounds/invalid-page handling, sliding windows, the chronological
recurrence and launch admission remain native. AOT includes and include-free JIT
source use the existing source-owner machinery.

Metal shares complete strided channel-normalization/ReLU between the batched
audio frontend and legacy audio export. A generic SIMD RMS statistics owner
serves embedding, vision and HD128 heads; different rotary layouts, scale/weight
ordering and schedules remain in their native epilogues. Default and safe Metal
math modes are qualified locally.

CPU normalization already uses common operation owners and native vector/ISA
selection. This wave adds no alternate CPU implementation or metadata-only CPU
reduction claim. Existing shared BF16 vision-attention tiles and unique geometry,
audio-relative and rotary semantics earn no new reduction credit.

## Net source accounting

`kernel_source_accounting.py --revision 3bc9609613` charges every new owner and
keeps generated text, tooling and compiled images separate.

| Maintained production source | Net bytes removed | Net lines removed |
| --- | ---: | ---: |
| Authored generator/owner code | 18,552 | 330 |
| Authored CUDA device files, including both new headers | -9,551 | -162 |
| Authored Metal shader source | 226 | 4 |
| **Total authored production** | **9,227** | **172** |

The raw CUDA file total grows because device math moved out of Zig string
emitters into native headers. Charging both sides gives the net above; the
18,552-byte generator reduction is not the overall saving. Generated CUDA text
separately loses **41,463 bytes / 924 lines**. New reusable qualification tools,
build wiring and inline tests have their own ledger and do not count as removed
kernel code. Compiled images are regenerated, not counted as authored savings:
the fatbin loses 7,464 bytes, portable PTX gains 4,763 bytes and the SM89 cubin
gains 128 bytes. Maintained-source savings are separate from image-size changes.

The original chronological-attention emitter cohort contains 21,171 bytes.
After charging the complete new native owner and all generator/owner wiring,
that consolidation saves **8,074 bytes, about 38.1%** of that selected cohort.
This exceeds the selected-cohort 20% target. It is not a 20% reduction across
all CPU/Metal/CUDA kernels, nor fewer executed instructions.

## Final generated code

The final warp-count policy is a template trait. Dynamic block width is acquired
inside the final-warp guard, as in the original. The earlier argument-policy
prototype evaluated that expression before entering the helper; its code and
measurements are retained separately.

On SM120/SM121, **448 of 451 core instruction bodies are identical to the
originals**. RMS, scaled RMS, residual/next-norm, VJP, pooling and contrastive
softmax are among the restored original streams. Only packed heads and the two
F32 LayerNorm entry points differ. Those three are unchanged from the versions
already timed. All six chronological-attention bodies are identical to the
originals on SM100/110/120/121.

SM75/80/89/90 retain 446/451 original core bodies; the two serial attention
consumer streams additionally differ there. Their resource budgets are
unchanged. Runtime CUDA qualification covers Glowworm's GB10; older GPUs were
not available for runtime performance checks.

All seven shipped architectures are audited for registers, stack, shared and
local memory. The final SM121 packed-head kernel uses 36 registers versus 34;
its measured GB10 occupancy remains **six active 256-thread blocks per SM** in
both images. The other two changed core kernels retain their resource budgets.
The final shipped SM120 instruction records match all 451 native SM121
qualification streams, ignoring only architecture metadata.

## Correctness and models

Raw sources, binaries, compiler logs, SASS/resources and every timing sample are
retained in `.benchmark-assets/1166/az-campaign/`. The final production images
pass **177 CUDA cases**, each checking independent references, original outputs
and changed-input/device-count graph replay:

- 89 row/epilogue cases, including widths 35/257 and LightOn widths 768/1024 at
  129/2,592 rows;
- 14 packed-head cases, including non-power-of-two width 96, optional weights,
  masks and vision rotary;
- 58 paged chronological-attention cases, including invalid pages, sliding
  windows, empty/tail/long rows, packed polar4/F16 keys and F32/F16 values;
- 16 VJP, masked mean/L2 and contrastive-softmax cases, including all-masked rows.

**40 Metal cases** pass bitwise original parity and independent CPU references
in both math modes. **160 compiler/renderer contract tests** pass without skips.
All CUDA artifacts are rebuilt with pinned CUDA 13.2; the canonical LightOn PTX
reproduces with CUDA 13.0.

Model smoke comparisons use five warmups, 31 measured forwards and AA/AB/BA
orders at batch 1, sequence lengths 16/128. Every output is byte-identical across
all **36 timing-protocol model processes**. Those model timing binaries predate
the final guarded warp-count tightening; the measured MiniLM hot kernels and
Metal source are unchanged by that tightening. A newly built final-image binary
also passes **four correctness-only MiniLM runs** (both geometries on CUDA and
CPU) against frozen original outputs. Those confirmations do not resample or
replace the failed timing gates:


| Model/backend | Latency gates passed | Interpretation |
| --- | ---: | --- |
| Cached Qwen3-Embedding-0.6B Q8 / local Metal | 6/6 | Both geometries pass; reported provider memory/counters match |
| MiniLM F32 fixture / Glowworm CUDA | 4/6 | Sequence 128 passes; short-sequence AA and candidate p95 miss |
| MiniLM F32 fixture / Glowworm CPU | 4/6 | Large timing drift; no CPU performance-parity claim |

Metal candidate model p50 ratios are 0.994650–1.002261 and p95 ratios
0.988480–1.001547. Median first-forward times are 123.381/123.120 ms
(original/candidate) for sequence 16 and 136.315/136.112 ms for sequence 128.
These are recorded first forwards, not an independently bounded end-user TTFT
guarantee. Runtime memory reports are identical at each geometry: 171,741,656
and 172,200,408 bytes. The reports are provider counters, not whole-process
resident memory. CUDA/CPU zero-valued Metal-specific counters do not qualify
CUDA/CPU retained memory. These model smokes also do not prove which fused
head/row route was selected; the focused fixtures invoke those exports directly.

## Retained performance evidence and misses

CUDA uses the user-authorized shared GPU. The fixed device protocol has 31
paired samples, 32-dispatch batches, AA/AB/BA orders and one-sided regression
limits of median <=1.05 and p95 <=1.10. Every raw sample remains, including
failed controls. No failed assay is repeated simply to remove a miss.

| Assay | Gates passed | Candidate median ratio range |
| --- | ---: | --- |
| Metal rows, default + safe math | 120/120 | 0.963207–1.010850 |
| CUDA row/epilogue argument-policy prototype, stream/graph | 90/90 | 0.984189–1.022677 / 0.991627–1.010823 |
| CUDA packed heads, stream/graph | 24/24 | 0.990758–1.000423 / 0.928329–0.963679 |
| CUDA chronological attention, stream/graph | 16/18 + 17/18 | 0.996293–1.007899 / 0.993457–1.002740 |
| CUDA statistics argument-policy prototype, stream/graph | 8/9 + 9/9 | 0.985253–1.004463 / 0.993863–0.999624 |

Attention retains candidate stream p95 misses of 1.409418/1.204054 and an AA
graph p95 miss of 1.120020. Statistics retains a candidate stream p95 miss of
1.238162. Final RMS/statistics code restores original instructions rather than
resampling those failed timings. Graph timing does not erase a direct-stream
miss. The one-sided AA gates also do not prove symmetric stability: the CUDA row
and attention AA p95 ranges extend down to 0.732/0.759. Shared-GPU tail timing
therefore remains inconclusive.

The CUDA short-model AA p95 ratio is 1.144727 and candidate BA p95 is 1.177617.
CPU candidate AB p50 is 1.313918 at sequence 16; the other candidate order is
0.931807. CPU sequence-128 AA p50/p95 are 1.315318/1.298767. Those misses remain
published. CPU kernel source is unchanged; these samples do not establish a
CPU speed improvement or a dependable regression bound.

AV's earlier Metal attention GPU p95 miss is a separate assay and remains open.
The new row/model measurements do not close it or establish full-model parity
for every EmbeddingGemma/LightOn/audio consumer.

## Other findings and reproduction

The first Metal fixture swapped legacy channel/position ABI fields. Its
independent reference caught the error while original/candidate parity passed.
The failed log remains; the corrected fixture resumes at the legacy family,
retaining the four unchanged frontend profiles' first measurements.
An accidentally unfiltered Zig invocation exceeded compile memory limits; its
log remains separate from the successful focused suite. A stale build test
already failed on baseline declaration adjacency; it now checks the actual
fail-closed block. The relocated LightOn oracle now includes its real native
ABI, row and rotary dependencies and compiles successfully. Its Torch-dependent
full fusion scripts were unavailable in the system Python; the new device
references require only the standard library and cover the changed norm.

The four `check_cuda_{row_families,packed_heads,score_families,row_statistics}.py`
tools take baseline/candidate directories containing `kernels.cubin` (which may
hold the production fatbin); the row tool also needs `lighton.ptx`. Point
`CUDA_CACHE_PATH` into the worktree's `.zig-cache/cuda-driver` for qualification.
`--bench`
enables the fixed protocol. In-place residual correctness/graphs are checked;
the row timing loop excludes those mutable chains. Their final instruction
identity supplies the no-added-work evidence.

`zig build test-metal-row-families` runs current shader references. Supply
`-Dmetal-row-baseline=/absolute/baseline_shader.h` for frozen-original parity;
the header contains the original `termite_metal_shader_source` factory renamed
to `antfly_row_baseline_shader_source`. ABI macros must match the frozen native
host ABI. Add `-- --bench` for Metal timing. Missing GPU availability fails
rather than counting a device skip. `check_model_kernel_families.py` runs the
model smoke protocol with existing immutable fixtures and retains all process
logs, setup/latency/provider metrics, outputs and fixture/binary hashes.
