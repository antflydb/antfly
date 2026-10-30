# BH: shared native matrix phases

Baseline: `f555ba4e1b`. This batch removes **3,185 authored production bytes**
after charging every replacement owner: 2,765 from the complete Metal translation
unit and 420 from CUDA. It adds no launch API, geometry descriptor or runtime
schedule switch. Generated source and image bytes are unchanged. Physical source
lines increase by 14 because the shared phases and their arguments are formatted
separately; no line-count reduction or overall 20% reduction is claimed.

## Shared programs

Eight Metal phases extend the existing source owners:

- Quantized weight swizzling shares the 16-element half-tile scatter across
  generic Q4/Q8, aligned Q4, typed Q8 and paired projection paths. Four/eight
  swizzle groups and native local identifiers remain explicit static operands.
- Four-by-two fragment MMA shares operand loading, accumulation and pointer
  advancement. Unroll spelling, the three optional SIMD barriers and the wider
  M64 right-operand advance retain their original policies. It also serves the
  existing paired-projection traversal, without restaging activations.
- Four-by-two fragment stores share direct and scratch publication while keeping
  the original destination and pitch. Four-by-one MMA retains the scalar right
  fragment used by the narrower paired tile.
- Scalar activation acquisition retains the guarded eight-element traversal;
  it is not replaced by the separate vectorized load schedule.
- Grouped 512-wide attention shares geometry, QK fragment traversal and PV
  accumulation. The 64-key path keeps its direct acquisition and outer barrier;
  the 32-key path keeps zero-filled query/key/value tails and per-tile barriers.
  Score association, visibility, online softmax, rescaling and final publication
  retain their existing owners and schedules.

The complete emitted Metal shader is **byte-identical** to the frozen original.
The phases expand at their original lexical sites. Shared allocations, ABI,
fragment counts, loads/stores, precision boundaries and synchronization do not
change. Historical entrypoint names remain compatible; new phase names describe
operations and physical schedules.

CUDA extends `wmma_tile_program.cuh` with a preloaded-row product phase. Both M64
relative-attention schedules share three maintained sites: content QK and the two
relative-product traversals. Left fragments stay in registers; this phase does
not substitute the existing two-load WMMA phase. Half/column-major right operands,
four depth fragments, caller-owned destinations and native fragment identifiers
remain explicit. Compact run discovery and both relative scatter schedules stay
with their callers.

## Performance-program preservation

With pinned NVCC 13.2.86 on Glowworm's GB10:

- The fresh original and candidate SM121 cubins are byte-identical. The strict
  native comparator covers all **453 functions**, including registers, stack,
  local/shared storage and complete instruction records.
- Rebuilding all three canonical production profiles produces byte-identical
  portable compute-75 PTX, the SM75/80/89/90/100/110/120 fatbin and the standalone
  SM89 cubin. This preserves all **3,171** architecture-specific native functions.
  Checked-in images require no update.

Metal preserves the entire shader input, with the same default/safe compiler
options. Hardware pipeline checks also confirm unchanged SIMD width, maximum
thread count and static shared storage. This batch preserves the compiled
performance programs; it does not claim a speedup from noisy timings.

## Hardware and source qualification

`check_metal_matrix_phases.m` requires a frozen shader and complete source
identity before compiling either side. It independently resets mutable outputs,
checks original bits and trailing canaries, and uses F64 references:

- **52 correctness cases** cover nine Q4/Q8 single-projection entrypoints,
  the paired Q4 32-column gated tile and both grouped-attention matrix tiles,
  in default and safe math modes. Projection cases exercise input dimensions
  96/128, output dimensions 32/64/65, row tails and 32/64-row schedules. Quantized
  fixtures use dyadic operands and known integer coefficients so staged half
  operands have an independent exact representation.
- Grouped attention covers independent ranges, completely empty rows, grouped
  heads, local windows and partial query/key tiles in its admitted tail path.
- **34 resource checks** span 12 entrypoints and both math modes. M64 tail tests
  reserve the original 16 KiB publication workspace; bulk timing retains 8 KiB.
- All **12** fixed AA/AB/BA timing gates pass: 31 pairs of 32 dispatches,
  median <=1.05 and p95 <=1.10. Candidate medians span 0.98815–1.01118.

The existing `check_cuda_wmma_tiles.py` gains `--m64-only`. On actual CUDA hardware,
**21 frozen-original bitwise/F64-reference cases pass** across both M64 schedules,
including uniform compact runs and non-monotonic bucket maps. Sequences
1/7/63/64/65/129/513 cover tile boundaries and tails; cases include masked documents,
an initially masked key tile and masked queries. Nonzero destinations and canaries
prevent missing writes from inheriting baseline results. The original 61-case
fixture remains available with its prior default behavior.

Generated-source freshness passes for 70 files and 129 owned regions. The nine
source-accounting regressions and Python lint pass. No new include dependency is
introduced: the existing WMMA owner and Metal translation unit already belong to
the backend identity closure. CPU kernels are unchanged.

The audit rejected apparent large wins that were already generated expansions or
existing shared owners. Source scaffolding is counted in full; moving text,
renaming symbols, generated copies and image size do not earn authored savings.
This is incremental progress toward the overall reduction target.

Evidence under `.benchmark-assets/1166/bh-campaign/` retains frozen source
closures, decoded shader identity, native reports, independent references,
production-profile commands/hashes, raw Metal timing samples and source accounting.
