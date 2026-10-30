# BE: native CUDA WMMA score-product owner

Baseline: `6f17b3713e`. One operation-based owner replaces ten repeated score
products across five CUDA entrypoints. Glowworm verifies identical native and
shipped program bytes, with original-output and independent-reference checks.
Metal and CPU kernels are unchanged.

## Native shape

`kernels/wmma_tile_program.cuh` owns `ANTFLY_CUDA_WMMA_DOT16_F32`: a 16x16
score product with row-major A, explicit B layout, explicit scalar storage,
FP32 accumulation, and FP32 publication. Depth, addresses and pitches remain
native source policies. It expands into the existing kernel block rather than
introducing a device-function call or moving shared arrays.

Six sites are the QK, content-to-position and position-to-content products of
the M16xN32/M32xN16 relative-attention family. Three sites are the corresponding
padded-column-major products in direct boundary attention. One BF16 vision site
serves both the F32 graph ABI and native BF16 I/O entrypoints. Half/BF16
acquisition, row/column layout, fragment names, unroll policy, reduction order,
warp assignment, barriers and scratch geometry retain their original contracts.

The longer M64 programs preload fragments differently; their existing owners
remain separate. Warm probability/value accumulation is also retained because
it must load the previous output rather than initialize a zero score product.
The source audit excludes already-shared expansions and unused prototype-only
similarities from production-reduction claims.

The new header is included in the CUDA qualification/cache closure and the
independent runtime-cache regression fixture. Native entrypoint names and ABIs
remain compatibility contracts; new shared owners have operation-based names.

## Performance preservation

On Glowworm's GB10 with CUDA 13.2.86:

- The frozen-original and candidate SM121 cubins are **byte-identical**. Complete
  instruction/resource reports for all **451 functions** are identical, including
  registers, stack, local and shared storage.
- Preprocessed C++ token streams are identical: **544,602 tokens**, after removing
  source-location directives and comments. `_Pragma("unroll")` retains the native
  unroll directive while permitting phase ownership in the macro.
- Rebuilding the exact production profiles produces byte-identical portable
  compute-75 PTX, the multi-architecture fatbin and the standalone SM89 cubin.
  The fatbin retains native SM75/80/89/90/100/110/120 and compute-75 fallback.
  Checked-in images therefore require no update.

This is preservation of the actual compiled performance program across the
shipping profiles, not a sampled speedup or a new isolated-GPU tail-latency
measurement. The wave does not rerun an unchanged instruction stream until a
noisy timing gate passes. Earlier shared-GPU timing qualifications and their
limits remain in the prior outcome registers.

## Correctness and tool coverage

The stdlib-only `check_cuda_wmma_tiles.py` fixture passes **61 frozen-original
bitwise and independent F64-reference cases** on real CUDA hardware:

- 37 relative-attention cases cover M16xN32, M32xN16 and direct boundary attention;
  sequence lengths 1/7/15/16/17/31/32/33/65/129/255/256, plus a 513-token direct
  boundary case; batches/heads; arbitrary bucket maps; partial masks; completely
  masked documents; and an initially masked key tile.
- 24 vision cases cover F32 input/output and native BF16 I/O, including
  sequence lengths 1/7/15/16/17/31/32/33/65/129/257/513 and partial query/key tiles.

Both sides reset their output allocation to a nonzero sentinel before launch,
so a missing candidate write cannot inherit the original result. Every case
checks trailing canaries and finite outputs. The independent oracle quantizes
the staged operands before evaluating masked softmax and weighted values in
F64. Predeclared reference bounds are `3e-4*(1+abs(ref))` for F16 relative
attention, `2e-3` for BF16 staging/F32 output, and `1e-2` for native BF16 output.
Worst absolute differences are respectively `7.04428482e-5`, `6.83216279e-4`
and `1.35868318e-3`. Original/candidate parity is bitwise throughout.

The fixture also exposes the existing fixed stream/graph AA/AB/BA assay through
`--bench` for future program changes. Ruff and source-accounting tests pass;
backend cache-identity regression validates the new header dependency.

Evidence under `.benchmark-assets/1166/be-campaign/` includes frozen include
closures, original/candidate SM121 binaries and disassembly/resource reports,
preprocessed source, actual shipped-image hashes, compiler commands, hardware
results and the complete source ledger. The task-owned remote worktree and
local build caches are removed after preservation of this evidence.

## Net accounting

After charging the new phase owner and all include edges, authored CUDA device
source removes **3,748 bytes / 36 lines**. The build/cache dependency adds 54
bytes / one line, leaving **3,694 fewer production bytes / 35 lines**. Tests
and documentation grow separately; generated text and image sizes earn no
reduction credit. This is not a claim of 20% savings across the entire tree.
