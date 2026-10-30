# BK: Smaller CUDA images with unchanged retained programs

Baseline: `b2dcf92c76`. The canonical fatbin falls from **58,414,520 to
15,000,488 bytes (74.3%)**. This combines lossless fast compression with removal
of three unreachable Q4_0 FP16 tensor-core epilogues and one duplicate GELU
export. It does not reduce resident device code by 74.3% or establish a kernel
throughput improvement.

| Artifact | Before bytes | After bytes | Reduction |
|---|---:|---:|---:|
| Multiarchitecture fatbin | 58,414,520 | 15,000,488 | 74.3% |
| Portable PTX | 11,739,003 | 11,679,367 | 0.51% |
| Standalone SM89 cubin | 6,671,424 | 6,641,472 | 0.45% |

## Export and instruction audit

All seven native architectures (SM75/80/89/90/100/110/120) and the compute75
fallback PTX remain. Each native image now has 449 exported programs, down from
453. The strict native comparator checks all **3,143 surviving programs** and
finds identical instructions and resource usage. Its retirement allowlist
requires exactly the four intended removals, including their absence from every
candidate architecture.

The three retired Q4_0 bias/bias-GELU/bias-add FP16 tensor-core epilogues had
required-symbol declarations and module fields but no launch routes. Reachable
no-bias and Q4_K/Q8 epilogues remain. Four apparently unreferenced gather fields
are actually selected through computed field names; they remain and execute in
the hardware tests.

The PLE and FFN BF16 GELU products had identical parameter layouts, instructions
and resources on every architecture. Both model-facing launch methods now use
one operation-named `gelu_product` module field and the existing compatibility
export. Launch count, rounding, tail handling and the native schedule remain.
The other six groups of identical compiled programs include live schedule and
benchmark identities; this batch preserves those identities rather than treating
textually similar exports as unreachable.

Net authored production source falls by **1,687 bytes / 39 lines**, counting all
new production owners. Generated source, inline tests and qualification tooling
are accounted separately. The large binary reduction principally comes from
compression, not source removal.

## Compression and startup tradeoff

The pinned CUDA 13.2.86 compiler now uses `--compress-mode=speed` with
`-Xfatbin=-compress-all`. A lossless repacking experiment compared the original
58.4 MB fatbin with default/balance (10.7 MB), speed (15.0 MB) and size (8.0 MB)
profiles. Speed was selected because tighter modes added about 5 ms to module
loading and first launch, versus about 1 ms for speed.

Final randomized measurements used 35 cycles, discarding four warmup cycles,
and included an original-versus-original control. Times cover module loading,
lookup and first launch with resident inputs; they are not whole-model startup.

| Loading mode | Original median us | Control median us | Final median us | Final p95 us |
|---|---:|---:|---:|---:|
| LAZY | 1,773.784 | 1,764.969 | 2,746.308 | 2,811.092 |
| EAGER | 4,065.438 | 4,058.735 | 5,083.739 | 5,157.450 |

This compressed format requires drivers compatible with CUDA 12.4 or later;
see NVIDIA's [NVCC compression documentation](https://docs.nvidia.com/cuda/archive/13.2.2/cuda-compiler-driver-nvcc/index.html).
The standalone PTX and SM89 profiles retain their existing uncompressed formats.

## Qualification and maintenance

Glowworm (GB10, driver 580.178.04) passes 200 independent-reference CUDA
correctness cases and all 114 AA/AB/BA timing gates: row programs 89/90,
WMMA products 21/18 and prefix normalization/pooling 90/6 respectively.
Maximum median timing ratios were 1.0163, 1.0013 and 1.0011; no timing gate
failed. This was a shared-GPU comparison with controls, not an exclusive-device
whole-model throughput qualification.

Five host-side CUDA tests also pass without skips, including exact-width typed
gathers, launch registry ownership and both GELU adapters at zero, short,
block-boundary and tail lengths. The new GELU test checks an independent GELU
reference, adapter equality, guard storage and launch counts.

Both source generators and Tile lowering checks pass. Six native-comparator
unit tests, nine regeneration tests, 105 selected compiler/proof tests and the
independent Metal/CUDA enabled-backend cache identity test pass. The CUDA cache
closure now includes the modified embedding host adapter. Historical Q4_0
benchmark timings remain historical; their current SM89 image binding was
renewed after exact native comparison of the qualified programs.

The three artifacts were built from the exact transferred source closure with
the canonical NVCC commands. The production script's real export and target
validators pass on all resulting profiles; wrapper integration is additionally
covered with a mock compiler. Compressed exports are inspected using decoded
ELF entry symbols, not raw byte searches. Regeneration now checks both source
generators and keeps compiler temporary files in the worktree cache.

Raw receipts, source accounting, source bundle, failed intermediate validator
checks and compression experiments are retained under
`.benchmark-assets/1166/bk-campaign/`. No Metal/CPU numerical program changes or
whole-model throughput improvement are claimed in this batch.
