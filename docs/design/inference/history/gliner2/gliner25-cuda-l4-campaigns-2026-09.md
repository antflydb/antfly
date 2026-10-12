# GLiNER2.5 CUDA L4 campaigns (2026-09)

> Relocated verbatim from `zig/pkg/inference/scripts/gliner25/CUDA.md` (lines 202–229, 308–354, 421–437, 444–477, and 511–553 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [CUDA.md](../../../../../zig/pkg/inference/scripts/gliner25/CUDA.md) before the move.

## Training comparison: v36–v39 trainer results

The unchanged v36 trainer passes independent 100-update comparisons against
eager Python CUDA for the small model, heads/full, microbatches two/eight,
accumulation two and default epsilon `1e-8`. All sampled weights and optimizer
moments have zero numerical difference. Three warmup updates and 12 balanced
pairs put native heads/B2, heads/B8 and full/B2 at approximately 3.26x, 1.68x
and 1.62x faster, respectively; full/B8 is 3.1% slower. These results use the
existing short synthetic fixtures and do not establish general learning quality.
The separately tested compiled Python candidate fails the 100-update equivalence
gate from update 33, so its faster timings remain diagnostic. V38 subsequently closes
that eager full/B8 gap: 35.51 versus 33.69 examples/s, 5.7% lower paired native
latency, and raw-bit equality for weights/moments through 100 updates. Its
training-only 2 GiB idle-cache default keeps physical allocation limits and
pressure eviction; other CUDA owners keep their 1 GiB default. The v38 suite
passes 107 regressions with three Metal-only skips. The other rows have also been refreshed on v38: heads/B2 is 48.09 versus 14.74
examples/s, heads/B8 86.34 versus 51.54, and full/B2 15.43 versus 9.04. All four
100-update rows have raw-bit equality at all 48 sampled states, and all paired
latency confidence intervals favor native. Broader release gates below remain open.

V39 batches finite/all-zero optimizer checks into bounded CUDA flag reductions,
while preserving clipping, AdamW arithmetic and durable-state hashes. Its four
100-update small-model eager rows pass with raw-bit identical weights/moments
at all 48 sampled states. Full/B2 reaches 18.56 versus 9.10 examples/s and full/B8
39.20 versus 33.31; paired speed estimates are 2.03x and 1.17x, respectively.
Heads/B2 and heads/B8 paired speed estimates are 3.62x and 1.80x. The integrated
suite passes 111 tests (three Metal-only skips), with clean memcheck, initcheck
and racecheck. These results retain the same synthetic/deterministic scope;
external benchmark reports record confidence intervals, binary identities and
remaining qualification work.

### L4 training measurements before optimization, 2026-09-15

**Diagnostic results: strict weight parity failed.** These are the published
small checkpoint against pinned Fastino/PyTorch 2.9.1+cu128 eager CUDA with fused
AdamW, using the deterministic FP32 contract above. Each row has three warmup
updates and ten measured update pairs, with accumulation two. Rates are median
examples/second; relative speeds and intervals use paired observations.

| Training | Microbatch / effective batch | Zig | Python CUDA | Faster arm, paired 95% interval |
| --- | --- | ---: | ---: | --- |
| Heads | 2 / 4 | 20.47 | 14.34 | Zig 1.44x [1.37, 1.50] |
| Heads | 8 / 16 | 60.73 | 52.24 | Zig 1.16x [1.13, 1.17] |
| Full | 2 / 4 | 4.84 | 9.04 | Python 1.88x [1.84, 1.93] |
| Full | 8 / 16 | 7.65 | 32.17 | Python 4.24x [4.10, 4.31] |

Initial weights and token IDs match. All four initial gradient comparisons pass:
136 tensors / 3,233,943 elements for heads, and 334 / 73,881,879 for full training.
All 15 loss components pass throughout the measured trajectories, as do Adam
moments in the inspected states. Strict updated-weight checks fail in 3–5 tensors
after the first update and 4–9 after the final update. The largest final absolute
weight differences are 8.75e-5 (heads B2), 3.76e-5 (heads B8), 2.77e-5 (full B2),
and 2.20e-5 (full B8). No tolerance was relaxed to accept these results.

A separate first-update diagnostic inspected the four failing head-only B2
tensors. Both arms match the AdamW formula using their own gradients within
2.44e-8. At the worst cross-framework coordinate, unclipped gradients differ by
2.15e-7; clipping and epsilon=1e-8 amplify this into a 3.06e-5 weight difference.
This explains the observed first-update discrepancy without establishing strict
cross-framework weight parity or equivalent learning quality.

Full training was slower than Python in this baseline. A separate Nsight trace covering
initialization and four full B8 microbatches records 7,907 allocations, 7,907 frees,
and 21,832 stream synchronizations. Allocation/free account for 42.6% of recorded
CUDA API time; generic reductions account for 43.2% of GPU kernel time. Prioritize
reusing device allocations and routing metadata, reducing scalar synchronization,
and fusing/replacing generic backward reductions. The profile is an investigation
aid and is excluded from the throughput samples.

Local evidence is in `/tmp/antfly-gliner25-cuda-training-campaign-v1/summary.json`,
with per-run reports, loss journals, complete tensor-error summaries, cleanup
receipts and manifests. The profile is under its `profile/` directory. The
optimizer diagnostic is in
`/tmp/antfly-gliner25-cuda-training-update-diagnostic/optimizer-sensitivity-analysis.json`.
All workers exited, snapshots were removed, and no host-fallback calls occurred
in timed native steps. These measurements do not qualify training performance;
compiled/mixed-precision baselines, other checkpoints, LoRA/DoRA, long documents
and held-out learning quality remain outside this campaign.

## Validation on L4

On 2026-09-14, the NVIDIA L4/SM89 passed the focused required-hardware CUDA
gate (40 selected, 40 passed, no skips), the published small-model three-window
regression, and both published heads/full training resume tests. The full test
performed four microbatches and two optimizer updates, rejected a mismatched
restore digest, reproduced the uninterrupted state/model exactly after partial
resume, and reloaded the portable export. Its final model SHA-256 was
`b5755248b202d79e035d0fd437af459f1d2a64a767f730ee3ebf687ace9f7cdd`.

The CUDA-enabled host regression run passed 515 tests with 74 hardware/external
fixture skips; the CUDA-disabled shared regression passed 387 with 75 skips.
Those runs excluded the existing socket-transport gate because the sandbox
disallows binding. The CUDA Python contract suite passed 13 checks, including
the original hash-pinned trained-export/merge contract loaders. The frozen CPU
driver, oracle and reference fixtures were not changed.

## Diagnostic inference results

The final L4 campaign used Zig 0.16.0 ReleaseFast, Python 3.12.3, Torch
2.9.1+cu128, and FlashDeBERTa 0.0.7 where selected. Each model/reference pair
used the same ten frozen requests, five warmups and thirty measured pairs per
request. All reported runs passed token/output parity and complete worker-tree
cleanup; every native sample reported GPU launches and zero generic host tensor
fallbacks. Loading and compilation were excluded from timings.

These are geometric means of the ten per-case paired speedups, calculated as
Python latency divided by Zig latency. Larger values favor Zig.

| Python CUDA reference | Small | Base | Multi |
| --- | ---: | ---: | ---: |
| Compiled FP32, dynamic shapes | 8.80x | 4.52x | 4.24x |
| Compiled autocast FP16 | 9.55x | Rejected | Rejected |
| Eager FlashDeBERTa FP32 | 13.12x | 6.88x | 6.21x |
| FlashDeBERTa FP32, static compilation requested | 13.41x | Not run | Not run |

Compiled FP32 was the fastest measured Python reference by model-level
geometric mean. Its slowest per-case Zig speedup was 2.31x (multi); that case's
paired bootstrap 95% lower bound was 2.30x. These short diagnostic requests do
not establish large-batch throughput, training speedup, or real-data quality.

Candidate validation matters: compiled FP16 changed a base record confidence
from 0.8021 to 0.9614 and changed the number of multilingual record instances.
Eager FP16 on small tied confidence scores and changed ordering. Dynamic
FlashDeBERTa compilation failed inside TorchDynamo on symbolic dimensions;
the separately recorded static candidate passed the small fixtures.

Reports and raw pairs from this device are under
`/tmp/antfly-gliner25-cuda-measured-v1`. The `compiled-fp32-all/report.json`
SHA-256 is `22ec0c8e3e3ef09c352fc2410d82186642ad0903f4cf4cbf509459e117ac4fe1`.
The reports remain explicitly unqualified for release.

### L4 measured batch results

All twelve model/batch combinations passed, with five warmups and thirty
measured pairs per workload (720 pairs total). Both arms use strict FP32;
Python uses the pinned Fastino implementation with dynamic `torch.compile`.
Every timed document passed the frozen output comparison, every validation
batch matched encoder tokens/shapes, and all worker trees exited cleanly.
Every native result recorded GPU work and zero generic host tensor fallbacks.

Throughput is median documents/second. Speedups are inverse paired median
latency ratios, so they can differ slightly from ratios of the displayed
separate throughput medians.

| Model | Batch | Mixed Zig / Python docs/s | Speedup | Records Zig / Python docs/s | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| small | 1 | 173 / 28 | 6.31x | 251 / 26 | 9.54x |
| small | 8 | 873 / 92 | 9.55x | 1,254 / 80 | 15.59x |
| small | 32 | 1,181 / 124 | 9.47x | 2,021 / 102 | 19.51x |
| small | 64 | 1,158 / 128 | 9.23x | 2,124 / 109 | 20.30x |
| base | 1 | 83 / 27 | 3.05x | 126 / 26 | 4.76x |
| base | 8 | 416 / 87 | 4.78x | 737 / 78 | 9.41x |
| base | 32 | 512 / 109 | 4.66x | 1,136 / 104 | 10.96x |
| base | 64 | 495 / 111 | 4.48x | 1,133 / 103 | 10.91x |
| multi | 1 | 71 / 27 | 2.61x | 114 / 26 | 4.31x |
| multi | 8 | 360 / 89 | 4.05x | 685 / 80 | 8.52x |
| multi | 32 | 422 / 103 | 4.05x | 967 / 101 | 9.63x |
| multi | 64 | 405 / 106 | 3.83x | 1,049 / 102 | 10.21x |

For batch 64, paired bootstrap 95% speedup intervals are:

| Model | Mixed extraction | Structured records |
| --- | ---: | ---: |
| small | 9.06–9.33x | 19.35–20.59x |
| base | 4.45–4.56x | 10.82–11.22x |
| multi | 3.80–3.87x | 10.16–10.29x |

Raw reports, validation outputs, timing pairs, worker provenance and evidence
manifests are in `/tmp/antfly-gliner25-cuda-batch-v1` on this device.
The campaign JSON SHA-256 is
`d5c949a625cfc2ba0a925c371c022d5028d90987dfe2bf33df97eb68f71872aa`.
These measurements cover homogeneous short fixtures and the named FP32
reference; they do not select the fastest Python precision/profile for
every batch, establish long/ragged workload performance, or measure training.
