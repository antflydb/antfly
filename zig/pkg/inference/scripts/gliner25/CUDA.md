# GLiNER2.5 CUDA

This implementation uses the same boundary descriptors, shape/admission planners,
request pipeline, training graphs, optimizer transactions, checkpoints and frozen
fixtures as the CPU/Metal implementations. Encoder/head tensor operations and
optimizer state remain on CUDA; proposal selection, loss construction, matching
and decoding retain the shared bounded host stages and transfer diagnostics.

## Build

From the repository root, using Zig 0.16.0:

```sh
zig build --build-file zig/pkg/inference/build.zig bench-gliner25-cuda-build \
  -Doptimize=ReleaseFast -Dcuda=true -Dmetal=false -Donnx=false -Dpjrt=false
```

The CUDA benchmark executable shares `src/bench/gliner25_cpu.zig` and the existing
Python pairing/output-checking helpers. Its CUDA path uses the strict device
request pipeline. CUDA artifacts are generated with the repository-pinned CUDA
13.2 toolkit; the SM89 cubin runs on the L4 without runtime PTX compilation.

```sh
zig/pkg/inference/scripts/regen-cuda-artifacts.sh --write --all
```

Resident training additionally embeds a CUDA 12.8.93 activation, prefix-scan,
reduction, gather-backward, binary-loss derivative and span-feature PTX module to match the pinned PyTorch 2.9.1+cu128 math. The CUDA driver loads it before weight
upload; Python and NVRTC are not runtime dependencies. Its hash is bound into
the training checkpoint identity. This module uses driver PTX compilation,
including on the L4; the main CUDA 13.2 inference artifacts remain unchanged.
The prefix profile preserves the original scan axis when flattening tensors:
content/relation pooling uses serial outer-axis accumulation, inside scores use
an innermost Sklansky scan, and single vectors use the pinned deterministic block
scan. The strict VJP reverses scan direction; no extra activation cache is needed.
Single-vector scans and multi-block reductions partition work using device
properties, so CUDA training checkpoint identity includes SM count and maximum
threads per SM. Reduction admission and dispatch share one checked static plan,
including explicitly owned workspace for multi-block sums. Both forward and
autodiff-generated sum/mean operations receive this training profile; generic
serving arithmetic retains its existing default. Sequential start/end score
addition is a separate graph profile bound into the training plan. CUDA training
also folds direct matrix transpose inputs into contraction-axis metadata before
autodiff, retaining physical operand storage in forward and backward BLAS calls.
That opt-in arithmetic profile is versioned in the plan and checkpoint identity.
The CUDA shared-candidate profile evaluates detached span-length features on the
GPU and computes the detached inside-score mean using the same reference
reductions. Host feature descriptors remain available for CPU/Metal, but CUDA
plans omit their unused uploads. The candidate profile and actual PTX bytes are
bound into plan/checkpoint compatibility; span geometry is explicitly frozen
under differentiation.
Batched Tensor.gather operations carry an explicit backward-reduction profile
through autodiff. CUDA uses a deterministic warp reduction that matches the
pinned Python path while reusing existing integer grouping and scratch ownership.
Relation advanced indexing and query expansion retain their separate arithmetic.
The gather profile is included in plan/checkpoint identity and its additional
work is checked at both admission and dispatch.
Binary BCE/focal loss gradients also use this module through an optional backend
callback. Shared loss code owns targets, masks and reduction denominators and
passes the final scalar loss cotangent before the derivative's FP32 operations.
The callback uses normal CUDA allocation limits and tracked transfers, with
shape/settings/finiteness validation and an explicit work charge. CPU/Metal
retain their existing loss arithmetic. Listwise gradients use a separate callback
that canonicalizes the candidate layout, reuses the reference reduction planner,
and optionally reduces shared proposal gradients across queries. Shared code
supplies masks, maxima and weighted cotangents; both loss callbacks participate
in transfer admission and device temporary bounds. Consistency, query/count and
record objectives still have separate arithmetic.
For the pinned CUDA training graph, weighted pair contributions accumulate as
listwise, soft-IoU, BCE, then consistency. This includes the nonzero consistency
warmup schedule; the association is part of the versioned trainer identity.
CUDA relation and candidate query gates use an explicit saved-output sigmoid
profile. Its backward kernel consumes the rounded forward result, preserving
PyTorch saturation and multiplication order. The default CPU/Metal graph keeps
its existing decomposition. The profile and PTX bytes are bound into training
plan and checkpoint compatibility.
Portable exported weights do not acquire these resume restrictions. CPU/Metal defaults retain their existing
prefix graph. Component parity is tested against the pinned deterministic Python
profile; this does not by itself qualify sustained training parity.

Regeneration needs NVRTC 12.8.93 but no GPU:

```sh
python3 zig/pkg/inference/scripts/regen-cuda-training-math.py --write \
  --nvrtc /path/to/cuda-12.8/lib64/libnvrtc.so.12
python3 zig/pkg/inference/scripts/regen-cuda-training-math.py --check \
  --nvrtc /path/to/cuda-12.8/lib64/libnvrtc.so.12
python3 zig/pkg/inference/scripts/test_regen_cuda_training_math.py
```

`ANTFLY_CUDA128_NVRTC` can supply the compiler path instead. The checked-in
manifest binds the source, compiler/options and PTX bytes; the ordinary CI
contract suite checks it without requiring NVRTC or a GPU. Regeneration
verification remains a separate compiler-backed check.

Zero-dropout D32 boundary attention has a separate resident forward/VJP profile
using pinned PyTorch/CUTLASS efficient-attention kernels. Its ordinary tape
value saves output and log-sum-exp; bounded temporary storage covers padded
bias, delta and backward accumulation. It requires only the CUDA driver at
runtime. Compute capability 8.x uses compiled SM89 or SM80 images; other
architectures, head dimensions and nonzero probability dropout retain the
existing materialized path. Both images have been checked on L4; that does not
qualify other GPUs or sustained full-model training.

Regeneration requires CUDA 13.2.78 and the matching build-only header trees:

```sh
python3 zig/pkg/inference/scripts/regen-cuda-boundary-attention.py --check \
  --torch-include /path/to/pytorch-2.9.1/include \
  --cutlass-include /path/to/cutlass-e51efbfe18fe/include \
  --curand-include /path/to/cuda-12-curand/include
python3 zig/pkg/inference/scripts/test_regen_cuda_boundary_attention.py
```

Use `--write` to publish regenerated artifacts. The transitive dependency lock
also pins host/toolkit headers; `--refresh-lock` is an intentional dependency
change requiring renewed numerical qualification. The generator serializes a
private workspace at `/tmp/antfly-gliner25-boundary-attention-build-v1` so NVCC
private symbol names remain stable across checkout locations. It rejects an
unsafe or foreign workspace. Artifact bytes are hashed at trainer startup and
bound into checkpoint identity. Licensing is recorded in `THIRD_PARTY_NOTICES.md`.

The 42 boundary operations and eight attention phases live in
`src/ops/cuda/kernels/gliner25_{boundary,attention}.cuh`. The latter shares the
bounded replay schedule with Metal in `deberta_training_attention_schedule.zig`.
There is no floating-point atomic accumulation in replay attention.

## Correctness

GPU tests must run where `/dev/nvidia*` is accessible. In a device-isolating
sandbox, an installed toolkit does not imply driver access. Set
`TERMITE_REQUIRE_CUDA_TESTS=1` to make unavailable hardware a failure.

```sh
zig build --build-file zig/pkg/inference/build.zig test-gliner25-cuda \
  -Dcuda=true -Dmetal=false -Donnx=false -Dpjrt=false

ANTFLY_GLINER25_TRAINING_JOB_CUDA_MODEL_DIR=/path/to/pinned/small \
TERMITE_REQUIRE_CUDA_TESTS=1 zig build --build-file zig/pkg/inference/build.zig test \
  -Doptimize=ReleaseFast -Dcuda=true -Dmetal=false -Donnx=false -Dpjrt=false -- 'job CUDA published small heads'

python -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p test_cuda_contract.py
```

The external projected-attention fixture is reused unchanged when
`ANTFLY_GLINER25_TRAINING_ATTENTION_FIXTURE_DIR` is set. Run the test filter
`pinned source CUDA` with required CUDA enabled; missing evidence then fails.
The checked-in tests also compare forward and all five attention VJPs with the
CPU reference for head widths 64/128/256, sequence lengths 1/17/65, dropout,
ragged masks and fully masked rows. Training tests reuse full/head jobs,
LoRA/DoRA accumulation, cancellation, recomputation and durable resume cases.

The published-checkpoint tests are separate from the model-free hardware gate:
`ANTFLY_GLINER25_SMALL_MODEL_DIR` enables the `multi window CUDA` regression,
which compares seven learned three-window task cases with native execution and
checks cancellation, output-limit failure and retry using real session owners.
`ANTFLY_GLINER25_TRAINING_JOB_CUDA_FULL_MODEL_DIR` enables the
`job CUDA published full` test. It reuses the heads-job fixture and checks two
full optimizer updates plus exact partial resume/export, with larger explicit
memory ceilings. Allow several GiB of temporary checkpoint disk space.

Training job JSON accepts `"execution": "resident_cuda"`. All existing training
modes and export/checkpoint machinery are shared. CUDA snapshots copy logical
tensor bytes independently of allocation-pool capacity; reshape retains immutable
storage. Strict resident AdamW clears the new epoch's accumulation buffers while
preserving the legacy CUDA optimizer caller contract.

## Training comparison

`benchmark_training_cuda.py` compares the production resident CUDA trainer with
the pinned Fastino `ExtractorTrainer` on CUDA, including its fused AdamW. Build
the diagnostic worker with the same backend flags:

```sh
zig build --build-file zig/pkg/inference/build.zig bench-gliner25-cuda-training-build \
  -Doptimize=ReleaseFast -Dcuda=true -Dmetal=false -Donnx=false -Dpjrt=false

python zig/pkg/inference/scripts/gliner25/benchmark_training_cuda.py \
  --native-bin zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-cuda-training-bench \
  --model-dir /path/to/models/small --upstream /path/to/pinned/GLiNER2 \
  --mode heads --batch-size 2 --python-profile eager_fp32 \
  --warmup 3 --pairs 10 --snapshot-root /path/to/temporary/space \
  --output /new/training/evidence
```

Use the pinned CUDA Python environment described below. The initial scope is
published small, `heads` or `full`, microbatch 1, 2, 4 or 8, accumulation 2, strict FP32
without TF32. `--validate-only` checks two microbatches and the first update.
`--python-profile compile_fp32` is a separate optional baseline.
`--adam-epsilon` sets the same explicitly recorded optimizer epsilon in both
arms. Its default remains `1e-8`; `--adam-epsilon 1e-6` is a separate numerical
stability experiment, not a tolerance adjustment or an automatic production
default. Changing epsilon can change learning behavior and needs quality
validation for the actual training task.

The current CUDA training arithmetic identity is `cuda_f32_training_v36_clipping`.
Resume deliberately rejects checkpoints from incompatible earlier CUDA arithmetic
contracts; CPU/Metal contracts are unchanged. CUDA full/heads training uses the
pinned FP32 clipping norm, authoritative parameter registration order, and
strict FP32 clipping-coefficient arithmetic. CUDA LoRA/DoRA retain their existing
clipping profile pending separate adapter-order qualification. See the
[training arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md) for implementation
and exact evidence; the older [loss investigation](LOSS_PARITY_FOLLOWUP.md)
records the earlier failures and rounding-sensitivity controls.

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
[the arithmetic follow-up](TRAINING_ARITHMETIC_FOLLOWUP.md) records confidence
intervals, binary identities and remaining qualification work.

The wider heads-training matrix found that cuBLAS runtime selection matters:
128x768 weight-gradient products at larger batches differ between native cuBLAS
13.4 and the pinned Python runtime's 12.8 implementation. Matching 12.8 in
controlled runs closes the demonstrated base/B8 drift and passes 100 updates
for base and multilingual/B8 at approximately 1.50x paired throughput. These
controls are not yet integrated production qualification.

V41 adds explicit resident-training selection via
`ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY=/absolute/path/to/libcublas.so`
or benchmark `--cublas-library /absolute/path/to/libcublas.so`. It fails on an
invalid configured library instead of falling back. Ordinary serving-library
selection is unchanged. The loaded cuBLAS version now contributes to the CUDA
trainer checkpoint fingerprint and is reported by the benchmark, alongside
Python's version. Older CUDA trainer fingerprints without runtime identity are
incompatible; portable model weights retain their existing format. V41 build,
resume and sustained integration qualification are still pending.

For the multilingual/B8 synthetic fixture, pass
`--encoder-forward-bytes 8589934592`: its conservative logical-forward estimate
exceeds the default 4 GiB even though a verified first step peaks at 1.74 GB of
tracked device allocations. This option changes only the logical encoder-work
allowance. The physical CUDA cap, host admission and benchmark process guard
remain in force, and effective resource settings are recorded in the report.

Compilation wraps individual Fastino submodules and changes their parameter
paths. The benchmark retains original names by parameter object identity and
checks that registration order is unchanged; it does not strip arbitrary name
components. Snapshot comparison also reports raw FP32 bit mismatches separately
from numerical tolerances, distinguishing signed-zero differences without
changing acceptance. Earlier zero-error reports did not record that distinction.

The driver reuses `training_job_small_v1/train.jsonl`, including entity,
classification, natural record and relation supervision. It repeats the two
examples to form complete accumulation windows and records the derived dataset
hash. This short synthetic workload measures execution and numerical agreement;
it does not measure learning quality on unseen data or long-document scaling.

Both arms disable dropout and schema augmentation, preserve example order, and
inject all gold spans. These deterministic overrides avoid assuming identical
cross-framework random generators. Removing native classifier dropout changes
its Sequential index: the benchmark aliases `classifier.3` to `classifier.2` in
the derived in-memory parameter inventory while retaining canonical names and
unchanged tensor values in comparison receipts. Published source files and the
production training implementation are unchanged by this override.

Validation compares actual token IDs, every reported loss component, gradient
presence, optimizer counters, and complete selected parameter tensors: weights,
accumulated gradients, Adam first moments and second moments. Initial and
post-microbatch snapshots are outside timing. A full campaign also compares the
final state. Tolerances are declared in the driver before measurement; failures
retain per-tensor errors, worst coordinates and associated Adam state, and raw
step responses. A failed correctness gate does
not produce a successful speed comparison. The explicit
`--diagnostic-throughput` option continues numerical comparisons and timing after
a mismatch, while retaining `parity_validated: false`, failed tensor/loss checks,
and `diagnostic_only_parity_failed` on the comparison. It does not relax any
tolerance; source/protocol, CUDA execution and supervision failures still stop
the run. Use these provisional timings to investigate performance, not as a
claim of equivalent training.

Each timed observation covers two complete microbatches and one optimizer update:
batch/schema preparation, transfers, forward, supervised losses and matching,
backward, accumulation, clipping, AdamW, temporary cleanup and CUDA synchronization.
Loading, initial compilation, snapshots, JSON and checkpoint I/O are excluded.
The supervisor alternates arm order and reports paired latency intervals and
examples/second. Workers run sequentially on one GPU and are checked for cleanup.
Full snapshots need about 2.4 GB of temporary space for both arms; RAM-backed
storage is suitable if sufficient host memory remains. Evidence is diagnostic
and does not change production qualification.

```sh
python -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p test_training_cuda_contract.py
```

See [the optimization investigation](TRAINING_OPTIMIZATION.md) for the subsequent
kernel changes, current measurements, numerical diagnosis and validation.

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

## Inference comparison

Use Python 3.12.3 and `requirements-cuda.txt` in an isolated environment. The
frozen oracle still verifies its original exact package versions and source
commit. PyPI torch 2.9.1 supplies a CUDA 12.8 runtime on Linux. Install the pinned
Fastino checkout at `3c913c7369301133d3b7699252074c4303ada50e` and the exact model
revisions from `oracle_manifest.json`. Each model directory must contain only
its five declared files; move downloader metadata outside it.

```sh
python zig/pkg/inference/scripts/gliner25/benchmark_cuda.py run \
  --native-bin zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-cuda-bench \
  --model all --model-root /path/to/models --upstream /path/to/pinned/GLiNER2 \
  --python-profile eager_fp32 --warmup 5 --pairs 30 --output /new/evidence/dir
```

Repeat in separate fresh output directories with `compile_fp32`,
`eager_amp_bf16`, `compile_amp_bf16`, `eager_amp_fp16`, and `compile_amp_fp16`.
Use `--validate-only` to screen candidates first; these reports contain no
warmup or measured pairs and cannot establish performance. Run timing campaigns
on an otherwise idle device after compilation and other validation have ended.
Compiled profiles use the upstream
model's own compilation method. Dynamic shapes are the default; add
`--compile-static` to screen fixed-shape compilation as a separate candidate,
including when an optional encoder cannot compile with symbolic dimensions.
Compilation during validation is excluded from
statistics; `--compile-timeout` bounds validation and warmup (including a possible
recompile after removing validation hooks). FP32 keeps exact decisions and 5e-4
confidence tolerance. BF16/FP16 candidates keep exact decisions with 5e-3 confidence
tolerance; they do not weaken the Zig FP32 or frozen oracle checks.

Install `requirements-cuda-flashdeberta.txt` and add `--flashdeberta` to evaluate
the optional [FlashDeBERTa](https://github.com/Knowledgator/FlashDeBERTa) candidate.
It uses the pinned Fastino loader's explicit opt-in. The worker verifies the
actual encoder class and rejects upstream's silent fallback. Missing packages,
unsupported operators and parity failures reject the candidate. They must not
be reported as successful FlashDeBERTa timings.
The strict worker sets both PyTorch's TF32 controls and
`TRITON_F32_DEFAULT=ieee` before loading or compiling kernels. The former alone
does not constrain the optional encoder's custom Triton dot products.

Both workers synchronize before the clock and after extraction/temporary cleanup.
Model loading, compilation, protocol transport and returned output destruction
are excluded. The harness verifies token IDs and outputs before warmup and checks
every measured output, alternates paired order, saves raw pairs, bootstrap
intervals, runtime provenance and evidence hashes. Cases are the existing ten
frozen task/Unicode cases. These are diagnostic fixtures, not a real-data quality
or production-serving qualification suite.

`benchmark_cpu.py` remains byte-for-byte unchanged because the existing trained
export/merge contracts pin that helper. The CUDA driver imports its fixture
adaptation, schema execution, canonical output comparison and paired statistics;
it reuses the Metal process supervisor and Python device worker. The CUDA mixed
precision wrapper changes only the confidence bound, leaving the frozen exact
decision/ordering/coordinate comparator in control.

CUDA evidence includes per-case validation responses (also for rejected
candidates), readiness receipts, process-tree cleanup, and per-sample upload,
readback and launch counts. Expected detached host decoding remains visible;
generic host tensor materialization inside the native timed request fails.
Compiled CUDA candidates allow at most 1,024 tracked process identities per
worker over its lifetime, covering short-lived compiler subprocesses. The
shared supervisor's default remains 64; live RSS, deadlines and complete
process-tree cleanup are still enforced for every candidate.

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

The published full-model fixture uses a 5 GiB host ceiling for atomic restore
and releases its completed reference output before creating the paused copy.
This keeps temporary checkpoint storage from unnecessarily competing with the
restore's current state, staged state and immutable checkpoint bytes. Normal
live-memory admission remains enabled.

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

## Batched CUDA comparison

The CUDA driver accepts `--batch-size 1..64`. Values above one repeat a frozen
extract request and its schema into one true device batch. Python uses the
pinned upstream `batch_extract(..., batch_size=B, num_workers=0)` API; Zig uses
one `processor.prepare` and device request invocation. Validation observes one
Python encoder execution, checks its actual `[B, sequence_length]` shape against
Zig, and compares every token and every document output with the frozen oracle.
The native benchmark explicitly admits up to 64 samples while retaining the
existing byte/workspace caps. Production defaults are unchanged.

```sh
python zig/pkg/inference/scripts/gliner25/benchmark_cuda.py run \
  --native-bin zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-cuda-bench \
  --model all --model-root /path/to/pinned-models --upstream /path/to/pinned-upstream \
  --python-profile compile_fp32 --batch-size 64 \
  --cases mixed_tasks record_natural --warmup 5 --pairs 30 \
  --output /path/to/new-batch64-report
```

Reports include batch latency distributions, documents/second, paired confidence
intervals, actual encoder shapes and per-measurement native transfer/work counters.
Loading, compilation and JSON serialization remain outside the clock. Schema
compilation, preprocessing, the complete encoder/head/decode path, synchronization
and temporary cleanup remain inside. These homogeneous short batches measure
batch scaling; they do not establish mixed-length or long-document throughput.
The L4 batch-64 native validation passed all eight extraction fixtures on all
three published models (1,536 checked document outputs), including Unicode
offsets, entity attributes, legacy structures, all three record modes and enum
fields. Constrained-classification and JointIE adapters are outside this batch driver;
the existing ten-case single-document campaign continues to cover them.

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

## Release gates still required

The implementation does not populate the intentionally empty production
qualification table. CUDA `reference_v1` is supported; Metal's `optimized_v2`
retained command scopes, immutable derived-weight cache and workspace admission
remain unavailable on CUDA and fail explicitly.

CUDA training now enforces an additional physical `DeviceBuffer` ceiling of
`memory.backend_bytes - memory.backend_metadata_bytes`, starting during backend
initialization. Cached and deferred-free buffers remain charged until their
CUDA free succeeds. On a cache miss that exceeds this ceiling, idle buffers
are reclaimed before retrying; live tensors cannot be evicted. Environment
cache settings cannot widen the owner's ceiling. The existing shared logical
admission and process-wide job reservations still apply.

The training worker reports current, peak reserved, and maximum managed device
bytes in `cuda_allocations`. These counters include application-managed
workspaces, but exclude module storage and driver/library-private allocations.
Deployment qualification still needs headroom for those allocations and other
GPU users; this is not a cap on the driver's total memory usage.

### Longer training and held-out checks

The training comparison accepts `--model small|base|multi` and
`--qualification-updates 0..512`. Extra updates are outside the throughput
sample set, check every component loss, and compare complete tensor state every
ten updates and at the end. The existing weight/gradient/moment thresholds
remain unchanged. For example, one initial update, two measured updates, and
97 extra updates produce a 100-update comparison:

```sh
python scripts/gliner25/benchmark_training_cuda.py \
  --native-bin zig-out/bin/antfly-inference-gliner25-cuda-training-bench \
  --model small --model-dir /path/to/pinned/small \
  --upstream /path/to/pinned/gliner2 \
  --output /path/to/new/report --snapshot-root /path/to/snapshot/scratch \
  --mode full --batch-size 2 --adam-epsilon 1e-6 \
  --pairs 2 --warmup 0 --qualification-updates 97 --evaluate
```

`--evaluate` checks extraction on the existing two annotated validation examples
at initialization, after the first update, and at the end. Both learned weight
sets run through the same pinned Python CUDA evaluator; this isolates training
quality from native inference parity. Python weights are restored after the
comparison, and optimizer state is unchanged. Reports retain exact extraction
decisions, confidence comparisons, per-task gold F1 and regressions from the
initial model. Matching implementations can still both regress in quality.
These two authored examples are a plumbing check, not production quality data.

Supply `--training-file /path/to/train.jsonl --validation-file /path/to/heldout.jsonl`
to reuse the same checks with external annotated data. Both files are required;
each is bounded to two MiB and 256 examples. Their hashes and counts are recorded.
Duplicate IDs and normalized exact train/validation text overlap are rejected.
The existing unique-surface fixture adapter still applies; ambiguous occurrences
are rejected rather than silently changing gold annotations. This overlap check
does not establish absence of near duplicates or pretraining contamination.
The caller must choose a representative split and extraction-quality target.

Use `--diagnostic-throughput` only to retain results after parity failures;
failures stay recorded and neither this option nor a passing comparison adds a
production qualification entry. Larger models also need sufficient host/device
and snapshot scratch capacity under the declared resource ceilings.

Complete the release campaign on L4/SM89, with Ampere/Hopper correctness coverage:

- Use a separately frozen real-data matrix covering all three models, task
  families, batches and sequence/word/query buckets. Reuse GLiNER2 release data
  and add GLiNER2.5 records/classification/JointIE cases.
- Measure full/head/LoRA/DoRA training against matched Python CUDA jobs, including
  accumulation, clipping, dropout policy, checkpoint resume and validation
  quality. Keep detached loss/matching transfers visible in receipts.
- Select the fastest quality-qualified Python candidate per mandatory row using
  separate selection and measurement samples; include FlashDeBERTa when the
  pinned model/runtime supports it. Record unsupported candidates explicitly.
- Require at least 1.20x geometric-mean speedup separately for inference and
  training, confidence-interval lower bound above 1, and no mandatory row slower
  by more than 5%. Require real-data quality within 0.5 percentage points.
- Profile before adding BF16/FP16 Zig training, fused epilogues, cuBLASLt shape
  tuning, attention specializations, CUDA graphs or persistent workspaces. Each
  path needs its own parity, memory/cancellation and benchmark evidence.

The diagnostic harness always reports `performance_release_qualified: false`;
passing fixture timings alone cannot satisfy these release gates.


For record-specific training diagnostics, `diagnose_training_cuda.py --records`
captures the live record logits and detached matching metadata in `trace-N.json`.
It retains the existing trace/response budgets and compacts the borrowed input
JSON. This flag does not replace gradients or participate in timed training.
