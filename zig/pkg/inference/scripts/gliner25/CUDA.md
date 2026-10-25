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
  -Doptimize=fast -Dcuda=true -Dmetal=false -Dpjrt=false
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
using self-contained CUDA kernels. Its ordinary tape
value saves output and log-sum-exp; bounded temporary storage covers padded
bias, delta and backward accumulation. It requires only the CUDA driver at
runtime. Compute capability 8.x uses compiled SM89 or SM80 images; other
architectures, head dimensions and nonzero probability dropout retain the
existing materialized path. Both images have been checked on L4; that does not
qualify other GPUs or sustained full-model training.

Regeneration requires only CUDA 13.2.78 and `nvcc`:

```sh
python3 zig/pkg/inference/scripts/regen-cuda-boundary-attention.py --check \
  --cuda /usr/local/cuda-13.2
python3 zig/pkg/inference/scripts/test_regen_cuda_boundary_attention.py
```

Use `--write` to publish regenerated artifacts. The generator serializes a
private workspace so NVCC
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
  -Dcuda=true -Dmetal=false -Dpjrt=false

ANTFLY_GLINER25_TRAINING_JOB_CUDA_MODEL_DIR=/path/to/pinned/small \
TERMITE_REQUIRE_CUDA_TESTS=1 zig build --build-file zig/pkg/inference/build.zig test \
  -Doptimize=fast -Dcuda=true -Dmetal=false -Dpjrt=false -- 'job CUDA published small heads'

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
  -Doptimize=fast -Dcuda=true -Dmetal=false -Dpjrt=false

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

The current CUDA training arithmetic identity is `cuda_f32_training_v41_blas_identity`.
Resume deliberately rejects checkpoints from incompatible earlier CUDA arithmetic
contracts; CPU/Metal contracts are unchanged. CUDA full/heads training uses the
pinned FP32 clipping norm, authoritative parameter registration order, and
strict FP32 clipping-coefficient arithmetic. CUDA LoRA/DoRA retain their existing
clipping profile pending separate adapter-order qualification. The older
[loss investigation](LOSS_PARITY_FOLLOWUP.md) records earlier failures and
rounding-sensitivity controls; detailed campaign evidence is kept outside the
source tree with the benchmark reports.

Current reference results (published small model, eager Python CUDA, accumulation
two, default epsilon `1e-8`, 100-update comparisons with raw-bit identical
weights and Adam moments at all 48 sampled states): paired native speedups of
3.62x (heads/B2), 1.80x (heads/B8), 2.03x (full/B2), and 1.17x (full/B8). These
use the short synthetic fixtures and do not establish learning quality. The
compiled Python candidate fails the 100-update equivalence gate, so its
timings stay diagnostic. Resident training uses a 2 GiB idle-cache default
(physical allocation limits and pressure eviction still apply); other CUDA
owners keep their 1 GiB default.

> **Relocated:** the v36, v38, and v39 per-version measurements → [CUDA L4 campaigns](../../../../../docs/design/inference/history/gliner2/gliner25-cuda-l4-campaigns-2026-09.md#training-comparison-v36v39-trainer-results).

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

The benchmark harness and external reports contain the current measurements,
numerical diagnosis and validation details.

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

The published full-model fixture uses a 5 GiB host ceiling for atomic restore
and releases its completed reference output before creating the paused copy.
This keeps temporary checkpoint storage from unnecessarily competing with the
restore's current state, staged state and immutable checkpoint bytes. Normal
live-memory admission remains enabled.

## Diagnostic inference results

Against the fastest measured Python reference (compiled FP32, dynamic shapes),
the geometric-mean per-case speedups on the ten frozen requests were 8.80x
(small), 4.52x (base), and 4.24x (multi); the slowest single case was 2.31x
(multi). Compiled autocast FP16 was rejected for base and multi because it
changed record confidences and record counts. These short requests do not
establish large-batch throughput, training speedup, or real-data quality,
and the reports remain unqualified for release.

> **Relocated:** the 2026-09-14 L4 validation run and the diagnostic inference campaign → [CUDA L4 campaigns](../../../../../docs/design/inference/history/gliner2/gliner25-cuda-l4-campaigns-2026-09.md#validation-on-l4).

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

At batch 64 with strict FP32 against dynamic `torch.compile`, paired speedups
were 9.23x/4.48x/3.83x (small/base/multi) on mixed extraction and
20.30x/10.91x/10.21x on structured records; all twelve model/batch
combinations passed output parity. These cover homogeneous short fixtures
only.

> **Relocated:** the full batch 1/8/32/64 tables → [CUDA L4 campaigns](../../../../../docs/design/inference/history/gliner2/gliner25-cuda-l4-campaigns-2026-09.md#l4-measured-batch-results).

## Release gates still required

CUDA serving now admits the exact FP32 `fastino/gliner2.5-base-v1` original
checkpoint (`72ac19b486cd4557424c8d61114e7530c243e9b0`) and Transformers wrapper
(`54785d51df8d86a0d8f2eb212fec15871aad2c4d`). The short single-window task profile
and bounded composition profile are separate. Composition retains the existing
1024-token encoder ceiling and planner limits (one request item, at most 64
windows); it does not inherit the older native/Metal 5708-token long-document row.
Exact file identities, features and geometry remain mandatory on every request.
Reduced precision and multilingual CUDA serving are not admitted by these rows.

On 2026-10-25 the GB10/SM121 in Glowworm (driver 580.178.04) passed the ten frozen
Python task cases for both artifacts at the unchanged FP32 confidence tolerance
5e-4. The bounded window capture also passed: one-character input, short input,
three-window relationship/classification composition, and a complete 1024-token
mixed prompt. These are correctness checks, not L4 throughput measurements.
The managed CUDA regression also checks plain entity/relationship extraction
without descriptions or classification over a longer input. Window preparation
derives its token ceiling from the exact artifact/backend qualification profiles
and shrinks word windows before admission; the production gate still validates
every prepared window and the complete request geometry.
The managed regression uses `createCudaSession`, a process watchdog, and the
production qualification `Gate` before `executeQualified`.

To reproduce from the repo root, with the immutable checkpoint directories set:

```sh
ANTFLY_GLINER25_BASE_MODEL_DIR=/absolute/path/to/original \
ANTFLY_GLINER25_BASE_TRANSFORMERS_MODEL_DIR=/absolute/path/to/wrapper \
ANTFLY_GLINER_WINDOW_MODEL_DIR=/absolute/path/to/wrapper \
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  --build-file zig/pkg/inference/build.zig test-inference-unit \
  -Doptimize=ReleaseFast -Dcuda=true -Dmetal=false -Dcuda-artifacts=fatbin \
  -Dtest-filter='gliner boundary device CUDA pinned' \
  -Dtest-filter='GLiNER bounded window composition pinned CUDA' \
  -Dtest-filter='CUDA boundary base profiles' --cache-dir .zig-cache/inference
```

A published image must include these profiles; RC.2 and the earlier RC.3 source
still reject this serving path. Qualify the image on the actual hosted L4,
including mixed-model capacity and cold starts, before a large ingestion.
CUDA `reference_v1` is supported; Metal's `optimized_v2` retained command scopes,
immutable derived-weight cache and workspace admission remain unavailable on CUDA
and fail explicitly.

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
