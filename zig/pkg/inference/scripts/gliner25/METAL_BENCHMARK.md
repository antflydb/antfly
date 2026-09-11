# GLiNER2.5 Metal comparison

This benchmark compares Antfly Metal with the pinned Fastino implementation on
PyTorch MPS and, separately, Python CPU. It uses the original small, base, and
multilingual FP32 checkpoints, the ten existing task requests, batch size one,
and one CPU math thread per process. These short correctness fixtures are not
a representative throughput corpus or a serving performance qualification.

## Recorded comparison

The complete 2026-09-10 AC campaign on an Apple M4 passed all 60 comparisons
across three fresh-process repetitions. A separate verification pass checked
13,080 inference outputs, recomputed all 180 per-repetition distributions and
confidence intervals, and verified source, binary, evidence, and cleanup
receipts. The ReleaseFast Metal build and 61 focused benchmark tests passed.

Against Python MPS, Metal was repeatably 2.04–3.62 times faster on the latent
and anchorless record cases across the three models (6 of 30 comparisons).
Python MPS was repeatably 1.29–6.66 times faster on the other 24 comparisons.
The separate Python CPU comparisons favored Python in all 30 cases, by
5.57–11.16 times. These factors use the paired estimator described below.

The [complete recorded tables and provenance](METAL_BENCHMARK_RESULTS.md)
retain every comparison and all three confidence intervals. These results
describe the ten short batch-one fixtures on this machine. They establish a
fair direct-core comparison and identify performance gaps; they do not
establish serving throughput or qualify a release.

## Measurement contract

Each measured request includes schema parsing and compilation, preprocessing,
input transfers, encoder and heads, host decoding, and temporary request
cleanup. Model loading, worker startup, validation hooks, protocol parsing,
response serialization, comparison, and returned-result destruction are
excluded.

Antfly keeps its model session, tokenizer, and shared Metal provider resident.
Its managed request wrapper and strict device weights retain their production
request lifetime: weight materialization, uploads, device work, readback, and
request teardown remain inside the clock. The strict request runs unframed;
its dispatch and readback path completes synchronously. GPU proof comes from
actual strict-device dispatch statistics rather than explicit-frame counters.
The arm is labelled **Antfly Metal, FP32 dtype, production default math**.
Eligible matrix multiplications can use Apple MPSMatrix; FP32 dtype is not a
claim of identical compiler math policy between the implementations.

Fastino loads the same local checkpoint once onto its selected device.
Compilation, quantization and FlashDeBERTa are disabled. The existing pinned
deterministic FP32 profile is retained. MPS synchronizes immediately before
the start clock and again after extraction, before the stop clock. Explicit
upstream host decoding is timed. MPS CPU-operator fallback is disabled, and
fallback warnings are rejected independently because Torch 2.9.1 includes
some unconditional fallback registrations. Unsupported operators and parity
failures produce blocked rows, never silent baseline changes.

The native compiler/runtime environment and Python MPS environment are
recorded. Native tuning overrides and MPS allocator/profiling/matmul overrides
are cleared for the declared default profile; fallback and fast math are
explicitly zero. Inherited values of these specific controls are retained in
the report. Unrelated environment values are not copied into evidence.

## Run

Build through the repository graph, from the zig directory:

    zig build inference-bench-gliner25-metal-build -Doptimize=ReleaseFast -Dmetal=true -Dcuda=false -Donnx=false -Dpjrt=false -j1

Use the pinned oracle environment and existing model directories. The runner
does not install dependencies or download checkpoints. Run a bounded smoke
from the repository root into a new output directory:

    PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/benchmark_metal.py --native-bin zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-metal-bench --model-root /private/tmp/antfly-gliner25-models --upstream /private/tmp/antfly-gliner25-upstream --model small --baseline both --cases mixed_tasks --repetitions 1 --warmup 1 --pairs 2 --output /private/tmp/gliner25-metal-smoke

For the full campaign:

    PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python zig/pkg/inference/scripts/gliner25/benchmark_metal.py --native-bin zig/pkg/inference/zig-out/bin/antfly-inference-gliner25-metal-bench --model-root /private/tmp/antfly-gliner25-models --upstream /private/tmp/antfly-gliner25-upstream --output /private/tmp/gliner25-metal-all-tasks

Defaults are all three models, both baselines, all ten cases, three fresh
process repetitions, five warmups, and thirty measured pairs per case.
All requested preflight comparisons finish before any measurement campaign.
Each fresh worker pair validates its inputs and outputs again before warmup.
Case/variant order rotates deterministically and arm ordering is balanced
AB/BA. MPS and CPU comparisons use separate worker pairs; only one model
request executes at a time and at most two model workers remain resident.
Power source and low-power mode are recorded around each worker pair. A change
within a measured repetition invalidates that repetition; different power
profiles across repetitions prevent a combined headline estimate. Raw samples
and the profile-change diagnosis remain available.

Every warmup and measured result must match the frozen output decisions,
text, order, spans, records, attributes and relations. Only confidence permits
an absolute difference of 5e-4. Actual encoder tokens must match between arms;
the eight extraction cases also match saved token tensors. Classification and
JointIE reference captures have no saved encoder-token tensor, which is
reported explicitly. Their output references and cross-arm token checks
still apply.

Workers use a 120-second startup deadline, a 30-second native request budget
with a five-second parent-response grace, an 8 GiB combined process-tree RSS
limit, 2048-byte commands, 4 MiB responses, and 64 MiB stderr/evidence limits
per run. The parent supervises Python requests with the same response limit.
Cancellation, startup failure, and deadlines trigger bounded cleanup of owned
workers and descendants. Only the direct child is described as reaped.

RSS is sampled at up to 100 ms intervals, so transient peaks can be missed.
Native owned-buffer statistics, native admission charges, PyTorch tensor
allocations, and PyTorch driver allocations use different accounting.
GPU snapshots are observations outside the request timer, not measured GPU
allocation peaks; they must not be added to RSS on unified-memory hardware.

## Evidence and interpretation

The output directory contains:

- report.json and summary.md: complete requested matrix, per-repetition
  distributions and confidence intervals, failures, identities, and settings.
- source_manifest.json: hashes of repository source/build inputs, including
  relevant dirty and untracked files, plus the current Git HEAD.
- Per-run run.json, events.jsonl and worker stderr logs: raw response timings,
  outputs, device receipts, preflight token IDs, and cleanup/resource evidence.
- The shared benchmark evidence manifest: hashes of output artifacts.

Speedup is **Python latency / Metal latency**. Each repetition uses the paired
median log-ratio estimator and a 95% bootstrap interval with 10,000 resamples
and seed 20260730. Headline latencies and speedup are medians of the repetition
estimates. Repetitions remain separate; ninety samples are not presented as
ninety independent host sessions. A repeatable advantage requires at least
three completed repetitions and all intervals above one. All intervals below
one indicate a repeatable Python advantage; other complete results are
inconclusive. Sample p95 remains descriptive.

All raw samples are retained. A parity failure invalidates that case's
comparison, including its earlier measurements. Independent supported cases
may finish, but missing/failed rows keep the overall report partial. No
outlier trimming, result-driven retries, automatic dependency changes, or
relaxed tolerances are performed. Exit code zero means the requested matrix
completed; code two means partial or failed evidence. Serving and performance
release qualification remain false even when every comparison completes.

Run the benchmark contract tests without loading models:

    PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python -m unittest discover -s zig/pkg/inference/scripts/gliner25 -p 'test_*metal*.py' -v

The older CPU benchmark and recorded CPU results are documented separately in
[BENCHMARK.md](BENCHMARK.md).
