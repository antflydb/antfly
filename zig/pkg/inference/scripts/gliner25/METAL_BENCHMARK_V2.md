# FP32 comparison and scaling profiles

See [the v2 results](METAL_BENCHMARK_RESULTS_V2.md) for measured results, source/build
identity, independent validation, and the retained scaling limitations.

The additive v2 runner compares Zig Metal with pinned Fastino/PyTorch MPS.
Native CPU preservation is a separate native-CPU/Fastino-CPU campaign. Original
helpers, ten requests, model bundles, tolerances and v1 reports are unchanged.

The fixture milestone is Metal latency within 20% of Fastino MPS on each of
30 model/task combinations. Every one of three independent process repetitions
must have a lower 95% paired Python/Metal ratio of at least 5/6. Five warmups
and 30 interleaved pairs per case remain the accepted profile. CPU preservation
requires an upper 95% native/Python CPU ratio below 1 in every repetition for
every case. Neither criterion qualifies serving load or release readiness.

## Runtime policy

The runner requires --execution-policy reference_v1 or optimized_v2. New workers
use scope gliner25_direct_core_metal_comparison_fp32_v2 for both. An original
v1 binary requires --legacy-reference-bin and reference_v1; it cannot claim
final v2 owner cleanup.

Optimized readiness fixes immutable model bytes after common preparation.
Every response reconciles physical counters with model, workspace and transient
owners. Model bytes stay fixed; transient and pending device bytes are zero at
request boundaries. Workspace capacity is an admission upper bound, separate
from live bytes. Growth is bounded and versioned during validation/warmup;
after all selected cases warm, bytes and generation remain stable. A positive
workspace capacity requires pending and peak-pending workspace counters. Stop
requires zero model, workspace, transient and pending bytes, plus child cleanup.

--purpose diagnostic forwards --diagnostics true only to a new Metal worker,
including that campaign's preflight. Optional phase timings cover the five
measured phases: schema, processor, backend setup, device and backend cleanup.
The device phase includes encoder, task heads, decoding and device cleanup;
their individual timings are unmeasured and remain absent. No extra GPU fences
are introduced. Diagnostic calls produce no acceptance
statistics; instrumented responses are rejected from measured fixture samples.

The timing boundary remains
schema_parse_compile+processor+encoder+heads+decode+temporary_cleanup.
Model loading, protocol JSON, validation hooks and comparison are excluded.
PyTorch MPS synchronizes before the start clock and after full extraction before
the stop clock. Explicit source host decoding is included; silent MPS operator
fallback, including unconditional fallback warnings, is forbidden. MPS allocator
observations are outside the clock.

~~~sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/benchmark_metal_v2.py \
  --native-bin /absolute/path/to/frozen-current-metal-worker \
  --model-root /private/tmp/antfly-gliner25-models \
  --execution-policy optimized_v2 --baseline mps --purpose benchmark \
  --output /private/tmp/gliner25-final-fp32-metal-v2
~~~

Use fresh output directories and freeze source files throughout each campaign.
The runner binds the executable, repository source snapshot, dependencies,
upstream checkout and model files before/after work. Default request/startup
limits are 30/120 seconds, aggregate owned-process RSS is capped at 8 GiB,
events at 4 MiB and journals at 64 MiB. Failed/unprocessed cases stay in the
report. Unsafe device or cleanup failure ends reuse. There is no automatic
numerical retry.

## Native CPU preservation

The saved cpu-refresh-1, cpu-refresh-2 and cpu-refresh-3 reports under
/private/tmp/gliner25-fp32-optimization-baseline-v1 are baseline evidence from
the preserved CPU executable. Their audited 90 case/repetition intervals all
show native CPU ahead of Fastino CPU. They do not prove preservation for later
shared-source changes. Build and freeze a current CPU executable separately:

~~~sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/benchmark_cpu_preservation.py run \
  --native-bin /absolute/path/to/frozen-current-cpu-worker \
  --model-root /private/tmp/antfly-gliner25-models \
  --output /private/tmp/gliner25-final-native-cpu-preservation-v1
~~~

This fixes three repetitions, five warmups, 30 pairs, all three models and ten
original tasks. It uses the original CPU worker/fixtures with bounded process
ownership and rederives every distribution/interval from raw pairs. Final
source preservation additionally needs a build receipt binding that exact
executable to the source snapshot; observing a binary beside source is
insufficient.

## Separate scaling profile

Preparation imports no Torch, reads no model weights, and performs no extraction.
It uses the pinned Python/Unicode/tokenizer package, local tokenizer JSON and
source word splitter. Each original body must reproduce the suffix of its
saved actual encoder IDs before reusing the schema prefix.

Each variant gets 43 cases: mixed tasks plus natural, latent and anchorless
records at inclusive encoded lengths 128/256/512 and batch sizes 1/2/4;
ragged Unicode and JointIE at those widths and batch size four; and batch-eight
mixed-task smoke at width 128. Full original sentences stay byte-identical;
deterministic one-token filler prefixes reach exact widths. Ragged members use
full, three-quarter, half and quarter widths, bounded below by template length.

The native case contains only id, schema, items, expected_encoded_lengths and
encoded_width. Its requests_sha256 binds the raw source file containing original
schema/kind and expected token sequences. The manifest pins all six variant
input files, tokenizers, original request/token evidence and preparation helpers.
Inputs contain no expected extraction decisions or inherited quality claim.

~~~sh
PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/prepare_benchmark_scaling_v1.py \
  --output /private/tmp/gliner25-fp32-scaling-inputs-fresh

PYTHONDONTWRITEBYTECODE=1 /private/tmp/antfly-gliner25-oracle-venv/bin/python \
  zig/pkg/inference/scripts/gliner25/benchmark_scaling_v1.py \
  --prepared /private/tmp/gliner25-fp32-scaling-inputs-fresh \
  --native-bin /absolute/path/to/frozen-current-metal-worker \
  --execution-policy optimized_v2 \
  --output /private/tmp/gliner25-fp32-scaling-comparison-fresh
~~~

Default scaling is one fresh repetition, one warmup and three interleaved pairs
per case, with descriptive distributions only. Three pairs alternate first arm;
they do not claim equal per-case counts or confidence intervals. Batch eight
gets CPU capture and paired correctness/cleanup preflight only. --purpose
validation omits warmups/samples; diagnostic produces separate phase receipts.

New-text outputs first come from actual Fastino CPU batch APIs. All selected
models/cases finish CPU capture and paired Metal/MPS preflight before optional
measurement. Failed CPU cases retain their denominator. Each fresh preflight
observes exactly one encoder call and verifies actual devices, padded IDs/masks
and [B,S] against the manifest. Every warmup/run checks shape, all ordered
outputs, original source coordinates, exact decisions and confidence at 5e-4.
Validation hooks/readbacks are absent from timed calls; token proof comes from
each fresh preflight, not per-sample observation.

Scaling scope is gliner25_fp32_scaling_inputs_v1 and results must carry the full
outputs array; first-output compatibility alone is insufficient. Bounds are
eight items, 512 words, 512 encoded tokens, 64 queries and 65,536 UTF-8 bytes
per text, with the same process/RSS/output guards. No data/schema/threshold
changes follow observed model results. Scaling never votes in the original
30-case latency milestone.

~~~sh
cd zig/pkg/inference/scripts/gliner25
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v \
  test_metal_runtime_v2 test_benchmark_metal_v2 \
  test_benchmark_cpu_preservation test_scaling_contract_v1 \
  test_metal_scaling_python_worker test_benchmark_scaling_v1
~~~
