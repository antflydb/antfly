# OCR kernel and reader qualification, M4 Max

Measurements use a shared 36 GiB Apple M4 Max. Our GPU workloads run sequentially;
these are order-of-magnitude comparisons, not clean-machine or universal parity claims.
The tables below describe the final integrated runtime. Historical and rejected
candidate results remain available, identified by their own source hashes.

## Integrated changes

- Decoder projections share activation loads across output columns. Small row groups
  limit register pressure; exact tail kernels avoid padded work where qualified.
- Narrow recurrent gates distribute K across eight or sixteen SIMDgroups and reduce
  inside the threadgroup. Each output column is an independent group.
- Qualified gate/up and down projections reduce split-K partials inside one dispatch,
  avoiding global partial buffers and a second reduction dispatch.
- Recurrent prefill gates use a dedicated narrow-output projection; the 0.8B recurrent
  output projection uses the qualified 8×32 TensorOps tile.
- Vision attention packs Q/K/V once and uses 16-query/128-key TensorOps tiles.
  The 0.8B vision architecture has hidden width 768 and twelve heads; 4B has width
  1024 and sixteen heads. Both are tested separately.

Original BF16 weights and F32 decoder activation storage, accumulation, outputs
and recurrent state are preserved. Vision uses the same half Q/K/V and probability
values as its earlier flash path, with F32 scores, accumulation and outputs.
Nonfinite scores are masked before the softmax maximum. There is no activation
precision shortcut or input-dependent BF16 fast path.

Eligibility checks model/device flags, exact dimensions, offsets, pipeline resources
and buffer bounds. Planned accesses preserve producer ordering. Unsupported cases
keep their previous routes. Decoder changes add no persistent scratch.

Vision packing has at most two buffers with command-buffer owners. Encoded owners
cannot grow; active or submitted owners cannot be recycled before host retirement.
Wait/cancel/standalone completion releases packing storage. Admission charges both
buffers beyond the existing score/FFN maximum, memory snapshots include them, and
pooled backend cleanup runs before the request lease is released.

## Final primitive timings

Thirty synchronized trials compare native and MLX. Three-participant comparisons
rotate all six candidate/production/MLX orders. The numeric input values match,
but native projection activations/output use F32 and MLX uses BF16; normalization
uses F32 on both sides. These are not fully precision-matched comparisons.

| Production sweep | Cases | Native median ≤ MLX | Native median ≤ 1.2× MLX |
|---|---:|---:|---:|
| `shipping_page_shapes_resident.json` | 81 | 30 | 81 |
| `shipping_page_shapes_cold64.json` | 81 | 27 | 79 |
| `shipping_decode_shapes_resident.json` | 146 | 97 | 140 |
| `shipping_decode_shapes_cold64.json` | 146 | 93 | 143 |

`decode-shapes` covers fifteen observed language projection widths at rows 1–8,
gate/up, vocabulary heads and normalization/recurrent prefill: 146 cases.
`page-shapes` covers 81 actual vision, merger and language-prefill dimensions.
The earlier 86-case suite omitted nine frequent decoder widths and substantially
understated the gap. It must not be used as evidence of comprehensive parity.

"Resident" means no explicit cache flush, not measured hardware cache residency.
The other policy flushes 64 MiB before each participant. Raw reports retain mean,
median, extrema and all samples. `shipping_summary.json` lists every final median
exception above 1.2× MLX. Remaining exceptions and short vision geometries prevent
a claim that every kernel has reached parity.

The rollback comparisons use **native = old, production = new**. Representative
cold-policy latency ratios below are new/old, so smaller is faster.

| Decoder operation | Rows | New / old | New / MLX |
|---|---:|---:|---:|
| 1024 → 16 | 1 | 0.725 | 0.869 |
| 2560 → 32 | 1 | 0.514 | 0.843 |
| 1024 → 16 | 4 | 0.291 | 0.619 |
| 2560 → 32 | 4 | 0.173 | 0.596 |
| 1024 → 16 | 8 | 0.180 | 0.767 |
| 2048 → 1024 | 8 | 0.261 | 1.037 |
| 1024 → 2048 | 8 | 0.357 | 0.919 |
| 1024 → 512 | 8 | 0.414 | 1.177 |
| 1024 → 4096 | 8 | 0.314 | 0.945 |
| 2560 → 32 | 8 | 0.129 | 0.785 |
| 4096 → 2560 | 8 | 0.236 | 1.078 |
| 2560 → 4096 | 8 | 0.247 | 1.007 |
| 2560 → 1024 | 8 | 0.249 | 1.075 |

`shipping_prefill_previous_vs_current_*` separately measures the prefill changes.
`shipping_vision_attention.json` measures production packing plus attention,
rather than an unpacked prototype. It checks full outputs and independent F64
CPU samples, for both head counts and seven geometries each. Buffer allocation
CPU time is outside primitive GPU timestamps and inside reader wall time.

## Reader controls

All LightOn native controls use one executable, C8/rows8/window8, continuous
refill and opt-in weight residency. Sixteen mixed pages rotate three synthetic
fixtures, with greedy decoding and a 256-token ceiling. Model loading and own
serial references precede timing. Budgets are 24/28/16/4/4 GiB for total/shared/
backend/KV/scratch. Raw reports include p50/p95 latency and batching counters.

The first bracket holds vision/prefill batching enabled with 100 ms collection
windows while disabling/enabling/disabling all three new kernel families.

| Model | Kernel off / on / off, pages/s | Nearby reference, pages/s |
|---|---:|---:|
| LightOnOCR-3 0.8B | 1.6602 / 1.7500 / 1.5979 | vLLM Metal 2.3158 |
| LightOnOCR-3 4B | 0.4984 / 0.5338 / 0.4602 | vLLM Metal 0.6464 |
| Florence-2 base | 4.1751 | MLX-VLM serial 4.4614 |

The second bracket keeps new kernels enabled and compares default collection
against combined vision/prefill batching, then default collection again.

| Model | Default / combined / default, pages/s |
|---|---:|
| 0.8B | 1.6936 / 1.6656 / 1.5872 |
| 4B | 0.5387 / 0.4786 / 0.5156 |

The combined setting did not improve 0.8B consistently and was slower than both
4B default endpoints. Longer waits remain opt-in; no new throughput default is
promoted from the earlier unbracketed pilots.

These are short shared-machine brackets, not statistical confidence intervals.
Machine load drifts; inspect both endpoints before attributing an isolated change
to a kernel. Instrumented `final_profile` runs use eight timed pages and prove
dispatch reachability; they are excluded from throughput tables.

Native timing includes preprocessing, queueing, inference and decoding; it excludes
HTTP/PDF/indexing. vLLM includes loopback HTTP, with prefix/processor caches disabled
and a unique media UUID per request to prevent encoder-output cache reuse. Installed
vLLM Metal has no Florence adapter, so Florence uses MLX-VLM serial. Native Florence
uses Q4_K weights/F16 encoder mirrors and no-repeat n-gram size three, versus original
FP16 and the reference greedy policy. These comparisons use different precision and generation policies.

Every timed native output must equal its own serial result. `output_validation.json`
also checks exact original native text and markup-stripped reference content. The
three synthetic fixtures test consistency, not real-document OCR accuracy.

## Throughput configuration and rollback

Defaults retain short waits. For throughput experiments, add these to the existing
eight-row/eight-page and weight-residency configuration:

```bash
export ANTFLY_LIGHTONOCR3_VISION_BATCH=1
export ANTFLY_LIGHTONOCR3_PREFILL_BATCH=1
export ANTFLY_LIGHTONOCR3_VISION_WAIT_US=100000
export ANTFLY_LIGHTONOCR3_PREFILL_WAIT_US=100000
```

Longer collection waits increase request latency. Benchmark representative page
geometries and admission budgets before adopting them. The CLI equivalents are
`--vision-batch --prefill-batch --vision-wait-us 100000 --prefill-wait-us 100000`.

Kernel rollback switches, set to `1`:

- `TERMITE_METAL_DISABLE_LIGHTONOCR3_LOCAL_KERNELS` — decoder projections/gate-up/down.
- `TERMITE_METAL_DISABLE_LIGHTONOCR3_VISION_MPP` — packed vision attention.
- `TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PROJECTIONS` — narrow/post prefill projections.

Existing individual rollback and precision switches remain effective. Diagnostic
runner options `--require-decode-projections`, `--require-prefill-gates`,
`--require-vision-mpp` and `--require-small-tail-fusion` verify dispatches, including
warmup. Tracing changes timing.

## Build, checks and evidence

`shipping_build_manifest.json` binds all nine runtime source files, the source diff
against `58d7e441`, and the release executable. `final_runtime.patch.gz` reconstructs
that runtime from the base; `runtime_snapshots/` retains exact Metal source versions.
Use Zig 0.17 with ReleaseFast and stripping; source and binary hashes are authoritative.

The final filtered run has 73 CPU passes and six GPU-dependent skips. With Metal,
all 79 selected tests pass, plus the separate real-checkpoint/HTTP default-policy test.
Both standalone C conformance suites pass. `shipping_conformance_manifest.json` binds
their sources and binaries. Coverage includes full outputs, independent CPU samples,
nonzero offsets/canaries, planned producers, missing pipelines, resource fallbacks,
tail cohorts, overlapping command buffers, cancellation and workspace cleanup.

Historical decoder-only builds are identified by `local_build_manifest.json` and
`decoder_runtime.patch.gz`. `actual_*` is the earlier finite-workload integration;
its primary build is `actual_build_manifest.json`. Other reports bind their own
runtime/bridge/runner/observer hashes. Hypothetical 3072→2560 merger experiments
are not the actual 4B architecture and were not promoted. Candidates and probes
must not be substituted for final production measurements.

`qualified_candidate_sources/manifest.json` archives exact runner/bridge versions.
`qualified_test_sources/` retains conformance sources. `prototype_sources/` archives
development scripts and rejected shaders. Restore archived original paths to match
report fingerprints. Raw logs and historical/candidate timing reports use deterministic gzip.
`compressed_evidence.json` records original paths and original/archive hashes.
Decompress to those original paths with `gzip -dk` when following raw log names
or reproducing historical scripts. Current `shipping_*.json` timing reports,
summaries and identity manifests remain directly readable.
