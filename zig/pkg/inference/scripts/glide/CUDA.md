# GLiDE CUDA qualification

The CUDA implementation supports the text decision paths of
`fastino/GLiDE-0.4B`, `fastino/GLiDE-0.8B`, and `fastino/GLiDE-2B`.
Decoder kernels require compute capability 8.0 or newer. Vision, loop
checkpoints, and NVFP4 checkpoints are outside this implementation.

## Execution

The 0.4B encoder packs real tokens, applies ModernBERT CUDA attention per row,
and prunes the final output projection and MLP to the readout rows.
The 0.8B and 2B decoders pack
real tokens, preserve bidirectional full attention and causal DeltaNet
recurrence, reset recurrent state between input rows, and prune the final
layer to the readout queries. Projections use F16 weights and operands with
F32 accumulation and activations. Embeddings preserve BF16 storage; readout,
normalization, attention softmax, and recurrent state use F32 arithmetic.

Full attention uses query tiles of at most 256 rows through 3,072 keys and
128 rows for longer contexts, with a bounded 256 MiB workspace. Disjoint ranges and shapes outside that workspace use streaming
attention. DeltaNet rows of at least 256 tokens use 32-token chunks, a
lower-triangular solve, and tensor-core state updates. Decay products do not
divide by cumulative decay, so an exact zero decay remains a valid reset.
The triangular solve retains prior rows in registers. Eight warps split the
chunk scan's products and retain F32 state accumulator fragments between
chunks. Dense attention packs V through a padded shared-memory transpose,
so both its input reads and packed writes are coalesced. K/V storage uses
only the KV heads. Query heads sharing a KV head are flattened into GEMM
rows, preserving head order without repeated K/V copies.
Short rows use the existing recurrent CUDA implementation.
The chunked path reuses consumed QKV projection storage for its workspace
and shares the gate/core/output buffer across ordered kernel stages. This
requires the GLiDE caller to grant input consumption explicitly; live input
and output overlap is rejected before work begins.

Decoder feed-forward layers use at most 4,096 rows of temporary storage.
Gate and up projections share one input conversion, and SiLU/multiply writes
F16 directly at the down-projection input boundary. Residuals remain F32.
For 256–16,384 packed tokens, the four DeltaNet input projections also share
one F16 input conversion; their weight matrices and GEMMs remain separate.
The encoder also tiles its GeGLU projections at 1,024 rows. Idle temporary
buffers are reused for repeated input geometry and retired when geometry
changes, so varied prompt lengths do not accumulate old working sets.

Diagnostic switches, scoped to GLiDE:

- `ANTFLY_GLIDE_CUDA_FP32=1`: F32 weights and attention, exact recurrence.
- `ANTFLY_GLIDE_CUDA_DISABLE_CHUNK_CORE=1`: compare with exact recurrence.
- `ANTFLY_GLIDE_CUDA_NO_DENSE_ATTENTION=1`: exercise streaming attention.
- `ANTFLY_GLIDE_CUDA_DISABLE_ENCODER_PACKING=1`: compare padded encoder execution.
- `ANTFLY_GLIDE_CUDA_DISABLE_TILED_FFN=1`: compare separate feed-forward operations.
- `ANTFLY_GLIDE_CUDA_FFN_TILE_ROWS=1024`: override the decoder FFN tile (1–4,096 rows).
- `ANTFLY_GLIDE_CUDA_DISABLE_SHARED_PROJECTIONS=1`: stage each DeltaNet input projection separately.
- `ANTFLY_GLIDE_CUDA_RETAIN_SHAPES=1`: retain temporary buffers across input geometries.

## Build and check

Run from a repository worktree with Zig 0.17. Keep caches and build output in
that worktree. Replace `sm89` when targeting a different GPU artifact set.

```bash
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
export LD_LIBRARY_PATH="/usr/local/cuda/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
zig build --build-file zig/pkg/inference/build.zig glide-bench \
  -Doptimize=fast -Dstrip=true -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/inference" --prefix "$PWD/zig-out" -j1
zig build --build-file zig/pkg/inference/build.zig test-glide-cuda \
  -Doptimize=fast -Dstrip=true -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/inference" --prefix "$PWD/zig-out" -j1
```

`test-glide-cuda` checks checkpoint qualification, attention against a dense
oracle, malformed tensor/range rejection, deadline handling, recurrence and
row resets, workspace bounds, and loader allocation-failure cleanup. It
requires a working CUDA device; absence of one is a failure for the GPU tests.

The standalone Python checks load the shipped PTX through the CUDA driver:

```bash
python zig/pkg/inference/scripts/glide/check_cuda_attention.py --kernel f32
python zig/pkg/inference/scripts/glide/check_cuda_attention.py --kernel f16
python zig/pkg/inference/scripts/glide/check_cuda_attention.py --kernel f32 --gapped
python zig/pkg/inference/scripts/glide/check_cuda_attention.py --kernel f16 --gapped
python zig/pkg/inference/scripts/glide/check_cuda_dense_attention.py
python zig/pkg/inference/scripts/glide/check_cuda_delta.py
```

Regenerate PTX using `tools/cuda/generate_glide_ptx.py`, NVRTC 12.8, and the
CUDA runtime and NVCC include directories. Both `--kernel glide_attention`
and `--kernel glide_delta`, plus `--kernel glide_activation`, produce compute-80
PTX with a source SHA-256 header.

## Reference and measurements

Use a pinned model revision, including its bundled `glide/` implementation,
tokenizer, chat template, and decision head. Captures used during development:

| Model | Hugging Face revision |
| --- | --- |
| 0.4B | `6bda7ab91aeb36dcad6d27828a8871e548c7fc71` |
| 0.8B | `9eacdeec79fa48ec52a8d6f54b331376b4bbfaf0` |
| 2B | `00f122c120cdeed82284106cb77d9bab5a50ad16` |

Capture FP32 CUDA logits with `capture_reference.py --device cuda --dtype fp32`.
Use a separate environment without FLA and causal-conv1d for this oracle,
which uses the upstream Torch recurrence. Optimized timing baselines
must separately include FLA and causal-conv1d and exercise both BF16 and FP16.
Development used Torch 2.9.1+cu128, Transformers 5.23.0, FLA 0.6.0, and
causal-conv1d 1.7.0 on an NVIDIA L4 with driver 580.159.03.
Nsight inspection confirmed `causal_conv1d_channellast_fwd_kernel`,
`chunk_gated_delta_rule_fwd_kernel_h_blockdim64`, `chunk_fwd_kernel_o`, and
the FLA solve/recomputation kernels in the Python FP16 baseline.

```bash
zig-out/bin/antfly-inference-glide-bench \
  --model-dir .models/GLiDE-0.8B --backend cuda \
  --fixture evidence/reference/fix08_cuda_fp32.json \
  --mode both --budget 8192 --warmup 2 --iters 5 --max-dlogit 0.05 --max-dprob 0.01
python zig/pkg/inference/scripts/glide/bench_cuda_reference.py \
  --model .models/GLiDE-0.8B --dtype bf16 \
  --fixture evidence/reference/fix08_cuda_fp32.json --budget 8192 --warmup 2 --measure 5
```

Both benchmarks measure padding, device transfer, the backbone, readout, and
CPU logits materialization from identical pretokenized prompts. They enforce
finite logits and unchanged winning options; the native runner also accepts
explicit maximum logit and probability differences. `--budget` controls the
padded-token microbatch budget in both runners. Compare equivalent budgets
when evaluating memory tuning.

Wrap each arm with `measure_process.py --out <prefix> -- <command>`. It saves
the child log and samples process RSS/PSS and per-process NVML GPU memory every
20 ms, separating startup and inference. These are sampled process peaks;
Torch allocator peaks are additional metrics, not substitutes for NVML.
The sampler follows the child PID across physical GPUs independently of
`CUDA_VISIBLE_DEVICES`, sums simultaneous usage across devices, and records
observed device UUIDs and names. Missing or unavailable process GPU memory
fails the measurement rather than publishing a zero-byte GPU peak. Attribution
regressions run without NVIDIA hardware or optional monitoring packages:

```sh
python3 -m unittest discover -s zig/pkg/inference/scripts/glide -p test_measure_process.py -v
```
Run fresh processes in alternating order for repeated lifecycles, with no
other GPU work or concurrent builds during final measurements.

For public API validation, build `http-server` with the same CUDA options,
start it with `ANTFLY_INFERENCE_REQUIRED_BACKEND=cuda`, and run:

```bash
python zig/pkg/inference/scripts/glide/check_cuda_http.py \
  --url http://127.0.0.1:18090/ai/v1 --model GLiDE-0.8B \
  --fixture evidence/reference/fix08_cuda_fp32.json
```

The client checks choice, predicate, and expected-level score outputs, then
mixed-length batches in reordered and repeated-input forms. Upstream custom
predicate descriptions map to an equivalent two-option public choice;
ordinary predicates retain the predicate API. HTTP timings include prompt
construction, tokenization, admission, JSON, and transport, and should be
reported separately from the pretokenized benchmark.

## Long-decoder update — 2026-11-14

The optimized V11 binary was compared with the saved V8 binary and fresh
Python FP16/BF16 processes on the same L4. All four arms used the same
8,192-token budget, all 16 prompts individually and as a mixed batch,
two warmups and five measurements per cell. Three fresh lifecycles used
rotated arm order, with no concurrent builds or GPU work. The tables report
medians of process medians; memory is the maximum sampled inference peak.

Long single prompts contain 5,129 tokens:

| Model | Before ms | After ms | Python FP16 ms | Python BF16 ms | Latency reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0.8B | 325.52 | 280.80 | 299.94 | 298.29 | 13.7% |
| 2B | 577.38 | 496.55 | 522.52 | 515.98 | 14.0% |

Both native medians beat Python FP16 and BF16. The 2B native lifecycle
medians ranged from 489.83 to 518.74 ms, overlapping Python BF16's
515.76–517.29 ms range; this is a median improvement, not an every-run win.

Short prompts and the mixed batch were included as regression checks:

| Model | Short before / after ms | Batch before / after ms | Python FP16 batch ms |
| --- | ---: | ---: | ---: |
| 0.8B | 9.63 / 9.73 | 779.40 / 700.00 | 1130.19 |
| 2B | 19.89 / 19.82 | 1398.20 / 1233.30 | 2035.96 |

Memory uses decimal GB, per-process NVML for GPU memory, and host PSS:

| Model | GPU before GB | GPU after GB | Python FP16 GPU GB | Host PSS after / Python GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0.8B | 2.487 | 2.491 | 2.464 | 0.503 / 1.742 |
| 2B | 4.687 | 4.601 | 5.098 | 0.489 / 1.789 |

The 0.8B GPU peak remains about 1.1% above Python; the 2B GPU peak is
about 9.8% lower. Host PSS is about 71–73% lower than Python FP16.
Separate single-lifecycle trials with smaller decoder FFN tiles did not
reduce sampled GPU peaks and were slower, so the default remains 4,096.

Native decisions matched the FP32 oracle in every single and batch cell
in all three lifecycles. Maximum native probability differences were 0.000870 / 0.000237 for 0.8B / 2B.

A separate single-lifecycle 0.4B encoder regression check also passed all
16 single prompts and the batch with zero decision flips. Its CUDA path
is unchanged by these decoder optimizations.

Validation of this update: 24 focused CUDA tests, full-suite NVIDIA memcheck
(zero errors), DeltaNet and transpose racecheck (zero hazards), independent
Torch kernel oracles, and a CUDA-disabled test build. The rebuilt HTTP
server passed all 11 API cases per model, including reordered/repeated
batches. No precision thresholds were relaxed.

The 16,384-token results below belong to the earlier V8 baseline; this
update’s repeated comparison uses 8,192 tokens. Machine-readable results
under `long_prompt_optimization` retain every cell, lifecycle ranges,
memory peaks, and binary/source/input hashes.

## NVIDIA L4 baseline results — 2026-11-14

The tables in this section describe the saved pre-optimization V8 binary.
They retain the starting point for the long-decoder optimization comparison.

The primary comparison uses the same 8,192-token budget in both runners,
16 prompts individually and as one mixed-length batch, two warmups and five
measurements per cell. Each arm ran in three fresh processes with rotated
order and no concurrent builds or GPU work. Latencies below are medians of
process medians. The Python FP16 and BF16 baselines use FLA and causal-conv1d.

| Model | Native batch ms | Python FP16 ms | Python BF16 ms | Speedup vs FP16 |
| --- | ---: | ---: | ---: | ---: |
| 0.4B | 604.40 | 856.06 | 838.75 | 1.42× |
| 0.8B | 803.60 | 1153.87 | 1145.69 | 1.44× |
| 2B | 1419.60 | 2088.30 | 2059.02 | 1.47× |

Representative single prompts show both the short-input gains and the
long-input limit. Short prompts contain 135 tokens for 0.4B and 106 for the
decoders; long prompts contain 4,869 and 5,129 tokens respectively.

| Model | Native short ms | Python FP16 short ms | Native long ms | Python FP16 long ms |
| --- | ---: | ---: | ---: | ---: |
| 0.4B | 5.75 | 25.34 | 247.25 | 247.92 |
| 0.8B | 9.59 | 58.40 | 332.99 | 303.12 |
| 2B | 20.19 | 59.08 | 579.03 | 540.51 |

In this V8 baseline, long decoder singles were about 7–10% slower than Python FP16.
The 0.4B long case is approximately tied with FP16; BF16 is slightly faster.

Inference memory is the maximum sampled process peak across the three
lifecycles, including warmup. GB below means decimal gigabytes. Host memory
uses PSS; GPU memory uses per-process NVML, not Torch allocator counters.

| Model | Native GPU GB | Python FP16 GPU GB | Native host PSS GB | Python FP16 host PSS GB |
| --- | ---: | ---: | ---: | ---: |
| 0.4B | 1.567 | 1.606 | 0.487 | 1.959 |
| 0.8B | 2.487 | 2.464 | 0.516 | 1.721 |
| 2B | 4.687 | 5.098 | 0.502 | 1.777 |

Host PSS is about 70–75% lower. At this smaller token budget, 0.8B uses
23 MB (0.94%) more GPU memory than Python; 0.4B and 2B use less. Python BF16
has the same measured GPU peaks as FP16 in this run.

A separate **single-lifecycle** check used the unchanged default
16,384-token budget, again with two warmups and five measurements per cell:

| Model | Native batch ms | Python FP16 ms | Python BF16 ms | Native GPU GB | Python FP16 GPU GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0.4B | 619.00 | 1229.16 | 1213.33 | 1.833 | 2.082 |
| 0.8B | 819.00 | 1556.27 | 1548.20 | 2.772 | 3.123 |
| 2B | 1469.20 | 2543.47 | 2527.59 | 5.029 | 5.356 |

At the default budget, native batch latency and GPU memory are lower for
all three models. This secondary check has fewer independent processes than
the primary comparison and should be interpreted accordingly.

Native and Python FP16 had zero decision flips in both budget checks.
Python BF16 had zero flips at 8,192 tokens; at 16,384 tokens its 0.8B batch
changed one decision relative to FP32. Native maximum probability differences
against FP32 at 8,192 tokens were 0.000551 / 0.000892 / 0.000342 for
0.4B / 0.8B / 2B, respectively.

Validation completed:

- 22 focused CUDA tests, including buffer consumption, alias rejection, row resets, and tile tails.
- NVIDIA memcheck: zero errors. Racecheck on `glide_*` and `lighton_gated_norm`: zero errors or warnings.
- Independent Torch oracles for the shipped PTX, including excluded attention gaps and zero decay.
- 18 prompt/contract tests: 48 prompt/token comparisons plus two truncation fixtures.
- Strict FP32 short/medium/long and batch checks: maximum option-logit difference at most 0.00002.
- Final HTTP binary: all 11 cases per model passed, including choices, predicates, expected-level scores, and reordered/repeated batches; maximum probability difference below 0.001.
- CUDA-disabled build, formatting, Python lint, and PTX/source hash checks passed.

[Machine-readable results](cuda-l4-2026-11-14.json) include every prompt cell,
lifecycle ranges, startup and inference RSS/PSS/NVML peaks, process-to-loaded
latencies, versions, pinned revisions, and binary/input hashes. Startup times
include each runner’s initialization and vary with the file cache; they are
not cold HTTP-request latency. Memory peaks are sampled every 20 ms.

These results qualify the pinned text checkpoints and this fixture on the L4.
A100 and Metal execution have not been tested as part of this CUDA work.
Vision, loop, and NVFP4 checkpoints remain outside the implementation.
