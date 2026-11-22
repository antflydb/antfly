# Mixed-precision GGUF quantization

Antfly exports GGUF models with one weight format for every eligible tensor
(`--format q4_k`). Tensors are not equally sensitive: the token table, the last
layers and some projections damage the output far more per lost bit than the
rest. Mixed-precision plans give each tensor its own format so a file meets a
size budget with the least measured damage, using only formats Antfly already
runs. No new kernels or file formats are involved: a mixed file is an ordinary
GGUF whose tensors have different types.

The approach follows quantization compilers such as
[LookAtMyQuant](https://github.com/ShinMegamiBoson/lookatmyquant)
([write-up](https://lookatmyquant.pages.dev)): measure how much quantizing each
piece of a model hurts its output, then allocate bits under a budget. That
project also introduces trellis codes and distillation; those are out of scope
here (see [Alternatives](#alternatives-considered)).

## Status

| Piece | State |
|---|---|
| Allocator, plan files, `quantize-plan` command | Implemented, unit tested |
| `export --quantize-sensitivity` (data-free weight error) | Implemented, tested |
| `export --quantize-plan` | Implemented, tested; fails closed |
| Fused-kernel groups and fusion targets | Implemented from dispatch code; device speed not yet measured |
| Embedding cosine sensitivity (Antfly runtime and PyTorch) | Implemented; measured on bge-base-en-v1.5 |
| Token-KL sensitivity for decoders (PyTorch) | Implemented; measured on Qwen3-0.6B (2x lower KL than `q5_k` at its size, untied) |
| Device benchmark (Metal, CUDA) | Script implemented; runs pending |
| Bundle exports (GLiNER, ClipClap, Florence-2) | Not supported; flags are rejected |

## Goals

- Lower output damage than a uniform format at the same file size, or the same
  damage in a smaller file.
- Reuse existing GGUF formats and kernels on every backend.
- Keep per-architecture export policy (norms, biases, split tensors and
  multimodal towers stay dense) and fail closed on any invalid plan.
- Keep plans on the fused kernels Metal and CUDA use for decode.
- Measure damage with the metric each model is used for: embedding cosine and
  retrieval recall for embedders, token KL for decoders, WER for Whisper, span
  F1 for GLiNER.

Non-goals: new weight formats, quantization-aware training, runtime
dequantization changes, and changing which tensors an architecture quantizes.

## Pipeline

```text
antfly inference export <model> --target gguf --format q4_k \
  --quantize-sensitivity weight.json          # 1. candidates, bytes, data-free loss
<output-space measurement>                    # 2. optional: replace each loss
antfly inference quantize-plan --sensitivity kl.json \
  --bits-per-weight 5.5 --fusion-target portable --output plan.json
antfly inference export <model> --target gguf --format q4_k \
  --quantize-plan plan.json --output model.mixed.gguf
```

### Sensitivity file

```json
{"schema": "antfly-quant-sensitivity/v1", "metric": "token_kl",
 "tensors": [
   {"name": "blk.0.attn_q.weight", "elements": 1048576, "candidates": [
     {"format": "q4_k", "bytes": 589824, "loss": 5.2e-4},
     {"format": "q8_0", "bytes": 1114112, "loss": 1.8e-5}]}],
 "groups": [
   {"name": "blk.0.attn_qkv", "kind": "qkv",
    "members": ["blk.0.attn_q.weight", "blk.0.attn_k.weight", "blk.0.attn_v.weight"]}]}
```

`--quantize-sensitivity` lists every tensor the base `--format` quantizes, each
candidate format whose blocks divide its rows, the encoded bytes, and the loss
`weight_sse` (summed squared reconstruction error). Default candidates are
`q4_0,q4_k,q5_k,q6_k,q8_0`, the formats Metal's fused decode block accepts. A
measurement script replaces each `loss` and keeps everything else.

### Plan file

```json
{"schema": "antfly-quant-plan/v1", "metric": "token_kl",
 "budget_bytes": 409739264, "planned_bytes": 409739264, "bits_per_weight": 5.5,
 "total_loss": 0.0812, "tensors": {"blk.0.attn_q.weight": "q4_k", "...": "..."}}
```

Only `schema` and `tensors` are read; the rest is a record.

### Applying a plan

A plan refines the base format. An entry may only re-target a tensor that the
base format already quantizes; `"none"` keeps it dense. The export fails, and
changes nothing, if an entry names a tensor the export does not write, a tensor
the base format keeps dense, an unknown format, or a format whose blocks do not
divide the tensor's rows. Split tensors (`source_byte_range`) are not eligible,
because the quantized writer does not honor byte ranges. A K-family base
(256-value blocks) leaves rows that are not multiples of 256 dense; a 32-value
base such as `q8_0` makes more tensors eligible.

## Allocation

Choosing one candidate per tensor to minimize summed loss under a byte budget is
a multiple-choice knapsack problem. With losses measured one tensor at a time
and treated as additive, it is the classic bit-allocation problem for an
arbitrary set of quantizers (Shoham and Gersho, 1988).

`src/quantize/mixed_plan.zig` runs two solvers and keeps the better plan:

1. Lagrangian / convex hull: walk each tensor's lower convex hull of
   (bytes, loss) and take upgrades in order of loss saved per byte. Every plan
   reached at a hull breakpoint is optimal for its byte total. Leftover budget
   is filled with the best upgrade that fits, including off-hull candidates,
   and pairwise exchanges (upgrade one tensor, downgrade another) repair gaps
   between breakpoints when the instance is small enough.
2. A dynamic program over bytes rounded up to 1/65536 of the budget. Rounding
   up keeps every DP plan within the real budget.

Unit tests compare against exhaustive search (exact on all 200 small random
instances) and an exact byte-level DP (within 0.1% on 40-tensor instances).

Additivity is a first-order assumption. On bge-base-en-v1.5 the sum of
single-tensor cosine losses predicted the held-out loss of every plan within 4%.

## Fused-kernel groups

A mixed file needs no new kernels: each tensor runs its own format's matmul, and
Metal's fused quantized decode block already takes one format per role
(attention output, FFN gate/up, FFN down, PLE). Two fusions constrain formats
*across* tensors:

| Group | Metal | CUDA | `portable` |
|---|---|---|---|
| FFN gate + up | one shared format in {q4_0, q4_k, q5_k, q6_k, q8_0}; otherwise the layer leaves the fused block (`directQuantizedBlockFormatForRequest`, `directQuantizedBlockFormatForKind` in `backends/metal_runtime.zig`) | both q4_0, q4_k or q8_0 fuse (`linearNoBiasPair` in `ops/cuda/cuda_compute.zig`); otherwise two matmuls (`linear_pair_fallbacks`) | one shared format in {q4_0, q4_k, q8_0} |
| Attention Q/K/V | all q4_0, all q4_k, all q8_0, or q5_k/q4_k/q4_k (`tryApplyQuantizedRuntimeLinearQkv`); otherwise three matmuls | all q4_0, q4_k or q8_0 (`linearNoBiasQkv`); otherwise three matmuls | all q4_0, q4_k or q8_0 |

`--quantize-sensitivity` records these as `groups`, found by GGUF name
(`blk.N.ffn_gate*`/`ffn_up*`, including MoE `_exps`, and
`blk.N.attn_q/k/v`). The allocator turns each group into one item whose
candidates are the combinations the chosen `--fusion-target` fuses
(`mixed_plan.fusionRule`), with bytes and losses summed over members.
`portable` (the default) keeps only combinations fused on both runtimes, so
one file is fast on either; `metal` and `cuda` use one runtime's rules;
`--untie qkv` or `--untie gate_up` ignores one kind and `--untied` all.
`--quantize-plan` prints a warning for every group a plan leaves unfused on
Metal or CUDA.

The cost of leaving a fused kernel has not been measured for these exact
fallbacks. For a different fallback off the fused frame (non-greedy sampling
before it was wired in), `zig/pkg/inference/models/gemma4/GEMMA4.md` recorded
about 5 tok/s against about 52. The device benchmark below measures it.

## Measuring loss

| Metric | Tool | Where it fits |
|---|---|---|
| `weight_sse` | `export --quantize-sensitivity` | Data-free baseline. Weak; see results. |
| `embedding_cosine_loss` | `scripts/quant_plan/embedding_sensitivity.py` (Antfly runtime), `torch_embedding_sensitivity.py` (PyTorch) | Embedders: mean 1 - cos to the dense model; `evaluate` adds neighbor recall. |
| `token_kl` | `scripts/quant_plan/torch_lm_sensitivity.py` | Decoders: mean next-token KL(dense, quantized) and top-1 agreement. |

The PyTorch tools export each candidate format once, uniformly, dequantize the
tensors Antfly wrote with gguf-py, map them to Hugging Face parameters by value,
and swap one tensor at a time into a reference model. The quantized values are
exactly Antfly's; only the forward pass differs. Measurement and evaluation use
disjoint text.

## Results

### bge-base-en-v1.5 (embedder, CPU)

76 planned tensors, base `q4_k`, candidates `q4_0,q4_k,q5_k,q6_k,q8_0`;
calibrated on 64 paragraphs from `docs/`, evaluated on 300 others. Bits per
weight cover the planned tensors; recall is top-10 neighbor overlap with the
dense model.

| Variant | bits/weight | 1 - cos | recall@10 |
|---|---:|---:|---:|
| uniform q4_k | 4.50 | 0.01950 | 0.846 |
| cosine plan | 4.50 | 0.01635 | 0.855 |
| cosine plan | 5.00 | 0.00345 | 0.920 |
| uniform q5_k | 5.50 | 0.00548 | 0.914 |
| `weight_sse` plan | 5.50 | 0.00548 | 0.914 |
| cosine plan | 5.50 | 0.00173 | 0.934 |
| cosine plan | 6.00 | 0.00091 | 0.957 |
| uniform q6_k | 6.56 | 0.00103 | 0.959 |

At the size of uniform `q5_k` the cosine plan cuts loss 3.2x. Through
`antfly inference embed`, the 5.5-bit plan had 3.1x lower loss than `q5_k` on a
four-text spot check. Every mixed GGUF matched its plan tensor for tensor.
BERT tensor names form no fused groups, so encoder plans are unconstrained.
Details: `zig/pkg/inference/scripts/quant_plan/results/bge-base-en-v1.5.json`.

### Qwen3-0.6B (decoder, CPU, PyTorch token KL)

197 planned tensors and 56 groups (28 layers x gate/up and Q/K/V), base `q4_k`,
512 evaluation tokens from `docs/`, disjoint from calibration.

Plans from the data-free `weight_sse` loss at 5.5 bits per weight:

| Variant | bits/weight | token KL | top-1 agreement | split groups |
|---|---:|---:|---:|---:|
| uniform q4_k | 4.50 | 0.3135 | 0.683 | 0 |
| uniform q5_k | 5.50 | 0.0900 | 0.831 | 28 QKV unfused on both; 28 gate/up unfused on CUDA |
| `weight_sse` plan, untied | 5.50 | 0.0903 | 0.827 | 29 on both, 27 more on CUDA |
| `weight_sse` plan, `metal` | 5.50 | 0.0991 | 0.809 | 56 on CUDA |
| `weight_sse` plan, `portable` | 5.50 | 0.1198 | 0.760 | 0 |
| uniform q6_k | 6.56 | 0.0145 | 0.915 | 28 QKV unfused on both; 28 gate/up unfused on CUDA |
| uniform q8_0 | 8.50 | 0.0014 | 0.986 | 0 |

The weight-error proxy does not find better plans for decoders. Plans from
measured token KL (`torch_lm_sensitivity.py measure` on 256 calibration
tokens), on the same 512 evaluation tokens:

| Plan (token KL) | bits/weight | token KL | top-1 agreement | vs uniform q5_k |
|---|---:|---:|---:|---:|
| `portable` | 5.00 | 0.1226 | 0.762 | |
| `metal` | 5.00 | 0.1101 | 0.789 | |
| gate/up tied, Q/K/V free | 5.00 | 0.0901 | 0.791 | equal KL, 9% fewer bytes |
| untied | 5.00 | 0.0915 | 0.803 | equal KL, 9% fewer bytes |
| `portable` | 5.50 | 0.0854 | 0.801 | 5% lower KL |
| `metal` | 5.50 | 0.0727 | 0.852 | 19% lower KL |
| gate/up tied, Q/K/V free | 5.50 | 0.0448 | 0.850 | 2.0x lower KL |
| untied | 5.50 | 0.0426 | 0.850 | 2.1x lower KL |

Measured token KL finds plans with half of `q5_k`'s KL at the same size, but
today's fusions keep most of that gain out of reach: the `portable` plan keeps
only 5% of it. The constraint that costs is Q/K/V, not gate/up. The untied
plan stores V as `q6_k` in 27 of 28 layers with Q and K mostly at `q5_k`,
while the only mixed QKV combination fused today (`q5_k`/`q4_k`/`q4_k`, Metal)
raises Q instead. Tying gate/up and leaving Q/K/V free (the "gate/up tied" row,
`--fusion-target metal --untie qkv`) loses almost nothing against untied.
Summed single-tensor losses predicted the held-out KL of these plans within
15%. The sample is small (one 0.6B model, 512 tokens); the device runs below
should repeat it on Gemma 4. Details:
`zig/pkg/inference/scripts/quant_plan/results/qwen3-0.6b.json`.

## Qualification plan

Device runs (Apple Silicon Metal and NVIDIA CUDA) answer what a plan costs in
speed and whether it keeps its quality on the real runtime:

1. Measure: `export --quantize-sensitivity`, export each candidate format
   uniformly, then `torch_lm_sensitivity.py measure --device cuda|mps` on
   calibration windows.
2. Plan at matched budgets (for example 4.5, 5.0 and 5.5 bits) for `portable`,
   `metal`, `cuda`, `--untied`, and `metal`/`cuda` with `--untie qkv`.
3. Quality: `torch_lm_sensitivity.py evaluate` on disjoint windows.
4. Speed: `bench_variants.py prepare` then `run --backend metal` and
   `run --backend cuda` with `--repeat 3`. It reports decode tok/s, prefill
   time, greedy agreement with `uniform-q8_0`, and fusion/fallback counters.
   For Gemma 4 E2B, export from the dense safetensors checkpoint and add
   Google's QAT Q4_0 GGUF with `--extra qat-q4_0=<dir>` as the baseline.

Acceptance, per model and backend:

- A `portable` plan at a uniform format's size has lower token KL than that
  format, and decode tok/s within 5% of it.
- An untied plan that splits gate/up shows the Metal fused-block fallback in
  the counters; the size of that slowdown is recorded.
- A plan with tied gate/up and free Q/K/V (`quantize-plan --untie qkv`;
  `bench_variants.py` targets `metal-qkv-free` and `cuda-qkv-free`) is timed
  against the `portable` plan: the difference is what a per-projection-format
  fused QKV kernel could recover.
- Greedy agreement with `uniform-q8_0` does not fall below the uniform format
  of the same size.

## Alternatives considered

- Fused kernels that take mixed formats inside one dispatch. For gate/up this
  is not worth it: tying the pair costs almost nothing (Qwen3-0.6B above). For
  Q/K/V it is the one kernel the measurements support: a fused QKV projection
  whose three outputs each use their own format (each output already has its
  own rows, so this is a per-output decoder choice rather than a mixed block),
  on Metal and CUDA, built through the quant kernel compiler
  (`zig/pkg/inference/QUANT_KERNEL_COMPILER.md`). With it, plus CUDA gate/up
  pair routes for `q5_k` and `q6_k` (Metal already fuses them), portable plans
  would reach the "gate/up tied, Q/K/V free" row. Build it only if the device
  runs show the unfused QKV fallback costs measurable decode speed; if three
  separate matmuls are nearly as fast, untied Q/K/V plans already get the
  quality.
- Trellis-coded weights (QTIP, as used by LookAtMyQuant). Better below 3 bits,
  but a new format with a runtime Hadamard transform, an offline Viterbi
  encoder and new kernels on every backend; revisit only for sub-3-bit
  decoders.
- Importance-weighted rounding (llama.cpp imatrix, GPTQ/AWQ-style) changes how
  each tensor is rounded rather than which format it uses. It is complementary
  and could raise every candidate's quality.
- Per-row bit allocation (LookAtMyQuant's Fisher row plans) needs formats with
  per-row types; GGUF types are per tensor.
- Quantization-aware training (Google's Gemma QAT checkpoints) is the strongest
  baseline at 4 bits and is the comparison point for Gemma 4, not a competitor
  to planning, since a plan can start from QAT weights.

## Future work

- Token-KL and output-space plans for Gemma 4 E2B/E4B, Whisper (WER) and GLiNER
  (span F1); plans for bundle exports.
- A fused QKV route with per-projection formats, and CUDA `q5_k`/`q6_k` gate/up
  pair routes, if device runs show the unfused fallbacks cost decode speed.
- Row-group allocation for token tables (frequency-ranked rows), which matters
  for EmbeddingGemma's and Gemma's 262k-token vocabularies.
- Runtime selection of mixed variants through `antfly_inference_variants.json`.

## Code

- `zig/pkg/inference/src/quantize/mixed_plan.zig`: file formats, allocator,
  fusion rules, `antfly inference quantize-plan`.
- `zig/pkg/inference/src/native_export_gguf.zig`: `--quantize-sensitivity`,
  `--quantize-plan`, group detection and split warnings.
- `zig/pkg/inference/scripts/quant_plan/`: measurement, evaluation and device
  benchmark scripts, and recorded results.
- `zig/pkg/inference/QUANTIZE.md`: CLI reference.

## References

- LookAtMyQuant, quantization compiler with per-piece sensitivity and budgeted
  allocation: <https://github.com/ShinMegamiBoson/lookatmyquant>,
  <https://lookatmyquant.pages.dev>
- Y. Shoham and A. Gersho, "Efficient bit allocation for an arbitrary set of
  quantizers," IEEE Trans. Acoustics, Speech, and Signal Processing 36(9),
  1988.
- Z. Dong et al., "HAWQ: Hessian AWare Quantization of Neural Networks with
  Mixed-Precision," <https://arxiv.org/abs/1905.03696>; "HAWQ-V2: Hessian Aware
  trace-Weighted Quantization of Neural Networks,"
  <https://arxiv.org/abs/1911.03852>
- E. Frantar et al., "GPTQ: Accurate Post-Training Quantization for Generative
  Pre-trained Transformers," <https://arxiv.org/abs/2210.17323>
- J. Lin et al., "AWQ: Activation-aware Weight Quantization for LLM Compression
  and Acceleration," <https://arxiv.org/abs/2306.00978>
- J. Chee et al., "QuIP: 2-Bit Quantization of Large Language Models With
  Guarantees," <https://arxiv.org/abs/2307.13304>
- A. Tseng et al., "QuIP#: Even Better LLM Quantization with Hadamard
  Incoherence and Lattice Codebooks," <https://arxiv.org/abs/2402.04396>
- A. Tseng et al., "QTIP: Quantization with Trellises and Incoherence
  Processing," <https://arxiv.org/abs/2406.11235>
- llama.cpp k-quants (mixed-type `_S`/`_M`/`_L` recipes):
  <https://github.com/ggml-org/llama.cpp/pull/1684>; importance matrix:
  <https://github.com/ggml-org/llama.cpp/pull/4861>
