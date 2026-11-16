# L4 native CUDA / PJRT comparison, 2026-11-16

Both models execute successfully through the new native Zig/PJRT artifact runner.
Gemma 4 favors Antfly's native CUDA Q4 path for decode throughput and memory.
EmbeddingGemma2 favors PJRT for warm latency, while native CUDA uses less memory.

These are **native prepared-HLO CLI** PJRT results, not Python frontend timings
and not the HTTP server's existing XLA backend. Server integration remains open.

## Method

NVIDIA L4 (23,034 MiB), SM89, driver 580.159.03; 4 vCPU, 16 GiB host, no swap.
PJRT CUDA plugin/JAX 0.10.2, CUDA runtime 12.8, cuDNN 9.8. Worktree base:
`3682e4426baf3d7891a4b0ffbd34ca76bf7aef7e`.

The native CUDA control is the existing V15 server binary, SHA256
`2c1b08ce31d7b6a30be9051497afe1542d60e74d73b43cbffb1d6327b648e7dc`,
source `9f775620c84be17414fe5c07990956287b4e1d56`. Relevant CUDA model paths
were checked unchanged against this worktree's base. It is not a binary built
from this branch. CUDA timings include the full HTTP request, preprocessing and
response parsing; PJRT starts with token IDs and ends after output readback.
The observed speed ratios include that entrypoint difference.

Runs were serialized with no other GPU compute process. Each case used one
input and one fixed shape, one process lifecycle, with loading, compilation
and warmups excluded from reported medians. There are four measured Gemma calls
per shape and eight embedding calls per shape. This is a local characterization,
not a randomized multi-lifecycle production performance qualification.

## Gemma 4 E4B

Same source Q4 GGUF, SHA256
`676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee`.
Native CUDA retains Q4 weights with F16 KV; PJRT receives the same GGUF tensors
**dequantized to dense BF16**, with BF16 KV. This comparison does not isolate
compiler/runtime effects from storage precision. Greedy fixed output lengths,
no prompt reuse, thinking disabled. The retained fixture supplies exact prompts
and token IDs.

| Prompt / output tokens | CUDA HTTP ms | PJRT native ms | CUDA decode tok/s | PJRT decode tok/s | CUDA completion speedup |
|---|---:|---:|---:|---:|---:|
| 32 / 256 | 3,629 | 11,705 | 71.51 | 21.87 | 3.23× |
| 419 / 128 | 1,907 | 5,993 | 70.75 | 21.49 | 3.14× |
| 1,775 / 64 | 1,234 | 3,232 | 69.79 | 20.76 | 2.62× |

PJRT prefill TTFT was lower: 43.7 / 83.4 / 197.9 ms, versus CUDA HTTP
63.1 / 113.4 / 331.2 ms. Native TTFT ends before token readback, whereas HTTP
TTFT includes serving and streaming. PJRT retained 14.926 GB of device weights.
Peak GPU use was **19.587 GB PJRT versus 7.086 GB CUDA**. Peak sampled PSS plus
NVML was 22.585 GB versus 8.124 GB. Peaks include loading/compilation and all
three shapes; host feeder memory is excluded from PJRT's process tree.

Gemma's native CUDA control enabled `ANTFLY_INFERENCE_CUDA_RMS_Q8_1_MIRROR=1`
and `ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX=1`.

The performance campaign's free-running token IDs were repeatable across warm
calls, but differed from the retained Torchax oracle starting at token indices
205, 101 and 63 respectively. A separate 216-step teacher-forced BF16 logit
campaign, with explicit JAX SPMD/Shardy compiler options and the CUDA 12.8 tool
directory, had **216/216 identical argmax choices**, minimum logit cosine
**0.999947974**, maximum relative L2 **0.0116636**, and maximum absolute logit
error **0.372202**. First prefill logits matched exactly. These are distinct
compiler-option campaigns; exact free-running parity is not established for the
performance configuration. A separate tiny CPU-BF16 model oracle matched all
three generated tokens exactly, including cached decode.

PJRT load/stream time was 132.6 s and total compilation 224.4 s for six graphs;
these costs are not included in the warm table.

## EmbeddingGemma2 text

Same original BF16 safetensors checkpoint and exact document-prefix tokens.
CUDA loads the complete multimodal checkpoint; PJRT selects the 413 text
weight tensors plus derived constants, totaling 542,008,372 resident bytes.
The memory comparison consequently does not isolate allocator efficiency.

The inputs repeat `river` to obtain the specified token counts after
`title: none | text: ` rendering. These isolate length scaling and are not
semantic retrieval-quality measurements. Both routes compute BF16 model
forwards with 768-dimensional normalized text outputs. CUDA usage confirmed
128 / 512 / 2048 / 8192 prompt tokens exactly.

| Tokens | CUDA default HTTP ms | CUDA opt-in HTTP ms | PJRT native ms | PJRT speedup vs opt-in |
|---|---:|---:|---:|---:|
| 128 | 14.56 | 11.00 | 3.73 | 2.95× |
| 512 | 26.33 | 22.01 | 6.94 | 3.17× |
| 2,048 | 243.53 | 89.89 | 32.97 | 2.73× |
| 8,192 | 2,970.14 | 659.97 | 333.12 | 1.98× |

Opt-in CUDA enabled these existing flags (each `=1`):

- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM`

Counters confirmed their consumers ran. The opt-in 8192-token case used 157
cuBLASLt matmuls and 219 kernel launches, with **zero scalar matmul or attention
fallbacks**. Default CUDA's 2048/8192 cases used four scalar attention fallbacks.
The default result therefore is not the best measured CUDA path.

| Whole-campaign sampled peak | CUDA default | CUDA opt-in | PJRT |
|---|---:|---:|---:|
| GPU GB | 2.013 | 2.345 | 4.538 |
| PSS + GPU GB | 3.693 | 3.675 | 5.848 |

GB means decimal bytes / 1e9. Memory sampling used process-tree smaps_rollup PSS
plus NVML every 0.2 seconds, including load and compilation. All samplers marked
their data valid with no errors. Peak components need not occur simultaneously.

PJRT vectors matched the separate Torchax/JAX BF16 oracle to printed cosine
1.0 at every length. CUDA default oracle cosine ranged from 0.999615 to
0.999937; CUDA opt-in ranged from 0.999595 to 0.999943. All vectors had the
expected dimension and finite, normalized values. This validates the synthetic
cases and does not establish accuracy on a retrieval evaluation set.

PJRT weight streaming/load took 1.07 s; compilation totaled 191.9 s across the
four static shapes. Native CUDA's model load took about 9 s. Warm speedups must
not be interpreted as first-request speedups.

## Evidence and reproduction

[README.md](README.md) contains build/export/oracle/measurement commands.
[results-l4.json](results-l4.json) retains unrounded metrics, measured binary
hashes, policies and limitations. In the worktree, full raw data is retained
under ignored `evidence/`:

- `gemma4-native-results.json`, `gemma4-native-memory.json`, and `gemma4-native.log`.
- `gemma4-validation-results.json`, validation HLO/oracles and all failed earlier
  numerical experiments. Early strict token/absolute-error gates failed; the
  complete later teacher-forced statistics above are retained separately.
- `embedding-native-results.json`, `embedding-native-memory.json`, and native logs.
- `embedding-cuda/` and `embedding-cuda-opt/`: every vector, usage, request time,
  model identity, server route counters and memory timeline.
- `public-export-*` and `public-native-smoke/`: checks of the self-contained
  export/stream/benchmark tools. Exported parameter manifests match the measured
  artifacts exactly.
- `tiny-model/final-results-v2.json`: final-runner CPU-oracle decode canary.
- `pjrt-tests-final.log`: 25/25 reported CPU tests passed, upload-failure executable
  passed; plugin-dependent library integration checks skipped without a configured
  test plugin. Actual CUDA execution is established by the separate GPU campaigns.

The earlier Gemma native CUDA control remains in the neighboring
`perf-gemma4-cuda/evidence/xla/native-text-v1/` worktree. Its relevant aggregate
metrics and binary identity are copied into `results-l4.json`.

## Current CUDA qualification

The [CUDA qualification index](../../../pkg/inference/scripts/embeddinggemma2/profiling/README.md)
links the final operator, media and automatic-default campaigns. The earlier
tables above retain their original HTTP timing scope and explicit configurations.

The [native global-attention follow-up](../../../pkg/inference/scripts/embeddinggemma2/profiling/GLOBAL_ATTENTION.md)
records the opt-in CUDA implementation, final paired timings, memory and vector
checks. Its 8192-token forward now beats the fresh native PJRT measurement.

The local Gemma4 logit oracle is retained losslessly compressed to save disk.
Restore it before repeating that validation with
`gzip -dk evidence/gemma4-artifact/oracle-logits.f32.gz`.
`evidence/gemma4-artifact/oracle-compression.json` retains its original checksum
and byte count.
