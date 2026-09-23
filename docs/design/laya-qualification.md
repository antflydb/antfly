# Laya CPU and Metal qualification

Measured September 19, 2026.

This qualification uses the English `convaiinnovations/laya` checkpoint at
`c5d78730f3493e4fe16d61507ef4b78eef7318cf`, with upstream `common.py` at
`6a5819129eb220570792e417e49723d697efd76f`. Hardware is an Apple M4 Max with
36 GiB of unified memory. Antfly is built with Zig 0.16.0, `ReleaseFast`, Metal
enabled, and CUDA/ONNX disabled. The Python oracle uses PyTorch 2.14.0 on CPU,
eight threads, float32 weights/activations, and eager Transformers attention.

The released checkpoint exposed an accuracy bug that the miniature fixture did
not catch: the shared Hugging Face ModernBERT feed-forward path used tanh GELU
instead of exact erf GELU. Correcting that path reduced the maximum Metal
probability error from 0.0032384 to 0.0000076. An always-on scalar regression
test distinguishes the two activations; the optional full checkpoint test
checks accumulation across all 28 encoder layers.

A second issue appeared at 32 four-option questions (128 scorer rows): the
Metal FP16 host-input projection selected a reduction kernel but used the FP32
tiled kernel's dispatch geometry, leaving output entries unwritten. Matching
the geometry to the selected dtype fixes this without limiting batch sizes.
The regression checks FP16 identity-plus-bias projections at 127, 128, 129,
and 256 rows, including every output element.

The released-checkpoint CPU API test also exposed retained temporary memory:
passing the request arena into inference kept freed encoder intermediates alive
across layers and exhausted the 512 MiB serving heap. The API now uses its
freeing, bounded allocator for model temporaries while keeping prepared tasks
and decoded results in the request arena. The released CPU API regression passes
without increasing or bypassing that heap limit. The managed Metal API regression
also passes with the same allocator change.

## Accuracy methodology

Use 64 seeded (`714`) shuffled examples from each of AG News test, BoolQ
validation, and SST-5 test. Questions, exact texts, token IDs, marker positions,
raw logits, calibrated probabilities, and targets are captured by
`scripts/laya/laya_qualify.py`. Reject overlength examples rather than using
upstream's silent truncation; no selected examples were excluded in this run.

| Dataset / primitive | Upstream correct | Accuracy |
| --- | ---: | ---: |
| AG News / choice | 61 / 64 | 95.3125% |
| BoolQ / boolean | 45 / 64 | 70.3125% |
| SST-5 / ordinal argmax | 21 / 64 | 32.8125% |

Both native CPU and Metal reproduce all 192 upstream label decisions and the
accuracy counts above. Token IDs and marker positions match exactly. Maximum
absolute probability differences from PyTorch are `0.0000083` on CPU and
`0.0000076` on Metal, within the asserted `5e-5` bound. Action probabilities
also pass that bound. The independent synthetic fixture checks raw decision
and action logits within `2e-4` on both backends.

SST-5 expected-level MAE is 1.003414 on the zero-based five-level scale.
These small, prompt-specific samples test integration fidelity and characterize
this checkpoint. They do not establish production accuracy, reproduce upstream's
full benchmark, or qualify the multilingual and fine-tuned checkpoints. AG News
and BoolQ are also represented in upstream's training mix.

## Batching and performance

The released FP16 checkpoint passes repeated fixed-input batches of 1, 2, 4,
8, 16, 32, 64, and 128 questions. Heterogeneous, reversed batches through 16
questions exercise different sequence lengths, option counts, and all three
primitives; the 192-example comparison additionally uses heterogeneous batches
of eight with sequence lengths from 48 to 442 tokens. Synthetic mixed batches
pass through the 512-task pipeline limit. The managed extraction test verifies
input IDs, result grouping, per-input schema replacement, and equivalence to
individual embedded requests. It also checks that the HTTP handler and embedded
API return identical JSON for a request containing all three primitives.

Metal fixed-input results for one 61-token, four-option question repeated per
batch (latency is for the entire batch):

| Questions / batch | Median ms | Observed p95 ms | Questions / second |
| ---: | ---: | ---: | ---: |
| 1 | 41.768 | 42.145 | 23.94 |
| 2 | 49.323 | 49.719 | 40.55 |
| 4 | 61.718 | 62.420 | 64.81 |
| 8 | 86.913 | 87.584 | 92.05 |
| 16 | 140.893 | 141.834 | 113.56 |
| 32 | 241.865 | 247.001 | 132.31 |
| 64 | 447.158 | 447.817 | 143.13 |
| 128 | 888.995 | 906.267 | 143.98 |

Native CPU, measured separately with the same binary and input:

| Questions / batch | Median ms | Observed p95 ms | Questions / second |
| ---: | ---: | ---: | ---: |
| 1 | 256.641 | 263.403 | 3.90 |
| 2 | 357.819 | 359.230 | 5.59 |
| 4 | 499.339 | 505.428 | 8.01 |
| 8 | 833.922 | 844.589 | 9.59 |
| 16 | 1471.585 | 1483.615 | 10.87 |
| 32 | 2747.847 | 2791.263 | 11.65 |
| 64 | 5400.396 | 5821.263 | 11.85 |
| 128 | 13773.618 | 14346.199 | 9.29 |

For this fixed shape, Metal is 6.1 times faster at batch one and 15.5 times
faster at batch 128. CPU throughput peaks at batch 64; larger batches are not
automatically faster. Metal throughput mostly levels off between 64 and 128.
Every fixed CPU batch also stays within `0.0000001` of the PyTorch probabilities.

Maximum probability difference from PyTorch across these fixed Metal batches
is `0.0000001`. Mixed-length Metal batches have median latency of 41.616,
53.764, 81.419, 178.343, and 344.846 ms at sizes 1, 2, 4, 8, and 16, respectively;
maximum probability error is `0.0000047`. Mixed lengths and padding materially
affect throughput: the 16-question mixed batch achieves 46.40 questions/second.
The corresponding CPU mixed medians are 249.897, 371.657, 691.859, 1847.814,
and 3759.179 ms, with maximum probability error `0.0000032`.
Large batches of the full checkpoint with long sequences have not been qualified
through the 512-task API limit. CUDA is deferred.

The focused Metal suite passed all 11 selected tests, including the kernel
threshold regression. Native CPU passed the nine pipeline/configuration tests;
the initially failing managed CPU API test passed after the allocator fix, as
did its Metal counterpart. Formatting, Python lint, and diff checks also pass.

## Reproduce

Run from the repository root. The model importer refuses to overwrite an existing
directory. Keep the generated model and datasets outside the repository.

```sh
mkdir -p /tmp/laya-qualification
uv run scripts/laya/prepare_laya.py convaiinnovations/laya \
  --revision c5d78730f3493e4fe16d61507ef4b78eef7318cf \
  --output /tmp/laya-qualification/model
curl -fsSL https://raw.githubusercontent.com/NandhaKishorM/laya/6a5819129eb220570792e417e49723d697efd76f/laya/common.py -o /tmp/laya-qualification/common.py
curl -fsSL https://huggingface.co/datasets/sh0416/ag_news/resolve/70e3fa1915be9a8daebec5e840f20df9a8e18793/test.jsonl -o /tmp/laya-qualification/ag-news.jsonl
curl -fsSL https://huggingface.co/datasets/google/boolq/resolve/35b264d03638db9f4ce671b711558bf7ff0f80d5/data/validation-00000-of-00001.parquet -o /tmp/laya-qualification/boolq.parquet
curl -fsSL https://huggingface.co/datasets/SetFit/sst5/resolve/e51bdcd8cd3a30da231967c1a249ba59361279a3/test.jsonl -o /tmp/laya-qualification/sst5.jsonl
uv run scripts/laya/laya_qualify.py \
  --model /tmp/laya-qualification/model \
  --common /tmp/laya-qualification/common.py \
  --ag-news /tmp/laya-qualification/ag-news.jsonl \
  --boolq /tmp/laya-qualification/boolq.parquet \
  --sst5 /tmp/laya-qualification/sst5.jsonl \
  --output /tmp/laya-qualification/qualification.json
cd zig
ANTFLY_LAYA_QUALIFICATION=/tmp/laya-qualification ANTFLY_LAYA_METAL=1 \
  python3 tools/run_bounded_zig_build.py --max-rss-cap 16000000000 \
  build inference-test -Doptimize=ReleaseFast -Dmetal=true -Dcuda=false -Donnx=false \
  -- --test-filter 'laya released' --test-filter 'laya extraction v2' \
  --test-filter 'HuggingFace ModernBERT' --test-filter 'metal native f16 host linear'
```

Omit `ANTFLY_LAYA_METAL` for CPU. Use the synthetic-fixture instructions in the
guide for the 512-row and raw-logit tests. Run CPU and Metal measurements
separately. Warm performance measurements include tokenization, forward pass,
and decision decoding, and exclude HTTP transport and checkpoint loading.
The direct-pipeline benchmarks use a per-iteration arena for allocations; the
managed API uses a freeing, bounded scratch allocator for model temporaries.
These timings therefore do not measure the API's request-memory behavior.
Each shape receives one warm-up and seven timed iterations; reported p95 is the
maximum of those seven observations. The fixed profile repeats one text/question
at every batch size; the mixed profile varies text length, primitive, option
count, and row order. These are local microbenchmarks, not service capacity tests.
