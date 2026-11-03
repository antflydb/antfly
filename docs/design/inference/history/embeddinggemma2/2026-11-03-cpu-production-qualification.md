# Native F32 CPU query qualification on GB10

**Passed:** native F32 CPU text-query serving on this Linux aarch64 GB10 host,
within the measured operating envelope below. The [machine-readable receipt](2026-11-03-cpu-production-qualification.json)
records the exact binary, source hashes, checkpoint, acceptance gates and evidence.
The model uses native F32 execution of the pinned upstream BF16 checkpoint. CPU and GPU run as separate services.

## Acceptance policy

The numerical, load and retrieval limits were declared in
`cpu_http_qualification.py` and `beir_cpu_query_qualification.py` before their
measured runs. The campaign checks the following conditions:

- Native CPU backend, stable CPU model identity, finite normalized vectors,
  maximum absolute component error at most 1e-5 against independent upstream
  F32 output. Check 768 dimensions plus renormalized 128/256/512 prefixes.
- All 300 SciFact test queries against all 5,183 fixed CUDA document vectors.
  Relative to CUDA queries, allow at most 0.005 absolute nDCG@10 loss and 0.01
  absolute recall@10 loss. These are engineering compatibility limits; SciFact
  does not establish a relevance target for a customer's own corpus.
- Closed-loop short-query load at 1/2/4/8 clients for 30 seconds each, then a
  600-second soak at four clients. Zero errors, correct vectors and identity,
  exact completion accounting, no leaked active requests, request admissions,
  work units or scratch leases.
  Accept p95 at most 250 ms per concurrent client, worker RSS below 4 GiB,
  and worker RSS growth below 128 MiB within each profile. These are reference
  host limits, not a customer SLO or an open-loop arrival-rate guarantee.
- Deadline expiry during execution returns retryable HTTP 504; a TCP reset
  cancels and releases the request; a legal request half-close completes; retries
  preserve output and identity. Invalid timeout headers return HTTP 400.
- Production batching/order, per-item errors, dimension projection, wrong
  identity rejection, and similarity-decision contracts pass the existing
  independent-oracle HTTP suite.
- A 32-client overload burst must reject excess work with retryable HTTP 503,
  never exceed 16 admissions, preserve admitted output correctness and permit
  a correct retry after cleanup.

## Numerical and retrieval results

The independent oracle uses torch 2.10.0+cpu and Transformers commit
`92cd495f2720c064bc78eb2d93e28704c5bce51f`. All 46 synthetic/edge text cases
and all 300 SciFact test queries passed through the production HTTP binary;
the largest component error was 3.2783e-7. Renormalized dimension prefixes,
ordered batches, stable identity and the public serving contracts passed.

| Metric | CPU queries | CUDA queries | CUDA minus CPU |
| --- | ---: | ---: | ---: |
| nDCG@10 | 0.853309 | 0.853329 | 0.0000195 |
| Recall@10 | 0.934667 | 0.935333 | 0.0006667 |
| Recall@100 | 0.980000 | 0.980000 | 0 |
| MRR@10 | 0.831175 | 0.830817 | -0.0003571 |

Both query sets search exactly the same 5,183 CUDA document vectors. This
comparison uses full 768-dimensional vectors, exact F64 cosine search, the
published test relevance labels and no sampled negatives.

The 100 ms execution deadline returned retryable HTTP 504 after 102 ms.
Request admission and scratch were released without a successful projection.
TCP reset and legal half-close behavior passed, and a correct retry followed
each cancelled/expired request. Flat native text uses the resident projection
counters; ordered media group counters do not describe this path.

## Sustained serving results

| Clients | Duration | Requests | Queries/sec | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 30 s | 338 | 10.90 | 84 ms | 101 ms | 270 ms |
| 2 | 30 s | 356 | 11.48 | 169 ms | 190 ms | 200 ms |
| 4 | 30 s | 359 | 11.58 | 338 ms | 376 ms | 394 ms |
| 8 | 30 s | 355 | 11.44 | 695 ms | 739 ms | 748 ms |
| 4 | 600 s | 6,934 | 11.53 | 345 ms | 386 ms | 401 ms |

All 8,342 load responses passed vector/identity checks, with zero errors.
The ten-minute four-client soak grew worker RSS by 52 KiB. Warm serving RSS
was about 898 MiB; the worker lifetime RSS high-water mark, including startup
and the long-query tests, was about 1.45 GiB. The same worker and supervisor
remained alive throughout, and the worker had no CUDA library mapped.

A separate 32-client burst admitted 16 requests, returned correct vectors for
all 16, and rejected 16 with retryable HTTP 503 `SERVICE_UNAVAILABLE`, reason
`inference_admission`, and `Retry-After: 1`. Observed admission never exceeded
16. Work units and scratch returned to zero, and a subsequent retry succeeded.
Execution-deadline, TCP-reset, half-close and correct-retry checks passed both
before and after the soak.

Use roughly four concurrent short queries as a conservative starting point
on this machine. Eight clients passed but throughput remained about 11–12
queries/sec, while tail latency increased. These measurements use the full
20-core affinity and do not predict performance under a smaller CPU quota.

## Scope and deployment

The host has 20 ARM cores (10 Cortex-X925 and 10 Cortex-A725), approximately
128 GB physical RAM, and CPU affinity 0–19. The qualified executable is the
stripped ReleaseFast HTTP server built with `-Dcuda=false`, with the native
backend required and one preloaded EmbeddingGemma 2 model. Both supervisor and
worker hashes and process lifetimes are captured in the receipts. The GPU is used
only by the separate fixed-corpus benchmark executable.

The production server has application budgets of 6 GiB host, 1 GiB backend,
7 GiB combined, 2 GiB scratch, 16 admitted requests, and 64 work units. It uses
the detected host memory envelope. This session is an unlimited shared cgroup;
setting an explicit 8 GiB process envelope conservatively charges that shared
cgroup's working set and denies preload. No cgroup guard was bypassed or
changed. This campaign establishes measured RSS on this host; it does not
qualify a hard 8 GiB pod limit or a dedicated CPU quota. Test the actual pod
configuration before using this result as a Kubernetes sizing guarantee.

Use the CPU identity from the receipt when pinning the CPU service. It differs
from the CUDA BF16 recipe identity. The fixed-index relevance test establishes
query compatibility for SciFact; it does not make these two recipes identical
or override an API identity pin. A GPU identity sent to the CPU service remains
a 409 conflict. Keep dimensions and task roles consistent with the index.

The sustained workload uses single text queries of at most 64 tokenizer tokens,
including the model's query prefix. Numerical tests also include document
inputs, empty/multilingual queries, and a 298-token query. Media, maximum-context
throughput, x86 hardware, FP16/BF16 CPU kernels, arbitrary application relevance,
and automatic startup routing are outside this qualification.

## Fixes found by the campaign

A native execution deadline propagated as `error.Timeout` after model
acquisition and was classified as an internal dense-embedding failure, producing
HTTP 500. Dense execution now preserves the existing timeout/cancellation
responses (504/408). The regression test covers Timeout, Canceled and Cancelled,
and the real HTTP recovery campaign exercises the deadline during computation.

The aarch64 CPU ReleaseFast/strip server exceeded its declared 6 GiB compiler
memory claim (measured 10.84 GB). This build profile now reserves 13 GiB, providing
at least 25% headroom. The x86 and accelerated build claims retain their own
measurements.

## Reproduction

Run from `zig/pkg/inference`; keep the model path outside output directories.
Set `MODEL` to the pinned, receipt-verified checkpoint and `PID` to the owned
server supervisor PID. The exact binary SHA256 and server invocation are in
the JSON receipt. Raw evidence lives under `.benchmark-results/cpu-qualification`.

Build the production candidate with:

```sh
zig build http-server -Doptimize=ReleaseFast -Dstrip=true -Dcuda=false
zig build test-inference-unit -Doptimize=ReleaseFast -Dcuda=false \
  '-Dtest-filter=EmbeddingGemma2 dense execution aborts'
```

Use Python 3.12 with torch 2.10.0+cpu and torchvision 0.25.0+cpu from the PyTorch
CPU wheel index; install Transformers at the exact commit pinned in
`scripts/embeddinggemma2/reference.py`. That verifier checks six upstream
implementation hashes and every checkpoint artifact before generating oracles.
`cpu_reference_oracle.py` emits token IDs and complete normalized F32 vectors
for each case in `testdata/cpu-query/suite.json` or the prepared SciFact queries.

```sh
python scripts/embeddinggemma2/cpu_reference_oracle.py \
  --model-dir "$MODEL" --suite scripts/embeddinggemma2/testdata/cpu-query/suite.json \
  --output .benchmark-results/cpu-qualification/reference/query-oracle.json
python scripts/embeddinggemma2/cpu_http_qualification.py collect \
  --server-pid "$PID" \
  --oracle .benchmark-results/cpu-qualification/reference/query-oracle.json \
  --output .benchmark-results/cpu-qualification/query-http.json
python scripts/embeddinggemma2/cpu_http_qualification.py recovery \
  --server-pid "$PID" \
  --oracle .benchmark-results/cpu-qualification/reference/query-oracle.json \
  --cpu-receipt .benchmark-results/cpu-qualification/query-http.json \
  --output .benchmark-results/cpu-qualification/recovery.json
python scripts/embeddinggemma2/cpu_http_qualification.py load \
  --server-pid "$PID" \
  --oracle .benchmark-results/cpu-qualification/reference/query-oracle.json \
  --cpu-receipt .benchmark-results/cpu-qualification/query-http.json \
  --output .benchmark-results/cpu-qualification/load.json
python scripts/embeddinggemma2/cpu_http_qualification.py overload \
  --server-pid "$PID" \
  --oracle .benchmark-results/cpu-qualification/reference/query-oracle.json \
  --cpu-receipt .benchmark-results/cpu-qualification/query-http.json \
  --output .benchmark-results/cpu-qualification/overload.json
```

For real retrieval, download the official BEIR SciFact ZIP once into the shared
immutable fixture cache. `beir_cpu_query_qualification.py prepare` verifies the
publisher MD5 and full corpus/test counts. Its `cuda` operation generates all
corpus/query vectors using `antfly-embeddinggemma2-query-bench` built with CUDA.
Generate the independent query oracle from the resulting `queries.json`, run
`cpu_http_qualification.py collect` on it, then pass that receipt to the BEIR
script's `evaluate` operation. Evaluation verifies every CUDA chunk digest,
backend, suite hash, identity and case coverage, then searches the entire same
CUDA corpus with both CPU and CUDA queries using F64 cosine ranking.

The dataset is [BEIR SciFact](https://huggingface.co/datasets/BeIR/scifact), using
its published test qrels. No corpus or checkpoint data is committed here.

Validation passed: the CPU production ReleaseFast build, one focused Zig
regression covering all three lifetime errors, 14 Python evidence-integrity
tests, Python lint/format checks and Zig formatting. FP16 support and automatic
CPU/GPU routing were not added.
