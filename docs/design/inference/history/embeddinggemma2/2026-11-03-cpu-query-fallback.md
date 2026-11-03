# CPU text query deployment qualification, 2026-11-03

EmbeddingGemma 2 already supports native CPU inference. The initial investigation
tested text queries and possible startup coverage. The deployment decision is to
run **separate CPU and GPU pods**; automatic startup handoff is out of scope.
It uses the same checkpoint and GB10 host as the [GIF latency investigation](2026-11-03-gif-latency.md).
The runtime and backend-selection defaults are unchanged; changes in this branch
add measurement tools and evidence.

The subsequent [production qualification on this ARM host](2026-11-03-cpu-production-qualification.md)
adds independent reference, full-corpus relevance, sustained serving and recovery
gates. This earlier receipt retains the initial diagnostic result.

## Existing implementation

- `LoadedModel.embeddingPipeline` selects a resident native text encoder for
  EmbeddingGemma 2. Short queries do not run the vision tower.
- `SessionManager.loadModelWithImportedOnnxContext` tries backends sequentially.
  CPU selection after a failed CUDA load is distinct from serving queries on CPU
  while a successful CUDA initialization is still in flight.
- `ModelManager.loadFromDirCoordinated` waits for the matching load flight to
  complete. Its expensive work runs outside the manager lock, and backend-specific
  cache/load keys already allow separately acquired CPU and CUDA variants.
- Native CPU uses F32; CUDA uses its independently versioned BF16 recipe.
  `embedding_gemma2_identity.forBackend` produces different identities, and
  `server.zig:pinEmbeddingModelIdentity` rejects a result that does not match a
  pinned index identity. Availability alone does not establish query compatibility.

A native query also completed in the filesystem sandbox where `nvidia-smi` could
not communicate with the NVIDIA driver. The CPU path in a CUDA-enabled binary
therefore works without initializing a usable CUDA device.

## Method

`build-embeddinggemma2-query-bench` loads through `ModelManager`, explicitly
requires the requested backend, pins the model, and calls its production text
embedding pipeline with the manifest's canonical query/document prefix.
The model identity is retained. Every vector must have 768 finite components and
unit norm. Token counts are independently checked against the same tokenizer;
the native singleton fast path does not populate `last_input_tokens`.

Measured calls include tokenization, the encoder, pooling, normalization, and
completed output. HTTP, broker scheduling and model loading are outside each
call's latency. Time to first vector starts at benchmark entry and includes managed
loading, tokenizer/identity setup and the first inference. The process/file caches
may already be warm. Linux peak RSS covers the whole child process, not only the
encoder workspace.

The synthetic corpus contains 24 retrieval queries, including German, Chinese
and Japanese, 20 text documents, an empty-query probe and a longer query. The
prior GIF experiment optionally supplies a CUDA-produced Earth image vector.
The retrieval assessment holds **CUDA document vectors fixed** and compares
CPU queries with CUDA queries against them; it does not substitute a CPU-built
index. Most query cases use two warmups and twenty measured calls. Documents
are encoded once; the longer query has five measured calls.

The startup race launches two separate fresh managed processes together and
records each time to first vector. It is an availability probe, not a test of an
automatic handoff inside the server.

## Results

CPU text search is substantially cheaper than CPU image embedding. The 24 short
retrieval queries contained 17–28 tokens, including the retrieval prefix. Their
CPU case medians ranged from **51.23 to 87.04 ms** (median of case medians:
66.35 ms), compared with **3.44–3.79 ms** on CUDA.

| Probe | Tokens | CPU warm p50 | CUDA warm p50 |
|---|---:|---:|---:|
| Password recovery query | 17 | 52.83 ms | 3.44 ms |
| German password query | 19 | 81.46 ms | 3.50 ms |
| Chinese password query | 18 | 78.10 ms | 3.48 ms |
| Japanese CSV query | 19 | 70.67 ms | 3.50 ms |
| Text query for the Earth GIF | 21 | 71.08 ms | 3.58 ms |
| Empty-query/prefix-only probe | 10 | 43.77 ms | 4.06 ms |
| Longer natural-language query | 298 | 541.68 ms | 19.40 ms |

All 46 cases completed on both backends with normalized, finite 768-dimensional
vectors. CPU and CUDA reported matching canonical token counts. The standalone
no-GPU CPU probe also succeeded with the corrected benchmark's 17-token query.

Against the fixed CUDA index (20 text documents plus the Earth GIF):

- All **24/24** CPU queries retrieved the same top result as CUDA queries.
- All **24/24** had the same top-five set, and both backends found the expected
  document/image at rank one for every query.
- Top-five order matched for **23/24** queries; full ordering matched for 17/24.
- Query-vector cosine was at least **0.99996401**, but maximum component
  differences reached **0.00129041**. Maximum document-score drift was 0.00167214.

This is promising query compatibility on a small corpus, not identical results
or a qualification of all existing indexes.

### Startup and memory

Separate sequential runs reached their first usable query vector in **4.545 s**
on CPU and **6.662 s** on CUDA. Managed loading accounted for 4.223 s and 6.472 s;
first inference itself took 0.322 s and 0.190 s respectively. Thus model setup,
including integrity/identity and tokenizer work, dominates process startup.

With both fresh managed processes launched together, CPU's first vector arrived
at **4.454 s** and CUDA's at **5.199 s**: CPU covered roughly **0.746 s** of that
particular startup window. These are individual observations with warm OS/file
caches, not startup percentiles. A previously warmed CPU session can cover a
larger CUDA restart window without paying its own load cost again.

Whole-process peak RSS was **1,479.7 MiB** for the CPU corpus and **1,791.8 MiB**
for CUDA. Those peaks include model mappings, tokenizer, caches and workspaces;
they are not per-query memory or additive physical memory requirements for a
shared-process deployment. Retaining the CPU variant still needs an explicit
memory budget.

The measured CPU model identity was
`7c034a322f810a719cc3183d82c71e902f9a5588722cdf585dc6065849d43b78`;
the CUDA identity was
`41ae617b720b4c5f32e711a8517d22df828a7d416b5cc7e643ea661125d04f8e`.
The serving identity guard therefore remains a concrete obstacle to transparent
fallback for a CUDA-pinned query, despite the encouraging retrieval results.

[The evidence receipt](2026-11-03-cpu-query-fallback.json) retains identities,
fingerprints, per-call timings, vector hashes and top-five comparisons. Full
vectors and logs remain in `zig/pkg/inference/.benchmark-results/cpu-query/`.

## Production qualification for separate CPU and GPU pods

**Status: not production-qualified by this investigation.** The 46-case execution
check and 24-query synthetic retrieval corpus establish functionality and a local
latency baseline. The repository's older numerical/HTTP qualification also
explicitly leaves production qualification open.

The remaining work for a CPU text-query pod is:

1. Match the exact CPU serving binary against the pinned independent F32 reference,
   and run its HTTP contract tests, including dimensions, input limits, deadlines,
   cancellation, concurrent requests and error recovery. The new benchmark uses
   the managed pipeline directly and does not exercise HTTP/broker scheduling.
2. Evaluate held-out, representative search queries against the actual index and
   languages, measuring relevance such as Recall@k/nDCG@k. If documents/images are
   embedded on GPU, keep those vectors fixed when evaluating CPU queries. The
   current corpus found one top-five ordering change despite identical top-five
   sets, so it is not an exact-ranking equivalence claim.
3. Measure sustained p50/p95/p99 latency, throughput, peak/steady memory and recovery
   under the intended pod CPU architecture, vCPU/thread settings and memory limits.
   The present numbers come from the ARM GB10 development machine, not the
   deployment's CPU instance or a resource-limited HTTP service.

Separate pods remove startup routing and in-process handoff requirements. Each
pod should explicitly require its intended backend. CPU queries against a
GPU-built index still need an explicit query-compatibility policy if requests
carry the CUDA identity; independent pods do not make F32 and BF16 identities
equal. Ingestion retains its configured recipe.

## Precision recommendation

Qualify the existing F32 CPU path first. Reduced-precision support is a separate
optimization experiment, not a prerequisite for production qualification.

The inspected checkpoint contains 744,371,992 BF16 elements and no F16/F32 tensor
payloads. `architectures/embedding_gemma2.zig:requireWeight` currently accepts BF16
or F32, and native inference constructs canonical F32 GEMM panels from typed
checkpoint rows. The generic CPU backend already has an F16-weight/F32-accumulate
GEMM path, but accepting F16 weights in Gemma 2 would need model-path integration
and qualification; enabling the datatype alone would not replace its prepared
F32 panel path or establish a speedup.

For the next memory/bandwidth experiment, prefer **compact BF16 weight panels
with F32 accumulation** on the target CPU. This keeps the checkpoint's weight
format and wider exponent range. FP16 conversion offers no storage-width
advantage over BF16, although either compact format could reduce the prepared
panel footprint relative to F32. BF16 has a wider dynamic range than FP16
([PyTorch numerical-accuracy notes](https://docs.pytorch.org/docs/2.15/notes/numerical_accuracy.html)).
This does not assert that this checkpoint overflows FP16; that has not been tested.

Benchmark compact BF16 and, if the target hardware favors it, an FP16-weight/F32-
accumulate candidate against the F32 baseline. Preserve F32 normalization/softmax
where required by the numerical contract. Treat true reduced-precision arithmetic
as a separate, versioned embedding recipe with independent reference, relevance,
performance and stability checks. Neither BF16 nor FP16 CPU kernels are qualified
by the current measurements, and no precision/runtime defaults changed.

## Reproduction

From `zig/pkg/inference` in the worktree:

```bash
zig build build-embeddinggemma2-query-bench \
  -Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=fatbin
python3 scripts/embeddinggemma2/cpu_query_campaign.py \
  --binary zig-out/bin/antfly-embeddinggemma2-query-bench \
  --model "$MODEL" --output-dir .benchmark-results/cpu-query \
  --cuda-image-receipt .benchmark-results/gif/default/animated_gif.json
```

Omit `--cuda-image-receipt` when the earlier GIF campaign is unavailable. The
benchmark forces `native` or `cuda` and fails if that backend was not actually
selected. The campaign preserves full vectors, per-call timing samples, startup
logs and process RSS. `test_cpu_query_campaign.py` checks that the assessment
keeps the CUDA index fixed and rejects unmatched tokenization, corpus, backend
or case coverage. The optional `--startup-race` retains the earlier startup
diagnostic; it is not part of the separate-pod deployment requirements.

The existing inference CLI can explicitly select CPU for a search query:

```bash
antfly inference embed "$MODEL" --backend native \
  --task-type RETRIEVAL_QUERY --text 'How can I reset my forgotten password?'
```

This explicitly requests the CPU recipe; it does not enable startup failover.
