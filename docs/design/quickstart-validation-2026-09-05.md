# Local quickstart follow-up — 2026-09-05

These are workflow observations, not a controlled benchmark or an end-to-end
release qualification. Source base: `7f93fa0bacf95a1a6fc7bd56682bd97e468dd13c`,
with the `fix/quickstart-gemma-rag-image` working changes. Performance binary:
Zig 0.16.0, ReleaseFast, Metal enabled. Machine: Apple M4 Max, 14 CPU cores,
36 GiB unified memory. The inference host budget was 8192 MiB.

## Embedding throughput

The original 10,000-article fixture was used, SHA-256
`a446ebb8721ce4dd262a8320ba65479a21b361140c639d8a6c4e198892382623`.
Model: `Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1`. Both the embedding
and Qwen3 text reranking models selected Metal. The unused embedding chat
template no longer emitted its initialization warning.

The managed-index observation used `{{title}} {{body}}`, target 200 tokens,
overlap 25, and the full fixture loaded via ten HTTP batches of 1,000 records.
The model was prewarmed by endpoint probes before ingestion. After 301.83 s:

- 576 sources covered, 9,424 pending, no skipped or failed sources.
- 2,537 chunk embeddings computed: **8.4 embeddings/s**.
- 2,369 query-visible vectors: **7.85 published vectors/s**.
- 320 embedding batches completed; worker embedding time totaled 294.48 s.

James's reported 3,208 embeddings in 1,200 s is **2.67 embeddings/s**, not
2.8 seconds/document. His 768 covered articles imply 1.56 seconds/covered
article. Our five-minute sample was about 3.1 times his aggregate chunk rate,
but this does not isolate hardware from backend, input lengths, power state,
thermal state, memory pressure, or other workloads. It is not a prediction
of time to index the full corpus.

Bounded warm embedding endpoint probes illustrate why a single embeddings/s
number is insufficient:

| Input sample | Batch | Mean model tokens/input | Embeddings/s |
| --- | ---: | ---: | ---: |
| Short title/excerpt | 1 | 31 | 16.5 |
| Short title/excerpt | 8 | 36 | 74.5 |
| Quickstart article chunks | 1 | 214 | 12.0 |
| Quickstart article chunks | 8 | 323 | 12.1 |
| Long passages | 1 | 937 | 4.6 |
| Long passages | 4 | 908 | 6.3 |

The batches used different samples; these are not matched batching-speedup
experiments. Chunker target tokens are not necessarily the embedding model's
token count. All returned embeddings were finite, normalized, and 1,024-wide.
The first cold short embedding request took 3.37 s including initialization.

The CLI rate now averages computed embeddings across observation checkpoints,
resetting when the worker epoch or counter resets. It is explicitly labeled
`avg_embeddings`; it is not source coverage or token throughput. Source-coverage
waits still require publication to catch up, so a covered count above the
threshold alone need not finish a wait. No durability check was weakened.

## Qwen3 Q8 reranking

Default: `ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF:gguf:Q8_0`, explicitly
pulled with `--tasks rerank`. This is the public text-only 0.6B GGUF, not the
separately qualified Qwen3-VL 2B bundle. The existing VL receipt restrictions
remain intact.

Source revision: `a02f48bb4f057028298c21fa033da2b30d7742d5`.
Weights: `qwen3-reranker-0.6b-q8_0.gguf`, 639,153,184 bytes, SHA-256
`22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48`.

The runtime uses the Qwen3 text scoring prompt and final-token yes/no head,
preserving the fixed scoring suffix when truncating a long document.
For “What is the capital of China?”, Beijing scored 0.9987424 and the
unrelated gravity passage scored 0.00000863048. Batched and individual
scores agreed; the cold two-document call took 0.702 s. These very short
inputs are not comparable to a hybrid query over full articles.

The documented hybrid query returned Felix Pirani first, with score
0.1110244, while indexing remained active. The API reported `took: 47`;
no matched cold/warm hybrid benchmark was collected. Full-body reranking was
visibly slow. A result limit is not necessarily a candidate scoring limit.
The Q8 switch removes the ONNX Metal rejection/native fallback from this
example, but is not a promise of lower reranking latency.

## Gemma and streaming checks

The current public Gemma repository has Q4_0, not Q4_K_M. Pulling now
classifies standalone Gemma GGUF as generation, preserves explicit task
choices, and recognizes image and audio in its unified Q8 projector.
The audio projection path now derives its intermediate width from the
projector weights rather than assuming the decoder width.

The Q4_0 artifact header and actual Q8 projector were checked. Live generation
used the already available local **Q4_K_M** weights, exposed through an
isolated test manifest and symlinked weights; this is **not** a fresh Q4_0
download or an end-to-end qualification of the exact quickstart model.

- Local Metal text generation succeeded.
- A one-article RAG request emitted a retrieved hit, the grounded answer
  “The Korean War began in 1950 [korea].”, and `done`; CLI exit 0.
- An intentionally incompatible generator emitted terminal
  `GenerationFailed`; CLI exit 1, rather than misleading success.
- CLI SSE output is forwarded incrementally. Tests cover error detection
  across network chunks and CRLF, without treating event-like text in data
  as an error event.
- Local `--image` generation described the colored PNG test pattern.
- Local `--audio` generation ran on the speech fixture, but transcribed
  “The green box just over the lazy dog.” rather than the expected
  “The quick brown fox jumps over the lazy dog.” Execution passed;
  transcription accuracy did not. Image/audio server-backed CLI generation
  remains unsupported; the documented examples use local model directories.
- `antfly inference generate --help` returned help and exit 0.

Classification and follow-up events currently come from built-in
heuristics/templates, not Gemma. A classification-enabled full-text smoke
request rewrote the query and retrieved no hits, although generation and
the stream completed. The implementation contains Antfly-domain templates;
replacing these with corpus-aware model-backed classification/follow-ups
remains separate work. The quickstart now requests answer generation only,
omitting classification, reasoning, and follow-up steps in every example.
It must not claim these heuristic steps validate LLM reasoning.

## Image-index investigation and limits

The reported image gate required 1,000 successful images out of 10,000
articles, despite only 2,446 articles having thumbnail URLs. It was not
10% of image-bearing articles. The walkthrough now waits for 250 durable
searchable image artifacts and explains intentional skips.

Latest-main `flushDeferredGeneratedWork` shares a bounded preparation and
publication window across asset work, plain embeddings, and chunked text
embeddings. Assets are flushed first, but the window also processes text
before publication. This is consistent with the supplied stack samples
showing Qwen work while image publication lagged; it does not establish a
slow ClipClap kernel. No image throughput improvement or scheduler fix is
claimed, and a fresh full image-index timing run is still needed.

The CLI now makes `source-covered` percentages exclude skips directly, retaining
pending and failed sources in the denominator, with early unreachable-target
diagnostics. No separate eligible-source wait option is needed.
See [exact-batch replay](embedding-batch-replay.md) for the later
matched-input throughput measurements; those supersede comparisons between
the different endpoint and managed input samples above.

## Reproduction and evidence

Local evidence is retained under
`/private/tmp/antfly-quickstart-perf.9au5Dl`: `measure.py`,
`measurements.json`, `server.log`, `text-wait.log`, fixture, isolated models,
and database. The bounded driver requires a fresh database on its configured
server and the embedding/reranking models already pulled. Endpoint probes
precede ingestion; do not run competing inference jobs during those probes.

Correctness checks used Debug where practical. The current Q8/progress changes
passed the command suite (89 tests), focused reranking prompt/compatibility
tests, and documentation generation/checks. Earlier full inference checks
passed before the final Q8-specific additions; they are not represented as
a fresh full-suite run of every final edit. The ReleaseFast + Metal binary
built successfully (27/27 steps). Test servers were shut down after use.
