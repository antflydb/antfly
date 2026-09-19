# dogfood

`dogfood` ingests Antfly's own design docs and work log into an embedded
Antfly Lite database (`.aflite`) and builds a small knowledge graph over them:

- **Chunking + embedding**: each markdown section is chunked at write time
  (fixed-size, ~400 tokens with 40 token overlap) and every chunk is embedded
  with `Qwen/Qwen3-Embedding-0.6B-GGUF`.
- **Knowledge graph ("autograph")**: a GLiNER-family entity/relation extractor
  reads each section's body and materializes a graph index (`knowledge`) with
  entities such as `component`, `subsystem`, `file`, `test`, `invariant`,
  `decision`, `person`, `model`, `backend`, `format`, `protocol`, and relation
  types `depends_on`, `owns`, `implements`, `supersedes`, `tested_by`,
  `documented_in`.
- **Hybrid search**: full-text search over the default full-text index merged
  (RRF) with semantic search over the chunk embeddings, followed by a
  depth-2, both-direction traversal of the knowledge graph starting from the
  matched documents.

This dogfoods the docs cleanup itself: every ingested row is tagged
`kind: "design"` (design docs under `zig/*.md`, `zig/pkg/**/*.md`,
`zig/lib/**/*.md`, `docs/design/**`) or `kind: "work-log"`
(`work-log/**/*.md`), so `dogfood query` and `dogfood entity` can be used to
sanity-check that the split between durable design docs and point-in-time
work-log entries reads sensibly end to end.

## Prerequisites

1. Build Antfly (from the repository's `zig/` directory):

   ```sh
   cd zig
   zig build
   ```

   This also produces `libantfly` for the Go cgo bindings (the minimal
   equivalent is `zig build capi`; see `go/pkg/antflylite/README.md`).

2. Inference runs in-process by default. `libantfly` links the standalone
   inference runtime (the same one the `antfly` executable embeds), so a Lite
   handle opened with `LocalRuntimeConfigured` runs the chunker, embedder, and
   extractor locally; `dogfood status` shows `inference: mode=local_embedded`
   and both models select the Metal backend on macOS.

   Model execution lives in a sandboxed worker process (Metal, CUDA, and PJRT
   calls need crash containment), which the library resolves in this order:
   `ANTFLY_INFERENCE_WORKER`, an `antfly` binary next to the loaded
   `libantfly`, then `antfly` on `PATH`. From a source tree:

   ```sh
   export ANTFLY_INFERENCE_WORKER=$PWD/../../zig/zig-out/bin/antfly
   ```

   To use an external server instead, start one and pass `-inference-url`:

   ```sh
   zig/zig-out/bin/antfly inference run --max-loaded-models 0
   GOWORK=off go run . ingest -db dogfood.aflite -repo ../.. -reset -inference-url http://127.0.0.1:8090
   ```

   That is Lite's "remote inference provider" mode (`zig/LITE.md`): the
   producer configs carry `api_url` and Lite calls the server over HTTP.

3. Pull the models this example uses:

   ```sh
   antfly inference pull Qwen/Qwen3-Embedding-0.6B-GGUF
   antfly inference pull fastino/gliner2.5-base-v1
   ```

   `fastino/gliner2.5-base-v1` is qualified for production extraction on
   native and Metal, including windowed long documents (see
   `zig/pkg/inference/models/gliner2/GLINER25.md` for the evidence and
   bounds); `dogfood` defaults `-extract-model` to it and requests
   `long_document: {mode: "window"}`. `antflydb/gliner2-base-v1` is a
   smaller fallback (`-extract-model antflydb/gliner2-base-v1`). An
   unqualified boundary checkpoint answers `MODEL_NOT_QUALIFIED`.

   In-process inference budgets (host, backend, combined, KV, scratch) are
   derived from the host memory policy the way `antfly inference run` derives
   its defaults, and can be overridden on `antflylite.OpenOptions`.

4. `dogfood ingest` fails fast before doing any work: in-process mode it
   checks that the linked `libantfly` advertises `local_inference_runtime`;
   in remote mode it probes `GET /healthz` and runs a one-sentence
   `POST /ai/v1/extract` against `-extract-model` so a gated model surfaces the
   server's own error (for an unqualified model, `MODEL_NOT_QUALIFIED`) instead of
   failing deep inside `RunUntilIdle`. `dogfood status` prints the same
   checks.

## Run

```sh
cd examples/dogfood
GOWORK=off go run . ingest -db dogfood.aflite -repo ../.. -reset
GOWORK=off go run . query "raft"
GOWORK=off go run . entity "raft"
GOWORK=off go run . status
```

- `dogfood ingest [-db dogfood.aflite] [-repo ../..] [-reset] [-inference-url URL] [-embed-model ...] [-extract-model ...] [-target-tokens 400] [-overlap-tokens 40] [-metrics=true]`
  creates (or reopens) the database with `LocalRuntimeConfigured` (or
  `RemoteProviderConfigured` when `-inference-url` is given), relies on the
  default `full_text_index_v0` that Lite provisions on create (adding it only
  for older libantfly builds), adds the two indexes (`chunk_vectors`,
  `knowledge`), walks the corpus, writes one document per markdown section,
  and drains chunk/embed/extract work with `RunUntilIdle`.
- `dogfood query "<text>" [-inference-url URL]` runs the hybrid search:
  `full_text_search` (match form, so a natural-language question scores every
  term) merged by RRF with `semantic_search`, which Antfly embeds itself
  through the index's configured embedder. It falls back to full-text only
  against an older libantfly that rejects `semantic_search` on Lite handles.
  It then prints the entities and edges reached by traversing `knowledge`
  from the top hits.
- `dogfood entity "<name>"` prints the direct (both-direction) edges of a
  named entity node in the knowledge graph, e.g. `dogfood entity raft`.
- `dogfood status [-inference-url ...] [-extract-model ...]` prints inference
  status, the configured inference provider URL and its health/extract-model
  probe results, configured indexes/enrichments, and pending work stats.

The `-db`/`-limit` flags on `query` and `entity` work whether they appear
before or after the positional text/name argument (`main.go`'s
`reorderFlags` works around the standard `flag` package stopping flag
recognition at the first positional token).

## Index configuration

`dogfood` talks to the raw Lite C ABI (`antfly_db_add_index_json`) and uses
the same two-stage chunk-artifact pattern as the server (`go/pkg/docsaf`,
`antfly.NewArtifactEmbeddingIndexConfig`). Both enrichments travel inline on
the index that owns them and Lite registers them in dependency order before
admitting the index; see `index_config.go`.

1. `chunk_vectors` (`dense_vector`): `type: "embeddings"`,
   `sources: [{artifact: "doc_chunk_dense_v1"}]`, `dimension: 1024`,
   `distance_metric: "cosine"`, `embedder: {provider: "antfly", model:
   "Qwen/Qwen3-Embedding-0.6B-GGUF"}`, and two inline enrichments:
   - `doc_chunks_v1` (`chunk`, field `body`): `chunk_size`/`chunk_overlap`
     on a chunk enrichment are **byte** counts (the runtime's fixed byte
     slicer), so the token-aware `fixed` chunker is selected through
     `chunker_json` (`target_tokens: 400`, `overlap_tokens: 40`) and the byte
     counts are kept consistent at four bytes per token.
   - `doc_chunk_dense_v1` (`embedding`, `source_artifact_name:
     "doc_chunks_v1"`, `expected_dims: 1024`).
   Chunks are a queryable artifact; semantic hits carry
   `hierarchy.parent_doc_key`, which `query` uses to project a chunk back to
   its section before graph traversal.
2. `knowledge` (`graph`): the extraction-fed graph ("autograph") --
   `artifact.producer_json` runs the `extractor` producer (`provider:
   "antfly"`, `model: fastino/gliner2.5-base-v1`, `long_document: {mode:
   "window"}`) over each section body with the entity/relation schema above,
   `include_confidence`/`include_spans`, and a `pagerank` metric (retried
   once without `metrics` if the engine rejects it). Edges use the canonical
   extraction envelope (`$.relations[*]` with `source.entity_index` /
   `target.entity_index`).
3. `full_text_index_v0`: the table's default full-text index, provisioned by
   every Lite creation surface (C ABI, embedded package, `antfly lite init`).

`dogfood query` sends one `SearchJSON` request: `full_text_search` (match
form) merged by RRF with `semantic_search`, which Antfly embeds through the
index's configured embedder (in-process or the index's `api_url`).

## Known limitations

Measured on an Apple Silicon laptop against this repository's corpus
(1,206 markdown sections), in-process on Metal, 2026-09-18, with GLiNER2.5
base and windowed long documents:

| Step | Result |
|---|---|
| Ingest wall time | 14.1 min (drain 846 s) |
| Chunks embedded | 5,024 in 174 batches, 17.1 chunks/s |
| Sections extracted | 1,231 items in 176 batches, 1.8 sections/s (5 sections lost) |
| `query "how does VOPR fence strong reads"` | 8 RRF-merged hits, 44 entities, 51 edges |
| `entity "Raft"` | 4 edges (`implements`, `supersedes`, ...) |

The previous run with GLiNER2 base and inline chunking took 9.4 min; the
fp32 GLiNER2.5 checkpoint with windowed long documents is heavier per
section, and extraction is now the bottleneck.

What remains:

- **Extraction throughput.** The asset lane runs at ~1.8 sections/s
  in-process versus ~4.5 sections/s measured through the standalone server
  on the same sections; the two enrichment lanes also do not overlap fully
  (wall 846 s versus 678 s of extraction and 293 s of embedding). An fp16
  encoder for GLiNER2.5 matched fp32 on every fixture but is not yet
  qualified as a production row.
- **Five sections are still lost**: the enrichment runtime occasionally
  groups two documents into one extraction request, which the boundary
  model's contract (one document per request) rejects, and the resulting
  deterministic rejections are retried five times before giving up.
- Entity node names are the extractor's surface strings (for example
  `Raft`, `Full-cluster v42`), so `dogfood entity` needs the exact extracted
  text; there is no entity resolution step in this example.

## Files

- `main.go` -- CLI entry point, flags, DB open/create in remote-provider
  mode, inference/extractor preflight probes, index bootstrap
  (`ensureSchemaAndIndexes`).
- `index_config.go` -- schema and raw index JSON builders.
- `ingest.go` -- corpus walk (via `go/pkg/docsaf`'s `FilesystemSource` and
  `MarkdownProcessor`), document shaping, batched writes.
- `query.go` -- hybrid search (with full-text fallback), graph
  traversal/edge lookup, and the `entity` command.
- `smoke_test.go` -- a small test that adds the three indexes against a real
  `libantfly` and checks it is idempotent. It does not exercise ingest or
  query (those need a running `antfly inference run` and pulled models).
