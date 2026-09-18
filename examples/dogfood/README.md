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
   antfly inference pull antflydb/gliner2-base-v1
   ```

   `fastino/gliner2.5-base-v1` (a bare HuggingFace `owner/name` reference,
   same form as the Qwen3 pull above; an explicit `hf:` prefix also works) is
   the target extractor and pulls successfully, but as of this writing the
   runtime intentionally keeps GLiNER2.5 gated off for serving until a
   production qualification row lands
   (`zig/pkg/inference/src/models/gliner_boundary_qualification.zig`) --
   `POST /ai/v1/extract` answers `INVALID_MODEL: model does not support
   entity extraction`. `dogfood` defaults `-extract-model` to
   `antflydb/gliner2-base-v1`, which is fully qualified and works end to end
   against a local `antfly inference run`. Switch back to
   `-extract-model fastino/gliner2.5-base-v1` once its qualification row
   lands.

4. `dogfood ingest` fails fast before doing any work: in-process mode it
   checks that the linked `libantfly` advertises `local_inference_runtime`;
   in remote mode it probes `GET /healthz` and runs a one-sentence
   `POST /ai/v1/extract` against `-extract-model` so a gated model surfaces the
   server's own error (for GLiNER2.5, `INVALID_MODEL` above) instead of
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

`dogfood` talks to the raw Lite C ABI (`antfly_db_add_index_json`), not the
network `CreateIndexRequest` API, so the JSON shapes below use the storage
engine's own field names rather than the public OpenAPI schema names. Two
differences worth calling out explicitly (found by testing against a real
`libantfly` build, see `index_config.go`):

- The raw `dense_vector` index kind uses `dims` and `metric`, not the public
  API's `dimension` and `distance_metric`.
- `dogfood` originally tried the artifact-indirection pattern used by
  `go/pkg/docsaf/cmd/docsaf` (an inline `chunk` enrichment producing a
  `doc_chunks_v1` artifact, consumed by the embeddings index via
  `"sources": [{"artifact": "doc_chunks_v1"}]`). That JSON parses cleanly
  against the engine's config parser, but `antfly_db_add_index_json`
  consistently returned a generic `ANTFLY_INTERNAL` error for every
  `sources`-based `dense_vector` config tried (including the two-stage
  chunk-enrichment + embedding-enrichment form docsaf uses), regardless of
  field/embedder details. This looks like artifact-sourced dense-vector
  indexes are not yet wired up for Lite's native profile. `dogfood` instead
  uses the inline `chunker` field on the `chunk_vectors` index itself (the
  same pattern `examples/epstein/main.go`'s `createEmbeddingIndexConfig`
  uses against full Antfly): Lite chunks each document at write time and
  embeds every chunk, without exposing the intermediate chunks as a
  separately queryable artifact. Lexical search still runs against the
  default full-text index over whole section bodies, unaffected by this.

The three indexes `dogfood` builds:

1. `chunk_vectors` (`dense_vector`): `field: "body"`, `dims: 1024`,
   `metric: "cosine"`, `embedder: {provider: "antfly", model:
   "Qwen/Qwen3-Embedding-0.6B-GGUF", api_url: <inference-url>}`, `chunker:
   {provider: "antfly", model: "fixed", api_url: <inference-url>, text:
   {target_tokens: 400, overlap_tokens: 40}}`.
2. `knowledge` (`graph`): a single artifact source/producer pair (the
   "autograph" pattern) -- `artifact.producer_json` runs the `extractor`
   producer (`provider: "antfly", model: <extract-model>, api_url:
   <inference-url>`) over each document's `body`, with the entity/relation
   schema described above, `include_confidence`/`include_spans` enabled, and
   a `pagerank` graph metric. If the engine rejects the `metrics` field,
   `dogfood` logs a warning and retries once without it.
3. `full_text_index_v0`: the table's default full-text index, matching
   `zig/pkg/antfly/src/api/full_text_indexes.zig`'s
   `default_full_text_index_name` constant. Current libantfly builds
   auto-provision this on `Create`, matching the full server; `dogfood`
   checks `IndexesJSON` first and only adds it explicitly (over `body`) as a
   fallback for older builds that do not.

`dogfood query` merges full-text and semantic search with
`"merge_config": {"strategy": "rrf"}` in one `SearchJSON` call
(`"full_text_search"`, `"full_text_index": "full_text_index_v0"`,
`"semantic_search"`, `"indexes": ["chunk_vectors"]`). Hits that come from the
chunked embedding index carry a `hierarchy.parent_doc_key` field (see
`specs/openapi/antfly/metadata.yaml`'s `QueryHit`/`QueryHitHierarchy`), which
`dogfood` uses to project each chunk hit back to its source document before
deduplicating and traversing the graph -- no separate merge step is needed.

## Known limitations

Measured on an Apple Silicon laptop against this repository's corpus
(1196 markdown sections), in-process on Metal, 2026-09-17:

| Step | Result |
|---|---|
| Ingest wall time | 9.4 min (drain 562 s) |
| Chunks embedded | 4,978 in 173 batches, 13.3 chunks/s |
| Sections extracted | 1,196 in 150 batches, 7.9 sections/s |
| `query "how does VOPR fence strong reads"` | 8 RRF-merged hits, 2 entities, 2 `tested_by` edges |

What remains:

- **Throughput is below the direct baseline.** Called directly through the
  inference server the same models do ~23 chunks/s and ~13 sections/s, so the
  corpus would take ~5 minutes. Lite's enrichment runtime runs the extract
  stream and the embed stream strictly one after the other per ~64-item
  window (`flushDeferredGeneratedWork` in
  `zig/pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig`), which
  both serializes the two models and lowers per-item throughput. Running the
  two streams concurrently needs care around crash-idempotent replay and
  belongs in a change with VOPR coverage.
- `fastino/gliner2.5-base-v1` is gated off for serving pending a
  qualification row (see Prerequisites).
- Artifact-sourced dense-vector indexes (`sources: [{artifact: ...}]`, the
  server's chunk-artifact pattern) fail with `ANTFLY_INTERNAL` in Lite's
  native profile, so `chunk_vectors` chunks inline via its `chunker` field
  and the chunks are not a separately queryable artifact.
- Lite's raw `dense_vector` config uses `dims`/`metric`, not the public
  schema's `dimension`/`distance_metric`.
- `antfly lite init` still does not provision `full_text_index_v0` (only the
  C ABI and embedded facade do), and `antfly lite query` cannot open a
  binding-created file (`IdentityNamespaceMismatch`).
- Entity node names are the extractor's surface strings (for example
  `Full-cluster v42`), so `dogfood entity` needs the exact extracted text;
  there is no entity resolution step in this example yet.

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
