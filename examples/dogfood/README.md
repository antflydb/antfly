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
  `documented_in`. An entity resolver canonicalizes every mention into a
  label-free `entity/<slug>` node (`entity/metadata_server`; the label stays
  a mention/document attribute so differently labeled mentions of one name
  converge), relations
  materialize as real entity->entity edges (owned by the producing section
  for retirement), and each section gets `mentions` edges to the canonical
  entities it references.
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
   equivalent is `zig build capi`; see `go/pkg/lite/README.md`).

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
   `long_document: {mode: "window"}`. An unqualified checkpoint answers
   `MODEL_NOT_QUALIFIED`.

   In-process inference budgets (host, backend, combined, KV, scratch) are
   derived from the host memory policy the way `antfly inference run` derives
   its defaults, and can be overridden on `lite.OpenOptions`.

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
  and drains chunk/embed/extract/resolution work with `RunUntilIdle`. When
  the indexes already exist their configuration is verified against this
  run's flags (see `verify.go`) and drift is a hard error naming `-reset`.
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
   once without `metrics` if the engine rejects it). The config also declares
   an `entities` **resolver** (`key_template: "entity/{{ slug _entity.text
   }}"`, `min_confidence: 0.5` to drop low-score junk), which Lite registers
   from the same
   `antfly_db_add_index_json` call. GLiNER relations reference entities
   positionally (`$.relations[*]` with `source.entity_index` /
   `target.entity_index`); the materializer resolves those positions against
   the resolver's canonical keys, so `A depends_on B` is a real
   `component/a -> component/b` edge (document-owned in the artifact key for
   retirement), withheld until the section's resolution artifact lands and
   re-rendered canonically by the resolution replay. `mention_edge_type:
   "mentions"` adds `doc:... --mentions--> entity/<slug>` provenance edges,
   which is what bridges search hits into the entity graph.
3. `full_text_index_v0`: the table's default full-text index, provisioned by
   every Lite creation surface (C ABI, embedded package, `antfly lite init`).

`dogfood query` sends one `SearchJSON` request: `full_text_search` (match
form) merged by RRF with `semantic_search`, which Antfly embeds through the
index's configured embedder (in-process or the index's `api_url`).

## Known limitations

Measured on an Apple Silicon laptop against this repository's corpus
(1,230 markdown sections), in-process on Metal, 2026-09-19, with GLiNER2.5
base and windowed long documents. The machine was also running other Zig
builds during this run, and the embedded inference node refused admission
52 times under that memory pressure (each refusal is retried), so the wall
time is an upper bound:

| Step | Result |
|---|---|
| Ingest wall time | 12.6 min (drain 757 s), no sections lost |
| Chunks embedded | 5,119 in 180 batches, 15.3 chunks/s |
| Sections extracted | 1,230 sections (1,277 attempts incl. admission retries) in 161 lane batches, 1.7 sections/s |
| `query "how does VOPR fence strong reads"` | 8 RRF-merged hits, 87 entities, 115 edges |
| `entity "Raft"` | 4 edges (`implements`, `supersedes`, ...) |

The two enrichment lanes now overlap fully: extraction (732 s) is the
whole drain, and the 335 s of embedding runs inside it. The previous run
with GLiNER2 base and inline chunking took 9.4 min; the fp32 GLiNER2.5
checkpoint with windowed long documents is heavier per section, and
extraction is the bottleneck.

Three defects this ingest exposed are fixed on the way (details in
`zig/pkg/inference/models/gliner2/GLINER25.md`, section 14): the
safetensors GLiNER2.5 checkpoint advertised a 128-item serial batch
contract, so the asset-producer batcher grouped sections into requests the
one-document boundary executor rejected (the "five lost sections" of
earlier runs); sections under about 600 bytes failed during invocation
planning because the planning budget scaled only with the request; and the
smallest real section (`zig/SCHEMA.md`'s "Related Docs", 20 bytes after
link stripping) was below the qualification rows' 26-byte floor, which is
now measured down to one byte.

What remains:

- **Extraction throughput.** The asset lane runs at ~1.7 sections/s
  in-process. Measured in isolation, the in-process provider entry runs at
  the same rate as the standalone HTTP server on a 40-section sample of
  this corpus, so the remaining gap to the ~4.5 sections/s seen through the
  server is GPU contention with the concurrent embedding lane, not the call
  path. An fp16 encoder for GLiNER2.5 converts deterministically and
  matches fp32 on 61 of 62 pinned confidence values, but one value misses
  the 5e-4 tolerance by about 18% on both backends, so it stays
  unqualified.
- Entity nodes are canonical label-free `entity/<slug>` keys minted
  deterministically by the resolver (`entity/raft`); `dogfood entity`
  slugifies a bare name into that namespace. Canonical entity *documents* are minted for
  the `entities` table by the promotion stage, which stays pending in
  embedded Lite (no cross-table entity sink); the graph topology, mention
  edges, and canonical keys do not depend on it.
- Re-ingesting without `-reset` reconciles changed sections but does not
  discover deleted files (path/heading-derived keys are upsert-only);
  deleting a stale section's document does retire its owned graph facts.

## Files

- `main.go` -- CLI entry point, flags, DB open/create in remote-provider
  mode, inference/extractor preflight probes, index bootstrap
  (`ensureSchemaAndIndexes`).
- `index_config.go` -- schema and raw index JSON builders.
- `ingest.go` -- corpus walk (via `go/pkg/docsaf`'s `FilesystemSource` and
  `MarkdownProcessor`), document shaping, batched writes.
- `query.go` -- hybrid search (with full-text fallback), graph
  traversal/edge lookup, and the `entity` command.
- `verify.go` -- drift verification for pre-existing indexes: re-running
  `ingest` with a different extractor, embedding model, chunk geometry, or a
  changed schema in `index_config.go` fails with a rebuild instruction
  instead of silently keeping the old configuration.
- `smoke_test.go` -- tests that add the three indexes against a real
  `libantfly`, check idempotence, and check that changed settings are
  rejected against an existing database. It does not exercise ingest or
  query (those need a running `antfly inference run` and pulled models).
