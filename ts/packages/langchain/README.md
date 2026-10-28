# Antfly for LangChain.js

Vector storage, text embeddings, native full-text/hybrid retrieval, and reranking
through `@antfly/sdk`. Node.js 24+. Connect to an existing Antfly server; this
package does not embed a database or download models.

## Release status

This is a release candidate. Release the SDK foundations in `@antfly/sdk` 0.2.0
or newer before releasing this package. Until then, use the local tarball verifier
below. After registry publication:

```sh
npm install @antfly/langchain @antfly/sdk @langchain/core
```

LangChain core support is `>=1.3.0 <2`. The lockfile pins the tested baseline;
weekly CI also checks latest compatible dependencies. ESM, CommonJS, and both
TypeScript declaration formats are checked from an independent npm consumer.
Browser, edge, and embedded runtimes are not qualified.

## First result

Start Antfly using its [installation guide](https://antfly.io/docs). Enable public
inference for `AntflyEmbeddings`. Select an installed text model from
`GET /ai/v1/models`. Use the same model for ingestion and querying.

```ts
import { AntflyClient, InferenceClient } from "@antfly/sdk";
import { AntflyEmbeddings, AntflyVectorStore } from "@antfly/langchain";

const baseUrl = process.env.ANTFLY_URL ?? "http://localhost:8080";
const headers: Record<string, string> = process.env.ANTFLY_TOKEN
  ? { Authorization: `Bearer ${process.env.ANTFLY_TOKEN}` } : {};
const client = new AntflyClient({ baseUrl, headers });
const embeddings = new AntflyEmbeddings({
  client: new InferenceClient({ baseUrl, headers }),
  model: process.env.ANTFLY_EMBEDDING_MODEL!,
});
const store = await AntflyVectorStore.fromDocuments([
  { id: "apples", pageContent: "Apples are stored in the cool orchard warehouse.", metadata: {} },
  { id: "owner", pageContent: "Mira Chen owns operations and handles shipment escalation.", metadata: {} },
  { id: "shipment", pageContent: "The Q3 shipment target is 120 crates.", metadata: {} },
], embeddings, { client, table: "langchain_quickstart", distanceMetric: "cosine" });
console.log((await store.asRetriever(1).invoke("Who owns operations?"))[0]);
// owner: Mira Chen owns operations...
await store.delete({ ids: ["apples", "owner", "shipment"] });
```

Run the example from the checkout:

```sh
# From ts/
pnpm install --frozen-lockfile
pnpm --filter @antfly/langchain... build
export ANTFLY_URL=http://localhost:8080
export ANTFLY_EMBEDDING_MODEL=your-installed-text-model
pnpm --filter @antfly/langchain exec tsx examples/quickstart.ts --cleanup
```

These three documents and the expected result match the Python quickstart.
Cold server/model setup is separate from warm first-result timing. This tiny
fixture is a functional check, not a broad retrieval-quality benchmark.

## Resource and vector contracts

- Constructors attach without network calls or creating resources. `validate()`
  checks the existing table/index. Supplied clients remain caller-owned.
- `create(embeddings, options)` creates a new table and fails if it exists. Index
  creation can wait until ingestion supplies the dimension.
- `fromDocuments` / `fromTexts` create missing resources and reuse compatible ones.
  They never drop/recreate resources. Empty inputs perform no ingestion.
- Any standard LangChain `EmbeddingsInterface` works; no Antfly model attributes
  are required. Dimension checks do not prove embedding model identity.
- Explicit `ids` override document IDs; otherwise IDs are generated. Reusing an
  ID replaces its record. Duplicate IDs within one call are rejected.
- `delete({ ids })` requires explicit IDs and never drops tables. Read filters
  are not mutation authorization: server credentials govern writes.
- `getByIds()` preserves first-occurrence input order and omits missing records.
  Authentication/server failures are not treated as missing documents.
- Metadata must be JSON. `_antfly` is reserved for retrieval provenance.

Vector stores require external dense indexes. Creation defaults to cosine;
attachment checks explicitly requested dimensions/metric. Scored search returns
**raw dense distance, lower is better**, never fused hybrid scores. MMR uses
cosine similarity over candidate vectors for diversity.

```ts
const existing = new AntflyVectorStore(embeddings, { client, table: "reports" });
const scored = await existing.similaritySearchWithScore("shipping", 5,
  { term: "handbook", field: "metadata.source" });
const diverse = await existing.asRetriever({
  searchType: "mmr", k: 3, searchKwargs: { fetchK: 20, lambda: 0.5 },
}).invoke("shipping");
```

## Native retrieval and reranking

```ts
import { AntflyRetriever, AntflyReranker } from "@antfly/langchain";
const reranker = new AntflyReranker({
  client: new InferenceClient({ baseUrl, headers }), model: "your-installed-reranker",
});
const retriever = new AntflyRetriever({
  vectorStore: existing, mode: "hybrid", k: 4, fetchK: 20, reranker,
  filter: { term: "handbook", field: "metadata.source" },
});
const documents = await retriever.invoke("Who handles shipments?", {
  signal: AbortSignal.timeout(30_000), tags: ["handbook-search"],
});
```

Modes are `text`, `dense`, and `hybrid` (RRF). Fixed store/retriever filters and
per-call `filter` intersect. Callbacks, tags, metadata and runnable composition
are preserved; concurrent invocations have independent scope.

For an existing managed embedding index, pass `client`, `table`, and `index`
instead of `vectorStore`; Antfly embeds the query. Managed indexes are not
provisioned here. Dense/hybrid retrieval rejects sparse indexes and external
indexes without a provider. Text mode needs no embedding model.

`AntflyReranker` extends `BaseDocumentCompressor`, composes with standard
`ContextualCompressionRetriever`, and exposes `rerank(query, documents)` and
`asRunnable().invoke({ query, documents })`. Scores map by input index; ties retain
input order. IDs/content/metadata survive, with scores under `metadata._antfly`.

## Reliability

Writes default to `sync_level: full_index` and require explicit `committed` status.
`wait: false` accepts `committed_pending`; repair-required outcomes always throw.
`WriteOutcomeError` exposes `status`, affected-batch `keys`, earlier successful
`committedKeys`, and `details`. Mutations are never automatically retried.
Cancellation or a lost response does not prove rollback; reconcile before replay.

Embedding/write requests use bounded batches (default 64 records); the SDK also
bounds write bytes. Multiple batches are not atomic together. Oversized records
fail rather than being truncated. Embeddings use LangChain's concurrency limiter,
no implicit retries, separate retrieval-document/query tasks, fail-fast item
policy, optional dimensions, and optional `queryInstruction`.

Native retriever calls, embedding methods, explicit writes and the reranker
runnable accept `AbortSignal`. Third-party embedding calls may not be cancellable;
subsequent Antfly work checks cancellation. Standard compressor calls have no
signal parameter; use the reranker runnable when cancellation is needed.

## Persistent Q&A

```sh
# From ts/, with the environment above. ANTFLY_TABLE optionally selects a table.
pnpm --filter @antfly/langchain exec tsx examples/knowledge-base.ts ingest
pnpm --filter @antfly/langchain exec tsx examples/knowledge-base.ts ask 'Who owns operations?'
# Restart the server on the SAME data directory, then:
pnpm --filter @antfly/langchain exec tsx examples/knowledge-base.ts verify
pnpm --filter @antfly/langchain exec tsx examples/knowledge-base.ts delete owner
```

Ingestion upserts stable IDs; asking reads persisted evidence. Optional
`ANTFLY_RERANKER_MODEL` enables reranking. Default mode is evidence-only. For real
generation, set `OPENAI_API_KEY` and `OPENAI_MODEL`, then append `--generate`.
This explicit opt-in may incur charges. The standard LangChain prompt/model/parser
chain rejects citations outside retrieved IDs; citation validity alone does not
prove factual correctness. OpenAI/classic are example/test development dependencies.

## Validation and distribution

```sh
pnpm --filter @antfly/langchain... typecheck
pnpm --filter @antfly/langchain... test
pnpm --filter @antfly/langchain lint
pnpm --filter @antfly/langchain test:package
# Dedicated server and installed embedding/reranking models required:
ANTFLY_LIVE=1 pnpm --filter @antfly/langchain test
```

Live tests create/remove uniquely named tables. Build before `test:package`: it
packs SDK/integration, installs them into `.state/langchain/consumer-*` outside
the workspace, and checks ESM/CJS/types. `ANTFLY_URL` also enables real-server
retrieval/deletion. All other dependencies resolve from npm.

See [VALIDATION.md](VALIDATION.md) for executed evidence. Distribution uses the
`ts/antfly/langchain/v*` tag workflow after npm ownership/Trusted Publishing are
configured. Initial publication may require registry-owner bootstrap. Release
and verify the SDK foundation first. Upstream discovery is a separate
[integration submission](https://docs.langchain.com/oss/javascript/contributing/publish-langchain).

## Persistent stores, graph and multimodal retrieval

`AntflyJSONStore`, `AntflyDocumentStore`, and `AntflyByteStore` implement LangChain's
`BaseStore<string, V>`. Constructors attach; inherited `create(options)` explicitly
creates a table and returns the selected store class. Use a separate namespace for
each codec. `mget` preserves order and returns `undefined` for missing keys; missing
tables remain errors. Writes validate all inputs, use bounded batches and wait for
index visibility. `yieldKeys(prefix)` uses cursor pagination. JSON codecs reject
lossy values; document codecs preserve IDs; byte codecs validate base64 and default
to an 8 MiB value limit. Write errors include physical keys and earlier committed
keys. A batch is not a transaction across its network requests.

```ts
const parents = new AntflyDocumentStore({ client, table, namespace: "parents" });
const media = new AntflyByteStore({ client, table, namespace: "original-media" });
await parents.mset([["page-1", new Document({ id: "page-1", pageContent: "Original text" })]]);
```

Pass `parents` as `docstore` to upstream `MultiVectorRetriever`. Its `byteStore`
option stores serialized **documents**, so use a distinct byte-store namespace
for that option and keep original images in `media`.

`AntflyGraphStore` accepts structurally compatible `GraphDocument` objects. It
uses source-owned contributions and optimistic transactions to preserve shared
nodes on replacement/deletion. Graph queries accept Antfly JSON, with `$param`
objects for bound values. Supported operations are MATCH, traversal, shortest
paths, k-shortest paths and exact counts. Every alias/path is constrained to the
configured table and namespace; optional MATCH, arbitrary WHERE expressions and
multi-hop MATCH edges are rejected. Explicit one-hop aliases remain available.
`expandDocuments` preserves seed evidence and adds bounded graph neighbors.
`createAntflyGraphTools` and `createAntflySearchTool` expose validated LangChain tools.
The caller is responsible for choosing trusted tenant scopes.

`AntflyMultimodalCorpus` persists summary vectors, original parent documents and
image bytes. It uses deterministic revision IDs, checks image hashes before
generation, rejects missing/stale references, and supports replacement/deletion.
Multi-store ingestion is not atomic: after interruption rerun the same source;
concurrent writes to one source must be serialized by the application. Orphaned
records from an interrupted revision can require application cleanup.
`AntflyEmbeddings.embedMultimodal` additionally supports native multimodal models;
select and install a capable model explicitly.

From this package directory, after building:

```sh
pnpm exec tsx examples/graph.ts create
pnpm exec tsx examples/graph.ts query
pnpm exec tsx examples/multimodal.ts ingest
# Can run after restarting Antfly; verify reads no local fixture files:
pnpm exec tsx examples/multimodal.ts verify
pnpm exec tsx examples/multimodal.ts ask 'What is the chart audit marker?'
# Explicit opt-in using OPENAI_MODEL and OPENAI_API_KEY:
pnpm exec tsx examples/multimodal.ts ask 'What is the chart audit marker?' --generate
pnpm exec tsx examples/multimodal.ts delete
```

Set `ANTFLY_URL` and `ANTFLY_EMBEDDING_MODEL` as in the quickstart. The bundled
synthetic PDF, two original PNG figures, extracted text and declared summaries
come from the Apache-2.0 Python-suite fixture. This example uses pre-extracted
fixture text; it is not a general PDF parser. Verification measures retrieval and
media integrity; it does not claim vision-answer quality. Generation accepts a
standard LangChain chat model and validates citation IDs.

Long-term memory lives in the separate [@antfly/langgraph-store](../langgraph-store/README.md)
package because LangGraph's BaseStore has a different contract.

Chat-model adapters, execution checkpointing, per-item TTL, arbitrary PDF parsing,
and embedded-runtime support are outside this release.
