# Enrichments

This is the canonical enrichment architecture note for Antfly. It covers the
cross-cutting contract between public artifact identity, storage-side enrichment
artifacts, derived replay/indexing, and serverless publication.

The shared direction is:

- document and artifact rows are the durable source of truth
- replay carries ordering, changed identities, and coarse hints, not payloads
- derived consumers own their apply/publish durability before advancing
  watermarks
- artifact identity exposed to users is stable and opaque, not an internal
  storage key
- serverless enrichment is asynchronous publication work, not a query-time
  dependency

## Artifact Identity And Lookup

This repo has two artifact identity layers:

- Internal storage keys: binary LMDB/DocStore keys. These are private
  implementation details.
- Public artifact identity: `ArtifactRef` plus an opaque `artifact_id` token.

Public contract:

- `ArtifactRef` is the canonical external artifact identity.
- `artifact_id` is a stable round-trippable convenience token.
- Clients must not parse storage layout out of any internal key bytes.
- Public APIs must not expose `artifact_key` or internal LMDB keys.

`ArtifactRef` currently includes:

- `document_id`
- `name`
- `kind`
- `chunk_id` for chunk artifacts
- `source` for derived embeddings

Transport rules:

- JSON/C API surfaces expose `artifact_ref` directly and base64-encode raw byte
  fields at the transport edge.
- Packed search wire responses intentionally return only opaque hit IDs plus
  scores. Clients that need artifact structure should decode the returned
  `artifact_id` token or use JSON search.

Lookup rules:

- Search hit `id` is the public identifier.
- Primary document hits use the raw document ID.
- Artifact hits use the opaque public `artifact_id`.
- Artifact lookup resolves the public token back to an `ArtifactRef`, then maps
  that to the internal key inside the DB layer.

Current helpers:

- `go/pkg/antfly/src/storage/db/artifact_ids.zig` encodes and decodes public artifact IDs and
  reconstructs internal keys from `ArtifactRef`.
- `DB.getArtifact` looks up a stored artifact by public artifact ID.
- `antfly_db_decode_artifact_id_json` decodes a public artifact ID to
  `artifact_ref`.
- `antfly_db_lookup_artifact_json` loads a stored artifact by public artifact ID
  and returns value plus `artifact_ref`.

## Derived Replay And Indexing Contract

"Derived" means every index or worker that consumes committed
document/artifact changes after the primary write: dense HBC, full-text,
sparse, graph, and generated enrichment.

The public write contract should not depend on derived index work. For
`sync_level=write`, the request should durably commit primary document/artifact
data plus the thin replay/change-journal record, then return. Derived indexes
must catch up from that replay stream in the background.

This boundary applies to more than dense/HBC. Dense made the issue obvious
because HBC publish can spend seconds in split/build/publish work at large
index sizes, but full-text segment building, sparse posting updates, graph
maintenance, and generated enrichment have the same architectural boundary:

- writes produce durable replay debt
- indexes consume replay debt independently
- queries see only fully published index state
- status reports both accepted target and query-visible/published progress

Without this boundary, background indexing can still behave like foreground
work. It can hold apply/index locks, cause large LSM flush/compaction bursts,
starve health/status/metrics, or make public write latency depend on derived
maintenance.

Every derived consumer should have the same outer contract:

1. The writer commits primary rows and one thin replay record.
2. The writer updates/notifies the derived target sequence.
3. A background derived catch-up scheduler chooses bounded work windows.
4. The index applies replay into private or bounded mutable state.
5. The index publishes atomically at window boundaries.
6. Status is updated from published facts, not by doing live index work.
7. Replay retention follows the minimum persisted applied watermark.

Index-specific work stays behind that interface:

- Dense/HBC may use bulk routing, deferred leaf splits, and atomic root publish.
- Full-text may build bounded segment batches and publish segment manifests.
- Sparse may batch posting-list updates and publish sparse manifests.
- Graph may batch edge updates and publish graph metadata.
- Enrichment may generate artifacts and append downstream replay refs.

Scheduler rules:

- one worker state per managed derived index
- accepted target sequence is cheap to update
- applied/published sequence advances only after an atomic publish
- windows are bounded by records, bytes, elapsed work, and resource-manager
  pressure
- writer and control-plane traffic get priority under pressure
- catch-up can pause or shrink windows without losing durable replay debt
- `.full_index` waits for the relevant published sequences
- `.write` does not wait for derived publish

The current implementation moves catch-up policy out of dense-specific executor
code and applies bounded replay windows through a shared derived-index policy.
Dense keeps its HBC-specific begin/finish implementation, but bounded catch-up
scheduling is no longer dense-only.

It also separates dense bulk-session deferral from non-dense executor progress.
When an external dense bulk session is open, weak sync levels defer dense worker
notification but still wake full-text, sparse, graph, and enrichment-derived
workers. `.full_text` remains target-specific and does not wait behind dense
bulk work.

The next cuts should:

1. Replace writer-cache driven dense auto-bulk finish with coordinator-owned
   catch-up windows.
2. Add elapsed-time/resource-pressure checks between windows, not just record
   and byte limits.
3. Move full-text segment publish behind the same begin/apply/finish lifecycle.
4. Make status expose per-index accepted/applied/published progress from the
   coordinator.
5. Add guardrails proving health/status/metrics remain responsive during 1M
   writes and catch-up.

## Serverless Publication Model

Async enrichers fit the serverless model well. Synchronous enrichers fight the
model. Enrichment should be modeled as background publication work, not a
query-time dependency.

The serverless path assumes:

- writes land in WAL first
- publication happens later
- published state is immutable
- newer namespace versions replace older visible versions

That matches async enrichment naturally:

1. append raw document mutation to WAL
2. make base state publishable without waiting on external enrichment
3. run enrichment jobs asynchronously
4. publish a newer manifest version containing the enriched result

This keeps ingest latency low and keeps external dependencies out of the query
critical path.

Current staged enrichment path:

- `lexical_sparse`: may use a configured sparse model, or fall back to
  deterministic lexical sparse derivation
- `chunk_preview`: deterministic only
- `chunk_embeddings`: may use a configured dense model, or fall back to
  deterministic chunk embeddings
- `rerank_terms`: deterministic only

Model-backed stages have explicit per-namespace policy:

- `deterministic_only`
- `prefer_model`
- `require_model`

The runtime has explicit failure behavior:

- `skip_document`
- `fail_stage`

The intended contract is:

- `prefer_model` uses the configured model when available and falls back when
  the model is missing or the model call fails.
- `require_model` does not silently degrade and surfaces a stage error instead.
- `skip_document` keeps stage progress moving around bad documents.
- `fail_stage` stops the stage for that maintenance tick and surfaces the
  failure.

This is important for operator behavior, because it lets one namespace tolerate
fallback while another namespace insists on model-backed outputs.

Enrichers must not:

- block query execution
- block head publication on slow third-party calls
- mutate local worker-owned state directly
- rely on shard ownership or replica-local transactions
- require distributed synchronous transactions for correctness

Recommended enrichment model:

- Use enrichment as a background build stage.
- Base publish should produce raw document state, basic searchable
  representation, and enough metadata for the document to exist immediately.
- Enrichment workers should consume namespace, WAL range or source document
  version, and enrichment policy/pipeline config.
- Enrichment workers should produce derived fields, chunks, embeddings,
  extracted metadata, and normalization outputs.
- Those results should publish as a later namespace version.

The safest first serverless representation is:

- `enrichment output -> derived mutation events`

That means enrichers append derived updates back into the same logical
namespace-change stream instead of inventing a second visibility model. This
keeps one replay model, one publication model, one consistency surface for
readers, and simpler idempotency/retry behavior.

Later, if needed, the system can move toward separate derived artifact streams,
explicit enrichment generations, and dependency-aware manifest composition. That
should come after the simpler model works.

The architecture should make it explicit that documents may be:

- base-published
- enriched-published

Readers should not infer enrichment completeness from mere document existence.

Recommended API-level framing:

- `published`: latest visible manifest only
- `latest`: published manifest plus bounded WAL overlay

If enrichment is eventually consistent, those views should make it clear that a
document may be queryable before enrichment completes, and a later publish may
improve that document's searchable/enriched shape.

Async enrichment only works well if repeated work is safe. Enrichment jobs
should be idempotent with respect to:

- namespace
- source version or source WAL range
- enrichment pipeline version
- document identity

That way duplicate workers are wasteful but not corrupting, retries after
crashes are safe, and publish races remain controlled by manifest/progress CAS.

The maintenance runtime tracks:

- enriched documents
- WAL appends from enrichment
- model-backed enrichment documents
- fallback documents
- failed documents
- stage failures

Those signals are first-class operator inputs. A namespace with persistent
fallback or repeated stage failures is not healthy in the same way as a
namespace whose configured model-backed pipeline is completing as expected.

Today the serverless path still publishes:

- mutation segments
- document snapshots

See:

- `pkg/antfly/src/serverless/build/builder.zig`
- `pkg/antfly/src/serverless/query/runtime.zig`
- `pkg/antfly/src/serverless/query/materializer.zig`

That is enough to prove publication mechanics, but it is not yet the final
steady state for enrichment-heavy retrieval.

Before enrichers become a major runtime feature, the serverless path should
gain:

1. real indexed query execution
2. local query cache
3. compaction into optimized published artifacts
4. stronger multi-process publish/prune hardening

Recommended next serverless enrichment steps:

1. Keep enrichers asynchronous and publication-driven.
2. Model first enrichment outputs as derived mutation events.
3. Batch enrichment publication rather than publishing every single enriched
   delta immediately.
4. Add explicit enrichment freshness/completeness semantics to query results
   only after the indexed read path exists.
5. Revisit separate derived artifact streams only once the base query/cache and
   compaction model is stable.

## Storage DB Enrichment Artifacts

This tracks the storage-side enrichment work under `storage/db/enrichment/`.
The target behavior should match the Go implementation's shape for chunks and
vectors: one Pebble/LSM row per enrichment artifact, with the row value carrying
the source hash needed to skip unchanged work.

## Current Gap

Zig now persists source hashes for dense and sparse embedding artifacts and
uses them to skip unchanged whole-document and chunked embedding work in the
async enrichment path. Stale async chunk cleanup deletes stale artifact rows and
emits dense/sparse index deletes for stale chunk vector identities. The
remaining gap is broader coverage: chunk artifacts are still JSON rows, the
synchronous precompute path still needs the same targeted cleanup/skip behavior
as the async worker, and we still need metrics around skip/provider behavior and
artifact bytes by kind.

There is also still a rebuild-source mismatch on reopen/startup: the current
dense "rebuild from stored embedding artifacts" path scans the full primary
docstore keyspace and then filters down to artifact keys. That is not the
intended architecture. Rebuild/catch-up for enrichment-backed indexes must scan
only the prefix ranges for the artifact families they own.

## Better Replay Architecture

The better long-term shape for this codebase is:

- `DocStore` is the only durable source of truth for document and artifact data
- a dedicated thin replay stream carries ordering plus changed identities only
- per-consumer applied watermarks drive replay and retention
- primary-store scans are reserved for backfill/rebuild/bootstrap, not
  steady-state replay

The replay stream must not become a second payload store. It should carry only:

- `sequence`
- `target_hints`
- changed document keys
- deleted document keys
- changed artifact keys

Derived workers then rehydrate the current concrete input from `DocStore` at
apply time.

The core problem with the current primary-replay-row direction is that it makes
steady-state derived catch-up depend on generic primary-store iteration. That
is the wrong hot path. The hot path should be:

- append thin replay record
- consume replay in sequence order
- point-read the touched docs/artifacts from `DocStore`

It should not be:

- scan primary-store replay rows as if the primary KV were the replay system
- or rescan the primary store outside explicit backfill/rebuild/bootstrap flows

### Why We Are Doing This

We need replay to look more like a Pebble/LSM iterator and less like a WAL
scanner.

The practical benefits are:

- replay consumers can `SeekGE(applied_sequence + 1)` and scan forward
- replay becomes block-cache friendly and ordered
- unrelated target kinds are skipped by key order, not by payload parsing
- catch-up windows stay bounded in memory without paying raw-log scan cost
- retention becomes explicit and consumer-driven

This applies to more than dense indexes. The same replay layer should support:

- enrichment workers
- dense indexes
- sparse indexes
- full-text
- graph
- any future derived consumer

### Correct Boundary

The correct steady-state boundary is:

1. primary write commits document/artifact data into `DocStore`
2. the same write appends one thin replay record to the replay stream
3. derived consumers read that replay stream in sequence order
4. derived consumers fetch current rows/artifacts from `DocStore` by identity
5. replay retention follows the minimum applied watermark across consumers

Primary-store scanning should happen only when:

- a new index is created
- an index is rebuilt or repaired
- a shard/bootstrap/split/restore flow needs a full durable scan

That boundary is stricter than the current tree. The primary store may still
carry compatibility replay rows during migration, but they are not the target
steady-state replay surface.

### Rollout Plan

1. Make the thin replay log authoritative again for steady-state replay.
   - enrichment
   - dense
   - sparse
   - full-text
   - graph

2. Restrict primary-store replay/state scans to backfill-style work only.
   - new index creation
   - rebuild/repair
   - bootstrap/split/restore

3. Add explicit replay-floor accounting per consumer.
   - Retain replay records until every interested consumer has advanced past
     them.

4. Delete compatibility replay surfaces once cutover is complete.
   - No duplicate steady-state replay path in the primary KV.

Go avoids that by storing a hash with each enrichment output:

- Dense embedding row: `[hashID:uint64][vector]`.
- Sparse embedding row: `[hashID:uint64][sparse_vector]`.
- Chunk row: `[hashID:uint64][chunk_json]`.
- Asset artifact row: `[hashID:uint64][payload]`.

Before generating new output, Go renders/extracts the source prompt, computes
the prompt hash, reads the existing enrichment row, and skips the operation when
the stored hash matches.

## Target Semantics

- One row per enrichment/embedding artifact.
- The artifact key remains the stable document-adjacent enrichment identity.
- The artifact value is binary for vectors and versioned for every enrichment
  kind that needs source-hash skip behavior.
- The stored hash is computed from the actual text/prompt/content that feeds the
  enrichment stage, not from the whole document unless the whole document is the
  configured source.
- A write that does not change the configured source hash must not call the
  embedder/chunker and must not rewrite downstream dense/sparse/full-text state.
- Ordinary document updates must keep existing enrichment artifact rows long
  enough for the worker to compare source hashes. If the write path deletes the
  artifact before the worker runs, unchanged-source updates are forced to
  re-enrich.
- Document deletes still delete all enrichment artifact rows for that document
  and emit derived deletes for downstream indexes.
- Chunked vectors keep parent-delete semantics: deleting the parent document
  deletes chunk artifacts and their embedding artifacts.
- Normal dense vector `doc_key` remains the vector identity. Chunked vectors use
  the chunk artifact key as the vector identity.

## Generated Replay Shape

The current Zig storage shape still has a separate derived replay log, but it
should behave much closer to the Go design:

- Primary document and artifact rows are the source of truth.
- The derived replay stream should carry thin output identities, not full
  documents, chunk payloads, or embedding payloads.
- Replay-time workers should rebuild the concrete generation request from the
  committed document plus the current index/enrichment catalog.
- If the document was deleted or the current catalog no longer wants that
  generated output, the replay ref should no-op.

The intended end state is stronger than the current thin-log slice:

- one durable primary commit path,
- per-index applied sequence watermarks,
- derived workers consuming committed sequence ranges,
- no second payload-heavy replay system.

The first migration slice implemented here is narrower:

- the derived log still exists,
- generated enrichment entries are now thin refs:
  - `kind`
  - `index_name`
  - `artifact_name`
  - `embedding_name`
  - `doc_key`
- the enrichment worker reloads the committed document and calls
  `planGeneratedEnrichments(...)` to reconstruct the current concrete request
  before doing chunk/dense/sparse work.

This keeps replay bounded to stable identities and removes the previous
"second payload store" behavior for generated chunk/dense/sparse work.

Tradeoff in this migration slice:

- replay reconstruction uses the current catalog, not a historical snapshot of
  generator config at write time.
- In practice that means removed/changed generator config naturally drains or
  no-ops old pending refs instead of replaying stale work. That matches the
  direction we want, but it is not yet the full sequence/watermark model.

## Artifact-Scoped Rebuild Contract

Any rebuild or reopen repair path for enrichment-backed indexes must enumerate
only the artifact/chunk prefixes relevant to that derived index family. It must
not scan the full primary document keyspace and then decode/filter unrelated
rows.

That applies to:

- dense embedding indexes: scan dense embedding artifact rows only
- sparse embedding indexes: scan sparse embedding artifact rows only
- chunk-backed enrichments: scan chunk artifact rows only
- graph or other enrichment-derived indexes: scan only their durable derived
  artifact/state prefix, if they have one

The embedder/chunker side should follow the same rule:

- chunk rebuild should enumerate chunk artifact prefixes, not whole documents
- dense rebuild should enumerate embedding artifact prefixes, not whole
  documents
- sparse rebuild should enumerate sparse artifact prefixes, not whole documents

Fallback to document-key scans is only acceptable for index families that do
not yet have a durable artifact/state source. That fallback should be explicit
and temporary, not hidden behind artifact-oriented method names.

### Required Follow-Up

1. Add internal-key range helpers for artifact families.
   - Lower/upper bounds for embedding, chunk, asset, and any other
     enrichment-owned artifact namespaces.

2. Add docstore/store scan APIs that enumerate those artifact ranges directly.
   - Rebuild should consume artifact-only scans, not `documentRangeLowerAlloc("")`.

3. Convert dense rebuild to true artifact-prefix enumeration.
   - The current `rebuildDenseIndexesFromStoredEmbeddingArtifacts*` path still
     scans the full store and filters artifact rows afterward. Fix that first.

4. Convert sparse/chunk/other enrichment-backed rebuilds onto the same pattern.
   - Keep one shared artifact-range abstraction instead of one-off scan logic
     per index type.

5. Add contract tests for each family.
   - Dense rebuild does not touch unrelated document rows.
   - Chunk rebuild enumerates chunk rows only.
   - Sparse rebuild enumerates sparse artifact rows only.
   - Startup/reopen status reports the actual artifact-backed rebuild phase and
     progress against that source.

## Longer-Term Generated Replay Plan

The current replay rows are already thin on disk, but the producer side still
does too much work before it appends them. We still materialize a full
`DerivedBatch` on the request path even when `.write`/`.propose` only needs:

- primary durability
- a replay sequence
- a thin replay record for async consumers

That is the main remaining architectural gap with the Go path.

### Replay-Thinning Task List

1. Producer fast path for non-blocking index sync levels.
   - For `.propose` and `.write`, stop building a full
     `DerivedBatch` when the request will only append replay and return.
   - Append a thin replay record directly from the extracted write/artifact
     state instead.
   - Status: implemented

2. Keep stronger sync levels on the full materialized path.
   - `.full_text`, `.enrichments`, and `.full_index` still need the existing
     richer `DerivedBatch` path until their wait/apply semantics are split out.
   - Status: implemented for the first replay-thinning slice

3. Split replay producers by consumer cost.
   - Dense/full-text/graph/enrichment should not all require the same request
     time shaping.
   - Build the minimum replay envelope needed for each target family.
   - Status: implemented for the indexed replay-row producer. The all-lane
     row keeps the canonical record, but hint lanes now store consumer-specific
     records: enrichment gets doc identities, full-text gets doc/delete keys,
     dense/sparse get doc/delete keys plus embedding artifacts, and graph gets
     delete keys plus graph artifacts.

4. Move more hydration to replay consumers.
   - Replay records should carry identities and hints.
   - Consumers should load current docs/artifacts when applying instead of
     expecting a fully precomputed request-time payload.
   - Status: implemented for the current replay-thinning slice. Full-text
     replay windows drop artifact identities before building their apply batch,
     while dense/sparse/graph retain only the artifact families they can
     hydrate. Replay document hydration uses the current-tip probe transaction
     instead of a cloned read snapshot.

5. Split apply ownership by index.
   - Keep DB-global coordination for shared metadata only.
   - Derived apply and replay watermarking should live in per-index domains.
   - Status: implemented for the current runtime shape. Each derived worker
     tracks its own applied and persisted sequence, metadata persistence is
     coalesced by index name, and replay retention uses the minimum persisted
     per-index sequence. The DB-global coordination that remains is limited to
     shared coalescing/truncation metadata, not index apply ownership.

6. Re-measure the HTTP path after each slice.
   - Guardrail:
     - provisioned ingest bench should not regress
   - Product:
     - `50k` HTTP load should close the gap to the Go public path
   - Status: storage guardrail remeasured at about 73.7us/doc for `50k`
     1536-dimensional dense writes. The product `50k` HTTP load still needs a
     fresh run after this slice.

## Derived Checkpoint Durability Boundary

Applied replay checkpoints are progress metadata, not an index durability
primitive.

The durable contract is:

1. The index apply/publish path owns durability for its own index state.
   - full-text segment apply/merge publishes durable full-text state
   - dense/HBC apply publishes durable node/leaf/quantized state
   - sparse apply publishes durable sparse state
   - graph apply publishes durable graph state
2. Only after that index-local apply/publish succeeds may the derived runtime
   record `applied_sequence(index) = N`.
3. Replay retention/truncation follows the minimum persisted applied checkpoint
   across all derived consumers.

The checkpoint path must not call back into index replay sync on every
watermark save. Doing so turns a tiny metadata update into a hidden
index-specific flush/sync, and it couples all derived consumers to the worst
storage behavior of whichever index is being checkpointed.

The checkpoint writer is allowed to:

- coalesce pending watermarks by index name
- persist one small applied-sequence checkpoint per changed index
- wake query/status visibility caches after the checkpoint is saved

The checkpoint writer is not allowed to:

- flush HBC/LSM replay state
- compact full-text segments
- rebuild sparse/graph state
- load or cold-open index structures
- make `.write` request latency depend on derived checkpoint durability

### Applied-Sequence Checkpoint File

Applied watermarks should not be ordinary hot-path LSM rows. They are monotonic
recovery checkpoints:

```text
index_name -> max_applied_sequence
```

The production shape is a per-table checkpoint file beside the table store:

- maintain the latest watermarks in memory for live status and sync waits
- coalesce pending updates by index name
- periodically or forcefully write a complete checkpoint file containing only
  the latest sequence per derived consumer
- write to a temp file, fsync the file, then atomically rename it over the old
  checkpoint
- on open, read the checkpoint file and fall back to the legacy LSM metadata row
  if the file or entry is missing
- if the checkpoint is stale after a crash, replay starts from the older
  sequence; duplicate replay is safe, fabricated progress is not

This intentionally avoids an append-only watermark WAL for the first production
slice. An append-only file would still need checkpoint/compaction. Rewriting
the complete file is simpler and remains O(number of derived consumers), not
O(number of documents, vectors, postings, or LSM tables).

The checkpoint file must remain outside the index durability contract. It may
only advance after the owning index has durably published the corresponding
state. Losing the latest checkpoint can increase restart replay; it must never
cause replay to skip unpublished index work.

Stronger sync levels still wait for derived visibility through the derived
runtime, but the index kind remains responsible for making its own state
publishable before the runtime advances the applied checkpoint.

The next architectural step is to stop treating generated replay as thousands
of tiny independent downstream publishes. The thin-ref migration removed the
payload duplication, but the worker still turns each pending ref into one or
more tiny `DerivedBatch` appends. That leaves too much tail work in deferred
catch-up and still behaves like a second replay subsystem.

The staged target shape is:

1. Primary commit remains the only durable source of truth for documents and
   artifacts.
2. Generated replay consumes committed identities and sequence ranges, not
   payload-heavy replay batches.
3. Each derived consumer advances an explicit `applied_seq` watermark.
4. Sync waits on `applied_seq >= commit_seq`, not on a second payload log
   draining one tiny publish at a time.

### Stage 1: Windowed Generated Replay

This is the first implementation slice after thin refs:

- Collect pending generated refs from `(applied_sequence, target_sequence]`.
- Materialize requests from committed primary state.
- Process requests in windows instead of appending one downstream batch per
  request.
- Publish far fewer `DerivedBatch` units per window:
  - chunk text docs/deletes batched together
  - dense chunk/whole-doc writes batched together
  - sparse chunk/whole-doc writes batched together
- Keep the current thin derived log as the transport, but make the worker emit
  larger, fewer downstream apply batches.

Operational goal:

- request-path remains cheap
- enrichment tail drain scales with windows, not with one append per generated
  ref
- dense replay reaches the existing outer HBC/LSM catch-up bulk session in
  larger units

Current status:

- thin generated refs are implemented
- worker-side chunk reuse is implemented
- windowed generated replay batching is the active migration slice
- enrichment replay now scans the thin change journal by `(applied_seq, target_seq]`
  and rebuilds generated work from committed documents instead of decoding
  `DerivedBatch` payloads
- dense, sparse, and full-text catch-up now consume thin journal windows and
  rebuild apply batches from committed state plus journal identities
- derived and enrichment workers now obtain replay windows through a replay-source
  abstraction, so direct primary-sequence/WAL enumeration can replace the journal
  implementation later without reworking worker logic again
- the remaining architectural correction is to move steady-state replay back to
  the thin replay log and treat primary-store replay rows, if kept at all, as
  compatibility-only migration state
- primary-store scanning remains valid for rebuild/backfill/bootstrap flows, but
  not for ordinary write-driven derived catch-up

### Stage 5: Backend-Neutral Commit Enumeration

The final replay collapse should not let DB replay parse backend WAL layouts
directly. The right boundary is a backend-neutral committed-change primitive
that the storage layer exposes and replay consumes through `ReplaySource`.

Task list:

1. Define a storage-level committed-change API below DB replay.
   - Status: implemented as the first migration slice.
   - The active seam now sits at the erased runtime-store layer and is surfaced
     through `DocStore`.
   - `DocStore` now exposes:
     - `lastReplaySequence(...)`
     - `nextReplaySequence(...)`
     - `appendReplayOpaque(...)`
     - `iterateReplayFrom(...)`
     - `truncateReplayUpTo(...)`
   - `backend_erased.Store` now also exposes the committed-change read/append/
     truncate capability for runtime-backed stores.
   - Replay/runtime code now depends on the storage API instead of the DB-local
     replay-stream helpers.

2. Keep the replay log in the same storage commit domain as the primary write.
   - LMDB shape: separate DBI, same write txn.
   - LSM shape: dedicated replay namespace/log structure, same write batch/WAL
     commit.
   - Do not append to a separate durable sidecar after the primary write has
     committed.

3. Keep backend-native commit machinery below the replay boundary.
   - WAL/commit backends remain durability and crash-recovery machinery.
   - The DB-level replay API should consume the thin replay log, not generic
     primary-store scans and not backend WAL parsing.

4. Reserve sequence numbers outside expensive request preparation.
   - Replay sequencing should not require a large mutex around:
     - `buildDerivedBatch`
     - replay payload encoding
     - inline/shadow preparation
     - sync-target collection
   - Reserve the next replay sequence cheaply, then do the expensive work
     outside the commit critical section.
   - Sequence gaps are acceptable; missing committed replay records are not.

5. Keep primary-store scanning limited to rebuild/backfill/bootstrap flows.
   - `DocStore` remains authoritative for data lookups.
   - Replay remains authoritative for steady-state sequence/order/hints.

### Stage 2: Applied-Sequence Watermark Contract

- Persist explicit per-consumer applied sequence state.
- Treat `applied_sequence` as the contract for sync/visibility.
- Wait on generated/full-text/dense/sparse `applied_sequence` instead of
  “derived replay drained”.
- Keep replay state conservative: stale is acceptable, fabricated progress is
  not.

This stage still allows a thin change journal to exist, but the sequence
watermark becomes the real correctness boundary.

### Stage 3: Thin Change Journal, Not Payload Replay

- Reduce the remaining derived replay stream to sequence + changed identities +
  lightweight hints.
- No full documents, vectors, sparse arrays, chunk payloads, or generated
  requests in the replay path.
- Workers rebuild concrete work from committed primary/artifact rows plus the
  current catalog.

At this point the journal is metadata for catch-up, not a second source of
truth.

### Stage 4: Primary-Sequence-Driven Replay

- Replace the separate generated replay dependency with committed primary change
  ranges plus per-consumer watermarks.
- Workers consume committed primary changes directly or through a very thin
  change journal.
- Generated replay becomes “derive from committed state up to sequence N”.

That is the clean end state for this architecture.

## Concrete Cutover Steps

The concrete implementation order from the current tree is:

1. Keep one backend-native thin replay record per committed sequence.
   - Commit it atomically with the primary write.
   - Keep it metadata-only and separate from document/artifact payload storage.
2. Move the enrichment worker to scan sequence windows from the thin replay log.
   - Do not decode `DerivedBatch` payloads for generated replay.
   - Rebuild concrete requests from committed primary/artifact rows.
   - Current status: implemented for generated enrichment replay.
3. Move dense/text/sparse/graph catch-up to the same sequence-window model.
   - Each consumer reads `(applied_seq, target_seq]`.
   - Each consumer rebuilds work from committed state plus thin journal hints.
   - Current status: implemented for dense, sparse, full-text, and graph.
4. Make sync entirely watermark-based.
   - `.enrichments`, `.full_text`, `.full_index`, and `.aknn` wait only on
     per-consumer `applied_seq`.
   - No sync path should depend on payload-log drain semantics.
   - Current status: implemented for the active journal-window path.
5. Keep sequence reservation cheap and the commit critical section narrow.
   - Sequence reservation should happen before expensive payload construction.
   - The critical section should contain only the backend commit that stages
     primary mutations plus the thin replay record.
6. Remove payload replay and any sidecar replay surface from steady-state use.
   - `DerivedBatch` durability is not part of the long-term steady-state path.
   - The backend-native thin replay log is the only replay authority for new
     traffic.

## Graph Artifact Model

Graph should follow the same primary-store artifact model as embeddings rather
than staying as a replay-only payload shape.

The clean target is:

- one primary-store row per graph edge artifact
- stable edge artifact identity under the owning document/index namespace
- graph catch-up driven by changed artifact keys plus deleted document keys
- no steady-state dependence on `graph_writes` / `graph_deletes` payload replay

### Why

The current thin journal is enough for:

- documents
- full text
- dense embeddings
- sparse embeddings

because those can be rebuilt from committed document/artifact state.

Graph is the remaining outlier because direct graph mutations are currently
carried as replay payloads, and a thin journal with only doc keys cannot
reconstruct exact edge deletes or updates.

If graph edges are primary artifacts, the model becomes consistent again:

- generated graph edges are materialized as graph artifacts
- direct graph API writes/deletes also write/delete graph artifacts
- graph indexing/catch-up consumes changed graph artifact keys and rebuilds from
  committed state

### Target Shape

Each edge should have one stable artifact row, for example conceptually:

- `graph/<index>/<owner-doc>/<edge-type>/<target-doc>`

with a value carrying:

- codec/version
- source doc / target doc / edge type
- weight
- metadata
- source hash when the edge is generated from document content

The exact key layout can follow the existing internal-key conventions, but the
important property is one durable row per edge identity.

### Direct vs Generated Graph

Both graph sources should converge on the same artifact format:

1. Generated graph edges
   - enrichment computes desired edges from document state
   - writes graph edge artifacts
   - stores source hash/version so unchanged input can skip recompute

2. Direct graph writes/deletes
   - request path translates graph mutations into graph artifact upserts or
     tombstones in the main committed batch
   - direct graph deletes delete/tombstone the specific edge artifact row

That removes the distinction at replay time. Replay sees changed graph artifact
keys, not a special graph payload stream.

### Recommended Ordering

Do this before trying to finish graph on top of the current payload-style
journal-window model.

Reason:

- extending the thin journal to carry explicit graph mutation payloads would
  preserve a graph-specific replay mechanism we do not want long term
- graph-as-artifact is the architecture we actually want
- once edges are artifacts, the remaining graph journal-window work becomes the
  same style as dense/sparse/full-text: scan thin journal windows, reload
  committed artifact rows, apply

So the next implementation order should be:

1. define graph edge artifact key/value codec
2. translate direct graph writes/deletes into primary artifact writes/deletes
3. materialize generated graph edges as artifacts too
4. move graph catch-up onto thin journal windows over changed artifact keys
5. remove graph payload replay from the steady-state path

Current status:

- 1-4 are implemented.
- 5 is implemented for new traffic: graph request apply and journal catch-up
  both prefer changed graph artifact keys and rebuild graph mutations from
  committed artifact rows, and newly built `DerivedBatch` records no longer
  duplicate `graph_writes` / `graph_deletes` payloads.
- Payload graph fields still exist in `DerivedBatch` and the payload codec only
  as migration/compatibility for old records that may still be replayed.

## Thin Replay Slice

Current status:

- one record per committed sequence
- new traffic should replay from thin sequence windows plus committed
  store/artifact state
- replay build/commit failures must fail the write atomically
- replay backlog accounting follows primary replay bytes
- replay-stream sequence is now the authoritative replay clock for new
  traffic, maintenance, and sync/watermark waits
- the target storage shape is one backend commit that stages:
  - document/artifact mutations
  - one thin replay record
- replay must not be appended in a second durable step after the primary write
- the steady-state replay path must not depend on a sidecar journal in a
  separate failure domain
- `derived_log` is not part of the long-term steady-state replay path

The thin journal record should contain:

- `sequence`
- `changed_doc_keys`
- `deleted_doc_keys`
- `overwritten_doc_keys`
- `changed_artifact_keys`
- coarse `target_hints`

Target hints are intentionally broad:

- `enrichment`
- `full_text`
- `dense_vector`
- `sparse_vector`
- `graph`

This is enough to let future consumers skip unrelated sequence ranges without
reintroducing payload duplication.

## Versioned Codec

The new format should carry an explicit codec version in the value header. Do
not rely on implicit length or payload shape.

Initial header:

```text
magic[8]        = "AFENRCH\0"
codec_version  = u16 little-endian
kind           = u8
flags          = u8
source_hash    = u64 little-endian
payload_len    = u32 little-endian
payload        = kind-specific bytes
```

Initial kind values:

- `1`: chunk JSON payload.
- `2`: dense embedding, `u32 dims` followed by `dims` little-endian `f32`
  values.
- `3`: sparse embedding, encoded sparse vector payload.
- `4`: asset payload.

Initial flags:

- `0x01`: source hash is present and valid.

Version rules:

- `codec_version = 1` for the first binary format.
- Any incompatible payload layout change increments `codec_version`.
- Decoders should validate magic, version, kind, payload length, and vector dims.
- No legacy JSON artifact compatibility is required for this migration.

## Implementation Task List

1. Add `artifact_codec.zig` under `storage/db/enrichment/`.
   - Encode/decode the versioned header.
   - Encode/decode dense embedding vectors as little-endian binary floats.
   - Encode/decode sparse embedding vectors as little-endian binary term/value
     pairs.
   - Encode/decode chunk JSON payloads with the same header.
   - Expose `sourceHash(value)` without decoding the full payload.
   - Status: dense and sparse codecs implemented. Chunk JSON remains unwrapped
     for now.
2. Store generated dense embedding artifacts with the new codec.
   - Replace JSON float-array payloads in `db.zig`,
     `catalog/index_manager.zig`, and `enrichment_runtime.zig`.
   - Include the source hash used for skip checks.
   - Status: done for generated and explicit dense artifacts.
3. Store generated chunk artifacts with the new codec.
   - Keep one row per chunk artifact.
   - Preserve full-text/chunk lookup projection by decoding the payload at read
     time.
   - Status: intentionally deferred. Chunk rows can stay JSON for now while
     embedding payload bytes and stale chunk cleanup are the bigger immediate
     wins.
4. Add source-hash skip checks before expensive enrichment.
   - For whole-document dense/sparse embeddings, compare the existing artifact
     hash before calling the embedder.
   - For chunked embeddings, compare the chunk artifact hash or the chunk
     embedding artifact hash, depending on which source feeds the embedder.
   - For chunks, compare the rendered source hash before re-chunking.
   - Do not blanket-delete enrichment artifacts during ordinary document
     updates. Deletes should remain eager; update cleanup should be based on the
     final desired artifact keys after regeneration.
  - Status: async whole-document dense/sparse skip is implemented. Async
    chunked dense/sparse skip is implemented from the chunk text hash.
    Ordinary updates keep artifacts, and async chunk replacement deletes stale
    chunk/embedding rows and stale chunk vector index entries after computing
    the desired chunk keys.
5. Update lookup/search projection.
   - Decode binary chunk and embedding artifacts into the existing public JSON
     result shape.
   - Keep C/API artifact retrieval opaque unless a public decoded endpoint is
     explicitly added.
   - Status: dense embedding projection decodes the binary codec. Chunk
     projection still reads JSON chunk rows.
6. Add tests matching Go behavior.
   - Rewriting a document with unchanged source text does not call the embedder.
   - Rewriting unrelated fields does not rewrite embedding artifacts.
   - Changing the source text updates the hash and regenerates output.
   - Chunk artifacts carry source hash and codec version.
   - Dense embedding artifacts carry source hash, codec version, dims, and
     binary vector bytes.
   - Document updates with unchanged source keep the existing artifact row and
     skip the embedder.
   - Document deletes remove existing chunk/embedding artifact rows.
7. Add metrics.
   - Count enrichment skip-by-hash events.
   - Count codec decode failures by kind/version.
   - Track artifact bytes written by kind so disk-amplification benchmarks can
     distinguish document, chunk, and embedding bytes.
   - Status: runtime/status counters now expose total skip-by-hash events,
     codec decode failures, dense artifact bytes, sparse artifact bytes, chunk
     artifact bytes, and total artifact bytes. Decode failures are currently
   totalized rather than split by kind/version.
8. Move generated replay fully to watermark-driven primary change consumption.
   - Keep the current thin refs as the intermediate step.
   - Replace the separate derived replay log with committed primary change
     ranges plus per-index applied sequence watermarks.
   - Status: thin generated refs are implemented. Windowed generated replay is
     the current in-flight migration slice. Full watermark-driven replay is
     still pending.

## Relationship To LSM Write Amplification

This work is part of the write-amplification fix, not just a storage-format
cleanup:

- JSON float arrays are far larger than binary `f32` vectors.
- Missing hash checks force unnecessary embedder calls and artifact rewrites.
- Unnecessary artifact rewrites feed dense/sparse/full-text derived indexes,
  which then amplify into HBC node/quantized writes and LSM table bytes.

After this lands, the write benchmark should show lower primary/artifact table
bytes and fewer derived dense apply operations on updates that do not change the
embedding source.

## Current Implementation Notes

- The dense embedding artifact value now uses the versioned codec for generated
  and explicit embedding artifacts.
- The HBC dense vector loader reads vectors from the artifact codec instead of a
  JSON float array.
- Lookup/search projection decodes binary dense embedding artifacts back into
  the existing public `_embeddings` JSON shape.
- Async whole-document dense enrichment compares the stored source hash before
  calling the embedder and returns early when the hash matches.
- Async chunked dense enrichment compares each chunk embedding artifact's stored
  source hash before calling the embedder. Unchanged chunks are kept in place;
  changed chunks get new embedding artifacts.
- Async chunk replacement computes the desired chunk keys first, then deletes
  stale chunk rows, stale derived chunk embedding rows, and stale chunk vector
  index entries. This preserves parent-delete semantics without forcing
  unchanged-source re-embedding.
- Generated dense and sparse embeddings both support chunked sources. Dense
  indexes and sparse indexes now remember the chunk artifact name, so vector
  search over chunk vectors uses the chunk-backed parent grouping/hydration path
  by default.
- Generated sparse embedding artifacts now use the versioned codec and source
  hash in the async path. The sparse index itself still consumes the existing
  derived sparse writes; the artifact row exists for locality, skip checks, and
  storage/accounting parity with Go.
- The DB write path no longer deletes all enrichment artifacts on ordinary
  document updates. This is required for the hash comparison to work.
- Delete paths still remove document-scoped enrichment artifacts.
- HBC no longer requires a persisted duplicate raw-vector row for
  skip-vector-store mutations. Bulk builds cache raw vectors when vector storage
  is skipped, HBC vector loads consult the cache before falling back to the
  legacy `.vecs` raw-vector namespace, and the DB index manager wires an
  external vector provider that reads row-adjacent embedding artifacts after
  reopen.
- Reopen coverage now verifies that chunked generated dense indexes can shrink
  a document after restart and delete stale HBC vectors through the artifact
  loader without re-embedding unchanged chunks.

Still needed:

- Keep chunk rows JSON for now and rely on source-hash sidecars/embedding
  artifact hashes for skip behavior. A chunk codec envelope is a later option,
  not the immediate migration target.
- Bring the synchronous precompute path to parity with async cleanup/skip
  behavior for chunk updates.
- Add provider miss/error counters so restart-time HBC vector loads distinguish
  missing artifacts, codec failures, dimension mismatches, and primary-document
  field fallbacks.
- Split codec failure counters by kind/version once codec failures become common
  enough to need finer triage.

## Compression Direction

Chunks can stay JSON in the immediate binary-embedding migration. The near-term
storage win is binary dense/sparse artifacts plus LSM table-block compression,
not wrapping every document-like value in a row codec.

Near-term order:

1. Keep dense and sparse embedding artifacts binary.
2. Add enrichment source-hash/projection sidecars so unchanged sources skip
   expensive generation and downstream index rewrites.
3. Add adaptive LSM table-block compression so JSON/text rows, metadata rows,
   and small derived rows compress without changing every public document
   payload shape.
4. Use simdjson-style/on-demand extraction during enrichment source rendering
   where it avoids full JSON materialization, but do not depend on repeated
   full-document parsing for skip decisions.

MAYBE/later:

- Add primary document and chunk codec envelopes. Small JSON/text values can
  remain raw; larger JSON/text values can be zstd/lz4-compressed behind an
  explicit versioned header.
- Add value separation for very large documents or artifacts if table-block
  compression still leaves high compaction rewrite cost.
- Query and lookup paths must continue returning the public JSON shape after
  any transparent decompression.
