# Source search streams and native processing

A source owns acquired v3 parent records. Native enrichments own derived artifacts;
indexes consume those artifacts. Source management declares discovery bindings,
not an alternate processing engine. The same Go contract is used by standalone
`antfly-source` and Colony. Provider connectors never run inference.

## Wire versions and compatibility

`search_version` absent or 1 keeps the legacy `TextStream` contract: textual
`field`, optional artifact/full-text `index` pair, and exact-search `default`.
`search_version: 2` uses `SearchStream.retrieval`, per-mode defaults, optional
`resource_field`, and generic `artifacts`. Management capabilities advertise
`search_versions: [1,2]` and `artifact_discovery`. Unsupported versions fail
explicitly. Source record v3, file catalog v2, catalog row v1 and search binding
v2 are independent versions.

The Go `TextStream` and `TextStreamBoundary` names remain compatibility aliases.
Native grep adapts only exact-enabled streams. A semantic-only image is not
silently replaced by caption text or accepted as an empty exact result. Existing
v1 exact scanners keep their exhaustive snapshot/budget behavior.

## Declare a representation once

```json
{
  "search_version": 2,
  "streams": [
    {
      "name": "transcript",
      "artifact": "transcript_chunks",
      "field": "text",
      "purpose": "transcription",
      "location_field": "provenance",
      "boundary": "unit",
      "retrieval": {
        "exact": {"default": true},
        "full_text": {"index": "transcript_text", "default": true},
        "semantic": {
          "index": "transcript_vectors",
          "member_artifacts": ["transcript_dense"],
          "query_inputs": ["text", "vector"],
          "vector_space": {
            "id": "research-text-v1", "dimension": 768, "metric": "cosine",
            "model": "PINNED_MODEL_ID", "revision": "IMMUTABLE_REVISION",
            "query_preprocessing": "query-recipe-v1",
            "document_preprocessing": "document-recipe-v1",
            "query_modality": "text", "document_modality": "text"
          }
        }
      }
    }
  ],
  "artifacts": [
    {"name": "facts", "artifact": "facts_v1", "content_type": "application/json", "purpose": "structured_extraction"}
  ]
}
```

Model identifiers above are placeholders, not a configured model. Use the actual
pinned model and preprocessing recipes. Native embedding enrichments declare
`vector_space: sources.VectorSpaceIdentity(recipe)`, binding the complete recipe
into native producer identity. The shared observer verifies index type, dimension,
metric, member lineage, native vector-space identity and the installed query model.
Authentication settings (including secret references) do not participate in vector
compatibility. Private processing configuration still requires content-addressed
index names; reconciliation compares the public readback fields while those names
bind write-only configuration. Retrieval instructions and other encoding settings
remain part of compatibility and must match the installed native configuration.
Original media uses `resource_field: content.resource` and may declare only
semantic retrieval. Query input support is explicit: indexing media does not
advertise an image-query HTTP route. This contract supports server-embedded text
and supplied dense vectors. Direct image queries can use compatible client vectors.

Managed original-media bindings require a direct typed native input such as
`template: "{{remoteMedia url=content.resource.uri}}"` or
`template: "{{media url=content.resource.uri}}"`, matching a non-text document
modality. A URI field or plain `{{content.resource.uri}}` interpolation embeds
text and cannot certify media readiness. Native `embedding_input: pdf_page_images`
consumes the nested resource URI through raw interpolation such as
`template: "{{{content.resource.uri}}}"`, with `document_modality: image`.
Native `field` selectors address literal root keys, so a dotted native field
cannot prove a nested representation dependency. Escaped interpolation is also
rejected for URI inputs because it can alter signed query parameters. Unknown input modes, text-producing helper options,
composed prompts and unproven artifact/media transitions report an unsupported
binding; they never become ready based only on a matching URI dependency.


A generic artifact need not contain text or have an index. Generic artifact
inspection uses native document artifact APIs. The source status reports
`unsupported/per_artifact_coverage_not_observed` when no safe aggregate observation
is available; it never fabricates corpus-wide artifact completion from declaration
presence. Index-backed stream readiness is observed independently.

## Configure native processing

Management Source and standalone profile Source accept `processing.indexes`, an
array of `{name, config}` ordinary native IndexConfig declarations. Models,
prompts, output schemas, dependencies and native secret references remain native
configuration. Raw credential literals are rejected for credential fields.

`BuildTextProcessing` builds asset/chunk/full-text and optional embedding branches.
Native chunk fanout consumes extraction units and explicitly typed text/JSON
assets, including summaries, captions and transcripts. Ordered chunk publication
retains the accepted upstream's causal proof and current-parent guards. Binary
assets are never implicitly decoded as text. Recipe changes version the affected
artifact descendants, while compatible acquisition and producer branches remain reusable.
Pass explicit native copy, document-extraction, transcriber, reader or generator
producer configuration for content, OCR, transcripts, captions or summaries.
Generated representations are not selected by default. Locations and generated
labels stay on stream bindings; generated text is not original-source evidence.

`NativeIndexName` content-addresses recipes containing write-only producer
configuration, which native readback redacts. Reconciliation creates missing
indexes, starts native backfill, and refuses to overwrite existing conflicting
names. Change a recipe/model by building a new index name, wait for its readiness,
and switch the binding. Shared table owners are not silently migrated. Native
readback is the source of operational truth; local declarations grant no access.

Standalone and Colony reconcile processing during source runs and management PUT.
Desired state is persisted before native effects; a failed PUT may have saved the
configuration. Read it back and retry PUT to resume reconciliation. There is no
provider download in processing-only reconciliation. Source acquisition hashes
exclude processing, streams, artifacts and search_version, so these changes do
not invalidate original byte references or acquisition checkpoints. Standalone
management PUT also republishes the existing portable catalog binding under the
runner state lock, preserving the acquired revision and access expiry. Changed
acquisition configurations require a normal source run. CLI processing can do
the same with `--state EXISTING_RUNNER_STATE_DIR`.

Colony requires **table.admin** to install native processing, including through
legacy import-job creation. Binding-only edits keep the existing table-write
requirement. Source detail's **Search and processing** panel shows observed
representation states and applies declarations without queuing a provider sync.
Hosted GET/list responses include opaque processing declarations only for table
administrators. Full processing/stream validation runs at configuration admission
and reconciliation. Cloud authorization reads project only permissions and TTL
from PostgreSQL, then validate policy and current publication fences; unrelated
private prompts and inference plans are neither transferred nor hashed there. Other table readers retain streams, artifacts and observed
readiness. On either runtime, omitted/null `processing` in an update preserves
the stored declaration atomically. An explicit empty plan clears desired
processing and requires table administration in Colony; physical shared indexes
are retired separately through native administration. Writer binding-only edits
never replay private native processing installation.

Credential classification follows native credential safety, including bearer and
capability tokens, alternate key spellings and ADC paths. Literal credentials
and credential-bearing endpoint URLs are rejected before desired state is saved;
opaque `*_json` provider configurations receive the same recursive validation.
Use complete native `${secret:name}` references.

Transcription presets use native document extraction with an explicit transcript
route and the supplied transcriber configuration. The existing unit protocol
retains timing and speaker spans, and versions the new recipe without changing
acquisition identity. Unit provenance exposes transcript spans. Chunk provenance
includes the covering time interval and only the overlapping unit-relative spans;
a single-speaker label is present only when the whole chunk is attributable to
that speaker. Segment timing is preserved rather than interpolated into invented
word timings. Chunk span selection and timing propagation use bounded range
lookups, avoiding a scan of every recording segment for each chunk. Reapply the
preset to migrate older raw-STT-JSON declarations to the new immutable recipe.
Grouped passage queries resolve source predicates in parent identity space and
apply shared parent ordinals to artifact postings before ranking and pagination.
Index-local document numbers are never reused across distinct retrieval indexes.

Native index backfill is durable; cancellation/reprocessing of artifact jobs uses
the existing native job APIs rather than the provider sync cancellation API.

## External vectors

`ExternalVectorIndex` creates an external dense index with an
`external_vector_space` compatibility identity. `WithExternalVector` attaches
finite, dimension-checked vectors and `metadata.embedding_spaces[index]` to a
revision-bound v3 parent. Native write admission independently rejects missing or
mismatched identities for supplied vectors. Legacy external indexes without the
new declaration retain their existing behavior. This is an authored assertion:
vector values cannot prove which model generated them. External writers retain
responsibility for refreshing or explicitly deleting their vectors when their
input representation changes; omission retains native external-vector semantics.
Ranked source queries require the current parent's authored embedding receipt.
Canonical provider publication replaces metadata without that receipt, hiding
retained old vectors until an external writer publishes the new representation.
Custom external writers must likewise clear receipts when changing their input;
the receipt cannot prove which model computed the asserted vector.

External discovery advertises supplied-vector input only. A native declaration
must match the full recipe hash before it is treated as compatible. Provider
acquisition never generates external vectors on the caller's behalf.

## Query and evidence

`BuildSearchQuery` emits ordinary bounded native table queries. `Publisher.Search`
validates the installed binding/readiness before and after retrieval, rejects a
changed generation, and returns the native response. Client vectors must match the
entire advertised recipe, dimensions and finite-value constraints. Requests return
at most 100 parents and 1,000 bounded members, using hierarchy source grouping and
explicit parent/member field projection. Original media evidence is a parent
resource projection; passage text is optional.

The current helper requires a stream's index to cover its explicit member set;
it rejects an index with undeclared extra members rather than returning another
representation's evidence. A semantic union can declare all compatible members
with a common ancestor representation. Native multi-index queries remain
available for explicit RRF fusion.

Native `_score`, `_distance`, `_index_scores` and hierarchy artifact/member
identity retain their native semantics. Raw scores are not comparable across
streams. An index-to-stream mapping is insufficient for multi-artifact indexes;
retain matched artifact/member identity and fusion provenance. Clients can rerank
text passages or authorized media evidence with a compatible reranker. Summary
matches are exact within the summary, not exhaustive original coverage.

The runtime's authenticated gateway/native policy authorizes sources, parents,
artifacts and snapshots. Discovery filters only narrow scope; they confer no
permission. Do not turn a native index-metadata preflight into a replacement for
query-time authorization or derive public access from producer credentials.

## Consistency and readiness

Management `search_status` is read-only, bounded to five seconds and caches each
native index lookup within the request. Distinct indexes are inspected by at most
eight concurrent workers with a shared 32 MiB metadata admission budget. Timeout
and metadata-budget failures report `unsupported` with
`native_observation_timeout` or `native_metadata_budget_exceeded`, rather than
claiming that the native index is building. It is advisory, not a snapshot lease.
It never turns a successful acquisition into an inference completion receipt.

Native query responses describe actual operation consistency through
`remote_snapshot` or `live_read`. `SearchConsistency` interprets those markers.
Unordered relevance responses without either marker are explicitly `unretained`;
this makes no claim that candidate and projection reads shared a retained cut.
The helper preserves native responses and adds `source_search` with source,
stream, index, purpose/generated labels and operation consistency.
`SearchRequest.require_snapshot` rejects live-only or unretained observations. Lite
live reads do not prove absence while indexing is pending. Existing source
publication/retirement and authoritative cleanup retain their full-index/scoped
merge barriers. Snapshot-dependent traversal must fail closed when unavailable;
current access still applies to historical snapshots. Permission validity,
acquisition completion and producer/index readiness are distinct clocks.

## Local fixture and CLI

With an isolated loopback server built from the same revision:

```sh
GOWORK=off go -C go/pkg/sources run ./examples/search-streams \
  --endpoint http://127.0.0.1:8080/db/v1 --directory .source-search-fixture
```

The process acquires real local text and a tiny image through the source service,
installs native copy/full-text processing, and supplies clearly labeled
three-dimensional synthetic vectors. It prints the catalog path and keeps the
revision-enforcing original-media service alive. No Colony or model deployment
is required. Tables remain for inspection after Ctrl-C; use a disposable server.
Fixture output establishes integration, not model quality or GPU performance.

```sh
GOWORK=off go -C go/pkg/sources run ./cmd/antfly-source processing \
  --config PROFILE --source SOURCE_ID --apply
GOWORK=off go -C go/pkg/sources run ./cmd/antfly-source search \
  --catalog CATALOG --source SOURCE_ID --stream STREAM --mode full_text --query launch
```

`search --vector FILE` accepts a JSON SearchRequest containing the vector and its
complete vector_space. It emits ordinary native grouped results. Provider and
inference credentials are never copied into the catalog.

New source tables declare `source_id` and `doc_type` as exact keyword predicates.
Ranked clients lower legacy catalog ownership filters to native canonical terms;
Colony likewise lowers its injected access predicates before ranked execution.
Existing tables are never silently migrated. For legacy tables without indexed
scope predicates, the CLI can explicitly request bounded value verification with
`--allow-scan --max-verify-documents 10000 --max-verify-bytes 67108864`.
Budget exhaustion fails rather than returning an apparently complete result.

Run the native contract regression against a dedicated server:

```sh
ANTFLY_SOURCES_E2E_URL=http://127.0.0.1:8080/db/v1 \
  GOWORK=off go -C go/pkg/sources test -run TestNativeSourceSearchStreams -timeout 3m
```

It checks raw-asset grouped evidence, media-only supplied-vector retrieval,
source isolation and rejection of unavailable snapshot guarantees.

This implementation reuses native artifact repair/reprocess job APIs for retries,
rebuilds and cancellation; those operations are separate from source sync runs.
It does not claim model quality, GPU throughput, or media-query routes beyond the
advertised text/vector inputs. Qualify deployed models and hardware separately.

Plain text scalar artifacts use the native `$` evidence projection. Their native
full-text source names its synthetic `text` indexing slot; validators accept that
mapping only for a matching asset declared `content_type: text/plain`. Object
artifacts keep explicit fields. Exact scanning never substitutes an arbitrary
object field or a caption for a selected representation.

## Resolved encoders, presets, and repair operations

Managed semantic bindings resolve an immutable native `antfly` encoder pin.
`NativeVectorSpace` derives the model revision from `embedder.model_identity` and
uses the native retrieval-query/document role encodings. Nonempty retrieval
overrides and other execution settings are included in a versioned preprocessing
identity; changing instructions or role overrides changes compatibility even when
the asset pin stays the same. Transport location does not change compatibility.
Native admission checks
that pin against the loaded assets and processor. A mutable hosted model alias,
an arbitrary preprocessing label, or an unpinned encoder is not advertised as
resolved native compatibility. Such applications can supply externally authored
vectors with the explicitly declared external-writer trust boundary.

Semantic bindings declare `member_field` independently of their representation
field. For example, a scalar text unit uses `$`, while embeddings of its chunks
return `member_field: text`. Validation checks each embedding/chunk projection
transition to the representation, not just the presence of an ancestor name.
`$` selects a declared scalar value; structured members retain public internal-field
suppression. The storage/query boundary wraps declared plaintext in the scalar `$` slot
before serialization, so JSON-looking plaintext is never
coerced to a number, string literal, or object by content sniffing.

Both management runtimes expose `POST /sources/{id}/processing-preview` for
content, extraction/OCR, transcription, caption, and summary presets. The preview
returns ordinary native declarations for review; applying them remains a source
PUT. Producer models, prompts, and native secret references are explicit. Only
semantic plans install chunk producers. Media-consuming presets use native media
parts or locator fields rather than send a resource URI as caption text.

`POST /sources/{id}/processing-jobs` delegates start/status/advance/cancel to native
artifact reprocess jobs. These jobs operate on the named artifact across the
**whole table**, including other sources sharing it. Colony requires table.admin
for every job operation, including status; the dashboard requires explicit scope
acknowledgment for mutations. Only artifacts declared by that source are accepted.
Native job IDs are exposed as decimal strings to avoid JavaScript u64 rounding.
Jobs are durable and advance in bounded pages; cancellation does not cancel source
acquisition, and retrying an artifact does not fetch provider content again.

Colony processing-only PUTs now refresh consumer catalog bindings while holding
the source job lock. Publication preserves the acquired revision and access lease;
competing edits, active acquisition, retirement, and acquisition changes prevent
a stale refresh. Standalone retains its filesystem state-lock equivalent.

The source CI gate now starts a disposable native owner and runs the strict HTTP
regression automatically. Mixed parents with no text input settle intentional
absence from the captured primary; nonempty or unresolved upstream inputs still
require publication proof. External-vector receipt identities are compiled into
the cached immutable write plan and relational writes reuse their parsed row.


Local generic assets commit a storage-owned receipt with their bytes, binding the
producer definition, exact logical parent revision, and output digest. Consumers
validate that receipt before preparing work and again at publication; cache reuse
does not reuse publication authority. Imported legacy skip state without a receipt
is repaired through ordinary producer execution. Ordered owners continue using
native causal proofs and never accept local receipts as ordered authority.

Externally authored semantic search requires an exact receipt for the selected
vector-space identity. Receipt presence alone does not authorize vector bytes
retained by native omission semantics.

An authored row that asserts the configured receipt must include that vector in
the same write. To update acquisition data without authoring a new vector, remove
the receipt; omission may retain physical vector bytes, but source search hides
them. This prevents an unchanged receipt from reauthorizing an old vector after a
parent edit. Direct text presets use literal fields or unescaped nested-path text projection,
preserving quotes, angle brackets, and resource URLs without HTML escaping.

Local asset consumers capture a digest of the complete stored upstream before
rendering producer input. Field selection, neighbor context and prior context
remain inputs to provider execution and skip-state identity; they never replace
the raw upstream identity checked in the publication transaction. Empty-input
retirement removes the output, skip state and local publication receipt under
the same source and producer fences.

Missing-input retirement uses the same current-producer recipe fence as output
publication. Obsolete work cannot delete the output, skip state or receipt of a
replacement producer, or publish skipped coverage for it. The ranked search
wrapper keeps native hits and extension fields as opaque JSON while attaching
binding metadata; external-vector helpers preserve parent JSON numbers during
deep copying. Large identifiers and high-precision numeric evidence are never
coerced through floating-point values by these adapters.
