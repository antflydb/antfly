# Declarative document models

`/ai/v1/read` and `/ai/v1/extract` accept complete document sources. Select an
ordinary model or register a bounded composite behind the same `model` field.
The HTTP and embedded entry points use the same in-process executor. Document
processing does not introduce another `/agents` resource.

```json
{
  "model": "antfly/document-balanced",
  "documents": [{
    "id": "invoice",
    "media_type": "application/pdf",
    "url": "data:application/pdf;base64,...",
    "pages": [1, 2]
  }],
  "retain_artifact": true
}
```

Supply exactly one input form. `/read` accepts `images` or `documents`;
`/extract` accepts `inputs`, `documents`, or `read_artifact`. Existing image
requests retain their image behavior. Document results carry `source_id` and the
one-based original page/slide `number`. `max_pages` bounds selected pages across
the run. Structural source inspection has a separate 10,000-page ceiling;
unselected slides are not decompressed or composed. Selection never renumbers source pages;
results follow original source order, regardless of selection order. Independent
sources remain independent extraction contexts. URLs inherit the deployment's
existing content security restrictions.
Extraction validates the complete source manifest before invoking an extractor,
including when reusing a retained reading. Every source must have at least one
read page; a source with no visible selected slides causes `DocumentUnresolved`
for the entire batch. Select visible slides or set `include_hidden: true` to
extract hidden slides. Successful results preserve source order and cardinality.
Encoded images use the ordinary reader MIME contract: type matching ignores
case, validated parameters are removed before dispatch, and declared types are
checked against the encoded bytes. Normalization borrows the existing bytes.

`max_regions` bounds each page's total regions, tables and cells, including
nested regions and inherited slide content. Native PDF runs and slide tables
are capped during parsing, and every retained page uses the active limit,
including native reads and interrupted model runs. PDF region bounds use the
transformed text basis and font metrics. Unsupported vertical-writing geometry
is left unknown while its native text remains available.

`antfly/native-document` reads native PDF and slide content without loading an
OCR model. A composite can retain trustworthy native text and render uncertain
pages lazily for OCR. `/ai/v1/models` advertises the document executor and format
capabilities, including unavailable rendering features.

## Registration and reading policy

Place a `document_agent.json` in the selected model directory, for example
`models/readers/antfly/document-balanced/document_agent.json`. JSON is the
configuration format; the issue's YAML examples describe the design vocabulary.
Register extraction agents under `models/extractors/<name>/document_agent.json`.
Resolution uses the requested operation: a reader and an extractor can share a
name and have independent definitions and limits. Child bindings resolve using
their declared `operation`. An extraction agent's optional `read` stage uses
that agent's own definition, including when another reader has the same name.
Child names below are examples and must resolve to installed models.

```json
{
  "version": 1,
  "kind": "document_agent",
  "models": [
    {"name": "fast", "model": "local/fast-reader", "operation": "read"},
    {"name": "accurate", "model": "local/accurate-reader", "operation": "read"},
    {"name": "tables", "model": "local/table-reader", "operation": "read"},
    {"name": "verify", "model": "local/verification-reader", "operation": "read"},
    {"name": "router", "model": "local/document-router", "operation": "decide"},
    {"name": "extractor", "model": "local/extractor", "operation": "extract"}
  ],
  "budget": {
    "deadline_ms": 60000,
    "max_model_calls": 24,
    "max_total_tokens": 100000,
    "max_region_attempts": 2,
    "max_parallel_pages": 4,
    "max_parallel_calls": 4,
    "max_total_pixels": 40000000
  },
  "read": {
    "steps": [
      {"id": "layout", "operation": "inspect_layout"},
      {"id": "initial", "operation": "read_region", "model": "fast"},
      {"id": "refined", "operation": "refine", "input": "initial",
       "router": "router", "accurate_reader": "accurate", "table_reader": "tables",
       "verification_reader": "verify"},
      {"id": "document", "operation": "assemble", "input": "refined"}
    ]
  },
  "extract": {"model": "extractor", "require_source_support": true}
}
```

The equivalent short reading form uses `reader`, `router`, `accurate_reader`,
`table_reader`, `fallback_reader`, and `verification_reader` directly inside
`read`. Omit `router` for deterministic routing: table regions use the table
reader, uncertain/empty regions use the accurate reader, and other regions retain
the initial reading. Layout comes from native source analysis and reader regions.
An ordinary fixed pipeline such as Surya can serve as any reader binding.
Reader bindings default to `output_origin: "transcription"`; use
`output_origin: "interpretation"` for a caption/diagram-description operation.
Assembly and source references retain that distinction through refinement.

Definitions compile before execution. Unknown fields, invalid references,
capability mismatches, cycles, and excessive dependency depth are rejected.
Routers receive observations and an executor-owned action set: `accept`,
`accurate`, `table`, `fallback`, or `unresolved`. They cannot introduce model
names, URLs, filesystem paths, or code. Document contents remain task data.
Choice abstentions (`choice: null`, such as a similarity tie or threshold miss)
retain the accepted reading with unresolved status for both Antfly and Cloudflare
routers. Missing or malformed choices and actions outside the permitted set
remain failures.

A replacement must cover the selected native text or be corroborated by a distinct
configured reader inspecting the same crop. Native support uses immutable text
runs in original source coordinates: text elsewhere on the page cannot validate
a region replacement, and a substring cannot establish whole-page coverage. A confidence increase alone cannot authorize replacement. Unsupported
revisions leave the original text unresolved. Read results distinguish native
content and transcription, preserve original source coordinates and render/crop
transforms, and report incomplete or truncated work. Confidence values remain
specific to each reader; they are not cross-model calibrated probabilities.

## Extraction and reuse

An extraction composite uses the same source adapters and can select its own
reading policy. Supply the ordinary extraction `schema` and optional `schema_version`.
Omitting the version delegates protocol selection to the native extractor, just
as a direct text extraction does. The response reports the effective version;
all rows, verifiers and fallbacks must agree on that protocol. Explicit versions
are forwarded unchanged. Application `inputs[].metadata` is preserved as output
metadata, including keys such as `segments`; only document readings and retained
artifacts supply page/table provenance for evidence mapping.
Named record fields, entity/relation behavior and long-document handling remain
owned by the selected extraction implementation. String inputs and arrays of
text content parts normalize once to the same canonical text before any model
call, so extraction and evidence offsets share one representation. Adjacent text
parts are separated by a newline. Image-only inputs use the direct document
form and the composite's reading policy.

Extraction builds a bounded canonical source view from each reading. Original
page and note text is retained, and accepted tables add explicit structured
views of the same page. One shared owner/payload index removes projection aliases
while retaining genuine duplicate tables. Cell order follows row/column order;
cell bytes, including Unicode and line breaks, are passed unchanged to every
extractor provider. Fresh documents and retained readings use the same path.

Generated table/layout labels carry derived provenance. Cell evidence retains
its table origin and includes `role: "table_cell"`, the original page number,
logical-owner `region_path`, and the authoritative `table_scope`, `table_index`,
`table_id`, `cell_index`, row/column and merge spans. `table_scope` identifies the
region table array (empty for page-level tables), and the indices address the
immutable original arrays even when display order changes. Evidence `start` and
`end` are UTF-8 byte offsets within that cell, rather than offsets into the page's
flat text. Available cell geometry and coordinate space remain attached.
Interpreted/derived/estimated cells and generated labels still obey extraction's
derived-value policy; their presence does not prevent located literal cells from
being verified. Canonical text and segment metadata are bounded before model
calls, with no intermediate serialization of the reading tree.
Table dimensions must be positive; cell coordinates and nonzero spans must fit
inside those dimensions. Native tables and model reports are checked before a
reading can be accepted, including nested and inherited tables. Retained reading
reuse enforces the same rules before dispatching extraction.

An optional extraction policy can set `verification_model` and `fallback_model`
to other extraction bindings. It compares independent candidate values, tries at
most one configured fallback, and rejects unresolved disagreement. Enable
`require_source_support` to require exact literal spans; `allow_derived_values`
controls explicitly marked derived/estimated values. Agreement is an advisory
quality check, separate from source-span validation. Verification compares named
field values, entity labels/text/attributes, relation types and endpoints, and
classification names/labels. Scores, evidence and provider metadata do not vote.
Entity, relation, classification and record result sets may be reordered; duplicate
counts and list-valued field order still matter. Relation references resolve
against each result's own entity indexes or IDs before comparison. Referenced
endpoint identities retain entity attributes, so same-named entities with different
roles remain distinct. Canonical identities are shared within each comparison;
relations reuse compact references without reprocessing attributes per edge.
Literal spans are checked against the exact input and mapped to original page text; declared Unicode or
UTF-16 offsets are converted before evidence mapping. Provider-native estimates
never acquire fabricated visual or literal grounding. Required source support
also applies to inline relation endpoints. Referenced endpoints reuse checked
entity evidence; dangling or conflicting references fail validation. Endpoint
grounding establishes source support for the endpoint text, not proof of the
relationship itself.

When a read requested `retain_artifact`, inspect `document.artifact_id` and
`artifact_ttl_ms`. Retention is best effort. Pass the opaque handle as
`read_artifact` on extraction to reuse the complete immutable reading without
fetching the source or running OCR again. Handles are bearer capabilities with
256 bits of randomness, process-local storage, a five-minute maximum lifetime,
and bounded storage with eviction. They are not content hashes or durable jobs.
A shared expiry timer releases unread payloads and their memory charges; it
reuses the host runtime's I/O pool, with one store-owned fallback for standalone
callers. Extraction borrows pinned immutable snapshots rather than copying the
cached JSON. Eviction drops cache ownership while active readers retain the bytes
and their memory charge. Cache reads and model work run outside the cache lock,
so allocator reclamation can safely evict entries. Document admission also removes
expired entries and can evict older readings before retrying memory admission. Shutdown wakes and joins maintenance.
Recently expired handles return 410; a bounded tombstone ring remembers the
last 128 expirations. Absent/evicted/older handles return 404. A restart invalidates
them.

Best-effort reading discloses unresolved pages. Extraction fails atomically when
reading, evidence validation, disagreement or execution budgets prevent a valid
result. It never presents a partial document as successful extraction.
If a call budget or unavailable renderer interrupts refinement, reading retains
the latest accepted text, fields and tables and reports the interruption as a
finding. Earlier accepted regional revisions survive later interruptions.
Cancellation, deadlines and hard resource limits still propagate as failures.

## Cloudflare providers

Cloudflare is a general Antfly provider, shared with SQL, retrieval and
enrichment. See [Cloudflare models](cloudflare-models.md) for coverage across model tasks. Document composites are consumers of those provider protocols.
Workers AI hosts the models; AI Gateway is optional routing configuration.

An ordinary extraction provider configuration uses:

```json
{
  "provider": "cloudflare",
  "account_id": "your-account-id",
  "model": "@cf/cloudflare/clairic",
  "api_key": "${secret:cloudflare.api_token}",
  "gateway_id": "optional-gateway",
  "schema_version": 2,
  "schema": {"structures": {"invoice": {"fields": {
    "number": "text",
    "total": {"type": "number", "decimals": 2, "unit": "USD"}
  }}}}
}
```

An ordinary decider uses `provider: "cloudflare"`, `account_id`, a secret-backed
`api_key`, optional `gateway_id`, and model `@cf/cloudflare/clef` or
`@cf/cloudflare/clef-flash`. Both task runtimes also support the
`CLOUDFLARE_API_TOKEN` environment default. Secret references use the existing
Antfly secrets configuration; native library callers supply resolved values.

A deployment-owned composite binding instead uses the provider/model/operation,
`account_id`, `token_env` and optional `gateway_id`; enable `allow_remote` in every
containing definition. Definitions contain credential references, never tokens.
Local-only parents cannot be widened by nested definitions.

The Clairic and Clef adapters send native `state` plus typed `fields` or
`questions`. Vision reading uses the Chat Completions protocol. Clairic supports one named record with text, text lists, numbers, dates
and estimates. Unsupported entities, relations, nested records, field/options
features and oversized contexts fail explicitly. Inline PNG/JPEG/WebP evidence
is supported by the ordinary providers within checked media limits. Null values
remain null rather than promoting a guess. Character spans become verified UTF-8
byte ranges; computed dates/numbers and estimates keep their distinct provenance.
Estimate metrics must be finite numbers, and probabilities must be between zero
and one. Malformed provider metrics fail before extraction output is returned.

Native transport uses Bearer authentication and, when configured, the
`cf-aig-gateway-id` header. Each Antfly invocation makes one upstream attempt;
configure gateway retries/fallback so they do not hide additional work or change
the selected model/schema. Live service availability and credentials are
independent of protocol fixture tests.

## Current execution boundaries

PDFs support native text and lazy visual rendering. PNG/JPEG/WebP photos support
OCR with original pixel coordinates and targeted crops. PPTX support includes
declared slide order, native text, native table cells/merge spans, explicit hidden
slide policy, and separately attributed speaker notes. Native Zig slide rendering
supports embedded pictures with alpha, rectangular crops, rotation and flips;
rectangles, ellipses and lines with explicit RGB fills/strokes; and solid RGB
backgrounds. Nested groups compose child coordinates, scaling, rotation and flips.
Visible master/layout decorations compose behind slide drawings,
with the nearest selected background beneath all shapes and template placeholders
excluded. Text boxes support per-run size, bold/italic, color/opacity, spacing,
wrapping and paragraph alignment/vertical anchoring. Layout and rendering share
bundled Roboto SFNT metrics and outlines; other Latin font names use this
deterministic fallback. Theme color schemes, color maps and ordered tint/shade,
luminance and opacity transforms resolve from source-owned metadata. Basic
bar/line/pie charts render cached series, category/value labels and legends
without fetching workbooks or evaluating formulas. Complex-script shaping,
placeholder geometry inheritance, theme overrides and format-style references,
stacked/3D/manual-layout charts and additional effects remain unsupported.
Unsupported content is reported
explicitly. XML inspection recognizes drawings independently of namespace prefixes,
including vector shapes, pictures, charts and embedded content. A slide with
unsupported visual content remains unresolved. Slide layouts and masters are
parsed once per source and shared across slides. Inherited parts retain their
source identity and visibility metadata; template prompts are excluded. Visible
conditional footer content and unsupported drawing styles keep
the slide unresolved. Incomplete slide images are never sent to OCR. PDF native inspection checks
painting operators without decoding images or materializing vector paths;
text-bearing pages with drawings still require visual inspection. PDF annotation
coverage is inspected separately from page content streams, using bounded metadata
and reference resolution. Static normal appearance streams for stamps and
widgets/form fields are composited as clipped Form XObjects, honoring the selected
appearance state, appearance matrix and page geometry. They trigger visual
inspection even on an otherwise text-only page. Viewer-generated appearances,
NoZoom/NoRotate, optional-content visibility, and annotation opacity/blend modes
that require separate compositing remain explicit unsupported findings. Native
painting and document OCR reject those unsupported paths; page-content-only
rasters cannot certify annotation coverage. Hidden annotations and
borderless links without appearances do not require visual work. See the
[PDF reference, annotation flags](https://printtechnologies.org/wp-content/uploads/2020/03/pdfreference_v1-4-1.pdf)
for the screen/print visibility rules.

PDF OCR executes prepared render plans with an encoded-output ceiling before
model dispatch. The renderer fits PNG output within the active source-byte limit
or fails explicitly, while preserving the initial raster's pixel charge. Page
calls reuse source-owned render-fork metadata. Fitted raster coordinates compose
the original render transform with exact per-axis resize ratios; rounded DPI is
advisory and cannot shift crop or evidence geometry. Cancellation applies during
painting and output fitting, and page buffers are released on failure.
Regional crops preserve their source pixels and geometry. PNG encoding enforces
the active byte ceiling, including the destination child model's limit, while
compressing. A crop that cannot fit fails before model dispatch; cancellation
and allocation failures release its transient buffers.
Regional crops preserve the source's RGBA pixels, including fully transparent
and partially transparent pixels. The page caches one RGBA decode, and the PNG
encoder streams borrowed rows with the original stride. Specialists and
verifiers inspect the same pixels without allocating and copying another raster
for each crop.

Image document sources can supply `quadrilateral` for perspective correction:

```json
{
  "id": "receipt",
  "url": "data:image/jpeg;base64,...",
  "media_type": "image/jpeg",
  "quadrilateral": [[100, 80], [900, 120], [950, 1400], [50, 1380]]
}
```

Corners use original image-edge coordinates in clockwise order: top-left,
top-right, bottom-right, bottom-left. The polygon must be finite, strictly convex,
and contained in the image. Rectification limits source/destination pixels,
checks cancellation between rows, and interpolates premultiplied alpha. The
projective `raster_to_source` mapping keeps OCR regions, cells, crops and evidence
in the original photo's coordinates. The quadrilateral participates in run
identity. Automatic corner detection is not implied by this input contract.

Slide text, tables and visual inspection share one namespace- and
visibility-checked XML view. Native text comes from supported PresentationML and
DrawingML shape/cell text bodies, and native tables from table graphic-data
containers. Namespace aliases and scoped rebindings resolve before projection.
Extension lists and custom-data payloads remain opaque: their text, tables and
drawing properties cannot enter visible readings or literal evidence. Unsupported
markup outside those metadata containers produces an unresolved visual finding.
Individual `cNvPr.hidden` flags apply to the owning shape, picture, table frame
or group, including hidden ancestors in slides, layouts, masters and notes.
Hidden content cannot become visible reading text or literal extraction evidence,
and hidden drawings do not trigger unnecessary visual inspection. `include_hidden`
continues to select hidden slides; it does not reveal hidden objects within them.
Slide visibility (`show`), inherited master visibility (`showMasterSp`), hidden
objects and text boxes share XML boolean parsing. It accepts character references
and surrounding whitespace, and rejects malformed or duplicate flags. One pass
checks namespaces, content context and visibility using a bounded stack. It
borrows unchanged part bytes and copies retained spans once when filtering.
OOXML `AlternateContent` selects the first choice whose required namespace URIs
are supported, otherwise its fallback. Namespace aliases, scoped rebindings and
encoded requirements resolve before selection. Unselected branch payloads are
excluded from text, tables, visual inspection and evidence. Control tags retain
namespace scopes so filtering and rechecking inherited parts gives the same
selection. Unchanged parts borrow their bytes; filtered output is bounded by the
original part size.
Checked layout/master fragments reuse their parent projection without reparsing
outside their namespace scope. Selected slides and shared layout/master parts
reuse their checked views; unselected slides retain their original indexes
without decompressing XML.

Slide discovery admits only namespace-qualified entries in the presentation's
slide list, including the selected compatibility branch. Extension metadata and
foreign lookalikes cannot add slides. Relationship attributes resolve by namespace
URI, so prefix aliases, scoped declarations and character references preserve
their meaning. Each relationship part is indexed once; slide, layout, master and
notes lookups check the relationship's declared role and reject duplicate IDs,
ambiguous parent links and external targets.

Unsupported-markup findings stay attached to their retained XML owners. Removing
a template placeholder or overriding an inherited background removes its findings
without clearing findings from visible shapes. Checked subtree views borrow both
their bytes and their scoped findings; filtered views remap findings while copying
retained spans, without another namespace parse.

Sources parse once. Remote routing/extraction/verification share one bounded,
resource-accounted HTTP pool per run. Source buffers release between documents and page scratch
buffers release between pages. Local child invocations borrow encoded image
bytes; base64 is introduced only at remote transport boundaries. Orchestration
holds no parent model execution permit while waiting for a child. Nested readers
and extractors share cumulative calls, measured tokens, pixels, output ceilings,
deadlines and cancellation. Child entry points intersect every resource ceiling
with the parent's remaining budget, retaining cumulative consumption when the
child returns; model identities are captured before execution and
checked again around child calls. Borrowed document views still obey each
child's encoded-byte, dimension and pixel ceilings; borrowing avoids a second
physical source charge. Reading metadata includes a run fingerprint of
source hashes, source-selection options, prompts, effective limits, model
identities and adapter/runtime versions. Remote providers expose model names
without an immutable weight revision. Independent pages run in bounded waves on
the host-owned executor. `max_parallel_pages` and `max_parallel_calls` both default
to four, with a maximum of 64; the smaller ceiling bounds page concurrency. Hosts
without a shared executor run serially. Each PDF page has an independent reader
fork and scratch arena. All workers share cumulative call/token/pixel/output
admission and one hosted HTTP pool. Results and retained events merge in source
order, so completion order cannot reorder document output. At most one wave of
completed pages is buffered. Independent regional refinement uses bounded waves
with worker-owned scratch, immutable crop pixels and canonical revision/event
assembly. A run-wide region gate bounds live refinement tasks; a separate leaf
call gate enforces total call concurrency across pages and regions. Composite
parents hold no leaf call capacity while invoking children. Compatible ready crops
coalesce into shared reader invocations; incompatible groups dispatch concurrently
within the call ceiling. Token usage is charged once for each physical batch.
Narrower child token budgets retain separate physical reports when aggregate
batch usage cannot be attributed safely to individual children.
Attempts within each region remain ordered. Model discovery reports
the effective parallel-call ceiling.

Initial OCR output and accepted revisions must retain trustworthy native text
coverage. Missing coverage triggers whole-page escalation and prevents a router
from accepting an incomplete transcription. Known broken font mappings can be
superseded only through source verification. Regional revisions preserve page
text outside the corrected region, nested regions, tables and structured fields;
ambiguous table ownership is reported as unresolved. An accepted regional revision
replaces its owned table set, including when the replacement contains no tables.
Replacement is tracked explicitly for the current run. Existing validation metadata
and nonempty region table arrays do not imply a new revision. Untouched owners
retain distinct page-level authoritative tables; matching flat aliases are rebuilt
through bounded owner and payload indices, preserving genuine multiplicity. Geometry
is consulted only for unowned tables affected by an accepted regional revision.

Hosted reader composition preserves structured regions, nested children, tables,
exact numeric fields and reported provenance. Unknown region or cell geometry
remains `null`; known boxes are transformed through crop and page coordinates.
Interpretation, derived and estimated origins remain nonliteral through binding
and parent overrides, so composition cannot turn them into literal source evidence.
The adapter validates shared structure totals before allocating typed arrays and
borrows parsed text and fields until the executor accepts the candidate.

Refinement distinguishes unchanged text and structure from a source-supported
replacement. Changed regions, tables and fields require structural corroboration,
even when OCR text is identical. Comparisons stream into content
signatures, ignore advisory model/quality metadata, and tolerate field-key and
table-cell ordering differences while preserving exact numeric values. Unchanged
answers retain accepted provenance, structure and completion without another copy.
A provenance upgrade requires matching native source text or an independent
literal verifier; nonliteral verifier output cannot establish literal evidence.
Regional native grounding requires every nonempty native run to have valid bounds.
Intersecting runs must be wholly contained in the crop; crossing or unknown bounds
require independent visual verification. Boundary-touching and disjoint runs do
not contribute crop text. Grounding compares ordered tokens directly without
allocating a joined text buffer.
Verifier independence uses precomputed sets of pinned leaf model identities,
including composite dependencies. Aliases, copied weights and shared hosted model
references are rejected during preflight before source downloads or model calls.
Accounts and gateways do not make the same hosted model independent.

Literal corroboration checks effective provenance over the matching text spans,
including nested regions. Mixed readings can support their literal spans without
promoting interpreted, derived or estimated spans. Unresolvable or ambiguous
provenance coverage fails closed; provider offsets are checked against the text before use.
This check uses bounded traversal stacks and does not allocate or copy region trees.
Validated UTF-8 offsets remain authoritative through unchanged reads and repairs;
invalid or partial offsets never select another occurrence of the same text.
Offset-free regions require a unique ordered placement. Forward and reverse scans
qualify sibling scopes without an occurrence index, including repeated native runs
whose order uniquely fixes their positions. Ambiguous readings retain their text
and metadata with an unresolved placement finding. Repairs validate all placements
before mutation, allocate the revised text once, and rebase descendant offsets
into the resulting page text. Unchanged acceptance reuses the existing text buffer.

Extraction applies the same checked text-placement rules. It indexes disjoint
provenance spans once per input, then uses binary search to map each field or
entity to source evidence. Missing, conflicting or unlocatable mixed provenance
makes that containing scope nonliteral; unrelated qualified scopes retain their
literal evidence. Strict extraction rejects those ambiguous values unless derived
values are allowed, in which case they carry derived provenance without literal
offsets. Nested region paths are emitted directly without intermediate JSON.
Region qualification uses the active `max_regions` allowance separately for each
original page number; page text and speaker-note segments share that page's
allowance. Total pages and serialized metadata remain subject to the active page
and output budgets. There is no additional fixed document-wide region or span
ceiling. Preflight counts every region, including ambiguous subtrees, before
allocation. A sizing pass then allocates compact node/span arrays for only the
qualified representation, borrowing segment metadata once per segment. Large
multi-page readings retain indexed lookups without reserving worst-case storage
for collapsed scopes.

Structural corroboration normalizes reader-local owner IDs to canonical tree
positions. Explicit `region_id` references address the immediate child scope,
falling back to the containing region; an omitted owner on a region-attached table
means that containing region. Page-level owners address top-level regions.
Duplicate sibling IDs and dangling references are rejected during owner indexing.
A bounded owner index is built only when explicit references require it and is
released after comparison; reports without such references stay allocation-free.

Corroboration compares a multiset of logical tables, so a region-owned table and
its redundant page-level view count as one table. Generated flat views carry
`projection: true`: their `region_id` points to a region whose `tables` array
contains the same table ID and contents. Nested projections may reference another
projection, preserving the authoritative table's deeper ownership. Missing,
contradictory or excess declared aliases are invalid. For older readers, unmarked
aliases are inferred only when the scoped ID, contents and logical owner agree.
Real duplicates, changed contents and different owners remain distinct.

The comparison hashes each physical table once and uses a bounded request-local
index to resolve aliases, without constructing normalized JSON or scanning every
pair of tables. Admission limits still count every physical view, including
projections; semantic deduplication does not relax structure or output budgets.

Native slide text preserves explicit DrawingML line breaks in text boxes, table
cells and included speaker notes.

Region `text_start` and `text_end` are UTF-8 byte offsets within the page text.
Nested regions retain source coordinates and qualified identities. Page origin
provides the default provenance, with mapped regional origins overriding it, so
literal fields remain extractable alongside interpreted captions. Interpreted
values never acquire literal source spans. Provider-supplied evidence is rebuilt
from verified spans; nonliteral, schema-derived and null values discard literal
offsets and unverified provider evidence while retaining their provenance. Fields without offsets use a single
summary of the input's effective provenance. If interpretation remains in that
input, unlocated fields are marked derived and obey `allow_derived_values` even
when literal span support is optional. Located literal fields on mixed pages
still use their precise regional provenance.

Document response decoding preserves unresolved/truncated page status and
aggregate usage for both HTTP and native readers, parsing the envelope once.
Composition and retained-page copies preserve exact JSON number tokens and
numeric types, including nested fields. Values are copied directly into the
owning arena without intermediate serialization or floating-point conversion.

Local reader results carry explicit per-item completion state and measured token
counts through direct execution and batching. A token count equal to a requested
limit does not establish truncation; decoder completion does. Document composition
can retain incomplete output for escalation or report it as truncated. Ordinary
Qwen reading continues to reject incomplete output. Measured local usage consumes
the shared token budget without estimating tokens from OCR text length.

Responses include bounded events and measured aggregate usage separately from
physical batch reports. Document formats and native visual compilation live in
`zig/lib/documents`: OOXML packages, DrawingML, themes, slide text/charts and
neutral page/region/output contracts. This library depends on image/PDF rendering;
it has no model, provider, inference-node or database dependency. Model selection,
verification, budgets and execution remain in `pkg/inference`.

### Durable read and extract jobs

The unified Antfly API supports `POST /ai/v1/read/jobs` and
`POST /ai/v1/extract/jobs`. Wrap an ordinary task request in `request`:

```json
{
  "request": {
    "model": "local/receipts",
    "documents": [{"id": "receipt", "url": "https://example.com/receipt.pdf", "media_type": "application/pdf"}]
  },
  "retention_seconds": 86400
}
```

Submission returns `202` with `id`, `operation`, `state`, `attempt`, `sequence`,
`expires_at_ms` and cumulative `usage`. It persists the effective backend request
before acknowledging submission. `GET /ai/v1/read/jobs/{id}` reads status;
`GET .../{id}/result` returns the complete ordinary task response after success.
The extract routes use the same suffixes. Result reads return `409` until success.
`POST .../{id}/cancel` fences an active attempt. `POST .../{id}/resume` leaves an
active lease alone; queued/interrupted attempts resume through the scheduler.
Terminal failures require a new request. The owner principal and task must match
for status, result, cancellation and events; internal artifact references are
never exposed by these routes. Missing jobs return `404`; expired jobs return `410`.

`GET .../{id}/events` streams committed progress groups using SSE numeric IDs.
Reconnect with `Last-Event-ID` to replay the suffix. Streams apply transport
backpressure, send heartbeats and close after one minute so clients can reconnect.
Disconnection detaches a subscriber; jobs retain their independent lifetime.
Progress events do not expose partial extraction rows as successful results.

The job owner uses the shared bounded background runtime, with up to four jobs
in flight and a periodic recovery/collection wakeup. It renews a 30-second lease
every five seconds. Explicit cancellation, ownership loss and retention expiry
fence publication; ambiguous calls remain charged when a new attempt takes over.
Eight interrupted attempts exhaust automatic retries. Standalone uses the
existing durable storage root and artifacts backend. Hosts may supply
`document_job_store` and `document_artifact_store`, or configured durable paths.
Document metadata has its own namespace alongside research jobs. Payloads use
scoped Antfly artifacts, not a separate blob-store format.

Durable jobs can execute locally or through an Antfly-compatible HTTP inference
connection. The database remains the job owner: artifacts, source snapshots,
checkpoint CAS, leases, retention and event replay stay with that authority.
The remote executor runs the same document graph and exchanges scoped storage
RPCs over one full-duplex HTTP connection. No storage credentials or callback
URLs are sent to the executor. Each connection pins one execution attempt;
proxies must stream both directions and must never retry, redirect or replay it.
After an ambiguous failure, only the job owner may claim a new fenced attempt.
Checkpoint replay does not count as newly measured model usage. Hosted admission
uses the remaining token ceiling, independently enforced by the executor;
unknown physical usage stays conservatively charged and is never estimated for
billing. Transport frames cap metadata at 1 MiB and raw payloads at 64 MiB, with
at most 1 GiB in each direction and bounded queue admission.

The native peer routes are `/ai/v1/read/execute` and `/ai/v1/extract/execute`;
applications keep using the public jobs endpoints. Colony sends those jobs to
the database and preserves a stable authenticated owner across polls and event
subscriptions. Its attached inference relay and shared inference proxy support
the duplex protocol. Older inference servers without these peer routes fail
closed. Document ownership follows the database's metadata leadership and
coordinated HA promotion when the job state and artifacts are shared.
Direct `/read` and `/extract` retain their synchronous behavior; process-local
reading handles keep their existing five-minute lifetime.
A durable extraction snapshots a retained reading before
using it, so subsequent handle expiry does not break resume.

### Durable document processing inside an index

Indexed reader and extractor producers invoke the same document executor with
an enrichment-owned persistence capability. They do not submit public jobs or
acquire an independently scheduled job owner. `/read/jobs` and `/extract/jobs`
are optional asynchronous APIs for requests independent of a table or index;
“independent” does not mean the standalone deployment mode.

For Antfly reader and extractor models, the invocation binds the exact request
to the document, producer, source sequence, reprocess generation and captured
catalog/source authority. The existing enrichment lease owns scheduling. Every
checkpoint mutation validates that captured tenure and source/catalog proof in
the same native storage transaction. Source bytes, renders, decisions, accepted
pages, extraction stages and final responses use the existing document
checkpoint protocol, stored with private enrichment preparation state. A
replacement invocation replays accepted stages and retains charges for unknown
provider calls. Local and remote inference receive the same retained capability;
remote calls use the existing duplex protocol with table routing, deadlines,
remaining-token admission and no transport replay.

Independent document invocations run in cohorts of at most four on the existing
runtime I/O executor. Their outputs publish in source order through the ordinary
asset publication path; its accepted source/catalog receipt advances enrichment
progress and feeds downstream chunk and embedding producers. A completed
inference response remains replayable while publication is pending. Other
providers and existing media-page batching retain their established paths.

Indexed checkpoints use the existing enrichment lease, including its heartbeat,
without a second job lease. They retain the complete causal source and upstream
artifact proof, including grouped inputs. Indexed checkpoints have no public-job
TTL. Acceptance retires intermediates; source replacement/deletion, producer
removal, and catalog/reprocess changes
invalidate previous preparation. The existing maintenance scheduler rotates
through bounded root pages and deletes at most 32 blobs per root per pass,
including while source replay is idle. Retirement compares the exact inspected
root before fencing writes and deleting blobs. A concurrent checkpoint or source
replacement requires a fresh lifecycle decision. Retained callbacks and responses use a lifetime-owned, bounded,
synchronized allocator and drain before their enrichment context is released.

A reader may receive a JSON document descriptor containing `documents` to
preserve media type, page selection, slide notes and visibility. PDF/PPTX source
URIs can also be inferred from their suffix or explicit media type. Extractor
producers support one document source per indexed invocation, while ordinary
text extraction keeps its existing response representation. Job APIs retain
their broader request forms and independent result/event retention. Authored
text/media parts retain their locator and prompt precedence over fallback
source text and the configured prompt.

### Ownership, recovery and shared artifacts

Document jobs reuse `BackendRuntime.durable_jobs` for bounded execution and the
existing restore-job leadership transition for promotion and draining. A common
job-state capability supplies authoritative point reads, paged inventory and
hash-conditional updates. Distributed metadata commits those updates through
Raft; coordinated standalone metadata includes them in its existing checkpoint
and HA WAL. The local research-job store remains the fallback for hosts without
that authority. Initial leadership imports existing local document metadata
without replacing an authoritative record.

Every publication carries its captured metadata term as well as the run's
revision, attempt and nonce. Promotion drains the old document owner before
installing its replacement. Failed acknowledgements reconcile against fresh
state; stale mirrors cannot grant a second dispatch. Accepted stages replay,
and uncertain provider calls retain their reservations. The coordinator and
worker ownership model follows graph-metric maintenance; document-specific
checkpoints and model accounting remain in the document executor.

Request installation commits the request journal entry, exact usage charge and
input root together. Payload writes that precede a rejected commit remain scoped
orphans for ordinary collection. A retry reconciles lost commit acknowledgements
without another journal entry or charge. Recovery also attaches a sole request
checkpoint written by the older two-step format, preserving its accepted payload
and journal identity. Public submissions and indexed producers use this same
installation primitive.

Runnable and retention indexes change atomically with job state and principal
quotas. Recovery visits at most 32 records per pass and admits four distinct
jobs concurrently. Lease renewal covers initial request persistence, request
hydration, inference, and draining retained callbacks. It moves the recovery
ticket to the new expiry.
Transient failures persist a retry deadline and bounded error code using the same
exponential backoff and deterministic jitter as index repair (250 ms base,
30 second cap). Native runtime errors and recognized remote error envelopes use
the same classification: rate limits and temporary capacity/transport failures
retry; invalid sources, incompatible models and exhausted budgets terminate.
Unknown HTTP 408, 425 and 429 responses also retry. Claiming the next attempt
clears the previous transient error code without resetting usage.
Collectors retain tombstones and revisit completed namespaces after a minute
to catch uploads that finish after ownership loss. Completed history is not
loaded into memory or scanned for ordinary job dispatch.

Public document submissions accept the operation’s request envelope and select
server-owned model configuration. Schema field names and extraction input data
are opaque document data; names such as `api_key` are valid there. Provider
configuration and credentials cannot be supplied as request envelope fields.

Configure the existing artifact lane before submitting independent jobs that
must survive HA promotion. Document jobs use a `document-jobs` subnamespace:

```json
{
  "public_inference": { "enabled": true },
  "connections": {
    "artifacts": {
      "kind": "external_io",
      "capabilities": ["storage.primary"],
      "external_io": {
        "protocol": "s3",
        "buckets": ["my-bucket"],
        "prefix": "my-deployment"
      }
    }
  },
  "storage": {
    "artifacts": {
      "connection": "artifacts",
      "bucket": "my-bucket",
      "prefix": "my-deployment/artifacts"
    }
  }
}
```

GCS is supported through an equivalent `gcs` connection. The configured opener
resolves secrets and refreshable credentials and enforces capability, bucket,
prefix and provisioning policy. Source download credentials remain separate
from this artifact writer. Remote executors receive scoped persistence
capabilities and cannot delete artifacts or obtain storage credentials.

Replacement owners must use the same artifact namespace. Jobs pin its protocol,
endpoint, bucket and prefix; changing the target rejects replay before reading
payloads. Credential rotation does not change that identity. The default local
artifact directory retains local ownership semantics. Changing placement does
not migrate existing payloads. The draft `document_artifact_uri` setting has
been removed in favor of this existing configuration.

HA job mutation is admitted only with authoritative metadata and a shared
artifact backend. Existing primary write gates fence demoted owners, and
Colony delegates only the known submission/cancel/resume routes to that
certification. Other HA mutation restrictions remain in force. Metadata peers
must support topology protocol version 43 before admitting the new job-state
command. Jobs inherit the configured replication durability policy; select
`remote_apply` when acknowledged state must be present on the takeover target.
Independent deployments require a common metadata authority or coordinated
replication before they can take over one another's jobs.

Raw sources, rendered views, reader/decision/extraction outputs, accepted pages,
validated rows and final responses use immutable checkpoints. The executor pins
request, source, definition, model, renderer and runtime identities; changed pins
reject resume. Reservations persist before model dispatch and CPU pixel work.
Replay uses committed stages without invoking the provider again; interrupted
work keeps conservative charges for model calls, tokens, pixels and output bytes.
`source_bytes` measures accepted input snapshots and active source preparation.
Abandoning a download releases its unpublished source reservation, allowing
retry even when preparation reserved all remaining source capacity. Committed
snapshots remain charged and replay without another download. Recovery also
normalizes source uncertainty written by older attempts.

Measured tokens and pixels carry bounded cost receipts through raster, leaf,
page and extraction-row checkpoints. Reusing identical content or overlapping
checkpoints restores each physical cost once per invocation, including after
resume. Receipt watermarks cover growing pixel grants; every item in a native
reader batch references the same aggregate model cost. Captures contain only
an operation's dependencies, rather than copying the entire run history.
Restoration validates budget admission before advancing any watermark, so a
denied replay cannot make a later attempt free. Logical model calls, page work
and output limits remain additive. Failed or ambiguous physical work retains
its conservative durable charge.

Model cost identities include the logical stage, run/owner, captured attempt and
lease nonce. Accepted checkpoints keep their original receipts across handoff,
while retrying a failed stage produces a distinct physical cost. A successful
sibling from an earlier native batch and a retried reporting item are therefore
charged independently, in either replay order. The nonce also prevents receipt
collisions if metadata rollback reuses an attempt number. Pixel grants already
have independent random identities. These cost identities do not change the
stable logical keys used to find reusable checkpoints.

Raster checkpoints store scalar JSON metadata separately from a versioned
binary receipt section. Fixed-width receipts avoid JSON expansion and fit within
the existing checkpoint frame ceiling; count admission uses the session's stage
capacity. The decoder validates frame lengths, version and receipt count before
allocating the receipt list, and can read the bounded older raster format.

The pinned document adapter identity is v7. Runs already prepared with an older
adapter must be resubmitted; request/source snapshots that have not been prepared
still use the existing storage format. The adapter pin prevents older executors
from taking over runs that use the current frame and physical-cost semantics.

Known failed workers settle their reservations into durable uncertainty without
refunding physical usage, allowing accepted partial results to finish. If storage
cannot acknowledge that transition, local liveness still excludes the failed
intent and ownership handoff recovers its charges. Admission waits only for this
attempt's known non-failed workers, which can still reconcile their upper bounds;
untracked reservations remain spent capacity. Pixel credit planning uses the same
distinction. Whole-view and crop failures abandon grant liveness at the error
boundary. Page and region workers also close any remaining grants before their
completion is observed, rather than waiting for group joins and arena teardown.
Physical charges remain consumed, including when a crop exceeds its encoding
ceiling. Exhaustion caused by failed work returns immediately; successful live work can
still release capacity and admit a waiting page or crop.

Duplex upload and response processing share a synchronized invocation allocator.
Memory admission, backing allocation/free and usage accounting are atomic with
respect to one another, including for indexed document producers; concurrent
transport cannot exceed the invocation ceiling by racing two admissions.
Opaque remote model bindings used inside a
durable composite must declare `max_total_tokens`, a deployment-enforced hard
ceiling for one call. An unknown ceiling fails preflight before source downloads.

Event chunks commit with stable sequence cursors. Subscribers scan the journal
once, then validate only newly appended manifests. Retention is absolute (60
seconds to seven days); polling, renewal and resume cannot extend it. Expiry first
persists a tombstone and fences uploads. Maintenance deletes bounded batches from
that run's artifact namespace, retries after restart and revisits tombstones to
catch late orphan uploads. Generic research-job cleanup never deletes document
roots. Hard token-limit failures terminate jobs through the shared native/HTTP
failure policy, preserving their error identity and conservative charges without
automatic redispatch. Recovery tests cover actual artifact/metadata close-and-reopen,
capability ownership, stage replay, reservation accounting and collection between
batches.

`max_output_bytes` admits full structured pages, extraction payloads and retained
events across the run, including child work and verification attempts. Extraction
reserves IDs and source metadata before calling any extractor. `usage.output_bytes`
reports this cumulative payload admission, rather than just visible text or the
size of the final envelope. Each response also has a hard encoded JSON byte cap:
pages and rows are written directly into one capped buffer, with temporary data
released after each item. An oversized page or row stops further source/model
processing. Artifact retention reserves room for its handle and TTL within
the same response cap.

Extraction request JSON has a maximum depth of 128, counting the root as depth
zero. Deeper schemas, options, content or metadata return `DocumentOutputTooLarge`
(HTTP 413) before document loading or model calls.

## Comparing readers

Run `scripts/eval_document_models.py --corpus CORPUS --model BASELINE --model
COMPOSITE --output report.json` against a configured inference server. The report
records exact character edit distance, selected-page coverage, unresolved pages,
table cell precision/recall where labelled, latency, source hashes and reported
work. Whitespace normalization is explicit per corpus case. Costs are not inferred
from model names or token counts.

`scripts/testdata/document_models/corpus.json` provides deterministic PDF and
slide-order smoke fixtures. Extend the same corpus format with labelled scanned
PDFs, receipt photos, long item lists, distortions and image-heavy slides for
quality qualification. The small smoke corpus does not establish OCR quality on
those populations. Existing `scripts/eval_extract.py` separately evaluates
schema-driven extraction against labelled source spans.


Cloudflare vision can also be a remote document reading binding:

```json
{
  "version": 1,
  "kind": "document_agent",
  "allow_remote": true,
  "models": [{
    "name": "vision",
    "provider": "cloudflare",
    "operation": "read",
    "account_id": "account",
    "token_env": "CLOUDFLARE_API_TOKEN",
    "model": "@cf/mistralai/mistral-small-3.1-24b-instruct"
  }],
  "read": { "reader": "vision" }
}
```

Remote vision uses the shared rendered page/crop cache and the Chat Completions
finish reason. Its output has interpretation provenance without invented OCR
geometry. Every visited extraction definition's source/page/token ceilings and
original deadline remain active across its preprocessing and extraction phases.
Numeric JSON lexemes survive provider decoding and ownership transfers, so
verification does not equate distinct decimals after floating-point rounding.
