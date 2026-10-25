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
excluded. Text rendering, charts, theme-dependent styles and
effects remain unsupported. Unsupported content is reported
explicitly. XML inspection recognizes drawings independently of namespace prefixes,
including vector shapes, pictures, charts and embedded content. A slide with
unsupported visual content remains unresolved. Slide layouts and masters are
parsed once per source and shared across slides. Inherited parts retain their
source identity and visibility metadata; template prompts are excluded. Visible
inherited text, conditional footer content, and unsupported drawing styles keep
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
completed pages is buffered. Compatible ready crops from independent pages
coalesce into shared reader invocations; incompatible groups dispatch concurrently
within the call ceiling. Token usage is charged once for each physical batch.
Region refinement within each page remains ordered. Model discovery reports
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
physical batch reports. Artifact reuse is implemented; durable checkpoint/resume,
job endpoints and task event streaming remain separate lifecycle work. No public
job or resume capability is advertised by this implementation.

The durable storage foundation reuses Antfly's existing artifact backend for
immutable payloads and a separate task metadata namespace for owner checks,
CAS revisions, lease attempts and random fencing nonces. Stage reservations are
durable before dispatch; interrupted work retains conservative charges across
attempts. The executor facade replays accepted stages from a bounded index, and
event chunks commit atomically with their stage results and receive stable
sequence cursors. Actual artifact/metadata close-and-reopen tests cover recovery.
This foundation is not yet connected to document requests or public job routes;
source snapshot admission, task lifecycle/GC and streamed HTTP events remain
integration work. Process-local reading handles retain their existing lifetime.

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
