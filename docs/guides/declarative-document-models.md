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
one-based original page/slide `number`. Selection never renumbers source pages;
results follow original source order, regardless of selection order. Independent
sources remain independent extraction contexts. URLs inherit the deployment's
existing content security restrictions.

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
reading policy. Supply the ordinary extraction `schema` and `schema_version`.
Named record fields, entity/relation behavior and long-document handling remain
owned by the selected extraction implementation. String inputs and arrays of
text content parts normalize once to the same canonical text before any model
call, so extraction and evidence offsets share one representation. Adjacent text
parts are separated by a newline. Image-only inputs use the direct document
form and the composite's reading policy.

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
against each result's own entity indexes or IDs before comparison. Literal spans are checked
against the exact input and mapped to original page text; declared Unicode or
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

## Cloudflare providers

Cloudflare is a general Antfly extraction/decision provider, shared with SQL and
enrichment. Document composites are consumers of those provider protocols.
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

The adapter sends native `state` plus typed `fields` or `questions`, never chat
messages. Clairic supports one named record with text, text lists, numbers, dates
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
slide policy, and separately attributed speaker notes. Visual slide rendering,
chart interpretation and perspective correction are unavailable and are reported
as such. XML inspection recognizes drawings independently of namespace prefixes,
including vector shapes, pictures, charts and embedded content. A slide with
unsupported visual content remains unresolved. Slide layouts and masters are
parsed once per source and shared across slides. Inherited parts retain their
source identity and visibility metadata; template prompts are excluded. Visible
inherited decorations, conditional footer content, and backgrounds that require
composition or rendering keep the slide unresolved until those capabilities are
available. PDF native inspection checks
painting operators without decoding images or materializing vector paths;
text-bearing pages with drawings still require visual inspection. PDF annotation
coverage is inspected separately from page content streams, using bounded metadata
and reference resolution. Potentially visible stamps, widgets/form fields and
other annotations remain unresolved because appearance compositing is unavailable.
Both native painting and document OCR reject that unsupported rendering path;
page-content-only rasters cannot certify coverage. Hidden annotations and
borderless links without appearances do not require visual work. See the
[PDF reference, annotation flags](https://printtechnologies.org/wp-content/uploads/2020/03/pdfreference_v1-4-1.pdf)
for the screen/print visibility rules.

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
without an immutable weight revision. Per-run execution is currently serial; existing
reader executors own physical batching. Model discovery reports this limit.

Initial OCR output and accepted revisions must retain trustworthy native text
coverage. Missing coverage triggers whole-page escalation and prevents a router
from accepting an incomplete transcription. Known broken font mappings can be
superseded only through source verification. Regional revisions preserve page
text outside the corrected region, nested regions, tables and structured fields;
ambiguous table ownership is reported as unresolved. An accepted regional revision
replaces its owned table set, including when the replacement contains no tables.

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
