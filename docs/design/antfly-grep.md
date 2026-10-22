# Antfly-grep: indexed query planning

## Decision (2026-10-19)

Use Antfly's existing query API for indexed grep. Reuse its full-text predicates,
filters, projections, ascending-ID pagination, retained snapshots and authorization.
Do not add a grep-specific `grep_scan` request or per-document miss-proof protocol.
The CLI performs final matching with the same matcher used for local files.

The earlier response-elision experiment reduced a synthetic miss's response bytes,
but still read and validated its content on the server. Existing indexed substring
and regex queries can exclude nonmatching documents before transfer. A separate
API, source-contract dependency in the server, SDK fields and snapshot certificates
are unnecessary for this capability. End-to-end latency and cost still need
representative indexed workload measurements; reduced bytes alone do not prove
lower total cost.

## Table contract and configuration

Documents retain the versioned source contract described in
[the CLI design and usage](../../zig/pkg/antfly-grep/README.md): content, path, URI,
revision, timestamps, metadata, regions and permissions. Providers such as S3,
GCS, R2, Box, Dropbox and GitHub are ingestion concerns. Query planning uses the
stored document and its indexed revision, independently of the original provider.

Configure table names and the document contract root, for example:

```json
{"name":"sources","document_field":"grep"}
```

The shared [logical-field query design](logical-field-queries.md) owns operation
semantics and server planning. There is no `content_index` routing declaration.
Mapping `grep.content` with `"x-antfly-types": ["text", "substring"]` allows the
server to accelerate containment. The server selects a compatible representation
and verifies original values; index suffixes are not part of the CLI contract.
The older raw-trigram `candidate_index` remains a compatibility option.

## Conservative predicate planning

Every pattern alternative must supply a proven necessary case-sensitive literal
from the native matcher. Send ordinary `contains` predicates against the logical
`grep.content` field, combined with OR. If any branch lacks a proven literal,
omit candidate pushdown. Never send the user's entire regex under a different
dialect and assume equivalence.

The request uses canonical `query`, preserves the user's `filter_query`, selects
ascending `_id` order from page one, and replays the predicate for every retained
snapshot page. `execution.value_verification.allow_scan` explicitly permits fallback
when no safe compatible index constraint exists. The server's verification
budgets and failures remain explicit. Fetch only required source-contract fields,
then apply permissions/path checks and the complete CLI matcher, including
line/word boundaries, captures, context and replacements.

The earlier `match`/`regexp` companion-field configuration was superseded by the
shared logical-field contract before merge. Historical benchmark reports describe
the binaries actually measured; they do not qualify the new server planner.

## Exhaustive modes and accounting

Do not prune candidates when output needs nonmatching documents: inversion,
files-without-match, file discovery, include-zero, passthrough, or unrestricted
natural-language/jevgrep exploration. JSON and statistics modes preserve their
existing exhaustive source validation and file/byte totals by omitting candidate
pushdown. Explicit lexical decision prefilters can use candidates when ordinary
output semantics allow it. Unsafe encodings and unprovable pattern branches
retain the full query traversal as well.

The optimization does not manufacture corpus-wide counts from a candidate page.
A future API improvement should expose generic examined/returned/transfer metrics
and index capabilities through existing query diagnostics. Any change to the
meaning of JSON scan totals must be explicit and versioned, independently of
candidate filtering. Server query errors, authorization failures and snapshot
expiry retain the existing explicit failure/partial-result behavior.

## Verification and further work

Compare filtered and ordinary queries for literals, regexes, alternatives,
normalization/case false positives, Unicode neighbors, punctuation and line
boundaries. Verify fallback for unsupported patterns and exhaustive modes,
explicit filter composition, named-index forwarding and stable pagination.
Run real-server checks with one and multiple shards alongside concurrent-update,
snapshot-expiry, authorization and large-document tests.

Measure representative repositories and indexed corpora before extending the
planner: first-result latency, server/client CPU, response bytes, index footprint
and ingestion cost. Prefer existing index/query capabilities and shared query
observability. Extend safe predicate extraction only with no-false-negative
properties; avoid a new grep-specific server protocol.


### Revision-bound original-value reads

Logical indexed predicates use the shared source reader described in
[Logical field queries](logical-field-queries.md#remaining-storage-work-and-performance-qualification).
Large document originals can reuse the authenticated `DPD2` object-path
authority (with `DPD1` fallback) and range reads; ordinary small documents and missing/legacy authority
retain full-source reads. This changes the physical work, not query semantics or
original-byte evaluation budgets. The source epoch is pinned for the query,
selected hashes are checked, arrays remain intact, and projected JSON cannot be
cached as a complete document. `stored_field_sources` makes this path observable.
The implementation adds no public scan endpoint or new document schema.

Substring-ingestion memory and indexed-source startup are specified in
[logical field queries](logical-field-queries.md#bounded-substring-token-production-and-source-bootstrap).
The indexing producer preserves the existing analyzer/posting contract while
avoiding materialized suffix arrays. Source verification starts with a bounded
two-document sample and avoids per-row timing for buffered data. These changes
require ordered analyzer and postings differentials, allocation-failure coverage,
and paired HTTP profiles; they do not imply a local CLI speedup over ripgrep.

## Common Decisions client and gateway ownership (2026-10-21)

Grep speaks the public named-question Decisions contract. The common denominator
is one `input` (text or ordered user messages containing text and images), a model,
and predicate questions; multiple questions share
one source serialization. The OpenAI profile uses its canonical `/v1/decisions`
endpoint. Antfly's `/ai/v1/decisions` gateway owns provider routing, including Jev's
private state/question protocol. Grep no longer carries provider-specific Jev
serialization or guessed third-party gateway routes.

Antfly batching (`inputs`) and versioned shared contexts are capabilities selected
explicitly by the Antfly/local profile or transport option, not assumed of a generic
OpenAI-compatible service. The scheduler uses an effective batch size of one for
the common contract, preserving bounded concurrent requests, cancellation, request
budgets, retry/split limits, persistent connections, and warm cache hits. Common
inputs reuse the existing streaming JSON string encoder so context is not copied
into a temporary concatenated source. Shared-context batching remains available
where supported; it reduces repeated serialization without dropping targets.

The lightweight shared predicate decoder lives in `zig/lib/decisions` and uses
bounded stack state with no per-answer allocation. It checks names, duplicates,
finite probabilities, and optional typed metadata. Refusal, abstention, missing
answers, and non-typed decision methods are incomplete evidence rather than
negative relevance. The cache transcript includes transport identity and a new
version; old semantic results are not silently reused across this transition.

Credentials are destination-bound. OpenAI's environment key is eligible only for
the canonical OpenAI endpoint; Antfly's key is eligible only for its profile.
An explicit `ANTFLY_GREP_TOKEN` configures a custom destination. Endpoint/provider
overrides invalidate saved credentials, and HTTP redirects cannot forward keys.
New auth profiles contain no secrets. Legacy token profiles are read for migration;
new `--stdin` persistence is rejected with environment-key guidance. Table access
continues to use its separate token. Local model hosting remains an explicit
backend and does not require a gateway.

Fixture conformance covers both wire shapes, shared-context retention, multiple
questions, bounded concurrency, warm-cache request elimination, refusal/abstention,
malformed answers, and credential routing. These checks establish protocol and
orchestration behavior, not live-provider relevance or token cost. The CLI's
compatibility baseline is Decisions, not Chat Completions or Structured Outputs.


## Multimodal source evidence and Cloud boundary (2026-10-21)

PR [#1252](https://github.com/antflydb/antfly/pull/1252) supplies the common
Decisions message contract. Grep uses it with `--media images --ask QUESTION`:
one visual unit and its descriptive text per batch item, with all requested
questions sharing that image upload. Antfly supports multimodal batch envelopes
from #1253; explicit common single-input services remain supported. `--media text` remains the default. This is visual
question evaluation, not a claim of OCR, object detection, or model relevance.

### Versioned units and storage

[Document v2](../../zig/pkg/antfly-grep/schema/document-v2.json) extends the
existing source row without changing the query API. The table still declares just
its logical `document_field`. The v1 text contract stays readable. V2 permits
empty extracted content for image-only documents and an optional `evidence`
manifest, with at most 4,096 uniquely named units per parent revision:

```json
{
  "id": "slide-7",
  "kind": "slide",
  "location": {"slide": 7},
  "text": "Optional slide transcript",
  "label": "Checkout flow",
  "asset": {
    "field": "payloads.slide7",
    "mime_type": "image/png",
    "sha256": "<64 lowercase hex characters>",
    "byte_length": 65536,
    "recipe": "slides-render-150dpi-v1"
  }
}
```

Kinds are `image` (whole image), `page`, and `slide`; page/slide numbers are
one-based. The asset field is an ordinary logical field **on the same parent
row**, containing an inline image data URL. It is never a provider URL, arbitrary
fetch target, private key, or cross-document reference. Parent content, manifest,
image fields, permissions, and revision must be published atomically. A rendering
recipe identifies all pixel-affecting settings and its version. The digest binds
the actual materialized image bytes, independently of the source/export digest.
Canonical `uri` identifies the source; it must not become a data URL or a signed
materialization URL. The source revision binds provider revision and export
identity. Metadata-only, move, export, render, and permission changes all need an
explicit revision/publication policy.

`zig/lib/source_evidence` owns native manifest validation and bounded encoded-image
inspection. `zig/lib/decisions/evidence.zig` is the shared server/client message
validator, replacing a server-private copy. DocsAF's `ToEvidenceRecord` builds
v2 rows from a source document with explicit access metadata. Its optional
`PreferProviderID` keeps Drive IDs stable across renames/moves, while retaining
canonical URI separately from inline/private materialization URLs. The Drive
source records provider version; connectors still own export and ACL completeness.
The adapter does not render documents or infer public access from OAuth consent.

### Retrieval, authorization and cost

Grep first enumerates projected metadata/manifests through the existing ascending-ID
snapshot query. It fetches each image separately with an ID-constrained projected
query against the **current** parent row, retaining table filters, table token,
and server row authorization. It checks parent identity, client permission
intersection, captured revision, declared length/MIME/digest, actual encoded
format, and image dimensions before any decision-cache lookup. Missing or denied
rows fail; revision changes and corrupt assets fail explicitly. It neither
restarts silently nor evaluates mismatched revisions. The configured principal
list is only an additional intersection; authenticated server row filters are the
security boundary (see [authorization](../reference/auth.md)).

The initial storage path is same-parent inline payloads, supported by existing
logical projections. Keep payload fields stored-only rather than indexing base64
as text; ingestion must bound total render count/size as well as individual units. A Cloud object-store materializer is future work: it must
preserve these same ownership, authorization and revision checks behind a generic
asset boundary. There is no new `grep_scan`, Drive-download endpoint, or public
media URL requirement. Fresh asset reads must reauthorize even when a decision
score is cached. Snapshot membership alone must never be treated as an enduring
permission grant; Cloud must specify and test revocation/current-auth fences.

Processing uses bounded waves of at most 16 units and 8 MiB of conservative
serialized-evidence estimates, inspects headers without allocating a decoded
raster, base64-encodes local bytes once, and checks
the 1 MiB serialized Decisions evidence limit with a counting writer. Defaults
are 512 KiB per encoded image and 16,777,216 pixels; `--max-image-bytes` (up to
768 KiB) and `--max-image-pixels` tune admission. Metadata/page memory, source bytes,
request count, evaluation count, deadlines, cancellation, retries and retained
ranked-output limits continue to apply. Antfly `inputs` batches and shared text
contexts preserve image messages. Custom services retain explicit single-input
transport. Image evidence is never subdivided into textual halves. Oversized
batches split into partitions, and a failed partition does not suppress valid
siblings. Envelope identities are checked before any score becomes visible;
refused/failed items remain incomplete, while valid siblings can be emitted and
cached. Reported usage is retained even when item decoding fails. The gateway admits image-capable deciders and reserves image-token
budgets; unsupported/text-only adapters reject rather than discard media.

Decision keys bind image digest/MIME, unit metadata (including recipe/location),
questions, model/transport, and existing authorization/revision scope. Persistent
score records contain scores, not images. A warm score avoids provider inference,
not asset authorization, image validation, index HTTP, or all CPU work. Zero total
cost is impossible. Cost reductions come from explicit scope filters, stable
exports/rendering, bounded thumbnails, shared questions, persistent connections,
and safe score reuse. Neither resizing nor PDF/slide rendering happens inside
this initial grep path. Local PNG/JPEG/WebP/GIF files work directly; PDF/pages
need indexed renderings. Stdin and workspace media queries are rejected until
captured media lifetimes can preserve snapshot semantics.

### Search and result semantics

Image mode evaluates every visual unit in the explicitly filtered source scope,
subject to visible budgets/partial results. It rejects lexical prefilters and
repository triage, avoiding false negatives when
an answer exists only in pixels. Text mode and visual mode are explicit choices;
automatic modality selection is not inferred from the question. Future semantic
candidate retrieval must be an explicit ranked/approximate mode with candidate
limits and coverage, separate from exhaustive visual evaluation. A lexical or
embedding candidate set cannot silently stand in for exhaustive visual search.

Ranked files retain complete evidence records within the existing output budget.
JSON `type: evidence` carries source/table/document/revision provenance, unit ID,
kind, page/slide location, image digest/recipe/dimensions, and model probability.
Text output uses `path#page=N` / `path#slide=N`. Results never invent source line
numbers, bounding boxes, OCR precision, or detection confidence. Model scores are
relevance estimates, not proof. In image mode counts refer to matching units.
An error after a successful unit preserves its completed output as partial results.

### Colony / Antfly Cloud delivery

Cloud work is specified in [Colony #790](https://github.com/antflydb/colony/issues/790),
not implemented in this repository. Colony owns OAuth/refresh secrets, Drive change
cursors, effective ACL normalization, group membership, sync/reconciliation,
private original/export/render storage, atomic publication and deletion/retention.
`antfly-cloud` owns login, connections, source discovery and short-lived scoped
sessions; `antfly-grep` consumes those authorized tables and the inference gateway.
Users should not copy Drive tokens or manually reconstruct table schemas.

Drive stable file IDs survive moves. A source revision must bind the native
provider version and exact exported bytes; derived pages/slides bind renderer
recipe and image digest. Drive links, original/export payloads, text, and visual
units share the parent source identity and effective access. No assumption that
successful ingestion grants every organization user access is permitted. Recheck
current access before returning private assets or using cached answers. Never
publish a new visible manifest before all required assets and ACL state are ready.

Fixture tests qualify wire shape, cache scope, narrow projection, authorization
failures, revision/digest failures, limits and partial output. They do not qualify
live provider relevance or image token prices. Cloud acceptance additionally
requires real Drive ACL/group/revocation scenarios, concurrent revisions,
rendering failure recovery, lifecycle cleanup, cross-tenant isolation and labeled
visual-question quality/cost evaluation. These remain issue deliverables.


### Multimodal batch retrieval and admission follow-up

Each indexed wave projects all distinct same-parent payload fields in one
current authenticated ID query. These point reads omit `order_by`, cursors and
`remote_snapshot`; requesting pagination ordering would otherwise capture and
publish an unnecessary retained cut for every image read. Manifest enumeration
continues using ascending-ID retained pagination. The point query preserves the
table credential, user filter, server row policy and explicit client principal
intersection, and checks the current parent revision before examining assets.

Every unit still checks digest/MIME/length/pixels before score reuse. Identical
field/digest/length/MIME references within a wave share one verified immutable
image view; distinct unit prompts and cache identities remain independent.
Materialization and provider work do not accumulate the whole document or corpus.
Waves are admitted in source order. Quiet/file-selection modes use one-unit
waves; finite result caps bound wave width by remaining selected-unit capacity.
Source/evaluation/request limits count actual admitted work and HTTP attempts,
including retries; exhaustion preserves completed evidence and exits 2.

`--max-filesize` skips oversized local images before encoding or inference;
`--max-file-size` enforces a hard local read limit. `--max-image-bytes` remains
an independent visual-evidence admission limit. Quiet mode stops on a selected
file, including the negated selection of `--files-without-match`, and emits no
filename. These semantics also apply to text, multiline and buffered input.

The byte/round-trip fixture compares the previous binary and this implementation
using identical image units and fixed-score local servers. It cannot establish
live relevance, token prices or a universal throughput ratio. Batching primarily
removes HTTP overhead and enables the gateway's bounded provider fan-out; it does
not imply reduced model tokens per image. The cross-parent scheduler below
preserves each parent's authorization, revision, result ownership and budgets.


### Bounded cross-parent pipeline (2026-10-22)

Indexed visual search uses at most eight persistent worker lanes, further bounded
by provider concurrency. Lanes borrow manifest page data; every lane is joined
before either its materialized asset page or manifest arena can be released.
Each lane owns mutable matcher and decision state and shares the invocation's
atomic budgets, cancellation, HTTP pools, score store and synchronized ranking or
spill owner. Lane output is charged to the common pending-output budget and
published in discovery order; ranked results retain their existing stable order.
Compatible model work coalesces across parents with unique wire IDs. A
single-parent window retains intra-document provider parallelism.

An asset window contains at most eight parents and 8 MiB conservative evidence.
Only equal image-field projection shapes are grouped. Unioning unrelated fields
would overfetch those fields on every row. A group is one current ID-filtered
query, with no ordering or retained pagination state. Each returned parent
independently checks access and revision before its payloads are validated.
Missing/denied/changed parents do not authorize their image bytes or score reuse.
The asset page is immutable and pinned until all lanes finish; validated URL
views can borrow that page without a second base64-string copy. The grant covers
one wave only: later waves fetch and authorize current parent state again.

Response-size/HTTP-413/memory rejection of a parent group falls back to bounded
individual parent reads. Oversized same-parent projections split the image-field
group recursively; each partition preserves credentials/filter/revision checks,
request accounting and validated siblings. One oversized field fails explicitly
rather than causing repeated identical subdivision attempts. Ordered manifest
pagination may shrink its page size; an unordered multi-ID projection must never
silently shrink its limit and omit requested parents.

Explicit finite request budgets, finite source/evaluation budgets and early file
selection retain ordered serial admission. The default 1,000-attempt safety cap
continues to apply to actual HTTP requests across all lanes; parallelism does not
turn it into an unlimited allowance. Groups respect per-document selection limits
and the remaining indexed-document quota. Active lanes, borrowed pages and output
are bounded independently of corpus size. This is a latency/throughput tradeoff:
more active parents may raise RSS even while reducing requests and CPU.

The benchmark driver includes multi-page/multi-parent cases and large encoded
images. Its Unix child wrapper records child wall/CPU time, first stdout byte and
peak RSS without charging the Python harness to the CLI. Reports retain exact
binary hashes, alternating trial order, output equivalence and fixture delays.
First stdout in the ranked fixture follows ranking completion; it is not a
claim of streaming an early unranked hit. Live providers and real Cloud ACLs
remain separate qualification.

Warm scores still require fresh authorized asset validation. Avoiding those image
bytes requires a generic server-owned source/asset identity contract: an identity
must bind the exact stored bytes at the authorized read cut, survive only valid
immutable revisions, and be distinct from caller-supplied revision or digest
metadata. A trusted identity may select a previously verified score; absent or
changed identity must materialize and validate bytes. No caller-written manifest,
TTL, cached permission result or segment ID establishes that guarantee. The CLI
retains full validation until that server contract exists.
