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

### Portable sources and Cloud delivery

A source is an existing Colony import job with `mode: source`. Existing
provider-auth connections hold acquisition credentials; existing import-job
endpoints own configuration, scheduling, runs, retries, pause/resume and archive.
The job ID is the stable source ID. There is no parallel Cloud source-management
or discovery API. [Colony #790](https://github.com/antflydb/colony/issues/790) tracks
the hosted experience and remaining qualification.

The public [sources module](../../go/pkg/sources/README.md) owns acquisition,
revision-bound export/normalization, stable identities and provider absence
checks. It supports GitHub code/collaboration, Drive Docs/Slides text exports,
GCS and S3. Colony injects its credential/installation adapters and supplies
PostgreSQL jobs, scheduling, private staging, access policy and hosted retention.
The standalone `antfly-source sync` runtime uses the same providers and normalizer
with a durable filesystem inventory and ownership journal; it needs no Colony or
Cloud account. It can run from an external scheduler independently of grep.

| Responsibility | Hosted runtime | Self-hosted runtime |
| --- | --- | --- |
| Connection/source management | Existing connection/import-job API/UI/CLI | Source profile or application API |
| Secret resolution | Encrypted Cloud credentials/current installation access | Injected resolver; runner references environment variables |
| Acquisition and canonical text | Shared sources module | Shared sources module |
| Jobs/checkpoints/reconciliation | Colony jobs, staging and ownership ledger | Runner lock, pinned inventory and ownership journal |
| Storage/query | Ordinary Antfly tables and query snapshots | Ordinary Antfly tables and query snapshots |
| Search configuration | Ordinary catalog queries with Cloud session | Ordinary catalog queries or file export with table credentials |

Grep owns catalog/profile loading, source aliases and selection, authentication
adapters, exact matching and evidence evaluation. It does not own provider refresh
secrets, background synchronization, or Cloud source-management state. A convenient
indexing command delegates to the shared ingestion runtime rather than embedding
a second connector/extraction pipeline into the search process.

The durable searchable-source catalog lives in an ordinary Antfly table named
`antfly_sources` by default (configurable for self-hosted profiles). It is shared
infrastructure for hosted and self-hosted ingestion, not Cloud-specific storage or
a new core database API. One version-1 `source_catalog` record per stable source
ID contains its v2 search binding, provider kind, document version, published
revision, readiness, freshness lease and explicit sharing metadata. It contains
no credentials, provider selection/configuration, session readers or job checkpoints.

The shared catalog writer verifies a snapshot-visible `syncing` record before
content mutation and publishes `searchable` only after content and reconciliation
are query-visible. Each standalone publication has a unique `published_revision`
fence, also included in file-export bindings. After acquiring its first content
snapshot, a self-hosted reader re-reads the current binding and requires the same
searchable publication and unexpired lease before consuming any rows. A sync
that starts before the snapshot therefore fails closed; a later sync can safely
coexist with continuations on the already pinned snapshot. Cloud additionally
checks current policy before execution and rechecks the job attempt/catalog
publication after acquiring query snapshots, before releasing any response bytes.
The selected query returns `X-Antfly-Source-Publication`; grep requires and pins it
alongside the access proof, and sends it as a continuation precondition. Job
attempt identity fences even a completed sync with an unchanged inventory.
There is no cross-table transaction: interruption leaves a
non-searchable record or a bounded prior lease, and retry repairs publication.
Both ingestion runtimes use the shared canonical query barrier. It compares a
canonical digest of version, URI, path, revision, indexing time, permissions and
all metadata (including the content SHA), rather than treating a provider revision
as the identity of normalized output. Metadata-only changes must be query-visible
before publication. Cached GCS bytes are authorized against their selected
`generation` with a matching generation precondition, never against the live
replacement's ACL. S3 inventories likewise persist `version_id`; prefix listings
resolve each selected entry with HEAD and reject an intervening ETag/size change.
Downloads and cache authorization request the exact version (including literal
`null`) and validate the returned version, ETag and size. Versionless legacy
snapshots fail closed on versioned responses, even for identical replacement
bytes; operators must enumerate a new snapshot. Unversioned objects retain
conditional ETag/size checks. Cache authorization uses the GET content data plane,
including SSE-KMS decryption, rather than metadata HEAD. It requests one byte for
nonempty objects, validates the complete-object size from Content-Range, and uses
an ordinary GET for empty objects. Ignored ranges still consume at most one byte.

MCP trusted-principal tokens retain their broad read scope and add concrete
`table_operations` write/admin grants for allowed application tables. Create/restore
requests add their concrete destinations only after intersection with current
Cloud grants and reservations. The signed `mcp_creation_discovery` hint exposes
creation tools on empty instances without granting invocation authority. Reserved
catalog/source tables receive no write grants. Wildcard writers use the current
table catalog to bound the request's write scope; missing scope cannot become a
wildcard grant. Older Antfly runtimes ignore the additive claim and retain only
the read scope, so a mixed-version rollout fails closed.

The ingestion runtime owns catalog writes. Server authentication and row policy
control catalog visibility as well as content; sharing metadata is descriptive,
not a grant. In Cloud, current connection/sharing/table policy overrides retained
catalog rows and snapshots, and non-admin catalog mutations are blocked. The Cloud gateway rejects
row-filtered lookups, scans, graph and artifact routes whose contracts cannot
consume the injected security filter; those readers must use supported query
routes. Drive Docs/Slides exports have unknown lengths: staging enforces each
export's byte ceiling and charges actual bytes atomically with its checkpoint
against the run budget, preserving the charge across retries. S3 canonical URIs
escape each key path component while acquisition/ownership use the raw key;
literal `%`, `?` and `#` characters never alter URI identity.

`antfly-grep --source-endpoint URL --source NAME` reads the catalog through normal
table queries with bounded snapshot pagination. `--source-table` selects a custom
catalog table. Provider secrets are not needed by grep. Cloud applies current policy on these same table queries. PostgreSQL remains
operational configuration/jobs, not a substitute for searchable catalog publication. File catalogs remain explicit exports/configuration.

Portable file exports use catalog version 2, independently of table configuration,
durable record and document versions. Files contain an absolute Antfly API
`endpoint`, `sources` bindings and optional explicit reader hints. The
[file schema](../../zig/pkg/antfly-grep/schema/sources-v2.json) contains no provider
or query credentials. Both table-record discovery and file loading adapt to the
same internal indexed-table configuration; a server never chooses a credential
destination through a catalog record.

Cloud grep gets its instance endpoint and short-lived data-plane credential from
`antfly-cloud context --json`, then uses the same catalog/table query implementation
as self-hosted grep. There is no `/cloud/v1/{instance}/sources` route. Catalog
traversal uses the ordinary query snapshot and ID cursor, bounded independently of
content search. Cloud returns `X-Antfly-Subject` and `X-Antfly-Catalog-Access` on
catalog queries; continuation requests pin the policy digest and fail explicitly
if visibility, effective table row policy or identity changes.

Content and parent/asset projections use existing table queries with
`X-Antfly-Source-ID`. The first successful selected-source query returns its
`X-Antfly-Source-Access` policy proof. Grep pins it across all later pages and
asset projections, including after token renewal. The proof binds current source
sharing, principal/group context and effective row policy, not unrelated source
sync state. Cloud checks current authorization and readiness on every request.
These headers grant no authority and do not introduce a database endpoint.

Cloud queries rely on gateway-enforced document ACLs. Grep does not consume or
union a separately exposed Cloud reader list, avoiding stale client reader hints
that could silently omit newly authorized rows. It accepts Cloud rows only after
verifying the authenticated subject and policy response headers. Self-hosted/file
catalogs retain the additional explicit local principal checks.

Self-hosted deployments retain their normal Antfly authentication/row-policy
boundary; catalogs and permission metadata cannot replace server authorization.

Colony retains effective ACL normalization, group membership, private
original/export/render storage, atomic publication and deletion/retention. The
standalone text runner does not implement browser consent, provider-native ACL
mirroring, distributed scheduling, binary/visual extraction or Cloud enrollment.
Use separate tables/keys or a trusted row-policy gateway when self-hosting shared
private content. Both runtimes keep provider, data-plane and inference credentials
separate. Live-provider and visual qualification remain required before claiming
all of #790's acceptance criteria.

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
by provider concurrency. Allocate lanes lazily for the actual window and omit
the local-file pipeline for indexed-only invocations. Lanes borrow manifest page data; every lane is joined
before either its materialized asset page or manifest arena can be released.
Each lane owns mutable matcher and decision state and shares the invocation's
atomic budgets, cancellation, HTTP pools, score store and synchronized ranking or
spill owner. Lane output is charged to the common pending-output budget and
published in discovery order; ranked results retain their existing stable order.
Completed lanes immediately accept later parents on the same manifest page.
Completion records own their output and its existing global quota reservation;
a fixed 16-slot reorder buffer also bounds zero-byte result metadata. When that
buffer approaches capacity, admission waits for the oldest outstanding parent.
A slow parent therefore cannot grow a corpus-sized queue. Error paths cancel,
join and release all borrowers without waiting for a missing sequence forever.
Each job independently retains its manifest and asset pages. Advancing discovery
releases the coordinator's reference; worker references persist until the jobs
are joined. Failure paths join every launched job before releasing its final
page references, including partial launch. Failed drains discard remaining staged
output while preserving already emitted records and completed accounting.
Compatible model work coalesces across parents with unique wire IDs. A
single-parent window retains intra-document provider parallelism.

An asset window contains at most eight parents and 8 MiB conservative evidence.
Plan complete first waves, shrinking the parent prefix at the conservative
transfer bound; never preload unused partial waves. Within that bounded prefix,
normalize field order and bucket equal projection shapes, including alternating
shapes. Singletons retain ordinary point loading. Independent groups use the
existing persistent worker pool concurrently; one-group windows remain inline.
Page-memory reservations are apportioned by estimated transfer size before
launch, with a combined bound of the configured page budget. Join every fetch
before retiring its manifest, including partial launch and error paths.
Unioning unrelated fields would overfetch those fields on every row. A group is one current ID-filtered
query, with no ordering or retained pagination state. Each returned parent
independently checks access and revision before its payloads are validated.
Missing/denied/changed parents do not authorize their image bytes or score reuse.
The asset page is immutable and pinned until all lanes finish; validated URL
views can borrow that page without a second base64-string copy. The grant covers
one wave only: later waves fetch and authorize current parent state again.
Reference counts retire shared asset responses after their last wave borrower,
not after the longest parent finishes. Parsed ordinary strings borrow the retained
HTTP body; escaped strings remain arena-owned. The response bytes and parser
allocations share the configured page memory bound. Split point responses also
remain pinned through their wave instead of copying their base64 payloads.
Response JSON rejects duplicate keys and nesting beyond 256 levels.

Response-size/HTTP-413/memory rejection of a parent group falls back to bounded
individual parent reads. Oversized same-parent projections split the image-field
group recursively; each partition preserves credentials/filter/revision checks,
request accounting and validated siblings. One oversized field fails explicitly
rather than causing repeated identical subdivision attempts. Ordered manifest
pagination may shrink its page size; an unordered multi-ID projection must never
silently shrink its limit and omit requested parents.

Explicit finite request budgets, finite source/evaluation budgets and early file
selection retain ordered serial admission. The default 1,000-attempt safety cap
continues to apply to actual HTTP requests across all lanes, including fallback
asset projections and their cancellation callbacks. Parallelism does not turn it
into an unlimited allowance. Default-budget image searches may prefetch one
manifest page only when a conservative bound for the current page's complete
provider/field split trees and retries leaves request headroom. Explicit finite
budgets and text stages retain serial metadata admission. Groups respect per-document selection limits
and the remaining indexed-document quota. Active lanes, borrowed pages and output
are bounded independently of corpus size. This is a latency/throughput tradeoff:
more active parents may raise RSS even while reducing requests and CPU.
Image waves additionally share a 64 MiB conservative working-set admission pool,
independent of cumulative source-byte and provider-concurrency limits. A wave's
weight includes decoded, encoded and serialization scratch; waiting is cancelable
and every failure releases its lease. This bounds admitted estimated work, not
all process RSS: HTTP bodies, manifest pages, output and allocator overhead retain
their separate bounds.

The benchmark driver includes multi-page/multi-parent cases and large encoded
images. Its Unix child wrapper records child wall/CPU time, first stdout byte and
peak RSS without charging the Python harness to the CLI. Reports retain exact
binary hashes, alternating trial order, output equivalence and fixture delays.
First stdout in the ranked fixture follows ranking completion; it is not a
claim of streaming an early unranked hit. Live providers and real Cloud ACLs
remain separate qualification. The [cross-parent qualification](../../zig/pkg/antfly-grep/bench/results/grep-cross-parent-summary.json)
records wall time, CPU, memory and first-output tradeoffs on macOS and the
constrained Linux VM; it does not establish zero-cost execution. The later
[rolling-evidence qualification](../../zig/pkg/antfly-grep/bench/results/grep-rolling-evidence-summary.json)
measures the rolling scheduler, shared admission and borrowed envelopes, including
the sustained-image warm-latency/memory tradeoff.

Warm scores require a fresh authorized query. General scalar projection lets a
server confirm previously validated image identities without transferring the
image again. Manifest digests and caller revision fields are never authority.
Missing or changed identities materialize and validate bytes; a score miss also
materializes bytes before provider dispatch.

### Conditional source projection and future storage authorities

The long-term public interface preserves `fields: string[]` as the source mask
and adds a `select` map of named pure expressions. Source fields remain in
`_source`; expressions appear in `_computed`. Existing typed SDK callers and
wildcard/exclusion semantics remain compatible. A source field requested by
`fields` is always returned: CASE never silently changes that mask. The earlier
unreleased `source_projection` / `_source_projection` delivery protocol is
removed rather than established as a second projection language.

```json
{
  "query": {"bool": {"filter": {"ids": ["source-1"]}}},
  "fields": ["grep.revision", "grep.permissions"],
  "params": {"known": {"source-1": "sv1-<64 lowercase hex characters>"}},
  "select": {
    "identity": {"call": "value_identity", "args": [{"field": "payloads.image"}]},
    "changed": {"is_distinct_from": [
      {"ref": "identity"},
      {"call": "jsonb_extract_path_text", "args": [{"param": "known"}, {"field": "_id"}]}
    ]},
    "image": {"case": [{
      "when": {"ref": "changed"},
      "then": {"field": "payloads.image"}
    }], "else": {"literal": null}}
  }
}
```

JSON expressions lower to the existing SQL scalar AST, binder and VM. CASE,
casts, pure functions and three-valued conditions share SQL semantics; this is
not a second evaluator. Dynamic document operands are jsonb; callers cast them
explicitly for typed scalar functions. `literal` and `param` accept data, never
SQL text. `ref` refers to a sibling expression; all dependencies are bound before
execution, cycles and collisions with `evaluate.compute` are rejected. Pure
projection does not change membership or ranking. AI/provider calls stay in
`evaluate`; they cannot occur implicitly in `select`, including untaken branches.
`value_identity` returns an opaque value identity for present null as well as
other values. An absent path returns SQL NULL; `is_missing` distinguishes that
from stored JSON null. Conditions never grant permissions or resurrect a row.

Binding validates the entire expression graph, but a row's lazy column loader
reads only executed branches. A matching identity does not load the image.
Selecting the image through `fields` still loads it independently. Parameters
are bounded to 1 MiB / 1,024 entries, selection to 64 named expressions, depth
64 and 8,192 expression nodes. SQL VM work and output budgets still apply.

Native output-only queries evaluate after retrieval under the publication lease.
Each 64-row window pins a read transaction, batches authenticated content and
identity metadata, and releases per-row scratch and read scopes. Identity and
existence use exact current metadata when available; selected fields load through
the ordinary verified logical range reader. Uncertified, small, stale and unsupported
layouts fall back safely. Source-dependent stages retain complete afterimages
and project at final output; column providers use lazy selected-column access.
Distributed retrieval selects the global page before scalar evaluation. Large
selected rows evaluate lazily at the physical owner during retained-cut hydration;
eager small rows and nondeferred/retained routes evaluate at the coordinator.
Discarded candidates never evaluate faulting output expressions. Internal worker
envelopes carry the original source mask separately from input dependencies. A request-bound internal receipt prevents repeated evaluation and
never appears in the public `_computed` map. Older workers retain source values
for coordinator evaluation; no public projection-state transport is required.

Gateway table-image materialization uses ordinary authorized queries with the
original ingress identity. Exact duplicate references share one request-local
lookup. Images of a parent are unioned first; parents batch only when their
field/image masks and filters match, at most 32 references per query. This avoids
Cartesian projection overfetch. Every reference still checks live row permissions,
revision, optional identity, byte length, digest, format and pixel bounds before
any provider dispatch. One deadline covers all groups. Missing rows become 404;
429, 503 and timeouts retain retryable gateway statuses. Source locators never
reach providers, and aggregate image limits apply before lookup and after expansion.

`sv1` is a versioned server-computed identity over the typed JSON value delivered
by that authorized read cut. Hashing streams into SHA-256 without allocating an
encoded copy. The reference path reads current selected values before
hashing and omission. Eligible native JSON retrieval instead uses bounded,
storage-owned canonical-value certificates published atomically with the primary
value, content proof, projection directory and mutation revision. Certificates
bind the original content length/digest, exact directory digest, value kind and
semantic canonicalizer ABI. They certify disjoint terminal ranges (scalar leaves, arrays and opaque objects); expanded object parents are excluded to avoid overlapping work. Raw directory
hashes are never value identities. The existing directory format and coverage
budget are unchanged. Rows below the existing 16 KiB projection threshold
receive no certificate. Each optional certificate is bounded below 18 KiB.

The ABI is an explicitly versioned typed-JSON contract, independent of comments,
tests, compiler identity, and lookup implementation. Fixed canonicalization and
ABI golden vectors gate toolchain changes; a semantic change requires an ABI
revision. Unknown ABIs fall back safely. Editing certificate plumbing does not
invalidate an existing table's proofs. Atomic writes and bounded repair collect
canonical terminal identities during the successful directory encoding walk, rather
than reparsing the finished directory. Already validated, unescaped UTF-8 JSON
strings and booleans/null use borrowed canonical bytes; escaped strings and
numbers retain typed canonicalization. Both routes must produce identical sv1
identities. Directory coverage and fallback admission remain unchanged.

Eligible reads prepare at most 64 rows from one pinned transaction. One sorted,
deduplicated batch loads content/revision/TTL metadata; a second loads directories
and certificates. Owned bounded metadata survives backend-buffer retirement.
Per-row lazy reads reuse the same proof and directory; no cross-request snapshot
or authorization cache is introduced. Final source and computed bytes belong to
the result allocator. Native final source uses the strict projection engine and
its immutable syntax receipt; unsupported source layouts retain ordinary strict
normalization. Benchmark qualification must include cold and warm reads,
metadata updates, full-source masks, row counts, output size, CPU and peak memory.
Earlier source-delivery benchmark reports used the removed experimental protocol;
they are historical measurements, not measurements of the select interface.

Primary rewrites may reuse canonical terminal certificates from the same writer
snapshot. The previous content proof must match the current mutation revision;
its directory and certificate must pass length, kind, digest and canonical ABI
checks. Layouts without a qualified terminal of at least 4 KiB retain the
fused reference writer. Reuse requires matching decoded ancestry/name, terminal kind, raw length and
raw SHA-256 digest. Packed indices and offsets are never sufficient. Added,
reordered, renamed or moved siblings cannot misassociate records. Repeated writes
in one transaction use its latest publication. Missing/stale/incompatible proofs
rebuild through the reference canonicalizer; unordered bulk imports keep the
reference writer path. The new body, directory, certificate and revision are
published atomically. No previous primary body is loaded to qualify reuse.

This optimization avoids re-decoding and canonical hashing of unchanged leaves;
it still validates/hashes the incoming JSON and rewrites the primary row. Removing
that write amplification requires a separate versioned storage representation:
immutable content-addressed large values, small mutable row manifests, atomic
manifest/authority publication, snapshot-aware blob ownership and reclamation,
replication/backup/export support, and logical full/range reads that reconstruct
the existing public JSON. A digest must never act as a read grant: current row
membership, revision and authorization remain mandatory. Such a representation
must be gated by mixed-version/rollback tests and metadata-update WAL/compaction
measurements before activation. A new content-authority version must bind the
manifest and its leaf digests; a manifest/Merkle digest cannot be interpreted as
the legacy SHA-256 of reconstructed primary JSON. Public `sv1` identities remain
canonical-value identities across either representation. Blob ownership belongs
to the table and pinned storage cut, with publication and reclamation fenced by
the same writer transaction and retained snapshots. Failed commits, leader
changes, export/import and crash recovery must preserve those ownership facts. The current format remains full primary JSON;
certificate reuse alone makes no claim about avoiding its disk writes.

A current certificate permits unchanged terminals to be omitted without loading or
hashing those payloads. Current permissions, revision, query membership and
publication fences still apply. Missing, stale, unsupported or ABI-incompatible
certificates use the full-value reference path. Existing bounded authority
backfill repairs eligible legacy rows without changing their logical revisions.
Uncertified opaque values and source-dependent operators retain reference execution.
Other selected metadata may still require storage-frame reads or a full-layout
fallback: certificate reuse is not a guarantee of zero physical disk I/O.


The CLI retains bounded verified descriptors (digest, MIME, dimensions, length
and server identity), never image payloads, in the existing atomic artifact
cache. Keys include endpoint, table, parent, field and credential context, plus
a build digest of the image validators. The cache uses 256 buckets and four
1 KiB slots per bucket; records can be evicted at any time. Current permissions
and expected revision are rechecked before any descriptor is accepted. Lower
current image-byte/pixel limits still apply. Before grouped retrieval, a bounded score-cache probe identifies fields whose
current question already has a reusable decision. Changed questions request full
values immediately in the same grouped query, avoiding conditional-then-full
round trips. This probe is only a planning hint; actual score use rechecks expiry
after fresh authorization and identity verification. The request retains the
exact verified descriptors it sends, so later cache eviction does not require
reopening them. Expired/evicted scores still force ordinary materialization.
Image constructors retain an immutable URL extent proven to contain only the
supported data-URL prefix and validated base64 alphabet. Exact evidence sizing
counts escaped text/framing and adds the known URL length without rescanning the
payload. Request encoders emit owned framing plus borrowed qualified URL segments
through HTTPx's existing H1/H2 transport. URLs below 4 KiB remain in contiguous
framing to avoid extra frames/syscalls for tiny inputs. Replacing a URL invalidates its extent
proof; unqualified strings retain ordinary escaping. Inputs stay pinned through
completion/cancellation/retries, and the original request-size, concurrency and
response limits still apply. The local inference ABI receives a contiguous body.

Descriptors never enter provider requests: an explicit materialization guard
runs before coalescing and dispatch. Conditional fallback retains ordinary
source/request budgets and avoids charging the same logical image twice.
Conditional-response transfer admission accounts for small identity responses
separately from full-image processing scratch. The shared processing admission
budget remains unchanged. Bucket-striped writer locks serialize cache bucket
read/choose/publish operations within the process, preserving fitting colliding
entries; readers remain lock-free and cross-process races remain safe misses.
Disabling caches also disables conditional identity work. Older servers that
ignore the extension return full values; those rejecting it receive one ordinary
projection retry, charged to the same request budget. No permission result is
cached by this mechanism.

Manifest pages have independent worker leases. Completed lanes can refill across
page boundaries while output stays in discovery order. At most eight active
jobs, sixteen staged results, the coordinator page and one prefetched page are
retained; there is no corpus-sized page or reorder inventory. Explicit finite
evaluation budgets retain serial admission. Every launch, cancellation, decode,
output and drain failure joins borrowers before releasing their pages. The shared
64 MiB conservative image admission pool remains in place; warm reuse removes
transfer and validation work instead of raising that pool's limit.


## Format migration split (2026-10-26)

PR #1263 retains the existing ASCH25 writer and reader contract. Automatic
format activation, immutable document manifests, logical-authority v2,
source/field publication packs, migration/replay coordination and their
format-specific transfer/recovery paths are deferred together to the
`experiment/immutable-document-format` follow-up branch. That branch preserves
the complete implementation, design history, fixtures and qualification tools;
it is experimental and is not approved for automatic migration.

The PR retains format-independent sparse FST cleanup, bounded analyzed-text
contribution reuse and the existing query/gateway improvements. The analysis
cache keys actual text bytes and the complete analyzer pipeline. It is lazy,
resource-accounted and reclaimable, and cache hits write no durable metadata.
It cannot supply source authority or select an old document revision. Tests
compare complete encoded segments across analyzers, reclamation, changed and
reordered document updates, and compaction. The update fixture checks that
schema format stays at 25 and primary rows remain ordinary JSON.

The experimental qualification showed 16% lower CPU and 15% lower RSS for dense
code updates against the earlier native implementation. Compared with legacy
inline storage, large metadata-only updates used about 32x fewer primary WAL
bytes, but 4 KiB rows in already-admitted tables used about 4x CPU and 3.7x update
elapsed time. The 2x update-speed target failed. These measurements describe
the experimental branch, not the smaller format-independent PR; they must not
be reused as performance claims for the split PR.

Before automatic upgrades can return, qualify a hybrid layout with cheap
small-row storage, source planning before body hydration, and compiled-field
reuse across publication/compaction. Require paired old-binary comparisons on
mixed small/large, sparse/dense, changing/unchanged, entropy/compressible,
read-heavy and sustained-compaction workloads, plus recovery and distributed
transfer tests. CPU, peak memory and tail latency must meet explicit regression
budgets; large-payload write savings alone do not qualify all-user migration.
Older-reader fencing and snapshot-based rollback remain mandatory for any new
physical format. No released table is converted by this PR.

Split validation on macOS passed 67 compiler/update tests (one benchmark skip),
21 FST tests, 152 query-contract tests and 97 logical-field tests (one skip),
with no reported leaks. The CLI sweep covered 290 cases: after correcting the
source-reference fixture and regenerating packaged tooling, its six failed
cases all passed on the optimized recheck; two optional cases were skipped.
This is split/correctness validation, not a new performance qualification or
an independent Linux hardware result.

### Conditional select qualification (2026-10-26)

The general expression interface was qualified on macOS ARM64 (14 logical CPUs)
with the native Debug binary `f8aae9f09f1a`, eight 256 KiB values, three alternating
server rounds and five reads per phase. Both compared paths use the same binary,
authorization, query membership, result values and SQL VM. The reference wraps
its identity operand in pure JSON functions so it must read the current value;
the direct operand can use the native certificate. Result digests are checked
across variants and phases. These are warm-cache, shared-host measurements.

| Delivery | Metadata-backed CPU / elapsed | Value-reading reference CPU / elapsed | Response |
|---|---:|---:|---:|
| Full values | 121.35 / 107.72 ms | 136.08 / 118.53 ms | 2,099,088 B |
| Matching known identities | 8.42 / 8.17 ms | 52.33 / 44.54 ms | about 1,952 B |

This fixture shows roughly 6.2× less CPU for matching identities and a smaller
full-value CPU saving. Ordinary source delivery without expressions is a separate
control: 24.92 ms CPU / 21.45 ms elapsed. General computed full-value delivery
still incurs additional VM/JSON serialization work. This is not a universal
speedup, zero-cost execution, a release throughput claim, or evidence for changing
the document format. The follow-up qualification below removes repeated
`_computed` parsing and serialization while preserving strict JSON and expression semantics.

The reproducible driver is `zig/e2e/antfly/bench_source_projection.py`; run with
`--rounds 3 --queries-per-phase 5 --documents 8 --value-bytes 262144 --metadata-updates 2`. Raw reports and logs stay ignored under `output/`.
Validation includes 157 native query/worker contracts, 17 source-identity/read
contracts, gateway suites, 353 Python query round trips, the Go SDK suite, and
291 CLI cases (two optional skips). Native indexed integration covers single-
and multi-shard final-page evaluation, default/wildcard/exclusion source masks,
NULL/missing, retained revisions, authorization and grouped image reads. Fixtures
establish orchestration, not live-provider relevance or production Cloud ACL quality.


### Projection review fixes and serialization follow-up (2026-10-26)

The projection compiler now resolves aliases iteratively, counts depth across
references, and evaluates requested aliases in dependency order without recursive
VM entry. Array operands resolve against original positions before output masks;
nested user keys such as `metadata._type` are accepted. Normalized local/worker
responses validate completed outputs before restoring source-finalization
receipts, so a source mask is applied once across both single- and multi-shard
routes. The API wire shape remains unchanged and ASCH25 is unchanged.

Computed output stays as validated JSON spans through worker decoding and final
serialization. Native scalar reads parse their authenticated row views directly;
growing encoding buffers use reclaimable scratch before exact output ownership.
This removes repeated payload DOMs and unescaping/re-escaping. SQL semantics,
lazy CASE, strict duplicate/malformed JSON rejection, original numeric lexemes,
revision proofs and authorization remain enforced. See the shared
[projection execution contract](logical-field-queries.md#projection-execution-and-ownership).

The paired driver compared baseline `f8aae9f09f1a` with candidate `1fde63794362`
on the same shared macOS ARM64 host (14 logical CPUs), using Debug builds, eight
256 KiB target values, three alternating rounds and five reads per phase. Two
metadata updates also verify unchanged-value identities. Mixed data includes
escaped strings, Unicode, numbers, wide objects, opaque arrays and small values.
The driver verifies result digests across variants and phases. Filesystem and
storage caches are warm after ingestion; full delivery means a client identity
miss, not a cold disk read. The candidate also includes the origin/main merge.

| Workload | Full computed CPU, before → after | Full elapsed, before → after | Known-identity CPU, before → after |
|---|---:|---:|---:|
| Ascii | 100.50 → 59.87 ms | 91.72 → 55.57 ms | 6.49 → 7.02 ms |
| Mixed | 110.77 → 68.26 ms | 99.45 → 61.87 ms | 11.14 → 11.66 ms |

Full computed CPU decreased about 38–40%. Ordinary stored-source controls were
20.48 → 21.81 ms CPU for ASCII and 22.86 → 23.39 ms for mixed data. Known-identity
reads were about 0.5 ms slower in these runs. Full computed output still costs
about 2.7–2.9 times ordinary source delivery; these results are not a universal
speedup or a zero-cost claim.

Process RSS was not consistently lower: full-phase medians were 336 → 378 MiB
for ASCII and 284 → 285 MiB for mixed data. These whole-process, sampled figures
include the main merge and background allocation/reclamation; they do not isolate
this patch's allocation costs and are not allocation bounds. Production builds,
other hosts and storage/cache conditions require separate qualification.

Reproduce with `zig/e2e/antfly/bench_source_projection.py BINARY --baseline BASELINE
--rounds 3 --queries-per-phase 5 --documents 8 --value-bytes 262144
--metadata-updates 2 --output REPORT`; repeat with `--workload mixed`. Both
executables should retain the name `antfly` for the native API fixture. Raw
reports/logs remain ignored under `output/review-1263/`.

Validation: 190 focused native query/worker contracts, the compiled native-array
source regression, 293 CLI cases (two optional skips), 32 indexed native-server
cases (eight optional profiling skips), 353 Python round trips, the Go SDK suite,
and OpenAPI/specification/license-boundary checks. Native tests cover small/large
array rows in one and two shards, nested underscore keys, rejected oversized alias
depth, and a legal 64-alias chain. Fixtures establish behavior, not live-provider
quality or an independent Linux hardware result.


### Bounded scalar delivery and opaque identity coverage

Admission walks every literal and parameter value iteratively before recursive
copying, hashing or encoding. The budget includes unused parameters and untaken
CASE branches; invalid requests fail without entering the fixed-depth JSON
writer. Limits apply to raw data independently of expression/alias depth.

Native scalar delivery prepares source masks and operand selectors once per
owner request, alongside its immutable SQL programs. Row-local views borrow
only bytes loaded under that row's pinned revision and authorization. Direct
JSON columns and the selected CASE branch can forward validated raw spans to
`_computed`; CASE conditions and operations still use the shared SQL VM. Alias
outputs keep those spans until a consumer needs a typed value, then materialize
once. Numeric coercions, array fanout and unsupported paths use the typed
reference path. Both paths enforce cancellation, nesting, work and output
budgets. This is request-local reuse; no global cache retains user parameters,
source bytes, snapshots or permissions across requests.

The final public source selector applies caller masks and reserved root-field
exclusions together, before native finalization. It is shared across hydrated
rows. Arrays are projected once, and nested user keys such as `metadata._type`
remain visible. Unsupported selector shapes keep the strict DOM fallback.

Opaque terminal certificates use coverage epoch SVI2 with the existing bounded
sidecar layout, without changing the document format or canonical ABI. New
readers accept SVI1 and SVI2; older readers safely fall back for an unknown
sidecar epoch. SVI1 directories containing opaque terminals request bounded
repair. SVI2 records that coverage was attempted, so unsupported deep terminals
do not trigger repeated repair. Captured object parents are not
hashed again. Exact path resolution is mandatory: an array or opaque object's
certificate cannot prove a descendant's existence or identity. Missing records
in older sidecars retain the reference read and bounded repair. Composite
canonicalization declines optional certificates beyond 128 levels before DOM
allocation; ordinary document ingestion does not require a certificate. Reuse
still checks revision, content proof, directory binding, ancestry, kind, length
and raw digest. It avoids canonicalizing an unchanged large array/object during
metadata updates, while continuing to validate and rewrite the incoming primary
row. No zero-cost or universal throughput guarantee follows from this change.
