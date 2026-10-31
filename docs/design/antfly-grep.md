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
lazy CASE, strict duplicate/malformed JSON rejection, typed JSON numeric semantics,
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
only bytes loaded under that row's pinned revision and authorization. Full-body
fallbacks share one prepared row view across operands, and resolved raw spans
are memoized, avoiding repeated full-row indexing for many-output requests. If
an earlier operator already materialized a value, subsequent outputs reuse it
rather than preparing a second view. Direct
JSON columns and the selected CASE branch can forward validated raw spans to
`_computed`; CASE conditions and operations still use the shared SQL VM. Alias
outputs keep those spans until a consumer needs a typed value, then materialize
once. Numbers are normalized with the same parser and formatter as typed JSON,
including nested values. SQL numeric coercions, array fanout and unsupported
paths use the typed reference path. Both paths enforce cancellation, nesting, work and output
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
canonicalization uses a bounded streaming walker and declines optional
certificates beyond 128 levels; ordinary document ingestion does not require a
certificate. Reuse
still checks revision, content proof, directory binding, ancestry, kind, length
and raw digest. It avoids canonicalizing an unchanged large array/object during
metadata updates, while continuing to validate and rewrite the incoming primary
row. No zero-cost or universal throughput guarantee follows from this change.


Projection admission now retains the bound program for the request lifetime.
Hydration and response encoding borrow it only when the encoded input has the
same immutable address and length; copied or transported inputs bind locally.
The parsed-request owner releases the plan separately from `SearchRequest`
value copies. Borrowers never release it, and the handle is never serialized.
The program holds its own data parameters; sharing is limited to that one
request. This removes repeated admission/output binding without a global plan
cache or cross-request parameter state. Failure injection verifies cleanup and
cloned-parameter isolation. Manually constructed requests retain owned local
preparation, so older workers and non-parser consumers preserve their behavior.


Raw output validation counts logical JSON values for the VM work budget, not
scanner fragments or object keys. Escaped strings and surrogate-pair Unicode
therefore have the same value-step budget as the typed VM. Fragment scanning
polls cancellation independently of those steps. Validation has a bounded
container stack isolated in a separate helper. Native equivalence fixtures include large escaped strings,
Unicode, wide objects, cancellation and empty malformed input.


### ReleaseFast qualification after the bounded-delivery follow-up

The 2026-10-27 paired run compares native macOS ARM64 ReleaseFast servers
built with pinned Zig 0.17.0. The baseline is original PR head `7badb5658d`
merged with the same `origin/main` revision `ca4339f030` as the candidate
(baseline tree commit `c26dd115c5465539c71d8a62b6fcc90e237e15f9`,
candidate runtime source `952f4161577b0f5de5a3e4f1b1a177562ef43b45`).
Both binaries report `antfly dev (zig runtime)`. Their SHA-256 hashes are:

- Baseline: `7456751926649113da3802df93fa6354005d01b3bb16a12218d8db90d2100db0`.
- Candidate: `e4b1d40f6e707e39cfe388f65be91af10da70435f766896c701dd08e0025afbe`.

Each workload uses five fresh-server trials per variant, alternating variant
order, with five queries per read phase: 25 samples per variant/read phase.
Each server ingests eight documents, with 256 KiB target large values, and
performs two metadata updates. The small workload adds 60 metadata outputs
for 64 computed outputs. Updates preserve and verify these fields. Medians
below include the whole server process; background work and a shared host
contribute noise. Filesystem/storage caches are warm after ingestion. A full
read means a client identity miss, not cold storage. There is no inference.

| Workload | Known-identity CPU ms | Known-identity elapsed ms | Full computed CPU ms | Full computed elapsed ms |
|---|---:|---:|---:|---:|
| ASCII | 0.69 → 0.69 | 1.18 → 1.20 | 10.03 → 10.63 | 11.48 → 12.23 |
| Mixed | 1.38 → 0.70 | 1.99 → 1.16 | 14.08 → 13.98 | 15.37 → 15.20 |
| Opaque array | 5.54 → 0.69 | 6.20 → 1.24 | 11.96 → 10.52 | 13.40 → 11.82 |
| Small / 64 outputs | 1.85 → 1.55 | 2.30 → 1.93 | 1.77 → 1.53 | 2.19 → 1.95 |

Every value is baseline → candidate. Opaque known-identity reads use about
8× less CPU and take about 5× less elapsed time; mixed known-identity CPU
decreases about 49%. Small full-output CPU decreases about 13%. Full ASCII
CPU increases about 6%, and full mixed delivery is nearly unchanged. These
results replace neither the typed reference path nor its equivalence checks.

| Workload | Ingest CPU ms | Metadata-update CPU ms | Ordinary source CPU ms | Known-identity RSS MiB |
|---|---:|---:|---:|---:|
| ASCII | 54.50 → 55.41 | 37.81 → 38.96 | 3.53 → 3.59 | 203.69 → 202.08 |
| Mixed | 65.34 → 69.66 | 44.13 → 43.78 | 4.07 → 4.31 | 205.67 → 207.95 |
| Opaque array | 51.07 → 57.02 | 38.90 → 39.14 | 3.57 → 3.67 | 199.89 → 202.80 |
| Small / 64 outputs | 17.39 → 18.25 | 10.17 → 10.12 | 0.97 → 1.32 | 170.41 → 170.94 |

Building reusable opaque certificates adds about 12% ingestion CPU in this
fixture. Metadata updates still rewrite primary documents; they are not free.
Ordinary small-row CPU rises by about 0.35 ms in this confirmation, while the
initial three-trial run showed the opposite direction. Sub-millisecond controls
need more isolated host qualification before attributing them to the patch.
RSS is sampled every 5 ms and is not an allocation bound; memory is not
consistently lower. This is a targeted read improvement, not a universal
speedup or a zero-cost claim. Linux hardware, cold storage, sustained writes
and concurrent production load need independent performance qualification.

The driver checks source/computed output hashes across binaries and across
optimized/reference phases, including results after each metadata update.
For each opaque candidate trial, identity profiling records 96 genuine
certificate hits and zero fallback rows; the baseline records 176 fallback
rows. Full-value output still reads and encodes the payload. The older binary
has no certificate-hit counter, so its zero counter is not evidence of zero
hits for other workloads. Small rows correctly retain full operand fallback.

Reproduce with `zig/e2e/antfly/bench_source_projection.py BINARY --baseline BASELINE
--rounds 5 --queries-per-phase 5 --documents 8 --value-bytes 262144
--metadata-updates 2 --workload WORKLOAD --output REPORT`, for `ascii`, `mixed`,
`opaque` and `small`; add `--metadata-outputs 60` for `small`. Both executable
basenames must be `antfly`. Reports retain binary hashes, host details, corpus
and output hashes, individual samples, medians and certificate counters. Raw
JSON/logs and the source/version manifest remain ignored under
`output/projection-followup/` rather than committed.

Final validation: ReleaseFast build and cached rebuild both passed 71/71
steps; nine server smoke tests passed; the full indexed-server suite passed
43 cases with seven opt-in profiling cases skipped. The suite includes
scalar/opaque persisted upgrades and older-reader rollback with one/two
shards. The 196 focused query/worker contracts, 18 identity/publication cases,
native borrowed-program retrieval, raw/VM work-budget equivalence,
allocation-failure cleanup, Go SDK tests, 356 Python query/resolver cases,
OpenAPI regeneration, Apache boundary (2,553 files) and control catalog
checks passed. Local tests do not replace maintainer-approved PR CI.


### Numeric consistency, bounded reuse and media materialization follow-up

A direct `select` field, an alias, a selected CASE arm and a typed SQL JSON
operation must return the same logical numbers regardless of output order or
whether another output materialized the source first. The native path now uses
the existing typed JSON number parser and formatter, including numbers inside
arrays and objects. For example, `1e-400` follows the current VM's underflow to
zero; the fast path cannot preserve a different numeric value. SQL numeric
casts retain their separate type/precision rules. Changing the canonical ABI
would require an explicit compatibility change; this fix does not change it.

One streaming walker handles numeric normalization, logical-value work budgets
and canonical identity encoding. Output strings can retain their original escape
spelling, while identity hashing uses the default typed canonical spelling.
The scanner supplies syntax/UTF-8 validation and fragmented-string cancellation;
strict duplicate-key policy is established by the admitted source, not replaced
by this walker. Unchanged output borrows the existing extent. A changed numeric
encoding allocates only the resulting wire bytes, not a payload DOM. Opaque
identity hashing retains only bounded scanner/container state and declines
optional certificates beyond 128 levels. Ordinary document format remains ASCH25.

Hydration finalization now borrows the admitted program under the same exact
encoded-input witness as response encoding. Unconditional direct-field operands
are deduplicated once in that immutable plan. Each selected row can read these
operands together through one pinned scope and shared projected view. Conditional
operands remain lazy; untaken CASE branches are not prefetched. The scope closes
before row scratch is released. Unsupported selectors retain the reference path.

Completed native outputs carry an owned, optional receipt bound to their exact
immutable bytes and program fingerprint. Ordinary candidates carry only a nullable
pointer; receipt storage is allocated only for projected hits. Cloning copies and
rebinds the receipt after copying the complete immutable bytes; moves transfer
ownership, replacements retire it, and allocation failures release both objects.
These are syntax/execution receipts, never read grants. Transported outputs must
validate their syntax, binding set and fingerprint locally. Standard marker-last
outputs can then regain a local receipt; other layouts use the general fallback.
The public serializer emits leading computed members plus their closing brace,
omitting the private trailer without copying the payload into a second object.
Only explicit immutable producer objects skip another generic raw-JSON validation;
ordinary constructors and parsed values preserve their validation behavior.
Generic `RawValue` retains its original slice-sized representation.

Media source groups use at most four concurrent queries under one ingress
identity and shared deadline. Isolated job arenas share the request memory budget;
no child uses the parent arena concurrently, and every child is joined/canceled
before its arena is freed. Scheduling failure degrades to inline work. Each group
still executes the ordinary authorized query and independently checks every
reference's revision, identity and permissions. Complete filters are encoded and
interned once with exact equality, including collision checks; comparisons do not
serialize them repeatedly. Temporary interned encodings die before source reads.
A regression groups 128 distinct filters with a bounded 2 MiB grouping budget.

Identical materialized URLs share decoding, digest and image inspection only
within the request, after source lookup. Every reference still checks its own
digest, MIME, length and pixel cap. Decode scratch is reclaimed through the
existing budget rather than retained in the request arena. The expanded JSON
limit is computed from the compact request and exact source-to-URL substitutions;
verified base64 data URLs are JSON-safe ASCII, so they need no second image-sized
counting-encoder pass. Deep source/filter data is rejected before recursive
cloning or fixed-depth serialization. These changes do not cache authorization
or claim lower model billing or improved relevance.

The isolated ReleaseFast digest benchmark uses five alternating trials and
32 hashes per trial on synthetic opaque arrays. Both paths check the same typed
identity ABI. Figures are medians per hash; allocation figures are allocator byte
volume, not peak memory or process RSS. Both implementations run in the same
binary with the same allocator, outside HTTP/storage and corpus construction.

| Workload | DOM reference µs → streaming µs | Allocated bytes, reference → streaming |
|---|---:|---:|
| opaque_ascii | 424.86 → 367.52 | 591293 → 129 |
| opaque_escaped | 1906.86 → 1642.73 | 654331 → 129 |
| opaque_unicode | 734.32 → 513.85 | 591265 → 129 |
| opaque_small | 0.43 → 0.31 | 1477 → 129 |

Reproduce from `zig/` with pinned Zig 0.17.0:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig run -O ReleaseFast --dep identity \
  -Mroot=tools/bench_canonical_json.zig \
  -Midentity=pkg/antfly-embedded/src/common/value_identity.zig
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig test --dep canonical \
  -Mroot=tools/test_canonical_json.zig \
  -Mcanonical=pkg/antfly-embedded/src/common/canonical_json.zig
```

The paired media driver is `zig/e2e/antfly/bench_media_materialization.py`.
It covers tiny/large PNGs, shared/distinct masks and duplicated/unique payloads.
Every invocation stops at `ModelRequired`, after source materialization and
verification but before any model/provider execution. Separate one/two-shard
server fixtures check successful materialization plus digest, permission and
revision rejection. Its timings measure source work, not inference latency.
Raw logs/reports remain ignored under `output/projection-followup/`.


### Release qualification for the bounded-reuse follow-up (2026-10-27)
The measured ReleaseFast executable uses production source `915e5493a2b8`,
including `main` through `7dabd028d66d`. Its SHA-256 is
`d2a0401ddb9573148dcadf2c524772dd8fd566469e00fc2c2dc8fe84fd8e508f`
(251,319,128 bytes). The preserved performance baseline uses runtime source
`952f4161577b`, `main` through `ca4339f030a6`, and SHA-256
`e4b1d40f6e707e39cfe388f65be91af10da70435f766896c701dd08e0025afbe`.
**The candidate includes newer main changes; whole-server deltas do not isolate
all effects of this follow-up.** The separate same-binary digest comparison above
isolates that kernel. Both server executables report `antfly dev (zig runtime)`.

The final current-source build passed. The indexed HTTP suite passed 49 cases;
seven opt-in profiling cases were skipped. It covers one/two-shard projections,
numeric order/alias/identity equivalence, source masks, retained pagination,
authorization, media claim rejection and persisted SVI1/SVI2 upgrade/old-reader
rollback. The rollback executable is the older SHA-256
`7456751926649113da3802df93fa6354005d01b3bb16a12218d8db90d2100db0`;
the performance baseline already understands SVI2 and cannot establish that
rollback property.

The paired source driver ran five alternating trials, five reads per phase,
eight documents and two metadata updates per trial on a shared 14-logical-CPU
macOS ARM64 host. Large target values are approximately 256 KiB; mixed data also
contains small/numeric/wide values. `small64` uses tiny values; `wide64` uses large
ASCII targets. Both add 60 distinct metadata outputs. Each run verifies output
hashes against ordinary reference reads and across binaries, certificate reuse,
and identities after metadata updates. Storage/filesystem caches are warm after
ingestion. CPU is whole-process CPU including background work; sampled RSS is
neither allocation volume nor a hard peak bound. Values below are medians,
baseline → candidate.

| Workload | Full computed elapsed ms | Full computed CPU ms | Full computed RSS MiB | Conditional CPU ms |
|---|---:|---:|---:|---:|
| ascii | 11.33 → 8.65 | 9.97 → 7.30 | 204.1 → 203.5 | 0.77 → 0.71 |
| mixed | 15.02 → 11.24 | 13.91 → 10.02 | 207.0 → 207.7 | 0.74 → 0.71 |
| opaque | 12.11 → 8.80 | 10.59 → 7.59 | 203.6 → 205.1 | 0.75 → 0.73 |
| small64 | 3.30 → 4.39 | 2.03 → 1.89 | 176.5 → 176.4 | 2.34 → 2.08 |
| wide64 | 13.09 → 9.75 | 11.76 → 8.45 | 204.8 → 206.0 | 2.17 → 1.70 |
| escaped | 33.80 → 23.47 | 32.48 → 22.20 | 216.4 → 217.9 | 0.68 → 0.58 |
| unicode | 36.07 → 25.40 | 35.39 → 24.29 | 204.0 → 206.5 | 0.66 → 0.65 |

Controls retain the actual costs outside computed reads. Positive percentages
mean more candidate CPU; negative percentages mean less. These are observed
shared-host deltas, not regression tolerances or performance guarantees.

| Workload | Ordinary-read CPU change | Ingestion CPU change | Metadata-update CPU change |
|---|---:|---:|---:|
| ascii | -6.7% | +3.7% | +3.7% |
| mixed | -2.1% | +0.7% | -1.3% |
| opaque | +3.5% | -2.9% | -1.7% |
| small64 | -6.7% | -10.0% | +3.7% |
| wide64 | +4.3% | +3.1% | -0.9% |
| escaped | -3.0% | -1.5% | +0.5% |
| unicode | -1.2% | +0.9% | -4.1% |

The model-free media comparison uses five alternating trials and five requests
per trial, with four images per request. Tiny PNGs are 1×1; large PNGs are
256×256 random RGBA data. Distinct-mask cases exercise separate source groups;
shared-mask cases already batch into one query. Duplicate cases reuse identical
payloads across distinct parent documents. Every request reaches `ModelRequired`
after materialization and verification, before any provider call. Median values
are baseline → candidate.

| Media workload | Elapsed ms | Server CPU ms | Sampled RSS MiB |
|---|---:|---:|---:|
| tiny_shared_duplicates | 0.87 → 0.83 | 1.36 → 1.24 | 167.97 → 167.81 |
| tiny_distinct_unique | 1.54 → 1.11 | 2.62 → 2.74 | 168.12 → 188.55 |
| large_shared_duplicates | 15.47 → 11.14 | 15.10 → 10.68 | 205.19 → 203.98 |
| large_distinct_unique | 16.38 → 5.83 | 16.00 → 12.54 | 215.42 → 229.86 |
| large_distinct_duplicates | 16.38 → 4.62 | 16.09 → 12.07 | 207.06 → 228.94 |

Concurrency reduces large distinct-source latency while increasing sampled RSS
by roughly 14–22 MiB in these cases. Tiny distinct-source CPU increases about
4.5%; this is a latency/memory tradeoff, not free throughput. Four concurrent
groups and the shared request quota bound scheduling and materialized payloads;
process RSS also includes runtime thread/allocator state outside those payloads.
No model latency, billing, quality, cold-cache behavior or cross-host guarantee is
measured. Large computed-source CPU falls about 27–32% here, while ordinary reads,
writes, small values and RSS do not improve uniformly.

Reproduce the source comparison from the repository root, substituting the
preserved prior executable for `/path/to/baseline/antfly`:

```sh
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_source_projection.py \
  zig/zig-out/bin/antfly --baseline /path/to/baseline/antfly \
  --rounds 5 --queries-per-phase 5 --documents 8 --value-bytes 262144 \
  --metadata-updates 2 --workload ascii \
  --output output/projection-followup/source-ascii.json
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_media_materialization.py \
  zig/zig-out/bin/antfly --baseline /path/to/baseline/antfly \
  --rounds 5 --queries-per-phase 5 \
  --output output/projection-followup/media.json
```

Repeat source runs with `mixed`, `opaque`, `escaped` and `unicode`. Add
`--metadata-outputs 60` for `ascii` (wide64) and `small` (small64). Reports retain
binary hashes, corpus/output hashes, host details, all samples and medians.

A separate five-trial small64 repeat retained the same hashes and observed full
computed elapsed 2.03 → 1.98 ms, CPU 1.54 → 1.48 ms and sampled RSS
170.8 → 170.4 MiB. Conditional elapsed was 2.13 → 2.10 ms and CPU
1.66 → 1.60 ms. Metadata updates were 8.82 → 9.14 ms and CPU
10.01 → 10.50 ms. The first run's full-read latency regression did not repeat;
both runs are retained rather than selecting a favorable sample. Metadata-update
CPU was about 4–5% higher in both small runs. These results support reduced read
work, not a universally better latency, memory or write-cost claim.


### Alias-preserving values and bounded result consumers

The follow-up keeps the public `fields` / `select` / `params` contract, ASCH25
primary documents and the value-identity ABI unchanged. Internal normalized
values now retain immutable bytes, their original extent, logical-node count
and height. Reuse charges the consuming VM's work, depth and output limits and
polls cancellation; a receipt cannot turn off resource accounting. Strict native
views can certify string/bool/null scalar extents immediately. Strings have no
numeric descendants and retain their admitted escape spelling, so this avoids a
second string scan. Unqualified sources still use the scanner; numbers and
composites still normalize with the typed JSON parser/formatter. Duplicate or
malformed native nodes cannot issue a receipt.

Raw outputs retain the selected column's provenance through aliases and CASE.
Only executed pure forwarding paths carry it: computed/coerced values keep the
ordinary typed VM. `value_identity(ref(alias))` can use the same pinned source
identity as the selected field, with row-local memoization. A present raw value,
including JSON null, proves `is_missing(ref(alias))` false without a DOM. Ordinary
consuming operators still materialize typed values when required. These facts
are scoped to immutable row bytes; they are neither wire metadata nor access
capabilities, and no request-global/user-global value cache is added.

The request prepares the union of public source fields and unconditional direct
operands. Native storage fetches selected rows in owned byte windows, at most
1 MiB of proven original bytes and 64 keys per batch, inside the existing pinned
transaction. Unknown proofs or individually large rows keep lazy per-row reads.
Metadata windows remain bounded independently. Public source output can reuse
that row view, applying its mask once; array operand positions remain original.
CASE-only operands are not included unless explicitly requested as public source.
The distributed hydration path uses the same mechanism and read-cut ownership.

Gateway lookups select flat value, image-presence and optional identity bindings
through ordinary scalar projection. Array indexes are resolved before public
mask compaction. Group planning accounts for every binding, splitting below the
64-output ceiling while retaining the 32-reference ceiling. Values, revisions
and permissions keep independent checks, and missing images remain distinct
from present null/invalid values.

A synchronous typed consumer receives the same canonical result that ordinary
query execution would encode. Query permits, catalog binding, table permission,
row policy, snapshot/revision checks, cancellation and result lifecycle fences
run through the existing public query service. The consumer copies only values
it retains before the producer retires; it does not manufacture a second HTTP
response and then parse it. Providers without typed delivery retain the wire
path. A producer omitting computed output may trigger one fresh authorized query
for complete root fields when no reference pins an identity. That conservative
fallback preserves original array positions. Identity-pinned references never
downgrade, and fallback queries retain the original deadline and recheck policy.
Consumer failures cross the generic delivery boundary as `QueryDeliveryFailed`
while the gateway retains their original classification (for example, revision
mismatch, absence or permission failure).

Media work now uses a rolling pool of at most four groups. Completed slots are
joined, copied and retired before being refilled; one slow group cannot force a
barrier between cohorts. An 8 MiB active estimate and the shared SQL I/O scheduler
bound concurrent admission across requests. Estimates never replace actual
request quotas or the verified expanded-input limit. Oversized required groups
run inline; tiny requests below 64 KiB estimated encoded payload also run inline
to avoid task/stack overhead. The latter is a cost policy with a possible remote
latency tradeoff, not a universal storage claim. Parent copies and child scratch
share a synchronizing allocator lock during overlap. Every task is joined or
canceled before its descriptor or arena retires, including failure paths.

`e2e/antfly/bench_projection_aliases.py` compares equivalent direct-field and alias
identity/missing expressions, including three-copy chains, with exact output
hashes across both modes and binaries. The media driver also covers eight groups
and skewed image sizes. Shared deterministic PNG generation lives in
`e2e/antfly/media_fixture.py`. No model/provider call is needed for these checks.
Qualification results and the source/host limitations follow below; raw reports
remain ignored under `output/projection-followup/`.


Typed consumers retain producer leases until the callback finishes and do not
opt into spooled/consumed delivery. Lake execution therefore hydrates before the
typed callback. Archive column plans include bound scalar dependencies independently
of the public source mask: `fields: []` cannot erase operands needed by `select`,
and constant-only outputs need no source columns. This is a conservative column
read set for archive providers, not a claim of native per-field lazy I/O there.
Column-backed scalar output also retains requested `_source` values; public array
masking and scalar resolution use separate views of original positions. Ordinary
column wire responses without scalar outputs keep streaming rather than building
an extra DOM. Owned fallback plans use reclaimable scratch without bypassing quotas.

Media query layout is selected from path structure. Object-only paths use the
ordinary public source projection and a borrowed typed result; computing a
second large scalar output adds work without changing path semantics. Any
numeric path segment conservatively selects flat value/missing bindings so
array masks cannot compact an index before resolution. Optional identity pins
use independent computed identity outputs in either layout; projected source
keys cannot impersonate them. Missing computed pins never downgrade. This
choice is independent of image size, document IDs and benchmark corpus. A paired
all-flat implementation regressed large four-image CPU about 41%, motivating
this structural source/scalar split; that rejected result remains in ignored
qualification evidence rather than becoming a performance claim.

The in-process boundary carries a `ResultView` with optional row-aligned
`encoded_sources`. A non-null encoded source replaces that row's `_source` only
when the existing public-wire source witness matches the immutable bytes. Native
JSON rows can therefore bypass producer parsing and cloning; already-typed
column results retain their `_source` maps. The consumer handles the available
representation before the same producer lease ends. Caller-parsed JSON and
unqualified sources cannot acquire this fast path. Graph requests retain normal
wire delivery. No public response schema or query authority changes.

Gateway decoding charges a cumulative 32 MiB encoded-input limit before parsing
borrowed sources or computed values, while retained payload, expansion, digest,
revision, identity and live-policy checks remain independent. Identity output is
never taken from source keys, including encoded-source and wire fallbacks. A
1 MiB native source is covered by a contract test with only a 128 KiB preparation
arena, proving that the adapter does not require a payload-sized parse/clone.
The column-backed source-mask test explicitly retains typed source delivery.

The intermediate `dfbc59d9de77` release passed 57 HTTP cases but still increased
large-media CPU 16–32% because ordinary typed-source preparation parsed and
cloned JSON before consumption. Its executable, manifest and reports are retained
under ignored `output/projection-followup/structural-dfbc/`. The encoded-source
view reuses the representation already accepted by the public wire encoder;
it does not infer a new validation receipt from untrusted payload bytes.

Native table readers forward synchronous result delivery through the same bound,
provisioned and hosted read callbacks and transient-read retries as wire queries.
A consumer-bearing query finalizes at that owner instead of escaping through a
single-shard encoded-response shortcut. Legacy providers without the optional
callback retain their owned wire response; no process-local consumer enters a
shard RPC or a new public endpoint.

A native encoded-result consumer can prepare source-only masks with the same
private projection IR and zero named scalar bindings. Ordinary public wire
queries retain their established projection path. Preparation belongs to the
physical delivery plan after catalog/lake routing, not public query parsing.
This admits source masks to bounded native hydration without
computing an unused identity. Completion fingerprints remain private worker
transport metadata; public source-only responses omit `_computed`. Count-only,
empty-source and unsupported execution shapes retain their existing paths, as
do source masks beyond the prepared program's metadata bounds. No empty public
`select` or new source-projection option is introduced.

A source-only response lacking a completion witness cannot certify whether
numeric array masks were already applied. The coordinator refuses to infer that
state. An unsupported source-only program can fall back once to the existing
whole-query read, preserving filters, policy, consistency, retained cut and
original deadlines. Named scalar expressions never downgrade. Consumer failures
are normalized to `QueryDeliveryFailed` and cannot trigger this compatibility
retry; the gateway retains the original failure classification locally.

Encoded-delivery source preparation leaves ordered, retained/cursor and offset
queries on their existing projection path. Native consumers with retained cuts
or graph dialects also retain finalized wire responses, preserving carrier-owned
continuation and exhaustion semantics. Private worker source programs still
reconstruct their exact completion fingerprint, including during authenticated
hydration; public source-only planning does not make a new pagination contract.

Release qualification of `106b1e4b254c` caught ordinary-read regressions when
source-only programs were injected during all public parsing: Unicode ordinary
CPU rose 69% and escaped-string ordinary CPU rose 26%. That executable and its
reports remain in ignored `output/projection-followup/delivery-global-106b/`;
it is not the final qualified build. Source-program preparation now belongs to
native encoded-result delivery, after lake routing. Ordinary wire reads keep
their existing physical plan. Media groups estimated below 64 KiB encoded use
owned-wire delivery; larger groups request the borrowed result view. Estimates
choose a representation only: actual source/wire budgets, authorization,
identity, digest, MIME and decoded bounds are enforced in both paths.

Native result delivery is an explicit query-execution option through both the
query-only and modern-search dispatch branches. Sharing catalog resolution with
an outer consumer does not give a nested join/retrieval query that consumer:
internal subqueries retain owned responses until final assembly. Regression
coverage invokes the dispatcher with both request shapes, requires the native
consumer and prepared source program, and separately verifies nested wire
ownership without inheriting the outer consumer. The pre-fix dispatcher test
failed with `UnexpectedLegacyQuery`, rather than relying on a fixture that
unconditionally returns a prepared response.

Qualification at `8420bd4f9ccf` passed 57 HTTP cases and retained alias/source CPU
gains; ordinary Unicode and escaped-string controls returned near their baseline.
The expanded media suite still regressed large-media CPU about 20–34%, exposing
a second dispatch branch that discarded delivery for query-only bodies. That
executable and its reports remain in ignored
`output/projection-followup/delivery-dispatch-8420/`; it is not the final media
qualification. The consumer is now passed explicitly through both branches,
with nested query dispatch explicitly retaining its owned response.


### Release qualification for alias and media reuse (2026-10-28)

The measured production source is `0826126861ed`, including `main` through
`125f49a9c5fd`. Its ReleaseFast server SHA-256 is
`45f189800b5c1cd675a650672a7a54cf92e2553c0929ac290140e273747fb1fe`
(251,637,048 bytes). The fresh grep SHA-256 is
`a0a088ceb721a5b980f09b58920694e4931f962fe6ab1d5ab6dc7aba6efb84ff`
(8,350,072 bytes). The preserved baseline is production source `915e5493a2b8`,
main through `7dabd028d66d`, server SHA-256
`d2a0401ddb9573148dcadf2c524772dd8fd566469e00fc2c2dc8fe84fd8e508f`.
Both servers report `antfly dev (zig runtime)`. The candidate includes newer
main changes: these whole-server comparisons do not isolate every optimization.
Primary documents remain ASCH25 and the canonical value ABI is unchanged.

The full indexed HTTP suite passed **57 cases**, with seven optional profiling
cases skipped. This includes small/large original array paths and object paths,
one/two shards, missing fields, digest/revision/permission rejection, 128 pinned
references split below the binding limit, 130 rows crossing byte/key windows,
retained pagination and persisted upgrade/old-reader rollback. Native checks
passed 208 query contracts, 19 identity/publication cases, 31 shared-provider and
67 gateway cases, targeted lake source/dependency tests, and eleven shared
scheduler cases including overlapping parent/child allocation. The Apache
boundary check covered 2,563 files; storage control, formatting and Python lint
also passed. The merged source also passed the full Go SDK suite, the source/joined OpenAPI comparison and native generated-API checks. These checks are distinct from approval-gated repository CI.

All comparisons use five alternating trials and five requests per phase, on the
same shared 14-logical-CPU macOS ARM64 host with warm synthetic storage. CPU is
whole-server CPU, including background work. RSS is periodically sampled, not an
allocation counter or a hard peak bound; residency changes on this shared host
can produce large swings. No model/provider is invoked. Reports retain binary,
corpus and output hashes and every sample under ignored `output/`. Tables show
median baseline → candidate; neither universal performance nor zero cost follows
from these measurements.

Alias comparisons return identical logical output hashes for direct fields and aliases, with four approximately 256 KiB values. Copy chains emit each intermediate output.

| Alias workload / mode | Elapsed ms | Server CPU ms | Sampled RSS MiB |
|---|---:|---:|---:|
| escaped_opaque_copies0 / field | 10.72 → 10.90 | 9.79 → 9.83 | 195.3 → 193.1 |
| escaped_opaque_copies0 / alias | 17.77 → 10.82 | 17.01 → 9.85 | 193.4 → 193.4 |
| escaped_opaque_copies3 / field | 34.15 → 28.26 | 31.97 → 26.13 | 215.4 → 214.8 |
| escaped_opaque_copies3 / alias | 40.88 → 28.09 | 38.87 → 25.50 | 213.6 → 214.6 |
| unicode_string_copies0 / field | 11.99 → 10.60 | 11.02 → 9.66 | 189.2 → 190.0 |
| unicode_string_copies0 / alias | 14.25 → 10.53 | 13.39 → 9.67 | 189.2 → 190.3 |
| unicode_string_copies3 / field | 39.95 → 34.99 | 38.71 → 33.31 | 201.8 → 205.4 |
| unicode_string_copies3 / alias | 42.09 → 34.42 | 40.65 → 32.94 | 202.2 → 205.7 |

The source comparison uses eight documents and two metadata updates per trial. `small64` and `wide64` add 60 distinct metadata outputs. Full and conditional outputs are checked against ordinary reference reads, with certificate reuse and updated identities verified.

| Source workload | Full elapsed ms | Full CPU ms | Full RSS MiB | Conditional CPU ms |
|---|---:|---:|---:|---:|
| ascii | 9.04 → 6.36 | 7.57 → 4.87 | 204.8 → 201.4 | 0.73 → 0.68 |
| mixed | 11.30 → 8.31 | 9.99 → 7.06 | 210.3 → 212.4 | 0.71 → 0.73 |
| opaque | 8.99 → 7.77 | 7.60 → 6.36 | 201.3 → 205.9 | 0.80 → 0.71 |
| small64 | 2.03 → 2.13 | 1.56 → 1.62 | 171.2 → 171.3 | 1.60 → 1.62 |
| wide64 | 10.25 → 7.67 | 9.03 → 6.12 | 208.8 → 205.4 | 1.89 → 1.93 |
| escaped | 24.51 → 16.92 | 22.99 → 15.32 | 217.2 → 214.3 | 0.77 → 0.74 |
| unicode | 25.58 → 20.50 | 24.48 → 19.33 | 205.1 → 206.9 | 0.76 → 0.75 |

Positive control percentages mean more candidate CPU, negative percentages less. These controls include background process work and are not regression tolerances.

| Source workload | Ordinary CPU change | Ingestion CPU change | Metadata-update CPU change |
|---|---:|---:|---:|
| ascii | +2.8% | +8.2% | +0.0% |
| mixed | -3.7% | -3.3% | -0.8% |
| opaque | -9.2% | +4.9% | -0.3% |
| small64 | -0.8% | +1.2% | +0.6% |
| wide64 | +1.1% | +1.0% | +1.3% |
| escaped | -1.2% | -0.6% | +3.0% |
| unicode | -2.7% | -2.6% | -0.7% |

Media requests reach `ModelRequired` after authorization and all payload checks, before model resolution. Tiny PNGs are 1×1; medium PNGs are 64×64; crossover PNGs are 128×128; large PNGs are 256×256 random RGBA. Four images are used unless the name ends in `eight`. The skewed case has one 512×512 and seven 64×64 images.

| Media workload | Elapsed ms | Server CPU ms | Sampled RSS MiB |
|---|---:|---:|---:|
| tiny_shared_duplicates | 0.82 → 0.70 | 1.22 → 0.97 | 167.8 → 166.4 |
| tiny_distinct_unique | 1.04 → 1.12 | 2.63 → 1.89 | 188.6 → 166.5 |
| medium_shared_duplicates | 1.78 → 1.50 | 1.97 → 2.48 | 170.5 → 170.1 |
| medium_distinct_unique | 2.13 → 1.46 | 3.53 → 2.60 | 191.2 → 190.3 |
| crossover_shared_duplicates | 3.67 → 3.55 | 3.11 → 3.01 | 176.8 → 176.2 |
| crossover_distinct_unique | 2.19 → 2.66 | 4.32 → 4.49 | 198.9 → 198.8 |
| large_shared_duplicates | 11.59 → 11.09 | 11.10 → 10.65 | 203.4 → 201.7 |
| large_distinct_unique | 5.64 → 5.00 | 12.71 → 12.17 | 229.4 → 227.9 |
| large_distinct_duplicates | 4.66 → 5.00 | 11.96 → 11.59 | 225.7 → 225.5 |
| large_distinct_unique_eight | 9.40 → 9.19 | 24.51 → 23.90 | 263.0 → 264.2 |
| skewed_distinct_eight | 13.57 → 12.07 | 16.42 → 15.38 | 233.9 → 232.4 |

The following diagnostic compares the prior intermediate `dfbc59d9de77`, which includes main through `ff2c66bd7ba6`. The candidate includes newer main, so this is not an isolated measurement of delivery changes; that baseline server SHA-256 is `b6b3eb53f6bcb1ab5d1b31ed1cf63723f83cf0fad6d7e3bce450bee4616120c7`.

| Prior-build media workload | Elapsed ms | Server CPU ms | CPU change | Sampled RSS MiB |
|---|---:|---:|---:|---:|
| tiny_shared_duplicates | 3.46 → 2.84 | 1.03 → 2.43 | +135.8% | 151.4 → 170.5 |
| tiny_distinct_unique | 1.35 → 1.62 | 2.06 → 2.06 | +0.1% | 163.4 → 166.5 |
| medium_shared_duplicates | 1.65 → 1.55 | 2.41 → 1.01 | -58.2% | 168.9 → 170.0 |
| medium_distinct_unique | 1.23 → 1.37 | 2.23 → 2.36 | +6.1% | 189.1 → 189.2 |
| crossover_shared_duplicates | 4.38 → 3.48 | 3.82 → 2.91 | -23.9% | 176.7 → 176.5 |
| crossover_distinct_unique | 3.36 → 2.16 | 5.32 → 4.36 | -18.1% | 197.0 → 198.6 |
| large_shared_duplicates | 14.63 → 11.42 | 14.12 → 10.91 | -22.8% | 202.8 → 202.9 |
| large_distinct_unique | 6.00 → 5.19 | 15.24 → 11.98 | -21.4% | 225.2 → 228.2 |
| large_distinct_duplicates | 5.58 → 4.43 | 14.95 → 11.56 | -22.7% | 223.3 → 225.2 |
| large_distinct_unique_eight | 10.95 → 8.98 | 31.14 → 23.29 | -25.2% | 263.8 → 266.4 |
| skewed_distinct_eight | 15.12 → 11.76 | 18.08 → 14.65 | -19.0% | 231.1 → 232.4 |

Control repeats retain the same binaries and corpus/output hashes; both first and repeated observations are retained.

| Repeated workload / phase | Elapsed ms | CPU ms | Sampled RSS MiB |
|---|---:|---:|---:|
| small64 / ordinary | 0.86 → 0.89 | 1.16 → 1.30 | 167.8 → 167.8 |
| small64 / full | 2.13 → 2.15 | 1.61 → 1.59 | 170.9 → 171.1 |
| small64 / conditional | 2.15 → 2.18 | 1.60 → 1.67 | 171.1 → 171.5 |
| small64 / ingest | 9.78 → 11.22 | 18.54 → 19.36 | 164.3 → 164.5 |
| small64 / metadata_update | 9.43 → 9.60 | 10.89 → 10.93 | 173.6 → 173.7 |
| escaped / ordinary | 9.38 → 9.16 | 7.68 → 7.65 | 212.5 → 214.0 |
| escaped / full | 23.82 → 16.57 | 22.43 → 14.89 | 215.2 → 217.7 |
| escaped / conditional | 1.25 → 1.25 | 0.71 → 0.74 | 215.2 → 217.6 |
| escaped / ingest | 108.18 → 107.80 | 122.46 → 124.25 | 206.8 → 211.0 |
| escaped / metadata_update | 76.83 → 76.05 | 75.60 → 75.36 | 238.7 → 239.6 |

The medium/crossover controls were repeated separately with the same binaries and corpus, rather than replacing the initial measurements.

| Repeated media workload | Elapsed ms | CPU ms | CPU change | Sampled RSS MiB |
|---|---:|---:|---:|---:|
| medium_shared_duplicates | 1.57 → 1.40 | 1.15 → 1.16 | +1.2% | 170.7 → 170.1 |
| medium_distinct_unique | 1.32 → 1.17 | 2.27 → 2.03 | -10.6% | 187.0 → 185.3 |
| crossover_shared_duplicates | 3.36 → 3.29 | 2.90 → 2.85 | -1.8% | 176.6 → 176.2 |
| crossover_distinct_unique | 2.23 → 2.38 | 4.34 → 4.24 | -2.3% | 198.8 → 198.0 |

An initial all-flat implementation at `5ae406f3a0ca` passed correctness but
increased large four-image CPU about 41% versus the same baseline. It is not the
qualified implementation. Its reports and manifest remain in ignored
`output/projection-followup/all-flat-5ae/`. The subsequent structural-only build `dfbc59d9de77` still regressed large-media CPU 16–32%; its executable and reports are retained in ignored `output/projection-followup/structural-dfbc/`. The qualified shape uses source-only preparation and borrowed native delivery to avoid unused scalar computation and the source DOM clone. Numeric paths still
pay for scalar resolution because public masks otherwise compact their indexes.


The strongest measured gain is eliminating repeated normalization of forwarded
values: escaped aliases use 42% less CPU and escaped alias chains 34% less CPU,
with matching outputs. Full source projections use 16–36% less CPU for the larger
fixtures; the 64-output small-value workload is near parity. Conditional hits
remain sub-millisecond in server CPU for the larger four-output cases, rather
than becoming free. Ordinary-read controls range from −9.2% to +2.8% in the
initial source runs. The small64 repeat increases ordinary CPU by 0.14 ms and
conditional CPU by 0.07 ms; these costs remain visible in the tables.

Media gains are smaller against the original baseline than against the regressed
intermediate. Large-media CPU falls about 2.5–6.3%; some elapsed times increase.
The initial medium shared-source result increases CPU 26% (0.51 ms), while its
repeat increases 1.2% (0.014 ms). Crossover distinct-source latency increases in
both runs despite nearly flat CPU. The prior-build tiny shared-source control
also has a large CPU percentage increase at small absolute times. The different
main revisions, background work and shared host prevent attributing these small
controls to a single code path or claiming a universal win. Both initial and
repeat observations are retained. Broader storage/host and sustained concurrency
qualification remains necessary for performance guarantees.

Source-only programs are prepared only for native encoded-result delivery.
Ordinary wire parsing keeps its existing plan. Both terminal dispatcher shapes
preserve explicit delivery, while nested retrieval/join calls require owned
intermediate results. These ownership and planning rules are covered by the
dispatch regression, rather than inferred from a successful end-to-end request.

Reproduce from the repository root with the preserved baseline executable:

```sh
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_projection_aliases.py \
  zig/zig-out/bin/antfly --baseline /path/to/baseline/antfly \
  --rounds 5 --queries-per-phase 5 \
  --output output/projection-followup/aliases.json
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_source_projection.py \
  zig/zig-out/bin/antfly --baseline /path/to/baseline/antfly \
  --rounds 5 --queries-per-phase 5 --documents 8 --value-bytes 262144 \
  --metadata-updates 2 --workload ascii \
  --output output/projection-followup/source-ascii.json
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_media_materialization.py \
  zig/zig-out/bin/antfly --baseline /path/to/baseline/antfly \
  --rounds 5 --queries-per-phase 5 \
  --output output/projection-followup/media.json
```

Repeat source runs with `mixed`, `opaque`, `escaped` and `unicode`; add
`--metadata-outputs 60` to `small` and `ascii` for `small64` and `wide64`.
These results do not measure inference quality/billing, cold storage, sustained
cross-request throughput or other hosts. Allocation and admission bounds remain
required even when CPU or latency improves.


After release qualification, main advanced again. The follow-up merges
`386c2761eafc0f8cbc4022b15c4f639f718d1024`, preserving both projection and new
prompt-cache types in generated clients. Native API bindings were regenerated;
93 generated-API steps and two tests passed. The source OpenAPI comparison, full
Go SDK suite, Python export syntax, 2,578-file Apache boundary and storage control
catalog passed. The merged native query/provider/gateway run passed all 76 build
steps, including 208 query contracts, 31 provider cases and 67 gateway cases with
no failures or leaks. The numerical tables above continue to identify the actual
measured `0826126861ed` runtime; they are not measurements of this newer merge.

### Measured query-read optimizations after #1263

The follow-up keeps the public `fields` / `select` / `params` contract and ASCH25
format. It narrows physical routing and reduces projection preparation work;
it adds no persisted index or authorization cache.

For point-shaped filtered queries, a mandatory positive document-ID clause can
prove that only some owners can contribute. The router captures its catalog
snapshot first; single-owner tables skip the proof. It follows conjunctions and
required Boolean arms. Optional, negative and disjunctive arms cannot alone
justify pruning. At most 256 IDs are used; each is nonempty, at most 4 KiB and
contains no NUL. Other shapes use ordinary routing. Original predicates and scoring inputs reach selected
owners unchanged. Retained pagination covers and ranked, graph and aggregate
shapes retain their existing routing. A mismatched topology/catalog fence causes
retry, rather than combining snapshots. The proof stays local to this routing
call; it does not add request hint fields or rewrite logical membership.

Projection preparation owns one encoded extent. Transport decoding parses it
once, validates the request and streams the canonical fingerprint through a
fixed 4 KiB stack buffer. The parsed-number canonical form and exact encoded-input
ownership witness remain unchanged. The returned transport still has independent
ownership. A buffer is necessary: streaming every short JSON write directly to
SHA-256 avoided allocation but made the small-plan hash phase slower.

The isolated ReleaseFast benchmark alternates an allocating reference, an
unbuffered streaming control and buffered streaming for five trials, 128 hashes
per trial. All modes require identical SHA-256 values and balanced tracked
allocations. With 64 source-field entries, median reference → buffered encoding
and hash time is 1.403 → 1.315 µs for a 64-byte parameter and 205.767 → 200.219 µs
for a 256 KiB parameter. Allocator byte volume is 2,844 → 0 and 398,156 → 0. The
unbuffered small-plan control is 3.385 µs. This measures the encoding/hash phase,
not parsing, full plan/query latency, stack memory or process RSS.

Reproduce from `zig/` with pinned Zig 0.17.0:

```sh
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig run -O ReleaseFast \
  tools/bench_projection_fingerprint.zig
```

The final runtime candidate is `e1a0289e9aaa102135bce29e284e4b89456a4704` and the baseline is
`f2b7b76d57d2b51bf78171dcef3e8c0ccc582ffa`, the merge of #1263. Both use
`-Doptimize=ReleaseFast -Dstrip=true -j2` and Zig 0.17.0 on the same M4 Max host
(macOS 27.0.1, 14 cores). Raw per-run data, source/binary/driver hashes and host
metadata remain ignored under `output/projection-followup/`. No raw JSON reports
or logs are added to the repository. Historical tables above use different
revisions and build settings; they are separate evidence.


Final paired runs use the exact reduced candidate, with output values, scores and
counts checked for equality. Each set has five alternating trials. Initial sets
use five reads per phase; the longer routing repeats use 50. The table includes
both sets: latency was inconsistent in the initial short runs, including slower
clustered-ID reads despite lower CPU. The longer runs improve routing CPU in all
five paired rounds for both layouts. This establishes a warm-workload resource
saving, rather than a guarantee for every query or host.

| Reads/phase | Eight-shard workload | Elapsed ms, baseline → candidate | Server CPU ms | Sampled RSS MiB |
|---|---|---:|---:|---:|
| 5 | clustered IDs, full | 1.939 → 2.675 | 2.707 → 1.384 | 189.219 → 165.797 |
| 5 | clustered IDs, conditional | 1.978 → 2.457 | 2.970 → 1.429 | 189.422 → 165.875 |
| 5 | spread IDs, full | 8.400 → 8.949 | 6.033 → 4.199 | 229.266 → 205.906 |
| 5 | spread IDs, conditional | 12.784 → 9.379 | 10.998 → 6.314 | 234.078 → 207.406 |
| 50 | clustered IDs, full | 2.363 → 1.188 | 3.056 → 0.626 | 189.812 → 166.500 |
| 50 | clustered IDs, conditional | 2.610 → 1.283 | 3.430 → 0.701 | 190.266 → 166.234 |
| 50 | spread IDs, full | 2.555 → 2.067 | 4.287 → 2.367 | 221.078 → 198.234 |
| 50 | spread IDs, conditional | 3.788 → 2.900 | 7.266 → 3.745 | 214.836 → 199.484 |

The longer ordinary-read clustered case is 2.255 → 0.339 ms CPU (85.0% less);
the spread ordinary case is 3.437 → 2.018 ms (41.3% less). Clustered conditional
reads use 79.6% less CPU; spread conditional reads use 48.5% less. These gains
come from fewer owner executions. They do not make matching, authorization,
snapshot capture, transport or output free.

Controls retain the actual costs around the optimized paths. CPU percentages
below are observations from the initial five-read final matrix; positive means
more candidate CPU. Ingestion and metadata-write controls vary substantially
although this patch changes read routing/preparation. Payload-dominated and
single-owner paths do not show a universal speedup. In particular, the 1 MiB
conditional case adds 0.712 ms CPU, and the small64 ordinary case adds 0.068 ms.
No tolerance threshold turns those observations into wins.

| Workload | Full computed CPU ms | Conditional CPU ms | Ordinary CPU change | Ingest CPU change | Metadata-write CPU change |
|---|---:|---:|---:|---:|---:|
| 8 × 256 KiB ASCII | 5.923 → 5.745 | 1.034 → 1.016 | +0.1% | -1.6% | -5.6% |
| 8 × 1 MiB ASCII | 23.226 → 23.141 | 1.724 → 2.436 | -2.1% | +24.2% | -4.7% |
| tiny values, 64 outputs | 2.189 → 1.664 | 2.094 → 1.758 | +4.8% | -6.7% | -6.1% |
| escaped values | 15.900 → 16.120 | 0.939 → 0.932 | -6.1% | -5.3% | -3.0% |
| one shard, residual | 1.319 → 0.577 | 1.030 → 0.665 | +1.7% | -25.9% | -5.4% |
| eight shards, clustered IDs | 2.707 → 1.384 | 2.970 → 1.429 | -65.9% | -11.4% | +23.5% |
| eight shards, spread IDs | 6.033 → 4.199 | 10.998 → 6.314 | -34.6% | -29.2% | -0.5% |

The identity-only 64-document controls keep payload bytes out of the response.
The 1 MiB case is 4.623 → 4.088 ms CPU and 5.577 → 4.918 ms elapsed, with sampled
RSS 430.3 → 443.7 MiB; the 16 KiB control is 2.025 → 1.989 ms CPU. The final
candidate does not contain the experimental range-based batch admission, so
these observations cannot qualify that rejected optimization. Alias-control CPU
changes range from −16.5% to +0.3%, with most cases near parity; large payload
formatting still dominates preparation cost.

Model-free media controls retain the original allocation ownership. Every
request reaches `ModelRequired` after verification, with no provider execution.
The final candidate has no repeatable large-media improvement to claim.

| Media workload | Elapsed ms | Server CPU ms | Sampled RSS MiB |
|---|---:|---:|---:|
| tiny_shared_duplicates | 0.783 → 0.960 | 1.221 → 1.386 | 154.125 → 154.234 |
| medium_shared_duplicates | 1.578 → 1.731 | 1.995 → 1.385 | 157.703 → 158.328 |
| large_shared_duplicates | 11.081 → 11.028 | 10.576 → 10.679 | 189.531 → 188.516 |
| large_distinct_unique | 5.790 → 6.069 | 13.561 → 14.047 | 216.516 → 216.719 |
| large_distinct_unique_eight | 10.049 → 10.368 | 26.162 → 26.089 | 253.234 → 254.797 |
| large_512_distinct_unique_eight | 47.837 → 48.103 | 87.422 → 87.683 | 416.844 → 416.297 |

The reduced runtime passes all 91 debug native build steps (including 210 query
contracts, logical-field, catalog, provider and gateway suites) and all 71
ReleaseFast build steps. HTTP validation passes 57 cases; seven optional profile
cases are skipped. The one-second expiry fixture was fragile before first-page
capture on a loaded host; it now allows five seconds, then deliberately waits
past that lease before page two. Timed decision coalescing may produce partial
batches, so the visual test checks exact input/evidence totals and no additional
warm provider requests, rather than assuming one call within a 20 ms window.
Both initially failing cases passed independently before these fixture changes;
the corrected full suite also passes. Product source/binary hashes are unchanged
by the fixture fixes. Boundary, storage-control, formatting and Python lint checks
pass. Raw reports and logs remain untracked.

Build the pinned baseline and candidate in their own worktrees with the recorded
compiler/flags, then reproduce from the candidate repository root:

```sh
mkdir -p output/projection-followup
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_source_projection.py \
  /path/to/candidate/antfly --baseline /path/to/baseline/antfly \
  --workload small --shards 8 --residual-filter --documents 8 \
  --rounds 5 --queries-per-phase 50 --metadata-updates 2 \
  --output output/projection-followup/routing.json
# Repeat with --id-layout spread --documents 64.
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_projection_batches.py \
  /path/to/candidate/antfly --baseline /path/to/baseline/antfly \
  --documents 64 --value-bytes 1048576 --rounds 5 --queries-per-phase 5 \
  --output output/projection-followup/batches.json
zig/e2e/antfly/.venv/bin/python zig/e2e/antfly/bench_media_materialization.py \
  /path/to/candidate/antfly --baseline /path/to/baseline/antfly \
  --workload large_512_distinct_unique_eight --rounds 5 --queries-per-phase 5 \
  --output output/projection-followup/media.json
```

The final candidate executable SHA-256 is
`bf32f93d94e6d47fa3bcadd5274299ebbcf9b40a3fa16d07fc74fbe68208ecb7`;
the baseline is `226502864fa6b790b5c0938d8cdedfa8e8d49cee765ea1b7dd2b0ac6d0ce1469`.

Two additional experiments were measured and left out of the final implementation:

- Admitting source batches by selected range/reconstruction bytes instead of whole
  source bytes reduced CPU 17.2% in the initial 64-document, 1 MiB-payload run.
  The repeat was 2.0% slower. That is insufficient evidence for the extra default
  batching/selection machinery. The reproduction driver keeps identity-only
  warmup and score/count/value equality checks for future work.
- Allocating concurrent media payloads directly in the parent request arena saved
  a copy, but changed allocator ownership/lifetimes. For eight distinct 512×512
  images, initial median CPU was 90.68 → 86.93 ms and sampled RSS 323.6 → 414.1 MiB.
  The repeat was 159.23 → 141.35 ms and 311.5 → 399.3 MiB. Four-image and shared-mask
  controls also varied. That memory tradeoff does not justify the change as a
  default; the existing child-scratch/completion-copy ownership remains.

These experimental runs used `1540395365e5218d1c7cde656b354195d5cbb28e`, not the
final reduced candidate. Initial and repeat reports are retained separately.
Host load changed substantially between sets, so absolute times across sets
must not be combined. Five alternating rounds with five reads each compare
paired binaries within each set. CPU is whole-process CPU including background
work; RSS is sampled, not allocation volume or a hard peak bound. These tests
measure warm synthetic storage and model-free materialization, not inference
quality, provider billing, cold/distributed storage or cross-host guarantees.

The source benchmark exposes shard count, clustered/spread IDs and a neutral
residual predicate so routing cost is separate from predicate evaluation cost.
Distributed row-profile logs may be incomplete at the coordinator; functional
output, score and total-count equality is checked independently. The media driver
can select workloads, including eight 512×512 images, and stops at `ModelRequired`
after source authorization, revision, digest, MIME, byte and pixel checks.

After the measured runtime was qualified, the follow-up merged main
`207cd9ca3ba6a5fb2c7db72c11b8a0e4f7479563`. The measured production and benchmark sources remain
byte-identical to the pinned runtime. Merged-tree native validation passes all
91 build steps (226 reported native tests plus the custom-runner suites), with
no failures or leaks. The full license workflow also passes: header checks,
Apache boundary checks and the boundary/header/provenance/asset unit tests.
Three upstream inference qualification scripts retained their Apache SPDX tags
and gained the complete required preamble. The numerical tables above remain
measurements of the explicitly pinned executables, rather than a new build
of the later main merge. Full merged-head CI still requires the repository's
maintainer approval signal.

### Selected-range ownership and shared window plans

Range-shaped APIs do not by themselves prove a small working set. The generic
cursor adapter previously copied complete values into its scope before slicing
ranges; admission based only on selected lengths could therefore retain many
whole large documents. The cursor adapter now copies only coalesced selected
spans while its current cursor value is borrowed. Those owned spans remain valid
across subsequent seeks and until scope reset/close. Engine caches and the
cursor's current value retain their existing ownership and limits.

Read scopes explicitly distinguish whole-value retention, copying selected spans
from a borrowed whole value, and native selected-range I/O. The native LSM reader
and cursor adapter retain selected copies; the native Lite reader performs
selected-range I/O. Unknown adapters default to conservative whole-value admission. Cursor-backed
windows check cancellation between whole-document reads; native selected I/O
retains its batched range walk. Existing document scanners keep their physical
work windows when copying selected spans still requires reading whole values.
The capability is internal runtime metadata.

For capable scopes, selected output windows reuse a single revision-bound range
plan for both admission and execution. Unplanned, selected and full-read choices
are distinct states, so full-read fallbacks are cached too. Parent selections
subsume descendants.
The admission estimate includes coalesced span bytes (including bounded gaps),
reconstructed values, escaped ancestor keys and field/range/value headers.
Windows admit at most 64 documents and target 1 MiB of planned work, with a
separate selected-range setup limit. Copy-backed scopes also retain a separate
1 MiB original-value work window; only native selected I/O can admit by selected
bytes alone. Oversized copy-backed documents keep the single-document path and
are rejected before allocating a batch plan. Cached selections store pointers,
so scalar and full-read fallbacks do not copy an inline range plan for every row. Actual request allocation quotas remain
in force; this target is not an RSS bound or an estimate of backend cache/page
footprints. Full-source, missing-authority, old/unsupported and nonselective
reads retain the existing full-read fallback and accounting. Source revisions,
range digests and authorization still determine correctness.

The regression suite checks overlap elimination, missing selections, full-read
fallback and range-plan reuse. A cursor test restricts its allocator to 8 KiB
while reading selected spans from a 32 KiB value and changes the borrowed source
on each seek. This proves that selected reads neither require retaining the
whole value nor return views invalidated by a later cursor move.

The native LSM range reader releases temporary full-value copies and block pins
after each document, retaining only packed selected spans in the output scope.
Temporary point results use the existing storage working-set admission owner;
selected outputs retain the caller allocation budget. Full-value `get` and
`getManySorted` keep their original ownership contract.

### Owned gateway media payloads

The public inference gateway can consume a request-local media payload lease.
The ordinary source lookup, row authorization, revision/identity binding and
content verification still run for every distinct source claim. The lease
changes allocation ownership only; it grants no authority and is never cached
across requests.

Concurrent source groups allocate retained strings in separate payload arenas.
Each rolling job continues to use a reclaiming scratch arena for query delivery,
JSON parsing and selector evaluation. Completion transfers string views, frees
scratch immediately and retains only the payload arenas. One stable allocator
lock serializes allocation through the original request budget chain. The
consumer releases the lease after its last provider read, before the request
arena and budget leave scope; error and verification paths release it too.
Batch items release their own leases rather than accumulating them across the
batch. Synchronous and streaming providers complete input consumption before
returning from the bounded gateway executor.

Allocator-owned lookup remains available. Its completion copy preserves pointer
sharing for independently checked references to the same stored field. Identical
source claims are already deduplicated before lookup; equal byte contents from
separate documents do not share read authorization.

Unlike allocating outputs directly in the parent request arena, this ownership
model makes payload buffers individually reclaimable after consumption. It
eliminates concurrent completion copies without retaining each job's parsing
scratch. Small/sequential groups retain their existing allocator-owned path.

Qualification compares the exact previous PR head with the candidate,
using the same main merge, compiler and release flags. It uses
alternating rounds and exact source verification before `ModelRequired`.
The media matrix includes shared fields, distinct images, skewed sizes and
repeated row/field references with independent pixel limits. Provider billing
and inference quality are outside these model-free measurements. Raw reports,
logs and binary manifests remain ignored under `output/projection-followup/`.


### Qualification-driven admission correction

The first ownership candidate (`006c424f5579dd282e25285f0321bf11d0ec8be9cf57b4facd65a908414a7946`)
was compared against the exact previous PR runtime at `c1666f8859`, using baseline
`5c47279a8f37f8ef4a682fa64425cee0f839469a801cff7d681326f96ebd3e1d`.
Both included main `207cd9ca3b` and used identical Zig 0.17.0 stripped ReleaseFast
flags. Five alternating rounds cover nine media cases, scalar projections,
nested object/array projections, wide outputs and escaped values. Scalar repeats
use 100 requests per round; nested controls use 50. Exact outputs, identities,
scores and total counts agree; media stops after content verification at
`ModelRequired`.

This candidate did not establish general query throughput gains. Repeated
64-document scalar reads cost 3.094 → 3.142 ms CPU for 1 MiB values and
1.459 → 1.502 ms for 16 KiB values. Nested 1 MiB values cost
3.412 → 3.574 ms. Large ordinary-source CPU changed direction between the
initial and longer run (13.331 → 14.067 ms, then 13.119 → 12.544 ms).
Eight distinct 512×512 images improved 103.729 → 95.937 ms CPU, but most
media cases were approximately flat. Repeated 256×256 references improved
11.652 → 11.281 ms CPU; repeated 512×512 references were flat.
Sampled RSS and process footprint differ substantially under host memory
compression and reclamation, so these observations do not prove an RSS bound
or universal allocation savings.

The planner was corrected before final qualification: avoid allocating a
64-row plan before rejecting oversized copy-backed input, store cached plans
by pointer, and preserve an independent original-byte window for copied
ranges. This also prevents a small selected-output estimate from widening
whole-value I/O and delaying cancellation or first delivery. Native selected
I/O retains selection-based admission. The ownership tests establish bounded
retention and lifetimes independently of these throughput measurements.
The initial reports remain separate from final merged-tree qualification;
none of these numbers describe the corrected executable.

The corrected implementation merges main `b6e576b8e5`; final merged-tree
qualification uses an exact prior-PR-plus-main runtime baseline.


### Corrected merged-tree qualification

The corrected runtime at `23485ebe57` includes main `b6e576b8e5` and passes
all 91 native build/test steps, with 236 standard-runner tests plus custom
suites (211 query contracts, 90 logical-field tests with one optional skip,
31 provider tests, 68 gateway tests and 24 catalog tests). No tests failed
or leaked. License headers, Apache dependency boundaries, provenance and
asset checks, storage-control catalog validation, Zig formatting and Ruff
also pass on this merged tree.

The matching baseline is merge tree
`e4152d6f5f5323c6d574004a3638047dc2649e53`: the previous PR head
`c1666f8859` combined with the same main revision. Only the ten runtime files
changed by the follow-up differ between that baseline and the corrected
runtime. Both executables use the pinned Zig 0.17.0 compiler and identical
stripped ReleaseFast flags. The manifest records binary hashes, runtime and
driver hashes, build flags and host details. This comparison separates the
follow-up from unrelated changes in main. Recreate that baseline tree with
`git merge-tree --write-tree c1666f8859 b6e576b8e5`; restore the ten runtime
paths listed by the manifest while keeping the candidate checkout, toolchain
and build options fixed. Restore the candidate files after capturing the
baseline executable. No on-disk format or public API changes are involved.


### Immutable capability metadata and cheap fallback planning

The first same-main comparison of `496f1644` against `7499a2be` exposed a
persistent medium-image regression: 0.853 → 0.970 ms CPU initially and
0.758 → 0.827 ms in seven longer alternating rounds. Tiny-image CPU changed
direction in the repeat, while the 64-document scalar repeats were approximately
0.9–2.1% slower. These results remain separate from the following correction.

Read modes are immutable capability metadata, not executable callbacks. They
now live directly in scope vtables; read transactions expose the returned scope
type's known capability before opening a scope. Unknown adapters remain
conservative. Large copy-backed or uncertified rows decline batching before
opening scopes, and the 64-row temporary authority headers stay on the stack.
Scope lifetimes and returned plans/rows remain allocator-owned as before.
Capability tables are constructed explicitly at compile time. A type-only helper
must not turn an escaping table literal into a temporary runtime allocation;
the per-type table has static storage, while its handles remain request-owned.

The shared runtime dispatcher ignores non-callback metadata fields when building
its method dispatch, preserving rejection of attempts to invoke metadata as a
method. This avoids adding dispatch work to unrelated storage operations every
time a capability is added. Regression checks cover forged metadata calls,
metadata-only rejection without scope creation, and continued eligibility of
actual selected I/O. The exact same-main baseline executable is retained for
final qualification; the earlier reports are not overwritten.


### Final ownership and capability qualification (2026-10-29)

The final candidate executable is
`114c5de324e4e3037b1c0c8164dec83ac0a917971116f3fd968cf87c19b09b8f`;
the same-main baseline is
`7499a2be8f72b980cd89c631becc76af3f4e456711e5253b23e019dc301be3d3`.
Both use main `b6e576b8e5`, pinned Zig 0.17.0, stripped ReleaseFast builds
and the same shared 14-core M4 Max with 36 GiB RAM. The executable grows
from 234,013,512 to 234,094,984 bytes (81,472 bytes, about 0.035%). The baseline is the
prior PR runtime plus that main merge (`e4152d6f5f5323c6d574004a3638047dc2649e53`).
The manifest includes all twelve changed runtime paths, their source hashes,
compiler identity, driver hashes and binary sizes. No public API or ASCH25
format changes are involved.

All 91 native steps pass: 167 standard-runner tests executed (other steps
reuse previously validated results), 212 query contracts, 90 logical-field
checks with one optional skip, 31 provider, 68 gateway and 24 catalog checks.
The shared runtime ABI suite separately passes 47 tests. No failures or leaks.
The release build passes all 71 steps; real-server HTTP integration passes
57 tests with seven optional profile skips. Header, dependency-boundary,
provenance, asset, storage-control, formatting and Ruff checks pass.

The complete warm matrix below uses five alternating baseline/candidate
rounds; the two small media controls use seven. Media/source phases use 50
requests per round, narrow batch phases and small media controls use 100.
Every response is checked for exact source values, identities, scores, totals
or verified media evidence as applicable. Media ends at `ModelRequired` and
measures no provider inference or billing. CPU includes the whole server;
RSS is sampled every 5 ms and is affected by compression and background
storage activity. Ingestion is included as a control even though these changes
do not modify the write path. These are measurements, not universal speed or
memory guarantees. Initial reports and independent repeats are both retained.

| Workload / phase | Elapsed ms, baseline → candidate | CPU ms, baseline → candidate | RSS MiB, baseline → candidate |
|---|---:|---:|---:|
| tiny_shared_duplicates | 0.516 → 0.505 | 0.177 → 0.169 | 153.922 → 154.375 |
| medium_shared_duplicates | 1.057 → 1.041 | 0.681 → 0.677 | 153.641 → 154.875 |
| large_distinct_unique | 4.426 → 4.426 | 11.227 → 11.113 | 216.344 → 216.797 |
| large_distinct_unique_eight | 8.144 → 8.168 | 22.151 → 21.962 | 252.766 → 256.688 |
| skewed_distinct_eight | 10.863 → 10.724 | 13.129 → 12.969 | 219.852 → 221.172 |
| large_512_distinct_unique_eight | 45.587 → 45.274 | 82.392 → 82.056 | 408.562 → 407.047 |
| large_shared_duplicates | 11.310 → 11.720 | 10.862 → 11.262 | 189.398 → 191.453 |
| large_distinct_repeated_refs | 7.053 → 7.220 | 11.532 → 11.821 | 209.312 → 206.078 |
| large_512_distinct_repeated_refs | 48.113 → 45.836 | 48.378 → 46.393 | 321.609 → 317.750 |
| batches-scalar-small | 2.306 → 2.149 | 1.768 → 1.670 | 164.297 → 161.148 |
| batches-scalar-large | 3.930 → 3.972 | 3.379 → 3.437 | 429.703 → 414.109 |
| batches-nested-small | 2.992 → 3.517 | 2.350 → 2.738 | 165.734 → 164.734 |
| batches-nested-medium | 3.105 → 2.904 | 2.441 → 2.296 | 177.609 → 177.625 |
| batches-nested-large | 5.415 → 7.026 | 4.703 → 6.159 | 474.969 → 476.000 |
| source-small64/ingest | 13.300 → 11.992 | 23.310 → 20.840 | 151.781 → 152.156 |
| source-small64/ordinary | 0.736 → 0.686 | 0.320 → 0.274 | 155.141 → 155.484 |
| source-small64/full | 2.261 → 2.011 | 1.683 → 1.502 | 159.391 → 159.047 |
| source-small64/reference_full | 2.251 → 2.012 | 1.727 → 1.517 | 158.641 → 158.438 |
| source-small64/conditional | 2.339 → 2.120 | 1.771 → 1.595 | 159.562 → 159.438 |
| source-small64/reference | 2.376 → 2.214 | 1.806 → 1.663 | 159.391 → 159.656 |
| source-large/ingest | 147.703 → 171.806 | 153.562 → 177.336 | 270.531 → 271.391 |
| source-large/ordinary | 21.369 → 24.247 | 16.581 → 18.841 | 468.156 → 484.727 |
| source-large/full | 28.584 → 28.295 | 23.705 → 23.318 | 643.328 → 633.391 |
| source-large/reference_full | 45.918 → 45.036 | 41.267 → 40.407 | 723.211 → 682.586 |
| source-large/conditional | 1.382 → 1.458 | 0.844 → 0.892 | 624.250 → 652.734 |
| source-large/reference | 18.979 → 20.045 | 18.561 → 19.741 | 624.469 → 624.438 |
| source-escaped/ingest | 109.784 → 110.518 | 125.992 → 125.582 | 198.891 → 198.266 |
| source-escaped/ordinary | 9.542 → 9.513 | 7.813 → 7.814 | 206.219 → 202.094 |
| source-escaped/full | 17.239 → 16.952 | 15.740 → 15.465 | 210.859 → 209.188 |
| source-escaped/reference_full | 31.083 → 29.845 | 29.851 → 28.817 | 211.188 → 209.000 |
| source-escaped/conditional | 1.172 → 1.122 | 0.640 → 0.630 | 210.906 → 125.453 |
| source-escaped/reference | 16.241 → 15.597 | 15.926 → 15.282 | 210.906 → 125.125 |
| source-wide-small/ingest | 19.948 → 21.878 | 38.716 → 38.153 | 160.188 → 161.328 |
| source-wide-small/ordinary | 2.231 → 2.157 | 1.331 → 1.295 | 165.516 → 167.094 |
| source-wide-small/full | 3.681 → 3.775 | 2.757 → 2.856 | 172.242 → 171.055 |
| source-wide-small/reference_full | 4.549 → 4.615 | 3.574 → 3.643 | 170.969 → 170.891 |
| source-wide-small/conditional | 2.383 → 2.378 | 1.759 → 1.777 | 178.188 → 176.656 |
| source-wide-small/reference | 3.485 → 3.316 | 2.719 → 2.723 | 178.391 → 176.688 |
| source-wide-large/ingest | 153.290 → 169.555 | 154.360 → 158.750 | 273.594 → 279.234 |
| source-wide-large/ordinary | 18.268 → 18.516 | 13.939 → 14.128 | 454.625 → 414.500 |
| source-wide-large/full | 23.759 → 23.913 | 19.762 → 19.641 | 631.000 → 627.500 |
| source-wide-large/reference_full | 34.877 → 35.901 | 31.275 → 31.663 | 630.008 → 588.422 |
| source-wide-large/conditional | 2.632 → 2.917 | 1.967 → 2.145 | 623.219 → 519.547 |
| source-wide-large/reference | 16.037 → 16.923 | 15.631 → 16.337 | 561.984 → 483.859 |


The medium shared-image overhead seen before immutable capability metadata is
absent in this matrix (0.681 → 0.677 ms CPU). Small scalar/nested-medium batches
improve about 5.5–6%; repeated 512×512 references improve about 4.1% CPU.
Other media gains are small, and some controls regress. The large nested batch
initially regresses 31% CPU and the wide-large conditional phase regresses 9%.
Those cases must not be advertised as performance wins. The selected-copy
contract provides bounded retained output ownership even when physical reads
still visit whole LSM values; it does not establish sparse LSM I/O or bounded
process RSS. The original-byte admission cap and cancellation checks remain
necessary independent of timing. Copy-backed engines can pay per-document
working-set ownership and selected-copy overhead in exchange for not retaining
irrelevant whole-value results until the scope ends. Actual selected I/O and
full-value consumers retain separate cost and ownership contracts.

Reproduce using the existing drivers with these exact binaries, alternating
`--rounds 5 --queries-per-phase 100` for `bench_projection_batches.py`,
`--rounds 5 --queries-per-phase 50` for `bench_source_projection.py` and
`bench_media_materialization.py`, and `--baseline <same-main-baseline>`.
Batch controls use 64 documents, scalar/nested metadata and 16 KiB/64 KiB/1 MiB
values. Source controls cover small 60-output rows, ASCII 1 MiB, escaped values,
and 60-output 64 KiB/1 MiB rows. Media controls cover tiny/medium/large shared
fields, distinct/skewed images and repeated independently checked references.
Raw manifests, per-request samples, logs and binaries stay ignored under
`output/projection-followup/`; no raw JSON or logs are added to the repository.


Independent repeats use seven alternating rounds and 100 requests per phase
with the identical frozen binaries and drivers. Every output check passes.
They do not replace the initial observations above.

| Repeat / phase | Elapsed ms, baseline → candidate | CPU ms, baseline → candidate | RSS MiB, baseline → candidate |
|---|---:|---:|---:|
| nested-large | 4.394 → 4.438 | 3.695 → 3.739 | 488.672 → 482.297 |
| nested-small | 2.769 → 2.810 | 2.116 → 2.172 | 165.766 → 165.742 |
| source-large/ingest | 165.227 → 235.633 | 159.147 → 162.273 | 220.109 → 275.062 |
| source-large/ordinary | 19.251 → 19.805 | 15.142 → 14.972 | 516.188 → 522.805 |
| source-large/full | 23.662 → 32.437 | 19.248 → 21.395 | 632.328 → 556.781 |
| source-large/reference_full | 35.958 → 37.871 | 31.215 → 31.833 | 607.367 → 604.523 |
| source-large/conditional | 1.447 → 2.457 | 0.869 → 1.167 | 631.812 → 516.375 |
| source-large/reference | 15.121 → 18.176 | 14.720 → 16.072 | 624.453 → 327.547 |
| source-wide-large/ingest | 145.952 → 151.511 | 146.816 → 149.560 | 276.594 → 275.078 |
| source-wide-large/ordinary | 17.388 → 17.270 | 13.499 → 13.423 | 615.492 → 609.133 |
| source-wide-large/full | 22.345 → 22.725 | 18.560 → 18.973 | 745.078 → 746.367 |
| source-wide-large/reference_full | 34.030 → 34.241 | 30.440 → 30.867 | 739.180 → 736.562 |
| source-wide-large/conditional | 2.374 → 2.400 | 1.823 → 1.840 | 740.359 → 738.594 |
| source-wide-large/reference | 15.863 → 15.908 | 15.503 → 15.521 | 728.188 → 731.594 |
| media-shared/large_shared_duplicates | 10.629 → 10.612 | 10.168 → 10.160 | 191.656 → 192.031 |
| media-shared/large_distinct_repeated_refs | 6.931 → 6.668 | 11.381 → 11.079 | 208.938 → 208.031 |


The larger apparent nested-batch regressions shrink to 1.2% and 2.6% CPU;
wide-large conditional CPU changes from an initial 9% regression to 0.9%.
Shared-image CPU is flat in the repeat; independently checked repeated 256×256
references improve about 2.7%. Ordinary large-source CPU is roughly flat, but conditional CPU
rises from 0.869 to 1.167 ms, and full-source CPU rises from 19.248 to 21.395 ms.
The evidence does not establish a universal improvement. These ownership and
admission changes are retained for their directly tested quota/lifetime and
bounded-work contracts, not described as a CPU win for these regressing cases.
Independent Linux hardware, cold storage and live providers remain unqualified.


### Borrowed selected points and verified native media (2026-10-29)

The follow-up merges main `c912271651` in `8a6bcad577` before qualification.
The baseline is the exact previous PR runtime with that merge, built using the
pinned Zig 0.17.0 stripped ReleaseFast flags. Its executable is
`21427ed8a500dda4f14d3789fb01ef9a2ddd274d4803baeefdbeda8baad8c453`
(234,373,864 bytes). It is independent of the earlier comparison against the
previous main revision; those earlier observations are retained above.

The LSM range path now treats its full point value as a temporary loan. It can
borrow the pinned transaction's memtables and adopt one decoded local block,
then copy only the coalesced selected spans into the output owner. The loan
retains its existing working-set/cache reservation. This includes uncached and
oversized local blocks, which ordinary bounded result pins deliberately decline.
A wide owned result can transfer into a temporary shared owner after admission;
optional admission denial preserves the compact-copy fallback. Metadata capacity
and the numeric L0 hint are reusable across range batches; payloads and pins
retire after every document. Full-value APIs, physical I/O windows and per-row
cancellation retain their contracts. This does not introduce sparse LSM I/O.

Native tests cover cache enabled/disabled, 32 KiB and over-1-MiB values, an 8 KiB
output budget, a working-set limit sized for one value, exact copy counts and
zero retained working-set credit after each call. The tests also cover output
stability after cache eviction, normal/private snapshots, concurrent replacement
and deletion, and a 4 KiB working budget reading selected memtable spans.

Source media resolution can retain its already verified codec bytes in a
request-owned lease. A lazy policy enables this only for compatible linked
native generation, including named generated deciders. Ordinary requests do
not resolve this policy. Reuse has a 1 MiB soft codec budget after reference
deduplication; exceeding it keeps ordinary verification and execution, rather
than retaining a maximum-sized request or rejecting it for reuse. The source
URLs and all content checks remain unchanged. Hosted, remote, chained, raw generated-envelope and
non-default-detail paths keep their existing representation. No remote
attachment capability is assumed and no model routing choice changes.

The invocation's `RequestContext` borrows immutable URL/codec/MIME views; durable
execution environments do not own them. Every source authorization, revision,
digest, MIME, size and per-reference pixel check still runs. Identical verified
payloads can share codec bytes within this invocation. The lease survives the
last provider read and is released on success, cancellation and failure.

The linked JSON generation adapter replaces compatible canonical image parts
with references into the existing binary attachment envelope. The typed linked
adapter uses the existing borrowed-attachment ABI. JSON frames and parsing
scratch use reclaiming allocators: their credit returns after every synchronous
provider invocation even when an outer request arena spans many decisions.
A regression sends 32 frames through one live request arena under a 512 KiB
budget and verifies exact content and restored credit after each invocation.
Repeated occurrences have
separate transport slots pointing to the same retained buffer. Existing
attachments, non-default detail and unrelated parts preserve their meaning;
opaque tools and schemas are not traversed. JSON number spellings are preserved
losslessly while rewriting metadata. Native consumers continue their own MIME,
pixel, resource and model admission checks. The attachment request carries the
caller's generation budget and cancellation/deadline control through native
admission; older internal attachment requests retain their 256-token default.
A configured remote default for decisions disables retention even when a linked
callback exists, following the decision runtime's effective routing precedence.

Regression checks verify that a source image reaches a named generated decider
with its bytes live through the native callback, then releases its source owner
exactly once. Separate checks cover lazy policy evaluation, deduplicated buffers,
failed verification cleanup, typed aliases, mixed media, attachment indices,
opaque tool/schema values, and large/tiny/signed-zero numeric spellings. Shared
execution-context tests are included in the inference contract gate.


The transport microbenchmark is `zig/tools/bench_verified_media.zig`. Build from
repository root, using worktree-local caches (all imported modules use the same
optimization mode):

```sh
zig build-exe -OReleaseFast -fstrip --dep media --dep httpx \
  -Mroot=zig/tools/bench_verified_media.zig \
  -OReleaseFast -fstrip --dep antfly_inference_execution_control --dep httpx \
  -Mmedia=zig/pkg/antfly-embedded/src/inference/execution_context.zig \
  -OReleaseFast -fstrip \
  -Mantfly_inference_execution_control=zig/pkg/inference/src/host/execution_control.zig \
  -OReleaseFast -fstrip -Mhttpx=zig/lib/httpx/src/httpx.zig \
  --cache-dir zig/.zig-cache --global-cache-dir zig/.zig-cache/global \
  -femit-bin=zig/zig-out/bin/bench-verified-media
zig/zig-out/bin/bench-verified-media
```

This measures the JSON transport adapter after source verification, including
framing, native parsing and byte equality. It does not measure authorization,
source verification, model inference or the typed attachment path. The inline
control parses JSON and decodes base64 again; the candidate retains verified
codec input and constructs a binary envelope. Framing can allocate more than
the inline control even when decoding costs fall. Retained codec input is
reported separately from transport allocations, and every trial checks that all
measured allocations were freed. This is not a process-RSS or provider-cost
benchmark. The 64-byte control is retained alongside 256-KiB and 1-MiB payloads.


#### Matched qualification of selected loans and native media

The final stripped ReleaseFast candidate is
`8b6774236c6d97b3e629b510c54527006270a75572b432b143eef6ccabd39818`
(234,407,192 bytes), compared with the merged prior PR head `8a6bcad577`:
`21427ed8a500dda4f14d3789fb01ef9a2ddd274d4803baeefdbeda8baad8c453`
(234,373,864 bytes). Both include main `c912271651` and use pinned Zig 0.17.0,
`-Doptimize=ReleaseFast -Dstrip=true -j2`. The binary grows 33,328 bytes.
The compiler executable SHA-256 is
`1c5f706db0ed6d55451940f31dc05a6177d05696bc1fa5d853696d04c718b523`.
Measurements use a shared Apple M4 Max, 14 logical CPUs, 36 GiB RAM,
macOS 27.0.1 ARM64. No task compiler runs overlap these server timing trials.

The full native check passed 102 build steps and 190 standard-runner tests,
plus 212 query-contract, 91 logical-field, 31 provider, 72 gateway and 24 catalog
cases, with one optional logical-field skip. Subsequent source-only media
refinements passed the 71-step gateway/contract check; the final gateway suite
executes 73 cases, including the repeated-frame budget regression. All report
zero failures and leaks. The final release passes 71 steps. Real-server indexed
HTTP integration passes 57 cases, with seven optional profile cases skipped.
License/header/boundary, storage-control catalog, provenance/asset tests, Zig
formatting and benchmark-driver Ruff checks also pass.

The following table retains all 44 initial controls: five source corpora with
six phases each, five projection batches and nine media cases. Each uses five
alternating baseline/candidate rounds and exact output comparisons. Source
and media trials run 50 reads per phase; projection batches run 100. Source
corpora contain eight documents. Large ASCII values are 1 MiB, escaped values
256 KiB, and wide-small ASCII values 64 KiB. The wide and small64 cases request
60 extra computed metadata outputs; small64 has short content. Projection
batches contain 64 documents with scalar or nested metadata and the indicated
16 KiB, 64 KiB or 1 MiB unrelated values. Media fixture names identify distinct,
shared, repeated and skewed 256/512-pixel inputs.

CPU and elapsed columns are milliseconds per operation; RSS is sampled MiB.
Ingest is per fixture insertion. Reference phases deliberately materialize
content and are retained as controls, not presented as optimized paths.
Whole-server CPU includes background work; sampled RSS includes allocator and
cache retention and is not an allocation bound. Media materialization fixtures
finish with `ModelRequired` after verifying sources: these are retrieval
controls, not model-inference or codec-reuse quality/billing measurements.


**Source delivery**

| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| escaped / ingest | 130.179 → 130.578 | 117.142 → 112.255 | 199.078 → 199.266 |
| escaped / ordinary | 7.848 → 7.725 | 9.516 → 9.380 | 205.258 → 206.047 |
| escaped / full | 15.484 → 15.471 | 16.926 → 17.002 | 211.797 → 216.297 |
| escaped / reference_full | 28.715 → 28.812 | 29.746 → 29.906 | 210.688 → 215.312 |
| escaped / conditional | 0.734 → 0.849 | 1.307 → 1.648 | 214.375 → 216.344 |
| escaped / reference | 15.441 → 15.644 | 15.787 → 16.026 | 214.391 → 216.344 |
| large / ingest | 143.326 → 144.395 | 138.017 → 135.913 | 276.578 → 274.938 |
| large / ordinary | 13.949 → 14.176 | 17.923 → 18.159 | 480.641 → 465.977 |
| large / full | 18.379 → 18.427 | 22.067 → 22.227 | 747.328 → 747.969 |
| large / reference_full | 30.747 → 30.389 | 34.137 → 33.693 | 740.172 → 740.594 |
| large / conditional | 0.879 → 0.887 | 1.490 → 1.519 | 739.672 → 741.359 |
| large / reference | 14.911 → 14.732 | 15.310 → 15.100 | 739.797 → 742.266 |
| small64 / ingest | 22.599 → 23.092 | 13.212 → 12.908 | 152.016 → 152.047 |
| small64 / ordinary | 0.340 → 0.409 | 0.810 → 0.973 | 155.844 → 155.688 |
| small64 / full | 1.612 → 1.670 | 2.291 → 2.448 | 159.609 → 160.062 |
| small64 / reference_full | 1.692 → 1.714 | 2.367 → 2.461 | 159.094 → 159.375 |
| small64 / conditional | 1.719 → 1.695 | 2.465 → 2.432 | 159.984 → 160.094 |
| small64 / reference | 1.723 → 1.742 | 2.451 → 2.449 | 160.406 → 160.141 |
| wide-large / ingest | 150.590 → 149.175 | 143.485 → 151.581 | 278.094 → 276.938 |
| wide-large / ordinary | 14.139 → 14.198 | 18.294 → 18.302 | 484.969 → 477.352 |
| wide-large / full | 19.458 → 19.055 | 23.277 → 22.884 | 755.398 → 746.438 |
| wide-large / reference_full | 31.632 → 31.692 | 34.923 → 35.068 | 743.797 → 741.281 |
| wide-large / conditional | 2.024 → 1.966 | 2.763 → 2.641 | 739.859 → 738.641 |
| wide-large / reference | 15.977 → 16.032 | 16.404 → 16.470 | 741.906 → 738.641 |
| wide-small / ingest | 35.766 → 35.349 | 18.436 → 18.870 | 160.984 → 160.641 |
| wide-small / ordinary | 1.245 → 1.299 | 2.091 → 2.167 | 166.922 → 166.500 |
| wide-small / full | 2.799 → 2.752 | 3.760 → 3.649 | 172.266 → 172.344 |
| wide-small / reference_full | 3.563 → 3.611 | 4.500 → 4.558 | 170.594 → 170.156 |
| wide-small / conditional | 1.770 → 1.742 | 2.444 → 2.368 | 176.438 → 177.125 |
| wide-small / reference | 2.736 → 2.690 | 3.426 → 3.365 | 176.500 → 177.188 |

**Projection batches**

| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| nested-large / read | 3.689 → 3.770 | 4.515 → 4.575 | 480.094 → 492.125 |
| nested-medium / read | 2.345 → 2.325 | 3.179 → 3.159 | 181.188 → 178.828 |
| nested-small / read | 2.349 → 2.307 | 3.287 → 3.216 | 166.469 → 166.359 |
| scalar-large / read | 3.262 → 3.357 | 3.908 → 4.027 | 461.781 → 457.203 |
| scalar-small / read | 1.807 → 1.808 | 2.434 → 2.419 | 165.125 → 164.922 |

**Media materialization**

| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| large_distinct_unique / read | 13.130 → 12.747 | 5.871 → 5.707 | 218.062 → 219.242 |
| large_distinct_unique_eight / read | 28.663 → 29.797 | 13.019 → 19.899 | 257.797 → 262.273 |
| skewed_distinct_eight / read | 15.185 → 15.240 | 12.077 → 12.316 | 224.461 → 220.766 |
| large_512_distinct_unique_eight / read | 86.292 → 86.541 | 47.520 → 47.506 | 406.766 → 407.172 |
| large_shared_duplicates / read | 11.292 → 10.676 | 12.984 → 11.459 | 193.609 → 190.469 |
| large_distinct_repeated_refs / read | 12.210 → 11.818 | 7.927 → 7.512 | 206.078 → 206.891 |
| large_512_distinct_repeated_refs / read | 43.134 → 43.304 | 42.683 → 42.866 | 320.906 → 322.695 |
| tiny_shared_duplicates / read | 0.396 → 0.395 | 1.035 → 1.060 | 154.312 → 154.188 |
| medium_shared_duplicates / read | 1.022 → 0.983 | 1.638 → 1.550 | 157.828 → 157.844 |

Reproduce the server comparisons from repository root with the pinned baseline
and candidate executables. Run each case serially; use the table's corpus sizes,
metadata shape and extra-output settings for the remaining cases:

```sh
grep_candidate=output/projection-followup/next-candidate/antfly
grep_baseline=output/projection-followup/next-baseline/antfly
python3 zig/e2e/antfly/bench_source_projection.py "$grep_candidate" \
  --baseline "$grep_baseline" --rounds 5 --queries-per-phase 50 \
  --workload ascii --value-bytes 1048576 --output output/source-large.json
python3 zig/e2e/antfly/bench_projection_batches.py "$grep_candidate" \
  --baseline "$grep_baseline" --rounds 5 --queries-per-phase 100 \
  --documents 64 --metadata-shape nested --value-bytes 65536 \
  --output output/batches-nested-medium.json
python3 zig/e2e/antfly/bench_media_materialization.py "$grep_candidate" \
  --baseline "$grep_baseline" --rounds 5 --queries-per-phase 50 \
  --workload large_shared_duplicates --workload large_distinct_unique_eight \
  --output output/media-controls.json
```

For source cases, add `--metadata-outputs 60` for wide-large, wide-small and
small64; use `--workload small` for small64 and `--workload escaped` for escaped.
Batch scalar cases use 16 KiB/1 MiB values; nested cases use 16 KiB/64 KiB/1 MiB.
The media driver accepts repeated `--workload` arguments using the exact names
in the table. Driver reports include corpus and executable hashes; require the
recorded matching baseline/candidate identities when comparing these results.
An older same-binary driver preflight with a different hash is explicitly
excluded from this matrix. All raw reports, logs and manifests remain ignored
under `output/projection-followup/`.


#### Independent repeats and native codec tradeoffs

Longer independent runs use seven alternating rounds and 100 reads per phase.
The initial small ordinary-read CPU regression (0.340 to 0.409 ms) does not
reproduce: the repeat is 0.348 to 0.329 ms. Escaped conditional reads similarly
change from an initial 0.734 to 0.849 ms to 0.656 to 0.588 ms in the repeat.
The distinct-media latency outlier shrinks from 13.019 to 19.899 ms initially
to 9.168 to 9.452 ms in the repeat. The initial shared-media CPU gain also does
not reproduce: 10.357 to 10.378 ms is effectively flat. Both sets are retained;
these observations do not establish a general query-throughput improvement.

| Independent repeat / phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| small64 / ingest | 25.646 → 19.109 | 14.359 → 10.847 | 152.125 → 151.953 |
| small64 / ordinary | 0.348 → 0.329 | 0.817 → 0.831 | 155.844 → 155.719 |
| small64 / full | 1.516 → 1.542 | 2.098 → 2.163 | 159.656 → 160.312 |
| small64 / reference_full | 1.542 → 1.597 | 2.131 → 2.256 | 159.391 → 159.609 |
| small64 / conditional | 1.629 → 1.626 | 2.259 → 2.292 | 162.531 → 165.625 |
| small64 / reference | 1.712 → 1.629 | 2.406 → 2.246 | 166.578 → 165.953 |
| escaped / ingest | 127.193 → 126.582 | 111.060 → 109.738 | 198.797 → 198.766 |
| escaped / ordinary | 7.857 → 7.750 | 9.451 → 9.321 | 210.281 → 206.062 |
| escaped / full | 15.394 → 15.340 | 16.787 → 16.766 | 215.672 → 216.422 |
| escaped / reference_full | 28.608 → 28.966 | 29.575 → 29.931 | 215.422 → 214.742 |
| escaped / conditional | 0.656 → 0.588 | 1.183 → 1.068 | 215.766 → 216.469 |
| escaped / reference | 15.340 → 15.515 | 15.691 → 15.860 | 215.781 → 216.484 |
| large_shared_duplicates / read | 10.357 → 10.378 | 10.861 → 10.888 | 192.188 → 194.453 |
| large_distinct_unique_eight / read | 23.873 → 24.286 | 9.168 → 9.452 | 261.078 → 257.258 |


The standalone native JSON transport benchmark uses the final production
allocation policy: both the temporary parser and the binary frame use
reclaiming allocation. Each run has five alternating rounds of 64 iterations
per variant and payload. A second independent run is shown rather than
selecting the more favorable first run. Times are **elapsed microseconds**,
including exact byte comparison; they are not process-CPU measurements.

| Codec bytes | First elapsed µs inline → verified | Repeat elapsed µs inline → verified | Allocated bytes/op inline → verified | Allocations/op inline → verified |
|---|---:|---:|---:|---:|
| 64 | 0.640 → 2.023 | 0.616 → 1.914 | 3,044 → 9,562 | 3 → 9 |
| 262,144 | 539.594 → 428.782 | 455.354 → 455.344 | 791,802 → 1,057,546 | 4 → 9 |
| 1,048,576 | 2013.040 → 1633.524 | 2179.157 → 1951.566 | 3,151,098 → 4,203,274 | 4 → 9 |


The 1 MiB transport stage is about 10–19% faster across these two runs;
256 KiB varies from flat to about 21% faster. The 64-byte control is about
three times slower, an absolute increase of roughly 1.3–1.4 µs. Frame transport
allocates about 33% more bytes than the inline control for the larger payloads.
It additionally borrows the retained 64/262,144/1,048,576 codec bytes from the
caller; those inputs, source authorization/verification and model work are
outside the measured allocation scope. This is not a whole-query, model-quality,
RSS or provider-billing result. Typed borrowed attachments avoid constructing
this JSON frame, but that path is verified by alias/lifetime tests rather than
timed by this transport tool.

The standalone tool was aligned with the production reclaiming allocator after
the server was captured. It is not a server dependency: all frozen server source
hashes and the server executable remain unchanged. Tool source SHA-256 is
`be0e26b534e6db3c537c1583a3e12a663c08fff0257fa559c484f8fb17ca2250`; executable SHA-256 is
`4be4d8784ed99e23c957c8949b05f4d0093752d114d6387b9986f420075d4d0b`. The earlier arena-output control and its
original source/executable identities remain separately recorded in the ignored
manifest, along with both production-policy runs.

A rejected borrowed-view experiment used the native JSON library to borrow
message/part spans instead of copying dynamic strings. It reduced allocation
volume by about 66% against its inline control. It required repeated validation
scans and measured about 49%/36% slower at 256 KiB/1 MiB. It is not shipped. A future
single-pass adapter would need to demonstrate gains without those rescans before
replacing the current encoder.

The retained improvements are the directly tested contracts: selected outputs
do not require retaining whole payload copies; one admitted point loan retires
after copying its spans; optional codec retention is capped; frame credit
returns between invocations; native token budgets and cancellation survive
adaptation. Timings vary across controls, so this is not described as universally
faster, sparse LSM I/O, a process RSS bound, or zero inference cost. Independent
Linux hardware, cold storage and live-provider quality/cost remain unqualified.

## Batch cancellation and structured native media (2026-10-30)

This follow-up merges main `911aa2f2fa` into PR #1365. Native selected-copy
batches now keep one scratch owner while checking cancellation before each
whole-value read. The erased storage scope has an optional controlled-batch
callback; older adapters retain per-key polling, and genuine selected-I/O
adapters retain their bounded batches. The polling callback uses fixed C integer
statuses, so private Zig error identities do not cross compilation owners.
Temporary payload loans and pins retire between keys, including cancellation and
failure exits. Existing source admission windows and snapshot cuts remain in
force.

The native allocation fixture reads 32 persisted 32 KiB values, selecting 32
bytes from each. Four alternating trials per variant allocate 384 scratch
objects through individual calls and 260 through a controlled batch: 124 fewer
allocations, or 32.3%. This is an allocation result. It does not establish a
whole-query latency improvement. The quota fixture also verifies cancellation
between values, no second-key delivery, released working-set credit and reuse
of the same pinned snapshot after cancellation.

Structured native generation now adapts verified image parts before serializing
the canonical request. Message headers contain explicit `attachment:N`
references, and codec bytes remain borrowed through the synchronous invocation.
Every occurrence gets a distinct reference; repeated occurrences can borrow the
same source buffer. Tools, tool history, response format, sampling parameters,
token limits, usage and completion handling use the existing canonical builder
and generation handler. High-detail parts, unmatched URLs and unrelated media
retain their existing representations. Ineligible requests allocate no adapter
headers; matching copied URLs still require an exact comparison. Source
materialization, authorization and revision verification remain required.

The internal provider operation is `generate_json_with_attachments`, advertised
by the optional `structured_attachments` capability. Older linked runtimes use
the existing JSON route. In-process calls borrow payloads; worker RPC sends
scatter/gather segments and the receiver owns one admitted payload slab.
Descriptor counts are checked before header allocation/reference validation, and
borrowed views use the same MIME, nonempty payload and aggregate limits as
framed requests. The shared generation handler retains model/pixel/token
admission, cancellation, explicit deadlines and document-store access. Borrowed
calls complete synchronously; ordinary HTTP streaming is unchanged.

Worker protocol **7** includes this operation. The existing exact startup
handshake rejects mismatched workers before processing provider requests.
Applications that explicitly set `ANTFLY_INFERENCE_WORKER` must supply a CLI with
the matching worker protocol when updating the native library. The public HTTP
API, document schema and storage format are unchanged.

### Physical-read qualification

An engine-cold fixture reopens persisted storage, disables application block
caches and counts storage API reads after reopening. It compares full-value
reads with a 32-byte selected copy using the same input. Byte counts include the
first query's index/data reads. These are MemoryStorage traffic measurements;
they do not measure device traffic or an operating-system cold cache.

| Value | Compression / contents | Full storage bytes | Selected storage bytes | Reads per variant |
|---|---|---:|---:|---:|
| 32 KiB | none / repeated ASCII | 33,046 | 33,046 | 3 |
| 1 MiB | none / repeated ASCII | 1,048,854 | 1,048,854 | 3 |
| 32 KiB | adaptive Snappy / repeated ASCII | 1,819 | 1,819 | 3 |
| 1 MiB | adaptive Snappy / repeated ASCII | 49,434 | 49,434 | 3 |
| 1 MiB | adaptive Snappy / high entropy | 1,048,854 | 1,048,854 | 3 |

Selecting fewer output bytes currently reduces retained output, while block
checksums and compression still require complete physical blocks. No sparse
read claim follows from the selected-copy capability. Bypassing block integrity
checks would weaken the storage contract. A future physical-range mechanism
needs independently verified field locations/content under the pinned source
authority, bounded decompression and workload evidence. Any persistent layout
migration remains separate from this follow-up.

Reproduce the native contracts with:

```sh
cd zig
zig build query-contract-test logical-field-query-test antfly-inference-host-test \
  antfly-inference-gateway-test antfly-system-catalog-api-test \
  runtime-error-identities-test -j2
zig build bench-native-generation-media -Doptimize=ReleaseFast -Dstrip=true -j2
```

The new transport tool measures request construction and received metadata
parsing for framed versus borrowed structured generation. Both variants start
with the same verified codec, source data URL and generation options. Seven
alternating rounds of 64 iterations cover 64-byte, 256 KiB and 1 MiB inputs,
check delivered bytes/references/options, and verify all measured allocations
are freed. Source authorization, codec/source materialization, caller-retained
inputs, fixed runtime dispatch and model execution are outside its scope.
The tool constructs codec-sized synthetic payloads and preverified views; it
does not decode/validate an image or qualify a model. Its timings therefore
cannot be compared directly with the earlier encoder-only benchmark or
described as inference/provider-billing savings.

### Release transport measurements and artifact identity

The final tool uses pinned Zig 0.17.0 with `ReleaseFast`, stripping and two
build jobs on a shared Apple M4 Max (14 logical CPUs, 36 GiB RAM), macOS 27.0.1.
These are median elapsed microseconds per invocation from seven alternating
rounds, with 64 invocations per round. Allocation counts/bytes cover request
construction, adaptation/framing and received metadata parsing; the identical
caller-owned codec and source data URL are retained outside both variants.

| Codec bytes | Framed µs/op | Borrowed µs/op | Allocated bytes/op framed → borrowed | Allocations/op framed → borrowed |
|---|---:|---:|---:|---:|
| 64 | 4.587 | 1.878 | 15,625 → 9,913 | 32 → 31 |
| 262,144 | 862.424 | 2.096 | 2,111,257 → 9,913 | 31 → 31 |
| 1,048,576 | 3,650.948 | 2.168 | 8,402,713 → 9,913 | 31 → 31 |

For the 1 MiB input, measured allocation volume falls by 99.88%. This stage
removes payload-sized JSON construction and contiguous-frame copies when the
verified view is reusable. Metadata still allocates, copied URLs still need
comparison, source materialization still constructs/verifies its inputs, and
worker transfers still copy at the process boundary. Model execution and fixed
runtime dispatch are excluded from this tool. Whole-query and provider-cost
conclusions require their own measurements.

The server baseline is the prior PR runtime plus the same main merge. Release
server SHA-256:

- Baseline, 234,755,720 bytes:
  `3acb3c28909cb4d5fe777a34705d57cc82394aea7b19d2c61a73b2dfe5a9a5b9`.
- Candidate, 234,822,248 bytes:
  `56a6f5067ef03c3bcad71aeb6f80f9731ff87769fb80b2524da1f78833e8bbe0`.

The candidate grows by 66,528 bytes (about 0.03%). The fresh standalone CLI is
7,596,584 bytes, SHA-256
`4a3e8a1559f839a1b7faac29b76778299da592b911a7bedcb75f35e8b1d9d87d`.
The measured tool source SHA-256 is
`6de49d8d56fa4ae843254b6f9e5673c39054498da50a9ade981612a1c32c6c15`;
its executable SHA-256 is
`366c64a5e332adeeb3aa3deafc2fc64b034fb9a7065a5c277f4bb060b06e192b`.
The ignored qualification manifest retains the exact 17 production-source
hashes, driver hashes, versions and per-run evidence. Raw reports, logs and
executables are not committed.

Native validation includes the full 98-step pass and final 67-step affected
suite: 213 query contracts, 94 logical cases (one optional skip), 32 provider,
73 gateway and 25 host cases across the corresponding owners. Seven standalone
envelope tests and the 2,332-file Apache source-boundary check pass. One existing
two-second hydration-lease fixture expired under concurrent compilation; it
passed the earlier full pass and the unchanged-source repeat. Real-server
indexed integration passes 57 cases, including persisted upgrade/rollback and
permission checks; seven optional profiling cases are skipped unless their
report environment variables are set.


### Whole-query controls for the controlled-batch follow-up

The paired server hashes above were measured over five alternating rounds with
50 queries per phase (100 for the 64-document batch cases). The following table
retains all 44 controls, including ingestion, ordinary full-value paths and
regressions. CPU/elapsed figures are milliseconds per request; RSS is sampled
process peak MiB and is neither retained allocation volume nor a memory bound.
The host is shared and the filesystem/storage caches are warm after ingestion.
Source controls use ASCII, escaped, tiny and wide metadata shapes; `wide-large`
requests 60 metadata outputs with 1 MiB values, `wide-small` uses 64 KiB values,
and `small64` uses the tiny-value fixture with 60 metadata outputs. The batch
controls select metadata across 64 documents with scalar or nested metadata.
Source/batch controls compare exact returned values, IDs, computed outputs,
scores and totals after sorting hits by ID; they do not qualify result order.
Media controls verify source materialization reaches the expected `ModelRequired`
gate without resolving or calling a model.

These server controls exercise projection/materialization without model
inference. They do not exercise the new structured native-generation invocation
measured by the separate transport tool. Media materialization is mostly flat;
the transport-stage reduction must not be substituted for those whole-query
measurements. CPU, elapsed time and process RSS vary independently.

Source projections:
| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| escaped / ingest | 146.851 → 157.636 | 168.056 → 240.231 | 176.422 → 173.578 |
| escaped / ordinary | 9.262 → 9.166 | 12.627 → 12.677 | 145.531 → 133.320 |
| escaped / full | 16.982 → 17.246 | 19.834 → 20.304 | 99.188 → 96.078 |
| escaped / reference_full | 32.157 → 32.755 | 35.060 → 36.063 | 108.922 → 106.352 |
| escaped / conditional | 1.056 → 1.058 | 2.129 → 2.261 | 93.062 → 96.156 |
| escaped / reference | 16.670 → 17.139 | 17.782 → 18.439 | 93.188 → 95.516 |
| large / ingest | 181.479 → 187.713 | 284.960 → 283.428 | 254.344 → 254.094 |
| large / ordinary | 17.364 → 17.702 | 25.683 → 26.142 | 362.000 → 371.508 |
| large / full | 22.686 → 23.325 | 30.588 → 31.345 | 496.188 → 461.805 |
| large / reference_full | 36.710 → 36.270 | 45.566 → 45.142 | 465.188 → 536.867 |
| large / conditional | 1.446 → 1.514 | 3.022 → 3.217 | 443.289 → 393.188 |
| large / reference | 17.538 → 17.929 | 20.078 → 20.653 | 386.664 → 348.047 |
| small64 / ingest | 31.223 → 27.486 | 18.516 → 17.226 | 132.109 → 131.875 |
| small64 / ordinary | 0.600 → 0.633 | 1.528 → 1.721 | 135.703 → 135.719 |
| small64 / full | 2.050 → 1.915 | 3.218 → 3.333 | 144.422 → 140.312 |
| small64 / reference_full | 2.061 → 1.871 | 3.325 → 3.016 | 139.766 → 140.094 |
| small64 / conditional | 2.121 → 2.054 | 3.293 → 3.636 | 145.594 → 146.102 |
| small64 / reference | 2.092 → 2.204 | 3.269 → 3.799 | 145.930 → 135.359 |
| wide-large / ingest | 178.304 → 186.654 | 184.443 → 245.917 | 254.438 → 248.688 |
| wide-large / ordinary | 16.695 → 16.715 | 23.087 → 22.823 | 367.281 → 347.328 |
| wide-large / full | 23.873 → 23.940 | 30.419 → 30.287 | 518.523 → 471.469 |
| wide-large / reference_full | 37.233 → 36.761 | 44.510 → 43.233 | 496.539 → 457.586 |
| wide-large / conditional | 2.497 → 2.992 | 3.915 → 4.999 | 429.672 → 391.906 |
| wide-large / reference | 18.417 → 19.107 | 19.895 → 21.181 | 371.797 → 331.023 |
| wide-small / ingest | 46.702 → 45.273 | 32.631 → 28.583 | 142.531 → 146.828 |
| wide-small / ordinary | 1.670 → 1.595 | 3.266 → 3.092 | 153.344 → 151.312 |
| wide-small / full | 3.383 → 3.259 | 5.222 → 4.798 | 161.641 → 162.516 |
| wide-small / reference_full | 4.395 → 4.218 | 6.561 → 6.048 | 156.266 → 156.797 |
| wide-small / conditional | 2.261 → 2.075 | 3.729 → 3.137 | 161.781 → 162.719 |
| wide-small / reference | 3.243 → 3.031 | 4.591 → 4.129 | 153.734 → 163.078 |

Batch projections:
| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| nested-large / read | 4.457 → 4.674 | 5.804 → 6.294 | 446.164 → 507.891 |
| nested-medium / read | 2.359 → 2.331 | 3.189 → 3.136 | 180.609 → 179.594 |
| nested-small / read | 2.260 → 2.201 | 3.064 → 2.949 | 166.047 → 166.141 |
| scalar-large / read | 4.032 → 3.845 | 5.497 → 4.987 | 421.000 → 413.766 |
| scalar-small / read | 2.905 → 3.001 | 4.823 → 5.034 | 150.719 → 150.062 |

Media materialization:
| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| large_distinct_unique / read | 13.413 → 13.132 | 6.102 → 6.162 | 214.688 → 199.062 |
| large_distinct_unique_eight / read | 29.378 → 29.368 | 12.992 → 13.217 | 252.094 → 254.672 |
| skewed_distinct_eight / read | 18.056 → 18.185 | 15.542 → 16.108 | 220.781 → 206.008 |
| large_512_distinct_unique_eight / read | 100.245 → 102.518 | 62.579 → 63.970 | 272.594 → 275.828 |
| large_shared_duplicates / read | 11.102 → 10.917 | 12.216 → 11.830 | 167.133 → 191.828 |
| large_distinct_repeated_refs / read | 12.349 → 12.528 | 7.940 → 8.242 | 207.312 → 205.938 |
| large_512_distinct_repeated_refs / read | 45.709 → 45.911 | 45.659 → 46.062 | 319.906 → 317.562 |
| tiny_shared_duplicates / read | 0.299 → 0.292 | 0.826 → 0.768 | 154.516 → 154.500 |
| medium_shared_duplicates / read | 1.007 → 1.077 | 1.598 → 1.745 | 158.141 → 158.234 |


### Independent repeated source controls

The wide 1 MiB, tiny/wide and escaped workloads were repeated with seven
alternating rounds and 100 queries per phase using the same immutable binary
hashes. These retain both improved and slower cases. Tiny-value ordinary/full reads improve in the repeat; reference elapsed remains
1.7% higher. The escaped ingestion surge does not reproduce. Wide conditional
CPU/elapsed remain about 7.1%/14.6% higher
(initially 19.8%/27.7%). That residual is recorded, not hidden by averaging other
phases into a single speedup. Allocation reduction alone does not establish
lower whole-query CPU or latency. Shared-host background work and sampled RSS
also prevent universal latency or footprint conclusions.

| Workload/phase | CPU ms baseline → candidate | Elapsed ms baseline → candidate | RSS MiB baseline → candidate |
|---|---:|---:|---:|
| wide-large / ingest | 176.626 → 165.608 | 205.122 → 190.613 | 275.938 → 276.969 |
| wide-large / ordinary | 15.689 → 15.722 | 20.938 → 21.393 | 497.406 → 539.914 |
| wide-large / full | 22.691 → 22.378 | 28.633 → 28.032 | 631.172 → 641.047 |
| wide-large / reference_full | 34.948 → 34.449 | 41.301 → 39.345 | 616.938 → 637.625 |
| wide-large / conditional | 2.327 → 2.492 | 3.470 → 3.975 | 569.875 → 626.516 |
| wide-large / reference | 17.458 → 17.789 | 18.723 → 19.443 | 413.312 → 619.531 |
| small64 / ingest | 25.872 → 26.667 | 14.330 → 15.441 | 131.688 → 131.719 |
| small64 / ordinary | 0.546 → 0.511 | 1.409 → 1.334 | 135.781 → 135.797 |
| small64 / full | 1.803 → 1.758 | 2.788 → 2.651 | 142.305 → 140.266 |
| small64 / reference_full | 1.912 → 1.841 | 3.039 → 2.856 | 139.812 → 140.016 |
| small64 / conditional | 1.872 → 1.850 | 2.823 → 2.775 | 91.922 → 123.062 |
| small64 / reference | 1.930 → 1.918 | 2.855 → 2.903 | 66.609 → 123.281 |
| escaped / ingest | 149.326 → 149.024 | 153.738 → 159.535 | 177.781 → 180.781 |
| escaped / ordinary | 9.249 → 9.380 | 12.493 → 13.007 | 109.539 → 134.359 |
| escaped / full | 17.783 → 17.419 | 21.314 → 20.559 | 85.594 → 94.625 |
| escaped / reference_full | 33.054 → 32.162 | 37.069 → 35.259 | 98.125 → 108.453 |
| escaped / conditional | 1.255 → 1.311 | 2.727 → 2.766 | 85.969 → 85.469 |
| escaped / reference | 17.418 → 17.617 | 19.196 → 19.402 | 81.125 → 84.922 |

Representative paired commands (set `CANDIDATE` and `BASELINE` to the captured
release executables; keep raw output under ignored `output/`):

```sh
uv run --project zig/e2e/antfly python zig/e2e/antfly/bench_source_projection.py \
  "$CANDIDATE" --baseline "$BASELINE" --rounds 7 --queries-per-phase 100 \
  --workload ascii --value-bytes 1048576 --metadata-outputs 60 \
  --output output/source-wide-large.json
uv run --project zig/e2e/antfly python zig/e2e/antfly/bench_projection_batches.py \
  "$CANDIDATE" --baseline "$BASELINE" --rounds 5 --queries-per-phase 100 \
  --documents 64 --metadata-shape nested --value-bytes 1048576 \
  --output output/batches-nested-large.json
uv run --project zig/e2e/antfly python zig/e2e/antfly/bench_media_materialization.py \
  "$CANDIDATE" --baseline "$BASELINE" --rounds 5 --queries-per-phase 50 \
  --workload large_distinct_unique_eight --output output/media-distinct.json
```


### Integration after the latest main merge

The branch subsequently merged fetched `origin/main` through `1d7ac5468b`
(source commit `8bf0a8ae40`). All 17 source hashes from the paired
qualification remain unchanged; the main merge adds separately validated
upstream changes. Post-merge native validation passes 98/98 build steps and
204/204 standard-runner tests, plus 213 query contracts, 94 logical cases
(one optional skip), 49 runtime ABI, 32 provider, 73 gateway and 24 catalog
source-owner cases. The host target includes 25 native fixtures. There are no
failed or leaked tests in this pass. The earlier hydration-lease timing failure
does not recur.

The stripped ReleaseFast server builds 71/71 steps; the standalone CLI
build also passes. Real-server indexed integration then passes 57 cases with
7 optional profiling skips using these captured executables:

- `antfly`, 235,402,360 bytes, SHA-256
  `18c7b4b001d466af3848415f9e70c91c372b3809906548e6a3fcf256405944e0`.
- `antfly-grep`, 7,596,584 bytes, SHA-256
  `4a3e8a1559f839a1b7faac29b76778299da592b911a7bedcb75f35e8b1d9d87d`.

These post-merge executables qualify integration and are distinct from the
same-main paired benchmark artifacts above; their sizes/timings must not be
attributed solely to this follow-up. License headers, the 2,334-file Apache
boundary, storage-control catalog and formatting checks pass. The ignored
`batch2-postmerge-manifest.json` retains the final artifact/source identities.
Raw qualification files stay outside the tracked source tree. Full GitHub PR
CI still requires the repository maintainer approval; these local checks do
not bypass that gate.


## Bounded source codecs and shared worker payloads (2026-10-30)

The next follow-up separates logical evidence from its transient transport
representation. This changes neither public query/inference syntax nor the
ASCH25 document format. Ordinary authorization, membership, revision, source
identity, SHA-256, MIME and pixel checks still precede inference and cache reuse.

### Admission and compatibility

- Binary generation, for both typed messages and canonical structured JSON,
  preserves the former 16 MiB HTTP attachment metadata ceiling. Raster/provider operations retain their 1 MiB metadata
  ceiling. Worker admission checks the actual complete 64 MiB envelope, rather
  than reserving the canonical worst-case metadata size for every call.
- An explicit request context without a deadline remains unbounded. A missing
  compatibility context retains the historical five-minute default; an explicit
  absolute deadline is never extended.
- Copy-backed projection batches check whether the first two physical values
  fit before opening a scope or constructing Selection plans. Unknown capability
  metadata defers admission until the actual scope mode is available, so genuine
  selected reads can still batch small spans from large physical documents.
  The existing 64-row, 1 MiB and 256-range bounds remain in force.

### Ownership and representation

Compatible native routes use a trusted producer callback that decodes and verifies
source data URLs while query views are borrowed. The invocation retains codec
buffers plus small content-addressed handles; it does not retain a second expanded
URL. Independently authorized references share a codec only after verification.
Their individual revision, permission, MIME, digest and pixel claims still apply.
The table of codecs is allocated lazily, and completed rolling jobs release parse
scratch before model execution.

The existing 16 MiB expanded source-request limit bounds compact codec retention
at 12 MiB, inside the shared 64 MiB gateway allocation budget. Legacy reuse hints
that supplement a retained URL keep their smaller 1 MiB soft limit. Distinct
locator/options claims do not charge the same verified codec repeatedly.
The rolling four-job/8 MiB scratch admission policy remains in place; an oversized
individual source runs alone with the established bounded response ceiling.

Internal handles cannot be submitted as public image URLs. They are usable only
with matching invocation-owned, verified codec sidecars. Logical evidence size,
per-image token reservations and decoded-pixel admission do not shrink with the
handle. Stable verified content identities preserve equivalent-request cache
reuse after upgrade; previous URL-based entries naturally miss. Cancellation
still spans source resolution and inference. Hosted/legacy consumers render data
URLs lazily; route changes cannot send an internal handle to an unaware adapter.
Only actual message image parts are rewritten, preserving text, tools, options
and opaque number spellings. Resolved source parts in chat messages normalize to
the existing `image_url` object shape, including nested detail; decision evidence
keeps its existing `input_image` shape.

Worker protocol **8** adds a unique physical payload table and a logical-slot
mapping. Repeated immutable buffer views with the same MIME cross the process
boundary once; the receiver expands descriptor headers that alias those bytes.
Message indices, per-reference provenance and logical model admission remain
intact. Invalid maps, unused payloads and out-of-range references are rejected.
Identity maps are omitted and singleton calls avoid the interning hash table.
Explicitly configured worker executables must match the exact protocol version.
The existing native C attachment boundary and capability remain unchanged.

The legacy/public HTTP framing fallback keeps its existing one-attachment-per-
occurrence contract and bounded copies. Physical sharing belongs to the private
worker provider transport. Source verification, storage block reads, model tensor
preparation and inference still perform real work; these changes do not make
search or model execution zero cost.

### Qualification

Use separate attribution runs with `bench_source_projection.py
--profile-source-batch`; its admission/planning counters identify fast-path use
and fallback without attributing unrelated wide-row regressions to batching.
Normal paired timing runs leave this logging disabled. The opt-in native source
preparation fixture uses real PNG codecs, multiple sizes and repeated references;
its scope includes producer ownership/verification and normalization JSON, and
excludes storage, fixed provider dispatch and model execution. The worker fixture
measures real encoded frame bytes, receiver aliasing and allocated bytes.
Raw results and binary/source manifests remain under ignored `output/`.


Optimized qualification on the shared ARM64 macOS host passes the real source
preparation fixture (5/5) and private worker transport fixture (9/9). The
source fixture records the following allocation volumes per operation; these
are allocated bytes, not peak RSS or allocation counts:

| PNG codec bytes | Logical references | URL/evidence path | Codec/handle path |
|---|---:|---:|---:|
| 1,108 | 1 | 11,274 B | 3,708 B |
| 65,835 | 1 | 626,164 B | 68,435 B |
| 591,250 | 1 | 5,617,622 B | 593,850 B |
| 1,050,904 | 1 | 9,984,336 B | 1,053,504 B |
| 1,050,904 | 8 | 32,055,066 B | 1,055,306 B |

Across all four sizes and 1/2/8-reference cases, allocation volume falls
67–97%. Each case uses seven alternating rounds with sixteen operations per
sample. This fixture includes actual producer ownership/verification and
representative decision evidence JSON construction. It does not measure an
entire gateway/native canonical invocation, storage, IPC or model execution;
the former native generation route already adapted small verified images
before canonical serialization. No whole-inference speedup follows from these
isolated results.

The actual worker encoder/parser carries eight shared 1 MiB logical payloads
in 1,049,538 bytes, compared with 8,389,711 bytes for eight independently backed
payloads. Sixteen-operation allocation volume is 25,333,248 versus 201,487,744
bytes. The independent control contains different bytes, so this is a physical
scaling and ownership comparison, not a same-input CPU or model-speed claim.
Logical references and model admission remain unchanged.

Portable worker fixtures also pass inside the available Linux VM: eight
passes/one opt-in skip for worker framing and two passes for evidence. These
are static x86_64-linux-musl ReleaseFast artifacts, executed in isolated,
network-disabled, read-only Docker Desktop containers on the ARM64 Mac. This
qualifies those contracts under emulation, not native Linux performance or full
Linux server conformance. The immutable fixture and source manifests are
`round3-micro-report.json` and `round3-linux-manifest.json` under ignored
`output/projection-followup/`.


Reproduce the whole-server controls with the checked-in drivers, using immutable
ReleaseFast/stripped binaries built with the same pinned Zig and merged main:

```sh
python zig/e2e/antfly/bench_source_projection.py CANDIDATE --baseline BASELINE --rounds 5 --queries-per-phase 50 --workload ascii --value-bytes 262144 --output output/source-eligible.json
python zig/e2e/antfly/bench_source_projection.py CANDIDATE --baseline BASELINE --rounds 5 --queries-per-phase 50 --workload ascii --value-bytes 786432 --metadata-outputs 60 --output output/source-pair-rejected.json
python zig/e2e/antfly/bench_source_projection.py CANDIDATE --baseline BASELINE --rounds 1 --queries-per-phase 2 --workload ascii --value-bytes 786432 --metadata-outputs 60 --profile-source-batch --output output/source-attribution.json
python zig/e2e/antfly/bench_projection_batches.py CANDIDATE --baseline BASELINE --rounds 5 --queries-per-phase 50 --metadata-shape scalar --value-bytes 16384 --documents 64 --output output/batches-scalar.json
python zig/e2e/antfly/bench_media_materialization.py CANDIDATE --baseline BASELINE --rounds 5 --queries-per-phase 50 --workload tiny_shared_duplicates --output output/media-control.json
```

The source matrix additionally covers one-MiB wide values, small wide rows and
escaped content. Projection controls include one-MiB scalar and 256-KiB nested
metadata; media controls include eight distinct large images. Drivers alternate
baseline/candidate order, assert response equivalence, record binary hashes and
capture CPU, elapsed and sampled RSS. Admission counters belong only to separate
attribution runs. Native fixtures are reachable through the existing
`antfly-api-test`, `antfly-inference-host-test`, `lib-decisions-test` and
`lib-structured-test` owners; `ANTFLY_BENCH_SOURCE_MEDIA=1` enables the opt-in
codec/worker benchmark fixtures. The captured optimized compiler commands and
per-sample results remain beside the ignored manifests.


The source-frozen follow-up passes **98/98 native build steps** with 246 standard
runner passes and one opt-in skip, plus the source-owner suites (213 query,
94 logical/one opt-in skip, 49 runtime, 33 provider, 77 gateway/one opt-in skip,
and 24 catalog contracts). Host ABI/transport coverage includes 25 fixtures.
The stripped ReleaseFast server passes **71/71 build steps**, the standalone
grep target **7/7**, and real-server indexed HTTP integration **57 passes with
7 opt-in profiling skips**. There are no failed or leaked native cases. License
headers, the 2,340-file Apache boundary, storage-control catalog and formatting
checks pass.

The paired executables include main `51864a57b0562353f8ce8db8889cf13738504367`
merged at `d2e1fd944f12004255bb508d4865cfb8a6960ccc`; the baseline was captured
before applying this follow-up and the candidate from the source-frozen changes
on that same merge. Both use the pinned Zig 0.17.0, ReleaseFast, stripping and
`-j2` on the shared M4 Max macOS host. The candidate used an isolated cache inside
the worktree after the ordinary active cache disappeared during compilation;
that failed attempt is not a qualification run. No production source changed
during the successful native, optimized fixture, release or HTTP checks.

- Baseline server SHA-256: `7ef6e7fea41908eedcd01416b297f10ca566366f116c251c00e84218e2bea395` (235,634,920 bytes).
- Candidate server SHA-256: `f7291e7222f0296a6526342a8e66bd96844794a50e7cc31e5553176c92f14807` (235,702,616 bytes).
- Candidate grep SHA-256: `e8f7f1ec76751de7cf1fc9157cc9d50a0d2ce41e795d9dfb32d8c18b28caaf25` (7,596,584 bytes).
- Pinned compiler SHA-256: `1c5f706db0ed6d55451940f31dc05a6177d05696bc1fa5d853696d04c718b523`.

The server grows by 67,696 bytes
(0.029%). Binary identities,
all 23 source/driver hashes and compiler flags are retained in the ignored
`round3-baseline-manifest.json` and `round3-candidate-manifest.json`. These do not
claim native Linux/cold-storage performance or full GitHub CI approval.


### Same-main whole-server controls

All 35 initial warm synthetic controls pass response-equivalence checks, with
five alternating baseline/candidate rounds and fifty reads per phase. Ingestion
is measured once per fresh server; read rows summarize individual operations.
CPU/elapsed values are milliseconds. Sampled RSS in MiB includes process state
and prior phases and is not an allocation bound. These controls are separate
from codec/worker microfixtures; legacy media controls deliberately do not
establish the speed of native typed source execution.

#### Source projections

| Case / phase | CPU ms before → after | Elapsed ms before → after | RSS MiB before → after |
|---|---:|---:|---:|
| eligible / ingest | 57.970 → 59.139 | 41.535 → 41.445 | 182.547 → 182.422 |
| eligible / ordinary | 3.928 → 3.773 | 5.565 → 5.352 | 186.734 → 186.266 |
| eligible / full | 5.014 → 5.067 | 6.698 → 6.651 | 196.078 → 195.125 |
| eligible / reference_full | 7.985 → 7.921 | 9.524 → 9.452 | 195.344 → 193.266 |
| eligible / conditional | 0.778 → 0.749 | 1.442 → 1.389 | 196.672 → 196.922 |
| eligible / reference | 4.388 → 4.433 | 5.111 → 5.158 | 196.703 → 196.938 |
| escaped / ingest | 131.001 → 130.823 | 113.534 → 113.910 | 199.000 → 199.641 |
| escaped / ordinary | 7.991 → 8.051 | 9.770 → 9.749 | 209.023 → 208.578 |
| escaped / full | 16.742 → 16.057 | 19.109 → 17.586 | 216.422 → 215.922 |
| escaped / reference_full | 29.515 → 29.354 | 30.625 → 30.381 | 218.500 → 215.859 |
| escaped / conditional | 1.110 → 0.798 | 2.359 → 1.465 | 216.516 → 216.172 |
| escaped / reference | 16.476 → 15.672 | 18.083 → 16.088 | 216.531 → 216.188 |
| pair-rejected / ingest | 122.161 → 125.460 | 110.642 → 121.805 | 245.266 → 245.141 |
| pair-rejected / ordinary | 10.613 → 10.449 | 14.069 → 13.780 | 403.531 → 402.836 |
| pair-rejected / full | 15.230 → 15.696 | 18.319 → 18.985 | 600.172 → 599.906 |
| pair-rejected / reference_full | 24.347 → 24.900 | 27.240 → 27.705 | 597.141 → 594.578 |
| pair-rejected / conditional | 1.977 → 2.126 | 2.739 → 3.037 | 596.875 → 594.047 |
| pair-rejected / reference | 12.612 → 12.819 | 13.209 → 13.480 | 596.875 → 594.250 |
| wide-large / ingest | 154.337 → 158.657 | 153.578 → 190.677 | 278.406 → 275.406 |
| wide-large / ordinary | 14.453 → 14.750 | 19.046 → 19.249 | 481.008 → 460.695 |
| wide-large / full | 20.563 → 20.409 | 24.949 → 24.607 | 643.500 → 652.297 |
| wide-large / reference_full | 32.590 → 32.495 | 36.403 → 36.350 | 642.766 → 647.281 |
| wide-large / conditional | 2.158 → 2.092 | 3.013 → 2.924 | 636.062 → 644.844 |
| wide-large / reference | 16.300 → 16.335 | 16.831 → 16.791 | 638.141 → 637.594 |
| wide-small / ingest | 22.630 → 23.022 | 12.949 → 13.251 | 152.094 → 152.219 |
| wide-small / ordinary | 0.382 → 0.351 | 0.920 → 0.875 | 155.562 → 155.844 |
| wide-small / full | 1.644 → 1.650 | 2.373 → 2.401 | 159.547 → 159.812 |
| wide-small / reference_full | 1.743 → 1.674 | 2.527 → 2.385 | 158.992 → 159.234 |
| wide-small / conditional | 1.680 → 1.733 | 2.396 → 2.482 | 159.781 → 160.016 |
| wide-small / reference | 1.716 → 1.753 | 2.423 → 2.486 | 160.031 → 160.094 |

#### Projection batch controls

| Case / phase | CPU ms before → after | Elapsed ms before → after | RSS MiB before → after |
|---|---:|---:|---:|
| nested-medium / read | 3.067 → 3.568 | 4.557 → 5.901 | 209.508 → 202.477 |
| scalar-large / read | 4.334 → 4.508 | 6.003 → 6.108 | 442.547 → 367.555 |
| scalar-small / read | 2.100 → 2.077 | 3.096 → 2.955 | 165.961 → 165.234 |

#### Legacy media controls

| Case / phase | CPU ms before → after | Elapsed ms before → after | RSS MiB before → after |
|---|---:|---:|---:|
| large_distinct_unique_eight / read | 28.754 → 27.526 | 14.312 → 15.245 | 257.969 → 242.109 |
| tiny_shared_duplicates / read | 0.572 → 0.604 | 2.068 → 1.986 | 155.297 → 158.594 |


Separate one-round/two-query attribution (excluded from those timings) confirms
eligible batching: 24 attempts, 24 admitted batches, 64 loaded/planned rows.
The rejected-pair control records 56 attempts, 56 preflight rejections, zero
admitted rows and **zero planned rows**. The pre-existing baseline has no new
counter fields; its empty counter list is not evidence of zero work. Candidate
counters confirm fast-path/fallback behavior directly.

The initial pair-rejected conditional read and nested metadata read are slower,
and wide large-row ingestion has a higher elapsed median. All three controls
are repeated independently with seven alternating rounds and one hundred reads
per phase; initial rows remain recorded above rather than being replaced by
selected repeats.


### Independent whole-server repeats

Seven alternating rounds, one hundred reads per phase. Same immutable binaries and corpus contracts as the initial matrix; response checks pass. Ingestion remains one measurement per fresh server.

| Case / phase | CPU ms before → after | Elapsed ms before → after | RSS MiB before → after |
|---|---:|---:|---:|
| pair-rejected / ingest | 138.973 → 138.815 | 156.127 → 152.682 | 245.031 → 229.016 |
| pair-rejected / ordinary | 11.843 → 11.806 | 15.659 → 15.993 | 423.336 → 396.336 |
| pair-rejected / full | 17.437 → 16.477 | 21.785 → 20.233 | 457.297 → 473.227 |
| pair-rejected / reference_full | 26.392 → 26.309 | 30.440 → 29.903 | 465.312 → 476.250 |
| pair-rejected / conditional | 2.362 → 2.197 | 3.700 → 3.265 | 433.969 → 484.953 |
| pair-rejected / reference | 13.442 → 13.446 | 14.297 → 14.715 | 241.234 → 479.641 |
| nested-medium / read | 2.666 → 2.635 | 3.547 → 3.481 | 235.641 → 232.391 |
| wide-large / ingest | 152.206 → 152.984 | 148.343 → 152.493 | 276.297 → 277.922 |
| wide-large / ordinary | 14.591 → 14.088 | 18.750 → 18.100 | 647.680 → 685.328 |
| wide-large / full | 20.122 → 20.129 | 24.128 → 23.945 | 747.000 → 747.953 |
| wide-large / reference_full | 32.579 → 31.854 | 36.174 → 35.157 | 742.859 → 743.016 |
| wide-large / conditional | 2.077 → 2.084 | 2.865 → 2.909 | 739.891 → 740.078 |
| wide-large / reference | 16.204 → 16.095 | 16.684 → 16.572 | 733.328 → 733.094 |


The longer repeats reverse the initial read regressions: rejected-pair
conditional CPU/elapsed fall 7.0%/11.8%; nested metadata falls 1.2%/1.9%.
Rejected-pair ordinary/reference elapsed remains 2.1%/2.9% higher, and wide
large-row ingestion is 0.5% higher CPU/2.8% higher elapsed. Other repeated reads
are mixed or close to flat. The allocation/wire reductions are structural;
these shared-host whole-server timings do not establish universal speed or RSS
improvements. After qualification, Ruff only wrapped two blocks in the driver;
an AST-equivalence check records that formatting changed no behavior. Native
source identities remain unchanged.
