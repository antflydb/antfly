# Native Antfly Memory

This is the long-term design for Antfly's first-party `/memory/v1` service.
The native implementation lives in `pkg/antfly-embedded/src/memory/`; HTTP,
MCP and embedded bindings adapt this policy rather than owning a second one.
There is no supported Rust extension or Go memory server backend.

Design provenance: [feature request #1211](https://github.com/antflydb/antfly/issues/1211).
This document preserves the evidence and intended acceptance criteria below.
Research claims are evidence for design choices, not measured product claims.

## Native contracts and storage ownership

`types.zig` is the wire-contract source. `schema.zig` derives both strict
constrained-producer schemas and request schemas with the same native defaults.
`scripts/generate_memory_openapi.py` emits the public API source; joined OpenAPI
and SDK artifacts must be regenerated when these contracts change.

A space maps to a catalog-authorized `memory_<space>` table. The table's catalog
identity owns its database/namespace binding and lifetime. Space names are
single path components, never arbitrary physical storage keys. Each mutation
uses a control-row version/digest predicate and atomically publishes revision
history, operation receipts, source fences and dependency work with its data.
The binding is checked at ingress. Renaming an underlying catalog table is not
an implicit memory-space rename; a mismatched binding fails explicitly.
Storage adapters must route through existing table transactions or the native
embedded DB; memory owns no additional storage engine or transaction manager.

Remote spaces require native catalog support. Their catalog resources carry an
immutable `native_service: memory` ownership binding, captured with both point
identity and batched query definitions. The shared catalog data-admission boundary
rejects ordinary HTTP, MCP, SQL, join and graph access to that storage, even for a
caller with table grants. Only memory's in-process service capability admits its
backing table. Public request bodies and headers cannot mint that capability.
Generic catalog mutation cannot rename, drop or remove service ownership; lifetime
operations belong to the service. Embedded raw access remains the database owner's
authority, rather than an authentication boundary between local principals.

Native provisioning uses a distinct body-bound administrative metadata call and
requires topology protocol 38; old metadata must reject it before private data is
published. Upgrade every public ingress and metadata peer before enabling spaces.
The private catalog protocol preserves ownership across restart and projections;
unsupported catalog providers fail closed instead of creating an ordinary table.

The embedded JSON envelope is `{action, id?, body?, idempotency_key?, principal?}`.
The database owner is its authority; local `principal` is attribution. Remote
transports derive principals and table grants from Antfly authentication.
No protocol accepts a caller-supplied authorization override. Arbitrary extra
row policies must be carried through all internal reads or rejected closed.

Evidence IDs identify immutable source versions; a source edit has a new evidence
ID. Assertion IDs identify immutable assertions, with lifecycle revisions stored
separately. Physical cleanup retires an assertion ID with a durable tombstone
before removing its payload, including when support was forgotten or expired.
Old receipts may acknowledge prior admission but cannot restore a retired ID;
remaining group and dependency indexes can never resolve to a different assertion.
Validity describes when an assertion holds in the world; recorded
revisions describe when Antfly learned or revised it. Unknown validity never
becomes an update order merely because a document was ingested later.

Provisional/settled stability, factual modality, provenance role and lifecycle
are independent. Identity corrections reconcile the affected canonical belief
groups in the same transaction as redirects. Raw subject/value IDs and explicit
human or producer dispositions remain immutable inputs to that projection; unmerge
rebuilds the separate groups without erasing explicit retractions or supersession.
Missing forgotten replacements never authorize revival. Subject and typed-object
reverse indexes bound the affected set; one atomic curation admits at most 256
assertions, rejecting a larger operation without changing identity or facts.
Ontology and reconciliation-policy transitions validate retained assertions and
rebuild their belief projection in the configuration transaction. Raw subject and
value reverse indexes exist independently of whether the current ontology types
a value, so enabling object typing does not lose old facts during later merges.
These semantic transitions admit at most 256 retained assertions; larger or
incompatible changes reject atomically before publishing a configuration or
starting a vector build. Vector-only changes use bounded asynchronous backfill.
Belief projection dispatches the admitted reconciliation strategy on both ingestion
and rebuild: explicit mode preserves client decisions without automatic disputes;
deterministic mode applies its temporal/authority rules; model mode invokes the
qualified decider on eligible active canonical groups. Model rebuilds preserve
private owner partitions even when an administrator curates another owner's
identities. A failed decider rolls back the whole identity/configuration change.
Typed links connect evidence, decisions, rationale, rejected
alternatives, episodes and procedures. Alias/name matches never authorize a
canonical identity merge. Explicit distinct IDs must remain distinct. Merge
admission checks `distinct_from` constraints on every member of both affected
alias components against the proposed redirect graph, including distinctions
declared on noncanonical members. Explicit alias replacement can move an alias
without moving its old target; it cannot collapse a declared distinct pair.
A rejected merge changes neither redirects nor belief projections.
The original assertion fingerprint and asserted upper validity bound survive
policy-derived interval closure, so source replay cannot overwrite current
lifecycle and an authorized explicit restoration can recover original bounds.
Retraction, TTL expiry and forgetting never implicitly revive superseded guidance;
restoration requires an explicit correction or newly admitted assertion.
Correction dispositions retain the authenticated principal, recorded time and
validated evidence separately from the fact's own support. A team-visible
correction cannot cite private evidence. Forgotten or expired correction support
redacts its reason and provenance references from point reads, history, enumeration
and recall, and physical cleanup scrubs those payloads. The lifecycle decision
survives that redaction; losing justification never implicitly reactivates a fact.
Model decision explanations have separate versioned internal provenance artifacts.
They retain the producing principal/configuration, incoming evidence, exact premise
revisions and previously supplied explanation artifacts, without duplicating model
input text. Each assertion/history revision binds its assertion explanation and
explicit disposition explanation independently. Forgetting or expiry of an input
redacts affected explanations across point reads, history, enumeration and recall;
cleanup scrubs the stored text while preserving the supported fact and accepted
lifecycle decision. New inference receives only authorized, already redacted
incoming and candidate explanations. Rebuilds capture the incoming explanation's
previous immutable artifact before replacing its text, so repeated policy/identity
re-evaluation preserves transitive forgetting fences. This edge refers directly to
the previous artifact, not to an assertion history row rewritten in the same
transaction. Redacted incoming explanations contribute neither text nor an inherited
explanation edge to new inference. A manual correction replaces disposition
provenance without removing the original assertion explanation's fences. Lifecycle
edits inherit bindings; recomputation publishes a new artifact instead of rewriting
old history.
Entity cleanup retains a durable identity tombstone. A retired entity ID cannot
become an untyped raw ID or be reused for a different entity after its payload is
removed; typed identity dependencies remain fenced through cleanup and restart.

Mutations require per-principal idempotency keys and fingerprint the action,
resource ID and exact request body. A conflicting reuse fails. Authenticated,
authorized receipt replay precedes the current space input budget. The global
4-MiB request ceiling applies to every action; `max_input_bytes` additionally
bounds new observations, assertions, corrections, outcomes, entity curation,
staged vectors and recall input. Configuration, erasure, maintenance and
structural reads use the global ceiling, preserving recovery even when a space
has a one-byte ingestion budget. Durable managed
operations expose evidence acceptance independently of assertion readiness,
bounded attempts, errors, retry times and configuration generation. Publication
uses the same revision predicate after inference; a stale result cannot commit.
Model calls use existing qualified Antfly producers and embedders, without
persisting consumer session credentials. Each enabled role is qualified
independently even when one model serves several roles.

Source snapshots are scoped to a native execution. Multi-stage generation receives
a service-owned inference boundary and completes it after **each** provider call,
after restoring embedded storage access. Before forwarding source text or derived
entities to the next stage, that boundary checks live authority, the original
memory control revision, and the entire admitted generation input closure,
including uncited context. Revocation, source drift or concurrent memory mutation
stops the pipeline before a subsequent provider can receive stale input.
Every generation, reconciliation and embedding completion uses the same native
fence: live destination authority, the original memory control revision, then the
ordinary resources in that invocation’s input closure. This also protects plaintext evidence,
assertions and explanation artifacts cached before a lease release: concurrent
forgetting or reconfiguration stops batch ingestion, reconciliation and reindexing
before another provider receives stale memory. The resource closure includes
uncited generation context, shared parents and candidate explanation dependencies. Captures include cached source
reads and rebuild visibility memoization within the invocation. Lease epochs force
inputs not checked since an intervening inference to be revalidated before reuse.
Before committing
inferred changes, it revalidates the complete transaction read set once.
Independent embedding inputs therefore require linear source reads across a batch;
earlier invocations do not inflate each later invocation’s boundary check.
Revoked access rolls back the attempt without spending the durable retry budget;
a changed source cancels
work admitted against that revision. Results never silently adopt a newer source.
These live checks accompany the memory control-row CAS; they do not create a
cross-table serializable transaction with ordinary source writers.

Structural enumeration reads batches of up to 128 rows within its 4096-row work
budget. Its cursor identifies the last consumed row, including filtered rows, so
stopping partway through a fetched batch cannot skip records on the next page.
Structural enumeration paginates the accessible set; ranked recall is budgeted
and reports omitted candidates, scan limits, revisions and actual retrieval
path. A truncated ranking is never a completeness claim. Context and authoritative
lifecycle filters apply before rendering citations. Private provenance cannot
be widened by a team-visible derived assertion. Forgetting erects durable
non-resurrection fences immediately in every source-backed view, including entity
lookup, identity resolution and enumeration; bounded physical cleanup is separate.

## Codex migration contracts (a2d59cfb01)

The implementation follows the [integration review on #1227](https://github.com/antflydb/antfly/pull/1227#issuecomment-6327590919)
and the [pinned Codex integration](https://github.com/antflydb/codex/blob/a2d59cfb01/ANTFLY_INTEGRATION.md).
The native service now owns the migration features below. Moving Codex's CLI,
writers and `knowledge.search` to these contracts remains a consumer cutover in
that repository; application thread storage and approval authority stay there.
There is one native policy implementation for server HTTP, dedicated
`/memory/v1/mcp`, and direct embedded calls.

| Integration behavior | Native contract |
| --- | --- |
| Concept identity screening and external judges | `entity_candidates`, `propose_entity`, `entity_proposals`, `entity_proposal`, `review_entity` |
| Claim-backed concept descriptions | `entity_view`; `POST /spaces/{space}/entities/{id}/view` |
| Repeated concepts in history turns | `observations.mentions`, independent of immutable entity identity |
| Ordered history and related threads | `episodes`, `episode`; `GET /spaces/{space}/episodes[/{id}]` |
| Typed concept links | `ontology_pack: software_team_v1`, external assertions, relationship proposals/verdicts |
| Graph retrieval / related concepts | `recall.graph`, indexed adjacency and personalized PageRank |

All paths in the table are under `/memory/v1`. Model-free defaults remain valid.
The optional `software_team_v1` pack types `is_a`, `part_of` and `alternative_to`
as concept-to-concept predicates. Assertions retain their existing kind,
`motivates` and `rejected_in_favor_of` fields, validity, scope and stability.
Pack changes participate in the same atomic ontology validation and belief
rebuild as explicit predicate changes. The pack's concept relationship typing
cannot be relaxed by an explicit predicate override.

### Derived descriptions and evidence dependencies

An entity view resolves explicit curation, traverses the raw-identity reverse
indexes for every alias component, and derives a deterministic description from
current authorized assertion statements. Settled and newer support comes first;
existing generated summary assertions are eligible grounded inputs. The view
returns exact entity/assertion resource revisions, selected evidence references,
a content fingerprint, configuration generation and completeness. It reads at
most 256 reverse-index entries, renders at most 32 statements and 8 KiB of text,
and never treats an omitted dependency as proof that the view is exhaustive.

The description is a read projection, not a separately persisted ungrounded
summary. Consequently ingestion, correction, unmerge, suppression, expiry,
source change and forgetting immediately regenerate the applicable view under
the requesting principal. No cache of old description text survives erasure.
`known_at` and `valid_at` select assertion revisions; current retention and
curation still fence disclosure. A fingerprint identifies the derived content;
it is not an independently revision-pinned historical resource. Dependencies
can be followed through the ordinary native `explain` contract. Clients wanting
a model-written description submit a grounded summary assertion or enable the
existing optional managed summary stage. No generator is required to read views.

### Episodes and repeated mentions

Evidence carrying `thread_id` and `ordinal` is indexed by signed ordinal and
immutable evidence ID, with `turn_id`, author and event time preserved. Equal
ordinals have a deterministic ID order. Pagination binds the space revision,
principal and episode. A repeated mention has its own immutable `id`,
`entity_id`, evidence references and visibility. It does not rewrite the entity's
original immutable identity evidence. Entity admission also creates a grounded
initial mention. Managed mentions inherit every causal generation input, just
as managed assertions do; external mentions use the ordinary evidence gate.

An episode read filters each source and mention before returning text or entity
IDs. Losing any conjunctive mention input removes that mention; independent
surviving turns remain readable. Reads perform no whole-thread rebuild and do
not invoke a model. One episode page examines at most 64 turns and 64 mention
bindings per turn and reports incomplete member scans. Episode catalogs return
only entries with a proven accessible member in the bounded preview. Foreground
forgetting/expiry fences all views immediately; maintenance removes retired
payload and orphaned indexes, retaining ID tombstones. Temporary resource denial
is not authority for background global deletion.

### External judgments and candidate policy

Identity candidate pages contain immutable entity revisions and grounded views.
Names/aliases, compact spelling and description overlap are recall signals,
never calibrated merge confidence. Identity candidates require matching types,
visibility/owner partitions and version-number signatures, respect explicit
and shared pinned distinctions, and exclude existing canonical aliases.
`kind: relationship` permits distinct/versioned concepts and advertises the
pack's relation vocabulary. Paging examines 64 entities at a time and returns
an explicit cursor/completeness; this bounded lexical/description discovery
does not claim exhaustive semantic nearest-neighbor search.

Entity proposals use the engine's [shared curation queue (#1233)](https://github.com/antflydb/antfly/pull/1233),
including stable proposal IDs, irreversible terminal review outcomes, reversible
merge/unmerge/suppression and reject-with-distinction. Separate service bindings
pin input entity revisions, configuration and evidence without duplicating
free-text prompts into the engine queue. Proposal/list/review require admin
access. An external verdict carries `accept: true`, `false`, or `null` (defer),
an expected space revision, declared actor, evidence and optional qualified
model metadata. The authenticated principal remains the authoritative initiator;
a supplied actor is declared agent attribution. Accept applies the same native
curation and belief rebuild atomically; config/input changes fence stale review.
Terminal queue entries cannot be reopened by changing an idempotency token.

Typed relationship candidates use `propose_relationship`,
`relationship_proposal`, and `review_relationship`, exposed at
`/relationship-proposals[/{id}[/verdict]]`. A proposal contains one immutable,
independently grounded typed assertion and does not participate in recall until
accepted. Only the owner or an administrator may inspect/review it. Accept
revalidates ontology and evidence and admits the assertion through ordinary
native policy in the verdict transaction. Reject leaves the claim unpublished;
defer leaves the proposal open. Additional judge-context evidence is conjunctive
causal provenance of an accepted assertion; losing it prevents disclosure even
if the original factual support survives. Rejection does not imply identity
separation: typed relationship review rejects `pin_distinct`. Attribution and
review reasons are retained only while their own evidence permits disclosure.
None of these contracts accepts a hosted client, credential or prompt to execute.

### Graph recall and bounded output

`recall.graph` admits up to 32 explicit entity seeds, 4 hops, 512 nodes,
4096 undirected edges and 64 fixed PageRank iterations. Defaults use relation
weight 0.5, provisional weight 0.5 and restart probability 0.15. With explicit
seeds, recall bypasses the global assertion scan and follows raw-identity and
mention reverse indexes; it never needs a client to guess foreign table names.
Without explicit seeds, deterministic lexical/vector hits personalize the graph.
All structural/temporal filters, source authorization and retention checks run
before graph admission. Typed relationship assertions remain nodes with their
proof, while a discounted concept-to-concept edge permits one conceptual hop.
Thread nodes are terminal results, preventing unrelated conversation fan-out.

The adjacency walk examines at most 4096 bindings; per-concept assertion and
mention scans are separately bounded and report incompleteness. PageRank
redistributes dangling mass to the personalized seeds. Stable ordering resolves
ties. The result carries ranked assertions, up to eight grounded entity views,
up to eight ordered episode previews, and graph bounds/status. The same byte-based
conservative token upper bound covers the **entire** response, including views,
turn text, citations and diagnostics. Budget trimming removes episode previews,
then descriptions, then lower-ranked assertions and reports omissions. Explicit
seed recall has no scan cursor; an input cursor is rejected. Flat recall remains
available and retains its existing lexical/vector pagination contract.

### Shared engine fixes and qualification

- [#1241](https://github.com/antflydb/antfly/issues/1241): Lite connection
  maintenance executes bounded grouped-source controls under owner/writer
  admission, alternating with detached enrichment replay. Legacy opened table
  objects start resident optional workers at their stable heap address. Waiting
  at `sync_level: enrichments` is not required to publish groups.
- [#1244](https://github.com/antflydb/antfly/issues/1244): the private owner ABI
  forwards the actual entity sink's curation capability. Missing capability
  callbacks fail closed. Distributed atomic curation no longer loses support
  while crossing the owner archive boundary. This changes private owner ABI 82
  and storage engine linkage v29; rebuild all native runtime units together.
- [#1245](https://github.com/antflydb/antfly/issues/1245): Lite accepts qualified
  start identities for complete local snapshot traverse/neighbors and preserves
  table identity through admission, visited sets and paths. Use
  `start_nodes.identities: [{key, table}]` on both backends. Bare keys continue
  to name the owning table; they are not a cross-table wildcard. Distributed
  incoming traversal still requires node deduplication and now reports that
  exact restriction before the generic direction fallback.

The pinned [Codex fixture corpus](pkg/antfly-embedded/src/memory/fixtures/codex-a2d59cfb01/README.md)
replays 154 labeled supersession, identity and typed-relation cases through native
policy. Labels are supplied as external verdicts; this tests admission/lifecycle
and does **not** reproduce or claim hosted/local model precision. Additional
native cases cover descriptions, graph rank/budgets, ordered episodes, repeated
mentions, correction and causal forgetting. Live Codex extraction/retrieval,
note-file changes, fork/revert and product cutover remain consumer qualification
work; no Antfly-only test claims to execute that separate application.

## Public operations and bounded execution

All timestamps are signed milliseconds since Unix epoch. `valid_at` asks about
business validity; `known_at` selects recorded assertion and premise revisions.
Forgetting and retention still apply to historical reads. `history` is the exact
audit view; current recall excludes stale dependencies and historical guidance.
Structural enumeration and ranked recall share lifecycle and validity filtering.
An explicit `valid_at` can select a superseded version in its original interval;
it never requires `include_history` merely to reconstruct that valid state.
`superseded_valid_at` stores the business-time boundary accepted by supersession,
separately from an assertion's original `valid_to`. A null boundary means the
replacement date is unknown: no current or valid-time query may use that
superseded premise as support. A `known_at` before the replacement can still
select the active premise revision. Dated boundaries survive policy rebuilds and
forgetting of the replacement; restoring a premise explicitly clears its boundary.
Dependency applicability is evaluated against the same valid and recorded times at
every depth, including during reconciliation. Ordinary supersession, dispute, or
retraction of a premise never rewrites dependent facts to globally `stale` during
maintenance. Thus historical reads and authority decisions are independent of
whether maintenance has run. Explicitly restoring a retained premise can make its
still-active dependents applicable again; it does not restore dependents that
were themselves retracted or superseded.

`stale` represents explicit semantic invalidation of an opaque derived artifact,
such as a summary or procedure invalidated by an ontology, policy, or identity
change. Such artifacts require regeneration under a new immutable ID. This
invalidation is versioned and historical reads still select the recorded artifact
revision; it is distinct from temporary or time-qualified support applicability.
Queued identity invalidation is fenced to its originating revision, so processing
an older event cannot invalidate an artifact regenerated after that change.

HTTP routes live under `/memory/v1/spaces/{space}`. Use
`X-Antfly-Database` and `X-Antfly-Namespace` for catalog scope (defaults:
`default` and `public`). These headers select resources, never identity or grants.
Memory MCP lives at **`/memory/v1/mcp`**, separately from the general database
MCP server at `/mcp/v1`. Each native action below has a typed `memory_<action>`
tool, including evidence retrieval, configuration history, entity curation and
external embedding staging. Arguments contain `space`, optional `database` and
`namespace`, a resource `id` where applicable, and the action's typed `body`.
Mutations require `idempotency_key`. For example:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_recall","arguments":{"space":"team","body":{"query":"deployment guidance","token_budget":2048}}}}
```

The endpoint owns the memory API major version; tool names do not repeat `v1`.
MCP protocol negotiation is independent of memory API versioning. Both MCP
servers reuse the native protocol transport, but expose separate tool catalogs
and endpoint-local session stores: a session from one endpoint cannot be used or
closed through the other. Both support POST, GET event streams and DELETE session
closure. The database endpoint does not expose memory tools or the former opaque
`memory` action dispatcher. A memory client opts into only the memory catalog.

Schemas are generated from `memory/operations.zig` and the same payload types
parsed by the policy, including exact integer domains, scalar defaults and
mutation idempotency requirements. REST and MCP share the native server ingress,
which constructs read/write capabilities over its owned, catalog-projected
credential and preserves request deadlines and cancellation. Logical table grants
therefore authorize resolved storage aliases without requiring wildcard access;
publication still rechecks live permissions and row-policy support.

Embedded SDKs call `antfly_db_memory_json` with the action/body envelope directly,
with no HTTP server or Wasmtime runtime.

| HTTP suffix | Native action | Behavior |
| --- | --- | --- |
| POST `/memory/v1/spaces` | `create` | Authorized catalog provisioning; model-free defaults |
| GET `/configurations/{id}` | `configuration` | Immutable historical model/ontology/policy binding snapshot |
| GET / PATCH space | `inspect` / `configure` | Effective bindings; revision-fenced transitions |
| POST `/observations` | `observe` | Immutable evidence, optional external claims/entities, durable work receipt |
| POST `/assertions` | `assertions` | Validated external assertions and explicit relationships |
| POST `/recall` | `recall` | Context/temporal filters, lexical/vector ranking and rendered-byte token bound |
| GET `/memories` | `list` | Exact accessible enumeration with explicit pagination |
| GET `/memories/{id}` / `/history` | `get` / `history` | Authoritative record and version history |
| GET `/evidence/{id}` | `evidence` | Authorized source version and original text |
| POST `/corrections` / `/forget` | `correct` / `forget` | Revision-fenced lifecycle/stability changes; non-resurrection fences |
| POST `/outcomes` | `outcome` | Utility feedback, independent of assertion truth |
| GET / POST `/operations/{id}` | `operation` / `advance` | Durable stage status and bounded explicit progress |
| POST `/maintenance` | `advance` with empty ID | One durable dependency/retention cleanup quantum |
| GET `/entities` / `/{id}` | `entities` / `resolve_entity` | Canonical entity view with provenance authorization |
| POST `/entity-corrections` | `curate_entity` | Shared resolver merge/unmerge/suppression policy |
| POST `/embeddings` | `stage_embeddings` | Stage externally produced vectors for a pending generation |

Use `relationship`, `target_id` and `missing_relationship` on structural listing
for exact typed relationship and missing-relationship queries. Predicate
`subject_type`/`object_type` constrain referenced entity types. Cardinality is
scoped by subject, predicate, scope, environment, task and visibility. Private
belief groups additionally include the authenticated owner; one principal's private
evidence cannot silently supersede another principal's state. Source authority rules
bind to authenticated principals; a client-supplied producer label is not a grant.
Authority is recomputed from the strongest complete, eligible support branch,
independently of primary/alternative ordering. Each rule must match the evidence's
persisted owner and its optional producer restriction, including after policy
changes; mismatched legacy evidence remains untrusted rather than acquiring the
new principal's authority. A complete untrusted branch has rank zero; absent
support has no rank and cannot participate in reconciliation. Competing claims
are evaluated at the start of their overlapping business interval, preserving
historically applicable superseded premises while excluding retracted premises.
Authorization, forgetting and expiry checks use the same support traversal as
reads. A surviving branch cannot borrow authority from an unavailable branch.
Source authority changes at logical forgetting/expiry boundaries, independently
of physical source cleanup and restart. Source-rule changes rebuild retained
belief groups atomically under these rules.

The implementation bounds assertion/evidence batches at 256, text at 64 KiB,
individual requests at the admitted space budget (at most 4 MiB), dependency
recursion at 32, and a recall scan at 4096 records. Large structural results must
be paginated; cursors bind space, principal, resource and space revision and
reject stale continuation after mutations. Ranked recall reports scan limits
and budget omissions explicitly. Its native baseline reads primary rows and
computes lexical/cosine scores under these bounds. It does not claim an indexed
ANN/BM25 performance result. Antfly index-backed candidate production can replace
this execution detail without changing policy, history or completeness contracts.

`context_evidence_ids` supplies an explicit bounded prior-conversation plan;
source thread/turn/ordinal/time fields preserve ordering and attribution.
Managed spaces process observations automatically; hybrid spaces use each source's
explicit `managed` ownership flag. All model output inherits the visibility taint
of the full input/context plan, not merely the evidence a model chooses to cite.
Durable internal generation provenance records the admitting operation,
configuration generation, and full input/context evidence plan for each newly
created assertion and entity. This is a conjunctive authorization and retention
dependency, separate from factual citations and alternative `support_sets`:
losing any inference input fences the output and its descendants, even if a model
omitted that input from its citations. Point reads, recorded-time history, recall,
identity resolution, enumeration, vector retention and physical cleanup share this
gate. Reprocessing an existing immutable ID never replaces its original generation
provenance. Cleanup includes this quantum's newly appended invalidation history,
so finishing a scrub cannot leave newly written forgotten text behind. Retired
outputs discard their internal generation metadata after fencing their identities.
Context is disambiguation evidence, not another authoritative extraction stream.
Generated assertions must cite a current observation as well as any prior support they use.
`support_sets` represents independent alternative support; each individual set is
conjunctive. Losing one set must not remove a claim with another retained set.
The durable dependency queue propagates irreversible retention loss as a bounded
retirement frontier. Only loss of all complete retained support branches retires a
fact: fence its immutable ID, remove its primary payload and generation metadata,
and enqueue descendants. Physical cleanup scrubs affected history and projections.
Retraction, dispute, supersession, and explicit artifact invalidation preserve
retained audit history. Forgetting and expiry fence every time-qualified read
immediately, even before retirement reaches deep descendants.
Evidence admission, reads, support traversal, managed execution and physical cleanup
use one retention gate covering expiry and global, source and evidence fences.
New assertions validate every support branch against this gate; observation context
must pass it as well. Corrections and explicit supersessions require the existing
assertion to remain retained, so they cannot rewrite forgotten payloads behind an
in-progress scrub cursor. Rejected admissions publish no evidence, assertions,
producer ownership, receipts or revision changes. These checks preserve bounded,
resumable cleanup without restarting its scan for every concurrent ingestion.
Dependency admission and all read/retention traversals share a maximum of 32
assertion levels, counting the new root. Every proposed support branch reserves
that root level while validating its parents, including alternative branches hidden
by currently visible primary support. Admission memoizes the full immutable DAG
and visits at most 4096 distinct ancestors per operation.
A deeper graph is rejected before committing any assertion or receipt; bounded
batch imports have the same limit and roll back atomically on rejection.
Actual visibility/retention/decision traversals also memoize shared subgraphs;
diamond-shaped support must not expand exponentially. Memo keys include premise
revision, temporal selectors, depth, configuration and principal. Writes invalidate
view/history caches, and an operation-local row cache observes overlay writes first;
its final control-row revision/CAS still rejects concurrent changes. Historical
premise selection is cached per ID/recorded-time selector. Each operation admits at
most 65,536 support evaluation, edge, evidence and history-row work units. Exhaustion
returns `MemoryWorkLimit`, never a partial truth or a partially committed import.

Model revisions are qualified provider capability digests (`sha256:<64 hex>`),
not arbitrary descriptive labels. Admission and invocation reject a missing or
mismatched revision. A role may use the same qualified model as another role.
A generator-backed extractor is a qualified implementation of the extraction
role; an extraction-only model cannot silently become a generator. For compatible
client-produced document/query vectors, use `provider: external`, a model-space
revision and dimensions, and supply `embedding_model` with each vector.

Embedder reconfiguration retains the serving configuration while a pending
configuration builds new vectors in bounded pages. External producers stage
vectors for the exact pending generation. Configuration generations use unique
admission revisions, including failed builds; they are never allocated from the
serving generation or reused after cancellation. An immutable generation snapshot
binds every staged vector to its exact model, so a later model cannot inherit
staging left by a failed transition. Publication switches the configuration
only when the build completes; old and new vector spaces never share scores.
Vectors are generation-keyed projections of immutable assertion text, stored
separately from assertion revisions and lifecycle history. Both current and
`known_at` reads resolve the serving generation's vector using assertion ID and
statement hash; reindexing covers all retained assertions, including historical
and retracted facts. It does not rewrite recorded knowledge time. Bounded cleanup
reclaims retired or failed vector generations and forgotten assertion vectors,
while retaining serving and pending generations.
Model failures and schema/grounding rejection retain bounded retry status and
publish no partial generated output. Recovery replays committed receipts.

Server work runs on the existing bounded maintenance executor and only the
current range leader schedules mutation work. A durable signed execution binding
contains a credential principal, not a password, bearer token or Codex model
session credential. Current grants and row restrictions are rechecked before
execution; unsupported background credentials are rejected at feature admission.
Each durable operation records the permission required at admission. The space
owner field is attribution, not an enduring grant. Deferred configure, entity
curation and vector staging require current admin authority; advancing a receipt
cannot downgrade that requirement to write. The same requirement is rechecked
before model calls, publication and idempotent receipt replay.
Foreground access and durable execution grants are rechecked before publication
and before returning read results. Producer paths requesting background processing
must use a qualified revocable Basic/API-key execution identity; ordinary external
capture does not persist a caller's session credentials. Per-source TTL requires
a space maintenance identity. Unsupported producer paths fail admission explicitly.
Both native storage adapters persist policy rows in a versioned object envelope
(`memory_record_version: 1`, `value: <policy JSON>`). Policy values may be objects,
scalars, or arrays. Routed scans unwrap that payload after handling transport
metadata; they never flatten policy values into the NDJSON document. This keeps
scalar forget fences and producer metadata readable during full-table cleanup,
with the same policy row representation in embedded and routed execution. OCC
versions and content digests continue to refer to the stored envelope.
Background selection rotates an opaque cursor per immutable table identity and
alternates operation work with cleanup. Each pass examines at most 64 pending
entries; an empty continuation may wrap once. Selection advances before checking
execution authority, retry readiness, inference, or publication. Revoked or
unsupported bindings stay fenced and pending without consuming model attempts;
they cannot pin later jobs, including jobs beyond the first queue page. Execution
deadlines and transient failures also leave the selection cursor advanced.
The worker gives model processing a 120-second budget, separate from five-second
selection/cleanup and finalization budgets. Provider deadlines leave a grace period
for durable retry/error publication. If the entire attempt overruns, the worker
re-enters the native service with a fresh, bounded finalization context and the
original revision/receipt fence, recording the failure without invoking a model.
Receipt replay handles an ambiguously committed result; a changed control revision
rejects stale recovery. Failure finalization writes only outcome metadata, without
reacquiring the source plan or invoking a model. It retains live destination
authorization, retry limits, backoff, configuration cleanup and leadership checks.
Actual processing and artifact publication retain their source authorization gates. Foreground
request cancellation and deadlines remain authoritative and cannot be renewed by
this worker-only path. A crash or unavailable storage can still require replay;
inference calls are not exactly once.
Resource-watch admission arms polling when idle and otherwise preserves the
earliest durable deadline. An unfinished bounded round remains immediately due,
with its persisted cursor intact; new observations cannot debounce that round or
postpone already scheduled source-change invalidation and deleted-source cleanup.
Only completing a polling round schedules the next periodic interval.
Cleanup uses the space's own saved binding, independently of every operation's
binding. A revoked cleanup identity does not block valid operations, and a busy
operation queue does not starve authorized cleanup. Each authorized quantum still
passes the shared live grant/publication checks. Cursors are in-memory scheduling
hints owned and serialized by the server's existing maintenance lifecycle;
pending receipts, retry state, and cleanup obligations remain durable, so cursor
loss on restart or leader transfer cannot lose work or grant authority.
Grant authenticators and worker cursors are never returned in public receipts.
Every admitted configuration generation is retained as an immutable snapshot,
so an assertion's producing generation remains auditable after reconfiguration.
The embedded owner advances the same operations explicitly; native model calls
release the connection's writer lease and rebind before the revision-fenced commit.

## Implementation and verification requirements

The policy suite runs with `zig build memory-test`. The native C ABI must be
compiled and exercised with persistent reopen tests; HTTP/MCP tests must verify
catalog identity, authorization and the same lifecycle semantics. Generated
contract, error identity, source-owner, header and SDK checks are release gates.

Required scenario coverage includes delayed Terraform/Helm evidence, same-batch
conflicts, proposed versus actual decisions, historical recorded-time reads,
independent support, source retention, invalidation beyond conversation horizons,
configuration changes during inference, restart/idempotent retry, forgetting and
private provenance in every derived view. No benchmark superiority is claimed
until state evolution and downstream task quality have been evaluated together.

## Design evidence and complete intended scope

## Design intent

Build **memoryaf as a first-party, optionally enabled Antfly memory service**, exposed through a versioned **`/memory/v1` API**, generated SDKs, MCP adapters, and an equivalent direct embedded interface. Use Antfly's managed artifacts, search, graph, inference, and entity resolution as its execution foundation.

**Long-term architecture decision:** implement one native memory service in Antfly's Zig codebase, with shared operation semantics for `/memory/v1`, generated SDKs, MCP adapters, and direct embedded callers. Retire both `extensions/memoryaf` (Rust/WASM) and `go/pkg/memoryaf` as memory implementations. Neither remains a supported backend, compatibility layer, deployment option, or policy authority. Existing code and fixtures are historical evidence and implementation lessons only; this design specifies the desired end state, not preservation of those implementations.

The intended behavior is that agents can remember what happened, determine what is currently applicable, recover historical states, explain their evidence, and reuse successful procedures across sessions and teammates. This issue records the research, repository investigation, proposed ontology, ownership boundaries, and implementation/evaluation plan behind that proposal.

The research comparisons below motivate design and evaluation; they do not establish that native memory outperforms mem0.

## Context and dependencies

- Research cutoff: **October 19, 2026**.
- Repository implementation examined at [`70fb8dd0`](https://github.com/antflydb/antfly/tree/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a).
- Principal dependency: **#1186 — Add resolver adjudication, curation, and constrained grouped producers — merged** on October 20, 2026 at 04:42 UTC (October 19 Pacific).
- Implementation baseline: merge commit [`d227ec28`](https://github.com/antflydb/antfly/commit/d227ec2811235d618b8e50d86ab4b1472b207cc4). The initial investigation examined PR head [`5b9c1d2`](https://github.com/antflydb/antfly/tree/5b9c1d232d110afb6e6b073a78adc5334cc55f7a); the contract links below now target the merge commit. The feature is no longer waiting for #1186 to merge. Qualify the native memory service and Codex consumer against this baseline before enabling the proposed memory behavior; merge status alone does not establish that integration.
- Research findings below are author-reported results and architectural evidence. The paper benchmarks were not independently reproduced during this investigation.

## Problem

Persistent memory needs more than storing text and retrieving similar records. A useful agent must distinguish:

- What a source said from what the agent inferred.
- A proposal from an approved decision or an observed outcome.
- A currently applicable fact from a superseded or disputed assertion.
- When something happened from when the system learned it.
- Similar subject matter from guidance applicable to the current task and environment.
- A relevant subset from an explicitly complete enumeration.
- A useful procedure from a superficially similar but unsuccessful attempt.

Today, memoryaf supplies a useful memory CRUD/search surface, but it does not yet implement these distinctions as a complete memory lifecycle. Adding more search signals alone will not address stale beliefs, conflicting evidence, incomplete structural answers, or unsupported conclusions.

### Example: changed deployment guidance

1. A January runbook says production deploys with Helm.
2. A February discussion proposes moving to Terraform.
3. A March approved change establishes Terraform for production.
4. The old January runbook is imported in April.

Expected behavior:

- Current production guidance remains Terraform.
- The proposal, approval, implementation evidence, and historical Helm guidance remain distinguishable.
- Historical questions can recover the earlier state.
- Import order does not determine truth.
- A staging-specific exception does not overwrite production guidance.
- Answers cite the supporting source versions and identify unresolved conflicts.

## Historical implementation evidence (not support requirements)

The historical Rust implementation is [`extensions/memoryaf`](https://github.com/antflydb/antfly/tree/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/extensions/memoryaf), compiled to a WASM component and hosted by Antfly. The following findings explain design requirements and failure cases; they do not require supporting, repairing, extending, or matching that implementation. The native service replaces it.

Rust extension evidence:

- [`Cargo.toml`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/extensions/memoryaf/Cargo.toml) defines `memoryaf-extension` with `wit-bindgen`.
- [`extension.json`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/extensions/memoryaf/extension.json) declares memory shapes, generated embedding/full-text objects, capabilities, MCP tools, and agent/skill descriptors.
- [`antfly-extension.wit`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/extensions/memoryaf/wit/antfly-extension.wit) imports `db.query`, `db.write`, and `ai.embed`, and exports `init` and `call-tool`.
- [`src/lib.rs`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/extensions/memoryaf/src/lib.rs) implements `store_memory`, `search_memories`, and `list_memories`. On `wasm32`, its host helpers invoke the WIT imports; native builds use test stubs.
- The README's statement that host calls are only planned is stale relative to those imports. Some responses still use `status: planned`; this investigation did not establish complete host integration solely from imports or response labels.
- The historical WIT surface exposes database query/write and embedding only. The native design avoids expanding that memory-specific ABI to reach managed producers, resolution, transactions, and visibility.
- `store_memory` returns top-level success even when an embedded write result reports an error, while search/list fall back to empty rows on query errors. The replacement must test failure propagation explicitly rather than copy those behaviors.
- Parsing, request-schema enforcement, configuration use, content-derived IDs, and source/scoping behavior are useful sources of replacement test cases, not legacy feature-parity requirements.

The historical [`go/pkg/memoryaf`](https://github.com/antflydb/antfly/tree/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf) implementation provides additional product lessons and code findings. It is also replaced, not supported alongside the native service. The capabilities below are historical context, not requirements to preserve its APIs, standalone server, or dashboard.

Capabilities documented in the Go package include:

- MCP and HTTP APIs, plus an embedded dashboard.
- Episodic, semantic, and procedural memory records.
- Namespace isolation, team/private visibility, and agent/session/device scoping.
- Ephemeral session memory with TTL.
- External source identities, versions, URLs, and section paths.
- Entity extraction and entity mention graphs.
- Full-text plus semantic retrieval with RRF and optional graph expansion.
- docsaf integration for filesystem, Git, S3, Google Drive, and web sources.

Go reference implementation evidence:

- [`StoreMemory`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/handler.go#L513) stores caller-supplied content and extracts entities. It does not automatically distill an interaction into durable assertions or reconcile competing claims.
- [`SearchMemories`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/handler.go#L686) fuses full-text and semantic hits; [graph-only results are appended](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/helpers.go#L120), rather than participating in a final memory-aware reranking/context-selection stage.
- [`Memory`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/types.go#L40) already has `event_time`, `confidence`, `supersedes`, `trigger`, `steps`, and `outcome`, but those fields are not a complete assertion, revision, or procedural-learning policy.
- [`entityKey`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/handler.go#L158) derives identities from normalized label/text. This does not by itself distinguish homonyms or resolve aliases across sources.
- The [docsaf example](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/examples/memoryaf/README.md) preserves source references and synchronizes sections, but arbitrary derived beliefs/procedures still need dependency-aware invalidation.
- The two historical implementations have different surfaces. Neither API nor their combined feature inventory defines the new v1 contract; the native service requirements below do.

The existing [agentic warehouse memory plan](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/docs/plans/agentic-warehouse-memory.md) has related ideas about source data, canonical entities, agent interaction, and feedback. Warehouse bridges and its learning proposals should remain separate deliverables rather than being implied prerequisites here.

## Relationship to mem0

Antfly and mem0 operate at different layers: Antfly provides database/search/inference infrastructure, while mem0 provides memory extraction, lifecycle, retrieval, and developer-facing APIs.

An Antfly backend for mem0 OSS is feasible through its [vector-store interface](https://github.com/mem0ai/mem0/blob/main/mem0/vector_stores/base.py), including metadata filtering and optional keyword search. Antfly was not listed in the [provider registry](https://github.com/mem0ai/mem0/blob/main/mem0/utils/factory.py) when inspected. That adapter is a useful independent adoption/integration project, but it would not deliver the full design proposed here.

Avoid positioning memoryaf around capabilities mem0 already has:

- The current mem0 platform documents semantic, BM25, entity, and temporal retrieval, plus native graph memory: [architecture](https://github.com/mem0ai/mem0/blob/main/skills/mem0/references/architecture.md).
- Its current graph connects entities and memories through shared mentions; it explicitly describes co-occurrence rather than typed entity relationships: [graph semantics](https://github.com/mem0ai/mem0/blob/main/docs/platform/features/graph-memory.mdx).
- Its current automatic extraction is additive and preserves changed facts with temporal context; explicit operations can update/delete records: [memory lifecycle](https://github.com/mem0ai/mem0/blob/main/docs/core-concepts/how-it-works.mdx).

These links are mutable upstream references; pin the actual version used for any evaluation. Platform and OSS behavior must be evaluated separately, since older external-graph documentation and newer native-graph behavior differ.

The proposed differentiation is evidence-backed temporal assertions, typed relationships, structural queries, source-change propagation, and reusable procedures, combined with Antfly's embedded/server deployment options and integrated inference. Performance and product superiority require controlled evaluation.

## Research evidence and transferable ideas

### Taxonomy and representations

- **[Memory in the Age of AI Agents](https://arxiv.org/abs/2512.13564)**, December 2025: organizes the field around forms, functions, and dynamics. Use this as a taxonomy rather than treating episodic/semantic/procedural labels as a complete architecture.
- **[Hindsight: Retain, Recall, Reflect](https://arxiv.org/abs/2512.12818)**, December 2025; [full text](https://arxiv.org/html/2512.12818v1): separates world facts, agent experience, synthesized observations, and opinions. Transfer the separation between evidence, synthesis, and beliefs. Its conversational evaluation does not establish all production governance/deletion behavior.
- **[A-MEM](https://arxiv.org/abs/2502.12110)**, February 2025: dynamically links notes and evolves contextual representations. Useful for derived associations and thematic organization, with explicit source dependencies and versioned updates.
- **[MIRIX](https://arxiv.org/abs/2507.07957)**, July 2025: distinguishes multiple memory types and includes multimodal experiences. Supports retaining resources and procedural/episodic structure rather than flattening everything into facts. Its use of multiple agents is not a requirement for this design.

### Temporal state and consolidation

- **[Zep / Graphiti](https://arxiv.org/abs/2501.13956)**, January 2025: temporal knowledge graphs integrate conversational and business data while preserving historical relationships. Supports explicit temporal validity and provenance.
- **[SimpleMem](https://arxiv.org/abs/2601.02553)**, January 2026; [full text](https://arxiv.org/html/2601.02553v1): self-contained memory units, background consolidation, and adaptive query-aware retrieval. Adopt coreference resolution, relative-time normalization, and different retrieval granularities. Treat its compression claims as experimental findings rather than a guarantee of losslessness for arbitrary evidence.
- **[EverMemOS](https://arxiv.org/abs/2601.02163)**, January 2026; [full text](https://arxiv.org/html/2601.02163v1): episodes/facts are organized into thematic structures, with time-bounded foresight. Adopt thematic derived views and explicit temporary plans/states. Foresight and profile behavior are partly demonstrated qualitatively rather than fully covered by reasoning benchmarks.
- **[TEM: Temporal State Tracking Memory](https://arxiv.org/abs/2610.21900)**, October 15, 2026; [full text](https://arxiv.org/html/2610.21900v1): preserves fact versions and renders historical status with retrieved evidence. Important limit: its update rule assumes single-valued updates arriving in source order, so it does not solve delayed evidence, concurrent valid values, or within-session conflicts. Large label gains appear in controlled state-update experiments; the public LoCoMo comparison shows little incremental benefit from labels alone. Adopt explicit temporal status, not the simplistic update rule wholesale.
- **[The Epistemics of Agent Memory](https://arxiv.org/abs/2609.33013)**, September 26, 2026: investigates consolidation quality, reversibility, and governance, and reports a negative result on a proposed metric's ability to predict real transfer. Treat consolidation metrics as hypotheses requiring downstream validation; do not optimize a surrogate without checking its usefulness.

### Ontology and context compatibility

- **[Grounding Agent Memory in Contextual Intent / STITCH](https://aclanthology.org/2026.findings-acl.584/)**, ACL Findings, July 2026; [preprint](https://arxiv.org/abs/2601.10702), [project/code](https://github.com/Seattleyrz/contextual-intent): goal, action type, and salient entity types help suppress semantically similar but context-incompatible history. Adopt task/environment/applicability context and evaluate on CAME-Bench.
- **[Ontology-Grounded Project Memory for Coding Agents](https://arxiv.org/abs/2608.13662)**, August 13, 2026; [full text](https://arxiv.org/html/2608.13662v1), [artifacts](https://github.com/Trivyn/moosedev): typed records improve completeness, missing-relationship queries, and supersession traversal; ordinary relevance retrieval was broadly comparable. This is a limited case study using a proprietary engine, with capture quality and agent-family constraints. The transferable result is the distinction between ranked relevance and exact structural operations, not universal graph superiority.
- **[PROV-O](https://www.w3.org/TR/prov-o/)** and **[SHACL](https://www.w3.org/TR/shacl/)**: established models for provenance and graph constraints. Borrow their concepts; full RDF/OWL/SPARQL adoption is not a prerequisite.

### Learning and evaluation

- **[AgeMem](https://arxiv.org/abs/2601.01885)**, January 2026: exposes memory operations as actions and learns unified short-/long-term management. Supports a coherent memory-operation API, while trained management policy remains a later step.
- **[MemRL](https://arxiv.org/abs/2601.03192)**, January 2026: separates semantic relevance from learned usefulness using outcome feedback. Record utility independently of truth or source authority.
- **[Hindsight Memory-PRM](https://arxiv.org/abs/2608.29605)**, August 30, 2026; [full text](https://arxiv.org/html/2608.29605v1): uses operation traces, retrievals, citations, and controlled interventions for memory credit. Useful direction for later offline policy learning. Its own limitations include domain schema design and insufficient stress on forgetting/deletion.
- **[Dual-Layer Agentic Memory](https://arxiv.org/abs/2608.22215)**, August 2026: explores selective write routing and parametric consolidation. Adopt cheap-to-expensive admission/routing experiments; defer internalizing tenant knowledge into weights until editability, attribution, and deletion semantics are established.
- **[Beyond Downstream QA / FEB-30](https://arxiv.org/abs/2610.22209)**, October 15, 2026; [full text](https://arxiv.org/html/2610.22209v1): directly inspecting stored fact evolution exposes failures hidden by correct final answers. Its 30 synthetic trajectories are useful diagnostics, not a broad statistical ranking or a complete episodic/procedural benchmark.
- **[LongMemEval-V2](https://arxiv.org/abs/2605.12493)**: include longer-term agent-memory evaluation beyond legacy conversational benchmarks.
- **[MemoryAgentBench](https://arxiv.org/abs/2507.05257)**: includes retrieval, test-time learning, long-range understanding, and selective forgetting.
- **[Mem2ActBench](https://arxiv.org/abs/2601.19935)**: evaluates whether memory is applied to tool selection and parameter grounding rather than merely recalled in answers.
- **[Infini Memory](https://arxiv.org/abs/2606.10677)**: maintainable topic documents are another representation worth testing against derived summaries; do not assume one representation must serve every query.

## What #1186 enables, and what it does not

Pinned contracts:

- [Generated producers](https://github.com/antflydb/antfly/blob/d227ec2811235d618b8e50d86ab4b1472b207cc4/zig/GENERATED_PRODUCERS.md).
- [Grouped sources](https://github.com/antflydb/antfly/blob/d227ec2811235d618b8e50d86ab4b1472b207cc4/zig/GROUPED_SOURCES.md).
- [Resolution and curation](https://github.com/antflydb/antfly/blob/d227ec2811235d618b8e50d86ab4b1472b207cc4/zig/RESOLUTION.md).

### Reuse these primitives

1. **Constrained producers:** schema validation, bounded batching, repair, source-aligned generated extraction, and explicit decision evidence modes.
2. **Canonical identity:** recorded resolution decisions, adjudication, reversible redirects, distinct pins, suppression, and shared destination-owned corrections.
3. **Conversation processing:** ordered bounded groups, prior-segment artifacts, durable finite invalidation, per-unit provenance, and retirement/replay behavior.
4. **Publication correctness:** source/configuration/policy revision fences, durable receipts, and distinguishable primary versus derived visibility.

Do not build a second model-provider client, resolver, or crash-recovery system in memoryaf.

### Preserve the contract boundaries

- Entity identity is different from assertion truth. A merge does not mean one deployment decision supersedes another.
- Grouping is within one table; prior context is bounded to configured earlier segments.
- Grouped invalidation is deliberately finite and does not recursively extend to an entire thread.
- Application fields such as `replaces` and `retracts` require application semantics; Antfly does not rewrite them automatically.
- Adjudication examines the best deterministic candidate, rather than arbitrating a candidate set.
- Candidate rejection does not disambiguate identical rendered keys. Homonym handling remains an explicit design requirement.
- Candidate description changes do not silently reconsider every committed identity decision; explicit re-resolution is required.
- Committed replay is stable, but inference execution is not exactly once: a crash before publication may repeat a call.
- Token probabilities and constrained verdicts are not measured factual correctness or identity precision. In verdict-only mode, selected values of one are verdict encodings, not calibrated confidence.
- Server destination-namespace curation and native/Lite resolver-local curation have different documented atomicity scopes; do not promise cross-mode equivalence beyond the contract.

## Proposed memory ontology

Use a small versioned ontology, retaining episodic/semantic/procedural as user-facing categories while separating internal object roles.

| Object | Meaning | Required information |
| --- | --- | --- |
| Evidence | Source message, document version, tool result, or observation | Source ID/version, author, timestamp, spans, access policy |
| Entity | Person, project, service, component, etc. | Stable identity, type, aliases, resolution provenance |
| Assertion | A source statement or system inference | Subject, predicate, value, scope, modality, validity, supporting evidence |
| Episode | Coherent task/event involving observations and actions | Goal, participants, environment, sequence, outcome |
| Procedure | Reusable guidance | Preconditions, steps, expected outcome, applicability, supporting episodes |

Summaries, profiles, thematic collections, and current-state answers should initially be derived views. Preserve their dependency references so they can be invalidated and regenerated.

### Orthogonal assertion dimensions

- **Epistemic role:** reported, observed, inferred, hypothesized.
- **Modality:** actual, planned, desired, conditional, hypothetical.
- **Lifecycle:** active, disputed, superseded, retracted.

Keep extraction/model confidence, identity confidence, source authority, assertion belief strength, and retrieval utility distinct. Do not collapse these into a single `confidence` value.

An assertion needs, at minimum, versioned equivalents of:

```text
id, subject_ref, predicate, value_or_entity_ref
scope, applicability, epistemic_role, modality, lifecycle
valid_from, valid_to, recorded_from, recorded_to
evidence_refs, supersedes_refs, contradicts_refs
ontology_version, producer_version, reconciliation_policy_version
```

Unknown validity must remain unknown; avoid inventing exact dates from ambiguous text. Relative dates must retain the source timestamp/timezone used for normalization.

### Ontology policy

- Begin with JSON-based definitions and validation, using predicate domain/range, cardinality, temporal behavior, and applicability rules.
- Start with a software-team pack: component/service, decision, constraint, incident, experiment, procedure, and outcome.
- Initial edges: `affects`, `supported_by`, `supersedes`, `contradicts`, `resolved_by`, and `requires`.
- Namespace domain-specific predicates and version schema/policy changes.
- Models may propose additions; admitted changes require an explicit migration/reprocessing policy and evaluation.
- Do not introduce arbitrary new predicates silently on every ingestion.
- Preserve interoperability options with PROV-O/SHACL without requiring a full ontology reasoner for the first implementation.

## Temporal assertion and belief management

Maintain both valid time (when a statement applies) and recorded time (when memoryaf learned/revised it). Preserve historical evidence according to retention policy; append-only history must not override explicit deletion obligations.

Candidate conflicts should be grouped by something like:

```text
tenant + canonical subject + predicate + applicability scope
```

Predicate policy determines whether differing values conflict. Single-valued deployment guidance and multi-valued responsibilities must behave differently. Environment, version, branch, role, and task can make apparently competing assertions simultaneously valid.

Reconciliation operations should include create, reinforce, dispute, supersede, and retract. A source edit/retraction and a real-world change are different operations. Record evidence, policy version, selected operation, and revision preconditions. Apply accepted changes with concurrency protection using existing Antfly transactions/primitives.

Replaying an accepted decision must preserve it. Re-evaluation against a changed policy or new evidence must be explicit and traceable. Authority policy belongs to memoryaf/domain configuration rather than a hard-coded universal rule that the newest or highest-scoring sentence wins.

## First-class memory service and API

### Service boundary

The public entry point is `/memory/v1`, independently versioned from `/db/v1`. Memoryaf remains the product name. The native first-party service is optionally enabled and requires no memory extension installation. A memory operation has the same policy, authorization, provenance, and completion semantics through HTTP, SDKs, MCP, or the direct embedded interface.

```text
HTTP /memory/v1       SDKs       MCP adapters       Embedded calls
          \            |            |                  /
                    Shared memory service
          space policy / assertions / recall / lifecycle
                              |
          Antfly documents, transactions, managed producers,
                resolvers, graph, search, inference
```

The memory service sits above the generic engine. Memory-specific meanings such as dispute, authority, and supersession belong in this service and its domain policy, not Raft/index internals. Extensions remain useful for custom ontology packs, connectors, experimental policies, and domain-specific memory tools.

Implement the memory operation/policy layer as a native Zig module that can run with the embedded engine without importing server-only routing or extension runtime code. HTTP authentication/routing and MCP tool adaptation belong in server/protocol layers; embedded bindings invoke the same operations with explicit execution context. Generated Go/Rust/Python/TypeScript SDKs or embedded bindings are clients of that implementation, not independent memory engines. Embedded operation requires neither an HTTP server nor Wasmtime.

### End-state scope and retirement

- One memory implementation, one durable memory schema and policy authority, and one versioned public operation contract.
- Retire the Rust/WASM memoryaf package and its memory-specific manifests, handlers, and deployment/documentation paths. Generic Antfly extensions remain supported independently.
- Retire the Go memoryaf handler/server/dashboard implementation. Do not retain it as a fallback service or implement feature parity against its old API.
- Expose memory MCP tools as thin first-party adapters over the native service. Their contract follows `/memory/v1` semantics, not the old extension ABI.
- Rewrite docsaf and Codex consumers to target the new native/remote contract. Preserve required product semantics and source provenance, rather than old implementation details, table layouts, or tool payloads.
- Import existing data only if there is an identified requirement. Such work is a bounded one-time importer with an explicit source/target cutover, never dual writes, bidirectional synchronization, or a permanent compatibility backend. Preservation of legacy memory IDs/routes is not a v1 requirement.
- Completion includes removing obsolete implementation code, examples, dependencies, tests, and docs where specific to the retired memory services. Generic engine/extension functionality and unrelated Go/Rust clients remain outside that removal scope.

### Initial route proposal

These routes describe the proposed contract, not existing endpoints. Final naming should follow Antfly's API conventions.

```text
POST   /memory/v1/spaces
GET    /memory/v1/spaces/{space}
PATCH  /memory/v1/spaces/{space}
POST   /memory/v1/spaces/{space}/observations
POST   /memory/v1/spaces/{space}/assertions
POST   /memory/v1/spaces/{space}/recall
GET    /memory/v1/spaces/{space}/memories/{id}
GET    /memory/v1/spaces/{space}/memories/{id}/history
POST   /memory/v1/spaces/{space}/corrections
POST   /memory/v1/spaces/{space}/forget
POST   /memory/v1/spaces/{space}/outcomes
GET    /memory/v1/spaces/{space}/operations/{id}
```

- `observations` accepts source evidence with stable identity/version and idempotency keys.
- `assertions` accepts externally produced structured claims, provenance, producer identity, and authorized explicit lifecycle operations. These are attributed assertions, not automatically trusted truth; apply the admitted space policy.
- `recall` accepts task context, token budget, and valid/recorded-time selectors. Return evidence references, uncertainty, temporal status, and explicit freshness/completeness information.
- `corrections` records evidence-backed explicit revisions under authorization and revision preconditions.
- `forget` has explicit selection, scope, retention, and non-resurrection semantics distinct from ordinary supersession.
- `outcomes` records task results and used-memory references without changing assertion truth by itself.

Mutations identify the accepted operation and source/configuration generation. Long-running processing exposes durable operation status, stage completion, structured failure/retry information, and a visibility target. Primary evidence acceptance, assertion reconciliation, and index readiness are separate states. Requested waits respect deadlines; asynchronous acceptance never pretends derived memory is already visible.

Transport retries use idempotency keys; the key's scope and request fingerprint detect conflicting reuse. Async workers retain admitted policy/model identities and check revision preconditions before publication. Operations and status reads use the same authorization boundary as the space and its evidence. Provide SDK operation helpers and a common embedded operation/status interface rather than making Codex poll HTTP internally.

### Memory spaces and authorization

A space is a named memory-policy/data boundary, explicitly bound to an Antfly database and namespace with identified backing resources. A memory space is not an additional identity system or an unchecked caller-supplied user ID. Resolve space IDs within the authenticated database/tenant context; cross-space access requires explicit grants. Space authorization cannot widen underlying resource access.

Each space defines ontology/version, applicability and predicate rules, source authority, retention, extraction ownership, processing features, model-role bindings, budgets, and an authorized background execution identity. Derive callers from authentication context; treat agent/session/task IDs as scoped metadata, not authorization credentials. Expose effective configuration and readiness separately from secrets. Space creation is explicitly authorized resource provisioning; model-free defaults do not justify an unauthenticated shared default space.

## Feature-based model requirements

**A space does not require a decider, generator, extractor, and embedder merely to exist.** Require capabilities for enabled features and admitted producer paths. Model-free evidence storage, external assertion ingestion, lexical recall, filters, history, and authorized explicit corrections must work without loading an ML model. This does not imply automatic fact extraction or automatic contradiction detection without a configured implementation.

| Enabled feature/path | Required capability |
| --- | --- |
| Evidence storage and externally supplied assertions | No model; schema, provenance, and policy validation still apply |
| Lexical recall, filters, history, explicit corrections | No model |
| Semantic recall | Compatible embedding configuration, or compatible externally supplied document and query vectors |
| Entity/relation extraction | Qualified extractor, which may be generator-backed |
| Managed conversation-to-assertion distillation | Qualified structured extraction pipeline, commonly a generator |
| Model-assisted reconciliation or identity adjudication | Qualified decider, which may be generator-backed |
| Summaries and procedural synthesis | Qualified generator |
| Deterministic reconciliation | No model; explicit rules, authority, cardinality, and applicability policy |

These are roles, not four distinct model processes. One admitted model can satisfy multiple roles; each role must be independently qualified on its task, schema, context limits, and evidence mode. A generator's structured output or verdict encoding is not calibrated factual confidence. Entity extraction and assertion distillation remain different operations even if one model serves both.

### Processing modes and extraction ownership

- **External:** accept evidence and client-produced assertions; no automatic raw-to-assertion distillation. Indexing, configured entity enrichment, deterministic reconciliation, and recall may still run if enabled.
- **Managed:** distill observations through an explicitly configured and permitted extraction pipeline.
- **Hybrid:** accept both, with explicit producer/source ownership and idempotency rules determining which inputs receive managed distillation.

Hybrid mode must identify a producer path per source/event or admitted source class. It must not independently extract the same evidence after a client already supplied its assertions unless an explicit parallel-evaluation policy keeps outputs separate. Source/evidence links allow later reprocessing without duplicate authoritative claims.
The shared assertion admission path enforces source ownership on primary evidence
and every alternative support set. Client-produced assertions cannot cite managed
evidence in any branch, whether submitted with new observations or through the
assertions endpoint. The managed executor supplies its internal producer context
to the same validator; a client-supplied producer label cannot select that path.
Managed generation has stage-specific ownership plans, reconstructed from immutable
observation metadata at execution and publication. Distillation marks client-owned
observations as context-only; they may disambiguate a claim but cannot independently
support a second authoritative extraction stream. Every primary and alternative
support branch must cite a current managed observation. Entity enrichment and
summary/procedural synthesis may use current client-owned observations. Prior
context never supplies a current root for any stage. The shared stage validator
rejects cross-stage output kinds before aggregation and rechecks the final output
before publication; neither model-authored kinds nor producer labels grant a stage.
The full admitted inference input plan remains a conjunctive privacy/retention
dependency, including inputs used only as context.

Codex initially uses external or hybrid mode: its authenticated model client supplies claims, while Antfly handles enabled indexing/entity processing and deliberately migrated reconciliation. Existing Codex settling, forgetting, source-note ownership, and access rules remain explicit compatibility requirements.

### Effective model bindings and profiles

Deployment configuration may provide named model resources and recommended profiles. Spaces select enabled features and resolve deployment defaults plus explicit overrides into an effective versioned configuration. Do not make an ambient change of defaults silently alter an existing space's behavior: applying changed bindings is an explicit configuration-generation transition. Allow explicitly disabling an inherited feature/binding.

Conceptual configuration (not a committed wire schema):

```yaml
processing: hybrid
features:
  semantic_recall: true
  entity_resolution: true
  assertion_distillation: false
  reconciliation: deterministic
  summaries: false
models:
  embedder: embeddings-default
  extractor: entities-default
```

Keep model-role bindings separate from memory semantics. The configuration above still needs ontology, authority/cardinality/temporal rules, source ownership, namespace binding, and budgets. Entity resolution may be deterministic; model adjudication is an additional explicitly enabled policy, not an unconditional decider requirement.

Expose resolved model revisions/specification identities, enabled capabilities, policy/ontology versions, producer paths, processing generation, and stage/index readiness. Managed credentials remain secret references with an authorized execution identity, not plaintext fields returned from space inspection. Codex session credentials do not become persistent service credentials.

Profiles are convenience configurations, not different protocols:

- **Minimal/external default:** model-free ingestion, explicit lifecycle operations, and lexical recall.
- **Managed profile:** explicit feature selection and recommended compatible bindings, enabled only when dependencies are admitted.

### Admission, runtime failures, and reconfiguration

1. At configuration admission, reject enabled paths with missing/unsupported model roles, schemas, dimensions, permissions, or execution capabilities. Resolving a valid model resource need not imply keeping it loaded; distinguish configuration validity from runtime availability.
2. Runtime load/resource/provider failures expose pending, retryable, or failed processing with bounded budgets. If configured for asynchronous durable capture, observations may be accepted while enrichment is pending. Calls requesting stronger completion must report whether the requested stage/visibility target was met.
3. Never convert model/query failure into successful empty recall, fabricated confidence, or an unreported substitute model. Lexical fallback, model cascades, and alternative producers must be explicit policies; return the actual path and reduced readiness when applicable. Required feature admission is not bypassed by a fallback that cannot satisfy the requested contract.
4. A model-free space does not require downloading/loading an unused model, and failures of disabled features cannot block basic storage/lexical operations.
5. Version model, ontology, policy, and source fingerprints. Configuration edits use revision preconditions and define whether new inputs only or explicit backfill/re-evaluation are affected. Existing committed judgments are not silently rewritten.
6. Embedder changes build a compatible new index generation before the serving cutover; external vectors carry compatible model-space identity and dimension. Do not mix incomparable vector spaces or presume supplied document vectors solve query embedding.
7. Separate idempotent recovery replay from explicit recomputation. In-flight work cannot publish through a superseding configuration/source generation; preserve auditable accepted history and forgetting fences during reprocessing.

## Ingestion and consolidation

Proposed flow:

```text
source events / docsaf documents / tool results
  -> idempotent evidence ingestion
  -> bounded ordered episodes/groups
  -> constrained extraction with attribution, modality, and source spans
  -> canonical entity resolution
  -> assertion validation and reconciliation
  -> searchable assertion/episode views
  -> asynchronous summaries, profiles, and procedures
```

Use #1186 for bounded conversation context and execution. Do not assume arbitrary generator output is already a fully maintained assertion table; materialization and reconciliation must be explicitly implemented and qualified.

Long-lived conclusions may depend on old evidence outside grouped prior-context horizons. Start with a memoryaf dependency index and durable work queue; reuse Antfly replay/artifact machinery where supported. Track dependency versions and fence stale publication. Updating or deleting one supporting source must reconcile that contribution while retaining independent support from others.

Consolidation should create derived summaries/procedures without erasing evidence needed for corrections or historical questions. Route routine inputs through cheap checks and escalate uncertain extraction/reconciliation selectively. Retention, compression, and promotion are separate policies.

## Recall and structural query design

Replace graph-result appending with a unified recall pipeline:

1. Identify query intent: current state, history, explanation, procedure, enumeration, or exploration.
2. Apply authorization and explicit scope constraints.
3. Resolve query entities through Antfly's serving resolver API.
4. Retrieve lexical, vector, temporal, and graph candidates as appropriate.
5. Validate lifecycle/applicability against authoritative assertion state, independently of which versions happened to be retrieved.
6. Rerank combined candidates and deduplicate redundant evidence.
7. Assemble a token-budgeted context with citations, historical labels, unresolved conflicts, and freshness/completeness metadata.

Use structured, paginated queries for questions such as:

- All accepted constraints affecting a component.
- Decisions with no recorded rationale.
- What replaced an earlier decision.
- Which procedures depend on a superseded assumption.

Top-k retrieval cannot establish completeness or absence. Report absence only within the caller's accessible, recorded corpus, and expose truncation or incomplete enumeration explicitly.

Possible application API:

```text
observe(events, source, scope)
recall(query, task_context, token_budget, valid_at, known_at)
record_outcome(task_id, result, used_memory_ids)
```

Map these operations to the first-party API, SDKs, MCP adapters, and embedded interface above. Define stable source/evidence identity within the new contract; old memory APIs and IDs do not constrain it. A local/Lite deployment requires explicit integration and semantic qualification, not something implied by Antfly Lite's existence alone.

## Memory service versus generic Antfly engine ownership

Memoryaf policies, ontology definitions, lifecycle behavior, and context assembly belong in the shared native memory service. Reusable execution, authorization, storage, transaction, resolver, and artifact capabilities belong in the generic Antfly engine. Reuse declarative managed producers rather than building a parallel inference or recovery system. Ontology packs and provider/connector adapters may specialize admitted service configuration; support for executing arbitrary custom memory-policy plugins is not required for v1 and must not create a second memory authority.

| Capability | Memoryaf owns | Antfly owns |
| --- | --- | --- |
| Capture | What to retain; ontology; attribution/modality; episode policy | Constrained execution, source alignment, budgets |
| Entity identity | Domain identity policy and review UX | Resolution, redirects, pins, suppression, curation persistence |
| Belief revision | Contradiction, authority, applicability, supersession | Transactions, revision fences, durable artifacts |
| Consolidation | Summary/procedure policies and quality gates | Artifact execution and reusable dependency primitives |
| Recall | Intent, temporal policy, context assembly, abstention | Search, filters, graph traversal, reranking, pagination |
| Retention | Expiration, archival, forgetting, retraction policy | TTL, deletion, index cleanup, durable execution |
| Learning | Task-specific rewards and policy evaluation | Inference and telemetry primitives |

### Antfly follow-ups to qualify

1. **Multi-candidate identity arbitration and safe homonym handling.** Build tests around aliases, same-name distinct people/services, and conflicting descriptions. Extend the reusable resolver if current best-candidate behavior cannot meet the requirements. Avoid normalization-only identity collisions.
2. **Cross-document derivation dependencies beyond grouped horizons.** First exercise existing artifact/replay/transaction primitives. Add a bounded dependency/invalidation primitive only for demonstrated reusable gaps. Maintain revision-fenced publication and distinct primary/derived visibility.
3. **Query completeness and readiness contracts.** Confirm existing query/graph APIs expose enough pagination, truncation, and derived-visibility information for exact structural memory operations; extend generic contracts only where necessary.

Do not add hard-coded belief semantics, software-team ontology classes, or memory-specific RL policies to Antfly core.

## Access and correction behavior

Before expanding derived team memory, make the access contract consistent across lookup, explicit filters, graph traversal, summaries, entity inspection, and procedures.

Historical observations in the **Go package**, retained as evidence for native-service authorization tests rather than work to maintain or repair that package:

- [`buildFilterQuery`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/handler.go#L311) adds team-or-owner filtering by default, but an explicit visibility value takes a separate branch that does not retain that default owner restriction.
- [`queryMemoryInTable`](https://github.com/antflydb/antfly/blob/70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a/go/pkg/memoryaf/handler.go#L454) performs direct-ID lookup without applying the same visibility filter.

These are code-level findings, not an end-to-end exploit assessment; behavior depends on surrounding authentication and Antfly authorization configuration. Test the new native service's lookup/filter/traversal boundaries directly. Fixing or maintaining the retired Go implementation is outside this feature's scope.

Derived summaries must not disclose inaccessible inputs. Removing a source's access or deleting evidence must affect derived recall according to an explicit policy. Source suppression, identity suppression, and forgetting a user's content are different operations and should not be conflated.

## Outcome-based learning

Record retrieval candidates, selected evidence, citations, policy/model versions, token costs, and task outcomes. Learn usefulness separately from truth: a previously successful procedure can become invalid when its preconditions change.

Begin with offline ablations and simple utility-aware reranking. Later investigate memory-operation policies and counterfactual credit using isolated evaluation copies. A cited memory is evidence of use, not proof that it caused success. Do not run destructive credit-assignment interventions against production memory.

Defer parametric consolidation of tenant knowledge until external memory semantics are reliable and editability, attribution, privacy, and deletion tradeoffs are understood.

## Implementation sequence

### Phase 1: First-party service contract, spaces, evidence, and identity

- [ ] Define the shared service operation/context contract and `/memory/v1` OpenAPI, SDK, MCP, and direct embedded adapters.
- [ ] Define space database/namespace binding, authorized source/producer ownership, and revisioned effective configuration.
- [ ] Implement model-free defaults and feature-to-capability admission; expose operation and readiness status.
- [ ] Implement one native Zig memory module shared by server and embedded operation; keep transport and server-only dependencies outside it.
- [ ] Map #1186 operations to the native service's engine interface with bounded background work, revision fences, cancellation, and publication status.
- [ ] Specify the removal/cutover of Rust/WASM and Go memory implementations; add a one-time data importer only for an identified need.
- [ ] Define versioned source/evidence and ontology contracts.
- [ ] Add idempotent event ingestion and explicit producer versions.
- [ ] Preserve source versions/spans, speaker attribution, and access scope.
- [ ] Implement the service's entity lifecycle through managed extraction/resolution using #1186, without reproducing the old manual entity-maintenance subsystem.
- [ ] Define source/evidence identity and provenance for the new service independently of legacy memory IDs and layouts.
- [ ] Qualify homonym, alias, and access-control behavior.

### Phase 2: Temporal assertions

- [ ] Implement assertions, modality, validity/recorded time, and lifecycle.
- [ ] Define predicate cardinality and applicability semantics.
- [ ] Implement evidence-backed reconciliation and a current-state view.
- [ ] Record revision-fenced decisions and explicit re-evaluation.
- [ ] Handle out-of-order evidence, source corrections, disputes, and retractions.

### Phase 3: Recall and structural answers

- [ ] Add task-aware, token-budgeted recall with unified reranking.
- [ ] Carry temporal status, citations, uncertainty, and freshness into evidence rendering.
- [ ] Support exact paginated structural queries and explicit completeness semantics.
- [ ] Compare against current memoryaf, simple hybrid RAG, and mem0 under controlled settings.

### Phase 4: Consolidation and learning

- [ ] Add derived episode/thematic summaries with dependency tracking.
- [ ] Add procedures with preconditions and supporting outcome records.
- [ ] Propagate changes beyond finite grouped replay horizons through explicit dependencies.
- [ ] Add offline utility tuning and evaluate learned management only after establishing baselines.
- [ ] Qualify remote and local/Lite execution separately where supported.

### Product cutover and retirement

- [ ] Switch Codex and docsaf consumers to the new service contract and disable their superseded memory paths.
- [ ] Remove `extensions/memoryaf` and `go/pkg/memoryaf` as supported implementations, including memory-specific packaging, standalone servers, fallback paths, and obsolete dependencies.
- [ ] Replace affected examples and documentation with `/memory/v1`, SDK, MCP-adapter, and native embedded usage.
- [ ] If data import is required, execute a bounded import/cutover with no continuing dual-write or synchronization contract.
- [ ] Run the new service acceptance suite without depending on either retired implementation.

## Evaluation plan

Use complementary layers rather than one leaderboard score:

| Layer | Evidence / tests |
| --- | --- |
| Stored-state correctness | FEB-30-style fact trajectories; inspect assertions after every ingestion |
| Retrieval | Supporting-evidence recall/precision, exact identifiers, entity ambiguity, scope compatibility |
| Temporal/context reasoning | LongMemEval-V2, CAME-Bench, current versus historical state |
| Memory use in actions | Mem2ActBench and real coding/operations tasks |
| Lifecycle correctness | Selective forgetting, retraction, source drift, concurrent changes |
| Operational correctness | Restart/replay, stale callbacks, merge/unmerge, derived visibility, deletion without resurrection |
| Efficiency | Ingestion/maintenance cost, inference usage, latency, storage, context tokens |

For comparisons, pin system versions and hold extraction/answer models, token budgets, prompts, and judging rules constant where possible. Record native product configurations separately from controlled comparisons. Include no-memory, full-context where feasible, simple hybrid retrieval, current memoryaf, and incremental feature ablations. Freeze held-out tests before tuning and verify that the memory tool actually executed.

Do not claim that improved QA alone proves correct memory maintenance. Conversely, correct stored assertions do not prove successful retrieval or action grounding.

## Acceptance criteria

- [ ] Exactly one native Zig memory-policy implementation serves HTTP, SDK, MCP, and embedded consumers; neither legacy memory implementation is a supported backend or fallback.
- [ ] Embedded memory runs without an HTTP server, Wasmtime, the Go memory server, or the Rust memory extension package.
- [ ] Codex/docsaf consumers and shipped documentation target the new service; obsolete memory implementations and memory-specific deployment paths are retired.
- [ ] `/memory/v1`, SDK, MCP, and embedded adapters share one policy implementation and equivalent authorization/lifecycle semantics.
- [ ] A minimal space stores evidence/external assertions, supports history and explicit corrections, and performs lexical recall with no model installed or loaded.
- [ ] Enabling a feature validates only the roles required by its admitted producer path; missing/unsupported dependencies return clear configuration errors.
- [ ] One qualified model can fill multiple roles; unused/disabled roles do not impose runtime dependencies.
- [ ] Space inspection exposes effective versioned bindings, capabilities, producer ownership, and readiness without disclosing credentials.
- [ ] External/managed/hybrid modes prevent duplicate authoritative extraction and preserve attributed externally generated claims.
- [ ] Async acceptance, reconciliation completion, and search visibility are distinguishable; retries are idempotent and failures cannot become apparent success/empty recall.
- [ ] Configured fallbacks/cascades report the actual execution path; no silent model substitution or implicit capability downgrade occurs.
- [ ] Binding/policy changes are explicit fenced generations; embedder transitions do not mix vector spaces or silently rewrite historical judgments.
- [ ] The deployment-guidance example yields correct current and historical answers despite late import of old evidence.
- [ ] Proposed/hypothetical statements do not silently become established facts.
- [ ] Predicate cardinality and applicability allow legitimate simultaneous values.
- [ ] Every derived assertion/procedure can identify its supporting evidence and producing policy/version.
- [ ] Source edits, retractions, deletions, and identity corrections reconcile dependent outputs without removing independent support.
- [ ] A source correction outside the grouped horizon invalidates affected long-lived conclusions through explicit dependencies.
- [ ] Concurrent reconciliations cannot silently overwrite newer accepted decisions.
- [ ] Restart/replay reuses committed decisions and cannot resurrect deleted or superseded contributions through stale callbacks.
- [ ] Same-name distinct entities have an explicit safe path; uncertainty is exposed rather than merged away.
- [ ] Structural queries return complete accessible sets or explicitly indicate pagination/truncation/incompleteness.
- [ ] Historical status comes from authoritative version state, not merely the retrieved subset.
- [ ] Private evidence is not exposed through lookup, graph expansion, entity inspection, or derived summaries/procedures.
- [ ] TTL expiration, historical supersession, identity suppression, and explicit forgetting have distinguishable tested semantics.
- [ ] Published comparisons include state correctness, downstream task quality, latency, and total ingestion/maintenance/recall cost.

## Recommended first deliverable

Deliver the native Zig memory module with a model-free space, idempotent external evidence/assertion ingestion, lexical recall, operation/readiness status, and matching `/memory/v1` and direct embedded semantics. Then enable one qualified managed profile and exercise an evidence-backed temporal assertion example through #1186's producers/resolvers using deterministic state-evolution fixtures and the Codex consumer tests. The new service is the sole implementation target; retirement of Rust/WASM and Go memory services is part of product completion.

## Compatibility with the Codex Antfly integration

Related implementation: [antflydb/codex ANTFLY_INTEGRATION.md](https://github.com/antflydb/codex/blob/antfly/integration/ANTFLY_INTEGRATION.md), updated against [pinned revision a2d59cfb01](https://github.com/antflydb/codex/blob/a2d59cfb01/ANTFLY_INTEGRATION.md) and the [October 21 integration review](https://github.com/antflydb/antfly/pull/1227#issuecomment-6327590919). The earlier design investigation used a0793a7b; it is not the current integration baseline. This document contains both an original plan and subsequent as-built findings; prioritize the as-built contracts and verify actual code before migrating.

The Codex integration is a reference implementation and potential first consumer of this memoryaf work. It already implements much of the application-level behavior this issue aims to generalize:

- Antfly-backed relational application state plus document tables for searchable history, approvals, and notes, with embedded and remote backends.
- Turn-based claims with thread/turn/cwd provenance, incremental extraction watermarks, leases, retry budgets, within-thread replacement/retraction, and cross-thread supersession.
- Provisional versus settled claims; active-session claims rank lower and cannot displace settled cross-thread knowledge until settling.
- Canonical concepts, query-time resolution, durable synonym merges, descriptions derived from claims, and concept relations.
- Rationale/lesson/constraint `motivates` links and `rejected_in_favor_of` links for alternatives.
- Hybrid search and graph expansion, personalized PageRank with a fallback, and optional historical retrieval.
- Extraction/decision logs and labeled fixtures for supersession, triage, concept identity, relations, and approvals.

### Component boundary

`codex-rs/ext/antfly` (`codex_antfly_extension`) is a **Codex runtime extension**, distinct from the Antfly-hosted Rust/WASM memoryaf package being retired. Codex becomes a consumer of the native memory service, using direct embedded bindings locally and `/memory/v1` remotely. Its Rust client/runtime adapter remains valid; it must not retain an independent generalized memory-policy engine or depend on the retired memoryaf WASM ABI.

Keep Codex session lifecycle, thread persistence, queue/goals, approval enforcement, and runtime credentials in Codex. Move deliberately generalized memory policy into the shared service with authorized engine access. Start Codex in external or hybrid processing mode rather than requiring a service-side generator for its authenticated extraction calls. Approval outcomes can provide evidence/telemetry, but approval authority and memory confidence are separate contracts.

### Preserve these integration contracts

1. **One owner for each extraction path.** The integration deliberately separates prompt-memory stage 1 from the near-real-time claim pipeline. Stage 1 remains the path to `MEMORY.md`; claims have their own extraction pipeline. Migration must select one authoritative claim producer rather than enabling both Codex and background memoryaf extraction over the same input.
2. **Filesystem note authority.** `AntflyMemoriesBackend` indexes changed notes and removes deleted ones; files remain the source of truth. Memoryaf should preserve source references and reconciliation semantics unless an explicit migration changes ownership. Do not silently take over writing `MEMORY.md`.
3. **Credentials and execution policy.** Current claim extraction uses Codex's authenticated model client. The document explains that ChatGPT-account-backed generation is not a table-background producer or embedded-library credential path. #1186's grouped/stateful producers remove some processing limitations, not this authorization boundary. Support externally produced, attributed claims or an explicitly configured permitted generator; do not forward session credentials into unattended background enrichments.
4. **Evidence selection.** Preserve the current exclusions of reasoning, developer messages, and injected host context from memory extraction, along with secret redaction, tool-output limits, memory-mode gates, and ephemeral-session policy.
5. **Settling is a separate axis.** Retain provisional/settled publication stability separately from active/superseded/retracted truth lifecycle and valid/recorded time. A debounce timer or settled session does not prove an assertion is true.
6. **Forgetting without resurrection.** The integration preserves extraction progress when claims are cleared so old history is not immediately re-extracted. Preserve an equivalent durable suppression/progress contract across backfill, model/policy changes, restart, and migration. Fork/revert lineage requires explicit inherited-evidence and retraction rules.
7. **Retain semantic relationships.** Add `motivates` and `rejected_in_favor_of` to the software-team ontology pack; preserve thread/turn attribution and historical links rather than flattening the existing claim graph.
8. **Embedded/remote qualification.** Qualify equivalent memory semantics through direct native bindings and `/memory/v1`, including SQL/document publication, resolver APIs, and backend capabilities. Do not infer equivalence from matching JSON alone. Core application state remains owned by Codex's existing stores.

### How #1186 changes the integration's open work

- The integration's grouped-history request (#1147) maps directly to #1186's ordered grouped sources. Reuse that processing instead of re-materializing complete turns in each consumer, while keeping the finite replay horizon explicit.
- Its request for generator-backed resolver adjudication and description assets now has corresponding #1186 contracts. Port and rerun the actual concept fixtures before replacing the existing judge; the document reports local typed models near chance on those tasks.
- The integration now has reversible curation, cascade suppression, attributed proposals and reject-with-distinction on #1233. The native external verdict contracts preserve those behaviors rather than replacing them with automatic similarity merges.
- Suppression can help with generic concepts, but identity suppression is different from source-content deletion or forgetting.
- Native derived entity views now follow canonical identity reverse indexes and regenerate from retained assertions. This removes the separate manual description-table rebuild from the service design; Codex must still switch its consumer and explicitly reconsider previously committed identities when appropriate.

### Remaining design gaps beyond the Codex implementation

The proposed assertion model adds structured subject/predicate/value, explicit scope/applicability, modality and epistemic role, valid/recorded time, source-authority policy, exact structural queries, dependency-aware long-lived procedures, and direct stored-state evaluation. Current cross-thread supersession uses a small similarity-selected candidate set and should not be treated as complete conflict detection. Model thresholds measured on the integration's small fixtures are baselines to preserve and expand, not universal precision guarantees.

### Integration acceptance additions

- [ ] Run the existing Codex extraction, supersession, concept-merge/relation, and retrieval fixtures through the proposed memoryaf-backed path before switching writers.
- [ ] Preserve rationale/alternative links, source IDs, turn provenance, provisional behavior, and historical queries through migration.
- [ ] Verify note-file changes/deletions reconcile correctly while files remain authoritative.
- [ ] Verify no duplicate extraction and no resurrection after forgetting, including backfill and restart.
- [ ] Test fork/revert lineage, imported old threads, and later-arriving contradictory evidence.
- [ ] Exercise direct embedded and `/memory/v1` remote integration paths, including model-free external assertions, shared native policy semantics, explicit capability checks, and no HTTP/WASM runtime dependency in the embedded path.
- [ ] Keep the integration's fresh-CODEX_HOME persistence gate and application-state behavior unchanged by the memory migration.


### Main integration and constrained-output follow-up (#1223)

The native memory branch incorporates main through `dd49f55102`, including merged
[#1223](https://github.com/antflydb/antfly/pull/1223). Memory continues to use shared
constrained-producer schema admission, exact-number validation, bounded JSON
ownership, and provider qualification; it does not maintain a second schema
validator, prior-artifact scanner, renderer, or credential sanitizer. Memory's
explicit bounded evidence/context plan remains independent of the generic prior
artifact window. Its model bindings contain qualified role identities rather than
arbitrary provider credential/configuration trees. HTTP and embedded request
boundaries preserve numeric tokens before typed native parsing, matching #1223's
lossless numeric contract. HTTP, MCP and embedded adapters serialize dynamic memory
bodies with the shared iterative JSON writer, so deeply nested invalid fields
reach normal typed admission instead of exhausting the serialization stack.
Regressions exercise 6,000 nested levels with an extreme numeric literal through
HTTP and the embedded C ABI.

Main now uses topology versions 36–37 for curation provenance and durable proposal
queues. Native service ownership therefore requires version **38** independently
of resolver-namespace support (37); older peers cannot admit private memory tables.
Memory identity corrections forward authenticated actor, revision and time to
shared curation. Free-text reasons are stored separately in bounded, entity-bound
curation records; identity policy retains decision metadata without reason text.
Source/evidence forgetting and expiry retire a reason when either bound identity
loses its support, while preserving applicable redirect/suppression decisions.
Global forgetting immediately removes identity policy; resumable physical cleanup
removes curation content and legacy inline policy behind the permanent fence.
Forgotten identities reject new curation content, including after restart. Suppressing an identity with absorbed aliases defaults to
`absorbed: reject`; callers explicitly choose `release` or `suppress`. Shared
curation removes the affected redirects. Unsuppressing a target does not silently
reattach or unsuppress its former aliases; restoration is explicit and revision
fenced. These choices share the resolver semantics introduced on main.


MCP must preserve exact JSON numeric tokens at the JSON-RPC ingress, tool argument
and structured-result boundaries, before memory's typed admission. Both HTTP and
standalone JSON-RPC transports use the shared lossless parser and iterative
writer. Integer count/dimension arguments accept exact integral decimal and
exponent forms within bounds; fractional, negative and overflowing values reject
rather than truncate or silently default. Regressions cover values beyond binary64
precision, extreme exponents, deep nesting, exact JSON-RPC IDs, and an actual MCP
memory assertion round trip.


### Bounded cleanup and outcome ownership

Entity admission accepts at most 256 direct evidence references, through the same
native gate for external observations and managed publication. Cleanup evaluates
one artifact atomically within the shared 65,536-unit work budget. If a later
artifact exhausts the remaining budget, it rolls back only that artifact's overlay
and state, commits the completed prefix, and resumes at the prior cursor. Both
full-table scrubbing and dependency propagation use this rule; fixed page sizes
alone do not bound support traversal. Earlier oversized entity rows can therefore
be scrubbed without rejecting every attempt at the same page.

Outcomes are immutable, principal-owned feedback records. An identical retry
preserves the original owner and recorded time; changed content under the same ID
returns `MemoryOutcomeConflict` (HTTP 409 / embedded version conflict). A different
unauthorized principal cannot overwrite a prior outcome. Detail text is UTF-8 and
bounded to 64 KiB. Retiring an outcome records a durable ID fence before removing
its content, so source retirement cannot silently release its identity for reuse.
The global forgetting fence rejects new outcomes, including empty-citation
feedback. Receipt replay remains content-free and cannot recreate retired records.

### Shared native provenance and recorded explanations

The [shared provenance requirements in issue #1211](https://github.com/antflydb/antfly/issues/1211#issuecomment-6307482701)
are implemented as native contracts in `common/provenance.zig`. Memory and ordinary
records share versioned resource references: database, namespace, logical table,
immutable table incarnation, kind, row ID, committed revision, and optional content
digest. The optional storage binding permits server table renames while reads
recheck permission against the current logical table. Recreating a table with the
same name cannot resolve an old reference to new content.

An observation may supply `evidence.resource_selector` with a table, row ID and
optional expected revision. Admission authorizes the ordinary row, captures its
exact JSON, revision and digest, and replaces the selector with an immutable
`resource` reference. Callers can also submit an already pinned reference. Supplied
text must match the authorized snapshot. An ordinary row remains authoritative;
importing evidence is an explicit memory admission operation. Merely storing a
trace never promotes its contents into memory.

Native consequential mutations atomically record a versioned decision alongside
publication and the operation receipt. Decisions distinguish the authenticated
initiator and execution principal from caller-declared agents and approvers.
They capture action, status, times, configuration generation, ontology and policy
versions, target versions, and optional workflow and trace/span origins. Caller
metadata cannot claim authenticated actor assurance. Exact configuration records
preserve model and extractor settings. Normalized target, event and artifact
indexes support bounded traversal without rescanning all decisions.

`POST /memory/v1/spaces/{space}/memories/{id}/explain` explains an assertion;
`POST /memory/v1/spaces/{space}/explain` accepts a resource reference, including an
ordinary record, relationship or trace. `revision`, `known_at` and `valid_at`
separate recorded history from temporal applicability. Explanations traverse typed
`retrieved`, `used`, `supported_by`, `generated_by`, `approved_by`, `supersedes` and
other declared edges without treating retrieval as evidence of use. Native
assertion support alternatives remain separate in the assertion payload. Outcome
usage links select memory as known when the outcome was recorded.

Every resource edge is authorized separately. The HTTP reader checks current
ordinary-table permissions and rejects unsupported row-filter policies; memory
permission alone does not grant business-table access. Embedded reads use the
local database owner's authority. Missing and denied targets share a redacted
edge result. Detailed trace expiration leaves the minimal native admission
decision available, with a missing-detail flag. Changed ordinary rows cannot
silently replace historical evidence: current recall excludes the old support,
while authorized historical explanations can use the admitted exact snapshot.
Deletion retires dependent evidence; loss of access hides it from that reader
immediately. A worker credential denied access cannot erase another principal's
support. Independent support can keep an assertion available. Background checks use the saved execution identity,
scan at most 16 watched sources per quantum, and revisit sources every 30 seconds;
foreground reads recheck source state immediately.

Explanations contain recorded justification only, never a model-generated
reconstruction. Node, depth and encoded-byte limits produce explicit truncation.
Explanation rendering measures each node through an allocation-free JSON counter, maintains
an additive size for the retained prefix, and emits the final graph once into an
exactly sized buffer. Removing nodes never retains discarded graph encodings;
serialization work is linear in visited payload bytes plus the final response.
Deadline/cancellation checks accompany node sizing and truncation. Graph lookup
and authorization remain bounded independently by the node/depth/work limits.
Historical snapshot lookup pages the immutable resource-version index independently
of graph node limits. It examines at most 4096 candidate bindings across the entire
explanation, rechecking access, knowledge time, digest and retention for each.
Rejected payloads use reusable page scratch storage; only a selected capture survives
the page. Lookup budget exhaustion produces a truncated payload and incomplete history,
never a false missing/changed result. Retirement removes the capture's lookup and watch
bindings atomically, and cleanup reclaims legacy orphan bindings.
Ordinary source snapshots retained during traversal are limited to 64 KiB; larger rows report
truncated payload and incomplete history. Transports release temporary row buffers
after each lookup. Admission rejects an oversized source instead of storing a partial
evidence snapshot.
`missing_evidence` and `incomplete_history` prevent an absent source or an ordinary
row without a provenance envelope from appearing to have complete lineage.
Ordinary rows can carry a versioned `provenance` envelope of typed links; it is
labeled source-declared metadata, not a native authenticated attestation. This
service does not invent historical catalog decisions for ordinary tables.

Traces remain ordinary tables using the versioned `TraceEvent` schema. The Python
SDK supplies `resource_reference` and `trace_event` helpers, and the native, HTTP
and SDK memory clients expose explanation. A separate trace service and a full
cross-product Explain UI are outside this implementation's agreed scope.

Retention is distinct from temporary identity suppression. Suppressing an entity
can hide a memory without destroying outcome feedback, so unsuppressing restores
that feedback. Assertion admission checks the same retained, authorized entity
references for subjects and typed objects, including durable forgetting fences
after physical entity cleanup.

### Retention, temporal closure, and integer transport

Persistent writes use a retention projection distinct from the caller's read
projection. Cleanup scrubs rationale and correction provenance only when their
support is irreversibly retired. Temporary entity suppression, source version
changes, or a worker's denied source read can redact a response without erasing
the durable record. The same distinction applies recursively to model decision
inputs and correction evidence. View caches include the retention mode.

`known_at` is enforced throughout explanation traversal, including evidence,
entities, decisions, operation snapshots, configurations, table creation and
outcomes. Future decisions and their edges are omitted. Legacy artifacts whose
observation time cannot be established are reported as incomplete history rather
than presented as known facts. Ordinary rows require a retained admitted snapshot
to establish when memory knew their exact version; a source-declared timestamp
is not an Antfly knowledge timestamp.

Operation status endpoints remain live receipts. Explanation references select
immutable operation snapshots published in the same transaction as each receipt
update. Their reference revision identifies that publication; the receipt's own
revision remains its accepted revision. Each snapshot links the decision that
published it, so later completion cannot rewrite an earlier explanation.

Captured ordinary snapshots are indexed by database, immutable table incarnation,
resource kind, row ID and revision. Logical names and storage routing hints do
not participate in that identity. Reads still authorize the current resource,
and a supplied digest must match the captured digest. Optional digest spelling
and routing metadata therefore cannot hide an otherwise valid historical snapshot.

The TypeScript HTTP and embedded memory clients preserve safe integers as
`number` and decode larger JSON integers as `bigint`. Requests serialize `bigint`
as exact numeric JSON and reject already-rounded unsafe `number` values. The HTTP
SDK exposes typed `memoryApi` routes and `MemorySchemas` with `number | bigint`
numeric fields; its ordinary `api` routes retain their existing numeric types.
Use `stringifyMemoryJSON` when serializing a decoded HTTP memory value outside
the client. This keeps the native wire contract identical for Go, Python, Rust,
HTTP, MCP and embedded callers.


### Outcome retirement, publication coverage, and nullable numbers

Outcome explanations, retries with a new key, and physical cleanup use one
retention gate. Global forgetting and retirement of any used assertion's support
hide feedback immediately, before the cleanup cursor reaches its row. Graph
traversal redacts the same unavailable outcome. Current source authorization is
also required to read feedback; temporary source denial and identity suppression
do not authorize its physical deletion.

Configuration publication counts every durably retained assertion as a coverage
obligation. Temporary source denial pauses the build at its committed cursor and
leaves the serving generation intact. Such a pause does not consume a producer
attempt or discard pending external vectors. Restoring access lets the same build
resume. Irreversibly retired assertions can be omitted. Ontology validation uses
the same coverage rule, so access loss cannot silently exclude retained knowledge
from a configuration migration.

Nullable numeric wire fields preserve presence in generated clients. Epoch zero,
zero retention duration, and zero-valued selectors remain distinct from absent
or null fields. The OpenAPI generator derives this rule from native optional
numeric types, including nested records, rather than maintaining a list of time
field names. List and recall time filters share that contract.


### Live embedded readers and complete operation progress

Embedded ordinary-resource reads resolve their storage owner through the active
handle guard. Inference may release the connection lease and resume on a new
storage generation; no resource reader retains a pointer into the retired one.
The C ABI regression releases that lease, commits a source edit through an independent
native writer, resumes, and verifies that the existing reader sees the new generation.

Managed observations and configuration builds distinguish the source-access
prerequisite from producer failures. `MemorySourceAccessDenied` leaves the
operation pending, queued, and at its committed cursor without spending an
attempt. This applies to both primary and contextual observations. Source expiry,
forgetting, deletion, and revision drift remain terminal cancellation causes.
Restoring access resumes the same admitted operation.

Every completed advance projects phase, readiness, attempts, error code and retry
time from the target operation through one shared function. Cancellation clears
readiness and retry time. The same status reaches the live operation, advance
receipt, immutable operation snapshots and recorded decision; terminal replay
cannot turn cancellation into successful publication.

Native wire schemas publish primitive defaults, signedness, exact integer bounds
and supported 32/64-bit numeric formats. Go bindings retain presence for nullable
scalars, defaulted scalar body fields, and every optional query/header parameter.
Consequently omitted options remain omitted, explicit false overrides true, epoch
zero remains a timestamp, and the full native uint64 domain round-trips. Strict
model schemas retain required fields and bounds without injecting wire defaults.
