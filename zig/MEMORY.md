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
requires topology protocol 36; old metadata must reject it before private data is
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
resource ID and exact request body. A conflicting reuse fails. Durable managed
operations expose evidence acceptance independently of assertion readiness,
bounded attempts, errors, retry times and configuration generation. Publication
uses the same revision predicate after inference; a stale result cannot commit.
Model calls use existing qualified Antfly producers and embedders, without
persisting consumer session credentials. Each enabled role is qualified
independently even when one model serves several roles.

Structural enumeration paginates the accessible set; ranked recall is budgeted
and reports omitted candidates, scan limits, revisions and actual retrieval
path. A truncated ranking is never a completeness claim. Context and authoritative
lifecycle filters apply before rendering citations. Private provenance cannot
be widened by a team-visible derived assertion. Forgetting erects durable
non-resurrection fences immediately in every source-backed view, including entity
lookup, identity resolution and enumeration; bounded physical cleanup is separate.

## Public operations and bounded execution

All timestamps are signed milliseconds since Unix epoch. `valid_at` asks about
business validity; `known_at` selects recorded assertion and premise revisions.
Forgetting and retention still apply to historical reads. `history` is the exact
audit view; current recall excludes stale dependencies and historical guidance.
Structural enumeration and ranked recall share lifecycle and validity filtering.
An explicit `valid_at` can select a superseded version in its original interval;
it never requires `include_history` merely to reconstruct that valid state.

HTTP routes live under `/memory/v1/spaces/{space}`. Use
`X-Antfly-Database` and `X-Antfly-Namespace` for catalog scope (defaults:
`default` and `public`). These headers select resources, never identity or grants.
MCP's native `memory` tool takes `{space, request}`; request contains `action`,
`id`, `body`, `idempotency_key`, and optional `database`/`namespace`. It calls the
same native server ingress as HTTP. Embedded SDKs call `antfly_db_memory_json`
with the action/body envelope directly, with no HTTP server or Wasmtime runtime.

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

Related implementation: [antflydb/codex ANTFLY_INTEGRATION.md](https://github.com/antflydb/codex/blob/antfly/integration/ANTFLY_INTEGRATION.md), inspected on October 19, 2026; [pinned revision a0793a7b](https://github.com/antflydb/codex/blob/a0793a7bf47254e0c29a95d0430d494b542a3da4/ANTFLY_INTEGRATION.md). This document contains both an original plan and subsequent as-built findings; prioritize the as-built contracts and verify actual code before migrating.

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
- Reversible curation can address the integration's inability to undo merges, subject to migration of existing corrections and qualification of native/Lite versus server namespace behavior.
- Suppression can help with generic concepts, but identity suppression is different from source-content deletion or forgetting.
- Description regeneration remains a dependency problem: the integration stores descriptions in a separate table because its evidence edges live on claim/note tables. #1186 does not automatically turn that layout into managed cross-table description assets. Qualify a migration and invalidation path, including explicit re-resolution where needed.

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
