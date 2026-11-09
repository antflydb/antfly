# Source knowledge discovery and native processing

This implements the initial deployment modes in [#1546](https://github.com/antflydb/antfly/issues/1546), alongside SearchStream v2. Acquired source records remain authoritative. Extraction, resolution, promotion, relationship ownership and curation remain native Antfly operations. Source synchronization acquires content; it never becomes an inference or graph execution engine.

## Declarations and ownership

A source may declare `knowledge` independently of its search streams:

```json
{
  "knowledge": {
    "version": 1,
    "mode": "instance_one_audience",
    "audience": "research",
    "entities": [{
      "name": "facts", "index": "facts_graph-PIN",
      "resolver": "facts", "table": "entities",
      "namespace": "source/SOURCE_UUID/facts/",
      "source_artifact": "facts_extraction-PIN",
      "resolution_artifact": "facts_resolution-PIN",
      "location_field": "provenance"
    }],
    "graphs": [{
      "name": "facts", "index": "facts_graph-PIN",
      "artifacts": ["facts_extraction-PIN"], "entities": ["facts"],
      "temporal": {"mapping": "native_intervals_v1", "history": "current_state"},
      "location_field": "provenance"
    }]
  }
}
```

Names and PIN values above are illustrative. The native declarations live in `processing.indexes`; knowledge bindings reference them rather than describe another producer language. Generic `artifacts` remain available for structured outputs without search or graph indexes. `schema_uri` and `location_field` are discovery mappings, not instructions to fetch an arbitrary schema or a claim that every output contains a location.

`BuildKnowledgeProcessing(KnowledgePreset)` builds an asset enrichment, graph source and resolver using their native schemas. Supply ordinary native `Producer`, graph `Source`, and `Resolver` configurations. The builder supports canonical fields, nested input paths and upstream artifacts; a nil producer copies structured fixture input. Production extractors must provide their native output schema, model/prompt and confidence policy. Structured outputs retain mention IDs, relation IDs, confidence meanings and applicable spans/locations from that producer. They are generated assertions, not original-source evidence.

The builder content-addresses extraction, resolution and index declarations. Changing a model or extraction recipe versions affected native resources without changing acquired-content identity. Key namespaces remain stable across those versions. Omitted/null knowledge on a source PUT preserves existing declarations for older clients. To clear managed knowledge, submit an explicit versioned declaration with empty entities/graphs and remove graph declarations from the desired processing plan; shared native resources are retired separately. Existing conflicting shared indexes are never overwritten. Prepare referenced entity tables through native administration before installing a resolver, even with promotion disabled: candidate resolution still reads its target table.

Desired-plan validation checks resolver identity, namespace/template, source membership and graph provenance before DDL. Independently administered resources may be referenced with omitted processing; their installed native declarations must still pass observation and query-time checks. Readback, not declaration presence, decides whether a binding is usable.

## Security and deployment

Knowledge is disabled by default. Initial server-side support requires an operator-enforced **one instance, one audience** boundary covering all participating source data, entity tables, graph outputs, curation, native API routes and cached projections. Every person able to reach those resources must belong to that audience. Equal source lists do not establish this boundary if row, attribute, table or curation permissions still differ.

Standalone enables this explicit deployment assertion with:

```sh
antfly-source serve --config PROFILE --state-dir STATE --public-url URL \
  --knowledge-audience research
```

The embedding Go API uses `ServiceOptions.KnowledgeAudience` or `Runner.KnowledgeAudience`. CLI `sync` and `processing` accept the same flag. This value is deliberately outside mutable source profiles and PUT bodies. Enabling it does not install authorization in the native server. The operator must isolate routing and configure native access accordingly. Do not enable it for mixed-audience deployments.

Colony reads `IMPORT_SOURCE_KNOWLEDGE_AUDIENCES` once at service creation: a JSON map from `ORGANIZATION_UUID/INSTANCE_UUID` to audience identity. An absent/invalid configuration disables knowledge. A source's audience must match its exact organization/instance entry. Hosted installation and queries require source-table administration and administration of referenced entity tables; access is rechecked after retrieval. Credentials remain runtime-owned. The dashboard displays discovery/status and offers bounded administrative inspection.

Unknown modes and mismatched/disabled audiences fail closed. Source graph/resolver processing cannot bypass this gate by omitting discovery: every installed resolver must have a matching entity binding, which drives hosted entity-table authorization. Native administration remains a separately authorized surface. There is no resolver-level security-domain implementation, multi-tenant knowledge isolation, or implicit mixed-audience promotion. Embedding-cache domains are unrelated. Current permissions continue to apply to historical queries. Native curation remains separately authorized by the existing native/gateway policy.

## Stable entities and client composition

Entity bindings expose the resolver, target table, source/resolution artifacts and a stable key namespace. Supported portable templates use normalized native mention label/text fields under that namespace. Candidate IDs, array positions and generated display names cannot define join identity. Native exact-key, prefix and ANN candidate search/scorers remain available. Native equal-score selection breaks ties by durable candidate table/key identity, independently of enumeration order. Changes to the candidate corpus or matching recipe can change decisions through native replay; clients preserve the returned canonical reference and redirect/revision evidence rather than reconstructing identity from a display name.

`EntityJoinKey` losslessly encodes an explicit deployment/local authority, table, namespace and canonical key. Use a stable native endpoint or a durable local deployment identity as authority; unrelated deployments with equal table/key strings remain distinct. It rejects keys outside the declared namespace. Equal-looking names from unrelated namespaces do not become the same entity. Clients can independently retrieve authorized graphs from different deployments and compose them locally, retaining each source's namespace, revision and evidence. Cross-namespace association is an explicit reversible client decision; it grants no additional hosted access.

`EntityResources` resolves existing native resolver inspection, render/resolve, redirects, suppressions and review routes. It returns relative references, not authority or alias contents. Read current redirect/curation revisions through those APIs; use their existing merge, unmerge, distinct, suppression and revision-fenced mutation semantics. There is no second source-specific curation store. Local compositions must revalidate current access and remove revoked evidence; persisted local associations cannot preserve access to hosted evidence.

Resolvers and indexes are native table-scoped resources. A source's discovery namespace does not turn its resolver into a row-filtered execution engine. Within an enabled one-audience table, resolution/promotion may consume all native inputs selected by the declaration. Use separate native tables/instances when matching or promoted projections require stronger isolation. The bounded source graph query below filters relationship contributions by source before expansion.

## Relationships, evidence and time

Graph bindings must name exactly their installed artifact source set. Every source carries native graph mapping context for the parent's `source`, plus these edge metadata templates:

```json
{
  "source_id": "{{ _doc.value.source.id }}",
  "parent_revision": "{{ _doc.value.source.revision }}"
}
```

For resolver-backed relations, use native endpoint resolution without `_item.source`/`_item.target` string templates: those templates can stringify a local mention object instead of selecting its canonical identity. Explicit endpoint templates are for already identified document/external endpoints.

Native `owner_document`, artifact/member identity and stable per-item `edge_id` distinguish independently supported claims, even between identical endpoints. Every portable graph source must map a stable member edge ID; the builder defaults to `{{ _item.id }}` and inputs must provide that stable ID. The builder preserves supplied edge mappings and adds parent provenance; it does not derive IDs from incidental array order. Native deletion/empty-output retirement retracts the producing document's contribution without erasing other owners' claims. Artifact evidence can be inspected through existing native document/artifact APIs under current access.

`native_intervals_v1` names the existing four relationship metadata fields:

| Axis | Inclusive lower bound | Exclusive upper bound |
|---|---|---|
| Valid time | `valid_at` | `invalid_at` |
| Knowledge time | `created_at` | `expired_at` |

Use RFC3339 instants with explicit UTC/offsets and preserve available precision. Producers/ingestion must populate these values from their actual meanings. A provider modification time is not automatically event time or knowledge time. Missing/null valid bounds are open. `known_at` requires a non-null `created_at`; a missing upper bound is open. Invalid timestamps do not match native temporal predicates.

Corrections update native projections. To preserve independently queryable historical assertions, publish separately identified retained claims with their actual knowledge intervals and retract/supersede them explicitly. Updating a mutable edge alone does not preserve its prior value. Discovery therefore advertises `history: current_state`: temporal predicates filter retained current relationship metadata, and **do not promise complete historical replay**. Historical access cannot resurrect revoked evidence.

## Bounded query, readiness and lifecycle

`BuildKnowledgeQuery` emits ordinary native traversal. `QueryKnowledge` verifies installed graph/resolver compatibility and complete/queryable readiness around execution. It pins native configuration/readiness, checks source freshness and preserves opaque native response JSON, including exact numeric evidence. An incompatible generation or expired access fails instead of returning apparently current results.

The source companion/Colony route is:

```text
POST /sources/{id}/knowledge-query
{"graph":"facts","start":["alice"],"depth":1,"limit":20,
 "valid_at":"2022-01-01T00:00:00Z","known_at":"2023-01-01T00:00:00Z"}
```

The helper limits requests to 16 starts, 8 hops and 100 returned paths, with a 30-second deadline, native shared work limits and a 2 MiB response budget. Every relationship is filtered on `/metadata/source_id` **before expansion**, not after paths/ranking/counts. Parallel claims remain distinct through `deduplicate_nodes: false`. Entity document hydration is disabled; fetch authorized entity/parent/artifact evidence separately. An optional `entity` binding derives table-qualified canonical starts from that graph's declared entity table and namespace. Arbitrary native query overrides, arbitrary table qualifiers and unbounded graph patterns are not accepted by this helper. Existing native query APIs remain available to independently authorized advanced clients.

Native graph statistics and truncation stay intact. `source_knowledge` adds binding identity and operation consistency. Unmarked native results are `unretained`; live reads do not prove absence. `require_snapshot` rejects a live/unretained result. Source acquisition completion, graph readiness, access expiry and native curation progress remain distinct.

Status observation uses the shared eight-worker index observer, five-second deadline, per-request deduplication and 32 MiB aggregate metadata admission budget. It reports building/partial/failed/unsupported honestly. Polling does not scan the corpus or fabricate per-artifact completion. Desired configuration and observed execution remain separate.

Backfill, resolver changes, graph projection, promotion and deletion reuse native durable replay and dependency invalidation. Source processing jobs accept declared extraction artifacts and retain their existing whole-table scope, bounded advancement, cancellation and table-admin requirement. Resolution artifacts are not falsely exposed as ordinary reprocessable assets. Reconfigure native resolver/curation state through native APIs to trigger re-resolution. Migrate recipes by creating new immutable resources, awaiting readiness and switching bindings; do not retire shared old indexes until their consumers are migrated. Provider resynchronization is unnecessary when acquired content remains valid.

## Local qualification

Against a disposable loopback native owner:

```sh
GOWORK=off go -C go/pkg/sources run ./examples/source-knowledge \
  --endpoint http://127.0.0.1:8080/db/v1 --catalog source-knowledge-catalog.json

GOWORK=off go -C go/pkg/sources run ./cmd/antfly-source knowledge \
  --catalog source-knowledge-catalog.json \
  --source 11111111-1111-4111-8111-111111111111 \
  --knowledge-audience fixture --graph facts --entity facts \
  --start source/11111111-1111-4111-8111-111111111111/facts/person/alice
```

The example copies explicitly synthetic structured facts, performs native resolution/graph publication, prints a bounded query and writes a portable catalog. It leaves its uniquely named table for inspection. It needs no Colony or model download.

`TestNativeSourceKnowledge`, included in `scripts/ci/check_source_native.py`, checks actual public native APIs for table/readiness validation, temporal filtering, source separation, parallel owner evidence, owner deletion and empty-output retraction. Shared Go tests cover declaration/version rejection, unstable keys, namespace collisions, alias boundaries, query limits, recipe mismatches, numeric evidence and unchanged acquisition identity. Colony adds deployment/tenant fencing and table-admin regressions; dashboard tests cover stale binding removal.

Extraction accuracy, promotion quality and model/hardware throughput still require deployed-data qualification. Deterministic fixtures and the microbenchmarks are not model-quality or full historical-replay evidence. Unsupported mixed-audience operation stays rejected while a future relationship/evidence-aware security model is developed.
