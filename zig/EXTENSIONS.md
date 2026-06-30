# Postgres-Style Extension System

## Context

PostgreSQL extensions are not just dynamic libraries. The useful product
contract is a managed package of database objects with install, update,
dependency, ownership, dump/restore, and drop semantics.

The closest Antfly equivalents today are split across:

- metadata table records in `zig/pkg/antfly/src/metadata/table_manager.zig`
- per-table index metadata in `TableRecord.indexes_json`
- shard-local durable index, enrichment, and resolver catalogs in
  `zig/pkg/antfly/src/storage/db/catalog/index_manager.zig`
- provider registries in `zig/pkg/antfly/src/common/provider_registry.zig`
- embedded DB lifecycle APIs in `zig/pkg/antfly/src/embedded/db.zig`

That gives us many extension-like object types, but not a single extension
catalog or lifecycle. The metadata note also calls out table/index lifecycle as
work that should move out of API-local state into metadata, which is the same
boundary an extension system needs.

Reference PostgreSQL behavior:

- `CREATE EXTENSION` runs an extension script and records the identities of the
  created objects so `DROP EXTENSION` can remove them together:
  https://www.postgresql.org/docs/current/sql-createextension.html
- extension packages are described by control files, install scripts, optional
  update scripts, dependencies, schema relocation options, and trust/superuser
  policy:
  https://www.postgresql.org/docs/current/extend-extensions.html
- C extensions are precompiled shared libraries loaded by path with an ABI
  compatibility marker:
  https://www.postgresql.org/docs/current/xfunc-c.html
- index access methods are a stable core-to-extension interface that lets new
  index types exist outside core:
  https://www.postgresql.org/docs/current/indexam.html

## Goals

- Give Antfly a durable extension lifecycle comparable to PostgreSQL:
  `CREATE EXTENSION`, `ALTER EXTENSION UPDATE`, `DROP EXTENSION`, dependency
  checks, and object membership tracking.
- Make built-in features and third-party features use the same registration
  shape where practical.
- Let extensions define data shape, not only metadata. Extension manifests
  should be able to declare table/document/row shapes, generated artifact
  shapes, extension-owned state relations, indexes, and app-facing request and
  response schemas.
- Support extension-owned Antfly objects: table schemas, indexes, enrichments,
  resolvers, provider configs, analyzers/tokenizers, query functions, graph
  algorithms, and eventually index backends.
- Make dump/restore and backup behavior deterministic: dump extension
  references plus user-owned extension configuration data, not a pile of loose
  generated objects.
- Keep hosted and embedded profiles viable. Extension install must converge
  through metadata and be replayable onto all relevant shards.
- Make native-code loading an optional later capability, not the foundation of
  v1.
- Preserve hot-path performance. Extension boundaries must be explicit about
  allocation, serialization, batching, and runtime isolation costs so core query,
  indexing, and replay paths can remain competitive with built-in code.

## Non-Goals

- Do not build a SQL language just to mirror PostgreSQL syntax exactly.
- Do not allow arbitrary native shared libraries in hosted clusters in v1.
- Do not make extension objects bypass metadata reconciliation.
- Do not make every internal hook public immediately. Start with a narrow,
  stable ABI.
- Do not route hot per-document, per-token, per-vector, or per-posting work
  through generic JSON or sidecar calls.

## Proposed Model

### 1. Extension Package Format

Use an Antfly package manifest rather than PostgreSQL `.control` files:

```json
{
  "name": "antfly_text_extras",
  "version": "1.0.0",
  "description": "Extra analyzers and query helpers",
  "dependencies": [{ "name": "antfly_core", "version_requirement": ">=1.0.0" }],
  "trusted": true,
  "relocatable": false,
  "artifacts": [{ "kind": "wasm", "path": "runtime/extension.wasm" }]
}
```

Each released version ships:

- `extension.json`: declarative object definitions
- optional update files named by source and target version
- optional runtime artifact, preferably WASM/component model for hosted safety
- optional native shared library only for local/embedded or explicitly trusted
  deployments

The declarative manifest should be enough for v1. Runtime code should be a
capability added to specific object types, not required for install.

Treat the filesystem as the v1 package source and cache, not as the installed
state. The key product distinction is:

```text
available package = files/artifacts Antfly can resolve
installed extension = metadata/catalog state Antfly reconciles
```

The package name, version, and digest are declared by `extension.json`. The
filesystem path is not part of package identity and must not be used as the
version authority. This mirrors the useful part of PostgreSQL's extension model:
stable extension names plus versioned metadata/update artifacts, without making
the source tree layout the lifecycle contract.

For embedded and local development, a configured extension source directory is
enough. The canonical source layout is flat by package name:

```text
$ANTFLY_HOME/extensions/
  memoryaf/
    extension.json
    updates/
      1.0.0--1.1.0.json
    runtime/
      extension.wasm
```

For hosted deployments, use an operator-controlled, read-only,
content-addressed package store:

```text
$ANTFLY_HOME/extension-store/sha256/<digest>/
  extension.json
  runtime/
    extension.wasm
```

`CREATE EXTENSION memoryaf` should not mean "run whatever is at this path." It
means resolve the package from configured trusted sources, verify its manifest,
digest/signature, version, and capabilities, then write installed-extension
metadata. Metadata remains the source of truth for installed extensions.

The v1 scanner accepts only canonical `<name>/extension.json` source packages or
content-addressed `sha256/<digest>/extension.json` packages. Other nested
`extension.json` files are ignored rather than treated as alternate package
layouts.

### 2. Extension Catalog

Add a metadata-owned catalog, not shard-local ad hoc rows:

- `extension_definitions`: available packages and versions known to the
  cluster, including content digest and trust policy
- `installed_extensions`: database/table-scope install records
- `extension_members`: every Antfly object created by an extension
- `extension_dependencies`: installed dependency graph
- `extension_config_relations`: user-owned configuration data that should be
  included in backup/export, analogous to PostgreSQL extension configuration
  tables

Antfly does not have PostgreSQL's schema/database split. Suggested scopes:

- `cluster`: providers, shared analyzers, shared model adapters
- `table`: schemas, indexes, enrichments, resolvers, graph algorithms
- `embedded_db`: local-only installs for embedded users

The first durable implementation should live in the metadata state path because
hosted shards need one canonical source of desired extension objects.

### 3. Data Shape Contract

Extension metadata is the control plane, but it is not the whole product
contract. A useful extension needs to own the shape of the data it creates,
reads, and exposes. That means extension manifests should define both catalog
objects and data contracts:

- primary table/document/row schema contributions
- extension-owned state tables or relations
- generated artifact schemas, such as embeddings, summaries, edge lists, or
  workflow checkpoints
- index input/output shape and supported query predicates
- MCP tool and HTTP endpoint request/response schemas
- migration rules for changing those shapes across extension versions

This makes an Antfly extension closer to a packaged application schema than a
bag of side effects. For example, a `memoryaf` extension should not only create
an MCP tool named `recall`; it should also declare the memory record shape, event
log shape, optional graph edge shape, generated summary/embedding artifact
shape, and the indexes those shapes require.

Treat shape declarations as extension-owned objects with durable identities and
versioned compatibility rules. The metadata catalog stores which shape version is
installed; shards materialize the storage/index/runtime consequences. Direct
data writes still go through normal Antfly validation, but validation can be
derived from the installed extension shape.

Extension-owned objects that consume or publish a shape should store the
referenced `shape_name` and `shape_version` on their extension member record.
That lets backup/export, update planning, and drop protection preserve the
dependency between a generated artifact, MCP tool, index, or enrichment and the
shape contract it was installed against.

For implementation v1, a table-scoped `data_shape` member uses the existing
Antfly table schema JSON as its `schema_json`/`owner_metadata_json`. That keeps
extension write validation on the same parser and runtime checks as native table
schemas, including `enforce_types`, document schemas, dynamic templates, and
future compatible additions. A generic JSON Schema/OpenAPI-based shape language
can be layered on later, but it should compile down to this table-validation
contract before it can own table writes.

Shape updates need the same discipline as code updates:

1. Validate the new shape against existing stored data and dependent objects.
2. Decide whether the update is additive, compatible, or requires backfill.
3. Stage required backfill or rewrite work before marking the extension ready.
4. Keep old readers/writers compatible until the migration reaches a stable
   point, or reject the update.

For v1, prefer declarative shape changes only: add fields, add generated
artifacts, add indexes, and introduce extension-owned state relations. Imperative
data rewrites can come later behind explicit migration hooks.

### 4. Install, Update, Drop

Add an operation layer:

- `CREATE EXTENSION name [VERSION version] [SCOPE cluster|table]`
- `ALTER EXTENSION name UPDATE [TO version]`
- `DROP EXTENSION name [CASCADE|RESTRICT]`
- `SHOW EXTENSIONS`
- `SHOW EXTENSION name OBJECTS`

These can be HTTP/OpenAPI operations first and CLI commands second. SQL-like
syntax can be a compatibility layer later.

SQL extension DDL should be a compatibility adapter over the canonical
extension lifecycle, not a second extension implementation. The SQL parser
should stay responsible only for syntax and typed catalog intent. Execution
should live behind a small adapter, for example `extensions/sql_adapter.zig`,
that maps `ExtensionCatalogPlan` values onto lifecycle calls:

- `CREATE EXTENSION` maps to `extensions.lifecycle.installOnService`.
- `ALTER EXTENSION ... UPDATE` maps to `extensions.lifecycle.updateOnService`.
- `DROP EXTENSION` maps to `extensions.lifecycle.dropOnService`.

The first supported SQL subset should be deliberately small:

```sql
CREATE EXTENSION [IF NOT EXISTS] name [VERSION 'version'];
ALTER EXTENSION name UPDATE [TO 'version'];
DROP EXTENSION [IF EXISTS] name [RESTRICT|CASCADE];
```

PostgreSQL details such as `WITH SCHEMA`, relocation, extension object
membership edits, and multi-extension drop lists should fail closed until they
have native lifecycle semantics.

Use the SQL extension name as the installed extension name. Resolve the package
name separately from the registered package catalog. Resolution first matches
the package name, then manifest-declared `sql_names` aliases for PostgreSQL
names that do not match Antfly package names, such as `"uuid-ossp"` resolving to
package `uuid_ossp`. If more than one package name declares the same SQL alias,
the SQL DDL fails as ambiguous. There are no hidden hard-coded SQL-name aliases;
compatibility names must be visible in package manifests. SQL `VERSION` maps to
`InstallExtensionRequest.version`; when omitted, lifecycle resolution chooses
the latest package version already present in the package catalog. SQL should
not accept an arbitrary package digest in v1. Digest trust comes from the
pre-registered package manifest and hosted package store policy, not from user
SQL text.

`IF EXISTS` and `IF NOT EXISTS` need strict, unsurprising semantics:

- `CREATE EXTENSION IF NOT EXISTS name` is a no-op only when installed extension
  `name` already exists.
- If `VERSION` is supplied and the installed version differs, fail rather than
  silently accepting a mismatch.
- `DROP EXTENSION IF EXISTS name` is a no-op only when `name` is absent.
- `DROP EXTENSION name RESTRICT` fails when dependent extensions exist.
- `DROP EXTENSION name CASCADE` uses the lifecycle cascade behavior.

Initial SQL extension DDL should be autocommit metadata DDL. Reject it inside a
user data transaction until Antfly has a unified transactional boundary for SQL
DDL, metadata transitions, extension member creation, and shard convergence.
The execution path should branch on `.extension_catalog` before schema JSON
mutation: ordinary relational schema DDL continues through the existing schema
apply path, while extension DDL routes to the lifecycle adapter and produces
metadata transition commands.

Prefer slash-action HTTP endpoints for the extension lifecycle, matching the
dominant public DB API shape (`/commit`, `/abort`, `/query`, `/batch`,
`/backup`, `/restore`, `/merge`). Antfly has some colon-action endpoints today,
but they are mostly narrow artifact/job controls such as `:reprocess`,
`:advance`, and `:cancel`. Extension install/update/drop are primary DB
lifecycle operations, so they should follow the broader `/action` convention
rather than `:action`.

Suggested API surface:

```text
GET    /extensions/v1/packages
GET    /extensions/v1/packages/{name}
GET    /extensions/v1/packages/{name}/versions/{version}

GET    /extensions/v1/installed
POST   /extensions/v1/installed/{name}
GET    /extensions/v1/installed/{name}
POST   /extensions/v1/installed/{name}/update
POST   /extensions/v1/installed/{name}/drop
POST   /extensions/v1/installed/{name}/enable
POST   /extensions/v1/installed/{name}/disable
GET    /extensions/v1/installed/{name}/objects
PUT    /extensions/v1/installed/{name}/config
```

Use `/extensions/v1` as the top-level subsystem for both package catalog and
installed extension lifecycle. Do not put these operations under `/db/v1`: an
extension can install hooks into the database API, AI/model API, MCP surface,
A2A surface, and auth policy surface. It is an Antfly platform capability, not a
database subresource.

`/extensions/v1/packages` is the catalog of signed, installable artifacts
Antfly knows about. Extension packages are the first package kind. In v1, MCP
tools, HTTP endpoints, workflows, model adapters, analyzers, connectors, and
index backends should be installed as objects owned by extension packages when
they need PostgreSQL-style lifecycle semantics. The same package catalog can
later hold standalone MCP apps or other thin wrappers that do not own storage
shape. `/extensions/v1/installed` is narrower: it is the installed extension
lifecycle, with PostgreSQL-style member tracking, dependencies, update/drop,
backup/restore, and shape ownership.

The split is:

```text
/extensions/v1/packages  = available installable artifacts
/extensions/v1/installed = installed extension instances and lifecycle
```

Package `{name}` values are path-safe package ids, not arbitrary registry paths.
They should be single path segments such as `memoryaf` or `antfly.memoryaf`.
Namespaced registry coordinates can be stored in package metadata later, but
the public lifecycle API should keep a stable, unambiguous package id in the
URL. When a client omits a package version, Antfly resolves the deterministic
highest known package version according to its package version ordering; hosted
profiles may additionally expose an explicit default/stable channel later.

Alternative roots are weaker:

- `/packages/v1` over-centers artifact distribution and separates package API
  versioning from install/update/drop semantics, even though they evolve
  together.
- `/pkg/v1` and `/pkgs/v1` are terse but less readable as a public API.
- `/db/v1/extensions` is too narrow once extensions can affect `/ai/v1`,
  `/mcp/v1`, A2A, and `/auth/v1`.

The SQL-like DDL maps directly onto those API calls:

```text
CREATE EXTENSION memoryaf
=> POST /extensions/v1/installed/memoryaf

ALTER EXTENSION memoryaf UPDATE TO '1.2.0'
=> POST /extensions/v1/installed/memoryaf/update

DROP EXTENSION memoryaf CASCADE
=> POST /extensions/v1/installed/memoryaf/drop
   { "mode": "cascade" }
```

`POST /extensions/v1/installed/{name}` should behave like "install this named
package into this scope", with body fields for version, target scope, config,
grants, and dry-run. `POST /extensions/v1/installed/{name}/update` should
accept target version, update policy, and dry-run.
`POST /extensions/v1/installed/{name}/drop` should accept `restrict`/`cascade`
mode and dry-run. A plain `DELETE /extensions/v1/installed/{name}` can exist
later as a shorthand for restricted drop, but the POST action is the better
primary operation because extension drop has policy, dependency, and dry-run
inputs.

Installed extension members can then augment existing Antfly surfaces:

```text
/db/v1        data shapes, indexes, enrichments, resolvers, query functions
/ai/v1        model adapters, provider config, embedding/generation hooks
/mcp/v1       extension-owned MCP tools
/a2a/v1       agent cards, skills, task handlers, and delegation hooks
/auth/v1      roles, permissions, capability policies, auth integration hooks
```

Install flow:

1. Resolve package and dependency versions.
2. Verify trust policy, digest/signature, target scope, and capability grants.
3. Dry-run manifest against metadata validators.
4. Write one metadata lifecycle transition containing table metadata deltas,
   installed extension rows, dependency rows, member rows, and desired objects.
5. Let existing table/range provisioning and shard reconciliation apply changes
   to shard-local `IndexManager` catalogs.
6. Mark runtime status per group as installed/ready/error.

Update flow:

1. Compute the shortest safe update path or require an explicit path.
2. Run update manifests in order.
3. Validate compatibility before changing desired metadata.
4. Stage shard-local replay/backfill work before exposing ready status.

Drop flow:

1. Refuse by default if user-created objects depend on extension members.
2. With cascade, remove extension-owned objects through metadata desired state.
3. Let reconciler remove shard-local indexes/enrichments/resolvers.
4. Remove installed extension row last.

### 5. Object Membership

Every extension-created object needs a stable identity:

```text
scope/table_name/object_kind/object_name
```

Treat `object_kind` as the kind of installed object Antfly must protect,
update, drop, back up, and restore. It should not include every implementation
subcomponent. Prefer kinds that line up with existing mutation surfaces and
future runtime boundaries.

V1 should support manifest-only or mostly declarative objects:

- `data_shape`: a named data contract, such as a document shape, row shape,
  generated artifact shape, MCP input/output schema, or HTTP request/response
  schema. This is the reusable shape definition.
- `table_schema`: a binding from extension-owned shape declarations into an
  Antfly table schema. This lines up with today's table `schema_json` and schema
  update flow.
- `extension_relation`: extension-owned table/relation/state, including config
  or state relations analogous to PostgreSQL extension-owned tables.
- `generated_artifact`: the shape and lifecycle of derived data written by
  enrichments or workflows, such as chunks, embeddings, summaries, extraction
  records, resolver outputs, checkpoints, or graph edges.
- `index`: an installed index instance over table data or generated artifacts.
  This lines up with current `indexes_json` and index create/drop flows.
- `enrichment`: a configured producer that creates generated artifacts. This
  lines up with the existing durable enrichment catalog.
- `resolver`: a configured resolver that consumes extraction artifacts and
  writes resolution artifacts. This lines up with the existing resolver catalog.
- `mcp_tool`: an MCP tool exposed from an installed extension, with schema,
  handler mode, capabilities, and audit semantics.
- `skill`: a first-class instruction and discovery artifact registered by an
  extension. Skills are not executable, but they bind usage guidance to visible
  agents, MCP tools, APIs, profiles, and capabilities.

V2 should add safe runtime and integration objects:

- `query_function`: a named callable used by query plans, API handlers, MCP
  handlers, or workflows. It should declare determinism, cost, result shape, and
  runtime mode.
- `api_endpoint`: an extension-owned HTTP/OpenAPI endpoint. This is useful, but
  should follow MCP tools because auth, routing, compatibility, and generated
  clients make it a wider public surface.
- `agent`: a callable multi-step or streaming agent exposed by an installed
  extension. It may be surfaced through Antfly agents APIs, A2A, MCP wrapper
  tools, ARD, and skills, but it remains one extension member object.
- `auth_policy`: extension-owned roles, permission templates, capability
  policies, or auth integration hooks. This should be declarative first and
  tightly constrained, because auth objects affect every other extension
  surface.
- `workflow`: a durable workflow definition, such as `antfly_durable` jobs or
  `memoryaf` summarization/compaction flows. Runtime state can live in
  extension relations and generated artifacts.
- `maintenance_task`: a scheduled or lease-owned background task, such as
  compaction, reweighting, graph metric refresh, or repair.
- `provider_config`: a model/provider/chunker/reranker/generator configuration
  entry. This should be separate from provider implementation code.
- `text_analyzer`: a configured text analyzer component or analyzer pipeline.
- `text_tokenizer`: a configured tokenizer component referenced by analyzers.

V3 and privileged objects should wait for stronger isolation and conformance
tests:

- `provider_adapter`: runtime code that implements a new provider backend.
- `connector`: runtime code and configuration for external data sources or
  sinks, including replication-like integrations.
- `index_backend`: a PostgreSQL access-method-style backend that defines how an
  index is built, updated, queried, snapshotted, and migrated. Individual index
  instances should still be `index` members.

Do not make `graph_algorithm` a core v1 object kind. A read-time graph algorithm
is usually a `query_function`; a materialized graph metric is usually a
`maintenance_task` plus `generated_artifact` or `extension_relation`; a graph
index remains an `index`. Add a dedicated graph object kind only when the
runtime contract is clearly distinct from those three cases.

This is the Antfly equivalent of PostgreSQL tracking extension member objects.
It prevents accidental deletion of individual extension-owned objects, makes
drop/update deterministic, and gives backup/restore a compact representation.

### 6. Runtime Extension Points

Expose narrow interfaces, not arbitrary internal structs:

- Query functions:
  - pure functions over JSON/scalars/vectors
  - deterministic flag, cost estimate, memory limit
  - manifest-only `query_function` objects persist as extension members with
    validated `config_json` metadata: `native_expression`, bounded `arity`, and
    optional `sql_names` aliases. The runtime binding resolver exposes only
    ready installed extensions, projects those declarations into checked
    row-expression kinds, and clones stable native bindings into the SQL routine
    runtime. Extension DDL refreshes those hooks from the projected metadata
    snapshot, and SQL read-plan lowering has an explicit binding-aware entrypoint
    for projecting those functions through native expression nodes, so disabled
    or dropped extensions cannot publish SQL-visible function hooks and the SQL
    adapter does not need parser-local native-expression string mappings.
- MCP tools and app-facing endpoints:
  - tool name, description, and JSON schema
  - handler mode: declarative Antfly API template, WASM, sidecar, or native
  - caller identity plus extension capability grants
  - audit record for every invocation
- A2A agent hooks:
  - agent card, skills, task input/output schemas, and handler mode
  - caller identity plus extension capability grants
  - audit record for every task invocation or delegation
- Auth policy hooks:
  - declarative roles, permission templates, and capability grants
  - no arbitrary runtime code in v1
  - explicit owner and scope so uninstall/update cannot orphan access
- Analyzers/tokenizers:
  - text in, token stream out
  - language/config metadata
- Enrichment producers:
  - document/artifact in, artifact out
  - explicit external network and secret capabilities
- Graph algorithms:
  - read-only graph view, bounded traversal budget, typed result
- Index access methods:
  - build/apply/delete/query/stats/snapshot hooks
  - versioned storage format and migration hook

Index access methods should be last, because they touch the most invariants:
write replay, snapshots, shard split handoff, maintenance scheduling, and query
visibility. A safer first public backend is a derived or enrichment-style
runtime, where failures are isolated from primary document writes.

### 7. Custom Agents, MCPs, and Skills

Agents, MCP tools, and skills should be different views over the same
extension-declared capability graph, not separate plugin systems.

The core distinction is:

```text
mcp_tool = executable callable tool
agent    = executable multi-step or streaming callable unit
skill    = instruction and discovery artifact for visible capabilities
```

An extension can declare all three in one manifest:

```json
{
  "objects": [
    {
      "kind": "agent",
      "name": "research_agent",
      "shape": "research_request",
      "config_json": {
        "displayName": "Research Agent",
        "description": "Plans, searches, retrieves, and summarizes tenant-scoped knowledge.",
        "protocols": ["agents-api", "a2a", "mcp-tool", "stream"],
        "handler": "wasm:research_agent/run",
        "stream_handler": "wasm:research_agent/stream",
        "required_capabilities": [
          { "name": "db:read", "scope": "docs" },
          { "name": "mcp:call", "scope": "tenant" },
          { "name": "agent:call", "scope": "tenant" }
        ],
        "allowed_tools": [
          "mcp:antfly.search",
          "mcp:memoryaf.search_memories"
        ],
        "allowed_agents": [
          "agent:antfly.retrieval",
          "agent:antfly.query-builder"
        ]
      }
    },
    {
      "kind": "mcp_tool",
      "name": "search_sources",
      "shape": "search_sources_request",
      "config_json": {
        "description": "Search tenant-scoped sources for the research agent.",
        "handler": "wasm:research_agent/search_sources"
      }
    },
    {
      "kind": "skill",
      "name": "research",
      "config_json": {
        "displayName": "Research Skill",
        "description": "Use the research agent with tenant-scoped search and citations.",
        "profile": "copilot",
        "capabilities": ["research", "retrieval", "citation"],
        "agents": ["research_agent"],
        "mcp_tools": ["search_sources"],
        "body": "# Research\n\nUse this when an agent needs to plan, retrieve, cite, and summarize from visible Antfly resources.\n"
      }
    }
  ]
}
```

Skills should be registered extension objects. They should not be executable and
they should not grant access by themselves. A skill is visible only when the
caller can also see or run the executable capabilities it describes. For
example, a MemoryAF skill should disappear if the caller cannot list the
MemoryAF MCP tools or run the related MemoryAF agent.

Extension agents should have a native Antfly run API:

```text
POST /agents/v1/extensions/{extension}/{agent}/runs
GET  /agents/v1/extensions/{extension}/{agent}/runs/{run_id}
GET  /agents/v1/extensions/{extension}/{agent}/runs/{run_id}/events
POST /agents/v1/extensions/{extension}/{agent}/runs/{run_id}/cancel
```

The non-streaming run endpoint should be enough for simple agents. Streaming
should use server-sent events first because it fits HTTP clients, A2A progress,
and MCP wrapper tools without requiring a persistent bidirectional protocol.
Typed events should be stable and append-only:

```json
{ "type": "run.started", "run_id": "run_123" }
{ "type": "step.started", "name": "retrieve_context" }
{ "type": "tool.call", "tool": "memoryaf.search_memories" }
{ "type": "delta", "content": "partial answer text" }
{ "type": "artifact", "kind": "citation", "ref": "doc:42" }
{ "type": "run.completed" }
```

Extension agents can be surfaced through multiple protocols:

- Antfly agents API: canonical run, status, streaming, and cancellation surface.
- A2A: an agent card and task handler backed by the same extension agent member.
- MCP: an optional generated MCP wrapper tool such as `run_research_agent`.
- ARD: catalog entries for the agent descriptor, A2A card, MCP descriptor, and
  related skill artifacts.
- OpenAPI: `/agents/v1/extensions/{extension}/{agent}` operations.
- Profiles: filtered views such as `copilot`, resolved through the same tenant
  visibility and permission checks as MCP profiles.

Agents should call other agents and MCP tools through brokered host APIs, not
through direct internal access:

```text
host.call_mcp(tool_ref, input)
host.call_agent(agent_ref, input)
host.emit_event(event)
host.read_secret(name)
host.query_table(table, query)
```

Every host call must apply both the caller identity and the installed
extension's granted capabilities. The effective permission is the intersection:
a caller cannot use an extension agent to reach MCP tools, agents, rows, secrets,
or providers that the same caller could not access through the native surface.

Useful capability names for this layer:

```text
agent:run
agent:call
agent:stream
mcp:tool
mcp:call
a2a:serve
db:read
db:write
secret:read
network:outbound
```

Dependencies on other agents and MCP tools should be declarative:

```json
{
  "dependencies": {
    "agents": [
      {
        "ref": "antfly:retrieval",
        "required": true,
        "capabilities": ["retrieval"]
      }
    ],
    "mcp_tools": [
      {
        "ref": "memoryaf.search_memories",
        "required": false
      }
    ]
  }
}
```

At install time Antfly should validate that required dependencies can be
resolved in the package or tenant catalog. At runtime it should resolve
dependencies against the authenticated caller's scoped catalog. Optional
dependencies should degrade the agent capability set instead of failing install.

Discovery should be generated from the same extension members:

- A `skill` member is served under
  `/ard/v1/skills/extensions/{extension}/{skill}` and appears as
  `application/ai-skill+md`.
- An `agent` member can appear as an Antfly agent descriptor, A2A agent card, or
  MCP wrapper descriptor depending on the declared protocols.
- An `mcp_tool` member appears in `/mcp/v1/extensions/{extension}`,
  `/mcp/v1`, profile-scoped MCP endpoints, and ARD MCP descriptors only when
  visible to the caller.

Implementation phases:

1. Add `skill` registration as a stable extension member object and keep ARD
   skill visibility tied to the capabilities it describes.
2. Add `agent` registration, manifest validation, and non-streaming
   `/agents/v1/extensions/{extension}/{agent}/runs`.
3. Add server-sent event streaming and cancellation for extension agent runs.
4. Generate MCP wrapper tools for extension agents that opt into `mcp-tool`.
5. Add A2A card/task routing for extension agents that opt into `a2a`.
6. Add ARD catalog entries and skill references for extension agents.
7. Add install-time and runtime validation for declared agent and MCP
   dependencies.

The design rule is that extensions register capabilities once. MCP, A2A, ARD,
skills, streaming APIs, and future profiles expose scoped views over those
registered members.

### 8. Runtime Isolation

Prefer a tiered model:

- `manifest_only`: declarative extension objects, no runtime code
- `wasm`: hosted-safe runtime with fuel, memory, syscall, and network limits
- `native_local`: dynamic library for embedded/local deployments only
- `native_hosted`: explicit enterprise/admin-only mode with signing, allowlist,
  restart policy, and crash containment

This is deliberately stricter than PostgreSQL. PostgreSQL assumes superuser
trust for dangerous extensions; Antfly hosted clusters need tenant and operator
isolation.

### 9. Backup, Restore, and Export

Backups should record:

- installed extension name, version, scope, and digest
- extension-owned data shape versions
- extension member object identities
- user-owned extension configuration data

Backups should not expand extension-owned declarative objects into unrelated
loose objects unless the target cluster lacks the required package and the user
requests a flattened export.

Restore should fail early if required packages are missing or digest checks do
not match, unless an explicit compatibility override is provided.

## Full-Blown Target System

A full-blown Antfly extension system should be a platform, not only a metadata
feature. The target architecture has six separable layers.

### 1. Package Distribution and Trust

Add a real package distribution path. Extensions are the first package kind, but
the catalog should be generic enough for later installable artifacts:

- local development packages: `antfly package build`, `antfly package test`
- private registries for enterprise/hosted deployments
- signed package manifests and content digests
- cluster-level allowlists and deny lists
- tenant-level install policy
- compatibility metadata for Antfly versions and hosted profiles
- package provenance in audit logs and backup metadata

The package registry should store immutable package versions. Mutable tags are
fine for discovery, but install records should pin name, version, digest, and
signer.

### 2. Developer SDK and ABI

Provide an SDK instead of making extension authors learn internal Zig structs.
The SDK should expose stable interfaces for:

- manifest schemas and validators
- object declaration helpers
- WASM bindings for query functions, analyzers, enrichers, and graph algorithms
- local test harnesses that simulate metadata install/update/drop
- conformance tests for replay, backup/restore, resource limits, and upgrade
  compatibility
- generated API clients for extension configuration

The ABI contract should be versioned independently from the Antfly binary. An
extension should declare both:

- minimum Antfly version
- required extension ABI versions by capability

This lets a query-function extension remain compatible even if index-backend ABI
changes.

### 3. Capability and Permission Model

PostgreSQL uses trusted/untrusted extensions and superuser boundaries. Antfly
needs a more explicit hosted-safe permission model:

- read document payloads
- write derived artifacts
- create indexes
- create enrichments
- create provider configs
- use configured secrets
- make outbound network calls
- use local filesystem or object storage
- consume model/provider runtime
- run maintenance tasks
- run native code

Capabilities should be requested in the package manifest, granted at install
time, stored in metadata, and included in audit logs. Runtime hosts must enforce
the granted capabilities, not only validate them at install.

### 4. Extension Runtime Plane

Runtime execution should be isolated from primary storage as much as possible:

- WASM/component runtime for hosted-safe code
- per-extension memory, CPU/fuel, wall-clock, and output-size limits
- deterministic cancellation and timeout propagation
- structured logs, metrics, and traces tagged by extension name/version
- circuit breakers when an extension repeatedly fails
- optional sidecar runtime for expensive or dependency-heavy extensions
- native runtime only behind explicit node/cluster policy

The full system should support three execution modes:

- in-process WASM for small deterministic functions
- sidecar WASM/native workers for enrichers, model adapters, and graph algorithms
- in-process native only for local/embedded or operator-installed core
  extensions

### 5. Hook Surface

A Postgres-equivalent system ultimately needs several families of hooks:

- catalog hooks:
  - validate object declarations
  - create/update/drop extension-owned objects
  - attach or detach member objects
- query hooks:
  - scalar/vector functions
  - filter predicates
  - score transforms
  - rerankers
  - custom result projections
- app/protocol hooks:
  - MCP tools
  - custom HTTP/OpenAPI endpoints
  - agent tool catalogs
  - prompt/tool-schema manifests
- analysis hooks:
  - tokenizers
  - analyzers
  - normalizers/stemmers
  - synonym sources
- enrichment hooks:
  - chunkers
  - asset producers
  - embedding producers
  - extraction/resolution stages
- graph hooks:
  - traversal algorithms
  - path scoring
  - centrality/community metrics
  - cross-table node hydration policies
- storage/index hooks:
  - index access methods
  - per-index config parsing
  - build/apply/delete/query
  - snapshot/split/merge handoff
  - storage format migration
- operational hooks:
  - background maintenance tasks
  - compaction/coalescing policies
  - runtime status reporting

The hook surface should grow from high-level and isolated to low-level and
dangerous. Query functions, analyzers, and enrichers are good early hooks. Index
access methods and storage hooks should require conformance tests and stronger
operator trust.

### 6. Operational Lifecycle

Full extension management needs operator workflows:

- install dry-run with object diff and dependency graph
- upgrade dry-run with data migration/backfill estimate
- rolling extension activation across shard groups
- pause/resume extension-owned background work
- rollback plan for failed upgrades
- extension health in admin snapshots and cluster status
- audit log entries for install/update/drop/config changes
- backup restore preflight for missing package versions
- emergency disable that blocks runtime execution without deleting catalog
  objects

This is where Antfly should intentionally differ from PostgreSQL. PostgreSQL
extensions often run in the server process and inherit server fate. Antfly should
make runtime failure a contained operational event wherever possible.

### 7. Performance Model

The extension system should make performance tiering explicit. PostgreSQL gets
excellent extension performance when extensions run in-process and avoid
serialization boundaries. Antfly should support the same class of fast path for
trusted/internal extensions while keeping hosted-safe extensions isolated.

Performance tiers:

- `core_internal`: compiled with Antfly, zero extension boundary on hot paths
- `trusted_native`: in-process native ABI, operator-installed, zero-copy where
  possible
- `batched_wasm`: hosted-safe runtime, explicit batch APIs, bounded overhead
- `sidecar`: isolated worker process, intended for I/O-heavy or long-running
  tasks, not inner-loop query/index work
- `manifest_only`: no runtime cost after metadata reconciliation

Hook APIs should be designed around the expected call frequency:

- per-query hooks may cross a WASM boundary if request/response payloads are
  bounded
- per-batch hooks should use columnar/vectorized buffers or arena-backed slices
- per-document hooks must batch by segment, shard, or replay batch
- per-token/per-posting hooks must be native or compiled-in unless there is a
  proven vectorized WASM path
- sidecar hooks must be limited to enrichers, workflows, model/provider calls,
  long-running graph jobs, and external I/O

The access-method ABI should avoid generic JSON on hot paths. JSON is acceptable
for install-time config and admin/status output; query, build, replay, and
snapshot paths should use typed structs, stable binary encodings, or borrowed
buffers with explicit ownership rules.

Every public runtime hook should declare:

- expected call granularity
- allowed runtime modes
- allocation ownership
- serialization format
- batching requirements
- deterministic timeout/cancellation behavior
- metrics for time, allocations, bytes copied, and failures

This keeps the product promise clear: safe third-party extensions may pay an
isolation tax, but trusted/native/internal extensions should be able to reach
built-in performance.

## Full-Blown Implementation Plan

### Phase A: Extension Control Plane

- Add package, install, dependency, capability, and member catalogs to
  metadata state.
- Add OpenAPI/admin operations for package registration, install, update, drop,
  list, inspect, enable, disable, and dry-run.
- Add audit-log events for every lifecycle operation.
- Add metadata snapshot and status exposure.

### Phase B: Manifest-Only Extension Objects

- Make existing schemas, indexes, enrichments, resolvers, and provider configs
  installable from a manifest.
- Make built-in features internal extensions.
- Add extension member ownership checks to direct object mutation APIs.
- Add backup/export and restore preflight support.

### Phase C: SDK and Registry

- Add `antfly package init/build/test/publish` CLI flows, plus
  `antfly extension install/update/drop` convenience wrappers.
- Add local conformance tests for manifests and upgrade paths.
- Add private registry support and digest-pinned package install.
- Add package signing and cluster allowlists.

### Phase D: Hosted-Safe Runtime

- Add WASM host, capability enforcement, and resource accounting.
- Support query functions, MCP tool handlers, analyzers/tokenizers, and
  enrichment producers.
- Add runtime metrics, traces, logs, circuit breakers, and emergency disable.
- Add sidecar worker mode for heavier runtime dependencies.

### Phase E: Advanced Hooks

- Add graph algorithms and custom rerankers.
- Add background maintenance tasks with strict scheduling budgets.
- Add user-defined provider/model adapters.
- Stabilize public access-method conformance tests before exposing custom index
  backends.

### Phase F: Native and Low-Level Extensions

- Add local/embedded native extension loading.
- Add operator-installed native extensions for hosted clusters only after
  signing, allowlisting, crash policy, and restart isolation are in place.
- Treat native index backends as privileged infrastructure, not tenant-installed
  packages.

## Full-Blown Design Stance

The important distinction is that "full-blown" does not mean "allow arbitrary
shared libraries everywhere." It means:

- package registry and pinned installs
- metadata-backed lifecycle
- dependency and ownership semantics
- stable SDK and ABI
- explicit capabilities
- safe runtime isolation
- operational controls
- advanced hooks only after conformance coverage exists

That gives Antfly PostgreSQL-like extensibility while fitting Antfly's hosted,
distributed, shard-reconciled architecture.

## Example: MCP Facade and Memory Extension

Antfly already has a small MCP surface: `zig/lib/mcp/src/root.zig` implements
MCP `tools/list` and `tools/call`, while `api/protocol_adapters.zig` wires a
fixed set of Antfly tools such as `create_table`, `query`, `batch`, `backup`,
and `restore`.

The extension system should generalize that hard-coded tool list into
extension-owned MCP tools. This would let a user package an application-facing
wrapper around Antfly, install it inside Antfly, and expose it as a scoped MCP
server without running a separate service.

For v1, MCP tools should usually be part of the extension package that owns the
underlying data shape. A `memoryaf` package should not be split into one storage
extension and one MCP app package. It should be a single extension package whose
manifest owns the full product surface: memory shapes, storage relations,
generated artifacts, indexes, enrichments, query/API handlers, MCP tools,
runtime declarations, and capability grants. This keeps install, update, drop,
backup, and restore coherent.

Standalone MCP app packages can come later for thin wrappers over already
installed capabilities, such as a read-only memory assistant or a Slack-facing
operator. Those packages should not be part of the first extension milestone.

### Memory Extension Shape

A memory-oriented extension package, for example `memoryaf`, could install:

- relational or document tables:
  - `memories`
  - `memory_events`
  - `memory_edges`
- indexes:
  - full-text memory search
  - dense vector semantic recall
  - graph links between entities, sessions, and memories
- enrichments:
  - embeddings
  - summarization
  - entity extraction
  - recency/importance scoring
- MCP tools:
  - `remember`
  - `recall`
  - `search_memory`
  - `forget`
  - `link_memory`
  - `summarize_memory`
- optional background tasks:
  - deduplication
  - compaction
  - decay/reweighting
  - periodic summary refresh

The important product behavior is:

```text
CREATE EXTENSION memoryaf;
GET /extensions/v1/installed/memoryaf/objects
GET /mcp/v1/extensions/memoryaf
```

or, for a merged tool list:

```text
GET /mcp/v1
```

with the installed extension's tools included for authorized callers.

### MCP Tool Object

An MCP tool should be an extension member object:

```json
{
  "kind": "mcp_tool",
  "name": "search_memory",
  "description": "Search long-term memory",
  "input_schema": {
    "type": "object",
    "properties": {
      "query": { "type": "string" },
      "limit": { "type": "integer", "default": 10 }
    },
    "required": ["query"]
  },
  "handler": {
    "type": "antfly_api_template",
    "table": "memories",
    "query": {
      "hybrid": {
        "text": "{{ query }}",
        "semantic": "{{ query }}",
        "limit": "{{ limit }}"
      }
    }
  }
}
```

Handler modes should be tiered:

- `antfly_api_template`: declarative mapping from tool args to existing Antfly
  API calls
- `workflow`: invoke an extension-owned durable workflow
- `wasm`: hosted-safe custom handler code
- `sidecar`: call an isolated extension runtime for heavier app logic
- `native`: operator-installed only

The manifest-only `antfly_api_template` mode should come first. It is enough for
many wrappers and keeps simple app extensions safe, inspectable, and cheap.

### Identity and Capability Semantics

MCP tool calls must run under both:

- the caller identity
- the extension's granted capabilities

The effective permission is the intersection. A `memoryaf` tool can write only
to the extension-owned memory tables unless the install grant explicitly allows
broader access. If a tool needs model/provider access or secrets, those grants
must be declared in the package manifest and approved at install time.

Useful capability examples:

- `read:table:memoryaf.memories`
- `write:table:memoryaf.memories`
- `read:index:memoryaf.memory_embedding`
- `use:embedder:default`
- `use:generator:summary`
- `read:secret:memoryaf.external_api`
- `network:allowlist:https://api.example.com`

Every tool invocation should emit an audit event with extension name, version,
tool name, caller identity, granted capabilities used, elapsed time, and error
class.

### Deployment Model

There are three deployment shapes:

- Extension-scoped MCP endpoint:
  - `/mcp/v1/extensions/{extension_name}`
  - exposes only that extension's tools
  - best for clients that want one app-specific MCP server
- Merged tenant MCP endpoint:
  - `/mcp/v1`
  - includes built-in tools plus installed extension tools allowed for the
    caller
  - best for general Antfly admin/agent clients
- Sidecar-backed MCP extension:
  - Antfly owns tool registration, auth, lifecycle, and audit
  - a sidecar executes expensive or dependency-heavy handlers
  - best for complex agent apps

The first two can share the same extension tool catalog. The sidecar-backed mode
is an implementation detail of selected tools, not a separate product concept.

### Why This Belongs in the Extension System

Without extensions, each wrapper like `memoryaf` becomes its own service,
deployment, auth model, backup model, and operational surface. With extensions:

- install creates the tables, indexes, enrichments, tools, and optional workers
- member tracking protects extension-owned memory state
- backup/restore can preserve the memory app as an installed package plus state
- MCP tools automatically inherit Antfly auth, tenant isolation, audit, and
  rate limits
- `DROP EXTENSION memoryaf RESTRICT` can refuse while memory state exists
- `DROP EXTENSION memoryaf CASCADE` can remove the tool surface and owned state
  intentionally

This is also a good early extension milestone because MCP calls are coarse
grained. They can tolerate manifest/template, WASM, or sidecar boundaries much
better than per-token or per-posting index hooks.

## Example: Durable Workflow Extension

Microsoft's `pg_durable` is a good target example for this system. InfoQ
describes it as a PostgreSQL extension for durable in-database workflows, with
retry state, progress tracking, checkpointing, fan-out, recovery, scheduling,
conditions, and parallel execution managed inside PostgreSQL rather than in
external orchestrators.

An Antfly equivalent could be an extension named `antfly_durable`. It should be
possible without making durable execution a hard-coded core feature, but only if
the extension platform includes both catalog lifecycle and runtime hooks.

### Extension-Owned Objects

The durable workflow extension would install:

- workflow definition objects
- workflow instance records
- history/checkpoint records
- timer records
- work-queue records
- retry-policy records
- runtime status views
- query functions or API handlers for start/cancel/signal/inspect
- a background worker or sidecar runtime

Those objects must be extension members so `DROP EXTENSION antfly_durable
RESTRICT` can refuse when live workflows exist, while `CASCADE` can perform a
controlled teardown.

### Runtime Capabilities Needed

The extension would need explicit grants for:

- durable metadata writes
- background task execution
- timers
- document reads/writes for target tables
- provider/model access for embedding or generation workflows
- outbound network calls when workflows call external APIs
- secrets access for those external APIs
- audit-log emission

This is exactly why capabilities need to be first-class. Durable execution is
safe only when the runtime can enforce which workflows may touch which tables,
providers, secrets, and network destinations.

### Antfly-Native Workflow Shape

The user-facing workflow language does not need to copy PostgreSQL SQL operators.
Antfly could expose workflows as JSON/YAML DAGs, a small expression language, or
SDK builders:

```json
{
  "name": "embed_new_documents",
  "steps": [
    {
      "id": "batch",
      "op": "scan",
      "table": "documents",
      "filter": { "term": { "processed": false } },
      "limit": 100
    },
    {
      "id": "embed",
      "op": "enrich",
      "input": "batch",
      "enrichment": "embedding_v1",
      "parallelism": 16,
      "retry": { "max_attempts": 5, "backoff": "exponential" }
    },
    {
      "id": "mark_processed",
      "op": "transform",
      "input": "embed",
      "set": { "processed": true }
    }
  ]
}
```

The key property is not syntax. The key property is deterministic progression:
each step records enough history to resume after process crash, node restart, or
leader failover.

### Distributed Semantics

Antfly would need to decide the execution scope:

- table-scoped workflows run under the table metadata/control-plane lifecycle
- shard-local workers execute shard-local work under leases
- cluster-scoped workflows coordinate cross-table or cross-shard steps through
  metadata-owned leases
- every external side effect must be idempotent or guarded by idempotency keys

The minimum viable semantics should be at-least-once step execution with durable
checkpoints and idempotency keys. Exactly-once external effects should not be
promised.

### Why the Extension Platform Matters

Without the full extension platform, `antfly_durable` would become a bespoke
core subsystem. With the platform, it becomes an ordinary privileged extension:

- package install creates the catalogs and runtime definitions
- metadata stores lifecycle and capabilities
- member tracking protects its objects
- the runtime plane runs its worker safely
- status appears in admin snapshots
- backup/restore knows which workflow state belongs to the extension
- disable/rollback paths are operator-visible

This is the kind of extension that proves the system is more than custom index
plugins.

## Implementation Plan

### Phase 0: Catalog and Shape Design

- Define `ExtensionManifest`, `InstalledExtension`, `ExtensionMember`, and
  `ExtensionDependency` structs.
- Define extension-owned data shape records for table/document/row shape,
  generated artifact shape, extension state relations, and endpoint/tool schemas.
- Add JSON encoding/decoding tests.
- Decide whether extension metadata is cluster-wide in metadata state or
  colocated with table records. Prefer metadata state with table references.

### Phase 1: Declarative Built-In Extensions

- Implement manifest-only install for table-scoped shapes,
  indexes/enrichments/resolvers, and generated artifact definitions.
- Add metadata admin operations and CLI wrappers.
- Register current built-ins as internal extensions:
  - `antfly_full_text`
  - `antfly_dense_vector`
  - `antfly_sparse_vector`
  - `antfly_graph`
  - `antfly_algebraic`
- Keep existing direct APIs working by translating them into the same metadata
  object model.

### Phase 2: Dependency and Update Semantics

- Add versioned update manifests.
- Add dependency resolution and restrict/cascade drop behavior.
- Add status reporting in metadata admin snapshots and runtime group statuses.
- Add backup/restore validation for installed extensions.

### Phase 3: Safe Runtime Hooks

- Add WASM runtime host for query functions and enrichment producers.
- Add capability grants for network, secrets, filesystem, and model/provider
  access.
- Add deterministic resource accounting and timeout tests.

### Phase 4: Public Index Backend Interface

- Extract a stable access-method interface from current index manager behavior.
- Require conformance tests for replay, snapshot, split handoff, and query
  visibility before an index backend can be marked installable.
- Keep native backends local-only until crash containment and operational policy
  are clear.

## Key Design Decisions

- Metadata owns desired extension state and installed shape versions. Shards
  materialize storage, index, artifact, and runtime consequences.
- Extensions define data contracts, not only lifecycle metadata. Shape
  declarations are extension member objects and participate in install, update,
  backup, restore, and drop semantics.
- Use `/extensions/v1/packages` for the generic catalog of available signed
  artifacts and `/extensions/v1/installed` for installed PostgreSQL-style
  extension lifecycle. Version them together because package manifests,
  dependency resolution, trust policy, capability grants, and install/update/drop
  semantics evolve together.
- In v1, MCP tools and skills are extension-owned objects, not separate package
  installs. A package such as `memoryaf` should include its data shape,
  relations, artifacts, indexes, enrichments, query/API handlers, MCP tools,
  skills, runtimes, and capability grants in one extension package.
- Extension agents should use the same member, capability, dependency, and
  visibility model as MCP tools. Antfly agents APIs, A2A, MCP wrapper tools,
  ARD, skills, streaming APIs, and profiles should expose scoped views over the
  same registered agent object.
- Extension install/update/drop is transactional at metadata level and
  eventually convergent at shard level.
- Start with `cluster`, `table`, and `embedded_db` extension scopes. Treat
  `tenant` as an authorization and availability boundary in hosted mode, not as
  the primary physical install scope. Do not add a `database` scope until Antfly
  has a first-class database concept.
- Local, development, and embedded profiles resolve packages from the
  filesystem. Hosted profiles resolve packages from a registry or object store
  and materialize them into an operator-controlled, read-only,
  content-addressed package store. Installed metadata pins package name, version,
  and digest.
- Hosted installs require layered allowlists and capabilities. Effective
  permission is the intersection of operator package allowlist, tenant allowlist,
  package trust policy, caller install permission, and runtime capability grants.
- Updates are declarative first. V1 supports additive shape/object diffs and
  explicit backfill requirements. Later imperative migration hooks must run as
  durable jobs, not inline inside the metadata install/update transaction.
- OpenAPI/metadata operations are canonical. CLI commands wrap those operations.
  SQL-like `CREATE EXTENSION`, `ALTER EXTENSION UPDATE`, and `DROP EXTENSION`
  are compatibility syntax over the same API, not the internal control plane.
- Make existing APIs extension-aware in this order: table data shape, generated
  artifacts, index creation, enrichment/resolver creation, MCP/API endpoints,
  A2A hooks, auth policy hooks, provider/model adapters, then public index
  backends.
- WASM is the default code extension mechanism. Native loading is opt-in and
  restricted.
- Existing object APIs should converge into extension member tracking rather
  than competing with it.
- Index access methods are important for PostgreSQL equivalence, but they should
  come after lower-risk object types.

## Deferred Questions

- Exact standalone package kinds beyond `extension`, such as thin `mcp_app`
  wrappers, `workflow_pack`, `model_adapter`, `analyzer_pack`, `connector`, and
  `index_backend`. These should wait until there is a clear need for packages
  that do not own extension lifecycle state directly.
- The public schema language for extension-owned shapes: JSON Schema, an
  Antfly-specific schema format, OpenAPI components, or a small combination.
- The durable job API used by imperative migration hooks and hosted-safe
  runtimes.
- The conformance suite required before a public index backend can be marked
  hosted-installable.

## First Useful Milestone

The smallest valuable slice is manifest-only table extensions:

1. Add extension catalog structs and metadata snapshot exposure.
2. Support installing a table-scoped extension that declares one data shape, one
   generated artifact shape, one full-text index, and one enrichment.
3. Track those shapes and runtime/storage objects as extension members.
4. Reject direct deletion of those member objects unless the extension is being
   dropped.
5. Validate writes against the installed extension shape where the extension
   owns fields or artifacts.
6. Include the installed extension record and shape versions in backup/export
   metadata.

That milestone proves the PostgreSQL-style lifecycle without taking on unsafe
native loading or public index backend ABI design.
