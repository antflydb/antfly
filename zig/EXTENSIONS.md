# Postgres-Style Extension System

## Context

Antfly has a working Postgres-style extension system. A package/extension
catalog, an install/update/drop/enable/disable/configure lifecycle, extension
object membership, table ownership guards, and a WASM runtime with fuel and
memory limits are implemented in `zig/pkg/antfly/src/extensions/` (`mod.zig`,
`lifecycle.zig`, `table_ownership.zig`, `wasmtime_runtime.zig`) and
`zig/pkg/antfly/src/metadata/extension_operations.zig`, and are served over
the `/extensions/v1/*` HTTP API described in
`specs/openapi/extensions/api.yaml`. Extension-owned MCP tools, skills, and
agents are exposed through `/mcp/v1`, `/ard/v1`, and `/agents/v1/extensions/*`
with identity- and capability-scoped visibility. What is not yet built —
package distribution/signing, a developer SDK, most non-storage object kinds,
non-WASM runtime handler modes, extension agent execution, and public index
backends — is called out in [Open work](#open-work) below rather than
described as shipped.

PostgreSQL extensions are not just dynamic libraries. The useful product
contract is a managed package of database objects with install, update,
dependency, ownership, dump/restore, and drop semantics. Before this system
existed, the closest Antfly equivalents were split across:

- metadata table records in `zig/pkg/antfly/src/metadata/table_manager.zig`
- per-table index metadata in `TableRecord.indexes_json`
- shard-local durable index, enrichment, and resolver catalogs in
  `zig/pkg/antfly/src/storage/db/catalog/index_manager.zig`
- provider registries in `zig/pkg/antfly/src/common/provider_registry.zig`
- embedded DB lifecycle APIs in `zig/pkg/antfly/src/embedded/db.zig`

That gave Antfly many extension-like object types but not a single extension
catalog or lifecycle. The extension catalog below sits on top of those
mechanisms rather than replacing them: installing an extension that owns an
index or enrichment still ends up editing a table's `indexes_json` (see
[Table Ownership](#table-ownership)), just through a tracked, ownership-aware
path instead of a direct API call.

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

## Package and Extension Catalog

Packages are described by an `extension.json` manifest rather than a
PostgreSQL `.control` file. The manifest shape (`PackageManifest` in
`zig/pkg/antfly/src/extensions/mod.zig`) is:

```json
{
  "manifest_api_version": "extensions/v1",
  "name": "antfly_text_extras",
  "version": "1.0.0",
  "kind": "extension",
  "description": "Extra analyzers and query helpers",
  "digest": "sha256:...",
  "trusted": false,
  "relocatable": false,
  "capabilities_requested": [{ "name": "db:read", "scope": "docs" }],
  "dependencies": [{ "name": "antfly_core", "version_requirement": ">=1.0.0" }],
  "artifacts": [{ "kind": "wasm", "path": "runtime/extension.wasm" }],
  "install": { "scopes_supported": ["table"], "shapes": [], "objects": [], "runtimes": [] },
  "updates": []
}
```

`kind` only accepts `"extension"` today (`PackageKind`). `antfly_min_version`,
`antfly_max_version`, and `trusted` are stored on the manifest and round-tripped,
but nothing currently checks them against the running Antfly version or a
trust policy — see [Open work](#open-work). `digest` is a self-declared
identity string used to pin an installed extension to an exact package
revision and to resolve content-addressed artifacts; it is not computed or
verified against the manifest/artifact bytes.

Packages are loaded from a filesystem package store, not registered through an
admin API. `zig/pkg/antfly/src/extensions/mod.zig`'s `scanPackageStoreAlloc`
walks a configured root directory for files named `extension.json` and accepts
two layouts:

```text
<root>/<package-name>/extension.json          # canonical
<root>/sha256/<digest>/extension.json         # content-addressed
```

Any other nested `extension.json` is ignored (`packageStoreLayout` returns
`null`). The metadata service and standalone/data runtimes call
`syncExtensionPackageStore` at startup against a directory configured with the
`--extension-package-store` CLI flag or the `ANTFLY_EXTENSION_PACKAGE_STORE`
environment variable (see `zig/pkg/antfly/src/metadata/runtime.zig`,
`zig/pkg/antfly/src/standalone/runtime.zig`, and
`zig/pkg/antfly/src/data/runtime.zig`), defaulting to `<local-base>/extensions`.
Scanned manifests are registered into the metadata-owned `ExtensionCatalog`
(`packages` list) and appear read-only under `/extensions/v1/packages`.

The catalog (`ExtensionCatalog` in `mod.zig`) holds four in-memory/projected
row sets, backed by metadata state:

- `packages`: registered `PackageManifest` rows
- `installed`: `InstalledExtension` rows (one per installed extension name)
- `members`: `ExtensionMember` rows (every object an installed extension owns)
- `dependencies`: `ExtensionDependency` rows (installed-extension-to-package
  dependency edges)

Antfly does not have PostgreSQL's schema/database split. `ExtensionScopeKind`
has three values: `cluster`, `table` (requires a `table_name`), and
`embedded_db`. There is no `tenant` or `database` scope.

## Data Shapes

`DataShapeDecl` lets a package declare a named, versioned data contract of one
of six kinds (`DataShapeKind`): `document`, `row`, `generated_artifact`,
`extension_relation`, `endpoint_schema`, `tool_schema`. Each shape carries a
`schema_json` string. For `document`/`row` shapes installed at `table` scope,
`planManifestOnlyInstallAlloc` in `mod.zig` runs
`validateTableWriteDataShapeSchema`, which parses `schema_json` through the
same table-schema parser used for native tables
(`schema_mod.parseValidatedTableSchema`) and requires at least one document
schema. This is the "v1 shape implementation" the original design called for:
table-owning shapes are checked with the real table-validation contract, not a
separate shape language.

For every other shape kind, and for shapes at `cluster`/`embedded_db` scope,
`schema_json` only has to be a valid JSON object — there is no semantic
validation, versioned migration support, or backfill mechanism. A public
shape language (JSON Schema/OpenAPI-based or otherwise) and imperative
migration hooks remain [open work](#open-work).

## Lifecycle: Install, Update, Drop, Enable, Disable, Configure

The lifecycle operations proposed in the original design exist and match the
OpenAPI surface in `specs/openapi/extensions/api.yaml` closely:

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

Each `InstalledExtension` carries a `status`: `installing`, `ready`,
`disabled`, `updating`, `dropping`, `error_state` (`ExtensionStatus` in
`mod.zig`). In the current implementation, `install`/`update`/`drop` compute
their result synchronously and land directly in `ready` (or are removed);
`installing`/`updating`/`dropping` exist as states but nothing currently drives
a long-running extension through them — they mainly gate concurrent lifecycle
calls (`disableInstalled`/`enableInstalled` reject a busy extension with
`ExtensionLifecycleBusy`).

**Install** (`ExtensionCatalog.installManifestOnly`, orchestrated by
`extension_lifecycle.installOnService` in `lifecycle.zig`): resolves the
requested package version (or the highest known version when `version` is
omitted), requires every non-optional package dependency to already be
installed, validates that requested capability grants are a subset of the
package's `capabilities_requested`, and materializes one `ExtensionMember` per
declared shape and object. `data_shape`/`table_schema`/`index`/`enrichment`
members that reference a table are folded into that table's `indexes_json`
(`planStorageMemberDeltaAlloc`), reusing the existing index/enrichment
validators (`indexes_api.addIndexToTableIndexesJson`,
`managed_embedder.validateEmbeddingProducerOwnershipJsonWithOptions`, and
friends) so an extension cannot install an index or enrichment that the
built-in APIs would reject.

**Update** (`ExtensionCatalog.updateManifestOnly`): requires an explicit
`UpdateManifestRef` path from the currently installed `package_version` to the
target version in the target package's `updates` list
(`requireUpdatePath`); there is no "shortest safe path" search. A no-op update
to the currently installed version is a successful no-op. Update replaces all
of the extension's members and dependencies with a freshly planned set from
the target package's manifest.

**Drop** (`ExtensionCatalog.dropInstalledWithMode`): supports `restrict`
(default; fails with `DependentExtensionExists` if another installed
extension depends on it) and `cascade` (recursively drops dependents first).
Dropping removes the extension's installed row, its members (and any table
`indexes_json` entries those members own), and its dependency edges.

**Enable/Disable** (`enableInstalled`/`disableInstalled`): toggle between
`ready` and `disabled` without touching membership rows — an extension's
tables, indexes, and enrichments stay in place, but a disabled extension's MCP
tools stop being served (see
[MCP Tool Exposure and Permission Filtering](#mcp-tool-exposure-and-permission-filtering)).
This is the "emergency disable that blocks runtime execution without deleting
catalog objects" the original design called for.

**Configure** (`configureInstalled`): replaces `config_json` wholesale. The
manifest's `config_schema_json` is stored and validated as a JSON object, but
it is not used to validate `config_json` against a schema.

**Dry run**: every mutating request accepts `dry_run`. The HTTP handlers
(`executeExtensionRoute` in `zig/pkg/antfly/src/api/http_server.zig`) compute
the planned `InstalledExtension` result and return it without proposing a
metadata mutation when `dry_run` is set.

**Consistency and failure semantics**: lifecycle mutations propose a
`TransitionCommand` (`apply_extension_lifecycle` or, when table `indexes_json`
changes are involved, `apply_extension_lifecycle_v2` with per-table
compare-and-swap preconditions keyed on a table definition fingerprint) and
then verify the exact projected result before returning
(`verifyLifecycleProjection`/`lifecycleDeltaApplied` in `lifecycle.zig`). Three
distinct outcomes are surfaced to callers:

- A confirmed conflict (the proposal committed but produced a different result
  than expected, e.g. a concurrent change) surfaces as
  `ExtensionLifecycleConflict` → HTTP 409, "extension lifecycle conflicted
  with a concurrent catalog transition; retry after observing current state".
- An ambiguous outcome (the Raft propose/apply round trip could not be
  confirmed, e.g. a leadership change mid-proposal) surfaces as
  `MetadataMutationOutcomeUnknown` → HTTP 500, "extension mutation outcome is
  unknown; observe extension state before retrying" — callers are told to
  re-read state rather than blindly retry.
- Validation and not-found errors map to 400/404/409 via
  `extensionLifecycleContextualResponse` in `http_server.zig`: `PackageNotFound`,
  `ExtensionNotInstalled`, `TableNotFound` → 404; `ExtensionAlreadyInstalled`,
  `DependentExtensionExists`, `RequiredExtensionNotInstalled` → 409; malformed
  manifests, scopes, capability grants, or shapes → 400.

Extension lifecycle mutations also require the metadata service's table
topology protocol to be ready before admitting a command that carries table
preconditions (`ensureTableTopologyProtocolReadyWithContext`), so lifecycle
changes cannot race an in-flight table topology transition.

## Object Membership

Every extension-created object is tracked as an `ExtensionMember` with a
stable identity:

```text
<scope.kind>/<scope-name>/<object_kind>/<object_name>
```

(`ExtensionMember.stableIdentityAlloc`; `scope-name` is the table name for
`table` scope, or the literal `cluster`/`embedded_db` otherwise.)

`ExtensionObjectKind` has 21 variants, but `objectKindV1` in `mod.zig` only
accepts nine of them at install time — any other kind fails with
`UnsupportedObjectKindForV1`:

| Accepted in v1 | Declared but rejected in v1 |
| --- | --- |
| `data_shape`, `table_schema`, `extension_relation`, `generated_artifact`, `index`, `enrichment`, `resolver`, `mcp_tool`, `skill`, `agent` | `query_function`, `api_endpoint`, `a2a_agent`, `auth_policy`, `workflow`, `maintenance_task`, `provider_config`, `text_analyzer`, `text_tokenizer`, `provider_adapter`, `connector`, `index_backend` |

(`agent` is accepted for *registration* — see
[MCP Tool Exposure and Permission Filtering](#mcp-tool-exposure-and-permission-filtering)
— but there is no runtime that executes an extension agent; see
[Open work](#open-work).)

Duplicate member identities within one install/update are rejected
(`DuplicateExtensionMember`), and `generated_artifact` objects must reference a
shape of kind `generated_artifact` (`GeneratedArtifactShapeRequired`).

## Table Ownership

`zig/pkg/antfly/src/extensions/table_ownership.zig` answers "does an extension
own this table object" for the direct table/index/enrichment mutation APIs:

- `ownsIndex(snapshot, table, index_name)` / `ownsEnrichment(...)`: true if an
  `ExtensionMember` of kind `index`/`enrichment` on that table has that name.
- `ownsTableShape(snapshot, table)`: true if a `table_schema` member exists for
  the table, or a `data_shape` member of kind `document`/`row` is scoped to it.
- `definitionMutationTouchesOwnedState(...)`: given a proposed table
  definition replacement, allows index-metadata-only changes on a table whose
  schema is extension-owned (schema, indexes, and enrichments are compared
  field by field against every extension member on that table) but reports
  `true` — meaning the mutation must go through the extension lifecycle
  instead of the direct table API — if the replacement would change the
  extension-owned schema itself, or would change an index/enrichment
  definition that an extension member owns.

Callers that hit this (direct table/index mutation paths) surface
`error.ExtensionOwnedObject`, mapped to "table topology changed or is
extension-owned" in `zig/pkg/antfly/src/api/httpx_handler.zig`.

## WASM Runtime and Sandboxing

`zig/pkg/antfly/src/extensions/wasmtime_runtime.zig` implements the one
executable runtime mode that ships today. `RuntimeDecl.mode` accepts
`manifest_only`, `antfly_api_template`, `workflow`, `wasm`, `sidecar`, and
`native`, but only `wasm` has a dispatcher — see [Open work](#open-work) for
the rest.

An extension's `mcp_tool` member handler is a string of the form
`wasm:<runtime_name>/<tool_name>` (`parseWasmHandler` in
`zig/pkg/antfly/src/api/protocol_adapters.zig`). Dispatch resolves the named
`RuntimeDecl` (mode `wasm`) from the installed extension's package, then
resolves an artifact path under the configured `--extension-package-store`
root, trying the content-addressed path first
(`sha256/<digest>/<artifact>`, only when the installed extension's
`package_digest` starts with `sha256:`), then the canonical
`<package_name>/<artifact>` and `<package_name>/<package_version>/<artifact>`
paths. Artifact path segments are restricted to a safe character set and
cannot be absolute or contain `..` (`safeRelativeArtifactPath`,
`safePathSegment`), which also gates the content-addressed digest itself
(`contentAddressedDigest`) against path traversal.

The runtime supports two WASM artifact shapes, detected by magic bytes:

- **WASM component** (`0x00 61 73 6d 0d 00 01 00`): executed through
  `wasmtime`'s component API with WASI preview 2 linked in
  (`wasmtime_component_linker_add_wasip2`). Before execution the store is
  configured with:
  - fuel-based CPU metering (`wasmtime_config_consume_fuel_set` +
    `wasmtime_context_set_fuel`), defaulting to 50,000,000 units
    (`InvokeOptions.fuel`)
  - a memory/table/instance limiter (`wasmtime_store_limiter`), defaulting to
    64 MiB of linear memory (`InvokeOptions.max_memory_bytes`)
  - two narrow host-import interfaces instead of ambient syscalls:
    `antfly:extension/db` (`query`, `write`) and `antfly:extension/ai`
    (`embed`), wired to `HostImports.db_query` / `db_write` / `ai_embed`
    callbacks
- **core WASM module** (`0x00 61 73 6d 01 00 00 00`): executed through a
  narrower C-ABI entrypoint (`invokeExtensionCAbi`) without the WASI/host
  import surface above.

Host imports are capability-checked at call time, not just at install time.
`ExtensionHostContext.dbQuery`/`dbWrite` in `protocol_adapters.zig` call
`requireCapability("db:read")` / `requireCapability("db:write")` before
resolving the target table and executing the query/batch, and resolve table
names through the extension's own scope rather than trusting the WASM guest's
table argument directly.

Failure modes are normalized rather than surfaced raw to MCP callers: runtime
unavailability, an unsupported artifact type, a missing package store, or a
missing artifact all become a generic "extension wasm runtime is unavailable"
tool error (logged with package/version/runtime/artifact detail); any other
invocation error becomes "extension wasm runtime invocation failed".

## API Surface

The `/extensions/v1/*` HTTP surface matches
`specs/openapi/extensions/api.yaml` exactly (see
[Lifecycle](#lifecycle-install-update-drop-enable-disable-configure) for the
path list) and is implemented in
`ApiHttpServer.executeExtensionRoute` (`zig/pkg/antfly/src/api/http_server.zig`),
routed from `zig/pkg/antfly/src/api/httpx_handler.zig`. Extension-owned
objects also augment three existing surfaces:

- `/mcp/v1` (merged) and `/mcp/v1/extensions/{name}` (extension-scoped): see
  [MCP Tool Exposure and Permission Filtering](#mcp-tool-exposure-and-permission-filtering).
- `/agents/v1/extensions/{extension}/{agent}/runs[...]`: routed
  (`extensionAgentRoute` → `ApiHttpServer.executeExtensionAgent`) and
  advertised through ARD, but every call currently returns a fixed
  `{"error":"extension agent runtime not implemented","status":"unsupported_runtime"}`
  response (`extensionAgentUnsupportedRuntimeJsonAlloc` in `http_server.zig`);
  see [Open work](#open-work).
- `/ard/v1/skills/extensions/{extension}/{skill}` and
  `/ard/v1/resources/agents/extensions/{extension}/{agent}`
  (`zig/pkg/antfly/src/api/ard_catalog.zig`): discovery descriptors for
  `skill` and `agent` members, filtered by the same identity/capability
  visibility used for MCP tools below — a caller who cannot see an
  extension's underlying tables or MCP tools does not see its skill or agent
  descriptors either.

`/extensions/v1/*` is not part of `/db/v1`, matching the original design's
reasoning that extensions can affect the database, AI, MCP, A2A, and auth
surfaces and are a platform capability rather than a database subresource.

## MCP Tool Exposure and Permission Filtering

`mcp_tool` members are surfaced as ordinary MCP tools, generalizing the fixed
built-in tool list in `zig/pkg/antfly/src/api/protocol_adapters.zig`
(`create_table`, `query`, `batch`, `backup`, `restore`, ...). Two endpoints
share the same tool-building path (`executeMcpRequestFiltered`):

- `/mcp/v1`: built-in tools plus every visible installed extension's tools.
- `/mcp/v1/extensions/{name}`: only that extension's tools (404 if the
  extension does not exist or is not `ready`).

An extension's tools are only listed while its `InstalledExtension.status ==
.ready` (`extensionRuntimeMemberVisible`) — `disabled`, `installing`,
`updating`, `dropping`, and `error_state` extensions are invisible to MCP
clients without any member data being deleted.

Visibility and invocation are both gated by the caller's identity permissions
intersected with what the tool is allowed to touch
(`extensionMcpToolAllowedForPermissions` in `protocol_adapters.zig`):

- If the `mcp_tool` member declares `required_capabilities` (parsed from its
  `owner_metadata_json`), each capability named `db:read`/`read:table`,
  `db:write`/`write:table`, or `db:admin`/`admin:table` is mapped to a
  required table permission and checked against the caller's permissions on
  the resolved table resource (the capability's own `scope`, falling back to
  the member's table scope, then the installed extension's table scope, then
  `"*"`).
- Otherwise, visibility falls back to requiring `read` on the member's (or
  installed extension's) table scope, or cluster `admin` for `cluster`/
  `embedded_db`-scoped tools.

This check runs twice: once to decide whether to list the tool at all, and
again immediately before dispatch (`ExtensionToolContext.call`) — the code
comment in `protocol_adapters.zig` notes discovery filtering is UX, not an
authorization boundary, since a client could retain a stale tool list or
hand-craft a `tools/call`.

## Auth and RBAC

The entire `/extensions/v1/*` path prefix requires cluster-wide `admin`
permission: `requiresAdminPermission` in `http_server.zig` routes
`isExtensionPath` results through the same admin gate used for Raft admin,
storage maintenance, secrets, and user management, so only an identity with
`("*", "*", admin)` can list packages, or install/update/drop/enable/disable/
configure an installed extension.

Below that gate, per-tool and per-table authorization for extension MCP tools
and WASM host calls follows the same `usermgr.Permission` model as the rest of
the API (resource type/resource/permission type, with `admin` permission
satisfying any lower requirement and `"*"` resources/types acting as
wildcards) — see
[MCP Tool Exposure and Permission Filtering](#mcp-tool-exposure-and-permission-filtering).
There is no separate `auth_policy` extension object kind in v1 (it is declared
in `ExtensionObjectKind` but rejected by `objectKindV1`), so extensions cannot
declare their own roles or permission templates yet.

## Backup, Restore, and Failure Semantics

Cluster backup manifests (`zig/pkg/antfly/src/api/backups.zig`) round-trip
`installed_extensions`, `extension_members`, and `extension_dependencies`
verbatim. Registered *packages* are not part of the backup manifest — restore
assumes the target cluster's package store already has (or will be scanned
into) the referenced package name/version/digest; `extension_lifecycle.
restoreOnService` in `lifecycle.zig` replays the installed/member/dependency
rows directly into metadata without re-running install validation.

Failure-response mapping for the lifecycle API is centralized in
`extensionLifecycleContextualResponse` (`http_server.zig`) and summarized in
[Lifecycle](#lifecycle-install-update-drop-enable-disable-configure) above:
404 for missing packages/extensions/tables, 409 for already-installed/
dependent-exists/required-not-installed/concurrent-conflict, 400 for
malformed requests, and a distinguished 500 for an unconfirmed (ambiguous)
metadata mutation outcome that tells the caller to observe state rather than
retry blindly.

## Open work

Everything below is design intent that has not shipped. Where the original
design proposed sequencing ("Phase A", "Phase 0", etc.), that sequencing is
omitted here — none of it is scheduled; it is simply not built yet.

**Package distribution and trust**

- No package registry, publishing flow, or signing. Packages exist only as
  files under an operator/developer-controlled local directory
  (`--extension-package-store`), scanned at startup.
- No cluster-level allowlist/denylist or tenant-level install policy.
- `PackageManifest.trusted`, `antfly_min_version`, and `antfly_max_version`
  are stored but never checked against a trust policy or the running Antfly
  version.
- `digest` is a self-declared string used for content-addressed lookup and
  install-time pinning; nothing computes or verifies a cryptographic digest
  of the manifest or artifact bytes.

**Developer SDK and ABI**

- No `antfly package init/build/test/publish` or `antfly extension
  install/update/drop` CLI wrappers — the HTTP API is the only surface today.
- No local conformance test harness for manifests, upgrade paths, or resource
  limits, and no versioned ABI contract independent of the Antfly binary.

**Data shapes and migration**

- Only `document`/`row` shapes at `table` scope are semantically validated
  (against the table schema parser). `generated_artifact`,
  `extension_relation`, `endpoint_schema`, and `tool_schema` shapes, and any
  shape at `cluster`/`embedded_db` scope, are only checked for being valid
  JSON.
- No public shape language beyond an opaque `schema_json` string, no shape
  versioning/compatibility rules beyond the plain `version` string field, and
  no imperative migration/backfill hooks.
- `config_schema_json` on a package's install manifest is stored but never
  used to validate an installed extension's `config_json`.

**Object kinds beyond v1**

`query_function`, `api_endpoint`, `a2a_agent`, `auth_policy`, `workflow`,
`maintenance_task`, `provider_config`, `text_analyzer`, `text_tokenizer`,
`provider_adapter`, `connector`, and `index_backend` are declared in
`ExtensionObjectKind` and in the `specs/openapi/extensions/api.yaml` enum, but
`objectKindV1` rejects all of them at install time
(`UnsupportedObjectKindForV1`). In particular there is no public index access
method interface (build/apply/delete/query/snapshot hooks) and no auth-policy
object kind, so extensions cannot yet define their own index backends, roles,
or permission templates.

**Runtime handler modes beyond WASM**

`RuntimeMode` includes `antfly_api_template`, `workflow`, `sidecar`, and
`native` in addition to `manifest_only` and `wasm`, but only `wasm` handlers
(`wasm:<runtime>/<tool>`) are dispatched. An `mcp_tool` member with any other
handler mode returns "extension MCP tool '...' is registered but has no
executable handler." There is no sidecar runtime, no native-code loading path
(local/embedded or operator-installed), and no declarative
`antfly_api_template` handler that maps tool arguments onto existing Antfly
API calls.

**Extension agents**

`agent` is a v1-installable member kind, and agent descriptors are discoverable
through ARD (`/ard/v1/resources/agents/extensions/{extension}/{agent}`) with
an advertised `runEndpoint` under `/agents/v1/extensions/{extension}/{agent}/
runs`. But every run/events/cancel call against that endpoint currently
returns a fixed `unsupported_runtime` error — there is no agent execution,
streaming, or cancellation runtime, no generated MCP wrapper tool for an
agent, and no A2A card/task routing for extension agents.

**Capability model**

Only capability names that map to a table permission
(`db:read`/`read:table`, `db:write`/`write:table`, `db:admin`/`admin:table`)
are enforced, and only against table resources. Other capability names from
the original design — secrets access, outbound network allowlists,
filesystem/object-storage access, provider/model consumption, maintenance-task
execution — can be requested and granted as opaque strings but are not
checked by any runtime path.

**Hook surface**

Beyond the WASM `db`/`ai` host imports used by MCP tool handlers, none of the
originally proposed hook families exist: query functions (scalar/vector
functions, filter predicates, score transforms, rerankers), analysis hooks
(tokenizers, analyzers, normalizers, synonym sources), graph hooks (traversal,
path scoring, centrality/community metrics), or storage/index hooks
(access-method build/apply/delete/query/snapshot/migration).

**Operational lifecycle and performance tiering**

No install/upgrade dry-run diff beyond returning the planned
`InstalledExtension` row, no rolling activation across shard groups, no
extension health surfaced in admin/cluster status snapshots beyond the raw
`InstalledExtension.status`, and no backup/restore preflight for missing
package versions. The originally proposed performance tiers
(`core_internal`, `trusted_native`, `batched_wasm`, `sidecar`,
`manifest_only`) are a design framing, not an implemented classification —
today an extension object is either manifest-only (no runtime cost after
metadata reconciliation) or dispatched through the single WASM runtime
described above.

**Worked examples from the original design**

The original design's `memoryaf` (memory extension: memory/event/edge tables,
full-text + vector + graph indexes, `remember`/`recall`/`search_memory`/
`forget`/`link_memory`/`summarize_memory` MCP tools, background
dedup/compaction/decay tasks) and `antfly_durable` (durable in-database
workflow extension modeled on Microsoft's `pg_durable`, with workflow/
instance/checkpoint/timer/work-queue records and start/cancel/signal/inspect
handlers) are illustrative target-state scenarios, not installed extensions
that exist in the codebase today. They remain useful as worked examples of
what the shipped catalog, membership, and (eventually) non-WASM/non-table
object kinds are meant to support, but every MCP tool, background task, and
workflow DAG shown for them is hypothetical.

**Deferred questions carried over from the original design**

- Standalone package kinds beyond `extension` (thin `mcp_app` wrappers,
  `workflow_pack`, `model_adapter`, `analyzer_pack`, `connector`,
  `index_backend`).
- The public schema language for extension-owned shapes (JSON Schema, an
  Antfly-specific format, OpenAPI components, or a combination).
- The durable job API that would back imperative migration hooks and
  hosted-safe long-running runtimes.
- The conformance suite required before a public index backend could be
  marked hosted-installable.
