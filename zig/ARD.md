# Antfly Agentic Resource Discovery Design

## Context

Google announced Agentic Resource Discovery (ARD) on June 17, 2026 as an open specification for publishing, discovering, and verifying tools, skills, agents, MCP servers, A2A agents, OpenAPI APIs, and related agentic resources across the web:

https://developers.googleblog.com/announcing-the-agentic-resource-discovery-specification/

Specification reference:

https://agenticresourcediscovery.org/spec/

The ARD specification is intentionally a discovery layer. It does not replace MCP, A2A, OpenAPI, Skills, or Antfly extensions. It publishes enough metadata for an agent or registry to find and verify a capability, then invocation proceeds through the capability's native protocol.

Antfly already has most of the native execution surfaces ARD should advertise:

- MCP: `/mcp/v1`
- extension-scoped MCP: `/mcp/v1/extensions/{extension}`
- extension MCP profiles: `/mcp/v1/extensions/profiles/{profile}`, starting with `copilot`
- A2A: `/a2a`
- A2A agent card: `/.well-known/agent-card.json`
- extension lifecycle and package metadata: `/extensions/v1`
- OpenAPI specs: `/ard/v1/openapi.yaml`, backed by `specs/openapi/ard/api.yaml`, and `/ard/v1/openapi/{spec}.yaml`, backed by generated or source specs from `openapi.yaml` and `specs/openapi/**`
- agent-like HTTP endpoints: retrieval, query builder, and table APIs

The design goal is to add ARD as a thin, tenant-aware discovery/export layer over these surfaces, not as a new execution runtime.

## Principles

1. Catalogs are scoped by auth, tenant, profile, and permissions.

   Antfly's valuable resources are usually tenant-local: installed extensions, table-bound tools, MCP profiles, table schemas, and private OpenAPI operations. The catalog builder must use the same visibility policy as the corresponding invocation surface. A client must not learn about a resource through ARD that it could not discover through MCP, extension APIs, or OpenAPI under the same identity.

2. `/.well-known/ai-catalog.json` can be tenant scoped.

   The ARD publishing path is `/.well-known/ai-catalog.json`. For public ARD crawling, this route must be reachable as ordinary JSON over HTTPS and should include crawler-friendly CORS. Antfly can also make the same path auth-aware for enterprise/internal clients: use request auth when present, return a scoped catalog for authenticated requests, and return only public bootstrap entries or `401` for unauthenticated requests depending on deployment policy.

3. `/ard/v1/catalog` is the canonical Antfly API route.

   This is not a required ARD path. It is an Antfly convenience route for authenticated agents, gateways, enterprise registries, and product documentation. The protocol-compatible publishing route remains `/.well-known/ai-catalog.json`. Both routes should call the same catalog builder.

4. ARD entries point to native protocols.

   MCP entries point to MCP endpoints, A2A entries point to the agent card or A2A endpoint, OpenAPI entries point to OpenAPI documents, and skills point to skill markdown or JSON descriptors. ARD should not proxy invocation unless a future product requirement needs it.

   Domain APIs such as `/db/v1`, `/ai/v1`, `/extensions/v1`, `/mcp/v1`, and `/a2a` should remain stable invocation surfaces. ARD owns discovery, cataloging, and skill artifacts that describe how to use those surfaces together.

5. Use `/ard/v1` as the Antfly ARD registry base.

   ARD discovers dynamic registry APIs through catalog entries with type `application/ai-registry+json`. If Antfly exposes a registry, that entry should point to `/ard/v1`, and the standard registry operations should be available underneath it as `POST /ard/v1/search`, `POST /ard/v1/explore`, and `GET /ard/v1/agents`.

6. Start with publishing before search.

   A correct catalog exporter is the foundation. Registry search (`POST /ard/v1/search`) should be a second layer over the same generated entries, with permission filtering preserved.

## Routes

### `GET /.well-known/ai-catalog.json`

Compatibility route for ARD clients and crawlers.

Behavior:

- If auth is present, return the same scoped catalog as `/ard/v1/catalog`.
- If auth is absent and public ARD publishing is enabled, return a coarse public bootstrap catalog.
- If auth is absent and public ARD publishing is disabled, return `401`.
- In public mode, serve with `Content-Type: application/json` and crawler-friendly CORS.

For authenticated deployments, public ARD publishing is controlled by `--ard-public-catalog <bool>` and defaults to disabled. Local/no-auth deployments may still serve the public bootstrap catalog because there is no tenant auth layer available to enforce.

The public bootstrap catalog should not include tenant-installed extensions, table names, private MCP profiles, private OpenAPI specs, or per-tenant skills. It may include entries such as:

- an `application/ai-registry+json` entry pointing at `/ard/v1`
- an `application/ai-catalog+json` entry pointing at `/ard/v1/catalog` if authenticated catalog fetches should be discoverable
- the public A2A agent card, if configured for public exposure
- public documentation or public OpenAPI specs, if explicitly configured

### `GET /ard/v1/catalog`

Canonical Antfly authenticated tenant catalog.

Behavior:

- Requires normal Antfly auth when auth is enabled.
- Applies tenant and permission filtering.
- Accepts optional query params:
  - `profile`: restrict to a profile such as `copilot`
  - `types`: comma-separated ARD media types
  - `include`: optional classes such as `mcp`, `a2a`, `openapi`, `skills`, `extensions`

### `POST /ard/v1/search`

Second-phase registry endpoint over the same scoped entries. This is the ARD `POST /search` operation when `/ard/v1` is advertised as the registry base URL.

Behavior:

- Requires normal Antfly auth when auth is enabled.
- Accepts the ARD query model: `{ "query": { "text": "...", "filter": { ... } } }`.
- Requires non-empty `query.text`, matching ARD Search semantics. Filter-only discovery belongs on `/ard/v1/explore`.
- Returns ranked catalog entries.
- Accepts ARD `federation` modes: `none`, `referrals`, and `auto`.
- Returns local scoped results for all modes. Until Colony or external registries are configured, `referrals` and `auto` return an explicit empty `referrals` array rather than silently pretending federation is unavailable.
- Uses Antfly itself to index and search generated catalog entries when practical.

This should come after the catalog builder is stable.

### `POST /ard/v1/explore`

Registry exploration endpoint over the same scoped entries. This is the ARD `POST /explore` operation when `/ard/v1` is advertised as the registry base URL.

Behavior:

- Requires normal Antfly auth when auth is enabled.
- Accepts the ARD query model: `{ "query": { "text": "...", "filter": { ... } } }`.
- Accepts a text query, a filter, both, or neither.
- Returns deterministic facet inventory for questions such as available media types, publishers, tags, or capabilities.
- Uses the same auth, tenant, profile, media-type, include, and permission scoping as `/ard/v1/catalog`.

### `GET /ard/v1/agents`

Registry agent-list endpoint over the same scoped entries. This is the ARD `GET /agents` operation when `/ard/v1` is advertised as the registry base URL.

Behavior:

- Requires normal Antfly auth when auth is enabled.
- Returns visible agent-like resources only, currently A2A agent cards and MCP server descriptors.
- Accepts `filter` as a percent-encoded ARD filter object.
- Accepts `orderBy` for `identifier`, `displayName`, or `type`, with optional `asc`, `desc`, or a leading `-`.
- Accepts `pageSize` and `pageToken`, bounded to the same maximum page size as search.
- Uses the same auth, tenant, profile, media-type, include, and permission scoping as `/ard/v1/catalog`.

### `GET /ard/v1/skills/{skill}`

Tenant-scoped skill artifact endpoint.

Behavior:

- Requires normal Antfly auth when auth is enabled.
- Returns a markdown or JSON skill artifact referenced by `/ard/v1/catalog`.
- Applies the same visibility policy as the catalog entry that linked to it.
- May describe workflows spanning several APIs or protocols, for example `/db/v1`, `/extensions/v1`, `/mcp/v1`, `/a2a`, and OpenAPI specs.

Skills should not live under individual invocation APIs such as `/db/v1/skills` or `/ai/v1/skills` by default. A skill is a discovery/instruction artifact and can span multiple APIs. Keeping it under `/ard/v1` lets skill formats evolve with the ARD lifecycle without moving or versioning the underlying product APIs.

### `GET /ard/v1/resources/{kind}/{resource...}`

Optional artifact descriptor endpoint for resources whose native invocation URL is not itself a JSON descriptor.

ARD catalog entries require exactly one of `url` or `data`, and `url` is the URL to retrieve the artifact document. For invocation protocols such as MCP, the runtime endpoint (`/mcp/v1`) is not necessarily the same as the JSON artifact document. Antfly should use one of these two patterns:

- Embed the descriptor directly with `data`.
- Point `url` at `/ard/v1/resources/{kind}/{resource...}` and include the native runtime endpoint inside that descriptor or entry metadata.

This keeps Antfly aligned with ARD's strict value-or-reference model while preserving native MCP/A2A/OpenAPI invocation URLs.

## Catalog Shape

Antfly should emit ARD-compatible `ai-catalog.json`:

```json
{
  "specVersion": "1.0",
  "host": {
    "displayName": "Antfly",
    "identifier": "did:web:example.com"
  },
  "entries": []
}
```

Host identity should be configurable. For hosted Colony deployments, the host identity should be tenant/deployment aware. For local dev, a domain identity may be absent.

Antfly exposes host identity through API server config and the data/standalone runtime flags:

```text
--ard-base-url <url>
--ard-publisher-domain <domain>
--ard-display-name <name>
```

These values drive artifact URL publication, the catalog `host.identifier`, host trust identity, publisher segment in `urn:ai:<publisher-domain>:...` identifiers, publisher facets, and publisher filters. If `--ard-base-url` is unset, Antfly keeps route-relative artifact URLs for local and in-process clients. Hosted or federated deployments should set it to the externally reachable HTTPS origin so ARD registries can dereference catalog artifacts.

Federated ARD identifiers still require the `urn:ai:<publisher-domain>:...` publisher segment to be a verifiable domain. Local development deployments without a configured publisher domain should either use a configured development domain or mark the catalog as non-federated/local-only so it is not advertised to external registries.

Entry identifiers should use ARD domain-anchored URNs:

```text
urn:ai:<publisher-domain>:<namespace>:<name>
```

Proposed namespaces:

- `antfly:mcp`
- `antfly:mcp-profile`
- `antfly:a2a`
- `antfly:openapi`
- `antfly:skill`
- `antfly:extension`

Examples:

```text
urn:ai:example.com:antfly:mcp
urn:ai:example.com:antfly:mcp-profile:copilot
urn:ai:example.com:antfly:a2a:default
urn:ai:example.com:antfly:openapi:public
urn:ai:example.com:antfly:skill:query-builder
urn:ai:example.com:antfly:extension:memoryaf:mcp
```

## Publication Discovery

Antfly should implement `/.well-known/ai-catalog.json` first because it is the primary ARD publishing path. Additional ARD discovery mechanisms can be added later without changing the catalog builder:

- robots or agent-map directive pointing at the catalog
- HTML `<link rel="ai-catalog" href="...">`
- DNS records pointing at a static catalog or registry search endpoint

These mechanisms should advertise only the public bootstrap catalog or the `/ard/v1` registry base. Tenant-scoped catalogs still require Antfly auth and should not be advertised as anonymously crawlable resources.

## Resource Entries

### MCP

Aggregate MCP entry:

- `type`: `application/mcp-server+json`
- `url`: `/ard/v1/resources/mcp/default` or `data`: embedded MCP server descriptor
- `metadata.endpoint`: `/mcp/v1`
- `capabilities`: derived from visible built-in MCP tools and visible extension MCP tools
- `representativeQueries`: Antfly search/query/table-management examples

Extension-scoped MCP entry:

- `type`: `application/mcp-server+json`
- `url`: `/ard/v1/resources/mcp/extensions/{extension}`
- `metadata.endpoint`: `/mcp/v1/extensions/{extension}`
- Visible only when the extension is installed, ready, and visible to the identity.
- The resource descriptor should include only MCP tools visible to the same identity.

MCP profile entry:

- `type`: `application/mcp-server+json`
- `url`: `/ard/v1/resources/mcp/profiles/{profile}` or `data`: embedded MCP server descriptor
- `metadata.endpoint`: `/mcp/v1/extensions/profiles/{profile}`
- `metadata.profile`: profile name, for example `copilot`
- Visible only when the profile resolves to at least one visible tool.

The first concrete profile should be `copilot`. Until Antfly has profile-specific policy configuration, `copilot` should resolve through the aggregate MCP visibility policy and remain explicitly identified by both the MCP endpoint and ARD resource descriptor:

```text
/mcp/v1/extensions/profiles/copilot
/ard/v1/resources/mcp/profiles/copilot
```

The catalog builder should reuse the same permission filtering used by MCP tool listing.

### A2A

A2A agent card entry:

- `type`: `application/a2a-agent-card+json`
- `url`: `/.well-known/agent-card.json`
- `metadata.endpoint`: `/a2a`
- `capabilities`: retrieval, query-builder, and other public agent capabilities

If the agent card itself becomes tenant-specific, the well-known card route should follow the same auth-aware behavior as the ARD catalog.

### OpenAPI

OpenAPI entries should advertise machine-readable Antfly API specs.

Initial entries:

- ARD discovery API: `/ard/v1/openapi.yaml`
- public Antfly API: `/ard/v1/openapi/antfly.yaml`
- metadata/table/retrieval API: `/ard/v1/openapi/metadata.yaml`
- extensions API: `/ard/v1/openapi/extensions.yaml`, admin-only
- auth/user management API: `/ard/v1/openapi/auth.yaml`, admin-only
- inference/config API: `/ard/v1/openapi/inference-config.yaml` where applicable

Candidate media types:

- `application/openapi+yaml`
- `application/openapi+json`
- `application/vnd.oai.openapi+json;version=3.0`

The ARD v0.9 spec is artifact-agnostic and identifies artifacts by IANA-style media type. It explicitly notes that some agent protocol media types are still settling, so Antfly should avoid strict media-type assumptions internally.

Each OpenAPI entry should include:

- `url`: route serving the JSON or YAML spec
- `capabilities`: coarse operation groups, not every operation
- `metadata.requiredPermissions`: Antfly permission hints
- `metadata.sourceSpec`: path or generated module name for debugging

Admin-only OpenAPI entries should be hidden from non-admin scoped catalogs and the corresponding `/ard/v1/openapi/{spec}.yaml` resource route should return `403` for authenticated non-admin identities when auth is enabled. Public, metadata/table, and inference specs can be visible to authenticated tenant identities by default; operation invocation remains protected by the native route permissions.

### Skills

Skills should be first-class discovery artifacts, not extension runtimes.

Antfly-owned default skills should be packaged as source-controlled ARD skill artifacts, not handwritten Zig entries. The default Antfly distribution can embed a manifest such as `pkg/antfly/src/api/ard/default_skills.json` so a fresh tenant has useful Antfly skills even before installing extensions.

Initial Antfly default skills:

- `antfly-query-builder`: translate user intent into Antfly queries.
- `antfly-retrieval`: retrieve and synthesize context from Antfly tables.
- `antfly-schema-design`: design tables, schemas, indexes, enrichments, and query processors.
- `antfly-extension-management`: install, configure, enable, disable, and inspect extensions.

Extension-owned skills must be declared by extension metadata, not hardcoded into Antfly. Antfly core should not keep a list of known extension names or built-in extension skills. An extension can contribute an `ExtensionObjectKind.skill` member whose `owner_metadata_json` describes display text, markdown body, tags, profile, capabilities, representative queries, and required capabilities. Memoryaf, for example, should contribute its memory skill from the Memoryaf package or install metadata, not from Antfly's default skill manifest.

Extension-owned skills must be emitted only when the corresponding installed extension skill member is visible to the caller. The skill artifact route must apply the same extension visibility check as the catalog entry.

Serve skill artifacts under the ARD namespace:

```text
/ard/v1/skills/{skill}
/ard/v1/skills/extensions/{extension}/{skill}
```

ARD entries:

- `type`: `application/ai-skill+md`
- `url`: `/ard/v1/skills/{skill}` for built-in Antfly skills, `/ard/v1/skills/extensions/{extension}/{skill}` for extension-owned skills
- `capabilities`: short task labels
- `representativeQueries`: 2-5 natural-language examples

Skills can also point to extension MCP profiles where the skill is only useful with tools installed.

### Extensions

Extension package and installed extension entries should be included only in authenticated scoped catalogs unless explicitly public.

Package visibility must be derived from a visible installed extension, not from the tenant package-store inventory. A table-scoped identity that can see `docsaf` must not learn that `memoryaf` packages are present merely because both packages exist in metadata.

Package entry:

- `identifier`: `urn:ai:<publisher-domain>:antfly:extension:package:{name}:{version}`
- `type`: `application/antfly-extension-package+json`
- `url`: `/extensions/v1/packages/{name}/versions/{version}`
- `metadata.digest`: package digest
- `metadata.kind`: package kind
- `metadata.trusted`: package trust flag
- `metadata.artifactCount`: count of manifest/wasm/native artifacts
- `metadata.capabilitiesRequestedCount`: count of requested extension capabilities
- `trustManifest.provenance`: package digest and artifact lineage

Antfly should keep the package URL as the canonical artifact document and put rich digest/artifact lineage in `trustManifest.provenance`. The catalog entry must preserve ARD's exactly-one-of `url` or `data` rule.

Catalog `metadata` must remain schema-compatible scalar key/value data. Extension package artifact lists, artifact digests, and install lineage belong in `trustManifest.provenance`; the catalog may keep scalar hints such as package digest, package kind, trust boolean, artifact count, scope string, and capability counts in `metadata`.

Installed extension entry:

- `type`: `application/antfly-installed-extension+json`
- `url`: `/extensions/v1/installed/{name}`
- `metadata.digest`: installed package digest
- `metadata.packageName`: package name
- `metadata.packageVersion`: package version
- `metadata.scope`: concrete scope string such as `cluster`, `embedded_db`, or `table:{name}`
- `metadata.scopeKind`: cluster/table/embedded_db
- `metadata.scopeTableName`: table name only for table-scoped installs
- `metadata.status`: ready/disabled/etc.
- `metadata.grantedCapabilitiesCount`: count of granted extension capabilities
- `trustManifest.provenance`: installed extension lineage back to the package version and digest

Extension MCP entries remain separate because they are directly invokable resources.

## Auth and Tenant Scoping

Catalog generation should take an identity object:

```text
CatalogContext {
  base_url
  publisher_domain
  authenticated_identity
  tenant
  profile
  include_public_bootstrap
}
```

Every entry producer must be pure with respect to this context:

- Built-in MCP entries use MCP tool visibility.
- Extension MCP entries use extension install status, granted capabilities, and table permissions.
- OpenAPI entries use route-level permissions.
- Skill entries use declared permissions and related capability visibility.
- Extension entries use extension lifecycle visibility and tenant boundaries.

The builder should support three modes:

1. `public_bootstrap`: unauthenticated safe catalog.
2. `tenant_catalog`: authenticated full scoped catalog.
3. `profile_catalog`: authenticated catalog narrowed to a profile such as `copilot`.

Only `public_bootstrap` is intended for open-web crawling. `tenant_catalog` and `profile_catalog` are Antfly enterprise/internal discovery modes. They are still valid JSON catalogs, but they are not meant to be indexed by unauthenticated public ARD crawlers.

## Trust Metadata

Start with structural fields; add cryptographic validation later.

Initial trust metadata:

- host identifier, if configured
- package digest for extension packages
- artifact digests from extension manifests
- source OpenAPI spec path or generation metadata

Initial implementation should emit:

- host `trustManifest.identity` matching the catalog `did:web` host identifier
- extension package `trustManifest.provenance` entries for package digest and artifact digests
- installed extension `trustManifest.provenance` pointing back to the package version and digest
- scalar-only `metadata` values so catalogs remain compatible with the ARD `ai-catalog` schema

Later:

- signed trust manifests
- deployment identity such as SPIFFE or DID
- extension package signature verification
- provenance links from package source, image, or git commit

## Implementation Plan

### PR 1: ARD Catalog Types and Routes

- Add ARD catalog structs and JSON serialization.
- Add route constants for `/.well-known/ai-catalog.json` and `/ard/v1/catalog`.
- Implement a catalog builder with public and authenticated modes.
- Emit A2A, aggregate MCP, and bootstrap entries.
- Emit an `application/ai-registry+json` entry for `/ard/v1` when the registry API is enabled.
- Add tests for:
  - unauthenticated public catalog does not leak tenant resources
  - authenticated catalog includes scoped MCP/A2A entries
  - well-known route delegates to the same builder
  - every entry contains `identifier`, `displayName`, `type`, and exactly one of `url` or `data`

### PR 2: OpenAPI and Skill Entries

- Add routes to serve selected OpenAPI specs if not already served.
- Add `/ard/v1/skills/{skill}` skill artifacts.
- Add OpenAPI and skill entry producers.
- Add tests for permission-filtered OpenAPI and skill visibility.

### PR 3: Extension and MCP Profile Entries

- Add extension MCP profile catalog entries.
- Add installed extension and extension package entries.
- Reuse extension MCP permission filtering from `/mcp/v1`.
- Add tests for tenant and table permission filtering.

### PR 4: ARD Search API

- Add `POST /ard/v1/search`.
- Build a scoped in-memory implementation first.
- Optionally index catalog entries into Antfly for hybrid search.
- Support common filters: `type`, `tags`, `capabilities`, `publisher`/`publisherId` derived from the `urn:ai` identifier, and `metadata.*`.
- Validate `federation` as `none`, `referrals`, or `auto`; return local scoped results for all modes.
- Return an empty `referrals` array for `referrals`/`auto` until Colony or external registries are integrated.

### PR 5: Trust and Federation

- Add `trustManifest` support.
- Add package and artifact digest links.
- Consider `application/ai-registry+json` entries for Antfly/Colony registries.
- Populate `referrals` from Colony-managed or external registries.

## Open Questions

- What is the canonical publisher domain in local, self-hosted, and Colony-hosted deployments?
- Should public `/.well-known/ai-catalog.json` default to `401` or a bootstrap-only catalog?
- Should `/ard/v1/catalog` and `/.well-known/ai-catalog.json` return identical authenticated output or should the well-known route always omit some internal metadata?
- Where should skill source content live before it is served through `/ard/v1/skills/{skill}`: source-controlled docs, generated from OpenAPI/docs, extension-owned package content, or a combination?
- Should individual OpenAPI specs eventually be operation-filtered per identity instead of exposed or hidden as whole documents?
- Do future profiles belong only under `/mcp/v1/extensions/profiles/{profile}`, or should ARD introduce profile catalogs independent of MCP after `copilot`?
- Should Colony own cross-tenant/global registry search while Antfly nodes only expose tenant-local catalogs?

## Recommended Default

Use `/ard/v1/catalog` as the primary authenticated integration point. Make `/.well-known/ai-catalog.json` auth-aware and safe by default:

- authenticated request: tenant-scoped catalog
- unauthenticated request with public publishing disabled in an authenticated deployment: `401`
- unauthenticated request with `--ard-public-catalog true`: public bootstrap catalog only

This preserves ARD compatibility without leaking tenant topology, extension inventory, table names, or profile-specific tools.
