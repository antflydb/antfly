# Web Search

This document defines the long-run web-search provider model for Antfly agents
and the public connection configuration shape.

Web search is a first-class external capability. It should not be modeled as
plain HTTP access because it has query semantics, ranking, freshness, snippets,
citations, provider display rules, and agent-tool behavior.

## Retrieval Agent: Exa and Tavily

Exa's provider wire types are generated from `zig/specs/exa-openapi.yaml`,
vendored from `exa-labs/openapi-spec` at commit
`57d917823aa0cec02385104dc3bb795cdf5d7da8`. The source URL and revision are recorded
in the file header. Like OpenAI, Exa has a checked-in Zig types module under
`pkg/antfly/src/openapi/generated/exa_api`; `make generate` regenerates it and
`make zig-openapi-check` detects drift. The build exposes the upstream inline
search request and response schemas as named components without changing their
fields. The adapter uses those generated types for serialization and parsing;
connection policy, credentials, response limits, and citation normalization stay
in the adapter. To update, replace the vendored spec from a pinned upstream
revision, update its header and this revision, then regenerate and test.

The Zig retrieval agent executes Exa searches in agentic mode. Configure a named
connection on the server:

```json
{
  "connections": {
    "agent-web": {
      "kind": "web_search",
      "provider": "exa",
      "capabilities": ["web.search", "agents.use"],
      "web_search": {
        "api_key": "${secret:exa.api_key}",
        "max_results": 5,
        "timeout_ms": 10000,
        "include_content": true,
        "include_highlights": true
      }
    }
  }
}
```

Use the secret store or `EXA_API_KEY` environment fallback. Secret references
are resolved through Antfly's existing secret handling. Send this request to
`POST /db/v1/agents/retrieval` (supply the OpenAI key or secret reference in the
generator's `api_key`):

```json
{
  "query": "Find Antfly hybrid-search documentation on the web and cite the source URLs.",
  "queries": [],
  "stream": false,
  "max_internal_iterations": 4,
  "generator": {
    "provider": "openai",
    "model": "gpt-4.1-mini",
    "url": "https://api.openai.com/v1",
    "api_key": "${secret:openai.api_key}"
  },
  "tools": {
    "enabled_tools": ["web_search"],
    "web_search_connection": "agent-web"
  },
  "steps": {"generation": {}}
}
```

An empty `queries` array is supported for web-only requests. To combine web and
database retrieval, supply the table queries and enable the corresponding
retrieval tools too (or omit `enabled_tools`). Web search requires a generator
and positive `max_internal_iterations`; pipeline mode does not perform web calls.
The same limits apply to web calls and database tools. A failed provider call
produces an error step and repair feedback, and does not qualify as evidence for
an answer. Search results are returned in `hits`, with IDs prefixed `web:`, and
`_source.provider`, `url`, `title`, and configured `text`/`highlights`. They use the
existing `hit` and step events when `stream` is true. The database-only
`strategy_used` field is omitted for web-only retrieval.

The CLI also accepts a named connection:

```sh
antfly agents retrieval --web-search-connection agent-web \
  --intent 'Find Antfly hybrid-search documentation and cite URLs' \
  --generator '{"provider":"openai","model":"gpt-4.1-mini","url":"https://api.openai.com/v1","api_key":"${secret:openai.api_key}"}'
```

For development, replace `web_search_connection` with
`web_search_config: {"provider":"exa","api_key":"...","include_content":true}`.
Inline Exa options include `max_results`/`num_results` (1–20), `timeout_ms`,
`safe_search` (sent as Exa `moderation`), `search_type`, published-date bounds,
`include_domains`, `exclude_domains`, `region` (two-letter country code), and
content/highlight switches. Domain filters accept domain names, including their
subdomains. Result hostnames are percent-decoded and compared without DNS root
dots; non-ASCII hostnames must use their IDNA ASCII form. Language filtering is
unsupported and rejected. The wire format
follows the [Exa search API](https://exa.ai/docs/reference/search).

Inline options preserve omission: generated clients leave unspecified fields
unset, while explicit `false` values disable content/highlights. Omitted settings
inherit the connection, or use server defaults when no connection is supplied.

A request may supply a named connection plus inline options to narrow its
result limit, timeout, domains, or content settings. It cannot replace the
connection's provider, endpoint, credentials, or expand content/domain access.
Configure custom endpoints on the server's named connection; inline requests
are restricted to the selected provider's default endpoint. Redirects are disabled. Provider
responses are capped at 1 MiB and text/highlights at 4,000 bytes each per result;
web, database, and navigation evidence retained in model history shares one
cumulative context budget. Retrieval returns `incomplete` if another result
cannot fit, including within a batch of parallel tool calls. Database counts,
aggregations, and other summaries can still support an answer when document
bodies are pruned, provided the summaries fit within the remaining budget. Configuration belongs
in either top-level `tools` or `steps.retrieval.tools`, not both. Both tool
allowlists still apply. Other provider tokens describe the shared connection
contract below; this retrieval adapter implements Exa and Tavily.

### Tavily

For Tavily, set the named connection's `provider` to `tavily`, use
`${secret:tavily.api_key}` (or omit `api_key` to use `TAVILY_API_KEY`), and set
`include_content: true` to retain search snippets as evidence. Omit
`include_highlights`, which Tavily does not support. The default endpoint is
`https://api.tavily.com/search`, authenticated with a bearer token.

Inline configuration uses `provider: tavily` and supports `max_results`,
`timeout_ms`, `safe_search`, `include_content`, domain filters, `search_depth`
(`basic` or `advanced`), `include_answer`, and `include_raw_content`. The default
search depth is `basic`. Raw content is returned as bounded `_source.text`, with
snippet fallback when unavailable. A named connection must allow content before
an inline request can enable raw content. Provider-generated answers are not
source evidence and are never added to hits or model history. Region, language,
and enabled highlights are rejected rather than silently ignored.

The wire types are generated from the unmodified official
[Tavily OpenAPI spec](https://docs.tavily.com/documentation/api-reference/openapi.json),
vendored at `zig/specs/tavily-openapi.json` on 2026-09-22 (SHA-256
`cddb9b1828c10e323584d495dbd768cd70f0ce86878f4749335d1dd8769b30c4`).
The existing OpenAPI generation pipeline aliases its inline request and result
schemas. Tavily documents nullable `raw_content` and `published_date` but omits
`nullable` in the schema; the adapter treats those nulls as absent before typed
decoding. Unused response metadata, including the conditionally present
`answer`, is not required for source retrieval.

An opt-in live smoke test exercises real Tavily search through Antfly's public
retrieval endpoint in JSON and SSE modes, with a deterministic local generator.
It performs two basic searches (up to two results each):

```sh
# Set TAVILY_API_KEY securely in the environment first.
cd zig
ANTFLY_BIN="$PWD/zig-out/bin/antfly" uv run --project e2e/antfly \
  pytest e2e/antfly/test_web_search.py -k tavily_live
```

The remaining tests in that file use local mock providers and do not need keys.

## Goals

- Give agents a configured, inspectable set of web-search providers.
- Keep provider-specific search details out of generic external IO.
- Remove legacy provider contracts that are deprecated, unavailable to new
  customers, or too weak for production agent search.
- Reuse the public `connections` inventory model for visibility, health, RBAC,
  and workflow authorization.
- Keep Google Cloud naming consistent with the existing `vertex` provider token.

## Relationship To Connections

`web_search` is a top-level connection kind alongside the existing physical
resource categories:

- `inference`: model providers and inference runtimes
- `web_search`: queryable external knowledge/search providers
- `external_io`: generic external bytes, objects, and content access
- `cdc`: external change streams and replication sources

Do not model web search as `external_io.protocol: http`. A web-search connection
may use HTTP under the hood, but the user-facing contract is search: queries,
result ranking, snippets, citations, freshness filters, content extraction, and
agent tool use.

## Provider Names

Supported production provider tokens:

- `exa`
- `tavily`
- `brave`
- `serper`
- `you`
- `linkup`
- `vertex`

Provider tokens should name the service account or platform Antfly talks to,
not the broad company when that would be ambiguous.

Use `vertex` for Google Cloud search services because Antfly already uses
`vertex` as the Google Cloud provider token for generators, embedders,
rerankers, readers, and related model-backed producers. Google Cloud
Agent Search, formerly Vertex AI Search, is configured as:

```yaml
provider: vertex
web_search:
  service: agent_search
```

Vertex-backed providers should share the credential vocabulary from
`specs/openapi/antfly/vertex.yaml`: `project_id`, `location`, and
`credentials_path`. Keep those fields flat on provider configs unless a future
provider has a strong reason to nest credentials.

Do not use `google` for the new provider. In older web-search config, `google`
meant Google Custom Search JSON API / CSE. That API is closed to new customers
and should not remain the public production contract.

Do not keep these as first-class production provider tokens:

- `google`: legacy Google CSE, ambiguous with Vertex/Gemini.
- `bing`: Bing Web Search API is no longer the right raw-SERP integration
  target. If Antfly later supports Microsoft's agent grounding/search product,
  model it as an Azure/Foundry provider with its own contract.
- `duckduckgo`: the public integration surface is too limited for reliable
  production agent search.

## Capabilities

Capabilities describe what Antfly is allowed to do with a connection. They are
also the future policy surface for RBAC and workflow authorization.

Common `web_search` capabilities:

- `web.search`: return ranked web results for a query.
- `web.semantic_search`: run semantic/neural web search when provider supports
  it.
- `web.news`: search news or freshness-sensitive results.
- `web.images`: search image results.
- `web.fetch`: fetch or extract page content through the provider.
- `web.answer`: return synthesized answers or answer-oriented snippets.
- `agents.use`: allow agents to use the connection as a tool.
- `indexing.use`: allow indexing/enrichment jobs to use the connection.

The same provider may expose only a subset of these capabilities.

## Configuration

Connections are configured under the public top-level `connections` map. The map
key is the stable connection ID used by agent configs, policy, dashboards, and
API responses.

```yaml
connections:
  agent-web:
    kind: web_search
    provider: tavily
    display_name: Tavily agent search
    capabilities:
      - web.search
      - web.fetch
      - web.news
      - agents.use
    web_search:
      max_results: 8
      timeout_ms: 10000
      safe_search: true
      include_content: true
      api_key: ${secret:tavily.api_key}

  semantic-web:
    kind: web_search
    provider: exa
    display_name: Exa semantic web
    capabilities:
      - web.search
      - web.semantic_search
      - web.fetch
      - agents.use
    web_search:
      max_results: 10
      include_highlights: true
      include_content: true
      api_key: ${secret:exa.api_key}

  google-doc-search:
    kind: web_search
    provider: vertex
    display_name: Google Agent Search docs
    capabilities:
      - web.search
      - web.answer
      - agents.use
      - indexing.use
    web_search:
      service: agent_search
      project_id: my-project
      location: global
      data_store: public-docs
      serving_config: default_config
      credentials_path: ${secret:vertex.service_account_path}
```

Provider-specific fields live under `web_search`. The top-level connection
fields remain stable across providers.

## Common Web Search Fields

Common fields:

- `service`: provider-specific service flavor when one provider exposes multiple
  search products. Example: `agent_search` for `provider: vertex`.
- `max_results`: maximum ranked results to return.
- `timeout_ms`: provider request timeout.
- `safe_search`: whether provider safety filtering should be requested.
- `language`: preferred result language, such as `en`.
- `region`: preferred result region, such as `us`.
- `include_content`: ask the provider to return extracted page content when
  supported.
- `include_highlights`: ask the provider to return highlighted passages when
  supported.
- `api_key`: direct secret reference or resolved API key value.
- `credentials_path`: shared Vertex service-account credential path for cloud
  providers that use ADC-style authentication.
- `project_id`: shared Vertex Google Cloud project for `provider: vertex`.
- `location`: shared Vertex cloud region/location for `provider: vertex`.

Provider implementations may accept additional fields, but unsupported fields
must not silently change behavior.

## API Shape

The public inventory response should mirror the connection model while hiding
secret values:

```json
{
  "id": "conn_agent_web",
  "name": "agent-web",
  "display_name": "Tavily agent search",
  "kind": "web_search",
  "provider": "tavily",
  "status": "connected",
  "capabilities": [
    "web.search",
    "web.fetch",
    "web.news",
    "agents.use"
  ],
  "web_search": {
    "max_results": 8,
    "safe_search": true,
    "include_content": true,
    "configured": true
  },
  "permissions": {
    "can_read": true,
    "can_use": true,
    "can_admin": false,
    "can_view_secret_refs": false
  }
}
```

Secret values are never returned. At most, the API may report that a required
credential is configured.

## Agent Use

Agents should reference a configured connection instead of embedding provider
secrets directly in request payloads.

```yaml
agents:
  support:
    tools:
      web_search:
        connection: agent-web
        max_results: 5
```

Request-level overrides may reduce scope, such as lowering `max_results` or
disabling content extraction, but should not expand capabilities beyond what the
connection and policy allow.

## Fetch

Agents read full pages with the `fetch` tool (`pkg/antfly/src/api/web_fetch.zig`).
Fetch is opt-in (`fetch` in `enabled_tools` or a `fetch_config`) and admits a
URL only when `web_search` returned it in the same run or its host is under
`fetch_config.allowed_hosts`:

```json
{
  "tools": {
    "enabled_tools": ["web_search", "fetch"],
    "web_search_connection": "agent-web",
    "fetch_config": {"max_content_length": 12000, "allowed_hosts": ["docs.example.com"]}
  }
}
```

A returned URL with a changed query string is a different URL and is rejected,
so text injected into a document cannot make the model send retrieved data to
an attacker-controlled endpoint. Downloads use the shared remote-content client
with private-address blocking always on and every redirect hop re-validated;
`block_private_ips: false` and `s3_credentials` are rejected. Size (20 MiB),
time (60 s), and extracted text (50,000 characters) have server ceilings that a
request can only lower. HTML is reduced to visible text; binary content is
refused. Fetched pages become `fetch:<url>` hits and count against the same
tool-result token budget as search results.

## RBAC And Policy

Long term, web-search authorization should be evaluated from:

- principal: user, service account, agent, or job
- connection: the configured `web_search` connection
- capability: for example `web.search`, `web.fetch`, or `agents.use`
- workflow: agent query, indexing job, enrichment, evaluation, or admin probe

Example policy intent:

```yaml
policies:
  - principal: group:support
    connection: agent-web
    allow:
      - web.search
      - agents.use

  - principal: service:indexer
    connection: google-doc-search
    allow:
      - web.search
      - indexing.use
```

This lets an operator expose a provider to agents without automatically allowing
indexing jobs, backups, or arbitrary content fetches to use it.

## Implementation Notes

The current web-search OpenAPI should be replaced before release rather than
kept as a legacy compatibility layer:

- remove `google`, `bing`, and `duckduckgo` from the production provider enum;
- add `exa`, `you`, `linkup`, and `vertex`;
- keep `tavily`, `brave`, and `serper`;
- route agent web-search tooling through named `connections`;
- expose configured web-search connections in `/connections`;
- keep provider secrets in config/secrets, not in dashboard or inventory
  responses.

Provider adapters can still share an internal interface:

```text
Search(ctx, query, options) -> ranked results with snippets/citations
Fetch(ctx, url, options) -> extracted content when provider supports it
```

The public API should stay connection-oriented even if internal adapters are
provider-oriented.
