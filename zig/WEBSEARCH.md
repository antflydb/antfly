# Web Search

This document defines the long-run web-search provider model for Antfly agents
and the public connection configuration shape.

Web search is a first-class external capability. It should not be modeled as
plain HTTP access because it has query semantics, ranking, freshness, snippets,
citations, provider display rules, and agent-tool behavior.

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
