# Row-Level Auth for Antfly Documents

## Context

Antfly currently has table-level auth (Casbin RBAC + API key scoping) but no document-level filtering. Enterprise customers need row-level security so API keys, user roles, and proxy bearer tokens can scope visibility to a subset of documents within a table. The mechanism is **bleve query-based filter injection** — a security filter query is ANDed with every query automatically via the existing `FilterQuery` pipeline, which already flows through full-text search, vector search (FilterIDs), foreign tables (SQL WHERE), and graph queries.

## Step 1: Add `RowFilter` to API Keys

Add a `row_filter` field to API keys so tokens can be scoped to specific document subsets per table.

### Files to modify

**`src/usermgr/api.yaml`** — OpenAPI spec changes:
- Add `row_filter` to `ApiKey` schema (after `permissions` field ~line 153):
  ```yaml
  row_filter:
    type: object
    additionalProperties: {}
    nullable: true
    description: >
      Per-table row filter. Keys are table names (or '*' for all tables).
      Values are bleve query JSON objects. Documents must match this query
      to be visible through this API key.
    example:
      "orders": {"term": {"department": "engineering"}}
  ```
- Add `row_filter` to `CreateApiKeyRequest` schema (after `permissions` ~line 204), same type
- `ApiKeyWithSecret` uses `allOf` referencing `ApiKey`, so it inherits automatically

**`src/usermgr/user.go`** — Data model + logic:
- Add field to `ApiKeyRecord` (after line 57, the `Permissions` field):
  ```go
  RowFilter map[string]json.RawMessage `json:"row_filter,omitempty"`
  ```
- Change `ValidateApiKey` signature to return row filter:
  ```go
  func (um *UserManager) ValidateApiKey(keyID, keySecret string) (string, []Permission, map[string]json.RawMessage, error)
  ```
  Return `record.RowFilter` as third value
- In `CreateApiKey`: accept `rowFilter` param, store it on the record, add privilege escalation check — if the creator was authenticated via an API key that has a row filter for table T, the new key must also have a filter for table T (cannot widen access)
- Add validation: parse each filter value with `query.ParseQuery()` at creation time, return 400 if invalid bleve query

**`src/usermgr/api.go`** — Handler updates:
- Update `CreateApiKey` handler to extract `RowFilter` from generated request type and pass to `um.CreateApiKey()`
- Update response builders to include `RowFilter` in `ApiKey`/`ApiKeyWithSecret` responses

**Run `make generate`** to regenerate `src/usermgr/api.gen.go`

### All callers of `ValidateApiKey` must be updated for the new signature:
- `src/metadata/auth.go:204` — primary call site (updated in Step 2)

## Step 2: Inject Row Filter in Query Path

Automatically AND the security filter with every query before it reaches bleve/vector indexes.

### Files to modify

**`src/metadata/auth.go`** — Store row filter resolver in request context:
- Add a unified resolver type and context key (near line 27, alongside existing `apiKeyPermissionsKey`):
  ```go
  type rowFilterResolverKey struct{}

  // RowFilterResolver returns the security filter JSON for a given table name.
  // Returns nil if no filter applies to that table.
  type RowFilterResolver func(table string) json.RawMessage

  func rowFilterResolverFromContext(r *http.Request) RowFilterResolver {
      fn, _ := r.Context().Value(rowFilterResolverKey{}).(RowFilterResolver)
      return fn
  }
  ```
- Add helper to build a resolver from a static map (used by API key auth and later by proxy):
  ```go
  func mapRowFilterResolver(filters map[string]json.RawMessage) RowFilterResolver {
      return func(table string) json.RawMessage {
          if f, ok := filters[table]; ok {
              return f
          }
          if f, ok := filters["*"]; ok {
              return f
          }
          return nil
      }
  }
  ```
- In `authnMiddleware` ApiKey/Bearer case (line 204): update call to `ValidateApiKey` for new 3-return signature, store resolver in context:
  ```go
  username, permissions, rowFilter, err := ms.um.ValidateApiKey(keyID, keySecret)
  // ... existing permissions context storage (line 210-213) ...
  if len(rowFilter) > 0 {
      ctx = context.WithValue(r.Context(), rowFilterResolverKey{}, mapRowFilterResolver(rowFilter))
      r = r.WithContext(ctx)
  }
  ```
  Note: reuse `ctx` from existing permissions block if it was set, otherwise use `r.Context()`.

**`src/metadata/api_query.go`** — Inject filter at `runQuery` entry point:
- Add helper function:
  ```go
  func injectRowFilter(qr *QueryRequest, resolve RowFilterResolver) {
      if resolve == nil {
          return
      }
      secFilter := resolve(qr.Table)
      if len(secFilter) == 0 || bytes.Equal(secFilter, []byte("null")) {
          return
      }
      if len(qr.FilterQuery) == 0 || bytes.Equal(qr.FilterQuery, []byte("null")) {
          qr.FilterQuery = secFilter
      } else {
          conjunction, _ := json.Marshal(map[string]interface{}{
              "conjuncts": []json.RawMessage{qr.FilterQuery, secFilter},
          })
          qr.FilterQuery = conjunction
      }
  }
  ```
- Inject at top of `runQuery` (line 404), BEFORE `queryReq.Validate()`:
  ```go
  if resolve, ok := ctx.Value(rowFilterResolverKey{}).(RowFilterResolver); ok {
      injectRowFilter(queryReq, resolve)
  }
  ```
  This covers ALL callers: `handleQuery`, `retrieval_agent.go`, `api_join.go`, `api_ai.go`, `mcp_adapter.go` — they all pass `r.Context()` through to `runQuery`.

### Why this works end-to-end
- `FilterQuery` JSON is parsed in `ToRemoteIndexQuery` (line 157-163) → bleve `query.Query`
- ANDed with `FullTextSearch` at line 540-541 via `query.NewConjunctionQuery`
- Converted to `FilterIDs` for vector search at `src/store/db/db.go:3923-3966`
- Translated to SQL WHERE for foreign tables in `foreign/sql_datasource.go`
- No index layer changes needed

### Batch read post-fetch filter
Documents fetched by key bypass the query pipeline and need explicit filtering:

**`src/metadata/api.go`** — `LookupKey` handler (line 706):
- After fetching document (line 726), before response: get resolver from context, evaluate the document against the filter
- If document doesn't match, return 404

**`src/metadata/retrieval_agent.go`** — `batchLookupDocuments` (line 1834):
- After collecting results (line 1870), filter out non-matching documents before returning

**Post-fetch evaluation approach**: Query the table's existing bleve index with `conjunction(term(_id, docKey), rowFilter)`. If the query returns 0 hits, the document is filtered out. This reuses the existing bleve index infrastructure rather than spinning up a `bleve.NewMemOnly()` per request, which would be expensive.
- Add helper: `func (t *TableApi) docMatchesRowFilter(ctx context.Context, tableName, key string, resolve RowFilterResolver) (bool, error)` — constructs a bleve conjunction query and runs it through `runQuery` with limit=0 + the key as an additional filter term
- For batch lookups: construct a single query with `disjuncts` of all keys ANDed with the row filter, then intersect returned keys with the fetched set

## Step 3: Casbin Filter Ptype for Role-Based Row Filters

Allow user roles to carry row filters, so Basic auth (user/password) users also get row-level filtering.

### Files to modify

**`src/usermgr/user.go`** — Casbin model and methods:
- Extend the Casbin model conf (line 62) to add a second policy definition for filter ptype. Use the Casbin **named policy** feature:
  ```
  [policy_definition]
  p = sub, typ, obj, act
  p2 = sub, obj, filter
  ```
  The existing matcher only references `p`, so `p2` is data-only (no enforcement evaluation).
- Add methods:
  ```go
  func (um *UserManager) SetRowFilter(role, table string, filterJSON json.RawMessage) error
  func (um *UserManager) RemoveRowFilter(role, table string) error
  func (um *UserManager) GetRowFilters(username string) (map[string]json.RawMessage, error)
  ```
  - `SetRowFilter`: validate with `query.ParseQuery()`, then `enforcer.AddNamedPolicy("p2", role, table, string(filterJSON))`
  - `GetRowFilters`: get user's roles via `enforcer.GetRolesForUser(username)`, collect all `p2` policies for each role + direct user policies, build `map[table]filter`. When multiple filters apply to the same table (from different roles), conjunct them.
  - `RemoveRowFilter`: `enforcer.RemoveNamedFilteredPolicy("p2", 0, role, table)`

**`src/metadata/kv/casbin-adapter/adapter.go`** — Adapter support for p2:
- Already ptype-agnostic: `CasbinRule.PType` is a string field, `convertRule()` (line 299-312) accepts any ptype, `loadPolicy()` (line 289-297) calls `persist.LoadPolicyLine` which handles arbitrary ptypes. **No changes needed** — just verify with a test.
- Key format follows existing pattern: `prefix + "p2::" + role + "::" + table + "::" + filterJSON`

**`src/usermgr/api.yaml`** — New endpoints (on roles, not users, since filters are set per role):
```yaml
/roles/{roleName}/row-filters:
  get:
    operationId: listRoleRowFilters
    summary: List row filters for a role
/roles/{roleName}/row-filters/{table}:
  put:
    operationId: setRoleRowFilter
    summary: Set row filter for a role on a table
  get:
    operationId: getRoleRowFilter
    summary: Get row filter for a role on a table
  delete:
    operationId: removeRoleRowFilter
    summary: Remove row filter for a role on a table
/users/{userName}/row-filters:
  get:
    operationId: getEffectiveRowFilters
    summary: Get effective (resolved) row filters for a user across all roles
```

**`src/usermgr/api.go`** — Implement new handler functions for the endpoints above.

**`src/metadata/auth.go`** — Basic auth row filter resolution:
- In `authnMiddleware` Basic case (line 173-189), after successful authentication (line 189):
  ```go
  // Store a lazy resolver that calls um.GetRowFilters on first use and caches
  resolver := func() RowFilterResolver {
      var once sync.Once
      var filters map[string]json.RawMessage
      return func(table string) json.RawMessage {
          once.Do(func() {
              filters, _ = ms.um.GetRowFilters(username)
          })
          if f, ok := filters[table]; ok { return f }
          if f, ok := filters["*"]; ok { return f }
          return nil
      }
  }()
  ctx := context.WithValue(r.Context(), rowFilterResolverKey{}, resolver)
  r = r.WithContext(ctx)
  ```
  This uses the same `rowFilterResolverKey` and `RowFilterResolver` type from Step 2 — no new context keys needed. The `sync.Once` ensures `GetRowFilters` is called at most once per request, only if a query is actually made.

**Run `make generate`** after OpenAPI changes

## Step 4: Proxy-Level Filter Injection

Allow Colony's SaaS proxy to inject row filters per bearer token for tenant-scoped document filtering.

### Files to modify

**`pkg/antfly-proxy/auth.go`** — Add field to `Principal` (line 9-16):
```go
type Principal struct {
    Subject           string
    Tenant            string
    Admin             bool
    AllowedTables     []string
    AllowedNamespaces []string
    AllowedOperations []OperationKind
    RowFilter         map[string]json.RawMessage  // NEW
}
```

**`pkg/antfly-proxy/env.go`** — Add to `bearerTokenPrincipal` (line 104-111):
```go
type bearerTokenPrincipal struct {
    Subject    string   `json:"subject"`
    Tenant     string   `json:"tenant"`
    Admin      bool     `json:"admin"`
    Tables     []string `json:"tables"`
    Namespaces []string `json:"namespaces"`
    Operations []string `json:"operations"`
    RowFilter  map[string]json.RawMessage `json:"row_filter,omitempty"`  // NEW
}
```
- In `parseBearerTokensJSON` (line 121-133): propagate `RowFilter` to `Principal`:
  ```go
  tokens[token] = Principal{
      // ... existing fields ...
      RowFilter:         principal.RowFilter,  // NEW
  }
  ```

**`pkg/antfly-proxy/gateway.go`** — Body injection for query requests:
- Add helpers in a new file `pkg/antfly-proxy/row_filter.go`:
  ```go
  func resolveRowFilter(filters map[string]json.RawMessage, table string) json.RawMessage
  func injectFilterIntoBody(body []byte, secFilter json.RawMessage) ([]byte, error)
  ```
  `injectFilterIntoBody`: decode NDJSON body (one or more JSON objects), for each object merge `filter_query` field (AND with existing if present), re-encode.
- In `handleProxy` (after authorization at line 114-117, before `outReq` creation at line 125):
  ```go
  if req.Operation == OperationRead && len(principal.RowFilter) > 0 {
      table := firstNonEmpty(req.Table, route.TableName())
      if secFilter := resolveRowFilter(principal.RowFilter, table); secFilter != nil {
          body, _ := io.ReadAll(r.Body)
          modified, err := injectFilterIntoBody(body, secFilter)
          if err != nil {
              http.Error(w, "failed to inject row filter", http.StatusInternalServerError)
              return
          }
          r.Body = io.NopCloser(bytes.NewReader(modified))
          r.ContentLength = int64(len(modified))
      }
  }
  ```
- Handle retrieval agent bodies too: if `req.BackendPath` starts with `/agents/`, the body has `queries[].filter_query` nested structure — inject into each query object in the array. Add `injectFilterIntoAgentBody` helper.

**Bearer token config example** (for `ANTFLY_PROXY_BEARER_TOKENS_JSON`):
```json
{
  "tok_acme": {
    "subject": "acme-api",
    "tenant": "acme",
    "tables": ["docs"],
    "row_filter": {
      "docs": {"term": {"tenant_id": "acme"}}
    }
  }
}
```

## Implementation Order

1. **Step 1** — Additive schema/data changes, no runtime behavior change
2. **Step 2** — Activates filtering for API key auth (the core mechanism)
3. **Step 3** — Adds role-based filters for Basic auth users (builds on Step 2 injection)
4. **Step 4** — Independent of Step 3, can be done in parallel; only touches `pkg/antfly-proxy/`

## Verification

### Unit tests
- `src/usermgr/user_test.go`: create API key with row_filter, validate round-trip, privilege escalation prevention (child key cannot widen parent's filter), invalid bleve query rejection at creation time
- `src/metadata/api_query_test.go`: `injectRowFilter` with no existing filter, with existing filter (conjunction), with nil resolver, with wildcard `"*"` table key, with table-specific key overriding wildcard
- `src/metadata/auth_test.go`: `mapRowFilterResolver` returns correct filter for table, wildcard, and missing table
- `pkg/antfly-proxy/row_filter_test.go`: `injectFilterIntoBody` — single query, multi-query (NDJSON), empty body, existing filter_query (conjunction), retrieval agent body format (`injectFilterIntoAgentBody`)
- `pkg/antfly-proxy/env_test.go`: `parseBearerTokensJSON` with `row_filter` field round-trips correctly

### Integration / E2E tests
Add `e2e/row_filter_test.go`:
- Create table, insert documents with varying attributes (e.g., `department: "eng"` vs `"hr"`)
- Create API key with `row_filter: {"test_table": {"term": {"department": "eng"}}}`
- Query via that API key — verify only `department=eng` docs returned
- Query with user-supplied `filter_query` — verify conjunction (both filters apply)
- Query via Basic auth without role filter — verify all docs returned
- Set role row filter via usermgr API, query via Basic auth — verify filtering
- `LookupKey` with row filter — verify non-matching doc returns 404
- `batchLookupDocuments` with row filter — verify non-matching docs omitted
- Proxy: configure bearer token with row_filter, query through proxy, verify filtering

### Build verification
```bash
make generate                                    # After OpenAPI changes
make build                                       # Verify compilation
GOEXPERIMENT=simd go test ./src/usermgr/...
GOEXPERIMENT=simd go test ./src/metadata/...
cd pkg/antfly-proxy && go test ./...
make e2e E2E_TEST=TestRowFilter > /tmp/test.log 2>&1
```
