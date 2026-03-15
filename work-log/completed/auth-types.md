# API Key and Bearer Token Authentication

## Context

Antfly currently supports HTTP Basic Auth only (username/password via `Authorization: Basic` header). For deployment behind a proxy or as a cloud offering, users need API key and bearer token authentication. API keys are long-lived, scoped, revocable credentials for programmatic access. Bearer tokens enable proxy-issued auth and standard HTTP token patterns. Auth lives in Antfly core so it works standalone, while proxies can layer on top or pass through.

## Design

Modeled after Elasticsearch's API key system, adapted for Antfly's architecture.

### API Key Format (Elasticsearch-style)

- **Key ID**: 20 alphanumeric characters (~119 bits), used for O(1) database lookup
- **Key Secret**: 22 Base64url characters (128 bits / 16 random bytes), used for hash verification
- **Wire format**: `Authorization: ApiKey base64(id:secret)`
- **Create response** includes a pre-encoded `encoded` field ready for the header (like ES)
- Generated using `crypto/rand` (CSPRNG)

### Hashing: Salted SHA-256 (not bcrypt, not unsalted)

Following Elasticsearch's approach: API key secrets are high-entropy machine-generated values (128 bits), so bcrypt's slow hash is unnecessary and would kill per-request performance (~250ms/hash at cost 13). Instead:
- Generate 16-byte random salt per key
- Store `salt` and `SHA256(salt + secret)` separately
- Verify with `crypto/subtle.ConstantTimeCompare` (timing-attack resistant)
- Always verify hash **before** checking expiration (prevents timing-based enumeration of valid key IDs, per ES PR #35318)

### Bearer Token Design

`Authorization: Bearer base64(id:secret)` -- same encoding as `ApiKey`, just different scheme name. The server parses both identically. This avoids the O(n) lookup problem that occurs when using a raw secret as a bearer token (where you'd need to hash against every stored salt). SDKs present the same credential either way.

For proxy-issued opaque tokens (JWT, OAuth2, etc.): the proxy validates the token itself and sets `X-Authenticated-User` header, which Antfly already trusts. No Antfly changes needed for this case.

### Permission Model (Elasticsearch-style intersection)

API keys support optional permission scoping at creation time:
- `CreateApiKeyRequest` includes an optional `permissions []Permission` field (reusing the existing `Permission` type: resource, resource_type, type)
- If permissions are specified: effective permissions = intersection of key's explicit permissions AND owner's current Casbin permissions. Both must allow the action.
- If no permissions specified: key inherits the owner's full Casbin permissions (no restriction)
- At creation time: validate that each requested permission is a subset of the creator's current permissions (prevent privilege escalation)
- At request time: `authnMiddleware` stores the key's permissions in request context. `ensureAuth()` checks both: (1) the owner has the permission via Casbin, AND (2) if the key has explicit permissions, the action matches at least one of them (using the same wildcard matching as the Casbin matchers)

### Invalidation vs Deletion

Initial implementation uses hard deletion. Soft invalidation with audit trail can be added later.

## Files to Modify

### Phase 1: Server -- API Key Storage & Validation

**`src/usermgr/user.go`** -- Add to `UserManager`:
- New prefix constant: `apiKeyPrefix = "apikey:"`
- New type:
  ```go
  type ApiKeyRecord struct {
      KeyID       string       `json:"key_id"`
      SecretHash  []byte       `json:"secret_hash"`  // SHA-256(salt + secret)
      SecretSalt  []byte       `json:"secret_salt"`  // 16-byte random salt
      Username    string       `json:"username"`      // owner
      Name        string       `json:"name"`
      Permissions []Permission `json:"permissions,omitempty"` // optional scoping
      CreatedAt   time.Time    `json:"created_at"`
      ExpiresAt   time.Time    `json:"expires_at,omitempty"` // zero = never
  }
  ```
- New in-memory map on `UserManager`: `apiKeys map[string]*ApiKeyRecord` (key_id -> record)
- `loadApiKeys()` -- called from `NewUserManager`, scans Pebble prefix `apikey:*`
- `CreateApiKey(username, name string, permissions []Permission, expiresAt time.Time) (keyID, keySecret string, err error)`:
  1. Verify user exists in `passwordHashes`
  2. If permissions specified, validate each is a subset of the creator's current Casbin permissions (prevent escalation)
  3. Generate key_id (20 alphanumeric chars via `crypto/rand`)
  4. Generate key_secret (16 random bytes, base64url-encoded to 22 chars)
  5. Generate 16-byte random salt
  6. Compute `SHA256(salt + secret_raw_bytes)`, store salt and hash
  7. Persist `ApiKeyRecord` (including permissions) to Pebble at `apikey:<key_id>`
  8. Return cleartext key_id and key_secret (shown once, never stored)
- `ValidateApiKey(keyID, keySecret string) (username string, permissions []Permission, err error)`:
  1. Lookup `ApiKeyRecord` by key_id in memory map
  2. Compute `SHA256(record.SecretSalt + secret_raw_bytes)`
  3. `subtle.ConstantTimeCompare` against `record.SecretHash`
  4. **Then** check expiration (timing-attack mitigation)
  5. Return `record.Username` and `record.Permissions`
- `ListApiKeys(username string) ([]*ApiKeyRecord, error)` -- filter by username, zero out hash/salt fields
- `DeleteApiKey(keyID string) error` -- remove from Pebble and in-memory map

**`src/metadata/auth.go`** -- Extend `authnMiddleware` (currently line 85-86 checks `authParts[0] != "Basic"`):
```go
switch authParts[0] {
case "Basic":
    // existing basic auth logic (lines 90-104)
case "ApiKey", "Bearer":
    // decode base64(id:secret), split on ":"
    // call um.ValidateApiKey(id, secret) -> (username, permissions)
    // set X-Authenticated-User to the key's owner
    // if permissions non-empty, store in request context via context.WithValue
default:
    // 401 Unauthorized
}
```
- Update `WWW-Authenticate` header (line 81) to `Basic, ApiKey, Bearer`
- Define context key type: `type apiKeyPermissionsKey struct{}`
- Helper: `apiKeyPermissionsFromContext(r) []Permission`

**`src/metadata/auth.go`** -- Extend `ensureAuth()` (line 12-43):
- After the existing Casbin `Enforce` check passes, also check API key permissions from context
- If API key permissions exist in context, verify the requested (resourceType, resource, permissionType) matches at least one of them (same wildcard logic as Casbin matchers: `"*"` matches any value)
- If neither check passes, return 403

### Phase 2: Server -- API Key CRUD Endpoints

**`src/usermgr/api.yaml`** -- Add:

Security schemes (alongside existing `BasicAuth`):
```yaml
ApiKeyAuth:
  type: apiKey
  in: header
  name: Authorization
BearerAuth:
  type: http
  scheme: bearer
```

Global security (any one is sufficient):
```yaml
security:
  - BasicAuth: []
  - ApiKeyAuth: []
  - BearerAuth: []
```

New schemas: `ApiKey` (public metadata including permissions), `ApiKeyWithSecret` (creation response with `key_id`, `key_secret`, `encoded`, permissions), `CreateApiKeyRequest` (name, optional expires_in, optional permissions array reusing existing `Permission` schema)

New paths:
- `GET /users/{userName}/api-keys` -- list keys (no secrets)
- `POST /users/{userName}/api-keys` -- create key, returns `ApiKeyWithSecret` (201)
- `DELETE /users/{userName}/api-keys/{keyId}` -- delete key (204)

**`src/metadata/api.yaml`** -- Add same security schemes (~line 1359) and update global `security` to accept any of the three

**`src/usermgr/api.go`** -- Implement `ListApiKeys`, `CreateApiKey`, `DeleteApiKey` handlers following existing handler patterns (JSON encode/decode, `httpError`/`jsonResponse`)

**`src/metadata/notfound.go`** -- Add new endpoint paths to `validEndpoints`

### Phase 3: Code Generation

Run `make generate` to regenerate:
- `src/usermgr/api.gen.go` -- new types + server interface methods
- `src/metadata/api.gen.go` -- updated security scheme constants
- `antfly-go/antfly/oapi/client.gen.go` -- Go SDK client with new endpoints
- `ts/packages/sdk/src/antfly-api.d.ts` -- TypeScript types
- `py/src/antfly/client_generated/` -- Python generated client
- `openapi.yaml` -- bundled spec

### Phase 4: Go SDK

**`antfly-go/antfly/client.go`** -- Change `NewAntflyClient` to accept variadic `oapi.ClientOption` instead of `*http.Client`. Add auth helper functions:
```go
func WithBasicAuth(username, password string) oapi.RequestEditorFn
func WithApiKey(keyID, keySecret string) oapi.RequestEditorFn   // Authorization: ApiKey base64(id:secret)
func WithBearerToken(token string) oapi.RequestEditorFn          // Authorization: Bearer <token>
```

Usage:
```go
client, _ := antfly.NewAntflyClient(url,
    oapi.WithRequestEditorFn(antfly.WithApiKey(id, secret)),
)
```

### Phase 5: TypeScript SDK

**`ts/packages/sdk/src/types.ts`** -- Extend `AntflyConfig.auth` to discriminated union:
```typescript
auth?:
  | { type: 'basic'; username: string; password: string }
  | { type: 'apiKey'; keyId: string; keySecret: string }
  | { type: 'bearer'; token: string }
  | { username: string; password: string }  // backwards compat (no 'type' field)
```

**`ts/packages/sdk/src/client.ts`**:
- Add private `getAuthHeader()` method that handles all auth types via the discriminated union
- Replace the 4 duplicated auth header construction sites (constructor line 43-46, `setAuth` line 65-84, `performRetrievalAgent` ~line 180, `scan` ~line 503) with calls to `getAuthHeader()`
- Update `setAuth()` to accept the full union type

### Phase 6: Python SDK

**`py/src/antfly/client.py`** -- Add `api_key` and `bearer_token` constructor params:
```python
def __init__(
    self,
    base_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[tuple[str, str]] = None,   # (key_id, key_secret)
    bearer_token: Optional[str] = None,
    timeout: float = 30.0,
):
```
- `api_key`: use `AuthenticatedClient(prefix="ApiKey", token=base64(id:secret))`
- `bearer_token`: use `AuthenticatedClient(prefix="Bearer", token=token)`
- `username`/`password`: unchanged (httpx `auth` tuple on `Client`)

The generated `AuthenticatedClient` (line 217) already sets `Authorization: {prefix} {token}`, so this maps directly.

### Phase 7: Tests

**`src/usermgr/api_key_test.go`** -- Unit tests:
- Create key, validate with correct secret
- Reject invalid secret (wrong bytes)
- Reject expired key (verify hash checked before expiration for timing safety)
- List keys omits sensitive fields (hash/salt)
- Delete key, subsequent validation fails
- Key creation fails for nonexistent user
- Create key with permissions, verify returned in validation
- Creation rejects permissions the creator doesn't have (privilege escalation prevention)

**`e2e/api_keys_test.go`** -- E2E test (following `e2e/secrets_test.go` pattern):
- Create user with basic auth, grant read+write on table "orders"
- Create API key with no permission scoping, verify full access
- Create API key scoped to read-only on "orders", verify write is rejected
- Attempt to create key with admin permission the user doesn't have, verify 403
- Use `Authorization: ApiKey <encoded>` to make authenticated requests
- Use `Authorization: Bearer <encoded>` with same credential
- Delete key, verify subsequent requests rejected

## Security Checklist

- [x] Salted SHA-256 (SSHA256) -- per-key 16-byte random salt, not unsalted
- [x] High-entropy secrets -- 128 bits from `crypto/rand`
- [x] Constant-time comparison -- `crypto/subtle.ConstantTimeCompare`
- [x] Hash-before-expiry check -- timing attack mitigation (ES PR #35318)
- [x] O(1) lookup -- key ID used for direct map lookup, no full-table scan
- [x] Secret shown once -- only returned in create response, never stored cleartext
- [x] No raw secret as bearer -- both ApiKey and Bearer use `base64(id:secret)` format
- [x] CSPRNG -- `crypto/rand` for all key material generation

## Verification

1. `GOEXPERIMENT=simd go build ./...` -- main module compiles
2. `GOEXPERIMENT=simd go test ./src/usermgr/...` -- unit tests pass
3. `cd antfly-go/antfly && go build ./...` -- Go SDK compiles
4. `cd ts && pnpm build` -- TypeScript SDK compiles
5. `cd py && uv run python -c "from antfly import AntflyClient"` -- Python SDK imports
6. `make e2e E2E_TEST=TestApiKey` -- E2E test passes
