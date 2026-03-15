// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Auth verifies that authentication and authorization work correctly
// when EnableAuth is true. A default admin:admin user with full permissions
// is auto-created at startup.
func TestE2E_Auth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start swarm with auth enabled.
	t.Log("Starting Antfly swarm with auth enabled...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{
		DisableTermite: true,
		EnableAuth:     true,
	})
	defer swarm.Cleanup()

	baseURL := swarm.MetadataAPIURL + "/api/v1"

	// ---- 1. Unauthenticated request → 401 ----
	t.Log("Step 1: Unauthenticated GET /secrets → 401")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/secrets", "", nil, http.StatusUnauthorized)

	// ---- 2. Bad credentials → 401 ----
	t.Log("Step 2: Bad credentials GET /secrets → 401")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/secrets", "", basicAuth("wrong", "creds"), http.StatusUnauthorized)

	// ---- 3. Valid admin credentials → 200 ----
	t.Log("Step 3: Admin GET /secrets → 200")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/secrets", "", basicAuth("admin", "admin"), http.StatusOK)

	// ---- 4. Unauthenticated PUT → 401 ----
	t.Log("Step 4: Unauthenticated PUT /secrets/test.key → 401")
	doRequestExpectStatus(t, ctx, "PUT", baseURL+"/secrets/test.key",
		`{"value":"secret-value"}`, nil, http.StatusUnauthorized)

	// ---- 5. Admin PUT → 200 ----
	t.Log("Step 5: Admin PUT /secrets/test.key → 200")
	doRequestExpectStatus(t, ctx, "PUT", baseURL+"/secrets/test.key",
		`{"value":"secret-value"}`, basicAuth("admin", "admin"), http.StatusOK)

	// ---- 6. Unauthenticated DELETE → 401 ----
	t.Log("Step 6: Unauthenticated DELETE /secrets/test.key → 401")
	doRequestExpectStatus(t, ctx, "DELETE", baseURL+"/secrets/test.key", "", nil, http.StatusUnauthorized)

	// ---- 7. Admin DELETE → 204 ----
	t.Log("Step 7: Admin DELETE /secrets/test.key → 204")
	doRequestExpectStatus(t, ctx, "DELETE", baseURL+"/secrets/test.key",
		"", basicAuth("admin", "admin"), http.StatusNoContent)

	// ---- 8. Table endpoints also require auth ----
	t.Log("Step 8: Unauthenticated GET /tables → 401")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/tables", "", nil, http.StatusUnauthorized)

	t.Log("Step 9: Admin GET /tables → 200")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/tables", "", basicAuth("admin", "admin"), http.StatusOK)

	// ---- 10. Status endpoint also requires auth ----
	t.Log("Step 10: Unauthenticated GET /status → 401")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "", nil, http.StatusUnauthorized)

	t.Log("Step 11: Admin GET /status → 200")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "", basicAuth("admin", "admin"), http.StatusOK)

	t.Log("Auth e2e test passed")
}

// ---------- helpers ----------

// basicAuth returns an Authorization header value for Basic auth.
func basicAuth(username, password string) http.Header {
	creds := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return http.Header{"Authorization": {fmt.Sprintf("Basic %s", creds)}}
}

// doRequestExpectStatus sends an HTTP request and asserts the response status code.
func doRequestExpectStatus(t *testing.T, ctx context.Context, method, url, body string, headers http.Header, wantStatus int) {
	t.Helper()

	var bodyReader *strings.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	var req *http.Request
	var err error
	if bodyReader != nil {
		req, err = http.NewRequestWithContext(ctx, method, url, bodyReader)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	}
	require.NoError(t, err)

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Set(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, wantStatus, resp.StatusCode,
		"%s %s expected status %d, got %d", method, url, wantStatus, resp.StatusCode)

	// Drain body to allow connection reuse.
	_, _ = io.ReadAll(resp.Body)
}

// ---------- API Key types ----------

// apiKeyResponse mirrors ApiKeyWithSecret from the API.
type apiKeyResponse struct {
	KeyID       string `json:"key_id"`
	KeySecret   string `json:"key_secret"`
	Encoded     string `json:"encoded"`
	Name        string `json:"name"`
	Username    string `json:"username"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	Permissions []struct {
		Resource     string `json:"resource"`
		ResourceType string `json:"resource_type"`
		Type         string `json:"type"`
	} `json:"permissions,omitempty"`
}

// apiKeyListEntry mirrors ApiKey (no secret) from the API.
type apiKeyListEntry struct {
	KeyID       string `json:"key_id"`
	Name        string `json:"name"`
	Username    string `json:"username"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	Permissions []struct {
		Resource     string `json:"resource"`
		ResourceType string `json:"resource_type"`
		Type         string `json:"type"`
	} `json:"permissions,omitempty"`
}

// apiKeyAuth returns an Authorization header for ApiKey auth.
func apiKeyAuth(encoded string) http.Header {
	return http.Header{"Authorization": {fmt.Sprintf("ApiKey %s", encoded)}}
}

// bearerAuth returns an Authorization header for Bearer auth.
func bearerAuth(encoded string) http.Header {
	return http.Header{"Authorization": {fmt.Sprintf("Bearer %s", encoded)}}
}

// TestE2E_ApiKeys tests the full API key lifecycle: create, authenticate,
// permission scoping, privilege escalation prevention, and deletion.
func TestE2E_ApiKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start swarm with auth enabled.
	t.Log("Starting Antfly swarm with auth enabled...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{
		DisableTermite: true,
		EnableAuth:     true,
	})
	defer swarm.Cleanup()

	baseURL := swarm.MetadataAPIURL + "/api/v1"
	adminAuth := basicAuth("admin", "admin")

	// ---- 1. Create a test user with permissions ----
	t.Log("Step 1: Create test user 'alice' with table permissions")
	createUser(t, ctx, baseURL, "alice", "password123", adminAuth)
	// Grant read on all tables (needed for ListTables which checks *, table, read)
	grantPermission(t, ctx, baseURL, "alice", "*", "table", "read", adminAuth)
	grantPermission(t, ctx, baseURL, "alice", "orders", "table", "write", adminAuth)

	// ---- 2. Create API key with no permission scoping (full access) ----
	t.Log("Step 2: Create API key for alice (no scoping)")
	key1 := createApiKey(t, ctx, baseURL, "alice", "full-access key", nil, adminAuth)
	require.NotEmpty(t, key1.KeyID, "key_id should not be empty")
	require.NotEmpty(t, key1.KeySecret, "key_secret should not be empty")
	require.NotEmpty(t, key1.Encoded, "encoded should not be empty")
	assert.Equal(t, "alice", key1.Username)

	// Verify the encoded field is base64(id:secret)
	expectedEncoded := base64.StdEncoding.EncodeToString(
		[]byte(key1.KeyID + ":" + key1.KeySecret))
	assert.Equal(t, expectedEncoded, key1.Encoded, "encoded should be base64(id:secret)")

	// ---- 3. Authenticate using ApiKey scheme ----
	t.Log("Step 3: Authenticate with ApiKey scheme")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		apiKeyAuth(key1.Encoded), http.StatusOK)

	// ---- 4. Authenticate using Bearer scheme with same credential ----
	t.Log("Step 4: Authenticate with Bearer scheme (same credential)")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		bearerAuth(key1.Encoded), http.StatusOK)

	// ---- 5. List API keys ----
	t.Log("Step 5: List API keys for alice")
	keys := listApiKeys(t, ctx, baseURL, "alice", adminAuth)
	assert.Len(t, keys, 1, "expected 1 key")
	assert.Equal(t, key1.KeyID, keys[0].KeyID)
	assert.Equal(t, "full-access key", keys[0].Name)

	// ---- 6. Unscoped key can list tables (alice has *, table, read) ----
	t.Log("Step 6: Unscoped key can list tables")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/tables", "",
		apiKeyAuth(key1.Encoded), http.StatusOK)

	// ---- 7. Create API key scoped to read-only on "orders" ----
	t.Log("Step 7: Create read-only scoped API key")
	readOnlyPerms := []map[string]string{
		{"resource": "orders", "resource_type": "table", "type": "read"},
	}
	key2 := createApiKey(t, ctx, baseURL, "alice", "read-only key", readOnlyPerms, adminAuth)
	require.NotEmpty(t, key2.KeyID)
	assert.Len(t, key2.Permissions, 1, "key should have 1 permission")

	// ---- 8. Scoped key can still access authn-only endpoints ----
	t.Log("Step 8: Scoped key can access status endpoint")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		apiKeyAuth(key2.Encoded), http.StatusOK)

	// ---- 9. Attempt privilege escalation: create key with admin perms alice doesn't have ----
	t.Log("Step 9: Privilege escalation prevented")
	escalatedPerms := []map[string]string{
		{"resource": "*", "resource_type": "*", "type": "admin"},
	}
	createApiKeyExpectError(t, ctx, baseURL, "alice", "escalated key", escalatedPerms,
		adminAuth, http.StatusForbidden)

	// ---- 10. Invalid credentials rejected ----
	t.Log("Step 10: Invalid API key rejected")
	fakeEncoded := base64.StdEncoding.EncodeToString([]byte("fakeid:fakesecret"))
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		apiKeyAuth(fakeEncoded), http.StatusUnauthorized)

	// ---- 11. Delete API key ----
	t.Log("Step 11: Delete API key")
	deleteApiKey(t, ctx, baseURL, "alice", key1.KeyID, adminAuth)

	// Subsequent requests with deleted key should fail
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		apiKeyAuth(key1.Encoded), http.StatusUnauthorized)

	// ---- 12. Verify second key still works ----
	t.Log("Step 12: Second key still works after first deleted")
	doRequestExpectStatus(t, ctx, "GET", baseURL+"/status", "",
		apiKeyAuth(key2.Encoded), http.StatusOK)

	// ---- 13. List shows only remaining key ----
	t.Log("Step 13: List shows 1 remaining key")
	keys = listApiKeys(t, ctx, baseURL, "alice", adminAuth)
	assert.Len(t, keys, 1, "expected 1 key after deletion")
	assert.Equal(t, key2.KeyID, keys[0].KeyID)

	// Clean up
	deleteApiKey(t, ctx, baseURL, "alice", key2.KeyID, adminAuth)

	t.Log("API Keys e2e test passed")
}

// ---------- API Key helpers ----------

func createUser(t *testing.T, ctx context.Context, baseURL, username, password string, auth http.Header) {
	t.Helper()
	body := fmt.Sprintf(`{"password":"%s"}`, password)
	doRequestExpectStatus(t, ctx, "POST", baseURL+"/users/"+username, body, auth, http.StatusCreated)
}

func grantPermission(t *testing.T, ctx context.Context, baseURL, username, resource, resourceType, permType string, auth http.Header) {
	t.Helper()
	body := fmt.Sprintf(`{"resource":"%s","resource_type":"%s","type":"%s"}`,
		resource, resourceType, permType)
	doRequestExpectStatus(t, ctx, "POST", baseURL+"/users/"+username+"/permissions",
		body, auth, http.StatusCreated)
}

func createApiKey(t *testing.T, ctx context.Context, baseURL, username, name string, permissions []map[string]string, auth http.Header) apiKeyResponse {
	t.Helper()

	reqBody := map[string]any{
		"name": name,
	}
	if permissions != nil {
		reqBody["permissions"] = permissions
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, "POST",
		baseURL+"/users/"+username+"/api-keys", strings.NewReader(string(bodyBytes)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, vals := range auth {
		for _, v := range vals {
			req.Header.Set(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusCreated, resp.StatusCode,
		"POST /users/%s/api-keys expected 201, got %d: %s", username, resp.StatusCode, string(respBody))

	var result apiKeyResponse
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result
}

func createApiKeyExpectError(t *testing.T, ctx context.Context, baseURL, username, name string, permissions []map[string]string, auth http.Header, wantStatus int) {
	t.Helper()

	reqBody := map[string]any{
		"name": name,
	}
	if permissions != nil {
		reqBody["permissions"] = permissions
	}

	bodyBytes, err := json.Marshal(reqBody)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(ctx, "POST",
		baseURL+"/users/"+username+"/api-keys", strings.NewReader(string(bodyBytes)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, vals := range auth {
		for _, v := range vals {
			req.Header.Set(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	assert.Equal(t, wantStatus, resp.StatusCode,
		"POST /users/%s/api-keys expected status %d", username, wantStatus)
}

func listApiKeys(t *testing.T, ctx context.Context, baseURL, username string, auth http.Header) []apiKeyListEntry {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, "GET",
		baseURL+"/users/"+username+"/api-keys", nil)
	require.NoError(t, err)
	for k, vals := range auth {
		for _, v := range vals {
			req.Header.Set(k, v)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"GET /users/%s/api-keys expected 200, got %d: %s", username, resp.StatusCode, string(respBody))

	var result []apiKeyListEntry
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result
}

func deleteApiKey(t *testing.T, ctx context.Context, baseURL, username, keyID string, auth http.Header) {
	t.Helper()
	doRequestExpectStatus(t, ctx, "DELETE",
		baseURL+"/users/"+username+"/api-keys/"+keyID,
		"", auth, http.StatusNoContent)
}
