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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// secretEntry mirrors the API response shape for a single secret.
type secretEntry struct {
	Key       string `json:"key"`
	Status    string `json:"status"`
	EnvVar    string `json:"env_var,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type secretList struct {
	Secrets []secretEntry `json:"secrets"`
}

// TestE2E_SecretsAPI tests the full secrets CRUD lifecycle via raw HTTP.
func TestE2E_SecretsAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start swarm — no Termite needed, secrets are metadata-only.
	t.Log("Starting Antfly swarm (no Termite)...")
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	defer swarm.Cleanup()

	baseURL := swarm.MetadataAPIURL + "/api/v1"

	// ---- 1. GET /secrets — initially empty (or env-var-only) ----
	t.Log("Step 1: List secrets (expect empty)")
	list := listSecrets(t, ctx, baseURL)
	t.Logf("  Got %d secrets", len(list.Secrets))
	// No keystore secrets should exist in a fresh swarm.
	for _, s := range list.Secrets {
		assert.NotEqual(t, "configured_keystore", s.Status,
			"fresh swarm should not have keystore secrets")
	}

	// ---- 2. PUT /secrets/test.api_key — store a secret ----
	t.Log("Step 2: Store secret test.api_key")
	entry := putSecret(t, ctx, baseURL, "test.api_key", "sk-test-12345")
	assert.Equal(t, "test.api_key", entry.Key)
	assert.Equal(t, "configured_keystore", entry.Status)
	assert.Equal(t, "TEST_API_KEY", entry.EnvVar)
	assert.NotEmpty(t, entry.CreatedAt, "expected created_at timestamp")
	t.Logf("  Stored: key=%s status=%s env_var=%s", entry.Key, entry.Status, entry.EnvVar)

	// ---- 3. GET /secrets — verify the secret appears ----
	t.Log("Step 3: List secrets (expect test.api_key)")
	list = listSecrets(t, ctx, baseURL)
	found := findSecret(list, "test.api_key")
	require.NotNil(t, found, "test.api_key should appear in list")
	assert.Equal(t, "configured_keystore", found.Status)
	t.Logf("  Found: key=%s status=%s", found.Key, found.Status)

	// ---- 4. PUT again — update the same key ----
	t.Log("Step 4: Update secret test.api_key")
	entry2 := putSecret(t, ctx, baseURL, "test.api_key", "sk-test-67890-updated")
	assert.Equal(t, "configured_keystore", entry2.Status)
	assert.NotEmpty(t, entry2.UpdatedAt, "expected updated_at timestamp after update")
	t.Logf("  Updated: updated_at=%s", entry2.UpdatedAt)

	// ---- 5. PUT a second secret ----
	t.Log("Step 5: Store second secret openai.api_key")
	putSecret(t, ctx, baseURL, "openai.api_key", "sk-openai-fake")

	list = listSecrets(t, ctx, baseURL)
	foundTest := findSecret(list, "test.api_key")
	foundOpenAI := findSecret(list, "openai.api_key")
	require.NotNil(t, foundTest, "test.api_key should still exist")
	require.NotNil(t, foundOpenAI, "openai.api_key should exist")
	t.Logf("  Total secrets: %d", len(list.Secrets))

	// ---- 6. DELETE /secrets/test.api_key ----
	t.Log("Step 6: Delete secret test.api_key")
	deleteSecret(t, ctx, baseURL, "test.api_key")

	list = listSecrets(t, ctx, baseURL)
	assert.Nil(t, findSecret(list, "test.api_key"), "test.api_key should be gone after delete")
	assert.NotNil(t, findSecret(list, "openai.api_key"), "openai.api_key should still exist")
	t.Logf("  Remaining secrets: %d", len(list.Secrets))

	// ---- 7. DELETE the second secret ----
	t.Log("Step 7: Delete secret openai.api_key")
	deleteSecret(t, ctx, baseURL, "openai.api_key")

	list = listSecrets(t, ctx, baseURL)
	for _, s := range list.Secrets {
		assert.NotEqual(t, "configured_keystore", s.Status,
			"all keystore secrets should be deleted")
	}
	t.Logf("  Final secrets: %d (env-only or none)", len(list.Secrets))

	// ---- 8. Validation: invalid key format ----
	t.Log("Step 8: Verify invalid key rejected")
	putSecretExpectError(t, ctx, baseURL, "invalid key!", "bad-value", http.StatusBadRequest)

	// ---- 9. DELETE non-existent key ----
	t.Log("Step 9: Delete non-existent key returns 404")
	deleteSecretExpectStatus(t, ctx, baseURL, "nonexistent.key", http.StatusNotFound)

	// ---- 10. Env var detection ----
	t.Log("Step 10: Verify env var detection")
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-value")
	list = listSecrets(t, ctx, baseURL)
	foundAnth := findSecret(list, "anthropic.api_key")
	require.NotNil(t, foundAnth, "anthropic.api_key should appear when ANTHROPIC_API_KEY is set")
	assert.Equal(t, "configured_env", foundAnth.Status)
	t.Logf("  Env-detected: key=%s status=%s env_var=%s", foundAnth.Key, foundAnth.Status, foundAnth.EnvVar)

	// ---- 11. Both: keystore + env var ----
	t.Log("Step 11: Verify 'both' status when keystore + env var")
	putSecret(t, ctx, baseURL, "anthropic.api_key", "sk-ant-keystore-value")
	list = listSecrets(t, ctx, baseURL)
	foundAnth = findSecret(list, "anthropic.api_key")
	require.NotNil(t, foundAnth)
	assert.Equal(t, "configured_both", foundAnth.Status)
	t.Logf("  Both: key=%s status=%s", foundAnth.Key, foundAnth.Status)

	// Clean up
	deleteSecret(t, ctx, baseURL, "anthropic.api_key")

	t.Log("Secrets API e2e test passed")
}

// ---------- helpers ----------

func listSecrets(t *testing.T, ctx context.Context, baseURL string) secretList {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "GET", baseURL+"/secrets", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /secrets should return 200")

	var result secretList
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	return result
}

func putSecret(t *testing.T, ctx context.Context, baseURL, key, value string) secretEntry {
	t.Helper()
	body := `{"value":"` + value + `"}`
	req, err := http.NewRequestWithContext(ctx, "PUT",
		baseURL+"/secrets/"+key, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "PUT /secrets/%s should return 200", key)

	var entry secretEntry
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&entry))
	return entry
}

func putSecretExpectError(t *testing.T, ctx context.Context, baseURL, key, value string, wantStatus int) {
	t.Helper()
	body := `{"value":"` + value + `"}`
	req, err := http.NewRequestWithContext(ctx, "PUT",
		baseURL+"/secrets/"+key, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, wantStatus, resp.StatusCode,
		"PUT /secrets/%s expected status %d", key, wantStatus)
	// Drain body to allow connection reuse.
	_, _ = io.ReadAll(resp.Body)
}

func deleteSecret(t *testing.T, ctx context.Context, baseURL, key string) {
	t.Helper()
	deleteSecretExpectStatus(t, ctx, baseURL, key, http.StatusNoContent)
}

func deleteSecretExpectStatus(t *testing.T, ctx context.Context, baseURL, key string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "DELETE",
		baseURL+"/secrets/"+key, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, wantStatus, resp.StatusCode,
		"DELETE /secrets/%s expected status %d", key, wantStatus)
	// Drain body to allow connection reuse.
	_, _ = io.ReadAll(resp.Body)
}

func findSecret(list secretList, key string) *secretEntry {
	for _, s := range list.Secrets {
		if s.Key == key {
			return &s
		}
	}
	return nil
}
