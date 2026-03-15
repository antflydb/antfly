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

package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveNoSecrets(t *testing.T) {
	resolver := NewResolver(nil)

	// Plain strings should pass through unchanged
	result, err := resolver.Resolve("plain-value")
	require.NoError(t, err)
	assert.Equal(t, "plain-value", result)

	result, err = resolver.Resolve("http://example.com/path")
	require.NoError(t, err)
	assert.Equal(t, "http://example.com/path", result)
}

func TestResolveFromKeystore(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	err = ks.Add("aws.access_key_id", []byte("AKIAIOSFODNN7EXAMPLE"))
	require.NoError(t, err)

	err = ks.Add("openai.api_key", []byte("sk-123456"))
	require.NoError(t, err)

	resolver := NewResolver(ks)

	// Single reference
	result, err := resolver.Resolve("${secret:aws.access_key_id}")
	require.NoError(t, err)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", result)

	// Reference in middle of string
	result, err = resolver.Resolve("Bearer ${secret:openai.api_key}")
	require.NoError(t, err)
	assert.Equal(t, "Bearer sk-123456", result)

	// Multiple references
	result, err = resolver.Resolve("${secret:aws.access_key_id}:${secret:openai.api_key}")
	require.NoError(t, err)
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE:sk-123456", result)
}

func TestResolveFromEnvironment(t *testing.T) {
	// Set environment variable
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "ENV_KEY_VALUE"); err != nil {
		t.Fatalf("Failed to set environment variable: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("AWS_ACCESS_KEY_ID"); err != nil {
			t.Logf("Warning: failed to unset AWS_ACCESS_KEY_ID: %v", err)
		}
	}()

	// Resolver without keystore
	resolver := NewResolver(nil)

	result, err := resolver.Resolve("${secret:aws.access_key_id}")
	require.NoError(t, err)
	assert.Equal(t, "ENV_KEY_VALUE", result)
}

func TestResolveKeystorePriority(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	// Add to keystore
	err = ks.Add("aws.access_key_id", []byte("KEYSTORE_VALUE"))
	require.NoError(t, err)

	// Also set environment variable
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "ENV_VALUE"); err != nil {
		t.Fatalf("Failed to set environment variable: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("AWS_ACCESS_KEY_ID"); err != nil {
			t.Logf("Warning: failed to unset AWS_ACCESS_KEY_ID: %v", err)
		}
	}()

	resolver := NewResolver(ks)

	// Keystore should take priority
	result, err := resolver.Resolve("${secret:aws.access_key_id}")
	require.NoError(t, err)
	assert.Equal(t, "KEYSTORE_VALUE", result)
}

func TestResolveNotFound(t *testing.T) {
	resolver := NewResolver(nil)

	_, err := resolver.Resolve("${secret:non.existent.key}")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "secret not found")
}

func TestResolveMap(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	err = ks.Add("aws.access_key_id", []byte("AKIAKEY"))
	require.NoError(t, err)
	err = ks.Add("aws.secret_access_key", []byte("SECRETKEY"))
	require.NoError(t, err)

	resolver := NewResolver(ks)

	config := map[string]any{
		"storage": map[string]any{
			"s3": map[string]any{
				"bucket":            "my-bucket",
				"access_key_id":     "${secret:aws.access_key_id}",
				"secret_access_key": "${secret:aws.secret_access_key}",
			},
		},
		"other": "plain-value",
	}

	err = resolver.ResolveMap(config)
	require.NoError(t, err)

	s3 := config["storage"].(map[string]any)["s3"].(map[string]any)
	assert.Equal(t, "AKIAKEY", s3["access_key_id"])
	assert.Equal(t, "SECRETKEY", s3["secret_access_key"])
	assert.Equal(t, "my-bucket", s3["bucket"])
	assert.Equal(t, "plain-value", config["other"])
}

func TestResolveSlice(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "keystore")

	ks, err := NewKeystore(path, "test-password")
	require.NoError(t, err)

	err = ks.Add("api.key", []byte("secret123"))
	require.NoError(t, err)

	resolver := NewResolver(ks)

	slice := []any{
		"plain",
		"${secret:api.key}",
		map[string]any{
			"nested": "${secret:api.key}",
		},
	}

	err = resolver.ResolveSlice(slice)
	require.NoError(t, err)

	assert.Equal(t, "plain", slice[0])
	assert.Equal(t, "secret123", slice[1])
	assert.Equal(t, "secret123", slice[2].(map[string]any)["nested"])
}

func TestHasSecretReference(t *testing.T) {
	assert.True(t, HasSecretReference("${secret:key}"))
	assert.True(t, HasSecretReference("prefix ${secret:key} suffix"))
	assert.False(t, HasSecretReference("plain value"))
	assert.False(t, HasSecretReference("${other:reference}"))
}

func TestEnvironmentVariableMappings(t *testing.T) {
	tests := []struct {
		key    string
		envVar string
		envVal string
	}{
		{"aws.access_key_id", "AWS_ACCESS_KEY_ID", "aws-key"},
		{"openai.api_key", "OPENAI_API_KEY", "openai-key"},
		{"anthropic.api_key", "ANTHROPIC_API_KEY", "anthropic-key"},
		{"gemini.api_key", "GEMINI_API_KEY", "gemini-key"},
		{"google.credentials", "GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds"},
		{"custom.key.name", "CUSTOM_KEY_NAME", "custom-value"},
	}

	resolver := NewResolver(nil)

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if err := os.Setenv(tt.envVar, tt.envVal); err != nil {
				t.Fatalf("Failed to set environment variable %s: %v", tt.envVar, err)
			}
			defer func() {
				if err := os.Unsetenv(tt.envVar); err != nil {
					t.Logf("Warning: failed to unset %s: %v", tt.envVar, err)
				}
			}()

			result, err := resolver.Resolve("${secret:" + tt.key + "}")
			require.NoError(t, err)
			assert.Equal(t, tt.envVal, result)
		})
	}
}
