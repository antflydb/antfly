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

package metadata

import (
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/secrets"
)

func TestEnvVarForKey(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"openai.api_key", "OPENAI_API_KEY"},
		{"anthropic.api_key", "ANTHROPIC_API_KEY"},
		{"gemini.api_key", "GEMINI_API_KEY"},
		{"aws.access_key_id", "AWS_ACCESS_KEY_ID"},
		{"custom.my_key", "CUSTOM_MY_KEY"},
		{"simple", "SIMPLE"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := secrets.EnvVarForKey(tt.key)
			if got != tt.want {
				t.Errorf("EnvVarForKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestIsEnvVarSet(t *testing.T) {
	// Set a test env var
	t.Setenv("OPENAI_API_KEY", "test-value")

	if !secrets.IsEnvVarSet("openai.api_key") {
		t.Error("expected openai.api_key to be set via OPENAI_API_KEY env var")
	}

	if secrets.IsEnvVarSet("nonexistent.key") {
		t.Error("expected nonexistent.key to not be set")
	}
}

func TestSecretKeyPattern(t *testing.T) {
	valid := []string{
		"openai.api_key",
		"my-secret",
		"test_key",
		"Simple",
		"a.b.c.d",
		"key-with-dash",
		"key_with_underscore",
		"123",
	}

	invalid := []string{
		"key with spaces",
		"key/slash",
		"key\\backslash",
		"key$dollar",
		"key@at",
		"",
	}

	for _, key := range valid {
		if !secretKeyPattern.MatchString(key) {
			t.Errorf("expected %q to be valid", key)
		}
	}

	for _, key := range invalid {
		if secretKeyPattern.MatchString(key) {
			t.Errorf("expected %q to be invalid", key)
		}
	}
}

func TestIsLoopback(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8080", true},
		{"[::1]:8080", true},
		{"localhost:8080", true},
		{"192.168.1.1:8080", false},
		{"10.0.0.1:8080", false},
		{"example.com:8080", false},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			got := isLoopback(tt.addr)
			if got != tt.want {
				t.Errorf("isLoopback(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}

func TestWellKnownSecrets(t *testing.T) {
	// Ensure well-known secrets list is populated
	if len(wellKnownSecrets) == 0 {
		t.Error("wellKnownSecrets should not be empty")
	}

	// Ensure each well-known secret has a valid env var mapping
	for _, key := range wellKnownSecrets {
		envVar := secrets.EnvVarForKey(key)
		if envVar == "" {
			t.Errorf("wellKnownSecret %q has no env var mapping", key)
		}
	}
}

func TestKeystoreOperations(t *testing.T) {
	// Create a temporary keystore for testing
	tmpDir := t.TempDir()
	ks, err := secrets.NewKeystore(tmpDir+"/test-keystore", "test-password")
	if err != nil {
		t.Fatalf("NewKeystore failed: %v", err)
	}

	// Add a secret
	if err := ks.Add("test.key", []byte("test-value")); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Verify it's listed
	keys := ks.List()
	if len(keys) != 1 || keys[0] != "test.key" {
		t.Errorf("List() = %v, want [test.key]", keys)
	}

	// Verify Has
	if !ks.Has("test.key") {
		t.Error("Has(test.key) = false, want true")
	}

	// Save and reload
	if err := ks.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	ks2, err := secrets.LoadKeystore(tmpDir+"/test-keystore", "test-password")
	if err != nil {
		t.Fatalf("LoadKeystore failed: %v", err)
	}

	// Verify secret persisted
	val, err := ks2.GetString("test.key")
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if val != "test-value" {
		t.Errorf("GetString() = %q, want %q", val, "test-value")
	}

	// Remove and verify
	if err := ks2.Remove("test.key"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if ks2.Has("test.key") {
		t.Error("Has(test.key) = true after Remove, want false")
	}
}

func TestEnvMappings(t *testing.T) {
	mappings := secrets.EnvMappings()

	// Ensure core mappings exist
	expected := map[string]string{
		"openai.api_key":    "OPENAI_API_KEY",
		"anthropic.api_key": "ANTHROPIC_API_KEY",
		"gemini.api_key":    "GEMINI_API_KEY",
	}

	for key, envVar := range expected {
		got, ok := mappings[key]
		if !ok {
			t.Errorf("EnvMappings missing key %q", key)
			continue
		}
		if got != envVar {
			t.Errorf("EnvMappings[%q] = %q, want %q", key, got, envVar)
		}
	}
}

func TestSetGlobalKeystore(t *testing.T) {
	// Save original state
	original := secrets.GetGlobalKeystore()
	defer func() {
		if original != nil {
			secrets.SetGlobalKeystore(original)
		}
	}()

	// Create and set a test keystore
	tmpDir := t.TempDir()
	ks, err := secrets.NewKeystore(tmpDir+"/test-ks", "pw")
	if err != nil {
		t.Fatalf("NewKeystore failed: %v", err)
	}

	secrets.SetGlobalKeystore(ks)

	got := secrets.GetGlobalKeystore()
	if got != ks {
		t.Error("SetGlobalKeystore/GetGlobalKeystore roundtrip failed")
	}

	// Resolver should also be updated
	resolver := secrets.GetGlobalResolver()
	if resolver == nil {
		t.Error("GetGlobalResolver should not be nil after SetGlobalKeystore")
	}
}

func TestIsEnvVarSetWithGenericConversion(t *testing.T) {
	// Test with a non-mapped key using generic UPPER_UNDERSCORE conversion
	t.Setenv("COHERE_API_KEY", "test-value")

	if !secrets.IsEnvVarSet("cohere.api_key") {
		t.Error("expected cohere.api_key to be set via COHERE_API_KEY env var (generic conversion)")
	}

	// Clean up
	os.Unsetenv("COHERE_API_KEY")
}
