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
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveViperSecrets_SetsEnvVars(t *testing.T) {
	// Clean up env vars before and after test
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	defer func() {
		if originalAccessKey != "" {
			_ = os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		} else {
			_ = os.Unsetenv("AWS_ACCESS_KEY_ID")
		}
		if originalSecretKey != "" {
			_ = os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
		} else {
			_ = os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		}
	}()

	// Clear env vars for test
	if err := os.Unsetenv("AWS_ACCESS_KEY_ID"); err != nil {
		t.Logf("Warning: failed to unset AWS_ACCESS_KEY_ID: %v", err)
	}
	if err := os.Unsetenv("AWS_SECRET_ACCESS_KEY"); err != nil {
		t.Logf("Warning: failed to unset AWS_SECRET_ACCESS_KEY: %v", err)
	}

	// Create viper instance with config that has S3 credentials
	v := viper.New()
	v.Set("storage.s3.access_key_id", "AKIAIOSFODNN7EXAMPLE")
	v.Set("storage.s3.secret_access_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	// Resolve secrets (in this case, no ${secret:...} references, just plain values)
	err := ResolveViperSecrets(v)
	require.NoError(t, err)

	// Verify environment variables were set
	assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", os.Getenv("AWS_ACCESS_KEY_ID"))
	assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", os.Getenv("AWS_SECRET_ACCESS_KEY"))
}

func TestResolveViperSecrets_WithKeystore(t *testing.T) {
	// Create temporary keystore
	tmpDir := t.TempDir()
	keystorePath := tmpDir + "/keystore"

	ks, err := NewKeystore(keystorePath, "test-password")
	require.NoError(t, err)

	err = ks.Add("aws.access_key_id", []byte("KEYSTORE_ACCESS_KEY"))
	require.NoError(t, err)
	err = ks.Add("aws.secret_access_key", []byte("KEYSTORE_SECRET_KEY"))
	require.NoError(t, err)

	// Set up global resolver with keystore
	globalResolver = NewResolver(ks)
	defer func() { globalResolver = nil }()

	// Clean up env vars
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	defer func() {
		if originalAccessKey != "" {
			_ = os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		} else {
			_ = os.Unsetenv("AWS_ACCESS_KEY_ID")
		}
		if originalSecretKey != "" {
			_ = os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
		} else {
			_ = os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		}
	}()
	if err := os.Unsetenv("AWS_ACCESS_KEY_ID"); err != nil {
		t.Logf("Warning: failed to unset AWS_ACCESS_KEY_ID: %v", err)
	}
	if err := os.Unsetenv("AWS_SECRET_ACCESS_KEY"); err != nil {
		t.Logf("Warning: failed to unset AWS_SECRET_ACCESS_KEY: %v", err)
	}

	// Create viper instance with ${secret:...} references
	v := viper.New()
	v.Set("storage.s3.access_key_id", "${secret:aws.access_key_id}")
	v.Set("storage.s3.secret_access_key", "${secret:aws.secret_access_key}")

	// Resolve secrets
	err = ResolveViperSecrets(v)
	require.NoError(t, err)

	// Verify Viper has resolved values
	assert.Equal(t, "KEYSTORE_ACCESS_KEY", v.GetString("storage.s3.access_key_id"))
	assert.Equal(t, "KEYSTORE_SECRET_KEY", v.GetString("storage.s3.secret_access_key"))

	// Verify environment variables were set from resolved values
	assert.Equal(t, "KEYSTORE_ACCESS_KEY", os.Getenv("AWS_ACCESS_KEY_ID"))
	assert.Equal(t, "KEYSTORE_SECRET_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY"))
}

func TestSetEnvVarsFromViper_DoesNotOverrideExisting(t *testing.T) {
	// Set existing env var
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "EXISTING_VALUE"); err != nil {
		t.Fatalf("Failed to set environment variable: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("AWS_ACCESS_KEY_ID"); err != nil {
			t.Logf("Warning: failed to unset AWS_ACCESS_KEY_ID: %v", err)
		}
	}()

	// Create viper with different value
	v := viper.New()
	v.Set("storage.s3.access_key_id", "NEW_VALUE")

	// Call setEnvVarsFromViper
	setEnvVarsFromViper(v)

	// Verify existing env var was NOT overridden
	assert.Equal(t, "EXISTING_VALUE", os.Getenv("AWS_ACCESS_KEY_ID"))
}

func TestSetEnvVarsFromViper_OnlySetsConfiguredPaths(t *testing.T) {
	// Clean up
	if err := os.Unsetenv("SOME_RANDOM_VAR"); err != nil {
		t.Logf("Warning: failed to unset SOME_RANDOM_VAR: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("SOME_RANDOM_VAR"); err != nil {
			t.Logf("Warning: failed to unset SOME_RANDOM_VAR: %v", err)
		}
	}()

	// Create viper with random path that's not in envMappings
	v := viper.New()
	v.Set("some.random.path", "value")

	// Call setEnvVarsFromViper
	setEnvVarsFromViper(v)

	// Verify random env var was NOT set (not in mapping)
	assert.Empty(t, os.Getenv("SOME_RANDOM_VAR"))
}
