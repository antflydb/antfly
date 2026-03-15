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
	"fmt"
	"os"

	"github.com/spf13/viper"
)

// global keystore instance loaded at startup
var globalKeystore *Keystore
var globalResolver *Resolver

// InitKeystoreFromEnv initializes the global keystore from environment variables
// Call this early in application startup, before config is used
func InitKeystoreFromEnv() error {
	// Check for keystore path in environment (will be set by viper flags)
	keystorePath := os.Getenv("ANTFLY_KEYSTORE_PATH")
	if keystorePath == "" {
		keystorePath = DefaultKeystorePath
	}

	// If keystore doesn't exist, that's OK - we'll fall back to env vars
	if !KeystoreExists(keystorePath) {
		globalResolver = NewResolver(nil)
		return nil
	}

	// Get password from environment (empty string is OK for passwordless keystore)
	// Try ANTFLY_KEYSTORE_PASSWORD first, then fall back to just KEYSTORE_PASSWORD
	password := os.Getenv("ANTFLY_KEYSTORE_PASSWORD")
	if password == "" {
		password = os.Getenv("KEYSTORE_PASSWORD")
	}

	// Try to load keystore
	ks, err := LoadKeystore(keystorePath, password)
	if err != nil {
		return fmt.Errorf("failed to load keystore from %s: %w", keystorePath, err)
	}

	globalKeystore = ks
	globalResolver = NewResolver(ks)
	return nil
}

// ResolveViperSecrets resolves all ${secret:...} references in viper's config
// Call this after viper.ReadInConfig() but before using the config
func ResolveViperSecrets(v *viper.Viper) error {
	if globalResolver == nil {
		// Initialize with no keystore (env vars only)
		globalResolver = NewResolver(nil)
	}

	// Get all settings as a map
	settings := v.AllSettings()

	// Resolve secrets in the map
	if err := globalResolver.ResolveMap(settings); err != nil {
		return fmt.Errorf("failed to resolve secrets: %w", err)
	}

	// Set resolved values back into viper using v.Set() for each key
	// This is more reliable than MergeConfigMap for nested structures
	setResolvedValues(v, "", settings)

	// Set environment variables from resolved credentials
	// This ensures services that read env vars directly (like MinIO client) work correctly
	setEnvVarsFromViper(v)

	return nil
}

// setResolvedValues recursively sets resolved values back into Viper
func setResolvedValues(v *viper.Viper, prefix string, settings map[string]any) {
	for key, value := range settings {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		switch val := value.(type) {
		case map[string]any:
			// Recursively handle nested maps
			setResolvedValues(v, fullKey, val)
		case []any:
			// Set array values directly
			v.Set(fullKey, val)
		default:
			// Set scalar values directly
			v.Set(fullKey, val)
		}
	}
}

// setEnvVarsFromViper sets environment variables from resolved Viper config
// This bridges the gap between Viper config and services that read env vars directly (like MinIO)
// Note: Most services (embedding providers, AI generators) have config structs and don't need this
func setEnvVarsFromViper(v *viper.Viper) {
	// Map of viper config paths to environment variable names
	// Only include services that read ONLY from env vars and don't have config struct fields
	envMappings := map[string]string{ //nolint:gosec // G101: env var name mapping, not credentials
		// AWS/S3 credentials - MinIO client reads only from env vars
		// We removed these fields from S3Info struct for security
		"storage.s3.access_key_id":     "AWS_ACCESS_KEY_ID",
		"storage.s3.secret_access_key": "AWS_SECRET_ACCESS_KEY",
		"storage.s3.session_token":     "AWS_SESSION_TOKEN",
	}

	for viperKey, envVar := range envMappings {
		if v.IsSet(viperKey) {
			value := v.GetString(viperKey)
			if value != "" {
				// Only set if not already set (don't override explicit env vars)
				if os.Getenv(envVar) == "" {
					_ = os.Setenv(envVar, value) // Best effort, ignore error
				}
			}
		}
	}
}

// GetGlobalKeystore returns the global keystore instance (may be nil)
func GetGlobalKeystore() *Keystore {
	return globalKeystore
}

// SetGlobalKeystore sets the global keystore instance and updates the resolver.
// Used when auto-creating a keystore at runtime (e.g., swarm mode dashboard).
func SetGlobalKeystore(ks *Keystore) {
	globalKeystore = ks
	globalResolver = NewResolver(ks)
}

// GetGlobalResolver returns the global resolver instance
func GetGlobalResolver() *Resolver {
	if globalResolver == nil {
		globalResolver = NewResolver(nil)
	}
	return globalResolver
}
