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
	"regexp"
	"strings"
)

var (
	// secretRefPattern matches ${secret:key.name} references
	secretRefPattern = regexp.MustCompile(`\$\{secret:([a-zA-Z0-9._-]+)\}`)
)

// Resolver resolves secret references in configuration values
type Resolver struct {
	keystore *Keystore
}

// NewResolver creates a new secret resolver
func NewResolver(keystore *Keystore) *Resolver {
	return &Resolver{
		keystore: keystore,
	}
}

// Resolve resolves a value that may contain ${secret:...} references
// Priority order:
// 1. Keystore (if available)
// 2. Environment variables (using standard names)
// 3. Return original reference (will cause error downstream)
func (r *Resolver) Resolve(value string) (string, error) {
	// If no secret references, return as-is
	if !strings.Contains(value, "${secret:") {
		return value, nil
	}

	// Find all secret references
	matches := secretRefPattern.FindAllStringSubmatch(value, -1)
	if len(matches) == 0 {
		return value, nil
	}

	result := value
	for _, match := range matches {
		fullRef := match[0] // ${secret:key.name}
		key := match[1]     // key.name

		// Try to resolve the secret
		secretValue, err := r.resolveSecret(key)
		if err != nil {
			return "", fmt.Errorf("failed to resolve secret '%s': %w", key, err)
		}

		result = strings.ReplaceAll(result, fullRef, secretValue)
	}

	return result, nil
}

// resolveSecret resolves a single secret key
func (r *Resolver) resolveSecret(key string) (string, error) {
	// Try keystore first
	if r.keystore != nil && r.keystore.Has(key) {
		value, err := r.keystore.GetString(key)
		if err != nil {
			return "", fmt.Errorf("keystore error: %w", err)
		}
		return value, nil
	}

	// Fall back to environment variables
	envValue := r.tryEnvironmentVariable(key)
	if envValue != "" {
		return envValue, nil
	}

	return "", fmt.Errorf("secret not found in keystore or environment variables")
}

// tryEnvironmentVariable attempts to find the secret in environment variables
// Maps secret keys to their conventional environment variable names
func (r *Resolver) tryEnvironmentVariable(key string) string {
	// Try exact match with dots replaced by underscores
	envKey := strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	if val := os.Getenv(envKey); val != "" {
		return val
	}

	// Try common environment variable mappings
	envMappings := map[string]string{ //nolint:gosec // G101: env var name mapping, not credentials
		"aws.access_key_id":     "AWS_ACCESS_KEY_ID",
		"aws.secret_access_key": "AWS_SECRET_ACCESS_KEY",
		"aws.session_token":     "AWS_SESSION_TOKEN",
		"openai.api_key":        "OPENAI_API_KEY",
		"openai.base_url":       "OPENAI_BASE_URL",
		"anthropic.api_key":     "ANTHROPIC_API_KEY",
		"gemini.api_key":        "GEMINI_API_KEY",
		"vertexai.project":      "VERTEXAI_PROJECT",
		"vertexai.location":     "VERTEXAI_LOCATION",
		"google.credentials":    "GOOGLE_APPLICATION_CREDENTIALS",
		"google.project":        "GOOGLE_CLOUD_PROJECT",
		"google.location":       "GOOGLE_CLOUD_LOCATION",
	}

	if envKey, ok := envMappings[key]; ok {
		if val := os.Getenv(envKey); val != "" {
			return val
		}
	}

	return ""
}

// ResolveMap resolves all string values in a map that may contain secret references
func (r *Resolver) ResolveMap(m map[string]any) error {
	for key, value := range m {
		switch v := value.(type) {
		case string:
			resolved, err := r.Resolve(v)
			if err != nil {
				return fmt.Errorf("failed to resolve '%s': %w", key, err)
			}
			m[key] = resolved
		case map[string]any:
			if err := r.ResolveMap(v); err != nil {
				return err
			}
		case []any:
			if err := r.ResolveSlice(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveSlice resolves all string values in a slice that may contain secret references
func (r *Resolver) ResolveSlice(s []any) error {
	for i, value := range s {
		switch v := value.(type) {
		case string:
			resolved, err := r.Resolve(v)
			if err != nil {
				return err
			}
			s[i] = resolved
		case map[string]any:
			if err := r.ResolveMap(v); err != nil {
				return err
			}
		case []any:
			if err := r.ResolveSlice(v); err != nil {
				return err
			}
		}
	}
	return nil
}

// HasSecretReference checks if a value contains secret references
func HasSecretReference(value string) bool {
	return strings.Contains(value, "${secret:")
}

// EnvMappings returns the known secret key to environment variable mappings.
func EnvMappings() map[string]string {
	return map[string]string{ //nolint:gosec // G101: env var name mapping, not credentials
		"aws.access_key_id":     "AWS_ACCESS_KEY_ID",
		"aws.secret_access_key": "AWS_SECRET_ACCESS_KEY",
		"aws.session_token":     "AWS_SESSION_TOKEN",
		"openai.api_key":        "OPENAI_API_KEY",
		"openai.base_url":       "OPENAI_BASE_URL",
		"anthropic.api_key":     "ANTHROPIC_API_KEY",
		"gemini.api_key":        "GEMINI_API_KEY",
		"vertexai.project":      "VERTEXAI_PROJECT",
		"vertexai.location":     "VERTEXAI_LOCATION",
		"google.credentials":    "GOOGLE_APPLICATION_CREDENTIALS",
		"google.project":        "GOOGLE_CLOUD_PROJECT",
		"google.location":       "GOOGLE_CLOUD_LOCATION",
	}
}

// EnvVarForKey returns the environment variable name for a given secret key.
// It checks the known mappings first, then falls back to the generic
// UPPER_UNDERSCORE conversion (dots replaced with underscores).
func EnvVarForKey(key string) string {
	mappings := EnvMappings()
	if envKey, ok := mappings[key]; ok {
		return envKey
	}
	return strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

// IsEnvVarSet checks if the environment variable for a given secret key is set.
func IsEnvVarSet(key string) bool {
	envVar := EnvVarForKey(key)
	return os.Getenv(envVar) != ""
}
