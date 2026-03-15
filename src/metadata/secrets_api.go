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
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"regexp"
	"slices"

	"github.com/antflydb/antfly/lib/secrets"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

var secretKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// wellKnownSecrets are commonly used secret names that should always be checked,
// even if they don't exist in the keystore.
var wellKnownSecrets = []string{
	"openai.api_key",
	"anthropic.api_key",
	"gemini.api_key",
	"cohere.api_key",
	"openrouter.api_key",
}

// SecretsApi handles secret management API endpoints.
type SecretsApi struct {
	ms     *MetadataStore
	logger *zap.Logger
}

// ListSecrets returns the status of all configured secrets.
// Never returns secret values — only names and configuration status.
func (sa *SecretsApi) ListSecrets(w http.ResponseWriter, r *http.Request) {
	if !sa.ms.ensureAuth(w, r, usermgr.ResourceTypeUser, "*", usermgr.PermissionTypeAdmin) {
		return
	}

	keystore := secrets.GetGlobalKeystore()
	seen := make(map[string]bool)
	var entries []SecretEntry

	// Add keystore entries
	if keystore != nil {
		for _, key := range keystore.List() {
			seen[key] = true
			entry := SecretEntry{
				Key:    key,
				EnvVar: secrets.EnvVarForKey(key),
			}

			// Check timestamps from keystore
			if e, ok := keystore.Entries[key]; ok {
				entry.CreatedAt = e.CreatedAt
				entry.UpdatedAt = e.UpdatedAt
			}

			if secrets.IsEnvVarSet(key) {
				entry.Status = SecretStatusConfiguredBoth
			} else {
				entry.Status = SecretStatusConfiguredKeystore
			}
			entries = append(entries, entry)
		}
	}

	// Check well-known secrets that may only be in env vars
	for _, key := range wellKnownSecrets {
		if seen[key] {
			continue
		}
		if secrets.IsEnvVarSet(key) {
			entries = append(entries, SecretEntry{
				Key:    key,
				Status: SecretStatusConfiguredEnv,
				EnvVar: secrets.EnvVarForKey(key),
			})
			seen[key] = true
		}
	}

	// Also check all known env mappings
	for key := range secrets.EnvMappings() {
		if seen[key] {
			continue
		}
		if secrets.IsEnvVarSet(key) {
			entries = append(entries, SecretEntry{
				Key:    key,
				Status: SecretStatusConfiguredEnv,
				EnvVar: secrets.EnvVarForKey(key),
			})
			seen[key] = true
		}
	}

	// Sort by key
	slices.SortFunc(entries, func(a, b SecretEntry) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(SecretList{Secrets: entries}); err != nil {
		sa.logger.Warn("Failed to encode secrets list", zap.Error(err))
	}
}

// PutSecret stores a secret in the keystore. Only available in swarm mode.
func (sa *SecretsApi) PutSecret(w http.ResponseWriter, r *http.Request, key string) {
	if !sa.ms.ensureAuth(w, r, usermgr.ResourceTypeUser, "*", usermgr.PermissionTypeAdmin) {
		return
	}

	if !sa.ms.config.SwarmMode {
		errorResponse(w, "Secret management via API is only available in swarm (single-node) mode. Configure secrets using environment variables, Kubernetes secrets, or the CLI on each node.", http.StatusServiceUnavailable)
		return
	}

	if !secretKeyPattern.MatchString(key) {
		errorResponse(w, "Invalid secret key format. Must match: ^[a-zA-Z0-9._-]+$", http.StatusBadRequest)
		return
	}

	var req SecretWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Value == "" {
		errorResponse(w, "Secret value is required", http.StatusBadRequest)
		return
	}

	keystore := sa.getOrCreateKeystore()
	if keystore == nil {
		errorResponse(w, "Keystore not available", http.StatusInternalServerError)
		return
	}

	if err := keystore.Add(key, []byte(req.Value)); err != nil {
		sa.logger.Error("Failed to add secret", zap.String("key", key), zap.Error(err))
		errorResponse(w, "Failed to store secret", http.StatusInternalServerError)
		return
	}

	if err := keystore.Save(); err != nil {
		sa.logger.Error("Failed to save keystore", zap.Error(err))
		errorResponse(w, "Failed to save keystore", http.StatusInternalServerError)
		return
	}

	sa.logger.Info("Secret stored via API", zap.String("key", key))

	// Build response
	entry := SecretEntry{
		Key:    key,
		EnvVar: secrets.EnvVarForKey(key),
	}
	if secrets.IsEnvVarSet(key) {
		entry.Status = SecretStatusConfiguredBoth
	} else {
		entry.Status = SecretStatusConfiguredKeystore
	}
	if e, ok := keystore.Entries[key]; ok {
		entry.CreatedAt = e.CreatedAt
		entry.UpdatedAt = e.UpdatedAt
	}

	// Warn about non-TLS on non-loopback
	if r.TLS == nil && !isLoopback(r.RemoteAddr) {
		sa.logger.Warn("Secret submitted over unencrypted connection from non-loopback address",
			zap.String("remote_addr", r.RemoteAddr))
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(entry); err != nil {
		sa.logger.Warn("Failed to encode secret entry", zap.Error(err))
	}
}

// DeleteSecret removes a secret from the keystore. Only available in swarm mode.
func (sa *SecretsApi) DeleteSecret(w http.ResponseWriter, r *http.Request, key string) {
	if !sa.ms.ensureAuth(w, r, usermgr.ResourceTypeUser, "*", usermgr.PermissionTypeAdmin) {
		return
	}

	if !sa.ms.config.SwarmMode {
		errorResponse(w, "Secret management via API is only available in swarm (single-node) mode. Configure secrets using environment variables, Kubernetes secrets, or the CLI on each node.", http.StatusServiceUnavailable)
		return
	}

	keystore := secrets.GetGlobalKeystore()
	if keystore == nil {
		errorResponse(w, "Keystore not available", http.StatusNotFound)
		return
	}

	if err := keystore.Remove(key); err != nil {
		errorResponse(w, "Secret not found", http.StatusNotFound)
		return
	}

	if err := keystore.Save(); err != nil {
		sa.logger.Error("Failed to save keystore after deletion", zap.Error(err))
		errorResponse(w, "Failed to save keystore", http.StatusInternalServerError)
		return
	}

	sa.logger.Info("Secret deleted via API", zap.String("key", key))
	w.WriteHeader(http.StatusNoContent)
}

// getOrCreateKeystore returns the global keystore, creating one if needed in swarm mode.
func (sa *SecretsApi) getOrCreateKeystore() *secrets.Keystore {
	ks := secrets.GetGlobalKeystore()
	if ks != nil {
		return ks
	}

	// Auto-create keystore in swarm mode
	if !sa.ms.config.SwarmMode {
		return nil
	}

	keystorePath := sa.ms.config.GetBaseDir() + "/keystore"
	password := generateKeystorePassword()

	sa.logger.Info("Auto-creating keystore for swarm mode",
		zap.String("path", keystorePath))
	sa.logger.Warn("Generated keystore password — note it for future use",
		zap.String("password", password))

	ks, err := secrets.NewKeystore(keystorePath, password)
	if err != nil {
		sa.logger.Error("Failed to create keystore", zap.Error(err))
		return nil
	}

	if err := ks.Save(); err != nil {
		sa.logger.Error("Failed to save new keystore", zap.Error(err))
		return nil
	}

	// Register as global keystore
	secrets.SetGlobalKeystore(ks)
	return ks
}

// generateKeystorePassword generates a random password for auto-created keystores.
func generateKeystorePassword() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback to a fixed string if crypto/rand fails (extremely unlikely)
		return "antfly-auto-keystore"
	}
	return hex.EncodeToString(b)
}

// isLoopback checks if the remote address is a loopback address.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
