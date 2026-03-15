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

// Package scraping provides content downloading and processing functionality.
// This file implements the remote content configuration system for resolving
// S3 credentials based on bucket patterns and explicit credential names.
package scraping

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
)

// RemoteContentManager handles credential resolution for remote content fetching.
// It supports multiple named S3 credentials with optional bucket glob patterns
// for auto-selection.
type RemoteContentManager struct {
	mu             sync.RWMutex
	globalSecurity *ContentSecurityConfig
	s3Creds        map[string]*s3CredentialEntry
	httpCreds      map[string]*httpCredentialEntry
	defaultS3      string
}

type s3CredentialEntry struct {
	creds    *S3Credentials
	buckets  []string
	security *ContentSecurityConfig
}

type httpCredentialEntry struct {
	baseURL  string
	headers  map[string]string
	security *ContentSecurityConfig
}

var (
	manager   *RemoteContentManager
	managerMu sync.RWMutex
)

// InitRemoteContentConfig initializes the global remote content configuration.
// This should be called at startup after loading the config file.
//
// Parameters:
//   - cfg: The remote_content config section (may be nil if not configured)
func InitRemoteContentConfig(cfg *RemoteContentConfig) {
	managerMu.Lock()
	defer managerMu.Unlock()

	m := &RemoteContentManager{
		s3Creds:   make(map[string]*s3CredentialEntry),
		httpCreds: make(map[string]*httpCredentialEntry),
	}

	if cfg == nil {
		manager = m
		return
	}

	// Set global security defaults
	if !IsSecurityConfigEmpty(cfg.Security) {
		sec := cfg.Security
		m.globalSecurity = &sec
	}

	m.defaultS3 = cfg.DefaultS3

	// Load S3 credentials
	for name, s3cfg := range cfg.S3 {
		var sec *ContentSecurityConfig
		if !IsSecurityConfigEmpty(s3cfg.Security) {
			s := s3cfg.Security
			sec = &s
		}

		m.s3Creds[name] = &s3CredentialEntry{
			creds: &S3Credentials{
				Endpoint:        s3cfg.Endpoint,
				UseSsl:          s3cfg.UseSsl,
				AccessKeyId:     s3cfg.AccessKeyId,
				SecretAccessKey: s3cfg.SecretAccessKey,
				SessionToken:    s3cfg.SessionToken,
			},
			buckets:  s3cfg.Buckets,
			security: sec,
		}
	}

	// Load HTTP credentials
	for name, httpcfg := range cfg.Http {
		var sec *ContentSecurityConfig
		if !IsSecurityConfigEmpty(httpcfg.Security) {
			s := httpcfg.Security
			sec = &s
		}

		m.httpCreds[name] = &httpCredentialEntry{
			baseURL:  httpcfg.BaseUrl,
			headers:  httpcfg.Headers,
			security: sec,
		}
	}

	manager = m
}

// ResolveS3Credentials resolves S3 credentials for a given URL.
//
// Resolution order:
// 1. Explicit credential name (if explicitCred is not empty)
// 2. First credential where buckets glob pattern matches URL's bucket
// 3. default_s3 credential
// 4. Error if nothing matches
//
// Returns the resolved credentials, security config, and any error.
func ResolveS3Credentials(s3URL, explicitCred string) (*S3Credentials, *ContentSecurityConfig, error) {
	managerMu.RLock()
	m := manager
	managerMu.RUnlock()

	if m == nil {
		// No remote content config - use package defaults
		return GetDefaultS3Credentials(), GetDefaultSecurityConfig(), nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. Explicit credential name
	if explicitCred != "" {
		entry, ok := m.s3Creds[explicitCred]
		if !ok {
			return nil, nil, fmt.Errorf("unknown S3 credential: %s", explicitCred)
		}
		return entry.creds, m.mergeSecurity(entry.security), nil
	}

	// 2. Match bucket patterns
	bucket, err := extractBucket(s3URL)
	if err == nil && bucket != "" {
		for _, entry := range m.s3Creds {
			if matchesBucket(entry.buckets, bucket) {
				return entry.creds, m.mergeSecurity(entry.security), nil
			}
		}
	}

	// 3. Default S3 credential
	if m.defaultS3 != "" {
		if entry, ok := m.s3Creds[m.defaultS3]; ok {
			return entry.creds, m.mergeSecurity(entry.security), nil
		}
	}

	// 4. No credentials found - return package defaults (may be nil)
	creds := GetDefaultS3Credentials()
	if creds == nil {
		return nil, nil, fmt.Errorf("no S3 credentials configured for: %s", s3URL)
	}
	return creds, GetDefaultSecurityConfig(), nil
}

// GetEffectiveSecurity returns the effective security config for remote content.
func GetEffectiveSecurity() *ContentSecurityConfig {
	managerMu.RLock()
	m := manager
	managerMu.RUnlock()

	if m == nil {
		return GetDefaultSecurityConfig()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getEffectiveSecurity()
}

// getEffectiveSecurity returns the security config to use (caller must hold lock).
func (m *RemoteContentManager) getEffectiveSecurity() *ContentSecurityConfig {
	if m.globalSecurity != nil {
		return m.globalSecurity
	}
	return GetDefaultSecurityConfig()
}

// mergeSecurity merges a per-credential security override with the global security config.
func (m *RemoteContentManager) mergeSecurity(override *ContentSecurityConfig) *ContentSecurityConfig {
	base := m.getEffectiveSecurity()
	if override == nil {
		return base
	}
	if base == nil {
		return override
	}

	// Start with base and apply overrides
	merged := *base

	if override.MaxDownloadSizeBytes > 0 {
		merged.MaxDownloadSizeBytes = override.MaxDownloadSizeBytes
	}
	if override.DownloadTimeoutSeconds > 0 {
		merged.DownloadTimeoutSeconds = override.DownloadTimeoutSeconds
	}
	if override.MaxImageDimension > 0 {
		merged.MaxImageDimension = override.MaxImageDimension
	}
	if len(override.AllowedPaths) > 0 {
		merged.AllowedPaths = override.AllowedPaths
	}
	if len(override.AllowedHosts) > 0 {
		merged.AllowedHosts = override.AllowedHosts
	}
	// BlockPrivateIps: only override if explicitly set to false (can't detect "not set" with bool)
	// For safety, we don't override this - the base config's setting is preserved

	return &merged
}

// extractBucket extracts the bucket name from an S3 URL.
// Supports both s3://bucket/key and s3://endpoint/bucket/key formats.
//
// For s3://bucket/key: bucket is in the host field
// For s3://endpoint.com/bucket/key: bucket is first path component
func extractBucket(s3URL string) (string, error) {
	u, err := url.Parse(s3URL)
	if err != nil {
		return "", err
	}

	// If host has no dots or port, it's the bucket (s3://bucket/key format)
	if u.Host != "" && !strings.Contains(u.Host, ".") && !strings.Contains(u.Host, ":") {
		return u.Host, nil
	}

	// Otherwise, bucket is the first path component (s3://endpoint/bucket/key format)
	pathParts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(pathParts) == 0 || pathParts[0] == "" {
		return "", fmt.Errorf("no bucket in URL path")
	}

	return pathParts[0], nil
}

// matchesBucket checks if the bucket matches any of the glob patterns.
func matchesBucket(patterns []string, bucket string) bool {
	for _, p := range patterns {
		if matched, _ := path.Match(p, bucket); matched {
			return true
		}
	}
	return false
}
