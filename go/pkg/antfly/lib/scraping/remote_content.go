// Copyright 2026 Antfly, Inc.
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
	globalSecurity *securityConfigOverride
	s3Creds        map[string]*s3CredentialEntry
	httpCreds      map[string]*httpCredentialEntry
	defaultS3      string
}

// RemoteContentInitOptions carries field-presence information that generated
// config structs cannot represent, such as an explicit block_private_ips: false.
type RemoteContentInitOptions struct {
	GlobalSecurityConfigured        bool
	GlobalBlockPrivateIpsConfigured bool
	S3SecurityConfigured            map[string]bool
	S3BlockPrivateIpsConfigured     map[string]bool
	HTTPSecurityConfigured          map[string]bool
	HTTPBlockPrivateIpsConfigured   map[string]bool
}

type securityConfigOverride struct {
	config             ContentSecurityConfig
	blockPrivateIPsSet bool
}

type s3CredentialEntry struct {
	creds    *S3Credentials
	buckets  []string
	security *securityConfigOverride
}

type httpCredentialEntry struct {
	baseURL  string
	headers  map[string]string
	security *securityConfigOverride
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
	InitRemoteContentConfigWithOptions(cfg, RemoteContentInitOptions{})
}

// InitRemoteContentConfigWithOptions initializes remote content configuration
// with field-presence metadata from the config loader.
func InitRemoteContentConfigWithOptions(cfg *RemoteContentConfig, opts RemoteContentInitOptions) {
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
	if !IsSecurityConfigEmpty(cfg.Security) ||
		opts.GlobalSecurityConfigured ||
		opts.GlobalBlockPrivateIpsConfigured {
		m.globalSecurity = &securityConfigOverride{
			config:             cfg.Security,
			blockPrivateIPsSet: cfg.Security.BlockPrivateIps || opts.GlobalBlockPrivateIpsConfigured,
		}
	}

	m.defaultS3 = cfg.DefaultS3

	// Load S3 credentials
	for name, s3cfg := range cfg.S3 {
		var sec *securityConfigOverride
		s3SecurityConfigured := opts.S3SecurityConfigured[name]
		s3BlockPrivateIpsConfigured := opts.S3BlockPrivateIpsConfigured[name]
		if !IsSecurityConfigEmpty(s3cfg.Security) ||
			s3SecurityConfigured ||
			s3BlockPrivateIpsConfigured {
			sec = &securityConfigOverride{
				config:             s3cfg.Security,
				blockPrivateIPsSet: s3cfg.Security.BlockPrivateIps || s3BlockPrivateIpsConfigured,
			}
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
		var sec *securityConfigOverride
		httpSecurityConfigured := opts.HTTPSecurityConfigured[name]
		httpBlockPrivateIpsConfigured := opts.HTTPBlockPrivateIpsConfigured[name]
		if !IsSecurityConfigEmpty(httpcfg.Security) ||
			httpSecurityConfigured ||
			httpBlockPrivateIpsConfigured {
			sec = &securityConfigOverride{
				config:             httpcfg.Security,
				blockPrivateIPsSet: httpcfg.Security.BlockPrivateIps || httpBlockPrivateIpsConfigured,
			}
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
	return creds, m.getEffectiveSecurity(), nil
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
	return m.mergeSecurity(nil)
}

// mergeSecurity merges a per-credential security override with the global security config.
func (m *RemoteContentManager) mergeSecurity(override *securityConfigOverride) *ContentSecurityConfig {
	merged := *GetDefaultSecurityConfig()
	applySecurityOverride(&merged, m.globalSecurity)
	applySecurityOverride(&merged, override)
	return &merged
}

func applySecurityOverride(base *ContentSecurityConfig, override *securityConfigOverride) {
	if override == nil {
		return
	}
	cfg := override.config
	if cfg.MaxDownloadSizeBytes > 0 {
		base.MaxDownloadSizeBytes = cfg.MaxDownloadSizeBytes
	}
	if cfg.DownloadTimeoutSeconds > 0 {
		base.DownloadTimeoutSeconds = cfg.DownloadTimeoutSeconds
	}
	if cfg.MaxImageDimension > 0 {
		base.MaxImageDimension = cfg.MaxImageDimension
	}
	if len(cfg.AllowedPaths) > 0 {
		base.AllowedPaths = cfg.AllowedPaths
	}
	if len(cfg.AllowedHosts) > 0 {
		base.AllowedHosts = cfg.AllowedHosts
	}
	if cfg.UserAgent != "" {
		base.UserAgent = cfg.UserAgent
	}
	if override.blockPrivateIPsSet {
		base.BlockPrivateIps = cfg.BlockPrivateIps
	}
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
