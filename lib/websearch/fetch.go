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

package websearch

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/antflydb/antfly/lib/scraping"
	"github.com/antflydb/antfly/pkg/libaf/s3"
)

// Fetcher wraps lib/scraping to provide URL content fetching for the chat agent.
// This consolidates all URL fetching through the scraping module's security controls.
type Fetcher struct {
	securityConfig   *scraping.ContentSecurityConfig
	s3Credentials    *s3.Credentials
	maxContentLength int
}

// NewFetcher creates a new URL fetcher with the given config
func NewFetcher(config FetchConfig) *Fetcher {
	// Build security config from FetchConfig
	securityConfig := &scraping.ContentSecurityConfig{
		BlockPrivateIps:        true,
		MaxDownloadSizeBytes:   100 * 1024 * 1024, // 100MB default
		DownloadTimeoutSeconds: 30,
	}

	if len(config.AllowedHosts) != 0 {
		securityConfig.AllowedHosts = config.AllowedHosts
	}

	if config.BlockPrivateIps != nil {
		securityConfig.BlockPrivateIps = *config.BlockPrivateIps
	}

	if config.MaxDownloadSizeBytes != 0 {
		securityConfig.MaxDownloadSizeBytes = int64(config.MaxDownloadSizeBytes)
	}

	if config.TimeoutSeconds != 0 {
		securityConfig.DownloadTimeoutSeconds = config.TimeoutSeconds
	}

	maxContentLength := 50000
	if config.MaxContentLength != 0 {
		maxContentLength = config.MaxContentLength
	}

	// Use S3 credentials from config if provided, otherwise use package defaults
	var s3Creds *s3.Credentials
	if config.S3Credentials != (s3.Credentials{}) {
		s3Creds = &config.S3Credentials
	}

	return &Fetcher{
		securityConfig:   securityConfig,
		s3Credentials:    s3Creds,
		maxContentLength: maxContentLength,
	}
}

// Fetch downloads and extracts content from a URL using lib/scraping
func (f *Fetcher) Fetch(ctx context.Context, targetURL string) (*FetchResult, error) {
	start := time.Now()

	// Parse and validate URL
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "s3" && u.Scheme != "file" {
		return nil, fmt.Errorf("unsupported URL scheme: %s (only http/https/s3/file supported)", u.Scheme)
	}

	// Use scraping module to download and process
	result, err := scraping.DownloadAndProcessLink(
		ctx,
		targetURL,
		f.securityConfig,
		f.s3Credentials,
		nil, // Use default processor
	)
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %w", err)
	}

	content := string(result.Data)
	truncated := false

	// Truncate if needed
	if len(content) > f.maxContentLength {
		content = content[:f.maxContentLength]
		truncated = true
	}

	// Determine content type from output format
	contentType := "text/plain"
	switch result.Format {
	case "text":
		contentType = "text/plain"
	case "image":
		contentType = "image/png" // scraping returns images as data URIs
	case "data-url":
		contentType = "application/octet-stream"
	}

	return &FetchResult{
		Url:         targetURL,
		Title:       result.Title,
		Content:     content,
		ContentType: contentType,
		Truncated:   truncated,
		FetchTimeMs: int(time.Since(start).Milliseconds()),
	}, nil
}
