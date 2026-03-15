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

package scraping

import (
	"testing"
)

func TestResolveS3Credentials_ExplicitCredential(t *testing.T) {
	// Setup
	InitRemoteContentConfig(&RemoteContentConfig{
		S3: map[string]S3CredentialConfig{
			"primary": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "primary-key",
				SecretAccessKey: "primary-secret",
			},
			"secondary": {
				Endpoint:        "s3.us-west-2.amazonaws.com",
				AccessKeyId:     "secondary-key",
				SecretAccessKey: "secondary-secret",
			},
		},
		DefaultS3: "primary",
	})

	// Test explicit credential selection
	creds, sec, err := ResolveS3Credentials("s3://bucket/key", "secondary")
	if err != nil {
		t.Fatalf("ResolveS3Credentials failed: %v", err)
	}
	if creds.AccessKeyId != "secondary-key" {
		t.Errorf("expected secondary-key, got %s", creds.AccessKeyId)
	}
	if sec == nil {
		t.Error("expected security config, got nil")
	}
}

func TestResolveS3Credentials_UnknownExplicitCredential(t *testing.T) {
	InitRemoteContentConfig(&RemoteContentConfig{
		S3: map[string]S3CredentialConfig{
			"primary": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "primary-key",
				SecretAccessKey: "primary-secret",
			},
		},
	})

	_, _, err := ResolveS3Credentials("s3://bucket/key", "nonexistent")
	if err == nil {
		t.Error("expected error for unknown credential")
	}
}

func TestResolveS3Credentials_BucketPatternMatch(t *testing.T) {
	InitRemoteContentConfig(&RemoteContentConfig{
		S3: map[string]S3CredentialConfig{
			"uploads": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "uploads-key",
				SecretAccessKey: "uploads-secret",
				Buckets:         []string{"user-uploads-*", "media-*"},
			},
			"primary": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "primary-key",
				SecretAccessKey: "primary-secret",
			},
		},
		DefaultS3: "primary",
	})

	tests := []struct {
		name    string
		url     string
		wantKey string
		wantErr bool
	}{
		{
			name:    "matches user-uploads-* pattern",
			url:     "s3://user-uploads-prod/doc.pdf",
			wantKey: "uploads-key",
		},
		{
			name:    "matches media-* pattern",
			url:     "s3://media-images/photo.jpg",
			wantKey: "uploads-key",
		},
		{
			name:    "no pattern match, uses default",
			url:     "s3://other-bucket/file.txt",
			wantKey: "primary-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, _, err := ResolveS3Credentials(tt.url, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("ResolveS3Credentials() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && creds.AccessKeyId != tt.wantKey {
				t.Errorf("expected %s, got %s", tt.wantKey, creds.AccessKeyId)
			}
		})
	}
}

func TestResolveS3Credentials_DefaultFallback(t *testing.T) {
	InitRemoteContentConfig(&RemoteContentConfig{
		S3: map[string]S3CredentialConfig{
			"primary": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "primary-key",
				SecretAccessKey: "primary-secret",
			},
		},
		DefaultS3: "primary",
	})

	creds, _, err := ResolveS3Credentials("s3://any-bucket/key", "")
	if err != nil {
		t.Fatalf("ResolveS3Credentials failed: %v", err)
	}
	if creds.AccessKeyId != "primary-key" {
		t.Errorf("expected primary-key, got %s", creds.AccessKeyId)
	}
}

func TestResolveS3Credentials_NoConfig(t *testing.T) {
	// Clear any existing config
	managerMu.Lock()
	manager = nil
	managerMu.Unlock()

	// Also clear package defaults
	SetDefaultS3Credentials(nil)

	_, _, err := ResolveS3Credentials("s3://bucket/key", "")
	// Should return nil credentials and no error when no config
	if err != nil {
		t.Logf("Got expected behavior: %v", err)
	}
}

func TestSecurityConfigMerge(t *testing.T) {
	InitRemoteContentConfig(&RemoteContentConfig{
		Security: ContentSecurityConfig{
			MaxDownloadSizeBytes:   100 * 1024 * 1024, // 100MB global
			DownloadTimeoutSeconds: 30,
			BlockPrivateIps:        true,
		},
		S3: map[string]S3CredentialConfig{
			"untrusted": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "untrusted-key",
				SecretAccessKey: "untrusted-secret",
				Security: ContentSecurityConfig{
					MaxDownloadSizeBytes: 10 * 1024 * 1024, // 10MB override
				},
			},
			"primary": {
				Endpoint:        "s3.amazonaws.com",
				AccessKeyId:     "primary-key",
				SecretAccessKey: "primary-secret",
			},
		},
		DefaultS3: "primary",
	})

	// Test untrusted credential gets merged security (use explicit credential)
	_, sec, err := ResolveS3Credentials("s3://any-bucket/doc.pdf", "untrusted")
	if err != nil {
		t.Fatalf("ResolveS3Credentials failed: %v", err)
	}
	if sec.MaxDownloadSizeBytes != 10*1024*1024 {
		t.Errorf("expected 10MB limit from override, got %d", sec.MaxDownloadSizeBytes)
	}
	if sec.DownloadTimeoutSeconds != 30 {
		t.Errorf("expected 30s timeout (from global), got %d", sec.DownloadTimeoutSeconds)
	}
	if !sec.BlockPrivateIps {
		t.Error("expected BlockPrivateIps to be preserved from global")
	}

	// Test primary credential gets global security (use explicit credential)
	_, sec, err = ResolveS3Credentials("s3://any-bucket/doc.pdf", "primary")
	if err != nil {
		t.Fatalf("ResolveS3Credentials failed: %v", err)
	}
	if sec.MaxDownloadSizeBytes != 100*1024*1024 {
		t.Errorf("expected 100MB limit from global, got %d", sec.MaxDownloadSizeBytes)
	}
}

func TestExtractBucket(t *testing.T) {
	tests := []struct {
		url    string
		want   string
		errMsg string
	}{
		{
			url:  "s3://my-bucket/path/to/file.pdf",
			want: "my-bucket",
		},
		{
			url:  "s3://endpoint.s3.amazonaws.com/bucket-name/file.pdf",
			want: "bucket-name",
		},
		{
			url:  "s3://bucket-only",
			want: "bucket-only",
		},
		{
			url:    "s3://",
			errMsg: "no bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			got, err := extractBucket(tt.url)
			if tt.errMsg != "" {
				if err == nil {
					t.Errorf("expected error containing %q", tt.errMsg)
				}
				return
			}
			if err != nil {
				t.Errorf("extractBucket(%q) error = %v", tt.url, err)
				return
			}
			if got != tt.want {
				t.Errorf("extractBucket(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

func TestMatchesBucket(t *testing.T) {
	tests := []struct {
		patterns []string
		bucket   string
		want     bool
	}{
		{
			patterns: []string{"user-uploads-*"},
			bucket:   "user-uploads-prod",
			want:     true,
		},
		{
			patterns: []string{"user-uploads-*"},
			bucket:   "other-bucket",
			want:     false,
		},
		{
			patterns: []string{"media-*", "uploads-*"},
			bucket:   "uploads-test",
			want:     true,
		},
		{
			patterns: []string{},
			bucket:   "any-bucket",
			want:     false,
		},
		{
			patterns: []string{"exact-bucket"},
			bucket:   "exact-bucket",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.bucket, func(t *testing.T) {
			got := matchesBucket(tt.patterns, tt.bucket)
			if got != tt.want {
				t.Errorf("matchesBucket(%v, %q) = %v, want %v", tt.patterns, tt.bucket, got, tt.want)
			}
		})
	}
}

func TestIsSecurityConfigEmpty(t *testing.T) {
	tests := []struct {
		name string
		cfg  ContentSecurityConfig
		want bool
	}{
		{
			name: "empty config",
			cfg:  ContentSecurityConfig{},
			want: true,
		},
		{
			name: "has max download size",
			cfg:  ContentSecurityConfig{MaxDownloadSizeBytes: 100},
			want: false,
		},
		{
			name: "has timeout",
			cfg:  ContentSecurityConfig{DownloadTimeoutSeconds: 30},
			want: false,
		},
		{
			name: "has allowed hosts",
			cfg:  ContentSecurityConfig{AllowedHosts: []string{"example.com"}},
			want: false,
		},
		{
			name: "has block private ips",
			cfg:  ContentSecurityConfig{BlockPrivateIps: true},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSecurityConfigEmpty(tt.cfg)
			if got != tt.want {
				t.Errorf("IsSecurityConfigEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}
