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
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupFakeS3 creates an in-memory S3 server for testing.
// Returns a MinIO client, the server URL, and a cleanup function.
func setupFakeS3(t *testing.T) (*minio.Client, string, func()) {
	t.Helper()

	// Create in-memory S3 backend
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	// Start HTTP server
	ts := httptest.NewServer(faker.Server())

	// Parse endpoint (remove http://)
	endpoint := strings.TrimPrefix(ts.URL, "http://")

	// Create MinIO client pointing to fake server
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("test-key", "test-secret", ""),
		Secure: false,
	})
	require.NoError(t, err)

	return client, endpoint, ts.Close
}

// createTestBucket creates a bucket and returns a cleanup function.
func createTestBucket(t *testing.T, client *minio.Client, bucket string) {
	t.Helper()
	ctx := context.Background()

	err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		// Remove all objects
		objectsCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
		for obj := range objectsCh {
			client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		client.RemoveBucket(ctx, bucket)
	})
}

// uploadTestContent uploads content to the fake S3 bucket.
func uploadTestContent(t *testing.T, client *minio.Client, bucket, key, contentType string, data []byte) {
	t.Helper()
	ctx := context.Background()

	_, err := client.PutObject(ctx, bucket, key, strings.NewReader(string(data)), int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType})
	require.NoError(t, err)
}

func TestResolveS3Credentials_WithFakeS3(t *testing.T) {
	client, endpoint, cleanup := setupFakeS3(t)
	defer cleanup()

	// Create test buckets
	createTestBucket(t, client, "primary-bucket")
	createTestBucket(t, client, "user-uploads-prod")

	// Upload test content
	uploadTestContent(t, client, "primary-bucket", "test.txt", "text/plain", []byte("Hello from primary"))
	uploadTestContent(t, client, "user-uploads-prod", "doc.txt", "text/plain", []byte("Hello from uploads"))

	// Configure remote content with credentials pointing to fake S3
	InitRemoteContentConfig(&RemoteContentConfig{
		Security: ContentSecurityConfig{
			MaxDownloadSizeBytes:   10 * 1024 * 1024,
			DownloadTimeoutSeconds: 30,
		},
		S3: map[string]S3CredentialConfig{
			"primary": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
			},
			"uploads": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
				Buckets:         []string{"user-uploads-*"},
			},
		},
		DefaultS3: "primary",
	})

	t.Run("explicit credential works", func(t *testing.T) {
		creds, sec, err := ResolveS3Credentials("s3://primary-bucket/test.txt", "primary")
		require.NoError(t, err)
		assert.Equal(t, endpoint, creds.Endpoint)
		assert.Equal(t, "test-key", creds.AccessKeyId)
		assert.NotNil(t, sec)
	})

	t.Run("bucket pattern matches uploads credential", func(t *testing.T) {
		creds, _, err := ResolveS3Credentials("s3://user-uploads-prod/doc.txt", "")
		require.NoError(t, err)
		// Should match "uploads" credential due to bucket pattern
		assert.Equal(t, endpoint, creds.Endpoint)
	})

	t.Run("unknown bucket falls back to default", func(t *testing.T) {
		creds, _, err := ResolveS3Credentials("s3://unknown-bucket/file.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint, creds.Endpoint)
	})
}

func TestDownloadAndProcessLink_WithFakeS3(t *testing.T) {
	client, endpoint, cleanup := setupFakeS3(t)
	defer cleanup()

	createTestBucket(t, client, "test-bucket")

	// Configure credentials
	InitRemoteContentConfig(&RemoteContentConfig{
		Security: ContentSecurityConfig{
			MaxDownloadSizeBytes:   10 * 1024 * 1024,
			DownloadTimeoutSeconds: 30,
			MaxImageDimension:      1024,
		},
		S3: map[string]S3CredentialConfig{
			"default": {
				Endpoint:        endpoint,
				AccessKeyId:     "test-key",
				SecretAccessKey: "test-secret",
				UseSsl:          false,
			},
		},
		DefaultS3: "default",
	})

	t.Run("download plain text", func(t *testing.T) {
		textContent := []byte("This is a test document with some content.")
		uploadTestContent(t, client, "test-bucket", "document.txt", "text/plain", textContent)

		creds, secConfig, err := ResolveS3Credentials("s3://test-bucket/document.txt", "")
		require.NoError(t, err)

		result, err := DownloadAndProcessLink(
			context.Background(),
			"s3://"+endpoint+"/test-bucket/document.txt",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "text", result.Format)
		assert.Equal(t, textContent, result.Data)
	})

	t.Run("download HTML and extract text", func(t *testing.T) {
		htmlContent := []byte(`<!DOCTYPE html>
<html>
<head><title>Test Page</title></head>
<body>
<article>
<h1>Hello World</h1>
<p>This is the main content of the page.</p>
</article>
</body>
</html>`)
		uploadTestContent(t, client, "test-bucket", "page.html", "text/html", htmlContent)

		creds, secConfig, err := ResolveS3Credentials("s3://test-bucket/page.html", "")
		require.NoError(t, err)

		result, err := DownloadAndProcessLink(
			context.Background(),
			"s3://"+endpoint+"/test-bucket/page.html",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "text", result.Format)
		// Should contain extracted text
		assert.Contains(t, string(result.Data), "Hello World")
	})
}

func TestDownloadAndProcessLink_S3Content(t *testing.T) {
	client, endpoint, cleanup := setupFakeS3(t)
	defer cleanup()

	createTestBucket(t, client, "test-bucket")

	t.Run("downloads content with security config", func(t *testing.T) {
		content := []byte("test content from S3")
		uploadTestContent(t, client, "test-bucket", "file.txt", "text/plain", content)

		InitRemoteContentConfig(&RemoteContentConfig{
			Security: ContentSecurityConfig{
				MaxDownloadSizeBytes:   1024 * 1024, // 1MB limit
				DownloadTimeoutSeconds: 30,
			},
			S3: map[string]S3CredentialConfig{
				"default": {
					Endpoint:        endpoint,
					AccessKeyId:     "test-key",
					SecretAccessKey: "test-secret",
					UseSsl:          false,
				},
			},
			DefaultS3: "default",
		})

		creds, secConfig, err := ResolveS3Credentials("s3://test-bucket/file.txt", "")
		require.NoError(t, err)
		require.NotNil(t, secConfig)

		result, err := DownloadAndProcessLink(
			context.Background(),
			"s3://"+endpoint+"/test-bucket/file.txt",
			secConfig,
			creds,
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, content, result.Data)
	})
}

func TestMultipleCredentialConfigs(t *testing.T) {
	// Test with multiple fake S3 servers simulating different environments
	client1, endpoint1, cleanup1 := setupFakeS3(t)
	defer cleanup1()

	client2, endpoint2, cleanup2 := setupFakeS3(t)
	defer cleanup2()

	createTestBucket(t, client1, "prod-bucket")
	createTestBucket(t, client2, "staging-bucket")

	uploadTestContent(t, client1, "prod-bucket", "data.txt", "text/plain", []byte("Production data"))
	uploadTestContent(t, client2, "staging-bucket", "data.txt", "text/plain", []byte("Staging data"))

	// Configure with two different S3 endpoints
	InitRemoteContentConfig(&RemoteContentConfig{
		S3: map[string]S3CredentialConfig{
			"prod": {
				Endpoint:        endpoint1,
				AccessKeyId:     "prod-key",
				SecretAccessKey: "prod-secret",
				UseSsl:          false,
				Buckets:         []string{"prod-*"},
			},
			"staging": {
				Endpoint:        endpoint2,
				AccessKeyId:     "staging-key",
				SecretAccessKey: "staging-secret",
				UseSsl:          false,
				Buckets:         []string{"staging-*"},
			},
		},
	})

	t.Run("prod bucket routes to prod credentials", func(t *testing.T) {
		creds, _, err := ResolveS3Credentials("s3://prod-bucket/data.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint1, creds.Endpoint)
		assert.Equal(t, "prod-key", creds.AccessKeyId)
	})

	t.Run("staging bucket routes to staging credentials", func(t *testing.T) {
		creds, _, err := ResolveS3Credentials("s3://staging-bucket/data.txt", "")
		require.NoError(t, err)
		assert.Equal(t, endpoint2, creds.Endpoint)
		assert.Equal(t, "staging-key", creds.AccessKeyId)
	})

	t.Run("explicit credential overrides pattern", func(t *testing.T) {
		// Even though bucket matches prod pattern, explicit credential wins
		creds, _, err := ResolveS3Credentials("s3://prod-bucket/data.txt", "staging")
		require.NoError(t, err)
		assert.Equal(t, endpoint2, creds.Endpoint)
		assert.Equal(t, "staging-key", creds.AccessKeyId)
	})
}

func TestPerCredentialSecurityOverrides(t *testing.T) {
	_, endpoint, cleanup := setupFakeS3(t)
	defer cleanup()

	InitRemoteContentConfig(&RemoteContentConfig{
		Security: ContentSecurityConfig{
			MaxDownloadSizeBytes:   100 * 1024 * 1024, // 100MB global
			DownloadTimeoutSeconds: 60,
			MaxImageDimension:      4096,
		},
		S3: map[string]S3CredentialConfig{
			"trusted": {
				Endpoint:        endpoint,
				AccessKeyId:     "trusted-key",
				SecretAccessKey: "trusted-secret",
				UseSsl:          false,
			},
			"untrusted": {
				Endpoint:        endpoint,
				AccessKeyId:     "untrusted-key",
				SecretAccessKey: "untrusted-secret",
				UseSsl:          false,
				Security: ContentSecurityConfig{
					MaxDownloadSizeBytes: 5 * 1024 * 1024, // 5MB override
					MaxImageDimension:    1024,            // Smaller images
				},
			},
		},
		DefaultS3: "trusted",
	})

	t.Run("trusted credential gets global security", func(t *testing.T) {
		_, sec, err := ResolveS3Credentials("s3://any-bucket/file.txt", "trusted")
		require.NoError(t, err)
		assert.Equal(t, int64(100*1024*1024), sec.MaxDownloadSizeBytes)
		assert.Equal(t, 60, sec.DownloadTimeoutSeconds)
		assert.Equal(t, 4096, sec.MaxImageDimension)
	})

	t.Run("untrusted credential gets merged security", func(t *testing.T) {
		_, sec, err := ResolveS3Credentials("s3://any-bucket/file.txt", "untrusted")
		require.NoError(t, err)
		assert.Equal(t, int64(5*1024*1024), sec.MaxDownloadSizeBytes) // Overridden
		assert.Equal(t, 60, sec.DownloadTimeoutSeconds)               // From global
		assert.Equal(t, 1024, sec.MaxImageDimension)                  // Overridden
	})
}
