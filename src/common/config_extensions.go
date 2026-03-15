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

package common

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/pkg/libaf/s3"
	"github.com/minio/minio-go/v7"
)

// Extension fields and methods for MetadataInfo
type (
	// Private fields for caching parsed orchestration URLs
	metadataInfoExtension struct {
		parsedMu sync.Mutex
		parsed   map[types.ID]string
	}
)

var metadataCache = make(map[*MetadataInfo]*metadataInfoExtension)
var metadataCacheMu sync.Mutex

// GetOrchestrationURLs returns parsed orchestration URLs with caching
func (m *MetadataInfo) GetOrchestrationURLs() (map[types.ID]string, error) {
	metadataCacheMu.Lock()
	ext, ok := metadataCache[m]
	if !ok {
		ext = &metadataInfoExtension{}
		metadataCache[m] = ext
	}
	metadataCacheMu.Unlock()

	ext.parsedMu.Lock()
	defer ext.parsedMu.Unlock()

	if ext.parsed != nil {
		return ext.parsed, nil
	}

	parsed := make(map[types.ID]string, len(m.OrchestrationUrls))
	for idStr, urlStr := range m.OrchestrationUrls {
		id, err := types.IDFromString(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid metadata node ID '%s': %w", idStr, err)
		}
		parsed[id] = urlStr
	}

	ext.parsed = parsed
	return ext.parsed, nil
}

// Validate performs comprehensive validation of the configuration
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config cannot be nil")
	}

	// Validate metadata configuration
	if err := c.validateMetadata(); err != nil {
		return fmt.Errorf("metadata config validation failed: %w", err)
	}

	// Validate TLS configuration
	if err := c.validateTLS(); err != nil {
		return fmt.Errorf("tls config validation failed: %w", err)
	}

	// Validate storage configuration
	if err := c.validateStorage(); err != nil {
		return fmt.Errorf("storage config validation failed: %w", err)
	}

	// Validate MaxShardSizeBytes
	if err := c.validateMaxShardSizeBytes(); err != nil {
		return fmt.Errorf("max_shard_size_bytes validation failed: %w", err)
	}

	// Validate ReplicationFactor
	if err := c.validateReplicationFactor(); err != nil {
		return fmt.Errorf("replication_factor validation failed: %w", err)
	}

	if c.DefaultShardsPerTable == 0 {
		return errors.New("default_shards_per_table must be greater than 0")
	}

	return nil
}

// validateMetadata validates the metadata configuration
func (c *Config) validateMetadata() error {
	if len(c.Metadata.OrchestrationUrls) == 0 {
		return errors.New("at least one orchestration URL is required")
	}

	// Validate each orchestration URL
	for idStr, urlStr := range c.Metadata.OrchestrationUrls {
		if strings.TrimSpace(urlStr) == "" {
			return fmt.Errorf("orchestration URL at %s cannot be empty", idStr)
		}

		if err := validateURL(urlStr); err != nil {
			return fmt.Errorf("invalid orchestration URL at %s (%s): %w", idStr, urlStr, err)
		}
	}

	// Check for duplicate URLs
	urlSet := make(map[string]bool)
	for idStr, urlStr := range c.Metadata.OrchestrationUrls {
		if urlSet[urlStr] {
			return fmt.Errorf("duplicate orchestration URL at %s: %s", idStr, urlStr)
		}
		urlSet[urlStr] = true
	}

	if _, err := c.Metadata.GetOrchestrationURLs(); err != nil {
		return fmt.Errorf("parsing orchestration URLs: %w", err)
	}

	return nil
}

// validateTLS validates the TLS configuration
func (c *Config) validateTLS() error {
	cert := strings.TrimSpace(c.Tls.Cert)
	key := strings.TrimSpace(c.Tls.Key)

	// If TLS is specified, both cert and key are required
	if cert == "" && key == "" {
		// Both empty is valid (TLS disabled)
		return nil
	}

	if cert == "" {
		return errors.New("TLS certificate path is required when TLS is enabled")
	}

	if key == "" {
		return errors.New("TLS key path is required when TLS is enabled")
	}

	// Validate certificate file exists and is readable
	if err := validateFileExists(cert); err != nil {
		return fmt.Errorf("TLS certificate file validation failed: %w", err)
	}

	// Validate key file exists and is readable
	if err := validateFileExists(key); err != nil {
		return fmt.Errorf("TLS key file validation failed: %w", err)
	}

	return nil
}

// validateStorage validates the storage configuration
func (c *Config) validateStorage() error {
	// Validate local storage base directory
	if strings.TrimSpace(c.Storage.Local.BaseDir) == "" {
		return errors.New("storage.local.base_dir is required")
	}

	// Validate data backend selection
	if c.Storage.Data != "" && c.Storage.Data != StorageBackendLocal && c.Storage.Data != StorageBackendS3 {
		return fmt.Errorf("storage.data must be 'local' or 's3', got '%s'", c.Storage.Data)
	}

	// Validate metadata backend selection
	if c.Storage.Metadata != "" && c.Storage.Metadata != StorageBackendLocal && c.Storage.Metadata != StorageBackendS3 {
		return fmt.Errorf("storage.metadata must be 'local' or 's3', got '%s'", c.Storage.Metadata)
	}

	// If either data or metadata uses S3, validate S3 configuration
	if c.Storage.Data == StorageBackendS3 || c.Storage.Metadata == StorageBackendS3 {
		// Validate S3 endpoint
		if strings.TrimSpace(c.Storage.S3.Endpoint) == "" {
			return errors.New("storage.s3.endpoint is required when using S3 storage")
		}

		// Validate S3 bucket
		if strings.TrimSpace(c.Storage.S3.Bucket) == "" {
			return errors.New("storage.s3.bucket is required when using S3 storage")
		}

		// Validate bucket name format (basic validation)
		bucket := strings.TrimSpace(c.Storage.S3.Bucket)
		if len(bucket) < 3 || len(bucket) > 63 {
			return fmt.Errorf("S3 bucket name must be between 3 and 63 characters, got %d", len(bucket))
		}

		// Check for credentials from config (resolved via secrets resolver) or environment variables
		creds := c.Storage.S3.GetS3Credentials()
		if creds.AccessKeyId == "" || creds.SecretAccessKey == "" {
			return errors.New(
				"S3 credentials required: set access_key_id and secret_access_key in config " +
					"(supports ${secret:aws.access_key_id} syntax for keystore lookup), " +
					"or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, " +
					"or configure IAM roles when running on AWS",
			)
		}
	}

	return nil
}

// validateMaxShardSizeBytes validates the MaxShardSizeBytes field
func (c *Config) validateMaxShardSizeBytes() error {
	// MaxShardSizeBytes should be reasonable (at least 1MB, at most 42TB)
	const minShardSize = 1024 * 1024                    // 1MB
	const maxShardSize = 42 * 1024 * 1024 * 1024 * 1024 // 42TB

	if c.MaxShardSizeBytes == 0 {
		return errors.New("max_shard_size_bytes must be greater than 0")
	}

	if c.MaxShardSizeBytes < minShardSize {
		return fmt.Errorf(
			"max_shard_size_bytes must be at least %d bytes (1MB), got %d",
			minShardSize,
			c.MaxShardSizeBytes,
		)
	}

	if c.MaxShardSizeBytes > maxShardSize {
		return fmt.Errorf(
			"max_shard_size_bytes must be at most %d bytes (42TB), got %d",
			maxShardSize,
			c.MaxShardSizeBytes,
		)
	}

	return nil
}

// validateReplicationFactor validates the ReplicationFactor field
func (c *Config) validateReplicationFactor() error {
	const minReplicationFactor = 1
	const maxReplicationFactor = 5

	if c.ReplicationFactor < minReplicationFactor {
		return fmt.Errorf(
			"replication_factor must be at least %d, got %d",
			minReplicationFactor,
			c.ReplicationFactor,
		)
	}

	if c.ReplicationFactor > maxReplicationFactor {
		return fmt.Errorf(
			"replication_factor must be at most %d, got %d",
			maxReplicationFactor,
			c.ReplicationFactor,
		)
	}

	return nil
}

// GetBaseDir returns the base directory for local storage, with fallback to default.
// Falls back to ~/.antfly on Unix or %USERPROFILE%\.antfly on Windows.
func (c *Config) GetBaseDir() string {
	if c == nil {
		return DefaultDataDir()
	}
	if c.Storage.Local.BaseDir == "" {
		return DefaultDataDir()
	}
	return c.Storage.Local.BaseDir
}

// GetKeyValueStorageType returns the storage type for key-value data ("local" or "s3")
func (c *Config) GetKeyValueStorageType() string {
	if c == nil {
		return "local"
	}
	if c.Storage.Data == "" {
		return "local"
	}
	return string(c.Storage.Data)
}

// GetMetadataStorageType returns the storage type for metadata ("local" or "s3")
func (c *Config) GetMetadataStorageType() string {
	if c == nil {
		return "local"
	}
	if c.Storage.Metadata == "" {
		return "local"
	}
	return string(c.Storage.Metadata)
}

// GetS3Credentials returns S3 credentials from config with fallback to environment variables.
// Config values take precedence; if not set, falls back to environment variables:
// - AWS_ACCESS_KEY_ID for access key
// - AWS_SECRET_ACCESS_KEY for secret key
// - AWS_SESSION_TOKEN for session token
// - AWS_ENDPOINT_URL for endpoint (useful for S3-compatible storage like GCS, MinIO)
func (s *S3Info) GetS3Credentials() *s3.Credentials {
	accessKeyID := s.AccessKeyId
	if accessKeyID == "" {
		accessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	secretAccessKey := s.SecretAccessKey
	if secretAccessKey == "" {
		secretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	sessionToken := s.SessionToken
	if sessionToken == "" {
		sessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}

	endpoint := s.Endpoint
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
	}

	return &s3.Credentials{
		Endpoint:        endpoint,
		UseSsl:          s.UseSsl,
		AccessKeyId:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
	}
}

// NewMinioClient creates a Minio client using this S3Info configuration.
// Credentials are read from config fields (which may have been resolved via ${secret:...} syntax)
// with fallback to environment variables.
func (s *S3Info) NewMinioClient() (*minio.Client, error) {
	return s.GetS3Credentials().NewMinioClient()
}

// validateURL validates that a URL string is well-formed and uses supported schemes
func validateURL(urlStr string) error {
	if strings.TrimSpace(urlStr) == "" {
		return errors.New("URL cannot be empty")
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("malformed URL: %w", err)
	}

	// Check for supported schemes
	supportedSchemes := map[string]bool{
		"http":  true,
		"https": true,
	}

	if !supportedSchemes[parsedURL.Scheme] {
		return fmt.Errorf(
			"unsupported URL scheme '%s', supported schemes are: [http, https]",
			parsedURL.Scheme,
		)
	}

	if parsedURL.Host == "" {
		return errors.New("URL must have a host")
	}

	return nil
}

// validateFileExists validates that a file exists and is readable
func validateFileExists(filePath string) error {
	if strings.TrimSpace(filePath) == "" {
		return errors.New("file path cannot be empty")
	}

	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", filePath)
		}
		return fmt.Errorf("cannot access file %s: %w", filePath, err)
	}

	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", filePath)
	}

	// Try to open file to check if it's readable
	file, err := os.Open(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("file is not readable: %s: %w", filePath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}

// ParseS3URL parses an S3 URL and returns the bucket name and object key prefix.
// The URL format is: s3://bucket-name/optional/path/prefix
// For example:
//   - "s3://my-bucket" returns ("my-bucket", "", nil)
//   - "s3://my-bucket/" returns ("my-bucket", "", nil)
//   - "s3://my-bucket/prefix" returns ("my-bucket", "prefix", nil)
//   - "s3://my-bucket/path/to/backups/" returns ("my-bucket", "path/to/backups/", nil)
func ParseS3URL(location string) (bucket string, prefix string, err error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", "", fmt.Errorf("invalid URL: %w", err)
	}

	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("expected s3:// scheme, got %s://", u.Scheme)
	}

	bucket = u.Host
	if bucket == "" {
		return "", "", errors.New("bucket name is required in S3 URL")
	}

	// Path starts with /, so trim the leading slash
	prefix = strings.TrimPrefix(u.Path, "/")

	return bucket, prefix, nil
}
