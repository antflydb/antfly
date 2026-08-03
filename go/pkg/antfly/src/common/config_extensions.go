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

package common

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"weak"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/minio/minio-go/v7"
	miniocredentials "github.com/minio/minio-go/v7/pkg/credentials"
)

// Extension fields and methods for MetadataInfo
type (
	// Private fields for caching parsed orchestration URLs
	metadataInfoExtension struct {
		parsedMu sync.Mutex
		parsed   map[types.ID]string
	}

	configRuntimeExtension struct {
		mu            sync.Mutex
		s3Connections map[string]*s3ConnectionRuntime
	}

	s3ConnectionRuntime struct {
		mu          sync.Mutex
		fingerprint [sha256.Size]byte
		initialized bool
		client      *minio.Client
		buckets     sync.Map
	}

	s3BucketAdmission struct {
		mu    sync.Mutex
		ready bool
	}

	// S3Info is the runtime projection of a capability-scoped S3 connection.
	// It is intentionally not part of the public configuration schema.
	S3Info struct {
		Endpoint              string
		Bucket                string
		Prefix                string
		Region                string
		UseSsl                bool
		AddressingStyle       S3ExternalIoConfigAddressingStyle
		BucketProvisioning    S3ExternalIoConfigBucketProvisioning
		CredentialSource      AwsCredentialConfigSource
		AccessKeyId           string
		SecretAccessKey       string
		SessionToken          string
		Profile               string
		SharedCredentialsFile string
		RoleArn               string
		TokenFile             string
		SessionName           string
		StsEndpoint           string
		runtime               *s3ConnectionRuntime
	}
)

var metadataCache = make(map[*MetadataInfo]*metadataInfoExtension)
var metadataCacheMu sync.Mutex
var configRuntimeCache = make(map[weak.Pointer[Config]]*configRuntimeExtension)
var configRuntimeCacheMu sync.Mutex

func (c *Config) s3Runtime(connectionID string) *s3ConnectionRuntime {
	key := weak.Make(c)
	configRuntimeCacheMu.Lock()
	ext, ok := configRuntimeCache[key]
	if !ok {
		for candidate := range configRuntimeCache {
			if candidate.Value() == nil {
				delete(configRuntimeCache, candidate)
			}
		}
		ext = &configRuntimeExtension{
			s3Connections: make(map[string]*s3ConnectionRuntime),
		}
		configRuntimeCache[key] = ext
	}
	configRuntimeCacheMu.Unlock()

	ext.mu.Lock()
	defer ext.mu.Unlock()
	runtime, ok := ext.s3Connections[connectionID]
	if !ok {
		runtime = &s3ConnectionRuntime{}
		ext.s3Connections[connectionID] = runtime
	}
	return runtime
}

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

// ValidationOptions controls command-specific config validation.
type ValidationOptions struct {
	// RequireMetadata requires metadata.orchestration_urls to be configured.
	// Commands that do not contact an external metadata cluster can leave this
	// false; malformed metadata is still rejected when metadata URLs are present.
	RequireMetadata bool
}

// Validate performs comprehensive validation of the configuration.
func (c *Config) Validate() error {
	return c.ValidateWithOptions(ValidationOptions{RequireMetadata: true})
}

// ValidateWithOptions performs comprehensive validation of the configuration.
func (c *Config) ValidateWithOptions(opts ValidationOptions) error {
	if c == nil {
		return errors.New("config cannot be nil")
	}
	if err := c.validateDeploymentMode(); err != nil {
		return fmt.Errorf("deployment_mode validation failed: %w", err)
	}

	// Validate metadata configuration
	requiresExternalMetadata := opts.RequireMetadata && c.EffectiveDeploymentMode() == ConfigDeploymentModeDistributed
	if requiresExternalMetadata || len(c.Metadata.OrchestrationUrls) > 0 {
		if err := c.validateMetadata(); err != nil {
			return fmt.Errorf("metadata config validation failed: %w", err)
		}
	}

	// Validate TLS configuration
	if err := c.validateTLS(); err != nil {
		return fmt.Errorf("tls config validation failed: %w", err)
	}

	// Validate storage configuration
	if err := c.validateStorage(); err != nil {
		return fmt.Errorf("storage config validation failed: %w", err)
	}
	if err := c.validateTransactionSessions(); err != nil {
		return fmt.Errorf("transaction_sessions config validation failed: %w", err)
	}

	// Validate MaxShardSizeBytes
	if err := c.validateMaxShardSizeBytes(); err != nil {
		return fmt.Errorf("max_shard_size_bytes validation failed: %w", err)
	}
	if err := c.validateShardMergeSettings(); err != nil {
		return fmt.Errorf("shard merge settings validation failed: %w", err)
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

func (c *Config) validateTransactionSessions() error {
	checks := []struct {
		name     string
		value    int
		min, max int
	}{
		{"ttl_seconds", c.TransactionSessions.TtlSeconds, 60, 604800},
		{"cleanup_interval_seconds", c.TransactionSessions.CleanupIntervalSeconds, 1, 3600},
		{"max_count", c.TransactionSessions.MaxCount, 1, 65536},
		{"max_record_bytes", c.TransactionSessions.MaxRecordBytes, 65536, 67108864},
		{"max_savepoints", c.TransactionSessions.MaxSavepoints, 1, 1024},
	}
	for _, check := range checks {
		if check.value != 0 && (check.value < check.min || check.value > check.max) {
			return fmt.Errorf("%s must be between %d and %d", check.name, check.min, check.max)
		}
	}
	return nil
}

func (c *Config) validateDeploymentMode() error {
	switch c.EffectiveDeploymentMode() {
	case ConfigDeploymentModeEmbedded, ConfigDeploymentModeDistributed, ConfigDeploymentModeStandalone, ConfigDeploymentModeServerless:
		return nil
	default:
		return fmt.Errorf("unsupported mode %q", c.DeploymentMode)
	}
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
	engine := c.Storage.Engine
	if engine == "" {
		engine = StorageEngineLocal
	}
	hasLite := strings.TrimSpace(c.Storage.Lite.Path) != ""
	hasLocal := strings.TrimSpace(c.Storage.Local.BaseDir) != ""
	hasObject := strings.TrimSpace(c.Storage.Object.Connection) != "" ||
		strings.TrimSpace(c.Storage.Object.Bucket) != "" ||
		strings.TrimSpace(c.Storage.Object.Prefix) != ""

	switch engine {
	case StorageEngineLite:
		if c.EffectiveDeploymentMode() != ConfigDeploymentModeStandalone && c.EffectiveDeploymentMode() != ConfigDeploymentModeEmbedded {
			return fmt.Errorf("storage.engine=lite requires deployment_mode standalone or embedded")
		}
		if !hasLite {
			return errors.New("storage.lite.path is required when storage.engine=lite")
		}
		if !strings.HasSuffix(c.Storage.Lite.Path, ".aflite") {
			return errors.New("storage.lite.path must end in .aflite")
		}
		if hasLocal || hasObject {
			return errors.New("storage.lite, storage.local, and storage.object are mutually exclusive")
		}
		if c.ReplicationFactor > 1 || c.DefaultShardsPerTable > 1 {
			return errors.New("Lite storage supports one writer and one shard; replication and horizontal sharding are not supported")
		}
		if len(c.Metadata.OrchestrationUrls) > 0 {
			return errors.New("Lite storage cannot use external metadata or Raft orchestration URLs")
		}
		return nil
	case StorageEngineObject:
		if c.EffectiveDeploymentMode() != ConfigDeploymentModeServerless {
			return errors.New("storage.engine=object requires deployment_mode serverless")
		}
		if hasLite || hasLocal || !hasObject {
			return errors.New("storage.object is required and must be the only storage member when storage.engine=object")
		}
		if bucketLen := len(strings.TrimSpace(c.Storage.Object.Bucket)); bucketLen < 3 || bucketLen > 63 {
			return fmt.Errorf("object storage bucket name must be between 3 and 63 characters, got %d", bucketLen)
		}
		for _, lane := range []string{"", "artifacts", "manifests", "wal", "progress", "catalog"} {
			if _, err := c.ResolveObjectStorageS3(lane); err != nil {
				label := lane
				if label == "" {
					label = "root"
				}
				return fmt.Errorf("storage.object %s validation failed: %w", label, err)
			}
		}
		return nil
	case StorageEngineLocal:
		if hasLite || hasObject {
			return errors.New("storage.local must be the only tagged storage member when storage.engine=local")
		}
	default:
		return fmt.Errorf("unsupported storage.engine %q", c.Storage.Engine)
	}

	// Validate local storage base directory
	if strings.TrimSpace(c.Storage.Local.BaseDir) == "" {
		return errors.New("storage.local.base_dir is required")
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

func (c *Config) validateShardMergeSettings() error {
	if c.MinShardsPerTable > 0 && c.MinShardsPerTable > c.MaxShardsPerTable {
		return fmt.Errorf(
			"min_shards_per_table must be less than or equal to max_shards_per_table, got %d > %d",
			c.MinShardsPerTable,
			c.MaxShardsPerTable,
		)
	}
	if c.MinShardSizeBytes > 0 && c.MinShardSizeBytes >= c.MaxShardSizeBytes {
		return fmt.Errorf(
			"min_shard_size_bytes must be less than max_shard_size_bytes, got %d >= %d",
			c.MinShardSizeBytes,
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
	if c == nil || c.Storage.Engine != StorageEngineObject {
		return "local"
	}
	return "s3"
}

// GetMetadataStorageType returns the storage type for metadata ("local" or "s3")
func (c *Config) GetMetadataStorageType() string {
	if c == nil || c.Storage.Engine != StorageEngineObject {
		return "local"
	}
	return "s3"
}

func hasCapability(capabilities []string, required string) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
}

func (c *Config) resolveExternalIOConnection(
	connectionID, requiredCapability string,
) (ExternalIoConnectionVariant, error) {
	if c == nil {
		return ExternalIoConnectionVariant{}, errors.New("config is required")
	}
	connectionID = strings.TrimSpace(connectionID)
	if connectionID == "" {
		return ExternalIoConnectionVariant{}, errors.New("connection is required")
	}
	connection, ok := c.Connections[connectionID]
	if !ok {
		return ExternalIoConnectionVariant{}, fmt.Errorf("connection %q was not found", connectionID)
	}
	kind, err := connection.Discriminator()
	if err != nil {
		return ExternalIoConnectionVariant{}, fmt.Errorf("reading connection %q kind: %w", connectionID, err)
	}
	if kind != "external_io" {
		return ExternalIoConnectionVariant{}, fmt.Errorf(
			"connection %q has kind %q, want external_io",
			connectionID,
			kind,
		)
	}
	external, err := connection.AsExternalIoConnectionVariant()
	if err != nil {
		return ExternalIoConnectionVariant{}, fmt.Errorf(
			"decoding external_io connection %q: %w",
			connectionID,
			err,
		)
	}
	if !hasCapability(external.Capabilities, requiredCapability) {
		return ExternalIoConnectionVariant{}, fmt.Errorf(
			"connection %q lacks required capability %q",
			connectionID,
			requiredCapability,
		)
	}
	return external, nil
}

func objectPrefixWithinScope(prefix, allowed string) bool {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	allowed = strings.Trim(strings.TrimSpace(allowed), "/")
	return allowed == "" || prefix == allowed || strings.HasPrefix(prefix, allowed+"/")
}

func validateObjectPrefix(prefix string) error {
	if strings.ContainsAny(prefix, "\\\x00") {
		return errors.New("object prefix cannot include backslashes or NUL bytes")
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == "." || segment == ".." {
			return errors.New("object prefix cannot include dot path segments")
		}
	}
	return nil
}

func validateAWSCredentialConfig(config AwsCredentialConfig) error {
	switch config.Source {
	case AwsCredentialConfigSourceDefault:
		if config.AccessKeyId != "" || config.SecretAccessKey != "" || config.SessionToken != "" ||
			config.Profile != "" || config.SharedCredentialsFile != "" || config.RoleArn != "" ||
			config.TokenFile != "" || config.SessionName != "" || config.StsEndpoint != "" {
			return errors.New("default AWS credential source cannot include static, profile, or web-identity fields")
		}
	case AwsCredentialConfigSourceStatic:
		if strings.TrimSpace(config.AccessKeyId) == "" || strings.TrimSpace(config.SecretAccessKey) == "" {
			return errors.New("static AWS credential source requires access_key_id and secret_access_key")
		}
		if config.Profile != "" || config.SharedCredentialsFile != "" || config.RoleArn != "" ||
			config.TokenFile != "" || config.SessionName != "" || config.StsEndpoint != "" {
			return errors.New("static AWS credential source cannot include profile or web-identity fields")
		}
	case AwsCredentialConfigSourceProfile:
		if strings.TrimSpace(config.Profile) == "" {
			return errors.New("profile AWS credential source requires profile")
		}
		if config.AccessKeyId != "" || config.SecretAccessKey != "" || config.SessionToken != "" ||
			config.RoleArn != "" || config.TokenFile != "" || config.SessionName != "" ||
			config.StsEndpoint != "" {
			return errors.New("profile AWS credential source cannot include static or web-identity fields")
		}
	case AwsCredentialConfigSourceWebIdentity:
		if strings.TrimSpace(config.RoleArn) == "" || strings.TrimSpace(config.TokenFile) == "" {
			return errors.New("web_identity AWS credential source requires role_arn and token_file")
		}
		if config.AccessKeyId != "" || config.SecretAccessKey != "" || config.SessionToken != "" ||
			config.Profile != "" || config.SharedCredentialsFile != "" {
			return errors.New("web_identity AWS credential source cannot include static or profile fields")
		}
	default:
		return fmt.Errorf("unsupported AWS credential source %q", config.Source)
	}
	return nil
}

func (c *Config) resolveS3Info(
	connectionID, requiredCapability, bucket, prefix string,
) (S3Info, error) {
	external, err := c.resolveExternalIOConnection(connectionID, requiredCapability)
	if err != nil {
		return S3Info{}, err
	}
	protocol, err := external.ExternalIo.Discriminator()
	if err != nil {
		return S3Info{}, fmt.Errorf("reading connection %q protocol: %w", connectionID, err)
	}
	if protocol != "s3" {
		return S3Info{}, fmt.Errorf("connection %q uses protocol %q, want s3", connectionID, protocol)
	}
	config, err := external.ExternalIo.AsS3ExternalIoConfig()
	if err != nil {
		return S3Info{}, fmt.Errorf("decoding S3 connection %q: %w", connectionID, err)
	}
	if err := validateObjectPrefix(config.Prefix); err != nil {
		return S3Info{}, fmt.Errorf("connection %q prefix: %w", connectionID, err)
	}
	if err := validateObjectPrefix(prefix); err != nil {
		return S3Info{}, fmt.Errorf("requested S3 prefix: %w", err)
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return S3Info{}, errors.New("S3 bucket is required")
	}
	bucketAllowed := false
	for _, allowed := range config.Buckets {
		if bucket == allowed {
			bucketAllowed = true
			break
		}
	}
	if !bucketAllowed {
		return S3Info{}, fmt.Errorf("connection %q does not allow bucket %q", connectionID, bucket)
	}
	if !objectPrefixWithinScope(prefix, config.Prefix) {
		return S3Info{}, fmt.Errorf("prefix %q is outside connection %q scope %q", prefix, connectionID, config.Prefix)
	}
	credentials := config.Credentials
	if credentials.Source == "" {
		credentials.Source = AwsCredentialConfigSourceDefault
	}
	if err := validateAWSCredentialConfig(credentials); err != nil {
		return S3Info{}, fmt.Errorf("connection %q credentials: %w", connectionID, err)
	}
	endpoint := strings.TrimSpace(config.Endpoint)
	useSSL := config.UseSsl
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
		useSSL = true
	}
	addressingStyle := config.AddressingStyle
	if addressingStyle == "" {
		addressingStyle = S3ExternalIoConfigAddressingStyleVirtualHosted
	}
	switch addressingStyle {
	case S3ExternalIoConfigAddressingStylePath,
		S3ExternalIoConfigAddressingStyleVirtualHosted:
	default:
		return S3Info{}, fmt.Errorf(
			"connection %q has unsupported S3 addressing_style %q",
			connectionID,
			addressingStyle,
		)
	}
	bucketProvisioning := config.BucketProvisioning
	if bucketProvisioning == "" {
		bucketProvisioning = S3ExternalIoConfigBucketProvisioningRequireExisting
	}
	switch bucketProvisioning {
	case S3ExternalIoConfigBucketProvisioningCreateIfMissing,
		S3ExternalIoConfigBucketProvisioningRequireExisting:
	default:
		return S3Info{}, fmt.Errorf(
			"connection %q has unsupported S3 bucket_provisioning %q",
			connectionID,
			bucketProvisioning,
		)
	}
	return S3Info{
		Endpoint:              endpoint,
		Bucket:                bucket,
		Prefix:                strings.Trim(strings.TrimSpace(prefix), "/"),
		Region:                strings.TrimSpace(config.Region),
		UseSsl:                useSSL,
		AddressingStyle:       addressingStyle,
		BucketProvisioning:    bucketProvisioning,
		CredentialSource:      credentials.Source,
		AccessKeyId:           credentials.AccessKeyId,
		SecretAccessKey:       credentials.SecretAccessKey,
		SessionToken:          credentials.SessionToken,
		Profile:               credentials.Profile,
		SharedCredentialsFile: credentials.SharedCredentialsFile,
		RoleArn:               credentials.RoleArn,
		TokenFile:             credentials.TokenFile,
		SessionName:           credentials.SessionName,
		StsEndpoint:           credentials.StsEndpoint,
		runtime:               c.s3Runtime(connectionID),
	}, nil
}

// ResolveS3Info authorizes one S3 location against a named connection.
func (c *Config) ResolveS3Info(connectionID, requiredCapability, location string) (S3Info, error) {
	bucket, prefix, err := ParseS3URL(location)
	if err != nil {
		return S3Info{}, err
	}
	return c.resolveS3Info(connectionID, requiredCapability, bucket, prefix)
}

func pathWithinFilesystemRoot(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil &&
		relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func resolvePathWithoutSymlinks(root, logical string) (string, error) {
	rootInfo, err := os.Stat(root)
	if err != nil {
		return "", fmt.Errorf("opening filesystem connection root: %w", err)
	}
	if !rootInfo.IsDir() {
		return "", errors.New("filesystem connection root must be a directory")
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolving filesystem connection root: %w", err)
	}

	candidate := canonicalRoot
	parts := strings.Split(filepath.Clean(logical), string(filepath.Separator))
	for i, part := range parts {
		if part == "" || part == "." {
			continue
		}
		next := filepath.Join(candidate, part)
		info, err := os.Lstat(next)
		if err != nil {
			if !os.IsNotExist(err) {
				return "", fmt.Errorf("inspecting filesystem path component %q: %w", part, err)
			}
			candidate = filepath.Join(candidate, filepath.Join(parts[i:]...))
			break
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("filesystem location cannot traverse symlink %q", next)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return "", fmt.Errorf("filesystem path component %q is not a directory", next)
		}
		candidate = next
	}
	candidate = filepath.Clean(candidate)
	if !pathWithinFilesystemRoot(canonicalRoot, candidate) {
		return "", errors.New("filesystem location escapes connection root")
	}
	return candidate, nil
}

func (c *Config) filesystemConnectionRootAndLogicalPath(
	connectionID, requiredCapability, location string,
) (string, string, error) {
	external, err := c.resolveExternalIOConnection(connectionID, requiredCapability)
	if err != nil {
		return "", "", err
	}
	protocol, err := external.ExternalIo.Discriminator()
	if err != nil {
		return "", "", fmt.Errorf("reading connection %q protocol: %w", connectionID, err)
	}
	if protocol != "filesystem" {
		return "", "", fmt.Errorf(
			"connection %q uses protocol %q, want filesystem",
			connectionID,
			protocol,
		)
	}
	config, err := external.ExternalIo.AsFilesystemExternalIoConfig()
	if err != nil {
		return "", "", fmt.Errorf("decoding filesystem connection %q: %w", connectionID, err)
	}
	rootConfig := strings.TrimSpace(config.Root)
	if !filepath.IsAbs(rootConfig) {
		return "", "", fmt.Errorf(
			"filesystem connection %q root must be absolute",
			connectionID,
		)
	}
	root := filepath.Clean(rootConfig)
	uri, err := url.Parse(location)
	if err != nil {
		return "", "", fmt.Errorf("parsing filesystem location: %w", err)
	}
	if uri.Scheme != "file" {
		return "", "", fmt.Errorf("expected file:// scheme, got %q", uri.Scheme)
	}
	if uri.Host != "" && uri.Host != "localhost" {
		return "", "", fmt.Errorf("filesystem location host %q is not local", uri.Host)
	}
	if uri.RawQuery != "" || uri.Fragment != "" {
		return "", "", errors.New(
			"filesystem location cannot include a query or fragment",
		)
	}
	logical := filepath.FromSlash(strings.TrimPrefix(uri.Path, "/"))
	if logical == "" || logical == "." {
		return root, ".", nil
	}
	if filepath.IsAbs(logical) {
		return "", "", errors.New(
			"filesystem location must be relative to the connection root",
		)
	}
	cleanLogical := filepath.Clean(logical)
	if cleanLogical == ".." || strings.HasPrefix(cleanLogical, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf(
			"filesystem location %q escapes connection %q root",
			location,
			connectionID,
		)
	}
	return root, cleanLogical, nil
}

// ResolveFilesystemPath authorizes a logical file URI against a named
// filesystem connection and resolves it beneath the administrator-owned root.
func (c *Config) ResolveFilesystemPath(
	connectionID, requiredCapability, location string,
) (string, error) {
	root, cleanLogical, err := c.filesystemConnectionRootAndLogicalPath(
		connectionID,
		requiredCapability,
		location,
	)
	if err != nil {
		return "", err
	}
	resolved, err := resolvePathWithoutSymlinks(root, cleanLogical)
	if err != nil {
		return "", fmt.Errorf("resolving filesystem location for connection %q: %w", connectionID, err)
	}
	return resolved, nil
}

// OpenFilesystemPath authorizes and opens a logical file URI beneath a named
// filesystem connection. Both the connection root and logical subdirectory are
// held by descriptors, so later renames or symlink swaps cannot redirect
// operations outside the administrator-owned root.
func (c *Config) OpenFilesystemPath(
	connectionID, requiredCapability, location string,
) (*os.Root, error) {
	rootPath, cleanLogical, err := c.filesystemConnectionRootAndLogicalPath(
		connectionID,
		requiredCapability,
		location,
	)
	if err != nil {
		return nil, err
	}
	connectionRoot, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf(
			"opening filesystem connection %q root: %w",
			connectionID,
			err,
		)
	}
	defer func() { _ = connectionRoot.Close() }()
	logicalRoot, err := connectionRoot.OpenRoot(cleanLogical)
	if err != nil {
		return nil, fmt.Errorf(
			"opening filesystem location for connection %q: %w",
			connectionID,
			err,
		)
	}
	return logicalRoot, nil
}

// OpenOrCreateFilesystemPath authorizes and creates a logical file URI beneath
// a named filesystem connection. Creation and the final open are both rooted
// at the administrator-owned connection descriptor, so a concurrent rename or
// symlink swap cannot redirect either operation outside that root.
func (c *Config) OpenOrCreateFilesystemPath(
	connectionID, requiredCapability, location string,
	perm os.FileMode,
) (*os.Root, error) {
	rootPath, cleanLogical, err := c.filesystemConnectionRootAndLogicalPath(
		connectionID,
		requiredCapability,
		location,
	)
	if err != nil {
		return nil, err
	}
	connectionRoot, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf(
			"opening filesystem connection %q root: %w",
			connectionID,
			err,
		)
	}
	defer func() { _ = connectionRoot.Close() }()
	if err := connectionRoot.MkdirAll(cleanLogical, perm); err != nil {
		return nil, fmt.Errorf(
			"creating filesystem location for connection %q: %w",
			connectionID,
			err,
		)
	}
	logicalRoot, err := connectionRoot.OpenRoot(cleanLogical)
	if err != nil {
		return nil, fmt.Errorf(
			"opening filesystem location for connection %q: %w",
			connectionID,
			err,
		)
	}
	return logicalRoot, nil
}

// ResolveObjectStorageS3 resolves one object-engine durability lane. An empty
// lane resolves the root location.
func (c *Config) ResolveObjectStorageS3(lane string) (S3Info, error) {
	if c == nil {
		return S3Info{}, errors.New("config is required")
	}
	location := ObjectStorageLocation{}
	switch lane {
	case "":
	case "artifacts":
		location = c.Storage.Object.Lanes.Artifacts
	case "manifests":
		location = c.Storage.Object.Lanes.Manifests
	case "wal":
		location = c.Storage.Object.Lanes.Wal
	case "progress":
		location = c.Storage.Object.Lanes.Progress
	case "catalog":
		location = c.Storage.Object.Lanes.Catalog
	default:
		return S3Info{}, fmt.Errorf("unsupported object storage lane %q", lane)
	}
	connection := c.Storage.Object.Connection
	bucket := c.Storage.Object.Bucket
	prefix := c.Storage.Object.Prefix
	if location.Connection != "" {
		connection = location.Connection
	}
	if location.Bucket != "" {
		bucket = location.Bucket
	}
	if location.Prefix != "" {
		prefix = location.Prefix
	}
	return c.resolveS3Info(connection, "storage.primary", bucket, prefix)
}

// GetS3Credentials constructs a refreshable provider for the configured
// credential identity. The provider is retained by the per-connection client.
func (s *S3Info) GetS3Credentials() (*miniocredentials.Credentials, error) {
	switch s.CredentialSource {
	case AwsCredentialConfigSourceDefault:
		return miniocredentials.NewChainCredentials([]miniocredentials.Provider{
			&miniocredentials.EnvAWS{},
			&miniocredentials.FileAWSCredentials{},
			&miniocredentials.IAM{Region: s.Region},
		}), nil
	case AwsCredentialConfigSourceStatic:
		if s.AccessKeyId == "" || s.SecretAccessKey == "" {
			return nil, errors.New("static S3 credentials require access_key_id and secret_access_key")
		}
		return miniocredentials.NewStaticV4(
			s.AccessKeyId,
			s.SecretAccessKey,
			s.SessionToken,
		), nil
	case AwsCredentialConfigSourceProfile:
		if strings.TrimSpace(s.Profile) == "" {
			return nil, errors.New("profile S3 credentials require profile")
		}
		return miniocredentials.NewFileAWSCredentials(
			s.SharedCredentialsFile,
			s.Profile,
		), nil
	case AwsCredentialConfigSourceWebIdentity:
		if strings.TrimSpace(s.RoleArn) == "" || strings.TrimSpace(s.TokenFile) == "" {
			return nil, errors.New("web_identity S3 credentials require role_arn and token_file")
		}
		sessionName := strings.TrimSpace(s.SessionName)
		if sessionName == "" {
			sessionName = "antfly"
		}
		provider := &miniocredentials.IAM{
			Endpoint: strings.TrimSpace(s.StsEndpoint),
			Region:   strings.TrimSpace(s.Region),
		}
		provider.EKSIdentity.TokenFile = s.TokenFile
		provider.EKSIdentity.RoleARN = s.RoleArn
		provider.EKSIdentity.RoleSessionName = sessionName
		return miniocredentials.New(provider), nil
	default:
		return nil, fmt.Errorf("unsupported S3 credential source %q", s.CredentialSource)
	}
}

func normalizeMinioEndpoint(endpoint string, secure bool) (string, bool, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", false, errors.New("S3 endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		if strings.ContainsAny(endpoint, "/?#") {
			return "", false, errors.New("S3 endpoint must not include a path, query, or fragment")
		}
		return endpoint, secure, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", false, fmt.Errorf("parsing S3 endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", false, fmt.Errorf("unsupported S3 endpoint scheme %q", parsed.Scheme)
	}
	if parsed.User != nil || parsed.Host == "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false, errors.New("S3 endpoint must contain only a scheme and host")
	}
	return parsed.Host, parsed.Scheme == "https", nil
}

func (s *S3Info) clientFingerprint() [sha256.Size]byte {
	values := []string{
		s.Endpoint,
		s.Region,
		fmt.Sprintf("%t", s.UseSsl),
		string(s.AddressingStyle),
		string(s.CredentialSource),
		s.AccessKeyId,
		s.SecretAccessKey,
		s.SessionToken,
		s.Profile,
		s.SharedCredentialsFile,
		s.RoleArn,
		s.TokenFile,
		s.SessionName,
		s.StsEndpoint,
	}
	return sha256.Sum256([]byte(strings.Join(values, "\x00")))
}

func (s *S3Info) newMinioClient() (*minio.Client, error) {
	credentialProvider, err := s.GetS3Credentials()
	if err != nil {
		return nil, err
	}
	endpoint, secure, err := normalizeMinioEndpoint(s.Endpoint, s.UseSsl)
	if err != nil {
		return nil, err
	}
	bucketLookup := minio.BucketLookupDNS
	if s.AddressingStyle == S3ExternalIoConfigAddressingStylePath {
		bucketLookup = minio.BucketLookupPath
	}
	return minio.New(endpoint, &minio.Options{
		Creds:        credentialProvider,
		Secure:       secure,
		Region:       s.Region,
		BucketLookup: bucketLookup,
	})
}

// NewMinioClient returns the connection-owned client. Reusing the client keeps
// credential refresh, endpoint discovery, and HTTP transport state bounded to
// one runtime per named connection.
func (s *S3Info) NewMinioClient() (*minio.Client, error) {
	if s == nil {
		return nil, errors.New("S3 configuration is required")
	}
	if s.runtime == nil {
		return s.newMinioClient()
	}
	fingerprint := s.clientFingerprint()
	s.runtime.mu.Lock()
	defer s.runtime.mu.Unlock()
	if s.runtime.initialized && s.runtime.fingerprint == fingerprint {
		return s.runtime.client, nil
	}
	client, err := s.newMinioClient()
	if err != nil {
		return nil, err
	}
	s.runtime.client = client
	s.runtime.fingerprint = fingerprint
	s.runtime.initialized = true
	return client, nil
}

func (s *S3Info) bucketAdmission() *s3BucketAdmission {
	if s.runtime == nil {
		return &s3BucketAdmission{}
	}
	key := fmt.Sprintf("%x\x00%s", s.clientFingerprint(), s.Bucket)
	admission, _ := s.runtime.buckets.LoadOrStore(key, &s3BucketAdmission{})
	return admission.(*s3BucketAdmission)
}

// EnsureBucket verifies write admission and applies the configured provisioning
// policy once per connection/bucket runtime. Concurrent creators are serialized
// and resolved by a final existence check.
func (s *S3Info) EnsureBucket(ctx context.Context) (*minio.Client, error) {
	client, err := s.NewMinioClient()
	if err != nil {
		return nil, err
	}
	admission := s.bucketAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.ready {
		return client, nil
	}
	exists, err := client.BucketExists(ctx, s.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket %s exists: %w", s.Bucket, err)
	}
	if exists {
		admission.ready = true
		return client, nil
	}
	if s.BucketProvisioning != S3ExternalIoConfigBucketProvisioningCreateIfMissing {
		return nil, fmt.Errorf("bucket %s does not exist", s.Bucket)
	}
	if err := client.MakeBucket(ctx, s.Bucket, minio.MakeBucketOptions{Region: s.Region}); err != nil {
		exists, existsErr := client.BucketExists(ctx, s.Bucket)
		if existsErr != nil {
			return nil, fmt.Errorf(
				"creating bucket %s: %w (rechecking existence: %v)",
				s.Bucket,
				err,
				existsErr,
			)
		}
		if !exists {
			return nil, fmt.Errorf("creating bucket %s: %w", s.Bucket, err)
		}
	}
	admission.ready = true
	return client, nil
}

// IsS3CreateConflict reports the two conditional-create conflict shapes used
// by AWS S3 and compatible implementations.
func IsS3CreateConflict(err error) bool {
	if err == nil {
		return false
	}
	code := minio.ToErrorResponse(err).Code
	return code == minio.PreconditionFailed || code == "ConditionalRequestConflict"
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
	if u.User != nil || u.Port() != "" {
		return "", "", errors.New("S3 URL cannot include user information or a port")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("S3 URL cannot include a query or fragment")
	}

	bucket = u.Host
	if bucket == "" {
		return "", "", errors.New("bucket name is required in S3 URL")
	}

	// Path starts with /, so trim the leading slash
	prefix = strings.TrimPrefix(u.Path, "/")
	if err := validateObjectPrefix(prefix); err != nil {
		return "", "", fmt.Errorf("invalid S3 prefix: %w", err)
	}

	return bucket, prefix, nil
}
