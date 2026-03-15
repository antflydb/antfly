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
	"os"
	"path/filepath"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/pkg/libaf/logging"
	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// JSONStructTag returns a viper.DecoderConfigOption that uses json tags instead of mapstructure tags
func JSONStructTag() viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		c.TagName = "json"
	}
}

// TestConfigUnmarshalFromYAML tests that Viper can unmarshal YAML into Config struct
func TestConfigUnmarshalFromYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		validate func(t *testing.T, cfg *Config)
		wantErr  bool
	}{
		{
			name: "minimal valid config",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
  data: "local"
  metadata: "local"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "0.0.1", cfg.Version)
				assert.Equal(t, 4200, cfg.HealthPort)
				assert.Equal(t, "antflydb", cfg.Storage.Local.BaseDir)
				assert.Equal(t, StorageBackendLocal, cfg.Storage.Data)
				assert.Equal(t, StorageBackendLocal, cfg.Storage.Metadata)
				assert.Len(t, cfg.Metadata.OrchestrationUrls, 1)
				assert.Equal(t, uint64(1), cfg.ReplicationFactor)
				assert.Equal(t, uint64(1073741824), cfg.MaxShardSizeBytes)
				assert.Equal(t, uint64(100), cfg.MaxShardsPerTable)
				assert.Equal(t, uint64(4), cfg.DefaultShardsPerTable)
			},
		},
		{
			name: "config with TLS",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
tls:
  cert: "/path/to/cert.pem"
  key: "/path/to/key.pem"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Tls)
				assert.Equal(t, "/path/to/cert.pem", cfg.Tls.Cert)
				assert.Equal(t, "/path/to/key.pem", cfg.Tls.Key)
			},
		},
		{
			name: "config with S3 storage",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
  data: "s3"
  metadata: "local"
  s3:
    endpoint: "s3.amazonaws.com"
    bucket: "my-antfly-bucket"
    prefix: "production/"
    use_ssl: true
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
replication_factor: 3
max_shard_size_bytes: 10737418240
max_shards_per_table: 100
default_shards_per_table: 8
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, StorageBackendS3, cfg.Storage.Data)
				assert.Equal(t, StorageBackendLocal, cfg.Storage.Metadata)
				assert.Equal(t, "s3.amazonaws.com", cfg.Storage.S3.Endpoint)
				assert.Equal(t, "my-antfly-bucket", cfg.Storage.S3.Bucket)
				assert.Equal(t, "production/", cfg.Storage.S3.Prefix)
				assert.True(t, cfg.Storage.S3.UseSsl)
			},
		},
		{
			name: "config with CORS",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
cors:
  enabled: true
  allowed_origins: ["https://example.com", "https://app.example.com"]
  allowed_methods: ["GET", "POST", "PUT", "DELETE"]
  allowed_headers: ["Content-Type", "Authorization"]
  allow_credentials: true
  max_age: 7200
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Cors)
				assert.True(t, cfg.Cors.Enabled)
				assert.Equal(t, []string{"https://example.com", "https://app.example.com"}, cfg.Cors.AllowedOrigins)
				assert.Equal(t, []string{"GET", "POST", "PUT", "DELETE"}, cfg.Cors.AllowedMethods)
				assert.Equal(t, []string{"Content-Type", "Authorization"}, cfg.Cors.AllowedHeaders)
				assert.True(t, cfg.Cors.AllowCredentials)
				assert.Equal(t, 7200, cfg.Cors.MaxAge)
			},
		},
		{
			name: "config with remote content",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
remote_content:
  security:
    allowed_hosts: ["example.com", "cdn.example.com"]
    block_private_ips: true
    max_download_size_bytes: 104857600
    download_timeout_seconds: 30
    max_image_dimension: 2048
  default_s3: "primary"
  s3:
    primary:
      endpoint: "s3.amazonaws.com"
      access_key_id: "test-key"
      secret_access_key: "test-secret"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, []string{"example.com", "cdn.example.com"}, cfg.RemoteContent.Security.AllowedHosts)
				assert.True(t, cfg.RemoteContent.Security.BlockPrivateIps)
				assert.Equal(t, int64(104857600), cfg.RemoteContent.Security.MaxDownloadSizeBytes)
				assert.Equal(t, 30, cfg.RemoteContent.Security.DownloadTimeoutSeconds)
				assert.Equal(t, 2048, cfg.RemoteContent.Security.MaxImageDimension)
				assert.Equal(t, "primary", cfg.RemoteContent.DefaultS3)
				assert.Contains(t, cfg.RemoteContent.S3, "primary")
				assert.Equal(t, "s3.amazonaws.com", cfg.RemoteContent.S3["primary"].Endpoint)
			},
		},
		{
			name: "config with logging",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
log:
  level: "debug"
  style: "json"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, logging.Level("debug"), cfg.Log.Level)
				assert.Equal(t, logging.Style("json"), cfg.Log.Style)
			},
		},
		{
			name: "config with termite",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
termite:
  api_url: "http://localhost:8080"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Termite)
				assert.Equal(t, "http://localhost:8080", cfg.Termite.ApiUrl)
			},
		},
		{
			name: "config with swarm mode and flags",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
swarm_mode: true
enable_auth: true
disable_shard_alloc: true
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.True(t, cfg.SwarmMode)
				assert.True(t, cfg.EnableAuth)
				assert.True(t, cfg.DisableShardAlloc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file with the YAML content
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(tt.yaml)
			require.NoError(t, err)
			require.NoError(t, tmpFile.Close())

			// Use Viper to read the config
			v := viper.New()
			v.SetConfigFile(tmpFile.Name())
			err = v.ReadInConfig()
			require.NoError(t, err)

			// Unmarshal into Config struct using JSON tags
			var cfg Config
			err = v.Unmarshal(&cfg, JSONStructTag())

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			tt.validate(t, &cfg)
		})
	}
}

// TestValidateMetadata tests the metadata validation logic
func TestValidateMetadata(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid metadata with single URL",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "http://localhost:5001",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid metadata with multiple URLs",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "http://localhost:5001",
						"2": "http://localhost:5002",
						"3": "http://localhost:5003",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing metadata",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: nil,
				},
			},
			wantErr: true,
			errMsg:  "at least one orchestration URL is required",
		},
		{
			name: "empty orchestration URLs",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{},
				},
			},
			wantErr: true,
			errMsg:  "at least one orchestration URL is required",
		},
		{
			name: "empty URL value",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "",
					},
				},
			},
			wantErr: true,
			errMsg:  "orchestration URL at 1 cannot be empty",
		},
		{
			name: "invalid URL format",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "not-a-url",
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid orchestration URL",
		},
		{
			name: "duplicate URLs",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "http://localhost:5001",
						"2": "http://localhost:5001",
					},
				},
			},
			wantErr: true,
			errMsg:  "duplicate orchestration URL",
		},
		{
			name: "unsupported URL scheme",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "ftp://localhost:5001",
					},
				},
			},
			wantErr: true,
			errMsg:  "unsupported URL scheme",
		},
		{
			name: "invalid node ID",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"invalid-id": "http://localhost:5001",
					},
				},
			},
			wantErr: true,
			errMsg:  "invalid metadata node ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validateMetadata()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateTLS tests the TLS validation logic
func TestValidateTLS(t *testing.T) {
	// Create temporary cert and key files for testing
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	err := os.WriteFile(certFile, []byte("fake cert"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(keyFile, []byte("fake key"), 0644)
	require.NoError(t, err)

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty TLS info (valid - TLS optional)",
			config: &Config{
				Tls: TLSInfo{},
			},
			wantErr: false,
		},
		{
			name: "empty TLS info (valid - TLS disabled)",
			config: &Config{
				Tls: TLSInfo{
					Cert: "",
					Key:  "",
				},
			},
			wantErr: false,
		},
		{
			name: "valid TLS with cert and key",
			config: &Config{
				Tls: TLSInfo{
					Cert: certFile,
					Key:  keyFile,
				},
			},
			wantErr: false,
		},
		{
			name: "missing cert",
			config: &Config{
				Tls: TLSInfo{
					Cert: "",
					Key:  keyFile,
				},
			},
			wantErr: true,
			errMsg:  "TLS certificate path is required when TLS is enabled",
		},
		{
			name: "missing key",
			config: &Config{
				Tls: TLSInfo{
					Cert: certFile,
					Key:  "",
				},
			},
			wantErr: true,
			errMsg:  "TLS key path is required when TLS is enabled",
		},
		{
			name: "cert file does not exist",
			config: &Config{
				Tls: TLSInfo{
					Cert: "/nonexistent/cert.pem",
					Key:  keyFile,
				},
			},
			wantErr: true,
			errMsg:  "TLS certificate file validation failed",
		},
		{
			name: "key file does not exist",
			config: &Config{
				Tls: TLSInfo{
					Cert: certFile,
					Key:  "/nonexistent/key.pem",
				},
			},
			wantErr: true,
			errMsg:  "TLS key file validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validateTLS()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateStorage tests the storage validation logic
func TestValidateStorage(t *testing.T) {
	// Set up environment variables for S3 tests
	originalAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	originalSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	defer func() {
		os.Setenv("AWS_ACCESS_KEY_ID", originalAccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", originalSecretKey)
	}()

	tests := []struct {
		name       string
		config     *Config
		setupEnv   func()
		cleanupEnv func()
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid local storage",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendLocal,
					Metadata: StorageBackendLocal,
				},
			},
			wantErr: false,
		},
		{
			name: "empty base directory",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "",
					},
					Data:     StorageBackendLocal,
					Metadata: StorageBackendLocal,
				},
			},
			wantErr: true,
			errMsg:  "storage.local.base_dir is required",
		},
		{
			name: "invalid keyvalue backend",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     "invalid",
					Metadata: StorageBackendLocal,
				},
			},
			wantErr: true,
			errMsg:  "storage.data must be 'local' or 's3'",
		},
		{
			name: "invalid metadatakv backend",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendLocal,
					Metadata: "invalid",
				},
			},
			wantErr: true,
			errMsg:  "storage.metadata must be 'local' or 's3'",
		},
		{
			name: "S3 keyvalue without S3 config",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3:       S3Info{},
				},
			},
			wantErr: true,
			errMsg:  "storage.s3.endpoint is required when using S3 storage",
		},
		{
			name: "S3 with valid config and credentials",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3: S3Info{
						Endpoint: "s3.amazonaws.com",
						Bucket:   "my-bucket",
						UseSsl:   true,
					},
				},
			},
			setupEnv: func() {
				os.Setenv("AWS_ACCESS_KEY_ID", "test-key")
				os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			},
			wantErr: false,
		},
		{
			name: "S3 without credentials",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3: S3Info{
						Endpoint: "s3.amazonaws.com",
						Bucket:   "my-bucket",
						UseSsl:   true,
					},
				},
			},
			setupEnv: func() {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			},
			wantErr: true,
			errMsg:  "S3 credentials required",
		},
		{
			name: "S3 with empty endpoint",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3: S3Info{
						Endpoint: "",
						Bucket:   "my-bucket",
					},
				},
			},
			setupEnv: func() {
				os.Setenv("AWS_ACCESS_KEY_ID", "test-key")
				os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			},
			wantErr: true,
			errMsg:  "storage.s3.endpoint is required when using S3 storage",
		},
		{
			name: "S3 with empty bucket",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3: S3Info{
						Endpoint: "s3.amazonaws.com",
						Bucket:   "",
					},
				},
			},
			setupEnv: func() {
				os.Setenv("AWS_ACCESS_KEY_ID", "test-key")
				os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			},
			wantErr: true,
			errMsg:  "storage.s3.bucket is required when using S3 storage",
		},
		{
			name: "S3 with bucket name too short",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendS3,
					Metadata: StorageBackendLocal,
					S3: S3Info{
						Endpoint: "s3.amazonaws.com",
						Bucket:   "ab",
					},
				},
			},
			setupEnv: func() {
				os.Setenv("AWS_ACCESS_KEY_ID", "test-key")
				os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_ACCESS_KEY_ID")
				os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			},
			wantErr: true,
			errMsg:  "S3 bucket name must be between 3 and 63 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupEnv != nil {
				tt.setupEnv()
			}
			if tt.cleanupEnv != nil {
				defer tt.cleanupEnv()
			}

			err := tt.config.validateStorage()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateMaxShardSizeBytes tests the max shard size validation
func TestValidateMaxShardSizeBytes(t *testing.T) {
	tests := []struct {
		name    string
		size    uint64
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid size (1GB)",
			size:    1073741824,
			wantErr: false,
		},
		{
			name:    "valid size (10GB)",
			size:    10737418240,
			wantErr: false,
		},
		{
			name:    "zero size",
			size:    0,
			wantErr: true,
			errMsg:  "max_shard_size_bytes must be greater than 0",
		},
		{
			name:    "too small (less than 1MB)",
			size:    1024,
			wantErr: true,
			errMsg:  "max_shard_size_bytes must be at least",
		},
		{
			name:    "too large (more than 42TB)",
			size:    50 * 1024 * 1024 * 1024 * 1024,
			wantErr: true,
			errMsg:  "max_shard_size_bytes must be at most",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				MaxShardSizeBytes: tt.size,
			}

			err := cfg.validateMaxShardSizeBytes()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestValidateReplicationFactor tests the replication factor validation
func TestValidateReplicationFactor(t *testing.T) {
	tests := []struct {
		name    string
		factor  uint64
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid factor 1",
			factor:  1,
			wantErr: false,
		},
		{
			name:    "valid factor 3",
			factor:  3,
			wantErr: false,
		},
		{
			name:    "valid factor 5",
			factor:  5,
			wantErr: false,
		},
		{
			name:    "zero factor",
			factor:  0,
			wantErr: true,
			errMsg:  "replication_factor must be at least 1",
		},
		{
			name:    "too large factor",
			factor:  6,
			wantErr: true,
			errMsg:  "replication_factor must be at most 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				ReplicationFactor: tt.factor,
			}

			err := cfg.validateReplicationFactor()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestConfigValidate tests the full Validate method
func TestConfigValidate(t *testing.T) {
	// Create temporary cert and key files
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	os.WriteFile(certFile, []byte("fake cert"), 0644)
	os.WriteFile(keyFile, []byte("fake key"), 0644)

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "fully valid config",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "http://localhost:5001",
					},
				},
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
					Data:     StorageBackendLocal,
					Metadata: StorageBackendLocal,
				},
				ReplicationFactor:     3,
				MaxShardSizeBytes:     1073741824,
				DefaultShardsPerTable: 4,
				MaxShardsPerTable:     100,
			},
			wantErr: false,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
			errMsg:  "config cannot be nil",
		},
		{
			name: "missing metadata",
			config: &Config{
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
				},
				ReplicationFactor:     1,
				MaxShardSizeBytes:     1073741824,
				DefaultShardsPerTable: 4,
			},
			wantErr: true,
			errMsg:  "metadata config validation failed",
		},
		{
			name: "zero default_shards_per_table",
			config: &Config{
				Metadata: MetadataInfo{
					OrchestrationUrls: map[string]string{
						"1": "http://localhost:5001",
					},
				},
				Storage: StorageConfig{
					Local: LocalStorageConfig{
						BaseDir: "antflydb",
					},
				},
				ReplicationFactor:     1,
				MaxShardSizeBytes:     1073741824,
				DefaultShardsPerTable: 0,
			},
			wantErr: true,
			errMsg:  "default_shards_per_table must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestHelperMethods tests the config helper methods
func TestHelperMethods(t *testing.T) {
	t.Run("GetBaseDir", func(t *testing.T) {
		tests := []struct {
			name     string
			config   *Config
			expected string
		}{
			{
				name:     "nil config returns default",
				config:   nil,
				expected: DefaultDataDir(),
			},
			{
				name: "empty base_dir returns default",
				config: &Config{
					Storage: StorageConfig{
						Local: LocalStorageConfig{
							BaseDir: "",
						},
					},
				},
				expected: DefaultDataDir(),
			},
			{
				name: "custom base_dir",
				config: &Config{
					Storage: StorageConfig{
						Local: LocalStorageConfig{
							BaseDir: "/custom/path",
						},
					},
				},
				expected: "/custom/path",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.config.GetBaseDir()
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("GetKeyValueStorageType", func(t *testing.T) {
		tests := []struct {
			name     string
			config   *Config
			expected string
		}{
			{
				name:     "nil config returns local",
				config:   nil,
				expected: "local",
			},
			{
				name: "empty data returns local",
				config: &Config{
					Storage: StorageConfig{
						Data: "",
					},
				},
				expected: "local",
			},
			{
				name: "s3 data",
				config: &Config{
					Storage: StorageConfig{
						Data: StorageBackendS3,
					},
				},
				expected: "s3",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.config.GetKeyValueStorageType()
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("GetMetadataStorageType", func(t *testing.T) {
		tests := []struct {
			name     string
			config   *Config
			expected string
		}{
			{
				name:     "nil config returns local",
				config:   nil,
				expected: "local",
			},
			{
				name: "empty metadata returns local",
				config: &Config{
					Storage: StorageConfig{
						Metadata: "",
					},
				},
				expected: "local",
			},
			{
				name: "s3 metadata",
				config: &Config{
					Storage: StorageConfig{
						Metadata: StorageBackendS3,
					},
				},
				expected: "s3",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := tt.config.GetMetadataStorageType()
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("GetOrchestrationURLs", func(t *testing.T) {
		t.Run("valid URLs with caching", func(t *testing.T) {
			meta := &MetadataInfo{
				OrchestrationUrls: map[string]string{
					"1": "http://localhost:5001",
					"2": "http://localhost:5002",
				},
			}

			// First call should parse
			urls1, err := meta.GetOrchestrationURLs()
			require.NoError(t, err)
			assert.Len(t, urls1, 2)

			// Second call should return cached result
			urls2, err := meta.GetOrchestrationURLs()
			require.NoError(t, err)
			assert.Equal(t, urls1, urls2)

			// Verify the IDs were parsed correctly
			id1, err := types.IDFromString("1")
			require.NoError(t, err)
			assert.Equal(t, "http://localhost:5001", urls1[id1])

			id2, err := types.IDFromString("2")
			require.NoError(t, err)
			assert.Equal(t, "http://localhost:5002", urls1[id2])
		})

		t.Run("invalid ID format", func(t *testing.T) {
			meta := &MetadataInfo{
				OrchestrationUrls: map[string]string{
					"invalid-id": "http://localhost:5001",
				},
			}

			_, err := meta.GetOrchestrationURLs()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid metadata node ID")
		})
	})
}

// TestParseS3URL tests the S3 URL parsing function
func TestParseS3URL(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedBucket string
		expectedPrefix string
		shouldError    bool
		errContains    string
	}{
		{
			name:           "bucket only",
			url:            "s3://my-bucket",
			expectedBucket: "my-bucket",
			expectedPrefix: "",
			shouldError:    false,
		},
		{
			name:           "bucket with trailing slash",
			url:            "s3://my-bucket/",
			expectedBucket: "my-bucket",
			expectedPrefix: "",
			shouldError:    false,
		},
		{
			name:           "bucket with single path segment",
			url:            "s3://my-bucket/prefix",
			expectedBucket: "my-bucket",
			expectedPrefix: "prefix",
			shouldError:    false,
		},
		{
			name:           "bucket with path and trailing slash",
			url:            "s3://my-bucket/prefix/",
			expectedBucket: "my-bucket",
			expectedPrefix: "prefix/",
			shouldError:    false,
		},
		{
			name:           "bucket with nested path",
			url:            "s3://my-bucket/path/to/backups/",
			expectedBucket: "my-bucket",
			expectedPrefix: "path/to/backups/",
			shouldError:    false,
		},
		{
			name:           "production case - GCS bucket with namespace prefix",
			url:            "s3://antfly-backups-production/antflydb-usc1-001/",
			expectedBucket: "antfly-backups-production",
			expectedPrefix: "antflydb-usc1-001/",
			shouldError:    false,
		},
		{
			name:           "bucket with complex path",
			url:            "s3://my-bucket/env/prod/cluster-01/backups",
			expectedBucket: "my-bucket",
			expectedPrefix: "env/prod/cluster-01/backups",
			shouldError:    false,
		},
		{
			name:        "invalid - missing bucket",
			url:         "s3://",
			shouldError: true,
			errContains: "bucket name is required",
		},
		{
			name:        "invalid - not s3 scheme",
			url:         "gs://my-bucket/prefix",
			shouldError: true,
			errContains: "expected s3:// scheme",
		},
		{
			name:        "invalid - http scheme",
			url:         "http://my-bucket/prefix",
			shouldError: true,
			errContains: "expected s3:// scheme",
		},
		{
			name:        "invalid - empty string",
			url:         "",
			shouldError: true,
			errContains: "expected s3:// scheme",
		},
		{
			name:        "invalid - no scheme",
			url:         "my-bucket/prefix",
			shouldError: true,
			errContains: "expected s3:// scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := ParseS3URL(tt.url)

			if tt.shouldError {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedBucket, bucket, "bucket mismatch")
			assert.Equal(t, tt.expectedPrefix, prefix, "prefix mismatch")
		})
	}
}
