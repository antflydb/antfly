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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/libaf/logging"
	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/minio/minio-go/v7"
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

func mustConnectionConfig(t *testing.T, encoded string) ConnectionConfig {
	t.Helper()
	var connection ConnectionConfig
	require.NoError(t, connection.UnmarshalJSON([]byte(encoded)))
	return connection
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
  engine: "local"
  local:
    base_dir: "antflydb"
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
			name: "config with object storage",
			yaml: `
version: "0.0.1"
health_port: 4200
deployment_mode: "serverless"
connections:
  primary:
    kind: "external_io"
    capabilities: ["storage.primary"]
    external_io:
      protocol: "s3"
      buckets: ["my-antfly-bucket"]
      prefix: "production"
      credentials:
        source: "default"
storage:
  engine: "object"
  object:
    connection: "primary"
    bucket: "my-antfly-bucket"
    prefix: "production/cluster"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
replication_factor: 3
max_shard_size_bytes: 10737418240
max_shards_per_table: 100
default_shards_per_table: 8
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, StorageEngineObject, cfg.Storage.Engine)
				assert.Equal(t, "primary", cfg.Storage.Object.Connection)
				assert.Equal(t, "my-antfly-bucket", cfg.Storage.Object.Bucket)
				assert.Equal(t, "production/cluster", cfg.Storage.Object.Prefix)
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
			name: "config with inference",
			yaml: `
version: "0.0.1"
health_port: 4200
storage:
  local:
    base_dir: "antflydb"
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
inference:
  api_url: "http://localhost:8080"
replication_factor: 1
max_shard_size_bytes: 1073741824
max_shards_per_table: 100
default_shards_per_table: 4
`,
			validate: func(t *testing.T, cfg *Config) {
				require.NotNil(t, cfg.Inference)
				assert.Equal(t, "http://localhost:8080", cfg.Inference.ApiUrl)
			},
		},
		{
			name: "config with standalone mode and flags",
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
deployment_mode: standalone
enable_auth: true
disable_shard_alloc: true
`,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, ConfigDeploymentModeStandalone, cfg.DeploymentMode)
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
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid local storage",
			config: &Config{
				Storage: StorageConfig{Engine: StorageEngineLocal, Local: LocalStorageConfig{BaseDir: "antflydb"}},
			},
			wantErr: false,
		},
		{
			name: "valid standalone Lite storage",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeStandalone,
				Storage: StorageConfig{
					Engine: StorageEngineLite,
					Lite:   LiteStorageConfig{Path: "data.antfly.aflite"},
				},
				ReplicationFactor:     1,
				DefaultShardsPerTable: 1,
			},
			wantErr: false,
		},
		{
			name: "Lite rejected for distributed topology",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeDistributed,
				Storage:        StorageConfig{Engine: StorageEngineLite, Lite: LiteStorageConfig{Path: "data.aflite"}},
			},
			wantErr: true,
			errMsg:  "requires deployment_mode standalone or embedded",
		},
		{
			name: "Lite rejects mixed local member",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeStandalone,
				Storage: StorageConfig{
					Engine: StorageEngineLite,
					Lite:   LiteStorageConfig{Path: "data.aflite"},
					Local:  LocalStorageConfig{BaseDir: "antflydb"},
				},
			},
			wantErr: true,
			errMsg:  "mutually exclusive",
		},
		{
			name: "Lite rejects external metadata orchestration",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeStandalone,
				Storage:        StorageConfig{Engine: StorageEngineLite, Lite: LiteStorageConfig{Path: "data.aflite"}},
				Metadata:       MetadataInfo{OrchestrationUrls: map[string]string{"1": "http://127.0.0.1:7001"}},
			},
			wantErr: true,
			errMsg:  "cannot use external metadata",
		},
		{
			name: "empty base directory",
			config: &Config{
				Storage: StorageConfig{
					Engine: StorageEngineLocal,
					Local:  LocalStorageConfig{BaseDir: ""},
				},
			},
			wantErr: true,
			errMsg:  "storage.local.base_dir is required",
		},
		{
			name: "object storage requires serverless",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeDistributed,
				Storage: StorageConfig{
					Engine: StorageEngineObject,
					Object: ObjectStorageConfig{Connection: "primary", Bucket: "bucket"},
				},
			},
			wantErr: true,
			errMsg:  "requires deployment_mode serverless",
		},
		{
			name: "object storage requires named connection",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeServerless,
				Storage: StorageConfig{
					Engine: StorageEngineObject,
					Object: ObjectStorageConfig{Connection: "missing", Bucket: "bucket"},
				},
			},
			wantErr: true,
			errMsg:  `connection "missing" was not found`,
		},
		{
			name: "object storage rejects mixed local member",
			config: &Config{
				DeploymentMode: ConfigDeploymentModeServerless,
				Storage: StorageConfig{
					Engine: StorageEngineObject,
					Local:  LocalStorageConfig{BaseDir: "antflydb"},
					Object: ObjectStorageConfig{Connection: "primary", Bucket: "bucket"},
				},
			},
			wantErr: true,
			errMsg:  "must be the only storage member",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

	t.Run("object storage validates connection capability and scope", func(t *testing.T) {
		connection := mustConnectionConfig(t, `{
			"kind":"external_io",
			"capabilities":["storage.primary"],
			"external_io":{
				"protocol":"s3",
				"buckets":["data-bucket","wal-bucket"],
				"prefix":"tenant-a",
				"credentials":{"source":"static","access_key_id":"key","secret_access_key":"secret"}
			}
		}`)
		config := &Config{
			DeploymentMode: ConfigDeploymentModeServerless,
			Connections:    map[string]ConnectionConfig{"primary": connection},
			Storage: StorageConfig{
				Engine: StorageEngineObject,
				Object: ObjectStorageConfig{
					Connection: "primary",
					Bucket:     "data-bucket",
					Prefix:     "tenant-a/cluster",
					Lanes: ObjectStorageLanes{
						Wal: ObjectStorageLocation{Bucket: "wal-bucket", Prefix: "tenant-a/cluster/wal"},
					},
				},
			},
		}
		require.NoError(t, config.validateStorage())

		config.Storage.Object.Prefix = "tenant-b/cluster"
		err := config.validateStorage()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "outside connection")
	})

	t.Run("external IO resolvers enforce capability and scope", func(t *testing.T) {
		root := t.TempDir()
		filesystem := mustConnectionConfig(t, fmt.Sprintf(`{
			"kind":"external_io",
			"capabilities":["backup.write","restore.read"],
			"external_io":{"protocol":"filesystem","root":%q}
		}`, root))
		s3Connection := mustConnectionConfig(t, `{
			"kind":"external_io",
			"capabilities":["backup.write"],
			"external_io":{
				"protocol":"s3",
				"buckets":["backup-bucket"],
				"prefix":"tenant-a",
				"credentials":{"source":"static","access_key_id":"key","secret_access_key":"secret"}
			}
		}`)
		config := &Config{Connections: map[string]ConnectionConfig{
			"filesystem": filesystem,
			"s3":         s3Connection,
		}}

		resolved, err := config.ResolveFilesystemPath(
			"filesystem",
			"backup.write",
			"file:///daily/backup-1",
		)
		require.NoError(t, err)
		canonicalRoot, err := filepath.EvalSymlinks(root)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(canonicalRoot, "daily", "backup-1"), resolved)
		require.NoError(t, os.MkdirAll(resolved, 0o750))
		opened, err := config.OpenFilesystemPath(
			"filesystem",
			"restore.read",
			"file:///daily/backup-1",
		)
		require.NoError(t, err)
		require.NoError(t, opened.Close())

		_, err = config.ResolveFilesystemPath(
			"filesystem",
			"backup.write",
			"file:///../outside",
		)
		require.ErrorContains(t, err, "escapes")

		_, err = config.ResolveFilesystemPath(
			"filesystem",
			"storage.primary",
			"file:///daily",
		)
		require.ErrorContains(t, err, "lacks required capability")

		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
			t.Skipf("symlinks are unavailable: %v", err)
		}
		_, err = config.ResolveFilesystemPath(
			"filesystem",
			"backup.write",
			"file:///escape/backup-1",
		)
		require.ErrorContains(t, err, "cannot traverse symlink")
		_, err = config.OpenFilesystemPath(
			"filesystem",
			"restore.read",
			"file:///escape",
		)
		require.Error(t, err)

		heldDir := filepath.Join(root, "held")
		require.NoError(t, os.Mkdir(heldDir, 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(heldDir, "artifact"),
			[]byte("authorized"),
			0o600,
		))
		heldRoot, err := config.OpenFilesystemPath(
			"filesystem",
			"restore.read",
			"file:///held",
		)
		require.NoError(t, err)
		defer func() { _ = heldRoot.Close() }()
		movedDir := filepath.Join(root, "held-moved")
		require.NoError(t, os.Rename(heldDir, movedDir))
		require.NoError(t, os.Symlink(outside, heldDir))
		heldArtifact, err := heldRoot.Open("artifact")
		require.NoError(t, err)
		body, err := io.ReadAll(heldArtifact)
		require.NoError(t, err)
		require.NoError(t, heldArtifact.Close())
		require.Equal(t, []byte("authorized"), body)

		s3Info, err := config.ResolveS3Info(
			"s3",
			"backup.write",
			"s3://backup-bucket/tenant-a/daily",
		)
		require.NoError(t, err)
		assert.Equal(t, "backup-bucket", s3Info.Bucket)
		assert.Equal(t, "tenant-a/daily", s3Info.Prefix)

		_, err = config.ResolveS3Info(
			"s3",
			"backup.write",
			"s3://backup-bucket/tenant-b/daily",
		)
		require.ErrorContains(t, err, "outside connection")
	})
}

func TestValidateAWSCredentialConfigRejectsCrossVariantFields(t *testing.T) {
	tests := []AwsCredentialConfig{
		{Source: AwsCredentialConfigSourceDefault, SessionName: "unexpected"},
		{
			Source:          AwsCredentialConfigSourceStatic,
			AccessKeyId:     "key",
			SecretAccessKey: "secret",
			StsEndpoint:     "https://sts.example.test",
		},
		{
			Source:      AwsCredentialConfigSourceProfile,
			Profile:     "production",
			SessionName: "unexpected",
		},
	}
	for _, config := range tests {
		require.Error(t, validateAWSCredentialConfig(config))
	}
}

func TestS3ProfileCredentialsAndConnectionClientReuse(t *testing.T) {
	credentialsPath := filepath.Join(t.TempDir(), "credentials")
	require.NoError(t, os.WriteFile(
		credentialsPath,
		[]byte("[production]\naws_access_key_id=profile-key\naws_secret_access_key=profile-secret\naws_session_token=profile-token\n"),
		0o600,
	))
	profileInfo := S3Info{
		CredentialSource:      AwsCredentialConfigSourceProfile,
		Profile:               "production",
		SharedCredentialsFile: credentialsPath,
	}
	provider, err := profileInfo.GetS3Credentials()
	require.NoError(t, err)
	value, err := provider.Get()
	require.NoError(t, err)
	assert.Equal(t, "profile-key", value.AccessKeyID)
	assert.Equal(t, "profile-secret", value.SecretAccessKey)
	assert.Equal(t, "profile-token", value.SessionToken)

	connection := mustConnectionConfig(t, `{
		"kind":"external_io",
		"capabilities":["backup.write"],
		"external_io":{
			"protocol":"s3",
			"endpoint":"http://localhost:9000",
			"addressing_style":"path",
			"buckets":["backup-bucket"],
			"credentials":{"source":"static","access_key_id":"key","secret_access_key":"secret"}
		}
	}`)
	config := &Config{Connections: map[string]ConnectionConfig{"backup": connection}}
	firstInfo, err := config.ResolveS3Info(
		"backup",
		"backup.write",
		"s3://backup-bucket/first",
	)
	require.NoError(t, err)
	secondInfo, err := config.ResolveS3Info(
		"backup",
		"backup.write",
		"s3://backup-bucket/second",
	)
	require.NoError(t, err)
	firstClient, err := firstInfo.NewMinioClient()
	require.NoError(t, err)
	secondClient, err := secondInfo.NewMinioClient()
	require.NoError(t, err)
	assert.Same(t, firstClient, secondClient)

	type clientResult struct {
		client *minio.Client
		err    error
	}
	results := make(chan clientResult, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := secondInfo.NewMinioClient()
			results <- clientResult{client: client, err: err}
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		assert.Same(t, firstClient, result.client)
	}
}

func TestIsS3CreateConflictNormalizesProviderResponses(t *testing.T) {
	assert.True(t, IsS3CreateConflict(minio.ErrorResponse{Code: minio.PreconditionFailed}))
	assert.True(t, IsS3CreateConflict(minio.ErrorResponse{Code: "ConditionalRequestConflict"}))
	assert.False(t, IsS3CreateConflict(minio.ErrorResponse{Code: minio.AccessDenied}))
	assert.False(t, IsS3CreateConflict(nil))
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
					Engine: StorageEngineLocal,
					Local:  LocalStorageConfig{BaseDir: "antflydb"},
				},
				ReplicationFactor:     3,
				MaxShardSizeBytes:     1073741824,
				DefaultShardsPerTable: 4,
				MaxShardsPerTable:     100,
			},
			wantErr: false,
		},
		{
			name: "fully valid standalone Lite config needs no external metadata",
			config: &Config{
				DeploymentMode:        ConfigDeploymentModeStandalone,
				Storage:               StorageConfig{Engine: StorageEngineLite, Lite: LiteStorageConfig{Path: "data.aflite"}},
				ReplicationFactor:     1,
				MaxShardSizeBytes:     1073741824,
				DefaultShardsPerTable: 1,
				MinShardsPerTable:     1,
				MaxShardsPerTable:     1,
				DisableShardAlloc:     true,
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
				name: "local engine returns local",
				config: &Config{
					Storage: StorageConfig{Engine: StorageEngineLocal},
				},
				expected: "local",
			},
			{
				name: "object engine returns s3",
				config: &Config{
					Storage: StorageConfig{Engine: StorageEngineObject},
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
				name: "local engine returns local",
				config: &Config{
					Storage: StorageConfig{Engine: StorageEngineLocal},
				},
				expected: "local",
			},
			{
				name: "object engine returns s3",
				config: &Config{
					Storage: StorageConfig{Engine: StorageEngineObject},
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
		{
			name:        "invalid - traversal segment",
			url:         "s3://my-bucket/tenant-a/../tenant-b",
			shouldError: true,
			errContains: "dot path segments",
		},
		{
			name:        "invalid - encoded traversal segment",
			url:         "s3://my-bucket/tenant-a/%2e%2e/tenant-b",
			shouldError: true,
			errContains: "dot path segments",
		},
		{
			name:        "invalid - query",
			url:         "s3://my-bucket/prefix?version=1",
			shouldError: true,
			errContains: "query or fragment",
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
