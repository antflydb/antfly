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

package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/pkg/libaf/logging"
	"github.com/antflydb/antfly/src/common"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestJSONUnionDecodeHook verifies that oapi-codegen union types (like GeneratorConfig)
// are correctly parsed from YAML config. Without the JSONUnionDecodeHook, mapstructure
// doesn't call the custom UnmarshalJSON method, causing union fields to be empty.
func TestJSONUnionDecodeHook(t *testing.T) {
	configYAML := `
generators:
  openai-gpt4:
    provider: openai
    model: gpt-4.1
  anthropic-claude:
    provider: anthropic
    model: claude-sonnet-4-5-20250929
chains:
  default:
    - generator: openai-gpt4
`

	t.Run("without_hook_union_data_is_empty", func(t *testing.T) {
		// This demonstrates the bug: without JSONUnionDecodeHook, the union data is not populated.
		// The discriminator field (provider) is populated since it's a direct struct field,
		// but the union's raw JSON data is empty, causing AsXxxConfig() to fail.
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

		var config common.Config
		// Only using JSONStructTag (the old behavior before the fix)
		err := v.Unmarshal(&config, JSONStructTag())
		require.NoError(t, err)

		// The provider is a direct field, so it gets populated
		require.Equal(t, ai.GeneratorProviderOpenai, config.Generators["openai-gpt4"].Provider)

		// But the union data is empty because mapstructure doesn't call UnmarshalJSON.
		// This causes AsOpenAIGeneratorConfig() to fail with "unexpected end of JSON input"
		// because it's trying to unmarshal an empty byte slice.
		_, err = config.Generators["openai-gpt4"].AsOpenAIGeneratorConfig()
		require.Error(t, err, "without JSONUnionDecodeHook, accessing union data should fail")
		require.Contains(t, err.Error(), "unexpected end of JSON input")
	})

	t.Run("with_hook_model_is_populated_unit", func(t *testing.T) {
		// This shows the fix: with JSONUnionDecodeHook, the model field is correctly populated
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

		var config common.Config
		err := v.Unmarshal(&config, JSONStructTag(), viper.DecodeHook(
			mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
				JSONUnionDecodeHook(),
			),
		))
		require.NoError(t, err)

		// Provider should still work
		require.Equal(t, ai.GeneratorProviderOpenai, config.Generators["openai-gpt4"].Provider)
		require.Equal(t, ai.GeneratorProviderAnthropic, config.Generators["anthropic-claude"].Provider)

		// And now the model should be populated correctly
		openaiConfig, err := config.Generators["openai-gpt4"].AsOpenAIGeneratorConfig()
		require.NoError(t, err)
		require.Equal(t, "gpt-4.1", openaiConfig.Model)

		anthropicConfig, err := config.Generators["anthropic-claude"].AsAnthropicGeneratorConfig()
		require.NoError(t, err)
		require.Equal(t, "claude-sonnet-4-5-20250929", anthropicConfig.Model)

		// Chains should also be parsed
		require.Len(t, config.Chains["default"], 1)
		require.Equal(t, "openai-gpt4", config.Chains["default"][0].Generator)
	})
}

// TestParseConfigGenerators is the true integration test - it calls parseConfig() directly.
// This test will FAIL on main (bug present) and PASS on the fix branch.
func TestParseConfigGenerators(t *testing.T) {
	configYAML := `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
generators:
  openai-gpt4:
    provider: openai
    model: gpt-4.1
  anthropic-claude:
    provider: anthropic
    model: claude-sonnet-4-5-20250929
chains:
  default:
    - generator: openai-gpt4
`

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	// Verify OpenAI generator config is fully populated
	require.Contains(t, config.Generators, "openai-gpt4")
	openaiConfig, err := config.Generators["openai-gpt4"].AsOpenAIGeneratorConfig()
	require.NoError(t, err, "AsOpenAIGeneratorConfig should succeed with the fix")
	require.Equal(t, "gpt-4.1", openaiConfig.Model)

	// Verify Anthropic generator config is fully populated
	require.Contains(t, config.Generators, "anthropic-claude")
	anthropicConfig, err := config.Generators["anthropic-claude"].AsAnthropicGeneratorConfig()
	require.NoError(t, err, "AsAnthropicGeneratorConfig should succeed with the fix")
	require.Equal(t, "claude-sonnet-4-5-20250929", anthropicConfig.Model)

	// Verify chains are parsed
	require.Len(t, config.Chains["default"], 1)
	require.Equal(t, "openai-gpt4", config.Chains["default"][0].Generator)
}

// minimalValidYAML is the smallest YAML that passes parseConfig validation.
// Only metadata orchestration URLs are required; everything else uses defaults.
const minimalValidYAML = `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
`

func TestParseConfigDefaults(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(minimalValidYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	require.Equal(t, uint64(64*1024*1024), config.MaxShardSizeBytes)
	require.Equal(t, uint64(20), config.MaxShardsPerTable)
	require.Equal(t, uint64(3), config.ReplicationFactor)
	require.Equal(t, uint64(3), config.DefaultShardsPerTable)
	require.Equal(t, 4200, config.HealthPort)
	require.Equal(t, common.DefaultDataDir(), config.Storage.Local.BaseDir)
	require.True(t, config.DisableShardAlloc)

	// Viper-level defaults for storage backends (these keys don't map to struct
	// fields via JSON tags, but are available via viper for other consumers).
	require.Equal(t, "local", v.GetString("storage.keyvalue"))
	require.Equal(t, "local", v.GetString("storage.metadatakv"))
}

func TestParseConfigSplitSettings(t *testing.T) {
	configYAML := `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
disable_shard_alloc: false
split_finalize_grace_period: 27s
`

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	require.False(t, config.DisableShardAlloc)
	require.Equal(t, 27*time.Second, config.SplitFinalizeGracePeriod)
}

func TestParseConfigDefaultsOverridden(t *testing.T) {
	configYAML := `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
max_shard_size_bytes: 134217728
max_shards_per_table: 50
replication_factor: 2
default_shards_per_table: 5
health_port: 9090
storage:
  local:
    base_dir: /tmp/antfly-test
`

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	require.Equal(t, uint64(134217728), config.MaxShardSizeBytes)
	require.Equal(t, uint64(50), config.MaxShardsPerTable)
	require.Equal(t, uint64(2), config.ReplicationFactor)
	require.Equal(t, uint64(5), config.DefaultShardsPerTable)
	require.Equal(t, 9090, config.HealthPort)
	require.Equal(t, "/tmp/antfly-test", config.Storage.Local.BaseDir)
}

func TestParseConfigLogLevel(t *testing.T) {
	t.Run("from_config_yaml", func(t *testing.T) {
		configYAML := `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
log:
  level: debug
  style: json
`
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

		config, err := parseConfig(v)
		require.NoError(t, err)

		require.Equal(t, logging.Level("debug"), config.Log.Level)
		require.Equal(t, logging.Style("json"), config.Log.Style)
	})

	t.Run("fallback_from_viper", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(minimalValidYAML)))

		// Simulate values set via flags / env vars
		v.Set("log_level", "warn")
		v.Set("log_style", "logfmt")

		config, err := parseConfig(v)
		require.NoError(t, err)

		require.Equal(t, logging.Level("warn"), config.Log.Level)
		require.Equal(t, logging.Style("logfmt"), config.Log.Style)
	})
}

func TestParseConfigValidationErrors(t *testing.T) {
	t.Run("missing_orchestration_urls", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(`
metadata:
  orchestration_urls: {}
`)))

		_, err := parseConfig(v)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one orchestration URL is required")
	})

	t.Run("invalid_orchestration_url_node_id", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(`
metadata:
  orchestration_urls:
    "not-a-hex-id": "http://localhost:5001"
`)))

		_, err := parseConfig(v)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid metadata node ID")
	})

	t.Run("max_shard_size_too_small", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(`
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
max_shard_size_bytes: 512
`)))

		_, err := parseConfig(v)
		require.Error(t, err)
		require.Contains(t, err.Error(), "max_shard_size_bytes must be at least")
	})

	t.Run("replication_factor_zero", func(t *testing.T) {
		// ReplicationFactor 0 is below minimum of 1
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(minimalValidYAML)))
		// Override after read so the SetDefault(3) conditional is bypassed
		v.Set("replication_factor", 0)

		_, err := parseConfig(v)
		require.Error(t, err)
		require.Contains(t, err.Error(), "replication_factor must be at least")
	})

	t.Run("replication_factor_too_high", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader(`
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
replication_factor: 6
`)))

		_, err := parseConfig(v)
		require.Error(t, err)
		require.Contains(t, err.Error(), "replication_factor must be at most")
	})
}

func TestParseConfigTermiteURL(t *testing.T) {
	configYAML := `
metadata:
  orchestration_urls:
    "1": "http://localhost:5001"
termite:
  api_url: "http://localhost:8082"
`
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "http://localhost:8082", config.Termite.ApiUrl)
}

func TestParseConfigVersion(t *testing.T) {
	origVersion := Version
	defer func() { Version = origVersion }()

	Version = "v1.2.3-test"

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(minimalValidYAML)))

	config, err := parseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "v1.2.3-test", config.Version)
}
