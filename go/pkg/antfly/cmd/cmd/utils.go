/*
Copyright © 2025 AJ Roetker ajroetker@antfly.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	_ "net/http/pprof" // #nosec DefaultServeMux is on internal ports only.
	"reflect"
	"strings"
	"sync/atomic"

	libinference "github.com/antflydb/antfly/go/pkg/antfly/lib/inference"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/scraping"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/libaf/logging"
	inference "github.com/antflydb/antfly/go/pkg/termite"
	"github.com/go-viper/mapstructure/v2"
	gojson "github.com/goccy/go-json"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const defaultMaxShardSizeBytes = 64 * 1024 * 1024 // 64MB
const defaultInferenceAPIURL = "http://0.0.0.0:8080"

type parseConfigOptions struct {
	RequireMetadata        bool
	DefaultInferenceAPIURL string
}

// mustBindPFlag binds a pflag to viper and panics on error.
// This is appropriate for init() functions where binding failures represent programming errors.
func mustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(err)
	}
}

// JSONStructTag returns a viper.DecoderConfigOption that uses json tags instead of mapstructure tags.
// This allows viper to work with oapi-codegen generated structs which only have json tags.
func JSONStructTag() viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		c.TagName = "json"
	}
}

// JSONUnionDecodeHook returns a mapstructure DecodeHookFunc that handles oapi-codegen union types.
// These types (like GeneratorConfig) have unexported union fields that are only populated via
// UnmarshalJSON. Without this hook, mapstructure sets exported fields but leaves the union empty.
func JSONUnionDecodeHook() mapstructure.DecodeHookFunc {
	return func(from, to reflect.Type, data any) (any, error) {
		if from.Kind() != reflect.Map || to.Kind() != reflect.Struct {
			return data, nil
		}

		toPtr := reflect.New(to)
		if _, ok := toPtr.Interface().(json.Unmarshaler); !ok {
			return data, nil
		}

		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return data, nil
		}

		if err := json.Unmarshal(jsonBytes, toPtr.Interface()); err != nil {
			return data, nil
		}

		return toPtr.Elem().Interface(), nil
	}
}

// parseConfig reads and parses the configuration using viper
func parseConfig(v *viper.Viper) (*common.Config, error) {
	return parseConfigWithOptions(v, parseConfigOptions{
		RequireMetadata: true,
	})
}

// parseConfigWithOptions reads and parses configuration using viper with
// command-specific defaults and validation requirements.
func parseConfigWithOptions(v *viper.Viper, opts parseConfigOptions) (*common.Config, error) {
	// Set defaults before parsing
	v.SetDefault("storage.engine", "local")
	liteStorage := strings.EqualFold(v.GetString("storage.engine"), "lite")
	v.SetDefault("max_shard_size_bytes", defaultMaxShardSizeBytes)
	if v.GetInt("max_shards_per_table") == 0 {
		v.SetDefault("max_shards_per_table", 20)
	}
	v.SetDefault("disable_shard_alloc", true)
	if v.GetInt("replication_factor") == 0 {
		// If the default wasn't set by standalone mode set it here
		if liteStorage {
			v.SetDefault("replication_factor", 1)
		} else {
			v.SetDefault("replication_factor", 3)
		}
	}
	if v.GetInt("default_shards_per_table") == 0 {
		// If the default wasn't set by standalone mode set it here
		if liteStorage {
			v.SetDefault("default_shards_per_table", 1)
		} else {
			v.SetDefault("default_shards_per_table", 3)
		}
	}
	v.SetDefault("health_enabled", true)
	v.SetDefault("health_port", 4200)
	// Storage defaults
	if !liteStorage && strings.EqualFold(v.GetString("storage.engine"), "local") {
		v.SetDefault("storage.local.base_dir", common.DefaultDataDir())
		v.SetDefault("storage.local.data", "local")
		v.SetDefault("storage.local.metadata", "local")
	}
	if opts.DefaultInferenceAPIURL != "" {
		v.SetDefault("inference.api_url", opts.DefaultInferenceAPIURL)
	}

	if err := validateStorageConfigShape(v.GetStringMap("storage")); err != nil {
		return nil, err
	}

	var config common.Config
	if err := v.Unmarshal(&config, JSONStructTag(), viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			JSONUnionDecodeHook(),
		),
	)); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	config.Version = Version
	config.NormalizeDeploymentMode()

	// Get log level from config or viper (env var / flag)
	logLevel := string(config.Log.Level)
	if logLevel == "" {
		logLevel = v.GetString("log_level")
	}

	// Get log style from config or viper (env var / flag)
	logStyle := string(config.Log.Style)
	if logStyle == "" {
		logStyle = v.GetString("log_style")
	}

	// Set log level and style using the logging package types
	if logLevel != "" {
		config.Log.Level = logging.Level(logLevel)
	}
	if logStyle != "" {
		config.Log.Style = logging.Style(logStyle)
	}

	// Validate the configuration
	if err := config.ValidateWithOptions(common.ValidationOptions{
		RequireMetadata: opts.RequireMetadata,
	}); err != nil {
		return nil, err
	}

	// Initialize remote content configuration for template helpers. Viper tracks
	// field presence, which lets block_private_ips: false override the safe
	// default even though the generated Go config uses a plain bool.
	scraping.InitRemoteContentConfigWithOptions(&config.RemoteContent, remoteContentInitOptions(v, &config))

	// Set default inference URL from config so all consumers (embeddings,
	// generators, rerankers, chunkers) can resolve it without explicit config.
	if config.Inference.ApiUrl != "" {
		libinference.SetDefaultURL(config.Inference.ApiUrl)
	}

	// Initialize all named providers from config
	common.InitRegistryFromConfig(&config)

	return &config, nil
}

func validateStorageConfigShape(storage map[string]any) error {
	allowed := map[string]struct{}{"engine": {}, "lite": {}, "local": {}, "object": {}}
	if err := validateConfigObjectFields("storage", storage, allowed); err != nil {
		return fmt.Errorf("%w; engine-specific settings belong under storage.<engine>", err)
	}

	local, err := optionalConfigObject(storage, "local", "storage.local")
	if err != nil {
		return err
	}
	if local != nil {
		if err := validateConfigObjectFields("storage.local", local, map[string]struct{}{"base_dir": {}, "data": {}, "metadata": {}, "s3": {}}); err != nil {
			return err
		}
		s3, err := optionalConfigObject(local, "s3", "storage.local.s3")
		if err != nil {
			return err
		}
		if s3 != nil {
			if err := validateConfigObjectFields("storage.local.s3", s3, map[string]struct{}{
				"endpoint": {}, "use_ssl": {}, "access_key_id": {}, "secret_access_key": {}, "session_token": {},
				"bucket": {}, "prefix": {},
			}); err != nil {
				return err
			}
		}
	}

	lite, err := optionalConfigObject(storage, "lite", "storage.lite")
	if err != nil {
		return err
	}
	if err := validateConfigObjectFields("storage.lite", lite, map[string]struct{}{"path": {}, "fsync": {}}); err != nil {
		return err
	}
	object, err := optionalConfigObject(storage, "object", "storage.object")
	if err != nil {
		return err
	}
	return validateConfigObjectFields("storage.object", object, map[string]struct{}{"provider": {}, "bucket": {}, "prefix": {}})
}

func optionalConfigObject(parent map[string]any, key, path string) (map[string]any, error) {
	value, ok := parent[key]
	if !ok {
		return nil, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", path)
	}
	return object, nil
}

func validateConfigObjectFields(path string, object map[string]any, allowed map[string]struct{}) error {
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("unsupported %s field %q", path, key)
		}
	}
	return nil
}

func remoteContentInitOptions(v *viper.Viper, config *common.Config) scraping.RemoteContentInitOptions {
	opts := scraping.RemoteContentInitOptions{
		GlobalSecurityConfigured:        v.IsSet("remote_content.security"),
		GlobalBlockPrivateIpsConfigured: v.IsSet("remote_content.security.block_private_ips"),
		S3SecurityConfigured:            make(map[string]bool, len(config.RemoteContent.S3)),
		S3BlockPrivateIpsConfigured:     make(map[string]bool, len(config.RemoteContent.S3)),
		HTTPSecurityConfigured:          make(map[string]bool, len(config.RemoteContent.Http)),
		HTTPBlockPrivateIpsConfigured:   make(map[string]bool, len(config.RemoteContent.Http)),
	}
	for name := range config.RemoteContent.S3 {
		opts.S3SecurityConfigured[name] = v.IsSet(fmt.Sprintf("remote_content.s3.%s.security", name))
		opts.S3BlockPrivateIpsConfigured[name] = v.IsSet(fmt.Sprintf("remote_content.s3.%s.security.block_private_ips", name))
	}
	for name := range config.RemoteContent.Http {
		opts.HTTPSecurityConfigured[name] = v.IsSet(fmt.Sprintf("remote_content.http.%s.security", name))
		opts.HTTPBlockPrivateIpsConfigured[name] = v.IsSet(fmt.Sprintf("remote_content.http.%s.security.block_private_ips", name))
	}
	return opts
}

func getLogger(c *common.Config) *zap.Logger {
	if c == nil {
		return logging.NewLogger(nil)
	}
	return logging.NewLogger(&c.Log)
}

// startHealthServer creates a readiness channel, starts the health server, and
// sets the ready flag when readyC is closed. Returns the readyC channel that
// callers pass to their RunAs* functions.
func startHealthServer(logger *zap.Logger, healthPort int, readyC chan struct{}, label string) {
	ready := &atomic.Bool{}
	healthserver.Start(logger, healthPort, ready.Load)
	go func() {
		<-readyC
		ready.Store(true)
		logger.Info(label + " is ready")
	}()
}

// parsePeers parses a JSON string of peer URLs into a common.Peers slice.
func parsePeers(clusterJSON string) (common.Peers, error) {
	peerMap := map[types.ID]string{}
	if err := gojson.Unmarshal([]byte(clusterJSON), &peerMap); err != nil {
		return nil, fmt.Errorf("parse metadata cluster peers: %w", err)
	}
	peers := make(common.Peers, 0, len(peerMap))
	for id, url := range peerMap {
		peers = append(peers, common.Peer{ID: id, URL: url})
	}
	return peers, nil
}

// inferenceConfigWithSecurity returns a copy of the inference config with security
// settings inherited from the top-level remote content config when the inference
// config does not define its own.
func inferenceConfigWithSecurity(config *common.Config) inference.Config {
	tc := config.Inference
	if scraping.IsSecurityConfigEmpty(tc.ContentSecurity) &&
		!scraping.IsSecurityConfigEmpty(config.RemoteContent.Security) {
		tc.ContentSecurity = config.RemoteContent.Security
	}
	return tc
}
