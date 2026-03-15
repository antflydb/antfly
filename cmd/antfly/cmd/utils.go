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
	"sync/atomic"

	"github.com/antflydb/antfly/lib/scraping"
	libtermite "github.com/antflydb/antfly/lib/termite"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/pkg/libaf/logging"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/termite/pkg/termite"
	"github.com/go-viper/mapstructure/v2"
	gojson "github.com/goccy/go-json"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const defaultMaxShardSizeBytes = 64 * 1024 * 1024 // 64MB

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
	// Set defaults before parsing
	v.SetDefault("max_shard_size_bytes", defaultMaxShardSizeBytes)
	if v.GetInt("max_shards_per_table") == 0 {
		v.SetDefault("max_shards_per_table", 20)
	}
	v.SetDefault("disable_shard_allocation", true)
	if v.GetInt("replication_factor") == 0 {
		// If the default wasn't set by swarm mode set it here
		v.SetDefault("replication_factor", 3)
	}
	if v.GetInt("default_shards_per_table") == 0 {
		// If the default wasn't set by swarm mode set it here
		v.SetDefault("default_shards_per_table", 3)
	}
	v.SetDefault("health_port", 4200)
	// Storage defaults
	v.SetDefault("storage.local.base_dir", common.DefaultDataDir())
	v.SetDefault("storage.keyvalue", "local")
	v.SetDefault("storage.metadatakv", "local")

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
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Initialize remote content configuration for S3 URL downloads in templates
	scraping.InitRemoteContentConfig(&config.RemoteContent)

	// Set default Termite URL from config so all consumers (embeddings,
	// generators, rerankers, chunkers) can resolve it without explicit config.
	if config.Termite.ApiUrl != "" {
		libtermite.SetDefaultURL(config.Termite.ApiUrl)
	}

	// Initialize all named providers from config
	common.InitRegistryFromConfig(&config)

	return &config, nil
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

// termiteConfigWithSecurity returns a copy of the termite config with security
// settings inherited from the top-level remote content config when the termite
// config does not define its own.
func termiteConfigWithSecurity(config *common.Config) termite.Config {
	tc := config.Termite
	if scraping.IsSecurityConfigEmpty(tc.ContentSecurity) &&
		!scraping.IsSecurityConfigEmpty(config.RemoteContent.Security) {
		tc.ContentSecurity = config.RemoteContent.Security
	}
	return tc
}
