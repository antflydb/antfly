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
	"context"
	"fmt"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/termite/pkg/termite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var swarmCmd = &cobra.Command{
	Use:   "swarm",
	Short: "Run as a swarm node (metadata + raft)",
	Long:  `Start the AntFly database in swarm mode, running both metadata and raft services.`,
	RunE:  runSwarm,
}

func init() {
	rootCmd.AddCommand(swarmCmd)

	swarmCmd.Flags().Uint64("id", 1, "node ID")
	swarmCmd.Flags().String("metadata-raft", "http://0.0.0.0:9017", "metadata raft server URL")
	swarmCmd.Flags().String("metadata-api", "http://0.0.0.0:8080", "metadata api server URL")
	swarmCmd.Flags().
		String("metadata-cluster", `{ "1": "http://0.0.0.0:9017" }`, "metadata cluster peer URLs (json object)")
	swarmCmd.Flags().String("store-raft", "http://0.0.0.0:9021", "store raft server URL")
	swarmCmd.Flags().String("store-api", "http://0.0.0.0:12380", "store api server URL")
	swarmCmd.Flags().Bool("termite", true, "also run as a termite node")
	swarmCmd.Flags().String("termite-api-url", "", "Termite API URL (http://host:port)")
	swarmCmd.Flags().Int("health-port", 4200, "health/metrics server port")

	mustBindPFlag("swarm.id", swarmCmd.Flags().Lookup("id"))
	mustBindPFlag("swarm.metadata-raft", swarmCmd.Flags().Lookup("metadata-raft"))
	mustBindPFlag("swarm.metadata-api", swarmCmd.Flags().Lookup("metadata-api"))
	mustBindPFlag("swarm.metadata-cluster", swarmCmd.Flags().Lookup("metadata-cluster"))
	mustBindPFlag("swarm.store-raft", swarmCmd.Flags().Lookup("store-raft"))
	mustBindPFlag("swarm.store-api", swarmCmd.Flags().Lookup("store-api"))
	mustBindPFlag("swarm.termite", swarmCmd.Flags().Lookup("termite"))
	mustBindPFlag("termite.api_url", swarmCmd.Flags().Lookup("termite-api-url"))
	mustBindPFlag("health_port", swarmCmd.Flags().Lookup("health-port"))
}

func runSwarm(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	id := viper.GetUint64("swarm.id")
	enableTermite := viper.GetBool("swarm.termite")
	if enableTermite {
		viper.SetDefault("termite.api_url", "http://0.0.0.0:11433")
	}
	viper.SetDefault("cors.enabled", true)
	viper.SetDefault("replication_factor", 1)
	viper.SetDefault("default_shards_per_table", 1)
	viper.SetDefault("metadata.orchestration_urls", map[string]string{
		types.ID(id).String(): viper.GetString("swarm.metadata-api"),
	})

	config, err := parseConfig(viper.GetViper())
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	logger := getLogger(config)
	defer func() { _ = logger.Sync() }()

	config.DisableShardAlloc = true
	config.SwarmMode = true

	peers, err := parsePeers(viper.GetString("swarm.metadata-cluster"))
	if err != nil {
		logger.Fatal("Failed to parse metadata cluster peers", zap.Error(err))
	}

	cache := pebbleutils.NewCache(pebbleutils.DefaultCacheSizeMB << 20)
	defer cache.Close()

	tid := types.ID(id)
	metadataReadyC := make(chan struct{})
	storeReadyC := make(chan struct{})
	var termiteReadyC chan struct{}
	if enableTermite {
		termiteReadyC = make(chan struct{})
	}

	// Aggregate readiness: health server reports ready once all sub-servers are ready
	ready := &atomic.Bool{}
	go func() {
		<-metadataReadyC
		<-storeReadyC
		if termiteReadyC != nil {
			<-termiteReadyC
		}
		ready.Store(true)
		logger.Info("Swarm mode: all servers are ready")
	}()
	healthserver.Start(logger, config.HealthPort, ready.Load)

	go metadata.RunAsMetadataServer(ctx, logger, config,
		&store.StoreInfo{
			ID:      tid,
			RaftURL: viper.GetString("swarm.metadata-raft"),
			ApiURL:  viper.GetString("swarm.metadata-api"),
		},
		peers,
		false, // join
		metadataReadyC,
		cache,
	)

	// Wait for metadata to finish Pebble initialization before starting other services
	<-metadataReadyC

	if enableTermite {
		go termite.RunAsTermite(ctx, logger, termiteConfigWithSecurity(config), termiteReadyC)
		// Wait for termite to finish Pebble initialization before starting store
		<-termiteReadyC
	}

	storeConf := &store.StoreInfo{
		ID:      tid,
		ApiURL:  viper.GetString("swarm.store-api"),
		RaftURL: viper.GetString("swarm.store-raft"),
	}
	store.RunAsStore(ctx, logger, config, storeConf, "", storeReadyC, cache)
	return nil
}
