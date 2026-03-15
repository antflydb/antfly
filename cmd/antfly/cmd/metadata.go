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
	"syscall"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var metadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "Run as a metadata node",
	Long:  `Start the AntFly database in metadata server mode to manage cluster metadata.`,
	RunE:  runMetadata,
}

func init() {
	rootCmd.AddCommand(metadataCmd)

	metadataCmd.Flags().Uint64("id", 1, "node ID")
	metadataCmd.Flags().String("raft", "http://0.0.0.0:9017", "metadata raft server URL")
	metadataCmd.Flags().String("api", "http://0.0.0.0:8080", "metadata api server URL")
	metadataCmd.Flags().
		String("cluster", `{ "1": "http://0.0.0.0:9017" }`, "metadata cluster peer URLs (json object)")
	metadataCmd.Flags().Bool("join", false, "join an existing cluster")
	metadataCmd.Flags().Int("health-port", 4200, "health/metrics server port")

	mustBindPFlag("metadata.id", metadataCmd.Flags().Lookup("id"))
	mustBindPFlag("metadata.raft", metadataCmd.Flags().Lookup("raft"))
	mustBindPFlag("metadata.api", metadataCmd.Flags().Lookup("api"))
	mustBindPFlag("metadata.cluster", metadataCmd.Flags().Lookup("cluster"))
	mustBindPFlag("metadata.join", metadataCmd.Flags().Lookup("join"))
	mustBindPFlag("health_port", metadataCmd.Flags().Lookup("health-port"))
}

func runMetadata(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	config, err := parseConfig(viper.GetViper())
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	logger := getLogger(config)
	defer func() { _ = logger.Sync() }()

	peers, err := parsePeers(viper.GetString("metadata.cluster"))
	if err != nil {
		logger.Fatal("Failed to parse metadata cluster peers", zap.Error(err))
	}

	cache := pebbleutils.NewCache(pebbleutils.DefaultCacheSizeMB << 20)
	defer cache.Close()

	readyC := make(chan struct{})
	startHealthServer(logger, config.HealthPort, readyC, "Metadata server")

	metadata.RunAsMetadataServer(ctx, logger, config,
		&store.StoreInfo{
			ID:      types.ID(viper.GetUint64("metadata.id")),
			RaftURL: viper.GetString("metadata.raft"),
			ApiURL:  viper.GetString("metadata.api"),
		},
		peers,
		viper.GetBool("metadata.join"),
		readyC,
		cache,
	)
	return nil
}
