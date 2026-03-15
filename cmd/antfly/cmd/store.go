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
	"github.com/antflydb/antfly/src/store"
	_ "github.com/antflydb/antfly/src/store/db/indexes" // for default indexes
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var storeCmd = &cobra.Command{
	Use:   "store",
	Short: "Run as a store node",
	Long:  `Start the AntFly database in store mode to handle data storage and retrieval.`,
	RunE:  runStore,
}

func init() {
	rootCmd.AddCommand(storeCmd)

	storeCmd.Flags().Uint64("id", 1, "node ID")
	storeCmd.Flags().String("raft", "http://0.0.0.0:9021", "raft server URL")
	storeCmd.Flags().String("api", "http://0.0.0.0:12380", "api server URL")
	storeCmd.Flags().String("service", "", "service name (for multi-tenant mode) with kubernetes")
	storeCmd.Flags().Int("health-port", 4200, "health/metrics server port")

	mustBindPFlag("store.id", storeCmd.Flags().Lookup("id"))
	mustBindPFlag("store.raft", storeCmd.Flags().Lookup("raft"))
	mustBindPFlag("store.api", storeCmd.Flags().Lookup("api"))
	mustBindPFlag("store.service", storeCmd.Flags().Lookup("service"))
	mustBindPFlag("health_port", storeCmd.Flags().Lookup("health-port"))
}

func runStore(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	config, err := parseConfig(viper.GetViper())
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	logger := getLogger(config)
	defer func() { _ = logger.Sync() }()

	logger.Info("Running as store")

	cache := pebbleutils.NewCache(pebbleutils.DefaultCacheSizeMB << 20)
	defer cache.Close()

	readyC := make(chan struct{})
	startHealthServer(logger, config.HealthPort, readyC, "Store")

	storeConf := &store.StoreInfo{
		ID:      types.ID(viper.GetUint64("store.id")),
		ApiURL:  viper.GetString("store.api"),
		RaftURL: viper.GetString("store.raft"),
	}
	store.RunAsStore(ctx, logger, config, storeConf, viper.GetString("store.service"), readyC, cache)
	return nil
}
