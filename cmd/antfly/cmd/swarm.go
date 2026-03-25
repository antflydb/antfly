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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/src/metadata"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/termite/pkg/termite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

	metaConf := &store.StoreInfo{
		ID:      tid,
		RaftURL: viper.GetString("swarm.metadata-raft"),
		ApiURL:  viper.GetString("swarm.metadata-api"),
	}

	// Create metadata runtime inline so we can wire the local store bypass.
	metaRuntime, err := metadata.NewRuntime(
		logger.Named("metadataServer"), config, metaConf, peers, false, cache,
	)
	if err != nil {
		return fmt.Errorf("creating metadata runtime: %w", err)
	}
	metaRuntime.StartRaft()
	defer func() {
		if err := metaRuntime.Close(); err != nil {
			logger.Error("failed to close metadata runtime", zap.Error(err))
		}
	}()

	// Start metadata HTTP server.
	go func() {
		u, err := url.Parse(metaConf.ApiURL)
		if err != nil {
			logger.Fatal("Error parsing metadata API URL", zap.Error(err))
		}
		srv := http.Server{
			Addr:        u.Host,
			Handler:     metaRuntime.HTTPHandler(),
			ReadTimeout: 10 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		listener, listenErr := net.Listen("tcp", u.Host)
		if listenErr != nil {
			logger.Fatal("Failed to create metadata listener", zap.Error(listenErr))
		}
		close(metadataReadyC)
		logger.Info("Metadata API server is ready", zap.String("address", u.Host))
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("Metadata HTTP server error", zap.Error(err))
		}
	}()

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

	// Create store runtime inline so we can wire the local bypass.
	storeRuntime, err := store.NewRuntime(logger.Named("store"), config, storeConf, cache)
	if err != nil {
		return fmt.Errorf("creating store runtime: %w", err)
	}
	storeRuntime.StartRaft()
	defer func() {
		if err := storeRuntime.Close(); err != nil {
			logger.Error("failed to close store runtime", zap.Error(err))
		}
	}()

	// Wire local bypass: metadata calls store directly in-process.
	metaRuntime.SetLocalStore(tid, storeRuntime.Store())
	logger.Info("Local shard bypass enabled for swarm mode")

	eg, egCtx := errgroup.WithContext(ctx)

	// Start store HTTP server (still needed for raft transport and registration).
	eg.Go(func() error {
		u, err := url.Parse(storeConf.ApiURL)
		if err != nil {
			return fmt.Errorf("parsing store API URL: %w", err)
		}
		srv := &http.Server{
			Addr:        u.Host,
			Handler:     storeRuntime.HTTPHandler(),
			ReadTimeout: time.Minute,
		}
		go func() {
			<-egCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
		listener, listenErr := net.Listen("tcp", u.Host)
		if listenErr != nil {
			return fmt.Errorf("creating store listener: %w", listenErr)
		}
		close(storeReadyC)
		logger.Info("Store HTTP server is ready", zap.String("address", u.Host))
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("store HTTP server: %w", err)
		}
		return nil
	})

	// Register store with metadata (still via HTTP for cluster bookkeeping).
	eg.Go(func() error {
		orchURLs, _ := config.Metadata.GetOrchestrationURLs()
		return store.RegisterWithLeaderWithRetry(egCtx, logger, storeRuntime.Store(), storeConf, orchURLs)
	})

	if err := eg.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Swarm shut down")
			return nil
		}
		return fmt.Errorf("swarm failure: %w", err)
	}
	return nil
}
