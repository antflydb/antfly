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

	libinference "github.com/antflydb/antfly/go/pkg/antfly/lib/inference"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/metadata"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	"github.com/antflydb/antfly/go/pkg/libaf/healthserver"
	inferenceRuntime "github.com/antflydb/antfly/go/pkg/termite"
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
	swarmCmd.Flags().Bool("inference", true, "also run the inference runtime")
	swarmCmd.Flags().String("inference-api-url", "", "Inference API URL (http://host:port)")
	swarmCmd.Flags().Bool("health", true, "enable health/metrics server")
	swarmCmd.Flags().Int("health-port", 4200, "health/metrics server port")

	mustBindPFlag("swarm.id", swarmCmd.Flags().Lookup("id"))
	mustBindPFlag("swarm.metadata-raft", swarmCmd.Flags().Lookup("metadata-raft"))
	mustBindPFlag("swarm.metadata-api", swarmCmd.Flags().Lookup("metadata-api"))
	mustBindPFlag("swarm.metadata-cluster", swarmCmd.Flags().Lookup("metadata-cluster"))
	mustBindPFlag("swarm.store-raft", swarmCmd.Flags().Lookup("store-raft"))
	mustBindPFlag("swarm.store-api", swarmCmd.Flags().Lookup("store-api"))
	mustBindPFlag("swarm.inference", swarmCmd.Flags().Lookup("inference"))
	mustBindPFlag("inference.api_url", swarmCmd.Flags().Lookup("inference-api-url"))
	mustBindPFlag("health_enabled", swarmCmd.Flags().Lookup("health"))
	mustBindPFlag("health_port", swarmCmd.Flags().Lookup("health-port"))
}

func runSwarm(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	id := viper.GetUint64("swarm.id")
	enableInference := viper.GetBool("swarm.inference")
	// When inference is enabled, decide whether to run it in-process (mounted on
	// Antfly's metadata listener under /ai/v1/) or as a separate HTTP server.
	// In-process is the default. If the user explicitly provides
	// --inference-api-url, we run inference standalone on that URL.
	inferenceAPIURL := viper.GetString("inference.api_url")
	runInferenceInProcess := enableInference && inferenceAPIURL == ""
	viper.SetDefault("cors.enabled", true)
	viper.SetDefault("replication_factor", 1)
	viper.SetDefault("default_shards_per_table", 1)
	defaultOrchestrationURLs := map[string]string{
		types.ID(id).String(): viper.GetString("swarm.metadata-api"),
	}
	viper.SetDefault("metadata.orchestration_urls", defaultOrchestrationURLs)

	config, err := parseConfigWithOptions(viper.GetViper(), parseConfigOptions{
		RequireMetadata: false,
	})
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	if len(config.Metadata.OrchestrationUrls) == 0 {
		config.Metadata.OrchestrationUrls = defaultOrchestrationURLs
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
	var inferenceReadyC chan struct{}
	// Only create inferenceReadyC when inference is run as a separate HTTP
	// server. In-process inference is ready as soon as the runtime node returns,
	// before the metadata server is even started.
	if enableInference && !runInferenceInProcess {
		inferenceReadyC = make(chan struct{})
	}

	// Aggregate readiness: health server reports ready once all sub-servers are ready
	ready := &atomic.Bool{}
	go func() {
		<-metadataReadyC
		<-storeReadyC
		if inferenceReadyC != nil {
			<-inferenceReadyC
		}
		ready.Store(true)
		logger.Info("Swarm mode: all servers are ready")
	}()
	if config.HealthEnabled {
		healthserver.Start(logger, config.HealthPort, ready.Load)
	}

	metaConf := &store.StoreInfo{
		ID:      tid,
		RaftURL: viper.GetString("swarm.metadata-raft"),
		ApiURL:  viper.GetString("swarm.metadata-api"),
	}

	storeConf := &store.StoreInfo{
		ID:      tid,
		ApiURL:  viper.GetString("swarm.store-api"),
		RaftURL: viper.GetString("swarm.store-raft"),
	}

	localProvider := metadata.NewDeferredLocalExecutionProvider()

	// If inference is running in-process, construct the node synchronously so
	// we can mount its /ai/v1/ handler on the metadata server. The node
	// holds Pebble resources that need to be closed at shutdown.
	var inferenceNode *inferenceRuntime.TermiteNode
	if runInferenceInProcess {
		inferenceNode = inferenceRuntime.NewTermiteNode(ctx, logger, inferenceConfigWithSecurity(config))
		defer func() {
			if err := inferenceNode.Close(); err != nil {
				logger.Error("failed to close inference node", zap.Error(err))
			}
		}()
		// Point the default inference URL at the metadata listener; the client
		// appends /ai/v1 automatically, and metadata mounts the in-process
		// inference handler under /ai/v1.
		libinference.SetDefaultURL(metaConf.ApiURL)
	}

	runtimeOpts := metadata.RuntimeOptions{
		ExecutionProvider: localProvider,
	}
	if inferenceNode != nil {
		runtimeOpts.InferenceMLHandler = inferenceNode.APIMLHandler()
	}

	metaRuntime, err := metadata.NewRuntime(
		logger.Named("metadataServer"),
		config,
		metaConf,
		peers,
		false,
		cache,
		runtimeOpts,
	)
	if err != nil {
		return fmt.Errorf("creating metadata runtime: %w", err)
	}
	metaRuntime.StartRaft()
	metaRuntime.StartDefaultAdminSeed(ctx)
	defer func() {
		if err := metaRuntime.Close(); err != nil {
			logger.Error("failed to close metadata runtime", zap.Error(err))
		}
	}()

	if enableInference && !runInferenceInProcess {
		go inferenceRuntime.RunAsTermite(ctx, logger, inferenceConfigWithSecurity(config), inferenceReadyC)
		// Wait for inference to finish Pebble initialization before opening store Pebble.
		<-inferenceReadyC
	}

	storeRuntime, err := store.NewRuntime(logger.Named("store"), config, storeConf, cache)
	if err != nil {
		return fmt.Errorf("creating store runtime: %w", err)
	}
	defer func() {
		if err := storeRuntime.Close(); err != nil {
			logger.Error("failed to close store runtime", zap.Error(err))
		}
	}()
	localProvider.BindStore(storeRuntime.Store())

	// Start metadata HTTP server after local bypass is fully bound.
	go func() {
		u, err := url.Parse(metaConf.ApiURL)
		if err != nil {
			logger.Fatal("Error parsing metadata API URL", zap.Error(err))
		}
		srv := metadata.NewAPIServer(u.Host, metaRuntime.HTTPHandler())
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
	<-metadataReadyC
	storeRuntime.StartRaft()
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
