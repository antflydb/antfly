// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command termite-proxy runs the model-aware routing proxy for Termite TPU instances.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/pkg/libaf/logging"
	"github.com/antflydb/antfly/pkg/termite-proxy"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	// Initialize viper for config file support
	viper.SetEnvPrefix("TERMITE_PROXY")
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("listen", ":8080")
	viper.SetDefault("health_port", 4200)
	viper.SetDefault("default_pool", "default")
	viper.SetDefault("refresh_interval", "10s")
	viper.SetDefault("namespace", "")
	viper.SetDefault("selector", "app.kubernetes.io/name=termite")
	viper.SetDefault("log.level", "info")
	// Default to JSON logging in Kubernetes for structured log aggregation
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		viper.SetDefault("log.style", "json")
	} else {
		viper.SetDefault("log.style", "terminal")
	}

	rootCmd := buildRootCommand()

	// Add config file flag
	rootCmd.PersistentFlags().String("config", "", "config file (default: $HOME/.termite-proxy.yaml)")
	cobra.OnInitialize(func() {
		cfgFile, _ := rootCmd.PersistentFlags().GetString("config")
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
		} else {
			home, err := os.UserHomeDir()
			if err == nil {
				viper.AddConfigPath(home)
				viper.SetConfigName(".termite-proxy")
			}
			viper.AddConfigPath(".")
			viper.SetConfigName("termite-proxy")
		}
		viper.SetConfigType("yaml")
		// Silently ignore if config file doesn't exist
		_ = viper.ReadInConfig()
	})

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "termite-proxy",
		Short: "Model-aware routing proxy for Termite TPU instances",
		Long: `Start the Termite proxy server that routes requests to appropriate
Termite instances based on loaded models and endpoint health.

The proxy provides:
  - Model-aware routing (route to endpoints with model already loaded)
  - Consistent hashing for read-heavy workloads
  - Least-loaded routing for write-heavy workloads
  - Circuit breaker pattern for resilience
  - Prometheus metrics for autoscaling (KEDA compatible)

Examples:
  # Run proxy with defaults
  termite-proxy

  # Run with custom listen address and health port
  termite-proxy --listen :8080 --health-port 4200

  # Run with Kubernetes watcher
  termite-proxy --namespace my-namespace --selector app=termite`,
		RunE: runProxy,
	}

	// Server flags
	cmd.Flags().String("listen", ":8080", "Address to listen on for API requests")
	cmd.Flags().Int("health-port", 4200, "Health/readiness/metrics server port")
	cmd.Flags().String("default-pool", "default", "Default pool for routing")
	cmd.Flags().Duration("refresh-interval", 10*time.Second, "Interval to refresh endpoint models")

	// Kubernetes flags
	cmd.Flags().String("kubeconfig", "", "Path to kubeconfig (uses in-cluster config if empty)")
	cmd.Flags().String("namespace", "", "Namespace to watch (empty for all namespaces)")
	cmd.Flags().String("selector", "app.kubernetes.io/name=termite", "Label selector for Termite pods")

	// Route watching flags
	cmd.Flags().Bool("enable-route-watching", true, "Enable watching TermiteRoute CRs for routing rules")
	cmd.Flags().String("route-namespace", "", "Namespace to watch for TermiteRoutes (empty for all)")

	// Logging flags
	cmd.Flags().String("log-level", "info", "Log level (debug, info, warn, error)")
	cmd.Flags().String("log-style", "terminal", "Log style (terminal, json, noop); defaults to json in Kubernetes")

	// Bind flags to viper
	mustBindFlag(cmd, "listen", "listen")
	mustBindFlag(cmd, "health-port", "health_port")
	mustBindFlag(cmd, "default-pool", "default_pool")
	mustBindFlag(cmd, "refresh-interval", "refresh_interval")
	mustBindFlag(cmd, "kubeconfig", "kubeconfig")
	mustBindFlag(cmd, "namespace", "namespace")
	mustBindFlag(cmd, "selector", "selector")
	mustBindFlag(cmd, "enable-route-watching", "enable_route_watching")
	mustBindFlag(cmd, "route-namespace", "route_namespace")
	mustBindFlag(cmd, "log-level", "log.level")
	mustBindFlag(cmd, "log-style", "log.style")

	return cmd
}

func mustBindFlag(cmd *cobra.Command, flagName, viperKey string) {
	if err := viper.BindPFlag(viperKey, cmd.Flags().Lookup(flagName)); err != nil {
		panic(err)
	}
}

func runProxy(cmd *cobra.Command, args []string) error {
	// Create logger from config
	logger := logging.NewLogger(&logging.Config{
		Level: logging.Level(viper.GetString("log.level")),
		Style: logging.Style(viper.GetString("log.style")),
	})
	defer func() { _ = logger.Sync() }()

	listenAddr := viper.GetString("listen")
	healthPort := viper.GetInt("health_port")
	defaultPool := viper.GetString("default_pool")
	refreshInterval := viper.GetDuration("refresh_interval")
	kubeconfig := viper.GetString("kubeconfig")
	namespace := viper.GetString("namespace")
	labelSelector := viper.GetString("selector")
	enableRouteWatching := viper.GetBool("enable_route_watching")
	routeNamespace := viper.GetString("route_namespace")

	// Determine if we're running in Kubernetes
	inKubernetes := kubeconfig != "" || os.Getenv("KUBERNETES_SERVICE_HOST") != ""

	// Create proxy
	cfg := proxy.Config{
		ListenAddr:           listenAddr,
		DefaultPool:          defaultPool,
		RefreshInterval:      refreshInterval,
		EnableRouteWatching:  enableRouteWatching && inKubernetes,
		RouteWatchNamespace:  routeNamespace,
		RouteWatchKubeconfig: kubeconfig,
		Logger:               logger,
	}
	p := proxy.NewProxy(cfg)

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start Kubernetes watcher if configured
	if inKubernetes {
		watcher, err := proxy.NewK8sWatcher(p, proxy.K8sWatcherConfig{
			Kubeconfig:    kubeconfig,
			Namespace:     namespace,
			LabelSelector: labelSelector,
		})
		if err != nil {
			logger.Fatal("failed to create k8s watcher", zap.Error(err))
		}
		go func() {
			if err := watcher.Start(ctx); err != nil {
				logger.Error("k8s watcher error", zap.Error(err))
			}
		}()
		logger.Info("kubernetes watcher started",
			zap.String("namespace", namespace),
			zap.String("selector", labelSelector),
			zap.Bool("route_watching", enableRouteWatching),
		)
	} else {
		logger.Info("running without kubernetes watcher (use --kubeconfig or run in-cluster)")
	}

	// Start health server with readiness checker that queries proxy's ready state
	readyChecker := func() bool {
		// Proxy is ready if it has at least one healthy endpoint
		p.Registry().GetLock().RLock()
		defer p.Registry().GetLock().RUnlock()
		for _, ep := range p.Registry().GetEndpoints() {
			if ep.Healthy {
				return true
			}
		}
		// Also consider ready if we're running without k8s watcher (static config)
		return !inKubernetes
	}
	healthserver.Start(logger, healthPort, readyChecker)

	// Start proxy
	logger.Info("starting proxy",
		zap.String("listen", listenAddr),
		zap.Int("health_port", healthPort),
	)

	if err := p.Start(ctx); err != nil {
		logger.Error("proxy error", zap.Error(err))
		return err
	}

	return nil
}
