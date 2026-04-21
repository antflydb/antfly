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

// Command termite-operator runs the Kubernetes operator for TermitePool and TermiteRoute CRDs.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/antflydb/antfly/pkg/libaf/logging"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
	"github.com/antflydb/antfly/pkg/termite-operator/controllers"
	webhookv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/internal/webhook/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(antflyaiv1alpha1.AddToScheme(scheme))
}

var (
	cfgFile string
	initErr error // set by initConfig, checked in runOperator
)

func main() {
	// Initialize viper for config file support
	viper.SetEnvPrefix("TERMITE_OPERATOR")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace . with _ in env var names
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("metrics_bind_address", ":8080")
	viper.SetDefault("health_probe_bind_address", ":8081")
	viper.SetDefault("leader_elect", false)
	viper.SetDefault("antfly_image", "antfly/antfly:omni")
	viper.SetDefault("log.level", "info")
	viper.SetDefault("log.style", "json") // JSON for production/k8s

	rootCmd := buildRootCommand()

	cobra.OnInitialize(initConfig)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "termite-operator",
		Short: "Kubernetes operator for TermitePool and TermiteRoute CRDs",
		Long: `Run the Termite Kubernetes operator that manages TermitePool and
TermiteRoute custom resources.

The operator provides:
  - TermitePool: Manage pools of Termite TPU instances with autoscaling
  - TermiteRoute: Configure model-aware routing rules

Examples:
  # Run operator with defaults
  termite-operator

  # Run with custom metrics address
  termite-operator --metrics-bind-address :8080

  # Run with leader election enabled
  termite-operator --leader-elect

  # Run with custom Antfly image
  termite-operator --antfly-image myregistry/antfly:omni

  # Run with debug logging
  termite-operator --log-level debug --log-style terminal`,
		RunE: runOperator,
	}

	// Global flags
	cmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path (e.g. termite-operator.yaml)")
	cmd.PersistentFlags().String("log-level", "info", "set the logging level (debug, info, warn, error)")
	cmd.PersistentFlags().String("log-style", "json", "set the logging output style (terminal, json, logfmt, noop)")

	// Controller-runtime flags
	cmd.Flags().String("metrics-bind-address", ":8080", "The address the metric endpoint binds to")
	cmd.Flags().String("health-probe-bind-address", ":8081", "The address the probe endpoint binds to")
	cmd.Flags().Bool("leader-elect", false, "Enable leader election for controller manager")

	// Operator-specific flags
	cmd.Flags().String("antfly-image", "antfly/antfly:omni", "Default Antfly container image for TermitePool pods")

	// Bind flags to viper
	_ = viper.BindPFlag("log.level", cmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("log.style", cmd.PersistentFlags().Lookup("log-style"))
	_ = viper.BindPFlag("metrics_bind_address", cmd.Flags().Lookup("metrics-bind-address"))
	_ = viper.BindPFlag("health_probe_bind_address", cmd.Flags().Lookup("health-probe-bind-address"))
	_ = viper.BindPFlag("leader_elect", cmd.Flags().Lookup("leader-elect"))
	_ = viper.BindPFlag("antfly_image", cmd.Flags().Lookup("antfly-image"))

	return cmd
}

// initConfig reads in config file and ENV variables if set.
// Errors are stored in initErr rather than calling os.Exit, so that Cobra's
// error propagation is respected (RunE will return the error).
func initConfig() {
	if cfgFile != "" {
		if _, err := os.Stat(cfgFile); err != nil {
			initErr = fmt.Errorf("config file not found: %s", cfgFile)
			return
		}
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		if err == nil {
			viper.AddConfigPath(home)
			viper.SetConfigName(".termite-operator")
		}
		viper.AddConfigPath(".")
		viper.SetConfigName("termite-operator")
	}

	viper.SetConfigType("yaml")

	// If a config file is found, read it in
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintf(os.Stderr, "Using config file: %s\n", viper.ConfigFileUsed())
	} else if cfgFile != "" {
		// Only error if user explicitly specified a config file
		initErr = fmt.Errorf("error reading config file [%s]: %w", viper.ConfigFileUsed(), err)
	}
}

func runOperator(cmd *cobra.Command, args []string) error {
	if initErr != nil {
		return initErr
	}

	metricsAddr := viper.GetString("metrics_bind_address")
	probeAddr := viper.GetString("health_probe_bind_address")
	enableLeaderElection := viper.GetBool("leader_elect")
	antflyImage := viper.GetString("antfly_image")

	// Setup logger using antfly's logging package for consistency
	logCfg := &logging.Config{
		Level: logging.Level(viper.GetString("log.level")),
		Style: logging.Style(viper.GetString("log.style")),
	}
	zapLogger := logging.NewLogger(logCfg)
	defer func() {
		_ = zapLogger.Sync()
	}()

	// Convert zap logger to logr for controller-runtime
	ctrl.SetLogger(zapr.NewLogger(zapLogger))

	mgrOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "termite-operator.antfly.io",
	}

	// Configure webhook server when webhooks are enabled
	if webhooksEnabled() {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port: 9443,
		})
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOpts)
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	// Setup TermitePool controller
	if err := (&controllers.TermitePoolReconciler{
		Client:      mgr.GetClient(),
		Scheme:      mgr.GetScheme(),
		AntflyImage: antflyImage,
		Recorder:    mgr.GetEventRecorder("termitepool-controller"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create TermitePool controller: %w", err)
	}

	// Setup TermiteRoute controller
	if err := (&controllers.TermiteRouteReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorder("termiteroute-controller"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create TermiteRoute controller: %w", err)
	}

	// Setup webhooks
	if webhooksEnabled() {
		if err := webhookv1alpha1.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("unable to create webhooks: %w", err)
		}
	}

	// Setup health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting manager",
		"metricsAddr", metricsAddr,
		"probeAddr", probeAddr,
		"leaderElection", enableLeaderElection,
		"antflyImage", antflyImage,
	)

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}

	return nil
}

// webhooksEnabled returns true only when ENABLE_WEBHOOKS is explicitly set to
// "true" or "1" (case-insensitive). Defaults to false so that local development
// with `make run` works without TLS certs. In-cluster deployments should set
// ENABLE_WEBHOOKS=true in the manager Deployment.
func webhooksEnabled() bool {
	v := strings.ToLower(os.Getenv("ENABLE_WEBHOOKS"))
	return v == "true" || v == "1"
}
