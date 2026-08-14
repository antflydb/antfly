// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/antflydb/antfly/go/pkg/libaf/healthserver"
	"github.com/antflydb/antfly/go/pkg/libaf/logging"
	"github.com/antflydb/antfly/go/pkg/termite"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var healthPort int

const (
	canonicalInferenceAdmissionKey = "admission.inference.max_concurrent_requests"
	legacyInferenceAdmissionKey    = "max_concurrent_requests"
)

func resolvedInferenceMaxConcurrentRequests() (int, error) {
	canonical := viper.GetInt(canonicalInferenceAdmissionKey)
	legacy := viper.GetInt(legacyInferenceAdmissionKey)
	_, canonicalEnv := os.LookupEnv("TERMITE_ADMISSION_INFERENCE_MAX_CONCURRENT_REQUESTS")
	_, legacyEnv := os.LookupEnv("TERMITE_MAX_CONCURRENT_REQUESTS")
	return resolveInferenceAdmissionValues(
		canonical,
		canonicalEnv || viper.InConfig(canonicalInferenceAdmissionKey),
		legacy,
		legacyEnv || viper.InConfig(legacyInferenceAdmissionKey),
	)
}

func resolveInferenceAdmissionValues(canonical int, canonicalExplicit bool, legacy int, legacyExplicit bool) (int, error) {
	if canonicalExplicit && canonical < 0 {
		return 0, fmt.Errorf("%s must be non-negative", canonicalInferenceAdmissionKey)
	}
	if legacyExplicit && legacy < 0 {
		return 0, fmt.Errorf("deprecated %s must be non-negative", legacyInferenceAdmissionKey)
	}
	if canonicalExplicit && legacyExplicit && canonical != legacy {
		return 0, fmt.Errorf(
			"conflicting %s (%d) and deprecated %s (%d)",
			canonicalInferenceAdmissionKey, canonical, legacyInferenceAdmissionKey, legacy,
		)
	}
	if legacyExplicit && !canonicalExplicit {
		return legacy, nil
	}
	return canonical, nil
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the termite server",
	Long:  `Start the termite server for ML operations (embeddings, chunking, reranking).`,
	RunE:  runServer,
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Run command flags
	runCmd.Flags().IntVar(&healthPort, "health-port", 4200, "health/metrics server port")
	mustBindPFlag("health_port", runCmd.Flags().Lookup("health-port"))
}

func runServer(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Create logger from config
	logger := logging.NewLogger(&logging.Config{
		Level: logging.Level(viper.GetString("log.level")),
		Style: logging.Style(viper.GetString("log.style")),
	})
	defer func() {
		_ = logger.Sync()
	}()

	logger.Info("Running as termite")

	// Build termite config from viper/env
	maxConcurrentRequests, err := resolvedInferenceMaxConcurrentRequests()
	if err != nil {
		return err
	}
	cfg := termite.Config{
		ApiUrl:          viper.GetString("api_url"),
		ModelsDir:       modelsDir, // Set from --models-dir flag (defaults to ~/.termite/models)
		BackendPriority: viper.GetStringSlice("backend_priority"),
		KeepAlive:       viper.GetString("keep_alive"),
		MaxLoadedModels: viper.GetInt("max_loaded_models"),
		MaxMemoryMb:     viper.GetInt("max_memory_mb"),
		Preload:         preloadModelRefs(viper.GetStringSlice("preload")),
		Admission: termite.AdmissionConfig{
			Inference: termite.RequestAdmissionConfig{
				MaxConcurrentRequests: &maxConcurrentRequests,
			},
		},
	}

	// Parse model_strategies from config (map[string]string -> map[string]ConfigModelStrategies)
	if rawStrategies := viper.GetStringMapString("model_strategies"); len(rawStrategies) > 0 {
		cfg.ModelStrategies = make(map[string]termite.ConfigModelStrategies, len(rawStrategies))
		for model, strategy := range rawStrategies {
			cfg.ModelStrategies[model] = termite.ConfigModelStrategies(strategy)
		}
	}

	// Track readiness state
	ready := &atomic.Bool{}
	ready.Store(false)
	readyC := make(chan struct{})

	// Start health server with readiness checker
	healthserver.Start(logger, viper.GetInt("health_port"), ready.Load)

	// Wait for ready signal in background
	go func() {
		<-readyC
		ready.Store(true)
		logger.Info("Termite is ready")
	}()

	termite.RunAsTermite(ctx, logger, cfg, readyC)
	return nil
}

func preloadModelRefs(names []string) []termite.ModelRef {
	refs := make([]termite.ModelRef, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		refs = append(refs, termite.ModelRef{
			Kind: termite.ModelKindEmbedder,
			Name: name,
		})
	}
	return refs
}
