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

	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/pkg/termite"
	"github.com/antflydb/antfly/pkg/termite/lib/cli"
	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	registryURL     string
	modelsDir       string
	termiteVariants []string
)

var termiteCmd = &cobra.Command{
	Use:   "termite",
	Short: "Run as a termite node or manage ONNX models",
	Long: `Start the AntFly database in termite mode for specialized operations,
or manage ONNX models used for embeddings, chunking, and reranking.

Examples:
  # Run termite server
  antfly termite run

  # List available models (local and remote)
  antfly termite list
  antfly termite list --remote

  # Pull a model from the registry
  antfly termite pull BAAI/bge-small-en-v1.5
  antfly termite pull --variants i8 mixedbread-ai/mxbai-rerank-base-v1`,
	// Default behavior when no subcommand is provided: run the server
	RunE: runTermite,
}

var termiteRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the termite server",
	Long:  `Start the termite server for ML operations (embeddings, chunking, reranking).`,
	RunE:  runTermite,
}

var termitePullCmd = &cobra.Command{
	Use:   "pull <owner/model-name> [owner/model-name...]",
	Short: "Pull ONNX model(s) from the registry",
	Long: `Download one or more ONNX models from the Antfly model registry or HuggingFace.

Model names use owner/model format (like HuggingFace and Ollama):
  - BAAI/bge-small-en-v1.5
  - mixedbread-ai/mxbai-rerank-base-v1

Models are downloaded to the appropriate directory based on their type:
  - Embedders:    models/embedders/<owner>/<model-name>/
  - Chunkers:     models/chunkers/<owner>/<model-name>/
  - Rerankers:    models/rerankers/<owner>/<model-name>/
  - Generators:   models/generators/<owner>/<model-name>/
  - Recognizers:  models/recognizers/<owner>/<model-name>/
  - Rewriters:    models/rewriters/<owner>/<model-name>/

Variants (append :variant to model name, e.g., BAAI/bge-small-en-v1.5:i8):
  f32     - FP32 baseline (default, highest accuracy)
  f16     - FP16 half precision (~50% smaller)
  i8      - INT8 dynamic quantization (smallest, fastest CPU)
  i8-st   - INT8 static quantization (calibrated)
  i4      - INT4 quantization

Examples:
  # Pull default FP32 model
  antfly termite pull BAAI/bge-small-en-v1.5

  # Pull only INT8 variant (smaller download)
  antfly termite pull BAAI/bge-small-en-v1.5:i8

  # Pull multiple variants
  antfly termite pull --variants f16,i8 BAAI/bge-small-en-v1.5

  # Pull multiple models with same variant
  antfly termite pull --variants i8 BAAI/bge-small-en-v1.5 mixedbread-ai/mxbai-rerank-base-v1

  # Pull to a custom directory
  antfly termite pull --models-dir /opt/antfly/models BAAI/bge-small-en-v1.5

  # Pull directly from HuggingFace (auto-detects generator type)
  antfly termite pull hf:onnxruntime/Gemma-3-ONNX

  # Pull from HuggingFace with explicit type
  antfly termite pull hf:onnx-community/embeddinggemma-300m-ONNX --type embedder`,
	Args: cobra.MinimumNArgs(1),
	RunE: runTermitePull,
}

var termiteListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available ONNX models",
	Long: `List ONNX models available locally or from the remote registry.

By default, shows locally installed models. Use --remote to show models
available for download from the registry.

Examples:
  # List local models
  antfly termite list

  # List remote models available for download
  antfly termite list --remote

  # Filter by model type
  antfly termite list --type embedder`,
	RunE: runTermiteList,
}

func init() {
	rootCmd.AddCommand(termiteCmd)

	// Add subcommands
	termiteCmd.AddCommand(termiteRunCmd)
	termiteCmd.AddCommand(termitePullCmd)
	termiteCmd.AddCommand(termiteListCmd)

	// Run command flags
	termiteRunCmd.Flags().Int("health-port", 4200, "health/metrics server port")
	mustBindPFlag("health_port", termiteRunCmd.Flags().Lookup("health-port"))

	// Also bind to parent for backward compatibility (antfly termite --health-port)
	termiteCmd.Flags().Int("health-port", 4200, "health/metrics server port")
	mustBindPFlag("health_port", termiteCmd.Flags().Lookup("health-port"))

	// Persistent flags for model management (shared across pull/list)
	termiteCmd.PersistentFlags().StringVar(&registryURL, "registry", modelregistry.DefaultRegistryURL,
		"Model registry URL")
	termiteCmd.PersistentFlags().StringVar(&modelsDir, "models-dir", common.DefaultModelsDir(),
		"Directory for storing models (default: ~/.termite/models)")

	// Pull command flags
	termitePullCmd.Flags().StringSliceVar(&termiteVariants, "variants", nil,
		"Variant IDs to download (f32,f16,i8,i8-st,i4). Defaults to f32 if not specified.")
	termitePullCmd.Flags().String("type", "",
		"Model type (embedder, chunker, reranker, generator, recognizer, rewriter) - auto-detected for generators")
	termitePullCmd.Flags().String("hf-token", "",
		"HuggingFace API token for gated models (or use HF_TOKEN env var)")
	termitePullCmd.Flags().String("variant", "",
		"ONNX variant for HuggingFace models (fp16, q4, q4f16, quantized)")

	// List command flags
	termiteListCmd.Flags().Bool("remote", false, "List models from remote registry")
	termiteListCmd.Flags().String("type", "", "Filter by model type (embedder, chunker, reranker, generator, recognizer, rewriter)")
}

func runTermite(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	config, err := parseConfig(viper.GetViper())
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	logger := getLogger(config)
	defer func() { _ = logger.Sync() }()

	logger.Info("Running as termite")

	readyC := make(chan struct{})
	startHealthServer(logger, config.HealthPort, readyC, "Termite")

	termite.RunAsTermite(ctx, logger, termiteConfigWithSecurity(config), readyC)
	return nil
}

func runTermitePull(cmd *cobra.Command, args []string) error {
	modelTypeStr, _ := cmd.Flags().GetString("type")
	hfToken, _ := cmd.Flags().GetString("hf-token")
	variant, _ := cmd.Flags().GetString("variant")

	for _, modelRef := range args {
		fmt.Printf("\n=== Pulling %s ===\n", modelRef)

		// Check for hf: prefix to route to HuggingFace
		if repoID, isHF := modelregistry.ParseHuggingFaceRef(modelRef); isHF {
			if err := cli.PullFromHuggingFace(repoID, cli.HuggingFaceOptions{
				ModelsDir: modelsDir,
				ModelType: modelTypeStr,
				HFToken:   hfToken,
				Variant:   variant,
			}); err != nil {
				return fmt.Errorf("failed to pull %s: %w", modelRef, err)
			}
			continue
		}

		// Standard registry pull
		if err := cli.PullFromRegistry(modelRef, cli.PullOptions{
			RegistryURL: registryURL,
			ModelsDir:   modelsDir,
			Variants:    termiteVariants,
		}); err != nil {
			return fmt.Errorf("failed to pull %s: %w", modelRef, err)
		}
	}

	return nil
}

func runTermiteList(cmd *cobra.Command, args []string) error {
	remote, _ := cmd.Flags().GetBool("remote")
	typeFilter, _ := cmd.Flags().GetString("type")

	opts := cli.ListOptions{
		RegistryURL: registryURL,
		ModelsDir:   modelsDir,
		TypeFilter:  typeFilter,
		BinaryName:  "antfly termite",
	}

	if remote {
		return cli.ListRemoteModels(opts)
	}
	return cli.ListLocalModels(opts)
}
