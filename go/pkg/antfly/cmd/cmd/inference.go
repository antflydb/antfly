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

	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	inferenceRuntime "github.com/antflydb/antfly/go/pkg/termite"
	"github.com/antflydb/antfly/go/pkg/termite/lib/cli"
	"github.com/antflydb/antfly/go/pkg/termite/lib/modelregistry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	registryURL       string
	modelsDir         string
	inferenceVariants []string
)

var inferenceCmd = &cobra.Command{
	Use:   "inference",
	Short: "Run inference or manage ONNX models",
	Long: `Start Antfly inference for model operations,
or manage ONNX models used for embeddings, chunking, and reranking.

Examples:
  # Run inference server
  antfly inference run

  # List available models (local and remote)
  antfly inference list
  antfly inference list --remote

  # Pull a model from the registry
  antfly inference pull BAAI/bge-small-en-v1.5
  antfly inference pull --variants i8 mixedbread-ai/mxbai-rerank-base-v1`,
	// Default behavior when no subcommand is provided: run the server
	RunE: runInference,
}

var inferenceRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the inference server",
	Long:  `Start the inference server for ML operations (embeddings, chunking, reranking).`,
	RunE:  runInference,
}

var inferencePullCmd = &cobra.Command{
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
  antfly inference pull BAAI/bge-small-en-v1.5

  # Pull only INT8 variant (smaller download)
  antfly inference pull BAAI/bge-small-en-v1.5:i8

  # Pull multiple variants
  antfly inference pull --variants f16,i8 BAAI/bge-small-en-v1.5

  # Pull multiple models with same variant
  antfly inference pull --variants i8 BAAI/bge-small-en-v1.5 mixedbread-ai/mxbai-rerank-base-v1

  # Pull to a custom directory
  antfly inference pull --models-dir /opt/antfly/models BAAI/bge-small-en-v1.5

  # Pull directly from HuggingFace (auto-detects generator type)
  antfly inference pull hf:onnxruntime/Gemma-3-ONNX

  # Pull from HuggingFace with explicit type
  antfly inference pull hf:onnx-community/embeddinggemma-300m-ONNX --type embedder`,
	Args: cobra.MinimumNArgs(1),
	RunE: runInferencePull,
}

var inferenceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available ONNX models",
	Long: `List ONNX models available locally or from the remote registry.

By default, shows locally installed models. Use --remote to show models
available for download from the registry.

Examples:
  # List local models
  antfly inference list

  # List remote models available for download
  antfly inference list --remote

  # Filter by model type
  antfly inference list --type embedder`,
	RunE: runInferenceList,
}

func init() {
	rootCmd.AddCommand(inferenceCmd)

	// Add subcommands
	inferenceCmd.AddCommand(inferenceRunCmd)
	inferenceCmd.AddCommand(inferencePullCmd)
	inferenceCmd.AddCommand(inferenceListCmd)

	// Run command flags
	inferenceRunCmd.Flags().Bool("health", true, "enable health/metrics server")
	inferenceRunCmd.Flags().Int("health-port", 4200, "health/metrics server port")
	mustBindPFlag("health_enabled", inferenceRunCmd.Flags().Lookup("health"))
	mustBindPFlag("health_port", inferenceRunCmd.Flags().Lookup("health-port"))

	// Allow `antfly inference --health-port` with the default run behavior.
	inferenceCmd.Flags().Bool("health", true, "enable health/metrics server")
	inferenceCmd.Flags().Int("health-port", 4200, "health/metrics server port")
	mustBindPFlag("health_enabled", inferenceCmd.Flags().Lookup("health"))
	mustBindPFlag("health_port", inferenceCmd.Flags().Lookup("health-port"))

	// Persistent flags for model management (shared across pull/list)
	inferenceCmd.PersistentFlags().StringVar(&registryURL, "registry", modelregistry.DefaultRegistryURL,
		"Model registry URL")
	inferenceCmd.PersistentFlags().StringVar(&modelsDir, "models-dir", common.DefaultModelsDir(),
		"Directory for storing models (default: ~/.antfly/inference/models)")

	// Pull command flags
	inferencePullCmd.Flags().StringSliceVar(&inferenceVariants, "variants", nil,
		"Variant IDs to download (f32,f16,i8,i8-st,i4). Defaults to f32 if not specified.")
	inferencePullCmd.Flags().String("type", "",
		"Model type (embedder, chunker, reranker, generator, recognizer, rewriter) - auto-detected for generators")
	inferencePullCmd.Flags().String("hf-token", "",
		"HuggingFace API token for gated models (or use HF_TOKEN env var)")
	inferencePullCmd.Flags().String("variant", "",
		"ONNX variant for HuggingFace models (fp16, q4, q4f16, quantized)")

	// List command flags
	inferenceListCmd.Flags().Bool("remote", false, "List models from remote registry")
	inferenceListCmd.Flags().String("type", "", "Filter by model type (embedder, chunker, reranker, generator, recognizer, rewriter)")
}

func runInference(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	config, err := parseConfigWithOptions(viper.GetViper(), parseConfigOptions{
		RequireMetadata:        false,
		DefaultInferenceAPIURL: defaultInferenceAPIURL,
	})
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	logger := getLogger(config)
	defer func() { _ = logger.Sync() }()

	logger.Info("Running as inference")

	readyC := make(chan struct{})
	if config.HealthEnabled {
		startHealthServer(logger, config.HealthPort, readyC, "Inference")
	}

	inferenceRuntime.RunAsTermite(ctx, logger, inferenceConfigWithSecurity(config), readyC)
	return nil
}

func runInferencePull(cmd *cobra.Command, args []string) error {
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
			Variants:    inferenceVariants,
		}); err != nil {
			return fmt.Errorf("failed to pull %s: %w", modelRef, err)
		}
	}

	return nil
}

func runInferenceList(cmd *cobra.Command, args []string) error {
	remote, _ := cmd.Flags().GetBool("remote")
	typeFilter, _ := cmd.Flags().GetString("type")

	opts := cli.ListOptions{
		RegistryURL: registryURL,
		ModelsDir:   modelsDir,
		TypeFilter:  typeFilter,
		BinaryName:  "antfly inference",
	}

	if remote {
		return cli.ListRemoteModels(opts)
	}
	return cli.ListLocalModels(opts)
}
