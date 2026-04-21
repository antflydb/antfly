// Copyright 2025 Antfly, Inc.
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
	"fmt"
	"os"
	"strings"

	"github.com/antflydb/antfly/pkg/termite/lib/modelregistry"
	"github.com/antflydb/antfly/pkg/termite/lib/paths"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile     string
	Version     string
	registryURL string
	modelsDir   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "termite",
	Short: "Run as a termite node or manage ONNX models",
	Long: `Start the Termite ML inference service or manage ONNX models
used for embeddings, chunking, and reranking.

Examples:
  # Run termite server
  termite run

  # List available models (local and remote)
  termite list
  termite list --remote

  # Pull a model from the registry
  termite pull bge-small-en-v1.5
  termite pull --variants i8 mxbai-rerank-base-v1`,
	// Default behavior when no subcommand is provided: run the server
	RunE: runServer,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	rootCmd.Version = Version
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().
		StringVar(&cfgFile, "config", "", "config file path (e.g. termite.yaml)")
	rootCmd.PersistentFlags().
		String("log-level", "info", "set the logging level (e.g. debug, info, warn, error)")
	rootCmd.PersistentFlags().
		String("log-style", "terminal", "set the logging output style (terminal, json, noop); defaults to json in Kubernetes")
	rootCmd.PersistentFlags().
		StringVar(&registryURL, "registry", modelregistry.DefaultRegistryURL, "Model registry URL")
	rootCmd.PersistentFlags().
		StringVar(&modelsDir, "models-dir", paths.DefaultModelsDir(), "Directory for storing models (default: ~/.termite/models)")

	// Bind to viper
	mustBindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	mustBindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))
	mustBindPFlag("log.style", rootCmd.PersistentFlags().Lookup("log-style"))

	// Default values
	viper.SetDefault("api_url", "http://localhost:11433")
	viper.SetDefault("models_dir", paths.DefaultModelsDir())
	viper.SetDefault("health_port", 4200)
	viper.SetDefault("log.level", "info")
	// Default to JSON logging in Kubernetes for structured log aggregation
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		viper.SetDefault("log.style", "json")
	} else {
		viper.SetDefault("log.style", "logfmt")
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		if _, err := os.Stat(cfgFile); err != nil {
			fmt.Fprintf(os.Stderr, "Config file not found: %s\n", cfgFile)
			os.Exit(1)
		}

		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search for config file in home directory and current directory
		home, err := os.UserHomeDir()
		if err == nil {
			viper.AddConfigPath(home)
			viper.SetConfigName(".termite")
		}
		viper.AddConfigPath(".")
		viper.SetConfigName("termite")
	}

	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("TERMITE")                          // TERMITE_ prefix for env vars
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace . with _ in env var names
	viper.AutomaticEnv()                                   // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintf(os.Stderr, "Using config file: %s\n", viper.ConfigFileUsed())
	} else if cfgFile != "" {
		// Only error if user explicitly specified a config file
		fmt.Fprintf(os.Stderr, "Error reading config file [%s]: %v\n", viper.ConfigFileUsed(), err)
		os.Exit(1)
	}
}
