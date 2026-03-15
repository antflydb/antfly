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

////go:generate sh -c "echo 'package cmd\n\nconst Version = \"'$(git describe --always --long --tags 2>/dev/null || echo 'dev')'\"' > version.go"

import (
	"fmt"
	"os"
	"strings"

	"github.com/antflydb/antfly/cmd/antfly/cmd/cli"
	"github.com/antflydb/antfly/lib/secrets"
	"github.com/antflydb/antfly/src/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	Version string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "antfly",
	Short: "Antfly global indexing layer",
	Long: `Antfly is a distributed database system that can run in multiple modes:

- store: Run as a data storage node (default)
- metadata: Run as a metadata server to manage cluster metadata
- swarm: Run as both metadata and raft services
- termite: Run in termite mode for specialized operations

Each mode has its own set of configuration options. Use 'antfly [mode] --help' for mode-specific options.`,
	SilenceUsage: true,
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

	// Register CLI subcommands directly on root:
	// antfly query, antfly table, antfly load, etc.
	cli.RegisterCommands(rootCmd)

	// Global flags
	rootCmd.PersistentFlags().
		StringVar(&cfgFile, "config", "", "config file path (e.g. config.json)")
	rootCmd.PersistentFlags().
		String("log-level", "info", "set the logging level (e.g. debug, info, warn, error)")
	rootCmd.PersistentFlags().
		String("log-style", "logfmt", "set the logging output style (logfmt, terminal, json, noop); defaults to json in Kubernetes")
	rootCmd.PersistentFlags().
		String("data-dir", common.DefaultDataDir(), "root directory for all antfly data storage (default: ~/.antfly)")
	rootCmd.PersistentFlags().
		String("keystore-path", secrets.DefaultKeystorePath, "path to encrypted keystore file")
	rootCmd.PersistentFlags().
		String("keystore-password", "", "keystore password (prefer ANTFLY_KEYSTORE_PASSWORD env var — flags are visible in ps output)")

	// Bind to viper
	mustBindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))
	mustBindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	mustBindPFlag("log_style", rootCmd.PersistentFlags().Lookup("log-style"))
	mustBindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	mustBindPFlag("keystore_path", rootCmd.PersistentFlags().Lookup("keystore-path"))
	mustBindPFlag("keystore_password", rootCmd.PersistentFlags().Lookup("keystore-password"))

	// Default values
	viper.SetDefault("data_dir", common.DefaultDataDir())
	viper.SetDefault("log_level", "info")
	// Default to JSON logging in Kubernetes for structured log aggregation
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		viper.SetDefault("log_style", "json")
	} else {
		viper.SetDefault("log_style", "logfmt")
	}

	// Content security defaults
	viper.SetDefault("content_security.block_private_ips", true)
	viper.SetDefault("content_security.max_download_size_bytes", 100*1024*1024) // 100MB
	viper.SetDefault("content_security.download_timeout_seconds", 30)
	viper.SetDefault("content_security.max_image_dimension", 2048)

	// Termite defaults
	viper.SetDefault("termite.models_dir", common.DefaultModelsDir())
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Initialize keystore early (before config is read)
	if err := secrets.InitKeystoreFromEnv(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to load keystore: %v\n", err)
		fmt.Fprintf(os.Stderr, "Continuing with environment variables only\n")
	}

	if cfgFile != "" {
		if _, err := os.Stat(cfgFile); err != nil {
			fmt.Fprintf(os.Stderr, "Config file not found: %s\n", cfgFile)
			os.Exit(1)
		}

		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	}

	viper.SetEnvPrefix("ANTFLY")                           // ANTFLY_ prefix for env vars
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace . with _ in env var names
	viper.AutomaticEnv()                                   // read in environment variables that match

	// If a config file is found, read it in.
	if cfgFile != "" {
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(
				os.Stderr,
				"Error reading config file:[%v] error:%v\n",
				viper.ConfigFileUsed(),
				err,
			)
		} else {
			_, _ = fmt.Fprintf(os.Stdout, "Using config file:[%v]\n", viper.ConfigFileUsed())
		}

		// Resolve ${secret:...} references in config
		if err := secrets.ResolveViperSecrets(viper.GetViper()); err != nil {
			fmt.Fprintf(os.Stderr, "Error resolving secrets in config: %v\n", err)
			os.Exit(1)
		}
	}
}
