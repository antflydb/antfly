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
	"github.com/antflydb/antfly/pkg/termite/lib/cli"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available ONNX models",
	Long: `List ONNX models available locally or from the remote registry.

By default, shows locally installed models. Use --remote to show models
available for download from the registry.

Examples:
  # List local models
  termite list

  # List remote models available for download
  termite list --remote

  # Filter by model type
  termite list --type embedder`,
	RunE: runList,
}

func init() {
	rootCmd.AddCommand(listCmd)

	// List command flags
	listCmd.Flags().Bool("remote", false, "List models from remote registry")
	listCmd.Flags().String("type", "", "Filter by model type (embedder, chunker, reranker, generator, recognizer, rewriter)")
}

func runList(cmd *cobra.Command, args []string) error {
	remote, _ := cmd.Flags().GetBool("remote")
	typeFilter, _ := cmd.Flags().GetString("type")

	opts := cli.ListOptions{
		RegistryURL: registryURL,
		ModelsDir:   modelsDir,
		TypeFilter:  typeFilter,
		BinaryName:  "termite",
	}

	if remote {
		return cli.ListRemoteModels(opts)
	}
	return cli.ListLocalModels(opts)
}
