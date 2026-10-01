// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "0.1.0"

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "evalaf",
	Short: "Evalaf - Embeddable LLM/RAG/Agent Evaluation Framework",
	Long: `Evalaf is a comprehensive evaluation framework for LLM applications.

It supports evaluating:
- RAG systems (faithfulness, relevance, retrieval quality, citations)
- Agents (classification, tool selection, reasoning)
- General LLM outputs (correctness, safety, quality)

Use evalaf to run evaluations, manage datasets, and monitor LLM quality.`,
	Version: version,
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(datasetsCmd)
	rootCmd.AddCommand(metricsCmd)
}
