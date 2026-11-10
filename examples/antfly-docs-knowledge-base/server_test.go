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

import "testing"

// TestServerIndexConfigsUsePublicShape checks that server mode sends the same
// index configs as Lite, unwrapped into the public create-table shape.
func TestServerIndexConfigsUsePublicShape(t *testing.T) {
	cfg := indexBuildConfig{
		EmbedModel:    defaultEmbedModel,
		ExtractModel:  defaultExtractModel,
		TargetTokens:  defaultTargetTokens,
		OverlapTokens: defaultOverlapTokens,
		Metrics:       true,
	}
	indexes, err := serverIndexConfigs(cfg)
	if err != nil {
		t.Fatal(err)
	}
	vectors, ok := indexes[chunkVectorsIndex].(map[string]any)
	if !ok || vectors["type"] != "embeddings" {
		t.Fatalf("%s config = %#v, want type embeddings", chunkVectorsIndex, indexes[chunkVectorsIndex])
	}
	graph, ok := indexes[knowledgeGraphIndex].(map[string]any)
	if !ok || graph["type"] != "graph" {
		t.Fatalf("%s config = %#v, want type graph", knowledgeGraphIndex, indexes[knowledgeGraphIndex])
	}
	if _, ok := graph["resolvers"]; !ok {
		t.Fatalf("%s config has no resolvers", knowledgeGraphIndex)
	}
}
