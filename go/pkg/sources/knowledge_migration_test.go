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

package sources

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKnowledgeRecipeIdentityAndExactIntegers(t *testing.T) {
	p := KnowledgePreset{Name: "facts", SourceID: testSourceID, Audience: "research", Field: "facts", EntityTable: "entities", Resolver: map[string]any{"config_generation": uint64(9007199254740993), "labels": []string{"person"}}}
	first, k1, a1, err := BuildKnowledgeProcessing(p)
	require.NoError(t, err)
	r1 := first.Indexes[0].Config["resolvers"].([]any)[0].(map[string]any)
	require.Equal(t, json.Number("9007199254740993"), r1["config_generation"])
	require.Equal(t, uint64(9007199254740993), p.Resolver["config_generation"])
	cloned := cloneProfile(Profile{Sources: []Source{{Processing: &first}}})
	clonedResolver := cloned.Sources[0].Processing.Indexes[0].Config["resolvers"].([]any)[0].(map[string]any)
	require.Equal(t, json.Number("9007199254740993"), clonedResolver["config_generation"])
	p.Resolver["config_generation"] = uint64(9007199254740994)
	second, k2, a2, err := BuildKnowledgeProcessing(p)
	require.NoError(t, err)
	require.Equal(t, k1.Entities[0].Namespace, k2.Entities[0].Namespace)
	require.Equal(t, a1.Artifact, a2.Artifact, "resolver migrations reuse expensive extraction")
	require.NotEqual(t, k1.Entities[0].Resolver, k2.Entities[0].Resolver)
	require.NotEqual(t, k1.Entities[0].SourceArtifact, k2.Entities[0].SourceArtifact, "native label routing has an independent stream")
	require.NotEqual(t, first.Indexes[0].Name, second.Indexes[0].Name)
	raw, err := json.Marshal(map[string]any{"config": first.Indexes[0].Config})
	require.NoError(t, err)
	installed, err := decodeSearchIndex(raw)
	require.NoError(t, err)
	require.True(t, containsNativeConfig(installed.Config, first.Indexes[0].Config))
	p.Field = "facts_v2"
	_, k3, a3, err := BuildKnowledgeProcessing(p)
	require.NoError(t, err)
	require.NotEqual(t, a2.Artifact, a3.Artifact)
	require.NotEqual(t, k2.Entities[0].Resolver, k3.Entities[0].Resolver)
}

func TestKnowledgeInstalledResolversCannotBeHidden(t *testing.T) {
	for _, mode := range []string{"omitted", "not-selected", "extra-native", "wrong-target"} {
		t.Run(mode, func(t *testing.T) {
			plan, k := knowledgeFixture(t)
			switch mode {
			case "omitted":
				k.Entities = nil
				k.Graphs[0].Entities = nil
			case "not-selected":
				k.Graphs[0].Entities = nil
			case "extra-native":
				values := plan.Indexes[0].Config["resolvers"].([]any)
				extra := map[string]any{"name": "hidden", "table": "private"}
				plan.Indexes[0].Config["resolvers"] = append(values, extra)
			case "wrong-target":
				plan.Indexes[0].Config["resolvers"].([]any)[0].(map[string]any)["table"] = "private"
			}
			require.Error(t, ValidateKnowledgeProcessing(&k, &plan, "research"))
			client := &knowledgeClient{config: plan.Indexes[0].Config, complete: true}
			called := false
			client.onQuery = func() { called = true }
			until := time.Now().Add(time.Minute).Unix()
			b := Binding{ID: testSourceID, Table: "docs", Knowledge: &k, Status: "searchable", ValidUntil: &until}
			require.Error(t, ValidateInstalledKnowledge(t.Context(), client, b, "research"))
			_, err := QueryKnowledge(t.Context(), client, b, KnowledgeRequest{Graph: "facts", Start: []string{"claim"}}, "research")
			require.Error(t, err)
			require.False(t, called, "do not execute with service credentials")
			require.EqualValues(t, "unsupported", ObserveKnowledge(t.Context(), client, b, "research").Graphs[0].State)
		})
	}
	plan, k := knowledgeFixture(t)
	client := &knowledgeClient{config: plan.Indexes[0].Config, complete: false}
	require.NoError(t, ValidateInstalledKnowledge(t.Context(), client, Binding{Table: "docs", Knowledge: &k}, "research"), "configuration admission does not require completed backfill")
}
