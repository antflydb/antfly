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
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProcessingCredentialsUseNativeClassification(t *testing.T) {
	for _, key := range []string{"api_key", "bearer_token", "capability_token", "credentials_path", "credentials", "X-API-Key", "Proxy-Authorization", "Cookie", "secretAccessKey", "x-auth"} {
		t.Run(key, func(t *testing.T) {
			_, err := PreviewProcessing(management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: map[string]any{"provider": "openai", "model": "whisper-1", key: "literal-private-value"}})
			require.Error(t, err)
			for _, value := range []string{"${secret:inference}", ""} {
				_, err = PreviewProcessing(management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: map[string]any{"provider": "openai", "model": "whisper-1", key: value}})
				require.NoError(t, err)
			}
		})
	}
	for _, value := range []string{"${secret:}", "${secret:key}literal", "${secret:key", "${secret:key\n}"} {
		require.Error(t, validateProcessingSecrets(map[string]any{"bearer_token": value}), value)
	}
	for _, key := range []string{"producer_json", "chunker_json"} {
		require.Error(t, validateProcessingSecrets(map[string]any{key: `{"bearer_token":"literal"}`}))
		require.NoError(t, validateProcessingSecrets(map[string]any{key: `{"bearer_token":"${secret:token}"}`}))
	}
	for _, endpoint := range []string{"https://user:password@inference.invalid/v1", "https://inference.invalid/v1?bearer_token=literal", "https://inference.invalid/v1?api%5Fkey=literal", "https://inference.invalid/v1#token=literal"} {
		require.Error(t, validateProcessingSecrets(map[string]any{"base_url": endpoint}), endpoint)
	}
	require.NoError(t, validateProcessingSecrets(map[string]any{"api_url": "${secret:endpoint}", "max_tokens": 32, "query_instruction": "find evidence"}))
}
func TestTranscriptionPresetUsesNativeTimedUnits(t *testing.T) {
	preview, err := PreviewProcessing(management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: map[string]any{"provider": "openai", "model": "whisper-1", "timestamps": true}})
	require.NoError(t, err)
	unit := preview.Processing.Indexes[0].Config["enrichments"].([]any)[0].(map[string]any)
	producer := unit["producer"].(map[string]any)
	require.Equal(t, "document_extraction", producer["type"])
	config := producer["config"].(map[string]any)
	require.Equal(t, "explicit_only", config["route_preset"])
	require.Equal(t, "whisper-1", config["transcription"].(map[string]any)["transcriber"].(map[string]any)["model"])
	require.Equal(t, "provenance", preview.Stream.LocationField)
	raw, err := BuildSearchQuery(Binding{SearchVersion: 2, Streams: []SearchStream{preview.Stream}, Filter: SourceFilter(testSourceID)}, SearchRequest{Stream: "speech", Mode: "full_text", Text: "launch"})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"fields":["text","provenance"]`)
}
func TestStandaloneProcessingOmissionPreservesAndEmptyClears(t *testing.T) {
	s, _, _ := serviceFixture(t)
	source := s.profile.Sources[0]
	source.Processing = &ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{{Name: "text", Config: map[string]any{"type": "full_text", "field": "content.text"}}}}
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	source.Processing = nil
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	require.Len(t, s.profile.Sources[0].Processing.Indexes, 1)
	source.Processing = &ProcessingDeclaration{Indexes: []management.NativeIndexDeclaration{}}
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	raw, _ := json.Marshal(s.profile.Sources[0].Processing)
	require.JSONEq(t, `{}`, string(raw))
}

func TestTimedTranscriptionSemanticBranchReusesNativeUnits(t *testing.T) {
	input := management.ProcessingPreset{Name: "speech", Kind: "transcription", ProducerConfig: map[string]any{"provider": "openai", "model": "whisper-1"}}
	text, err := PreviewProcessing(input)
	require.NoError(t, err)
	input.Embedder = map[string]any{"provider": "antfly", "model": "pinned", "model_identity": testVectorSpace().Revision}
	input.Dimension = 3
	semantic, err := PreviewProcessing(input)
	require.NoError(t, err)
	require.Equal(t, text.Artifact, semantic.Artifact)
	require.Equal(t, text.Processing.Indexes[0].Name, semantic.Processing.Indexes[0].Name)
	for i, index := range semantic.Processing.Indexes {
		raw, err := json.Marshal(map[string]any{"config": index.Config})
		require.NoError(t, err)
		observed, err := decodeSearchIndex(raw)
		require.NoError(t, err)
		require.NoError(t, checkNativeStream(semantic.Stream, observed, i == 1))
	}
}

func TestTranscriptionBuildersRejectMisleadingOutputProjection(t *testing.T) {
	producer := map[string]any{"type": "transcriber", "config": map[string]any{"provider": "openai", "model": "whisper-1"}}
	_, _, _, err := BuildTextProcessing(TextProcessingPreset{Name: "speech", Field: "url", Producer: producer, ContentType: "text/plain"})
	require.Error(t, err)
	_, _, _, err = BuildTextProcessing(TextProcessingPreset{Name: "speech", Field: "url", Producer: producer, OutputField: "body"})
	require.Error(t, err)
	plan, stream, _, err := BuildTextProcessing(TextProcessingPreset{Name: "speech", Field: "url", Producer: producer})
	require.NoError(t, err)
	require.Equal(t, "text", stream.Field)
	unit := plan.Indexes[0].Config["enrichments"].([]any)[0].(map[string]any)
	require.Equal(t, "document_extraction", unit["producer"].(map[string]any)["type"])
}
