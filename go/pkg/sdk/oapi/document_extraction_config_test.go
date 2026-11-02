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

package oapi

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Round trips through the typed producer union must preserve explicit false
// and zero options: the server uses nonzero defaults when these are absent.
func TestDocumentExtractionConfigPreservesOptionPresence(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"unset", `{}`},
		{"html tags retained", `{"html_strip_tags":false}`},
		{"unset quality thresholds", `{"ocr":{"reader":{"provider":"antfly","model":"test-reader"}}}`},
		{"zero quality thresholds", `{"ocr":{"reader":{"provider":"antfly","model":"test-reader"},"quality":{"min_content_chars":0,"garbled_min_words":0,"max_single_char_word_ratio":0,"max_corrupted_line_ratio":0,"font_corruption_score_threshold":0,"max_replacement_char_ratio":0}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var config DocumentExtractionConfig
			if err := json.Unmarshal([]byte(tc.payload), &config); err != nil {
				t.Fatal(err)
			}
			var union EnrichmentProducerConfig_Config
			if err := union.FromDocumentExtractionConfig(config); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(union)
			if err != nil {
				t.Fatal(err)
			}
			var want, got map[string]any
			if err := json.Unmarshal([]byte(tc.payload), &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("producer config changed options: got %s, want %s", encoded, tc.payload)
			}
		})
	}
}

// Exercise the provider-specific constructor as well as the producer union:
// raw STTConfig round trips bypass AppleSTTConfig's field encoding.
func TestDocumentExtractionAppleTranscriptionTimestamps(t *testing.T) {
	for _, tc := range []struct{ name, providerJSON, want string }{
		{"unset", `{}`, `{"transcription":{"transcriber":{"provider":"apple"}}}`},
		{"disabled", `{"timestamps":false}`, `{"transcription":{"transcriber":{"provider":"apple","timestamps":false}}}`},
		{"enabled", `{"timestamps":true}`, `{"transcription":{"transcriber":{"provider":"apple","timestamps":true}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var apple AppleSTTConfig
			if err := json.Unmarshal([]byte(tc.providerJSON), &apple); err != nil {
				t.Fatal(err)
			}
			transcriber := STTConfig{Provider: STTProviderApple}
			if err := transcriber.FromAppleSTTConfig(apple); err != nil {
				t.Fatal(err)
			}
			config := DocumentExtractionConfig{
				Transcription: DocumentExtractionTranscriptionConfig{Transcriber: transcriber},
			}
			var union EnrichmentProducerConfig_Config
			if err := union.FromDocumentExtractionConfig(config); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(union)
			if err != nil {
				t.Fatal(err)
			}
			var want, got map[string]any
			if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Apple transcription changed timestamp option: got %s, want %s", encoded, tc.want)
			}
		})
	}
}
