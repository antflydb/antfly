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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The database fetches and interprets real media. Only the remote model transport
// is deterministic; these tests qualify orchestration/provenance, not model quality.
func TestSourceNativeMultimodalIntegration(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCE_TEST_ENDPOINT for native database integration")
	}
	var transcriptionRetries atomic.Int32
	var summaries atomic.Int32
	inference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/audio/transcriptions") {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
			if bytes.Contains(body, []byte("whisper-retry")) && transcriptionRetries.Add(1) == 1 {
				w.WriteHeader(503)
				fmt.Fprint(w, `{"error":{"message":"temporary fixture overload"}}`)
				return
			}
			fmt.Fprint(w, `{"text":"Tuesday meeting began","language":"en","duration":1,"segments":[{"start":0.1,"end":0.8,"text":"Tuesday meeting began"}]}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/chat/completions") {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
			content := "A red vehicle beside a building."
			if bytes.Contains(body, []byte("gpt-4o-mini")) {
				summaries.Add(1)
				require.Contains(t, string(body), "Tuesday meeting notes")
				content = `{"items":[{"item":0,"value":{"text":"Generated meeting summary."}}]}`
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "smoke", "object": "chat.completion", "model": "smoke-reader", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}})
			return
		}
		http.NotFound(w, r)
	}))
	defer inference.Close()
	var picture bytes.Buffer
	require.NoError(t, png.Encode(&picture, image.NewRGBA(image.Rect(0, 0, 32, 32))))
	wav := make([]byte, 44+32000)
	copy(wav, "RIFF")
	binary.LittleEndian.PutUint32(wav[4:], uint32(len(wav)-8))
	copy(wav[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(wav[16:], 16)
	binary.LittleEndian.PutUint16(wav[20:], 1)
	binary.LittleEndian.PutUint16(wav[22:], 1)
	binary.LittleEndian.PutUint32(wav[24:], 16000)
	binary.LittleEndian.PutUint32(wav[28:], 32000)
	binary.LittleEndian.PutUint16(wav[32:], 2)
	binary.LittleEndian.PutUint16(wav[34:], 16)
	copy(wav[36:], "data")
	binary.LittleEndian.PutUint32(wav[40:], 32000)
	cases := []struct {
		name, mime, term, purpose string
		data                      []byte
		config                    map[string]any
	}{
		{"summary", "text/plain", "meeting", "summary", []byte("Tuesday meeting notes: discuss native artifact summaries."), map[string]any{}},
		{"unconfigured", "audio/wav", "Tuesday", "transcription", wav, map[string]any{}},
		{"audio-retry", "audio/wav", "Tuesday", "transcription", wav, map[string]any{"transcription": map[string]any{"transcriber": map[string]any{"provider": "openai", "model": "whisper-retry", "base_url": inference.URL + "/v1", "api_key": "fixture"}}}},
		{"audio", "audio/wav", "Tuesday", "transcription", wav, map[string]any{"transcription": map[string]any{"transcriber": map[string]any{"provider": "openai", "model": "whisper-1", "base_url": inference.URL + "/v1", "api_key": "fixture"}}}},
		{"image", "image/png", "red vehicle", "caption", picture.Bytes(), map[string]any{"ocr": map[string]any{"generator": map[string]any{"provider": "openai", "model": "smoke-reader", "url": inference.URL + "/v1", "api_key": "fixture"}, "prompt": "Describe this image."}}},
	}
	if video := os.Getenv("ANTFLY_SOURCE_TEST_VIDEO"); video != "" {
		data, err := os.ReadFile(video)
		require.NoError(t, err)
		config := map[string]any{"transcription": map[string]any{"transcriber": map[string]any{"provider": "openai", "model": "whisper-1", "base_url": inference.URL + "/v1", "api_key": "fixture"}}, "routes": []any{map[string]any{"match": map[string]any{"content_type_prefix": "video/"}, "extractor": map[string]any{"type": "audio"}}}}
		cases = append(cases, struct {
			name, mime, term, purpose string
			data                      []byte
			config                    map[string]any
		}{"video", "video/mp4", "Tuesday", "transcription", data, config})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			runner, fixture, _ := runnerFixture(t)
			table := fmt.Sprintf("native_%s_%d", test.name, time.Now().UnixNano())
			runner.Profile.Endpoint = endpoint
			runner.Profile.CatalogTable = table + "_catalog"
			runner.Profile.Sources[0].Table = table
			runner.Profile.Sources[0].Publication = "native"
			source := runner.Profile.Sources[0]
			item := &fixture.inventory.Objects[0]
			item.ID = strings.Replace(item.ID, "manual.txt", "sample."+test.name, 1)
			item.Path = "sample." + test.name
			item.Filename = item.Path
			item.Size = int64(len(test.data))
			item.Metadata = map[string]any{"mime_type": test.mime}
			fixture.data = map[string][]byte{item.ID: test.data}
			origin := httptest.NewServer(http.NotFoundHandler())
			defer origin.Close()
			service, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: origin.URL, AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: runner.Factory})
			require.NoError(t, err)
			defer service.Close()
			origin.Config.Handler = service
			publisher := &Publisher{Endpoint: endpoint, Client: &http.Client{Transport: smokeDiagnosticTransport{t}}}
			producer, _ := json.Marshal(map[string]any{"type": "document_extraction", "config": test.config})
			enrichments := []any{map[string]any{"name": "native_units", "kind": "asset", "template": "{{content.resource.uri}}", "content_type": "application/json", "producer_json": string(producer)}}
			artifact := "native_units"
			if test.name == "summary" {
				artifact = "summaries"
				enrichments = []any{map[string]any{"name": "native_units", "kind": "asset", "template": "{{content.text}}", "producer_json": `{"type":"copy"}`}}
				summaryProducer, _ := json.Marshal(map[string]any{"type": "generator", "config": map[string]any{"generator": map[string]any{"provider": "openai", "model": "gpt-4o-mini", "url": inference.URL + "/v1", "api_key": "fixture"}, "output": "json_schema", "output_schema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}, "additionalProperties": false}}})
				enrichments = append(enrichments, map[string]any{"name": artifact, "kind": "asset", "source_artifact_name": "native_units", "content_type": "application/json", "producer_json": string(summaryProducer)})
			}
			_, err = publisher.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 1, "indexes": map[string]any{"unit_text": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": artifact, "field": "text"}}, "enrichments": enrichments}}})

			require.NoError(t, err)
			defer func() {
				_, _ = publisher.call(context.Background(), "DELETE", tablePath(table), nil)
				_, _ = publisher.call(context.Background(), "DELETE", tablePath(runner.Profile.CatalogTable), nil)
			}()
			response := serviceCall(service, "POST", "/v1/sync", map[string]any{})
			require.Equal(t, 202, response.Code, response.Body.String())
			var job SyncJob
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
			require.Equal(t, "completed", waitServiceJob(t, service, job.ID).State)
			key, err := RecordKey(source.ID, item.ID)
			require.NoError(t, err)
			var manifest []byte
			if test.name != "summary" {
				require.Eventually(t, func() bool {
					manifest, err = publisher.call(t.Context(), "GET", tablePath(table)+"/documents/"+url.PathEscape(key)+"/artifacts/native_units?detail=raw", nil)
					return err == nil && bytes.Contains(manifest, []byte(`"unit_count":1`))
				}, 90*time.Second, 250*time.Millisecond, string(manifest))
			}
			if test.name == "unconfigured" {
				var detail struct {
					ManifestJSON string `json:"manifest_json"`
				}
				require.NoError(t, json.Unmarshal(manifest, &detail))
				var state struct {
					IncompleteUnits int `json:"incomplete_unit_count"`
				}
				require.NoError(t, json.Unmarshal([]byte(detail.ManifestJSON), &state))
				require.Equal(t, 1, state.IncompleteUnits)
				file := filepath.Join(t.TempDir(), "grep.json")
				config := map[string]any{"version": 1, "endpoint": endpoint, "principals": []string{"user:self"}, "tables": []any{map[string]any{"name": test.name, "table": table, "fields": CanonicalDocumentFields(), "streams": []TextStream{{Name: test.name, Artifact: "native_units", Index: "unit_text", Field: "text", Default: true}}}}}
				require.NoError(t, writeJSON(file, config, 1<<20))
				if binary := os.Getenv("ANTFLY_GREP_TEST_BINARY"); binary != "" {
					output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "--json", test.term).CombinedOutput()
					var exit *exec.ExitError
					require.ErrorAs(t, err, &exit, string(output))
					require.Equal(t, 2, exit.ExitCode(), string(output))
				}
				return
			}
			require.Eventually(t, func() bool {
				raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/unit_text", nil)
				if err != nil {
					return false
				}
				var status struct {
					Status struct{ Readiness struct{ Complete bool } }
				}
				return json.Unmarshal(raw, &status) == nil && status.Status.Readiness.Complete
			}, 90*time.Second, 250*time.Millisecond)
			if test.name == "summary" {
				require.Positive(t, summaries.Load(), "native generation must actually run")
			}

			if binary := os.Getenv("ANTFLY_GREP_TEST_BINARY"); binary != "" {
				file := filepath.Join(t.TempDir(), "grep.json")
				config := map[string]any{"version": 1, "endpoint": endpoint, "principals": []string{"user:self"}, "page_size": 1, "tables": []any{map[string]any{"name": test.name, "table": table, "fields": CanonicalDocumentFields(), "source_filter": json.RawMessage(SourceFilter(source.ID)), "streams": []TextStream{{Name: test.name, Artifact: artifact, Index: "unit_text", Field: "text", Purpose: test.purpose, Default: true, Generated: test.name == "image" || test.name == "summary", Boundary: "unit", LocationField: "provenance"}}}}}
				require.NoError(t, writeJSON(file, config, 1<<20))
				output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "--json", test.term).CombinedOutput()
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), `"artifact_revision":"sha256:`)
				if strings.HasPrefix(test.name, "audio") || test.name == "video" {
					require.Contains(t, string(output), `"transcript_spans"`)
					require.Contains(t, string(output), `"start_ms":100`)
				}
				if test.name == "image" || test.name == "summary" {
					require.Contains(t, string(output), `"generated":true`)
				}
			}
		})
	}
}
