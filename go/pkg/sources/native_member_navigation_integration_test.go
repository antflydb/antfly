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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativeMemberNavigationIntegration(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT")
	binary := os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set native source endpoint and grep binary")
	}
	publisher := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("native_members_%d", time.Now().UnixNano())
	indexes := map[string]any{
		"copies": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "copied"}}, "enrichments": []any{
			map[string]any{"name": "copied", "kind": "asset", "template": "{{content.text}}", "producer_json": `{"type":"copy"}`},
			map[string]any{"name": "pieces", "kind": "chunk", "field": "text", "chunk_size": 512, "chunk_overlap": 0},
		}},
		"pieces": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "pieces", "field": "text"}}},
	}
	_, err := publisher.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 3, "indexes": indexes})
	require.NoError(t, err)
	defer publisher.call(context.Background(), "DELETE", tablePath(table), nil)
	records := map[string]any{}
	for i := 0; i < 13; i++ {
		owner := "other"
		if i == 0 {
			owner = "selected"
		}
		records[fmt.Sprintf("doc-%02d", i)] = map[string]any{"source_id": owner, "source": map[string]any{"uri": fmt.Sprintf("gs://fixture/%d.txt", i), "path": fmt.Sprintf("%d.txt", i), "revision": "1", "indexed_at": time.Now().Unix()}, "content": map[string]any{"text": "needle " + strings.Repeat("alpha beta gamma delta ", 256)}, "text": "needle " + strings.Repeat("alpha beta gamma delta ", 256), "permissions": map[string]any{"public": true, "readers": []string{}}, "metadata": map[string]any{}}
	}
	require.NoError(t, publisher.Batch(t.Context(), table, records, nil))
	for _, index := range []string{"copies", "pieces"} {
		var last string
		defer func() {
			if t.Failed() {
				t.Logf("readiness %s: %s", index, last)
			}
		}()
		require.Eventually(t, func() bool {
			raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/"+index, nil)
			if err != nil {
				return false
			}
			last = string(raw)
			var body struct {
				Status struct {
					Readiness struct {
						Complete bool `json:"complete"`
					} `json:"readiness"`
				} `json:"status"`
			}
			return json.Unmarshal(raw, &body) == nil && body.Status.Readiness.Complete
		}, 30*time.Second, 50*time.Millisecond)
	}
	for _, stream := range []TextStream{{Name: "copy", Artifact: "copied", Index: "copies", Field: "$", Default: true}, {Name: "chunks", Artifact: "pieces", Index: "pieces", Field: "text", Default: true}} {
		t.Run(stream.Name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "grep.json")
			config := map[string]any{"version": 1, "endpoint": endpoint, "principals": []string{"user:self"}, "page_size": 1, "tables": []any{map[string]any{"name": "selected", "table": table, "fields": CanonicalDocumentFields(), "source_filter": map[string]any{"equals": "selected", "field": "source_id"}, "streams": []TextStream{stream}}}}
			require.NoError(t, writeJSON(file, config, 1<<20))
			output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "--json", "needle").CombinedOutput()
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), `"complete":true`)
			require.Equal(t, 1, strings.Count(string(output), `"type":"match"`), string(output))
			require.Contains(t, string(output), `"document_id":"doc-00"`)
		})
	}

	// Source predicates admit only matching parents before cross-shard member
	// enumeration; they never run against generated scalar/chunk payloads.
	for _, artifact := range []string{"copied", "pieces"} {
		for _, source := range []string{"selected", "absent"} {
			query := map[string]any{"filter_query": map[string]any{"equals": source, "field": "source_id"}, "hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "ids": []string{"doc-00", "doc-01"}}, "level": "member", "artifact": artifact}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 100}
			raw, err := publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
			require.NoError(t, err)
			var result struct {
				Responses []struct {
					Status int `json:"status"`
					Hits   struct {
						Hits []struct {
							Hierarchy struct {
								Parent string `json:"parent_doc_key"`
							} `json:"hierarchy"`
						} `json:"hits"`
					} `json:"hits"`
				} `json:"responses"`
			}
			require.NoError(t, json.Unmarshal(raw, &result))
			require.Len(t, result.Responses, 1)
			require.Equal(t, 200, result.Responses[0].Status, string(raw))
			if source == "absent" {
				require.Empty(t, result.Responses[0].Hits.Hits)
			} else {
				require.NotEmpty(t, result.Responses[0].Hits.Hits)
				for _, hit := range result.Responses[0].Hits.Hits {
					require.Equal(t, "doc-00", hit.Hierarchy.Parent, string(raw))
				}
			}
		}
	}
	// A cursor without its retained capability must not capture a newer cut.
	query := map[string]any{"hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "id": "doc-00"}, "level": "member", "artifact": "copied"}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 1}
	raw, err := publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	require.NoError(t, err)
	var first struct {
		Responses []struct {
			Hits struct {
				Hits []struct {
					Sort []any `json:"_sort"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &first))
	require.Len(t, first.Responses, 1)
	require.Len(t, first.Responses[0].Hits.Hits, 1)
	query["search_after"] = first.Responses[0].Hits.Hits[0].Sort
	raw, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	if err == nil {
		var rejection struct {
			Responses []struct {
				Status int `json:"status"`
			} `json:"responses"`
		}
		require.NoError(t, json.Unmarshal(raw, &rejection))
		require.Len(t, rejection.Responses, 1)
		require.NotEqual(t, 200, rejection.Responses[0].Status, "member continuation requires the retained snapshot")
	}
	// Public callers must never be able to manufacture the coordinator proof.
	_, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", map[string]any{"_hierarchy_parent_admitted": true, "hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "id": "doc-00"}, "level": "member", "artifact": "copied"}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 1})
	require.Error(t, err, "public requests must reject private parent admission")
}

// Current readiness must never certify a retained cut containing unfinished work.
func TestNativeMemberSnapshotCompletionIntegration(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set native source endpoint")
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		content := `{"items":[{"item":0,"value":{"text":"finished summary"}}]}`
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "fixture", "object": "chat.completion", "model": "gpt-4o-mini", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}}})
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		provider.Close()
	}()
	publisher := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("native_cut_%d", time.Now().UnixNano())
	producer, _ := json.Marshal(map[string]any{"type": "generator", "config": map[string]any{"generator": map[string]any{"provider": "openai", "model": "gpt-4o-mini", "url": provider.URL + "/v1", "api_key": "fixture"}, "output": "json_schema", "output_schema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]any{"type": "string"}}, "required": []string{"text"}, "additionalProperties": false}}})
	_, err := publisher.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 1, "indexes": map[string]any{"summaries": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "summary", "field": "text"}}, "enrichments": []any{map[string]any{"name": "input", "kind": "asset", "template": "{{content.text}}", "producer_json": `{"type":"copy"}`}, map[string]any{"name": "summary", "kind": "asset", "source_artifact_name": "input", "content_type": "application/json", "producer_json": string(producer)}}}}})
	require.NoError(t, err)
	defer publisher.call(context.Background(), "DELETE", tablePath(table), nil)
	require.NoError(t, publisher.Batch(t.Context(), table, map[string]any{"source-doc": map[string]any{"content": map[string]any{"text": "draft text"}}}, nil))
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("generator did not begin")
	}
	raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/summaries", nil)
	require.NoError(t, err)
	var initial struct {
		Status struct {
			Readiness struct {
				Complete    bool   `json:"complete"`
				Incarnation string `json:"incarnation"`
			} `json:"readiness"`
		} `json:"status"`
	}
	require.NoError(t, json.Unmarshal(raw, &initial))
	require.False(t, initial.Status.Readiness.Complete)
	incarnation := initial.Status.Readiness.Incarnation
	raw, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", map[string]any{"query": map[string]any{"match_all": map[string]any{}}, "fields": []string{}, "order_by": []any{map[string]any{"field": "_id"}}, "limit": 1})
	require.NoError(t, err)
	var captured struct {
		Responses []struct {
			Snapshot string `json:"remote_snapshot"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &captured))
	require.Len(t, captured.Responses, 1)
	require.NotEmpty(t, captured.Responses[0].Snapshot)
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/summaries", nil)
		if err != nil {
			return false
		}
		var body struct {
			Status struct {
				Readiness struct {
					Complete    bool   `json:"complete"`
					Incarnation string `json:"incarnation"`
				} `json:"readiness"`
			} `json:"status"`
		}
		return json.Unmarshal(raw, &body) == nil && body.Status.Readiness.Complete && body.Status.Readiness.Incarnation == incarnation
	}, 60*time.Second, 50*time.Millisecond)
	query := map[string]any{"hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "id": "source-doc"}, "level": "member", "artifact": "summary"}}, "fields": []string{"text"}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 1, "remote_snapshot": captured.Responses[0].Snapshot}
	raw, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	if err == nil {
		var result struct {
			Responses []struct {
				Status int `json:"status"`
			} `json:"responses"`
		}
		require.NoError(t, json.Unmarshal(raw, &result))
		require.Len(t, result.Responses, 1)
		require.NotEqual(t, 200, result.Responses[0].Status, "current completion certified an incomplete retained cut: %s", raw)
	}
	delete(query, "remote_snapshot")
	raw, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	require.NoError(t, err)
	require.Contains(t, string(raw), "finished summary")
}

func TestNativeMemberBatchIntegration(t *testing.T) {
	endpoint, binary := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT"), os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set native source endpoint and grep binary")
	}
	publisher := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("native_batch_%d", time.Now().UnixNano())
	_, err := publisher.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 3, "indexes": map[string]any{"summary_text": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "summary"}}, "enrichments": []any{map[string]any{"name": "summary", "kind": "asset", "template": "Tuesday", "producer_json": `{"type":"copy"}`}}}}})
	require.NoError(t, err)
	defer publisher.call(context.Background(), "DELETE", tablePath(table), nil)
	records := map[string]any{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		records[id] = map[string]any{"source": map[string]any{"uri": "gs://fixture/" + id + ".txt", "path": id + ".txt", "revision": "1", "indexed_at": 1}, "content": map[string]any{"text": "Monday"}, "permissions": map[string]any{"public": true, "readers": []string{}}, "metadata": map[string]any{}, "grep_trigrams_version": "raw-byte-trigrams-v1", "grep_trigrams": []string{"4d6f6e", "6f6e64", "6e6461", "646179"}}
	}
	require.NoError(t, publisher.Batch(t.Context(), table, records, nil))
	var readiness string
	require.Eventually(t, func() bool {
		raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/summary_text", nil)
		if err != nil {
			return false
		}
		readiness = string(raw)
		var body struct {
			Status struct {
				Readiness struct {
					Complete bool `json:"complete"`
				} `json:"readiness"`
			} `json:"status"`
		}
		return json.Unmarshal(raw, &body) == nil && body.Status.Readiness.Complete
	}, time.Minute, 100*time.Millisecond, "producer did not complete: %s", readiness)
	file := filepath.Join(t.TempDir(), "grep.json")
	config := map[string]any{"version": 1, "endpoint": endpoint, "principals": []string{}, "page_size": 100, "tables": []any{map[string]any{"name": table, "fields": CanonicalDocumentFields(), "candidate_index": map[string]any{"version": 1, "field": "grep_trigrams"}, "streams": []TextStream{{Name: "summary", Artifact: "summary", Index: "summary_text", Field: "$", Generated: true, Default: true}}}}}
	require.NoError(t, writeJSON(file, config, 1<<20))
	// Plain output enables optional trigram pruning. Source trigrams certify
	// Monday, whereas the selected artifact contains Tuesday.
	output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "Tuesday").CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, 1000, strings.Count(string(output), "Tuesday"), string(output))
	output, err = exec.CommandContext(t.Context(), binary, "--table-config", file, "--json", "Tuesday").CombinedOutput()
	require.NoError(t, err, string(output))
	var summary struct {
		Stats struct {
			Complete bool `json:"complete"`
			Files    int  `json:"files"`
			Requests int  `json:"index_requests"`
		} `json:"stats"`
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))
	require.True(t, summary.Stats.Complete)
	require.Equal(t, 1000, summary.Stats.Files)
	require.LessOrEqual(t, summary.Stats.Requests, 40)
	t.Logf("searched %d native members using %d index requests", summary.Stats.Files, summary.Stats.Requests)
	// The maximum parent batch must apply predicates to the source values,
	// including when every generated member contains different text.
	parentIDs := make([]string, 100)
	for i := range parentIDs {
		parentIDs[i] = fmt.Sprintf("doc-%04d", i)
	}
	for _, text := range []string{"Monday", "absent"} {
		request := map[string]any{"filter_query": map[string]any{"equals": text, "field": "content.text"}, "hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "ids": parentIDs}, "level": "member", "artifact": "summary"}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 100}
		raw, err := publisher.call(t.Context(), "POST", tablePath(table)+"/query", request)
		require.NoError(t, err)
		var result struct {
			Responses []struct {
				Status int `json:"status"`
				Hits   struct {
					Hits []json.RawMessage `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		require.NoError(t, json.Unmarshal(raw, &result))
		require.Len(t, result.Responses, 1)
		require.Equal(t, 200, result.Responses[0].Status, string(raw))
		expected := 0
		if text == "Monday" {
			expected = 100
		}
		require.Len(t, result.Responses[0].Hits.Hits, expected)
	}
	// A direct public traversal establishes its own retained snapshot, and its
	// continuation resumes across two independent source prefixes.
	query := map[string]any{"hierarchy": map[string]any{"children": map[string]any{"parent": map[string]any{"level": "source", "ids": []string{"doc-0000", "doc-0001"}}, "level": "member", "artifact": "summary"}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 1}
	raw, err := publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	require.NoError(t, err)
	var first struct {
		Responses []struct {
			Snapshot string `json:"remote_snapshot"`
			Hits     struct {
				Hits []struct {
					ID   string `json:"_id"`
					Sort []any  `json:"_sort"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &first))
	require.Len(t, first.Responses, 1)
	require.NotEmpty(t, first.Responses[0].Snapshot)
	require.Len(t, first.Responses[0].Hits.Hits, 1)
	query["remote_snapshot"], query["search_after"] = first.Responses[0].Snapshot, first.Responses[0].Hits.Hits[0].Sort
	raw, err = publisher.call(t.Context(), "POST", tablePath(table)+"/query", query)
	require.NoError(t, err)
	var second struct {
		Responses []struct {
			Hits struct {
				Hits []struct {
					ID string `json:"_id"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &second))
	require.Len(t, second.Responses, 1)
	require.Len(t, second.Responses[0].Hits.Hits, 1)
	require.NotEqual(t, first.Responses[0].Hits.Hits[0].ID, second.Responses[0].Hits.Hits[0].ID)
}

func TestNativeMemberSourceAuthorityIntegration(t *testing.T) {
	endpoint, binary := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT"), os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set native source endpoint and grep binary")
	}
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("native_authority_%d", time.Now().UnixNano())
	_, err := p.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 1, "schema": nativeCertificateSchema(), "indexes": map[string]any{"copies": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "copied", "field": "text"}}, "enrichments": []any{map[string]any{"name": "copied", "kind": "asset", "field": "payload", "content_type": "application/json", "producer_json": `{"type":"copy"}`}}}}})
	require.NoError(t, err)
	defer p.call(context.Background(), "DELETE", tablePath(table), nil)
	records := map[string]any{}
	for _, id := range []string{"allowed", "secret"} {
		content := map[string]any{"text": "ordinary text"}
		if id == "allowed" {
			content = map[string]any{"resource": map[string]any{"type": "source", "uri": "gs://fixture/file.pdf"}}
		}
		records[id] = map[string]any{"source": map[string]any{"uri": "gs://fixture/" + id, "path": id + ".pdf", "revision": "1", "indexed_at": time.Now().Unix()}, "permissions": map[string]any{"public": id == "allowed", "readers": []string{}}, "metadata": map[string]any{}, "marker": id, "certificate": "raw-byte-trigrams-v1", "trigrams": []string{}, "content": content, "payload": `{"text":"needle","parent_doc_key":"secret","_parent_doc_key":"secret"}`}
	}
	require.NoError(t, p.Batch(t.Context(), table, records, nil))
	require.Eventually(t, func() bool {
		raw, err := p.call(t.Context(), "GET", tablePath(table)+"/indexes/copies", nil)
		var body struct {
			Status struct {
				Readiness struct {
					Complete bool `json:"complete"`
				} `json:"readiness"`
			} `json:"status"`
		}
		return err == nil && json.Unmarshal(raw, &body) == nil && body.Status.Readiness.Complete
	}, 30*time.Second, 50*time.Millisecond)
	raw, err := p.call(t.Context(), "POST", tablePath(table)+"/query", map[string]any{"full_text_index": "copies", "full_text_search": map[string]any{"match_all": map[string]any{}}, "filter_query": map[string]any{"bool": true, "field": "permissions.public"}, "execution": map[string]any{"value_verification": map[string]any{"allow_scan": true}}, "hierarchy": map[string]any{"ancestors": map[string]any{"source": map[string]any{"fields": []string{"marker", "permissions"}}}}, "fields": []string{"text"}, "limit": 10})
	require.NoError(t, err)
	var result struct {
		Responses []struct {
			Status int `json:"status"`
			Hits   struct {
				Hits []struct {
					Hierarchy struct {
						Ancestors struct {
							Source struct {
								Document struct {
									Marker string `json:"marker"`
								} `json:"document"`
							} `json:"source"`
						} `json:"ancestors"`
					} `json:"hierarchy"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	require.NoError(t, json.Unmarshal(raw, &result))
	require.Len(t, result.Responses, 1)
	require.Equal(t, 200, result.Responses[0].Status, string(raw))
	require.Len(t, result.Responses[0].Hits.Hits, 1, string(raw))
	require.Equal(t, "allowed", result.Responses[0].Hits.Hits[0].Hierarchy.Ancestors.Source.Document.Marker, string(raw))
	source := TextStream{Name: "source", Field: "content.text", Default: true}
	for _, streams := range [][]TextStream{
		{source, {Name: "extracted", Artifact: "copied", Index: "copies", Field: "text", Default: true}},
		{source},
		{source, {Name: "title", Field: "metadata.title", Default: false}},
		{source, {Name: "summary", Artifact: "copied", Index: "copies", Field: "text", Default: false}},
	} {
		mixed := len(streams) > 1 && streams[1].Default

		file := filepath.Join(t.TempDir(), "grep.json")
		require.NoError(t, writeJSON(file, map[string]any{"version": 1, "endpoint": endpoint, "principals": []string{"user:self"}, "tables": []any{map[string]any{"name": table, "candidate_index": map[string]any{"version": 1, "field": "trigrams", "version_field": "certificate"}, "fields": CanonicalDocumentFields(), "source_filter": map[string]any{"doc_id": map[string]any{"ids": []string{"allowed"}}}, "streams": streams}}}, 1<<20))
		for _, flags := range [][]string{nil, {"--json"}} {
			args := append([]string{"--table-config", file}, flags...)
			output, err := exec.CommandContext(t.Context(), binary, append(args, "needle")...).CombinedOutput()
			if mixed {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "needle")
			} else {
				require.Error(t, err, string(output))
				require.Contains(t, string(output), "NativeArtifactStreamRequired")
			}
		}
		output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "--glob", "!**", "needle").CombinedOutput()
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, 1, exit.ExitCode(), string(output))
	}
}

// A complete certificate over a modest original can exceed the generic JSON
// traversal cap. Candidate membership is an indexed-term operation, independent
// of the original-value matcher used for the final source-text predicate.
func TestNativeIndexedCertificateIntegration(t *testing.T) {
	endpoint, binary := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT"), os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if endpoint == "" || binary == "" {
		t.Skip("set native source endpoint and grep binary")
	}
	// De Bruijn sequence: every lowercase three-byte window occurs once.
	// This gives 17,576 valid certificate terms for only 17,578 source bytes.
	var digits [79]int
	var sequence []byte
	var visit func(int, int)
	visit = func(t, period int) {
		if t > 3 {
			if 3%period == 0 {
				for _, value := range digits[1 : period+1] {
					sequence = append(sequence, byte('a'+value))
				}
			}
			return
		}
		digits[t] = digits[t-period]
		visit(t+1, period)
		for value := digits[t-period] + 1; value < 26; value++ {
			digits[t] = value
			visit(t+1, t)
		}
	}
	visit(1, 1)
	sequence = append(sequence, sequence[:2]...)
	content := string(sequence)
	set := map[string]bool{}
	for i := 0; i+3 <= len(content); i++ {
		set[fmt.Sprintf("%x", content[i:i+3])] = true
	}
	grams := make([]string, 0, len(set))
	for gram := range set {
		grams = append(grams, gram)
	}
	sort.Strings(grams)
	require.Greater(t, len(grams), 16384)
	p := &Publisher{Endpoint: endpoint}
	table := fmt.Sprintf("native_certificate_%d", time.Now().UnixNano())
	_, err := p.call(t.Context(), "POST", tablePath(table), map[string]any{"num_shards": 1, "schema": nativeCertificateSchema()})
	require.NoError(t, err)
	defer p.call(context.Background(), "DELETE", tablePath(table), nil)
	require.NoError(t, p.Batch(t.Context(), table, map[string]any{"text": map[string]any{
		"source":  map[string]any{"uri": "gs://fixture/text.txt", "path": "text.txt", "revision": "1", "indexed_at": time.Now().Unix()},
		"content": map[string]any{"text": content}, "permissions": map[string]any{"public": true, "readers": []string{}}, "metadata": map[string]any{},
		"certificate": "raw-byte-trigrams-v1", "trigrams": grams,
	}}, nil))
	require.Eventually(t, func() bool {
		raw, err := p.call(t.Context(), "GET", tablePath(table)+"/indexes/full_text_index_v0", nil)
		var body struct {
			Status struct {
				Readiness struct {
					Complete bool `json:"complete"`
				} `json:"readiness"`
			} `json:"status"`
		}
		return err == nil && json.Unmarshal(raw, &body) == nil && body.Status.Readiness.Complete
	}, 30*time.Second, 50*time.Millisecond)
	var baseline []byte
	for _, accelerated := range []bool{false, true} {
		binding := map[string]any{"name": table, "fields": CanonicalDocumentFields(), "streams": DefaultTextStreams()}
		if accelerated {
			binding["candidate_index"] = map[string]any{"version": 1, "field": "trigrams", "version_field": "certificate"}
		}
		file := filepath.Join(t.TempDir(), "grep.json")
		require.NoError(t, writeJSON(file, map[string]any{"version": 1, "endpoint": endpoint, "tables": []any{binding}}, 1<<20))
		output, err := exec.CommandContext(t.Context(), binary, "--table-config", file, "zzz").CombinedOutput()
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "zzz")
		if accelerated {
			require.Equal(t, string(baseline), string(output), "candidate acceleration must preserve exact results")
		} else {
			baseline = output
		}
	}
}

func nativeCertificateSchema() map[string]any {
	// Candidate certificates are declared keyword representations, not analyzed
	// prose or implicitly inferred fields (which may initially be empty).
	return map[string]any{"default_type": "doc", "document_schemas": map[string]any{"doc": map[string]any{"schema": map[string]any{
		"type": "object", "additionalProperties": true, "properties": map[string]any{
			"trigrams":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "x-antfly-types": []string{"keyword"}},
			"certificate": map[string]any{"type": "string", "x-antfly-types": []string{"keyword"}},
		},
	}}}}
}
