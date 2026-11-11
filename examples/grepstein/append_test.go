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
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func writeAppendState(t *testing.T, state string, records []corpusRecord, config corpusConfig) {
	t.Helper()
	var data bytes.Buffer
	for _, r := range records {
		if err := json.NewEncoder(&data).Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(state, "published.ndjson"), data.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{
		"publication.json": map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(data.Bytes())), "records": len(records)},
		"prepare.json":     corpusProgress{Complete: true, Records: len(records)}, "sources.json": config,
	} {
		if err := atomicCorpusJSON(filepath.Join(state, name), value); err != nil {
			t.Fatal(err)
		}
	}
}

func nativeAppendReaderFixture(t *testing.T, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/ai/v1/models") {
			json.NewEncoder(w).Encode(map[string]any{"readers": map[string]any{"e2e-reader": map[string]any{
				"inputs": []string{"text", "image"}, "inference_capabilities": map[string]any{
					"version": 4, "task": "read", "input_modalities": []string{"text", "image"}, "accepted_mime_types": []string{"image/png", "image/jpeg"}, "input_granularity": "page", "output": "read_result", "result_cardinality": "one_per_item", "prompt_policy": "explicit", "borrowed_attachments": false,
					"task_limits": map[string]any{"max_text_bytes_per_item": 65536, "max_input_tokens_per_item": 8192, "max_output_tokens_per_item": 8192, "max_candidates_per_request": nil, "max_schema_bytes": nil},
					"batch":       map[string]any{"mode": "native", "preferred_items": 8, "max_items": 8, "max_encoded_media_bytes": 67108864, "max_decoded_pixels": 32000000, "max_media_parts_per_item": 1, "per_item_failures": false},
				},
			}}})
			return
		}
		if r.Method != "POST" || r.URL.Path != "/ai/v1/read" {
			http.Error(w, "unexpected fixture request", 404)
			return
		}
		var body struct {
			Images []map[string]any `json:"images"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		data := []any{}
		for i, image := range body.Images {
			source, _ := image["url"].(string)
			if !strings.HasPrefix(source, "data:image/png;base64,") {
				http.Error(w, "expected native PDF raster", 400)
				return
			}
			calls.Add(1)
			data = append(data, map[string]any{"object": "read_result", "index": i, "status": "complete", "text": "Native GCS extraction evidence. This scanned document contains ordinary readable evidence text. Its native units and chunks retain the original source identity after appending more documents."})
		}
		json.NewEncoder(w).Encode(map[string]any{"object": "list", "model": "e2e-reader", "data": data, "usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 40, "total_tokens": 41}})
	}))
	t.Cleanup(server.Close)
	return server
}

func appendTestRecord(id, content string) corpusRecord {
	h := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	return corpusRecord{ID: id, Document: map[string]any{
		"url": "gs://originals/objects/sha256/" + h + ".pdf?generation=123", "version": h + ":123", "filename": id + ".pdf", "mime_type": "application/pdf", "indexed_at": float64(1), "metadata": map[string]any{"page_start": float64(1), "page_end": float64(1)},
	}}
}

func TestAppendPreflightRejectsLateConflictsWithoutWriting(t *testing.T) {
	state := t.TempDir()
	first, last := appendTestRecord("new", "new"), appendTestRecord("existing", "old")
	writeAppendState(t, state, []corpusRecord{first, last}, corpusConfig{PagesPerRecord: 25})
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
			http.Error(w, "unexpected write", 500)
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/documents/new"):
			http.Error(w, "absent", 404)
		case strings.HasSuffix(r.URL.Path, "/documents/existing"):
			doc := appendTestRecord("existing", "changed").Document
			json.NewEncoder(w).Encode(doc)
		default:
			json.NewEncoder(w).Encode(map[string]any{"table_id": "table-1", "description": corpusDescription(indexes, 25)})
		}
	}))
	defer server.Close()
	err = corpusLoadCmd([]string{"--state", state, "--url", server.URL, "--append-only", "--batch-size", "1"})
	if err == nil || !strings.Contains(err.Error(), "existing document differs") {
		t.Fatalf("accepted conflict: %v", err)
	}
	if writes.Load() != 0 {
		t.Fatal("preflight wrote before finding late conflict")
	}
}

func TestAppendPreservesExistingAndRecoversLostCommitResponse(t *testing.T) {
	state := t.TempDir()
	old, newRow := appendTestRecord("sample", "old"), appendTestRecord("next", "new")
	writeAppendState(t, state, []corpusRecord{old, newRow}, corpusConfig{PagesPerRecord: 25})
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]map[string]any{"sample": old.Document}
	var transactions atomic.Int64
	var lock sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		if r.Method == "GET" {
			if strings.Contains(r.URL.Path, "/documents/") {
				id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
				if row := rows[id]; row != nil {
					json.NewEncoder(w).Encode(map[string]any{"_source": row})
				} else {
					http.Error(w, "absent", 404)
				}
			} else {
				json.NewEncoder(w).Encode(map[string]any{"table_id": "table-1", "description": corpusDescription(indexes, 25)})
			}
			return
		}
		if r.URL.Path != "/transactions/commit" {
			t.Errorf("append used upsert: %s", r.URL.Path)
			http.Error(w, "bad route", 400)
			return
		}
		var body struct {
			ReadSet []struct{ Table, Key, Version string } `json:"read_set"`
			Tables  map[string]struct {
				Inserts map[string]map[string]any `json:"inserts"`
			} `json:"tables"`
			Sync string `json:"sync_level"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if body.Sync != "write" || len(body.ReadSet) != 1 || body.ReadSet[0].Key != "next" || body.ReadSet[0].Version != "0" || body.ReadSet[0].Table != "grepstein_corpus" {
			t.Errorf("missing absence fence: %+v", body)
		}
		inserts := body.Tables["grepstein_corpus"].Inserts
		if len(inserts) != 1 || inserts["next"] == nil {
			t.Errorf("sample was rewritten: %+v", inserts)
		}
		for id, row := range inserts {
			rows[id] = row
		}
		transactions.Add(1)
		// The primary committed, but the response never established an outcome.
		http.Error(w, `{"code":"transaction_outcome_unknown"}`, 500)
	}))
	defer server.Close()
	args := []string{"--state", state, "--url", server.URL, "--append-only"}
	if err := corpusLoadCmd(append(append([]string{}, args...), "--plan-only")); err != nil {
		t.Fatal(err)
	}
	if transactions.Load() != 0 {
		t.Fatal("plan wrote documents")
	}
	if err := corpusLoadCmd(args); err == nil {
		t.Fatal("unknown outcome advanced checkpoint")
	}
	if err := corpusLoadCmd(args); err != nil {
		t.Fatal("resume", err)
	}
	if transactions.Load() != 1 {
		t.Fatal("resume repeated committed work")
	}
	var progress corpusProgress
	if err := readCorpusJSON(filepath.Join(state, "load.json"), &progress); err != nil {
		t.Fatal(err)
	}
	if !progress.Complete || progress.Records != 2 || progress.Reused != 2 {
		t.Fatalf("bad resume: %+v", progress)
	}
	// Switching from append to upsert requires a fresh, explicitly chosen state.
	if err := corpusLoadCmd([]string{"--state", state, "--url", server.URL}); err == nil {
		t.Fatal("append checkpoint allowed upserts")
	}
}

func TestAppendConditionalConflictDoesNotRetryOrOverwrite(t *testing.T) {
	record := appendTestRecord("next", "new")
	var writes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			http.Error(w, "absent", 404)
			return
		}
		writes.Add(1)
		http.Error(w, `{"status":"aborted","conflict":{"code":"transaction_conflict"}}`, 409)
	}))
	defer server.Close()
	if _, _, err := appendCorpusBatch(server.Client(), server.URL, "corpus", server.URL+"/tables/corpus", map[string]map[string]any{record.ID: record.Document}); err == nil {
		t.Fatal("accepted racing insert")
	}
	if writes.Load() != 1 {
		t.Fatal("retried aborted transaction")
	}
}

func TestAppendRejectsRenamedDuplicateAndUnpinnedSources(t *testing.T) {
	for _, test := range []string{"alias", "unpinned"} {
		t.Run(test, func(t *testing.T) {
			first, last := appendTestRecord("first", "same"), appendTestRecord("alias", "same")
			if test == "unpinned" {
				last.Document["url"] = "gs://originals/file.pdf"
			}
			state := t.TempDir()
			writeAppendState(t, state, []corpusRecord{first, last}, corpusConfig{PagesPerRecord: 25})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "absent", 404) }))
			defer server.Close()
			f, err := os.Open(filepath.Join(state, "published.ndjson"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := planCorpusAppend(server.Client(), server.URL, f, nil, "", "", "", ""); err == nil {
				t.Fatal("accepted duplicate or unpinned source")
			}
		})
	}
}
