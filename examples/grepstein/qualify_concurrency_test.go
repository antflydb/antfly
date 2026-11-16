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
	"context"
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
	"time"
)

func qualificationConcurrencyFixture(t *testing.T, count int, document func(http.ResponseWriter, *http.Request) bool) (string, *httptest.Server) {
	t.Helper()
	state := t.TempDir()
	hash := strings.Repeat("b", 64)
	source := "gs://originals/objects/sha256/" + hash + ".pdf?generation=123"
	var manifest bytes.Buffer
	for i := 0; i < count; i++ {
		record := corpusRecord{ID: fmt.Sprintf("doc-%d", i), Document: map[string]any{"url": source, "version": hash + ":123", "metadata": map[string]any{"page_start": 1, "page_end": 1}}}
		if err := json.NewEncoder(&manifest).Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(state, "published.ndjson"), manifest.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := atomicCorpusJSON(filepath.Join(state, "publication.json"), map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(manifest.Bytes())), "records": count}); err != nil {
		t.Fatal(err)
	}
	if err := atomicCorpusJSON(filepath.Join(state, "sources.json"), corpusConfig{Profile: "pdf"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/indexes/"):
			fmt.Fprint(w, `{"status":{"readiness":{"complete":true,"queryable":true}},"shard_status":{"one":{"readiness":{"complete":true,"queryable":true}}}}`)
		case strings.Contains(r.URL.Path, "/artifacts/"):
			json.NewEncoder(w).Encode(map[string]any{"source_url": source, "generation": 1, "unit_count": 1, "route_type": "pdf", "merge_status": "converged", "manifest_json": `{"ocr_attempted_count":0,"ocr_failed_count":0,"incomplete_unit_count":0,"retryable_incomplete_unit_count":0}`})
		default:
			if document != nil && document(w, r) {
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"url": source, "version": hash + ":123"})
		}
	}))
	t.Cleanup(server.Close)
	return state, server
}

func TestQualificationBoundsConcurrentRequests(t *testing.T) {
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			var active, peak atomic.Int64
			var ready sync.Once
			gate := make(chan struct{})
			state, server := qualificationConcurrencyFixture(t, 9, func(w http.ResponseWriter, r *http.Request) bool {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				if n == int64(limit) {
					ready.Do(func() { close(gate) })
				}
				select {
				case <-gate:
					return false
				case <-r.Context().Done():
					return true
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			q, err := qualifyCorpusWithConcurrency(ctx, server.Client(), server.URL+"/tables/corpus", state, limit)
			if err != nil || !q.Complete || q.Records != 9 || q.Pages != 9 || q.Seconds <= 0 || q.Concurrency != limit {
				t.Fatalf("qualification: %+v %v", q, err)
			}
			if peak.Load() != int64(limit) {
				t.Fatalf("peak requests=%d, expected %d", peak.Load(), limit)
			}
		})
	}
}

func TestQualificationFailureCancelsSiblingRequests(t *testing.T) {
	var active atomic.Int64
	var gateOnce, stoppedOnce sync.Once
	gate, stopped := make(chan struct{}), make(chan struct{})
	state, server := qualificationConcurrencyFixture(t, 20, func(w http.ResponseWriter, r *http.Request) bool {
		if active.Add(1) == 2 {
			gateOnce.Do(func() { close(gate) })
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return true
		}
		if strings.HasSuffix(r.URL.Path, "/doc-0") {
			http.Error(w, "denied", 403)
			return true
		}
		<-r.Context().Done()
		stoppedOnce.Do(func() { close(stopped) })
		return true
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	q, err := qualifyCorpusWithConcurrency(ctx, server.Client(), server.URL+"/tables/corpus", state, 2)
	if err == nil || q.Complete {
		t.Fatalf("failed verification accepted: %+v %v", q, err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("failed verification did not cancel sibling HTTP work")
	}
	if active.Load() != 2 {
		t.Fatalf("failure dispatched more work: %d", active.Load())
	}
}
