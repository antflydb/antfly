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
	"context"
	"encoding/json"
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
)

func mustNativeProfile(t *testing.T, name string) corpusProfile {
	t.Helper()
	p, err := processingProfile(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// The provider fixture truncates requests below 8K. Exercise the public policy
// update and incomplete-only range routes without fabricating stored manifests.
func testNativeExecutionPolicyRetry(t *testing.T, ctx context.Context, client *http.Client, tableURL string, source map[string]any, calls *atomic.Int64) {
	t.Helper()
	type manifest struct {
		Generation uint64 `json:"generation"`
		Raw        string `json:"manifest_json"`
	}
	get := func(id string) manifest {
		t.Helper()
		raw, err := corpusRequestWithContext(ctx, client, "GET", tableURL+"/documents/"+id+"/artifacts/"+corpusUnits+"?detail=raw", nil)
		var m manifest
		if err != nil || json.Unmarshal(raw, &m) != nil || m.Generation == 0 || m.Raw == "" {
			t.Fatalf("read manifest %s: %v", id, err)
		}
		return m
	}
	wait := func(failed bool) manifest {
		t.Helper()
		var last []byte
		for start := time.Now(); time.Since(start) < 90*time.Second; {
			raw, err := corpusRequestWithContext(ctx, client, "GET", tableURL+"/documents/retry/artifacts/"+corpusUnits+"?detail=raw", nil)
			last = raw
			var m manifest
			if err == nil && json.Unmarshal(raw, &m) == nil {
				var h struct {
					Failed    uint64 `json:"ocr_failed_count"`
					Execution struct {
						Tokens int `json:"max_output_tokens_per_item"`
					} `json:"execution"`
					Merge struct {
						Status string `json:"status"`
					} `json:"merge_plan"`
				}
				if json.Unmarshal([]byte(m.Raw), &h) == nil && ((failed && (h.Merge.Status == "converged" || h.Merge.Status == "failed") && h.Failed > 0 && h.Execution.Tokens == 4096) || (!failed && h.Merge.Status == "converged" && h.Failed == 0 && h.Execution.Tokens == 8192)) {
					return m
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("retry manifest did not reach expected failed=%t state: %s", failed, last)
		return manifest{}
	}
	update := func(tokens int) {
		t.Helper()
		for attempt := 0; attempt < 20; attempt++ {
			_, err := corpusRequestWithContext(ctx, client, "PUT", tableURL+"/artifacts/"+corpusUnits+"/enrichment", map[string]any{"name": corpusUnits, "kind": "asset", "field": "url", "content_type": "application/json", "execution": map[string]any{"max_output_tokens_per_item": tokens}})
			if err == nil {
				return
			}
			if !strings.Contains(err.Error(), "HTTP 409:") || attempt == 19 {
				t.Fatal("execution update", err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	accepted := get("evidence")
	before := calls.Load()
	update(4096)
	if get("evidence").Generation != accepted.Generation || calls.Load() != before {
		t.Fatal("policy update regenerated accepted output")
	}
	if _, err := corpusRequestWithContext(ctx, client, "POST", tableURL+"/batch", map[string]any{"inserts": map[string]any{"retry": source}, "sync_level": "write"}); err != nil {
		t.Fatal(err)
	}
	partial := wait(true)
	update(8192)
	raw, err := corpusRequestWithContext(ctx, client, "POST", tableURL+"/artifacts/"+corpusUnits+"/reprocess", map[string]any{"selection": "incomplete", "limit": 10})
	if err != nil {
		t.Fatal("incomplete reprocess", err)
	}
	var submitted struct{ Scanned, Reprocessed, Skipped, Failed int }
	if err := json.Unmarshal(raw, &submitted); err != nil || submitted.Scanned != 3 || submitted.Reprocessed != 1 || submitted.Skipped != 2 || submitted.Failed != 0 {
		t.Fatalf("unexpected incomplete selection: %s (%v)", raw, err)
	}
	t.Logf("incomplete submission: %s", raw)
	complete := wait(false)
	if complete.Generation <= partial.Generation {
		t.Fatal("retry did not advance generation")
	}
	if get("evidence").Generation != accepted.Generation {
		t.Fatal("incomplete retry regenerated accepted output")
	}
	if calls.Load() != before+2 {
		t.Fatalf("expected one truncated call and one successful retry; calls=%d -> %d", before, calls.Load())
	}
	t.Log("4K truncation repaired at 8K; completed extraction retained its generation")
}

type nativeBrowserTransport func(*http.Request) (*http.Response, error)

func (f nativeBrowserTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testNativeExplorerBrowser(t *testing.T, ctx context.Context, client *http.Client, tableURL, profile, storageURL string) {
	t.Helper()
	if os.Getenv("ANTFLY_GREPSTEIN_E2E_BROWSER") != "1" {
		return
	}
	target, err := url.Parse(storageURL)
	if err != nil {
		t.Fatal(err)
	}
	storage := &http.Client{Transport: nativeBrowserTransport(func(r *http.Request) (*http.Response, error) {
		clone := r.Clone(r.Context())
		u := *r.URL
		clone.URL = &u
		clone.URL.Scheme, clone.URL.Host = target.Scheme, target.Host
		clone.Header.Set("Authorization", "Bearer local-gcs-e2e-token")
		return http.DefaultTransport.RoundTrip(clone)
	})}
	cs := []explorerCollection{{ID: "fixture", Label: "Native " + profile, Profile: profile, Table: "gcs_e2e", tableURL: tableURL}}
	explorer := httptest.NewServer(explorerCollectionsHandler(cs, client, "e2e-originals", func(context.Context) (*http.Client, error) { return storage, nil }))
	defer explorer.Close()
	script, err := filepath.Abs("native_browser_smoke.mjs")
	if err != nil {
		t.Fatal(err)
	}
	artifactDir := t.TempDir()
	if configured := os.Getenv("ANTFLY_GREPSTEIN_E2E_ARTIFACTS"); configured != "" {
		artifactDir = filepath.Join(configured, strings.ReplaceAll(t.Name(), "/", "-"))
		if err := os.MkdirAll(artifactDir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(ctx, "node", script, explorer.URL, profile, artifactDir)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("real browser smoke: %v\n%s", err, output)
	}
	t.Logf("real browser: %s", output)
}
