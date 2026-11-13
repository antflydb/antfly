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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testExplorerCollections = `[{"id":"pdf","label":"PDF records","table":"pdf_table","profile":"pdf"},{"id":"audio","label":"Audio recordings","table":"audio_table","profile":"transcription"}]`

func collectionFixture(t *testing.T, endpoint string) []explorerCollection {
	t.Helper()
	c, err := parseExplorerCollections(endpoint, "ignored", testExplorerCollections)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestExplorerCollectionsRejectUnscopedConfiguration(t *testing.T) {
	for _, raw := range []string{`[]`, `null`, testExplorerCollections + ` {}`, `[{"id":"all","table":"records","profile":"pdf"}]`, `[{"id":"audio","table":"../private","profile":"transcription"}]`, `[{"id":"pdf","table":"records","profile":"unknown"}]`, `[{"id":"pdf","table":"records"}]`, `[{"id":"pdf","table":"records","profile":"pdf","api_key":"token"}]`, `[{"id":"pdf","table":"records","profile":"pdf"},{"id":"pdf","table":"audio","profile":"transcription"}]`, `[{"id":"pdf","table":"records","profile":"pdf"},{"id":"audio","table":"records","profile":"transcription"}]`} {
		if _, err := parseExplorerCollections("https://native.test/db/v1", "records", raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, endpoint := range []string{"https://user:password@native.test/db/v1", "file:///etc/passwd", "https://native.test/db/v1?secret=value"} {
		if _, err := parseExplorerCollections(endpoint, "records", testExplorerCollections); err == nil {
			t.Error("accepted unsafe endpoint")
		}
	}
}
func TestExplorerCollectionSearchBoundsScopeAndTiming(t *testing.T) {
	t.Setenv("ANTFLY_API_KEY", "read-only-fixture")
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer read-only-fixture" {
			t.Error("wrong credential or method")
		}
		if r.URL.Path != "/tables/pdf_table/query" && r.URL.Path != "/tables/audio_table/query" {
			t.Error("escaped configured tables")
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if payload["limit"] != float64(10) {
			t.Error("combined source result bound changed")
		}
		if strings.Contains(r.URL.Path, "audio_table") {
			raw, _ := json.Marshal(payload["hierarchy"])
			if !strings.Contains(string(raw), "provenance.start_ms") || !strings.Contains(string(raw), "provenance.end_ms") {
				t.Error("audio citations lost temporal evidence")
			}
		}
		fmt.Fprint(w, `{"responses":[{"hits":{"hits":[]}}]}`)
	}))
	defer upstream.Close()
	h := explorerCollectionsHandler(collectionFixture(t, upstream.URL), upstream.Client(), "", nil)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/api/search?q=hello&collection=all&table=private&limit=999999", nil))
	if out.Code != 200 || requests.Load() != 2 || strings.Contains(out.Body.String(), "read-only-fixture") {
		t.Fatal("combined scoped search failed", out.Code)
	}
	var result struct {
		Responses []struct {
			Collection struct {
				ID      string `json:"id"`
				Profile string `json:"profile"`
			} `json:"collection"`
		} `json:"responses"`
	}
	if err := json.Unmarshal(out.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Responses) != 2 || result.Responses[0].Collection.ID != "pdf" || result.Responses[1].Collection.Profile != "transcription" {
		t.Fatal("collection identity lost")
	}
	for _, p := range []string{"/api/search?q=hello&collection=private", "/original?id=x&collection=all", "/pdf?id=x&collection=audio"} {
		out = httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("GET", p, nil))
		if out.Code < 400 {
			t.Error("accepted invalid collection/path", p)
		}
	}
	if requests.Load() != 2 {
		t.Fatal("invalid request reached upstream")
	}
	for _, method := range []string{"POST", "DELETE", "PUT"} {
		out = httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(method, "/api/search?q=x", nil))
		if out.Code != 405 {
			t.Error("accepted mutation")
		}
	}
}
func TestExplorerCombinedFailureCancelsSiblingWithoutFalseSuccess(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		if strings.Contains(r.URL.Path, "audio_table") {
			close(started)
			select {
			case <-r.Context().Done():
				close(cancelled)
			case <-release:
			}

			return
		}
		<-started
		http.Error(w, "fixture failure", 500)
	}))
	defer upstream.Close()
	defer close(release)
	h := explorerCollectionsHandler(collectionFixture(t, upstream.URL), upstream.Client(), "", nil)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/api/search?q=x", nil))
	if out.Code != 502 {
		t.Fatal("partial response reported success")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("failed aggregate left sibling running")
	}
}
func TestExplorerMalformedOrFailedNativeQueryNeverLooksEmpty(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"responses":[]}`, `{"responses":[null]}`, `{"responses":[{"error":"unavailable"}]}`, `{"errors":["unavailable"]}`} {
		client := &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) { return response(200, body), nil })}
		h := explorerCollectionsHandler(collectionFixture(t, "https://native.test"), client, "", nil)
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("GET", "/api/search?q=x", nil))
		if out.Code != 502 {
			t.Errorf("accepted invalid query response %s", body)
		}
	}
}
func TestAudioOriginalRangeHeadAndProfileBoundaries(t *testing.T) {
	t.Setenv("ANTFLY_API_KEY", "read-only-fixture")
	source := "gs://private-originals/objects/sha256/" + strings.Repeat("a", 64) + ".m4a?generation=123"
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/tables/audio_table/documents/record" || r.Header.Get("Authorization") != "Bearer read-only-fixture" {
			t.Error("unscoped native lookup")
		}
		raw, _ := json.Marshal(map[string]any{"original_url": source})
		return response(200, string(raw)), nil
	})}
	var reads atomic.Int32
	storage := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		reads.Add(1)
		if r.Header.Get("Authorization") != "" || r.URL.Host != "storage.googleapis.com" || r.URL.Query().Get("generation") != "123" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("storage credential/generation boundary failed")
		}
		if r.Method == "HEAD" {
			res := response(200, "")
			res.Header.Set("Content-Length", "100")
			return res, nil
		}
		if r.Header.Get("Range") != "bytes=2-5" {
			t.Error("audio seek range lost")
		}
		res := response(206, "data")
		res.Header.Set("Content-Range", "bytes 2-5/100")
		return res, nil
	})}
	h := explorerCollectionsHandler(collectionFixture(t, "https://native.test"), client, "private-originals", func(context.Context) (*http.Client, error) { return storage, nil })
	req := httptest.NewRequest("GET", "/original?id=record&collection=audio", nil)
	req.Header.Set("Range", "bytes=2-5")
	out := httptest.NewRecorder()
	h.ServeHTTP(out, req)
	if out.Code != 206 || out.Body.String() != "data" || out.Header().Get("Content-Type") != "audio/mp4" || out.Header().Get("Content-Range") == "" {
		t.Fatal("audio seeking failed", out.Code)
	}
	out = httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("HEAD", "/original?id=record&collection=audio", nil))
	if out.Code != 200 || out.Body.Len() != 0 || out.Header().Get("Content-Length") != "100" {
		t.Fatal("HEAD fetched/wrote media body")
	}
	if storage.Timeout != 0 {
		t.Fatal("mutated shared storage client")
	}
	req.Header.Set("Range", "bytes=0-2,4-6")
	out = httptest.NewRecorder()
	h.ServeHTTP(out, req)
	if out.Code != 416 || reads.Load() != 2 {
		t.Fatal("multipart range accepted")
	}
	for _, bad := range []string{strings.Replace(source, ".m4a", ".pdf", 1), strings.Replace(source, "private-originals", "another-bucket", 1), strings.Replace(source, "generation=123", "generation=123&generation=456", 1)} {
		source = bad
		req.Header.Del("Range")
		out = httptest.NewRecorder()
		h.ServeHTTP(out, req)
		if out.Code != 404 || reads.Load() != 2 {
			t.Fatal("source escaped audio profile")
		}
	}
}
func TestProfileMediaTypesAndGraphLimits(t *testing.T) {
	for _, ext := range []string{"wav", "mp3", "m4a", "ogg", "opus", "aac", "aif", "aiff", "caf", "flac"} {
		u, _ := url.Parse("gs://originals/objects/sha256/" + strings.Repeat("b", 64) + "." + ext + "?generation=3")
		if profileOriginalContentType(u, "originals", "transcription") == "" || profileOriginalContentType(u, "originals", "pdf") != "" {
			t.Error("incorrect media profile", ext)
		}
	}
	payload, err := explorerProfileQuery("hello", "graph", "transcription")
	if err != nil || payload["limit"] != 8 {
		t.Fatal("graph root bound changed")
	}
}

func TestExplorerClientInteractionLogic(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node is required for explorer client interaction tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, node, "--test", "explorer_ui_test.mjs").CombinedOutput()
	if err != nil {
		t.Fatalf("explorer client interactions: %v\n%s", err, output)
	}
}
