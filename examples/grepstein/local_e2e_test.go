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
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Runs the real standalone, storage owner, GCS JSON client, PDF extractor and
// artifact indexes. Only Google storage is a loopback protocol fixture; this
// does not qualify GKE identity exchange or a real-model OCR/embedding workload.
func TestLocalNativeGCSExtraction(t *testing.T) {
	binary := os.Getenv("ANTFLY_GREPSTEIN_E2E_BINARY")
	if binary == "" {
		t.Skip("set ANTFLY_GREPSTEIN_E2E_BINARY to the native Antfly binary")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	root := t.TempDir()
	pdf := nativeGCSFixturePDF()
	digest := fmt.Sprintf("%x", sha256.Sum256(pdf))
	key := "objects/sha256/" + digest + ".pdf"
	var reads atomic.Int64
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/storage/v1/b/e2e-originals/o/"+key || r.URL.Query().Get("alt") != "media" || r.Header.Get("Authorization") != "Bearer local-gcs-e2e-token" {
			http.Error(w, "invalid scoped GCS request", 403)
			return
		}
		generation := r.URL.Query().Get("generation")
		if generation != "123" && generation != "124" {
			http.Error(w, "immutable generation required", 400)
			return
		}
		reads.Add(1)
		w.Header().Set("x-goog-generation", "123")
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("Content-Length", strconv.Itoa(len(pdf)))
		w.Write(pdf)
	}))
	t.Cleanup(provider.Close)
	config := map[string]any{
		"remote_content":   map[string]any{"security": map[string]any{"block_private_ips": false, "allowed_hosts": []string{"127.0.0.1"}, "max_download_size_bytes": 64 << 20}},
		"secrets":          map[string]any{"native": map[string]any{"name": "native", "path": filepath.Join(root, "secrets.json")}, "environment": true},
		"inference":        map[string]any{"models_dir": filepath.Join(root, "models")},
		"public_inference": map[string]any{"enabled": false},
		"connections":      map[string]any{"gcs-e2e": map[string]any{"kind": "external_io", "capabilities": []string{"content.fetch"}, "external_io": map[string]any{"protocol": "gcs", "buckets": []string{"e2e-originals"}, "prefix": "objects/sha256", "endpoint": provider.URL + "/storage/v1", "credentials": map[string]any{"source": "bearer_token", "bearer_token": "${secret:antfly.e2e.gcs.token}"}}}},
	}
	configPath := filepath.Join(root, "antfly.json")
	if err := atomicCorpusJSON(configPath, config); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	logPath := filepath.Join(root, "server.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "standalone", "--config", configPath, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--data-dir", filepath.Join(root, "data"), "--control-tick-ms", "5", "--health", "false", "--auth", "false")
	command.Dir = root
	command.Env = append(os.Environ(), "ANTFLY_E2E_GCS_TOKEN=local-gcs-e2e-token")
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var processErr error
	go func() { processErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		command.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			command.Process.Kill()
			<-done
		}
		logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			if len(data) > 12000 {
				data = data[len(data)-12000:]
			}
			t.Logf("native server log:\n%s", data)
		}
	})
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/db/v1", port)
	client := &http.Client{Timeout: 10 * time.Second}
	ready := false
	for started := time.Now(); time.Since(started) < 90*time.Second; {
		select {
		case <-done:
			t.Fatalf("native server exited: %v", processErr)
		default:
		}
		if _, err := corpusRequestWithContext(ctx, client, "GET", endpoint+"/tables", nil); err == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("local native server did not become ready")
	}
	indexes, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, "", "", false, false, corpusIndexOptions{SourceConnection: "gcs-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	tableURL := endpoint + "/tables/gcs_e2e"
	if _, err := corpusRequestWithContext(ctx, client, "POST", tableURL, map[string]any{"num_shards": 1, "schema": corpusTableSchema(), "indexes": indexes}); err != nil {
		t.Fatal(err)
	}
	source := "gs://e2e-originals/" + key + "?generation=123"
	document := map[string]any{"url": source, "original_url": source, "filename": "evidence.pdf", "mime_type": "application/pdf", "version": digest + ":123", "metadata": map[string]any{"page_start": 1, "page_end": 1}}
	if _, err := corpusRequestWithContext(ctx, client, "POST", tableURL+"/batch", map[string]any{"inserts": map[string]any{"evidence": document}, "sync_level": "write"}); err != nil {
		t.Fatal(err)
	}
	record := corpusRecord{ID: "evidence", Document: document}
	manifest, _ := json.Marshal(record)
	manifest = append(manifest, '\n')
	manifestHash := fmt.Sprintf("%x", sha256.Sum256(manifest))
	if err := os.WriteFile(filepath.Join(root, "published.ndjson"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := atomicCorpusJSON(filepath.Join(root, "publication.json"), map[string]any{"sha256": manifestHash, "records": 1}); err != nil {
		t.Fatal(err)
	}
	var qualification extractionQualification
	var last error
	for started := time.Now(); time.Since(started) < 90*time.Second; {
		qualification, last = qualifyCorpus(client, tableURL, root)
		if last == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last != nil || !qualification.Complete || qualification.Pages != 1 {
		t.Fatalf("native GCS qualification: %+v %v", qualification, last)
	}
	query := map[string]any{"fields": []string{"text", "provenance"}, "hierarchy": map[string]any{"children": map[string]any{"level": "unit", "parent": map[string]any{"level": "source", "id": "evidence"}}}, "order_by": []any{map[string]any{"field": "_hierarchy.position"}}, "limit": 2}
	data, err := corpusRequestWithContext(ctx, client, "POST", tableURL+"/query", query)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("Native GCS extraction evidence")) {
		t.Fatalf("missing actual PDF text: %s", data)
	}
	if reads.Load() < 1 {
		t.Fatal("artifact did not fetch from the GCS JSON API")
	}
	t.Logf("native GCS PDF fetch, actual text extraction and qualification passed (%d provider reads)", reads.Load())
	// Exercise owner-side policy failures and immutable-version rejection without
	// contaminating the successfully qualified source inventory.
	badURL := endpoint + "/tables/gcs_e2e_rejections"
	if _, err := corpusRequestWithContext(ctx, client, "POST", badURL, map[string]any{"num_shards": 1, "schema": corpusTableSchema(), "indexes": indexes}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		id, source, code string
		fetch            bool
	}{
		{"wrong_bucket", strings.Replace(source, "e2e-originals", "private-other", 1), "GcsConnectionNotFoundOrOutOfScope", false},
		{"wrong_prefix", strings.Replace(source, "objects/sha256/", "elsewhere/", 1), "GcsConnectionNotFoundOrOutOfScope", false},
		{"wrong_generation", strings.Replace(source, "generation=123", "generation=124", 1), "PreconditionFailed", true},
	} {
		before := reads.Load()
		row := map[string]any{}
		for k, v := range document {
			row[k] = v
		}
		row["url"] = test.source
		row["version"] = test.id
		if _, err := corpusRequestWithContext(ctx, client, "POST", badURL+"/batch", map[string]any{"inserts": map[string]any{test.id: row}, "sync_level": "write"}); err != nil {
			t.Fatal(err)
		}
		found := false
		for started := time.Now(); time.Since(started) < 60*time.Second; {
			raw, err := corpusRequestWithContext(ctx, client, "GET", badURL+"/documents/"+url.PathEscape(test.id)+"/artifacts/"+corpusUnits+"?detail=raw", nil)
			if err == nil {
				var artifact struct {
					Manifest string `json:"manifest_json"`
				}
				if json.Unmarshal(raw, &artifact) == nil {
					var state struct {
						Merge struct {
							Status string `json:"status"`
						} `json:"merge_plan"`
						Error struct {
							Code string `json:"code"`
						} `json:"last_error"`
					}
					if json.Unmarshal([]byte(artifact.Manifest), &state) == nil && state.Error.Code != "" {
						if state.Error.Code != test.code || state.Merge.Status != "failed" {
							t.Fatalf("%s: unexpected failure %s (merge=%s)", test.id, state.Error.Code, state.Merge.Status)
						}
						found = true
						break
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !found {
			t.Fatalf("%s: no terminal scoped/version failure", test.id)
		}
		if !test.fetch && reads.Load() != before {
			t.Fatalf("%s contacted storage outside scope", test.id)
		}
		if test.fetch && reads.Load() == before {
			t.Fatal("generation mismatch was not checked against the provider")
		}
	}
	t.Log("owner-side bucket/prefix denials and generation mismatch are terminal and scoped")
}

func nativeGCSFixturePDF() []byte {
	content := "BT /F1 12 Tf 40 700 Td (Native GCS extraction evidence) Tj ET\n"
	objects := []string{"<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>", "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>", "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>", fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(content), content)}
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, object := range objects {
		offsets = append(offsets, out.Len())
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, object)
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&out, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xref)
	return out.Bytes()
}
