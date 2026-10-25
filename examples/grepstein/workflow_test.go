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
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

func TestDOJInventoryTraversesAllPagesAndRejectsAccessGate(t *testing.T) {
	gated := false
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if gated {
			fmt.Fprint(w, "Confirm your age")
			return
		}
		fmt.Fprint(w, `<a href="?page=1">Next</a>`)
		if r.URL.Query().Get("page") == "1" {
			fmt.Fprint(w, `<a href="/epstein/files/DataSet%201/EFTA00000002.pdf">PDF</a>`)
		} else {
			fmt.Fprint(w, `<a href="/epstein/files/DataSet%201/EFTA00000001.pdf">PDF</a>`)
		}
	}))
	defer server.Close()
	files, err := discoverDOJ(server.Client(), server.URL+"/release")
	if err != nil || len(files) != 2 || requests != 2 {
		t.Fatalf("incomplete inventory: %v %v %d", files, err, requests)
	}
	gated = true
	if _, err = discoverDOJ(server.Client(), server.URL+"/release"); err == nil {
		t.Fatal("age page became a complete inventory")
	}
}

func TestDownloadRejectsHTMLAndRepairsTamperedCache(t *testing.T) {
	html := true
	requests := 0
	pdf := createMinimalPDF()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if html {
			fmt.Fprint(w, "<html>age gate</html>")
		} else {
			w.Write(pdf)
		}
	}))
	defer server.Close()
	dest := filepath.Join(t.TempDir(), "source.pdf")
	if err := downloadPDF(server.Client(), server.URL, dest); err == nil {
		t.Fatal("accepted non-PDF")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("published rejected download")
	}
	html = false
	if err := downloadPDF(server.Client(), server.URL, dest); err != nil {
		t.Fatal(err)
	}
	if err := downloadPDF(server.Client(), server.URL, dest); err != nil || requests != 2 {
		t.Fatalf("cache was not verified: %v", err)
	}
	os.WriteFile(dest, []byte("tampered"), 0600)
	if err := downloadPDF(server.Client(), server.URL, dest); err != nil || requests != 3 {
		t.Fatalf("tampered cache accepted: %v", err)
	}
}

func TestGCSCreateOnlyRetriesVerifyExistingObject(t *testing.T) {
	data := []byte("canonical page\n")
	md := md5.Sum(data)
	sum := base64.StdEncoding.EncodeToString(md[:])
	requests := 0
	bad := false
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.URL.Host != "storage.googleapis.com" {
			t.Fatal("credential destination changed")
		}
		if r.Method == "POST" {
			if r.URL.Query().Get("ifGenerationMatch") != "0" {
				t.Fatal("upload may overwrite originals")
			}
			return response(412, ""), nil
		}
		checksum := sum
		if bad {
			checksum = "different"
		}
		return response(200, fmt.Sprintf(`{"generation":"42","size":%q,"md5Hash":%q}`, fmt.Sprint(len(data)), checksum)), nil
	})}
	object, err := putGCS(client, "private-bucket", "objects/key.txt", "text/plain", bytes.NewReader(data), int64(len(data)), sum)
	if err != nil || object.Generation != "42" || requests != 2 {
		t.Fatalf("retry: %v %v", object, err)
	}
	bad = true
	if _, err = putGCS(client, "private-bucket", "objects/key.txt", "text/plain", bytes.NewReader(data), int64(len(data)), sum); err == nil {
		t.Fatal("accepted different remote bytes")
	}
}

func TestCloudProjectionPublishesTextAndUsesExistingSourceJobs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "doj-01-a")
	os.Mkdir(dir, 0700)
	page := grepPage{Item: sources.Item{Path: "doj-01/source/page-0000001.txt"}, Content: "native artifact bytes\n"}
	data, _ := json.Marshal(page)
	os.WriteFile(filepath.Join(dir, "page.json"), data, 0600)
	uploaded := 0
	client := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		uploaded++
		if r.Method != "POST" || !strings.HasPrefix(r.URL.Query().Get("name"), "grep/v1/revision/doj-01-a/") {
			t.Fatal("unscoped projection")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != page.Content {
			t.Fatal("projection rewrote extracted text")
		}
		digest := md5.Sum(body)
		return response(200, fmt.Sprintf(`{"generation":"42","size":%q,"md5Hash":%q}`, fmt.Sprint(len(body)), base64.StdEncoding.EncodeToString(digest[:]))), nil
	})}
	plan := filepath.Join(t.TempDir(), "jobs.json")
	if err := publishGrepProjectionWithClient(client, root, "revision", "derived", "connection-id", "grepstein_pages", plan); err != nil {
		t.Fatal(err)
	}
	var jobs []cloudSourceJob
	if err := readCorpusJSON(plan, &jobs); err != nil {
		t.Fatal(err)
	}
	if uploaded != 1 || len(jobs) != 1 || jobs[0].Mode != "source" || jobs[0].SourceConfig["permission_ttl_seconds"] != float64(86400) {
		t.Fatal("Cloud catalog must be owned by scheduled source jobs")
	}
}

func TestCloudJobsRecoverLostCreateResponseAndKeepPolicy(t *testing.T) {
	job := cloudSourceJob{Name: "grepstein-doj-01-a", ProviderAuthID: "connection", TargetTable: "pages", Mode: "source", RunPolicy: "scheduled", ScheduleInterval: 3600, SourceConfig: map[string]any{"bucket": "private", "permissions": map[string]any{"public": true}}}
	var created []cloudSourceJob
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer management-token" {
			t.Error("missing auth")
		}
		if r.Method == "GET" {
			json.NewEncoder(w).Encode(map[string]any{"data": created})
			return
		}
		posts++
		var received cloudSourceJob
		json.NewDecoder(r.Body).Decode(&received)
		received.ID = "job-id"
		received.TableName = received.TargetTable
		received.TargetTable = ""
		created = append(created, received)
		http.Error(w, "response lost after commit", 503)
	}))
	defer server.Close()
	if err := reconcileCloudJobs(server.Client(), server.URL, "management-token", []cloudSourceJob{job}); err == nil {
		t.Fatal("lost response reported success")
	}
	if err := reconcileCloudJobs(server.Client(), server.URL, "management-token", []cloudSourceJob{job}); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatal("retry duplicated Cloud jobs")
	}
	job.SourceConfig = map[string]any{"bucket": "changed"}
	if err := reconcileCloudJobs(server.Client(), server.URL, "management-token", []cloudSourceJob{job}); err == nil {
		t.Fatal("changed acquisition scope was silently reused")
	}
}

func TestSnapshotReadsAreRevisionAndContentBound(t *testing.T) {
	root := t.TempDir()
	text := "one page\n"
	digest := sha256.Sum256([]byte(text))
	page := grepPage{Item: sources.Item{Path: "page.txt", Revision: "artifact-42", SourceKind: "gcs", Metadata: map[string]any{"content_sha256": fmt.Sprintf("%x", digest)}}, Content: text}
	raw, _ := json.Marshal(page)
	os.WriteFile(filepath.Join(root, "page.json"), raw, 0600)
	config, _ := json.Marshal(grepSnapshotConfig{root, "snapshot-42"})
	request := sources.Request{SourceNamespace: "job/identity", Config: config}
	connector := snapshotConnector{}
	inventory, err := connector.Enumerate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	item := inventory.Objects[0]
	if _, err = connector.Read(context.Background(), request, item); err != nil {
		t.Fatal(err)
	}
	if err = connector.ValidateAbsentSources(context.Background(), request, []string{item.ID}); err == nil {
		t.Fatal("present page was authorized for deletion")
	}
	page.Content = "changed"
	raw, _ = json.Marshal(page)
	os.WriteFile(filepath.Join(root, "page.json"), raw, 0600)
	if _, err = connector.Read(context.Background(), request, item); err == nil {
		t.Fatal("snapshot corruption accepted")
	}
}

func TestEnrichmentDoesNotChangeImportIdentity(t *testing.T) {
	baseline, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, DefaultInferenceURL, "", false, false)
	if err != nil {
		t.Fatal(err)
	}
	enriched, err := corpusIndexes(DefaultEmbeddingModel, DefaultAutographModel, DefaultInferenceURL, "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if corpusDescription(baseline, 25) != corpusDescription(enriched, 25) {
		t.Fatal("enrichment requires reloading source rows")
	}
	data, _ := json.Marshal(baseline)
	if strings.Contains(string(data), "apple") || strings.Contains(string(data), "docsaf") {
		t.Fatal("Cloud indexes depend on local extraction")
	}
}

func TestExplorerHasFixedReadOnlyQueryBoundary(t *testing.T) {
	t.Setenv("ANTFLY_API_KEY", "table-read-token")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tables/corpus/query" || r.Header.Get("Authorization") != "Bearer table-read-token" {
			t.Error("unscoped request")
		}
		var request map[string]any
		json.NewDecoder(r.Body).Decode(&request)
		if request["limit"] != float64(20) {
			t.Error("browser controlled query limit")
		}
		fmt.Fprint(w, `{"responses":[{"hits":{"hits":[]}}]}`)
	}))
	defer upstream.Close()
	handler := explorerHandler(upstream.URL+"/tables/corpus", upstream.Client())
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, httptest.NewRequest("GET", "/api/search?q=hello&limit=9999", nil))
	if result.Code != 200 || strings.Contains(result.Body.String(), "table-read-token") {
		t.Fatal("read-only proxy failed")
	}
	result = httptest.NewRecorder()
	handler.ServeHTTP(result, httptest.NewRequest("POST", "/api/search", nil))
	if result.Code != 405 {
		t.Fatal("browser mutation accepted")
	}
	if _, err := explorerQuery("hello", "arbitrary"); err == nil {
		t.Fatal("arbitrary query mode accepted")
	}
}

func TestCompleteReadinessRequiresEveryShardAndServingMilestone(t *testing.T) {
	for _, payload := range []string{
		`{}`,
		`{"status":{"readiness":{"complete":true,"queryable":true}}}`,
		`{"status":{"readiness":{"complete":true,"queryable":true}},"shard_status":{"one":{"readiness":{"complete":true,"queryable":false}}}}`,
	} {
		if ready, err := indexComplete([]byte(payload)); err != nil || ready {
			t.Fatalf("incomplete readiness accepted: %s %v", payload, err)
		}
	}
	complete := []byte(`{"status":{"readiness":{"complete":true,"queryable":true}},"shard_status":{"one":{"readiness":{"complete":true,"queryable":true}}}}`)
	if ready, err := indexComplete(complete); err != nil || !ready {
		t.Fatal("complete readiness rejected")
	}
	if _, err := indexComplete([]byte(`{"status":{"readiness":{"state":"failed","pending_reasons":["provider_failed"]}}}`)); err == nil {
		t.Fatal("terminal provider failure was hidden")
	}
}
func TestStoredSnapshotCannotBeReusedAfterCorruption(t *testing.T) {
	current, existing := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(current, "page.json"), []byte("expected"), 0600)
	os.WriteFile(filepath.Join(existing, "page.json"), []byte("changed"), 0600)
	if err := verifySnapshot(current, existing); err == nil {
		t.Fatal("stored corruption accepted")
	}
}

func TestOriginalPDFProxyConservesRangeAndCredentialBoundaries(t *testing.T) {
	t.Setenv("ANTFLY_API_KEY", "read-key")
	source := "s3://private-originals/objects/sha256/" + strings.Repeat("a", 64) + ".pdf#page=3"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer read-key" {
			t.Error("missing Antfly authentication")
		}
		json.NewEncoder(w).Encode(map[string]any{"original_url": source})
	}))
	defer upstream.Close()
	reads := 0
	storage := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		reads++
		if r.URL.Host != "storage.googleapis.com" || r.Header.Get("Authorization") != "" || r.Header.Get("Range") != "bytes=0-4" {
			t.Fatal("PDF credential or range boundary changed")
		}
		result := response(206, "%PDF-")
		result.Header.Set("Content-Range", "bytes 0-4/100")
		return result, nil
	})}
	handler := explorerHandlerWithStorage(upstream.URL, upstream.Client(), "private-originals", func(context.Context) (*http.Client, error) { return storage, nil })
	request := httptest.NewRequest("GET", "/pdf?id=document", nil)
	request.Header.Set("Range", "bytes=0-4")
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, request)
	if result.Code != 206 || result.Body.String() != "%PDF-" || result.Header().Get("Content-Range") == "" {
		t.Fatalf("PDF range failed: %d", result.Code)
	}
	source = "s3://other-bucket/objects/sha256/" + strings.Repeat("a", 64) + ".pdf"
	result = httptest.NewRecorder()
	handler.ServeHTTP(result, request)
	if result.Code != 404 || reads != 1 {
		t.Fatal("PDF request escaped configured bucket")
	}
}

func TestSealedPublicationRejectsMissingAndDuplicateRecords(t *testing.T) {
	state := t.TempDir()
	manifest := []byte("{\"id\":\"one\",\"document\":{}}\n{\"id\":\"two\",\"document\":{}}\n")
	os.WriteFile(filepath.Join(state, "published.ndjson"), manifest, 0600)
	atomicCorpusJSON(filepath.Join(state, "publication.json"), map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(manifest)), "records": 2})
	if err := validatePublication(state); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(state, "published.ndjson"), bytes.SplitAfter(manifest, []byte("\n"))[0], 0600)
	if err := validatePublication(state); err == nil {
		t.Fatal("lost manifest row was published as complete")
	}
	duplicate := bytes.ReplaceAll(manifest, []byte("two"), []byte("one"))
	os.WriteFile(filepath.Join(state, "published.ndjson"), duplicate, 0600)
	atomicCorpusJSON(filepath.Join(state, "publication.json"), map[string]any{"sha256": fmt.Sprintf("%x", sha256.Sum256(duplicate)), "records": 2})
	if err := validatePublication(state); err == nil {
		t.Fatal("duplicate publication IDs accepted")
	}
}
func TestCompletedDownloadSnapshotRejectsMissingPDF(t *testing.T) {
	root := t.TempDir()
	source := "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000001.pdf"
	file := filepath.Join(root, "EFTA00000001.pdf")
	data := createMinimalPDF()
	digest := sha256.Sum256(data)
	os.WriteFile(file, data, 0600)
	atomicCorpusJSON(file+".json", downloadReceipt{URL: source, SHA256: fmt.Sprintf("%x", digest), Bytes: int64(len(data))})
	atomicCorpusJSON(filepath.Join(root, "inventory.json"), []string{source})
	snapshot := downloadSnapshot{Directory: root, Files: 1}
	if err := validateDownloadSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	os.Remove(file)
	if err := validateDownloadSnapshot(snapshot); err == nil {
		t.Fatal("missing downloaded PDF became a complete inventory")
	}
}
