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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestStandaloneCommandPublishesS3AndSearchableCatalog(t *testing.T) {
	content := "retry budget\n"
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/manual.txt") {
			if r.Method == http.MethodGet && strings.Trim(r.Header.Get("If-Match"), `"`) != "revision-1" {
				http.Error(w, "missing revision precondition", 412)
				return
			}
			w.Header().Set("Last-Modified", "Fri, 23 Oct 2026 00:00:00 GMT")
			w.Header().Set("ETag", `"revision-1"`)
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", fmt.Sprint(len(content)))
			if r.Method != http.MethodHead {
				fmt.Fprint(w, content)
			}
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>docs</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>manual.txt</Key><LastModified>2026-10-23T00:00:00Z</LastModified><ETag>"revision-1"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>`, len(content))
	}))
	defer provider.Close()
	backend := &publicationFixture{records: map[string]map[string]any{}}
	database := httptest.NewServer(backend)
	defer database.Close()
	root := t.TempDir()
	binary := filepath.Join(root, "antfly-source")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/antfly-source")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	providerURL, _ := url.Parse(provider.URL)
	profile := Profile{Version: 1, Endpoint: database.URL + "/api/v1", DataPlaneTokenEnv: "SOURCE_TEST_DATABASE_TOKEN", Connections: map[string]Connection{"docs": {Provider: ProviderS3, S3Endpoint: providerURL.Host, S3Insecure: true, AccessKeyEnv: "SOURCE_TEST_ACCESS", SecretKeyEnv: "SOURCE_TEST_SECRET"}}, Sources: []Source{{ID: testSourceID, Alias: "manuals", Connection: "docs", Table: "sources", Config: json.RawMessage(`{"bucket":"docs"}`), Permissions: &Permissions{Public: true}}}}
	config := filepath.Join(root, "profile.json")
	if err := writeJSON(config, profile, 1<<20); err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(root, "catalog.json")
	command := exec.CommandContext(t.Context(), binary, "sync", "--config", config, "--state-dir", filepath.Join(root, "state"), "--catalog-out", catalog)
	command.Env = append(os.Environ(), "SOURCE_TEST_DATABASE_TOKEN=database-test-token", "SOURCE_TEST_ACCESS=test-access", "SOURCE_TEST_SECRET=test-secret-value")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("self-hosted sync: %v: %s", err, output)
	}
	raw, err := os.ReadFile(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "test-secret-value") || strings.Contains(string(raw), "database-test-token") {
		t.Fatal("catalog persisted credentials")
	}
	var result Catalog
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Sources) != 1 || result.Sources[0].Status != "searchable" || result.Sources[0].Table != "sources" {
		t.Fatalf("invalid catalog: %s", raw)
	}
	backend.mu.Lock()
	if len(backend.records) != 1 {
		t.Fatal("CLI did not publish the canonical item")
	}
	backend.mu.Unlock()
	grep := os.Getenv("ANTFLY_GREP_TEST_BINARY")
	if grep == "" {
		return
	}
	search := exec.CommandContext(t.Context(), grep, "--source-catalog", catalog, "--source", "manuals", "retry budget")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "ANTFLY_GREP_") && !strings.HasPrefix(entry, "RIPGREP_CONFIG_PATH=") {
			search.Env = append(search.Env, entry)
		}
	}
	search.Env = append(search.Env, "ANTFLY_GREP_CONFIG_PATH=", "RIPGREP_CONFIG_PATH=", "ANTFLY_GREP_TABLE_TOKEN=database-test-token", "PATH="+t.TempDir())
	output, err := search.CombinedOutput()
	if err != nil {
		t.Fatalf("search standalone output without Cloud: %v: %s", err, output)
	}
	if !strings.Contains(string(output), "retry budget") {
		t.Fatalf("missing source text: %s", output)
	}
}
