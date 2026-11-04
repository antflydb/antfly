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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Run against a current standalone server with private-IP remote fetches enabled:
// ANTFLY_SOURCE_TEST_ENDPOINT=http://127.0.0.1:PORT/db/v1 go test -run TestSourceMediaArtifactIntegration
// This deliberately uses the real database, artifact scheduler, remote fetcher,
// extractor, and full-text index rather than publicationFixture.
func TestSourceMediaArtifactIntegration(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_SOURCE_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ANTFLY_SOURCE_TEST_ENDPOINT for real Antfly integration")
	}
	runner, fixture, _ := runnerFixture(t)
	table := fmt.Sprintf("source_media_%d", time.Now().UnixNano())
	runner.Profile.Endpoint = endpoint
	runner.Profile.CatalogTable = table + "_catalog"
	runner.Profile.Sources[0].Table = table
	runner.Profile.Sources[0].Publication = "native"
	source := runner.Profile.Sources[0]
	item := &fixture.inventory.Objects[0]
	item.ID = strings.Replace(item.ID, "manual.txt", "manual.pdf", 1)
	item.Path, item.Filename = "manual.pdf", "manual.pdf"
	data := integrationPDF()
	fixture.data = map[string][]byte{item.ID: data}
	item.Size = int64(len(data))
	item.Metadata = map[string]any{"mime_type": "application/pdf"}
	for i := 1; i < 3; i++ {
		copy := *item
		copy.ID = strings.Replace(item.ID, "manual.pdf", fmt.Sprintf("manual-%d.pdf", i), 1)
		copy.Path, copy.Filename = fmt.Sprintf("manual-%d.pdf", i), fmt.Sprintf("manual-%d.pdf", i)
		fixture.inventory.Objects = append(fixture.inventory.Objects, copy)
		fixture.data[copy.ID] = data
	}
	origin := httptest.NewServer(http.NotFoundHandler())
	defer origin.Close()
	s, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: origin.URL, AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: runner.Factory})
	require.NoError(t, err)
	defer s.Close()
	origin.Config.Handler = s
	publisher := &Publisher{Endpoint: endpoint, Client: &http.Client{Transport: smokeDiagnosticTransport{t}}}
	asset := map[string]any{
		"name": "document_units_v1", "kind": "asset", "template": "{{content.resource.uri}}", "content_type": "application/json",
		"producer_json": `{"type":"document_extraction","config":{}}`,
	}
	chunks := map[string]any{"name": "document_chunks_v1", "kind": "chunk", "source_artifact_name": "document_units_v1", "field": "text", "chunk_size": 512, "chunk_overlap": 0}
	_, err = publisher.call(t.Context(), "POST", tablePath(table), map[string]any{
		"num_shards": 1,
		"indexes": map[string]any{"source_units": map[string]any{"type": "full_text", "sources": []any{map[string]any{"artifact": "document_units_v1", "field": "text"}}}, "source_chunks": map[string]any{
			"type": "full_text", "field": "text", "artifact_name": "document_chunks_v1",
			"enrichments": []any{asset, chunks},
		}},
	})
	require.NoError(t, err)
	defer func() {
		if os.Getenv("ANTFLY_SOURCE_TEST_KEEP_TABLES") == "1" {
			t.Logf("retained native smoke table=%s", table)
			return
		}
		_, _ = publisher.call(context.Background(), "DELETE", tablePath(table), nil)
		_, _ = publisher.call(context.Background(), "DELETE", tablePath(runner.Profile.CatalogTable), nil)
	}()
	// Real indexing may take longer than the short unit-fixture polling budget.
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{})
	require.Equal(t, 202, response.Code, response.Body.String())
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Eventually(t, func() bool {
		result := serviceCall(s, "GET", "/v1/jobs/"+job.ID, nil)
		_ = json.Unmarshal(result.Body.Bytes(), &job)
		return job.State != "running"
	}, 90*time.Second, 100*time.Millisecond)
	require.Equal(t, "completed", job.State, job)
	// A matching member from another source sorts before this source's rows.
	// Source predicates must run before pagination, not after a one-hit page.
	ref, err := s.pinMedia(t.Context(), source, Request{SourceNamespace: "job/" + source.ID, Config: source.Config, Mode: ImportModeSource}, fixture.inventory.Objects[0])
	require.NoError(t, err)
	otherID := "00000000-0000-4000-8000-000000000001"
	otherItem := fixture.inventory.Objects[0]
	otherItem.ID = strings.Replace(otherItem.ID, source.ID, otherID, 1)
	other, err := NormalizeMedia(otherID, otherItem, ref, Permissions{Readers: []string{"user:self"}}, time.Now())
	require.NoError(t, err)
	otherKey, err := RecordKey(otherID, otherItem.ID)
	require.NoError(t, err)
	otherRow, err := SourceRow(otherKey, otherID, other)
	require.NoError(t, err)
	require.NoError(t, publisher.Batch(t.Context(), table, map[string]any{otherKey: otherRow}, nil))
	key, err := RecordKey(source.ID, fixture.inventory.Objects[0].ID)
	require.NoError(t, err)
	path := tablePath(table) + "/documents/" + url.PathEscape(key) + "/artifacts/document_units_v1?detail=raw"
	var manifest map[string]any
	require.Eventually(t, func() bool {
		raw, err := publisher.call(t.Context(), "GET", path, nil)
		if err != nil {
			return false
		}
		if json.Unmarshal(raw, &manifest) != nil {
			return false
		}
		return manifest["unit_count"] == float64(1)
	}, 90*time.Second, 250*time.Millisecond)
	require.Equal(t, "application/pdf", manifest["content_type"])
	require.Equal(t, "pdf", manifest["route_type"])
	require.Eventually(t, func() bool {
		raw, err := publisher.Query(t.Context(), table, []byte(`{"full_text_index":"source_chunks","full_text_search":{"field":"text","match":"retry"},"limit":10}`))
		if err != nil {
			return false
		}
		var result struct {
			Responses []struct {
				Hits struct {
					Hits []json.RawMessage `json:"hits"`
				} `json:"hits"`
			} `json:"responses"`
		}
		return json.Unmarshal(raw, &result) == nil && len(result.Responses) > 0 && len(result.Responses[0].Hits.Hits) > 0
	}, 60*time.Second, 250*time.Millisecond)
	if binary := os.Getenv("ANTFLY_GREP_TEST_BINARY"); binary != "" {
		require.Eventually(t, func() bool {
			raw, err := publisher.call(t.Context(), "GET", tablePath(table)+"/indexes/source_units", nil)
			if err != nil {
				return false
			}
			var status struct {
				Status struct{ Readiness struct{ Complete bool } }
			}
			return json.Unmarshal(raw, &status) == nil && status.Status.Readiness.Complete
		}, 90*time.Second, 250*time.Millisecond)
		config := map[string]any{"version": 1, "endpoint": endpoint, "page_size": 1, "principals": []string{"user:self"}, "tables": []any{map[string]any{
			"name": "native-pdfs", "table": table, "fields": CanonicalDocumentFields(), "source_filter": json.RawMessage(SourceFilter(source.ID)),
			"streams": []TextStream{{Name: "pages", Artifact: "document_units_v1", Index: "source_units", Field: "text", Purpose: "extraction", Default: true, Boundary: "unit", LocationField: "provenance"}},
		}}}
		file := filepath.Join(t.TempDir(), "grep.json")
		require.NoError(t, writeJSON(file, config, 1<<20))
		run := func(pattern string, args ...string) ([]byte, error) {
			command := []string{"--table-config", file, "--json"}
			command = append(command, args...)
			command = append(command, pattern)
			return exec.CommandContext(t.Context(), binary, command...).CombinedOutput()
		}

		output, err := run("retry")
		require.NoError(t, err, string(output))
		require.Equal(t, 3, bytes.Count(output, []byte(`"type":"match"`)), string(output))
		require.Contains(t, string(output), `"artifact_key"`)
		require.Contains(t, string(output), `"document_units_v1"`)
		require.Contains(t, string(output), `"text_field":"text"`)
		output, err = run("no-such-phrase")
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, 1, exit.ExitCode(), string(output))
		output, err = run("retry", "--max-index-documents", "1")
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, 2, exit.ExitCode(), string(output))
		output, err = run("retry", "--artifact", "not-configured")
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, 2, exit.ExitCode(), string(output))
	}
	require.NoError(t, publisher.Batch(t.Context(), table, map[string]any{"foreign": map[string]any{"title": "preserve"}}, nil))
	suspended := source
	suspended.ID = "22222222-2222-4222-8222-222222222222"
	suspended.Alias = "suspended"
	suspended.Connection = "suspended"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+suspended.ID, suspended).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	response = serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, response.Code, response.Body.String())
	job = SyncJob{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Eventually(t, func() bool {
		result := serviceCall(s, "GET", "/v1/jobs/"+job.ID, nil)
		_ = json.Unmarshal(result.Body.Bytes(), &job)
		return job.State != "running"
	}, 90*time.Second, 100*time.Millisecond)
	require.Equal(t, "completed", job.State, job)
	require.Equal(t, "purge", job.Operation)
	require.Equal(t, 1, job.PurgedSourceCount)
	require.Len(t, s.profile.Sources, 1)

	require.NoError(t, WaitForAbsent(t.Context(), publisher.Query, table, []string{key}))
	foreign, err := publisher.Query(t.Context(), table, []byte(`{"query":{"doc_id":{"ids":["foreign"]}},"fields":["title"],"limit":1}`))
	require.NoError(t, err)
	require.Contains(t, string(foreign), `"preserve"`)
}

func integrationPDF() []byte {
	stream := "BT /F1 12 Tf 30 700 Td (retry budget. Native acquisition preserves original bytes and source identity. The database extracts this complete text layer into a revision fenced native artifact. Search clients consume those artifacts without exporting or ingesting duplicate pages.) Tj ET"
	objects := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream),
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
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

type smokeDiagnosticTransport struct{ t *testing.T }

func (transport smokeDiagnosticTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(request)
	if err == nil && response.StatusCode >= 400 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		transport.t.Logf("native smoke API %s %s status=%d body=%s read_error=%v", request.Method, request.URL.Path, response.StatusCode, body, readErr)
	}
	return response, err
}
