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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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
	runner.Profile.Sources[0].Publication = "media"
	source := runner.Profile.Sources[0]
	item := &fixture.inventory.Objects[0]
	item.ID = strings.Replace(item.ID, "manual.txt", "manual.pdf", 1)
	item.Path, item.Filename = "manual.pdf", "manual.pdf"
	data := integrationPDF()
	fixture.data = map[string][]byte{item.ID: data}
	item.Size = int64(len(data))
	item.Metadata = map[string]any{"mime_type": "application/pdf"}
	origin := httptest.NewServer(http.NotFoundHandler())
	defer origin.Close()
	s, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: origin.URL, AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: runner.Factory})
	require.NoError(t, err)
	defer s.Close()
	origin.Config.Handler = s
	publisher := &Publisher{Endpoint: endpoint}
	asset := map[string]any{
		"name": "document_units_v1", "kind": "asset", "template": "{{grep.media.uri}}", "content_type": "application/json",
		"producer_json": `{"type":"document_extraction","config":{}}`,
	}
	chunks := map[string]any{"name": "document_chunks_v1", "kind": "chunk", "source_artifact_name": "document_units_v1", "field": "text", "chunk_size": 512, "chunk_overlap": 0}
	_, err = publisher.call(t.Context(), "POST", tablePath(table), map[string]any{
		"num_shards": 1,
		"indexes": map[string]any{"source_chunks": map[string]any{
			"type": "full_text", "field": "text", "artifact_name": "document_chunks_v1",
			"enrichments": []any{asset, chunks},
		}},
	})
	require.NoError(t, err)
	defer func() {
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
	require.NoError(t, publisher.Batch(t.Context(), table, map[string]any{"foreign": map[string]any{"title": "preserve"}}, nil))
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	response = serviceCall(s, "POST", "/v1/sync", map[string]any{})
	require.Equal(t, 202, response.Code, response.Body.String())
	job = SyncJob{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Eventually(t, func() bool {
		result := serviceCall(s, "GET", "/v1/jobs/"+job.ID, nil)
		_ = json.Unmarshal(result.Body.Bytes(), &job)
		return job.State != "running"
	}, 90*time.Second, 100*time.Millisecond)
	require.Equal(t, "completed", job.State, job)
	require.NoError(t, WaitForAbsent(t.Context(), publisher.Query, table, []string{key}))
	foreign, err := publisher.Query(t.Context(), table, []byte(`{"query":{"doc_id":{"ids":["foreign"]}},"fields":["title"],"limit":1}`))
	require.NoError(t, err)
	require.Contains(t, string(foreign), `"preserve"`)
}

func integrationPDF() []byte {
	stream := "BT /F1 12 Tf 30 700 Td (retry budget) Tj ET"
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
