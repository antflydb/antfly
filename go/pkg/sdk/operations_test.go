/*
Copyright 2026 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

func TestReadSSEEvents(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []struct{ event, data string }
	}{
		{
			name:  "single event",
			input: "event: test\ndata: hello\n\n",
			want:  []struct{ event, data string }{{"test", "hello"}},
		},
		{
			name:  "multiple events same type",
			input: "event: msg\ndata: one\ndata: two\ndata: three\n",
			want: []struct{ event, data string }{
				{"msg", "one"},
				{"msg", "two"},
				{"msg", "three"},
			},
		},
		{
			name:  "different event types",
			input: "event: classification\ndata: {\"type\":\"search\"}\nevent: hit\ndata: {\"id\":\"1\"}\nevent: done\ndata: {}\n",
			want: []struct{ event, data string }{
				{"classification", `{"type":"search"}`},
				{"hit", `{"id":"1"}`},
				{"done", "{}"},
			},
		},
		{
			name:  "data without event type",
			input: "data: orphan\n",
			want:  []struct{ event, data string }{{"", "orphan"}},
		},
		{
			name:  "event type persists",
			input: "event: generation\ndata: chunk1\ndata: chunk2\nevent: done\ndata: {}\n",
			want: []struct{ event, data string }{
				{"generation", "chunk1"},
				{"generation", "chunk2"},
				{"done", "{}"},
			},
		},
		{
			name:  "ignores non-sse lines",
			input: "comment line\nevent: test\ndata: value\nrandom\n",
			want:  []struct{ event, data string }{{"test", "value"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []struct{ event, data string }
			for event, data := range readSSEEvents(strings.NewReader(tt.input)) {
				got = append(got, struct{ event, data string }{event, data})
			}

			if len(got) != len(tt.want) {
				t.Errorf("got %d events, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i].event != tt.want[i].event {
					t.Errorf("event[%d].event = %q, want %q", i, got[i].event, tt.want[i].event)
				}
				if got[i].data != tt.want[i].data {
					t.Errorf("event[%d].data = %q, want %q", i, got[i].data, tt.want[i].data)
				}
			}
		})
	}
}

// chunkedReader splits reads at arbitrary boundaries to test partial line handling
type chunkedReader struct {
	data      string
	chunkSize int
	pos       int
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	end := min(r.pos+r.chunkSize, len(r.data))
	n = copy(p, r.data[r.pos:end])
	r.pos = end
	return n, nil
}

func TestReadSSEEventsPartialLines(t *testing.T) {
	input := "event: classification\ndata: {\"query\":\"test\"}\nevent: hit\ndata: {\"id\":\"doc1\"}\nevent: done\ndata: {}\n"

	// Test with various chunk sizes to ensure partial line handling works
	for _, chunkSize := range []int{1, 2, 3, 5, 7, 13, 17, 64, len(input)} {
		t.Run(fmt.Sprintf("chunk_%d", chunkSize), func(t *testing.T) {
			reader := &chunkedReader{data: input, chunkSize: chunkSize}
			var events []struct{ event, data string }
			for event, data := range readSSEEvents(reader) {
				events = append(events, struct{ event, data string }{event, data})
			}

			if len(events) != 3 {
				t.Errorf("chunkSize=%d: got %d events, want 3", chunkSize, len(events))
				return
			}
			if events[0].event != "classification" || events[0].data != `{"query":"test"}` {
				t.Errorf("chunkSize=%d: event[0] = %+v", chunkSize, events[0])
			}
			if events[1].event != "hit" || events[1].data != `{"id":"doc1"}` {
				t.Errorf("chunkSize=%d: event[1] = %+v", chunkSize, events[1])
			}
			if events[2].event != "done" || events[2].data != "{}" {
				t.Errorf("chunkSize=%d: event[2] = %+v", chunkSize, events[2])
			}
		})
	}
}

func TestReadSSEEventsEarlyTermination(t *testing.T) {
	input := "event: a\ndata: 1\nevent: b\ndata: 2\nevent: c\ndata: 3\n"

	// Stop after first event
	count := 0
	for range readSSEEvents(strings.NewReader(input)) {
		count++
		if count >= 1 {
			break
		}
	}
	if count != 1 {
		t.Errorf("early termination: got %d events, want 1", count)
	}
}

func TestCreateIndexReturnsNormalizedConfigAndUsesPathIdentity(t *testing.T) {
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll request body: %v", err)
			return
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"thumbnail image","type":"embeddings","dimension":512}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	request, err := NewCreateIndexRequest(EmbeddingsIndexConfig{Dimension: 512})
	if err != nil {
		t.Fatalf("NewCreateIndexRequest: %v", err)
	}
	created, err := client.CreateIndex(context.Background(), "wiki/media", "thumbnail image", *request)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	createdEmbedding, err := created.AsCreatedEmbeddingsIndex()
	if err != nil {
		t.Fatalf("created.AsCreatedEmbeddingsIndex: %v", err)
	}
	if createdEmbedding.Name != "thumbnail image" || createdEmbedding.Type != CreatedEmbeddingsIndexTypeEmbeddings {
		t.Fatalf("created = %#v", createdEmbedding)
	}
	if kind, err := created.Kind(); err != nil || kind != IndexTypeEmbeddings {
		t.Fatalf("created.Kind() = %q, %v", kind, err)
	}
	if _, err := created.AsCreatedGraphIndex(); err == nil {
		t.Fatal("created.AsCreatedGraphIndex unexpectedly accepted embeddings response")
	}
	if gotPath != "/db/v1/tables/wiki%2Fmedia/indexes/thumbnail%20image" {
		t.Fatalf("path = %q", gotPath)
	}
	if strings.Contains(gotBody, `"name"`) {
		t.Fatalf("request duplicated path identity: %s", gotBody)
	}
}

func TestCreateIndexRejectsInvalidDiscriminatedResponse(t *testing.T) {
	for _, body := range []string{
		`{"name":"vectors","type":"future_index"}`,
		`{"name":"vectors"}`,
		`{"type":"embeddings","dimension":512}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()

			client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatalf("NewAntflyClientWithOptions: %v", err)
			}
			request, err := NewCreateIndexRequest(EmbeddingsIndexConfig{Dimension: 512})
			if err != nil {
				t.Fatalf("NewCreateIndexRequest: %v", err)
			}
			if _, err := client.CreateIndex(context.Background(), "docs", "vectors", *request); err == nil {
				t.Fatalf("CreateIndex accepted invalid response %s", body)
			}
		})
	}
}

func TestCreateIndexPreservesStorageAdmissionRetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"storage_resource_exhausted","error":"storage_resource_exhausted","message":"storage capacity is temporarily exhausted","retryable":true,"retry_after_ms":1250}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	request, err := NewCreateIndexRequest(EmbeddingsIndexConfig{Dimension: 512})
	if err != nil {
		t.Fatalf("NewCreateIndexRequest: %v", err)
	}
	_, err = client.CreateIndex(context.Background(), "docs", "vectors", *request)
	var exhausted *StorageResourceExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("CreateIndex error = %T %[1]v, want StorageResourceExhaustedError", err)
	}
	if exhausted.StatusCode != http.StatusTooManyRequests || exhausted.Code != "storage_resource_exhausted" ||
		!exhausted.Retryable || exhausted.RetryAfterMS != 1250 || exhausted.RetryAfterSeconds != 2 {
		t.Fatalf("StorageResourceExhaustedError = %#v", exhausted)
	}
}

func TestBatchSendsContentLengthRequestAndParsesResponse(t *testing.T) {
	var gotPath string
	var gotBody string
	var gotContentLength int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentLength = r.ContentLength
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"inserted":1}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	result, err := client.BatchWithOptions(context.Background(), "files", BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	}, WriteOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatalf("BatchWithOptions: %v", err)
	}
	if result.Inserted != 1 {
		t.Fatalf("Inserted = %d, want 1", result.Inserted)
	}
	if result.Status != "committed" {
		t.Fatalf("Status = %q, want committed", result.Status)
	}
	if gotPath != "/db/v1/tables/files/batch" {
		t.Fatalf("path = %q, want /db/v1/tables/files/batch", gotPath)
	}
	if gotContentLength <= 0 {
		t.Fatalf("ContentLength = %d, want fixed positive content length", gotContentLength)
	}
	if !strings.Contains(gotBody, `"doc-1"`) || !strings.Contains(gotBody, `"title":"hello"`) {
		t.Fatalf("request body = %q, want encoded insert", gotBody)
	}
}

func TestBatchReportsAcceptedPendingStatusForLegacyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"inserted":1}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.Batch(context.Background(), "files", BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if result.Status != "committed_pending" {
		t.Fatalf("Status = %q, want committed_pending", result.Status)
	}
}

func TestBatchSerializesMinTransformFromGeneratedSDKType(t *testing.T) {
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"transformed":1}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.Batch(context.Background(), "scores", BatchRequest{
		Transforms: []Transform{{
			Key: "doc-1",
			Operations: []TransformOp{{
				Op:    TransformOpTypeMin,
				Path:  "score",
				Value: 4,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if result.Transformed != 1 {
		t.Fatalf("Transformed = %d, want 1", result.Transformed)
	}
	if !strings.Contains(gotBody, `"op":"$min"`) || !strings.Contains(gotBody, `"value":4`) {
		t.Fatalf("request body = %q, want generated $min transform", gotBody)
	}
}

func TestMultiBatchPreservesAcceptedRecoveryStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"committed_recovery_pending","tables":{}}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.MultiBatch(context.Background(), MultiBatchRequest{
		Tables: map[string]BatchRequest{"files": {Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}}}},
	})
	if err != nil {
		t.Fatalf("MultiBatch: %v", err)
	}
	if result.Status != "committed_recovery_pending" {
		t.Fatalf("Status = %q, want committed_recovery_pending", result.Status)
	}
}

func TestMultiBatchReturnsStructuredConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"aborted","conflict":{"table":"files","key":"doc-1","message":"participant unavailable","kind":"participant_unavailable","retryable":true,"retry_after_ms":50,"retry_scope":"participant","expected_version":41,"current_version":42,"participant":{"group_id":7,"phase":"prepare"}}}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.MultiBatch(context.Background(), MultiBatchRequest{
		Tables: map[string]BatchRequest{"files": {Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}}}},
	})
	if err != nil {
		t.Fatalf("MultiBatch: %v", err)
	}
	if result.Status != "aborted" {
		t.Fatalf("Status = %q, want aborted", result.Status)
	}
	if result.Conflict == nil {
		t.Fatal("Conflict = nil, want structured conflict")
	}
	assertCompleteTransactionConflict(t, result.Conflict)
}

func assertCompleteTransactionConflict(t *testing.T, conflict *TransactionConflict) {
	t.Helper()
	if conflict.Table != "files" || conflict.Key != "doc-1" || conflict.Message != "participant unavailable" {
		t.Fatalf("Conflict identity = %#v", conflict)
	}
	if conflict.Kind != TransactionConflictParticipantUnavailable || !conflict.Retryable {
		t.Fatalf("Conflict classification = %#v", conflict)
	}
	if conflict.RetryAfterMS == nil || *conflict.RetryAfterMS != 50 || conflict.RetryScope != TransactionConflictRetryScopeParticipant {
		t.Fatalf("Conflict retry metadata = %#v", conflict)
	}
	if conflict.ExpectedVersion == nil || *conflict.ExpectedVersion != 41 || conflict.CurrentVersion == nil || *conflict.CurrentVersion != 42 {
		t.Fatalf("Conflict versions = %#v", conflict)
	}
	if conflict.Participant == nil || conflict.Participant.GroupID == nil || *conflict.Participant.GroupID != 7 || conflict.Participant.Phase != TransactionConflictPhasePrepare {
		t.Fatalf("Conflict participant = %#v", conflict)
	}
}

func TestMultiBatchRejectsMalformedConflictResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"committed"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	_, err = client.MultiBatch(context.Background(), MultiBatchRequest{Tables: map[string]BatchRequest{}})
	if err == nil || !strings.Contains(err.Error(), `unexpected status "committed"`) {
		t.Fatalf("MultiBatch error = %v, want unexpected conflict status", err)
	}
}

func TestMultiBatchRejectsOversizedConflictResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"aborted","conflict":{"message":"response exceeds the configured limit"}}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	_, err = client.MultiBatchWithOptions(context.Background(), MultiBatchRequest{Tables: map[string]BatchRequest{}}, WriteOptions{
		MaxResponseBytes: 16,
	})
	if err == nil || !strings.Contains(err.Error(), "multi-batch response exceeded 16 bytes") {
		t.Fatalf("MultiBatchWithOptions error = %v, want response limit error", err)
	}
}

func TestBatchRejectsOversizedSuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte(strings.Repeat("x", 17)))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.BatchWithOptions(context.Background(), "files", BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	}, WriteOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 16,
	})
	if err == nil || !strings.Contains(err.Error(), "batch response exceeded 16 bytes") {
		t.Fatalf("BatchWithOptions error = %v, want response limit error", err)
	}
}

func TestReadErrorResponseCapsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, strings.Repeat("x", int(maxErrorResponseBytes)+1), http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.BatchWithOptions(context.Background(), "files", BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	}, WriteOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 1024,
	})
	if err == nil {
		t.Fatal("BatchWithOptions error = nil, want API error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %T %[1]v, want APIError", err)
	}
	if !strings.Contains(apiErr.Message, "response body exceeded") {
		t.Fatalf("APIError.Message missing truncation marker: %q", apiErr.Message)
	}
	if len(apiErr.Message) > int(maxErrorResponseBytes)+128 {
		t.Fatalf("APIError.Message length = %d, want capped message", len(apiErr.Message))
	}
}

func TestQueryPreservesHierarchyCursorRestartGuidance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":409,"error":"hierarchy_cursor_stale","message":"the source artifact changed during traversal","action":"restart_hierarchy_traversal","restart_without":"search_after","retryable":false}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.Query(context.Background(), QueryRequest{Table: "files", Limit: 10})
	if err == nil {
		t.Fatal("Query error = nil, want HierarchyCursorStaleError")
	}
	var stale *HierarchyCursorStaleError
	if !errors.As(err, &stale) {
		t.Fatalf("error = %T %[1]v, want HierarchyCursorStaleError", err)
	}
	if stale.StatusCode != http.StatusConflict ||
		stale.Code != "hierarchy_cursor_stale" ||
		stale.Action != "restart_hierarchy_traversal" ||
		stale.RestartWithout != "search_after" ||
		stale.Retryable {
		t.Fatalf("stale cursor error = %#v, want restart-without-search-after guidance", stale)
	}
}

func TestQueryPreservesTopologyRetryGuidance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":409,"error":"topology_changed","message":"the table topology changed while the query was running","action":"retry_query","retryable":true}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.Query(context.Background(), QueryRequest{Table: "files", Limit: 10})
	if err == nil {
		t.Fatal("Query error = nil, want TopologyChangedError")
	}
	var topology *TopologyChangedError
	if !errors.As(err, &topology) {
		t.Fatalf("error = %T %[1]v, want TopologyChangedError", err)
	}
	if topology.StatusCode != http.StatusConflict ||
		topology.Code != "topology_changed" ||
		topology.Action != "retry_query" ||
		!topology.Retryable {
		t.Fatalf("topology error = %#v, want retry-query guidance", topology)
	}
}

func TestQueryPreservesTemporaryAvailabilityRetryGuidance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"storage_read_temporarily_unavailable","message":"storage read temporarily unavailable","retryable":true}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.Query(context.Background(), QueryRequest{Table: "files", Limit: 10})
	if err == nil {
		t.Fatal("Query error = nil, want QueryTemporarilyUnavailableError")
	}
	var unavailable *QueryTemporarilyUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("error = %T %[1]v, want QueryTemporarilyUnavailableError", err)
	}
	if unavailable.StatusCode != http.StatusServiceUnavailable ||
		unavailable.Code != "storage_read_temporarily_unavailable" ||
		!unavailable.Retryable ||
		unavailable.RetryAfterSeconds != 3 {
		t.Fatalf("temporary availability error = %#v, want retryable 3-second guidance", unavailable)
	}
}

func TestBatchRejectsOversizedRequestBeforeSending(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.BatchWithOptions(context.Background(), "files", BatchRequest{
		Inserts: map[string]any{"doc-1": map[string]any{"title": strings.Repeat("x", 128)}},
	}, WriteOptions{
		MaxRequestBytes:  64,
		MaxResponseBytes: 1024,
	})
	if err == nil || !strings.Contains(err.Error(), "encoded request exceeded 64 bytes") {
		t.Fatalf("BatchWithOptions error = %v, want request limit error", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want no request sent after local request limit failure", requests)
	}
}

func TestLinearMergeSendsContentLengthRequestAndParsesResponse(t *testing.T) {
	var gotPath string
	var gotBody string
	var gotContentLength int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentLength = r.ContentLength
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"upserted":1,"status":"success"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	result, err := client.LinearMergeWithOptions(context.Background(), "files", LinearMergeRequest{
		Records: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	}, WriteOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatalf("LinearMergeWithOptions: %v", err)
	}
	if result.Upserted != 1 {
		t.Fatalf("Upserted = %d, want 1", result.Upserted)
	}
	if gotPath != "/db/v1/tables/files/merge" {
		t.Fatalf("path = %q, want /db/v1/tables/files/merge", gotPath)
	}
	if gotContentLength <= 0 {
		t.Fatalf("ContentLength = %d, want fixed positive content length", gotContentLength)
	}
	if !strings.Contains(gotBody, `"doc-1"`) || !strings.Contains(gotBody, `"title":"hello"`) {
		t.Fatalf("request body = %q, want encoded record", gotBody)
	}
}

func TestLinearMergeRejectsOversizedSuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte(strings.Repeat("x", 17)))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.LinearMergeWithOptions(context.Background(), "files", LinearMergeRequest{
		Records: map[string]any{"doc-1": map[string]any{"title": "hello"}},
	}, WriteOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 16,
	})
	if err == nil || !strings.Contains(err.Error(), "linear merge response exceeded 16 bytes") {
		t.Fatalf("LinearMergeWithOptions error = %v, want response limit error", err)
	}
}

func TestExecuteLinearMergeUsesWriteOptions(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	_, err = client.ExecuteLinearMerge(context.Background(), "files", SortedPages(map[string]any{
		"doc-1": map[string]any{"title": strings.Repeat("x", 128)},
	}, 1), ExecuteLinearMergeOptions{
		WriteOptions: WriteOptions{
			MaxRequestBytes:  64,
			MaxResponseBytes: 1024,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "encoded request exceeded 64 bytes") {
		t.Fatalf("ExecuteLinearMerge error = %v, want request limit error", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want no request sent after local request limit failure", requests)
	}
}

func TestSortedLinearMergePagesRespectsByteLimit(t *testing.T) {
	records := map[string]any{
		"a": map[string]any{"text": strings.Repeat("a", 24)},
		"b": map[string]any{"text": strings.Repeat("b", 24)},
		"c": map[string]any{"text": strings.Repeat("c", 24)},
	}
	allPages, err := SortedLinearMergePages(records, LinearMergePageOptions{MaxRecords: 10})
	if err != nil {
		t.Fatalf("SortedLinearMergePages without byte limit: %v", err)
	}
	if len(allPages) != 1 {
		t.Fatalf("pages without byte limit = %d, want 1", len(allPages))
	}

	oneRecordSize, err := linearMergeRequestSize(map[string]any{
		"a": records["a"],
	}, "x", false, "")
	if err != nil {
		t.Fatalf("linearMergeRequestSize one record: %v", err)
	}
	twoRecordSize, err := linearMergeRequestSize(map[string]any{
		"a": records["a"],
		"b": records["b"],
	}, "x", false, "")
	if err != nil {
		t.Fatalf("linearMergeRequestSize two records: %v", err)
	}
	pages, err := SortedLinearMergePages(records, LinearMergePageOptions{
		MaxRecords:      10,
		MaxRequestBytes: oneRecordSize + (twoRecordSize-oneRecordSize)/2,
	})
	if err != nil {
		t.Fatalf("SortedLinearMergePages with byte limit: %v", err)
	}
	if len(pages) != 3 {
		t.Fatalf("pages with byte limit = %d, want 3", len(pages))
	}
	for i, page := range pages {
		if len(page) != 1 {
			t.Fatalf("page %d len = %d, want 1", i, len(page))
		}
	}
}

func TestSortedLinearMergePagesRejectsSingleOversizedRecord(t *testing.T) {
	records := map[string]any{
		"a": map[string]any{"text": strings.Repeat("a", 128)},
	}
	_, err := SortedLinearMergePages(records, LinearMergePageOptions{
		MaxRecords:      10,
		MaxRequestBytes: 64,
	})
	if err == nil || !strings.Contains(err.Error(), `linear merge record "a"`) {
		t.Fatalf("SortedLinearMergePages error = %v, want oversized record error", err)
	}
}

func TestLinearMergeRequestSizerMatchesEncodedSize(t *testing.T) {
	records := map[string]any{
		"a": map[string]any{"text": "alpha", "n": 1},
		"b": map[string]any{"text": "bravo", "n": 2},
	}
	sizer, err := newLinearMergeRequestSizer("cursor", true, SyncLevelFullIndex)
	if err != nil {
		t.Fatalf("newLinearMergeRequestSizer: %v", err)
	}

	total := int64(0)
	count := 0
	for _, id := range []string{"a", "b"} {
		entrySize, err := linearMergeRecordEntrySize(id, records[id])
		if err != nil {
			t.Fatalf("linearMergeRecordEntrySize: %v", err)
		}
		total += entrySize
		count++
	}

	got := sizer.emptyRequestBytes + total + int64(count-1)
	want, err := linearMergeRequestSize(records, "cursor", true, SyncLevelFullIndex)
	if err != nil {
		t.Fatalf("linearMergeRequestSize: %v", err)
	}
	if got != want {
		t.Fatalf("estimated size = %d, want encoded size %d", got, want)
	}
}

func TestTransactionCommitUsesWriteOptions(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"committed"}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}

	tx := client.NewTransaction()
	_, err = tx.CommitWithOptions(context.Background(), map[string]BatchRequest{
		"files": {
			Inserts: map[string]any{"doc-1": map[string]any{"title": strings.Repeat("x", 128)}},
		},
	}, WriteOptions{
		MaxRequestBytes:  64,
		MaxResponseBytes: 1024,
	})
	if err == nil || !strings.Contains(err.Error(), "encoded request exceeded 64 bytes") {
		t.Fatalf("CommitWithOptions error = %v, want request limit error", err)
	}
	if requests != 0 {
		t.Fatalf("requests = %d, want no request sent after local request limit failure", requests)
	}
}

func TestTransactionCommitPreservesAcceptedRecoveryStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"committed_recovery_pending","tables":{}}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.NewTransaction().Commit(context.Background(), map[string]BatchRequest{
		"files": {Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}}},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if result.Status != "committed_recovery_pending" {
		t.Fatalf("Status = %q, want committed_recovery_pending", result.Status)
	}
}

func TestTransactionCommitPreservesStructuredConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"aborted","conflict":{"table":"files","key":"doc-1","message":"participant unavailable","kind":"participant_unavailable","retryable":true,"retry_after_ms":50,"retry_scope":"participant","expected_version":41,"current_version":42,"participant":{"group_id":7,"phase":"prepare"}}}`))
	}))
	defer server.Close()

	client, err := NewAntflyClientWithOptions(server.URL, oapi.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewAntflyClientWithOptions: %v", err)
	}
	result, err := client.NewTransaction().Commit(context.Background(), map[string]BatchRequest{
		"files": {Inserts: map[string]any{"doc-1": map[string]any{"title": "hello"}}},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if result.Status != "aborted" || result.Conflict == nil {
		t.Fatalf("Commit result = %#v, want aborted conflict", result)
	}
	assertCompleteTransactionConflict(t, result.Conflict)
}
