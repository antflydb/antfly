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

func TestBatchStreamsRequestAndParsesResponse(t *testing.T) {
	var gotPath string
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
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
	}, BatchOptions{
		MaxRequestBytes:  1024,
		MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatalf("BatchWithOptions: %v", err)
	}
	if result.Inserted != 1 {
		t.Fatalf("Inserted = %d, want 1", result.Inserted)
	}
	if gotPath != "/db/v1/tables/files/batch" {
		t.Fatalf("path = %q, want /db/v1/tables/files/batch", gotPath)
	}
	if !strings.Contains(gotBody, `"doc-1"`) || !strings.Contains(gotBody, `"title":"hello"`) {
		t.Fatalf("request body = %q, want encoded insert", gotBody)
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
	}, BatchOptions{
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
	}, BatchOptions{
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
