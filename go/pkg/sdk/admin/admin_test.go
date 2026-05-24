package admin

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalClientGetMetadataStatusSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodGet)
		}
		if r.URL.Path != "/_internal/v1/status" {
			t.Fatalf("path = %s, want /_internal/v1/status", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		_, _ = fmt.Fprint(w, `{"raft_status":{"leader_id":1,"voters":{"1":"raft://node-1"}}}`)
	}))
	defer server.Close()

	status, err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").GetMetadataStatus()
	if err != nil {
		t.Fatalf("GetMetadataStatus returned error: %v", err)
	}
	if status.Leader != 1 {
		t.Fatalf("Leader = %d, want 1", status.Leader)
	}
	if got := status.Members[1]; got != "raft://node-1" {
		t.Fatalf("Members[1] = %q, want raft://node-1", got)
	}
}

func TestInternalClientAddMetadataPeerSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if r.URL.Path != "/_internal/v1/peer/2" {
			t.Fatalf("path = %s, want /_internal/v1/peer/2", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/octet-stream" {
			t.Fatalf("Content-Type = %q, want application/octet-stream", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll returned error: %v", err)
		}
		if got := string(body); got != "raft://node-2" {
			t.Fatalf("body = %q, want raft://node-2", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").AddMetadataPeer(2, "raft://node-2"); err != nil {
		t.Fatalf("AddMetadataPeer returned error: %v", err)
	}
}

func TestInternalClientRemoveMetadataPeerSendsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodDelete)
		}
		if r.URL.Path != "/_internal/v1/peer/2" {
			t.Fatalf("path = %s, want /_internal/v1/peer/2", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("Authorization = %q, want Bearer test-token", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := NewInternalClient(server.URL, server.Client()).WithToken("test-token").RemoveMetadataPeer(2); err != nil {
		t.Fatalf("RemoveMetadataPeer returned error: %v", err)
	}
}
