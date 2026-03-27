package memoryaf

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPHandler_Info(t *testing.T) {
	h := NewHTTPHandler(newTestHandler(newMockClient(), nil), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var info ServerInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode info: %v", err)
	}
	if info.Name != serverName {
		t.Fatalf("name = %q, want %q", info.Name, serverName)
	}
}

func TestHTTPHandler_CreateMemory(t *testing.T) {
	handler := NewHTTPHandler(newTestHandler(newMockClient(), nil), nil)
	body := bytes.NewBufferString(`{"content":"docsaf sync memory","memory_type":"semantic","source_backend":"git","source_id":"repo@main:docs/a.md","section_path":["A","B"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/memories", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user42")
	req.Header.Set("X-Namespace", "docs")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var memory Memory
	if err := json.Unmarshal(rec.Body.Bytes(), &memory); err != nil {
		t.Fatalf("decode memory: %v", err)
	}
	if memory.CreatedBy != "user42" {
		t.Fatalf("created_by = %q, want user42", memory.CreatedBy)
	}
	if memory.SourceBackend != "git" {
		t.Fatalf("source_backend = %q, want git", memory.SourceBackend)
	}
	if len(memory.SectionPath) != 2 {
		t.Fatalf("section_path = %v, want 2 entries", memory.SectionPath)
	}
}

func TestHTTPHandler_InitNamespaceRequiresAdmin(t *testing.T) {
	handler := NewHTTPHandler(newTestHandler(newMockClient(), nil), nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/team-alpha/init", nil)
	req = req.WithContext(context.Background())
	req.Header.Set("X-Role", "member")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestHTTPHandler_ServesDashboard(t *testing.T) {
	handler := NewHTTPHandler(newTestHandler(newMockClient(), nil), nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "<title>memoryaf</title>") {
		t.Fatalf("expected dashboard HTML, got %q", rec.Body.String())
	}
}

func TestHTTPHandler_ListSessions(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		if req["table"] != "ephemeral_memories" {
			return json.Marshal(queryResponse{})
		}
		return json.Marshal(queryResponse{
			Responses: []struct {
				Hits struct {
					Hits  []rawHit `json:"hits"`
					Total uint64   `json:"total"`
				} `json:"hits"`
				Aggregations map[string]aggregationResult `json:"aggregations"`
				Error        string                       `json:"error"`
			}{
				{
					Aggregations: map[string]aggregationResult{
						"by_session": {Buckets: []aggregationBucket{{Key: "sess-1", DocCount: 3}}},
					},
				},
			},
		})
	}
	handler := NewHTTPHandler(newTestHandler(mc, nil), nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "sess-1") {
		t.Fatalf("expected session payload, got %s", rec.Body.String())
	}
}
