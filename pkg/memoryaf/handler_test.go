package memoryaf

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/antflydb/antfly/pkg/client"
	"go.uber.org/zap"
)

// --- Mock Client ---

type mockClient struct {
	mu       sync.Mutex
	batches  []client.BatchRequest
	tables   map[string]bool
	docs     map[string]any
	queryFn  func(body []byte) ([]byte, error)
	batchFn  func(table string, req client.BatchRequest) (*client.BatchResult, error)
}

func newMockClient() *mockClient {
	return &mockClient{
		tables: make(map[string]bool),
		docs:   make(map[string]any),
	}
}

func (m *mockClient) CreateTable(_ context.Context, name string, _ *client.CreateTableRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tables[name] {
		return fmt.Errorf("table %s already exists", name)
	}
	m.tables[name] = true
	return nil
}

func (m *mockClient) Batch(_ context.Context, tableID string, req client.BatchRequest) (*client.BatchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, req)
	if m.batchFn != nil {
		return m.batchFn(tableID, req)
	}
	return &client.BatchResult{}, nil
}

func (m *mockClient) QueryWithBody(_ context.Context, body []byte) ([]byte, error) {
	if m.queryFn != nil {
		return m.queryFn(body)
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
			{Hits: struct {
				Hits  []rawHit `json:"hits"`
				Total uint64   `json:"total"`
			}{}},
		},
	})
}

// --- Mock Extractor ---

type mockExtractor struct {
	extractFn func(ctx context.Context, texts []string, opts ExtractOptions) ([]Extraction, error)
}

func (m *mockExtractor) Extract(ctx context.Context, texts []string, opts ExtractOptions) ([]Extraction, error) {
	if m.extractFn != nil {
		return m.extractFn(ctx, texts, opts)
	}
	out := make([]Extraction, len(texts))
	return out, nil
}

// --- Helpers ---

func newTestHandler(c *mockClient, ext Extractor) *Handler {
	return NewHandler(c, ext, zapNop())
}

func zapNop() *zap.Logger {
	return zap.NewNop()
}

func defaultUctx() UserContext {
	return UserContext{UserID: "user1", Namespace: "default", Role: "member"}
}

// mockQueryHit builds a query response with a single hit.
func mockQueryHit(id string, source map[string]any) []byte {
	resp := queryResponse{
		Responses: []struct {
			Hits struct {
				Hits  []rawHit `json:"hits"`
				Total uint64   `json:"total"`
			} `json:"hits"`
			Aggregations map[string]aggregationResult `json:"aggregations"`
			Error        string                       `json:"error"`
		}{
			{Hits: struct {
				Hits  []rawHit `json:"hits"`
				Total uint64   `json:"total"`
			}{
				Hits:  []rawHit{{ID: id, Score: 1.0, Source: source}},
				Total: 1,
			}},
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

// --- Tests ---

func TestStoreMemory(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	mem, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content:    "Go uses goroutines for concurrency",
		MemoryType: MemoryTypeSemantic,
		Tags:       []string{"go", "concurrency"},
		Project:    "myproject",
	}, defaultUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}

	if mem.Content != "Go uses goroutines for concurrency" {
		t.Errorf("got content %q", mem.Content)
	}
	if mem.MemoryType != MemoryTypeSemantic {
		t.Errorf("got type %q, want %q", mem.MemoryType, MemoryTypeSemantic)
	}
	if mem.ID == "" {
		t.Error("expected non-empty ID")
	}
	if mem.CreatedBy != "user1" {
		t.Errorf("got created_by %q, want %q", mem.CreatedBy, "user1")
	}

	mc.mu.Lock()
	if len(mc.batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(mc.batches))
	}
	mc.mu.Unlock()
}

func TestStoreMemory_EmptyContent(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	_, err := h.StoreMemory(context.Background(), StoreMemoryArgs{}, defaultUctx())
	if err == nil || !strings.Contains(err.Error(), "content is required") {
		t.Fatalf("expected content required error, got: %v", err)
	}
}

func TestStoreMemory_WithExtractor(t *testing.T) {
	mc := newMockClient()
	ext := &mockExtractor{
		extractFn: func(_ context.Context, texts []string, opts ExtractOptions) ([]Extraction, error) {
			return []Extraction{{
				Entities: []ExtractedEntity{
					{Text: "Go", Label: "technology", Score: 0.95},
					{Text: "goroutines", Label: "technology", Score: 0.8},
				},
			}}, nil
		},
	}
	h := newTestHandler(mc, ext)

	mem, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content: "Go uses goroutines for concurrency",
	}, defaultUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}
	if mem.ID == "" {
		t.Error("expected non-empty ID")
	}

	// Entity extraction runs async, so we can't deterministically check
	// the batch count here. Just verify the store itself succeeded.
}

func TestGetMemory(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		return mockQueryHit("mem:abc123", map[string]any{
			"content":     "test memory",
			"memory_type": MemoryTypeSemantic,
			"created_by":  "user1",
			"visibility":  VisibilityTeam,
			"tags":        []any{"tag1"},
		}), nil
	}
	h := newTestHandler(mc, nil)

	mem, err := h.GetMemory(context.Background(), "abc123", defaultUctx())
	if err != nil {
		t.Fatalf("GetMemory: %v", err)
	}
	if mem.ID != "abc123" {
		t.Errorf("got ID %q, want %q", mem.ID, "abc123")
	}
	if mem.Content != "test memory" {
		t.Errorf("got content %q", mem.Content)
	}
}

func TestGetMemory_NotFound(t *testing.T) {
	mc := newMockClient()
	// Return empty results.
	h := newTestHandler(mc, nil)

	_, err := h.GetMemory(context.Background(), "nonexistent", defaultUctx())
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not found error, got: %v", err)
	}
}

func TestDeleteMemory_Forbidden(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		return mockQueryHit("mem:abc123", map[string]any{
			"content":     "someone else's memory",
			"memory_type": MemoryTypeSemantic,
			"created_by":  "other_user",
			"visibility":  VisibilityTeam,
		}), nil
	}
	h := newTestHandler(mc, nil)

	err := h.DeleteMemory(context.Background(), "abc123", defaultUctx())
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("expected forbidden error, got: %v", err)
	}
}

func TestDeleteMemory_AdminOverride(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		return mockQueryHit("mem:abc123", map[string]any{
			"content":     "someone else's memory",
			"memory_type": MemoryTypeSemantic,
			"created_by":  "other_user",
			"visibility":  VisibilityTeam,
		}), nil
	}
	h := newTestHandler(mc, nil)

	uctx := UserContext{UserID: "admin1", Namespace: "default", Role: "admin"}
	err := h.DeleteMemory(context.Background(), "abc123", uctx)
	if err != nil {
		t.Fatalf("admin should be able to delete: %v", err)
	}
}

func TestSearchMemories(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		resp := queryResponse{
			Responses: []struct {
				Hits struct {
					Hits  []rawHit `json:"hits"`
					Total uint64   `json:"total"`
				} `json:"hits"`
				Aggregations map[string]aggregationResult `json:"aggregations"`
				Error        string                       `json:"error"`
			}{
				{Hits: struct {
					Hits  []rawHit `json:"hits"`
					Total uint64   `json:"total"`
				}{
					Hits: []rawHit{
						{ID: "mem:a", Score: 0.9, Source: map[string]any{"content": "first", "memory_type": "semantic", "visibility": "team"}},
						{ID: "mem:b", Score: 0.7, Source: map[string]any{"content": "second", "memory_type": "semantic", "visibility": "team"}},
						{ID: "ent:technology:go", Score: 0.5, Source: map[string]any{"entity_type": "entity"}},
					},
					Total: 3,
				}},
			},
		}
		b, _ := json.Marshal(resp)
		return b, nil
	}
	h := newTestHandler(mc, nil)

	results, err := h.SearchMemories(context.Background(), SearchMemoriesArgs{
		Query: "concurrency",
	}, defaultUctx())
	if err != nil {
		t.Fatalf("SearchMemories: %v", err)
	}

	// Should filter out entity hits.
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Memory.Content != "first" {
		t.Errorf("got content %q, want %q", results[0].Memory.Content, "first")
	}
	if results[0].Score != 0.9 {
		t.Errorf("got score %f, want 0.9", results[0].Score)
	}
}

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		ns      string
		wantErr bool
	}{
		{"", false},
		{"default", false},
		{"my-team", false},
		{"team_123", false},
		{"invalid namespace!", true},
		{"../escape", true},
		{"has spaces", true},
	}
	for _, tt := range tests {
		err := ValidateNamespace(tt.ns)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateNamespace(%q) = %v, wantErr %v", tt.ns, err, tt.wantErr)
		}
	}
}

func TestTableName(t *testing.T) {
	if got := tableName(""); got != "memories" {
		t.Errorf("tableName(\"\") = %q", got)
	}
	if got := tableName("default"); got != "memories" {
		t.Errorf("tableName(\"default\") = %q", got)
	}
	if got := tableName("team1"); got != "team1_memories" {
		t.Errorf("tableName(\"team1\") = %q", got)
	}
}

func TestEntityKey(t *testing.T) {
	key := entityKey("technology", "Go")
	if !strings.HasPrefix(key, "ent:technology:") {
		t.Errorf("entityKey = %q, want prefix ent:technology:", key)
	}
	// Same entity should produce same key.
	if entityKey("technology", "Go") != entityKey("technology", "  Go  ") {
		t.Error("entityKey should normalize whitespace")
	}
}

func TestIsMemoryHit(t *testing.T) {
	if !isMemoryHit("mem:abc") {
		t.Error("mem:abc should be a memory hit")
	}
	if isMemoryHit("ent:technology:go") {
		t.Error("ent:technology:go should not be a memory hit")
	}
}

func TestBuildFilterQuery_VisibilityDefault(t *testing.T) {
	uctx := &UserContext{UserID: "user1"}
	q := buildFilterQuery(filterOpts{}, uctx)
	if q == nil {
		t.Fatal("expected visibility filter when no explicit visibility set")
	}
	data, _ := json.Marshal(q)
	s := string(data)
	if !strings.Contains(s, VisibilityTeam) {
		t.Errorf("expected team visibility in filter, got: %s", s)
	}
}

func TestBuildFilterQuery_ExplicitVisibility(t *testing.T) {
	uctx := &UserContext{UserID: "user1"}
	q := buildFilterQuery(filterOpts{Visibility: VisibilityPrivate}, uctx)
	if q == nil {
		t.Fatal("expected filter")
	}
	data, _ := json.Marshal(q)
	s := string(data)
	if !strings.Contains(s, VisibilityPrivate) {
		t.Errorf("expected private visibility in filter, got: %s", s)
	}
	// Should NOT contain the team disjunction.
	if strings.Contains(s, "disjuncts") {
		t.Errorf("explicit visibility should not produce disjunction: %s", s)
	}
}

func TestExtractEntities_NilExtractor(t *testing.T) {
	h := newTestHandler(newMockClient(), nil)
	entities := h.extractEntities(context.Background(), "some text")
	if entities != nil {
		t.Errorf("expected nil with no extractor, got %v", entities)
	}
}

func TestExtractEntities_ThresholdFiltering(t *testing.T) {
	ext := &mockExtractor{
		extractFn: func(_ context.Context, texts []string, _ ExtractOptions) ([]Extraction, error) {
			return []Extraction{{
				Entities: []ExtractedEntity{
					{Text: "Go", Label: "technology", Score: 0.9},
					{Text: "thing", Label: "unknown", Score: 0.2},
				},
			}}, nil
		},
	}
	h := newTestHandler(newMockClient(), ext)

	entities := h.extractEntities(context.Background(), "Go thing")
	if len(entities) != 1 {
		t.Fatalf("expected 1 entity after threshold, got %d", len(entities))
	}
	if entities[0].Text != "Go" {
		t.Errorf("got entity %q, want %q", entities[0].Text, "Go")
	}
}

func TestEnsureNamespace_Idempotent(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	if err := h.ensureNamespace(context.Background(), "test"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// Second call should not hit CreateTable again (table already marked initialized).
	if err := h.ensureNamespace(context.Background(), "test"); err != nil {
		t.Fatalf("second call: %v", err)
	}

	mc.mu.Lock()
	if !mc.tables["test_memories"] {
		t.Error("expected table to be created")
	}
	mc.mu.Unlock()
}

func TestHitToMemory(t *testing.T) {
	conf := 0.85
	source := map[string]any{
		"content":     "test",
		"memory_type": MemoryTypeEpisodic,
		"tags":        []any{"a", "b"},
		"project":     "proj",
		"created_by":  "user1",
		"visibility":  VisibilityPrivate,
		"created_at":  "2025-01-01T00:00:00Z",
		"updated_at":  "2025-01-02T00:00:00Z",
		"event_time":  "2025-01-01T12:00:00Z",
		"confidence":  conf,
		"trigger":     "deploy",
		"steps":       []any{"step1", "step2"},
		"outcome":     "success",
	}

	m := hitToMemory("mem:xyz", source)
	if m.ID != "xyz" {
		t.Errorf("ID = %q, want xyz", m.ID)
	}
	if m.MemoryType != MemoryTypeEpisodic {
		t.Errorf("MemoryType = %q", m.MemoryType)
	}
	if len(m.Tags) != 2 {
		t.Errorf("Tags = %v", m.Tags)
	}
	if m.Visibility != VisibilityPrivate {
		t.Errorf("Visibility = %q", m.Visibility)
	}
	if m.Confidence == nil || *m.Confidence != conf {
		t.Errorf("Confidence = %v", m.Confidence)
	}
	if m.Trigger != "deploy" {
		t.Errorf("Trigger = %q", m.Trigger)
	}
	if len(m.Steps) != 2 {
		t.Errorf("Steps = %v", m.Steps)
	}
}
