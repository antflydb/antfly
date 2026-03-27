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
	mu          sync.Mutex
	batches     []client.BatchRequest
	batchTables []string
	queryBodies [][]byte
	tables      map[string]bool
	docs        map[string]any
	queryFn     func(body []byte) ([]byte, error)
	batchFn     func(table string, req client.BatchRequest) (*client.BatchResult, error)
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
	m.batchTables = append(m.batchTables, tableID)
	if m.batchFn != nil {
		return m.batchFn(tableID, req)
	}
	return &client.BatchResult{}, nil
}

func (m *mockClient) QueryWithBody(_ context.Context, body []byte) ([]byte, error) {
	m.mu.Lock()
	m.queryBodies = append(m.queryBodies, append([]byte(nil), body...))
	m.mu.Unlock()
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

func agentUctx() UserContext {
	return UserContext{UserID: "user1", Namespace: "default", Role: "member", AgentID: "claude-code", DeviceID: "laptop-1"}
}

func sessionUctx() UserContext {
	return UserContext{UserID: "user1", Namespace: "default", Role: "member", SessionID: "sess-ctx", AgentID: "claude-code", DeviceID: "laptop-1"}
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

	// Wait for async entity extraction to complete.
	h.Close()

	mc.mu.Lock()
	// 1 batch for the memory insert + 2 for entity extraction (nodes + edges).
	if len(mc.batches) != 3 {
		t.Errorf("expected 3 batches (insert + entity nodes + edges), got %d", len(mc.batches))
	}
	mc.mu.Unlock()
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

func TestGetMemory_FallsBackToEphemeral(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal query: %v", err)
		}
		switch req["table"] {
		case "memories":
			return json.Marshal(queryResponse{})
		case "ephemeral_memories":
			return mockQueryHit("mem:abc123", map[string]any{
				"content":     "ephemeral memory",
				"memory_type": MemoryTypeSemantic,
				"created_by":  "user1",
				"visibility":  VisibilityTeam,
			}), nil
		default:
			return json.Marshal(queryResponse{})
		}
	}
	h := newTestHandler(mc, nil)

	mem, err := h.GetMemory(context.Background(), "abc123", defaultUctx())
	if err != nil {
		t.Fatalf("GetMemory: %v", err)
	}
	if !mem.Ephemeral {
		t.Fatal("expected memory to be marked ephemeral")
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

func TestDeleteMemory_EphemeralUsesEphemeralTable(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal query: %v", err)
		}
		switch req["table"] {
		case "memories":
			return json.Marshal(queryResponse{})
		case "ephemeral_memories":
			return mockQueryHit("mem:abc123", map[string]any{
				"content":     "ephemeral memory",
				"memory_type": MemoryTypeSemantic,
				"created_by":  "user1",
				"visibility":  VisibilityTeam,
				"ephemeral":   true,
			}), nil
		default:
			return json.Marshal(queryResponse{})
		}
	}
	h := newTestHandler(mc, nil)

	if err := h.DeleteMemory(context.Background(), "abc123", defaultUctx()); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.batchTables) == 0 || mc.batchTables[len(mc.batchTables)-1] != "ephemeral_memories" {
		t.Fatalf("expected delete against ephemeral_memories, got %v", mc.batchTables)
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
	if entityKey("technology", "Go") != entityKey("technology", "\"Go.\"") {
		t.Error("entityKey should normalize quotes and trailing punctuation")
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

func TestNormalizeEntityText(t *testing.T) {
	if got := normalizeEntityText(`  "Go."  `); got != "Go" {
		t.Fatalf("normalizeEntityText = %q, want %q", got, "Go")
	}
	if got := normalizeEntityText(" Apache   Kafka! "); got != "Apache Kafka" {
		t.Fatalf("normalizeEntityText = %q, want %q", got, "Apache Kafka")
	}
}

func TestExtractEntities_NormalizesAndDedupes(t *testing.T) {
	ext := &mockExtractor{
		extractFn: func(_ context.Context, texts []string, _ ExtractOptions) ([]Extraction, error) {
			return []Extraction{{
				Entities: []ExtractedEntity{
					{Text: "Go", Label: "technology", Score: 0.80},
					{Text: ` "Go." `, Label: "project", Score: 0.92},
					{Text: "Apache Kafka!", Label: "technology", Score: 0.89},
				},
			}}, nil
		},
	}
	h := newTestHandler(newMockClient(), ext)

	entities := h.extractEntities(context.Background(), "Go and Apache Kafka")
	if len(entities) != 2 {
		t.Fatalf("expected 2 deduped entities, got %d", len(entities))
	}
	if entities[0].Text != "Go" {
		t.Fatalf("first entity text = %q, want Go", entities[0].Text)
	}
	if entities[0].Label != "project" {
		t.Fatalf("expected highest-score label to win, got %q", entities[0].Label)
	}
	if entities[1].Text != "Apache Kafka" {
		t.Fatalf("second entity text = %q, want Apache Kafka", entities[1].Text)
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
		"content":        "test",
		"memory_type":    MemoryTypeEpisodic,
		"tags":           []any{"a", "b"},
		"project":        "proj",
		"created_by":     "user1",
		"visibility":     VisibilityPrivate,
		"created_at":     "2025-01-01T00:00:00Z",
		"updated_at":     "2025-01-02T00:00:00Z",
		"event_time":     "2025-01-01T12:00:00Z",
		"confidence":     conf,
		"trigger":        "deploy",
		"steps":          []any{"step1", "step2"},
		"outcome":        "success",
		"session_id":     "sess-1",
		"agent_id":       "claude-code",
		"device_id":      "laptop-1",
		"ephemeral":      true,
		"source_backend": "google_drive",
		"source_id":      "drive-file-123",
		"source_path":    "team/runbooks/incidents.md",
		"source_url":     "https://drive.google.com/file/d/drive-file-123/view",
		"source_version": "2026-03-26T12:00:00Z",
		"section_path":   []any{"Incidents", "Rollback"},
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
	if m.SessionID != "sess-1" {
		t.Errorf("SessionID = %q, want sess-1", m.SessionID)
	}
	if m.AgentID != "claude-code" {
		t.Errorf("AgentID = %q, want claude-code", m.AgentID)
	}
	if m.DeviceID != "laptop-1" {
		t.Errorf("DeviceID = %q, want laptop-1", m.DeviceID)
	}
	if !m.Ephemeral {
		t.Error("Ephemeral should be true")
	}
	if m.SourceBackend != "google_drive" {
		t.Errorf("SourceBackend = %q, want google_drive", m.SourceBackend)
	}
	if m.SourceID != "drive-file-123" {
		t.Errorf("SourceID = %q, want drive-file-123", m.SourceID)
	}
	if m.SourcePath != "team/runbooks/incidents.md" {
		t.Errorf("SourcePath = %q, want team/runbooks/incidents.md", m.SourcePath)
	}
	if m.SourceURL != "https://drive.google.com/file/d/drive-file-123/view" {
		t.Errorf("SourceURL = %q", m.SourceURL)
	}
	if m.SourceVersion != "2026-03-26T12:00:00Z" {
		t.Errorf("SourceVersion = %q", m.SourceVersion)
	}
	if len(m.SectionPath) != 2 || m.SectionPath[0] != "Incidents" || m.SectionPath[1] != "Rollback" {
		t.Errorf("SectionPath = %v", m.SectionPath)
	}
}

func TestStoreMemory_AutoFillScoping(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	mem, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content:   "test scoping",
		SessionID: "sess-1",
	}, agentUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}

	// Should auto-fill agent_id and device_id from UserContext.
	if mem.AgentID != "claude-code" {
		t.Errorf("AgentID = %q, want claude-code", mem.AgentID)
	}
	if mem.DeviceID != "laptop-1" {
		t.Errorf("DeviceID = %q, want laptop-1", mem.DeviceID)
	}
	if mem.SessionID != "sess-1" {
		t.Errorf("SessionID = %q, want sess-1", mem.SessionID)
	}
}

func TestStoreMemory_AutoFillSessionIDFromContext(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	mem, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content:   "session scoped memory",
		Ephemeral: true,
	}, sessionUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}

	if mem.SessionID != "sess-ctx" {
		t.Errorf("SessionID = %q, want sess-ctx", mem.SessionID)
	}
}

func TestStoreMemory_EphemeralRouting(t *testing.T) {
	mc := newMockClient()
	var batchTable string
	mc.batchFn = func(table string, req client.BatchRequest) (*client.BatchResult, error) {
		batchTable = table
		return &client.BatchResult{}, nil
	}
	h := newTestHandler(mc, nil)

	_, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content:   "ephemeral memory",
		Ephemeral: true,
		SessionID: "sess-1",
	}, defaultUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}

	if batchTable != "ephemeral_memories" {
		t.Errorf("expected ephemeral_memories table, got %q", batchTable)
	}
}

func TestStoreMemory_EphemeralExtractorUsesEphemeralTable(t *testing.T) {
	mc := newMockClient()
	ext := &mockExtractor{
		extractFn: func(_ context.Context, texts []string, _ ExtractOptions) ([]Extraction, error) {
			return []Extraction{{
				Entities: []ExtractedEntity{
					{Text: "Antfly", Label: "project", Score: 0.9},
				},
			}}, nil
		},
	}
	h := newTestHandler(mc, ext)

	_, err := h.StoreMemory(context.Background(), StoreMemoryArgs{
		Content:       "Antfly rollback playbook",
		Ephemeral:     true,
		SourceBackend: "git",
		SourceID:      "github.com/antflydb/colony@main:docs/runbooks.md",
		SourcePath:    "docs/runbooks.md",
		SectionPath:   []string{"Incidents", "Rollback"},
	}, sessionUctx())
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}

	h.Close()

	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, table := range mc.batchTables {
		if table != "ephemeral_memories" {
			t.Fatalf("expected all batch writes to use ephemeral_memories, got %v", mc.batchTables)
		}
	}
}

func TestBuildFilterQuery_ScopeFields(t *testing.T) {
	q := buildFilterQuery(filterOpts{
		SessionID: "sess-1",
		AgentID:   "claude-code",
		DeviceID:  "laptop-1",
	}, nil)
	if q == nil {
		t.Fatal("expected filter")
	}
	data, _ := json.Marshal(q)
	s := string(data)
	if !strings.Contains(s, "sess-1") {
		t.Errorf("expected session_id in filter: %s", s)
	}
	if !strings.Contains(s, "claude-code") {
		t.Errorf("expected agent_id in filter: %s", s)
	}
	if !strings.Contains(s, "laptop-1") {
		t.Errorf("expected device_id in filter: %s", s)
	}
}

func TestBuildFilterQuery_SourceFields(t *testing.T) {
	q := buildFilterQuery(filterOpts{
		SourceBackend: "git",
		SourceID:      "github.com/antflydb/colony@main:docs/runbooks.md",
	}, nil)
	if q == nil {
		t.Fatal("expected filter")
	}
	data, _ := json.Marshal(q)
	s := string(data)
	if !strings.Contains(s, "source_backend") || !strings.Contains(s, "git") {
		t.Fatalf("expected source_backend filter, got %s", s)
	}
	if !strings.Contains(s, "source_id") || !strings.Contains(s, "runbooks.md") {
		t.Fatalf("expected source_id filter, got %s", s)
	}
}

func TestResolveScope_Agent(t *testing.T) {
	args := &SearchMemoriesArgs{Query: "test", Scope: ScopeAgent}
	uctx := &UserContext{AgentID: "claude-code"}
	if err := resolveScope(args, uctx); err != nil {
		t.Fatalf("resolveScope: %v", err)
	}

	if args.AgentID != "claude-code" {
		t.Errorf("AgentID = %q, want claude-code", args.AgentID)
	}
}

func TestResolveScope_Device(t *testing.T) {
	args := &SearchMemoriesArgs{Query: "test", Scope: ScopeDevice}
	uctx := &UserContext{DeviceID: "laptop-1"}
	if err := resolveScope(args, uctx); err != nil {
		t.Fatalf("resolveScope: %v", err)
	}

	if args.DeviceID != "laptop-1" {
		t.Errorf("DeviceID = %q, want laptop-1", args.DeviceID)
	}
}

func TestResolveScope_Session(t *testing.T) {
	args := &SearchMemoriesArgs{Query: "test", Scope: ScopeSession}
	uctx := &UserContext{SessionID: "sess-ctx"}
	if err := resolveScope(args, uctx); err != nil {
		t.Fatalf("resolveScope: %v", err)
	}

	if !args.Ephemeral {
		t.Error("session scope should set Ephemeral=true")
	}
	if args.SessionID != "sess-ctx" {
		t.Errorf("SessionID = %q, want sess-ctx", args.SessionID)
	}
}

func TestResolveScope_MissingSessionID(t *testing.T) {
	args := &SearchMemoriesArgs{Query: "test", Scope: ScopeSession}
	err := resolveScope(args, &UserContext{})
	if err == nil || !strings.Contains(err.Error(), "session_id") {
		t.Fatalf("expected session_id error, got %v", err)
	}
}

func TestEphemeralTableName(t *testing.T) {
	if got := ephemeralTableName(""); got != "ephemeral_memories" {
		t.Errorf("ephemeralTableName(\"\") = %q", got)
	}
	if got := ephemeralTableName("default"); got != "ephemeral_memories" {
		t.Errorf("ephemeralTableName(\"default\") = %q", got)
	}
	if got := ephemeralTableName("team1"); got != "team1_ephemeral_memories" {
		t.Errorf("ephemeralTableName(\"team1\") = %q", got)
	}
}

func TestEnsureNamespace_CreatesEphemeralTable(t *testing.T) {
	mc := newMockClient()
	h := newTestHandler(mc, nil)

	if err := h.ensureNamespace(context.Background(), "test"); err != nil {
		t.Fatalf("ensureNamespace: %v", err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !mc.tables["test_memories"] {
		t.Error("expected main table to be created")
	}
	if !mc.tables["test_ephemeral_memories"] {
		t.Error("expected ephemeral table to be created")
	}
}

func TestEndSession_DeletesOwnMemoriesOnly(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal query: %v", err)
		}
		filterJSON, _ := json.Marshal(req["filter_query"])
		filter := string(filterJSON)
		if !strings.Contains(filter, "sess-1") {
			t.Fatalf("expected session filter, got %s", filter)
		}
		if !strings.Contains(filter, "user1") {
			t.Fatalf("expected created_by filter for non-admin, got %s", filter)
		}
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
						{ID: "mem:a", Score: 1.0, Source: map[string]any{"session_id": "sess-1", "created_by": "user1"}},
						{ID: "mem:b", Score: 1.0, Source: map[string]any{"session_id": "sess-1", "created_by": "user1"}},
					},
					Total: 2,
				}},
			},
		}
		if req["offset"].(float64) > 0 {
			return json.Marshal(queryResponse{})
		}
		b, _ := json.Marshal(resp)
		return b, nil
	}
	h := newTestHandler(mc, nil)

	err := h.EndSession(context.Background(), EndSessionArgs{SessionID: "sess-1"}, defaultUctx())
	if err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	found := false
	for i, b := range mc.batches {
		if mc.batchTables[i] == "ephemeral_memories" && len(b.Deletes) == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a delete batch against ephemeral_memories")
	}
}

func TestEndSession_AdminDeletesAcrossPages(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal query: %v", err)
		}
		filterJSON, _ := json.Marshal(req["filter_query"])
		filter := string(filterJSON)
		if strings.Contains(filter, "admin1") {
			t.Fatalf("admin session cleanup should not be restricted to created_by: %s", filter)
		}
		offset := int(req["offset"].(float64))
		var hits []rawHit
		switch offset {
		case 0:
			for i := 0; i < 500; i++ {
				hits = append(hits, rawHit{ID: fmt.Sprintf("mem:%03d", i), Score: 1.0, Source: map[string]any{"session_id": "sess-1"}})
			}
		case 500:
			hits = append(hits,
				rawHit{ID: "mem:500", Score: 1.0, Source: map[string]any{"session_id": "sess-1"}},
				rawHit{ID: "mem:501", Score: 1.0, Source: map[string]any{"session_id": "sess-1"}},
			)
		default:
			hits = nil
		}
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
					Hits:  hits,
					Total: uint64(len(hits)),
				}},
			},
		}
		return json.Marshal(resp)
	}
	h := newTestHandler(mc, nil)

	admin := UserContext{UserID: "admin1", Namespace: "default", Role: "admin"}
	if err := h.EndSession(context.Background(), EndSessionArgs{SessionID: "sess-1"}, admin); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	totalDeleted := 0
	for _, b := range mc.batches {
		totalDeleted += len(b.Deletes)
	}
	if totalDeleted != 502 {
		t.Fatalf("expected 502 deletes, got %d", totalDeleted)
	}
}

func TestEndSession_EmptySessionID(t *testing.T) {
	h := newTestHandler(newMockClient(), nil)
	err := h.EndSession(context.Background(), EndSessionArgs{}, defaultUctx())
	if err == nil || !strings.Contains(err.Error(), "session_id is required") {
		t.Fatalf("expected session_id required error, got: %v", err)
	}
}

func TestListSessions(t *testing.T) {
	mc := newMockClient()
	mc.queryFn = func(body []byte) ([]byte, error) {
		var req map[string]any
		json.Unmarshal(body, &req)
		if req["table"] == "ephemeral_memories" && req["aggregations"] != nil {
			resp := queryResponse{
				Responses: []struct {
					Hits struct {
						Hits  []rawHit `json:"hits"`
						Total uint64   `json:"total"`
					} `json:"hits"`
					Aggregations map[string]aggregationResult `json:"aggregations"`
					Error        string                       `json:"error"`
				}{
					{
						Hits: struct {
							Hits  []rawHit `json:"hits"`
							Total uint64   `json:"total"`
						}{Total: 5},
						Aggregations: map[string]aggregationResult{
							"by_session": {Buckets: []aggregationBucket{
								{Key: "sess-1", DocCount: 3},
								{Key: "sess-2", DocCount: 2},
							}},
						},
					},
				},
			}
			b, _ := json.Marshal(resp)
			return b, nil
		}
		return json.Marshal(queryResponse{})
	}
	h := newTestHandler(mc, nil)

	sessions, err := h.ListSessions(context.Background(), ListSessionsArgs{}, defaultUctx())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}

	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
	if sessions[0].SessionID != "sess-1" || sessions[0].MemoryCount != 3 {
		t.Errorf("sessions[0] = %+v", sessions[0])
	}
	if sessions[1].SessionID != "sess-2" || sessions[1].MemoryCount != 2 {
		t.Errorf("sessions[1] = %+v", sessions[1])
	}
}
