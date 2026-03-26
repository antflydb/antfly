package memoryaf

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/query"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	validNamespaceRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	whitespaceRe     = regexp.MustCompile(`\s+`)
)

const (
	embeddingIndex    = "memory_embeddings"
	graphIndex        = "memory_graph"
	embedderDimension = 384
	embedderProvider  = "antfly"
)

// Handler implements all memoryaf operations against an Antfly instance.
type Handler struct {
	client    Client
	extractor Extractor
	logger    *zap.Logger

	entityLabels    []string
	entityThreshold float32

	mu                    sync.RWMutex
	initializedNamespaces map[string]bool
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithEntityLabels sets the NER labels used for entity extraction.
func WithEntityLabels(labels []string) HandlerOption {
	return func(h *Handler) { h.entityLabels = append([]string(nil), labels...) }
}

// WithEntityThreshold sets the minimum entity score to keep (default 0.5).
func WithEntityThreshold(threshold float32) HandlerOption {
	return func(h *Handler) { h.entityThreshold = threshold }
}

// NewHandler creates a new memory handler.
// The extractor can be nil to disable entity extraction.
func NewHandler(
	client Client,
	extractor Extractor,
	logger *zap.Logger,
	opts ...HandlerOption,
) *Handler {
	h := &Handler{
		client:                client,
		extractor:             extractor,
		logger:                logger,
		entityLabels:          defaultNERLabels,
		entityThreshold:       0.5,
		initializedNamespaces: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// ValidateNamespace checks that a namespace is safe for use in table names.
func ValidateNamespace(namespace string) error {
	if namespace == "" || namespace == "default" {
		return nil
	}
	if !validNamespaceRe.MatchString(namespace) {
		return fmt.Errorf("invalid namespace %q: must match [a-zA-Z0-9_-]+", namespace)
	}
	return nil
}

// tableName returns the Antfly table name for a namespace.
func tableName(namespace string) string {
	if namespace == "" || namespace == "default" {
		return "memories"
	}
	return namespace + "_memories"
}

// memoryKey returns the Antfly document key for a memory ID.
func memoryKey(id string) string {
	return "mem:" + id
}

// entityKey returns the Antfly document key for an entity.
func entityKey(label, text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	normalized = whitespaceRe.ReplaceAllString(normalized, "_")
	return "ent:" + label + ":" + normalized
}

// isMemoryHit returns true if the hit is a memory (not an entity node).
func isMemoryHit(id string) bool {
	return !strings.HasPrefix(id, "ent:")
}

// ensureNamespace creates the memoryaf table and indexes if they don't exist.
func (h *Handler) ensureNamespace(ctx context.Context, namespace string) error {
	if err := ValidateNamespace(namespace); err != nil {
		return err
	}

	h.mu.RLock()
	initialized := h.initializedNamespaces[namespace]
	h.mu.RUnlock()
	if initialized {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.initializedNamespaces[namespace] {
		return nil
	}

	table := tableName(namespace)

	memorySchema := client.DocumentSchema{
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"content":       map[string]any{"type": "string"},
				"memory_type":   map[string]any{"type": "string"},
				"tags":          map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"project":       map[string]any{"type": "string"},
				"source":        map[string]any{"type": "string"},
				"created_by":    map[string]any{"type": "string"},
				"visibility":    map[string]any{"type": "string"},
				"created_at":    map[string]any{"type": "string"},
				"updated_at":    map[string]any{"type": "string"},
				"entities":      map[string]any{"type": "array"},
				"trigger":       map[string]any{"type": "string"},
				"steps":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"outcome":       map[string]any{"type": "string"},
				"confidence":    map[string]any{"type": "number"},
				"supersedes":    map[string]any{"type": "string"},
				"event_time":    map[string]any{"type": "string"},
				"context":       map[string]any{"type": "string"},
				"session_id":    map[string]any{"type": "string"},
				"entity_type":   map[string]any{"type": "string"},
				"label":         map[string]any{"type": "string"},
				"text":          map[string]any{"type": "string"},
				"mention_count": map[string]any{"type": "number"},
				"first_seen":    map[string]any{"type": "string"},
				"last_seen":     map[string]any{"type": "string"},
			},
		},
	}

	embIdx, err := client.NewIndexConfig(embeddingIndex, client.EmbeddingsIndexConfig{
		Dimension: embedderDimension,
		Field:     "content",
		Embedder: client.EmbedderConfig{
			Provider: client.EmbedderProvider(embedderProvider),
		},
	})
	if err != nil {
		return fmt.Errorf("build embedding index config: %w", err)
	}

	graphIdx, err := client.NewIndexConfig(graphIndex, client.GraphIndexConfig{
		EdgeTypes: []client.EdgeTypeConfig{
			{Name: "mentions", MaxWeight: 1.0, MinWeight: 0.0, AllowSelfLoops: false},
			{Name: "related_to", MaxWeight: 1.0, MinWeight: 0.0, AllowSelfLoops: false},
			{Name: "supersedes", MaxWeight: 1.0, MinWeight: 0.0, AllowSelfLoops: false},
		},
		MaxEdgesPerDocument: 0,
	})
	if err != nil {
		return fmt.Errorf("build graph index config: %w", err)
	}

	err = h.client.CreateTable(ctx, table, &client.CreateTableRequest{
		Schema: client.TableSchema{
			DefaultType: "memory",
			DocumentSchemas: map[string]client.DocumentSchema{
				"memory": memorySchema,
			},
		},
		Indexes: map[string]client.IndexConfig{
			embeddingIndex: *embIdx,
			graphIndex:     *graphIdx,
		},
	})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create table %s: %w", table, err)
		}
	}

	h.initializedNamespaces[namespace] = true
	h.logger.Info("initialized memoryaf namespace", zap.String("namespace", namespace), zap.String("table", table))
	return nil
}

// --- Query helpers ---

func buildFilterQuery(opts filterOpts, ctx *UserContext) *query.Query {
	var clauses []query.Query

	if opts.Project != "" {
		clauses = append(clauses, query.NewTerm(opts.Project, "project"))
	}
	if opts.MemoryType != "" {
		clauses = append(clauses, query.NewTerm(opts.MemoryType, "memory_type"))
	}
	if opts.CreatedBy != "" {
		clauses = append(clauses, query.NewTerm(opts.CreatedBy, "created_by"))
	}
	if opts.EntityType != "" {
		clauses = append(clauses, query.NewTerm(opts.EntityType, "entity_type"))
	}
	for _, tag := range opts.Tags {
		clauses = append(clauses, query.NewTerm(tag, "tags"))
	}

	// Visibility filtering: when no explicit visibility is requested and we have
	// a user context, show team memories + the caller's own private memories.
	if ctx != nil && opts.Visibility == "" {
		teamQ := query.NewTerm(VisibilityTeam, "visibility")
		ownerQ := query.NewTerm(ctx.UserID, "created_by")
		disj := query.DisjunctionQuery{Disjuncts: []query.Query{teamQ, ownerQ}}.ToQuery()
		clauses = append(clauses, disj)
	} else if opts.Visibility != "" {
		clauses = append(clauses, query.NewTerm(opts.Visibility, "visibility"))
	}

	if len(clauses) == 0 {
		return nil
	}
	if len(clauses) == 1 {
		return &clauses[0]
	}
	conj := query.ConjunctionQuery{Conjuncts: clauses}.ToQuery()
	return &conj
}

type filterOpts struct {
	Project    string
	Tags       []string
	MemoryType string
	CreatedBy  string
	Visibility string
	EntityType string
}


// --- hitToMemory conversion ---

func hitToMemory(id string, source map[string]any) Memory {
	memID := id
	if strings.HasPrefix(memID, "mem:") {
		memID = memID[4:]
	}

	m := Memory{
		ID:         memID,
		Content:    getString(source, "content"),
		MemoryType: getStringDefault(source, "memory_type", MemoryTypeSemantic),
		Tags:       getStringSlice(source, "tags"),
		Project:    getString(source, "project"),
		Source:     getString(source, "source"),
		CreatedBy:  getString(source, "created_by"),
		Visibility: getStringDefault(source, "visibility", VisibilityTeam),
		CreatedAt:  getString(source, "created_at"),
		UpdatedAt:  getString(source, "updated_at"),
		Entities:   getEntities(source, "entities"),
		EventTime:  getString(source, "event_time"),
		Context:    getString(source, "context"),
		Supersedes: getString(source, "supersedes"),
		Trigger:    getString(source, "trigger"),
		Steps:      getStringSlice(source, "steps"),
		Outcome:    getString(source, "outcome"),
	}
	if v, ok := source["confidence"]; ok {
		if f, ok := v.(float64); ok {
			m.Confidence = &f
		}
	}
	return m
}

// --- Memory CRUD ---

func (h *Handler) StoreMemory(ctx context.Context, args StoreMemoryArgs, uctx UserContext) (*Memory, error) {
	if args.Content == "" {
		return nil, fmt.Errorf("content is required")
	}

	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	id := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	key := memoryKey(id)

	doc := buildMemoryDoc(args, uctx.UserID, now)
	doc["created_at"] = now

	// Insert the memory document immediately; entity extraction runs async.
	_, err := h.client.Batch(ctx, table, client.BatchRequest{
		Inserts: map[string]any{key: doc},
	})
	if err != nil {
		return nil, fmt.Errorf("batch insert: %w", err)
	}

	// Extract entities and link graph edges in the background.
	go h.extractAndLinkEntities(context.WithoutCancel(ctx), id, args.Content, uctx.Namespace)

	m := hitToMemory(key, doc)
	return &m, nil
}

func (h *Handler) GetMemory(ctx context.Context, id string, uctx UserContext) (*Memory, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	key := memoryKey(id)

	ma := query.NewMatchAll()
	resp, err := h.client.QueryWithBody(ctx, mustMarshal(map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(ma)),
		"filter_prefix":    key,
		"limit":            1,
	}))
	if err != nil {
		return nil, fmt.Errorf("lookup memory: %w", err)
	}

	hit, err := firstHitFromResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("lookup memory %s: %w", id, err)
	}
	if hit == nil || hit.ID != key {
		return nil, fmt.Errorf("memory not found: %s", id)
	}

	m := hitToMemory(hit.ID, hit.Source)
	return &m, nil
}

func (h *Handler) UpdateMemory(ctx context.Context, args UpdateMemoryArgs, uctx UserContext) (*Memory, error) {
	if args.ID == "" {
		return nil, fmt.Errorf("id is required")
	}

	existing, err := h.GetMemory(ctx, args.ID, uctx)
	if err != nil {
		return nil, err
	}

	if existing.CreatedBy != uctx.UserID && uctx.Role != "admin" {
		return nil, fmt.Errorf("forbidden: you can only update your own memories")
	}

	table := tableName(uctx.Namespace)
	key := memoryKey(args.ID)
	now := time.Now().UTC().Format(time.RFC3339)

	updated := mergeMemoryFields(existing, args, now)

	_, err = h.client.Batch(ctx, table, client.BatchRequest{
		Inserts: map[string]any{key: updated},
	})
	if err != nil {
		return nil, fmt.Errorf("batch update: %w", err)
	}

	// Re-extract entities in the background if content changed.
	if args.Content != "" && args.Content != existing.Content {
		go h.extractAndLinkEntities(context.WithoutCancel(ctx), args.ID, args.Content, uctx.Namespace)
	}

	m := hitToMemory(key, updated)
	return &m, nil
}

func (h *Handler) DeleteMemory(ctx context.Context, id string, uctx UserContext) error {
	if id == "" {
		return fmt.Errorf("id is required")
	}

	existing, err := h.GetMemory(ctx, id, uctx)
	if err != nil {
		return err
	}

	if existing.CreatedBy != uctx.UserID && uctx.Role != "admin" {
		return fmt.Errorf("forbidden: you can only delete your own memories")
	}

	table := tableName(uctx.Namespace)
	_, err = h.client.Batch(ctx, table, client.BatchRequest{
		Deletes: []string{memoryKey(id)},
	})
	return err
}

func (h *Handler) ListMemories(ctx context.Context, args ListMemoriesArgs, uctx UserContext) ([]Memory, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	limit := args.Limit
	if limit <= 0 {
		limit = 20
	}

	filter := buildFilterQuery(filterOpts{
		Project:    args.Project,
		Tags:       args.Tags,
		MemoryType: args.MemoryType,
		CreatedBy:  args.CreatedBy,
		Visibility: args.Visibility,
	}, &uctx)

	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(query.NewMatchAll())),
		"limit":            limit,
	}
	if args.Offset > 0 {
		reqMap["offset"] = args.Offset
	}
	if filter != nil {
		reqMap["filter_query"] = json.RawMessage(mustMarshal(filter))
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("list memories: %w", err)
	}

	hits, err := hitsFromResponse(resp)
	if err != nil {
		return nil, err
	}

	var memories []Memory
	for _, hit := range hits {
		if isMemoryHit(hit.ID) {
			memories = append(memories, hitToMemory(hit.ID, hit.Source))
		}
	}
	return memories, nil
}

func (h *Handler) SearchMemories(ctx context.Context, args SearchMemoriesArgs, uctx UserContext) ([]SearchResult, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	limit := args.Limit
	if limit <= 0 {
		limit = 10
	}

	filter := buildFilterQuery(filterOpts{
		Project:    args.Project,
		Tags:       args.Tags,
		MemoryType: args.MemoryType,
		CreatedBy:  args.CreatedBy,
	}, &uctx)

	ftsQuery := query.MatchQuery{Match: args.Query}.ToQuery()
	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(ftsQuery)),
		"semantic_search":  args.Query,
		"indexes":          []string{embeddingIndex},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"limit":            limit,
	}
	if filter != nil {
		reqMap["filter_query"] = json.RawMessage(mustMarshal(filter))
	}

	if args.ExpandGraph && h.extractor != nil {
		queryEntities := h.extractEntities(ctx, args.Query)
		if len(queryEntities) > 0 {
			var entityKeys []string
			for _, e := range queryEntities {
				entityKeys = append(entityKeys, entityKey(e.Label, e.Text))
			}
			reqMap["graph_searches"] = map[string]any{
				"entity_expansion": map[string]any{
					"type":       "neighbors",
					"index_name": graphIndex,
					"start_nodes": map[string]any{
						"keys": entityKeys,
					},
					"params": map[string]any{
						"edge_types": []string{"mentions"},
						"direction":  "in",
						"max_depth":  1,
					},
					"include_documents": true,
				},
			}
			reqMap["graph_merge_strategy"] = "union"
		}
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("search memories: %w", err)
	}

	hits, err := hitsFromResponse(resp)
	if err != nil {
		return nil, err
	}

	var results []SearchResult
	for _, hit := range hits {
		if isMemoryHit(hit.ID) {
			results = append(results, SearchResult{
				Memory: hitToMemory(hit.ID, hit.Source),
				Score:  hit.Score,
			})
		}
	}
	return results, nil
}

func (h *Handler) FindRelated(ctx context.Context, args FindRelatedArgs, uctx UserContext) ([]SearchResult, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	limit := args.Limit
	if limit <= 0 {
		limit = 10
	}
	depth := args.Depth
	if depth <= 0 {
		depth = 2
	}
	mKey := memoryKey(args.ID)

	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(query.NewMatchAll())),
		"limit":            limit,
		"graph_searches": map[string]any{
			"related": map[string]any{
				"type":       "traverse",
				"index_name": graphIndex,
				"start_nodes": map[string]any{
					"keys": []string{mKey},
				},
				"params": map[string]any{
					"edge_types":  []string{"mentions", "related_to"},
					"direction":   "both",
					"max_depth":   depth,
					"max_results": limit,
				},
				"include_documents": true,
			},
		},
		"graph_merge_strategy": "union",
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("find related: %w", err)
	}

	hits, err := hitsFromResponse(resp)
	if err != nil {
		return nil, err
	}

	var results []SearchResult
	for _, hit := range hits {
		if isMemoryHit(hit.ID) && hit.ID != mKey {
			results = append(results, SearchResult{
				Memory: hitToMemory(hit.ID, hit.Source),
				Score:  hit.Score,
			})
		}
	}
	return results, nil
}

func (h *Handler) GetEntityMemories(ctx context.Context, args EntityMemoriesArgs, uctx UserContext) ([]SearchResult, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	limit := args.Limit
	if limit <= 0 {
		limit = 20
	}
	eKey := entityKey(args.EntityLabel, args.EntityText)

	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(query.NewMatchAll())),
		"limit":            limit,
		"graph_searches": map[string]any{
			"mentions": map[string]any{
				"type":       "neighbors",
				"index_name": graphIndex,
				"start_nodes": map[string]any{
					"keys": []string{eKey},
				},
				"params": map[string]any{
					"edge_types": []string{"mentions"},
					"direction":  "in",
					"max_depth":  1,
				},
				"include_documents": true,
			},
		},
		"graph_merge_strategy": "union",
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("entity memories: %w", err)
	}

	hits, err := hitsFromResponse(resp)
	if err != nil {
		return nil, err
	}

	var results []SearchResult
	for _, hit := range hits {
		if isMemoryHit(hit.ID) {
			results = append(results, SearchResult{
				Memory: hitToMemory(hit.ID, hit.Source),
				Score:  hit.Score,
			})
		}
	}
	return results, nil
}

func (h *Handler) ListEntities(ctx context.Context, args ListEntitiesArgs, uctx UserContext) ([]EntityNode, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	limit := args.Limit
	if limit <= 0 {
		limit = 50
	}

	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(query.NewTerm(entityDocType, "entity_type"))),
		"limit":            limit,
	}
	if args.Label != "" {
		reqMap["filter_query"] = json.RawMessage(mustMarshal(query.NewTerm(args.Label, "label")))
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("list entities: %w", err)
	}

	hits, err := hitsFromResponse(resp)
	if err != nil {
		return nil, err
	}

	var nodes []EntityNode
	for _, hit := range hits {
		nodes = append(nodes, EntityNode{
			EntityType:   entityDocType,
			Text:         getString(hit.Source, "text"),
			Label:        getString(hit.Source, "label"),
			MentionCount: getInt(hit.Source, "mention_count"),
			FirstSeen:    getString(hit.Source, "first_seen"),
			LastSeen:     getString(hit.Source, "last_seen"),
		})
	}
	return nodes, nil
}

func (h *Handler) GetStats(ctx context.Context, args MemoryStatsArgs, uctx UserContext) (*MemoryStats, error) {
	if err := h.ensureNamespace(ctx, uctx.Namespace); err != nil {
		return nil, err
	}

	table := tableName(uctx.Namespace)
	filter := buildFilterQuery(filterOpts{Project: args.Project}, &uctx)

	reqMap := map[string]any{
		"table":            table,
		"full_text_search": json.RawMessage(mustMarshal(query.NewMatchAll())),
		"limit":            0,
		"count":            true,
		"aggregations": map[string]any{
			"by_type":       map[string]any{"type": "terms", "field": "memory_type", "size": 5},
			"by_project":    map[string]any{"type": "terms", "field": "project", "size": 20},
			"by_tag":        map[string]any{"type": "terms", "field": "tags", "size": 30},
			"by_visibility": map[string]any{"type": "terms", "field": "visibility", "size": 2},
		},
	}
	if filter != nil {
		reqMap["filter_query"] = json.RawMessage(mustMarshal(filter))
	}

	resp, err := h.client.QueryWithBody(ctx, mustMarshal(reqMap))
	if err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}

	return statsFromResponse(resp)
}

// --- Entity extraction and graph linking ---

// extractEntities runs the extractor on a single text and returns filtered entities.
func (h *Handler) extractEntities(ctx context.Context, text string) []Entity {
	if h.extractor == nil {
		return nil
	}
	extractions, err := h.extractor.Extract(ctx, []string{text}, ExtractOptions{
		EntityLabels: h.entityLabels,
	})
	if err != nil {
		h.logger.Warn("entity extraction failed", zap.Error(err))
		return nil
	}
	if len(extractions) == 0 || len(extractions[0].Entities) == 0 {
		return nil
	}
	var entities []Entity
	for _, e := range extractions[0].Entities {
		if e.Score >= h.entityThreshold {
			entities = append(entities, Entity{
				Text:  e.Text,
				Label: e.Label,
				Score: float64(e.Score),
			})
		}
	}
	return entities
}

func (h *Handler) extractAndLinkEntities(ctx context.Context, memoryID, content, namespace string) []Entity {
	if h.extractor == nil {
		return nil
	}

	extractions, err := h.extractor.Extract(ctx, []string{content}, ExtractOptions{
		EntityLabels: h.entityLabels,
	})
	if err != nil {
		h.logger.Warn("entity extraction failed", zap.Error(err))
		return nil
	}
	if len(extractions) == 0 || len(extractions[0].Entities) == 0 {
		return nil
	}

	var filtered []Entity
	for _, e := range extractions[0].Entities {
		if e.Score >= h.entityThreshold {
			filtered = append(filtered, Entity{
				Text:  e.Text,
				Label: e.Label,
				Score: float64(e.Score),
			})
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	table := tableName(namespace)
	mKey := memoryKey(memoryID)
	now := time.Now().UTC().Format(time.RFC3339)

	// Build entity node inserts and transforms together to reduce round-trips.
	inserts := make(map[string]any, len(filtered)+1)
	var transforms []client.Transform
	edgesByType := make(map[string][]map[string]any, 1)
	for _, entity := range filtered {
		eKey := entityKey(entity.Label, entity.Text)
		inserts[eKey] = map[string]any{
			"entity_type": entityDocType,
			"text":        entity.Text,
			"label":       entity.Label,
			"first_seen":  now,
			"last_seen":   now,
		}
		edgesByType["mentions"] = append(edgesByType["mentions"], map[string]any{
			"target": eKey,
			"weight": entity.Score,
		})
		transforms = append(transforms, client.Transform{
			Key: eKey,
			Operations: []client.TransformOp{
				{Op: client.TransformOpTypeInc, Path: "$.mention_count", Value: 1},
				{Op: client.TransformOpTypeSet, Path: "$.last_seen", Value: now},
			},
		})
	}

	// Batch 1: entity node upserts + mention count transforms.
	if _, err := h.client.Batch(ctx, table, client.BatchRequest{Inserts: inserts, Transforms: transforms}); err != nil {
		h.logger.Warn("entity node batch failed", zap.Error(err))
	}

	// Batch 2: graph edges from memory to entity nodes (separate key structure).
	edgeInserts := map[string]any{
		mKey: map[string]any{
			"_edges": map[string]any{
				graphIndex: edgesByType,
			},
		},
	}
	if _, err := h.client.Batch(ctx, table, client.BatchRequest{Inserts: edgeInserts}); err != nil {
		h.logger.Warn("edge creation failed", zap.Error(err))
	}

	return filtered
}
