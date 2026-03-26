package memoryaf

import "context"

const (
	MemoryTypeEpisodic   = "episodic"
	MemoryTypeSemantic   = "semantic"
	MemoryTypeProcedural = "procedural"

	VisibilityTeam    = "team"
	VisibilityPrivate = "private"

	entityDocType = "entity"
)

// Memory represents a stored memory document.
type Memory struct {
	ID         string   `json:"id"`
	Content    string   `json:"content"`
	MemoryType string   `json:"memory_type"`
	Tags       []string `json:"tags"`
	Project    string   `json:"project"`
	Source     string   `json:"source"`
	CreatedBy  string   `json:"created_by"`
	Visibility string   `json:"visibility"`
	CreatedAt  string   `json:"created_at"`
	UpdatedAt  string   `json:"updated_at"`
	Entities   []Entity `json:"entities"`

	// Episodic
	EventTime string `json:"event_time,omitempty"`
	Context   string `json:"context,omitempty"`

	// Semantic
	Confidence *float64 `json:"confidence,omitempty"`
	Supersedes string   `json:"supersedes,omitempty"`

	// Procedural
	Trigger string   `json:"trigger,omitempty"`
	Steps   []string `json:"steps,omitempty"`
	Outcome string   `json:"outcome,omitempty"`
}

// Entity is an NER-extracted entity.
type Entity struct {
	Text  string  `json:"text"`
	Label string  `json:"label"`
	Score float64 `json:"score"`
}

// ExtractedEntity is a single entity returned by an Extractor.
type ExtractedEntity struct {
	Text  string
	Label string
	Score float32
}

// Extraction contains the entities extracted from one input text.
type Extraction struct {
	Entities []ExtractedEntity
}

// ExtractOptions configures an Extractor request.
type ExtractOptions struct {
	EntityLabels []string
}

// Extractor extracts entities from a batch of texts.
// Implementations may use GLiNER2 (via Termite), tool-calling LLMs, or any other backend.
type Extractor interface {
	Extract(ctx context.Context, texts []string, opts ExtractOptions) ([]Extraction, error)
}

// EntityNode is a graph node representing a named entity.
type EntityNode struct {
	EntityType   string `json:"entity_type"`
	Text         string `json:"text"`
	Label        string `json:"label"`
	MentionCount int    `json:"mention_count"`
	FirstSeen    string `json:"first_seen"`
	LastSeen     string `json:"last_seen"`
}

// SearchResult pairs a Memory with its relevance score.
type SearchResult struct {
	Memory Memory  `json:"memory"`
	Score  float64 `json:"score"`
}

// MemoryStats holds aggregated memory statistics.
type MemoryStats struct {
	TotalMemories int            `json:"total_memories"`
	ByType        map[string]int `json:"by_type"`
	ByProject     map[string]int `json:"by_project"`
	ByTag         map[string]int `json:"by_tag"`
	ByVisibility  map[string]int `json:"by_visibility"`
}

// UserContext carries identity and authorization context.
type UserContext struct {
	UserID    string `json:"user_id"`
	Namespace string `json:"namespace"`
	Role      string `json:"role"` // "admin" or "member"
}

// --- Tool argument types ---

type StoreMemoryArgs struct {
	Content    string   `json:"content"    mcp:"The memory text. Be specific and atomic."`
	MemoryType string   `json:"memory_type,omitempty" mcp:"Memory type: episodic, semantic, or procedural (default: semantic)"`
	Tags       []string `json:"tags,omitempty"        mcp:"Category tags"`
	Project    string   `json:"project,omitempty"     mcp:"Project or codebase name"`
	Source     string   `json:"source,omitempty"      mcp:"Origin context"`
	Visibility string   `json:"visibility,omitempty"  mcp:"Visibility: team or private (default: team)"`
	// Episodic
	EventTime string `json:"event_time,omitempty" mcp:"When the event happened (ISO 8601)"`
	Context   string `json:"context,omitempty"    mcp:"Situational context for episodic memories"`
	// Semantic
	Confidence *float64 `json:"confidence,omitempty" mcp:"Fact confidence (0-1)"`
	Supersedes string   `json:"supersedes,omitempty" mcp:"ID of memory this replaces"`
	// Procedural
	Trigger string   `json:"trigger,omitempty" mcp:"What activates this workflow"`
	Steps   []string `json:"steps,omitempty"   mcp:"Ordered workflow steps"`
	Outcome string   `json:"outcome,omitempty" mcp:"Expected result"`
}

type SearchMemoriesArgs struct {
	Query       string   `json:"query"                mcp:"Natural language search query"`
	Project     string   `json:"project,omitempty"    mcp:"Filter to a specific project"`
	Tags        []string `json:"tags,omitempty"        mcp:"Filter by tags"`
	MemoryType  string   `json:"memory_type,omitempty" mcp:"Filter by type: episodic, semantic, procedural"`
	CreatedBy   string   `json:"created_by,omitempty"  mcp:"Filter by creator"`
	ExpandGraph bool     `json:"expand_graph,omitempty" mcp:"Expand results via entity graph (default: false)"`
	Limit       int      `json:"limit,omitempty"       mcp:"Max results (default: 10)"`
}

type ListMemoriesArgs struct {
	Project    string   `json:"project,omitempty"     mcp:"Filter to a specific project"`
	Tags       []string `json:"tags,omitempty"         mcp:"Filter by tags"`
	MemoryType string   `json:"memory_type,omitempty"  mcp:"Filter by type"`
	CreatedBy  string   `json:"created_by,omitempty"   mcp:"Filter by creator"`
	Visibility string   `json:"visibility,omitempty"   mcp:"Filter by visibility"`
	Limit      int      `json:"limit,omitempty"        mcp:"Max results (default: 20)"`
	Offset     int      `json:"offset,omitempty"       mcp:"Pagination offset"`
}

type UpdateMemoryArgs struct {
	ID         string   `json:"id"                    mcp:"The memory ID to update"`
	Content    string   `json:"content,omitempty"     mcp:"Updated content"`
	MemoryType string   `json:"memory_type,omitempty" mcp:"Updated type"`
	Tags       []string `json:"tags,omitempty"         mcp:"Updated tags"`
	Project    string   `json:"project,omitempty"     mcp:"Updated project"`
	Source     string   `json:"source,omitempty"      mcp:"Updated source"`
	Visibility string   `json:"visibility,omitempty"  mcp:"Updated visibility"`
	// Episodic
	EventTime string `json:"event_time,omitempty"`
	Context   string `json:"context,omitempty"`
	// Semantic
	Confidence *float64 `json:"confidence,omitempty"`
	Supersedes string   `json:"supersedes,omitempty"`
	// Procedural
	Trigger string   `json:"trigger,omitempty"`
	Steps   []string `json:"steps,omitempty"`
	Outcome string   `json:"outcome,omitempty"`
}

type FindRelatedArgs struct {
	ID    string `json:"id"              mcp:"The memory ID to find related memories for"`
	Depth int    `json:"depth,omitempty" mcp:"Graph traversal depth (default: 2)"`
	Limit int    `json:"limit,omitempty" mcp:"Max results (default: 10)"`
}

type EntityMemoriesArgs struct {
	EntityText  string `json:"entity_text"  mcp:"Entity text (e.g., TypeScript)"`
	EntityLabel string `json:"entity_label" mcp:"Entity type (e.g., technology)"`
	Limit       int    `json:"limit,omitempty" mcp:"Max results (default: 20)"`
}

type ListEntitiesArgs struct {
	Label string `json:"label,omitempty" mcp:"Filter by entity type"`
	Limit int    `json:"limit,omitempty" mcp:"Max results (default: 50)"`
}

type MemoryStatsArgs struct {
	Project string `json:"project,omitempty" mcp:"Filter stats to a specific project"`
}
