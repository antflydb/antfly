package entity

import (
	"context"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/pkg/docsaf"
	"github.com/cespare/xxhash/v2"
)

const DefaultBatchSize = 32

// Entity is a generic entity extracted from text.
type Entity struct {
	Text  string
	Label string
	Score float32
	Start int
	End   int
}

// Relation is a typed edge between two extracted entities.
type Relation struct {
	Head  Entity
	Label string
	Score float32
	Tail  Entity
}

// Extraction contains the entities and relations extracted from one input text.
type Extraction struct {
	Entities  []Entity
	Relations []Relation
}

// ExtractOptions configures an Extractor request.
type ExtractOptions struct {
	EntityLabels   []string
	RelationLabels []string
}

// Extractor extracts entities and optionally relations from a batch of texts.
type Extractor interface {
	Extract(ctx context.Context, texts []string, opts ExtractOptions) ([]Extraction, error)
}

// EntityRecord tracks a canonical entity node and how often it was mentioned.
type EntityRecord struct {
	ID           string
	Name         string
	Label        string
	MentionCount int
}

// ToDocument converts an EntityRecord to a storage-ready document map.
func (r EntityRecord) ToDocument() map[string]any {
	return map[string]any{
		"id":            r.ID,
		"name":          r.Name,
		"label":         r.Label,
		"_type":         "entity",
		"mention_count": r.MentionCount,
	}
}

// RelationRecord tracks a canonical relation node and the sections that mention it.
type RelationRecord struct {
	ID           string
	Label        string
	HeadEntity   string
	TailEntity   string
	HeadName     string
	TailName     string
	HeadLabel    string
	TailLabel    string
	Weight       float64
	MentionCount int
}

// ToDocument converts a RelationRecord to a storage-ready document map.
func (r RelationRecord) ToDocument() map[string]any {
	return map[string]any{
		"id":            r.ID,
		"label":         r.Label,
		"head_entity":   r.HeadEntity,
		"tail_entity":   r.TailEntity,
		"head_name":     r.HeadName,
		"tail_name":     r.TailName,
		"head_label":    r.HeadLabel,
		"tail_label":    r.TailLabel,
		"weight":        r.Weight,
		"_type":         "relation",
		"mention_count": r.MentionCount,
	}
}

// Result contains extracted entities and relations grouped by section ID.
type Result struct {
	EntityRecords       map[string]EntityRecord
	SectionEntityKeys   map[string][]string
	RelationRecords     map[string]RelationRecord
	SectionRelationKeys map[string][]string
}

// EntityLabelCounts summarizes unique entities by label.
func (r *Result) EntityLabelCounts() map[string]int {
	counts := make(map[string]int)
	if r == nil {
		return counts
	}
	for _, entity := range r.EntityRecords {
		counts[entity.Label]++
	}
	return counts
}

// RelationLabelCounts summarizes unique relations by label.
func (r *Result) RelationLabelCounts() map[string]int {
	counts := make(map[string]int)
	if r == nil {
		return counts
	}
	for _, relation := range r.RelationRecords {
		counts[relation.Label]++
	}
	return counts
}

// Option configures an Enricher.
type Option func(*Enricher)

// WithEntityLabels sets entity labels used by the extractor.
func WithEntityLabels(labels []string) Option {
	return func(e *Enricher) {
		e.entityLabels = append([]string(nil), labels...)
	}
}

// WithRelationLabels sets relation labels used by the extractor.
func WithRelationLabels(labels []string) Option {
	return func(e *Enricher) {
		e.relationLabels = append([]string(nil), labels...)
	}
}

// WithEntityThreshold sets the minimum entity score to keep.
func WithEntityThreshold(threshold float32) Option {
	return func(e *Enricher) {
		e.entityThreshold = threshold
	}
}

// WithRelationThreshold sets the minimum relation score to keep.
func WithRelationThreshold(threshold float32) Option {
	return func(e *Enricher) {
		e.relationThreshold = threshold
	}
}

// WithBatchSize sets the number of sections per extractor request.
func WithBatchSize(batchSize int) Option {
	return func(e *Enricher) {
		e.batchSize = batchSize
	}
}

// WithTextBuilder overrides how section text is prepared for extraction.
func WithTextBuilder(fn func(docsaf.DocumentSection) string) Option {
	return func(e *Enricher) {
		e.textBuilder = fn
	}
}

// Enricher runs batched entity extraction over docsaf sections.
type Enricher struct {
	extractor         Extractor
	entityLabels      []string
	relationLabels    []string
	entityThreshold   float32
	relationThreshold float32
	batchSize         int
	textBuilder       func(docsaf.DocumentSection) string
}

// NewEnricher creates a new entity enricher.
func NewEnricher(extractor Extractor, opts ...Option) *Enricher {
	e := &Enricher{
		extractor:         extractor,
		batchSize:         DefaultBatchSize,
		textBuilder:       defaultTextBuilder,
		entityThreshold:   0.5,
		relationThreshold: 0.5,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Enrich extracts entities and relations from sections and groups them by section ID.
func (e *Enricher) Enrich(ctx context.Context, sections []docsaf.DocumentSection) (*Result, error) {
	if e.extractor == nil {
		return nil, fmt.Errorf("entity enricher: no extractor configured")
	}

	result := &Result{
		EntityRecords:       make(map[string]EntityRecord),
		SectionEntityKeys:   make(map[string][]string),
		RelationRecords:     make(map[string]RelationRecord),
		SectionRelationKeys: make(map[string][]string),
	}
	if len(sections) == 0 {
		return result, nil
	}

	batchSize := e.batchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	for start := 0; start < len(sections); start += batchSize {
		end := min(start+batchSize, len(sections))
		texts := make([]string, end-start)
		for i, section := range sections[start:end] {
			texts[i] = e.textBuilder(section)
		}

		extractions, err := e.extractor.Extract(ctx, texts, ExtractOptions{
			EntityLabels:   e.entityLabels,
			RelationLabels: e.relationLabels,
		})
		if err != nil {
			return nil, fmt.Errorf("entity batch %d (sections %d-%d): %w", start/batchSize+1, start+1, end, err)
		}
		if len(extractions) != len(texts) {
			return nil, fmt.Errorf(
				"entity batch %d (sections %d-%d): extractor returned %d results for %d texts",
				start/batchSize+1,
				start+1,
				end,
				len(extractions),
				len(texts),
			)
		}

		for i, extraction := range extractions {
			sectionID := sections[start+i].ID
			seenEntities := make(map[string]bool)
			seenRelations := make(map[string]bool)

			for _, extractedEntity := range extraction.Entities {
				e.recordEntity(result, sectionID, extractedEntity, seenEntities)
			}

			for _, relation := range extraction.Relations {
				if relation.Score < e.relationThreshold {
					continue
				}

				headKey, ok := e.recordEntity(result, sectionID, relation.Head, seenEntities)
				if !ok {
					continue
				}
				tailKey, ok := e.recordEntity(result, sectionID, relation.Tail, seenEntities)
				if !ok {
					continue
				}

				relationKey := NormalizeRelationKey(relation.Label, headKey, tailKey)
				if seenRelations[relationKey] {
					continue
				}
				seenRelations[relationKey] = true
				result.SectionRelationKeys[sectionID] = append(result.SectionRelationKeys[sectionID], relationKey)

				record, exists := result.RelationRecords[relationKey]
				if exists {
					record.MentionCount++
					if float64(relation.Score) > record.Weight {
						record.Weight = float64(relation.Score)
					}
				} else {
					record = RelationRecord{
						ID:           relationKey,
						Label:        relation.Label,
						HeadEntity:   headKey,
						TailEntity:   tailKey,
						HeadName:     relation.Head.Text,
						TailName:     relation.Tail.Text,
						HeadLabel:    relation.Head.Label,
						TailLabel:    relation.Tail.Label,
						Weight:       float64(relation.Score),
						MentionCount: 1,
					}
				}
				result.RelationRecords[relationKey] = record
			}
		}
	}

	return result, nil
}

func (e *Enricher) recordEntity(result *Result, sectionID string, extracted Entity, seenEntities map[string]bool) (string, bool) {
	if extracted.Score < e.entityThreshold {
		return "", false
	}

	key := NormalizeEntityKey(extracted.Label, extracted.Text)
	if !seenEntities[key] {
		seenEntities[key] = true
		result.SectionEntityKeys[sectionID] = append(result.SectionEntityKeys[sectionID], key)

		record, exists := result.EntityRecords[key]
		if exists {
			record.MentionCount++
			result.EntityRecords[key] = record
		} else {
			result.EntityRecords[key] = EntityRecord{
				ID:           key,
				Name:         extracted.Text,
				Label:        extracted.Label,
				MentionCount: 1,
			}
		}
	}

	return key, true
}

// NormalizeEntityKey creates a stable, sortable key from an entity label and text.
func NormalizeEntityKey(label, text string) string {
	normalized := strings.ToLower(strings.TrimSpace(text))
	normalized = strings.ReplaceAll(normalized, " ", "-")
	var b strings.Builder
	for _, r := range normalized {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else if r > 127 {
			fmt.Fprintf(&b, "%x", r)
		}
	}
	key := b.String()
	if key == "" {
		key = fmt.Sprintf("%x", xxhash.Sum64String(text))
	}
	return fmt.Sprintf("entity:%s:%s", strings.ToLower(label), key)
}

// NormalizeRelationKey creates a stable key for a relation between two entity keys.
func NormalizeRelationKey(label, headEntityKey, tailEntityKey string) string {
	return fmt.Sprintf("relation:%s:%s:%s", strings.ToLower(strings.TrimSpace(label)), headEntityKey, tailEntityKey)
}

func defaultTextBuilder(section docsaf.DocumentSection) string {
	title := strings.TrimSpace(section.Title)
	content := strings.TrimSpace(section.Content)
	switch {
	case title == "":
		return content
	case content == "":
		return title
	default:
		return title + "\n\n" + content
	}
}
