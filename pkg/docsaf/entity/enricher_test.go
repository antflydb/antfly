package entity

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/antflydb/antfly/pkg/docsaf"
)

type extractCall struct {
	texts []string
	opts  ExtractOptions
}

type mockExtractor struct {
	calls   []extractCall
	results [][]Extraction
	err     error
}

func (m *mockExtractor) Extract(_ context.Context, texts []string, opts ExtractOptions) ([]Extraction, error) {
	m.calls = append(m.calls, extractCall{
		texts: append([]string(nil), texts...),
		opts: ExtractOptions{
			EntityLabels:   append([]string(nil), opts.EntityLabels...),
			RelationLabels: append([]string(nil), opts.RelationLabels...),
		},
	})
	if m.err != nil {
		return nil, m.err
	}
	if len(m.results) == 0 {
		return []Extraction{}, nil
	}
	result := m.results[0]
	m.results = m.results[1:]
	return result, nil
}

func TestNormalizeKeys(t *testing.T) {
	t.Parallel()

	if got := NormalizeEntityKey("technology", "Raft Consensus"); got != "entity:technology:raft-consensus" {
		t.Fatalf("NormalizeEntityKey returned %q", got)
	}

	if got := NormalizeEntityKey("location", "München"); got != "entity:location:mfcnchen" {
		t.Fatalf("NormalizeEntityKey non-ASCII return = %q", got)
	}

	if got := NormalizeRelationKey("depends_on", "entity:concept:raft", "entity:concept:consensus"); got != "relation:depends_on:entity:concept:raft:entity:concept:consensus" {
		t.Fatalf("NormalizeRelationKey returned %q", got)
	}
}

func TestEnricherEnrichEntitiesAndRelations(t *testing.T) {
	t.Parallel()

	extractor := &mockExtractor{
		results: [][]Extraction{
			{
				{
					Entities: []Entity{
						{Text: "Raft", Label: "technology", Score: 0.9},
						{Text: "Consensus", Label: "concept", Score: 0.8},
						{Text: "discard", Label: "noise", Score: 0.1},
					},
					Relations: []Relation{
						{
							Head:  Entity{Text: "Raft", Label: "technology", Score: 0.9},
							Label: "implements",
							Score: 0.95,
							Tail:  Entity{Text: "Consensus", Label: "concept", Score: 0.8},
						},
					},
				},
				{
					Entities: []Entity{
						{Text: "Raft", Label: "technology", Score: 0.92},
					},
				},
			},
		},
	}

	sections := []docsaf.DocumentSection{
		{ID: "doc-1", Title: "Raft", Content: "Consensus overview"},
		{ID: "doc-2", Title: "System", Content: "Raft drives replication"},
	}

	result, err := NewEnricher(
		extractor,
		WithEntityLabels([]string{"technology", "concept"}),
		WithRelationLabels([]string{"implements"}),
		WithEntityThreshold(0.5),
		WithRelationThreshold(0.5),
	).Enrich(context.Background(), sections)
	if err != nil {
		t.Fatalf("Enrich returned error: %v", err)
	}

	if len(extractor.calls) != 1 {
		t.Fatalf("Extractor call count = %d, want 1", len(extractor.calls))
	}

	wantTexts := []string{"Raft\n\nConsensus overview", "System\n\nRaft drives replication"}
	if !reflect.DeepEqual(extractor.calls[0].texts, wantTexts) {
		t.Fatalf("Texts = %#v, want %#v", extractor.calls[0].texts, wantTexts)
	}

	wantEntityKeys := []string{"entity:technology:raft", "entity:concept:consensus"}
	if !reflect.DeepEqual(result.SectionEntityKeys["doc-1"], wantEntityKeys) {
		t.Fatalf("SectionEntityKeys[doc-1] = %#v, want %#v", result.SectionEntityKeys["doc-1"], wantEntityKeys)
	}

	wantRelationKeys := []string{"relation:implements:entity:technology:raft:entity:concept:consensus"}
	if !reflect.DeepEqual(result.SectionRelationKeys["doc-1"], wantRelationKeys) {
		t.Fatalf("SectionRelationKeys[doc-1] = %#v, want %#v", result.SectionRelationKeys["doc-1"], wantRelationKeys)
	}

	if got := result.EntityRecords["entity:technology:raft"].MentionCount; got != 2 {
		t.Fatalf("Raft mention count = %d, want 2", got)
	}

	if got := result.RelationRecords[wantRelationKeys[0]].MentionCount; got != 1 {
		t.Fatalf("Relation mention count = %d, want 1", got)
	}

	if got := result.EntityLabelCounts(); !reflect.DeepEqual(got, map[string]int{"technology": 1, "concept": 1}) {
		t.Fatalf("EntityLabelCounts = %#v", got)
	}

	if got := result.RelationLabelCounts(); !reflect.DeepEqual(got, map[string]int{"implements": 1}) {
		t.Fatalf("RelationLabelCounts = %#v", got)
	}
}

func TestEnricherExtractError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	_, err := NewEnricher(&mockExtractor{err: wantErr}).Enrich(context.Background(), []docsaf.DocumentSection{
		{ID: "doc-1", Content: "text"},
	})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("Enrich error = %v, want wrapped %v", err, wantErr)
	}
}

func TestEnricherLengthMismatch(t *testing.T) {
	t.Parallel()

	_, err := NewEnricher(&mockExtractor{
		results: [][]Extraction{
			{
				{Entities: []Entity{{Text: "one", Label: "concept", Score: 0.9}}},
			},
		},
	}).Enrich(context.Background(), []docsaf.DocumentSection{
		{ID: "doc-1", Content: "first"},
		{ID: "doc-2", Content: "second"},
	})
	if err == nil {
		t.Fatal("Enrich error = nil, want mismatch error")
	}
}
