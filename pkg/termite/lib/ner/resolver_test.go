// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ner

import (
	"math"
	"testing"
)

func TestJaroWinkler_Identical(t *testing.T) {
	sim := JaroWinkler("hello", "hello")
	if sim != 1.0 {
		t.Errorf("expected 1.0, got %f", sim)
	}
}

func TestJaroWinkler_Empty(t *testing.T) {
	if JaroWinkler("", "hello") != 0.0 {
		t.Error("expected 0.0 for empty first string")
	}
	if JaroWinkler("hello", "") != 0.0 {
		t.Error("expected 0.0 for empty second string")
	}
	if JaroWinkler("", "") != 1.0 {
		t.Error("expected 1.0 for two empty strings")
	}
}

func TestJaroWinkler_SimilarStrings(t *testing.T) {
	tests := []struct {
		s1, s2 string
		minSim float64
		maxSim float64
	}{
		{"elon musk", "elon musk", 1.0, 1.0},
		{"spacex", "spacex inc", 0.85, 1.0},
		{"google", "alphabet", 0.0, 0.6},
		{"tesla", "tesla motors", 0.85, 1.0},
		{"john smith", "j. smith", 0.7, 0.95},
		{"musk", "elon musk", 0.0, 0.75}, // Jaro-Winkler: substring, lower similarity expected
	}

	for _, tt := range tests {
		sim := JaroWinkler(tt.s1, tt.s2)
		if sim < tt.minSim || sim > tt.maxSim {
			t.Errorf("JaroWinkler(%q, %q) = %f, expected in [%f, %f]",
				tt.s1, tt.s2, sim, tt.minSim, tt.maxSim)
		}
	}
}

func TestJaroWinkler_Symmetry(t *testing.T) {
	s1, s2 := "elon musk", "musk elon"
	sim1 := JaroWinkler(s1, s2)
	sim2 := JaroWinkler(s2, s1)
	if math.Abs(sim1-sim2) > 1e-10 {
		t.Errorf("JaroWinkler not symmetric: (%q,%q)=%f, (%q,%q)=%f",
			s1, s2, sim1, s2, s1, sim2)
	}
}

func TestEntitySimilarity_Containment(t *testing.T) {
	tests := []struct {
		s1, s2 string
		minSim float64
	}{
		{"elon musk", "musk", 0.7},     // "musk" is a token of "elon musk"
		{"spacex", "spacex inc", 0.85}, // "spacex" is a token of "spacex inc"
		{"tesla motors", "tesla", 0.7}, // "tesla" is a token of "tesla motors"
		{"elon musk", "elon musk", 1.0},
	}

	for _, tt := range tests {
		sim := EntitySimilarity(tt.s1, tt.s2)
		if sim < tt.minSim {
			t.Errorf("EntitySimilarity(%q, %q) = %f, expected >= %f",
				tt.s1, tt.s2, sim, tt.minSim)
		}
	}
}

func TestBuildKnowledgeGraph_BasicResolution(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Elon Musk", Label: "person", Score: 0.95, Start: 0, End: 9},
			{Text: "SpaceX", Label: "organization", Score: 0.92, Start: 18, End: 24},
		},
		{
			{Text: "Musk", Label: "person", Score: 0.88, Start: 0, End: 4},
			{Text: "Tesla", Label: "organization", Score: 0.90, Start: 22, End: 27},
		},
	}

	relations := [][]Relation{
		{
			{
				HeadEntity: Entity{Text: "Elon Musk", Label: "person", Score: 0.95},
				TailEntity: Entity{Text: "SpaceX", Label: "organization", Score: 0.92},
				Label:      "founded",
				Score:      0.89,
			},
		},
		{
			{
				HeadEntity: Entity{Text: "Musk", Label: "person", Score: 0.88},
				TailEntity: Entity{Text: "Tesla", Label: "organization", Score: 0.90},
				Label:      "ceo_of",
				Score:      0.85,
			},
		},
	}

	cfg := DefaultResolverConfig()
	// Lower threshold to merge "Elon Musk" and "Musk" via token containment
	cfg.SimilarityThreshold = 0.7

	kg := BuildKnowledgeGraph(entities, relations, cfg)

	// "Elon Musk" and "Musk" should be resolved to one entity.
	personCount := 0
	for _, e := range kg.Entities {
		if e.Label == "person" {
			personCount++
			if e.CanonicalName != "Elon Musk" {
				t.Errorf("expected canonical name 'Elon Musk', got %q", e.CanonicalName)
			}
			if len(e.Mentions) != 2 {
				t.Errorf("expected 2 mentions, got %d: %v", len(e.Mentions), e.Mentions)
			}
		}
	}
	if personCount != 1 {
		t.Errorf("expected 1 resolved person entity, got %d", personCount)
	}

	// Should have 3 entities total: 1 person, 2 orgs.
	if len(kg.Entities) != 3 {
		t.Errorf("expected 3 resolved entities, got %d", len(kg.Entities))
	}

	// Should have 2 relations.
	if len(kg.Relations) != 2 {
		t.Errorf("expected 2 relations, got %d", len(kg.Relations))
	}
}

func TestBuildKnowledgeGraph_TypeMustMatch(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Apple", Label: "organization", Score: 0.90},
			{Text: "Apple", Label: "product", Score: 0.85},
		},
	}

	cfg := DefaultResolverConfig()
	cfg.TypeMustMatch = true

	kg := BuildKnowledgeGraph(entities, nil, cfg)

	// With type_must_match, "Apple" (organization) and "Apple" (product) should NOT merge.
	if len(kg.Entities) != 2 {
		t.Errorf("expected 2 entities with type_must_match=true, got %d", len(kg.Entities))
	}

	// Without type_must_match, they should merge.
	cfg.TypeMustMatch = false
	kg = BuildKnowledgeGraph(entities, nil, cfg)
	if len(kg.Entities) != 1 {
		t.Errorf("expected 1 entity with type_must_match=false, got %d", len(kg.Entities))
	}
}

func TestBuildKnowledgeGraph_ConfidenceFilter(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Google", Label: "organization", Score: 0.95},
			{Text: "Noisy Entity", Label: "misc", Score: 0.15},
		},
	}

	cfg := DefaultResolverConfig()
	cfg.MinEntityConfidence = 0.5

	kg := BuildKnowledgeGraph(entities, nil, cfg)

	if len(kg.Entities) != 1 {
		t.Errorf("expected 1 entity after confidence filter, got %d", len(kg.Entities))
	}
	if kg.Entities[0].CanonicalName != "Google" {
		t.Errorf("expected 'Google', got %q", kg.Entities[0].CanonicalName)
	}
}

func TestBuildKnowledgeGraph_RelationConfidenceFilter(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Alice", Label: "person", Score: 0.90},
			{Text: "Bob", Label: "person", Score: 0.90},
		},
	}
	relations := [][]Relation{
		{
			{
				HeadEntity: Entity{Text: "Alice", Label: "person"},
				TailEntity: Entity{Text: "Bob", Label: "person"},
				Label:      "knows",
				Score:      0.3,
			},
		},
	}

	cfg := DefaultResolverConfig()
	cfg.MinRelationConfidence = 0.5

	kg := BuildKnowledgeGraph(entities, relations, cfg)

	if len(kg.Relations) != 0 {
		t.Errorf("expected 0 relations after confidence filter, got %d", len(kg.Relations))
	}
}

func TestBuildKnowledgeGraph_DeduplicateRelations(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Alice", Label: "person", Score: 0.90},
			{Text: "Google", Label: "organization", Score: 0.90},
		},
		{
			{Text: "Alice", Label: "person", Score: 0.88},
			{Text: "Google", Label: "organization", Score: 0.92},
		},
	}
	relations := [][]Relation{
		{
			{
				HeadEntity: Entity{Text: "Alice", Label: "person"},
				TailEntity: Entity{Text: "Google", Label: "organization"},
				Label:      "works_at",
				Score:      0.80,
			},
		},
		{
			{
				HeadEntity: Entity{Text: "Alice", Label: "person"},
				TailEntity: Entity{Text: "Google", Label: "organization"},
				Label:      "works_at",
				Score:      0.90,
			},
		},
	}

	cfg := DefaultResolverConfig()
	cfg.DeduplicateRelations = true

	kg := BuildKnowledgeGraph(entities, relations, cfg)

	if len(kg.Relations) != 1 {
		t.Errorf("expected 1 deduplicated relation, got %d", len(kg.Relations))
	}
	if len(kg.Relations) > 0 && kg.Relations[0].Score != 0.90 {
		t.Errorf("expected max score 0.90, got %f", kg.Relations[0].Score)
	}
}

func TestBuildKnowledgeGraph_NoProvenance(t *testing.T) {
	entities := [][]Entity{
		{
			{Text: "Elon Musk", Label: "person", Score: 0.95},
			{Text: "Musk", Label: "person", Score: 0.88},
		},
	}

	cfg := DefaultResolverConfig()
	cfg.SimilarityThreshold = 0.7
	cfg.TrackProvenance = false

	kg := BuildKnowledgeGraph(entities, nil, cfg)

	if len(kg.Entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(kg.Entities))
	}
	if kg.Entities[0].Mentions != nil {
		t.Errorf("expected nil mentions with track_provenance=false, got %v", kg.Entities[0].Mentions)
	}
}

func TestBuildKnowledgeGraph_EmptyInput(t *testing.T) {
	kg := BuildKnowledgeGraph(nil, nil, DefaultResolverConfig())
	if len(kg.Entities) != 0 {
		t.Errorf("expected 0 entities for nil input, got %d", len(kg.Entities))
	}
	if len(kg.Relations) != 0 {
		t.Errorf("expected 0 relations for nil input, got %d", len(kg.Relations))
	}
}
