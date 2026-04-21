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
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
)

// ResolverConfig controls entity resolution behavior.
type ResolverConfig struct {
	// SimilarityThreshold is the Jaro-Winkler threshold for merging entities (0.0-1.0).
	SimilarityThreshold float64
	// TypeMustMatch requires entity types to match for merging.
	TypeMustMatch bool
	// MinEntityConfidence filters entities below this score.
	MinEntityConfidence float32
	// MinRelationConfidence filters relations below this score.
	MinRelationConfidence float32
	// DeduplicateRelations merges duplicate relations after entity resolution.
	DeduplicateRelations bool
	// TrackProvenance keeps track of all surface forms (mentions) per resolved entity.
	TrackProvenance bool
}

// DefaultResolverConfig returns sensible defaults for entity resolution.
func DefaultResolverConfig() ResolverConfig {
	return ResolverConfig{
		SimilarityThreshold:   0.85,
		TypeMustMatch:         true,
		MinEntityConfidence:   0.0,
		MinRelationConfidence: 0.0,
		DeduplicateRelations:  true,
		TrackProvenance:       true,
	}
}

// ResolvedEntity represents a deduplicated entity with merged mentions.
type ResolvedEntity struct {
	// ID is a stable identifier for the resolved entity.
	ID string
	// CanonicalName is the best surface form (longest or highest confidence).
	CanonicalName string
	// Label is the entity type.
	Label string
	// Score is the maximum confidence across all mentions.
	Score float32
	// Mentions are the distinct surface forms that resolved to this entity.
	Mentions []string
}

// ResolvedRelation represents a relation between resolved entities.
type ResolvedRelation struct {
	// HeadID is the resolved entity ID for the head.
	HeadID string
	// TailID is the resolved entity ID for the tail.
	TailID string
	// Label is the relation type.
	Label string
	// Score is the maximum confidence across duplicate relations.
	Score float32
}

// KnowledgeGraph holds the resolved entity graph.
type KnowledgeGraph struct {
	Entities  []ResolvedEntity
	Relations []ResolvedRelation
}

// BuildKnowledgeGraph takes raw entities and relations from one or more texts
// and produces a deduplicated knowledge graph via entity resolution.
func BuildKnowledgeGraph(entities [][]Entity, relations [][]Relation, cfg ResolverConfig) KnowledgeGraph {
	// Flatten all entities across texts, applying confidence filter.
	var allEntities []Entity
	for _, textEntities := range entities {
		for _, e := range textEntities {
			if e.Score >= cfg.MinEntityConfidence {
				allEntities = append(allEntities, e)
			}
		}
	}

	// Flatten all relations across texts, applying confidence filter.
	var allRelations []Relation
	for _, textRelations := range relations {
		for _, r := range textRelations {
			if r.Score >= cfg.MinRelationConfidence {
				allRelations = append(allRelations, r)
			}
		}
	}

	// Resolve entities: group similar mentions into clusters.
	resolved, mentionToID := resolveEntities(allEntities, cfg)

	// Map relations to resolved entity IDs.
	var resolvedRelations []ResolvedRelation
	for _, rel := range allRelations {
		headID := lookupEntityID(mentionToID, rel.HeadEntity, cfg)
		tailID := lookupEntityID(mentionToID, rel.TailEntity, cfg)
		if headID == "" || tailID == "" {
			continue
		}
		resolvedRelations = append(resolvedRelations, ResolvedRelation{
			HeadID: headID,
			TailID: tailID,
			Label:  rel.Label,
			Score:  rel.Score,
		})
	}

	// Deduplicate relations if configured.
	if cfg.DeduplicateRelations {
		resolvedRelations = deduplicateRelations(resolvedRelations)
	}

	return KnowledgeGraph{
		Entities:  resolved,
		Relations: resolvedRelations,
	}
}

// mentionKey is used to index entity mentions for resolution.
type mentionKey struct {
	text  string
	label string
}

// resolveEntities clusters entities by similarity and returns resolved entities
// plus a mapping from (text, label) -> resolved entity ID.
func resolveEntities(entities []Entity, cfg ResolverConfig) ([]ResolvedEntity, map[mentionKey]string) {
	if len(entities) == 0 {
		return nil, nil
	}

	// clusters[i] holds indices into entities that belong to cluster i.
	type cluster struct {
		indices []int
	}

	var clusters []cluster
	// entityToCluster maps entity index to cluster index.
	entityToCluster := make([]int, len(entities))
	for i := range entityToCluster {
		entityToCluster[i] = -1
	}

	for i, e := range entities {
		if entityToCluster[i] >= 0 {
			continue
		}

		// Start a new cluster with this entity.
		clusterIdx := len(clusters)
		clusters = append(clusters, cluster{indices: []int{i}})
		entityToCluster[i] = clusterIdx

		// Find all unassigned entities that match.
		for j := i + 1; j < len(entities); j++ {
			if entityToCluster[j] >= 0 {
				continue
			}
			if cfg.TypeMustMatch && e.Label != entities[j].Label {
				continue
			}
			sim := EntitySimilarity(normalizeText(e.Text), normalizeText(entities[j].Text))
			if sim >= cfg.SimilarityThreshold {
				clusters[clusterIdx].indices = append(clusters[clusterIdx].indices, j)
				entityToCluster[j] = clusterIdx
			}
		}
	}

	// Build resolved entities from clusters.
	resolved := make([]ResolvedEntity, 0, len(clusters))
	mentionMap := make(map[mentionKey]string)

	for i, c := range clusters {
		re := buildResolvedEntity(i, c.indices, entities, cfg)
		resolved = append(resolved, re)

		// Map each mention to this resolved entity ID.
		for _, idx := range c.indices {
			e := entities[idx]
			key := mentionKey{text: normalizeText(e.Text), label: e.Label}
			mentionMap[key] = re.ID
		}
	}

	return resolved, mentionMap
}

// buildResolvedEntity creates a ResolvedEntity from a cluster of entity indices.
func buildResolvedEntity(clusterIdx int, indices []int, entities []Entity, cfg ResolverConfig) ResolvedEntity {
	// Pick canonical name: prefer longest mention, break ties by highest score.
	bestIdx := indices[0]
	var maxScore float32
	seen := make(map[string]bool)
	var mentions []string

	for _, idx := range indices {
		e := entities[idx]
		if e.Score > maxScore {
			maxScore = e.Score
		}
		// Prefer longer canonical name, or higher score if same length.
		best := entities[bestIdx]
		if len(e.Text) > len(best.Text) || (len(e.Text) == len(best.Text) && e.Score > best.Score) {
			bestIdx = idx
		}
		normalized := normalizeText(e.Text)
		if !seen[normalized] {
			seen[normalized] = true
			mentions = append(mentions, e.Text)
		}
	}

	re := ResolvedEntity{
		ID:            fmt.Sprintf("entity-%d", clusterIdx),
		CanonicalName: entities[bestIdx].Text,
		Label:         entities[bestIdx].Label,
		Score:         maxScore,
	}

	if cfg.TrackProvenance {
		re.Mentions = mentions
	}

	return re
}

// lookupEntityID finds the resolved entity ID for a raw entity.
func lookupEntityID(mentionMap map[mentionKey]string, e Entity, cfg ResolverConfig) string {
	key := mentionKey{text: normalizeText(e.Text), label: e.Label}
	if id, ok := mentionMap[key]; ok {
		return id
	}
	// Fuzzy fallback: find closest match above threshold.
	var bestID string
	var bestSim float64
	for mk, id := range mentionMap {
		if cfg.TypeMustMatch && mk.label != e.Label {
			continue
		}
		sim := EntitySimilarity(normalizeText(e.Text), mk.text)
		if sim >= cfg.SimilarityThreshold && sim > bestSim {
			bestSim = sim
			bestID = id
		}
	}
	return bestID
}

// deduplicateRelations merges relations with the same head, tail, and label,
// keeping the highest score.
func deduplicateRelations(relations []ResolvedRelation) []ResolvedRelation {
	type relKey struct {
		headID string
		tailID string
		label  string
	}

	best := make(map[relKey]*ResolvedRelation)
	for i := range relations {
		r := &relations[i]
		key := relKey{headID: r.HeadID, tailID: r.TailID, label: r.Label}
		if existing, ok := best[key]; ok {
			if r.Score > existing.Score {
				existing.Score = r.Score
			}
		} else {
			cp := *r
			best[key] = &cp
		}
	}

	result := make([]ResolvedRelation, 0, len(best))
	for _, r := range best {
		result = append(result, *r)
	}

	// Sort for deterministic output: by head, tail, label.
	sort.Slice(result, func(i, j int) bool {
		if result[i].HeadID != result[j].HeadID {
			return result[i].HeadID < result[j].HeadID
		}
		if result[i].TailID != result[j].TailID {
			return result[i].TailID < result[j].TailID
		}
		return result[i].Label < result[j].Label
	})

	return result
}

// normalizeText lowercases and trims whitespace for comparison.
func normalizeText(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// EntitySimilarity computes similarity between two entity mentions for resolution.
// It combines Jaro-Winkler with containment checking: if one mention is a token
// substring of the other (e.g., "Musk" contained in "Elon Musk"), the similarity
// is boosted based on the overlap ratio.
func EntitySimilarity(a, b string) float64 {
	if a == b {
		return 1.0
	}
	if len(a) == 0 || len(b) == 0 {
		return 0.0
	}

	jw := JaroWinkler(a, b)

	// Check if one is a token substring of the other.
	shorter, longer := a, b
	if len(a) > len(b) {
		shorter, longer = b, a
	}

	// Token-level containment: check if all tokens of the shorter string
	// appear in the longer string.
	shortTokens := strings.Fields(shorter)
	longTokens := strings.Fields(longer)

	if len(shortTokens) > 0 && len(longTokens) > 0 {
		matched := 0
		for _, st := range shortTokens {
			if slices.Contains(longTokens, st) {
				matched++
			}
		}
		if matched == len(shortTokens) {
			// All tokens of shorter are in longer. Compute containment ratio.
			ratio := float64(len(shorter)) / float64(len(longer))
			containment := 0.7 + 0.3*ratio // range: 0.7 (short substring) to 1.0 (equal)
			if containment > jw {
				return containment
			}
		}
	}

	return jw
}

// JaroWinkler computes the Jaro-Winkler similarity between two strings (0.0-1.0).
func JaroWinkler(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	if len(s1) == 0 || len(s2) == 0 {
		return 0.0
	}

	jaro := jaroSimilarity(s1, s2)

	// Winkler modification: boost for common prefix (up to 4 chars).
	prefixLen := 0
	maxPrefix := min(len(s2), min(len(s1), 4))
	for i := range maxPrefix {
		if s1[i] == s2[i] {
			prefixLen++
		} else {
			break
		}
	}

	const winklerScale = 0.1
	return jaro + float64(prefixLen)*winklerScale*(1.0-jaro)
}

func jaroSimilarity(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}

	len1 := len(s1)
	len2 := len(s2)

	// Maximum matching distance.
	maxDist := max(int(math.Floor(float64(max(len1, len2))/2.0))-1, 0)

	s1Matches := make([]bool, len1)
	s2Matches := make([]bool, len2)

	matches := 0
	transpositions := 0

	// Find matching characters.
	for i := range len1 {
		start := max(0, i-maxDist)
		end := min(len2, i+maxDist+1)

		for j := start; j < end; j++ {
			if s2Matches[j] || s1[i] != s2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return 0.0
	}

	// Count transpositions.
	k := 0
	for i := range len1 {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}

	m := float64(matches)
	return (m/float64(len1) + m/float64(len2) + (m-float64(transpositions)/2.0)/m) / 3.0
}
