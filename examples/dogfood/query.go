// Copyright 2026 Antfly, Inc.
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

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/antflydb/antfly/go/pkg/antflylite"
)

// searchHitHierarchy is the ancestry envelope QueryHit carries for a chunk or
// embedding artifact hit (specs/openapi/antfly/metadata.yaml QueryHit /
// QueryHitHierarchy). ParentDocKey lets dogfood project a semantic hit over
// the chunk_vectors artifact back to the source document.
type searchHitHierarchy struct {
	Level           string `json:"level"`
	ParentDocKey    string `json:"parent_doc_key"`
	Artifact        string `json:"artifact"`
	MatchedArtifact string `json:"matched_artifact"`
}

type searchHit struct {
	ID        string              `json:"_id"`
	Score     float64             `json:"_score"`
	Source    map[string]any      `json:"_source"`
	Hierarchy *searchHitHierarchy `json:"hierarchy"`
}

type searchHits struct {
	Hits []searchHit `json:"hits"`
}

// searchResponse is one entry of the "responses" envelope antfly_db_search_json
// returns for the public query JSON path (confirmed empirically: even a single
// query comes back as {"responses":[{"hits":{...}, ...}]}, matching the
// network QueryResponses shape).
type searchResponse struct {
	Hits searchHits `json:"hits"`
}

type searchEnvelope struct {
	Responses []searchResponse `json:"responses"`
}

// traverseEdgesRequest mirrors the internal Request struct parsed by
// antfly_db_traverse_edges_json (zig/pkg/antfly/src/capi/db.zig). Direction:
// 0=out, 1=in, 2=both.
type traverseEdgesRequest struct {
	IndexName        string   `json:"index_name"`
	StartKeyB64      string   `json:"start_key_b64"`
	EdgeTypes        []string `json:"edge_types,omitempty"`
	Direction        uint8    `json:"direction"`
	MaxDepth         uint32   `json:"max_depth"`
	MaxResults       uint32   `json:"max_results"`
	DeduplicateNodes bool     `json:"deduplicate_nodes"`
	IncludePaths     bool     `json:"include_paths"`
}

type traversalResult struct {
	KeyB64      string  `json:"key_b64"`
	Depth       uint32  `json:"depth"`
	TotalWeight float64 `json:"total_weight"`
}

type graphEdge struct {
	SourceB64    string  `json:"source_b64"`
	TargetB64    string  `json:"target_b64"`
	EdgeType     string  `json:"edge_type"`
	Weight       float64 `json:"weight"`
	MetadataJSON string  `json:"metadata_json"`
}

const (
	edgeDirectionOut  uint8 = 0
	edgeDirectionIn   uint8 = 1
	edgeDirectionBoth uint8 = 2
)

// runQuery executes dogfood's hybrid retrieval: full-text search over the
// default full-text index merged (RRF) with semantic search over
// chunk_vectors, then a depth-2 both-direction traversal of the knowledge
// graph starting from the resolved source documents of the top hits. If the
// hybrid request fails (for example, chunk_vectors has no indexed vectors yet
// because ingest has not finished draining, or the local libantfly build's
// enrichment worker never started for a remote-provider handle -- see the
// dogfood README's "Known limitations" section), runQuery falls back to
// full-text-only search rather than failing outright.
func runQuery(db *antflylite.DB, text string, limit int) error {
	// Antfly embeds the semantic_search text itself through the index's
	// configured embedder (the handle's in-process runtime, or the index's
	// api_url provider), exactly like the server does. The match form scores
	// every query term (BM25 OR semantics), which a natural-language question
	// needs; the "query" string form requires every term to match.
	request := map[string]any{
		"full_text_search": map[string]any{"match": map[string]any{"field": "body", "text": text}},
		"full_text_index":  fullTextIndexName,
		"semantic_search":  text,
		"indexes":          []string{chunkVectorsIndex},
		"merge_config":     map[string]any{"strategy": "rrf"},
		"limit":            limit,
		"fields":           []string{"title", "body", "source", "kind"},
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal search request: %w", err)
	}

	raw, err := db.SearchJSON(body)
	if err != nil {
		// Older libantfly builds reject semantic_search on Lite handles; keep
		// the demo useful with full-text plus graph traversal.
		fmt.Printf("(hybrid search unavailable: %v; falling back to full-text only)\n", err)
		delete(request, "semantic_search")
		delete(request, "indexes")
		delete(request, "merge_config")
		fallbackBody, ferr := json.Marshal(request)
		if ferr != nil {
			return fmt.Errorf("marshal fallback search request: %w", ferr)
		}
		raw, err = db.SearchJSON(fallbackBody)
		if err != nil {
			return fmt.Errorf("search: %w", err)
		}
	}
	var envelope searchEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode search results: %w\nraw: %s", err, raw)
	}
	var results searchHits
	if len(envelope.Responses) > 0 {
		results = envelope.Responses[0].Hits
	}

	fmt.Printf("Query: %q (%d hits)\n\n", text, len(results.Hits))

	resolvedKeys := make([]string, 0, len(results.Hits))
	seen := make(map[string]bool)
	for i, hit := range results.Hits {
		docKey := hit.ID
		note := ""
		if hit.Hierarchy != nil && hit.Hierarchy.ParentDocKey != "" {
			docKey = hit.Hierarchy.ParentDocKey
			note = fmt.Sprintf(" (via chunk artifact %s)", hit.Hierarchy.Artifact)
		}
		title, _ := hit.Source["title"].(string)
		source, _ := hit.Source["source"].(string)
		fmt.Printf("%2d. [%.4f] %s -- %s%s\n", i+1, hit.Score, docKey, firstNonEmpty(title, source), note)

		if !seen[docKey] {
			seen[docKey] = true
			resolvedKeys = append(resolvedKeys, docKey)
		}
	}

	const maxTraversalSeeds = 5
	seeds := resolvedKeys
	if len(seeds) > maxTraversalSeeds {
		seeds = seeds[:maxTraversalSeeds]
	}

	nodeKeys := make(map[string]bool)
	for _, key := range seeds {
		nodeKeys[key] = true
		reached, err := traverseKnowledgeGraph(db, key, 2)
		if err != nil {
			fmt.Printf("\n(graph traversal from %s skipped: %v)\n", key, err)
			continue
		}
		for _, key := range reached {
			nodeKeys[key] = true
		}
	}

	entities, edges, failures := collectGraphNeighborhood(db, nodeKeys)

	fmt.Printf("\nEntities reached (%d):\n", len(entities))
	for _, entity := range entities {
		fmt.Printf("  - %s\n", entity)
	}

	fmt.Printf("\nEdges (%d):\n", len(edges))
	for _, edge := range edges {
		fmt.Printf("  %s --[%s]--> %s (weight=%.3f)\n", edge.source, edge.edgeType, edge.target, edge.weight)
	}
	if failures.reads > 0 || failures.decodes > 0 {
		fmt.Printf("\n(warning: %d edge reads and %d result decodes failed while collecting the neighborhood; last error: %v)\n",
			failures.reads, failures.decodes, failures.last)
	}
	if failures.truncated {
		fmt.Printf("(neighborhood truncated at %d edges; narrow the query or raise maxNeighborhoodEdges to see more)\n", maxNeighborhoodEdges)
	}
	return nil
}

// traverseKnowledgeGraph runs a both-direction breadth-first traversal from
// startKey over the knowledge graph index and returns the reached node keys
// (decoded from base64), including nodes at every depth up to maxDepth.
func traverseKnowledgeGraph(db *antflylite.DB, startKey string, maxDepth uint32) ([]string, error) {
	req := traverseEdgesRequest{
		IndexName:        knowledgeGraphIndex,
		StartKeyB64:      base64.StdEncoding.EncodeToString([]byte(startKey)),
		Direction:        edgeDirectionBoth,
		MaxDepth:         maxDepth,
		MaxResults:       200,
		DeduplicateNodes: true,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	raw, err := db.TraverseEdgesJSON(body)
	if err != nil {
		return nil, err
	}
	var results []traversalResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("decode traversal results: %w", err)
	}
	keys := make([]string, 0, len(results))
	for _, result := range results {
		decoded, err := base64.StdEncoding.DecodeString(result.KeyB64)
		if err != nil {
			continue
		}
		keys = append(keys, string(decoded))
	}
	return keys, nil
}

type resolvedEdge struct {
	source, target, edgeType string
	weight                   float64
}

// maxNeighborhoodEdges bounds the total edge output of
// collectGraphNeighborhood, so one high-degree node cannot expand the printed
// neighborhood far beyond the traversal's own caps.
const maxNeighborhoodEdges = 200

// neighborhoodFailures reports what went wrong (and what was withheld) while
// collecting the neighborhood, so a broken graph looks broken rather than
// empty.
type neighborhoodFailures struct {
	reads     int
	decodes   int
	last      error
	truncated bool
}

// collectGraphNeighborhood fetches the direct edges (both directions) of
// every node in nodeKeys and returns the deduplicated entity node set
// (non-"doc:" keys) and the deduplicated edge set. Only edges with at least
// one endpoint inside the traversal-reached node set are admitted (a direct
// edge of a reached node always qualifies through that node itself; its far
// endpoint is listed but never expanded), the total output is capped at
// maxNeighborhoodEdges, and read/decode failures are counted instead of
// silently swallowed. Iteration is sorted so output and truncation are
// deterministic.
func collectGraphNeighborhood(db *antflylite.DB, nodeKeys map[string]bool) ([]string, []resolvedEdge, neighborhoodFailures) {
	entitySet := make(map[string]bool)
	edgeSeen := make(map[string]bool)
	var edges []resolvedEdge
	var failures neighborhoodFailures

	orderedKeys := make([]string, 0, len(nodeKeys))
	for key := range nodeKeys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)

collect:
	for _, key := range orderedKeys {
		raw, err := db.EdgesJSON(knowledgeGraphIndex, key, "", edgeDirectionBoth)
		if err != nil {
			failures.reads++
			failures.last = err
			continue
		}
		var rawEdges []graphEdge
		if err := json.Unmarshal(raw, &rawEdges); err != nil {
			failures.decodes++
			failures.last = err
			continue
		}
		for _, edge := range rawEdges {
			source, err1 := base64.StdEncoding.DecodeString(edge.SourceB64)
			target, err2 := base64.StdEncoding.DecodeString(edge.TargetB64)
			if err1 != nil || err2 != nil {
				failures.decodes++
				if err1 != nil {
					failures.last = err1
				} else {
					failures.last = err2
				}
				continue
			}
			sourceKey, targetKey := string(source), string(target)
			dedupeKey := sourceKey + "\x00" + edge.EdgeType + "\x00" + targetKey
			if edgeSeen[dedupeKey] {
				continue
			}
			if len(edges) >= maxNeighborhoodEdges {
				failures.truncated = true
				break collect
			}
			edgeSeen[dedupeKey] = true
			edges = append(edges, resolvedEdge{source: sourceKey, target: targetKey, edgeType: edge.EdgeType, weight: edge.Weight})
			if !isDocumentKey(sourceKey) {
				entitySet[sourceKey] = true
			}
			if !isDocumentKey(targetKey) {
				entitySet[targetKey] = true
			}
		}
	}

	entities := make([]string, 0, len(entitySet))
	for entity := range entitySet {
		entities = append(entities, entity)
	}
	sort.Strings(entities)
	return entities, edges, failures
}

func isDocumentKey(key string) bool {
	return len(key) >= 4 && key[:4] == "doc:"
}

// runEntity prints the direct (both-direction) edges of a named entity node
// in the knowledge graph. Entity nodes live under the resolver's canonical
// `label/slug` keys (e.g. component/metadata_server), so a bare name like
// "metadata server" is slugified and probed under every extraction label; an
// exact key is used as-is.
func runEntity(db *antflylite.DB, name string) error {
	candidates := entityKeyCandidates(name)
	for _, key := range candidates {
		edges, err := entityEdges(db, key)
		if err != nil {
			return fmt.Errorf("get edges for %q: %w", key, err)
		}
		if len(edges) == 0 {
			continue
		}
		fmt.Printf("Entity: %s (%d edges)\n\n", key, len(edges))
		for _, edge := range edges {
			source, _ := base64.StdEncoding.DecodeString(edge.SourceB64)
			target, _ := base64.StdEncoding.DecodeString(edge.TargetB64)
			fmt.Printf("  %s --[%s]--> %s (weight=%.3f)\n", source, edge.EdgeType, target, edge.Weight)
			if edge.MetadataJSON != "" && edge.MetadataJSON != "{}" {
				fmt.Printf("      metadata: %s\n", edge.MetadataJSON)
			}
		}
		return nil
	}
	fmt.Printf("Entity %q: no edges found (tried %s)\n", name, strings.Join(candidates, ", "))
	return nil
}

func entityEdges(db *antflylite.DB, key string) ([]graphEdge, error) {
	raw, err := db.EdgesJSON(knowledgeGraphIndex, key, "", edgeDirectionBoth)
	if err != nil {
		return nil, err
	}
	var edges []graphEdge
	if err := json.Unmarshal(raw, &edges); err != nil {
		return nil, fmt.Errorf("decode edges: %w\nraw: %s", err, raw)
	}
	return edges, nil
}

// entityKeyCandidates expands a user-supplied entity name into the canonical
// keys it may live under. A name already containing '/' is treated as an
// exact canonical key; otherwise the name is slugified with the same rules as
// the resolver's `slug` template helper (lowercased alphanumeric runs joined
// by '_', possessives stripped) and probed under the label-free `entity/`
// namespace the resolver mints.
func entityKeyCandidates(name string) []string {
	if strings.Contains(name, "/") {
		return []string{name}
	}
	return []string{"entity/" + slugify(name)}
}

// slugify mirrors zig/lib/resolver's `slug` template helper byte-for-byte:
// ASCII-lowercased alphanumeric runs separated by single '_', no
// leading/trailing separators, English possessives stripped
// ("A. Lovelace" -> "a_lovelace", "Epstein's island" -> "epstein_island").
func slugify(value string) string {
	var b strings.Builder
	pendingSep := false
	wrote := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		apostropheLen := 0
		if c == '\'' {
			apostropheLen = 1
		} else if c == 0xE2 && i+2 < len(value) && value[i+1] == 0x80 && value[i+2] == 0x99 {
			apostropheLen = 3
		}
		if apostropheLen > 0 && wrote && !pendingSep {
			sIndex := i + apostropheLen
			afterS := sIndex + 1
			isAlnum := func(b byte) bool {
				return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
			}
			if sIndex < len(value) && (value[sIndex] == 's' || value[sIndex] == 'S') &&
				(afterS >= len(value) || !isAlnum(value[afterS])) {
				i = sIndex
				continue
			}
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			if pendingSep && wrote {
				b.WriteByte('_')
			}
			b.WriteByte(c)
			wrote = true
			pendingSep = false
		} else {
			pendingSep = true
		}
		if apostropheLen == 3 {
			i += 2
		}
	}
	return b.String()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
