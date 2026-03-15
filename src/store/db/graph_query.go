// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package db

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2"
	"go.uber.org/zap"
)

// GraphQueryEngine executes declarative graph queries
type GraphQueryEngine struct {
	db     *DBImpl
	logger *zap.Logger
}

// NewGraphQueryEngine creates a new graph query engine
func NewGraphQueryEngine(db *DBImpl, logger *zap.Logger) *GraphQueryEngine {
	return &GraphQueryEngine{
		db:     db,
		logger: logger,
	}
}

// Execute runs a graph query and returns results with status
func (gqe *GraphQueryEngine) Execute(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, *indexes.SearchComponentStatus, error) {
	start := time.Now()
	status := &indexes.SearchComponentStatus{Success: false}

	// Early validation
	if len(startNodes) == 0 {
		status.Error = "no start nodes provided"
		return nil, status, fmt.Errorf("no start nodes provided")
	}

	var result *indexes.GraphQueryResult
	var err error

	switch query.Type {
	case "traverse":
		result, err = gqe.executeTraverse(ctx, query, startNodes)
	case "neighbors":
		result, err = gqe.executeNeighbors(ctx, query, startNodes)
	case "shortest_path":
		result, err = gqe.executeShortestPath(ctx, query, startNodes)
	case "k_shortest_paths":
		result, err = gqe.executeKShortestPaths(ctx, query, startNodes)
	default:
		status.Error = fmt.Sprintf("unknown graph query type: %s", query.Type)
		return nil, status, fmt.Errorf("unknown graph query type: %s", query.Type)
	}

	if err != nil {
		status.Error = err.Error()
		return nil, status, err
	}

	status.Success = true
	result.Took = time.Since(start)
	return result, status, nil
}

// executeTraverse consolidates TraverseEdges logic
func (gqe *GraphQueryEngine) executeTraverse(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	// Convert GraphQueryParams to TraversalRules
	rules := indexes.TraversalRules{
		EdgeTypes:        query.Params.EdgeTypes,
		Direction:        parseDirection(query.Params.Direction),
		MaxDepth:         query.Params.MaxDepth,
		MinWeight:        query.Params.MinWeight,
		MaxWeight:        query.Params.MaxWeight,
		MaxResults:       query.Params.MaxResults,
		DeduplicateNodes: query.Params.DeduplicateNodes,
		IncludePaths:     query.Params.IncludePaths,
	}

	// Execute traversal from all start nodes
	allResults := make([]*indexes.TraversalResult, 0)
	for _, startKey := range startNodes {
		results, err := gqe.db.TraverseEdges(ctx, query.IndexName, startKey, rules)
		if err != nil {
			gqe.logger.Warn("Failed to traverse from node",
				zap.String("key", types.FormatKey(startKey)),
				zap.Error(err))
			continue
		}
		allResults = append(allResults, results...)
	}

	// Convert to GraphResultNode format
	return gqe.convertTraversalToGraphResult(ctx, query, allResults)
}

// executeNeighbors gets immediate neighbors of nodes
func (gqe *GraphQueryEngine) executeNeighbors(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	allResults := make([]*indexes.TraversalResult, 0)

	for _, startKey := range startNodes {
		edgeType := ""
		if len(query.Params.EdgeTypes) > 0 {
			edgeType = query.Params.EdgeTypes[0]
		}

		results, err := gqe.db.GetNeighbors(
			ctx,
			query.IndexName,
			startKey,
			edgeType,
			parseDirection(query.Params.Direction),
		)
		if err != nil {
			gqe.logger.Warn("Failed to get neighbors for node",
				zap.String("key", types.FormatKey(startKey)),
				zap.Error(err))
			continue
		}
		allResults = append(allResults, results...)
	}

	return gqe.convertTraversalToGraphResult(ctx, query, allResults)
}

// executeShortestPath finds shortest path between nodes
func (gqe *GraphQueryEngine) executeShortestPath(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	if query.TargetNodes.Keys == nil && query.TargetNodes.ResultRef == "" {
		return nil, fmt.Errorf("shortest_path requires target_nodes")
	}

	targetNodes, err := gqe.resolveNodeSelector(ctx, &query.TargetNodes, nil)
	if err != nil {
		return nil, fmt.Errorf("resolving target nodes: %w", err)
	}

	if len(targetNodes) == 0 {
		return nil, fmt.Errorf("no target nodes resolved")
	}

	paths := make([]*indexes.Path, 0)

	// Find paths from each start node to each target node
	for _, startKey := range startNodes {
		for _, targetKey := range targetNodes {
			path, err := gqe.db.FindShortestPath(
				ctx,
				query.IndexName,
				startKey,
				targetKey,
				query.Params.EdgeTypes,
				parseDirection(query.Params.Direction),
				query.Params.WeightMode,
				query.Params.MaxDepth,
				query.Params.MinWeight,
				query.Params.MaxWeight,
			)
			if err != nil {
				gqe.logger.Warn("Failed to find path",
					zap.ByteString("source", startKey),
					zap.ByteString("target", targetKey),
					zap.Error(err))
				continue
			}
			if path != nil {
				paths = append(paths, path)
			}
		}
	}

	// Convert []*Path to []Path
	pathValues := make([]indexes.Path, len(paths))
	for i, p := range paths {
		if p != nil {
			pathValues[i] = *p
		}
	}

	return &indexes.GraphQueryResult{
		Type:  "shortest_path",
		Paths: pathValues,
		Total: len(paths),
	}, nil
}

// executeKShortestPaths finds k shortest paths using Yen's algorithm
func (gqe *GraphQueryEngine) executeKShortestPaths(
	ctx context.Context,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	if query.TargetNodes.Keys == nil && query.TargetNodes.ResultRef == "" {
		return nil, fmt.Errorf("k_shortest_paths requires target_nodes")
	}

	targetNodes, err := gqe.resolveNodeSelector(ctx, &query.TargetNodes, nil)
	if err != nil {
		return nil, fmt.Errorf("resolving target nodes: %w", err)
	}

	if len(targetNodes) == 0 {
		return nil, fmt.Errorf("no target nodes resolved")
	}

	k := query.Params.K
	if k <= 0 {
		k = 3 // default to 3 paths
	}

	direction := parseDirection(query.Params.Direction)
	weightMode := query.Params.WeightMode
	if weightMode == "" {
		weightMode = "min_hops"
	}
	maxDepth := query.Params.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 50
	}

	allPaths := make([]indexes.Path, 0)

	// Find k-shortest paths for each source-target pair
	for _, source := range startNodes {
		for _, target := range targetNodes {
			paths, err := gqe.yenKShortestPaths(
				ctx,
				query.IndexName,
				source,
				target,
				k,
				query.Params.EdgeTypes,
				direction,
				weightMode,
				maxDepth,
				query.Params.MinWeight,
				query.Params.MaxWeight,
			)
			if err != nil {
				gqe.logger.Warn("Failed to find k-shortest paths",
					zap.ByteString("source", source),
					zap.ByteString("target", target),
					zap.Error(err))
				continue
			}
			allPaths = append(allPaths, paths...)
		}
	}

	return &indexes.GraphQueryResult{
		Type:  "k_shortest_paths",
		Paths: allPaths,
		Total: len(allPaths),
	}, nil
}

// yenKShortestPaths implements Yen's algorithm for finding k shortest loopless paths
func (gqe *GraphQueryEngine) yenKShortestPaths(
	ctx context.Context,
	indexName string,
	source, target []byte,
	k int,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	weightMode indexes.PathWeightMode,
	maxDepth int,
	minWeight, maxWeight float64,
) ([]indexes.Path, error) {
	// A stores the k shortest paths found
	A := make([]indexes.Path, 0, k)

	// Find the first shortest path
	firstPath, err := gqe.db.FindShortestPath(
		ctx, indexName, source, target,
		edgeTypes, direction, weightMode, maxDepth,
		minWeight, maxWeight,
	)
	if err != nil {
		return nil, err
	}
	if firstPath == nil {
		return nil, fmt.Errorf("no path found between nodes")
	}
	A = append(A, *firstPath)

	if k == 1 {
		return A, nil
	}

	// B is a list of candidate paths (we'll sort by weight)
	type candidatePath struct {
		path   indexes.Path
		weight float64
	}
	B := make([]candidatePath, 0)

	// Set to track seen paths (to avoid duplicates)
	seenPaths := make(map[string]bool)
	seenPaths[PathToKey(firstPath)] = true

	// Find remaining k-1 paths
	for i := 1; i < k; i++ {
		// Get the last found path
		prevPath := A[len(A)-1]

		// For each node in the previous path (except the last), try deviating
		for spurIdx := 0; spurIdx < len(prevPath.Nodes)-1; spurIdx++ {
			spurNodeB64 := prevPath.Nodes[spurIdx]
			spurNode, decErr := base64.StdEncoding.DecodeString(spurNodeB64)
			if decErr != nil {
				continue
			}

			// Root path: source to spur node
			rootPath := indexes.Path{
				Nodes:       prevPath.Nodes[:spurIdx+1],
				Edges:       make([]indexes.PathEdge, 0),
				TotalWeight: 0,
				Length:      spurIdx,
			}
			if spurIdx > 0 {
				rootPath.Edges = prevPath.Edges[:spurIdx]
				for _, e := range rootPath.Edges {
					rootPath.TotalWeight += e.Weight
				}
			}

			// Build exclusion sets
			excludedEdges := make(map[string]bool)
			for _, path := range A {
				if len(path.Nodes) > spurIdx && path.Nodes[spurIdx] == spurNodeB64 {
					// This path goes through spur node at same position
					if spurIdx < len(path.Edges) {
						edgeKey := path.Edges[spurIdx].Source + "->" + path.Edges[spurIdx].Target + ":" + path.Edges[spurIdx].Type
						excludedEdges[edgeKey] = true
					}
				}
			}

			// Exclude root path nodes (except spur) to avoid loops
			excludedNodes := make(map[string]bool)
			for j := 0; j < spurIdx; j++ {
				excludedNodes[prevPath.Nodes[j]] = true
			}

			// Find spur path: from spur node to target
			spurPath, err := gqe.db.FindShortestPathWithExclusions(
				ctx, indexName, spurNode, target,
				edgeTypes, direction, weightMode, maxDepth-spurIdx,
				minWeight, maxWeight,
				excludedEdges, excludedNodes,
			)
			if err != nil || spurPath == nil {
				continue
			}

			// Combine root + spur path
			totalPath := indexes.Path{
				Nodes:       make([]string, 0, len(rootPath.Nodes)+len(spurPath.Nodes)-1),
				Edges:       make([]indexes.PathEdge, 0, len(rootPath.Edges)+len(spurPath.Edges)),
				TotalWeight: rootPath.TotalWeight + spurPath.TotalWeight,
				Length:      rootPath.Length + spurPath.Length,
			}

			totalPath.Nodes = append(totalPath.Nodes, rootPath.Nodes...)
			totalPath.Edges = append(totalPath.Edges, rootPath.Edges...)

			// Skip first spur node (already in root)
			if len(spurPath.Nodes) > 1 {
				totalPath.Nodes = append(totalPath.Nodes, spurPath.Nodes[1:]...)
			}
			totalPath.Edges = append(totalPath.Edges, spurPath.Edges...)

			// Check for duplicates
			pathKey := PathToKey(&totalPath)
			if seenPaths[pathKey] {
				continue
			}
			seenPaths[pathKey] = true

			// Calculate weight for priority
			var weight float64
			switch weightMode {
			case "max_weight":
				weight = -totalPath.TotalWeight // negate for min-heap behavior
			case "min_hops":
				weight = float64(totalPath.Length)
			default: // min_weight
				weight = totalPath.TotalWeight
			}

			B = append(B, candidatePath{path: totalPath, weight: weight})
		}

		if len(B) == 0 {
			break // No more candidates
		}

		// Sort candidates by weight and pick the best
		slices.SortFunc(B, func(a, b candidatePath) int {
			if a.weight < b.weight {
				return -1
			}
			if a.weight > b.weight {
				return 1
			}
			return 0
		})

		A = append(A, B[0].path)
		B = B[1:]
	}

	return A, nil
}

// convertTraversalToGraphResult converts TraversalResult to GraphResultNode format
func (gqe *GraphQueryEngine) convertTraversalToGraphResult(
	ctx context.Context,
	query *indexes.GraphQuery,
	results []*indexes.TraversalResult,
) (*indexes.GraphQueryResult, error) {
	nodes := make([]*indexes.GraphResultNode, 0, len(results))

	for _, result := range results {
		node := &indexes.GraphResultNode{
			Key:      base64.StdEncoding.EncodeToString(result.Key),
			Depth:    result.Depth,
			Distance: result.TotalWeight,
			Document: result.Document,
		}

		// Convert path keys to base64 strings
		if len(result.Path) > 0 {
			node.Path = make([]string, len(result.Path))
			for i, pathKey := range result.Path {
				node.Path[i] = base64.StdEncoding.EncodeToString(pathKey)
			}
		}

		// Convert edges to PathEdge format
		if len(result.PathEdges) > 0 {
			node.PathEdges = make([]indexes.PathEdge, len(result.PathEdges))
			for i, edge := range result.PathEdges {
				node.PathEdges[i] = indexes.PathEdge{
					Source:   base64.StdEncoding.EncodeToString(edge.Source),
					Target:   base64.StdEncoding.EncodeToString(edge.Target),
					Type:     edge.Type,
					Weight:   edge.Weight,
					Metadata: edge.Metadata,
				}
			}
		}

		// Fetch full document if requested
		if query.IncludeDocuments && node.Document == nil {
			doc, err := gqe.db.Get(ctx, result.Key)
			if err != nil {
				gqe.logger.Warn("Failed to fetch document for node",
					zap.String("key", types.FormatKey(result.Key)),
					zap.Error(err))
			} else {
				node.Document = doc
			}
		}

		// Fetch edges if requested
		if query.IncludeEdges {
			edges, err := gqe.db.GetEdges(ctx, query.IndexName, result.Key, "", parseDirection(query.Params.Direction))
			if err != nil {
				gqe.logger.Warn("Failed to fetch edges for node",
					zap.String("key", types.FormatKey(result.Key)),
					zap.Error(err))
			} else {
				node.Edges = edges
			}
		}

		// Apply field projection if specified
		if len(query.Fields) > 0 && node.Document != nil {
			node.Document = ProjectFields(node.Document, query.Fields)
		}

		nodes = append(nodes, node)
	}

	// Convert []*GraphResultNode to []GraphResultNode
	nodeValues := make([]indexes.GraphResultNode, len(nodes))
	for i, n := range nodes {
		if n != nil {
			nodeValues[i] = *n
		}
	}

	return &indexes.GraphQueryResult{
		Type:  query.Type,
		Nodes: nodeValues,
		Total: len(nodes),
	}, nil
}

// resolveNodeSelector converts selector to concrete node keys
func (gqe *GraphQueryEngine) resolveNodeSelector(
	ctx context.Context,
	selector *indexes.GraphNodeSelector,
	searchResult *indexes.RemoteIndexSearchResult,
) ([][]byte, error) {
	// Explicit keys
	if len(selector.Keys) > 0 {
		keys := make([][]byte, 0, len(selector.Keys))
		for _, keyStr := range selector.Keys {
			key, err := base64.StdEncoding.DecodeString(keyStr)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 key: %w", err)
			}
			keys = append(keys, key)
		}
		return keys, nil
	}

	// Result reference
	if selector.ResultRef != "" {
		return gqe.resolveResultRef(ctx, selector.ResultRef, 0, searchResult) // 0 = no limit
	}

	return nil, fmt.Errorf("node selector must specify keys or result_ref")
}

// resolveResultRef extracts keys from previous query results
func (gqe *GraphQueryEngine) resolveResultRef(
	ctx context.Context,
	ref string,
	limit int,
	searchResult *indexes.RemoteIndexSearchResult,
) ([][]byte, error) {
	if searchResult == nil {
		return nil, fmt.Errorf("cannot resolve result reference without search results")
	}

	switch {
	case ref == "$full_text_results":
		if searchResult.BleveSearchResult == nil {
			return nil, fmt.Errorf("no full-text results available")
		}
		return extractKeysFromBleve(searchResult.BleveSearchResult, limit), nil

	case strings.HasPrefix(ref, "$aknn_results."):
		indexName := strings.TrimPrefix(ref, "$aknn_results.")
		if searchResult.VectorSearchResult == nil {
			return nil, fmt.Errorf("no vector results available")
		}
		vectorResult := searchResult.VectorSearchResult[indexName]
		if vectorResult == nil {
			return nil, fmt.Errorf("vector index not found: %s", indexName)
		}
		return extractKeysFromVector(vectorResult, limit), nil

	case strings.HasPrefix(ref, "$graph_results."):
		queryName := strings.TrimPrefix(ref, "$graph_results.")
		if searchResult.GraphResults == nil {
			return nil, fmt.Errorf("no graph results available")
		}
		graphResult := searchResult.GraphResults[queryName]
		if graphResult == nil {
			return nil, fmt.Errorf("graph query not found: %s", queryName)
		}
		return extractKeysFromGraph(graphResult, limit), nil

	default:
		return nil, fmt.Errorf("unknown result reference: %s", ref)
	}
}

// extractKeysFromHitIDs decodes base64-encoded hit IDs into raw byte keys
func extractKeysFromHitIDs(ids []string, limit int) [][]byte {
	if len(ids) == 0 {
		return nil
	}

	maxHits := len(ids)
	if limit > 0 && limit < maxHits {
		maxHits = limit
	}

	keys := make([][]byte, 0, maxHits)
	for i := 0; i < maxHits; i++ {
		key, err := base64.StdEncoding.DecodeString(ids[i])
		if err != nil {
			key = []byte(ids[i])
		}
		keys = append(keys, key)
	}
	return keys
}

// extractKeysFromBleve extracts document keys from Bleve search results
func extractKeysFromBleve(result *bleve.SearchResult, limit int) [][]byte {
	if result == nil || len(result.Hits) == 0 {
		return nil
	}
	ids := make([]string, len(result.Hits))
	for i, hit := range result.Hits {
		ids[i] = hit.ID
	}
	return extractKeysFromHitIDs(ids, limit)
}

// extractKeysFromVector extracts document keys from vector search results
func extractKeysFromVector(result *vectorindex.SearchResult, limit int) [][]byte {
	if result == nil || len(result.Hits) == 0 {
		return nil
	}
	ids := make([]string, len(result.Hits))
	for i, hit := range result.Hits {
		ids[i] = hit.ID
	}
	return extractKeysFromHitIDs(ids, limit)
}

// extractKeysFromGraph extracts document keys from graph query results
func extractKeysFromGraph(result *indexes.GraphQueryResult, limit int) [][]byte {
	if result == nil {
		return nil
	}

	// Prefer nodes over paths
	if len(result.Nodes) > 0 {
		maxNodes := len(result.Nodes)
		if limit > 0 && limit < maxNodes {
			maxNodes = limit
		}

		keys := make([][]byte, 0, maxNodes)
		for i := 0; i < maxNodes; i++ {
			node := result.Nodes[i]
			key, err := base64.StdEncoding.DecodeString(node.Key)
			if err != nil {
				continue
			}
			keys = append(keys, key)
		}
		return keys
	}

	// Fall back to path nodes
	if len(result.Paths) > 0 {
		keys := make([][]byte, 0)
		for _, path := range result.Paths {
			for _, nodeKey := range path.Nodes {
				key, err := base64.StdEncoding.DecodeString(nodeKey)
				if err != nil {
					continue
				}
				keys = append(keys, key)
				if limit > 0 && len(keys) >= limit {
					return keys
				}
			}
		}
		return keys
	}

	return nil
}

// parseDirection converts EdgeDirection type to EdgeDirection constant
func parseDirection(direction indexes.EdgeDirection) indexes.EdgeDirection {
	switch direction {
	case "out":
		return indexes.EdgeDirectionOut
	case "in":
		return indexes.EdgeDirectionIn
	case "both":
		return indexes.EdgeDirectionBoth
	default:
		return indexes.EdgeDirectionOut // Default to outgoing
	}
}

// SortGraphQueriesByDependencies performs topological sort on graph queries
// Returns queries in dependency order (queries that don't depend on others come first)
func SortGraphQueriesByDependencies(queries map[string]*indexes.GraphQuery) ([]string, error) {
	// Build dependency graph
	deps := make(map[string][]string)   // query -> queries it depends on
	inDegree := make(map[string]int)    // query -> number of dependencies
	allQueries := make(map[string]bool) // all query names

	// Initialize
	for name := range queries {
		allQueries[name] = true
		inDegree[name] = 0
		deps[name] = []string{}
	}

	// Extract dependencies from ResultRef fields
	for name, query := range queries {
		// Check start_nodes dependencies
		if dep := extractGraphDependency(query.StartNodes.ResultRef); dep != "" {
			if _, exists := allQueries[dep]; exists {
				deps[name] = append(deps[name], dep)
				inDegree[name]++
			}
		}

		// Check target_nodes dependencies (for shortest_path queries)
		if dep := extractGraphDependency(query.TargetNodes.ResultRef); dep != "" {
			if _, exists := allQueries[dep]; exists {
				deps[name] = append(deps[name], dep)
				inDegree[name]++
			}
		}
	}

	// Kahn's algorithm for topological sort
	queue := make([]string, 0)

	// Start with queries that have no dependencies
	for name := range allQueries {
		if inDegree[name] == 0 {
			queue = append(queue, name)
		}
	}

	sorted := make([]string, 0, len(allQueries))

	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, current)

		// Update neighbors (queries that depend on current)
		for name, queryDeps := range deps {
			for _, dep := range queryDeps {
				if dep == current {
					inDegree[name]--
					if inDegree[name] == 0 {
						queue = append(queue, name)
					}
				}
			}
		}
	}

	// Check for circular dependencies
	if len(sorted) != len(allQueries) {
		remaining := make([]string, 0)
		for name := range allQueries {
			found := slices.Contains(sorted, name)
			if !found {
				remaining = append(remaining, name)
			}
		}
		return nil, fmt.Errorf("circular dependency detected among queries: %v", remaining)
	}

	return sorted, nil
}

// extractGraphDependency extracts query name from $graph_results.query_name references
func extractGraphDependency(ref string) string {
	if after, ok := strings.CutPrefix(ref, "$graph_results."); ok {
		return after
	}
	return ""
}
