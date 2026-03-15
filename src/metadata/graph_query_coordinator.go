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

package metadata

import (
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/blevesearch/bleve/v2/search/query"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// GraphQueryCoordinator handles cross-shard graph query execution
type GraphQueryCoordinator struct {
	ms     *MetadataStore
	logger *zap.Logger
}

// NewGraphQueryCoordinator creates a new graph query coordinator
func NewGraphQueryCoordinator(ms *MetadataStore) *GraphQueryCoordinator {
	return &GraphQueryCoordinator{
		ms:     ms,
		logger: ms.logger,
	}
}

// ExecuteGraphSearches executes graph searches across shards, handling cross-shard edges
func (gqc *GraphQueryCoordinator) ExecuteGraphSearches(
	ctx context.Context,
	table *store.Table,
	graphSearches map[string]*indexes.GraphQuery,
	searchResult *indexes.RemoteIndexSearchResult,
) (map[string]indexes.GraphQueryResult, error) {
	if len(graphSearches) == 0 {
		return nil, nil
	}

	results := make(map[string]indexes.GraphQueryResult)

	// Execute each graph search
	for queryName, graphQuery := range graphSearches {
		result, err := gqc.executeGraphQuery(ctx, table, graphQuery, queryName, searchResult, results)
		if err != nil {
			gqc.logger.Warn("Failed to execute graph query",
				zap.String("query_name", queryName),
				zap.Error(err))
			// Store error in result
			results[queryName] = indexes.GraphQueryResult{
				Type:  graphQuery.Type,
				Total: 0,
			}
			continue
		}
		results[queryName] = *result
	}

	return results, nil
}

// executeGraphQuery executes a single graph query with cross-shard support
func (gqc *GraphQueryCoordinator) executeGraphQuery(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	queryName string,
	searchResult *indexes.RemoteIndexSearchResult,
	previousResults map[string]indexes.GraphQueryResult,
) (*indexes.GraphQueryResult, error) {
	startTime := time.Now()

	// Resolve start nodes
	startNodes, err := gqc.resolveNodeSelector(ctx, query.StartNodes, searchResult, previousResults)
	if err != nil {
		return nil, fmt.Errorf("resolving start nodes: %w", err)
	}

	if len(startNodes) == 0 {
		return nil, fmt.Errorf("no start nodes resolved")
	}

	// Execute based on query type
	var result *indexes.GraphQueryResult
	switch query.Type {
	case "traverse":
		result, err = gqc.executeTraverse(ctx, table, query, startNodes)
	case "neighbors":
		result, err = gqc.executeNeighbors(ctx, table, query, startNodes)
	case "shortest_path":
		result, err = gqc.executeShortestPath(ctx, table, query, startNodes, searchResult, previousResults)
	case "k_shortest_paths":
		result, err = gqc.executeKShortestPaths(ctx, table, query, startNodes, searchResult, previousResults)
	case "pattern":
		result, err = gqc.executePattern(ctx, table, query, startNodes)
	default:
		return nil, fmt.Errorf("unknown graph query type: %s", query.Type)
	}

	if err != nil {
		return nil, err
	}

	result.Took = time.Since(startTime)
	return result, nil
}

// executeTraverse performs cross-shard graph traversal
func (gqc *GraphQueryCoordinator) executeTraverse(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	direction := parseDirection(query.Params.Direction)
	maxDepth := query.Params.MaxDepth
	if maxDepth == 0 {
		maxDepth = 3 // default
	}

	nodeFilter := query.Params.NodeFilter
	hasQueryFilter := len(nodeFilter.FilterQuery) > 0

	visited := make(map[string]*indexes.GraphResultNode)
	frontier := make(map[string][]byte)

	// Initialize frontier with start nodes
	for _, node := range startNodes {
		frontier[string(node)] = node
	}

	// Pre-build edge type filter (constant across all nodes)
	var edgeTypeMap map[string]bool
	if len(query.Params.EdgeTypes) > 0 {
		edgeTypeMap = make(map[string]bool, len(query.Params.EdgeTypes))
		for _, et := range query.Params.EdgeTypes {
			edgeTypeMap[et] = true
		}
	}

	// BFS traversal
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		nextFrontier := make(map[string][]byte)

		// If we have a query filter, we need to fetch documents for the current frontier
		var frontierDocs map[string]map[string]any
		if hasQueryFilter {
			frontierDocs = gqc.fetchDocumentsForKeys(ctx, table, frontier)
		}

		// Process each node in current frontier
		for keyStr, nodeKey := range frontier {
			// Skip if already visited
			if _, seen := visited[keyStr]; seen {
				continue
			}

			// Apply node filter (query filter if we have documents)
			if hasQueryFilter {
				doc, hasDoc := frontierDocs[keyStr]
				if !hasDoc || !nodePassesQueryFilter(doc, &nodeFilter) {
					continue
				}
			}

			// Get edges for this node (cross-shard aware)
			edges, err := gqc.ms.getEdgesAcrossShards(
				ctx,
				table,
				query.IndexName,
				string(nodeKey),
				"", // all edge types (filter later if needed)
				direction,
			)
			if err != nil {
				gqc.logger.Warn("Failed to get edges during traversal",
					zap.String("key", types.FormatKey(nodeKey)),
					zap.Error(err))
				continue
			}

			// Filter by edge types if specified
			if edgeTypeMap != nil {
				filtered := make([]indexes.Edge, 0, len(edges))
				for _, edge := range edges {
					if edgeTypeMap[edge.Type] {
						filtered = append(filtered, edge)
					}
				}
				edges = filtered
			}

			// Add current node to visited
			resultNode := &indexes.GraphResultNode{
				Key:   base64.StdEncoding.EncodeToString(nodeKey),
				Depth: depth,
			}

			// Include edges if requested
			if query.IncludeEdges {
				resultNode.Edges = edges
			}

			// Store document if we already fetched it
			if hasQueryFilter {
				if doc, hasDoc := frontierDocs[keyStr]; hasDoc {
					resultNode.Document = doc
				}
			}

			visited[keyStr] = resultNode

			// Add neighbors to next frontier
			for _, edge := range edges {
				var targetKey []byte
				switch direction {
				case indexes.EdgeDirectionOut:
					targetKey = edge.Target
				case indexes.EdgeDirectionIn:
					targetKey = edge.Source
				default: // both
					// Add both source and target (with prefix filter)
					if nodePassesPrefixFilter(edge.Source, &nodeFilter) {
						if _, seen := visited[string(edge.Source)]; !seen {
							nextFrontier[string(edge.Source)] = edge.Source
						}
					}
					if nodePassesPrefixFilter(edge.Target, &nodeFilter) {
						if _, seen := visited[string(edge.Target)]; !seen {
							nextFrontier[string(edge.Target)] = edge.Target
						}
					}
					continue
				}

				// Apply prefix filter before adding to frontier
				if !nodePassesPrefixFilter(targetKey, &nodeFilter) {
					continue
				}

				targetKeyStr := string(targetKey)
				if _, seen := visited[targetKeyStr]; !seen {
					nextFrontier[targetKeyStr] = targetKey
				}
			}
		}

		frontier = nextFrontier

		// Check max results
		if query.Params.MaxResults > 0 && len(visited) >= query.Params.MaxResults {
			break
		}
	}

	// Convert visited map to slice
	nodes := make([]*indexes.GraphResultNode, 0, len(visited))
	for _, node := range visited {
		nodes = append(nodes, node)
	}

	// Fetch documents if requested (and not already fetched for filtering)
	if query.IncludeDocuments && !hasQueryFilter {
		if err := gqc.fetchDocumentsForNodes(ctx, table, nodes, query.Fields); err != nil {
			gqc.logger.Warn("Failed to fetch some documents", zap.Error(err))
		}
	}

	// Convert pointer slice to value slice
	nodeValues := make([]indexes.GraphResultNode, len(nodes))
	for i, node := range nodes {
		nodeValues[i] = *node
	}

	return &indexes.GraphQueryResult{
		Type:  "traverse",
		Nodes: nodeValues,
		Total: len(nodes),
	}, nil
}

// fetchDocumentsForKeys fetches documents for a set of keys, returning a map
func (gqc *GraphQueryCoordinator) fetchDocumentsForKeys(
	ctx context.Context,
	table *store.Table,
	keys map[string][]byte,
) map[string]map[string]any {
	result := make(map[string]map[string]any)

	// Group keys by shard
	type shardKeys struct {
		shardID types.ID
		keys    []string
	}
	shardGroups := make(map[types.ID]*shardKeys)

	for keyStr := range keys {
		shardID, err := table.FindShardForKey(keyStr)
		if err != nil {
			continue
		}

		if _, exists := shardGroups[shardID]; !exists {
			shardGroups[shardID] = &shardKeys{
				shardID: shardID,
				keys:    make([]string, 0),
			}
		}
		shardGroups[shardID].keys = append(shardGroups[shardID].keys, keyStr)
	}

	// Fetch documents in parallel by shard.
	// Use errgroup (not the shared pool) to avoid deadlock: this function is
	// called from within pool tasks during graph query execution (e.g.,
	// runQueries → runQuery → executeTraverse → fetchDocumentsForKeys).
	var mu sync.Mutex
	eg := new(errgroup.Group)
	eg.SetLimit(innerFanOutLimit)

	for _, group := range shardGroups {
		eg.Go(func() error {
			effectiveShardID, leaderClient, err := gqc.ms.leaderClientForShardWithEffectiveID(ctx, group.shardID)
			if err != nil {
				return nil // Don't fail the whole operation
			}

			// Batch lookup all keys for this shard in one call
			b64Keys := make([]string, len(group.keys))
			b64ToKey := make(map[string]string, len(group.keys))
			for i, keyStr := range group.keys {
				b64 := base64.StdEncoding.EncodeToString([]byte(keyStr))
				b64Keys[i] = b64
				b64ToKey[b64] = keyStr
			}

			results, err := leaderClient.Lookup(ctx, effectiveShardID, b64Keys)
			if err != nil {
				return nil
			}

			// Unmarshal outside the lock to avoid serializing CPU work.
			local := make(map[string]map[string]any, len(results))
			for b64Key, docBytes := range results {
				var doc map[string]any
				if err := json.Unmarshal(docBytes, &doc); err != nil {
					continue
				}
				local[b64ToKey[b64Key]] = doc
			}

			mu.Lock()
			maps.Copy(result, local)
			mu.Unlock()
			return nil
		})
	}

	_ = eg.Wait()
	return result
}

// executeNeighbors gets direct neighbors (1-hop traversal)
func (gqc *GraphQueryCoordinator) executeNeighbors(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	direction := parseDirection(query.Params.Direction)
	nodeFilter := query.Params.NodeFilter
	hasQueryFilter := len(nodeFilter.FilterQuery) > 0

	nodesMap := make(map[string]*indexes.GraphResultNode)

	// Pre-build edge type filter (constant across all nodes)
	edgeTypeFilter := ""
	if len(query.Params.EdgeTypes) == 1 {
		edgeTypeFilter = query.Params.EdgeTypes[0]
	}
	var edgeTypeMap map[string]bool
	if len(query.Params.EdgeTypes) > 1 {
		edgeTypeMap = make(map[string]bool, len(query.Params.EdgeTypes))
		for _, et := range query.Params.EdgeTypes {
			edgeTypeMap[et] = true
		}
	}

	// Get neighbors for each start node
	for _, nodeKey := range startNodes {
		// Get edges (cross-shard aware)
		edges, err := gqc.ms.getEdgesAcrossShards(
			ctx,
			table,
			query.IndexName,
			string(nodeKey),
			edgeTypeFilter,
			direction,
		)
		if err != nil {
			gqc.logger.Warn("Failed to get edges for neighbor query",
				zap.String("key", types.FormatKey(nodeKey)),
				zap.Error(err))
			continue
		}

		// Filter by edge types if multiple specified
		if edgeTypeMap != nil {
			filtered := make([]indexes.Edge, 0, len(edges))
			for _, edge := range edges {
				if edgeTypeMap[edge.Type] {
					filtered = append(filtered, edge)
				}
			}
			edges = filtered
		}

		// Extract neighbor keys
		for _, edge := range edges {
			var neighborKey []byte
			switch direction {
			case indexes.EdgeDirectionOut:
				neighborKey = edge.Target
			case indexes.EdgeDirectionIn:
				neighborKey = edge.Source
			default: // both
				// Add both (with prefix filter)
				sourceKeyStr := string(edge.Source)
				if sourceKeyStr != string(nodeKey) && nodePassesPrefixFilter(edge.Source, &nodeFilter) {
					if _, exists := nodesMap[sourceKeyStr]; !exists {
						nodesMap[sourceKeyStr] = &indexes.GraphResultNode{
							Key:   base64.StdEncoding.EncodeToString(edge.Source),
							Depth: 1,
						}
					}
				}
				targetKeyStr := string(edge.Target)
				if targetKeyStr != string(nodeKey) && nodePassesPrefixFilter(edge.Target, &nodeFilter) {
					if _, exists := nodesMap[targetKeyStr]; !exists {
						nodesMap[targetKeyStr] = &indexes.GraphResultNode{
							Key:   base64.StdEncoding.EncodeToString(edge.Target),
							Depth: 1,
						}
					}
				}
				continue
			}

			// Apply prefix filter
			if !nodePassesPrefixFilter(neighborKey, &nodeFilter) {
				continue
			}

			neighborKeyStr := string(neighborKey)
			if _, exists := nodesMap[neighborKeyStr]; !exists {
				resultNode := &indexes.GraphResultNode{
					Key:   base64.StdEncoding.EncodeToString(neighborKey),
					Depth: 1,
				}

				// Include edges if requested
				if query.IncludeEdges {
					// Get edges for this neighbor node
					neighborEdges, err := gqc.ms.getEdgesAcrossShards(
						ctx,
						table,
						query.IndexName,
						string(neighborKey),
						"",
						direction,
					)
					if err == nil {
						resultNode.Edges = neighborEdges
					}
				}

				nodesMap[neighborKeyStr] = resultNode
			}
		}
	}

	// Convert map to slice
	nodes := make([]*indexes.GraphResultNode, 0, len(nodesMap))
	for _, node := range nodesMap {
		nodes = append(nodes, node)
	}

	// Fetch documents if requested or needed for filtering
	needDocs := query.IncludeDocuments || hasQueryFilter
	if needDocs {
		if err := gqc.fetchDocumentsForNodes(ctx, table, nodes, query.Fields); err != nil {
			gqc.logger.Warn("Failed to fetch some documents", zap.Error(err))
		}
	}

	// Apply query filter if specified
	if hasQueryFilter {
		filteredNodes := make([]*indexes.GraphResultNode, 0, len(nodes))
		for _, node := range nodes {
			if nodePassesQueryFilter(node.Document, &nodeFilter) {
				filteredNodes = append(filteredNodes, node)
			}
		}
		nodes = filteredNodes
	}

	// Clear documents if not requested (but were fetched for filtering)
	if !query.IncludeDocuments && hasQueryFilter {
		for _, node := range nodes {
			node.Document = nil
		}
	}

	// Convert pointer slice to value slice
	nodeValues := make([]indexes.GraphResultNode, len(nodes))
	for i, node := range nodes {
		nodeValues[i] = *node
	}

	return &indexes.GraphQueryResult{
		Type:  "neighbors",
		Nodes: nodeValues,
		Total: len(nodes),
	}, nil
}

// executePattern executes a pattern matching query
func (gqc *GraphQueryCoordinator) executePattern(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	startNodes [][]byte,
) (*indexes.GraphQueryResult, error) {
	if len(query.Pattern) == 0 {
		return nil, fmt.Errorf("pattern query requires at least one pattern step")
	}

	maxResults := query.Params.MaxResults
	if maxResults == 0 {
		maxResults = 100 // default limit
	}

	// Initialize matches with start nodes bound to the first step's alias
	firstStep := query.Pattern[0]
	firstAlias := firstStep.Alias
	if firstAlias == "" {
		firstAlias = "_step0"
	}

	// Each match tracks bindings and the path taken
	type matchState struct {
		bindings map[string]*indexes.GraphResultNode
		path     []indexes.PathEdge
	}

	currentMatches := make([]matchState, 0, len(startNodes))

	// Filter start nodes by first step's node filter
	for _, nodeKey := range startNodes {
		if !nodePassesPrefixFilter(nodeKey, &firstStep.NodeFilter) {
			continue
		}

		resultNode := &indexes.GraphResultNode{
			Key:   base64.StdEncoding.EncodeToString(nodeKey),
			Depth: 0,
		}

		currentMatches = append(currentMatches, matchState{
			bindings: map[string]*indexes.GraphResultNode{firstAlias: resultNode},
			path:     []indexes.PathEdge{},
		})
	}

	// If first step has a query filter, fetch docs and filter
	if len(firstStep.NodeFilter.FilterQuery) > 0 {
		keysMap := make(map[string][]byte)
		for _, nodeKey := range startNodes {
			keysMap[string(nodeKey)] = nodeKey
		}
		docs := gqc.fetchDocumentsForKeys(ctx, table, keysMap)

		filteredMatches := make([]matchState, 0)
		for _, match := range currentMatches {
			node := match.bindings[firstAlias]
			keyBytes, _ := base64.StdEncoding.DecodeString(node.Key)
			if doc, ok := docs[string(keyBytes)]; ok {
				if nodePassesQueryFilter(doc, &firstStep.NodeFilter) {
					node.Document = doc
					filteredMatches = append(filteredMatches, match)
				}
			}
		}
		currentMatches = filteredMatches
	}

	// Process remaining pattern steps
	for stepIdx := 1; stepIdx < len(query.Pattern); stepIdx++ {
		step := query.Pattern[stepIdx]
		stepAlias := step.Alias
		if stepAlias == "" {
			stepAlias = fmt.Sprintf("_step%d", stepIdx)
		}

		// Get edge constraints
		edgeStep := &step.Edge
		minHops := edgeStep.MinHops
		if minHops == 0 {
			minHops = 1
		}
		maxHops := edgeStep.MaxHops
		if maxHops == 0 {
			maxHops = 1
		}
		direction := parseDirection(edgeStep.Direction)

		nextMatches := make([]matchState, 0)

		// For each current match, find nodes reachable via the edge constraints
		for _, match := range currentMatches {
			// Get the current node (last bound node from previous step)
			prevAlias := query.Pattern[stepIdx-1].Alias
			if prevAlias == "" {
				prevAlias = fmt.Sprintf("_step%d", stepIdx-1)
			}
			currentNode := match.bindings[prevAlias]
			currentKeyBytes, _ := base64.StdEncoding.DecodeString(currentNode.Key)

			// Check if this step requires returning to a previously bound alias (cycle detection)
			isCycleCheck := false
			var cycleTargetNode *indexes.GraphResultNode
			if existingNode, exists := match.bindings[stepAlias]; exists {
				isCycleCheck = true
				cycleTargetNode = existingNode
			}

			// Perform variable-length traversal
			reachable := gqc.findReachableNodes(ctx, table, query.IndexName, currentKeyBytes, edgeStep, direction, minHops, maxHops, &step.NodeFilter)

			for _, reached := range reachable {
				// If cycle check, only match if we reached the expected node
				if isCycleCheck {
					if reached.node.Key != cycleTargetNode.Key {
						continue
					}
				}

				// Apply node filter
				if !nodePassesPrefixFilter([]byte(reached.rawKey), &step.NodeFilter) {
					continue
				}

				// Create new match with this binding
				newBindings := make(map[string]*indexes.GraphResultNode)
				maps.Copy(newBindings, match.bindings)
				if !isCycleCheck {
					newBindings[stepAlias] = reached.node
				}

				newPath := make([]indexes.PathEdge, len(match.path)+len(reached.path))
				copy(newPath, match.path)
				copy(newPath[len(match.path):], reached.path)

				nextMatches = append(nextMatches, matchState{
					bindings: newBindings,
					path:     newPath,
				})

				// Check limit
				if len(nextMatches) >= maxResults*10 { // Allow some extra for later filtering
					break
				}
			}

			if len(nextMatches) >= maxResults*10 {
				break
			}
		}

		currentMatches = nextMatches

		if len(currentMatches) == 0 {
			break
		}
	}

	// If last step has query filter, apply it
	lastStep := query.Pattern[len(query.Pattern)-1]
	if len(lastStep.NodeFilter.FilterQuery) > 0 {
		lastAlias := lastStep.Alias
		if lastAlias == "" {
			lastAlias = fmt.Sprintf("_step%d", len(query.Pattern)-1)
		}

		// Collect keys to fetch
		keysToFetch := make(map[string][]byte)
		for _, match := range currentMatches {
			if node, ok := match.bindings[lastAlias]; ok {
				if node.Document == nil {
					keyBytes, _ := base64.StdEncoding.DecodeString(node.Key)
					keysToFetch[string(keyBytes)] = keyBytes
				}
			}
		}

		if len(keysToFetch) > 0 {
			docs := gqc.fetchDocumentsForKeys(ctx, table, keysToFetch)

			filteredMatches := make([]matchState, 0)
			for _, match := range currentMatches {
				node := match.bindings[lastAlias]
				keyBytes, _ := base64.StdEncoding.DecodeString(node.Key)
				doc, ok := docs[string(keyBytes)]
				if !ok {
					doc = node.Document
				}
				if nodePassesQueryFilter(doc, &lastStep.NodeFilter) {
					node.Document = doc
					filteredMatches = append(filteredMatches, match)
				}
			}
			currentMatches = filteredMatches
		}
	}

	// Limit results
	if len(currentMatches) > maxResults {
		currentMatches = currentMatches[:maxResults]
	}

	// Convert to PatternMatch results
	matches := make([]indexes.PatternMatch, len(currentMatches))
	for i, match := range currentMatches {
		bindings := make(map[string]indexes.GraphResultNode)
		for alias, node := range match.bindings {
			// Filter by return_aliases if specified
			if len(query.ReturnAliases) > 0 {
				found := slices.Contains(query.ReturnAliases, alias)
				if !found {
					continue
				}
			}
			bindings[alias] = *node
		}
		matches[i] = indexes.PatternMatch{
			Bindings: bindings,
			Path:     match.path,
		}
	}

	// Fetch documents for returned nodes if requested
	if query.IncludeDocuments {
		keysToFetch := make(map[string][]byte)
		for _, match := range matches {
			for _, node := range match.Bindings {
				if node.Document == nil {
					keyBytes, _ := base64.StdEncoding.DecodeString(node.Key)
					keysToFetch[string(keyBytes)] = keyBytes
				}
			}
		}

		if len(keysToFetch) > 0 {
			docs := gqc.fetchDocumentsForKeys(ctx, table, keysToFetch)
			for i := range matches {
				for alias, node := range matches[i].Bindings {
					if node.Document == nil {
						keyBytes, _ := base64.StdEncoding.DecodeString(node.Key)
						if doc, ok := docs[string(keyBytes)]; ok {
							node.Document = doc
							matches[i].Bindings[alias] = node
						}
					}
				}
			}
		}
	}

	return &indexes.GraphQueryResult{
		Type:    "pattern",
		Matches: matches,
		Total:   len(matches),
	}, nil
}

// reachableNode represents a node reached during pattern traversal
type reachableNode struct {
	node   *indexes.GraphResultNode
	rawKey string
	path   []indexes.PathEdge
}

// findReachableNodes finds nodes reachable via the given edge constraints
func (gqc *GraphQueryCoordinator) findReachableNodes(
	ctx context.Context,
	table *store.Table,
	indexName string,
	startKey []byte,
	edgeStep *indexes.PatternEdgeStep,
	direction indexes.EdgeDirection,
	minHops, maxHops int,
	nodeFilter *indexes.NodeFilter,
) []reachableNode {
	results := make([]reachableNode, 0)
	visited := make(map[string]bool)

	type frontier struct {
		key  []byte
		path []indexes.PathEdge
		hops int
	}

	current := []frontier{{key: startKey, path: []indexes.PathEdge{}, hops: 0}}
	visited[string(startKey)] = true

	for len(current) > 0 && len(results) < 1000 { // Safety limit
		next := make([]frontier, 0)

		for _, f := range current {
			if f.hops >= maxHops {
				continue
			}

			// Get edges from this node
			edgeTypeFilter := ""
			if edgeStep != nil && len(edgeStep.Types) == 1 {
				edgeTypeFilter = edgeStep.Types[0]
			}

			edges, err := gqc.ms.getEdgesAcrossShards(ctx, table, indexName, string(f.key), edgeTypeFilter, direction)
			if err != nil {
				gqc.ms.logger.Warn("getEdgesAcrossShards error",
					zap.String("key", string(f.key)),
					zap.Error(err))
				continue
			}

			// Filter by edge types if multiple
			if edgeStep != nil && len(edgeStep.Types) > 1 {
				filtered := make([]indexes.Edge, 0)
				typeMap := make(map[string]bool)
				for _, t := range edgeStep.Types {
					typeMap[t] = true
				}
				for _, e := range edges {
					if typeMap[e.Type] {
						filtered = append(filtered, e)
					}
				}
				edges = filtered
			}

			// Filter by weight
			if edgeStep != nil && (edgeStep.MinWeight > 0 || edgeStep.MaxWeight > 0) {
				filtered := make([]indexes.Edge, 0)
				for _, e := range edges {
					if edgeStep.MinWeight > 0 && e.Weight < edgeStep.MinWeight {
						continue
					}
					if edgeStep.MaxWeight > 0 && e.Weight > edgeStep.MaxWeight {
						continue
					}
					filtered = append(filtered, e)
				}
				edges = filtered
			}

			// Process edges
			for _, edge := range edges {
				var targetKeys [][]byte
				switch direction {
				case indexes.EdgeDirectionOut:
					targetKeys = [][]byte{edge.Target}
				case indexes.EdgeDirectionIn:
					targetKeys = [][]byte{edge.Source}
				default: // both
					targetKeys = [][]byte{edge.Source, edge.Target}
				}

				for _, targetKey := range targetKeys {
					if string(targetKey) == string(f.key) {
						continue
					}

					targetKeyStr := string(targetKey)
					newHops := f.hops + 1

					// Build path edge
					pathEdge := indexes.PathEdge{
						Source:   base64.StdEncoding.EncodeToString(f.key),
						Target:   base64.StdEncoding.EncodeToString(targetKey),
						Type:     edge.Type,
						Weight:   edge.Weight,
						Metadata: edge.Metadata,
					}

					newPath := make([]indexes.PathEdge, len(f.path)+1)
					copy(newPath, f.path)
					newPath[len(f.path)] = pathEdge

					// If we've reached minimum hops and pass filters, add to results
					if newHops >= minHops {
						if nodePassesPrefixFilter(targetKey, nodeFilter) {
							results = append(results, reachableNode{
								node: &indexes.GraphResultNode{
									Key:   base64.StdEncoding.EncodeToString(targetKey),
									Depth: newHops,
								},
								rawKey: targetKeyStr,
								path:   newPath,
							})
						}
					}

					// If we can continue traversing and haven't visited this node yet
					if newHops < maxHops && !visited[targetKeyStr] {
						visited[targetKeyStr] = true
						next = append(next, frontier{key: targetKey, path: newPath, hops: newHops})
					}
				}
			}
		}

		current = next
	}

	return results
}

// executeShortestPath finds shortest paths between nodes
func (gqc *GraphQueryCoordinator) executeShortestPath(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	startNodes [][]byte,
	searchResult *indexes.RemoteIndexSearchResult,
	previousResults map[string]indexes.GraphQueryResult,
) (*indexes.GraphQueryResult, error) {
	if len(query.TargetNodes.Keys) == 0 {
		return nil, fmt.Errorf("shortest_path requires target_nodes")
	}

	// Resolve target nodes
	targetNodes, err := gqc.resolveNodeSelector(ctx, query.TargetNodes, searchResult, previousResults)
	if err != nil {
		return nil, fmt.Errorf("resolving target nodes: %w", err)
	}

	if len(targetNodes) == 0 {
		return nil, fmt.Errorf("no target nodes resolved")
	}

	direction := parseDirection(query.Params.Direction)
	weightMode := query.Params.WeightMode
	if weightMode == "" {
		weightMode = "min_hops"
	}
	maxDepth := query.Params.MaxDepth
	if maxDepth == 0 {
		maxDepth = 50
	}

	paths := make([]indexes.Path, 0)

	// Find paths from each start to each target
	for _, startKey := range startNodes {
		for _, targetKey := range targetNodes {
			path, err := gqc.ms.findCrossShardShortestPath(
				ctx,
				table,
				query.IndexName,
				startKey,
				targetKey,
				query.Params.EdgeTypes,
				direction,
				weightMode,
				maxDepth,
				query.Params.MinWeight,
				query.Params.MaxWeight,
			)
			if err != nil {
				gqc.logger.Warn("Failed to find shortest path",
					zap.ByteString("source", startKey),
					zap.ByteString("target", targetKey),
					zap.Error(err))
				continue
			}
			if path != nil {
				paths = append(paths, *path)
			}
		}
	}

	return &indexes.GraphQueryResult{
		Type:  "shortest_path",
		Paths: paths,
		Total: len(paths),
	}, nil
}

// executeKShortestPaths finds k shortest paths between nodes using Yen's algorithm
func (gqc *GraphQueryCoordinator) executeKShortestPaths(
	ctx context.Context,
	table *store.Table,
	query *indexes.GraphQuery,
	startNodes [][]byte,
	searchResult *indexes.RemoteIndexSearchResult,
	previousResults map[string]indexes.GraphQueryResult,
) (*indexes.GraphQueryResult, error) {
	if len(query.TargetNodes.Keys) == 0 {
		return nil, fmt.Errorf("k_shortest_paths requires target_nodes")
	}

	// Resolve target nodes
	targetNodes, err := gqc.resolveNodeSelector(ctx, query.TargetNodes, searchResult, previousResults)
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
	if maxDepth == 0 {
		maxDepth = 50
	}

	allPaths := make([]indexes.Path, 0)

	// Find k-shortest paths for each source-target pair
	for _, source := range startNodes {
		for _, target := range targetNodes {
			paths, err := gqc.yenKShortestPaths(
				ctx,
				table,
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
				gqc.logger.Warn("Failed to find k-shortest paths",
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

// yenKShortestPaths implements Yen's algorithm for finding k shortest paths
func (gqc *GraphQueryCoordinator) yenKShortestPaths(
	ctx context.Context,
	table *store.Table,
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
	firstPath, err := gqc.ms.findCrossShardShortestPath(
		ctx, table, indexName, source, target,
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

	// B is a min-heap of candidate paths
	type candidatePath struct {
		path   indexes.Path
		weight float64
	}
	B := make([]candidatePath, 0)

	// Set to track which paths we've already found (to avoid duplicates)
	seenPaths := make(map[string]bool)
	seenPaths[pathToKey(firstPath)] = true

	// Find the remaining k-1 paths
	for i := 1; i < k; i++ {
		// Get the (i-1)th path (last found)
		prevPath := A[len(A)-1]

		// For each node in the previous path (except the last), try deviating
		for spurIdx := 0; spurIdx < len(prevPath.Nodes)-1; spurIdx++ {
			// The spur node is where we deviate from previous paths
			spurNodeB64 := prevPath.Nodes[spurIdx]
			spurNode, err := base64.StdEncoding.DecodeString(spurNodeB64)
			if err != nil {
				continue
			}

			// Root path is the path from source to spur node
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

			// Build the set of edges to exclude:
			// All edges that leave the spur node in previously found paths
			excludedEdges := make(map[string]bool)
			for _, path := range A {
				if len(path.Nodes) > spurIdx && path.Nodes[spurIdx] == spurNodeB64 {
					// This path goes through the spur node at the same position
					if spurIdx < len(path.Edges) {
						edgeKey := path.Edges[spurIdx].Source + "->" + path.Edges[spurIdx].Target + ":" + path.Edges[spurIdx].Type
						excludedEdges[edgeKey] = true
					}
				}
			}

			// Also exclude nodes in the root path (to avoid loops)
			excludedNodes := make(map[string]bool)
			for j := 0; j < spurIdx; j++ {
				excludedNodes[prevPath.Nodes[j]] = true
			}

			// Find shortest path from spur node to target, excluding certain edges/nodes
			spurPath, err := gqc.findShortestPathWithExclusions(
				ctx, table, indexName,
				spurNode, target,
				edgeTypes, direction, weightMode, maxDepth-spurIdx,
				minWeight, maxWeight,
				excludedEdges, excludedNodes,
			)
			if err != nil || spurPath == nil {
				continue
			}

			// Combine root path + spur path to form total path
			totalPath := indexes.Path{
				Nodes:       make([]string, 0, len(rootPath.Nodes)+len(spurPath.Nodes)-1),
				Edges:       make([]indexes.PathEdge, 0, len(rootPath.Edges)+len(spurPath.Edges)),
				TotalWeight: rootPath.TotalWeight + spurPath.TotalWeight,
				Length:      rootPath.Length + spurPath.Length,
			}

			// Add root path nodes
			totalPath.Nodes = append(totalPath.Nodes, rootPath.Nodes...)
			totalPath.Edges = append(totalPath.Edges, rootPath.Edges...)

			// Add spur path (skip first node since it's the spur node already in root)
			if len(spurPath.Nodes) > 1 {
				totalPath.Nodes = append(totalPath.Nodes, spurPath.Nodes[1:]...)
			}
			totalPath.Edges = append(totalPath.Edges, spurPath.Edges...)

			// Check if we've seen this path before
			pathKey := pathToKey(&totalPath)
			if seenPaths[pathKey] {
				continue
			}
			seenPaths[pathKey] = true

			// Calculate weight for priority
			var weight float64
			switch weightMode {
			case "max_weight":
				// For max_weight, we want higher weights to come first (negate for min-heap)
				weight = -totalPath.TotalWeight
			default: // min_hops, min_weight
				weight = totalPath.TotalWeight
				if weightMode == "min_hops" {
					weight = float64(totalPath.Length)
				}
			}

			// Add to candidate heap
			B = append(B, candidatePath{path: totalPath, weight: weight})
		}

		if len(B) == 0 {
			// No more candidate paths
			break
		}

		// Sort B by weight and pick the best
		slices.SortFunc(B, func(a, b candidatePath) int {
			return cmp.Compare(a.weight, b.weight)
		})
		bestCandidate := B[0]
		B = B[1:]

		A = append(A, bestCandidate.path)
	}

	return A, nil
}

// findShortestPathWithExclusions finds shortest path while excluding certain edges and nodes
func (gqc *GraphQueryCoordinator) findShortestPathWithExclusions(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	weightMode indexes.PathWeightMode,
	maxDepth int,
	minWeight, maxWeight float64,
	excludedEdges map[string]bool,
	excludedNodes map[string]bool,
) (*indexes.Path, error) {
	if len(source) == 0 || len(target) == 0 {
		return nil, fmt.Errorf("source and target must not be empty")
	}
	if maxDepth <= 0 {
		maxDepth = 50
	}

	// Check if source == target
	if string(source) == string(target) {
		return &indexes.Path{
			Nodes:       []string{base64.StdEncoding.EncodeToString(source)},
			Edges:       []indexes.PathEdge{},
			TotalWeight: 0.0,
			Length:      0,
		}, nil
	}

	switch weightMode {
	case indexes.PathWeightModeMinHops:
		return gqc.bfsWithExclusions(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes)
	case indexes.PathWeightModeMaxWeight:
		return gqc.dijkstraWithExclusions(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes, true)
	case indexes.PathWeightModeMinWeight:
		return gqc.dijkstraWithExclusions(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes, false)
	default:
		return gqc.bfsWithExclusions(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes)
	}
}

// bfsWithExclusions performs BFS shortest path with edge/node exclusions
func (gqc *GraphQueryCoordinator) bfsWithExclusions(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
	excludedEdges map[string]bool,
	excludedNodes map[string]bool,
) (*indexes.Path, error) {
	type pathNode struct {
		key        []byte
		hops       int
		parent     *pathNode
		parentEdge indexes.Edge
	}

	visited := make(map[string]bool)
	visited[string(source)] = true

	parent := make(map[string]*pathNode)
	parent[string(source)] = &pathNode{key: source, hops: 0}

	frontier := [][]byte{source}
	targetStr := string(target)

	// Helper to reconstruct path
	reconstructPath := func() *indexes.Path {
		var nodes []string
		var edges []indexes.PathEdge

		current := parent[targetStr]
		for current != nil {
			nodes = append(nodes, base64.StdEncoding.EncodeToString(current.key))
			if current.parent != nil {
				edges = append(edges, indexes.PathEdge{
					Source:   base64.StdEncoding.EncodeToString(current.parentEdge.Source),
					Target:   base64.StdEncoding.EncodeToString(current.parentEdge.Target),
					Type:     current.parentEdge.Type,
					Weight:   current.parentEdge.Weight,
					Metadata: current.parentEdge.Metadata,
				})
			}
			current = current.parent
		}

		// Reverse to get source -> target order
		for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		}
		for i, j := 0, len(edges)-1; i < j; i, j = i+1, j-1 {
			edges[i], edges[j] = edges[j], edges[i]
		}

		totalWeight := 0.0
		for _, e := range edges {
			totalWeight += e.Weight
		}

		return &indexes.Path{
			Nodes:       nodes,
			Edges:       edges,
			TotalWeight: totalWeight,
			Length:      len(edges),
		}
	}

	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		// Check if target is in current frontier
		for _, node := range frontier {
			if string(node) == targetStr {
				return reconstructPath(), nil
			}
		}

		nextFrontier := make([][]byte, 0)

		for _, node := range frontier {
			nodeStr := string(node)

			edges, err := gqc.ms.getEdgesAcrossShards(ctx, table, indexName, nodeStr, "", direction)
			if err != nil {
				continue
			}

			for _, edge := range edges {
				// Filter by edge type
				if len(edgeTypes) > 0 {
					found := slices.Contains(edgeTypes, edge.Type)
					if !found {
						continue
					}
				}

				// Filter by weight
				if edge.Weight < minWeight || edge.Weight > maxWeight {
					continue
				}

				// Check if edge is excluded
				edgeKey := base64.StdEncoding.EncodeToString(edge.Source) + "->" +
					base64.StdEncoding.EncodeToString(edge.Target) + ":" + edge.Type
				if excludedEdges[edgeKey] {
					continue
				}

				// Determine neighbor
				var neighborKey []byte
				if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
					neighborKey = edge.Target
				} else {
					neighborKey = edge.Source
				}

				neighborB64 := base64.StdEncoding.EncodeToString(neighborKey)
				neighborStr := string(neighborKey)

				// Check if node is excluded
				if excludedNodes[neighborB64] {
					continue
				}

				if !visited[neighborStr] {
					visited[neighborStr] = true
					parent[neighborStr] = &pathNode{
						key:        neighborKey,
						hops:       depth + 1,
						parent:     parent[nodeStr],
						parentEdge: edge,
					}
					nextFrontier = append(nextFrontier, neighborKey)
				}
			}
		}

		frontier = nextFrontier
	}

	return nil, nil // No path found
}

// dijkstraWithExclusions performs Dijkstra shortest path with edge/node exclusions
func (gqc *GraphQueryCoordinator) dijkstraWithExclusions(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
	excludedEdges map[string]bool,
	excludedNodes map[string]bool,
	maxWeightMode bool,
) (*indexes.Path, error) {
	type pathNode struct {
		key        []byte
		distance   float64
		hops       int
		parent     *pathNode
		parentEdge indexes.Edge
	}

	distances := make(map[string]float64)
	distances[string(source)] = 0

	visited := make(map[string]bool)
	parent := make(map[string]*pathNode)
	parent[string(source)] = &pathNode{key: source, distance: 0, hops: 0}

	type queueItem struct {
		key      []byte
		distance float64
		hops     int
	}
	pq := []queueItem{{key: source, distance: 0, hops: 0}}

	targetStr := string(target)

	// Helper to reconstruct path
	reconstructPath := func() *indexes.Path {
		var nodes []string
		var edges []indexes.PathEdge

		current := parent[targetStr]
		for current != nil {
			nodes = append(nodes, base64.StdEncoding.EncodeToString(current.key))
			if current.parent != nil {
				edges = append(edges, indexes.PathEdge{
					Source:   base64.StdEncoding.EncodeToString(current.parentEdge.Source),
					Target:   base64.StdEncoding.EncodeToString(current.parentEdge.Target),
					Type:     current.parentEdge.Type,
					Weight:   current.parentEdge.Weight,
					Metadata: current.parentEdge.Metadata,
				})
			}
			current = current.parent
		}

		// Reverse to get source -> target order
		for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		}
		for i, j := 0, len(edges)-1; i < j; i, j = i+1, j-1 {
			edges[i], edges[j] = edges[j], edges[i]
		}

		totalWeight := 0.0
		for _, e := range edges {
			totalWeight += e.Weight
		}

		return &indexes.Path{
			Nodes:       nodes,
			Edges:       edges,
			TotalWeight: totalWeight,
			Length:      len(edges),
		}
	}

	for len(pq) > 0 {
		// Pop min/max distance node
		bestIdx := 0
		for i := range pq {
			if maxWeightMode {
				if pq[i].distance > pq[bestIdx].distance {
					bestIdx = i
				}
			} else {
				if pq[i].distance < pq[bestIdx].distance {
					bestIdx = i
				}
			}
		}
		current := pq[bestIdx]
		pq = append(pq[:bestIdx], pq[bestIdx+1:]...)

		currentStr := string(current.key)

		if visited[currentStr] {
			continue
		}
		visited[currentStr] = true

		if currentStr == targetStr {
			return reconstructPath(), nil
		}

		if current.hops >= maxDepth {
			continue
		}

		edges, err := gqc.ms.getEdgesAcrossShards(ctx, table, indexName, currentStr, "", direction)
		if err != nil {
			continue
		}

		for _, edge := range edges {
			// Filter by edge type
			if len(edgeTypes) > 0 {
				found := slices.Contains(edgeTypes, edge.Type)
				if !found {
					continue
				}
			}

			// Filter by weight
			if edge.Weight < minWeight || edge.Weight > maxWeight {
				continue
			}
			if maxWeightMode && edge.Weight <= 0 {
				continue
			}

			// Check if edge is excluded
			edgeKey := base64.StdEncoding.EncodeToString(edge.Source) + "->" +
				base64.StdEncoding.EncodeToString(edge.Target) + ":" + edge.Type
			if excludedEdges[edgeKey] {
				continue
			}

			// Determine neighbor
			var neighborKey []byte
			if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
				neighborKey = edge.Target
			} else {
				neighborKey = edge.Source
			}

			neighborB64 := base64.StdEncoding.EncodeToString(neighborKey)
			neighborStr := string(neighborKey)

			// Check if node is excluded
			if excludedNodes[neighborB64] {
				continue
			}

			if visited[neighborStr] {
				continue
			}

			// Calculate new distance
			var newDistance float64
			if maxWeightMode {
				// For max weight, use negative log for min-heap behavior
				newDistance = current.distance + (-1.0 / edge.Weight)
			} else {
				newDistance = current.distance + edge.Weight
			}

			oldDistance, exists := distances[neighborStr]
			better := !exists
			if exists {
				if maxWeightMode {
					better = newDistance > oldDistance
				} else {
					better = newDistance < oldDistance
				}
			}

			if better {
				distances[neighborStr] = newDistance
				parent[neighborStr] = &pathNode{
					key:        neighborKey,
					distance:   newDistance,
					hops:       current.hops + 1,
					parent:     parent[currentStr],
					parentEdge: edge,
				}
				pq = append(pq, queueItem{
					key:      neighborKey,
					distance: newDistance,
					hops:     current.hops + 1,
				})
			}
		}
	}

	return nil, nil // No path found
}

// pathToKey creates a unique string key for a path
func pathToKey(p *indexes.Path) string {
	return strings.Join(p.Nodes, "->")
}

// resolveNodeSelector converts node selector to concrete keys
func (gqc *GraphQueryCoordinator) resolveNodeSelector(
	ctx context.Context,
	selector indexes.GraphNodeSelector,
	searchResult *indexes.RemoteIndexSearchResult,
	previousResults map[string]indexes.GraphQueryResult,
) ([][]byte, error) {
	// Explicit keys
	if len(selector.Keys) > 0 {
		keys := make([][]byte, 0, len(selector.Keys))
		for _, keyStr := range selector.Keys {
			keyBytes, err := base64.StdEncoding.DecodeString(keyStr)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 key %q: %w", keyStr, err)
			}
			keys = append(keys, keyBytes)
		}
		return keys, nil
	}

	// Result reference
	if selector.ResultRef != "" {
		return gqc.resolveResultRef(selector.ResultRef, searchResult, previousResults, selector.Limit)
	}

	return nil, fmt.Errorf("node selector must specify either keys or result_ref")
}

// resolveResultRef resolves a result reference to node keys
func (gqc *GraphQueryCoordinator) resolveResultRef(
	ref string,
	searchResult *indexes.RemoteIndexSearchResult,
	previousResults map[string]indexes.GraphQueryResult,
	limit int,
) ([][]byte, error) {
	var keys [][]byte

	switch {
	case ref == "$full_text_results":
		if searchResult == nil || searchResult.BleveSearchResult == nil {
			return nil, fmt.Errorf("no full-text search results available")
		}
		for _, hit := range searchResult.BleveSearchResult.Hits {
			keyBytes := []byte(hit.ID)
			keys = append(keys, keyBytes)
			if limit > 0 && len(keys) >= limit {
				break
			}
		}

	case len(ref) > 15 && ref[:15] == "$aknn_results.":
		indexName := ref[15:]
		if searchResult == nil || searchResult.VectorSearchResult == nil {
			return nil, fmt.Errorf("no vector search results available")
		}
		vectorResult, ok := searchResult.VectorSearchResult[indexName]
		if !ok {
			return nil, fmt.Errorf("no vector results for index %q", indexName)
		}
		for _, hit := range vectorResult.Hits {
			keys = append(keys, []byte(hit.ID))
			if limit > 0 && len(keys) >= limit {
				break
			}
		}

	case len(ref) > 16 && ref[:16] == "$graph_results.":
		queryName := ref[16:]
		if previousResults == nil {
			return nil, fmt.Errorf("no previous graph results available")
		}
		graphResult, ok := previousResults[queryName]
		if !ok {
			return nil, fmt.Errorf("no graph results for query %q", queryName)
		}
		for _, node := range graphResult.Nodes {
			keyBytes, err := base64.StdEncoding.DecodeString(node.Key)
			if err != nil {
				continue
			}
			keys = append(keys, keyBytes)
			if limit > 0 && len(keys) >= limit {
				break
			}
		}

	default:
		return nil, fmt.Errorf("unknown result reference: %s", ref)
	}

	return keys, nil
}

// fetchDocumentsForNodes fetches documents for graph result nodes
func (gqc *GraphQueryCoordinator) fetchDocumentsForNodes(
	ctx context.Context,
	table *store.Table,
	nodes []*indexes.GraphResultNode,
	fields []string,
) error {
	// Group nodes by shard
	type shardNodes struct {
		shardID types.ID
		nodes   []*indexes.GraphResultNode
	}

	shardGroups := make(map[types.ID]*shardNodes)
	for _, node := range nodes {
		keyBytes, err := base64.StdEncoding.DecodeString(node.Key)
		if err != nil {
			continue
		}

		shardID, err := table.FindShardForKey(string(keyBytes))
		if err != nil {
			continue
		}

		if _, exists := shardGroups[shardID]; !exists {
			shardGroups[shardID] = &shardNodes{
				shardID: shardID,
				nodes:   make([]*indexes.GraphResultNode, 0),
			}
		}
		shardGroups[shardID].nodes = append(shardGroups[shardID].nodes, node)
	}

	// Fetch documents in parallel by shard.
	// Use errgroup (not the shared pool) to avoid deadlock: this function is
	// called from within pool tasks during graph query execution.
	eg := new(errgroup.Group)
	eg.SetLimit(innerFanOutLimit)

	for _, group := range shardGroups {
		eg.Go(func() error {
			effectiveShardID, leaderClient, err := gqc.ms.leaderClientForShardWithEffectiveID(ctx, group.shardID)
			if err != nil {
				return err
			}

			// Batch lookup all node keys for this shard in one call
			keys := make([]string, len(group.nodes))
			nodeByKey := make(map[string]*indexes.GraphResultNode, len(group.nodes))
			for i, node := range group.nodes {
				keys[i] = node.Key
				nodeByKey[node.Key] = node
			}

			results, err := leaderClient.Lookup(ctx, effectiveShardID, keys)
			if err != nil {
				return err
			}

			// Each shard task writes to disjoint node pointers (partitioned by shard),
			// so no lock is needed here.
			for key, docBytes := range results {
				node := nodeByKey[key]

				var doc map[string]any
				if err := json.Unmarshal(docBytes, &doc); err != nil {
					gqc.logger.Debug("Failed to unmarshal document",
						zap.String("key", key),
						zap.Error(err))
					continue
				}

				// Filter to requested fields if specified
				if len(fields) > 0 {
					filtered := make(map[string]any)
					for _, field := range fields {
						if val, ok := doc[field]; ok {
							filtered[field] = val
						}
					}
					node.Document = filtered
				} else {
					node.Document = doc
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

// parseDirection converts string direction to EdgeDirection
func parseDirection(dir indexes.EdgeDirection) indexes.EdgeDirection {
	switch dir {
	case "in":
		return indexes.EdgeDirectionIn
	case "both":
		return indexes.EdgeDirectionBoth
	default:
		return indexes.EdgeDirectionOut
	}
}

// nodePassesPrefixFilter checks if a node key passes the prefix filter
func nodePassesPrefixFilter(nodeKey []byte, filter *indexes.NodeFilter) bool {
	if filter == nil || filter.FilterPrefix == "" {
		return true
	}
	return strings.HasPrefix(string(nodeKey), filter.FilterPrefix)
}

// nodePassesQueryFilter checks if a node's document passes the filter query
// This requires the document to be fetched first
func nodePassesQueryFilter(doc map[string]any, filter *indexes.NodeFilter) bool {
	if filter == nil || filter.FilterQuery == nil || len(filter.FilterQuery) == 0 {
		return true
	}

	// Parse the filter query into a Bleve query
	bleveQuery, err := parseBleveQuery(filter.FilterQuery)
	if err != nil {
		// If we can't parse the query, don't filter (fail open)
		return true
	}

	// Check if the document matches the query
	return documentMatchesQuery(doc, bleveQuery)
}

// parseBleveQuery converts a filter_query map to a Bleve query
func parseBleveQuery(filterQuery map[string]any) (query.Query, error) {
	// Marshal to JSON and use Bleve's query parser
	queryBytes, err := json.Marshal(filterQuery)
	if err != nil {
		return nil, err
	}

	return query.ParseQuery(queryBytes)
}

// documentMatchesQuery checks if a document matches a Bleve query
// This is a simplified check that handles common query types
func documentMatchesQuery(doc map[string]any, q query.Query) bool {
	switch typedQuery := q.(type) {
	case *query.TermQuery:
		return matchTermQuery(doc, typedQuery)
	case *query.MatchQuery:
		return matchMatchQuery(doc, typedQuery)
	case *query.BooleanQuery:
		return matchBooleanQuery(doc, typedQuery)
	case *query.ConjunctionQuery:
		return matchConjunctionQuery(doc, typedQuery)
	case *query.DisjunctionQuery:
		return matchDisjunctionQuery(doc, typedQuery)
	default:
		// For unsupported query types, fail open
		return true
	}
}

func matchTermQuery(doc map[string]any, q *query.TermQuery) bool {
	field := q.FieldVal
	term := q.Term

	val, ok := doc[field]
	if !ok {
		return false
	}

	// Handle string comparison
	if strVal, ok := val.(string); ok {
		return strings.EqualFold(strVal, term)
	}

	return false
}

func matchMatchQuery(doc map[string]any, q *query.MatchQuery) bool {
	field := q.FieldVal
	match := q.Match

	val, ok := doc[field]
	if !ok {
		return false
	}

	// Handle string comparison (simplified - just checks contains)
	if strVal, ok := val.(string); ok {
		return strings.Contains(strings.ToLower(strVal), strings.ToLower(match))
	}

	return false
}

func matchBooleanQuery(doc map[string]any, q *query.BooleanQuery) bool {
	// Must clauses (all must match)
	if q.Must != nil {
		if conjQ, ok := q.Must.(*query.ConjunctionQuery); ok {
			if !matchConjunctionQuery(doc, conjQ) {
				return false
			}
		} else {
			// Single query in Must
			if !documentMatchesQuery(doc, q.Must) {
				return false
			}
		}
	}

	// Should clauses (at least one should match, if present)
	if q.Should != nil {
		if disjQ, ok := q.Should.(*query.DisjunctionQuery); ok {
			if len(disjQ.Disjuncts) > 0 && !matchDisjunctionQuery(doc, disjQ) {
				return false
			}
		} else {
			// Single query in Should - just check it matches
			if !documentMatchesQuery(doc, q.Should) {
				return false
			}
		}
	}

	// MustNot clauses (none should match)
	if q.MustNot != nil {
		if disjQ, ok := q.MustNot.(*query.DisjunctionQuery); ok {
			for _, subQ := range disjQ.Disjuncts {
				if documentMatchesQuery(doc, subQ) {
					return false
				}
			}
		} else {
			// Single query in MustNot
			if documentMatchesQuery(doc, q.MustNot) {
				return false
			}
		}
	}

	return true
}

func matchConjunctionQuery(doc map[string]any, q *query.ConjunctionQuery) bool {
	for _, subQ := range q.Conjuncts {
		if !documentMatchesQuery(doc, subQ) {
			return false
		}
	}
	return true
}

func matchDisjunctionQuery(doc map[string]any, q *query.DisjunctionQuery) bool {
	if len(q.Disjuncts) == 0 {
		return true
	}
	for _, subQ := range q.Disjuncts {
		if documentMatchesQuery(doc, subQ) {
			return true
		}
	}
	return false
}
