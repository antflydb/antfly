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
	"container/heap"
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/antflydb/antfly/src/store/db/indexes"
	"go.uber.org/zap"
)

// PathNode represents a node in the path-finding algorithm
type PathNode struct {
	key        []byte
	distance   float64 // For min_weight: sum; for max_weight: product (as negative for min-heap)
	hops       int
	parent     *PathNode
	parentEdge *indexes.Edge
	index      int // for heap operations
}

// PathNodeHeap implements heap.Interface for priority queue
type PathNodeHeap []*PathNode

func (h PathNodeHeap) Len() int { return len(h) }
func (h PathNodeHeap) Less(i, j int) bool {
	// Always prioritize distance first (works for all algorithms)
	// For BFS, distance equals hops anyway
	// For Dijkstra (min/max weight), distance is the key metric
	if h[i].distance == h[j].distance {
		// Tie-breaker: prefer fewer hops
		return h[i].hops < h[j].hops
	}
	return h[i].distance < h[j].distance
}
func (h PathNodeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *PathNodeHeap) Push(x any) {
	n := len(*h)
	node := x.(*PathNode)
	node.index = n
	*h = append(*h, node)
}

func (h *PathNodeHeap) Pop() any {
	old := *h
	n := len(old)
	node := old[n-1]
	old[n-1] = nil  // avoid memory leak
	node.index = -1 // for safety
	*h = old[0 : n-1]
	return node
}

// dijkstraConfig parameterizes Dijkstra's algorithm for min-weight vs max-weight modes.
type dijkstraConfig struct {
	// distanceFn computes the new cumulative distance given the current distance and edge weight.
	distanceFn func(currentDist, edgeWeight float64) float64
	// extraFilter rejects edges before distance calculation. Nil means no extra filtering.
	extraFilter func(edgeWeight float64) bool
	// logLabel is used in warning messages for debugging.
	logLabel string
}

var (
	dijkstraMinWeightConfig = dijkstraConfig{
		distanceFn:  func(d, w float64) float64 { return d + w },
		extraFilter: nil,
		logLabel:    "min-weight Dijkstra",
	}
	dijkstraMaxWeightConfig = dijkstraConfig{
		// max(product) = max(w1*w2*...) = min(-log(w1) - log(w2) - ...)
		distanceFn:  func(d, w float64) float64 { return d + (-math.Log(w)) },
		extraFilter: func(w float64) bool { return w > 0 }, // reject zero/negative for log
		logLabel:    "max-weight Dijkstra",
	}
)

// FindShortestPath finds a single shortest path between source and target
func (db *DBImpl) FindShortestPath(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	weightMode indexes.PathWeightMode,
	maxDepth int,
	minWeight, maxWeight float64,
) (*indexes.Path, error) {

	// Validate inputs
	if len(source) == 0 || len(target) == 0 {
		return nil, fmt.Errorf("source and target must not be empty")
	}
	if maxDepth <= 0 {
		maxDepth = 50 // default max depth
	}

	// Set default weight range if not specified
	if minWeight == 0 && maxWeight == 0 {
		minWeight = 0
		maxWeight = math.Inf(1) // No upper limit
	}

	// Early exit if source == target
	if string(source) == string(target) {
		return &indexes.Path{
			Nodes:       []string{base64.StdEncoding.EncodeToString(source)},
			Edges:       []indexes.PathEdge{},
			TotalWeight: 0.0,
			Length:      0,
		}, nil
	}

	// Choose algorithm based on weight mode
	var path *indexes.Path
	var err error
	switch weightMode {
	case "min_hops":
		path, err = db.bfsShortestPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, nil, nil)
	case "max_weight":
		path, err = db.dijkstraPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, nil, nil, dijkstraMaxWeightConfig)
	case "min_weight":
		path, err = db.dijkstraPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, nil, nil, dijkstraMinWeightConfig)
	default:
		return nil, fmt.Errorf("invalid weight_mode: %s (must be min_hops, max_weight, or min_weight)", weightMode)
	}

	if err != nil {
		return nil, err
	}
	if path == nil {
		return nil, fmt.Errorf("no path found between nodes")
	}
	return path, nil
}

// PathToKey generates a unique string key from a path's node sequence.
// Used for deduplication in k-shortest paths algorithm.
func PathToKey(path *indexes.Path) string {
	if path == nil || len(path.Nodes) == 0 {
		return ""
	}
	return strings.Join(path.Nodes, "->")
}

// FindShortestPathWithExclusions finds a single shortest path while excluding certain edges and nodes.
// Used by Yen's k-shortest paths algorithm.
func (db *DBImpl) FindShortestPathWithExclusions(
	ctx context.Context,
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
	// Validate inputs
	if len(source) == 0 || len(target) == 0 {
		return nil, fmt.Errorf("source and target must not be empty")
	}
	if maxDepth <= 0 {
		maxDepth = 50
	}

	// Set default weight range
	if minWeight == 0 && maxWeight == 0 {
		minWeight = 0
		maxWeight = math.Inf(1)
	}

	// Early exit if source == target
	if string(source) == string(target) {
		return &indexes.Path{
			Nodes:       []string{base64.StdEncoding.EncodeToString(source)},
			Edges:       []indexes.PathEdge{},
			TotalWeight: 0.0,
			Length:      0,
		}, nil
	}

	switch weightMode {
	case "min_hops":
		return db.bfsShortestPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes)
	case "max_weight":
		return db.dijkstraPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes, dijkstraMaxWeightConfig)
	case "min_weight":
		return db.dijkstraPath(ctx, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight, excludedEdges, excludedNodes, dijkstraMinWeightConfig)
	default:
		return nil, fmt.Errorf("invalid weight_mode: %s (must be min_hops, max_weight, or min_weight)", weightMode)
	}
}

// bfsShortestPath implements breadth-first search for unweighted shortest path.
// excludedEdges and excludedNodes may be nil when no exclusions are needed.
func (db *DBImpl) bfsShortestPath(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
	excludedEdges map[string]bool,
	excludedNodes map[string]bool,
) (*indexes.Path, error) {
	visited := make(map[string]bool)
	pq := &PathNodeHeap{}
	heap.Init(pq)

	// Start node
	startNode := &PathNode{
		key:      source,
		distance: 0,
		hops:     0,
		parent:   nil,
	}
	heap.Push(pq, startNode)
	visited[string(source)] = true

	targetStr := string(target)

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*PathNode)

		// Check if we've reached the target
		if string(current.key) == targetStr {
			return db.reconstructPath(current), nil
		}

		// Check max depth
		if current.hops >= maxDepth {
			continue
		}

		// Get neighbors
		edges, err := db.GetEdges(ctx, indexName, current.key, "", direction)
		if err != nil {
			db.logger.Warn("Failed to get edges during BFS",
				zap.String("key", string(current.key)),
				zap.Error(err))
			continue
		}

		// Process each edge
		for _, edge := range edges {
			// Filter by edge type
			if len(edgeTypes) > 0 && !slices.Contains(edgeTypes, edge.Type) {
				continue
			}

			// Filter by weight
			if edge.Weight < minWeight || edge.Weight > maxWeight {
				continue
			}

			// Check if edge is excluded
			if excludedEdges != nil {
				edgeKey := base64.StdEncoding.EncodeToString(edge.Source) + "->" +
					base64.StdEncoding.EncodeToString(edge.Target) + ":" + edge.Type
				if excludedEdges[edgeKey] {
					continue
				}
			}

			// Determine neighbor key based on direction
			var neighborKey []byte
			if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
				neighborKey = edge.Target
			} else {
				neighborKey = edge.Source
			}

			neighborStr := string(neighborKey)

			// Check if node is excluded
			if excludedNodes != nil {
				if excludedNodes[base64.StdEncoding.EncodeToString(neighborKey)] {
					continue
				}
			}

			// Skip if already visited
			if visited[neighborStr] {
				continue
			}

			// Add to queue (BFS marks visited on push, not on pop)
			visited[neighborStr] = true
			neighborNode := &PathNode{
				key:        neighborKey,
				distance:   current.distance + 1,
				hops:       current.hops + 1,
				parent:     current,
				parentEdge: &edge,
			}
			heap.Push(pq, neighborNode)
		}
	}

	// No path found
	return nil, nil
}

// dijkstraPath implements Dijkstra's algorithm parameterized by dijkstraConfig.
// Used for both min-weight (sum) and max-weight (product via negative log transform) modes.
// excludedEdges and excludedNodes may be nil when no exclusions are needed.
func (db *DBImpl) dijkstraPath(
	ctx context.Context,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
	excludedEdges map[string]bool,
	excludedNodes map[string]bool,
	cfg dijkstraConfig,
) (*indexes.Path, error) {
	distances := make(map[string]float64)
	visited := make(map[string]bool)
	pq := &PathNodeHeap{}
	heap.Init(pq)

	// Start node
	startNode := &PathNode{
		key:      source,
		distance: 0,
		hops:     0,
		parent:   nil,
	}
	heap.Push(pq, startNode)
	distances[string(source)] = 0

	targetStr := string(target)

	for pq.Len() > 0 {
		current := heap.Pop(pq).(*PathNode)
		currentStr := string(current.key)

		// Skip if already visited (Dijkstra marks visited on pop)
		if visited[currentStr] {
			continue
		}
		visited[currentStr] = true

		// Check if we've reached the target
		if currentStr == targetStr {
			return db.reconstructPath(current), nil
		}

		// Check max depth
		if current.hops >= maxDepth {
			continue
		}

		// Get neighbors
		edges, err := db.GetEdges(ctx, indexName, current.key, "", direction)
		if err != nil {
			db.logger.Warn("Failed to get edges during "+cfg.logLabel,
				zap.String("key", currentStr),
				zap.Error(err))
			continue
		}

		// Process each edge
		for _, edge := range edges {
			// Filter by edge type
			if len(edgeTypes) > 0 && !slices.Contains(edgeTypes, edge.Type) {
				continue
			}

			// Filter by weight
			if edge.Weight < minWeight || edge.Weight > maxWeight {
				continue
			}

			// Apply mode-specific filter (e.g., reject non-positive weights for log transform)
			if cfg.extraFilter != nil && !cfg.extraFilter(edge.Weight) {
				continue
			}

			// Check if edge is excluded
			if excludedEdges != nil {
				edgeKey := base64.StdEncoding.EncodeToString(edge.Source) + "->" +
					base64.StdEncoding.EncodeToString(edge.Target) + ":" + edge.Type
				if excludedEdges[edgeKey] {
					continue
				}
			}

			// Determine neighbor key
			var neighborKey []byte
			if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
				neighborKey = edge.Target
			} else {
				neighborKey = edge.Source
			}

			neighborStr := string(neighborKey)

			// Check if node is excluded
			if excludedNodes != nil {
				if excludedNodes[base64.StdEncoding.EncodeToString(neighborKey)] {
					continue
				}
			}

			if visited[neighborStr] {
				continue
			}

			// Calculate new distance using the mode-specific distance function
			newDistance := cfg.distanceFn(current.distance, edge.Weight)
			oldDistance, exists := distances[neighborStr]

			if !exists || newDistance < oldDistance {
				distances[neighborStr] = newDistance
				neighborNode := &PathNode{
					key:        neighborKey,
					distance:   newDistance,
					hops:       current.hops + 1,
					parent:     current,
					parentEdge: &edge,
				}
				heap.Push(pq, neighborNode)
			}
		}
	}

	// No path found
	return nil, nil
}

// reconstructPath builds the Path result from the final PathNode
func (db *DBImpl) reconstructPath(finalNode *PathNode) *indexes.Path {
	// Trace back from final node to start
	var nodes []string
	var edges []indexes.PathEdge

	current := finalNode
	for current != nil {
		nodes = append(nodes, base64.StdEncoding.EncodeToString(current.key))
		if current.parentEdge != nil {
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
	slices.Reverse(nodes)
	slices.Reverse(edges)

	// Calculate total weight
	totalWeight := 0.0
	for _, edge := range edges {
		totalWeight += edge.Weight
	}

	return &indexes.Path{
		Nodes:       nodes,
		Edges:       edges,
		TotalWeight: totalWeight,
		Length:      len(edges),
	}
}
