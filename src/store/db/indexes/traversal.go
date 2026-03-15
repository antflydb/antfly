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

package indexes

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"
)

// DefaultTraversalRules returns sensible defaults for traversal
func DefaultTraversalRules() TraversalRules {
	deduplicateNodes := true
	direction := EdgeDirectionOut
	maxDepth := 3
	maxResults := 100
	includePaths := false
	minWeight := 0.0
	maxWeight := 1.0

	return TraversalRules{
		EdgeTypes:        nil,              // All edge types
		MinWeight:        minWeight,        // No minimum
		MaxWeight:        maxWeight,        // Maximum possible
		Direction:        direction,        // Outgoing edges
		MaxDepth:         maxDepth,         // 3 hops
		MaxResults:       maxResults,       // Limit results
		IncludePaths:     includePaths,     // Don't include paths by default
		DeduplicateNodes: deduplicateNodes, // Visit each node once
	}
}

// ShouldTraverseEdge checks if an edge passes the traversal filters
func (r *TraversalRules) ShouldTraverseEdge(edge Edge) bool {
	// Check weight range
	if r.MinWeight != 0 && edge.Weight < r.MinWeight {
		return false
	}
	if r.MaxWeight != 0 && edge.Weight > r.MaxWeight {
		return false
	}

	// Check edge type filter
	if len(r.EdgeTypes) > 0 {
		found := slices.Contains(r.EdgeTypes, edge.Type)
		if !found {
			return false
		}
	}

	return true
}

// EncodeEdgeValue serializes edge data to binary format
// Format: [8:weight][8:created_at][8:updated_at][n:metadata_json]
func EncodeEdgeValue(edge *Edge) ([]byte, error) {
	metadataJSON := []byte("{}")
	if len(edge.Metadata) > 0 {
		var err error
		metadataJSON, err = json.Marshal(edge.Metadata)
		if err != nil {
			return nil, fmt.Errorf("encoding edge metadata: %w", err)
		}
	}

	buf := make([]byte, 24+len(metadataJSON))

	// Weight as float64 (8 bytes)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(edge.Weight))

	// Created timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(edge.CreatedAt.Unix())) //nolint:gosec // G115: bounded value, cannot overflow in practice

	// Updated timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(edge.UpdatedAt.Unix())) //nolint:gosec // G115: bounded value, cannot overflow in practice

	// Metadata JSON (remaining bytes)
	copy(buf[24:], metadataJSON)

	return buf, nil
}

// DecodeEdgeValue deserializes edge data from binary format
func DecodeEdgeValue(data []byte) (*Edge, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("edge value too short: %d bytes", len(data))
	}

	edge := &Edge{}

	// Decode weight
	edge.Weight = math.Float64frombits(binary.LittleEndian.Uint64(data[0:8]))

	// Decode timestamps
	edge.CreatedAt = time.Unix(int64(binary.LittleEndian.Uint64(data[8:16])), 0)  //nolint:gosec // G115: bounded value, cannot overflow in practice
	edge.UpdatedAt = time.Unix(int64(binary.LittleEndian.Uint64(data[16:24])), 0) //nolint:gosec // G115: bounded value, cannot overflow in practice

	// Decode metadata JSON
	if len(data) > 24 {
		if err := json.Unmarshal(data[24:], &edge.Metadata); err != nil {
			return nil, fmt.Errorf("decoding edge metadata: %w", err)
		}
	}

	return edge, nil
}
