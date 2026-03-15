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

package storeutils

import (
	"bytes"
	"fmt"
)

// MakeEdgeKey constructs an edge key using index-scoped pattern
// Format: sourceKey:i:<indexName>:out:<edgeType>:<targetKey>:o
func MakeEdgeKey(source, target []byte, indexName, edgeType string) []byte {
	key := bytes.Clone(source)
	key = append(key, []byte(":i:")...)
	key = append(key, []byte(indexName)...)
	key = append(key, ':')
	key = append(key, []byte("out")...)
	key = append(key, ':')
	key = append(key, []byte(edgeType)...)
	key = append(key, ':')
	key = append(key, target...)
	key = append(key, EdgeOutSuffix...)

	return key
}

// ParseEdgeKey extracts components from an edge key
func ParseEdgeKey(edgeKey []byte) (source, target []byte, indexName, edgeType string, err error) {
	// Find :i: marker
	indexMarker := []byte(":i:")
	before, after, ok := bytes.Cut(edgeKey, indexMarker)
	if !ok {
		return nil, nil, "", "", fmt.Errorf("not an index-scoped key")
	}

	source = before
	remaining := after

	// Parse: <indexName>:out:<edgeType>:<target>:o
	parts := bytes.SplitN(remaining, []byte{':'}, 4)
	if len(parts) < 4 {
		return nil, nil, "", "", fmt.Errorf("malformed edge key")
	}

	indexName = string(parts[0])
	// parts[1] should be "out"
	edgeType = string(parts[2])

	// Remove :o suffix from the target
	targetWithSuffix := parts[3]
	if len(targetWithSuffix) < 2 || !bytes.HasSuffix(targetWithSuffix, []byte(":o")) {
		return nil, nil, "", "", fmt.Errorf("target too short or missing :o suffix")
	}
	target = targetWithSuffix[:len(targetWithSuffix)-2] // Remove :o

	return source, target, indexName, edgeType, nil
}

// EdgeIteratorPrefix constructs prefix for iterating edges
// Example: GetOutgoingEdges("paper_123", "citations", "cites") → "paper_123:i:citations:out:cites:"
func EdgeIteratorPrefix(key []byte, indexName string, edgeType string) []byte {
	prefix := bytes.Clone(key)
	prefix = append(prefix, []byte(":i:")...)
	prefix = append(prefix, []byte(indexName)...)
	prefix = append(prefix, []byte(":out:")...)
	if edgeType != "" {
		prefix = append(prefix, []byte(edgeType)...)
		prefix = append(prefix, ':')
	}
	return prefix
}
