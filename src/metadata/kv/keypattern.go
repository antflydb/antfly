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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"strings"
)

// KeyPattern represents a pattern for matching keys with parameter extraction.
// Supports patterns like:
// - "tables/{tableID}/shards/{shardID}" - named parameters
// - "tables/*/shards/*" - wildcard matching
// - "prefix/" - simple prefix matching
type KeyPattern struct {
	// Pattern is the pattern string, e.g., "tables/{tableID}/shards/{shardID}"
	Pattern string

	// Handler is called when a key matches this pattern
	// params contains extracted parameter values from the pattern
	Handler func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error

	// compiled pattern information
	segments     []patternSegment
	separator    string // separator used (":" or "/")
	hasWildcards bool
}

type patternSegment struct {
	literal    string // literal string to match
	paramName  string // parameter name if this is a parameter segment
	isParam    bool   // true if this is a {param} segment
	isWildcard bool   // true if this is a * segment
}

// NewKeyPattern creates a new key pattern from a pattern string.
// Pattern syntax:
// - Literal segments: "tables" matches exactly "tables"
// - Named parameters: "{tableID}" matches any segment and extracts it as "tableID"
// - Wildcards: "*" matches any segment but doesn't extract it
// - Separators: Auto-detected (":" for metadata keys like "tm:t:users", "/" for paths like "tables/users")
//
// Examples with "/" separator:
// - "tables/{tableID}/shards/{shardID}" - matches "tables/foo/shards/bar" with params {"tableID": "foo", "shardID": "bar"}
// - "tables/*/shards/*" - matches "tables/foo/shards/bar" with no params
// - "tables/" - simple prefix match for anything starting with "tables/"
//
// Examples with ":" separator (metadata keys):
// - "tm:t:{tableName}" - matches "tm:t:users" with params {"tableName": "users"}
// - "tm:t:{tableName}" - does NOT match "tm:t:users:i:myindex" (segment count must match)
// - "tm:shs:{shardID}" - matches "tm:shs:shard1" with params {"shardID": "shard1"}
func NewKeyPattern(pattern string, handler func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error) (*KeyPattern, error) {
	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	kp := &KeyPattern{
		Pattern: pattern,
		Handler: handler,
	}

	// Parse the pattern into segments
	if err := kp.compile(); err != nil {
		return nil, err
	}

	return kp, nil
}

func (kp *KeyPattern) compile() error {
	patternStr := kp.Pattern

	// Determine separator based on pattern
	// Use ":" for metadata keys (tm:t:users), "/" for hierarchical paths (tables/users)
	kp.separator = ":"
	if strings.Contains(patternStr, "/") {
		kp.separator = "/"
	}

	// Split by separator
	parts := strings.SplitSeq(patternStr, kp.separator)

	for part := range parts {
		if part == "" {
			continue // skip empty segments
		}

		seg := patternSegment{}

		// Check if it's a parameter {name}
		if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			paramName := strings.TrimPrefix(strings.TrimSuffix(part, "}"), "{")
			if paramName == "" {
				return fmt.Errorf("empty parameter name in pattern: %s", kp.Pattern)
			}
			seg.isParam = true
			seg.paramName = paramName
			kp.hasWildcards = true
		} else if part == "*" {
			// Wildcard
			seg.isWildcard = true
			kp.hasWildcards = true
		} else {
			// Literal segment
			seg.literal = part
		}

		kp.segments = append(kp.segments, seg)
	}

	return nil
}

// Match checks if a key matches this pattern and extracts parameters.
// Returns (matches, params, error)
func (kp *KeyPattern) Match(key []byte) (bool, map[string]string, error) {
	keyStr := string(key)
	keyParts := strings.Split(keyStr, kp.separator)

	// Remove empty parts
	var cleanParts []string
	for _, part := range keyParts {
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}
	keyParts = cleanParts

	// If no wildcards, check if it's a simple prefix match
	if !kp.hasWildcards {
		// Simple prefix matching
		prefix := kp.Pattern
		if strings.HasPrefix(keyStr, prefix) {
			return true, nil, nil
		}
		return false, nil, nil
	}

	// Must have same number of segments for wildcard patterns
	// This prevents over-matching: "tm:t:{tableName}" won't match "tm:t:users:i:indexName"
	if len(keyParts) != len(kp.segments) {
		return false, nil, nil
	}

	params := make(map[string]string)

	for i, seg := range kp.segments {
		keyPart := keyParts[i]

		if seg.isParam {
			// Extract parameter
			params[seg.paramName] = keyPart
		} else if seg.isWildcard {
			// Wildcard matches anything, don't extract
			continue
		} else {
			// Literal must match exactly
			if keyPart != seg.literal {
				return false, nil, nil
			}
		}
	}

	return true, params, nil
}

// String returns the pattern string
func (kp *KeyPattern) String() string {
	return kp.Pattern
}

// KeyPrefixListener is a simpler prefix-based listener (backward compatibility)
type KeyPrefixListener struct {
	Prefix  []byte
	Handler func(ctx context.Context, key, value []byte, isDelete bool) error
}

// Match checks if a key matches this prefix
func (kpl *KeyPrefixListener) Match(key []byte) bool {
	return bytes.HasPrefix(key, kpl.Prefix)
}
