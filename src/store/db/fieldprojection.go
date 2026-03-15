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
	"strings"
)

// ProjectFields applies field projection patterns to a document
// Supports:
// - Simple fields: "title", "author"
// - Nested paths: "_chunks.my_index.0._content"
// - Wildcard patterns: "_chunks.*", "_chunks.*._content"
// - Exclusion patterns: "-_chunks.*._content" (exclude _content from all chunks)
func ProjectFields(doc map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		// Empty fields list means no fields should be included (key-only mode)
		return make(map[string]any)
	}

	// Separate inclusion and exclusion patterns
	includes := make([]string, 0)
	excludes := make([]string, 0)

	for _, field := range fields {
		if after, ok := strings.CutPrefix(field, "-"); ok {
			excludes = append(excludes, after)
		} else {
			includes = append(includes, field)
		}
	}

	// If there are inclusions, start with empty and add matching fields
	// If only exclusions, start with full doc and remove matching fields
	var result map[string]any
	if len(includes) > 0 {
		result = make(map[string]any)
		for _, pattern := range includes {
			applyIncludePattern(doc, result, pattern)
		}
	} else {
		// Deep copy the document for exclusion-only mode
		result = deepCopy(doc)
	}

	// Apply exclusions
	for _, pattern := range excludes {
		applyExcludePattern(result, pattern)
	}

	return result
}

// applyIncludePattern includes fields matching the pattern
func applyIncludePattern(src, dst map[string]any, pattern string) {
	parts := strings.Split(pattern, ".")
	applyIncludeRecursive(src, dst, parts, 0)
}

// applyIncludeRecursive recursively includes fields
func applyIncludeRecursive(src, dst map[string]any, parts []string, depth int) {
	if depth >= len(parts) {
		return
	}

	part := parts[depth]

	if part == "*" {
		// Wildcard: include all keys at this level
		for key, val := range src {
			includeField(key, val, dst, parts, depth)
		}
	} else {
		// Exact key match
		if val, ok := src[part]; ok {
			includeField(part, val, dst, parts, depth)
		}
	}
}

// includeField includes a single field value into the destination map,
// handling terminal values, nested maps, and slices.
func includeField(key string, val any, dst map[string]any, parts []string, depth int) {
	if depth == len(parts)-1 {
		// Last part: include the whole value
		dst[key] = val
		return
	}

	// Not last: recurse into nested structure
	if nestedSrc, ok := val.(map[string]any); ok {
		var nestedDst map[string]any
		if existing, exists := dst[key]; exists {
			if nd, ok := existing.(map[string]any); ok {
				nestedDst = nd
			} else {
				nestedDst = make(map[string]any)
				dst[key] = nestedDst
			}
		} else {
			nestedDst = make(map[string]any)
			dst[key] = nestedDst
		}
		applyIncludeRecursive(nestedSrc, nestedDst, parts, depth+1)
	} else if sliceSrc, ok := val.([]any); ok {
		var result []any
		if existing, exists := dst[key]; exists {
			if r, ok := existing.([]any); ok {
				result = r
			} else {
				result = make([]any, 0)
			}
		} else {
			result = make([]any, 0)
		}

		for i, item := range sliceSrc {
			if itemMap, ok := item.(map[string]any); ok {
				var itemDst map[string]any
				if i < len(result) {
					if existingItem, ok := result[i].(map[string]any); ok {
						itemDst = existingItem
					} else {
						itemDst = make(map[string]any)
					}
				} else {
					itemDst = make(map[string]any)
				}
				applyIncludeRecursive(itemMap, itemDst, parts, depth+1)
				if i < len(result) {
					result[i] = itemDst
				} else {
					result = append(result, itemDst)
				}
			}
		}
		if len(result) > 0 {
			dst[key] = result
		}
	}
}

// applyExcludePattern removes fields matching the pattern
func applyExcludePattern(doc map[string]any, pattern string) {
	parts := strings.Split(pattern, ".")
	applyExcludeRecursive(doc, parts, 0)
}

// applyExcludeRecursive recursively excludes fields
func applyExcludeRecursive(doc map[string]any, parts []string, depth int) {
	if depth >= len(parts) {
		return
	}

	part := parts[depth]
	isLast := depth == len(parts)-1

	if part == "*" {
		// Wildcard: apply to all keys at this level
		for key, val := range doc {
			if isLast {
				// Last part: delete the key
				delete(doc, key)
			} else {
				// Not last: recurse into nested structure
				if nestedDoc, ok := val.(map[string]any); ok {
					applyExcludeRecursive(nestedDoc, parts, depth+1)
				} else if sliceDoc, ok := val.([]any); ok {
					// Handle arrays
					for _, item := range sliceDoc {
						if itemMap, ok := item.(map[string]any); ok {
							applyExcludeRecursive(itemMap, parts, depth+1)
						}
					}
				}
			}
		}
	} else {
		// Exact key match
		if val, ok := doc[part]; ok {
			if isLast {
				// Last part: delete the key
				delete(doc, part)
			} else {
				// Not last: recurse into nested structure
				if nestedDoc, ok := val.(map[string]any); ok {
					applyExcludeRecursive(nestedDoc, parts, depth+1)
				} else if sliceDoc, ok := val.([]any); ok {
					// Handle arrays
					for _, item := range sliceDoc {
						if itemMap, ok := item.(map[string]any); ok {
							applyExcludeRecursive(itemMap, parts, depth+1)
						}
					}
				}
			}
		}
	}
}

// deepCopy creates a deep copy of a map[string]any
func deepCopy(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for key, val := range src {
		switch v := val.(type) {
		case map[string]any:
			dst[key] = deepCopy(v)
		case []any:
			copied := make([]any, len(v))
			for i, item := range v {
				if itemMap, ok := item.(map[string]any); ok {
					copied[i] = deepCopy(itemMap)
				} else {
					copied[i] = item
				}
			}
			dst[key] = copied
		default:
			dst[key] = val
		}
	}
	return dst
}
