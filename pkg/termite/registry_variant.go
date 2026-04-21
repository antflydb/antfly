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

package termite

import "strings"

// resolveRefName resolves a model name to the key used in the cache and
// ref-tracker. This ensures Acquire/Release use the same key that the
// cache eviction callback passes to deferCloseIfInUse.
//
// The caller must hold at least a read lock on the discovered map.
func resolveRefName[T any](requested string, discovered map[string]*T) string {
	if _, ok := discovered[requested]; ok {
		return requested
	}
	if _, resolved, ok := resolveVariant(requested, discovered); ok {
		return resolved
	}
	return requested
}

// resolveVariant looks up a model by exact name in the discovered map,
// falling back to variant resolution if no exact match is found.
// This handles the case where a model is pulled with --variants i8
// (creating only model_i8.onnx), which registers as "owner/model-i8"
// instead of "owner/model".
//
// When multiple variants match (e.g., -i8 and -f16 both exist),
// the shortest suffix wins for determinism.
//
// The caller must hold at least a read lock on the discovered map.
func resolveVariant[T any](requested string, discovered map[string]*T) (*T, string, bool) {
	// Exact match.
	if info, ok := discovered[requested]; ok {
		return info, requested, true
	}

	// Variant fallback: find entries prefixed with "requested-".
	prefix := requested + "-"
	var bestName string
	var bestInfo *T
	for name, info := range discovered {
		if strings.HasPrefix(name, prefix) {
			// Pick shortest suffix for determinism (e.g., "-i8" over "-i8-qt").
			if bestName == "" || len(name) < len(bestName) {
				bestName = name
				bestInfo = info
			}
		}
	}
	if bestName != "" {
		return bestInfo, bestName, true
	}
	return nil, "", false
}
