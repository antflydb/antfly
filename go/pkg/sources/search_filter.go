// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"encoding/json"
	"fmt"
	"strings"
)

func bindingPointer(field string) string {
	parts := strings.Split(field, ".")
	for i, part := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(parts, "/")
}

// Catalog ownership filters predate the native canonical AST. Vector queries
// require canonical predicates; translate only understood legacy operators,
// preserving all constraints. Unknown filters are rejected, never dropped.
func canonicalSearchFilter(raw json.RawMessage) (any, error) {
	var object map[string]any
	if len(raw) == 0 || json.Unmarshal(raw, &object) != nil || len(object) == 0 {
		return nil, fmt.Errorf("source search requires an explicit scope filter")
	}
	var convert func(map[string]any, int) (any, error)
	convert = func(node map[string]any, depth int) (any, error) {
		if depth > 64 {
			return nil, fmt.Errorf("source filter exceeds depth budget")
		}
		if children, ok := node["conjuncts"]; ok {
			if len(node) != 1 {
				return nil, fmt.Errorf("unsupported mixed source filter")
			}
			values, ok := children.([]any)
			if !ok || len(values) == 0 {
				return nil, fmt.Errorf("invalid source conjunction")
			}
			out := []any{}
			for _, child := range values {
				value, ok := child.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid source filter child")
				}
				mapped, err := convert(value, depth+1)
				if err != nil {
					return nil, err
				}
				out = append(out, mapped)
			}
			return map[string]any{"bool": map[string]any{"filter": out}}, nil
		}
		if value, ok := node["equals"]; ok {
			field, ok := node["field"].(string)
			if !ok || !validBindingName(field) || len(node) != 2 {
				return nil, fmt.Errorf("invalid source equality")
			}
			return map[string]any{"term": map[string]any{"path": bindingPointer(field), "value": value}}, nil
		}
		// Already canonical predicates retain their complete JSON structure.
		for _, key := range []string{"bool", "term", "exists", "doc_id", "match_all"} {
			if _, ok := node[key]; ok && len(node) == 1 {
				return node, nil
			}
		}
		return nil, fmt.Errorf("unsupported source filter for ranked retrieval")
	}
	return convert(object, 0)
}
