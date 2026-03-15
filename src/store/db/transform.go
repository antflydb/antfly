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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/evaluator"
	json "github.com/antflydb/antfly/pkg/libaf/json"
)

// normalizeJSONPath adds $. prefix if missing
// Follows existing pattern from db.go:1850 and aknn_v0.go:310
func normalizeJSONPath(path string) string {
	if len(path) == 0 || path[0] != '$' {
		return "$." + path
	}
	return path
}

// parseSimplePath parses "$.foo.bar" into ["foo", "bar"]
func parseSimplePath(path string) ([]string, error) {
	normalized := normalizeJSONPath(path)

	// Remove $. prefix
	if !strings.HasPrefix(normalized, "$.") {
		return nil, fmt.Errorf("invalid JSONPath: %s", path)
	}

	pathStr := strings.TrimPrefix(normalized, "$.")
	if pathStr == "" {
		return []string{}, nil
	}

	return strings.Split(pathStr, "."), nil
}

// getNestedValue retrieves value at nested path
func getNestedValue(doc map[string]any, parts []string) (any, bool) {
	if len(parts) == 0 {
		return doc, true
	}

	current := any(doc)
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}

		val, exists := m[part]
		if !exists {
			return nil, false
		}
		current = val
	}

	return current, true
}

// setNestedValue sets value at nested path, creating intermediate objects as needed
func setNestedValue(doc map[string]any, parts []string, value any) map[string]any {
	if len(parts) == 0 {
		return doc
	}

	// Navigate to parent
	current := doc
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]

		// Get or create intermediate object
		if next, ok := current[part]; ok {
			if nextMap, ok := next.(map[string]any); ok {
				current = nextMap
			} else {
				// Overwrite non-object with new object
				newMap := make(map[string]any)
				current[part] = newMap
				current = newMap
			}
		} else {
			newMap := make(map[string]any)
			current[part] = newMap
			current = newMap
		}
	}

	// Set final value
	current[parts[len(parts)-1]] = value
	return doc
}

// removeNestedValue removes value at nested path
func removeNestedValue(doc map[string]any, parts []string) map[string]any {
	if len(parts) == 0 {
		return doc
	}

	// Navigate to parent
	current := doc
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		next, ok := current[part]
		if !ok {
			return doc // Path doesn't exist
		}

		nextMap, ok := next.(map[string]any)
		if !ok {
			return doc // Not a map
		}
		current = nextMap
	}

	// Remove final key
	delete(current, parts[len(parts)-1])
	return doc
}

// ApplyTransformOp applies a single transform operation to a document
func ApplyTransformOp(doc map[string]any, op *TransformOp) (map[string]any, error) {
	switch op.GetOp() {
	case TransformOp_SET:
		return setOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_UNSET:
		return unsetOp(doc, op.GetPath())
	case TransformOp_INC:
		return incOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_PUSH:
		return pushOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_PULL:
		return pullOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_ADD_TO_SET:
		return addToSetOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_POP:
		return popOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_MUL:
		return mulOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_MIN:
		return minOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_MAX:
		return maxOp(doc, op.GetPath(), op.GetValue())
	case TransformOp_CURRENT_DATE:
		return currentDateOp(doc, op.GetPath())
	case TransformOp_RENAME:
		return renameOp(doc, op.GetPath(), op.GetValue())
	default:
		return doc, fmt.Errorf("unknown transform operation: %v", op.GetOp())
	}
}

// setOp implements $set operator
func setOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value any
	if len(valueBytes) > 0 {
		if err := json.Unmarshal(valueBytes, &value); err != nil {
			return doc, fmt.Errorf("unmarshal value: %w", err)
		}
	}

	return setNestedValue(doc, parts, value), nil
}

// unsetOp implements $unset operator
func unsetOp(doc map[string]any, path string) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	return removeNestedValue(doc, parts), nil
}

// incOp implements $inc operator
func incOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var delta float64
	if err := json.Unmarshal(valueBytes, &delta); err != nil {
		return doc, fmt.Errorf("delta must be numeric: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// Initialize to delta if missing
		return setNestedValue(doc, parts, delta), nil
	}

	currentNum, ok := evaluator.ToFloat64(current)
	if !ok {
		return doc, fmt.Errorf("cannot increment non-numeric field at %s", path)
	}

	return setNestedValue(doc, parts, currentNum+delta), nil
}

// pushOp implements $push operator
func pushOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value any
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return doc, fmt.Errorf("unmarshal value: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// Create new array
		return setNestedValue(doc, parts, []any{value}), nil
	}

	arr, ok := current.([]any)
	if !ok {
		return doc, fmt.Errorf("cannot push to non-array field at %s", path)
	}

	arr = append(arr, value)
	return setNestedValue(doc, parts, arr), nil
}

// pullOp implements $pull operator
func pullOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value any
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return doc, fmt.Errorf("unmarshal value: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		return doc, nil // No-op if array doesn't exist
	}

	arr, ok := current.([]any)
	if !ok {
		return doc, fmt.Errorf("cannot pull from non-array field at %s", path)
	}

	// Remove matching elements
	result := make([]any, 0, len(arr))
	for _, item := range arr {
		if !reflect.DeepEqual(item, value) {
			result = append(result, item)
		}
	}

	return setNestedValue(doc, parts, result), nil
}

// addToSetOp implements $addToSet operator
func addToSetOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value any
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return doc, fmt.Errorf("unmarshal value: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// Create new array
		return setNestedValue(doc, parts, []any{value}), nil
	}

	arr, ok := current.([]any)
	if !ok {
		return doc, fmt.Errorf("cannot addToSet on non-array field at %s", path)
	}

	// Check if value already exists
	for _, item := range arr {
		if reflect.DeepEqual(item, value) {
			return doc, nil // Already exists, no-op
		}
	}

	arr = append(arr, value)
	return setNestedValue(doc, parts, arr), nil
}

// popOp implements $pop operator
func popOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var position int
	if err := json.Unmarshal(valueBytes, &position); err != nil {
		return doc, fmt.Errorf("position must be -1 or 1: %w", err)
	}

	if position != -1 && position != 1 {
		return doc, fmt.Errorf("position must be -1 (first) or 1 (last), got %d", position)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		return doc, nil // No-op if array doesn't exist
	}

	arr, ok := current.([]any)
	if !ok {
		return doc, fmt.Errorf("cannot pop from non-array field at %s", path)
	}

	if len(arr) == 0 {
		return doc, nil // No-op on empty array
	}

	var result []any
	if position == -1 {
		// Remove first element
		result = arr[1:]
	} else {
		// Remove last element
		result = arr[:len(arr)-1]
	}

	return setNestedValue(doc, parts, result), nil
}

// mulOp implements $mul operator
func mulOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var multiplier float64
	if err := json.Unmarshal(valueBytes, &multiplier); err != nil {
		return doc, fmt.Errorf("multiplier must be numeric: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// MongoDB behavior: missing field is treated as 0
		return setNestedValue(doc, parts, float64(0)), nil
	}

	currentNum, ok := evaluator.ToFloat64(current)
	if !ok {
		return doc, fmt.Errorf("cannot multiply non-numeric field at %s", path)
	}

	return setNestedValue(doc, parts, currentNum*multiplier), nil
}

// minOp implements $min operator
func minOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value float64
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return doc, fmt.Errorf("value must be numeric: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// Set to value if missing
		return setNestedValue(doc, parts, value), nil
	}

	currentNum, ok := evaluator.ToFloat64(current)
	if !ok {
		return doc, fmt.Errorf("cannot compare non-numeric field at %s", path)
	}

	if value < currentNum {
		return setNestedValue(doc, parts, value), nil
	}

	return doc, nil
}

// maxOp implements $max operator
func maxOp(doc map[string]any, path string, valueBytes []byte) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	var value float64
	if err := json.Unmarshal(valueBytes, &value); err != nil {
		return doc, fmt.Errorf("value must be numeric: %w", err)
	}

	current, exists := getNestedValue(doc, parts)
	if !exists {
		// Set to value if missing
		return setNestedValue(doc, parts, value), nil
	}

	currentNum, ok := evaluator.ToFloat64(current)
	if !ok {
		return doc, fmt.Errorf("cannot compare non-numeric field at %s", path)
	}

	if value > currentNum {
		return setNestedValue(doc, parts, value), nil
	}

	return doc, nil
}

// currentDateOp implements $currentDate operator
func currentDateOp(doc map[string]any, path string) (map[string]any, error) {
	parts, err := parseSimplePath(path)
	if err != nil {
		return doc, err
	}

	timestamp := time.Now().Format(time.RFC3339Nano)
	return setNestedValue(doc, parts, timestamp), nil
}

// renameOp implements $rename operator
func renameOp(doc map[string]any, oldPath string, newPathBytes []byte) (map[string]any, error) {
	oldParts, err := parseSimplePath(oldPath)
	if err != nil {
		return doc, err
	}

	var newPath string
	if err := json.Unmarshal(newPathBytes, &newPath); err != nil {
		return doc, fmt.Errorf("unmarshal new path: %w", err)
	}

	newParts, err := parseSimplePath(newPath)
	if err != nil {
		return doc, err
	}

	// Get value from old path
	value, exists := getNestedValue(doc, oldParts)
	if !exists {
		return doc, nil // No-op if old path doesn't exist
	}

	// Remove from old path
	doc = removeNestedValue(doc, oldParts)

	// Set at new path
	doc = setNestedValue(doc, newParts, value)

	return doc, nil
}
