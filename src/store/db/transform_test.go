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
	"testing"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeJSONPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"with prefix", "$.foo.bar", "$.foo.bar"},
		{"without prefix", "foo.bar", "$.foo.bar"},
		{"empty", "", "$."},
		{"just $", "$", "$"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeJSONPath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseSimplePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
		wantErr  bool
	}{
		{"simple path", "$.foo.bar", []string{"foo", "bar"}, false},
		{"without prefix", "foo.bar", []string{"foo", "bar"}, false},
		{"single field", "$.name", []string{"name"}, false},
		{"root", "$.", []string{}, false},
		{"nested", "$.user.profile.name", []string{"user", "profile", "name"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSimplePath(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetNestedValue(t *testing.T) {
	doc := map[string]any{
		"user": map[string]any{
			"name": "John",
			"age":  30,
		},
		"tags": []any{"go", "rust"},
	}

	tests := []struct {
		name     string
		parts    []string
		expected any
		exists   bool
	}{
		{"simple field", []string{"tags"}, []any{"go", "rust"}, true},
		{"nested field", []string{"user", "name"}, "John", true},
		{"missing field", []string{"missing"}, nil, false},
		{"missing nested", []string{"user", "missing"}, nil, false},
		{"empty path", []string{}, doc, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, exists := getNestedValue(doc, tt.parts)
			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSetNestedValue(t *testing.T) {
	tests := []struct {
		name     string
		doc      map[string]any
		parts    []string
		value    any
		expected map[string]any
	}{
		{
			name:  "simple field",
			doc:   map[string]any{},
			parts: []string{"name"},
			value: "John",
			expected: map[string]any{
				"name": "John",
			},
		},
		{
			name:  "nested field - create intermediate",
			doc:   map[string]any{},
			parts: []string{"user", "name"},
			value: "John",
			expected: map[string]any{
				"user": map[string]any{
					"name": "John",
				},
			},
		},
		{
			name: "nested field - existing intermediate",
			doc: map[string]any{
				"user": map[string]any{
					"age": 30,
				},
			},
			parts: []string{"user", "name"},
			value: "John",
			expected: map[string]any{
				"user": map[string]any{
					"age":  30,
					"name": "John",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := setNestedValue(tt.doc, tt.parts, tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveNestedValue(t *testing.T) {
	tests := []struct {
		name     string
		doc      map[string]any
		parts    []string
		expected map[string]any
	}{
		{
			name: "simple field",
			doc: map[string]any{
				"name": "John",
				"age":  30,
			},
			parts: []string{"name"},
			expected: map[string]any{
				"age": 30,
			},
		},
		{
			name: "nested field",
			doc: map[string]any{
				"user": map[string]any{
					"name": "John",
					"age":  30,
				},
			},
			parts: []string{"user", "name"},
			expected: map[string]any{
				"user": map[string]any{
					"age": 30,
				},
			},
		},
		{
			name: "missing field - no-op",
			doc: map[string]any{
				"name": "John",
			},
			parts: []string{"missing"},
			expected: map[string]any{
				"name": "John",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeNestedValue(tt.doc, tt.parts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSetOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     any
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "set simple field",
			doc:   map[string]any{},
			path:  "$.name",
			value: "John",
			expected: map[string]any{
				"name": "John",
			},
		},
		{
			name:  "set nested field",
			doc:   map[string]any{},
			path:  "$.user.name",
			value: "John",
			expected: map[string]any{
				"user": map[string]any{
					"name": "John",
				},
			},
		},
		{
			name: "overwrite existing",
			doc: map[string]any{
				"name": "Old",
			},
			path:  "name",
			value: "New",
			expected: map[string]any{
				"name": "New",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := setOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestUnsetOp(t *testing.T) {
	tests := []struct {
		name     string
		doc      map[string]any
		path     string
		expected map[string]any
	}{
		{
			name: "remove simple field",
			doc: map[string]any{
				"name": "John",
				"age":  30,
			},
			path: "$.name",
			expected: map[string]any{
				"age": 30,
			},
		},
		{
			name: "remove nested field",
			doc: map[string]any{
				"user": map[string]any{
					"name": "John",
					"age":  30,
				},
			},
			path: "user.name",
			expected: map[string]any{
				"user": map[string]any{
					"age": 30,
				},
			},
		},
		{
			name: "remove missing field - no-op",
			doc: map[string]any{
				"name": "John",
			},
			path: "$.missing",
			expected: map[string]any{
				"name": "John",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unsetOp(tt.doc, tt.path)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIncOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		delta     float64
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "increment existing",
			doc:   map[string]any{"count": float64(10)},
			path:  "count",
			delta: 5,
			expected: map[string]any{
				"count": float64(15),
			},
		},
		{
			name:  "increment missing - initialize",
			doc:   map[string]any{},
			path:  "$.count",
			delta: 5,
			expected: map[string]any{
				"count": float64(5),
			},
		},
		{
			name:  "decrement",
			doc:   map[string]any{"count": float64(10)},
			path:  "count",
			delta: -3,
			expected: map[string]any{
				"count": float64(7),
			},
		},
		{
			name:      "increment non-numeric",
			doc:       map[string]any{"name": "John"},
			path:      "name",
			delta:     5,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deltaBytes, err := json.Marshal(tt.delta)
			require.NoError(t, err)

			result, err := incOp(tt.doc, tt.path, deltaBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestPushOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     any
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "push to existing array",
			doc:   map[string]any{"tags": []any{"go"}},
			path:  "tags",
			value: "rust",
			expected: map[string]any{
				"tags": []any{"go", "rust"},
			},
		},
		{
			name:  "push to missing array - create",
			doc:   map[string]any{},
			path:  "$.tags",
			value: "go",
			expected: map[string]any{
				"tags": []any{"go"},
			},
		},
		{
			name:      "push to non-array",
			doc:       map[string]any{"tags": "not-array"},
			path:      "tags",
			value:     "go",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := pushOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestPullOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     any
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "pull matching elements",
			doc:   map[string]any{"tags": []any{"go", "rust", "go"}},
			path:  "tags",
			value: "go",
			expected: map[string]any{
				"tags": []any{"rust"},
			},
		},
		{
			name:  "pull non-matching - no change",
			doc:   map[string]any{"tags": []any{"go", "rust"}},
			path:  "tags",
			value: "python",
			expected: map[string]any{
				"tags": []any{"go", "rust"},
			},
		},
		{
			name:     "pull from missing array - no-op",
			doc:      map[string]any{},
			path:     "$.tags",
			value:    "go",
			expected: map[string]any{},
		},
		{
			name:      "pull from non-array",
			doc:       map[string]any{"tags": "not-array"},
			path:      "tags",
			value:     "go",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := pullOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestAddToSetOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     any
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "add to existing array - new value",
			doc:   map[string]any{"tags": []any{"go"}},
			path:  "tags",
			value: "rust",
			expected: map[string]any{
				"tags": []any{"go", "rust"},
			},
		},
		{
			name:  "add to existing array - duplicate",
			doc:   map[string]any{"tags": []any{"go", "rust"}},
			path:  "tags",
			value: "go",
			expected: map[string]any{
				"tags": []any{"go", "rust"},
			},
		},
		{
			name:  "add to missing array - create",
			doc:   map[string]any{},
			path:  "$.tags",
			value: "go",
			expected: map[string]any{
				"tags": []any{"go"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := addToSetOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestPopOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		position  int
		expected  map[string]any
		wantError bool
	}{
		{
			name:     "pop first element",
			doc:      map[string]any{"tags": []any{"go", "rust", "python"}},
			path:     "tags",
			position: -1,
			expected: map[string]any{
				"tags": []any{"rust", "python"},
			},
		},
		{
			name:     "pop last element",
			doc:      map[string]any{"tags": []any{"go", "rust", "python"}},
			path:     "tags",
			position: 1,
			expected: map[string]any{
				"tags": []any{"go", "rust"},
			},
		},
		{
			name:     "pop from empty array - no-op",
			doc:      map[string]any{"tags": []any{}},
			path:     "tags",
			position: 1,
			expected: map[string]any{
				"tags": []any{},
			},
		},
		{
			name:     "pop from missing array - no-op",
			doc:      map[string]any{},
			path:     "$.tags",
			position: 1,
			expected: map[string]any{},
		},
		{
			name:      "invalid position",
			doc:       map[string]any{"tags": []any{"go"}},
			path:      "tags",
			position:  2,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			posBytes, err := json.Marshal(tt.position)
			require.NoError(t, err)

			result, err := popOp(tt.doc, tt.path, posBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMulOp(t *testing.T) {
	tests := []struct {
		name       string
		doc        map[string]any
		path       string
		multiplier float64
		expected   map[string]any
		wantError  bool
	}{
		{
			name:       "multiply existing",
			doc:        map[string]any{"count": float64(10)},
			path:       "count",
			multiplier: 2.5,
			expected: map[string]any{
				"count": float64(25),
			},
		},
		{
			name:       "multiply missing - set to 0",
			doc:        map[string]any{},
			path:       "$.count",
			multiplier: 5,
			expected: map[string]any{
				"count": float64(0),
			},
		},
		{
			name:       "multiply non-numeric",
			doc:        map[string]any{"name": "John"},
			path:       "name",
			multiplier: 5,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mulBytes, err := json.Marshal(tt.multiplier)
			require.NoError(t, err)

			result, err := mulOp(tt.doc, tt.path, mulBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMinOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     float64
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "min with smaller value",
			doc:   map[string]any{"score": float64(100)},
			path:  "score",
			value: 50,
			expected: map[string]any{
				"score": float64(50),
			},
		},
		{
			name:  "min with larger value - no change",
			doc:   map[string]any{"score": float64(50)},
			path:  "score",
			value: 100,
			expected: map[string]any{
				"score": float64(50),
			},
		},
		{
			name:  "min with missing field",
			doc:   map[string]any{},
			path:  "$.score",
			value: 50,
			expected: map[string]any{
				"score": float64(50),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := minOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMaxOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		path      string
		value     float64
		expected  map[string]any
		wantError bool
	}{
		{
			name:  "max with larger value",
			doc:   map[string]any{"score": float64(50)},
			path:  "score",
			value: 100,
			expected: map[string]any{
				"score": float64(100),
			},
		},
		{
			name:  "max with smaller value - no change",
			doc:   map[string]any{"score": float64(100)},
			path:  "score",
			value: 50,
			expected: map[string]any{
				"score": float64(100),
			},
		},
		{
			name:  "max with missing field",
			doc:   map[string]any{},
			path:  "$.score",
			value: 100,
			expected: map[string]any{
				"score": float64(100),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueBytes, err := json.Marshal(tt.value)
			require.NoError(t, err)

			result, err := maxOp(tt.doc, tt.path, valueBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCurrentDateOp(t *testing.T) {
	doc := map[string]any{}
	result, err := currentDateOp(doc, "$.updatedAt")
	assert.NoError(t, err)

	// Check that updatedAt was set
	updatedAt, exists := result["updatedAt"]
	assert.True(t, exists)
	assert.IsType(t, "", updatedAt)

	// Verify it's a valid timestamp string
	timestampStr, ok := updatedAt.(string)
	assert.True(t, ok)
	assert.NotEmpty(t, timestampStr)
}

func TestRenameOp(t *testing.T) {
	tests := []struct {
		name      string
		doc       map[string]any
		oldPath   string
		newPath   string
		expected  map[string]any
		wantError bool
	}{
		{
			name: "rename simple field",
			doc: map[string]any{
				"name": "John",
				"age":  30,
			},
			oldPath: "name",
			newPath: "fullName",
			expected: map[string]any{
				"fullName": "John",
				"age":      30,
			},
		},
		{
			name: "rename nested field",
			doc: map[string]any{
				"user": map[string]any{
					"name": "John",
				},
			},
			oldPath: "user.name",
			newPath: "user.fullName",
			expected: map[string]any{
				"user": map[string]any{
					"fullName": "John",
				},
			},
		},
		{
			name: "rename missing field - no-op",
			doc: map[string]any{
				"name": "John",
			},
			oldPath: "$.missing",
			newPath: "other",
			expected: map[string]any{
				"name": "John",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newPathBytes, err := json.Marshal(tt.newPath)
			require.NoError(t, err)

			result, err := renameOp(tt.doc, tt.oldPath, newPathBytes)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestApplyTransformOp(t *testing.T) {
	// Test applying operations through the main dispatcher
	doc := map[string]any{
		"name":  "John",
		"count": float64(10),
		"tags":  []any{"go"},
	}

	// Test $set
	op1 := &TransformOp{}
	op1.SetPath("name")
	op1.SetOp(TransformOp_SET)
	op1.SetValue([]byte(`"Jane"`))
	result, err := ApplyTransformOp(doc, op1)
	assert.NoError(t, err)
	assert.Equal(t, "Jane", result["name"])

	// Test $inc
	op2 := &TransformOp{}
	op2.SetPath("count")
	op2.SetOp(TransformOp_INC)
	op2.SetValue([]byte(`5`))
	result, err = ApplyTransformOp(result, op2)
	assert.NoError(t, err)
	assert.Equal(t, float64(15), result["count"])

	// Test $push
	op3 := &TransformOp{}
	op3.SetPath("tags")
	op3.SetOp(TransformOp_PUSH)
	op3.SetValue([]byte(`"rust"`))
	result, err = ApplyTransformOp(result, op3)
	assert.NoError(t, err)
	assert.Equal(t, []any{"go", "rust"}, result["tags"])
}
