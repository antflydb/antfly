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
	"testing"

	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransformFromAPI tests the TransformFromAPI helper function
func TestTransformFromAPI(t *testing.T) {
	t.Run("ValidTransform", func(t *testing.T) {
		apiTransform := Transform{
			Key: "user:123",
			Operations: []TransformOp{
				{
					Op:    "$set",
					Path:  "$.name",
					Value: "John Doe",
				},
				{
					Op:    "$inc",
					Path:  "$.views",
					Value: 1,
				},
			},
			Upsert: true,
		}

		result, err := TransformFromAPI(apiTransform)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, []byte("user:123"), result.GetKey())
		assert.True(t, result.GetUpsert())

		ops := result.GetOperations()
		require.Len(t, ops, 2)

		// First operation: $set
		assert.Equal(t, db.TransformOp_SET, ops[0].GetOp())
		assert.Equal(t, "$.name", ops[0].GetPath())
		assert.NotNil(t, ops[0].GetValue())

		// Second operation: $inc
		assert.Equal(t, db.TransformOp_INC, ops[1].GetOp())
		assert.Equal(t, "$.views", ops[1].GetPath())
		assert.NotNil(t, ops[1].GetValue())
	})

	t.Run("EmptyKey", func(t *testing.T) {
		apiTransform := Transform{
			Key: "",
			Operations: []TransformOp{
				{Op: "$set", Path: "$.name", Value: "test"},
			},
		}

		result, err := TransformFromAPI(apiTransform)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "nonempty key required")
	})

	t.Run("NoUpsert", func(t *testing.T) {
		apiTransform := Transform{
			Key: "user:456",
			Operations: []TransformOp{
				{Op: "$set", Path: "$.status", Value: "active"},
			},
			Upsert: false,
		}

		result, err := TransformFromAPI(apiTransform)
		require.NoError(t, err)
		assert.False(t, result.GetUpsert())
	})

	t.Run("MultipleOperations", func(t *testing.T) {
		apiTransform := Transform{
			Key: "doc:789",
			Operations: []TransformOp{
				{Op: "$set", Path: "$.title", Value: "New Title"},
				{Op: "$unset", Path: "$.deprecated", Value: nil},
				{Op: "$push", Path: "$.tags", Value: "important"},
				{Op: "$inc", Path: "$.version", Value: 1},
			},
		}

		result, err := TransformFromAPI(apiTransform)
		require.NoError(t, err)

		ops := result.GetOperations()
		require.Len(t, ops, 4)

		assert.Equal(t, db.TransformOp_SET, ops[0].GetOp())
		assert.Equal(t, db.TransformOp_UNSET, ops[1].GetOp())
		assert.Equal(t, db.TransformOp_PUSH, ops[2].GetOp())
		assert.Equal(t, db.TransformOp_INC, ops[3].GetOp())
	})

	t.Run("AllOperatorTypes", func(t *testing.T) {
		apiTransform := Transform{
			Key: "test:ops",
			Operations: []TransformOp{
				{Op: "$set", Path: "$.a", Value: 1},
				{Op: "$unset", Path: "$.b"},
				{Op: "$inc", Path: "$.c", Value: 1},
				{Op: "$push", Path: "$.d", Value: "x"},
				{Op: "$pull", Path: "$.e", Value: "y"},
				{Op: "$addToSet", Path: "$.f", Value: "z"},
				{Op: "$pop", Path: "$.g", Value: -1},
				{Op: "$mul", Path: "$.h", Value: 2},
				{Op: "$min", Path: "$.i", Value: 10},
				{Op: "$max", Path: "$.j", Value: 100},
				{Op: "$currentDate", Path: "$.k"},
				{Op: "$rename", Path: "$.l", Value: "new_l"},
			},
		}

		result, err := TransformFromAPI(apiTransform)
		require.NoError(t, err)

		ops := result.GetOperations()
		require.Len(t, ops, 12)

		expectedOps := []db.TransformOp_OpType{
			db.TransformOp_SET,
			db.TransformOp_UNSET,
			db.TransformOp_INC,
			db.TransformOp_PUSH,
			db.TransformOp_PULL,
			db.TransformOp_ADD_TO_SET,
			db.TransformOp_POP,
			db.TransformOp_MUL,
			db.TransformOp_MIN,
			db.TransformOp_MAX,
			db.TransformOp_CURRENT_DATE,
			db.TransformOp_RENAME,
		}

		for i, expectedOp := range expectedOps {
			assert.Equal(t, expectedOp, ops[i].GetOp(), "Operation %d should match", i)
		}
	})
}

// TestParseTransformOpType tests the operator type conversion
func TestParseTransformOpType(t *testing.T) {
	tests := []struct {
		name     string
		input    TransformOpType
		expected db.TransformOp_OpType
	}{
		{"Set", "$set", db.TransformOp_SET},
		{"Unset", "$unset", db.TransformOp_UNSET},
		{"Inc", "$inc", db.TransformOp_INC},
		{"Push", "$push", db.TransformOp_PUSH},
		{"Pull", "$pull", db.TransformOp_PULL},
		{"AddToSet", "$addToSet", db.TransformOp_ADD_TO_SET},
		{"Pop", "$pop", db.TransformOp_POP},
		{"Mul", "$mul", db.TransformOp_MUL},
		{"Min", "$min", db.TransformOp_MIN},
		{"Max", "$max", db.TransformOp_MAX},
		{"CurrentDate", "$currentDate", db.TransformOp_CURRENT_DATE},
		{"Rename", "$rename", db.TransformOp_RENAME},
		{"Unknown", "unknown", db.TransformOp_SET}, // defaults to SET
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTransformOpType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseSyncLevel tests the sync level conversion helper
func TestParseSyncLevel(t *testing.T) {
	tests := []struct {
		name      string
		input     SyncLevel
		expected  db.Op_SyncLevel
		expectErr bool
	}{
		{"Empty (default)", "", db.Op_SyncLevelPropose, false},
		{"Propose", "propose", db.Op_SyncLevelPropose, false},
		{"Write", "write", db.Op_SyncLevelWrite, false},
		{"FullText", "full_text", db.Op_SyncLevelFullText, false},
		{"Aknn", "aknn", db.Op_SyncLevelEmbeddings, false},
		{"Invalid", "invalid_level", db.Op_SyncLevelPropose, true},
		{"Unknown", "unknown", db.Op_SyncLevelPropose, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSyncLevel(tt.input)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid sync_level")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
