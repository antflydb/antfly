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

	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestEncodeDecode tests protobuf encoding/decoding for all message types
func TestEncodeDecode(t *testing.T) {
	addIndexOp, err := NewAddIndexOp(
		indexes.NewEmbeddingsConfig("test-index", indexes.EmbeddingsIndexConfig{
			Dimension: 128,
			Field:     "foo",
			Embedder: embeddings.NewEmbedderConfigFromJSON(
				"mock",
				[]byte(`{ "model": "test-model" }`),
			),
		}),
	)
	require.NoError(t, err)

	updateSchemaOp, err := NewUpdateSchemaOp(&schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"field1": map[string]any{"type": "array"},
						"field2": map[string]any{"type": "number"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	testCases := []struct {
		name string
		op   *Op
	}{
		{
			name: "OpBatch",
			op: NewBatchOp(BatchOp_builder{
				Writes: []*Write{
					Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
					Write_builder{Key: []byte("key2"), Value: []byte("value2")}.Build(),
				},
				Deletes: [][]byte{[]byte("key3"), []byte("key4")},
			}.Build()),
		},
		{
			name: "OpSplit",
			op:   NewSplitOp(uint64(12345), []byte("median-key")),
		},
		{
			name: "OpSetRange",
			op:   NewSetRangeOp([2][]byte{[]byte("start"), []byte("end")}),
		},
		{
			name: "OpUpdateSchema",
			op:   updateSchemaOp,
		},
		{
			name: "OpAddIndex",
			op:   addIndexOp,
		},
		{
			name: "OpDeleteIndex",
			op:   NewDeleteIndexOp("test-index"),
		},
		{
			name: "OpBackup",
			op: Op_builder{
				Op: Op_OpBackup,
				Backup: BackupOp_builder{
					BackupId: "s3://bucket/path",
					Location: "backup-123",
				}.Build(),
			}.Build(),
		},
		{
			name: "EmptyBatch",
			op:   NewBatchOp(BatchOp_builder{}.Build()),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeProto(tc.op)
			require.NoError(t, err, "encoding should succeed")
			require.NotEmpty(t, encoded, "encoded data should not be empty")

			// Decode
			decoded := &Op{}
			require.NoError(t, DecodeProto(encoded, decoded), "decoding should succeed")

			// Verify
			assert.Equal(t, tc.op.GetOp(), decoded.GetOp(), "Op should match")
			assert.Equal(
				t,
				proto.ValueOrNil(tc.op.HasUuid(), tc.op.GetUuid),
				proto.ValueOrNil(decoded.HasUuid(), decoded.GetUuid),
				"UUID should match",
			)

			// Compare operation-specific data based on Op type
			switch tc.op.GetOp() {
			case Op_OpBatch:
				batch := tc.op.GetBatch()
				decodedBatch := decoded.GetBatch()
				assert.NotNil(t, decodedBatch, "Batch should not be nil")
				assert.Len(
					t,
					decodedBatch.GetWrites(),
					len(batch.GetWrites()),
					"Writes length should match",
				)
				for i := range batch.GetWrites() {
					assert.Equal(
						t,
						batch.GetWrites()[i].GetKey(),
						decodedBatch.GetWrites()[i].GetKey(),
						"Write key should match",
					)
					assert.Equal(
						t,
						batch.GetWrites()[i].GetValue(),
						decodedBatch.GetWrites()[i].GetValue(),
						"Write value should match",
					)
				}
				assert.Len(
					t,
					decodedBatch.GetDeletes(),
					len(batch.GetDeletes()),
					"Deletes length should match",
				)
				for i := range batch.GetDeletes() {
					assert.Equal(
						t,
						batch.GetDeletes()[i],
						decodedBatch.GetDeletes()[i],
						"Delete key should match",
					)
				}
			case Op_OpSplit:
				split := tc.op.GetSplit()
				decodedSplit := decoded.GetSplit()
				assert.NotNil(t, decodedSplit, "Split should not be nil")
				assert.Equal(
					t,
					split.GetNewShardId(),
					decodedSplit.GetNewShardId(),
					"NewShardID should match",
				)
				assert.Equal(
					t,
					split.GetSplitKey(),
					decodedSplit.GetSplitKey(),
					"SplitKey should match",
				)
			case Op_OpSetRange:
				setRange := tc.op.GetSetRange()
				decodedSetRange := decoded.GetSetRange()
				assert.NotNil(t, decodedSetRange, "SetRange should not be nil")
				assert.Equal(
					t,
					setRange.GetStartKey(),
					decodedSetRange.GetStartKey(),
					"StartKey should match",
				)
				assert.Equal(
					t,
					setRange.GetEndKey(),
					decodedSetRange.GetEndKey(),
					"EndKey should match",
				)
			case Op_OpUpdateSchema:
				updateSchema := tc.op.GetUpdateSchema()
				decodedUpdateSchema := decoded.GetUpdateSchema()
				assert.NotNil(t, decodedUpdateSchema, "UpdateSchema should not be nil")
				assert.Equal(
					t,
					updateSchema.GetSchema(),
					decodedUpdateSchema.GetSchema(),
					"Schema should match",
				)
			case Op_OpAddIndex:
				addIndex := tc.op.GetAddIndex()
				decodedAddIndex := decoded.GetAddIndex()
				assert.NotNil(t, decodedAddIndex, "AddIndex should not be nil")
				assert.Equal(t, addIndex.GetName(), decodedAddIndex.GetName(), "Name should match")
				assert.Equal(t, addIndex.GetType(), decodedAddIndex.GetType(), "Type should match")
				assert.Len(
					t,
					decodedAddIndex.GetConfig(),
					len(addIndex.GetConfig()),
					"Config length should match",
				)
			case Op_OpDeleteIndex:
				deleteIndex := tc.op.GetDeleteIndex()
				decodedDeleteIndex := decoded.GetDeleteIndex()
				assert.NotNil(t, decodedDeleteIndex, "DeleteIndex should not be nil")
				assert.Equal(
					t,
					deleteIndex.GetName(),
					decodedDeleteIndex.GetName(),
					"Name should match",
				)
			case Op_OpBackup:
				backup := tc.op.GetBackup()
				decodedBackup := decoded.GetBackup()
				assert.NotNil(t, decodedBackup, "Backup should not be nil")
				assert.Equal(
					t,
					backup.GetBackupId(),
					decodedBackup.GetBackupId(),
					"BackupID should match",
				)
				assert.Equal(
					t,
					backup.GetLocation(),
					decodedBackup.GetLocation(),
					"Location should match",
				)
			}
		})
	}
}

// TestEncodeDecodeErrs tests error handling in encoding/decoding
func TestEncodeDecodeErrs(t *testing.T) {
	t.Run("DecodeInvalidData", func(t *testing.T) {
		invalidData := []byte("invalid data")
		var decoded *Op
		err := DecodeProto(invalidData, decoded)
		assert.Error(t, err, "Protobuf decode should fail on invalid data")
	})

	t.Run("DecodeEmptyData", func(t *testing.T) {
		var decoded *Op
		err := DecodeProto([]byte{}, decoded)
		assert.Error(t, err, "Protobuf decode should fail on empty data")
	})
}

// TestWritesFromTuples tests conversion from [][2][]byte to []*Write
func TestWritesFromTuples(t *testing.T) {
	t.Run("EmptyTuples", func(t *testing.T) {
		tuples := [][2][]byte{}
		writes := WritesFromTuples(tuples)
		assert.Nil(t, writes, "Empty tuples should return nil")
	})

	t.Run("NilTuples", func(t *testing.T) {
		var tuples [][2][]byte
		writes := WritesFromTuples(tuples)
		assert.Nil(t, writes, "Nil tuples should return nil")
	})

	t.Run("SingleTuple", func(t *testing.T) {
		tuples := [][2][]byte{
			{[]byte("key1"), []byte("value1")},
		}
		writes := WritesFromTuples(tuples)
		require.Len(t, writes, 1, "Should convert single tuple")
		assert.Equal(t, []byte("key1"), writes[0].GetKey())
		assert.Equal(t, []byte("value1"), writes[0].GetValue())
	})

	t.Run("MultipleTuples", func(t *testing.T) {
		tuples := [][2][]byte{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
			{[]byte("key3"), []byte("value3")},
		}
		writes := WritesFromTuples(tuples)
		require.Len(t, writes, 3, "Should convert all tuples")
		for i, w := range writes {
			assert.Equal(t, tuples[i][0], w.GetKey(), "Key should match at index %d", i)
			assert.Equal(t, tuples[i][1], w.GetValue(), "Value should match at index %d", i)
		}
	})

	t.Run("EmptyKeysAndValues", func(t *testing.T) {
		tuples := [][2][]byte{
			{[]byte(""), []byte("")},
			{[]byte("key"), []byte("")},
			{[]byte(""), []byte("value")},
		}
		writes := WritesFromTuples(tuples)
		require.Len(t, writes, 3, "Should handle empty keys and values")
		assert.Equal(t, []byte(""), writes[0].GetKey())
		assert.Equal(t, []byte(""), writes[0].GetValue())
		assert.Equal(t, []byte("key"), writes[1].GetKey())
		assert.Equal(t, []byte(""), writes[1].GetValue())
		assert.Equal(t, []byte(""), writes[2].GetKey())
		assert.Equal(t, []byte("value"), writes[2].GetValue())
	})
}

// TestWritesToTuples tests conversion from []*Write to [][2][]byte
func TestWritesToTuples(t *testing.T) {
	t.Run("EmptyWrites", func(t *testing.T) {
		writes := []*Write{}
		tuples := WritesToTuples(writes)
		assert.Nil(t, tuples, "Empty writes should return nil")
	})

	t.Run("NilWrites", func(t *testing.T) {
		var writes []*Write
		tuples := WritesToTuples(writes)
		assert.Nil(t, tuples, "Nil writes should return nil")
	})

	t.Run("SingleWrite", func(t *testing.T) {
		writes := []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
		}
		tuples := WritesToTuples(writes)
		require.Len(t, tuples, 1, "Should convert single write")
		assert.Equal(t, []byte("key1"), tuples[0][0])
		assert.Equal(t, []byte("value1"), tuples[0][1])
	})

	t.Run("MultipleWrites", func(t *testing.T) {
		writes := []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
			Write_builder{Key: []byte("key2"), Value: []byte("value2")}.Build(),
			Write_builder{Key: []byte("key3"), Value: []byte("value3")}.Build(),
		}
		tuples := WritesToTuples(writes)
		require.Len(t, tuples, 3, "Should convert all writes")
		for i, tuple := range tuples {
			assert.Equal(t, writes[i].GetKey(), tuple[0], "Key should match at index %d", i)
			assert.Equal(t, writes[i].GetValue(), tuple[1], "Value should match at index %d", i)
		}
	})

	t.Run("EmptyKeysAndValues", func(t *testing.T) {
		writes := []*Write{
			Write_builder{Key: []byte(""), Value: []byte("")}.Build(),
			Write_builder{Key: []byte("key"), Value: []byte("")}.Build(),
			Write_builder{Key: []byte(""), Value: []byte("value")}.Build(),
		}
		tuples := WritesToTuples(writes)
		require.Len(t, tuples, 3, "Should handle empty keys and values")
		assert.Equal(t, []byte(""), tuples[0][0])
		assert.Equal(t, []byte(""), tuples[0][1])
		assert.Equal(t, []byte("key"), tuples[1][0])
		assert.Equal(t, []byte(""), tuples[1][1])
		assert.Equal(t, []byte(""), tuples[2][0])
		assert.Equal(t, []byte("value"), tuples[2][1])
	})
}

// TestWritesRoundTrip tests that conversions are reversible
func TestWritesRoundTrip(t *testing.T) {
	t.Run("TuplesToWritesToTuples", func(t *testing.T) {
		original := [][2][]byte{
			{[]byte("key1"), []byte("value1")},
			{[]byte("key2"), []byte("value2")},
			{[]byte("key3"), []byte("value3")},
		}

		// Convert to writes and back
		writes := WritesFromTuples(original)
		result := WritesToTuples(writes)

		require.Len(t, result, len(original), "Length should match after round trip")
		for i := range original {
			assert.Equal(t, original[i][0], result[i][0], "Key should match at index %d", i)
			assert.Equal(t, original[i][1], result[i][1], "Value should match at index %d", i)
		}
	})

	t.Run("WritesToTuplesToWrites", func(t *testing.T) {
		original := []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
			Write_builder{Key: []byte("key2"), Value: []byte("value2")}.Build(),
			Write_builder{Key: []byte("key3"), Value: []byte("value3")}.Build(),
		}

		// Convert to tuples and back
		tuples := WritesToTuples(original)
		result := WritesFromTuples(tuples)

		require.Len(t, result, len(original), "Length should match after round trip")
		for i := range original {
			assert.Equal(t, original[i].GetKey(), result[i].GetKey(), "Key should match at index %d", i)
			assert.Equal(t, original[i].GetValue(), result[i].GetValue(), "Value should match at index %d", i)
		}
	})

	t.Run("EmptyRoundTrip", func(t *testing.T) {
		// Empty tuples
		emptyTuples := [][2][]byte{}
		writes := WritesFromTuples(emptyTuples)
		tuples := WritesToTuples(writes)
		assert.Nil(t, tuples, "Empty round trip should return nil")

		// Empty writes
		emptyWrites := []*Write{}
		tuples2 := WritesToTuples(emptyWrites)
		writes2 := WritesFromTuples(tuples2)
		assert.Nil(t, writes2, "Empty round trip should return nil")
	})
}
