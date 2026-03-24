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
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestTransactionHelperFunctions tests the key generation helpers
func TestTransactionHelperFunctions(t *testing.T) {
	txnID := uuid.New()
	txnIDBytes := txnID[:]
	userKey := []byte("user:123")

	t.Run("makeTxnKey", func(t *testing.T) {
		key := makeTxnKey(txnIDBytes)
		assert.NotEmpty(t, key)
		assert.Equal(t, []byte("\x00\x00__txn_records__:"), key[:len("\x00\x00__txn_records__:")])
	})

	t.Run("makeIntentKey", func(t *testing.T) {
		key := makeIntentKey(txnIDBytes, userKey)
		assert.NotEmpty(t, key)
		assert.Equal(t, []byte("\x00\x00__txn_intents__:"), key[:len("\x00\x00__txn_intents__:")])
	})

	t.Run("parseIntentKey_valid", func(t *testing.T) {
		intentKey := makeIntentKey(txnIDBytes, userKey)
		parsedTxnID, parsedUserKey := parseIntentKey(intentKey)

		assert.Equal(t, txnIDBytes, parsedTxnID)
		assert.Equal(t, userKey, parsedUserKey)
	})

	t.Run("parseIntentKey_invalid", func(t *testing.T) {
		invalidKey := []byte("not-an-intent-key")
		parsedTxnID, parsedUserKey := parseIntentKey(invalidKey)

		assert.Nil(t, parsedTxnID)
		assert.Nil(t, parsedUserKey)
	})
}

// TestInitTransaction tests transaction initialization
func TestInitTransaction(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	timestamp := uint64(time.Now().Unix())
	participants := [][]byte{
		{1, 0, 0, 0, 0, 0, 0, 0},
		{2, 0, 0, 0, 0, 0, 0, 0},
	}

	op := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    timestamp,
		Participants: participants,
	}.Build()

	err := db.InitTransaction(context.Background(), op)
	require.NoError(t, err)

	// Verify transaction record was created
	status, err := db.GetTransactionStatus(context.Background(), txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(0), status) // Pending
}

// TestCommitTransaction tests transaction commit
func TestCommitTransaction(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	ctx := context.Background()

	// Initialize transaction first
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    uint64(time.Now().Unix()),
		Participants: [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Commit the transaction
	commitOp := CommitTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	_, err = db.CommitTransaction(ctx, commitOp)
	require.NoError(t, err)

	// Verify status is now Committed
	status, err := db.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(1), status) // Committed
}

// TestAbortTransaction tests transaction abort
func TestAbortTransaction(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	ctx := context.Background()

	// Initialize transaction first
	initOp := InitTransactionOp_builder{
		TxnId:        txnID[:],
		Timestamp:    uint64(time.Now().Unix()),
		Participants: [][]byte{{1, 0, 0, 0, 0, 0, 0, 0}},
	}.Build()
	err := db.InitTransaction(ctx, initOp)
	require.NoError(t, err)

	// Abort the transaction
	abortOp := AbortTransactionOp_builder{
		TxnId: txnID[:],
	}.Build()
	err = db.AbortTransaction(ctx, abortOp)
	require.NoError(t, err)

	// Verify status is now Aborted
	status, err := db.GetTransactionStatus(ctx, txnID[:])
	require.NoError(t, err)
	assert.Equal(t, int32(2), status) // Aborted
}

// TestWriteIntent tests writing transaction intents
func TestWriteIntent(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	coordinatorShard := []byte{99, 0, 0, 0, 0, 0, 0, 0}
	ctx := context.Background()

	// Create write intent
	batchOp := BatchOp_builder{
		Writes: []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
			Write_builder{Key: []byte("key2"), Value: []byte("value2")}.Build(),
		},
		Deletes: [][]byte{
			[]byte("key3"),
		},
	}.Build()

	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        uint64(time.Now().Unix()),
		CoordinatorShard: coordinatorShard,
		Batch:            batchOp,
	}.Build()

	err := db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Verify intents were created by checking they exist in Pebble
	intentKey1 := makeIntentKey(txnID[:], []byte("key1"))
	value1, closer, err := db.pdb.Get(intentKey1)
	require.NoError(t, err)
	defer closer.Close()
	assert.NotNil(t, value1)

	intentKey2 := makeIntentKey(txnID[:], []byte("key2"))
	value2, closer2, err := db.pdb.Get(intentKey2)
	require.NoError(t, err)
	defer closer2.Close()
	assert.NotNil(t, value2)

	intentKey3 := makeIntentKey(txnID[:], []byte("key3"))
	value3, closer3, err := db.pdb.Get(intentKey3)
	require.NoError(t, err)
	defer closer3.Close()
	assert.NotNil(t, value3)
}

// TestResolveIntents_Commit tests intent resolution for committed transactions
func TestResolveIntents_Commit(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	ctx := context.Background()

	// Write intents
	batchOp := BatchOp_builder{
		Writes: []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
		},
	}.Build()

	ts := uint64(time.Now().Unix())
	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        ts,
		CoordinatorShard: []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Batch:            batchOp,
	}.Build()

	err := db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Resolve intents with Committed status
	resolveOp := ResolveIntentsOp_builder{
		TxnId:         txnID[:],
		Status:        1, // Committed
		CommitVersion: ts,
	}.Build()

	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Verify intent was removed
	intentKey := makeIntentKey(txnID[:], []byte("key1"))
	_, _, err = db.pdb.Get(intentKey)
	assert.Error(t, err) // Should be not found
}

// TestResolveIntents_Abort tests intent resolution for aborted transactions
func TestResolveIntents_Abort(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	txnID := uuid.New()
	ctx := context.Background()

	// Write intents
	batchOp := BatchOp_builder{
		Writes: []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
		},
	}.Build()

	writeIntentOp := WriteIntentOp_builder{
		TxnId:            txnID[:],
		Timestamp:        uint64(time.Now().Unix()),
		CoordinatorShard: []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Batch:            batchOp,
	}.Build()

	err := db.WriteIntent(ctx, writeIntentOp)
	require.NoError(t, err)

	// Resolve intents with Aborted status
	resolveOp := ResolveIntentsOp_builder{
		TxnId:  txnID[:],
		Status: 2, // Aborted
	}.Build()

	err = db.ResolveIntents(ctx, resolveOp)
	require.NoError(t, err)

	// Verify intent was removed but actual data was not written
	intentKey := makeIntentKey(txnID[:], []byte("key1"))
	_, _, err = db.pdb.Get(intentKey)
	assert.Error(t, err) // Should be not found
}

// TestGetTransactionStatus_NotFound tests status query for non-existent transaction
func TestGetTransactionStatus_NotFound(t *testing.T) {
	dir := t.TempDir()
	db := createTestDB(t, dir)
	defer db.Close()

	nonExistentTxnID := uuid.New()
	status, err := db.GetTransactionStatus(context.Background(), nonExistentTxnID[:])
	require.NoError(t, err)
	// Should return Aborted for non-existent transactions (cleaned up)
	assert.Equal(t, int32(2), status)
}

// Helper function to create a test database
func createTestDB(t *testing.T, dir string) *DBImpl {
	t.Helper()

	db := &DBImpl{
		logger:       zap.NewNop(),
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	db.setByteRange(types.Range{[]byte(""), []byte("")})

	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
				},
			},
		},
	}

	err := db.Open(dir, false, testSchema, types.Range{[]byte(""), []byte("")})
	require.NoError(t, err)

	return db
}
