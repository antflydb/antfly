//go:build with_tla

package db

import (
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/tracing"
)

// traceTxnEvent is the shared helper for all transaction trace events.
// It handles the nil-writer guard, txnID formatting, and shard ID caching.
func (db *DBImpl) traceTxnEvent(name string, txnID []byte, shardID string, state map[string]any) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    name,
		TxnID:   types.FormatKey(txnID),
		ShardID: shardID,
		State:   state,
	})
}

// traceShardID returns the cached shard ID string for trace events.
func (db *DBImpl) traceShardID() string {
	if db.traceShardIDStr == "" {
		shardID, _, err := common.ParseStorageDBDir(db.dir)
		if err != nil {
			db.traceShardIDStr = "unknown"
		} else {
			db.traceShardIDStr = shardID.String()
		}
	}
	return db.traceShardIDStr
}

func (db *DBImpl) traceInitTransaction(record *TxnRecord) {
	db.traceTxnEvent("InitTransaction", record.TxnID, db.traceShardID(), map[string]any{
		"txnStatus":    record.Status,
		"timestamp":    record.Timestamp,
		"participants": len(record.Participants),
	})
}

// traceWriteIntent emits a WriteIntentOnShard event with full key data
// so the TLA+ trace spec can derive TxnKeys, TxnReadSet, and Keys constants.
func (db *DBImpl) traceWriteIntent(op *WriteIntentOp) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	batch := op.GetBatch()
	writeKeys := make([]string, len(batch.GetWrites()))
	for i, w := range batch.GetWrites() {
		writeKeys[i] = types.FormatKey(w.GetKey())
	}
	deleteKeys := make([]string, len(batch.GetDeletes()))
	for i, d := range batch.GetDeletes() {
		deleteKeys[i] = types.FormatKey(d)
	}
	predicateKeys := make([]string, len(op.GetPredicates()))
	for i, p := range op.GetPredicates() {
		predicateKeys[i] = types.FormatKey(p.GetKey())
	}
	db.traceTxnEvent("WriteIntentOnShard", op.GetTxnId(), db.traceShardID(), map[string]any{
		"writeKeys":     writeKeys,
		"deleteKeys":    deleteKeys,
		"predicateKeys": predicateKeys,
	})
}

// traceWriteIntentFails emits a WriteIntentFails event with key data.
// Keys are needed even for failed intents so the TLA+ spec can derive TxnKeys
// for the conflicting transaction.
func (db *DBImpl) traceWriteIntentFails(op *WriteIntentOp, err error) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	batch := op.GetBatch()
	writeKeys := make([]string, len(batch.GetWrites()))
	for i, w := range batch.GetWrites() {
		writeKeys[i] = types.FormatKey(w.GetKey())
	}
	deleteKeys := make([]string, len(batch.GetDeletes()))
	for i, d := range batch.GetDeletes() {
		deleteKeys[i] = types.FormatKey(d)
	}
	db.traceTxnEvent("WriteIntentFails", op.GetTxnId(), db.traceShardID(), map[string]any{
		"writeKeys":  writeKeys,
		"deleteKeys": deleteKeys,
		"reason":     err.Error(),
	})
}

func (db *DBImpl) traceFinalizeTransaction(txnID []byte, status int32, commitVersion uint64) {
	name := "CommitTransaction"
	if status == TxnStatusAborted {
		name = "AbortTransaction"
	}
	db.traceTxnEvent(name, txnID, db.traceShardID(), map[string]any{
		"txnStatus":     status,
		"commitVersion": commitVersion,
	})
}

func (db *DBImpl) traceResolveIntents(txnID []byte, status int32, count int) {
	db.traceTxnEvent("ResolveIntentsOnShard", txnID, db.traceShardID(), map[string]any{
		"txnStatus":    status,
		"intentsCount": count,
	})
}

// traceRecoveryAbort emits a RecoveryResolve event with empty ShardID.
// The TLA+ spec uses the empty shardId to distinguish auto-abort (coordinator-level)
// from per-shard recovery resolve.
func (db *DBImpl) traceRecoveryAbort(txnID []byte) {
	db.traceTxnEvent("RecoveryResolve", txnID, "", nil)
}

func (db *DBImpl) traceCleanupTxnRecord(txnID []byte) {
	db.traceTxnEvent("CleanupTxnRecord", txnID, db.traceShardID(), nil)
}
