//go:build with_tla

package db

import (
	"encoding/hex"

	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/tracing"
)

// traceInitTransaction emits a TLA+ trace event for InitTransaction.
func (db *DBImpl) traceInitTransaction(record *TxnRecord) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "InitTransaction",
		TxnID:   hex.EncodeToString(record.TxnID),
		ShardID: db.traceShardID(),
		State: map[string]any{
			"txnStatus":    record.Status,
			"timestamp":    record.Timestamp,
			"participants": len(record.Participants),
		},
	})
}

// traceWriteIntent emits a TLA+ trace event for a successful WriteIntent.
func (db *DBImpl) traceWriteIntent(txnID []byte, numWrites, numDeletes int) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "WriteIntentOnShard",
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
		State: map[string]any{
			"numWrites":  numWrites,
			"numDeletes": numDeletes,
		},
	})
}

// traceWriteIntentFails emits a TLA+ trace event for a predicate check failure.
func (db *DBImpl) traceWriteIntentFails(txnID []byte, err error) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "WriteIntentFails",
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
		State: map[string]any{
			"reason": err.Error(),
		},
	})
}

// traceFinalizeTransaction emits a TLA+ trace event for CommitTransaction or AbortTransaction.
func (db *DBImpl) traceFinalizeTransaction(txnID []byte, status int32, commitVersion uint64) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	name := "CommitTransaction"
	if status == TxnStatusAborted {
		name = "AbortTransaction"
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    name,
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
		State: map[string]any{
			"txnStatus":     status,
			"commitVersion": commitVersion,
		},
	})
}

// traceResolveIntents emits a TLA+ trace event for ResolveIntents.
func (db *DBImpl) traceResolveIntents(txnID []byte, status int32, count int) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "ResolveIntentsOnShard",
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
		State: map[string]any{
			"txnStatus":     status,
			"intentsCount":  count,
		},
	})
}

// traceRecoveryAbort emits a TLA+ trace event for auto-abort of stale transactions.
func (db *DBImpl) traceRecoveryAbort(txnID []byte) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "RecoveryResolve",
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
	})
}

// traceCleanupTxnRecord emits a TLA+ trace event for removing a completed transaction record.
func (db *DBImpl) traceCleanupTxnRecord(txnID []byte) {
	tw := db.traceWriter
	if tw == nil {
		return
	}
	tw.TraceAntflyEvent(&tracing.AntflyTracingEvent{
		Name:    "CleanupTxnRecord",
		TxnID:   hex.EncodeToString(txnID),
		ShardID: db.traceShardID(),
	})
}

// traceShardID returns the shard ID string for trace events.
func (db *DBImpl) traceShardID() string {
	_, shardID, err := common.ParseStorageDBDir(db.dir)
	if err != nil {
		return "unknown"
	}
	return shardID.String()
}
