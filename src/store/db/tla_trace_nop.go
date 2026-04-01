//go:build !with_tla

package db

// traceInitTransaction is a no-op without the with_tla build tag.
func (db *DBImpl) traceInitTransaction(_ *TxnRecord) {}

// traceWriteIntent is a no-op without the with_tla build tag.
func (db *DBImpl) traceWriteIntent(_ []byte, _, _ int) {}

// traceWriteIntentFails is a no-op without the with_tla build tag.
func (db *DBImpl) traceWriteIntentFails(_ []byte, _ error) {}

// traceFinalizeTransaction is a no-op without the with_tla build tag.
func (db *DBImpl) traceFinalizeTransaction(_ []byte, _ int32, _ uint64) {}

// traceResolveIntents is a no-op without the with_tla build tag.
func (db *DBImpl) traceResolveIntents(_ []byte, _ int32, _ int) {}

// traceRecoveryAbort is a no-op without the with_tla build tag.
func (db *DBImpl) traceRecoveryAbort(_ []byte) {}

// traceCleanupTxnRecord is a no-op without the with_tla build tag.
func (db *DBImpl) traceCleanupTxnRecord(_ []byte) {}
