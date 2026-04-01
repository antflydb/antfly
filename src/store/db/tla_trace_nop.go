//go:build !with_tla

package db

func (db *DBImpl) traceInitTransaction(_ *TxnRecord)                      {}
func (db *DBImpl) traceWriteIntent(_ *WriteIntentOp)                      {}
func (db *DBImpl) traceWriteIntentFails(_ *WriteIntentOp, _ error)        {}
func (db *DBImpl) traceFinalizeTransaction(_ []byte, _ int32, _ uint64)   {}
func (db *DBImpl) traceResolveIntents(_ []byte, _ int32, _ int)           {}
func (db *DBImpl) traceRecoveryAbort(_ []byte)                            {}
func (db *DBImpl) traceCleanupTxnRecord(_ []byte)                         {}
