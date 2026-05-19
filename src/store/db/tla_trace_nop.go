// Copyright 2026 Antfly, Inc.
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

//go:build !with_tla

package db

func (db *DBImpl) traceInitTransaction(_ *TxnRecord)                      {}
func (db *DBImpl) traceWriteIntent(_ *WriteIntentOp)                      {}
func (db *DBImpl) traceWriteIntentFails(_ *WriteIntentOp, _ error)        {}
func (db *DBImpl) traceFinalizeTransaction(_ []byte, _ int32, _ uint64)   {}
func (db *DBImpl) traceResolveIntents(_ []byte, _ int32, _ int)           {}
func (db *DBImpl) traceRecoveryAbort(_ []byte)                            {}
func (db *DBImpl) traceCleanupTxnRecord(_ []byte)                         {}
