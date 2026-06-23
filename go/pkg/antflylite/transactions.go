// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo

package antflylite

/*
#include "antfly.h"
#include <stdlib.h>
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// TxnID is the stable 16-byte transaction identifier used by the C ABI.
type TxnID [16]byte

// TxnStatus is a transaction intent lifecycle state.
type TxnStatus uint8

const (
	TxnPending   TxnStatus = C.ANTFLY_TXN_PENDING
	TxnCommitted TxnStatus = C.ANTFLY_TXN_COMMITTED
	TxnAborted   TxnStatus = C.ANTFLY_TXN_ABORTED
)

// BeginTransaction starts a local transaction with an explicit transaction ID.
func (db *DB) BeginTransaction(txnID TxnID, timestampNS uint64, participants []string) error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	cParticipants, cleanup, err := makeCParticipantSlices(participants)
	if err != nil {
		return err
	}
	defer cleanup()

	return check(C.antfly_db_begin_transaction_with_id(
		handle,
		cTxnIDPtr(txnID),
		C.uint64_t(timestampNS),
		cParticipants,
		C.size_t(len(participants)),
	))
}

// WriteTransaction appends write intents to an open local transaction.
func (db *DB) WriteTransaction(txnID TxnID, writes []WriteIntent) error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	cWrites, cleanup, err := makeCWriteIntents(writes)
	if err != nil {
		return err
	}
	defer cleanup()

	return check(C.antfly_db_write_transaction(
		handle,
		cTxnIDPtr(txnID),
		cWrites,
		C.size_t(len(writes)),
		(*C.antfly_version_predicate)(nil),
		0,
	))
}

// ResolveTransaction resolves transaction intents as committed or aborted.
func (db *DB) ResolveTransaction(txnID TxnID, status TxnStatus, commitVersion uint64) error {
	handle, err := db.requireHandle()
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(db)
	return check(C.antfly_db_resolve_intents(
		handle,
		cTxnIDPtr(txnID),
		C.uint8_t(status),
		C.uint64_t(commitVersion),
	))
}

// TransactionStatus returns the current transaction lifecycle state.
func (db *DB) TransactionStatus(txnID TxnID) (TxnStatus, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return 0, err
	}
	defer runtime.KeepAlive(db)
	var status C.uint8_t
	if err := check(C.antfly_db_get_transaction_status(handle, cTxnIDPtr(txnID), &status)); err != nil {
		return 0, err
	}
	return TxnStatus(status), nil
}

// CommitVersion returns the commit version recorded for a committed transaction.
func (db *DB) CommitVersion(txnID TxnID) (uint64, error) {
	handle, err := db.requireHandle()
	if err != nil {
		return 0, err
	}
	defer runtime.KeepAlive(db)
	var version C.uint64_t
	if err := check(C.antfly_db_get_commit_version(handle, cTxnIDPtr(txnID), &version)); err != nil {
		return 0, err
	}
	return uint64(version), nil
}

func cTxnIDPtr(txnID TxnID) *[16]C.uint8_t {
	return (*[16]C.uint8_t)(unsafe.Pointer(&txnID[0]))
}

func makeCParticipantSlices(participants []string) (*C.antfly_slice, func(), error) {
	if len(participants) == 0 {
		return nil, func() {}, nil
	}
	size := C.size_t(len(participants)) * C.size_t(unsafe.Sizeof(C.antfly_slice{}))
	ptr := C.malloc(size)
	if ptr == nil {
		return nil, nil, Internal
	}
	cParticipants := unsafe.Slice((*C.antfly_slice)(ptr), len(participants))
	cleanups := make([]func(), 0, len(participants))
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
		C.free(ptr)
	}
	for i, participant := range participants {
		cParticipant, participantCleanup := makeCStringSlice([]byte(participant))
		cleanups = append(cleanups, participantCleanup)
		cParticipants[i] = cParticipant
	}
	return (*C.antfly_slice)(ptr), cleanup, nil
}
