// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Local transaction lifecycle: begin, write intents, resolve, and query
//! status/commit version. Mirrors `go/pkg/lite/transactions.go`.

use antfly_lite_sys as sys;

use crate::db::Database;
use crate::error::Result;
use crate::ffi::{borrow_slice, check};
use crate::options::WriteIntent;

/// The stable 16-byte transaction identifier used by the C ABI.
pub type TxnId = [u8; 16];

/// A transaction intent lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Pending,
    Committed,
    Aborted,
}

impl TxnStatus {
    fn from_u8(value: u8) -> TxnStatus {
        match value {
            1 => TxnStatus::Committed,
            2 => TxnStatus::Aborted,
            _ => TxnStatus::Pending,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            TxnStatus::Pending => sys::ANTFLY_TXN_PENDING,
            TxnStatus::Committed => sys::ANTFLY_TXN_COMMITTED,
            TxnStatus::Aborted => sys::ANTFLY_TXN_ABORTED,
        }
    }
}

impl Database {
    /// Starts a local transaction with an explicit transaction ID.
    pub fn begin_transaction(
        &self,
        txn_id: TxnId,
        timestamp_ns: u64,
        participants: &[&str],
    ) -> Result<()> {
        self.with_handle(|handle| {
            let c_participants: Vec<sys::antfly_slice> = participants
                .iter()
                .map(|p| borrow_slice(p.as_bytes()))
                .collect();
            let ptr = if c_participants.is_empty() {
                std::ptr::null()
            } else {
                c_participants.as_ptr()
            };
            check(unsafe {
                sys::antfly_db_begin_transaction_with_id(
                    handle,
                    &txn_id,
                    timestamp_ns,
                    ptr,
                    c_participants.len(),
                )
            })
        })
    }

    /// Appends write intents to an open local transaction.
    pub fn write_transaction(&self, txn_id: TxnId, writes: &[WriteIntent]) -> Result<()> {
        self.with_handle(|handle| {
            let c_writes: Vec<sys::antfly_write_intent> = writes
                .iter()
                .map(|w| sys::antfly_write_intent {
                    key: borrow_slice(&w.key),
                    value: borrow_slice(&w.value),
                    is_delete: w.delete,
                })
                .collect();
            let ptr = if c_writes.is_empty() {
                std::ptr::null()
            } else {
                c_writes.as_ptr()
            };
            check(unsafe {
                sys::antfly_db_write_transaction(
                    handle,
                    &txn_id,
                    ptr,
                    c_writes.len(),
                    std::ptr::null(),
                    0,
                )
            })
        })
    }

    /// Resolves transaction intents as committed or aborted.
    pub fn resolve_transaction(
        &self,
        txn_id: TxnId,
        status: TxnStatus,
        commit_version: u64,
    ) -> Result<()> {
        self.with_handle(|handle| {
            check(unsafe {
                sys::antfly_db_resolve_intents(handle, &txn_id, status.as_u8(), commit_version)
            })
        })
    }

    /// Returns the current transaction lifecycle state.
    pub fn transaction_status(&self, txn_id: TxnId) -> Result<TxnStatus> {
        self.with_handle(|handle| {
            let mut status: u8 = 0;
            check(unsafe { sys::antfly_db_get_transaction_status(handle, &txn_id, &mut status) })?;
            Ok(TxnStatus::from_u8(status))
        })
    }

    /// Returns the commit version recorded for a committed transaction.
    pub fn commit_version(&self, txn_id: TxnId) -> Result<u64> {
        self.with_handle(|handle| {
            let mut version: u64 = 0;
            check(unsafe { sys::antfly_db_get_commit_version(handle, &txn_id, &mut version) })?;
            Ok(version)
        })
    }
}
