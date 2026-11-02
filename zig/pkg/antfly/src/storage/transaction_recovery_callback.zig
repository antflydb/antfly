// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Fixed-width external decision relay shared by the native owner and control
//! tests. This module has no physical handles, storage implementation or I/O.
const abi = @import("kernel_owner_abi");
const identity = @import("kernel_error_identity");
const transactions = @import("antfly_local_sources").storage_transactions;

pub fn resolveExternal(config: abi.TransactionRecoveryConfig, txn: transactions.TxnId, participant: []const u8, status: transactions.TxnStatus, version: u64, owner: transactions.ExternalDecisionOwner) !void {
    const callback = config.resolve_external_participant_fn orelse return error.ExternalTransactionDecisionRequired;
    const bytes = try owner.encode();
    const id: abi.TxnId = .{ .bytes = txn };
    try identity.statusToError(callback(config.callback_ctx, &id, .fromSlice(participant), switch (status) {
        .pending => .pending,
        .committed => .committed,
        .aborted => .aborted,
    }, version, .fromSlice(&bytes)));
}
