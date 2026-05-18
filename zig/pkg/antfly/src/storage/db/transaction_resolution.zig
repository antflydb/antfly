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

const transactions_mod = @import("../transactions.zig");

pub const ResolveParticipantFn = *const fn (
    ctx_ptr: *anyopaque,
    txn_id: transactions_mod.TxnId,
    participant: []const u8,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
) anyerror!void;
