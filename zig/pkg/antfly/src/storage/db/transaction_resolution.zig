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

const transactions_mod = @import("../transactions.zig");

pub const ResolveParticipantFn = *const fn (
    ctx_ptr: *anyopaque,
    txn_id: transactions_mod.TxnId,
    participant: []const u8,
    status: transactions_mod.TxnStatus,
    commit_version: u64,
) anyerror!void;
