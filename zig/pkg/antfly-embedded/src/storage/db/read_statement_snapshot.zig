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

//! Owner-local retained visibility cut. It never crosses the native boundary.
const Source = @import("read_session_source_types").types;
pub const Implementation = struct {
    index_catalog_captured: bool = false,
    indexes: ?Source.index_catalog.WriteSnapshot = null,
    row_policy_lease: ?Source.row_policy_gate.Gate.Lease = null,
    row_policy_phase: Source.table_catalog.RowPolicyPhase = .disabled,
    row_policy_generation: u64 = 0,
    row_policy_catalog_epoch: u64 = 0,
    read: Source.store.DocStore.Txn,
    schema_version: u32,
    pub fn deinit(self: *@This()) void {
        if (self.indexes) |*indexes| indexes.deinit();
        self.read.abort();
        if (self.row_policy_lease) |*lease| lease.release();
        self.* = undefined;
    }
};
