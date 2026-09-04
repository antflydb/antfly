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

//! Coarse control contract for storage-owner maintenance. Distributed runtime
//! code schedules whole maintenance quanta through this interface and never
//! imports the DB, LSM, or index maintenance implementations.

pub const RoundResult = struct {
    progressed: bool = false,
    group_id: ?u64 = null,
};

pub const Snapshot = struct {
    maintenance_score: u64 = 0,
    next_wake_delay_ns: ?u64 = null,
    owner_count: usize = 0,
};

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        run_lsm_round: *const fn (*anyopaque, bool) anyerror!RoundResult,
        run_dense_posting_round: *const fn (*anyopaque) anyerror!usize,
        snapshot: *const fn (*anyopaque, bool) anyerror!Snapshot,
    };

    pub fn runLsmRound(self: Source, best_effort: bool) !RoundResult {
        return try self.vtable.run_lsm_round(self.ptr, best_effort);
    }

    pub fn runDensePostingRound(self: Source) !usize {
        return try self.vtable.run_dense_posting_round(self.ptr);
    }

    pub fn maintenanceSnapshot(self: Source, best_effort: bool) !Snapshot {
        return try self.vtable.snapshot(self.ptr, best_effort);
    }
};
