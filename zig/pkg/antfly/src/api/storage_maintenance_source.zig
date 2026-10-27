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

//! Coarse control contract for storage-owner maintenance. Distributed runtime
//! code schedules whole maintenance quanta through this interface and never
//! imports the DB, LSM, or index maintenance implementations.

const Reentry = @import("antfly_runtime_abi").runnable_abi.Reentry;

pub const PostingRefreshProgress = @import("antfly_local_sources").storage_posting_refresh_progress.Progress;

pub const RoundResult = struct {
    /// A bounded window or a peer selection is unfinished; absence of progress
    /// must not be interpreted as a complete no-work observation.
    selection_pending: bool = false,
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
        run_lsm_round_with_lifetime: ?*const fn (*anyopaque, bool, ?*const Reentry) anyerror!RoundResult = null,
        run_dense_posting_round: *const fn (*anyopaque) anyerror!PostingRefreshProgress,
        publish_dense_checkpoints: *const fn (*anyopaque) anyerror!@import("antfly_local_sources").storage_db_types.NativePublicationResult,
        run_vector_block_round: *const fn (*anyopaque) anyerror!usize,
        publish_runtime_statuses: ?*const fn (*anyopaque) void = null,
        snapshot: *const fn (*anyopaque, bool) anyerror!Snapshot,
    };

    pub fn runLsmRound(self: Source, best_effort: bool) !RoundResult {
        return try self.vtable.run_lsm_round(self.ptr, best_effort);
    }

    pub fn runLsmRoundWithLifetime(self: Source, best_effort: bool, lifetime: ?*const Reentry) !RoundResult {
        if (lifetime) |original| try original.checkpoint();
        const result = if (self.vtable.run_lsm_round_with_lifetime) |controlled|
            try controlled(self.ptr, best_effort, lifetime)
        else
            try self.runLsmRound(best_effort);
        if (lifetime) |original| try original.checkpoint();
        return result;
    }

    pub fn runDensePostingRound(self: Source) !PostingRefreshProgress {
        return try self.vtable.run_dense_posting_round(self.ptr);
    }

    pub fn publishDenseCheckpoints(self: Source) !@import("antfly_local_sources").storage_db_types.NativePublicationResult {
        return try self.vtable.publish_dense_checkpoints(self.ptr);
    }

    pub fn runVectorBlockRound(self: Source) !usize {
        return try self.vtable.run_vector_block_round(self.ptr);
    }

    pub fn publishRuntimeStatuses(self: Source) void {
        if (self.vtable.publish_runtime_statuses) |publish| publish(self.ptr);
    }

    pub fn maintenanceSnapshot(self: Source, best_effort: bool) !Snapshot {
        return try self.vtable.snapshot(self.ptr, best_effort);
    }
};
