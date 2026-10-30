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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Full-text index state owned by `DB.indexes`: the optional segment merge runtime, which pauses around structural index mutations.
pub const TextIndexes = struct {
    merge_owner: @import("../local_runtime_owner.zig").RuntimeOwner(db_mod.text_merge_runtime_mod.TextMergeRuntime) = .{},
    merge_runtime: ?*db_mod.text_merge_runtime_mod.TextMergeRuntime = null,

    fn dbPtr(self: *TextIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("text", self));
        return parent.dbPtr();
    }


    pub fn initMergeRuntime(self: *TextIndexes, cfg: db_mod.text_merge_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initOptionalTextMergeRuntime", .{ db, cfg });
        if (!db.start_index_workers) return;
        if (!cfg.enabled) return;
        const resources = db.core.asyncResources();
        self.merge_owner = try @TypeOf(self.merge_owner).create(db.runtime_alloc, .{
            db.runtime_alloc,
            resources.index_manager,
            resources.apply_mutex,
            db.backend_runtime,
            cfg,
        });
        const runtime = self.merge_owner.runtime.?;
        self.merge_runtime = runtime;
        db.async_context.text_merge_runtime = runtime;
    }

    fn snapshotMergeStats(self: *TextIndexes) db_mod.types.TextMergeStats {
        const db = self.dbPtr();
        db_mod.lockApplyShared(db);
        defer db.core.unlockApplyShared();
        var merge_stats = db.core.index_manager.textMergeStatsSnapshot();
        if (self.merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
        return merge_stats;
    }

    pub fn trySnapshotMergeStats(self: *TextIndexes) ?db_mod.types.TextMergeStats {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("trySnapshotTextMergeStats", .{db}) catch @panic("incompatible storage owner");
        if (!db.core.tryLockApplyShared()) return null;
        defer db.core.unlockApplyShared();
        var merge_stats = db.core.index_manager.textMergeStatsSnapshot();
        if (self.merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
        return merge_stats;
    }

    pub fn attemptMergeRestart(ptr: *anyopaque) !bool {
        const ctx: *db_mod.AsyncContext = @ptrCast(@alignCast(ptr));
        return if (ctx.text_merge_runtime) |runtime| try runtime.ensureRunning() else true;
    }

    pub fn quiesceMerge(self: *TextIndexes) bool {
        const runtime = self.merge_runtime orelse return false;
        return runtime.pause();
    }

    pub fn restartMerge(self: *TextIndexes, operation: []const u8, index_name: []const u8) void {
        const db = self.dbPtr();
        const runtime = self.merge_runtime orelse return;
        runtime.resumeAfterPause() catch |err| {
            // Index catalog durability is already decided at this point.
            // Queries and foreground indexing remain available; surface the
            // maintenance degradation without misreporting the mutation.
            std.log.warn("failed to restart text merge runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
            DB.scheduleMaintenanceRestartContext(
                db.async_context,
                db.backend_runtime.durable_jobs,
                db.indexes.repair.cleanup_owner_id,
                .text_merge,
            );
        };
    }

    pub fn drainScheduledMerges(self: *TextIndexes) !void {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var snapshot_replay = try db.acquireSnapshotReplayMutation();
        defer snapshot_replay.release();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        try db.core.drainScheduledTextMerges();
    }
};
