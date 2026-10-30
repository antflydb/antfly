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

/// Sparse vector index state owned by `DB.indexes`: the optional compaction
/// runtime, which pauses around structural index mutations.
pub const SparseIndexes = struct {
    compaction_owner: @import("../local_runtime_owner.zig").RuntimeOwner(db_mod.sparse_compaction_runtime_mod.SparseCompactionRuntime) = .{},
    compaction_runtime: ?*db_mod.sparse_compaction_runtime_mod.SparseCompactionRuntime = null,

    fn dbPtr(self: *SparseIndexes) *DB {
        const indexes: *Indexes = @alignCast(@fieldParentPtr("sparse", self));
        return indexes.dbPtr();
    }

    pub fn initCompactionRuntime(self: *SparseIndexes, cfg: db_mod.sparse_compaction_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initOptionalSparseCompactionRuntime", .{ db, cfg });
        if (!db.start_index_workers) return;
        if (!cfg.enabled) return;
        const resources = db.core.asyncResources();
        self.compaction_owner = try @TypeOf(self.compaction_owner).create(db.runtime_alloc, .{
            db.runtime_alloc,
            resources.index_manager,
            resources.apply_mutex,
            db.backend_runtime,
            cfg,
        });
        const runtime = self.compaction_owner.runtime.?;
        self.compaction_runtime = runtime;
        db.async_context.sparse_compaction_runtime = runtime;
    }

    pub fn attemptCompactionRestart(ptr: *anyopaque) !bool {
        const ctx: *db_mod.AsyncContext = @ptrCast(@alignCast(ptr));
        return if (ctx.sparse_compaction_runtime) |runtime| try runtime.ensureRunning() else true;
    }

    pub fn quiesceCompaction(self: *SparseIndexes) bool {
        const runtime = self.compaction_runtime orelse return false;
        return runtime.pause();
    }

    pub fn restartCompaction(self: *SparseIndexes, operation: []const u8, index_name: []const u8) void {
        const db = self.dbPtr();
        const runtime = self.compaction_runtime orelse return;
        runtime.resumeAfterPause() catch |err| {
            std.log.warn("failed to restart sparse compaction runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
            DB.scheduleMaintenanceRestartContext(
                db.async_context,
                db.backend_runtime.durable_jobs,
                db.repair_cleanup_owner_id,
                .sparse_compaction,
            );
        };
    }
};
