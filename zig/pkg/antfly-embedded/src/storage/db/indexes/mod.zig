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

/// Index state owned by `DB`, grouped by index kind. Each kind keeps its
/// runtimes and background work in its own subsystem. Range splits touch every
/// kind, so their state stays here: the shadow index manager that builds the
/// new range's indexes, and the lock that serializes swapping in a split's
/// primary documents and indexes.
pub const Indexes = struct {
    generation_replace_mutex: std.atomic.Mutex = .unlocked,
    split_shadow: ?*db_mod.ShadowState = null,

    text: @import("text.zig").TextIndexes = .{},
    sparse: @import("sparse.zig").SparseIndexes = .{},
    graph: @import("graph.zig").GraphIndexes = .{},
    relational: @import("relational.zig").RelationalIndexes = .{},
    algebraic: @import("algebraic.zig").AlgebraicIndexes = .{},
    repair: @import("repair.zig").RepairIndexes = .{},

    pub fn dbPtr(self: *Indexes) *DB {
        return @alignCast(@fieldParentPtr("indexes", self));
    }

    pub fn dbConstPtr(self: *const Indexes) *const DB {
        return @alignCast(@fieldParentPtr("indexes", self));
    }

    pub fn createSplitShadow(self: *Indexes, split_key: []const u8, original_range_end: []const u8) !void {
        const db = self.dbPtr();
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        if (self.split_shadow != null) return error.ShadowIndexManagerExists;

        const base_path = try std.fmt.allocPrint(db.alloc, "{s}/.shadow-{d}", .{ db.core.path, db_mod.monotonicTimeNs() });
        errdefer db.alloc.free(base_path);
        const indexes_path = try std.fmt.allocPrint(db.alloc, "{s}/indexes", .{base_path});
        errdefer db.alloc.free(indexes_path);

        try db_mod.ensureDirPath(indexes_path);

        const shadow_manager = try db.alloc.create(db_mod.index_manager_mod.IndexManager);
        errdefer db.alloc.destroy(shadow_manager);
        shadow_manager.* = try db_mod.index_manager_mod.IndexManager.initWithOptions(
            db.alloc,
            base_path,
            db.index_backends,
        );
        errdefer shadow_manager.deinit();

        const shadow_start = try db.alloc.dupe(u8, split_key);
        errdefer db.alloc.free(shadow_start);
        const shadow_end = try db.alloc.dupe(u8, original_range_end);
        errdefer db.alloc.free(shadow_end);
        shadow_manager.updateRange(.{ .start = shadow_start, .end = shadow_end });

        try db.core.registerShadowIndexes(db.alloc, shadow_manager);

        const shadow = try db.alloc.create(db_mod.ShadowState);
        shadow.* = .{
            .manager = shadow_manager,
            .base_path = base_path,
            .indexes_path = indexes_path,
            .range_start = shadow_start,
            .range_end = shadow_end,
        };
        self.split_shadow = shadow;
        if (db.recovery.transactions.local_context) |ctx| {
            ctx.split_shadow = self.split_shadow;
        }
    }

    pub fn closeSplitShadow(self: *Indexes) !void {
        const db = self.dbPtr();
        var replication_mutation = db.acquireReplicationMutationShared();
        defer if (replication_mutation) |*lease| lease.release();
        try db.enforceReplicationWriteGate();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        try self.closeSplitShadowLocked();
    }

    pub fn closeSplitShadowLocked(self: *Indexes) !void {
        const db = self.dbPtr();
        try db_mod.waitForSplitShadowDrainLocked(db, false);
        const shadow = self.split_shadow orelse return;
        if (db.recovery.transactions.local_context) |ctx| {
            ctx.split_shadow = null;
        }
        shadow.manager.deinit();
        db.alloc.destroy(shadow.manager);
        db.alloc.free(shadow.base_path);
        db.alloc.free(shadow.indexes_path);
        db.alloc.free(shadow.range_start);
        db.alloc.free(shadow.range_end);
        db.alloc.destroy(shadow);
        self.split_shadow = null;
    }

    pub fn splitShadowDir(self: *Indexes) []const u8 {
        const shadow = self.split_shadow orelse return "";
        return shadow.indexes_path;
    }
};

test "db shadow index manager backfills split-off range and ignores parent-range live writes after split" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{\"field\":\"title\"}",
    });
    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
        .graph_writes = &.{
            .{ .index_name = "graph_v1", .source = "doc:z", .target = "doc:y", .edge_type = "cites", .weight = 1.0 },
        },
        .sync_level = .full_index,
    });

    try db.indexes.createSplitShadow("doc:m", "");
    try std.testing.expect(db.indexes.split_shadow != null);
    try std.testing.expect(db.indexes.splitShadowDir().len > 0);
    try std.testing.expectEqual(@as(u32, 1), db.indexes.split_shadow.?.manager.textIndex("ft_v1").?.snapshot().liveDocCount());

    try db.core.shard_manager.prepareSplit("doc:m");
    try db.core.shard_manager.split(0, "doc:m");
    db.core.index_manager.updateRange(db.core.shard_manager.getByteRange());

    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"bravo\"}" }},
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(u64, 1), db.indexes.split_shadow.?.next_ticket);
    try std.testing.expectEqual(db.indexes.split_shadow.?.next_ticket, db.indexes.split_shadow.?.applied_ticket);
    try std.testing.expect(!db.indexes.split_shadow.?.repair_required);
    try std.testing.expectEqual(@as(u32, 1), db.indexes.split_shadow.?.manager.textIndex("ft_v1").?.snapshot().liveDocCount());

    try db.indexes.closeSplitShadow();
    try std.testing.expectEqualStrings("", db.indexes.splitShadowDir());
}
