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

const std = @import("std");
const backup_restore = @import("../raft/storage/backup_restore.zig");
const db_mod = @import("../storage/db/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const tables_api = @import("../api/tables.zig");

pub const ShardDbAdapter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        fetch_median_key: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) anyerror!?[]u8,
        schema_index_ready: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            group_id: u64,
            schema_version: u32,
            read_schema_version: u32,
        ) anyerror!bool,
    };

    pub fn fetchMedianKey(self: ShardDbAdapter, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        return try self.vtable.fetch_median_key(self.ptr, alloc, group_id);
    }

    pub fn schemaIndexReady(
        self: ShardDbAdapter,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        schema_version: u32,
        read_schema_version: u32,
    ) !bool {
        return try self.vtable.schema_index_ready(self.ptr, alloc, table_name, group_id, schema_version, read_schema_version);
    }
};

pub const FallbackLocalShardDbAdapter = struct {
    replica_root_dir: []const u8,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,

    pub fn adapter(self: *@This()) ShardDbAdapter {
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
                .schema_index_ready = schemaIndexReady,
            },
        };
    }

    fn openOptions(self: *const @This(), status_only: bool) db_mod.OpenOptions {
        return .{
            .open_mode = if (status_only) .status_only else .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .backend_runtime = self.backend_runtime,
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const path = try backup_restore.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var db = db_mod.DB.open(alloc, path, self.openOptions(false)) catch |err| switch (err) {
            error.FileNotFound => return error.UnknownGroup,
            else => return err,
        };
        defer db.close();

        return db.findMedianKey(alloc) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }

    fn schemaIndexReady(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        schema_version: u32,
        read_schema_version: u32,
    ) !bool {
        _ = table_name;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const path = try backup_restore.groupDbPathFromReplicaRoot(alloc, self.replica_root_dir, group_id);
        defer alloc.free(path);

        var db = try db_mod.DB.open(alloc, path, self.openOptions(true));
        defer db.close();

        const stats = try db.stats(alloc);
        defer db_mod.types.freeDBStats(alloc, stats);

        return try dbStatsSchemaIndexReady(alloc, stats, schema_version, read_schema_version);
    }
};

pub fn dbStatsSchemaIndexReady(
    alloc: std.mem.Allocator,
    stats: db_mod.types.DBStats,
    schema_version: u32,
    read_schema_version: u32,
) !bool {
    const target_name = try fullTextIndexName(alloc, schema_version);
    defer if (schema_version != 0) alloc.free(target_name);
    const target_index = findIndexStats(stats.indexes, target_name) orelse return false;
    if (!indexStatsReady(target_index)) return false;
    if (schema_version == read_schema_version) return true;

    const read_name = try fullTextIndexName(alloc, read_schema_version);
    defer if (read_schema_version != 0) alloc.free(read_name);
    const read_index = findIndexStats(stats.indexes, read_name) orelse return true;
    if (!indexStatsReady(read_index)) return false;
    return true;
}

fn fullTextIndexName(alloc: std.mem.Allocator, version: u32) ![]const u8 {
    if (version == 0) return tables_api.default_full_text_index_name;
    return try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{version});
}

fn findIndexStats(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return index;
    }
    return null;
}

fn indexStatsReady(index: db_mod.types.DBIndexStats) bool {
    if (index.kind != .full_text) return false;
    if (index.backfill_active) return false;
    if (index.replay_catch_up_required) return false;
    if (index.replay_applied_sequence < index.replay_target_sequence) return false;
    return true;
}
