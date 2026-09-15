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

//! Exclusive stopped-server operator. The catalog lock is also acquired by
//! standalone startup; do not invoke against an older running binary.
const std = @import("std");
const antfly = @import("antfly-zig");
const migration = antfly.vector_migration;

pub fn main(init: std.process.Init) !void {
    const alloc = init.arena.allocator();
    const args = try init.minimal.args.toSlice(alloc);
    var catalog_path: ?[]const u8 = null;
    var replicas: ?[]const u8 = null;
    var table_name: ?[]const u8 = null;
    var job_id: ?[]const u8 = null;
    var budget: migration.Budget = .{};
    var once = false;
    var cancelling = false;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            std.debug.print("usage: antfly-vector-migrate --catalog PATH --replica-root PATH --table NAME --job ID [--once | --cancel] [--batch-bytes N] [--temporary-bytes N] [--disk-reserve-bytes N]\nStop standalone before running; retry with the same job ID and budgets to resume.\n", .{});
            return;
        }
        if (std.mem.eql(u8, args[i], "--cancel")) {
            cancelling = true;
            continue;
        }
        if (std.mem.eql(u8, args[i], "--once")) {
            once = true;
            continue;
        }
        if (i + 1 == args.len) return error.MissingOptionValue;
        const value = args[i + 1];
        if (std.mem.eql(u8, args[i], "--catalog")) catalog_path = value else if (std.mem.eql(u8, args[i], "--replica-root")) replicas = value else if (std.mem.eql(u8, args[i], "--table")) table_name = value else if (std.mem.eql(u8, args[i], "--job")) job_id = value else if (std.mem.eql(u8, args[i], "--batch-bytes")) budget.batch_bytes = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, args[i], "--temporary-bytes")) budget.temporary_bytes = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, args[i], "--disk-reserve-bytes")) budget.disk_reserve_bytes = try std.fmt.parseInt(u64, value, 10) else return error.UnknownOption;
        i += 1;
    }
    const path = catalog_path orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const root = replicas orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const name = table_name orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const request = migration.Request{ .job_id = job_id orelse return error.ExpectedCatalogReplicaRootTableAndJob, .mode = .offline, .budget = budget };
    try request.validate();
    const lock = try antfly.migration_files.lockCatalog(alloc, init.io, path);
    defer lock.close(init.io);
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, path, alloc, .limited(64 * 1024 * 1024));
    // Preserve unknown fields and extension catalogs, changing only this
    // table's ownership/marker and the catalog epoch.
    var catalog = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer catalog.deinit();
    const json_alloc = catalog.arena.allocator();
    const table_values = catalog.value.object.getPtr("tables") orelse return error.InvalidCatalog;
    var table_value: *std.json.Value = blk: {
        for (table_values.array.items) |*entry| {
            const n = entry.object.get("name") orelse continue;
            if (n == .string and std.mem.eql(u8, n.string, name)) break :blk entry;
        }
        return error.TableNotFound;
    };
    const table_json = try std.json.Stringify.valueAlloc(alloc, table_value.*, .{});
    var table = try std.json.parseFromSlice(antfly.metadata.TableRecord, alloc, table_json, .{ .ignore_unknown_fields = true });
    defer table.deinit();
    if (table.value.desired_replica_count != 1 or table.value.min_ranges != 1 or
        table.value.read_schema_json.len != 0 or table.value.restore_backup_id.len != 0) return error.VectorStoreRequiresLocalSingleShardTable;
    var replication = try std.json.parseFromSlice(std.json.Value, alloc, table.value.replication_sources_json, .{});
    defer replication.deinit();
    if (replication.value != .array or replication.value.array.items.len != 0) return error.VectorStoreRequiresLocalSingleShardTable;
    const ranges_json = try std.json.Stringify.valueAlloc(alloc, catalog.value.object.get("ranges") orelse return error.InvalidCatalog, .{});
    var ranges = try std.json.parseFromSlice([]const antfly.metadata.RangeRecord, alloc, ranges_json, .{ .ignore_unknown_fields = true });
    defer ranges.deinit();
    const range = blk: {
        var selected: ?antfly.metadata.RangeRecord = null;
        for (ranges.value) |entry| if (entry.table_id == table.value.table_id) {
            if (selected != null or entry.start_key.len != 0 or (entry.end_key != null and entry.end_key.?.len != 0) or entry.restore_backup_id.len != 0)
                return error.VectorStoreRequiresLocalSingleShardTable;
            selected = entry;
        };
        break :blk selected orelse return error.TableNotFound;
    };
    if (table.value.storage_migration) |admitted| {
        if (!admitted.eql(.{ .request = request })) return error.VectorMigrationIdempotencyConflict;
    } else if (!cancelling and table.value.storage.dense_embeddings == .primary_lsm) {
        const admission_json = try std.json.Stringify.valueAlloc(alloc, migration.Admission{ .request = request }, .{});
        const admitted = try std.json.parseFromSliceLeaky(std.json.Value, json_alloc, admission_json, .{ .allocate = .alloc_always });
        try table_value.object.put(json_alloc, "storage_migration", admitted);
        try publishCatalog(json_alloc, init.io, path, &catalog.value);
    }
    const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, root, range.group_id);
    if (cancelling) {
        try antfly.vector_migration_offline.cancel(std.heap.smp_allocator, init.io, db_path, request, .{ .identity_namespace = .{
            .table_id = table.value.table_id,
            .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
            .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
        } });
        _ = table_value.object.swapRemove("storage_migration");
        try publishCatalog(json_alloc, init.io, path, &catalog.value);
        std.debug.print("offline vector migration cancelled\n", .{});
        return;
    }
    const result = try antfly.vector_migration_offline.run(std.heap.smp_allocator, init.io, db_path, request, .{
        .open = .{
            .identity_namespace = .{
                .table_id = table.value.table_id,
                .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
                .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
            },
        },
        .max_steps = if (once) 1 else 0,
        .progress_fn = printProgress,
    });
    if (result == .complete) {
        var storage = std.json.ObjectMap{};
        try storage.put(json_alloc, "dense_embeddings", .{ .string = "vector_store" });
        try table_value.object.put(json_alloc, "storage", .{ .object = storage });
        _ = table_value.object.swapRemove("storage_migration");
        try publishCatalog(json_alloc, init.io, path, &catalog.value);
    }
    std.debug.print("offline vector migration {s}\n", .{@tagName(result)});
}
fn printProgress(_: ?*anyopaque, raw: []const u8) !void {
    std.debug.print("{s}\n", .{raw});
}
fn publishCatalog(alloc: std.mem.Allocator, io: std.Io, path: []const u8, value: *std.json.Value) !void {
    const epoch = value.object.get("epoch") orelse std.json.Value{ .integer = 0 };
    try value.object.put(alloc, "epoch", .{ .integer = try std.math.add(i64, epoch.integer, 1) });
    const encoded = try std.json.Stringify.valueAlloc(alloc, value.*, .{});
    defer alloc.free(encoded);
    try antfly.migration_files.writeAtomic(alloc, io, path, encoded);
}
