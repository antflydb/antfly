// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Snapshot-owned delete indexes. Delete pages are decoded once; equality
//! membership is tested on the scan's current vectors, without a second data
//! scan or a materialized list of every deleted data row.
const std = @import("std");
const iceberg = @import("lake_iceberg_snapshot.zig");
const deletes = @import("lake_iceberg_deletes.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const Cursor = @import("lake_parquet_cursor.zig").Cursor;
const types = @import("../../storage/rowsource/types.zig");
const A = std.mem.Allocator;
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    delete_files: []const iceberg.IcebergDeleteFile,
    equality: []Equality,
    positions: std.AutoHashMapUnmanaged(Position, void) = .empty,
    columns: []const []const u8,
    files: std.StringHashMapUnmanaged(FileIndex) = .empty,
    decoded_pages: usize = 0,
    object_versions: [32]u8 = undefined,
    const FileIndex = struct { index: usize, equality: []const usize = &.{} };
    const Equality = struct { file: usize, keys: std.StringHashMapUnmanaged(void) = .empty };
    const Position = struct { file: usize, ordinal: u64 };
    pub fn hashObjectVersion(hash: *std.crypto.hash.sha2.Sha256, uri: []const u8, etag: []const u8, version: []const u8) void {
        for ([_][]const u8{ uri, etag, version }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, @intCast(part.len), .little);
            hash.update(&length);
            hash.update(part);
        }
    }
    pub fn create(a: A, request: iceberg.DeleteRowRefsReadRequest) !*Prepared {
        const self = try a.create(Prepared);
        errdefer a.destroy(self);
        self.* = .{ .arena = .init(a), .delete_files = &.{}, .equality = &.{}, .columns = &.{} };
        errdefer self.arena.deinit();
        const owned = self.arena.allocator();
        var equalities: std.ArrayList(Equality) = .empty;
        var columns: std.ArrayList([]const u8) = .empty;
        var scanned: u64 = 0;
        var key_bytes: usize = 0;
        var versions = std.crypto.hash.sha2.Sha256.init(.{});
        for (request.delete_plan.files, 0..) |file, file_index| {
            var inventory = try iceberg.singleDeleteFileInventoryAlloc(a, request.data_inventory, file, request.client, if (file.content == .equality_deletes) "iceberg-equality-delete" else "iceberg-position-delete");
            defer inventory.deinit(a);
            for (inventory.files) |entry| {
                hashObjectVersion(&versions, entry.object_uri, entry.etag, entry.version_id);
            }
            const names: []const []const u8 = if (file.content == .equality_deletes) file.equality_columns else &.{ "file_path", "pos" };
            var discovered = if (file.content == .equality_deletes)
                try iceberg.discoverEqualityColumnsAlloc(a, request, inventory, file.equality_ids, names)
            else
                try parquet.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(a, request.reader, inventory, names, request.footer_probe_bytes);
            defer discovered.deinit(a);
            var equality: Equality = .{ .file = file_index };
            for (names) |name| {
                if (file.content != .equality_deletes) break;
                if (for (columns.items) |prior| {
                    if (std.mem.eql(u8, prior, name)) break true;
                } else false) continue;
                try columns.append(owned, try owned.dupe(u8, name));
            }
            for (discovered.row_group_plan.row_groups) |group| {
                var limits = request.materialization_limits;
                limits.decimal_representation = .exact_string;
                var cursor = try Cursor.init(a, request.reader, discovered.inventory, group.file_id, group.row_group_ordinal, names, limits);
                defer cursor.deinit();
                while (try cursor.next()) |batch| {
                    scanned +|= batch.rowCount();
                    if (scanned > request.application_limits.max_scanned_rows) return error.IcebergDeleteApplicationTooLarge;
                    for (0..batch.rowCount()) |index| {
                        if (file.content == .position_deletes) {
                            const position = try iceberg.positionDeleteRowFromBatch(batch, index);
                            const data_file = deletes.fileForDeletePath(request.data_inventory, position.data_file_path) orelse continue;
                            if (!try iceberg.positionDeleteAppliesToFile(data_file, file)) continue;
                            if (position.row_position >= data_file.row_count) return error.ExternalSourceRowOutOfBounds;
                            const data_index = for (request.data_inventory.files, 0..) |candidate, i| {
                                if (std.mem.eql(u8, candidate.file_id, data_file.file_id)) break i;
                            } else unreachable;
                            try self.positions.put(owned, .{ .file = data_index, .ordinal = position.row_position }, {});
                            if (self.positions.count() > request.application_limits.max_deleted_rows) return error.IcebergDeleteApplicationTooLarge;
                        } else {
                            const key = try deletes.equalityKeyFromBatchRowAlloc(a, batch, index, names);
                            defer a.free(key);
                            if (equality.keys.contains(key)) continue;
                            key_bytes = try deletes.admitEqualityDeleteKeyStorage(key_bytes, key.len, 1);
                            try equality.keys.put(owned, try owned.dupe(u8, key), {});
                        }
                    }
                }
                self.decoded_pages += cursor.pages_decoded;
            }
            if (file.content == .equality_deletes) try equalities.append(owned, equality);
        }
        self.equality = equalities.items;
        self.columns = columns.items;
        for (request.data_inventory.files, 0..) |file, index| {
            var applicable: std.ArrayList(usize) = .empty;
            for (self.equality, 0..) |entry, equality_index| {
                if (try iceberg.equalityDeleteAppliesToFile(file, request.delete_plan.files[entry.file])) try applicable.append(owned, equality_index);
            }
            try self.files.put(owned, try owned.dupe(u8, file.file_id), .{ .index = index, .equality = applicable.items });
        }
        // Cached indexes retain only immutable, owned metadata. Provider and
        // request cancellation handles must never escape source admission.
        const bytes = try std.json.Stringify.valueAlloc(a, request.delete_plan.files, .{});
        defer a.free(bytes);
        self.object_versions = versions.finalResult();
        self.delete_files = try std.json.parseFromSliceLeaky([]iceberg.IcebergDeleteFile, owned, bytes, .{ .allocate = .alloc_always });
        return self;
    }
    pub fn destroy(self: *Prepared, a: A) void {
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn matches(self: *Prepared, a: A, file: @import("../external_source/types.zig").FileEntry, batch: types.ColumnBatch, index: usize) !bool {
        return self.matchesPositions(a, file, batch, index, &.{});
    }
    pub fn matchesPositions(self: *Prepared, a: A, file: @import("../external_source/types.zig").FileEntry, batch: types.ColumnBatch, index: usize, starts: []const u64) !bool {
        const ref = batch.row_refs[index];
        const info = self.files.getPtr(file.file_id) orelse return error.ExternalSourceFileNotFound;
        if (self.positions.count() != 0) {
            const ordinal = try std.math.add(u64, ref.external.row_ordinal, try positionPrefix(file, ref.external.row_group_ordinal, starts));
            if (self.positions.contains(.{ .file = info.index, .ordinal = ordinal })) return true;
        }
        for (info.equality) |equality_index| {
            const equality = self.equality[equality_index];
            const delete_file = self.delete_files[equality.file];
            const key = try deletes.projectedEqualityKeyFromBatchRowAlloc(a, batch, index, delete_file.equality_columns);
            defer a.free(key);
            if (equality.keys.contains(key)) return true;
        }
        return false;
    }
    /// Build one deletion mask per physical batch. Bind field sets once and
    /// cache dictionary key parts; reuse the canonical key buffer for lanes.
    pub fn mask(self: *Prepared, a: A, file: @import("../external_source/types.zig").FileEntry, batch: types.ColumnBatch, selected: []bool) !void {
        return self.maskPositions(a, file, batch, selected, &.{});
    }
    pub fn maskPositions(self: *Prepared, a: A, file: @import("../external_source/types.zig").FileEntry, batch: types.ColumnBatch, selected: []bool, starts: []const u64) !void {
        if (selected.len != batch.rowCount()) return error.InvalidParquetRowGroupBatch;
        try batch.validate();
        const info = self.files.getPtr(file.file_id) orelse return error.ExternalSourceFileNotFound;
        if (self.positions.count() != 0) {
            var group: ?u32 = null;
            var prefix: u64 = 0;
            for (batch.row_refs, selected) |ref, *keep| {
                if (!keep.*) continue;
                if (group != ref.external.row_group_ordinal) {
                    prefix = try positionPrefix(file, ref.external.row_group_ordinal, starts);
                    group = ref.external.row_group_ordinal;
                }
                const ordinal = try std.math.add(u64, ref.external.row_ordinal, prefix);
                if (self.positions.contains(.{ .file = info.index, .ordinal = ordinal })) keep.* = false;
            }
        }
        var key: std.ArrayListUnmanaged(u8) = .empty;
        defer key.deinit(a);
        for (info.equality, 0..) |equality_index, set_index| {
            const equality = self.equality[equality_index];
            const names = self.delete_files[equality.file].equality_columns;
            const repeated = for (info.equality[0..set_index]) |earlier| {
                if (sameFields(names, self.delete_files[self.equality[earlier].file].equality_columns)) break true;
            } else false;
            if (repeated) continue;
            const columns = try a.alloc(?types.ColumnVector, names.len);
            defer a.free(columns);
            const parts = try a.alloc(std.AutoHashMapUnmanaged(u32, []const u8), names.len);
            defer a.free(parts);
            @memset(parts, .empty);
            defer for (parts) |*dictionary| {
                var iterator = dictionary.valueIterator();
                while (iterator.next()) |part| a.free(part.*);
                dictionary.deinit(a);
            };
            for (names, columns) |name, *column| column.* = batch.findColumn(name);
            for (selected, 0..) |*keep, row| {
                if (!keep.*) continue;
                key.clearRetainingCapacity();
                for (columns, parts) |column, *dictionary| {
                    if (column == null or column.?.nulls.isNull(row)) {
                        try key.append(a, 0);
                    } else if (column.?.values == .dictionary_bytes) {
                        const id = column.?.values.dictionary_bytes.indices[row];
                        if (dictionary.get(id)) |part| {
                            try key.appendSlice(a, part);
                        } else {
                            const begin = key.items.len;
                            try deletes.appendEqualityColumnPart(a, &key, column.?, row);
                            const part = try a.dupe(u8, key.items[begin..]);
                            dictionary.put(a, id, part) catch |err| {
                                a.free(part);
                                return err;
                            };
                        }
                    } else try deletes.appendEqualityColumnPart(a, &key, column.?, row);
                }
                for (info.equality[set_index..]) |candidate| {
                    const entry = self.equality[candidate];
                    if (sameFields(names, self.delete_files[entry.file].equality_columns) and entry.keys.contains(key.items)) {
                        keep.* = false;
                        break;
                    }
                }
            }
        }
    }
    fn sameFields(left: []const []const u8, right: []const []const u8) bool {
        if (left.len != right.len) return false;
        for (left, right) |a, b| if (!std.mem.eql(u8, a, b)) return false;
        return true;
    }
    pub fn bindFile(self: *Prepared, file: @import("../external_source/types.zig").FileEntry) !void {
        if (self.positions.count() == 0) return;
        if (!self.files.contains(file.file_id)) return error.ExternalSourceFileNotFound;
        if (file.row_groups.len != 0) _ = try positionPrefix(file, @intCast(file.row_groups.len - 1), &.{});
    }
    // Footer-derived offsets belong to the request's file plan. The cached
    // membership index remains immutable and its retained size cannot grow.
    fn positionPrefix(file: @import("../external_source/types.zig").FileEntry, ordinal: u32, starts: []const u64) !u64 {
        if (ordinal >= file.row_groups.len) return error.InvalidParquetRowGroupBatch;
        if (starts.len != 0) {
            if (starts.len != file.row_groups.len) return error.InvalidParquetRowGroupBatch;
            return starts[ordinal];
        }
        var total: u64 = 0;
        for (file.row_groups[0..ordinal]) |group| total = try std.math.add(u64, total, group.row_count);
        return total;
    }
};

test "external lake request position offsets preserve empty groups and cached membership" {
    const a = std.testing.allocator;
    var prepared: Prepared = .{ .arena = .init(a), .delete_files = &.{}, .equality = &.{}, .columns = &.{} };
    defer prepared.arena.deinit();
    const owned = prepared.arena.allocator();
    try prepared.files.put(owned, "f", .{ .index = 0 });
    try prepared.positions.put(owned, .{ .file = 0, .ordinal = 8 }, {});
    var groups = [_]@import("../external_source/types.zig").RowGroup{
        .{ .ordinal = 0, .row_count = 3 },
        .{ .ordinal = 1, .row_count = 0 },
        .{ .ordinal = 2, .row_count = 5 },
        .{ .ordinal = 3, .row_count = 2 },
    };
    const file: @import("../external_source/types.zig").FileEntry = .{ .file_id = @constCast("f"), .object_uri = @constCast("object://b/f"), .byte_len = 1, .row_count = 10, .row_groups = &groups };
    const batch: types.ColumnBatch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &.{
            .{ .external = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 3, .row_ordinal = 0 } },
            .{ .external = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 3, .row_ordinal = 1 } },
        },
        .columns = &.{},
    };
    var selected = [_]bool{ true, true };
    try prepared.maskPositions(a, file, batch, &selected, &.{ 0, 3, 3, 8 });
    try std.testing.expectEqualSlices(bool, &.{ false, true }, &selected);
    try std.testing.expect(try prepared.matches(a, file, batch, 0));
    try std.testing.expect(!try prepared.matchesPositions(a, file, batch, 1, &.{ 0, 3, 3, 8 }));
    try std.testing.expectError(error.InvalidParquetRowGroupBatch, prepared.matchesPositions(a, file, batch, 0, &.{0}));
    try std.testing.expectEqual(@as(usize, 1), prepared.positions.count());
}
