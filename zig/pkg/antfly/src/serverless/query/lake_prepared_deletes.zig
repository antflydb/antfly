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
    request: iceberg.DeleteRowRefsReadRequest,
    equality: []Equality,
    positions: std.AutoHashMapUnmanaged(Position, void) = .empty,
    columns: []const []const u8,
    files: std.StringHashMapUnmanaged(FileIndex) = .empty,
    decoded_pages: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    const FileIndex = struct { index: usize, equality: []const usize = &.{}, prefixes: ?[]const u64 = null };
    const Equality = struct { file: usize, keys: std.StringHashMapUnmanaged(void) = .empty };
    const Position = struct { file: usize, ordinal: u64 };
    pub fn create(a: A, request: iceberg.DeleteRowRefsReadRequest) !*Prepared {
        const self = try a.create(Prepared);
        errdefer a.destroy(self);
        self.* = .{ .arena = .init(a), .request = request, .equality = &.{}, .columns = &.{} };
        errdefer self.arena.deinit();
        const owned = self.arena.allocator();
        var equalities: std.ArrayList(Equality) = .empty;
        var columns: std.ArrayList([]const u8) = .empty;
        var scanned: u64 = 0;
        var key_bytes: usize = 0;
        for (request.delete_plan.files, 0..) |file, file_index| {
            var inventory = try iceberg.singleDeleteFileInventoryAlloc(a, request.data_inventory, file, request.client, if (file.content == .equality_deletes) "iceberg-equality-delete" else "iceberg-position-delete");
            defer inventory.deinit(a);
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
                try columns.append(owned, name);
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
            try self.files.put(owned, file.file_id, .{ .index = index, .equality = applicable.items });
        }
        return self;
    }
    pub fn destroy(self: *Prepared, a: A) void {
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn matches(self: *Prepared, a: A, file: @import("../external_source/types.zig").FileEntry, batch: types.ColumnBatch, index: usize) !bool {
        const ref = batch.row_refs[index];
        const info = self.files.getPtr(file.file_id) orelse return error.ExternalSourceFileNotFound;
        if (self.positions.count() != 0) {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
            const prefixes = self.positionPrefixes(info, file) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.mutex.unlock();
            const ordinal = try std.math.add(u64, ref.external.row_ordinal, prefixes[ref.external.row_group_ordinal]);
            if (self.positions.contains(.{ .file = info.index, .ordinal = ordinal })) return true;
        }
        for (info.equality) |equality_index| {
            const equality = self.equality[equality_index];
            const delete_file = self.request.delete_plan.files[equality.file];
            const key = try deletes.projectedEqualityKeyFromBatchRowAlloc(a, batch, index, delete_file.equality_columns);
            defer a.free(key);
            if (equality.keys.contains(key)) return true;
        }
        return false;
    }
    pub fn bindFile(self: *Prepared, file: @import("../external_source/types.zig").FileEntry) !void {
        if (self.positions.count() == 0) return;
        const info = self.files.getPtr(file.file_id) orelse return error.ExternalSourceFileNotFound;
        _ = try self.positionPrefixes(info, file);
    }
    fn positionPrefixes(self: *Prepared, info: *FileIndex, file: @import("../external_source/types.zig").FileEntry) ![]const u64 {
        if (info.prefixes == null) {
            const prefixes = try self.arena.allocator().alloc(u64, file.row_groups.len);
            var total: u64 = 0;
            for (file.row_groups, prefixes) |group, *prefix| {
                prefix.* = total;
                total = try std.math.add(u64, total, group.row_count);
            }
            info.prefixes = prefixes;
        }
        return info.prefixes.?;
    }
};
