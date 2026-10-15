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
    positions: Positions = .{},
    columns: []const []const u8,
    files: std.StringHashMapUnmanaged(FileIndex) = .empty,
    decoded_pages: usize = 0,
    foreign_bytes: usize = 0,
    object_versions: [32]u8 = undefined,
    file_leases: std.ArrayList(@import("lake_decoded_cache.zig").Lease) = .empty,
    const FileIndex = struct { index: usize, equality: []const usize = &.{}, fingerprint: [32]u8 = @splat(0) };
    const Equality = struct { file: usize, fingerprint: [32]u8 = @splat(0), keys: std.StringHashMapUnmanaged(void) = .empty };
    const Position = struct { file: usize, ordinal: u64 };
    const PositionSet = @import("lake_delete_positions.zig").Set;
    const Positions = struct {
        borrowed: std.AutoHashMapUnmanaged(usize, *const PositionSet) = .empty,
        owned: std.AutoHashMapUnmanaged(usize, PositionSet) = .empty,
        total: usize = 0,
        pub fn count(self: *const @This()) usize {
            return self.total;
        }
        fn get(self: *const @This(), file: usize) ?*const PositionSet {
            return self.borrowed.get(file) orelse self.owned.getPtr(file);
        }
        fn contains(self: *const @This(), position: Position) bool {
            return if (self.get(position.file)) |set| set.contains(position.ordinal) else false;
        }
        fn mutable(self: *@This(), a: A, file: usize) !*PositionSet {
            const slot = try self.owned.getOrPut(a, file);
            if (!slot.found_existing) slot.value_ptr.* = .{};
            if (self.borrowed.get(file)) |prior| {
                try slot.value_ptr.merge(a, prior);
                _ = self.borrowed.remove(file);
            }
            return slot.value_ptr;
        }
        fn put(self: *@This(), a: A, position: Position, _: void) !void {
            const set = try self.mutable(a, position.file);
            const before = set.count;
            try set.add(a, position.ordinal);
            self.total += set.count - before;
        }
        fn adopt(self: *@This(), a: A, file: usize, set: *const PositionSet) !void {
            if (self.get(file)) |prior| {
                if (prior == set) return;
                const target = try self.mutable(a, file);
                const before = target.count;
                try target.merge(a, set);
                self.total += target.count - before;
            } else {
                try self.borrowed.put(a, file, set);
                self.total += set.count;
            }
        }
        fn seal(self: *@This(), a: A) !void {
            var values = self.owned.valueIterator();
            while (values.next()) |set| try set.seal(a);
        }
    };
    pub fn hashObjectVersion(hash: *std.crypto.hash.sha2.Sha256, uri: []const u8, etag: []const u8, version: []const u8) void {
        for ([_][]const u8{ uri, etag, version }) |part| {
            var length: [8]u8 = undefined;
            std.mem.writeInt(u64, &length, @intCast(part.len), .little);
            hash.update(&length);
            hash.update(part);
        }
    }
    pub fn create(a: A, request: iceberg.DeleteRowRefsReadRequest) !*Prepared {
        return createCached(a, request, null, @splat(0), .{});
    }
    pub fn createCached(a: A, request: iceberg.DeleteRowRefsReadRequest, cache: ?*@import("lake_decoded_cache.zig").Cache, scope: [32]u8, context: @import("lake_read_context.zig").Context) !*Prepared {
        const self = try a.create(Prepared);
        errdefer a.destroy(self);
        self.* = .{ .arena = .init(a), .delete_files = &.{}, .equality = &.{}, .columns = &.{} };
        errdefer {
            for (self.file_leases.items) |lease| lease.release();
            self.arena.deinit();
        }
        const owned = self.arena.allocator();
        var equalities: std.ArrayList(Equality) = .empty;
        var columns: std.ArrayList([]const u8) = .empty;
        var scanned: u64 = 0;
        var key_bytes: usize = 0;
        var data_paths: std.StringHashMapUnmanaged(usize) = .empty;
        for (request.data_inventory.files, 0..) |entry, i| for ([_][]const u8{ entry.file_id, entry.object_uri }) |path| {
            const slot = try data_paths.getOrPut(owned, path);
            if (!slot.found_existing) slot.value_ptr.* = i;
        };
        var versions = std.crypto.hash.sha2.Sha256.init(.{});
        for (request.delete_plan.files, 0..) |file, file_index| {
            var inventory = try iceberg.singleDeleteFileInventoryAlloc(a, request.data_inventory, file, request.client, if (file.content == .equality_deletes) "iceberg-equality-delete" else "iceberg-position-delete");
            defer inventory.deinit(a);
            var file_versions = std.crypto.hash.sha2.Sha256.init(.{});
            const recipe = try std.json.Stringify.valueAlloc(a, file, .{});
            defer a.free(recipe);
            file_versions.update(recipe);
            for (inventory.files) |entry| {
                hashObjectVersion(&versions, entry.object_uri, entry.etag, entry.version_id);
                hashObjectVersion(&file_versions, entry.object_uri, entry.etag, entry.version_id);
            }
            if (cache) |shared| {
                var key_hash = std.crypto.hash.sha2.Sha256.init(.{});
                key_hash.update("immutable-delete-file-index-v1");
                key_hash.update(&scope);
                key_hash.update(&file_versions.finalResult());
                key_hash.update(request.data_inventory.schema_fingerprint);
                const policy = try std.json.Stringify.valueAlloc(a, .{ request.materialization_limits, request.application_limits }, .{});
                defer a.free(policy);
                key_hash.update(policy);
                const Loader = struct {
                    request: iceberg.DeleteRowRefsReadRequest,
                    inventory_: @import("../external_source/types.zig").Inventory,
                    file_: iceberg.IcebergDeleteFile,
                    fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                        const value: *@This() = @ptrCast(@alignCast(raw));
                        const decoded = try @import("lake_delete_file_index.zig").Index.create(item.budget.allocator(), value.request, value.inventory_, value.file_);
                        item.payload = .{ .delete_file = decoded };
                    }
                };
                var loader: Loader = .{ .request = request, .inventory_ = inventory, .file_ = file };
                const strong = for (inventory.files) |entry| {
                    if (entry.etag.len == 0 and (entry.version_id.len == 0 or std.mem.eql(u8, entry.version_id, "null") or std.mem.startsWith(u8, entry.version_id, "object-stat:"))) break false;
                } else true;
                const candidate_lease = if (strong) shared.acquire(key_hash.finalResult(), 64 * 1024 * 1024, context, .{ .ptr = &loader, .load = Loader.load }) catch |err| switch (err) {
                    error.OutOfMemory, error.SqlMemoryLimitExceeded => null,
                    else => return err,
                } else null;
                const lease = if (candidate_lease) |held| available: {
                    if (held.item.budget.live > 256 * 1024 * 1024 -| self.foreign_bytes) {
                        held.release();
                        break :available null;
                    }
                    break :available @as(?@import("lake_decoded_cache.zig").Lease, held);
                } else null;
                if (lease) |held| {
                    var adopted = false;
                    defer if (!adopted) held.release();
                    const decoded = held.item.payload.delete_file;
                    scanned +|= decoded.scanned;
                    if (scanned > request.application_limits.max_scanned_rows) return error.IcebergDeleteApplicationTooLarge;
                    key_bytes = try deletes.admitEqualityDeleteStorage(key_bytes, decoded.key_bytes);
                    self.decoded_pages += decoded.decoded_pages;
                    if (file.content == .equality_deletes) {
                        try equalities.append(owned, .{ .file = file_index, .fingerprint = file_versions.finalResult(), .keys = decoded.equality });
                        for (file.equality_columns) |name| {
                            const found = for (columns.items) |prior| {
                                if (std.mem.eql(u8, prior, name)) break true;
                            } else false;
                            if (!found) try columns.append(owned, try owned.dupe(u8, name));
                        }
                    } else {
                        for (request.data_inventory.files, 0..) |data_file, data_index| {
                            if (!try iceberg.positionDeleteAppliesToFile(data_file, file)) continue;
                            for ([_][]const u8{ data_file.file_id, data_file.object_uri }, 0..) |path, alias| {
                                if (alias == 1 and std.mem.eql(u8, path, data_file.file_id)) continue;
                                if (data_paths.get(path).? != data_index) continue;
                                const ordinals = decoded.positions.getPtr(path) orelse continue;
                                if (ordinals.count != 0 and ordinals.maximum >= data_file.row_count) return error.ExternalSourceRowOutOfBounds;
                                try self.positions.adopt(owned, data_index, ordinals);
                                if (self.positions.count() > request.application_limits.max_deleted_rows) return error.IcebergDeleteApplicationTooLarge;
                            }
                        }
                    }
                    try self.file_leases.append(owned, held);
                    self.foreign_bytes += held.item.budget.live;
                    adopted = true;
                    continue;
                }
                // Oversized optional indexes retain the streaming subset path.
            }
            const names: []const []const u8 = if (file.content == .equality_deletes) file.equality_columns else &.{ "file_path", "pos" };
            var discovered = if (file.content == .equality_deletes)
                try iceberg.discoverEqualityColumnsAlloc(a, request, inventory, file.equality_ids, names)
            else
                try parquet.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(a, request.reader, inventory, names, request.footer_probe_bytes);
            defer discovered.deinit(a);
            var equality: Equality = .{ .file = file_index, .fingerprint = file_versions.finalResult() };
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
                            const data_index = data_paths.get(position.data_file_path) orelse continue;
                            const data_file = request.data_inventory.files[data_index];
                            if (!try iceberg.positionDeleteAppliesToFile(data_file, file)) continue;
                            if (position.row_position >= data_file.row_count) return error.ExternalSourceRowOutOfBounds;
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
        try self.positions.seal(owned);
        for (request.data_inventory.files, 0..) |file, index| {
            var applicable: std.ArrayList(usize) = .empty;
            for (self.equality, 0..) |entry, equality_index| {
                if (try iceberg.equalityDeleteAppliesToFile(file, request.delete_plan.files[entry.file])) try applicable.append(owned, equality_index);
            }
            var empty: PositionSet = .{};
            // Empty sets allocate no blocks; the prefix preserves v1 identities.
            try empty.seal(owned);
            var digest = (self.positions.get(index) orelse &empty).fingerprint.?;
            for (applicable.items) |equality_index| digest.update(&self.equality[equality_index].fingerprint);
            try self.files.put(owned, try owned.dupe(u8, file.file_id), .{ .index = index, .equality = applicable.items, .fingerprint = digest.finalResult() });
        }
        // Cached indexes retain only immutable, owned metadata. Provider and
        // request cancellation handles must never escape source admission.
        const bytes = try std.json.Stringify.valueAlloc(a, request.delete_plan.files, .{});
        defer a.free(bytes);
        self.object_versions = versions.finalResult();
        self.delete_files = try std.json.parseFromSliceLeaky([]iceberg.IcebergDeleteFile, owned, bytes, .{ .allocate = .alloc_always });
        return self;
    }
    pub fn emptyFileFingerprint() [32]u8 {
        var digest = std.crypto.hash.sha2.Sha256.init(.{});
        digest.update("native-lake-file-deletes-v1");
        const empty_count: [8]u8 = @splat(0);
        digest.update(&empty_count);
        return digest.finalResult();
    }
    pub fn fileFingerprint(self: *const Prepared, id: []const u8) ![32]u8 {
        return (self.files.get(id) orelse return error.ExternalSourceFileNotFound).fingerprint;
    }
    pub fn foreignBytes(self: *const Prepared) usize {
        return self.foreign_bytes;
    }
    pub fn destroy(self: *Prepared, a: A) void {
        for (self.file_leases.items) |lease| lease.release();
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

test "external lake position delete fingerprints preserve unaffected file identities" {
    const external = @import("../external_source/types.zig");
    const Reader = struct {
        bytes: []const u8,
        fn read(raw: *anyopaque, a: A, _: []const u8, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (offset > self.bytes.len or len > self.bytes.len - offset) return error.InvalidLakeRangeRead;
            return a.dupe(u8, self.bytes[@intCast(offset)..][0..len]);
        }
    };
    const a = std.testing.allocator;
    const first = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "pos", .values = &.{0} }}, &.{.{ .column_id = "file_path", .values = &.{"s3://bucket/a"} }});
    defer a.free(first);
    const next = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "pos", .values = &.{1} }}, &.{.{ .column_id = "file_path", .values = &.{"s3://bucket/a"} }});
    defer a.free(next);
    var files = [_]external.FileEntry{
        .{ .file_id = @constCast("s3://bucket/a"), .object_uri = @constCast("s3://bucket/a"), .version_id = @constCast("a"), .byte_len = 10, .row_count = 2, .data_sequence_number = 5, .partition_spec_id = 0, .row_groups = &.{} },
        .{ .file_id = @constCast("s3://bucket/b"), .object_uri = @constCast("s3://bucket/b"), .version_id = @constCast("b"), .byte_len = 10, .row_count = 2, .data_sequence_number = 5, .partition_spec_id = 0, .row_groups = &.{} },
    };
    const inventory: external.Inventory = .{ .format = .iceberg, .source_id = @constCast("events"), .source_uri = @constCast("s3://bucket/t"), .snapshot_id = @constCast("12"), .schema_fingerprint = @constCast("schema"), .files = &files };
    var reader: Reader = .{ .bytes = first };
    var delete_files = [_]iceberg.IcebergDeleteFile{.{ .content = .position_deletes, .file_path = @constCast("s3://bucket/deleted"), .file_format = @constCast("PARQUET"), .snapshot_id = 12, .data_sequence_number = 7, .file_sequence_number = 8, .record_count = 1, .file_size_in_bytes = first.len }};
    const before = try Prepared.create(a, .{ .reader = .{ .ctx = &reader, .read_range_alloc = Reader.read }, .data_inventory = inventory, .delete_plan = .{ .files = &delete_files } });
    defer before.destroy(a);
    reader.bytes = next;
    delete_files[0].file_size_in_bytes = next.len;
    const after = try Prepared.create(a, .{ .reader = .{ .ctx = &reader, .read_range_alloc = Reader.read }, .data_inventory = inventory, .delete_plan = .{ .files = &delete_files } });
    defer after.destroy(a);
    try std.testing.expect(!std.mem.eql(u8, &try before.fileFingerprint(files[0].file_id), &try after.fileFingerprint(files[0].file_id)));
    try std.testing.expectEqualSlices(u8, &try before.fileFingerprint(files[1].file_id), &try after.fileFingerprint(files[1].file_id));
    try std.testing.expectEqualSlices(u8, &Prepared.emptyFileFingerprint(), &try after.fileFingerprint(files[1].file_id));
}

test "external lake metadata navigation shares immutable delete files across selected inventories" {
    const a = std.testing.allocator;
    const external = @import("../external_source/types.zig");
    const Reader = struct {
        bytes: []const u8,
        calls: usize = 0,
        fn read(raw: *anyopaque, alloc: A, _: []const u8, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (offset > self.bytes.len or len > self.bytes.len - offset) return error.InvalidLakeRangeRead;
            return alloc.dupe(u8, self.bytes[@intCast(offset)..][0..len]);
        }
    };
    const bytes = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "pos", .values = &.{ 0, 1 } }}, &.{.{ .column_id = "file_path", .values = &.{ "s3://bucket/a", "s3://bucket/b" } }});
    defer a.free(bytes);
    var memory = @import("../../storage/object_storage.zig").MemoryObjectStorage.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    var put = try client.putObject("bucket", "deleted", bytes, .{});
    put.deinit(a);
    var reader: Reader = .{ .bytes = bytes };
    var files = [_]external.FileEntry{
        .{ .file_id = @constCast("s3://bucket/a"), .object_uri = @constCast("s3://bucket/a"), .version_id = @constCast("a"), .byte_len = 10, .row_count = 2, .data_sequence_number = 5, .partition_spec_id = 0, .row_groups = &.{} },
        .{ .file_id = @constCast("s3://bucket/b"), .object_uri = @constCast("s3://bucket/b"), .version_id = @constCast("b"), .byte_len = 10, .row_count = 2, .data_sequence_number = 5, .partition_spec_id = 0, .row_groups = &.{} },
    };
    var inventory: external.Inventory = .{ .format = .iceberg, .source_id = @constCast("events"), .source_uri = @constCast("s3://bucket/t"), .snapshot_id = @constCast("12"), .schema_fingerprint = @constCast("schema"), .files = files[0..1] };
    var delete_files = [_]iceberg.IcebergDeleteFile{.{ .content = .position_deletes, .file_path = @constCast("s3://bucket/deleted"), .file_format = @constCast("PARQUET"), .snapshot_id = 12, .data_sequence_number = 7, .file_sequence_number = 8, .record_count = 2, .file_size_in_bytes = bytes.len }};
    var cache: @import("lake_decoded_cache.zig").Cache = .{ .a = a };
    defer cache.deinit();
    var request: iceberg.DeleteRowRefsReadRequest = .{ .reader = .{ .ctx = &reader, .read_range_alloc = Reader.read }, .client = client, .data_inventory = inventory, .delete_plan = .{ .files = &delete_files } };
    const first = try Prepared.createCached(a, request, &cache, @splat(1), .{});
    defer first.destroy(a);
    const calls = reader.calls;
    inventory.files = files[1..2];
    request.data_inventory = inventory;
    const second = try Prepared.createCached(a, request, &cache, @splat(1), .{});
    defer second.destroy(a);
    try std.testing.expectEqual(calls, reader.calls);
    try std.testing.expect(first.file_leases.items[0].item == second.file_leases.items[0].item);
    try std.testing.expectEqual(@as(usize, 0), first.positions.owned.count());
    try std.testing.expectEqual(@as(usize, 0), second.positions.owned.count());
    const repeated = try Prepared.createCached(a, request, &cache, @splat(1), .{});
    defer repeated.destroy(a);
    try std.testing.expect(second.positions.get(0).? == repeated.positions.get(0).?);
    try std.testing.expect(first.positions.contains(.{ .file = 0, .ordinal = 0 }));
    try std.testing.expect(second.positions.contains(.{ .file = 0, .ordinal = 1 }));
    cache.max_loading_bytes = 0;
    const fallback = try Prepared.createCached(a, request, &cache, @splat(1), .{});
    defer fallback.destroy(a);
    try std.testing.expect(reader.calls > calls);
    try std.testing.expect(fallback.positions.contains(.{ .file = 0, .ordinal = 1 }));
    cache.max_loading_bytes = 64 * 1024 * 1024;
    const changed = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{.{ .column_id = "pos", .values = &.{ 1, 0 } }}, &.{.{ .column_id = "file_path", .values = &.{ "s3://bucket/a", "s3://bucket/b" } }});
    defer a.free(changed);
    try std.testing.expectEqual(bytes.len, changed.len);
    put = try client.putObject("bucket", "deleted", changed, .{});
    put.deinit(a);
    reader.bytes = changed;
    const replaced = try Prepared.createCached(a, request, &cache, @splat(1), .{});
    defer replaced.destroy(a);
    try std.testing.expect(replaced.positions.contains(.{ .file = 0, .ordinal = 0 }));
    try std.testing.expect(replaced.file_leases.items[0].item != second.file_leases.items[0].item);
}
