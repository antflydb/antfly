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

//! Immutable delete-file decoding, independent of a query's selected data files.
const std = @import("std");
const iceberg = @import("lake_iceberg_snapshot.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const deletes = @import("lake_iceberg_deletes.zig");
const A = std.mem.Allocator;
pub const Index = struct {
    arena: std.heap.ArenaAllocator,
    positions: std.StringHashMapUnmanaged(@import("lake_delete_positions.zig").Set) = .empty,
    equality: std.StringHashMapUnmanaged(void) = .empty,
    scanned: u64 = 0,
    decoded_pages: usize = 0,
    key_bytes: usize = 0,
    pub fn destroy(self: *Index, a: A) void {
        self.arena.deinit();
        a.destroy(self);
    }
    pub fn create(a: A, request: iceberg.DeleteRowRefsReadRequest, inventory: @import("../external_source/types.zig").Inventory, file: iceberg.IcebergDeleteFile) !*Index {
        const self = try a.create(Index);
        self.* = .{ .arena = .init(a) };
        errdefer self.destroy(a);
        const owned = self.arena.allocator();
        const names: []const []const u8 = if (file.content == .equality_deletes) file.equality_columns else &.{ "file_path", "pos" };
        var discovered = if (file.content == .equality_deletes)
            try iceberg.discoverEqualityColumnsAlloc(a, request, inventory, file.equality_ids, names)
        else
            try parquet.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(a, request.reader, inventory, names, request.footer_probe_bytes);
        defer discovered.deinit(a);
        for (discovered.row_group_plan.row_groups) |group| {
            var limits = request.materialization_limits;
            limits.decimal_representation = .exact_string;
            var cursor = try @import("lake_parquet_cursor.zig").Cursor.init(a, request.reader, discovered.inventory, group.file_id, group.row_group_ordinal, names, limits);
            defer cursor.deinit();
            while (try cursor.next()) |batch| {
                self.scanned +|= batch.rowCount();
                if (self.scanned > request.application_limits.max_scanned_rows) return error.IcebergDeleteApplicationTooLarge;
                for (0..batch.rowCount()) |row| {
                    if (file.content == .position_deletes) {
                        const value = try iceberg.positionDeleteRowFromBatch(batch, row);
                        const entry = try self.positions.getOrPut(owned, value.data_file_path);
                        if (!entry.found_existing) {
                            entry.key_ptr.* = try owned.dupe(u8, value.data_file_path);
                            entry.value_ptr.* = .{};
                        }
                        try entry.value_ptr.add(owned, value.row_position);
                    } else {
                        const key = try deletes.equalityKeyFromBatchRowAlloc(a, batch, row, names);
                        defer a.free(key);
                        if (!self.equality.contains(key)) {
                            self.key_bytes = try deletes.admitEqualityDeleteKeyStorage(self.key_bytes, key.len, 1);
                            try self.equality.put(owned, try owned.dupe(u8, key), {});
                        }
                    }
                }
            }
            self.decoded_pages += cursor.pages_decoded;
        }
        var positions = self.positions.valueIterator();
        while (positions.next()) |set| try set.seal(owned);
        return self;
    }
};
