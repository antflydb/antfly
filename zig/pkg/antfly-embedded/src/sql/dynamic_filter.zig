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

//! Immutable build-side evidence installed before a probe scan is pulled.
//! Bloom collisions retain candidates; exact join/residual checks remain.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
pub const Column = struct { name: []const u8, type: @import("ast.zig").ColumnType };
pub const Domain = struct {
    minimum: ?Datum = null,
    maximum: ?Datum = null,
    min_owner: std.heap.ArenaAllocator,
    max_owner: std.heap.ArenaAllocator,
    fn add(self: *Domain, value: Datum) !void {
        if (value.sql_null) return;
        if (self.minimum == null or (try scalar.compareDatums(value, self.minimum.?)) == .lt) {
            _ = self.min_owner.reset(.free_all);
            self.minimum = try @import("operators.zig").cloneDatum(self.min_owner.allocator(), value);
        }
        if (self.maximum == null or (try scalar.compareDatums(value, self.maximum.?)) == .gt) {
            _ = self.max_owner.reset(.free_all);
            self.maximum = try @import("operators.zig").cloneDatum(self.max_owner.allocator(), value);
        }
    }
};
pub const Filter = struct {
    a: A,
    columns: []Column,
    domains: []Domain,
    bits: []u64,
    rows: usize = 0,
    sealed: bool = false,
    failed: bool = false,
    pub fn create(a: A, columns: []const Column, bytes: usize) !*Filter {
        const self = try a.create(Filter);
        errdefer a.destroy(self);
        const keys = try a.dupe(Column, columns);
        errdefer a.free(keys);
        const domains = try a.alloc(Domain, columns.len);
        errdefer a.free(domains);
        const bits = try a.alloc(u64, @max(16, @min(8192, bytes / 8)));
        @memset(bits, 0);
        for (domains) |*domain| domain.* = .{ .min_owner = .init(a), .max_owner = .init(a) };
        self.* = .{ .a = a, .columns = keys, .domains = domains, .bits = bits };
        return self;
    }
    pub fn close(self: *Filter) void {
        for (self.domains) |*domain| {
            domain.min_owner.deinit();
            domain.max_owner.deinit();
        }
        self.a.free(self.domains);
        self.a.free(self.columns);
        self.a.free(self.bits);
        self.a.destroy(self);
    }
    fn mask(self: *const Filter, hash: u64) struct { index: usize, bit: u64 } {
        const position = hash % (self.bits.len * 64);
        return .{ .index = @intCast(position / 64), .bit = @as(u64, 1) << @as(u6, @intCast(position % 64)) };
    }
    pub fn add(self: *Filter, values: []const Datum) !void {
        if (self.failed or self.sealed or values.len != self.columns.len) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        const hash = (try @import("operators.zig").HashJoin.keyHash(values)) orelse return;
        for (self.domains, values) |*domain, value| try domain.add(value);
        const first = self.mask(hash);
        const second = self.mask(std.math.rotr(u64, hash, 23));
        self.bits[first.index] |= first.bit;
        self.bits[second.index] |= second.bit;
        self.rows += 1;
    }
    pub fn applyBatch(self: *const Filter, a: A, batch: @import("../storage/rowsource/types.zig").ColumnBatch, selected: []bool) !void {
        if (self.failed or !self.sealed or selected.len != batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (self.rows == 0) {
            @memset(selected, false);
            return;
        }
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const columns = try a.alloc([]const Datum, self.columns.len);
        defer a.free(columns);
        @memset(columns, &.{});
        defer for (columns) |column| a.free(column);
        for (self.columns, columns, self.domains) |definition, *column, domain| {
            const values = try a.alloc(Datum, batch.rowCount());
            column.* = values;
            @memset(values, .{});
            const physical = batch.findColumn(definition.name);
            var dictionary: std.AutoHashMapUnmanaged(u32, Datum) = .empty;
            defer dictionary.deinit(a);
            for (selected, values, 0..) |*keep, *value, index| {
                if (!keep.*) continue;
                const one: @import("catalog.zig").ColumnPage = .{ .batch = batch, .selection = &.{index} };
                value.* = if (if (physical) |source| try source.dictionaryId(index) else null) |id| blk: {
                    if (dictionary.get(id)) |cached| break :blk cached;
                    const raw = try one.cell(scratch.allocator(), 0, definition.name);
                    const cell = Datum.json(try @import("describe.zig").coerceAlloc(scratch.allocator(), raw.value, definition.type));
                    try dictionary.put(a, id, cell);
                    break :blk cell;
                } else blk: {
                    const cell = try one.cell(scratch.allocator(), 0, definition.name);
                    break :blk Datum{ .value = if (cell.sql_null) .null else try @import("describe.zig").coerceAlloc(scratch.allocator(), cell.value, definition.type), .sql_null = cell.sql_null };
                };
                if (value.sql_null or (try scalar.compareDatums(value.*, domain.minimum.?)) == .lt or (try scalar.compareDatums(value.*, domain.maximum.?)) == .gt) keep.* = false;
            }
        }
        const hashes = try @import("batch_hash.zig").columns(a, columns, batch.rowCount(), false);
        defer a.free(hashes);
        for (selected, hashes) |*keep, optional| {
            if (!keep.*) continue;
            const hash = optional orelse {
                keep.* = false;
                continue;
            };
            const first = self.mask(hash);
            const second = self.mask(std.math.rotr(u64, hash, 23));
            keep.* = self.bits[first.index] & first.bit != 0 and self.bits[second.index] & second.bit != 0;
        }
    }
    pub fn contains(self: *const Filter, values: []const Datum) !bool {
        if (self.failed or !self.sealed or values.len != self.columns.len) return error.InvalidSqlBackendResponse;
        if (self.rows == 0) return false;
        const hash = (try @import("operators.zig").HashJoin.keyHash(values)) orelse return false;
        const first = self.mask(hash);
        const second = self.mask(std.math.rotr(u64, hash, 23));
        if (self.bits[first.index] & first.bit == 0 or self.bits[second.index] & second.bit == 0) return false;
        for (self.domains, values) |domain, value| {
            if ((try scalar.compareDatums(value, domain.minimum.?)) == .lt or (try scalar.compareDatums(value, domain.maximum.?)) == .gt) return false;
        }
        return true;
    }
};
test "SQL scan dynamic filters preserve composite numeric equality and SQL nulls" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{ .{ .name = "n", .type = .integer }, .{ .name = "s", .type = .string } }, 256);
    defer filter.close();
    for (0..1000) |i| try filter.add(&.{ Datum.json(.{ .integer = @intCast(i * 3) }), Datum.json(.{ .string = "x" }) });
    try filter.add(&.{ .{}, Datum.json(.{ .string = "null" }) });
    filter.sealed = true;
    for (0..1000) |i| try std.testing.expect(try filter.contains(&.{ Datum.json(.{ .float = @floatFromInt(i * 3) }), Datum.json(.{ .string = "x" }) }));
    try std.testing.expect(!try filter.contains(&.{ .{}, Datum.json(.{ .string = "x" }) }));
    try std.testing.expect(!try filter.contains(&.{ Datum.json(.{ .integer = 4000 }), Datum.json(.{ .string = "x" }) }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, filter.add(&.{ .{}, .{} }));
}

test "SQL batch dynamic masks match scalar membership for dictionaries nulls and selections" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{ .{ .name = "s", .type = .string }, .{ .name = "n", .type = .integer } }, 256);
    defer filter.close();
    try filter.add(&.{ Datum.json(.{ .string = "yes" }), Datum.json(.{ .integer = 9007199254740993 }) });
    filter.sealed = true;
    const batch: @import("../storage/rowsource/types.zig").ColumnBatch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &.{ .{ .relational_key = "1" }, .{ .relational_key = "2" }, .{ .relational_key = "3" }, .{ .relational_key = "4" } },
        .columns = &.{
            .{ .name = "s", .values = .{ .dictionary_bytes = .{ .values = &.{ "no", "yes" }, .indices = &.{ 1, 0, 99, 1 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
            .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{9007199254740993}, .indices = &.{ 0, 0, 99, 0 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        },
    };
    var selected = [_]bool{ true, true, true, false };
    try filter.applyBatch(a, batch, &selected);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false, false }, &selected);
}
