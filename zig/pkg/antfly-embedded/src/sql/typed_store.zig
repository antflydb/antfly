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

//! Column-oriented retained SQL state. Primitive values never retain Datum
//! tags; complex or heterogeneous columns retain native JSON values.
//! Datum rows are reconstructed only at an expression/result boundary. Validity is a packed SQL-null bitmap.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
/// Conservative physical payload estimate before cardinality is known.
/// Allocation admission still accounts for actual vector/hash capacity growth.
pub fn retainedCellBytes(value: Datum) !usize {
    return 1 +| (if (value.patterns != null) @sizeOf(?*scalar.PatternSet) else @as(usize, 0)) +| switch (value.value) {
        .integer, .float => @as(usize, 8),
        .bool => @as(usize, 1),
        .string, .number_string => |text| @sizeOf([]const u8) +| text.len +| 32,
        else => try @import("operators.zig").datumBytes(value),
    };
}
pub fn columnMetadataBytes(width: usize) usize {
    return width *| @sizeOf(Column);
}
const Dictionary = struct {
    values: std.ArrayList([]const u8) = .empty,
    indices: std.ArrayList(u32) = .empty,
    lookup: std.StringHashMapUnmanaged(u32) = .empty,
    flat: ?std.ArrayList([]const u8) = null,
    const empty: Dictionary = .{};
    fn growthBytes(self: *const Dictionary, additional: usize) usize {
        if (self.flat != null or additional <= self.lookup.available) return 0;
        // A hash-table resize allocates the replacement before releasing the
        // old table. Account for that capacity jump rather than one cell's
        // payload, including metadata, keys, values and alignment/header space.
        const entries = self.lookup.count() +| additional;
        const required = entries *| 100 / std.hash_map.default_max_load_percentage +| 1;
        const capacity = std.math.ceilPowerOfTwo(usize, @max(8, required)) catch return std.math.maxInt(usize);
        return capacity *| (@sizeOf(u8) + @sizeOf([]const u8) + @sizeOf(u32)) +| 64;
    }
    fn deinit(self: *Dictionary, a: A) void {
        self.values.deinit(a);
        self.indices.deinit(a);
        self.lookup.deinit(a);
        if (self.flat) |*values| values.deinit(a);
    }
    fn append(self: *Dictionary, a: A, owned: A, value: ?[]const u8) !void {
        // High-cardinality columns revert to flat storage after a bounded
        // sample; dictionary overhead must not penalize unique identifiers.
        if (self.flat == null and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
            var flat: std.ArrayList([]const u8) = .empty;
            errdefer flat.deinit(a);
            try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
            for (self.indices.items) |index| flat.appendAssumeCapacity(self.values.items[index]);
            self.values.clearAndFree(a);
            self.indices.clearAndFree(a);
            self.lookup.clearAndFree(a);
            self.flat = flat;
        }
        if (self.flat) |*values| {
            try values.append(a, if (value) |bytes| try owned.dupe(u8, bytes) else &.{});
            return;
        }
        const text = value orelse {
            try self.indices.append(a, 0);
            return;
        };
        if (self.lookup.get(text)) |index| {
            try self.indices.append(a, index);
            return;
        }
        const index = std.math.cast(u32, self.values.items.len) orelse return error.SqlProgramLimitExceeded;
        const bytes = try owned.dupe(u8, text);
        try self.values.append(a, bytes);
        try self.lookup.put(a, bytes, index);
        try self.indices.append(a, index);
    }
    fn resizeNulls(self: *Dictionary, a: A, count: usize) !void {
        try self.indices.resize(a, count);
        @memset(self.indices.items, 0);
    }
    fn getText(self: Dictionary, row: usize) []const u8 {
        return if (self.flat) |values| values.items[row] else self.values.items[self.indices.items[row]];
    }
};
// Sample once; retain repeated numeric values by ID without penalizing unique
// keys with a hash lookup on every subsequent append. Float bit patterns keep
// signed zero and NaN payloads distinct at this physical ownership boundary.
fn Numeric(comptime T: type) type {
    return struct {
        flat: std.ArrayList(T) = .empty,
        values: std.ArrayList(T) = .empty,
        indices: std.ArrayList(u32) = .empty,
        lookup: std.AutoHashMapUnmanaged(u64, u32) = .empty,
        encoded: bool = false,
        sampled: bool = false,
        const empty: @This() = .{};
        fn key(value: T) u64 {
            return @bitCast(value);
        }
        fn deinit(self: *@This(), a: A) void {
            self.flat.deinit(a);
            self.values.deinit(a);
            self.indices.deinit(a);
            self.lookup.deinit(a);
        }
        fn get(self: @This(), row: usize) T {
            return if (self.encoded) self.values.items[self.indices.items[row]] else self.flat.items[row];
        }
        fn resize(self: *@This(), a: A, count: usize) !void {
            try self.flat.resize(a, count);
            @memset(self.flat.items, 0);
        }
        fn encodeOne(self: *@This(), a: A, value: T) !void {
            const slot = try self.lookup.getOrPut(a, key(value));
            if (!slot.found_existing) {
                slot.value_ptr.* = @intCast(self.values.items.len);
                try self.values.append(a, value);
            }
            try self.indices.append(a, slot.value_ptr.*);
        }
        fn repeatedSample(self: *const @This()) bool {
            var keys: [512]u64 = undefined;
            var used: [8]u64 = @splat(0);
            var count: usize = 0;
            for (self.flat.items[0..256]) |value| {
                const bits = key(value);
                var slot: usize = @intCast((bits *% 0x9e3779b97f4a7c15) >> 55);
                while (true) : (slot = (slot + 1) & 511) {
                    const mask = @as(u64, 1) << @as(u6, @intCast(slot % 64));
                    if (used[slot / 64] & mask == 0) {
                        used[slot / 64] |= mask;
                        keys[slot] = bits;
                        count += 1;
                        if (count > 128) return false;
                        break;
                    }
                    if (keys[slot] == bits) break;
                }
            }
            return true;
        }
        fn append(self: *@This(), a: A, value: T) !void {
            if (!self.sampled and self.flat.items.len >= 256 and !@call(.never_inline, repeatedSample, .{self})) self.sampled = true;
            if (!self.sampled and self.flat.items.len >= 256) {
                // Build a candidate before publishing its representation.
                var candidate: @This() = .{ .sampled = true, .encoded = true };
                errdefer candidate.deinit(a);
                for (self.flat.items) |prior| try candidate.encodeOne(a, prior);
                self.sampled = true;
                if (candidate.values.items.len <= self.flat.items.len / 2) {
                    self.flat.deinit(a);
                    self.* = candidate;
                } else candidate.deinit(a);
            }
            if (self.encoded and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
                var flat: std.ArrayList(T) = .empty;
                errdefer flat.deinit(a);
                try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
                for (self.indices.items) |id| flat.appendAssumeCapacity(self.values.items[id]);
                self.values.clearAndFree(a);
                self.indices.clearAndFree(a);
                self.lookup.clearAndFree(a);
                self.flat = flat;
                self.encoded = false;
            }
            if (self.encoded) try self.encodeOne(a, value) else try self.flat.append(a, value);
        }
    };
}
const Column = struct {
    const Values = union(enum) {
        unknown,
        integers: Numeric(i64),
        numbers: Numeric(f64),
        booleans: std.ArrayList(bool),
        strings: Dictionary,
        decimals: Dictionary,
        encoded: std.ArrayList(std.json.Value),
    };
    values: Values = .unknown,
    nulls: std.ArrayList(u64) = .empty,
    patterns: std.ArrayList(?*scalar.PatternSet) = .empty,
    arrays: std.ArrayList(?*const @import("array_value.zig").Value) = .empty,
    numerics: std.ArrayList(?*const @import("numeric_value.zig").Value) = .empty,
    fn deinit(self: *Column, a: A) void {
        switch (self.values) {
            .unknown => {},
            inline else => |*values| values.deinit(a),
        }
        self.nulls.deinit(a);
        self.patterns.deinit(a);
        self.arrays.deinit(a);
        self.numerics.deinit(a);
    }
    fn isNull(self: Column, row: usize) bool {
        return self.nulls.items[row / 64] & (@as(u64, 1) << @as(u6, @intCast(row % 64))) != 0;
    }
    fn cell(self: Column, a: A, row: usize) !Datum {
        if (self.isNull(row)) return .{};
        _ = a;
        const value: std.json.Value = switch (self.values) {
            .unknown => return error.InvalidSqlBackendResponse,
            .integers => |v| .{ .integer = v.get(row) },
            .numbers => |v| .{ .float = v.get(row) },
            .booleans => |v| .{ .bool = v.items[row] },
            .strings => |v| .{ .string = v.getText(row) },
            .decimals => |v| .{ .number_string = v.getText(row) },
            .encoded => |v| v.items[row],
        };
        return .{ .value = value, .sql_null = false, .patterns = if (self.patterns.items.len == 0) null else self.patterns.items[row], .array = if (self.arrays.items.len == 0) null else self.arrays.items[row], .numeric = if (self.numerics.items.len == 0) null else self.numerics.items[row] };
    }
    fn appendDictionary(self: *Column, a: A, owned: A, begin: usize, batch: @import("execution_batch.zig").Batch) !bool {
        if (batch != .dictionary or self.patterns.items.len != 0 or self.arrays.items.len != 0 or self.numerics.items.len != 0) return false;
        const source = batch.dictionary;
        for (source.indices) |id| if (id >= source.values.len) return error.InvalidSqlBackendResponse;
        const remap = try a.alloc(u32, source.values.len);
        defer a.free(remap);
        @memset(remap, std.math.maxInt(u32));
        for (source.indices) |id| {
            remap[id] = 0;
        }
        var tag: ?std.meta.Tag(std.json.Value) = null;
        var referenced: usize = 0;
        for (source.values, remap) |value, id| {
            if (id == std.math.maxInt(u32)) continue;
            if (value.patterns != null or value.array != null or value.numeric != null) return false;
            if (value.sql_null) continue;
            const actual = std.meta.activeTag(value.value);
            if (actual != .integer and actual != .float and actual != .string and actual != .number_string) return false;
            if (tag != null and tag.? != actual) return false;
            tag = actual;
            referenced += 1;
        }
        if (tag == null) return false;
        // Preserve encoding only when it saves numeric storage. Parquet may
        // dictionary-encode unique IDs; importing that representation must not
        // override the retained store's flat/high-cardinality policy.
        const flat_numeric = switch (self.values) {
            .unknown => tag.? == .integer or tag.? == .float,
            inline .integers, .numbers => |v| !v.encoded,
            else => false,
        };
        if (flat_numeric and referenced > source.indices.len / 2) return false;
        if (self.values != .unknown) {
            const compatible = switch (self.values) {
                .integers => |v| tag.? == .integer and (v.encoded or !v.sampled),
                .numbers => |v| tag.? == .float and (v.encoded or !v.sampled),
                .strings => |v| tag.? == .string and v.flat == null,
                .decimals => |v| tag.? == .number_string and v.flat == null,
                else => false,
            };
            if (!compatible) return false;
        } else {
            self.values = switch (tag.?) {
                .integer => .{ .integers = .empty },
                .float => .{ .numbers = .empty },
                .string => .{ .strings = .empty },
                .number_string => .{ .decimals = .empty },
                else => unreachable,
            };
            switch (self.values) {
                inline .integers, .numbers => |*v| try v.resize(a, begin),
                .strings, .decimals => |*v| try v.resizeNulls(a, begin),
                else => unreachable,
            }
        }
        switch (self.values) {
            inline .integers, .numbers => |*v| {
                if (!v.encoded) {
                    for (v.flat.items) |value| try v.encodeOne(a, value);
                    v.flat.clearAndFree(a);
                    v.encoded = true;
                    v.sampled = true;
                }
                for (source.values, remap) |value, *id| {
                    if (id.* == std.math.maxInt(u32)) continue;
                    id.* = 0;
                    if (value.sql_null) continue;
                    const number = if (@TypeOf(v.*) == Numeric(i64)) value.value.integer else value.value.float;
                    const entry = try v.lookup.getOrPut(a, @as(u64, @bitCast(number)));
                    if (!entry.found_existing) {
                        entry.value_ptr.* = std.math.cast(u32, v.values.items.len) orelse return error.SqlProgramLimitExceeded;
                        try v.values.append(a, number);
                    }
                    id.* = entry.value_ptr.*;
                }
                try v.indices.ensureUnusedCapacity(a, source.indices.len);
                for (source.indices) |id| v.indices.appendAssumeCapacity(remap[id]);
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: @TypeOf(v.flat) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| flat.appendAssumeCapacity(v.values.items[id]);
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.flat = flat;
                    v.encoded = false;
                }
            },
            .strings, .decimals => |*v| {
                for (source.values, remap) |value, *id| {
                    if (id.* == std.math.maxInt(u32)) continue;
                    id.* = 0;
                    if (value.sql_null) continue;
                    const text = if (value.value == .string) value.value.string else value.value.number_string;
                    if (v.lookup.get(text)) |prior| {
                        id.* = prior;
                        continue;
                    }
                    const bytes = try owned.dupe(u8, text);
                    id.* = std.math.cast(u32, v.values.items.len) orelse return error.SqlProgramLimitExceeded;
                    try v.values.append(a, bytes);
                    try v.lookup.put(a, bytes, id.*);
                }
                try v.indices.ensureUnusedCapacity(a, source.indices.len);
                for (source.indices) |id| v.indices.appendAssumeCapacity(remap[id]);
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: std.ArrayList([]const u8) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| flat.appendAssumeCapacity(if (v.values.items.len == 0) &.{} else v.values.items[id]);
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.flat = flat;
                }
            },
            else => unreachable,
        }
        for (source.indices, 0..) |id, offset| {
            const row = begin + offset;
            if (row % 64 == 0) try self.nulls.append(a, 0);
            if (source.values[id].sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        }
        return true;
    }
    fn append(self: *Column, a: A, owned: A, scratch: A, row: usize, value: Datum) !void {
        if (self.numerics.items.len != 0 or value.numeric != null) {
            if (self.numerics.items.len == 0) {
                try self.numerics.resize(a, row);
                @memset(self.numerics.items, null);
            }
            try self.numerics.append(a, if (value.numeric != null) (try @import("operators.zig").cloneDatum(owned, value)).numeric else null);
        }
        if (self.arrays.items.len != 0 or value.array != null) {
            if (self.arrays.items.len == 0) {
                try self.arrays.resize(a, row);
                @memset(self.arrays.items, null);
            }
            try self.arrays.append(a, if (value.array != null) (try @import("operators.zig").cloneDatum(owned, value)).array else null);
        }
        if (row % 64 == 0) try self.nulls.append(a, 0);
        if (value.sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        if (self.patterns.items.len != 0 or value.patterns != null) {
            if (self.patterns.items.len == 0) {
                try self.patterns.resize(a, row);
                @memset(self.patterns.items, null);
            }
            try self.patterns.append(a, value.patterns);
        }
        if (self.values == .unknown and !value.sql_null) {
            self.values = switch (value.value) {
                .integer => .{ .integers = .empty },
                .float => .{ .numbers = .empty },
                .bool => .{ .booleans = .empty },
                .string => .{ .strings = .empty },
                .number_string => .{ .decimals = .empty },
                else => .{ .encoded = .empty },
            };
            switch (self.values) {
                .unknown => unreachable,
                .strings, .decimals => |*values| try values.resizeNulls(a, row),
                inline .integers, .numbers => |*values| try values.resize(a, row),
                inline else => |*values| {
                    try values.resize(a, row);
                    @memset(values.items, switch (@typeInfo(@TypeOf(values.items)).pointer.child) {
                        i64, f64 => 0,
                        bool => false,
                        std.json.Value => .null,
                        else => &.{},
                    });
                },
            }
        }
        const compatible = value.sql_null or switch (self.values) {
            .unknown => true,
            .integers => value.value == .integer,
            .numbers => value.value == .float,
            .booleans => value.value == .bool,
            .strings => value.value == .string,
            .decimals => value.value == .number_string,
            .encoded => true,
        };
        if (!compatible) {
            var encoded: std.ArrayList(std.json.Value) = .empty;
            errdefer encoded.deinit(a);
            try encoded.ensureTotalCapacity(a, row + 1);
            for (0..row) |i| {
                const prior = try self.cell(scratch, i);
                encoded.appendAssumeCapacity(if (prior.sql_null) .null else prior.value);
            }
            switch (self.values) {
                .unknown => unreachable,
                inline else => |*values| values.deinit(a),
            }
            self.values = .{ .encoded = encoded };
        }
        switch (self.values) {
            .unknown => {},
            .integers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.integer),
            .numbers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.float),
            .booleans => |*v| try v.append(a, !value.sql_null and value.value.bool),
            .strings => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.string),
            .decimals => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.number_string),
            .encoded => |*v| try v.append(a, if (value.sql_null or value.array != null or value.numeric != null) .null else (try @import("operators.zig").cloneDatum(owned, value)).value),
        }
    }
};
pub const Store = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    columns: []Column = &.{},
    len: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    pub fn init(a: A) Store {
        return .{ .a = a, .arena = .init(a) };
    }
    pub fn deinit(self: *Store) void {
        for (self.columns) |*column| column.deinit(self.a);
        self.a.free(self.columns);
        self.arena.deinit();
    }
    /// Conservative admission estimate that recognizes already retained
    /// dictionary bytes. Capacity growth is still enforced by the allocator.
    /// Representation identity for expression reuse, including a distinct NULL
    /// lane. No semantic comparison or coercion is implied by dictionary IDs.
    pub fn dictionaryId(self: *const Store, row_index: usize, column: usize) !?u64 {
        if (row_index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        if (stored.patterns.items.len != 0 or stored.arrays.items.len != 0 or stored.numerics.items.len != 0) return null;
        const id: u32 = switch (stored.values) {
            inline .integers, .numbers => |v| if (v.encoded) v.indices.items[row_index] else return null,
            .strings, .decimals => |v| if (v.flat == null) v.indices.items[row_index] else return null,
            else => return null,
        };
        return if (stored.isNull(row_index)) 0 else @as(u64, id) + 1;
    }
    pub fn dictionaryBatch(self: *const Store, a: A, column: usize, begin: usize, count: usize) !?@import("execution_batch.zig").Batch {
        if (self.failed or column >= self.columns.len or begin > self.len or count > self.len - begin) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        if (stored.patterns.items.len != 0 or stored.arrays.items.len != 0 or stored.numerics.items.len != 0) return null;
        switch (stored.values) {
            inline .integers, .numbers => |v| if (!v.encoded) return null,
            .strings, .decimals => |v| if (v.flat != null) return null,
            else => return null,
        }
        // Export only referenced entries. A small delivery slice must not
        // retain or reconstruct a relation's entire dictionary.
        const values = try a.alloc(Datum, count + 1);
        errdefer a.free(values);
        values[0] = .{};
        const indices = try a.alloc(u32, count);
        errdefer a.free(indices);
        var ids: std.AutoHashMapUnmanaged(u64, u32) = .empty;
        defer ids.deinit(a);
        var used: usize = 1;
        for (indices, 0..) |*id, offset| {
            const physical = (try self.dictionaryId(begin + offset, column)).?;
            if (physical == 0) {
                id.* = 0;
                continue;
            }
            const entry = try ids.getOrPut(a, physical);
            if (!entry.found_existing) {
                entry.value_ptr.* = @intCast(used);
                values[used] = try stored.cell(a, begin + offset);
                used += 1;
            }
            id.* = entry.value_ptr.*;
        }
        return .{ .dictionary = .{ .values = try a.realloc(values, used), .indices = indices } };
    }
    pub fn appendBytes(self: *const Store, values: []const Datum) !usize {
        // Estimate retained column storage, not a reconstructed Datum row.
        // Callers reserve growth headroom separately; the allocator remains
        // the authority for actual admission, including representation changes.
        var bytes: usize = if (self.initialized) 0 else columnMetadataBytes(values.len);
        for (values, 0..) |value, index| {
            var repeated = false;
            if (!value.sql_null and index < self.columns.len) {
                const column = self.columns[index];
                if (column.values == .strings and value.value == .string) repeated = column.values.strings.lookup.contains(value.value.string);
                if (column.values == .decimals and value.value == .number_string) repeated = column.values.decimals.lookup.contains(value.value.number_string);
                if (!repeated) {
                    if (column.values == .strings and value.value == .string) bytes +|= column.values.strings.growthBytes(1);
                    if (column.values == .decimals and value.value == .number_string) bytes +|= column.values.decimals.growthBytes(1);
                }
            }
            bytes +|= if (repeated) @sizeOf(u32) + 1 else try retainedCellBytes(value);
            if (repeated and value.patterns != null) bytes +|= @sizeOf(?*scalar.PatternSet);
        }
        return bytes;
    }
    /// Reserve dictionary replacement capacity before admitting a batch. Treat
    /// its incoming keys as distinct until their cardinality is established.
    pub fn dictionaryGrowthBytes(self: *const Store, rows: usize) usize {
        var bytes: usize = 0;
        for (self.columns) |column| switch (column.values) {
            .strings, .decimals => |dictionary| bytes +|= dictionary.growthBytes(rows),
            else => {},
        };
        return bytes;
    }
    /// Column-major payload admission. No per-row Datum slices or arenas;
    /// primitive and dictionary state is retained directly in each column.
    pub fn appendBatch(self: *Store, batch: @import("execution_batch.zig").Batch) !void {
        if (self.failed or (self.initialized and self.columns.len != batch.width())) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, batch.width());
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, 0..) |*column, ordinal| {
            _ = scratch.reset(.free_all);
            if (try batch.dictionaryColumn(scratch.allocator(), ordinal)) |encoded| {
                if (encoded.len() != batch.len()) return error.InvalidSqlBackendResponse;
                if (try column.appendDictionary(self.a, self.arena.allocator(), self.len, encoded)) continue;
            }
            _ = scratch.reset(.free_all);
            for (0..batch.len()) |index| {
                _ = scratch.reset(.retain_capacity);
                const value = try batch.cell(scratch.allocator(), index, ordinal);
                try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, value);
            }
        }
        self.len += batch.len();
    }
    pub fn append(self: *Store, values: []const Datum) !usize {
        if (self.failed or (self.initialized and values.len != self.columns.len)) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, values.len);
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, values) |*column, value| try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len, value);
        const index = self.len;
        self.len += 1;
        return index;
    }
    pub fn appendRagged(self: *Store, values: []const Datum) !usize {
        if (!self.initialized or values.len == self.columns.len) return self.append(values);
        errdefer self.failed = true;
        if (values.len > self.columns.len) {
            const wider = try self.a.alloc(Column, values.len);
            @memcpy(wider[0..self.columns.len], self.columns);
            @memset(wider[self.columns.len..], .{});
            const prior_width = self.columns.len;
            self.a.free(self.columns);
            self.columns = wider;
            for (self.columns[prior_width..]) |*column| for (0..self.len) |index| try column.append(self.a, self.arena.allocator(), self.a, index, .{});
        }
        const padded = try self.a.alloc(Datum, self.columns.len);
        defer self.a.free(padded);
        @memset(padded, .{});
        @memcpy(padded[0..values.len], values);
        return self.append(padded);
    }
    pub fn rowWidth(self: *const Store, a: A, index: usize, width: usize) ![]const Datum {
        if (self.failed or index >= self.len or width > self.columns.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, width);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn cell(self: *const Store, a: A, index: usize, column: usize) !Datum {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        return self.columns[column].cell(a, index);
    }
    pub fn row(self: *const Store, a: A, index: usize) ![]const Datum {
        if (self.failed or index >= self.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, self.columns.len);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn equal(self: *const Store, a: A, row_index: usize, values: []const Datum, null_equal: bool) !bool {
        if (self.failed or values.len != self.columns.len or row_index >= self.len) return error.InvalidSqlBackendResponse;
        for (values, 0..) |value, column| {
            const stored = self.columns[column];
            const is_null = stored.isNull(row_index);
            if (is_null or value.sql_null) {
                if (!null_equal or is_null != value.sql_null) return false;
                continue;
            }
            if (value.array != null or value.numeric != null or (stored.arrays.items.len != 0 and stored.arrays.items[row_index] != null) or (stored.numerics.items.len != 0 and stored.numerics.items[row_index] != null)) {
                if ((try scalar.compareDatums(try stored.cell(a, row_index), value)) != .eq) return false;
                continue;
            }
            // Dispatch on the retained physical type, without constructing a
            // Datum or invoking JSON comparison for homogeneous primitive keys.
            const equal_ = switch (stored.values) {
                .integers => |v| if (value.value == .integer) v.get(row_index) == value.value.integer else null,
                .numbers => |v| if (value.value == .float and std.math.isFinite(v.get(row_index)) and std.math.isFinite(value.value.float)) v.get(row_index) == value.value.float else null,
                .booleans => |v| if (value.value == .bool) v.items[row_index] == value.value.bool else null,
                .strings => |v| if (value.value == .string) std.mem.eql(u8, v.getText(row_index), value.value.string) else null,
                else => null,
            };
            if (equal_) |matches| {
                if (!matches) return false;
            } else if ((try scalar.compareDatums(try stored.cell(a, row_index), value)) != .eq) return false;
        }
        return true;
    }
};
test "SQL exact NUMERIC retained columns own limbs preserve scale and reject placeholder equality" {
    const Harness = struct {
        fn run(a: A) !void {
            const numeric = @import("numeric_value.zig");
            var context: numeric.Context = .{ .alloc = a };
            var original = try numeric.parse(&context, "9007199254740993.1200");
            defer original.deinit();
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.append(&.{.{}});
            _ = try store.append(&.{Datum.typedNumeric(&original.value)});
            _ = try store.append(&.{Datum.json(.null)});
            _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
            const retained = try store.cell(a, 1, 0);
            try std.testing.expect(retained.numeric != null and !retained.sql_null);
            try std.testing.expect(retained.numeric.?.digits.ptr != original.value.digits.ptr);
            try std.testing.expectEqual(@as(u16, 4), retained.numeric.?.scale);
            try std.testing.expect(try store.equal(a, 1, &.{Datum.typedNumeric(&original.value)}, true));
            try std.testing.expectError(error.SqlTypeMismatch, store.equal(a, 1, &.{Datum.json(.null)}, true));
            try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
            try std.testing.expect((try store.cell(a, 2, 0)).numeric == null);
            try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 3, 0)).value.integer);
            try std.testing.expect(try store.dictionaryId(1, 0) == null);
        }
    };
    try Harness.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed array retained columns preserve bounds ownership and type separation under allocation faults" {
    const Harness = struct {
        fn run(a: A) !void {
            const arrays = @import("array_value.zig");
            var text: [3]u8 = "abc".*;
            var dimensions = [_]arrays.Dimension{.{ .length = 3, .lower = -2 }};
            const value = try arrays.Value.init(.text, &dimensions, &.{ Datum.json(.{ .string = &text }), .{}, Datum.json(.{ .string = "tail" }) }, .{});
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.append(&.{.{}});
            _ = try store.append(&.{Datum.typedArray(&value)});
            _ = try store.append(&.{Datum.json(.null)});
            _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
            _ = try store.append(&.{Datum.typedArray(&value)});
            @memset(&text, 'z');
            dimensions[0].lower = 1;
            const cell = try store.cell(a, 1, 0);
            try std.testing.expect(!cell.sql_null and cell.array != null);
            try std.testing.expectEqual(@as(i32, -2), cell.array.?.dimensions[0].lower);
            try std.testing.expectEqualStrings("abc", cell.array.?.elements[0].value.string);
            try std.testing.expect(cell.array.?.elements[1].sql_null);
            try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
            const json_null = try store.cell(a, 2, 0);
            try std.testing.expect(!json_null.sql_null and json_null.array == null);
            try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 3, 0)).value.integer);
            try std.testing.expect(try store.equal(a, 4, &.{cell}, true));
            try std.testing.expectError(error.SqlTypeMismatch, store.equal(a, 1, &.{json_null}, true));
            const shifted = try arrays.Value.init(.text, &dimensions, cell.array.?.elements, .{});
            try std.testing.expect(!try store.equal(a, 1, &.{Datum.typedArray(&shifted)}, true));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed store preserves bitmaps ownership mixed exact numerics and JSON null" {
    const a = std.testing.allocator;
    var store = Store.init(a);
    defer store.deinit();
    for (0..130) |i| _ = try store.append(&.{ if (i % 7 == 0) .{} else Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = "owned" }) });
    _ = try store.append(&.{ Datum.json(.{ .float = 1.5 }), Datum.json(.null) });
    _ = try store.append(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{} });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..130) |i| {
        const value = try store.cell(arena.allocator(), i, 0);
        try std.testing.expectEqual(i % 7 == 0, value.sql_null);
        if (!value.sql_null) try std.testing.expectEqual(@as(i64, @intCast(i)), value.value.integer);
    }
    try std.testing.expect(!(try store.cell(arena.allocator(), 130, 1)).sql_null);
    try std.testing.expect((try store.cell(arena.allocator(), 131, 1)).sql_null);
    try std.testing.expect(try store.equal(arena.allocator(), 131, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, true));
}

test "SQL typed store promotions and ragged widths release every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.appendRagged(&.{ .{}, Datum.json(.{ .string = "owned" }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.{ .bool = true }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .float = 2.5 }), Datum.json(.null) });
            const cells = try store.rowWidth(a, 0, 2);
            defer a.free(cells);
            try std.testing.expect(cells[0].sql_null);
            try std.testing.expectEqualStrings("owned", cells[1].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed dictionaries retain repeated payloads once and preserve promotion" {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    const a = budget.allocator();
    {
        var store = Store.init(a);
        defer store.deinit();
        var payload: [1024]u8 = @splat('x');
        for (0..10_000) |index| _ = try store.append(&.{if (index % 7 == 0) Datum{} else Datum.json(.{ .string = &payload })});
        try std.testing.expectEqual(@as(usize, 1), store.columns[0].values.strings.values.items.len);
        payload[0] = 'y';
        try std.testing.expectEqual(@as(u8, 'x'), (try store.cell(a, 1, 0)).value.string[0]);
        try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

fn batchAllocationScenario(a: A) !void {
    var store = Store.init(a);
    defer store.deinit();
    const rows = [_][]const Datum{
        &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .string = "shared" }) },
        &.{ .{}, Datum.json(.{ .string = "shared" }) },
        &.{ Datum.json(.{ .float = 2.5 }), Datum.json(.{ .string = "unique" }) },
    };
    try store.appendBatch(.{ .rows = &rows });
    for (rows, 0..) |row, index| for (row, 0..) |expected, column| {
        const actual = try store.cell(a, index, column);
        try std.testing.expectEqualDeep(expected.value, actual.value);
        try std.testing.expectEqual(expected.sql_null, actual.sql_null);
    };
}
test "SQL retained column batch promotion unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, batchAllocationScenario, .{});
}

fn numericRetentionScenario(a: A) !void {
    var store = Store.init(a);
    defer store.deinit();
    for (0..1024) |row| _ = try store.append(&.{
        if (row % 7 == 0) Datum{} else Datum.json(.{ .integer = 9007199254740993 + @as(i64, @intCast(row % 3)) }),
        Datum.json(.{ .float = if (row % 2 == 0) -0.0 else 2.5 }),
    });
    try std.testing.expect(store.columns[0].values.integers.encoded);
    try std.testing.expect(store.columns[1].values.numbers.encoded);
    for (0..1024) |row| {
        const value = try store.cell(a, row, 0);
        try std.testing.expectEqual(row % 7 == 0, value.sql_null);
        if (!value.sql_null) try std.testing.expectEqual(9007199254740993 + @as(i64, @intCast(row % 3)), value.value.integer);
        if (row % 2 == 0) try std.testing.expect(std.math.signbit((try store.cell(a, row, 1)).value.float));
    }
    // A later high-cardinality suffix returns to flat storage safely.
    for (0..4096) |row| _ = try store.append(&.{ Datum.json(.{ .integer = @intCast(row) }), Datum.json(.{ .float = @floatFromInt(row) }) });
    try std.testing.expect(!store.columns[0].values.integers.encoded);
    try std.testing.expectEqual(@as(i64, 4095), (try store.cell(a, store.len - 1, 0)).value.integer);
}
test "SQL retained numeric dictionaries preserve exact values nulls and allocation failure ownership" {
    // Force allocate/copy growth so the exhaustive fault sequence does not
    // depend on whether the backing allocator happens to grow in place.
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), numericRetentionScenario, .{});
}

fn dictionaryHandoffScenario(a: A) !void {
    const Batch = @import("execution_batch.zig").Batch;
    var store = Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{.{}});
    const source: Batch = .{ .dictionary = .{ .values = &.{ .{}, Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .string = "unused incompatible entry" }), Datum.json(.{ .integer = -7 }) }, .indices = &.{ 1, 0, 3, 1, 3, 0 } } };
    try store.appendBatch(source);
    try std.testing.expect(store.columns[0].values.integers.encoded);
    for (0..source.len()) |index| try std.testing.expectEqualDeep(try source.cell(a, index, 0), try store.cell(a, index + 1, 0));
    const exported = (try store.dictionaryBatch(a, 0, 1, source.len())).?;
    defer a.free(exported.dictionary.values);
    defer a.free(exported.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 3), exported.dictionary.values.len);
    var target = Store.init(a);
    defer target.deinit();
    try target.appendBatch(exported);
    for (0..source.len()) |index| try std.testing.expectEqualDeep(try source.cell(a, index, 0), try target.cell(a, index, 0));
    try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
    var unique = Store.init(a);
    defer unique.deinit();
    var unique_values: [64]Datum = undefined;
    var unique_indices: [64]u32 = undefined;
    for (&unique_values, &unique_indices, 0..) |*value, *id, index| {
        value.* = Datum.json(.{ .integer = @intCast(index) });
        id.* = @intCast(index);
    }
    try unique.appendBatch(.{ .dictionary = .{ .values = &unique_values, .indices = &unique_indices } });
    try std.testing.expect(!unique.columns[0].values.integers.encoded);
    try std.testing.expectEqual(@as(i64, 63), (try unique.cell(a, 63, 0)).value.integer);
    try std.testing.expectError(error.InvalidSqlBackendResponse, target.appendBatch(.{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = &.{9} } }));
}
test "SQL dictionary handoff remaps referenced entries preserves NULL prefixes and unwinds failures" {
    try dictionaryHandoffScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dictionaryHandoffScenario, .{});
}
