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

//! Cursor-owned projection pages retain wire payloads and row coordinates.
//! Only selected values cross the native/decimal interpretation boundary;
//! complete framing is validated once and no partial vector enters a cache.
const std = @import("std");
const page = @import("lake_parquet_page.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const types = @import("../../storage/rowsource/types.zig");
const Chunk = @import("../external_source/types.zig").ColumnChunk;
const A = std.mem.Allocator;
const Self = @This();
const encoded_page = @import("lake_parquet_encoded.zig");
const absent = std.math.maxInt(u32);
a: A,
budget: @import("../../sql/memory_budget.zig"),
arena: std.heap.ArenaAllocator = undefined,
chunk: Chunk,
kind: enum { integer, number, boolean, bytes },
definitions: ?page.DefinitionIndex = null,
indices: ?page.HybridIndex = null,
offsets: []u32 = &.{},
data: []const u8 = &.{},
dictionary: ?page.Dictionary = null,
width: usize = 0,
count: usize,
delta: ?encoded_page.Delta = null,
strings: ?encoded_page.Strings = null,
split: bool = false,
present: usize = 0,
converted: usize = 0,

pub fn create(a: A, chunk: Chunk, header: page.Header, encoded: []const u8, dictionary: ?page.Dictionary, limit: usize) !?*Self {
    const dictionary_encoded = header.encoding == .plain_dictionary or header.encoding == .rle_dictionary;
    if (header.encoding != .plain and !dictionary_encoded and !extended(header.encoding)) return null;
    if (header.repetition_level_bytes != 0) return null;
    const kind: @FieldType(Self, "kind") = if (std.mem.eql(u8, chunk.physical_type, "int64") or std.mem.eql(u8, chunk.physical_type, "int32")) .integer else if (std.mem.eql(u8, chunk.physical_type, "float") or std.mem.eql(u8, chunk.physical_type, "double")) .number else if (std.mem.eql(u8, chunk.physical_type, "boolean")) .boolean else if (std.mem.eql(u8, chunk.physical_type, "byte_array") or std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array")) .bytes else return null;
    // Keep unsupported logical interpretations on the established decoder.
    const logical = chunk.logical_type;
    // Match the complete decoder's physical/logical eligibility before taking
    // the encoded path; unsupported annotations retain its established errors.
    if (std.mem.startsWith(u8, logical, "int")) {
        const physical = if (std.mem.eql(u8, logical, "int64")) "int64" else "int32";
        if (!std.mem.eql(u8, chunk.physical_type, physical)) return null;
    }
    if (std.mem.startsWith(u8, logical, "timestamp_") and !std.mem.eql(u8, chunk.physical_type, "int64")) return null;
    if (std.mem.eql(u8, logical, "decimal") and kind != .integer and kind != .bytes) return null;

    if (logical.len != 0 and !std.mem.eql(u8, logical, "string") and !std.mem.eql(u8, logical, "decimal") and !std.mem.startsWith(u8, logical, "int") and !std.mem.startsWith(u8, logical, "timestamp_")) return null;
    if (std.mem.eql(u8, logical, "decimal") and (chunk.decimal_precision < 1 or chunk.decimal_precision > 38 or chunk.decimal_scale < 0 or chunk.decimal_scale > chunk.decimal_precision)) return null;
    if (kind == .bytes and std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array") and !std.mem.eql(u8, logical, "decimal")) return null;
    if (dictionary_encoded and (dictionary == null or kind == .boolean)) return null;
    if (dictionary_encoded) {
        const valid = switch (kind) {
            .integer => dictionary.? == .i64,
            .number => dictionary.? == .f64,
            .bytes => dictionary.? == .bytes,
            .boolean => false,
        };
        if (!valid) return error.InvalidParquetPage;
    }
    const self = try a.create(Self);
    self.* = .{ .a = a, .budget = .{ .backing = a, .limit = limit }, .chunk = chunk, .kind = kind, .dictionary = if (dictionary_encoded) dictionary else null, .count = header.value_count };
    self.arena = .init(self.budget.allocator());
    errdefer self.destroy();
    const alloc = self.arena.allocator();
    // Projection framing outlives the fetched RangeLease. Retain an owned
    // payload in this arena; ordinary synchronous decoders borrow that lease.
    const payload = try (try page.decodePagePayloadAlloc(alloc, header, try parquet.compressionCodecForColumnChunk(chunk), encoded)).intoOwned(alloc);
    var data: []const u8 = payload.bytes;
    var present: usize = 0;
    if (chunk.nullable) {
        const flat = try page.flatOptionalPage(header, data);
        if (flat.header.definition_level_bytes == 0 or flat.header.data_payload_offset > flat.payload.len) return error.InvalidParquetPage;
        self.definitions = try page.DefinitionIndex.init(alloc, flat.payload[0..flat.header.definition_level_bytes], self.count);
        present = self.definitions.?.present;
        data = flat.payload[flat.header.data_payload_offset..];
    } else {
        present = self.count;
    }
    self.data = data;
    self.present = present;
    if (extended(header.encoding)) {
        self.width = if (std.mem.eql(u8, chunk.physical_type, "int32") or std.mem.eql(u8, chunk.physical_type, "float")) 4 else if (kind == .integer or kind == .number) 8 else std.math.cast(usize, chunk.type_length) orelse return error.InvalidParquetPage;
        switch (header.encoding) {
            .delta_binary_packed => {
                if (kind != .integer) return error.UnsupportedParquetPage;
                self.delta = try encoded_page.Delta.init(alloc, data, present, @intCast(self.width * 8));
                if (self.delta.?.consumed != data.len) return error.InvalidParquetPage;
            },
            .delta_length_byte_array, .delta_byte_array => {
                if (!std.mem.eql(u8, chunk.physical_type, "byte_array")) return error.UnsupportedParquetPage;
                self.strings = try encoded_page.Strings.init(alloc, data, present, header.encoding == .delta_byte_array);
            },
            .byte_stream_split => {
                if (kind == .boolean or self.width == 0 or self.width > 16 or data.len != std.math.mul(usize, present, self.width) catch return error.InvalidParquetPage) return error.InvalidParquetPage;
                self.split = true;
            },
            else => unreachable,
        }
        return self;
    }
    if (dictionary_encoded) {
        if (present == 0) return self;
        if (data.len == 0 or data[0] > 32) return error.InvalidParquetPage;

        // All IDs are structural evidence, even when their values are deferred.
        const size = switch (dictionary.?) {
            inline else => |values| values.len,
        };
        self.indices = try page.HybridIndex.init(alloc, data[1..], @intCast(data[0]), present, size);
        return self;
    }
    self.width = if (std.mem.eql(u8, chunk.physical_type, "int32") or std.mem.eql(u8, chunk.physical_type, "float")) 4 else if (kind == .integer or kind == .number) 8 else if (std.mem.eql(u8, chunk.physical_type, "fixed_len_byte_array")) std.math.cast(usize, chunk.type_length) orelse return error.InvalidParquetPage else 0;
    if (kind == .bytes and self.width == 0) {
        self.offsets = try alloc.alloc(u32, present);
        var offset: usize = 0;
        for (self.offsets) |*position| {
            if (offset > data.len or data.len - offset < 4) return error.InvalidParquetPage;
            position.* = std.math.cast(u32, offset) orelse return error.ParquetPageTooLarge;
            const length = std.mem.readInt(u32, data[offset..][0..4], .little);
            offset += 4;
            if (length > data.len - offset) return error.InvalidParquetPage;
            offset += length;
        }
        if (offset != data.len) return error.InvalidParquetPage;
    } else {
        const expected = if (kind == .boolean) (present + 7) / 8 else std.math.mul(usize, present, self.width) catch return error.InvalidParquetPage;
        if (self.width == 0 and kind != .boolean) return error.InvalidParquetPage;
        if (data.len != expected) return error.InvalidParquetPage;
    }
    return self;
}
pub fn stabilize(self: *Self, a: A) void {
    self.a = a;
    self.budget.backing = a;
}
pub fn destroyWithAllocator(self: *Self, a: A) void {
    self.budget.backing = a;
    self.arena.deinit();
    a.destroy(self);
}
pub fn destroy(self: *Self) void {
    const a = self.a;
    self.arena.deinit();
    a.destroy(self);
}
fn lanePosition(self: *const Self, row: usize) u32 {
    return if (self.definitions) |index| index.position(row) orelse absent else @intCast(row);
}
pub fn extended(encoding: page.Encoding) bool {
    return encoding == .delta_binary_packed or encoding == .delta_length_byte_array or encoding == .delta_byte_array or encoding == .byte_stream_split;
}
fn splitValue(self: *Self, position: usize, buffer: *[16]u8) []const u8 {
    for (0..self.width) |byte| buffer[byte] = self.data[byte * self.present + position];
    return buffer[0..self.width];
}
fn integer(self: *Self, position: usize) !i64 {
    if (self.delta) |*delta| return delta.at(position);
    if (self.split) {
        var buffer: [16]u8 = undefined;
        const value = self.splitValue(position, &buffer);
        return if (self.width == 4) std.mem.readInt(i32, value[0..4], .little) else std.mem.readInt(i64, value[0..8], .little);
    }
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .i64 => |values| values[self.indices.?.at(position)],
        else => error.InvalidParquetPage,
    };
    const offset = position * self.width;
    return if (self.width == 4) std.mem.readInt(i32, self.data[offset..][0..4], .little) else std.mem.readInt(i64, self.data[offset..][0..8], .little);
}
fn number(self: *Self, position: usize) !f64 {
    if (self.split) {
        var buffer: [16]u8 = undefined;
        const value = self.splitValue(position, &buffer);
        return if (self.width == 4) @as(f32, @bitCast(std.mem.readInt(u32, value[0..4], .little))) else @as(f64, @bitCast(std.mem.readInt(u64, value[0..8], .little)));
    }
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .f64 => |values| values[self.indices.?.at(position)],
        else => error.InvalidParquetPage,
    };
    const offset = position * self.width;
    return if (self.width == 4) @as(f32, @bitCast(std.mem.readInt(u32, self.data[offset..][0..4], .little))) else @as(f64, @bitCast(std.mem.readInt(u64, self.data[offset..][0..8], .little)));
}
fn bytes(self: *Self, position: usize) ![]const u8 {
    if (self.strings) |*strings| return strings.at(position);
    if (self.split) {
        var buffer: [16]u8 = undefined;
        return self.arena.allocator().dupe(u8, self.splitValue(position, &buffer));
    }
    if (self.dictionary) |dictionary| return switch (dictionary) {
        .bytes => |values| values[self.indices.?.at(position)],
        else => error.InvalidParquetPage,
    };
    if (self.width != 0) return self.data[position * self.width ..][0..self.width];
    const offset = self.offsets[position];
    const length = std.mem.readInt(u32, self.data[offset..][0..4], .little);
    return self.data[offset + 4 ..][0..length];
}
fn predicateValue(self: *Self, comptime T: type, position: usize, domain: types.FloatDomain) !T {
    if (T == i64) {
        const factor: i64 = if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_millis")) 1_000_000 else if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_micros")) 1_000 else 1;
        return std.math.mul(i64, try self.integer(position), factor) catch error.InvalidParquetPage;
    }
    if (T == f64) return @import("../../common/float_domain.zig").normalize(try self.number(position), domain);
    if (T == []const u8) {
        if (self.strings) |*strings| return strings.borrow(position);
        return self.bytes(position);
    }
    if (T == bool) return self.data[position / 8] & (@as(u8, 1) << @as(u3, @intCast(position % 8))) != 0;
    unreachable;
}
fn comparePredicate(comptime T: type, comptime op: types.EncodedOp, value: T, literal: T) bool {
    const order = if (T == []const u8) std.mem.order(u8, value, literal) else if (T == bool) std.math.order(@intFromBool(value), @intFromBool(literal)) else if (T == f64) @import("../../common/float_domain.zig").order(value, literal) else std.math.order(value, literal);
    return switch (op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .lte => order != .gt,
        .gt => order == .gt,
        .gte => order != .lt,
    };
}
fn predicateMask(self: *Self, comptime T: type, begin: usize, keep: []bool, literal: T, op: types.EncodedOp) !bool {
    if (T == f64) if (!std.math.isFinite(literal)) return false;
    // Decline before changing masks so the scalar coercion boundary retains
    // authority over nonfinite source errors, including excluded lanes.
    if (T == f64) for (keep, begin..) |active, row| {
        const position = self.lanePosition(row);
        if (active and position != absent and !std.math.isFinite(try self.number(position))) return false;
    };
    switch (op) {
        inline else => |operation| try self.predicateMaskOp(T, operation, begin, keep, literal, .finite),
    }
    return true;
}
fn predicateMaskOp(self: *Self, comptime T: type, comptime op: types.EncodedOp, begin: usize, keep: []bool, literal: T, domain: types.FloatDomain) !void {
    var memo: [8192]u8 = @splat(0);
    var identities: [8192]u32 = undefined;
    var cached: usize = 0;
    var lane: usize = 0;
    while (lane < keep.len) {
        if (T == i64 or T == f64) {
            const timestamp = std.mem.startsWith(u8, self.chunk.logical_type, "timestamp_") and !std.mem.eql(u8, self.chunk.logical_type, "timestamp_nanos");
            const position = self.lanePosition(begin + lane);
            if (T == i64) if (!timestamp and position != absent) if (self.delta) |*delta| {
                var count: usize = 0;
                while (count < @min(32, keep.len - lane) and keep[lane + count] and self.lanePosition(begin + lane + count) == position + count) : (count += 1) {}
                if (count >= 4) {
                    try delta.mask(position, keep[lane..][0..count], literal, op);
                    lane += count;
                    continue;
                }
            };
            const dense = !timestamp and self.dictionary == null and lane + 4 <= keep.len and position != absent and for (keep[lane..][0..4], 0..) |active, offset| {
                if (!active or self.lanePosition(begin + lane + offset) != position + offset) break false;
                if (T == f64) if (!std.math.isFinite(try self.number(position + offset)) or !std.math.isFinite(literal)) break false;
            } else true;
            if (dense) {
                var native: [4]T = undefined;
                for (&native, 0..) |*value, offset| value.* = try self.predicateValue(T, position + offset, domain);
                const left: @Vector(4, T) = native;
                const right: @Vector(4, T) = @splat(literal);
                keep[lane..][0..4].* = switch (op) {
                    .eq => left == right,
                    .neq => left != right,
                    .lt => left < right,
                    .lte => left <= right,
                    .gt => left > right,
                    .gte => left >= right,
                };
                lane += 4;
                continue;
            }
        }
        defer lane += 1;
        if (!keep[lane]) continue;
        const position = self.lanePosition(begin + lane);
        if (position == absent) {
            const end = @min(keep.len, self.definitions.?.intervalEnd(begin + lane) - begin);
            @memset(keep[lane..end], false);
            lane = end - 1;
            continue;
        }
        if (self.indices) |index| if (index.runAt(position)) |run| if (run.repeated != null) {
            const definition_end = if (self.definitions) |definitions| definitions.intervalEnd(begin + lane) - begin else keep.len;
            const end = @min(definition_end, @min(keep.len, lane + run.first + run.count - position));
            const matches = comparePredicate(T, op, try self.predicateValue(T, position, domain), literal);
            if (!matches) @memset(keep[lane..end], false);
            lane = end - 1;
            continue;
        };
        var slot: usize = 0;
        var id: u32 = 0;
        if (self.dictionary != null) {
            id = self.indices.?.at(position);
            slot = (id *% 2654435761) & (memo.len - 1);
            while (memo[slot] != 0 and identities[slot] != id) slot = (slot + 1) & (memo.len - 1);
            if (memo[slot] != 0) {
                keep[lane] = memo[slot] == 2;
                continue;
            }
        }
        keep[lane] = comparePredicate(T, op, try self.predicateValue(T, position, domain), literal);
        if (self.dictionary != null and cached < 4096) {
            identities[slot] = id;
            memo[slot] = if (keep[lane]) 2 else 1;
            cached += 1;
        }
    }
}
fn encodedNumberMask(raw: *const anyopaque, begin: usize, keep: []bool, literal: f64, op: types.EncodedOp, domain: types.FloatDomain) !bool {
    const self: *Self = @ptrCast(@alignCast(@constCast(raw)));
    if (self.kind != .number) return false;
    if (begin > self.count or keep.len > self.count - begin) return error.InvalidParquetPage;
    switch (op) {
        inline else => |operation| try self.predicateMaskOp(f64, operation, begin, keep, literal, domain),
    }
    return true;
}
fn encodedMask(raw: *const anyopaque, begin: usize, keep: []bool, literal: std.json.Value, op: types.EncodedOp) !bool {
    const self: *Self = @ptrCast(@alignCast(@constCast(raw)));
    if (begin > self.count or keep.len > self.count - begin) return error.InvalidParquetPage;
    return switch (self.kind) {
        .integer => if (literal == .integer) self.predicateMask(i64, begin, keep, literal.integer, op) else false,
        .number => if (literal == .float) self.predicateMask(f64, begin, keep, literal.float, op) else false,
        .bytes => if (literal == .string) self.predicateMask([]const u8, begin, keep, literal.string, op) else false,
        .boolean => if (literal == .bool) self.predicateMask(bool, begin, keep, literal.bool, op) else false,
    };
}
fn encodedValue(raw: *const anyopaque, row: usize) !std.json.Value {
    const self: *Self = @ptrCast(@alignCast(@constCast(raw)));
    const position = self.lanePosition(row);
    if (position == absent) return .null;
    return switch (self.kind) {
        .integer => blk: {
            const factor: i64 = if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_millis")) 1_000_000 else if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_micros")) 1_000 else 1;
            break :blk .{ .integer = std.math.mul(i64, try self.integer(position), factor) catch return error.InvalidParquetPage };
        },
        .number => .{ .float = try self.number(position) },
        .bytes => .{ .string = try self.bytes(position) },
        .boolean => .{ .bool = self.data[position / 8] & (@as(u8, 1) << @as(u3, @intCast(position % 8))) != 0 },
    };
}
fn encodedId(raw: *const anyopaque, row: usize) ?u32 {
    const self: *const Self = @ptrCast(@alignCast(raw));
    return if (self.lanePosition(row) == absent) null else self.indices.?.at(self.lanePosition(row));
}
pub fn evidence(self: *Self, a: A, begin: usize, count: usize) !?types.ColumnVector {
    if (std.mem.eql(u8, self.chunk.logical_type, "decimal")) return null;
    if (begin > self.count or count > self.count - begin) return error.InvalidParquetPage;
    const nulls = try a.alloc(u8, count);
    for (nulls, begin..) |*flag, row| flag.* = @intFromBool(self.lanePosition(row) == absent);
    return .{ .name = self.chunk.column_id, .nulls = .{ .bytes = nulls }, .values = .{ .encoded = .{
        .ptr = self,
        .begin = begin,
        .count = count,
        .kind = switch (self.kind) {
            .integer => .i64,
            .number => .f64,
            .bytes => .bytes,
            .boolean => .bool,
        },
        .read = encodedValue,
        .mask = encodedMask,
        .mask_number = encodedNumberMask,
        .dictionary_id = if (self.dictionary != null) encodedId else null,
    } } };
}
pub fn project(self: *Self, a: A, begin: usize, keep: []const bool) !types.ColumnVector {
    if (begin > self.count or keep.len > self.count - begin) return error.InvalidParquetPage;
    const decimal = std.mem.eql(u8, self.chunk.logical_type, "decimal");
    const nulls = try a.alloc(u8, keep.len);
    var vector: types.ColumnVector = .{ .name = self.chunk.column_id, .nulls = .{ .bytes = nulls }, .values = undefined };
    if (!decimal and !std.mem.startsWith(u8, self.chunk.logical_type, "timestamp_")) if (self.dictionary) |dictionary| {
        // Preserve dictionary identities through projection so downstream
        // expression/group kernels can work on referenced entries once.
        const indices = try a.alloc(u32, keep.len);
        for (keep, nulls, indices, begin..) |selected, *flag, *id, row| {
            const position = self.lanePosition(row);
            flag.* = @intFromBool(!selected or position == absent);
            id.* = if (flag.* == 0) self.indices.?.at(position) else 0;
            if (flag.* == 0) self.converted += 1;
        }
        vector.values = switch (dictionary) {
            .i64 => |values| .{ .dictionary_i64 = .{ .values = values, .indices = indices } },
            .f64 => |values| .{ .dictionary_f64 = .{ .values = values, .indices = indices } },
            .bytes => |values| .{ .dictionary_bytes = .{ .values = values, .indices = indices } },
        };
        return vector;
    };
    vector.values = if (decimal) .{ .decimal128 = .{ .values = try a.alloc(i128, keep.len), .precision = self.chunk.decimal_precision, .scale = self.chunk.decimal_scale } } else if (self.kind == .bytes) .{ .bytes = try a.alloc([]const u8, keep.len) } else switch (self.kind) {
        .integer => .{ .i64 = try a.alloc(i64, keep.len) },
        .number => .{ .f64 = try a.alloc(f64, keep.len) },
        .boolean => .{ .bool = try a.alloc(bool, keep.len) },
        .bytes => unreachable,
    };
    for (keep, nulls, begin.., 0..) |selected, *flag, row, lane| {
        const position = self.lanePosition(row);
        flag.* = @intFromBool(!selected or position == absent);
        switch (vector.values) {
            .bytes => |values_| {
                const values = @constCast(values_);
                if (flag.* != 0) {
                    values[lane] = "";
                    continue;
                }
                values[lane] = try self.bytes(position);
            },
            .decimal128 => |values| {
                const value: i128 = if (flag.* != 0) 0 else if (self.kind == .integer) try self.integer(position) else try @import("lake_decimal.zig").signedBigEndian(try self.bytes(position));
                try @import("lake_decimal.zig").validateCoefficient(value, values.precision, values.scale);
                @constCast(values.values)[lane] = value;
            },
            .i64 => |values_| {
                var value: i64 = if (flag.* != 0) 0 else try self.integer(position);
                if (flag.* == 0) {
                    const factor: i64 = if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_millis")) 1_000_000 else if (std.mem.eql(u8, self.chunk.logical_type, "timestamp_micros")) 1_000 else 1;
                    value = std.math.mul(i64, value, factor) catch return error.InvalidParquetPage;
                }
                @constCast(values_)[lane] = value;
            },
            .f64 => |values_| @constCast(values_)[lane] = if (flag.* != 0) 0 else try self.number(position),
            .bool => |values_| @constCast(values_)[lane] = flag.* == 0 and (self.data[position / 8] & (@as(u8, 1) << @as(u3, @intCast(position % 8)))) != 0,
            else => unreachable,
        }
        if (flag.* == 0) self.converted += 1;
    }
    return vector;
}

test "lake SQL selected page owns framing borrows strings and preserves nullable coordinates" {
    const Helper = struct {
        fn run(a: A) !void {
            // Nullable V1 levels: present, NULL, present. Only one wide value
            // survives; the other stays as an immutable wire slice.
            var payload = [_]u8{ 6, 0, 0, 0, 2, 1, 2, 0, 2, 1, 3, 0, 0, 0, 'o', 'n', 'e', 3, 0, 0, 0, 't', 'w', 'o' };
            const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = payload.len, .uncompressed_page_size = payload.len, .value_count = 3, .encoding = .plain };
            const chunk: Chunk = .{ .column_id = @constCast("s"), .file_offset = 0, .compressed_len = payload.len, .physical_type = @constCast("byte_array"), .nullable = true };
            const selected = (try create(a, chunk, header, &payload, null, 65536)).?;
            defer selected.destroy();
            // The fetch lease may be released/reused before deferred framing
            // or selected strings are read, including nullable level indexes.
            @memset(&payload, 0xff);
            var output = std.heap.ArenaAllocator.init(a);
            defer output.deinit();
            const encoded_evidence = (try selected.evidence(output.allocator(), 0, 3)).?;
            try std.testing.expectEqual(types.ColumnKind.encoded, encoded_evidence.kind());
            try std.testing.expect(encoded_evidence.nulls.isNull(1));
            try std.testing.expectEqualStrings("two", (try encoded_evidence.values.encoded.at(2)).string);
            try std.testing.expectEqual(@as(usize, 0), selected.converted);
            var string_mask = [_]bool{ true, true, true };
            try std.testing.expect(try encodedMask(selected, 0, &string_mask, .{ .string = "one" }, .eq));
            try std.testing.expectEqualSlices(bool, &.{ true, false, false }, &string_mask);

            const vector = try selected.project(output.allocator(), 0, &.{ false, true, true });
            try std.testing.expect(vector.nulls.isNull(0) and vector.nulls.isNull(1) and !vector.nulls.isNull(2));
            try std.testing.expectEqualStrings("two", vector.values.bytes[2]);
            try std.testing.expectEqual(@as(usize, 1), selected.converted);
            // A later cohort can select an earlier value from the same page.
            const first = try selected.project(output.allocator(), 0, &.{true});
            try std.testing.expectEqualStrings("one", first.values.bytes[0]);
            var dictionary_values = [_][]u8{ @constCast("red"), @constCast("blue") };
            const encoded_ids = [_]u8{ 1, 2, 1, 2, 0 };
            var dictionary_header = header;
            dictionary_header.encoding = .rle_dictionary;
            dictionary_header.value_count = 2;
            dictionary_header.compressed_page_size = encoded_ids.len;
            dictionary_header.uncompressed_page_size = encoded_ids.len;
            var dictionary_chunk = chunk;
            dictionary_chunk.nullable = false;
            try std.testing.expectError(error.InvalidParquetPage, create(a, dictionary_chunk, dictionary_header, &encoded_ids, .{ .i64 = &.{} }, 65536));
            const borrowed = (try create(a, dictionary_chunk, dictionary_header, &encoded_ids, .{ .bytes = &dictionary_values }, 65536)).?;
            defer borrowed.destroy();
            const dictionary_vector = try borrowed.project(output.allocator(), 0, &.{ true, false });
            try std.testing.expectEqualSlices(u32, &.{ 1, 0 }, dictionary_vector.values.dictionary_bytes.indices);
            try std.testing.expect(@intFromPtr(dictionary_vector.values.dictionary_bytes.values.ptr) == @intFromPtr(dictionary_values[0..].ptr));
            try std.testing.expect(!dictionary_vector.nulls.isNull(0) and dictionary_vector.nulls.isNull(1));
        }
    };
    try Helper.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Helper.run, .{});
}

test "lake SQL encoded numeric predicate runs preserve exact integers selections and nonfinite errors" {
    const a = std.testing.allocator;
    const values = [_]i64{ -9, 0, 7, 9007199254740993, 9007199254740992, 12, 17, 20 };
    var payload: [values.len * 8]u8 = undefined;
    for (values, 0..) |value, row| std.mem.writeInt(i64, payload[row * 8 ..][0..8], value, .little);
    const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = payload.len, .uncompressed_page_size = payload.len, .value_count = values.len, .encoding = .plain };
    const chunk: Chunk = .{ .column_id = @constCast("n"), .file_offset = 0, .compressed_len = payload.len, .physical_type = @constCast("int64") };
    const selected = (try create(a, chunk, header, &payload, null, 65536)).?;
    defer selected.destroy();
    @memset(&payload, 0xff);
    var unsupported = chunk;
    unsupported.logical_type = @constCast("int8");
    try std.testing.expect((try create(a, unsupported, header, &payload, null, 65536)) == null);

    for ([_]types.EncodedOp{ .eq, .neq, .lt, .lte, .gt, .gte }) |op| {
        var dense: [8]bool = @splat(true);
        var sparse = [_]bool{ true, false, true, true, false, true, true, false };
        try std.testing.expect(try encodedMask(selected, 0, &dense, .{ .integer = 9007199254740993 }, op));
        try std.testing.expect(try encodedMask(selected, 0, &sparse, .{ .integer = 9007199254740993 }, op));
        for (values, dense, sparse, 0..) |value, actual, filtered, row| {
            const order = std.math.order(value, @as(i64, 9007199254740993));
            const expected = switch (op) {
                .eq => order == .eq,
                .neq => order != .eq,
                .lt => order == .lt,
                .lte => order != .gt,
                .gt => order == .gt,
                .gte => order != .lt,
            };
            try std.testing.expectEqual(expected, actual);
            try std.testing.expectEqual(expected and row != 1 and row != 4 and row != 7, filtered);
        }
    }
    var float_payload: [16]u8 = undefined;
    std.mem.writeInt(u64, float_payload[0..8], @bitCast(@as(f64, 7)), .little);
    std.mem.writeInt(u64, float_payload[8..16], @bitCast(std.math.nan(f64)), .little);
    var float_header = header;
    float_header.value_count = 2;
    float_header.compressed_page_size = float_payload.len;
    float_header.uncompressed_page_size = float_payload.len;
    var float_chunk = chunk;
    float_chunk.physical_type = @constCast("double");
    const floats = (try create(a, float_chunk, float_header, &float_payload, null, 65536)).?;
    defer floats.destroy();
    var keep = [_]bool{ true, true };
    try std.testing.expect(!try encodedMask(floats, 0, &keep, .{ .float = 7 }, .eq));
    try std.testing.expectEqualSlices(bool, &.{ true, true }, &keep);
    keep[1] = false;
    try std.testing.expect(try encodedMask(floats, 0, &keep, .{ .float = 7 }, .eq));
    try std.testing.expectEqualSlices(bool, &.{ true, false }, &keep);
}

test "lake SQL repeated dictionary predicate masks need no expanded page ID vector" {
    const a = std.testing.allocator;
    const payload = [_]u8{ 1, 0x80, 0x80, 8, 1 };
    const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = @intCast(payload.len), .uncompressed_page_size = @intCast(payload.len), .value_count = 65536, .encoding = .rle_dictionary };
    const chunk: Chunk = .{ .column_id = @constCast("n"), .file_offset = 0, .compressed_len = payload.len, .physical_type = @constCast("int64") };
    var dictionary_values = [_]i64{ 7, 99 };
    const selected = (try create(a, chunk, header, &payload, .{ .i64 = &dictionary_values }, 16 * 1024)).?;
    defer selected.destroy();
    try std.testing.expectEqual(@as(usize, 0), selected.indices.?.flat.len);
    try std.testing.expectEqual(@as(usize, 1), selected.indices.?.runs.len);
    var mask: [1024]bool = @splat(true);
    mask[17] = false;
    try std.testing.expect(try encodedMask(selected, 60000, &mask, .{ .integer = 99 }, .eq));
    try std.testing.expect(!mask[17] and mask[0] and mask[1023]);
    try std.testing.expect(try encodedMask(selected, 60000, &mask, .{ .integer = 7 }, .eq));
    for (mask) |active| try std.testing.expect(!active);
    try std.testing.expectEqual(@as(usize, 0), selected.converted);
}

test "lake SQL nullable repeated pages retain native masks within a tiny decode budget" {
    const a = std.testing.allocator;
    var dictionary_values = [_]i64{ 7, 99 };
    for ([_]bool{ false, true }) |mixed| {
        const full = [_]u8{ 4, 0, 0, 0, 0x80, 0x80, 8, 1, 1, 0x80, 0x80, 8, 1 };
        const half = [_]u8{ 8, 0, 0, 0, 0x80, 0x80, 4, 0, 0x80, 0x80, 4, 1, 1, 0x80, 0x80, 4, 1 };
        const payload: []const u8 = if (mixed) &half else &full;
        const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = @intCast(payload.len), .uncompressed_page_size = @intCast(payload.len), .value_count = 65536, .encoding = .rle_dictionary };
        const chunk: Chunk = .{ .column_id = @constCast("n"), .file_offset = 0, .compressed_len = payload.len, .physical_type = @constCast("int64"), .nullable = true };
        const selected = (try create(a, chunk, header, payload, .{ .i64 = &dictionary_values }, 16 * 1024)).?;
        defer selected.destroy();
        try std.testing.expectEqual(@as(usize, if (mixed) 2 else 0), selected.definitions.?.runs.len);
        try std.testing.expectEqual(@as(usize, 0), selected.definitions.?.bitmap.len);
        var mask: [1024]bool = @splat(true);
        mask[1000] = false;
        try std.testing.expect(try encodedMask(selected, 32000, &mask, .{ .integer = 99 }, .eq));
        for (mask, 0..) |active, lane| try std.testing.expectEqual(lane != 1000 and (!mixed or lane >= 768), active);
        try std.testing.expectEqual(@as(usize, 0), selected.converted);
    }
}

test "lake SQL compact prefix predicates keep only stable projected strings" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const prefixes = try encoded_page.testDelta(work, &.{ 0, 3, 4, 0 });
    const lengths = try encoded_page.testDelta(work, &.{ 6, 1, 2, 3 });
    const payload = try std.mem.concat(work, u8, &.{ prefixes, lengths, "abcdefX12new" });
    const header: page.Header = .{ .page_type = .data_page, .compressed_page_size = @intCast(payload.len), .uncompressed_page_size = @intCast(payload.len), .value_count = 4, .encoding = .delta_byte_array };
    const chunk: Chunk = .{ .column_id = @constCast("s"), .file_offset = 0, .compressed_len = @intCast(payload.len), .physical_type = @constCast("byte_array"), .logical_type = @constCast("string") };
    const selected = (try create(a, chunk, header, payload, null, 65536)).?;
    defer selected.destroy();
    var keep: [4]bool = @splat(true);
    try std.testing.expect(try encodedMask(selected, 0, &keep, .{ .string = "abcX12" }, .eq));
    try std.testing.expectEqualSlices(bool, &.{ false, false, true, false }, &keep);
    try std.testing.expectEqual(@as(usize, 0), selected.strings.?.materialized.count());
    const vector = try selected.project(work, 0, &keep);
    try std.testing.expectEqualStrings("abcX12", vector.values.bytes[2]);
    try std.testing.expectEqual(@as(usize, 1), selected.strings.?.materialized.count());
}
