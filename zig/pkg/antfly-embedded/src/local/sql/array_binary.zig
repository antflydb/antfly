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

//! PostgreSQL binary array payloads. Element OIDs are checked against the
//! pinned expected type, never used to choose an untrusted runtime codec.
//! Wire bytes and actual decoded arena capacity have independent hard bounds.
const std = @import("std");
const arrays = @import("array_value.zig");
const uuid = @import("../common/uuid.zig");
const MemoryBudget = @import("memory_budget.zig");
const A = std.mem.Allocator;
pub const Options = struct { values: arrays.Limits = .{}, wire_bytes: usize = 8 * 1024 * 1024 };

fn jsonSize(value: std.json.Value) !usize {
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(value, .{}, &counter.writer);
    return std.math.cast(usize, counter.count) orelse error.SqlProgramLimitExceeded;
}

fn elementSize(kind: arrays.ElementType, element: arrays.Element) !usize {
    if (element.sql_null) return 0;
    return switch (kind) {
        .text => element.value.string.len,
        .int16 => 2,
        .int32, .float32 => 4,
        .int64, .float64 => 8,
        .boolean => 1,
        .uuid => 16,
        .jsonb => std.math.add(usize, 1, try jsonSize(element.value)) catch error.SqlProgramLimitExceeded,
    };
}

/// Validate and size before writing any bytes. Primitive and text cells stream
/// directly; JSONB uses a counting pass for its required length prefix, not a
/// per-element temporary allocation. Writer failure remains a writer failure.
pub fn encode(value: arrays.Value, writer: *std.Io.Writer, options: Options) !void {
    const validated = try arrays.Value.init(value.element_type, value.dimensions, value.elements, options.values);
    if (validated.dimensions.len != value.dimensions.len) return error.InvalidSqlArrayShape;
    for (value.dimensions) |dimension| if (dimension.length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
    var size: usize = 12 + value.dimensions.len * 8;
    var has_null = false;
    for (value.elements) |element| {
        has_null = has_null or element.sql_null;
        const length = try elementSize(value.element_type, element);
        if (length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        size = std.math.add(usize, size, 4 + length) catch return error.SqlProgramLimitExceeded;
        if (size > options.wire_bytes) return error.SqlProgramLimitExceeded;
    }
    if (size > options.wire_bytes) return error.SqlProgramLimitExceeded;
    try writer.writeInt(i32, @intCast(value.dimensions.len), .big);
    try writer.writeInt(i32, @intFromBool(has_null), .big);
    try writer.writeInt(u32, value.element_type.oid(), .big);
    for (value.dimensions) |dimension| {
        try writer.writeInt(i32, @intCast(dimension.length), .big);
        try writer.writeInt(i32, dimension.lower, .big);
    }
    for (value.elements) |element| {
        if (element.sql_null) {
            try writer.writeInt(i32, -1, .big);
            continue;
        }
        try writer.writeInt(i32, @intCast(try elementSize(value.element_type, element)), .big);
        switch (value.element_type) {
            .text => try writer.writeAll(element.value.string),
            .int16 => try writer.writeInt(i16, @intCast(element.value.integer), .big),
            .int32 => try writer.writeInt(i32, @intCast(element.value.integer), .big),
            .int64 => try writer.writeInt(i64, element.value.integer, .big),
            .float32 => {
                const narrowed: f32 = @floatCast(element.value.float);
                try writer.writeInt(u32, @bitCast(narrowed), .big);
            },
            .float64 => try writer.writeInt(u64, @bitCast(element.value.float), .big),
            .boolean => try writer.writeByte(@intFromBool(element.value.bool)),
            .uuid => try writer.writeAll(&try uuid.parse(element.value.string)),
            .jsonb => {
                try writer.writeByte(1);
                try std.json.Stringify.value(element.value, .{}, writer);
            },
        }
    }
}

const Reader = struct {
    bytes: []const u8,
    position: usize = 0,
    fn take(self: *Reader, count: usize) ![]const u8 {
        if (count > self.bytes.len -| self.position) return error.InvalidSqlBinaryRepresentation;
        const result = self.bytes[self.position..][0..count];
        self.position += count;
        return result;
    }
    fn int(self: *Reader, comptime T: type) !T {
        const size = @sizeOf(T);
        return std.mem.readInt(T, (try self.take(size))[0..size], .big);
    }
};

fn decodeElement(a: A, kind: arrays.ElementType, bytes: []const u8) !arrays.Element {
    const width: ?usize = switch (kind) {
        .int16 => 2,
        .int32, .float32 => 4,
        .int64, .float64 => 8,
        .boolean => 1,
        .uuid => 16,
        else => null,
    };
    if (width) |expected| if (bytes.len != expected) return error.InvalidSqlBinaryRepresentation;
    return arrays.Element.json(switch (kind) {
        .text => blk: {
            if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.SqlInvalidTextEncoding;
            break :blk .{ .string = try a.dupe(u8, bytes) };
        },
        .int16 => .{ .integer = std.mem.readInt(i16, bytes[0..2], .big) },
        .int32 => .{ .integer = std.mem.readInt(i32, bytes[0..4], .big) },
        .int64 => .{ .integer = std.mem.readInt(i64, bytes[0..8], .big) },
        .float32 => blk: {
            const narrowed: f32 = @bitCast(std.mem.readInt(u32, bytes[0..4], .big));
            break :blk .{ .float = narrowed };
        },
        .float64 => .{ .float = @bitCast(std.mem.readInt(u64, bytes[0..8], .big)) },
        .boolean => .{ .bool = bytes[0] != 0 },
        .uuid => .{ .string = try a.dupe(u8, &uuid.format(bytes[0..16].*)) },
        .jsonb => blk: {
            if (bytes.len < 2 or bytes[0] != 1) return error.InvalidSqlBinaryRepresentation;
            break :blk std.json.parseFromSliceLeaky(std.json.Value, a, bytes[1..], .{ .allocate = .alloc_always, .parse_numbers = false, .max_value_len = bytes.len }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidSqlBinaryRepresentation,
            };
        },
    });
}

/// Own all decoded values before the wire buffer is released. Allocation
/// failures unwind the entire unpublished arena; quota exhaustion is distinct
/// from an injected or genuine backing-allocator failure.
pub fn decode(backing: A, expected: arrays.ElementType, bytes: []const u8, options: Options) !arrays.Owned {
    if (bytes.len > options.wire_bytes) return error.SqlProgramLimitExceeded;
    var reader: Reader = .{ .bytes = bytes };
    const rank = try reader.int(i32);
    if (rank < 0) return error.InvalidSqlBinaryRepresentation;
    if (rank > 6) return error.SqlProgramLimitExceeded;
    const flags = try reader.int(i32);
    if (flags != 0 and flags != 1) return error.InvalidSqlBinaryRepresentation;
    if (try reader.int(u32) != expected.oid()) return error.SqlBinaryTypeMismatch;
    var dimensions: [6]arrays.Dimension = undefined;
    var count: usize = @intFromBool(rank != 0);
    for (dimensions[0..@intCast(rank)]) |*dimension| {
        const length = try reader.int(i32);
        if (length < 0) return error.InvalidSqlBinaryRepresentation;
        dimension.* = .{ .length = @intCast(length), .lower = try reader.int(i32) };
        if (@as(i64, dimension.lower) + length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        count = std.math.mul(usize, count, dimension.length) catch return error.SqlProgramLimitExceeded;
        if (count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
    }
    if (count > options.values.elements) return error.SqlProgramLimitExceeded;
    // Each cell requires at least its signed length prefix. Reject impossible
    // shapes before attempting a count-sized allocation.
    if (count > (bytes.len - reader.position) / 4) return error.InvalidSqlBinaryRepresentation;
    const budget = try backing.create(MemoryBudget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = options.values.bytes };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    const a = arena.allocator();
    const cells = a.alloc(arrays.Element, count) catch |err| return quotaError(budget, err);
    for (cells) |*cell| {
        const length = try reader.int(i32);
        if (length == -1) {
            cell.* = .{};
            continue;
        }
        if (length < -1) return error.InvalidSqlBinaryRepresentation;
        cell.* = decodeElement(a, expected, try reader.take(@intCast(length))) catch |err| return quotaError(budget, err);
    }
    if (reader.position != bytes.len) return error.InvalidSqlBinaryRepresentation;
    const owned_dimensions = a.dupe(arrays.Dimension, dimensions[0..@intCast(rank)]) catch |err| return quotaError(budget, err);
    const value = try arrays.Value.init(expected, owned_dimensions, cells, options.values);
    return .{ .arena = arena, .budget = budget, .value = value };
}

fn quotaError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

test "SQL PostgreSQL array binary roundtrips exact server wire fixtures for every supported element codec" {
    const Entry = struct { sql: []const u8, element_type: arrays.ElementType, array_oid: u32, binary: []const u8, native_binary: ?[]const u8 = null };
    const Fixture = struct { reference: []const u8, scope: []const u8, entries: []const Entry };
    const Faults = struct {
        fn run(backing: A, kind: arrays.ElementType, bytes: []const u8) !void {
            // Arena growth must take the allocation path deterministically:
            // the testing backing allocator's in-place resize availability
            // otherwise depends on addresses from earlier fault iterations.
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const deterministic: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            var owned = try decode(deterministic, kind, bytes, .{});
            defer owned.deinit();
        }
    };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_binary_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 11), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const bytes = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(bytes);
        _ = try std.fmt.hexToBytes(bytes, entry.binary);
        try std.testing.checkAllAllocationFailures(a, Faults.run, .{ entry.element_type, bytes });
        var owned = try decode(a, entry.element_type, bytes, .{});
        defer owned.deinit();
        try std.testing.expectEqual(entry.array_oid, owned.value.element_type.arrayOid());
        var writer: std.Io.Writer.Allocating = .init(a);
        defer writer.deinit();
        try encode(owned.value, &writer.writer, .{});
        const expected = entry.native_binary orelse entry.binary;
        const encoded = try a.alloc(u8, expected.len / 2);
        defer a.free(encoded);
        _ = try std.fmt.hexToBytes(encoded, expected);
        try std.testing.expectEqualSlices(u8, encoded, writer.written());
        var redecoded = try decode(a, entry.element_type, writer.written(), .{});
        defer redecoded.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try owned.value.compare(redecoded.value, &work));
        try std.testing.expectEqual(try owned.value.semanticHash(&work), try redecoded.value.semanticHash(&work));
    }
}

test "SQL PostgreSQL array binary primitive ownership quotas and truncation faults" {
    const Harness = struct {
        fn run(a: A) !void {
            const value = try arrays.Value.init(.text, &.{.{ .length = 3, .lower = -2 }}, &.{ arrays.Element.json(.{ .string = "é" }), .{}, arrays.Element.json(.{ .string = "NULL" }) }, .{});
            var writer: std.Io.Writer.Allocating = .init(a);
            defer writer.deinit();
            // Allocating's only WriteFailed cause is backing allocation
            // failure; preserve that fact for the allocation-fault harness.
            encode(value, &writer.writer, .{}) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const wire = try a.dupe(u8, writer.written());
            defer a.free(wire);
            var owned = try decode(a, .text, wire, .{});
            defer owned.deinit();
            @memset(wire, 0);
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try value.compare(owned.value, &work));
            for (0..writer.written().len) |length| {
                if (decode(a, .text, writer.written()[0..length], .{})) |result| {
                    var unexpected = result;
                    unexpected.deinit();
                    return error.ExpectedTruncationFailure;
                } else |err| {
                    if (err == error.OutOfMemory) return err;
                    try std.testing.expectEqual(error.InvalidSqlBinaryRepresentation, err);
                }
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const empty = try arrays.Value.init(.text, &.{}, &.{}, .{});
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, encode(empty, &writer.writer, .{ .wire_bytes = 11 }));
    try std.testing.expectEqual(@as(usize, 0), writer.written().len);
    try encode(empty, &writer.writer, .{});
    try std.testing.expectError(error.SqlBinaryTypeMismatch, decode(std.testing.allocator, .int32, writer.written(), .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(std.testing.allocator, .text, writer.written(), .{ .wire_bytes = 11 }));
}

test "SQL PostgreSQL array binary boundaries match receive normalization and diagnostics" {
    const a = std.testing.allocator;
    // PostgreSQL bool_recv treats every nonzero byte as true. The array flags
    // are advisory, not a substitute for per-element length/NULL markers.
    const bool_bytes = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 2 };
    var boolean = try decode(a, .boolean, &bool_bytes, .{});
    defer boolean.deinit();
    try std.testing.expect(boolean.value.elements[0].value.bool);
    var integers = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 1, 0, 0, 0, 1, 255, 255, 255, 255 };
    var nullable = try decode(a, .int32, &integers, .{});
    defer nullable.deinit();
    try std.testing.expect(nullable.value.elements[0].sql_null);
    std.mem.writeInt(i32, integers[16..20], std.math.maxInt(i32), .big);
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .int32, &integers, .{}));
    std.mem.writeInt(i32, integers[16..20], std.math.maxInt(i32) - 1, .big);
    var last = try decode(a, .int32, &integers, .{});
    defer last.deinit();
    try std.testing.expectEqual(@as(?i32, std.math.maxInt(i32) - 1), last.value.upper(1));
    const empty_bytes = [_]u8{ 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 23, 0, 1, 134, 160, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    var empty = try decode(a, .int32, &empty_bytes, .{});
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.value.dimensions.len);
    const text_nul = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0 };
    try std.testing.expectError(error.SqlInvalidTextEncoding, decode(a, .text, &text_nul, .{}));
    const diagnostics = @import("errors.zig");
    try std.testing.expectEqualStrings("22P03", diagnostics.describe(error.InvalidSqlBinaryRepresentation).code);
    try std.testing.expectEqualStrings("42804", diagnostics.describe(error.SqlBinaryTypeMismatch).code);
    try std.testing.expectEqualStrings("22021", diagnostics.describe(error.SqlInvalidTextEncoding).code);
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .boolean, &bool_bytes, .{ .values = .{ .bytes = 1 } }));
    var short: [4]u8 = undefined;
    var writer = std.Io.Writer.fixed(&short);
    try std.testing.expectError(error.WriteFailed, encode(boolean.value, &writer, .{}));
}
