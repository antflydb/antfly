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

//! Canonical compact storage adaptation for immutable typed SQL arrays.
//! Directory addressing is storage-independent; typed ownership and JSONB
//! validation live here. Native publication must bind the format capability
//! and precise element identity before admitting these bytes as a row cell.
const std = @import("std");
const arrays = @import("array_value.zig");
pub const layout = @import("../common/sql_array_layout.zig");
const canonical_json = @import("../storage/db/document_content_hash.zig");
const json_order = @import("json_order.zig");
const uuid = @import("../common/uuid.zig");
const MemoryBudget = @import("memory_budget.zig");
const A = std.mem.Allocator;
pub const Options = struct { values: arrays.Limits = .{}, wire_bytes: usize = 8 * 1024 * 1024 };

/// Borrows a pinned immutable input. Primitive preparation allocates nothing;
/// JSONB canonicalizes once into an independently bounded scratch cohort.
/// The final output has a separate wire-byte bound, so peak ownership is bounded
/// even when the caller uses an arena which cannot reclaim individual chunks.
pub const Prepared = struct {
    value: arrays.Value,
    allocator: A,
    jsonb: []?[]const u8,
    encoded_size: usize,
    compact: bool,

    pub fn init(backing: A, input: arrays.Value, options: Options) !Prepared {
        const value = try arrays.Value.init(input.element_type, input.dimensions, input.elements, options.values);
        if (value.dimensions.len != input.dimensions.len) return error.InvalidSqlArrayShape;
        var budget: MemoryBudget = .{ .backing = backing, .limit = options.values.bytes };
        return initAdmitted(backing, budget.allocator(), value, options) catch |err| return quotaError(&budget, err);
    }

    fn initAdmitted(backing: A, scratch: A, value: arrays.Value, options: Options) !Prepared {
        var non_null: usize = 0;
        for (value.elements) |element| non_null += @intFromBool(!element.sql_null);
        var size = try layout.encodedSectionSize(value.element_type, value.dimensions.len, value.elements.len, non_null);
        if (size > options.wire_bytes) return error.SqlProgramLimitExceeded;
        const jsonb = try scratch.alloc(?[]const u8, if (value.element_type == .jsonb) value.elements.len else 0);
        @memset(jsonb, null);
        errdefer {
            for (jsonb) |bytes| if (bytes) |owned| scratch.free(owned);
            scratch.free(jsonb);
        }
        if (layout.width(value.element_type) == 0) for (value.elements, 0..) |element, i| {
            if (element.sql_null) continue;
            if (value.element_type == .numeric) {
                var none = std.heap.FixedBufferAllocator.init(&.{});
                var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator(), .max_output_bytes = options.wire_bytes };
                const length = try @import("numeric_binary.zig").encodedSize(&ctx, element.numeric.?.*);
                size = std.math.add(usize, size, length) catch return error.SqlProgramLimitExceeded;
                if (size > options.wire_bytes or size > std.math.maxInt(u32)) return error.SqlProgramLimitExceeded;
                continue;
            }
            const payload = if (value.element_type == .text) element.value.string else blk: {
                const bytes = try canonical_json.canonicalJsonValueAlloc(scratch, element.value);
                jsonb[i] = bytes;
                break :blk bytes;
            };
            size = std.math.add(usize, size, payload.len) catch return error.SqlProgramLimitExceeded;
            if (size > options.wire_bytes or size > std.math.maxInt(u32)) return error.SqlProgramLimitExceeded;
        };
        return .{ .value = value, .allocator = backing, .jsonb = jsonb, .encoded_size = size, .compact = layout.usesCompact(value.element_type, value.elements.len, non_null) };
    }

    pub fn deinit(self: *Prepared) void {
        for (self.jsonb) |bytes| if (bytes) |owned| self.allocator.free(owned);
        self.allocator.free(self.jsonb);
        self.* = undefined;
    }

    pub fn writeInto(self: Prepared, bytes: []u8) !void {
        if (bytes.len != self.encoded_size) return error.InvalidSqlArrayStorage;
        @memset(bytes, 0);
        bytes[0] = layout.version;
        bytes[1] = @intCast(self.value.dimensions.len);
        bytes[2] = @intFromBool(self.compact);
        std.mem.writeInt(u32, bytes[4..8], @intCast(self.value.elements.len), .little);
        if (self.value.elements.len == 0) return;
        for (self.value.dimensions, 0..) |dimension, i| {
            const at = layout.header_size + i * 8;
            std.mem.writeInt(u32, bytes[at..][0..4], dimension.length, .little);
            std.mem.writeInt(i32, bytes[at + 4 ..][0..4], dimension.lower, .little);
        }
        const bitmap_start = layout.header_size + self.value.dimensions.len * 8;
        const slots_start = bitmap_start + layout.bitmapSize(self.value.elements.len);
        const width = layout.width(self.value.element_type);
        const values_start = slots_start + if (self.compact) layout.checkpointSize(self.value.elements.len) else @as(usize, 0);
        const payload_start = try layout.sectionSize(self.value.element_type, self.value.dimensions.len, self.value.elements.len);
        var payload_at: usize = 0;
        var non_null: usize = 0;
        for (self.value.elements, 0..) |element, i| {
            if (self.compact and i % 64 == 0) std.mem.writeInt(u32, bytes[slots_start + i / 64 * 4 ..][0..4], @intCast(non_null), .little);
            if (element.sql_null) bytes[bitmap_start + i / 8] |= @as(u8, 1) << @intCast(i % 8);
            if (width == 0) {
                std.mem.writeInt(u32, bytes[slots_start + i * 4 ..][0..4], @intCast(payload_at), .little);
                if (!element.sql_null) {
                    if (self.value.element_type == .numeric) {
                        var none = std.heap.FixedBufferAllocator.init(&.{});
                        var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator(), .max_output_bytes = self.encoded_size };
                        const length = try @import("numeric_binary.zig").encodedSize(&ctx, element.numeric.?.*);
                        var writer: std.Io.Writer = .fixed(bytes[payload_start + payload_at ..][0..length]);
                        try @import("numeric_binary.zig").encode(&ctx, element.numeric.?.*, &writer);
                        payload_at += length;
                        continue;
                    }
                    const payload = if (self.value.element_type == .text) element.value.string else self.jsonb[i].?;
                    @memcpy(bytes[payload_start + payload_at ..][0..payload.len], payload);
                    payload_at += payload.len;
                }
                continue;
            }
            if (element.sql_null) continue;
            const index = if (self.compact) non_null else i;
            non_null += 1;
            const out = bytes[values_start + index * width ..][0..width];
            switch (self.value.element_type) {
                .int16 => std.mem.writeInt(i16, out[0..2], @intCast(element.value.integer), .little),
                .int32 => std.mem.writeInt(i32, out[0..4], @intCast(element.value.integer), .little),
                .int64 => std.mem.writeInt(i64, out[0..8], element.value.integer, .little),
                .float32 => {
                    const narrowed: f32 = @floatCast(element.value.float);
                    std.mem.writeInt(u32, out[0..4], if (std.math.isNan(narrowed)) 0x7fc00000 else @bitCast(narrowed), .little);
                },
                .float64 => std.mem.writeInt(u64, out[0..8], if (std.math.isNan(element.value.float)) 0x7ff8000000000000 else @bitCast(element.value.float), .little),
                .boolean => out[0] = @intFromBool(element.value.bool),
                .uuid => @memcpy(out, &try uuid.parse(element.value.string)),
                else => unreachable,
            }
        }
        if (self.compact) std.mem.writeInt(u32, bytes[slots_start + (self.value.elements.len + 63) / 64 * 4 ..][0..4], @intCast(non_null), .little);
        if (width == 0) std.mem.writeInt(u32, bytes[slots_start + self.value.elements.len * 4 ..][0..4], @intCast(payload_at), .little);
    }
};

pub fn encodeAlloc(a: A, value: arrays.Value, options: Options) ![]u8 {
    var prepared = try Prepared.init(a, value, options);
    defer prepared.deinit();
    const bytes = try a.alloc(u8, prepared.encoded_size);
    errdefer a.free(bytes);
    try prepared.writeInto(bytes);
    return bytes;
}

pub const Decoded = struct { value: arrays.Value, work: usize };

pub fn decode(backing: A, expected: arrays.ElementType, bytes: []const u8, options: Options) !arrays.Owned {
    const budget = try backing.create(MemoryBudget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = options.values.bytes };
    const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return quotaError(budget, err);
    errdefer budget.allocator().destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    const decoded = decodeLeaky(arena.allocator(), expected, bytes, options) catch |err| return quotaError(budget, err);
    return .{ .arena = arena, .budget = budget, .value = decoded.value };
}

/// Owns all decoded payloads in the caller's region; no scratch-allocator
/// references escape in nested JSON arrays. Callers destroy the region on error.
pub fn decodeLeaky(backing: A, expected: arrays.ElementType, bytes: []const u8, options: Options) !Decoded {
    var budget: MemoryBudget = .{ .backing = backing, .limit = options.values.bytes };
    return decodeAdmitted(budget.allocator(), backing, expected, bytes, options) catch |err| return quotaError(&budget, err);
}

fn decodeAdmitted(a: A, owner: A, expected: arrays.ElementType, bytes: []const u8, options: Options) !Decoded {
    if (bytes.len > options.values.work) return error.SqlProgramLimitExceeded;
    const view = try layout.View.open(expected, bytes, .{ .elements = options.values.elements, .bytes = options.wire_bytes });
    var work: arrays.Budget = .{ .remaining = options.values.work };
    try work.consume(bytes.len);
    const dimensions = try a.alloc(arrays.Dimension, view.rank);
    for (dimensions, 0..) |*dimension, i| dimension.* = try view.dimension(i);
    const cells = try a.alloc(arrays.Element, view.count);
    for (cells, 0..) |*cell, i| {
        const raw = try view.cell(i);
        if (raw.sql_null) {
            cell.* = .{};
            continue;
        }
        if (expected == .numeric) {
            cell.* = try @import("scalar.zig").numericBinaryLeaky(a, raw.bytes, &work);
            var ctx: @import("numeric_value.zig").Context = .{ .alloc = a, .remaining = work.remaining, .max_output_bytes = options.wire_bytes };
            defer work.remaining = @intCast(ctx.remaining);
            @import("numeric_binary.zig").verifyCanonical(&ctx, raw.bytes, cell.numeric.?.*) catch |err| return switch (err) {
                error.InvalidSqlBinaryRepresentation => error.InvalidSqlArrayStorage,
                else => err,
            };
            continue;
        }
        cell.* = arrays.Element.json(switch (expected) {
            .numeric => unreachable,
            .text => .{ .string = try a.dupe(u8, raw.bytes) },
            .int16 => .{ .integer = std.mem.readInt(i16, raw.bytes[0..2], .little) },
            .int32 => .{ .integer = std.mem.readInt(i32, raw.bytes[0..4], .little) },
            .int64 => .{ .integer = std.mem.readInt(i64, raw.bytes[0..8], .little) },
            .float32 => .{ .float = @as(f32, @bitCast(std.mem.readInt(u32, raw.bytes[0..4], .little))) },
            .float64 => .{ .float = @bitCast(std.mem.readInt(u64, raw.bytes[0..8], .little)) },
            .boolean => .{ .bool = raw.bytes[0] == 1 },
            .uuid => .{ .string = try a.dupe(u8, &uuid.format(raw.bytes[0..16].*)) },
            .jsonb => try decodeJsonb(a, owner, raw.bytes, &work),
        });
    }
    const value = try arrays.Value.initWithBudget(expected, dimensions, cells, options.values, &work);
    return .{ .value = value, .work = options.values.work - work.remaining };
}

fn decodeJsonb(a: A, owner: A, bytes: []const u8, work: *arrays.Budget) !std.json.Value {
    var value = try json_order.parseTextLeaky(a, bytes, work);
    try json_order.validateTextDomain(value, work, 0);
    const canonical = try canonical_json.canonicalJsonValueAlloc(a, value);
    defer a.free(canonical);
    if (!std.mem.eql(u8, bytes, canonical)) return error.NonCanonicalSqlArrayStorage;
    try json_order.rehomeArrayAllocators(&value, owner, work, 0);
    return value;
}

/// Strict publication/restore check without allocating the flat SQL cell
/// vector. Primitive arrays stay allocation-free; JSONB reuses one bounded
/// region, so peak scratch is proportional to one element, not the whole array.
pub fn validateCanonical(a: A, expected: arrays.ElementType, bytes: []const u8, options: Options) !layout.View {
    if (bytes.len > options.values.work) return error.SqlProgramLimitExceeded;
    const view = try layout.View.open(expected, bytes, .{ .elements = options.values.elements, .bytes = options.wire_bytes });
    if (expected != .jsonb) return view;
    var work: arrays.Budget = .{ .remaining = options.values.work };
    try work.consume(bytes.len);
    var budget: MemoryBudget = .{ .backing = a, .limit = options.values.bytes };
    var scratch = std.heap.ArenaAllocator.init(budget.allocator());
    defer scratch.deinit();
    for (0..view.count) |i| {
        const cell = try view.cell(i);
        if (cell.sql_null) continue;
        if (!scratch.reset(.{ .retain_with_limit = @min(256 * 1024, options.values.bytes / 2) })) return quotaError(&budget, error.OutOfMemory);
        _ = decodeJsonb(scratch.allocator(), scratch.allocator(), cell.bytes, &work) catch |err| return quotaError(&budget, err);
    }
    return view;
}

fn quotaError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

test "SQL flat stored arrays roundtrip PostgreSQL binary goldens with canonical JSONB" {
    const Fixture = struct { entries: []const struct { element_type: arrays.ElementType, binary: []const u8 } };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 11), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        const pg_bytes = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(pg_bytes);
        _ = try std.fmt.hexToBytes(pg_bytes, entry.binary);
        var original = try @import("array_binary.zig").decode(a, entry.element_type, pg_bytes, .{});
        defer original.deinit();
        const stored = try encodeAlloc(a, original.value, .{});
        defer a.free(stored);
        _ = try validateCanonical(a, entry.element_type, stored, .{});
        var decoded = try decode(a, entry.element_type, stored, .{});
        defer decoded.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try original.value.compare(decoded.value, &work));
        if (entry.element_type == .float32 or entry.element_type == .float64) for (original.value.elements, decoded.value.elements) |left, right| {
            if (left.sql_null or std.math.isNan(left.value.float)) continue;
            try std.testing.expectEqual(@as(u64, @bitCast(left.value.float)), @as(u64, @bitCast(right.value.float)));
        };
        const again = try encodeAlloc(a, decoded.value, .{});
        defer a.free(again);
        try std.testing.expectEqualSlices(u8, stored, again);
    }
}

test "SQL flat stored array ownership and strict validation clean up every allocation failure" {
    const Fixture = struct { entries: []const struct { element_type: arrays.ElementType, binary: []const u8 } };
    const Faults = struct {
        fn run(backing: A, value: arrays.Value) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            const bytes = try encodeAlloc(a, value, .{});
            defer a.free(bytes);
            _ = try validateCanonical(a, value.element_type, bytes, .{});
            var owned = try decode(a, value.element_type, bytes, .{});
            defer owned.deinit();
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try value.compare(owned.value, &work));
        }
    };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_binary_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        const bytes = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(bytes);
        _ = try std.fmt.hexToBytes(bytes, entry.binary);
        var original = try @import("array_binary.zig").decode(a, entry.element_type, bytes, .{});
        defer original.deinit();
        try std.testing.checkAllAllocationFailures(a, Faults.run, .{original.value});
    }
}

test "SQL flat array dense compact rank directories agree across bitmap and checkpoint boundaries" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int16, .int64 }) |kind| {
        for ([_]usize{ 1, 7, 8, 63, 64, 65, 127, 128, 129 }) |count| {
            for ([_]usize{ 0, 1, 2, 7, 64 }) |null_every| {
                const cells = try a.alloc(arrays.Element, count);
                defer a.free(cells);
                var non_null: usize = 0;
                for (cells, 0..) |*cell, i| {
                    cell.* = if (null_every != 0 and i % null_every == 0) .{} else arrays.Element.json(if (kind == .boolean) .{ .bool = i % 3 == 0 } else .{ .integer = @intCast(i) });
                    non_null += @intFromBool(!cell.sql_null);
                }
                const value = try arrays.Value.init(kind, &.{.{ .length = @intCast(count), .lower = -5 }}, cells, .{});
                const bytes = try encodeAlloc(a, value, .{});
                defer a.free(bytes);
                const view = try layout.View.open(kind, bytes, .{});
                try std.testing.expectEqual(layout.usesCompact(kind, count, non_null), view.compact);
                for (cells, 0..) |cell, i| {
                    const raw = try view.cell(i);
                    try std.testing.expectEqual(cell.sql_null, raw.sql_null);
                    if (cell.sql_null) continue;
                    if (kind == .boolean) try std.testing.expectEqual(@intFromBool(cell.value.bool), raw.bytes[0]) else if (kind == .int16) try std.testing.expectEqual(cell.value.integer, std.mem.readInt(i16, raw.bytes[0..2], .little)) else try std.testing.expectEqual(cell.value.integer, std.mem.readInt(i64, raw.bytes[0..8], .little));
                    try std.testing.expectEqual(i, (try view.ordinal(&.{@as(i32, @intCast(i)) - 5})).?);
                }
                var decoded = try decode(a, kind, bytes, .{});
                defer decoded.deinit();
                var work: arrays.Budget = .{};
                try std.testing.expectEqual(std.math.Order.eq, try value.compare(decoded.value, &work));
            }
        }
    }
}

fn rawJsonbFrame(a: A, text: []const u8) ![]u8 {
    const start = try layout.sectionSize(.jsonb, 1, 1);
    const bytes = try a.alloc(u8, start + text.len);
    @memset(bytes, 0);
    bytes[0] = layout.version;
    bytes[1] = 1;
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u32, bytes[8..12], 1, .little);
    std.mem.writeInt(i32, bytes[12..16], 1, .little);
    std.mem.writeInt(u32, bytes[21..25], @intCast(text.len), .little);
    @memcpy(bytes[start..], text);
    return bytes;
}

test "SQL flat array strict JSONB restore rejects equivalent noncanonical bytes without repairing them" {
    const a = std.testing.allocator;
    for ([_][]const u8{ " 1", "1.0", "{\"b\":1,\"a\":2}", "[1, 2]", "\"\\u0061\"" }) |text| {
        const bytes = try rawJsonbFrame(a, text);
        defer a.free(bytes);
        _ = try layout.View.open(.jsonb, bytes, .{}); // Directory integrity is insufficient.
        try std.testing.expectError(error.NonCanonicalSqlArrayStorage, validateCanonical(a, .jsonb, bytes, .{}));
        try std.testing.expectError(error.NonCanonicalSqlArrayStorage, decode(a, .jsonb, bytes, .{}));
    }
    const invalid_text = try rawJsonbFrame(a, "{\"nested\":[\"a\\u0000b\"]}");
    defer a.free(invalid_text);
    try std.testing.expectError(error.SqlTypeMismatch, validateCanonical(a, .jsonb, invalid_text, .{}));
    const syntax = try rawJsonbFrame(a, "invalid");
    defer a.free(syntax);
    try std.testing.expectError(error.SqlInvalidTextRepresentation, validateCanonical(a, .jsonb, syntax, .{}));
    const canonical = try rawJsonbFrame(a, "{\"a\":[null,true,9007199254740993]}");
    defer a.free(canonical);
    _ = try validateCanonical(a, .jsonb, canonical, .{});
    var owned = try decode(a, .jsonb, canonical, .{});
    defer owned.deinit();
    // Nested managed arrays retain the owned region, not a stack quota wrapper.
    const nested = owned.value.elements[0].value.object.getPtr("a").?;
    try nested.array.append(.{ .integer = 4 });
    try std.testing.expectEqual(@as(usize, 4), nested.array.items.len);
}

test "SQL flat array rejects truncated corrupt rank directories padding offsets and excessive budgets" {
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, 129);
    defer a.free(cells);
    for (cells, 0..) |*cell, i| cell.* = if (i % 2 == 0) .{} else arrays.Element.json(.{ .integer = @intCast(i) });
    const value = try arrays.Value.init(.int64, &.{.{ .length = 129 }}, cells, .{});
    const bytes = try encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const view = try layout.View.open(.int64, bytes, .{});
    try std.testing.expect(view.compact);
    for (0..bytes.len) |length| {
        if (layout.View.open(.int64, bytes[0..length], .{})) |_| return error.TestExpectedError else |_| {}
    }
    const corrupt = try a.dupe(u8, bytes);
    defer a.free(corrupt);
    std.mem.writeInt(u32, corrupt[view.slots_start + 4 ..][0..4], 0, .little);
    try std.testing.expectError(error.InvalidSqlArrayStorage, layout.View.open(.int64, corrupt, .{}));
    @memcpy(corrupt, bytes);
    corrupt[view.slots_start - 1] |= 0x80;
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, layout.View.open(.int64, corrupt, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, encodeAlloc(a, value, .{ .wire_bytes = 8 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .int64, bytes, .{ .values = .{ .bytes = 16 } }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, validateCanonical(a, .int64, bytes, .{ .values = .{ .work = 0 } }));
    const variable = try rawJsonbFrame(a, "null");
    defer a.free(variable);
    std.mem.writeInt(u32, variable[17..21], 1, .little);
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, layout.View.open(.jsonb, variable, .{}));
}

test "SQL flat JSONB stored arrays canonicalize in-memory and parsed numeric values identically" {
    const a = std.testing.allocator;
    const pairs = [_]struct { value: std.json.Value, token: ?[]const u8 }{
        .{ .value = .{ .float = 1.0 }, .token = "1.00" },
        .{ .value = .{ .integer = 1 }, .token = "10e-1" },
        .{ .value = .{ .float = -0.0 }, .token = "0e999" },
        .{ .value = .{ .float = 0.1 }, .token = "0.1000000000000000055511151231257827021181583404541015625" },
        .{ .value = .{ .float = 1e30 }, .token = "1000000000000000019884624838656" },
        .{ .value = .{ .float = @bitCast(@as(u64, 1)) }, .token = null },
        .{ .value = .{ .float = std.math.floatMax(f64) }, .token = null },
    };
    for (pairs) |pair| {
        const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(pair.value)}, .{});
        const left = try encodeAlloc(a, value, .{});
        defer a.free(left);
        if (pair.token) |token| {
            const parsed = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .number_string = token })}, .{});
            const right = try encodeAlloc(a, parsed, .{});
            defer a.free(right);
            try std.testing.expectEqualSlices(u8, left, right);
        }
        _ = try validateCanonical(a, .jsonb, left, .{});
        var decoded = try decode(a, .jsonb, left, .{});
        defer decoded.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try value.compare(decoded.value, &work));
    }
    try std.testing.expectError(error.InvalidJsonNumber, canonical_json.canonicalJsonValueAlloc(a, .{ .float = std.math.inf(f64) }));
    try std.testing.expectError(error.InvalidJsonNumber, canonical_json.canonicalJsonValueAlloc(a, .{ .float = std.math.nan(f64) }));
}

test "SQL flat array logical hashes exclude dense compact representation choices" {
    const a = std.testing.allocator;
    var cells: [129]arrays.Element = undefined;
    for (&cells, 0..) |*cell, i| cell.* = if (i % 2 == 0) .{} else arrays.Element.json(.{ .integer = @intCast(i) });
    const value = try arrays.Value.init(.int64, &.{.{ .length = 129, .lower = -3 }}, &cells, .{});
    const compact = try encodeAlloc(a, value, .{});
    defer a.free(compact);
    const canonical = try layout.View.open(.int64, compact, .{});
    try std.testing.expect(canonical.compact);
    // Simulate another physical encoder. These dense bytes are intentionally
    // not today's canonical format and must not pass the restore gate.
    var prepared = try Prepared.init(a, value, .{});
    defer prepared.deinit();
    prepared.compact = false;
    prepared.encoded_size = try layout.sectionSize(.int64, 1, cells.len);
    const dense = try a.alloc(u8, prepared.encoded_size);
    defer a.free(dense);
    try prepared.writeInto(dense);
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, layout.View.open(.int64, dense, .{}));
    const alternate = try layout.View.openAuthenticated(.int64, dense, .{});
    var left = std.crypto.hash.Blake3.init(.{});
    var right = std.crypto.hash.Blake3.init(.{});
    canonical.updateLogicalHash(&left);
    alternate.updateLogicalHash(&right);
    var left_digest: [32]u8 = undefined;
    var right_digest: [32]u8 = undefined;
    left.final(&left_digest);
    right.final(&right_digest);
    try std.testing.expectEqualSlices(u8, &left_digest, &right_digest);
}

test "SQL flat array primitive preparation uses one output allocation and cold shape projection stays bounded" {
    const a = std.testing.allocator;
    const count = 4096;
    const cells = try a.alloc(arrays.Element, count);
    defer a.free(cells);
    for ([_]arrays.ElementType{ .boolean, .int16, .int64 }) |kind| {
        for (cells, 0..) |*cell, i| cell.* = if (i % 8 == 0) .{} else arrays.Element.json(if (kind == .boolean) .{ .bool = i % 3 == 0 } else .{ .integer = @intCast(i) });
        const value = try arrays.Value.init(kind, &.{.{ .length = count, .lower = -7 }}, cells, .{});
        var no_alloc = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var prepared = try Prepared.init(no_alloc.allocator(), value, .{});
        defer prepared.deinit();
        try std.testing.expectEqual(@as(usize, 0), no_alloc.alloc_index);
        var tracked = std.testing.FailingAllocator.init(a, .{});
        const bytes = try encodeAlloc(tracked.allocator(), value, .{});
        defer tracked.allocator().free(bytes);
        try std.testing.expectEqual(@as(usize, 1), tracked.alloc_index);
        const view = try layout.View.open(kind, bytes, .{});
        var pg: std.Io.Writer.Discarding = .init(&.{});
        try @import("array_binary.zig").encode(value, &pg.writer, .{});
        try std.testing.expect(bytes.len < pg.count);
        const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        var total: u64 = 0;
        for (0..10_000) |i| {
            const shape = try layout.inspectShape(kind, bytes, .{});
            const cell = try view.cell(i % count);
            total += shape.count + @intFromBool(cell.sql_null);
        }
        try std.testing.expect(total >= 10_000 * count);
        std.debug.print("SQL flat {s} array: cells={} stored_bytes={} pg_wire_bytes={} projected=10000 elapsed_ns={} preparation_allocations=0 output_allocations=1\n", .{ @tagName(kind), count, bytes.len, pg.count, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started });
    }
}
