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

//! PostgreSQL text-array input, independent of JSON array representations.
//! A no-allocation shape pass admits exact cell storage before decoding. One
//! reusable escape buffer serves all cells; retained payloads own their bytes.
const std = @import("std");
const arrays = @import("array_value.zig");
const builtin_cast = @import("builtin_cast.zig");
const uuid = @import("../common/uuid.zig");
const MemoryBudget = @import("memory_budget.zig");
const A = std.mem.Allocator;
pub const Options = struct {
    values: arrays.Limits = .{},
    wire_bytes: usize = 8 * 1024 * 1024,
    /// Borrowed invocation identity, retained across both parsing passes and
    /// typed validation. Local limits cannot restart this parent's budget.
    context: ?*@import("numeric_value.zig").Context = null,
};
pub const Decoded = struct { value: arrays.Value, allocated_bytes: usize, work: usize };

/// An unbuffered escaping adapter lets JSONB serialization write directly to
/// an array element. No per-cell JSON string or temporary allocator is needed.
const QuotedWriter = struct {
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    target: *std.Io.Writer,

    fn escaped(self: *QuotedWriter, bytes: []const u8) std.Io.Writer.Error!void {
        var start: usize = 0;
        for (bytes, 0..) |byte, index| if (byte == '"' or byte == '\\') {
            try self.target.writeAll(bytes[start..index]);
            try self.target.writeByte('\\');
            try self.target.writeByte(byte);
            start = index + 1;
        };
        try self.target.writeAll(bytes[start..]);
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *QuotedWriter = @fieldParentPtr("writer", w);
        std.debug.assert(w.end == 0);
        var count: usize = 0;
        for (data, 0..) |bytes, index| {
            const repetitions = if (index + 1 == data.len) splat else 1;
            for (0..repetitions) |_| {
                try self.escaped(bytes);
                count = std.math.add(usize, count, bytes.len) catch return error.WriteFailed;
            }
        }
        return count;
    }
};

fn writeElement(kind: arrays.ElementType, cell: arrays.Element, writer: *std.Io.Writer) !void {
    if (cell.sql_null) return writer.writeAll("NULL");
    switch (kind) {
        .numeric => {
            var none = std.heap.FixedBufferAllocator.init(&.{});
            var ctx: @import("numeric_value.zig").Context = .{ .alloc = none.allocator() };
            try @import("numeric_value.zig").write(&ctx, cell.numeric.?.*, writer);
        },
        .text, .uuid, .jsonb => {
            try writer.writeByte('"');
            var escaped: QuotedWriter = .{ .target = writer };
            if (kind == .jsonb) {
                try std.json.Stringify.value(cell.value, .{}, &escaped.writer);
            } else try escaped.writer.writeAll(cell.value.string);
            try writer.writeByte('"');
        },
        .int16, .int32, .int64 => try writer.print("{d}", .{cell.value.integer}),
        .float32, .float64 => {
            var buffer: [64]u8 = undefined;
            const text = if (kind == .float32)
                try builtin_cast.floatText(f32, @floatCast(cell.value.float), &buffer)
            else
                try builtin_cast.floatText(f64, cell.value.float, &buffer);
            try writer.writeAll(text);
        },
        .boolean => try writer.writeAll(if (cell.value.bool) "t" else "f"),
    }
}

fn writeAxis(value: arrays.Value, depth: usize, offset: *usize, writer: *std.Io.Writer) anyerror!void {
    try writer.writeByte('{');
    for (0..value.dimensions[depth].length) |index| {
        if (index != 0) try writer.writeByte(',');
        if (depth + 1 == value.dimensions.len) {
            try writeElement(value.element_type, value.elements[offset.*], writer);
            offset.* += 1;
        } else try writeAxis(value, depth + 1, offset, writer);
    }
    try writer.writeByte('}');
}

fn writeValue(value: arrays.Value, writer: *std.Io.Writer) !void {
    if (value.dimensions.len == 0) return writer.writeAll("{}");
    for (value.dimensions) |dimension| if (dimension.lower != 1) {
        for (value.dimensions) |axis| try writer.print("[{d}:{d}]", .{ axis.lower, @as(i64, axis.lower) + axis.length - 1 });
        try writer.writeByte('=');
        break;
    };
    var offset: usize = 0;
    try writeAxis(value, 0, &offset, writer);
    std.debug.assert(offset == value.elements.len);
}

/// Validate shape, payload domains and complete wire/work admission before
/// touching the destination. Emission streams with bounded (rank <= 6) stack
/// depth and zero allocations, including escaped JSONB and non-finite floats.
pub fn encode(value: arrays.Value, writer: *std.Io.Writer, options: Options) !void {
    return encodeInner(value, writer, options) catch |err| return sharedFailure(options, err);
}

fn encodeInner(value: arrays.Value, writer: *std.Io.Writer, options: Options) !void {
    var work: arrays.Budget = .{ .remaining = options.values.work, .shared = options.context };
    try work.consume(0);
    const canonical = try arrays.Value.initWithBudget(value.element_type, value.dimensions, value.elements, options.values, &work);
    if (canonical.dimensions.len != value.dimensions.len) return error.InvalidSqlArrayShape;
    var size: std.Io.Writer.Discarding = .init(&.{});
    try writeValue(value, &size.writer);
    if (size.count > options.wire_bytes or size.count > work.remaining / 2) return error.SqlProgramLimitExceeded;
    try work.consume(@as(usize, @intCast(size.count)) * 2);
    try writeValue(value, writer);
}

const Shape = struct {
    lengths: [6]u32 = @splat(0),
    rank: usize = 1,
    fn same(left: Shape, right: Shape) bool {
        return left.rank == right.rank and std.mem.eql(u32, left.lengths[0..left.rank], right.lengths[0..right.rank]);
    }
};

const Token = struct {
    raw: []const u8,
    decoded_len: usize,
    escaped: bool,
    sql_null: bool,
    fn text(self: Token, scratch: []u8) []const u8 {
        if (!self.escaped) return self.raw;
        var read: usize = 0;
        var write: usize = 0;
        while (read < self.raw.len) : (read += 1) {
            if (self.raw[read] == '\\') read += 1;
            scratch[write] = self.raw[read];
            write += 1;
        }
        return scratch[0..write];
    }
};

const Reader = struct {
    bytes: []const u8,
    at: usize = 0,
    count: usize = 0,
    max_escape: usize = 0,
    limit: usize,
    cells: ?[]arrays.Element = null,
    scratch: []u8 = &.{},
    alloc: A = undefined,
    kind: arrays.ElementType = undefined,
    work: *arrays.Budget,
    accounted: usize = 0,

    fn account(self: *Reader, finish: bool) !void {
        const count = self.at - self.accounted;
        if (finish or count >= 256) {
            try self.work.consume(count);
            self.accounted = self.at;
        }
    }

    fn take(self: *Reader, byte: u8) bool {
        if (self.at < self.bytes.len and self.bytes[self.at] == byte) {
            self.at += 1;
            return true;
        }
        return false;
    }
    fn space(self: *Reader) !void {
        while (self.at < self.bytes.len and std.mem.indexOfScalar(u8, builtin_cast.whitespace, self.bytes[self.at]) != null) {
            try self.account(false);
            self.at += 1;
        }
        try self.account(false);
    }
    fn bound(self: *Reader) !i32 {
        const start = self.at;
        if (self.at < self.bytes.len and (self.bytes[self.at] == '-' or self.bytes[self.at] == '+')) self.at += 1;
        const digits = self.at;
        while (self.at < self.bytes.len and std.ascii.isDigit(self.bytes[self.at])) {
            try self.account(false);
            self.at += 1;
        }
        if (self.at == digits) return error.SqlInvalidTextRepresentation;
        return std.fmt.parseInt(i32, self.bytes[start..self.at], 10) catch return error.SqlProgramLimitExceeded;
    }
    fn token(self: *Reader) !Token {
        const quoted = self.take('"');
        const start = self.at;
        var end = start;
        var escaped = false;
        var closed = !quoted;
        while (self.at < self.bytes.len) {
            try self.account(false);
            const byte = self.bytes[self.at];
            if (byte == '\\') {
                escaped = true;
                self.at += 1;
                if (self.at == self.bytes.len) return error.SqlInvalidTextRepresentation;
                self.at += 1;
                end = self.at;
                continue;
            }
            if (quoted and byte == '"') {
                end = self.at;
                self.at += 1;
                closed = true;
                break;
            }
            if (!quoted and (byte == ',' or byte == '}')) break;
            if (!quoted and (byte == '"' or byte == '{')) return error.SqlInvalidTextRepresentation;
            self.at += 1;
            if (quoted or std.mem.indexOfScalar(u8, builtin_cast.whitespace, byte) == null) end = self.at;
        }
        if (!closed or (!quoted and end == start)) return error.SqlInvalidTextRepresentation;
        const raw = self.bytes[start..end];
        try self.work.consume(raw.len);
        var decoded = raw.len;
        var at: usize = 0;
        while (at < raw.len) : (at += 1) if (raw[at] == '\\') {
            decoded -= 1;
            at += 1;
        };
        return .{ .raw = raw, .decoded_len = decoded, .escaped = escaped, .sql_null = !quoted and !escaped and std.ascii.eqlIgnoreCase(raw, "NULL") };
    }
    fn array(self: *Reader, depth: usize) anyerror!Shape {
        if (depth >= 6) return error.SqlProgramLimitExceeded;
        try self.space();
        if (!self.take('{')) return error.SqlInvalidTextRepresentation;
        try self.space();
        var result: Shape = .{};
        if (self.take('}')) return result;
        const nested = self.at < self.bytes.len and self.bytes[self.at] == '{';
        var child_shape: ?Shape = null;
        while (true) {
            try self.space();
            if (self.at == self.bytes.len or nested != (self.bytes[self.at] == '{')) return error.SqlInvalidTextRepresentation;
            if (nested) {
                const child = try self.array(depth + 1);
                if (child_shape) |expected| {
                    if (!expected.same(child)) return error.SqlInvalidTextRepresentation;
                } else child_shape = child;
            } else {
                const cell = try self.token();
                try self.work.consume(1);
                if (self.count >= self.limit) return error.SqlProgramLimitExceeded;
                if (cell.escaped) self.max_escape = @max(self.max_escape, cell.decoded_len);
                if (self.cells) |cells| cells[self.count] = if (cell.sql_null) .{} else try decodeElement(self.alloc, self.kind, cell.text(self.scratch), self.work);
                self.count += 1;
            }
            result.lengths[0] = std.math.add(u32, result.lengths[0], 1) catch return error.SqlProgramLimitExceeded;
            try self.space();
            if (self.take('}')) break;
            if (!self.take(',')) return error.SqlInvalidTextRepresentation;
        }
        if (child_shape) |child| {
            if (child.rank == 6) return error.SqlProgramLimitExceeded;
            result.rank = child.rank + 1;
            @memcpy(result.lengths[1..result.rank], child.lengths[0..child.rank]);
        }
        return result;
    }
};

fn decodeElement(a: A, kind: arrays.ElementType, text: []const u8, work: *arrays.Budget) !arrays.Element {
    if (kind == .numeric) return @import("scalar.zig").numericTextLeaky(a, text, work);
    return arrays.Element.json(switch (kind) {
        .numeric => unreachable,
        .text => .{ .string = try a.dupe(u8, text) },
        .int16, .int32, .int64 => .{ .integer = try builtin_cast.integerText(text, kind) },
        .float32 => .{ .float = try builtin_cast.floatValue(f32, .{ .string = text }) },
        .float64 => .{ .float = try builtin_cast.floatValue(f64, .{ .string = text }) },
        .boolean => .{ .bool = try builtin_cast.booleanText(text) },
        .uuid => .{ .string = uuid.canonicalAlloc(a, text) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.SqlInvalidTextRepresentation,
        } },
        .jsonb => try @import("json_order.zig").parseTextLeaky(a, text, work),
    });
}

/// Scalar text-protocol input uses the same builtin codecs as array cells,
/// without array tokenization or SQL NULL inference from the string "NULL".
pub fn decodeElementLeaky(a: A, kind: arrays.ElementType, text: []const u8, work: *arrays.Budget) !arrays.Element {
    try work.consume(text.len);
    if (!std.unicode.utf8ValidateSlice(text) or std.mem.indexOfScalar(u8, text, 0) != null) return error.SqlInvalidTextEncoding;
    return decodeElement(a, kind, text, work);
}

/// Allocate into the caller's statement/row arena; that arena owns cleanup on
/// both success and error, like parseFromSliceLeaky. No allocator references
/// escape: the local budget bounds all requested allocations, including JSONB.
pub fn decodeLeaky(a: A, kind: arrays.ElementType, bytes: []const u8, options: Options) !Decoded {
    return decodeLeakyInner(a, kind, bytes, options) catch |err| return sharedFailure(options, err);
}

fn sharedFailure(options: Options, err: anyerror) anyerror {
    return if (err == error.SqlProgramLimitExceeded and options.context != null) options.context.?.limit() else err;
}

fn decodeLeakyInner(a: A, kind: arrays.ElementType, bytes: []const u8, options: Options) !Decoded {
    var work: arrays.Budget = .{ .remaining = options.values.work, .shared = options.context };
    try work.consume(0);
    if (bytes.len > options.wire_bytes or bytes.len > options.values.work / 2) return if (options.context) |context| context.limit() else error.SqlProgramLimitExceeded;
    try work.consume(bytes.len);
    if (!std.unicode.utf8ValidateSlice(bytes) or std.mem.indexOfScalar(u8, bytes, 0) != null) return error.SqlInvalidTextEncoding;
    var reader: Reader = .{ .bytes = bytes, .limit = options.values.elements, .work = &work };
    try reader.space();
    var declared: [6]arrays.Dimension = undefined;
    var rank: usize = 0;
    while (reader.take('[')) {
        if (rank == 6) return error.SqlProgramLimitExceeded;
        const first = try reader.bound();
        const has_lower = reader.take(':');
        const lower = if (has_lower) first else 1;
        const upper = if (has_lower) try reader.bound() else first;
        if (!reader.take(']')) return error.SqlInvalidTextRepresentation;
        if (upper < lower) return error.SqlArraySubscriptError;
        const length = @as(i64, upper) - lower + 1;
        if (length > std.math.maxInt(i32) or @as(i64, lower) + length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        declared[rank] = .{ .lower = lower, .length = @intCast(length) };
        rank += 1;
        try reader.space();
    }
    if (rank != 0 and !reader.take('=')) return error.SqlInvalidTextRepresentation;
    const body = reader.at;
    const shape = try reader.array(0);
    try reader.space();
    try reader.account(true);
    if (reader.at != bytes.len) return error.SqlInvalidTextRepresentation;
    if (rank != 0) {
        if (rank != shape.rank) return error.SqlInvalidTextRepresentation;
        for (declared[0..rank], shape.lengths[0..rank]) |dimension, length| if (dimension.length != length) return error.SqlInvalidTextRepresentation;
    } else {
        rank = shape.rank;
        for (declared[0..rank], shape.lengths[0..rank]) |*dimension, length| dimension.* = .{ .length = length };
    }
    var budget: MemoryBudget = .{ .backing = a, .limit = options.values.bytes };
    return decodeAdmitted(&budget, kind, bytes, options, body, declared[0..rank], reader.count, reader.max_escape, &work) catch |err| {
        const mapped = quotaError(&budget, err);
        return if (mapped == error.SqlProgramLimitExceeded and options.context != null) options.context.?.limit() else mapped;
    };
}

test "SQL array text shared work owner cancels both passes and retains failures without leaked ownership" {
    const a = std.testing.allocator;
    var text: [5121]u8 = undefined;
    text[0] = '{';
    for (0..1024) |index| {
        @memcpy(text[1 + index * 5 ..][0..4], "NULL");
        text[5 + index * 5] = if (index == 1023) '}' else ',';
    }
    const Control = struct {
        calls: usize = 0,
        stop: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.QueryCanceled;
        }
    };
    const backing = try a.alloc(u8, 512 * 1024);
    defer a.free(backing);
    for ([_]usize{ 2, 50 }) |stop| {
        var fixed = std.heap.FixedBufferAllocator.init(backing);
        var control: Control = .{ .stop = stop };
        var context: @import("numeric_value.zig").Context = .{ .alloc = fixed.allocator(), .checkpoint = Control.check, .ptr = &control };
        try std.testing.expectError(error.QueryCanceled, decodeLeaky(fixed.allocator(), .int64, &text, .{ .context = &context }));
        try std.testing.expectEqual(stop, control.calls);
        if (stop == 2) try std.testing.expectEqual(@as(usize, 0), fixed.end_index) else try std.testing.expect(fixed.end_index > 0);
        const retained = fixed.end_index;
        const remaining = context.remaining;
        try std.testing.expectError(error.QueryCanceled, decodeLeaky(fixed.allocator(), .int64, "{1}", .{ .context = &context }));
        try std.testing.expectEqual(retained, fixed.end_index);
        try std.testing.expectEqual(remaining, context.remaining);
        try std.testing.expectEqual(stop, control.calls);
    }
    // Owned adaptation must unwind allocations after decoding has started.
    var control: Control = .{ .stop = 50 };
    var context: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Control.check, .ptr = &control };
    try std.testing.expectError(error.QueryCanceled, decode(a, .int64, &text, .{ .context = &context }));
}

test "SQL array text shared work owner distinguishes real OOM from sticky quota exhaustion" {
    var none = std.heap.FixedBufferAllocator.init(&.{});
    var context: @import("numeric_value.zig").Context = .{ .alloc = none.allocator() };
    try std.testing.expectError(error.OutOfMemory, decodeLeaky(none.allocator(), .int64, "{1,NULL,2}", .{ .context = &context }));
    try std.testing.expect(context.failure == null);
    try std.testing.expectError(error.SqlProgramLimitExceeded, decodeLeaky(none.allocator(), .int64, "{1,NULL,2}", .{ .values = .{ .bytes = 0 }, .context = &context }));
    try std.testing.expectEqual(error.SqlProgramLimitExceeded, context.failure.?);
    const remaining = context.remaining;
    try std.testing.expectError(error.SqlProgramLimitExceeded, decodeLeaky(none.allocator(), .int64, "{1}", .{ .context = &context }));
    try std.testing.expectEqual(remaining, context.remaining);
}

fn decodeAdmitted(budget: *MemoryBudget, kind: arrays.ElementType, bytes: []const u8, options: Options, body: usize, dimensions: []const arrays.Dimension, count: usize, escape_bytes: usize, work: *arrays.Budget) !Decoded {
    const a = budget.allocator();
    const cells = try a.alloc(arrays.Element, count);
    const scratch = try a.alloc(u8, escape_bytes);
    var reader: Reader = .{ .bytes = bytes, .at = body, .accounted = body, .limit = options.values.elements, .cells = cells, .scratch = scratch, .alloc = a, .kind = kind, .work = work };
    _ = try reader.array(0);
    try reader.account(true);
    const owned_dimensions = try a.dupe(arrays.Dimension, dimensions);
    if (kind == .jsonb) for (cells) |*cell| if (!cell.sql_null) {
        try @import("json_order.zig").rehomeArrayAllocators(&cell.value, budget.backing, work, 0);
    };
    const value = try arrays.Value.initWithBudget(kind, owned_dimensions, cells, options.values, work);
    a.free(scratch);
    return .{ .value = value, .allocated_bytes = budget.live, .work = options.values.work - work.remaining };
}

/// Stable owned ingress form. The outer budget also accounts arena capacity,
/// not only requested cell/JSON bytes. Failure destroys the unpublished owner.
pub fn decode(backing: A, kind: arrays.ElementType, bytes: []const u8, options: Options) !arrays.Owned {
    if (options.context) |context| try context.charge(0);
    const budget = try backing.create(MemoryBudget);
    errdefer backing.destroy(budget);
    budget.* = .{ .backing = backing, .limit = options.values.bytes };
    const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return sharedFailure(options, quotaError(budget, err));
    errdefer budget.allocator().destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    const decoded = decodeLeaky(arena.allocator(), kind, bytes, options) catch |err| return sharedFailure(options, quotaError(budget, err));
    return .{ .value = decoded.value, .arena = arena, .budget = budget };
}

fn quotaError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

const Fixture = struct {
    reference: []const u8,
    entries: []const struct { input: []const u8, element_type: arrays.ElementType, sql_type: []const u8, binary: []const u8 },
    errors: []const struct { input: []const u8, element_type: arrays.ElementType, sql_type: []const u8, code: []const u8 },
};

test "SQL text arrays match PostgreSQL builtin values bounds escapes and diagnostics" {
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_text_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 19), fixture.value.entries.len);
    try std.testing.expectEqual(@as(usize, 19), fixture.value.errors.len);
    for (fixture.value.entries) |entry| {
        const wire = try a.alloc(u8, entry.binary.len / 2);
        defer a.free(wire);
        _ = try std.fmt.hexToBytes(wire, entry.binary);
        var expected = try @import("array_binary.zig").decode(a, entry.element_type, wire, .{});
        defer expected.deinit();
        const text = try a.dupe(u8, entry.input);
        defer a.free(text);
        var actual = try decode(a, entry.element_type, text, .{});
        defer actual.deinit();
        @memset(text, 0);
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(std.math.Order.eq, try expected.value.compare(actual.value, &work));
        try std.testing.expectEqual(try expected.value.semanticHash(&work), try actual.value.semanticHash(&work));
        if (entry.element_type != .jsonb) {
            var writer: std.Io.Writer.Allocating = .init(a);
            defer writer.deinit();
            try @import("array_binary.zig").encode(actual.value, &writer.writer, .{});
            try std.testing.expectEqualSlices(u8, wire, writer.written());
        }
        try std.testing.expect(actual.budget.peak <= actual.budget.limit);
        var encoded: std.Io.Writer.Allocating = .init(a);
        defer encoded.deinit();
        try encode(actual.value, &encoded.writer, .{});
        var roundtrip = try decode(a, entry.element_type, encoded.written(), .{});
        defer roundtrip.deinit();
        try std.testing.expectEqual(std.math.Order.eq, try expected.value.compare(roundtrip.value, &work));
    }
    for (fixture.value.errors) |entry| {
        if (decode(a, entry.element_type, entry.input, .{})) |result| {
            var unexpected = result;
            unexpected.deinit();
            std.debug.print("Unexpected array text admission: {s}\n", .{entry.input});
            return error.ExpectedPostgresRejection;
        } else |err| try std.testing.expectEqualStrings(entry.code, @import("errors.zig").describe(err).code);
    }
}

test "SQL text array output streams escaping bounds nulls and quotas without allocation" {
    const a = std.testing.allocator;
    for ([_]struct { kind: arrays.ElementType, input: []const u8, output: []const u8 }{
        .{ .kind = .text, .input = "[0:1][3:4]={{NULL,\"NULL\"},{\"a\\\"b\",\"c\\\\d\"}}", .output = "[0:1][3:4]={{NULL,\"NULL\"},{\"a\\\"b\",\"c\\\\d\"}}" },
        .{ .kind = .int64, .input = "{-9223372036854775808,9223372036854775807,NULL}", .output = "{-9223372036854775808,9223372036854775807,NULL}" },
        .{ .kind = .float32, .input = "{NaN,Infinity,-Infinity,-0,1.5}", .output = "{NaN,Infinity,-Infinity,-0,1.5}" },
        .{ .kind = .boolean, .input = "{true,false,NULL}", .output = "{t,f,NULL}" },
        .{ .kind = .jsonb, .input = "{\"null\",NULL,\"[1,2]\",\"{\\\"k\\\":\\\"a\\\\\\\\b\\\"}\"}", .output = "{\"null\",NULL,\"[1,2]\",\"{\\\"k\\\":\\\"a\\\\\\\\b\\\"}\"}" },
        .{ .kind = .text, .input = "{}", .output = "{}" },
    }) |entry| {
        var owned = try decode(a, entry.kind, entry.input, .{});
        defer owned.deinit();
        var buffer: [512]u8 = undefined;
        var writer: std.Io.Writer = .fixed(&buffer);
        try encode(owned.value, &writer, .{});
        try std.testing.expectEqualStrings(entry.output, writer.buffered());
        writer.end = 0;
        try std.testing.expectError(error.SqlProgramLimitExceeded, encode(owned.value, &writer, .{ .wire_bytes = entry.output.len - 1 }));
        try std.testing.expectEqual(@as(usize, 0), writer.end);
        try std.testing.expectError(error.SqlProgramLimitExceeded, encode(owned.value, &writer, .{ .values = .{ .work = 1 } }));
        try std.testing.expectEqual(@as(usize, 0), writer.end);
        var short: std.Io.Writer = .fixed(buffer[0..1]);
        try std.testing.expectError(error.WriteFailed, encode(owned.value, &short, .{}));
    }
}

test "SQL text arrays unwind allocation faults and admit shape before cell storage" {
    const Faults = struct {
        fn run(backing: A, kind: arrays.ElementType, input: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            var owned = try decode(.{ .ptr = backing.ptr, .vtable = &vtable }, kind, input, .{});
            defer owned.deinit();
        }
    };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_text_reference.json"), .{});
    defer fixture.deinit();
    for (fixture.value.entries) |entry| {
        try std.testing.checkAllAllocationFailures(a, Faults.run, .{ entry.element_type, entry.input });
        for (0..entry.input.len) |length| {
            if (decode(a, entry.element_type, entry.input[0..length], .{})) |result| {
                var unexpected = result;
                unexpected.deinit();
                return error.ExpectedTruncationFailure;
            } else |_| {}
        }
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .text, "{a,b}", .{ .values = .{ .elements = 1 } }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .text, "{a,b}", .{ .values = .{ .bytes = 1 } }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .text, "{a,b}", .{ .values = .{ .work = 1 } }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, decode(a, .text, "{a,b}", .{ .wire_bytes = 1 }));
    try std.testing.expectError(error.SqlInvalidTextEncoding, decode(a, .text, "{a\x00}", .{}));
    try std.testing.expectError(error.SqlInvalidTextEncoding, decode(a, .text, "{\xff}", .{}));
}
