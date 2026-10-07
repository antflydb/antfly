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

//! Immutable execution-owned SQL parameters. Decode/canonicalize once and bind
//! every scalar program once; the row loop only borrows validated typed cells.
const std = @import("std");
const scalar = @import("scalar.zig");
const arrays = @import("array_value.zig");
const operators = @import("operators.zig");
const text = @import("array_text.zig");
const binary = @import("array_binary.zig");
const MemoryBudget = @import("memory_budget.zig");
const A = std.mem.Allocator;

test "SQL prepared parameter frames own typed arrays and bind shared programs once" {
    const a = std.testing.allocator;
    const compiler = @import("compiler.zig");
    const hints: []const scalar.Type = &.{.{ .kind = .array, .element_type = .int64 }};
    var compiled = try compiler.compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer }}, hints, .{});
    defer program.deinit();
    var bounds_sql = try compiler.compileScalar(a, "array_lower($1, 1)", .{});
    defer bounds_sql.deinit();
    var bounds = try scalar.bindTyped(a, bounds_sql.expression, &.{}, hints, .{});
    defer bounds.deinit();
    const input = try a.dupe(u8, "[-1:0][2:3]={{9007199254740993,2},{3,NULL}}");
    defer a.free(input);
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = input }}, .{});
    defer frame.deinit();
    @memset(input, 0);
    const prepared = try program.bindParameters(&frame);
    const bound_bounds = try bounds.bindParameters(&frame);
    var none = std.heap.FixedBufferAllocator.init(&.{});
    try std.testing.expectEqual(@as(i64, -1), (try bound_bounds.evaluate(none.allocator(), &.{}, .{})).value.integer);
    const array = frame.values[0].array.?;
    try std.testing.expectEqual(@as(i32, 2), array.dimensions[1].lower);
    try std.testing.expect(array.elements[3].sql_null);
    var matches: usize = 0;
    var nulls: usize = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10000) |row| {
        const result = try prepared.evaluate(none.allocator(), &.{scalar.Datum.json(.{ .integer = if (row % 2 == 0) 9007199254740993 else 9 })}, .{});
        if (result.sql_null) nulls += 1 else matches += @intFromBool(result.value.bool);
    }
    try std.testing.expectEqual(@as(usize, 5000), matches);
    try std.testing.expectEqual(@as(usize, 5000), nulls);
    std.debug.print("SQL prepared array parameters: rows=10000 decode_count=1 evaluation_scratch_bytes=0 frame_peak_bytes={} elapsed_ns={}\n", .{ frame.budget.peak, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    try std.testing.expectError(error.UnsupportedSqlShape, program.evaluate(none.allocator(), &.{scalar.Datum.json(.{ .integer = 1 })}, &.{.{ .string = "{1,2}" }}, .{}));
}

test "SQL typed parameter inference preserves element identity and guards compatibility ingress" {
    const a = std.testing.allocator;
    const compiler = @import("compiler.zig");
    var compiled = try compiler.compileScalar(a, "ARRAY[cardinality($1::integer[]),cardinality($1::text[])]", .{});
    defer compiled.deinit();
    var parameters: [1]scalar.Type = .{.{}};
    try std.testing.expect(try scalar.inferTypedParameters(a, compiled.expression, &.{}, &parameters, .{}));
    try std.testing.expectEqual(@as(?@import("ast.zig").ColumnType, .array), parameters[0].kind);
    try std.testing.expectEqual(@as(?arrays.ElementType, .int32), parameters[0].element_type);
    try std.testing.expect(!try scalar.inferTypedParameters(a, compiled.expression, &.{}, &parameters, .{}));
    var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = "{1,2}" }}, .{});
    defer frame.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = try (try program.bindParameters(&frame)).evaluate(arena.allocator(), &.{}, .{});
    try std.testing.expectEqual(@as(i64, 2), result.array.?.elements[0].value.integer);
    try std.testing.expectEqual(@as(i64, 2), result.array.?.elements[1].value.integer);
    var wrong = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .text }}, &.{.{ .text = "{1,2}" }}, .{});
    defer wrong.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, program.bindParameters(&wrong));
    try std.testing.expectError(error.UnsupportedSqlShape, scalar.bind(a, compiled.expression, &.{}, &.{.array}, .{}));
    try std.testing.expectError(error.InvalidSqlParameters, scalar.bindTyped(a, compiled.expression, &.{}, &.{.{ .kind = .array }}, .{}));
    var scalar_cast = try compiler.compileScalar(a, "$1::smallint", .{});
    defer scalar_cast.deinit();
    var scalar_parameter = try scalar.bindTyped(a, scalar_cast.expression, &.{}, &.{}, .{});
    defer scalar_parameter.deinit();
    try std.testing.expectEqual(@as(?arrays.ElementType, .int16), scalar_parameter.parameter_descriptors[0].element_type);
    try std.testing.expectError(error.SqlNumericOutOfRange, Frame.prepare(a, scalar_parameter.parameter_descriptors, &.{.{ .text = "32768" }}, .{}));
    var quantified = try compiler.compileScalar(a, "$1 = ANY($2::smallint[])", .{});
    defer quantified.deinit();
    var inferred = try scalar.bindTyped(a, quantified.expression, &.{}, &.{}, .{});
    defer inferred.deinit();
    try std.testing.expectEqual(@as(?arrays.ElementType, .int16), inferred.parameter_descriptors[0].element_type);
    try std.testing.expectEqual(@as(?arrays.ElementType, .int16), inferred.parameter_descriptors[1].element_type);
}

pub const Input = union(enum) {
    sql_null,
    /// Already decoded logical values, including JSON null distinct from SQL
    /// NULL. A JSON array is never inferred to be a typed SQL array.
    datum: scalar.Datum,
    text: []const u8,
    binary: []const u8,
};

fn referenceType(name: []const u8) !scalar.Type {
    const array = std.mem.endsWith(u8, name, "[]");
    const base = if (array) name[0 .. name.len - 2] else name;
    if (std.mem.eql(u8, base, "timestamptz") and !array) return .{ .kind = .datetime };
    const kinds = .{ .{ "smallint", arrays.ElementType.int16, @import("ast.zig").ColumnType.integer }, .{ "integer", arrays.ElementType.int32, @import("ast.zig").ColumnType.integer }, .{ "bigint", arrays.ElementType.int64, @import("ast.zig").ColumnType.integer }, .{ "real", arrays.ElementType.float32, @import("ast.zig").ColumnType.number }, .{ "double precision", arrays.ElementType.float64, @import("ast.zig").ColumnType.number }, .{ "boolean", arrays.ElementType.boolean, @import("ast.zig").ColumnType.boolean }, .{ "text", arrays.ElementType.text, @import("ast.zig").ColumnType.string }, .{ "uuid", arrays.ElementType.uuid, @import("ast.zig").ColumnType.uuid }, .{ "jsonb", arrays.ElementType.jsonb, @import("ast.zig").ColumnType.json } };
    inline for (kinds) |kind| if (std.mem.eql(u8, base, kind[0])) return .{ .kind = if (array) .array else kind[2], .element_type = kind[1] };
    return error.UnexpectedReferenceType;
}

test "SQL prepared parameter frames match PostgreSQL descriptors values and diagnostics" {
    const Entry = struct { sql: []const u8, types: []const []const u8, oids: []const u32, values: []const ?[]const u8, value: std.json.Value };
    const Failure = struct { sql: []const u8, types: []const []const u8, values: []const ?[]const u8, code: []const u8 };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, entries: []const Entry, errors: []const Failure }, std.testing.allocator, @embedFile("fixtures/sql_parameter_frame_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 24), fixture.value.entries.len);
    try std.testing.expectEqual(@as(usize, 9), fixture.value.errors.len);
    const Contract = struct {
        fn run(a: A, sql: []const u8, names: []const []const u8, values: []const ?[]const u8) !scalar.Datum {
            const types = try a.alloc(scalar.Type, names.len);
            const inputs = try a.alloc(Input, values.len);
            for (names, types, values, inputs) |name, *descriptor, value, *input| {
                descriptor.* = try referenceType(name);
                input.* = if (value) |text_value| .{ .text = text_value } else .sql_null;
            }
            var compiled = try @import("compiler.zig").compileScalar(a, sql, .{});
            defer compiled.deinit();
            var program = try scalar.bindTyped(a, compiled.expression, &.{}, types, .{});
            defer program.deinit();
            var frame = try Frame.prepare(a, program.parameter_descriptors, inputs, .{});
            defer frame.deinit();
            const result = try (try program.bindParameters(&frame)).evaluate(a, &.{}, .{});
            return operators.cloneDatum(a, result);
        }
    };
    for (fixture.value.entries) |entry| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        for (entry.types, entry.oids) |name, oid| {
            const descriptor = try referenceType(name);
            const actual = if (descriptor.kind == .datetime) 1184 else if (descriptor.kind == .array) descriptor.element_type.?.arrayOid() else descriptor.element_type.?.oid();
            try std.testing.expectEqual(oid, actual);
        }
        const result = try Contract.run(arena.allocator(), entry.sql, entry.types, entry.values);
        try std.testing.expectEqual(entry.value == .null, result.sql_null);
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(entry.value, result.value));
    }
    for (fixture.value.errors) |entry| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        _ = Contract.run(arena.allocator(), entry.sql, entry.types, entry.values) catch |err| {
            try std.testing.expectEqualStrings(entry.code, @import("errors.zig").describe(err).code);
            continue;
        };
        return error.ExpectedPostgresRejection;
    }
}
pub const Limits = struct {
    bytes: usize = 8 * 1024 * 1024,
    wire_bytes: usize = 8 * 1024 * 1024,
    work: usize = 1_048_576,
    elements: usize = 65_536,
};

test "SQL prepared parameter frames bound admission and unwind every allocation fault" {
    const Faults = struct {
        fn run(backing: A) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            var frame = try Frame.prepare(a, &.{ .{ .kind = .array, .element_type = .jsonb }, .{ .kind = .array, .element_type = .text }, .{ .kind = .integer, .element_type = .int64 } }, &.{ .{ .text = "{\"[1,{\\\"a\\\":2}]\",NULL}" }, .{ .text = "{\"é,quoted\",NULL}" }, .{ .text = "9007199254740993" } }, .{});
            defer frame.deinit();
            const json = frame.values[0].array.?.elements[0].value.array;
            try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(json.allocator.ptr));
            try std.testing.expect(frame.values[0].array.?.elements[1].sql_null);
            try std.testing.expectEqual(@as(i64, 9007199254740993), frame.values[2].value.integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    const types: []const scalar.Type = &.{.{ .kind = .array, .element_type = .int32 }};
    const input: []const Input = &.{.{ .text = "{1,2}" }};
    try std.testing.expectError(error.InvalidSqlParameters, Frame.prepare(std.testing.allocator, types, &.{}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(std.testing.allocator, types, input, .{ .bytes = 1 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(std.testing.allocator, types, input, .{ .wire_bytes = 1 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(std.testing.allocator, types, input, .{ .work = 1 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(std.testing.allocator, types, input, .{ .elements = 1 }));
    const json_array = scalar.Datum.json(.{ .array = std.array_list.Managed(std.json.Value).init(std.testing.allocator) });
    try std.testing.expectError(error.SqlTypeMismatch, Frame.prepare(std.testing.allocator, types, &.{.{ .datum = json_array }}, .{}));
    var json_null = try Frame.prepare(std.testing.allocator, &.{ .{ .kind = .json, .element_type = .jsonb }, .{ .kind = .json, .element_type = .jsonb } }, &.{ .{ .datum = scalar.Datum.json(.null) }, .sql_null }, .{});
    defer json_null.deinit();
    try std.testing.expect(!json_null.values[0].sql_null);
    try std.testing.expect(json_null.values[1].sql_null);
}

test "SQL prepared binary array frames reuse PostgreSQL codecs and stable JSON owners" {
    const Entry = struct { sql: []const u8, element_type: arrays.ElementType, array_oid: u32, binary: []const u8, native_binary: ?[]const u8 = null };
    const fixture = try std.json.parseFromSlice(struct { reference: []const u8, scope: []const u8, entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_array_binary_reference.json"), .{});
    defer fixture.deinit();
    const Faults = struct {
        fn run(backing: A, kind: arrays.ElementType, wire: []const u8, scalar_input: bool) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            var frame = try Frame.prepare(.{ .ptr = backing.ptr, .vtable = &vtable }, &.{.{ .kind = if (scalar_input) scalarKind(kind) else .array, .element_type = kind }}, &.{.{ .binary = wire }}, .{});
            defer frame.deinit();
            if (!scalar_input) try std.testing.expectEqual(kind, frame.values[0].array.?.element_type);
        }
    };
    var scalar_kinds: [9]bool = @splat(false);
    for (fixture.value.entries) |entry| {
        const wire = try std.testing.allocator.alloc(u8, entry.binary.len / 2);
        defer std.testing.allocator.free(wire);
        _ = try std.fmt.hexToBytes(wire, entry.binary);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{ entry.element_type, wire, false });
        var expected = try binary.decode(std.testing.allocator, entry.element_type, wire, .{});
        defer expected.deinit();
        var frame = try Frame.prepare(std.testing.allocator, &.{.{ .kind = .array, .element_type = entry.element_type }}, &.{.{ .binary = wire }}, .{});
        defer frame.deinit();
        var work: arrays.Budget = .{};
        try std.testing.expectEqual(try expected.value.semanticHash(&work), try frame.values[0].array.?.semanticHash(&work));
        if (entry.element_type == .jsonb) for (frame.values[0].array.?.elements) |element| {
            if (element.value == .array) try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(element.value.array.allocator.ptr));
        };
        const rank = std.mem.readInt(u32, wire[0..4], .big);
        var at: usize = 12 + 8 * @as(usize, rank);
        for (expected.value.elements) |element| {
            const length = std.mem.readInt(i32, wire[at..][0..4], .big);
            at += 4;
            if (length == -1) continue;
            const payload = wire[at..][0..@intCast(length)];
            try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{ entry.element_type, payload, true });
            var scalar_frame = try Frame.prepare(std.testing.allocator, &.{.{ .kind = scalarKind(entry.element_type), .element_type = entry.element_type }}, &.{.{ .binary = payload }}, .{});
            defer scalar_frame.deinit();
            const actual = try arrays.Value.init(entry.element_type, &.{.{ .length = 1 }}, scalar_frame.values, .{});
            const reference = try arrays.Value.init(entry.element_type, &.{.{ .length = 1 }}, &.{element}, .{});
            try std.testing.expectEqual(std.math.Order.eq, try actual.compare(reference, &work));
            scalar_kinds[@backingInt(entry.element_type)] = true;
            break;
        }
    }
    for (scalar_kinds) |covered| try std.testing.expect(covered);
}

fn scalarKind(kind: arrays.ElementType) @import("ast.zig").ColumnType {
    return switch (kind) {
        .int16, .int32, .int64 => .integer,
        .float32, .float64 => .number,
        .text => .string,
        .boolean => .boolean,
        .uuid => .uuid,
        .jsonb => .json,
    };
}

test "SQL JSON array allocator references survive program and codec owner moves" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "cardinality(ARRAY['[1,2]'::jsonb])", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var constants = program.constant_arrays.valueIterator();
    const cached = constants.next().?.*;
    try std.testing.expectEqual(@intFromPtr(program.arena), @intFromPtr(cached.elements[0].value.array.allocator.ptr));
    var owned = try text.decode(a, .jsonb, "{\"[1,2]\"}", .{});
    defer owned.deinit();
    try std.testing.expectEqual(@intFromPtr(owned.arena), @intFromPtr(owned.value.elements[0].value.array.allocator.ptr));
    var cloned = try arrays.Owned.init(a, owned.value, .{});
    defer cloned.deinit();
    try std.testing.expectEqual(@intFromPtr(cloned.arena), @intFromPtr(cloned.value.elements[0].value.array.allocator.ptr));
    var frame = try Frame.prepare(a, &.{.{ .kind = .json }}, &.{.{ .datum = owned.value.elements[0] }}, .{});
    defer frame.deinit();
    try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(frame.values[0].value.array.allocator.ptr));
    try std.testing.expectEqual(@as(?arrays.ElementType, .jsonb), frame.descriptors[0].element_type);
}

pub const Frame = struct {
    arena: *std.heap.ArenaAllocator,
    budget: *MemoryBudget,
    descriptors: []const scalar.Type,
    values: []const scalar.Datum,
    work: usize,

    pub fn deinit(self: *Frame) void {
        const backing = self.budget.backing;
        self.arena.deinit();
        self.budget.allocator().destroy(self.arena);
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        self.* = undefined;
    }

    pub fn prepare(backing: A, descriptors: []const scalar.Type, inputs: []const Input, limits: Limits) !Frame {
        if (inputs.len != descriptors.len or inputs.len > 1024) return error.InvalidSqlParameters;
        for (descriptors) |descriptor| {
            try scalar.validateParameterType(descriptor);
            if (descriptor.kind == null) return error.UnknownSqlParameterType;
        }
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes };
        const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
        errdefer budget.allocator().destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(budget.allocator());
        errdefer arena.deinit();
        const decoded = prepareLeaky(arena.allocator(), descriptors, inputs, limits) catch |err| return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
        return .{ .arena = arena, .budget = budget, .descriptors = decoded.descriptors, .values = decoded.values, .work = decoded.work };
    }
};

fn prepareLeaky(a: A, descriptors: []const scalar.Type, inputs: []const Input, limits: Limits) !struct { descriptors: []const scalar.Type, values: []const scalar.Datum, work: usize } {
    var work: arrays.Budget = .{ .remaining = limits.work };
    var wire_bytes: usize = 0;
    for (inputs) |input| {
        const size = switch (input) {
            .text, .binary => |bytes| bytes.len,
            else => 0,
        };
        if (size > limits.wire_bytes -| wire_bytes) return error.SqlProgramLimitExceeded;
        wire_bytes += size;
    }
    try work.consume(inputs.len);
    const types = try a.dupe(scalar.Type, descriptors);
    for (types) |*descriptor| if (descriptor.kind != .datetime and descriptor.element_type == null) {
        descriptor.element_type = try scalar.parameterElementType(descriptor.*);
    };
    const values = try a.alloc(scalar.Datum, inputs.len);
    for (types, inputs, values) |descriptor, input, *value| {
        value.* = try decodeInput(a, descriptor, input, limits, &work);
    }
    return .{ .descriptors = types, .values = values, .work = limits.work - work.remaining };
}

fn decodeInput(a: A, descriptor: scalar.Type, input: Input, limits: Limits, work: *arrays.Budget) !scalar.Datum {
    if (input == .datum and input.datum.patterns != null) return error.SqlTypeMismatch;
    if (input == .sql_null or (input == .datum and input.datum.sql_null)) {
        if (!descriptor.nullable) return error.InvalidSqlParameters;
        if (input == .datum and (input.datum.array != null or input.datum.patterns != null or input.datum.value != .null)) return error.SqlTypeMismatch;
        return .{};
    }
    if (descriptor.kind == .datetime) {
        if (input == .binary) return error.UnsupportedSqlShape;
        const raw = if (input == .text) input.text else if (input.datum.array == null and input.datum.value == .string) input.datum.value.string else return error.SqlTypeMismatch;
        if (!std.unicode.utf8ValidateSlice(raw) or std.mem.indexOfScalar(u8, raw, 0) != null) return error.SqlInvalidTextEncoding;
        try work.consume(raw.len);
        const datetime = @import("../datetime.zig");
        const ns = datetime.parseDateTimeToNs(raw) orelse return error.SqlInvalidDateTime;
        return scalar.Datum.json(.{ .string = try datetime.formatDateTimeNsAlloc(a, ns) });
    }
    const kind = try scalar.parameterElementType(descriptor);
    if (descriptor.kind == .array) {
        if (input == .datum) {
            const value = input.datum;
            const array = value.array orelse return error.SqlTypeMismatch;
            if (array.element_type != kind) return error.ConflictingSqlParameterTypes;
            if (value.patterns != null or value.value != .null) return error.SqlTypeMismatch;
            _ = try arrays.Value.initWithBudget(kind, array.dimensions, array.elements, .{ .bytes = limits.bytes, .elements = limits.elements }, work);
            return operators.cloneDatum(a, value);
        }
        const value = try a.create(arrays.Value);
        const value_limits: arrays.Limits = .{ .bytes = limits.bytes, .elements = limits.elements, .work = work.remaining };
        if (input == .text) {
            const decoded = try text.decodeLeaky(a, kind, input.text, .{ .values = value_limits, .wire_bytes = limits.wire_bytes });
            try work.consume(decoded.work);
            value.* = decoded.value;
        } else {
            const decoded = try binary.decodeLeaky(a, kind, input.binary, .{ .values = value_limits, .wire_bytes = limits.wire_bytes });
            try work.consume(decoded.work);
            value.* = decoded.value;
        }
        return scalar.Datum.typedArray(value);
    }
    const value = switch (input) {
        .text => |bytes| try text.decodeElementLeaky(a, kind, bytes, work),
        .binary => |bytes| try binary.decodeElementLeaky(a, kind, bytes, work),
        .datum => |datum| {
            if (datum.array != null or datum.patterns != null) return error.SqlTypeMismatch;
            var owned = datum;
            // These declared scalar types normalize representation once.
            if (kind == .uuid and datum.value == .string) {
                owned = try text.decodeElementLeaky(a, .uuid, datum.value.string, work);
                _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{owned}, .{ .bytes = limits.bytes }, work);
                return owned;
            } else if (kind == .float32 or kind == .float64) {
                if (datum.value != .integer and datum.value != .float) return error.SqlTypeMismatch;
                owned.value = .{ .float = if (kind == .float32) try @import("builtin_cast.zig").floatValue(f32, datum.value) else try @import("builtin_cast.zig").floatValue(f64, datum.value) };
            }
            _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{owned}, .{ .bytes = limits.bytes }, work);
            return operators.cloneDatum(a, owned);
        },
        .sql_null => unreachable,
    };
    _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{value}, .{ .bytes = limits.bytes }, work);
    return value;
}
