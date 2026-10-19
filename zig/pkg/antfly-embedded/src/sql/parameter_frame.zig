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

test "SQL exact NUMERIC frames arrays and constant casts retain typed ownership" {
    const a = std.testing.allocator;
    const compiler = @import("compiler.zig");
    const numeric = @import("numeric_value.zig");
    var compiled = try compiler.compileScalar(a, "'9007199254740993.1200'::numeric", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{}, .{});
    defer program.deinit();
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const constant = try program.evaluate(none.allocator(), &.{}, &.{}, .{});
    try std.testing.expect(constant.numeric != null and !constant.sql_null);
    var context: numeric.Context = .{ .alloc = a };
    const display = try numeric.format(&context, constant.numeric.?.*);
    defer a.free(display);
    try std.testing.expectEqualStrings("9007199254740993.1200", display);
    var frame = try Frame.prepare(a, &.{ .{ .kind = .number, .element_type = .numeric }, .{ .kind = .array, .element_type = .numeric } }, &.{ .{ .text = "9007199254740993.1200" }, .{ .text = "[-1:1]={9007199254740993.1200,NULL,NaN}" } }, .{});
    defer frame.deinit();
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(constant, frame.values[0]));
    const array = frame.values[1].array.?;
    try std.testing.expectEqual(@as(i32, -1), array.dimensions[0].lower);
    const storage = @import("array_storage.zig");
    const bytes = try storage.encodeAlloc(a, array.*, .{});
    defer a.free(bytes);
    var restored = try storage.decode(a, .numeric, bytes, .{});
    defer restored.deinit();
    var work: arrays.Budget = .{};
    try std.testing.expectEqual(std.math.Order.eq, try array.compare(restored.value, &work));
    const envelope = try @import("array_wire.zig").encodeAlloc(a, restored.value, .{});
    defer a.free(envelope);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, envelope, .{});
    defer parsed.deinit();
    var wire = try @import("array_wire.zig").decode(a, .numeric, parsed.value, .{});
    defer wire.deinit();
    try std.testing.expectEqual(std.math.Order.eq, try array.compare(wire.value, &work));
}

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
    try std.testing.expect(array.flatCell(3).sql_null);
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
    try std.testing.expectEqual(@as(i64, 2), result.array.?.flatCell(0).value.integer);
    try std.testing.expectEqual(@as(i64, 2), result.array.?.flatCell(1).value.integer);
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
    /// Lossless public array envelope, admitted only with a declared array
    /// descriptor. Plain JSON arrays never acquire SQL-array semantics.
    array_envelope: std.json.Value,
};

test "SQL statement frames compose independent programs without constraining unused slots" {
    const a = std.testing.allocator;
    var left_sql = try @import("compiler.zig").compileScalar(a, "$1::integer", .{});
    defer left_sql.deinit();
    var left = try scalar.bindTyped(a, left_sql.expression, &.{}, &.{}, .{});
    defer left.deinit();
    var right_sql = try @import("compiler.zig").compileScalar(a, "cardinality($2::bigint[])", .{});
    defer right_sql.deinit();
    var right = try scalar.bindTyped(a, right_sql.expression, &.{}, &.{}, .{});
    defer right.deinit();
    var constant_sql = try @import("compiler.zig").compileScalar(a, "1", .{});
    defer constant_sql.deinit();
    var constant = try scalar.bindTyped(a, constant_sql.expression, &.{}, &.{}, .{});
    defer constant.deinit();
    var frame = try Frame.prepareJson(a, &.{ .{ .kind = .integer, .element_type = .int32 }, .{ .kind = .array, .element_type = .int64 } }, &.{ .{ .string = "42" }, .{ .string = "[-1:1]={9007199254740993,NULL,2}" } }, .{});
    defer frame.deinit();
    const bound_left = try left.bindParameters(&frame);
    const bound_right = try right.bindParameters(&frame);
    const bound_constant = try constant.bindParameters(&frame);
    var none: std.heap.FixedBufferAllocator = .init(&.{});
    for (0..10000) |_| {
        try std.testing.expectEqual(@as(i64, 42), (try bound_left.evaluate(none.allocator(), &.{}, .{})).value.integer);
        try std.testing.expectEqual(@as(i64, 3), (try bound_right.evaluate(none.allocator(), &.{}, .{})).value.integer);
        try std.testing.expectEqual(@as(i64, 1), (try bound_constant.evaluate(none.allocator(), &.{}, .{})).value.integer);
    }
    var wrong = try Frame.prepareJson(a, &.{ .{ .kind = .integer, .element_type = .int64 }, .{ .kind = .array, .element_type = .int32 } }, &.{ .{ .string = "42" }, .{ .string = "{1,2}" } }, .{});
    defer wrong.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, left.bindParameters(&wrong));
    try std.testing.expectError(error.ConflictingSqlParameterTypes, right.bindParameters(&wrong));
}

test "SQL JSON parameter ingress owns typed envelopes and shares wire and work admission" {
    const a = std.testing.allocator;
    const descriptor = scalar.Type{ .kind = .array, .element_type = .jsonb };
    var source = try text.decode(a, .jsonb, "[0:2]={\"null\",NULL,\"{\\\"x\\\":[1,2]}\"}", .{});
    defer source.deinit();
    var input_owner: std.heap.ArenaAllocator = .init(a);
    const envelope = try @import("array_wire.zig").toJsonLeaky(input_owner.allocator(), source.value, .{});
    const encoded = try std.json.Stringify.valueAlloc(a, envelope, .{});
    defer a.free(encoded);
    var frame = try Frame.prepareJson(a, &.{descriptor}, &.{envelope}, .{ .wire_bytes = encoded.len });
    defer frame.deinit();
    input_owner.deinit();
    const value = frame.values[0].array.?;
    try std.testing.expectEqual(@as(i32, 0), value.dimensions[0].lower);
    try std.testing.expect(!value.flatCell(0).sql_null and value.flatCell(0).value == .null);
    try std.testing.expect(value.flatCell(1).sql_null);
    try std.testing.expectEqualStrings("2", value.flatCell(2).value.object.get("x").?.array.items[1].number_string);
    const Faults = struct {
        fn run(backing: A, input: std.json.Value) !void {
            var owner = try Frame.prepareJson(backing, &.{descriptor}, &.{input}, .{});
            defer owner.deinit();
        }
    };
    var retained: std.heap.ArenaAllocator = .init(a);
    defer retained.deinit();
    const input = try @import("array_wire.zig").toJsonLeaky(retained.allocator(), source.value, .{});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{input});
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepareJson(a, &.{ descriptor, descriptor }, &.{ input, input }, .{ .wire_bytes = encoded.len * 2 - 1 }));
    const measured = try @import("array_wire.zig").decodeLeakyMeasured(retained.allocator(), .jsonb, input, .{});
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepareJson(a, &.{ descriptor, descriptor }, &.{ input, input }, .{ .work = measured.work * 2 + 1 }));
    var json = try Frame.prepareJson(a, &.{.{ .kind = .json }}, &.{.{ .string = "null" }}, .{});
    defer json.deinit();
    try std.testing.expectEqualStrings("null", json.values[0].value.string);
    var empty_json_array = std.json.Array.init(a);
    defer empty_json_array.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, Frame.prepareJson(a, &.{descriptor}, &.{.{ .array = empty_json_array }}, .{}));
    try std.testing.expectError(error.SqlTypeMismatch, Frame.prepareJson(a, &.{.{ .kind = .string }}, &.{.{ .number_string = "123" }}, .{}));
}

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
/// Immutable ingress payload ownership. A frame may borrow compact arrays
/// only with an explicit lease, charged against its retained-memory budget.
pub const PayloadLease = struct {
    ptr: *anyopaque,
    bytes: usize,
    retain_fn: *const fn (*anyopaque) void,
    release_fn: *const fn (*anyopaque) void,
    pub fn retain(self: PayloadLease) void {
        self.retain_fn(self.ptr);
    }
    pub fn release(self: PayloadLease) void {
        self.release_fn(self.ptr);
    }
};

/// Stable arena identity for transport decoding, independent of portal moves.
/// After publishing lease(), callers must treat this region as immutable.
pub const Ingress = struct {
    alloc: A,
    budget: MemoryBudget,
    arena: std.heap.ArenaAllocator,
    refs: std.atomic.Value(usize) = .init(1),
    pub fn create(a: A, bytes: usize) !*Ingress {
        const self = try a.create(Ingress);
        self.* = .{ .alloc = a, .budget = .{ .backing = a, .limit = bytes }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    pub fn allocator(self: *Ingress) A {
        return self.arena.allocator();
    }
    pub fn lease(self: *Ingress) PayloadLease {
        return .{ .ptr = self, .bytes = self.budget.live, .retain_fn = retainRaw, .release_fn = releaseRaw };
    }
    fn retainRaw(raw: *anyopaque) void {
        const self: *Ingress = @ptrCast(@alignCast(raw));
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    fn releaseRaw(raw: *anyopaque) void {
        const self: *Ingress = @ptrCast(@alignCast(raw));
        self.release();
    }
    pub fn release(self: *Ingress) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        self.alloc.destroy(self);
    }
};

pub const Limits = struct {
    payload_owner: ?PayloadLease = null,
    error_context: ?*@import("errors.zig").Context = null,
    bytes: usize = @import("resource_limits.zig").default_memory_bytes,
    wire_bytes: usize = @import("resource_limits.zig").request_bytes,
    work: usize = @import("resource_limits.zig").default_memory_bytes,
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
            const json = frame.values[0].array.?.flatCell(0).value.array;
            try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(json.allocator.ptr));
            try std.testing.expect(frame.values[0].array.?.flatCell(1).sql_null);
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
        if (entry.element_type == .jsonb) for (0..frame.values[0].array.?.cardinality()) |array_cell_0| {
            const element = frame.values[0].array.?.flatCell(array_cell_0);
            if (element.value == .array) try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(element.value.array.allocator.ptr));
        };
        const rank = std.mem.readInt(u32, wire[0..4], .big);
        var at: usize = 12 + 8 * @as(usize, rank);
        for (0..expected.value.cardinality()) |array_cell_0| {
            const element = expected.value.flatCell(array_cell_0);
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
        .float32, .float64, .numeric => .number,
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
    const allocator_ptr = cached.flatCell(0).value.array.allocator.ptr;
    const pinned_owner = for (program.constant_pool.?.regions.items) |region| {
        if (@intFromPtr(region) == @intFromPtr(allocator_ptr)) break region;
    } else return error.UnpinnedConstantAllocator;
    try std.testing.expectEqual(@intFromPtr(pinned_owner), @intFromPtr(allocator_ptr));
    var moved_program = program;
    var moved_constants = moved_program.constant_arrays.valueIterator();
    const moved_cached = moved_constants.next().?.*;
    const moved_allocator = moved_cached.flatCell(0).value.array.allocator;
    const probe = try moved_allocator.alloc(u8, 256);
    defer moved_allocator.free(probe);
    @memset(probe, 7);
    try std.testing.expectEqual(@as(u8, 7), probe[255]);
    try std.testing.expectEqual(@as(usize, 2), moved_cached.flatCell(0).value.array.items.len);
    var owned = try text.decode(a, .jsonb, "{\"[1,2]\"}", .{});
    defer owned.deinit();
    try std.testing.expectEqual(@intFromPtr(owned.arena), @intFromPtr(owned.value.flatCell(0).value.array.allocator.ptr));
    var cloned = try arrays.Owned.init(a, owned.value, .{});
    defer cloned.deinit();
    try std.testing.expectEqual(@intFromPtr(cloned.arena), @intFromPtr(cloned.value.flatCell(0).value.array.allocator.ptr));
    var frame = try Frame.prepare(a, &.{.{ .kind = .json }}, &.{.{ .datum = owned.value.flatCell(0) }}, .{});
    defer frame.deinit();
    try std.testing.expectEqual(@intFromPtr(frame.arena), @intFromPtr(frame.values[0].value.array.allocator.ptr));
    try std.testing.expectEqual(@as(?arrays.ElementType, .jsonb), frame.descriptors[0].element_type);
}

test "SQL datetime parameters preserve signed epoch instants across JSON and text ingress" {
    const Run = struct {
        fn run(a: A) !void {
            for ([_][]const u8{ "1969-12-31T23:59:59.999Z", "1970-01-01T00:59:59.999+01:00" }) |input| {
                var json = try Frame.prepareJson(a, &.{.{ .kind = .datetime }}, &.{.{ .string = input }}, .{});
                defer json.deinit();
                var text_frame = try Frame.prepare(a, &.{.{ .kind = .datetime }}, &.{.{ .text = input }}, .{});
                defer text_frame.deinit();
                try std.testing.expectEqualStrings("1969-12-31T23:59:59.999000000Z", json.values[0].value.string);
                try std.testing.expectEqualStrings(json.values[0].value.string, text_frame.values[0].value.string);
            }
        }
    };
    try Run.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

/// Frame-owned acceleration shared by all programs bound to one immutable
/// execution. Warm reads are lock-free; cold preparation is serialized. Only
/// used parameters warm up, and unsuccessful speculative builds are disabled.
const MembershipCache = struct {
    const Slot = struct {
        state: std.atomic.Value(u8) = .init(0), // cold / disabled / ready / eligible
        recent_scans: usize = 0,
        recent_cells: usize = 0,
        build_credit: usize = 0,
        build_work: usize = 0,
        index: ?*arrays.Membership = null,
    };
    backing: A,
    memory: MemoryBudget,
    slots: []Slot,
    /// Only admission-complete frame storage is registered here. The cache,
    /// values and payload lease share one execution lifetime.
    values: []const scalar.Datum,
    remaining: usize,
    mutex: std.atomic.Mutex = .unlocked,

    fn init(backing: A, values: []const scalar.Datum, work: usize) !?*MembershipCache {
        for (values) |value| {
            if (value.array) |array| if (array.cardinality() >= 32 and array.element_type != .numeric) break;
        } else return null;
        const available = MemoryBudget.headroom(backing) orelse return null;
        if (available < 64 * 1024 or work == 0) return null;
        const self = try backing.create(MembershipCache);
        errdefer backing.destroy(self);
        const slots = try backing.alloc(Slot, values.len);
        errdefer backing.free(slots);
        for (slots, values) |*slot, value| {
            slot.* = .{};
            const eligible = if (value.array) |array| array.cardinality() >= 32 and array.element_type != .numeric else false;
            if (!eligible) {
                slot.state.store(1, .release);
            } else {
                const array = value.array.?;
                slot.build_work = array.cardinality() *| 8;
                if (array.element_type == .text or array.element_type == .jsonb or array.element_type == .uuid)
                    slot.build_work = @max(slot.build_work, (try array.retainedBytes()) *| 2);
            }
        }
        self.* = .{ .backing = backing, .memory = .{ .backing = backing, .limit = @min(2 * 1024 * 1024, available / 4) }, .slots = slots, .values = values, .remaining = @min(work, 1024 * 1024) };
        return self;
    }
    /// Only this admission-complete registry mints array capabilities. The
    /// opaque pointer needs no additional object or per-element metadata.
    fn arraySource(self: *const MembershipCache, ordinal: usize) ?*const Frame.ArraySource {
        if (ordinal >= self.values.len) return null;
        const value = self.values[ordinal].array orelse return null;
        return @ptrCast(value);
    }

    fn get(self: *MembershipCache, ordinal: usize, caller: *arrays.Budget) !?*const arrays.Membership {
        if (ordinal >= self.slots.len) return null;
        const slot = &self.slots[ordinal];
        switch (slot.state.load(.acquire)) {
            0, 1 => return null,
            2 => return slot.index,
            else => {},
        }
        // Other evaluations retain their scan path while preparation is in
        // flight. Never wait under a row checkpoint or on a starved worker.
        try caller.poll();
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        switch (slot.state.load(.acquire)) {
            0, 1 => return null,
            2 => return slot.index,
            else => {},
        }
        const available = MemoryBudget.headroom(self.memory.allocator()).?;
        if (available < 4096 or self.remaining == 0 or self.memory.isExhausted()) {
            slot.state.store(1, .release);
            return null;
        }
        const Poll = struct {
            fn check(raw: ?*anyopaque) !void {
                const owner: *arrays.Budget = @ptrCast(@alignCast(raw.?));
                try owner.poll();
            }
        };
        // Build work is bounded once per execution, independently of row work.
        // Cancellation still belongs to the evaluating row and stays sticky.
        var context: @import("numeric_value.zig").Context = .{ .alloc = self.memory.allocator(), .remaining = self.remaining, .checkpoint = Poll.check, .ptr = caller };
        var work_budget: arrays.Budget = .{ .remaining = self.remaining, .shared = &context };
        defer self.remaining = work_budget.remaining;
        const index = self.memory.allocator().create(arrays.Membership) catch |err| {
            if (err == error.OutOfMemory and self.memory.isExhausted()) {
                slot.state.store(1, .release);
                return null;
            }
            return err;
        };
        errdefer self.memory.allocator().destroy(index);
        index.* = arrays.Membership.initFrameOwnedWithBudget(self.memory.allocator(), self.arraySource(ordinal).?, available - @sizeOf(arrays.Membership) - @sizeOf(MemoryBudget), &work_budget) catch |err| {
            if (err == error.SqlProgramLimitExceeded or (err == error.OutOfMemory and self.memory.isExhausted())) {
                self.memory.allocator().destroy(index);
                slot.state.store(1, .release);
                return null;
            }
            return err;
        };
        slot.index = index;
        slot.state.store(2, .release);
        return index;
    }
    fn observe(self: *MembershipCache, ordinal: usize, spent: usize, visited: usize) void {
        if (ordinal >= self.slots.len) return;
        const slot = &self.slots[ordinal];
        if (slot.state.load(.acquire) != 0 or !self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        if (slot.state.load(.acquire) != 0) return;
        // Keep the useful-prefix estimate recent: halve its weight every 32
        // observations once full, so arbitrarily old early hits cannot delay
        // a new expensive phase. No per-observation storage is required.
        if (slot.recent_scans == 64) {
            slot.recent_scans /= 2;
            // Round upward so a one-cell residual from an old cheap phase
            // eventually disappears at the exact useful-prefix threshold.
            slot.recent_cells = slot.recent_cells / 2 + slot.recent_cells % 2;
        }
        slot.recent_scans += 1;
        slot.recent_cells +|= visited;
        // Only substantial scans can repay construction. Credit is capped,
        // rather than decayed, so large sources with moderate prefix lengths
        // can still amortize a build over more than one history window.
        if (visited >= 16) slot.build_credit = @min(slot.build_work, slot.build_credit +| spent);
        // Publish eligibility once; cold readers avoid the build lock and
        // headroom checks. Observations remain best-effort under contention.
        if (slot.recent_scans >= 16 and slot.recent_cells / slot.recent_scans >= 16 and slot.build_credit >= slot.build_work)
            slot.state.store(3, .release);
    }
    fn deinit(self: *MembershipCache) void {
        for (self.slots) |slot| if (slot.index) |index| {
            index.deinit();
            self.memory.allocator().destroy(index);
        };
        std.debug.assert(self.memory.live == 0);
        const backing = self.backing;
        backing.free(self.slots);
        backing.destroy(self);
    }
};

pub const Frame = struct {
    memberships: ?*MembershipCache = null,
    payload_owner: ?PayloadLease = null,
    arena: *std.heap.ArenaAllocator,
    budget: *MemoryBudget,
    descriptors: []const scalar.Type,
    values: []const scalar.Datum,
    work: usize,

    /// JSON API ingress is type-directed, not shape-inferred. Exact scalar
    /// text is decoded once; JSON strings remain JSON data for JSON columns.
    /// Outer null follows the existing API's SQL NULL parameter convention.
    pub fn prepareJson(backing: A, descriptors: []const scalar.Type, values: []const std.json.Value, limits: Limits) !Frame {
        if (descriptors.len != values.len or values.len > 1024) return error.InvalidSqlParameters;
        var inputs: [1024]Input = undefined;
        for (descriptors, values, inputs[0..values.len]) |descriptor, value, *input| {
            input.* = if (value == .null) .sql_null else if (descriptor.kind == .array) switch (value) {
                .string => |bytes| .{ .text = bytes },
                .object => .{ .array_envelope = value },
                else => return error.SqlTypeMismatch,
            } else if (descriptor.kind != .json and (value == .string or (value == .number_string and (descriptor.kind == .integer or descriptor.kind == .number))))
                .{ .text = if (value == .string) value.string else value.number_string }
            else
                .{ .datum = scalar.Datum.json(value) };
        }
        return prepare(backing, descriptors, inputs[0..values.len], limits);
    }

    pub fn deinit(self: *Frame) void {
        const backing = self.budget.backing;
        if (self.memberships) |cache| cache.deinit();
        self.arena.deinit();
        self.budget.allocator().destroy(self.arena);
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        if (self.payload_owner) |owner| owner.release();
        self.* = undefined;
    }

    /// Opaque capability minted only from admission-complete frame storage.
    /// The immutable frame and payload lease must outlive the capability and
    /// any index borrowing it. Casting arbitrary Values to this type violates
    /// the contract; callers must retain ordinary ingress validation.
    pub const ArraySource = opaque {
        pub fn borrowValidated(self: *const ArraySource) arrays.Value {
            const value: *const arrays.Value = @ptrCast(@alignCast(self));
            return value.*;
        }
    };

    /// Borrows stable execution storage, never the movable Frame wrapper.
    pub const Lookup = struct {
        cache: ?*MembershipCache = null,
        pub fn equalAny(self: Lookup, ordinal: usize, array: *const arrays.Value, values: []const scalar.Datum, element: arrays.Element, work: *arrays.Budget) !?bool {
            // The cache registry is the sole binding authority. A copied
            // evaluator with replaced parameters keeps its scan path without
            // consulting or warming the original frame's indexes.
            const cache: ?*MembershipCache = if (!element.sql_null) bound: {
                const owner = self.cache orelse break :bound null;
                if (values.ptr != owner.values.ptr or values.len != owner.values.len or ordinal >= values.len or values[ordinal].array != array) break :bound null;
                break :bound owner;
            } else null;
            if (cache) |owner| if (try owner.get(ordinal, work)) |index| return index.equalAny(element, work);
            const before = work.remaining;
            const observed = try array.quantifiedObserved(element, .eq, .any, work);
            if (cache) |owner| owner.observe(ordinal, before -| work.remaining, observed.visited);
            return observed.result;
        }
    };
    pub fn lookup(self: *const Frame) Lookup {
        return .{ .cache = self.memberships };
    }
    pub fn equalAny(self: *const Frame, ordinal: usize, array: *const arrays.Value, element: arrays.Element, work: *arrays.Budget) !?bool {
        return self.lookup().equalAny(ordinal, array, self.values, element, work);
    }

    pub fn prepare(backing: A, descriptors: []const scalar.Type, inputs: []const Input, limits: Limits) !Frame {
        if (inputs.len != descriptors.len or inputs.len > 1024) return error.InvalidSqlParameters;
        for (descriptors) |descriptor| {
            try scalar.validateParameterType(descriptor);
            if (descriptor.kind == null) return error.UnknownSqlParameterType;
        }
        const leased_bytes = if (limits.payload_owner) |owner| owner.bytes else 0;
        if (leased_bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes - leased_bytes };
        const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
        errdefer budget.allocator().destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(budget.allocator());
        errdefer arena.deinit();
        const decoded = prepareLeaky(arena.allocator(), descriptors, inputs, limits) catch |err| return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
        const memberships = try MembershipCache.init(MemoryBudget.reclaimingAllocator(budget.allocator()), decoded.values, limits.work -| decoded.work);
        if (limits.payload_owner) |owner| owner.retain();
        return .{ .memberships = memberships, .payload_owner = limits.payload_owner, .arena = arena, .budget = budget, .descriptors = decoded.descriptors, .values = decoded.values, .work = decoded.work };
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
    for (types, inputs, values, 1..) |descriptor, input, *value, index| {
        value.* = decodeInput(a, descriptor, input, limits, &work, &wire_bytes) catch |err| {
            if (err == error.SqlUntranslatableCharacter) if (limits.error_context) |context| {
                context.untranslatableParameter(index);
            };
            return err;
        };
    }
    return .{ .descriptors = types, .values = values, .work = limits.work - work.remaining };
}

fn decodeInput(a: A, descriptor: scalar.Type, input: Input, limits: Limits, work: *arrays.Budget, wire_bytes: *usize) !scalar.Datum {
    if (input == .datum and input.datum.patterns != null) return error.SqlTypeMismatch;
    if (input == .sql_null or (input == .datum and input.datum.sql_null)) {
        if (!descriptor.nullable) return error.InvalidSqlParameters;
        if (input == .datum and (input.datum.array != null or input.datum.numeric != null or input.datum.patterns != null or input.datum.value != .null)) return error.SqlTypeMismatch;
        return .{};
    }
    if (descriptor.kind == .datetime) {
        if (input == .array_envelope) return error.SqlTypeMismatch;
        if (input == .binary) return error.UnsupportedSqlShape;
        const raw = if (input == .text) input.text else if (input.datum.array == null and input.datum.value == .string) input.datum.value.string else return error.SqlTypeMismatch;
        if (!std.unicode.utf8ValidateSlice(raw) or std.mem.indexOfScalar(u8, raw, 0) != null) return error.SqlInvalidTextEncoding;
        try work.consume(raw.len);
        const datetime = @import("../datetime.zig");
        const ns = datetime.parseDateTimeToSignedNs(raw) orelse return error.SqlInvalidDateTime;
        return scalar.Datum.json(.{ .string = try datetime.formatDateTimeSignedNsAlloc(a, ns) });
    }
    const kind = try scalar.parameterElementType(descriptor);
    if (descriptor.kind == .array) {
        if (input == .datum) {
            const value = input.datum;
            const array = value.array orelse return error.SqlTypeMismatch;
            if (array.element_type != kind) return error.ConflictingSqlParameterTypes;
            if (value.patterns != null or value.numeric != null or value.value != .null) return error.SqlTypeMismatch;
            const admitted = try array.validateWithBudget(.{ .bytes = limits.bytes, .elements = limits.elements }, work);
            if (array.stored != null and limits.payload_owner != null) return value;
            // Clone the admitted header so caller-provided sizing metadata is
            // refreshed by the existing validation walk before publication.
            return operators.cloneDatum(a, .typedArray(&admitted));
        }
        const value = try a.create(arrays.Value);
        const value_limits: arrays.Limits = .{ .bytes = limits.bytes, .elements = limits.elements, .work = work.remaining };
        if (input == .text) {
            const decoded = try text.decodeLeaky(a, kind, input.text, .{ .values = value_limits, .wire_bytes = limits.wire_bytes });
            try work.consume(decoded.work);
            value.* = decoded.value;
        } else if (input == .binary) {
            const decoded = try binary.decodeLeaky(a, kind, input.binary, .{ .values = value_limits, .wire_bytes = limits.wire_bytes });
            try work.consume(decoded.work);
            value.* = decoded.value;
        } else {
            const decoded = try @import("array_wire.zig").decodeLeakyMeasured(a, kind, input.array_envelope, .{ .values = value_limits, .wire_bytes = limits.wire_bytes - wire_bytes.* });
            try work.consume(decoded.work);
            wire_bytes.* += decoded.wire_bytes;
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
            if (kind == .numeric and datum.numeric == null) {
                if (datum.value == .float) {
                    const exact = @import("numeric_value.zig");
                    const parsed = conversion: {
                        var context: exact.Context = .{ .alloc = a, .remaining = work.remaining, .max_output_bytes = limits.bytes };
                        const initial = context.remaining;
                        defer work.consume(initial - context.remaining) catch {};
                        break :conversion try exact.fromFloat(&context, datum.value.float, false);
                    };
                    const value = try a.create(exact.Value);
                    value.* = parsed.value;
                    owned = scalar.Datum.typedNumeric(value);
                    _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{owned}, .{ .bytes = limits.bytes }, work);
                    return owned;
                }
                var buffer: [20]u8 = undefined;
                const bytes = switch (datum.value) {
                    .integer => |integer| try std.fmt.bufPrint(&buffer, "{d}", .{integer}),
                    .string, .number_string => |bytes| bytes,
                    else => return error.SqlTypeMismatch,
                };
                owned = try text.decodeElementLeaky(a, .numeric, bytes, work);
                _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{owned}, .{ .bytes = limits.bytes }, work);
                return owned;
            }
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
        .array_envelope => return error.SqlTypeMismatch,
    };
    _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{value}, .{ .bytes = limits.bytes }, work);
    return value;
}

test "SQL NUL parameters retain position for nested JSONB strings keys and TEXT" {
    const a = std.testing.allocator;
    const cases = [_]struct { kind: @import("ast.zig").ColumnType, json: []const u8 }{
        .{ .kind = .string, .json = "\"a\\u0000b\"" },
        .{ .kind = .json, .json = "{\"output\":[\"a\\u0000b\"]}" },
        .{ .kind = .json, .json = "{\"a\\u0000b\":1}" },
    };
    for (cases) |case| {
        const parsed = try std.json.parseFromSlice(std.json.Value, a, case.json, .{});
        defer parsed.deinit();
        var context: @import("errors.zig").Context = .{};
        try std.testing.expectError(error.SqlUntranslatableCharacter, Frame.prepareJson(a, &.{ .{ .kind = .integer }, .{ .kind = case.kind } }, &.{ .{ .integer = 1 }, parsed.value }, .{ .error_context = &context }));
        const diagnostic = context.diagnostic(error.SqlUntranslatableCharacter);
        try std.testing.expectEqualStrings("22P05", diagnostic.code);
        try std.testing.expectEqual(@as(?usize, 2), diagnostic.parameter_index);
        try std.testing.expect(std.mem.indexOf(u8, diagnostic.message, "parameter $2") != null);
    }
}

test "SQL native parameter payload leases survive transport release and every allocation failure" {
    const Probe = struct {
        fn run(a: A) !void {
            const ingress = try Ingress.create(a, 1024 * 1024);
            var transport_live = true;
            defer if (transport_live) ingress.release();
            const input = try ingress.allocator().create(arrays.Value);
            input.* = (try text.decodeLeaky(ingress.allocator(), .text, "[-1:1]={\"é\",NULL,\"retained\"}", .{ .compact = true })).value;
            const descriptor = [_]scalar.Type{.{ .kind = .array, .element_type = .text }};
            var first = try Frame.prepare(a, &descriptor, &.{.{ .datum = .typedArray(input) }}, .{ .payload_owner = ingress.lease() });
            var first_live = true;
            defer if (first_live) first.deinit();
            var second = try Frame.prepare(a, &descriptor, &.{.{ .datum = .typedArray(input) }}, .{ .payload_owner = ingress.lease() });
            defer second.deinit();
            try std.testing.expectEqual(@intFromPtr(input), @intFromPtr(second.values[0].array.?));
            try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(a, &descriptor, &.{.{ .datum = .typedArray(input) }}, .{ .payload_owner = ingress.lease(), .bytes = ingress.lease().bytes - 1 }));
            ingress.release();
            transport_live = false;
            first.deinit();
            first_live = false;
            try std.testing.expectEqualStrings("retained", second.values[0].array.?.flatCell(2).value.string);
            try std.testing.expect(second.values[0].array.?.flatCell(1).sql_null);
            try std.testing.expectEqual(@as(i32, -1), second.values[0].array.?.dimensions[0].lower);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "SQL native parameter payload leases avoid compact frame copies" {
    const a = std.testing.allocator;
    const test_alloc = @import("../storage/lite/test_allocator.zig");
    const ingress = try Ingress.create(a, 1024 * 1024);
    defer ingress.release();
    const elements = try a.alloc(arrays.Element, 32768);
    defer a.free(elements);
    @memset(elements, .json(.{ .integer = 42 }));
    const value = try arrays.Value.init(.int64, &.{.{ .length = 32768 }}, elements, .{});
    const bytes = try @import("array_storage.zig").encodeAlloc(a, value, .{});
    defer a.free(bytes);
    const compact = try ingress.allocator().create(arrays.Value);
    compact.* = try @import("array_storage.zig").viewLeaky(ingress.allocator(), .int64, bytes, .{});
    var peaks: [2]usize = undefined;
    for ([_]bool{ false, true }, 0..) |leased, i| {
        var nr: test_alloc.NoResizeAllocator = .{ .backing = a };
        var budget: test_alloc.BudgetAllocator = .{ .backing = nr.allocator() };
        {
            var frame = try Frame.prepare(budget.allocator(), &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(compact) }}, .{ .payload_owner = if (leased) ingress.lease() else null });
            defer frame.deinit();
            try std.testing.expectEqual(@as(i64, 42), frame.values[0].array.?.flatCell(32767).value.integer);
            peaks[i] = budget.peak;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(peaks[1] < peaks[0] / 8);
    std.debug.print("SQL native leased parameter frame: copied_peak={} leased_peak={} retained_payload={}\n", .{ peaks[0], peaks[1], ingress.lease().bytes });
}

test "SQL prepared equality indexes warm once remain frame owned and preserve NULL semantics" {
    const a = std.testing.allocator;
    var cells: [128]scalar.Datum = undefined;
    for (&cells, 0..) |*cell, i| cell.* = if (i == 127) .{} else .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{ .{ .length = 2, .lower = -3 }, .{ .length = 64, .lower = 4 } }, &cells, .{});
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int64 }}, &.{.{ .kind = .array, .element_type = .int64 }}, .{});
    defer program.deinit();
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    var shifted_cells: [128]scalar.Datum = undefined;
    for (&shifted_cells, 0..) |*cell, i| cell.* = if (i == 127) .{} else .json(.{ .integer = @intCast(1000 + i) });
    const shifted = try arrays.Value.init(.int64, source.dimensions, &shifted_cells, .{});
    var second = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(&shifted) }}, .{});
    defer second.deinit();
    const prepared = try program.bindParameters(&frame);
    const other = try program.bindParameters(&second);
    for (0..64) |row| {
        const needle: scalar.Datum = if (row % 3 == 0) .{} else .json(.{ .integer = if (row % 3 == 1) 42 else 999 });
        const result = try prepared.evaluate(std.testing.failing_allocator, &.{needle}, .{});
        if (row % 3 == 1) try std.testing.expect(result.value.bool) else try std.testing.expect(result.sql_null);
    }
    try std.testing.expectEqual(@as(u8, 2), frame.memberships.?.slots[0].state.load(.acquire));
    const cached = frame.memberships.?.slots[0].index.?;
    const live = frame.budget.live;
    for (0..100) |_| _ = try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 42 })}, .{});
    try std.testing.expectEqual(live, frame.budget.live);
    try std.testing.expect(cached == frame.memberships.?.slots[0].index.?);
    try std.testing.expectEqual(@as(u8, 0), second.memberships.?.slots[0].state.load(.acquire));
    var rebound = prepared;
    rebound.parameters = second.values;
    try std.testing.expect((try rebound.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 42 })}, .{})).sql_null);
    try std.testing.expectEqual(@as(u8, 0), second.memberships.?.slots[0].state.load(.acquire));
    for (0..32) |_| _ = try other.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 42 })}, .{});
    try std.testing.expect(cached != second.memberships.?.slots[0].index.?);
    // A frozen frame/cached index remains valid when the move-only value moves.
    var moved = frame;
    try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 42 })}, .{})).value.bool);
    const moved_prepared = try program.bindParameters(&moved);
    try std.testing.expect((try moved_prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 42 })}, .{})).value.bool);
}

test "SQL prepared equality cache unwinds allocation failures limits and canceled builds" {
    const a = std.testing.allocator;
    var cells: [129]scalar.Datum = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    const Fixture = struct {
        fn run(backing: A, input: *const arrays.Value) !void {
            var frame = try Frame.prepare(backing, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(input) }}, .{});
            defer frame.deinit();
            var work: arrays.Budget = .{};
            for (0..32) |_| _ = try frame.equalAny(0, frame.values[0].array.?, .json(.{ .integer = 128 }), &work);
            const index = frame.memberships.?.slots[0].index.?;
            try std.testing.expectEqual(@as(?bool, true), try index.equalAny(.json(.{ .integer = 128 }), &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{&source});
    var frame = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    frame.memberships.?.memory.limit = 1024;
    var work: arrays.Budget = .{};
    for (0..32) |_| try std.testing.expectEqual(@as(?bool, true), try frame.equalAny(0, frame.values[0].array.?, .json(.{ .integer = 128 }), &work));
    try std.testing.expect(!frame.budget.isExhausted());
    var canceled = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer canceled.deinit();
    for (0..16) |_| _ = try canceled.equalAny(0, canceled.values[0].array.?, .json(.{ .integer = 128 }), &work);
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.Canceled;
        }
    };
    var cancel: Cancel = .{};
    var owner: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.check, .ptr = &cancel };
    var cancel_work: arrays.Budget = .{ .shared = &owner };
    try std.testing.expectError(error.Canceled, canceled.equalAny(0, canceled.values[0].array.?, .json(.{ .integer = 128 }), &cancel_work));
    try std.testing.expectEqual(@as(usize, 0), canceled.memberships.?.memory.live);
    try std.testing.expectError(error.Canceled, cancel_work.consume(0));
}

test "SQL prepared equality lookup measures warm native rows without row allocations" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var builder = try @import("array_compact.zig").ResultBuilder.initKnown(region.allocator(), .int64, &.{.{ .length = 8192 }}, 8192, 8192, .{});
    var work: arrays.Budget = .{};
    for (0..8192) |i| try builder.append(.json(.{ .integer = @intCast(i) }), &work);
    const source = try builder.finish(region.allocator(), .{}, &work);
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int64 }}, &.{.{ .kind = .array, .element_type = .int64 }}, .{});
    defer program.deinit();
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    const prepared = try program.bindParameters(&frame);
    for (0..32) |_| _ = try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 99999 })}, .{});
    const index = frame.memberships.?.slots[0].index.?;
    var scan_prepared = prepared;
    scan_prepared.lookup = .{};
    for (0..2) |mode| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..1000) |i| {
            const needle: scalar.Datum = .json(.{ .integer = if (i % 2 == 0) 8191 else 99999 });
            const result = (try (if (mode == 0) scan_prepared else prepared).evaluate(std.testing.failing_allocator, &.{needle}, .{})).value.bool;
            try std.testing.expectEqual(i % 2 == 0, result);
        }
        std.debug.print("SQL_PREPARED_ANY indexed={} rows=1000 source_cells=8192 retained_index_bytes={} elapsed_ns={} row_allocations=0\n", .{ mode == 1, index.budget.live, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    }
}

test "SQL prepared equality cache publishes immutable indexes to concurrent workers" {
    if (comptime @import("builtin").single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var cells: [129]scalar.Datum = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    var frame = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    var io_impl = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(2) });
    defer io_impl.deinit();
    const io = io_impl.io();
    var gate: std.Io.Event = .unset;
    const Worker = struct {
        fn run(input: *const Frame, active_io: std.Io, start: *std.Io.Event) !void {
            try start.wait(active_io);
            for (0..64) |_| {
                var work: arrays.Budget = .{};
                const matched = try input.equalAny(0, input.values[0].array.?, .json(.{ .integer = 128 }), &work);
                try std.testing.expectEqual(@as(?bool, true), matched);
            }
        }
    };
    var first = try io.concurrent(Worker.run, .{ &frame, io, &gate });
    defer first.cancel(io) catch {};
    var second = try io.concurrent(Worker.run, .{ &frame, io, &gate });
    defer second.cancel(io) catch {};
    gate.set(io);
    try first.await(io);
    try second.await(io);
    try std.testing.expectEqual(@as(u8, 2), frame.memberships.?.slots[0].state.load(.acquire));
}

test "SQL prepared equality indexes retain widened mixed and exact numeric fallback" {
    const a = std.testing.allocator;
    var cells: [129]scalar.Datum = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    var frame = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    var work: arrays.Budget = .{};
    for (0..32) |_| _ = try frame.equalAny(0, frame.values[0].array.?, .json(.{ .integer = 128 }), &work);
    const index = frame.memberships.?.slots[0].index.?;
    for ([_]struct { sql: []const u8, expected: bool }{
        .{ .sql = "127.5::double precision = ANY($1)", .expected = false },
        .{ .sql = "127.5::numeric = ANY($1)", .expected = false },
        .{ .sql = "128::real = ANY($1::real[])", .expected = true },
        .{ .sql = "128 <> ALL($1)", .expected = false },
        .{ .sql = "999 <> ALL($1)", .expected = true },
    }) |case| {
        var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        var program = try scalar.bindTyped(a, compiled.expression, &.{}, frame.descriptors, .{});
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const prepared = try program.bindParameters(&frame);
        const result = try prepared.evaluate(arena.allocator(), &.{}, .{});
        try std.testing.expect(!result.sql_null);
        try std.testing.expectEqual(case.expected, result.value.bool);
        try std.testing.expect(index == frame.memberships.?.slots[0].index.?);
    }
}

test "SQL prepared equality optional indexes respect parent quotas through arenas" {
    const a = std.testing.allocator;
    var parent: MemoryBudget = .{ .backing = a, .limit = 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(parent.allocator());
    defer arena.deinit();
    var cells: [129]scalar.Datum = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    var frame = try Frame.prepare(arena.allocator(), &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer frame.deinit();
    try std.testing.expect(frame.memberships != null);
    const retained = parent.live;
    parent.limit = retained + 1024;
    var work: arrays.Budget = .{};
    for (0..32) |_| try std.testing.expectEqual(@as(?bool, true), try frame.equalAny(0, frame.values[0].array.?, .json(.{ .integer = 128 }), &work));
    try std.testing.expectEqual(retained, parent.live);
    try std.testing.expect(!parent.isExhausted());
    try std.testing.expectEqual(@as(?bool, true), try frame.values[0].array.?.quantified(.json(.{ .integer = 128 }), .eq, .any, &work));
}

test "SQL prepared equality admission measures cold short early tail and miss rows" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var builder = try @import("array_compact.zig").ResultBuilder.initKnown(region.allocator(), .int64, &.{.{ .length = 8192 }}, 8192, 8192, .{});
    var work: arrays.Budget = .{};
    for (0..8192) |i| try builder.append(.json(.{ .integer = @intCast(i) }), &work);
    const source = try builder.finish(region.allocator(), .{}, &work);
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int64 }}, &.{.{ .kind = .array, .element_type = .int64 }}, .{});
    defer program.deinit();
    for ([_]i64{ 0, 8191, 99999 }) |needle| for ([_]usize{ 8, 32, 1000 }) |rows| for (0..2) |mode| {
        var best: i96 = std.math.maxInt(i96);
        var retained: usize = 0;
        var peak: usize = 0;
        const trials: usize = if (@import("builtin").mode == .debug) 1 else 5;
        for (0..trials) |_| {
            var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(&source) }}, .{});
            defer frame.deinit();
            var prepared = try program.bindParameters(&frame);
            if (mode == 0) prepared.lookup = .{};
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            for (0..rows) |_| {
                const result = try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = needle })}, .{});
                try std.testing.expectEqual(needle != 99999, result.value.bool);
            }
            best = @min(best, @as(i96, @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start)));
            const cache = frame.memberships.?;
            retained = cache.memory.live;
            peak = cache.memory.peak;
            const should_index = mode == 1 and needle != 0 and rows > 16;
            try std.testing.expectEqual(should_index, cache.slots[0].state.load(.acquire) == 2);
            if (!should_index) try std.testing.expectEqual(@as(usize, 0), retained);
            // One bucket vector owns u32 ordinals and byte fingerprints.
            if (should_index) try std.testing.expect(retained < 85 * 1024 and peak < 125 * 1024);
        }
        std.debug.print("SQL_COLD_ANY automatic={} needle={} rows={} trials={} best_ns={} index_live={} index_peak={} row_allocations=0\n", .{ mode == 1, needle, rows, trials, best, retained, peak });
    };
}

test "SQL prepared equality admission keeps long first hits cheap and adapts to later scans" {
    const a = std.testing.allocator;
    const payload: [1024]u8 = @splat('a');
    var cells: [128]scalar.Datum = @splat(.json(.{ .string = &payload }));
    const source = try arrays.Value.init(.text, &.{.{ .length = cells.len }}, &cells, .{});
    // Exercise both scalar cells and native text, including rank/lower bounds.
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const bytes = try @import("array_storage.zig").encodeAlloc(region.allocator(), source, .{});
    const native = try @import("array_storage.zig").viewLeaky(region.allocator(), .text, bytes, .{});
    for ([_]*const arrays.Value{ &source, &native }) |input| {
        var frame = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .text }}, &.{.{ .datum = .typedArray(input) }}, .{});
        defer frame.deinit();
        for (0..256) |_| {
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(@as(?bool, true), try frame.equalAny(0, frame.values[0].array.?, .json(.{ .string = &payload }), &work));
        }
        try std.testing.expectEqual(@as(u8, 0), frame.memberships.?.slots[0].state.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), frame.memberships.?.memory.live);
        const missing: [1024]u8 = @splat('b');
        for (0..256) |_| {
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(@as(?bool, false), try frame.equalAny(0, frame.values[0].array.?, .json(.{ .string = &missing }), &work));
        }
        try std.testing.expectEqual(@as(u8, 2), frame.memberships.?.slots[0].state.load(.acquire));
    }
}

test "SQL prepared equality admits large equivalent representations within index quotas" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const cells = try region.allocator().alloc(scalar.Datum, 32768);
    for (cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = @intCast(cells.len) }}, cells, .{});
    const storage = @import("array_storage.zig");
    const bytes = try storage.encodeAlloc(region.allocator(), source, .{});
    const native = try storage.viewLeaky(region.allocator(), .int64, bytes, .{});
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int64 }}, &.{.{ .kind = .array, .element_type = .int64 }}, .{});
    defer program.deinit();
    for ([_]*const arrays.Value{ &source, &native }, 0..) |input, representation| for (0..2) |mode| {
        var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(input) }}, .{});
        defer frame.deinit();
        var prepared = try program.bindParameters(&frame);
        if (mode == 0) prepared.lookup = .{};
        const source_bytes = try frame.values[0].array.?.retainedBytes();
        const cache = frame.memberships.?;
        if (representation == 0) try std.testing.expect(source_bytes > cache.memory.limit);
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..1000) |_| try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 32767 })}, .{ .steps = 1_048_576 })).value.bool);
        const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
        try std.testing.expectEqual(mode == 1, cache.slots[0].state.load(.acquire) == 2);
        try std.testing.expect(cache.memory.peak < 512 * 1024);
        if (mode == 0) try std.testing.expectEqual(@as(usize, 0), cache.memory.live);
        std.debug.print("SQL_SOURCE_INDEX automatic={} native={} rows=1000 source_cells=32768 source_bytes={} index_live={} index_peak={} elapsed_ns={} row_allocations=0\n", .{ mode == 1, representation == 1, source_bytes, cache.memory.live, cache.memory.peak, elapsed });
    };
}

test "SQL prepared equality recent admission adapts after long cheap phases and preserves build credit" {
    const a = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var builder = try @import("array_compact.zig").ResultBuilder.initKnown(region.allocator(), .int64, &.{.{ .length = 8192 }}, 8192, 8192, .{});
    var initial: arrays.Budget = .{};
    for (0..8192) |i| try builder.append(.json(.{ .integer = @intCast(i) }), &initial);
    const source = try builder.finish(region.allocator(), .{}, &initial);
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .integer, .element_type = .int64 }}, &.{.{ .kind = .array, .element_type = .int64 }}, .{});
    defer program.deinit();
    for ([_]i64{ 15, 31, 511, 8191 }) |needle| for (0..2) |phase| {
        var frame = try Frame.prepare(a, program.parameter_descriptors, &.{.{ .datum = .typedArray(&source) }}, .{});
        defer frame.deinit();
        const prepared = try program.bindParameters(&frame);
        const cache = frame.memberships.?;
        if (phase == 1) {
            for (0..65536) |_| try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = 0 })}, .{})).value.bool);
            try std.testing.expectEqual(@as(usize, 0), cache.memory.live);
            try std.testing.expectEqual(@as(usize, 0), cache.slots[0].build_credit);
            try std.testing.expect(cache.slots[0].recent_scans <= 64);
            try std.testing.expectEqual(@as(u8, 0), cache.slots[0].state.load(.acquire));
        }
        var first_index_row: ?usize = null;
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..2048) |i| {
            try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{.json(.{ .integer = needle })}, .{})).value.bool);
            if (first_index_row == null and cache.slots[0].state.load(.acquire) == 2) first_index_row = i + 1;
        }
        const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
        try std.testing.expect(first_index_row != null);
        try std.testing.expect(first_index_row.? <= (if (needle == 8191) @as(usize, 32) else if (needle == 511) @as(usize, 128) else @as(usize, 2048)));
        try std.testing.expect(cache.slots[0].recent_scans <= 64);
        try std.testing.expect(cache.slots[0].build_credit <= cache.slots[0].build_work);
        std.debug.print("SQL_RECENT_ANY antecedent_cheap_rows={} prefix_cells={} timed_rows=2048 first_index_row={} index_live={} elapsed_ns={} row_allocations=0\n", .{ if (phase == 1) @as(usize, 65536) else @as(usize, 0), needle + 1, first_index_row.?, cache.memory.live, elapsed });
    };
}

test "SQL frame array capabilities reuse admitted JSONB and survive owner moves" {
    const a = std.testing.allocator;
    const n = 1024;
    var frame = admitted: {
        var input = std.heap.ArenaAllocator.init(a);
        defer input.deinit();
        const cells = try input.allocator().alloc(scalar.Datum, n);
        for (cells, 0..) |*cell, i| {
            const payload = try input.allocator().alloc(u8, 256);
            @memset(payload, 'a');
            _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i});
            cell.* = .json(.{ .string = payload });
        }
        const source = try arrays.Value.init(.jsonb, &.{.{ .length = n }}, cells, .{});
        break :admitted try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    };
    const capability = frame.memberships.?.arraySource(0).?;
    var moved = frame;
    frame = undefined;
    defer moved.deinit();
    const owned = moved.values[0].array.?;
    var spent: [2]usize = undefined;
    for (0..2) |mode| {
        var elapsed: i96 = 0;
        var peak: usize = 0;
        for (0..20) |_| {
            var work: arrays.Budget = .{ .remaining = 1024 * 1024 };
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            var index = if (mode == 0)
                try arrays.Membership.initBorrowedWithBudget(a, owned.*, .{}, 128 * 1024, &work)
            else
                try arrays.Membership.initFrameOwnedWithBudget(a, capability, 128 * 1024, &work);
            defer index.deinit();
            elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
            spent[mode] = 1024 * 1024 - work.remaining;
            peak = @max(peak, index.budget.peak);
            try std.testing.expectEqual(@as(usize, n), index.count);
            try std.testing.expectEqual(@as(?i32, n), try index.firstPosition(owned.flatCell(n - 1), &work));
            try std.testing.expectEqual(@as(usize, 10240), index.budget.live);
        }
        std.debug.print("SQL_FRAME_SOURCE owned={} cells={} repetitions=20 work={} peak={} elapsed_ns={}\n", .{ mode == 1, n, spent[mode], peak, elapsed });
    }
    var validation: arrays.Budget = .{ .remaining = 1024 * 1024 };
    _ = try owned.validateWithBudget(.{}, &validation);
    try std.testing.expectEqual(spent[0] - (1024 * 1024 - validation.remaining) + 1, spent[1]);
    // Only the authenticated owner path fits this construction allowance.
    var limited: arrays.Budget = .{ .remaining = spent[1] };
    var index = try arrays.Membership.initFrameOwnedWithBudget(a, capability, 128 * 1024, &limited);
    defer index.deinit();
    try std.testing.expectEqual(@as(usize, 0), limited.remaining);
    limited.remaining = spent[1];
    try std.testing.expectError(error.SqlProgramLimitExceeded, arrays.Membership.initBorrowedWithBudget(a, owned.*, .{}, 128 * 1024, &limited));
    var denied: arrays.Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, arrays.Membership.initFrameOwnedWithBudget(std.testing.failing_allocator, capability, 128 * 1024, &denied));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var context: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.check };
    var interrupted: arrays.Budget = .{ .shared = &context };
    try std.testing.expectError(error.Canceled, arrays.Membership.initFrameOwnedWithBudget(std.testing.failing_allocator, capability, 128 * 1024, &interrupted));
    try std.testing.expectError(error.Canceled, interrupted.consume(0));

    // Exercise complete prepared JSONB evaluation and automatic admission.
    // Numeric probes remain logical JSON without text conversion scratch.
    const mixed_cells = try a.dupe(scalar.Datum, owned.elements);
    defer a.free(mixed_cells);
    mixed_cells[n - 1] = .json(.{ .integer = 42 });
    const mixed = try arrays.Value.init(.jsonb, &.{.{ .length = n }}, mixed_cells, .{});
    var evaluation = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&mixed) }}, .{});
    defer evaluation.deinit();
    var compiled = try @import("compiler.zig").compileScalar(a, "needle = ANY($1)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{.{ .name = "needle", .type = .json, .element_type = .jsonb }}, &.{.{ .kind = .array, .element_type = .jsonb }}, .{});
    defer program.deinit();
    const prepared = try program.bindParameters(&evaluation);
    for (0..1000) |i| {
        const needle: scalar.Datum = .json(.{ .integer = if (i % 2 == 0) 42 else -1 });
        try std.testing.expectEqual(i % 2 == 0, (try prepared.evaluate(std.testing.failing_allocator, &.{needle}, .{})).value.bool);
    }
    try std.testing.expectEqual(@as(u8, 2), evaluation.memberships.?.slots[0].state.load(.acquire));
    try std.testing.expect(evaluation.memberships.?.remaining < 1024 * 1024);
}

test "SQL frame array capabilities preserve ingress admission and allocation faults" {
    const a = std.testing.allocator;
    var payloads: [64][256]u8 = undefined;
    var cells: [64]scalar.Datum = undefined;
    for (&payloads, &cells, 0..) |*payload, *cell, i| {
        @memset(payload, 'a');
        _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i});
        cell.* = .json(.{ .string = payload });
    }
    const source = try arrays.Value.init(.jsonb, &.{.{ .length = cells.len }}, &cells, .{});
    const Fixture = struct {
        fn run(backing: A, value: *const arrays.Value) !void {
            var frame = try Frame.prepare(backing, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(value) }}, .{});
            defer frame.deinit();
            var work: arrays.Budget = .{};
            var index = try arrays.Membership.initFrameOwnedWithBudget(backing, frame.memberships.?.arraySource(0).?, 128 * 1024, &work);
            defer index.deinit();
            try std.testing.expectEqual(@as(usize, 64), index.count);
            try std.testing.expectEqual(@as(?bool, true), try index.equalAny(frame.values[0].array.?.flatCell(63), &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{&source});
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&source) }}, .{ .elements = 63 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&source) }}, .{ .work = 100 }));
    var stale = source;
    stale.prepared_footprint = 1;
    var refreshed = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&stale) }}, .{});
    defer refreshed.deinit();
    try std.testing.expectEqual(source.prepared_footprint, refreshed.values[0].array.?.prepared_footprint);
    var zero: @import("numeric_value.zig").Value = .{};
    var malformed = scalar.Datum.typedArray(&source);
    malformed.numeric = &zero;
    try std.testing.expectError(error.SqlTypeMismatch, Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = malformed }}, .{}));
    cells[63] = .json(.{ .string = "\xff" });
    try std.testing.expectError(error.SqlInvalidTextEncoding, Frame.prepare(a, &.{.{ .kind = .array, .element_type = .jsonb }}, &.{.{ .datum = .typedArray(&source) }}, .{}));
    var work: arrays.Budget = .{};
    try std.testing.expectError(error.SqlInvalidTextEncoding, arrays.Membership.initBorrowedWithBudget(a, source, .{}, 128 * 1024, &work));
}

test "SQL frame array capabilities cannot cross rebound parameter registries" {
    const a = std.testing.allocator;
    var cells: [64]scalar.Datum = undefined;
    var other_cells: [64]scalar.Datum = undefined;
    for (&cells, &other_cells, 0..) |*cell, *other, i| {
        cell.* = .json(.{ .integer = @intCast(i) });
        other.* = .json(.{ .integer = @as(i64, @intCast(i)) + 100 });
    }
    const source = try arrays.Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    const other = try arrays.Value.init(.int64, &.{.{ .length = other_cells.len }}, &other_cells, .{});
    var first = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&source) }}, .{});
    defer first.deinit();
    var second = try Frame.prepare(a, &.{.{ .kind = .array, .element_type = .int64 }}, &.{.{ .datum = .typedArray(&other) }}, .{});
    defer second.deinit();
    const rebound = first.lookup();
    var work: arrays.Budget = .{};
    for (0..32) |_| try std.testing.expectEqual(@as(?bool, true), try rebound.equalAny(0, second.values[0].array.?, second.values, .json(.{ .integer = 163 }), &work));
    try std.testing.expectEqual(@as(usize, 0), first.memberships.?.slots[0].recent_scans);
    try std.testing.expectEqual(@as(usize, 0), first.memberships.?.slots[0].build_credit);
    for (0..32) |_| _ = try first.equalAny(0, first.values[0].array.?, .json(.{ .integer = 63 }), &work);
    try std.testing.expect(first.memberships.?.slots[0].index != null);
    try std.testing.expectEqual(@as(?bool, false), try rebound.equalAny(0, second.values[0].array.?, second.values, .json(.{ .integer = 63 }), &work));
    try std.testing.expect(first.memberships.?.arraySource(1) == null);
    std.debug.print("SQL_FRAME_LOOKUP handle_bytes={} registry_bound=true\n", .{@sizeOf(Frame.Lookup)});
}

test "SQL quantified JSONB logical strings preserve identity without row allocations" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compileScalar(a, "$1 = ANY($2)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{ .{ .kind = .json, .element_type = .jsonb }, .{ .kind = .array, .element_type = .jsonb } }, .{});
    defer program.deinit();
    const Case = struct { probe: []const u8, element: scalar.Datum, expected: bool };
    for ([_]Case{
        .{ .probe = "alpha", .element = .json(.{ .string = "alpha" }), .expected = true },
        .{ .probe = "123", .element = .json(.{ .string = "123" }), .expected = true },
        .{ .probe = "null", .element = .json(.null), .expected = false },
    }) |case| {
        const array = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{case.element}, .{});
        var frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .{ .datum = .json(.{ .string = case.probe }) }, .{ .datum = .typedArray(&array) } }, .{});
        defer frame.deinit();
        var work: arrays.Budget = .{};
        const direct = try frame.values[1].array.?.quantified(frame.values[0], .eq, .any, &work);
        try std.testing.expectEqual(@as(?bool, case.expected), direct);
        const prepared = try program.bindParameters(&frame);
        const actual = try prepared.evaluate(std.testing.failing_allocator, &.{}, .{});
        try std.testing.expectEqual(case.expected, actual.value.bool);
    }
}

test "SQL quantified UUID cold and warm probes need no row allocations" {
    const a = std.testing.allocator;
    var payloads: [32][36]u8 = undefined;
    var cells: [32]scalar.Datum = undefined;
    for (&payloads, &cells, 0..) |*payload, *cell, i| {
        _ = try std.fmt.bufPrint(payload, "00000000-0000-0000-0000-{d:0>12}", .{i});
        cell.* = .json(.{ .string = payload });
    }
    const source = try arrays.Value.init(.uuid, &.{.{ .length = cells.len }}, &cells, .{});
    var compiled = try @import("compiler.zig").compileScalar(a, "$1 = ANY($2)", .{});
    defer compiled.deinit();
    var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{ .{ .kind = .uuid, .element_type = .uuid }, .{ .kind = .array, .element_type = .uuid } }, .{});
    defer program.deinit();
    var frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .{ .datum = cells[31] }, .{ .datum = .typedArray(&source) } }, .{});
    defer frame.deinit();
    const prepared = try program.bindParameters(&frame);
    // Cold scans and the eventual cached lookup both borrow canonical input.
    for (0..32) |_| try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{}, .{})).value.bool);
    try std.testing.expectEqual(@as(u8, 2), frame.memberships.?.slots[1].state.load(.acquire));
    for (0..1000) |_| try std.testing.expect((try prepared.evaluate(std.testing.failing_allocator, &.{}, .{})).value.bool);
}

test "SQL quantified JSONB strings agree across scans indexes and all comparisons" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, comparison: arrays.Comparison, quantifier: arrays.Quantifier };
    for ([_]Case{
        .{ .sql = "$1 = ANY($2)", .comparison = .eq, .quantifier = .any },
        .{ .sql = "$1 <> ALL($2)", .comparison = .ne, .quantifier = .all },
        .{ .sql = "$1 = ALL($2)", .comparison = .eq, .quantifier = .all },
        .{ .sql = "$1 <> ANY($2)", .comparison = .ne, .quantifier = .any },
        .{ .sql = "$1 < ANY($2)", .comparison = .lt, .quantifier = .any },
        .{ .sql = "$1 <= ALL($2)", .comparison = .le, .quantifier = .all },
        .{ .sql = "$1 > ALL($2)", .comparison = .gt, .quantifier = .all },
        .{ .sql = "$1 >= ANY($2)", .comparison = .ge, .quantifier = .any },
    }) |case| {
        var compiled = try @import("compiler.zig").compileScalar(a, case.sql, .{});
        defer compiled.deinit();
        var program = try scalar.bindTyped(a, compiled.expression, &.{}, &.{ .{ .kind = .json, .element_type = .jsonb }, .{ .kind = .array, .element_type = .jsonb } }, .{});
        defer program.deinit();
        // A long matching tail forces optional index admission; misses, JSON
        // null, and SQL NULL must keep their ordinary three-valued semantics.
        for ([_]scalar.Datum{ .json(.{ .string = "123" }), .json(.{ .string = "null" }), .json(.null), .{} }) |probe| {
            for ([_]usize{ 0, 1, 64 }) |count| {
                var cells: [64]scalar.Datum = @splat(.json(.{ .string = "alpha" }));
                if (count != 0) cells[count - 1] = .json(.{ .string = "123" });
                if (count > 1) cells[0] = .{};
                const axes: [1]arrays.Dimension = .{.{ .length = @intCast(count) }};
                const source = try arrays.Value.init(.jsonb, if (count == 0) &.{} else &axes, cells[0..count], .{});
                var frame = try Frame.prepare(a, program.parameter_descriptors, &.{ .{ .datum = probe }, .{ .datum = .typedArray(&source) } }, .{});
                defer frame.deinit();
                var work: arrays.Budget = .{};
                const expected = try source.quantified(probe, case.comparison, case.quantifier, &work);
                const prepared = try program.bindParameters(&frame);
                for (0..64) |_| {
                    const actual = try prepared.evaluate(std.testing.failing_allocator, &.{}, .{});
                    if (expected) |value| {
                        try std.testing.expect(!actual.sql_null);
                        try std.testing.expectEqual(value, actual.value.bool);
                    } else try std.testing.expect(actual.sql_null);
                }
                if (count == 64 and probe.value == .string and !probe.sql_null and
                    ((case.comparison == .eq and case.quantifier == .any) or (case.comparison == .ne and case.quantifier == .all)))
                    try std.testing.expectEqual(@as(u8, 2), frame.memberships.?.slots[1].state.load(.acquire));
            }
        }
    }
}
