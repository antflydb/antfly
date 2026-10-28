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

//! Enum execution invariants shared by graph operators and retained cursors.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const operators = @import("operators.zig");
const types = @import("../system_catalog/named_types.zig");
const enums = @import("../common/sql_enum_value.zig");
const spill = @import("spill.zig");
const A = std.mem.Allocator;
const Fixture = struct {
    definition: types.Owned,
    view: enums.View,
    fn init(a: A, id: u64, origin: u8) !Fixture {
        var definition = try types.Owned.create(a, &.{ "z", "a", "" });
        errdefer definition.deinit();
        const record: types.Record = .{ .identity = try types.Identity.init(id, 2, 1, @splat(origin)), .definition = definition.definition, .owner = .auth_disabled };
        return .{ .definition = definition, .view = try enums.View.init(a, record) };
    }
    fn deinit(self: *Fixture, a: A) void {
        self.view.deinit(a);
        self.definition.deinit();
    }
    fn value(self: *const Fixture, text: []const u8) !Datum {
        return .typedEnum(try self.view.input(text));
    }
};

test "SQL named enum graph datums compare and hash by immutable identity without row allocation" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const z = try source.value("z");
    const aa = try source.value("a");
    const empty = try source.value("");
    try std.testing.expectEqual(std.math.Order.lt, try scalar.compareDatums(z, aa));
    try std.testing.expectEqual(std.math.Order.lt, try scalar.compareDatums(aa, empty));
    try std.testing.expectEqual(std.math.Order.gt, try scalar.compareDatums(.{}, empty));
    try std.testing.expect(!empty.sql_null and empty.value == .null);
    var other = try Fixture.init(a, 6, 1);
    defer other.deinit(a);
    var recreated = try Fixture.init(a, 5, 2);
    defer recreated.deinit(a);
    try std.testing.expectError(error.SqlUndefinedOperator, scalar.compareDatums(z, try other.value("z")));
    try std.testing.expectError(error.SqlUndefinedOperator, scalar.compareDatums(z, try recreated.value("z")));
    try std.testing.expectError(error.SqlTypeMismatch, scalar.compareDatums(z, Datum.json(.{ .string = "z" })));
    try std.testing.expectError(error.SqlTypeMismatch, scalar.compareDatums(Datum.json(.{ .integer = 1 }), z));
    try std.testing.expect(try scalar.semanticHashDatum(z) != try scalar.semanticHashDatum(try other.value("z")));
    try std.testing.expect(try scalar.semanticHashDatum(z) != try scalar.semanticHashDatum(try recreated.value("z")));
    for (0..100000) |_| {
        try std.testing.expectEqual(std.math.Order.lt, try scalar.compareDatums(z, aa));
        try std.testing.expectEqual(try scalar.semanticHashDatum(z), try scalar.semanticHashDatum(z));
    }
    const orders = &[_]operators.Order{.{}};
    // A prefix alone cannot prove enum type identity; exact ordering is used.
    try std.testing.expect(@import("sort_key.zig").encode(&.{z}, orders) == null);
    var work: @import("json_order.zig").Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, scalar.compareDatumsWithBudget(z, aa, &work));
}

test "SQL named enum retained stores and portable row column codecs own producer data" {
    const a = std.testing.allocator;
    var retained = @import("typed_store.zig").Store.init(a);
    defer retained.deinit();
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var cloned: Datum = undefined;
    var encoded: []u8 = undefined;
    var block: []u8 = undefined;
    {
        var source = try Fixture.init(a, 5, 1);
        defer source.deinit(a);
        const z = try source.value("z");
        const empty = try source.value("");
        cloned = try operators.cloneDatum(owner.allocator(), z);
        _ = try retained.append(&.{ z, empty, .{} });
        _ = try retained.append(&.{ empty, .{}, z });
        encoded = try spill.encodeDatumsAlloc(a, &.{ z, empty, .{} }, 16384);
        block = try spill.encodeColumnarBlockAlloc(a, &.{ .{ .values = &.{ z, empty, .{} }, .keys = &.{empty}, .ordinal = 7 }, .{ .values = &.{ empty, .{}, z }, .keys = &.{z}, .ordinal = 8 } }, 16384);
    }
    defer a.free(encoded);
    defer a.free(block);
    const decoded = try spill.decodeDatumsInArena(owner.allocator(), encoded, 16384);
    const columns = try spill.decodeColumnarBlockInArena(owner.allocator(), block, 16384);
    try std.testing.expectEqualStrings("z", cloned.enumeration().?.text);
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(cloned, decoded[0]));
    try std.testing.expectEqual(try scalar.semanticHashDatum(cloned), try scalar.semanticHashDatum(decoded[0]));
    try std.testing.expectEqualStrings("", decoded[1].enumeration().?.text);
    try std.testing.expect(decoded[2].sql_null);
    try std.testing.expect((try columns.cell(0, 2)).sql_null);
    try std.testing.expectEqualStrings("", (try columns.keyCell(0, 0)).enumeration().?.text);
    for (0..2) |row| for (0..3) |column| {
        const value = try retained.cell(owner.allocator(), row, column);
        const restored = try columns.cell(row, column);
        try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(value, restored));
        try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatum(restored));
    };
    try std.testing.expect(try operators.datumBytes(cloned) >= @sizeOf(Datum) + @sizeOf(enums.Value) + 1);
}

test "SQL named enum hash joins membership grouping and top k retain declaration order" {
    const a = std.testing.allocator;
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const z = try source.value("z");
    const aa = try source.value("a");
    const empty = try source.value("");
    const join = try operators.HashJoin.create(a, .{});
    defer join.deinit();
    try join.add(&.{aa}, &.{aa});
    try join.add(&.{z}, &.{z});
    try join.add(&.{empty}, &.{empty});
    var probe = try join.probe(&.{z});
    const match = (try probe.next()).?;
    const cells = try match.materializeValues(owner.allocator());
    try std.testing.expectEqualStrings("z", cells[0].enumeration().?.text);
    try std.testing.expect(try probe.next() == null);
    const membership = try operators.TupleMembership.create(a, 1, .{}, null);
    defer membership.deinit();
    try membership.add(&.{z});
    try membership.add(&.{.{}});
    try membership.seal();
    try std.testing.expectEqual(@import("tuple_membership.zig").Truth.yes, try membership.probe(&.{z}));
    try std.testing.expectEqual(@import("tuple_membership.zig").Truth.unknown, try membership.probe(&.{aa}));
    var top = try operators.TopK.init(a, 3, &.{.{}}, 1024 * 1024);
    defer top.deinit();
    for ([_]Datum{ empty, aa, z }, 0..) |cell, i| try top.add(.{ .values = &.{cell}, .keys = &.{cell}, .ordinal = i });
    const sorted = try top.finish(owner.allocator());
    for (sorted, [_][]const u8{ "z", "a", "" }) |row, text| try std.testing.expectEqualStrings(text, row.values[0].enumeration().?.text);
    const grouped = try operators.Grouped.create(a, &.{.{ .kind = .count }}, .{});
    defer grouped.deinit();
    try grouped.add(&.{z}, &.{z});
    try grouped.add(&.{aa}, &.{aa});
    try grouped.add(&.{z}, &.{z});
    try std.testing.expectEqual(@as(usize, 2), grouped.groupCount());
    const groups = try grouped.finish(owner.allocator());
    for (groups) |group| {
        const count: i64 = if (std.mem.eql(u8, group.keys[0].enumeration().?.text, "z")) 2 else 1;
        try std.testing.expectEqual(count, group.aggregates[0].value.integer);
    }
}

test "SQL named enum portable codecs reject malformed identity rank null and truncated data" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const z = try source.value("z");
    const bytes = try spill.encodeDatumsAlloc(a, &.{z}, 16384);
    defer a.free(bytes);
    for (0..bytes.len) |length| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bytes[0..length], 16384));
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bad = try a.dupe(u8, bytes);
    defer a.free(bad);
    // count:u64, null:u8, tag:u8, local ID:u64, origin:16, symbol:u64.
    bad[8] = 1;
    try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bad, 16384));
    @memcpy(bad, bytes);
    @memset(bad[10..18], 0);
    try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bad, 16384));
    @memcpy(bad, bytes);
    @memset(bad[18..34], 0);
    try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bad, 16384));
    @memcpy(bad, bytes);
    @memset(bad[34..42], 0);
    try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bad, 16384));
    @memcpy(bad, bytes);
    bad[bad.len - 1] = 0;
    try std.testing.expectError(error.InvalidSqlSpill, spill.decodeDatumsInArena(arena.allocator(), bad, 16384));
    var invalid = z;
    invalid.sql_null = true;
    try std.testing.expectError(error.SqlTypeMismatch, operators.cloneDatum(arena.allocator(), invalid));
    invalid = z;
    invalid.value = .{ .string = "z" };
    try std.testing.expectError(error.SqlTypeMismatch, scalar.semanticHashDatum(invalid));
    var contradict = z.enumeration().?.*;
    contradict.order_key = &.{1};
    try std.testing.expectError(error.InvalidSqlNamedType, scalar.compareDatums(z, .typedEnum(&contradict)));
}

test "SQL named enum execution retention and codecs unwind every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var source = try Fixture.init(a, 5, 1);
            defer source.deinit(a);
            const z = try source.value("z");
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            _ = try operators.cloneDatum(arena.allocator(), z);
            var store = @import("typed_store.zig").Store.init(a);
            defer store.deinit();
            _ = try store.append(&.{z});
            const bytes = try spill.encodeDatumsAlloc(a, &.{z}, 16384);
            defer a.free(bytes);
            _ = try spill.decodeDatumsInArena(arena.allocator(), bytes, 16384);
            const block = try spill.encodeColumnarBlockAlloc(a, &.{.{ .values = &.{z}, .keys = &.{z}, .ordinal = 0 }}, 16384);
            defer a.free(block);
            _ = try spill.decodeColumnarBlockInArena(arena.allocator(), block, 16384);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL named enum scalar operators unknown input lazy branches and casts preserve domains" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const columns = &[_]scalar.Column{.{ .name = "v", .type = .enumeration, .enum_view = &source.view }};
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for ([_]struct { sql: []const u8, value: bool }{
        .{ .sql = "v = 'z'", .value = true },
        .{ .sql = "v < 'a'", .value = true },
        .{ .sql = "'a' > v", .value = true },
        .{ .sql = "v = v", .value = true },
        .{ .sql = "v IS DISTINCT FROM NULL", .value = true },
        .{ .sql = "v IS NOT DISTINCT FROM 'z'", .value = true },
        .{ .sql = "v IN ('a', 'z')", .value = true },
        .{ .sql = "v NOT IN ('a', '')", .value = true },
        .{ .sql = "v >= ''", .value = false },
        .{ .sql = "false AND v = 'a'", .value = false },
        .{ .sql = "true OR v = 'a'", .value = true },
    }) |entry| {
        var parsed = try @import("compiler.zig").compileScalar(a, entry.sql, .{});
        defer parsed.deinit();
        var program = try scalar.bind(a, parsed.expression, columns, &.{}, .{});
        defer program.deinit();
        const value = try program.evaluate(arena.allocator(), &.{try source.value("z")}, &.{}, .{});
        try std.testing.expect(!value.sql_null);
        try std.testing.expectEqual(entry.value, value.value.bool);
    }
    for ([_][]const u8{ "v", "coalesce(v, NULL)", "coalesce(NULL, v, 'a')", "greatest(v, v)", "least(v, v)", "CASE WHEN true THEN v ELSE NULL END" }) |sql| {
        errdefer std.debug.print("enum expression: {s}\n", .{sql});
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        var program = try scalar.bind(a, parsed.expression, columns, &.{}, .{});
        defer program.deinit();
        try std.testing.expect(program.output_type.kind == .enumeration and program.output_type.enum_view != null);
        const value = try program.evaluate(arena.allocator(), &.{try source.value("z")}, &.{}, .{});
        try std.testing.expectEqualStrings("z", value.enumeration().?.text);
    }
    for ([_][]const u8{ "v IN ('a', NULL)", "v = NULL", "nullif(v, 'z')" }) |sql| {
        errdefer std.debug.print("enum expression: {s}\n", .{sql});
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        var program = try scalar.bind(a, parsed.expression, columns, &.{}, .{});
        defer program.deinit();
        try std.testing.expect((try program.evaluate(arena.allocator(), &.{try source.value("z")}, &.{}, .{})).sql_null);
    }
    for ([_][]const u8{ "v = 'z'::text", "CASE WHEN false THEN v::bigint ELSE NULL END", "v + v", "v::boolean" }) |sql| {
        errdefer std.debug.print("enum expression: {s}\n", .{sql});
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        if (scalar.bind(a, parsed.expression, columns, &.{}, .{})) |result| {
            var program = result;
            program.deinit();
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expect(err == error.SqlTypeMismatch or err == error.SqlCannotCoerce or err == error.SqlUndefinedFunction or err == error.SqlUndefinedOperator or err == error.SqlCommonTypeMismatch);
    }
    var parsed = try @import("compiler.zig").compileScalar(a, "v::text", .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, columns, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqualStrings("z", (try program.evaluate(arena.allocator(), &.{try source.value("z")}, &.{}, .{})).value.string);
    var other = try Fixture.init(a, 6, 1);
    defer other.deinit(a);
    var bad = try @import("compiler.zig").compileScalar(a, "v = w", .{});
    defer bad.deinit();
    try std.testing.expectError(error.SqlUndefinedOperator, scalar.bind(a, bad.expression, &.{ columns[0], .{ .name = "w", .type = .enumeration, .enum_view = &other.view } }, &.{}, .{}));
}

test "SQL named enum prepared parameters own immutable directories and reject another type frame" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var program: scalar.Program = undefined;
    var frame: @import("parameter_frame.zig").Frame = undefined;
    {
        var source = try Fixture.init(a, 5, 1);
        defer source.deinit(a);
        var parsed = try @import("compiler.zig").compileScalar(a, "v < $1", .{});
        defer parsed.deinit();
        program = try scalar.bindTyped(a, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &source.view }}, &.{.{}}, .{});
        errdefer program.deinit();
        try std.testing.expect(program.parameter_descriptors[0].kind == .enumeration);
        frame = try @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{.{ .binary = "a" }}, .{});
    }
    defer program.deinit();
    defer frame.deinit();
    const invocation = try program.bindParameters(&frame);
    const view = program.parameter_descriptors[0].enum_view.?;
    const value = try invocation.evaluate(arena.allocator(), &.{.typedEnum(try view.input("z"))}, .{});
    try std.testing.expect(!value.sql_null and value.value.bool);
    try std.testing.expectError(error.SqlInvalidTextRepresentation, @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = "Z" }}, .{}));
    var other = try Fixture.init(a, 6, 1);
    defer other.deinit(a);
    var wrong = try @import("parameter_frame.zig").Frame.prepare(a, &.{.{ .kind = .enumeration, .enum_view = &other.view }}, &.{.{ .text = "a" }}, .{});
    defer wrong.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, program.bindParameters(&wrong));
    try std.testing.expectError(error.SqlProgramLimitExceeded, @import("parameter_frame.zig").Frame.prepare(a, program.parameter_descriptors, &.{.{ .text = "a" }}, .{ .bytes = 1 }));
}

test "SQL named enum bound programs use symbolic serialization and unwind binding allocation failures" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const encoded = try std.json.Stringify.valueAlloc(a, scalar.Type{ .kind = .enumeration, .enum_view = &source.view }, .{});
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "named_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "symbols") == null);
    var reference = try std.json.parseFromSlice(scalar.Type, a, encoded, .{});
    defer reference.deinit();
    try std.testing.expect(reference.value.enum_view == null and reference.value.enum_reference != null);
    try std.testing.expectEqual(source.view.record.identity.id, reference.value.enum_reference.?.id);
    try scalar.validateParameterDescriptorWire(reference.value);
    try std.testing.expectError(error.InvalidSqlParameters, scalar.validateParameterType(reference.value));
    try std.testing.expectError(error.InvalidSqlParameters, @import("parameter_frame.zig").Frame.prepare(a, &.{reference.value}, &.{.{ .text = "z" }}, .{}));
    const builtin = try std.json.Stringify.valueAlloc(a, scalar.Type{ .kind = .integer }, .{});
    defer a.free(builtin);
    try std.testing.expectEqualStrings("{\"kind\":\"integer\",\"temporal_kind\":null,\"nullable\":true,\"element_type\":null,\"numeric_modifier\":null}", builtin);
    const Harness = struct {
        fn run(alloc: A) !void {
            var fixture = try Fixture.init(alloc, 5, 1);
            defer fixture.deinit(alloc);
            var parsed = try @import("compiler.zig").compileScalar(alloc, "coalesce(v, NULL) < $1 AND v < 'a'", .{});
            defer parsed.deinit();
            var program = try scalar.bindTyped(alloc, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &fixture.view }}, &.{.{}}, .{});
            defer program.deinit();
            var frame = try @import("parameter_frame.zig").Frame.prepare(alloc, program.parameter_descriptors, &.{.{ .text = "a" }}, .{});
            defer frame.deinit();
            _ = try program.bindParameters(&frame);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{});
}

fn runEnumProbe(a: A, fixture: *const Fixture, expression: []const u8, input: ?[]const u8) !Datum {
    var parsed = try @import("compiler.zig").compileScalar(a, expression, .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &fixture.view }}, &.{}, .{});
    defer program.deinit();
    const result = try program.evaluate(a, &.{if (input) |text| try fixture.value(text) else .{}}, &.{}, .{});
    return operators.cloneDatum(a, result);
}

test "SQL named enum PostgreSQL oracle verifies complete scalar values NULLs and diagnostics" {
    const a = std.testing.allocator;
    const Probe = struct { expression: []const u8, input: ?[]const u8, value: std.json.Value = .null, sqlstate: ?[]const u8 = null };
    var golden = try std.json.parseFromSlice(struct { entries: []const Probe }, a, @import("parity_fixtures.zig").enum_execution_reference, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    var fixture = try Fixture.init(a, 5, 1);
    defer fixture.deinit(a);
    try std.testing.expect(golden.value.entries.len >= 180);
    for (golden.value.entries) |probe| {
        errdefer std.debug.print("PostgreSQL enum probe: {s} input={?s} expected_state={?s}\n", .{ probe.expression, probe.input, probe.sqlstate });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const actual = runEnumProbe(arena.allocator(), &fixture, probe.expression, probe.input) catch |err| {
            try std.testing.expectEqualStrings(probe.sqlstate orelse return err, @import("errors.zig").describe(err).code);
            continue;
        };
        try std.testing.expect(probe.sqlstate == null);
        if (probe.value == .null) {
            try std.testing.expect(actual.sql_null);
        } else {
            try std.testing.expect(!actual.sql_null);
            const output: std.json.Value = if (actual.enumeration()) |atom| .{ .string = atom.text } else actual.value;
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(output, probe.value));
        }
    }
}

test "SQL named enum disk hash joins retain typed keys payloads and duplicate fanout" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    const join = try operators.HashJoin.create(a, .{ .bytes = 16384, .spill = &manager });
    defer join.deinit();
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const text: [1024]u8 = @splat('x');
    const labels = [_][]const u8{ "z", "a", "" };
    for (0..192) |i| {
        const key = try source.value(labels[i % 3]);
        try join.add(&.{ key, Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = &text }) }, &.{key});
    }
    try std.testing.expect(join.disk != null);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (labels, 0..) |label, slot| {
        const key = try source.value(label);
        var probe = try join.probe(&.{key});
        var matches: usize = 0;
        while (try probe.next()) |match| {
            _ = scratch.reset(.retain_capacity);
            const cells = try match.materializeValues(scratch.allocator());
            try std.testing.expectEqualStrings(label, cells[0].enumeration().?.text);
            try std.testing.expectEqual(@as(i64, @intCast(slot)), @mod(cells[1].value.integer, 3));
            try std.testing.expectEqualStrings(&text, cells[2].value.string);
            matches += 1;
        }
        try std.testing.expectEqual(@as(usize, 64), matches);
    }
}

test "SQL named enum MIN MAX partial transport preserves selected value identity and ordering" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    for ([_]operators.Aggregate.Kind{ .min, .max }) |kind| {
        var state = try operators.Aggregate.init(a, kind, .enumeration);
        defer state.deinit();
        for ([_][]const u8{ "a", "", "z" }) |label| try state.update(try source.value(label));
        try state.update(.{});
        const expected = if (kind == .min) "z" else "";
        try std.testing.expectEqualStrings(expected, (try state.finish()).enumeration().?.text);
        const encoded = try @import("aggregate_partial.zig").encode(a, &.{state});
        defer {
            for (encoded) |cell| a.free(cell.value.string);
            a.free(encoded);
        }
        var owner = std.heap.ArenaAllocator.init(a);
        defer owner.deinit();
        var restored = try @import("aggregate_partial.zig").decode(owner.allocator(), encoded[0], .{ .kind = kind, .input_type = .enumeration });
        defer restored.deinit();
        try std.testing.expectEqualStrings(expected, (try restored.finish()).enumeration().?.text);
    }
}

test "SQL named enum retained columns share symbols without per row payload allocations" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var counter = std.testing.FailingAllocator.init(a, .{});
    var store = @import("typed_store.zig").Store.init(counter.allocator());
    defer store.deinit();
    const z = try source.value("z");
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..8192) |_| _ = try store.append(&.{z});
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const first = (try store.cell(none.allocator(), 0, 0)).enumeration().?;
    const last = (try store.cell(none.allocator(), 8191, 0)).enumeration().?;
    try std.testing.expect(first == last and first != z.enumeration().?);
    try std.testing.expect(counter.alloc_index < 100);
    try std.testing.expect(try store.appendBytes(&.{z}) <= 16);
    try std.testing.expect(try store.physicalColumn(none.allocator(), 0, 0, 8192) == null);
    std.debug.print("SQL enum retained symbols: rows=8192 allocations={} elapsed_ns={}\n", .{ counter.alloc_index, elapsed });
    // Widening a mixed producer after an enum-only prefix must preserve every
    // earlier extension and keep JSON null distinct from SQL NULL.
    _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
    _ = try store.append(&.{.{}});
    _ = try store.append(&.{Datum.json(.null)});
    try std.testing.expectEqualStrings("z", (try store.cell(none.allocator(), 0, 0)).enumeration().?.text);
    try std.testing.expectEqual(@as(i64, 7), (try store.cell(none.allocator(), 8192, 0)).value.integer);
    try std.testing.expect((try store.cell(none.allocator(), 8193, 0)).sql_null);
    try std.testing.expect(!(try store.cell(none.allocator(), 8194, 0)).sql_null);
    var forged = z.enumeration().?.*;
    forged.order_key = &.{1};
    try std.testing.expectError(error.InvalidSqlNamedType, store.append(&.{.typedEnum(&forged)}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, store.append(&.{z}));
}

test "SQL named enum assignment and shared parameter resolution reject incompatible domains at binding" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var other = try Fixture.init(a, 6, 1);
    defer other.deinit(a);
    var conflict = try @import("compiler.zig").compileScalar(a, "v = $1 AND w = $1", .{});
    defer conflict.deinit();
    try std.testing.expectError(error.ConflictingSqlParameterTypes, scalar.bindTyped(a, conflict.expression, &.{ .{ .name = "v", .type = .enumeration, .enum_view = &source.view }, .{ .name = "w", .type = .enumeration, .enum_view = &other.view } }, &.{.{}}, .{}));
    const target: scalar.Type = .{ .kind = .enumeration, .enum_view = &source.view };
    for ([_][]const u8{ "'z'", "NULL", "'z'::text" }, 0..) |text, index| {
        var parsed = try @import("compiler.zig").compileScalar(a, text, .{});
        defer parsed.deinit();
        if (index == 2) {
            try std.testing.expectError(error.SqlCommonTypeMismatch, scalar.bindTypedExpectedWithSettings(a, parsed.expression, &.{}, &.{}, target, .{ .assignment = true }, null));
            continue;
        }
        var program = try scalar.bindTypedExpectedWithSettings(a, parsed.expression, &.{}, &.{}, target, .{ .assignment = true }, null);
        defer program.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const value = try program.evaluate(arena.allocator(), &.{}, &.{}, .{});
        if (index == 0) try std.testing.expectEqualStrings("z", value.enumeration().?.text) else try std.testing.expect(value.sql_null);
    }
}

test "SQL named enum common domains select additive successor without retyping unrelated enums" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var successor = try types.Owned.add(a, source.view.record.definition, "new", .{ .before = "a" });
    defer successor.deinit();
    var record = source.view.record;
    record.identity.generation += 1;
    record.definition = successor.definition;
    var view = try enums.View.init(a, record);
    defer view.deinit(a);
    var parsed = try @import("compiler.zig").compileScalar(a, "greatest(v, w)", .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, &.{ .{ .name = "v", .type = .enumeration, .enum_view = &source.view }, .{ .name = "w", .type = .enumeration, .enum_view = &view } }, &.{}, .{});
    defer program.deinit();
    try std.testing.expectEqual(@as(usize, 4), program.output_type.enum_view.?.record.definition.labels.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const value = try program.evaluate(arena.allocator(), &.{ try source.value("z"), .typedEnum(try view.input("new")) }, &.{}, .{});
    try std.testing.expectEqualStrings("new", value.enumeration().?.text);
}

test "SQL named enum compatibility boundaries never reinterpret enum placeholders as JSON NULL" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const empty = try source.value("");
    var budget: @import("array_value.zig").Budget = .{};
    try std.testing.expectError(error.SqlTypeMismatch, @import("array_value.zig").validateElement(.jsonb, empty, &budget));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, @import("describe.zig").coerceDatum(arena.allocator(), empty, .json, null));
    const context: @import("runtime.zig").Context = .{ .alloc = a, .arena = arena.allocator(), .backend = undefined, .binding = .{ .table = null, .action = .read, .columns = &.{}, .parameter_types = &.{}, .json_literals = .empty }, .parameters = &.{}, .limits = .{} };
    try std.testing.expectEqualStrings("", (try context.outputDatum(empty, .enumeration, null)).string);
    try std.testing.expectError(error.SqlTypeMismatch, context.outputDatum(empty, .json, null));
    try std.testing.expectEqualStrings("", (try @import("runtime.zig").encodeStorageDatum(arena.allocator(), empty, .{ .name = "v", .path = "v", .type = .string }, 1024)).string);
}

test "SQL named enum retained numeric transitions preserve both domains in either order" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    var work: @import("array_value.zig").Budget = .{};
    const number = try scalar.numericTextLeaky(owner.allocator(), "9007199254740993.1250", &work);
    const atom = try source.value("z");
    for ([_]bool{ false, true }) |reverse| {
        var store = @import("typed_store.zig").Store.init(a);
        defer store.deinit();
        const values = if (reverse) [_]Datum{ number, atom } else [_]Datum{ atom, number };
        for (values) |value| _ = try store.append(&.{value});
        _ = try store.append(&.{.{}});
        for (values, 0..) |value, row| {
            const restored = try store.cell(owner.allocator(), row, 0);
            try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatums(value, restored));
            try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatum(restored));
        }
        try std.testing.expect((try store.cell(owner.allocator(), 2, 0)).sql_null);
        try std.testing.expect(try store.physicalColumn(owner.allocator(), 0, 0, 3) == null);
    }
}

test "SQL named enum literal input is prepared before lazy branches and rank work shares the quota" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    for ([_][]const u8{ "false AND v = 'Z'", "true OR v = 'Z'", "CASE WHEN false THEN 'Z' ELSE v END", "coalesce(v, 'Z')" }) |sql| {
        var parsed = try @import("compiler.zig").compileScalar(a, sql, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.SqlInvalidTextRepresentation, scalar.bind(a, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &source.view }}, &.{}, .{}));
    }
    var parsed = try @import("compiler.zig").compileScalar(a, "v < 'a'", .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &source.view }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expect(program.constant_inputs.count() != 0);
    var no_memory = std.heap.FixedBufferAllocator.init(&.{});
    const z = try source.value("z");
    const result = try program.evaluate(no_memory.allocator(), &.{z}, &.{}, .{});
    try std.testing.expect(!result.sql_null and result.value.bool);
    var work: @import("json_order.zig").Budget = .{ .remaining = 2 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, scalar.compareDatumsWithBudget(z, try source.value("a"), &work));
}

const EnumTableBackend = struct {
    fixture: *Fixture,
    fn resolve(raw: *anyopaque, a: A, name: @import("ast.zig").Name, _: @import("catalog.zig").Action) !@import("catalog.zig").Table {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, name.table, "enum_rows")) return error.UndefinedTable;
        const schema = @import("../storage/schema.zig");
        const columns = [_]schema.RelationalColumn{ .{ .name = "id", .path = "id", .column_type = .integer, .required = true }, .{ .name = "state", .path = "state", .column_type = .enumeration, .named_type_id = 5, .allows_null = true } };
        const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns, .named_types = &.{self.fixture.view.record} };
        return .{ .id = 1, .physical_name = "enum_rows", .schema_version = 1, .storage_mode = .relational, .columns = try @import("catalog.zig").relationalColumnsLeaky(a, table) };
    }
    fn scan(_: *anyopaque, a: A, table: @import("catalog.zig").Table, request: @import("catalog.zig").Scan) !@import("catalog.zig").Page {
        const catalog = @import("catalog.zig");
        if (request.after != null) return .{ .rows = &.{} };
        const input = try std.json.parseFromSliceLeaky(std.json.Value, a, "[{\"id\":1,\"state\":\"z\"},{\"id\":2,\"state\":\"a\"},{\"id\":3,\"state\":\"\"},{\"id\":4,\"state\":null}]", .{});
        var rows: std.ArrayList(catalog.Row) = .empty;
        for (input.array.items, 0..) |row, i| {
            var include = true;
            for (request.conditions) |condition| {
                const column = try table.column(condition.column);
                const value = try @import("document_row.zig").declaredCell(a, column, Datum.fromJson(row.object.get(condition.column) orelse .null));
                const actual = if (condition.op == .is_null) value.sql_null else if (condition.op == .is_not_null) !value.sql_null else compare: {
                    if (value.sql_null or condition.value == .null) break :compare false;
                    const bound = try @import("describe.zig").coerceDatumTyped(a, Datum.fromJson(condition.value), scalar.columnType(column));
                    const order = try scalar.compareDatums(value, bound);
                    break :compare switch (condition.op) {
                        .eq => order == .eq,
                        .neq => order != .eq,
                        .lt => order == .lt,
                        .lte => order != .gt,
                        .gt => order == .gt,
                        .gte => order != .lt,
                        else => return error.UnsupportedSqlShape,
                    };
                };
                include = include and actual;
            }
            if (include) try rows.append(a, .{ .id = try std.fmt.allocPrint(a, "{d}", .{i + 1}), .version = 1, .value = row });
        }
        return .{ .rows = try rows.toOwnedSlice(a) };
    }
    const Cursor = struct {
        a: A,
        source: *anyopaque,
        table: @import("catalog.zig").Table,
        request: @import("catalog.zig").Scan,
        done: bool = false,
        fn next(raw: *anyopaque, a: A, _: u32) !@import("catalog.zig").Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.done) return .{ .rows = &.{} };
            self.done = true;
            return EnumTableBackend.scan(self.source, a, self.table, self.request);
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.a.destroy(self);
        }
    };
    const Read = struct {
        a: A,
        cursors: []@import("catalog.zig").Cursor,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (self.cursors) |cursor| cursor.close(cursor.ptr);
            self.a.free(self.cursors);
            self.a.destroy(self);
        }
    };
    fn capture(raw: *anyopaque, a: A, scans: []const @import("catalog.zig").StatementScan) !@import("catalog.zig").StatementRead {
        const owner = try a.create(Read);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .cursors = try a.alloc(@import("catalog.zig").Cursor, scans.len) };
        var initialized: usize = 0;
        errdefer {
            for (owner.cursors[0..initialized]) |cursor| cursor.close(cursor.ptr);
            a.free(owner.cursors);
        }
        for (scans, owner.cursors) |request, *cursor| {
            const state = try a.create(Cursor);
            state.* = .{ .a = a, .source = raw, .table = request.table, .request = request.request };
            cursor.* = .{ .ptr = state, .next = Cursor.next, .close = Cursor.close };
            initialized += 1;
        }
        return .{ .ptr = owner, .cursors = owner.cursors, .close = Read.close };
    }
    fn mutate(_: *anyopaque, _: A, _: A, _: @import("catalog.zig").Table, _: []const @import("catalog.zig").Mutation) !@import("catalog.zig").MutationOutcome {
        return error.TestUnexpectedResult;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn backend(self: *@This()) @import("catalog.zig").Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .open_statement = capture } };
    }
};

test "SQL named enum declared tables preserve domains through relational graph execution" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var backend: EnumTableBackend = .{ .fixture = &source };
    for ([_]struct { sql: []const u8, rows: []const u8 }{
        .{ .sql = "SELECT state FROM enum_rows ORDER BY state", .rows = "[[\"z\"],[\"a\"],[\"\"],[null]]" },
        .{ .sql = "SELECT state FROM enum_rows WHERE state < 'a' ORDER BY id", .rows = "[[\"z\"]]" },
        .{ .sql = "SELECT state::text FROM enum_rows ORDER BY id", .rows = "[[\"z\"],[\"a\"],[\"\"],[null]]" },
        .{ .sql = "SELECT state::text FROM enum_rows ORDER BY state", .rows = "[[\"\"],[\"a\"],[\"z\"],[null]]" },
        .{ .sql = "SELECT state::text FROM enum_rows ORDER BY enum_rows.state", .rows = "[[\"z\"],[\"a\"],[\"\"],[null]]" },
        .{ .sql = "SELECT coalesce(state,'z') FROM enum_rows ORDER BY id", .rows = "[[\"z\"],[\"a\"],[\"\"],[\"z\"]]" },
        .{ .sql = "SELECT DISTINCT state FROM enum_rows ORDER BY state DESC NULLS LAST", .rows = "[[\"\"],[\"a\"],[\"z\"],[null]]" },
        .{ .sql = "SELECT state, count(*) FROM enum_rows GROUP BY state ORDER BY state", .rows = "[[\"z\",1],[\"a\",1],[\"\",1],[null,1]]" },
        .{ .sql = "SELECT min(state),max(state) FROM enum_rows", .rows = "[[\"z\",\"\"]]" },
        .{ .sql = "SELECT first_value(state) OVER (ORDER BY id) FROM enum_rows ORDER BY id", .rows = "[[\"z\"],[\"z\"],[\"z\"],[\"z\"]]" },
        .{ .sql = "SELECT lag(state) OVER (ORDER BY id) FROM enum_rows ORDER BY id", .rows = "[[null],[\"z\"],[\"a\"],[\"\"]]" },
        .{ .sql = "SELECT a.state FROM enum_rows a JOIN enum_rows b ON a.state=b.state ORDER BY a.id", .rows = "[[\"z\"],[\"a\"],[\"\"]]" },
        .{ .sql = "WITH q AS (SELECT state,id FROM enum_rows) SELECT state FROM q ORDER BY id", .rows = "[[\"z\"],[\"a\"],[\"\"],[null]]" },
        .{ .sql = "SELECT state FROM enum_rows UNION SELECT state FROM enum_rows ORDER BY state", .rows = "[[\"z\"],[\"a\"],[\"\"],[null]]" },
    }) |probe| {
        errdefer std.debug.print("enum declared-table graph: {s}\n", .{probe.sql});
        var compiled = try @import("compiler.zig").compile(a, probe.sql, .{});
        defer compiled.deinit();
        var result = try @import("runtime.zig").execute(a, backend.backend(), &compiled, &.{}, .{ .native_result = true });
        defer result.deinit();
        var expected = try std.json.parseFromSlice(std.json.Value, a, probe.rows, .{});
        defer expected.deinit();
        const actual = result.output.native_rows orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(expected.value.array.items.len, actual.len);
        for (expected.value.array.items, actual) |wanted, row| {
            try std.testing.expectEqual(wanted.array.items.len, row.len);
            for (wanted.array.items, row) |cell, value| {
                if (cell == .null) {
                    try std.testing.expect(value.sql_null);
                    continue;
                }
                try std.testing.expect(!value.sql_null);
                const logical: std.json.Value = if (value.enumeration()) |atom| .{ .string = atom.text } else value.value;
                try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(cell, logical));
            }
        }
    }
}

test "SQL named enum native execution preserves prepared input domains through scan predicates" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var fixture: EnumTableBackend = .{ .fixture = &source };
    var backend = fixture.backend();
    backend.parameter_descriptor_hints = &.{.{ .kind = .enumeration, .enum_view = &source.view }};
    var compiled = try @import("compiler.zig").compile(a, "SELECT state FROM enum_rows WHERE state = $1", .{});
    defer compiled.deinit();
    var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{.null}, .{ .native_result = true, .parameter_cells = &.{try source.value("a")} });
    defer result.deinit();
    const rows = result.output.native_rows orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("a", try source.view.output(rows[0][0].enumeration().?));
}

test "SQL named enum conflict update bindings preserve excluded and returning domains" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var fixture: EnumTableBackend = .{ .fixture = &source };
    var compiled = try @import("compiler.zig").compile(a, "INSERT INTO enum_rows (_id,id,state) VALUES ('new',5,'z') ON CONFLICT (_id) DO UPDATE SET state=excluded.state RETURNING state", .{});
    defer compiled.deinit();
    var description = try @import("describe.zig").describe(a, fixture.backend(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqual(@as(usize, 1), description.binding.columns.len);
    try std.testing.expect(scalar.sameEnumDomain(&source.view, description.binding.columns[0].enum_view));
}

test "SQL named enum durable views reject incomplete named outputs while explicit text remains executable" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var fixture: EnumTableBackend = .{ .fixture = &source };
    const views = @import("view_definition.zig");
    const Resolver = struct {
        fn resolve(raw: *anyopaque, alloc: A, name: @import("ast.zig").Name) !views.ResolvedSource {
            var table = try EnumTableBackend.resolve(raw, alloc, name, .read);
            table.scope = .{ .database = "default", .namespace = "public", .name = name.table, .revision = 1 };
            return .{ .table = table, .schema_digest = @splat(1) };
        }
    };
    const resolver: views.Resolver = .{ .ptr = &fixture, .resolve = Resolver.resolve, .checkpoint = EnumTableBackend.checkpoint };
    try std.testing.expectError(error.SqlTypeActivationRequired, views.prepare(a, resolver, "SELECT state FROM enum_rows", .{ .owner = .auth_disabled }));
    var text = try views.prepare(a, resolver, "SELECT state::text AS state FROM enum_rows", .{ .owner = .auth_disabled });
    defer text.deinit();
    try std.testing.expectEqual(@import("ast.zig").ColumnType.string, text.value.columns[0].type);
}

test "SQL named enum invocation descriptors compare immutable domains instead of owner addresses" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    var clone = try Fixture.init(a, 5, 1);
    defer clone.deinit(a);
    var unrelated = try Fixture.init(a, 6, 1);
    defer unrelated.deinit(a);
    const descriptor: scalar.Type = .{ .kind = .enumeration, .enum_view = &source.view };
    try std.testing.expect(scalar.sameParameterType(descriptor, .{ .kind = .enumeration, .enum_view = &clone.view }));
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const invocation = try @import("parameter_binding.zig").Invocation.initLeaky(region.allocator(), 1);
    try invocation.mergeInferred(&.{descriptor});
    try std.testing.expect(invocation.descriptors[0].enum_view != null);
    try invocation.mergeInferred(&.{.{ .kind = .enumeration, .enum_view = &clone.view }});
    try std.testing.expectError(error.ConflictingSqlParameterTypes, invocation.mergeInferred(&.{.{ .kind = .enumeration, .enum_view = &unrelated.view }}));
    try std.testing.expect(scalar.sameEnumDomain(invocation.descriptors[0].enum_view, &source.view));
}

test "SQL named enum contextual inputs remain nullable for required column comparisons" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const expression: @import("ast.zig").Scalar = .{ .literal = .{ .parameter = 1 } };
    var descriptors = [_]scalar.Type{.{}};
    _ = try scalar.inferTypedParametersExpected(a, &expression, &.{}, &descriptors, .{ .kind = .enumeration, .enum_view = &source.view, .nullable = false }, .{});
    try std.testing.expect(descriptors[0].nullable);
    var parsed = try @import("compiler.zig").compileScalar(a, "v = $1", .{});
    defer parsed.deinit();
    var program = try scalar.bind(a, parsed.expression, &.{.{ .name = "v", .type = .enumeration, .enum_view = &source.view, .nullable = false }}, &.{}, .{});
    defer program.deinit();
    try std.testing.expect(program.parameter_descriptors[0].nullable);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    try std.testing.expect((try program.evaluate(region.allocator(), &.{try source.value("z")}, &.{.null}, .{})).sql_null);
}

test "SQL named enum native column streams preserve direct projections and scalar error recovery" {
    const a = std.testing.allocator;
    var source = try Fixture.init(a, 5, 1);
    defer source.deinit(a);
    const catalog = @import("catalog.zig");
    const Native = struct {
        source: EnumTableBackend,
        invalid: bool,
        done: bool = false,
        fn resolve(raw: *anyopaque, alloc: A, name: @import("ast.zig").Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return EnumTableBackend.resolve(&self.source, alloc, name, action);
        }
        fn open(raw: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return .{ .ptr = raw, .next = rows, .next_columns = columns, .close = close };
        }
        fn rows(_: *anyopaque, _: A, _: u32) !catalog.Page {
            return error.UnexpectedRowScan;
        }
        fn close(_: *anyopaque) void {}
        fn cell(raw: *anyopaque, _: A, row: usize, column: usize) anyerror!Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (column == 0) return .json(.{ .integer = @intCast(row + 1) });
            if (row == 3) return .{};
            if (self.invalid and row == 0) return .json(.{ .string = "Z" });
            return self.source.fixture.value(([_][]const u8{ "z", "a", "" })[row]);
        }
        fn columns(raw: *anyopaque, alloc: A, wanted: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.done) return .{ .selection = &.{} };
            try std.testing.expect(wanted >= 4);
            self.done = true;
            const batch = try alloc.create(@import("execution_batch.zig").Batch);
            batch.* = .{ .reader = .{ .ptr = self, .read = cell, .count = 4, .width = 2 } };
            return .{ .native = .{ .values = batch, .names = &.{ "id", "state" } }, .selection = &.{ 0, 1, 2, 3 } };
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = EnumTableBackend.scan, .open_scan = open, .mutate = EnumTableBackend.mutate, .checkpoint = EnumTableBackend.checkpoint } };
        }
    };
    for ([_]bool{ false, true }) |invalid| {
        var native: Native = .{ .source = .{ .fixture = &source }, .invalid = invalid };
        var compiled = try @import("compiler.zig").compile(a, "SELECT state FROM enum_rows", .{});
        defer compiled.deinit();
        const stream = (try @import("read_stream.zig").Stream.open(a, native.backend(), &compiled, &.{}, .{})) orelse return error.TestUnexpectedResult;
        defer stream.close();
        if (invalid) {
            try std.testing.expectError(error.SqlInvalidTextRepresentation, stream.nextBatch(8));
        } else {
            var page = try stream.nextBatch(8);
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 4), page.values.len());
            for ([_][]const u8{ "z", "a", "" }, 0..) |label, row| {
                const value = try page.values.cell(page.arena.allocator(), row, 0);
                try std.testing.expectEqualStrings(label, try source.view.output(value.enumeration().?));
            }
            try std.testing.expect((try page.values.cell(page.arena.allocator(), 3, 0)).sql_null);
        }
    }
}
