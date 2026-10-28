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
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(scalar.Type, a, encoded, .{}));
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
    try std.testing.expectError(error.UnsupportedSqlShape, @import("runtime.zig").encodeStorageDatum(arena.allocator(), empty, .{ .name = "v", .path = "v", .type = .string }, 1024));
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
