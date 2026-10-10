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

//! Typed relational iterator execution. All physical cursors open together;
//! hash join retains one build side and streams the probe side under quota.

/// Search requests are execution-local, including statement captures used by
/// mutation inputs. Never write parameter values into the immutable plan.
pub fn bindSearchScans(a: Allocator, input_scans: []const catalog.StatementScan, parameters: []const std.json.Value) ![]const catalog.StatementScan {
    const has_search = for (input_scans) |scan| {
        if (scan.request.search != null) break true;
    } else false;
    return if (has_search) bound: {
        const bound_scans = try a.dupe(catalog.StatementScan, input_scans);
        for (bound_scans) |*scan| if (scan.request.search) |original| {
            const search = try a.create(catalog.Scan.Search);
            search.* = original.*;
            scan.request.search = search;
            const expression = search.expression;
            const request = if (expression.request == .parameter) parameters[expression.request.parameter - 1] else try describe.bindLiteral(a, expression.request, .string);
            if (request != .string) return error.InvalidSqlParameters;
            search.request_text = request.string;
            if (expression.limit) |input| {
                const limit = if (input == .parameter) parameters[input.parameter - 1] else try describe.bindLiteral(a, input, .integer);
                const number = if (limit == .integer) limit.integer else if (limit == .string) std.fmt.parseInt(i64, limit.string, 10) catch return error.InvalidSqlParameters else return error.InvalidSqlParameters;
                if (number < 1 or number > 10000) return error.InvalidSqlParameters;
                search.limit = @intCast(number);
            }
        };
        break :bound bound_scans;
    } else input_scans;
}

const std = @import("std");
const catalog = @import("catalog.zig");
const binding = @import("relation_binding.zig");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const describe = @import("describe.zig");
const Datum = scalar.Datum;
const Allocator = std.mem.Allocator;
const Worklist = @import("recursive_worklist.zig").Worklist;
const Replay = @import("replay_rows.zig").Replay;

fn dependsOn(node: *const binding.Node, id: usize) bool {
    return switch (node.operation) {
        .recursive_ref => |reference| reference == id,
        .materialized_ref => |source| dependsOn(source, id),
        .query => |query| dependsOn(query.source, id),
        .join => |join| dependsOn(join.left, id) or dependsOn(join.right, id),
        .apply => |apply| dependsOn(apply.left, id) or dependsOn(apply.right, id),
        .set => |set| dependsOn(set.left, id) or dependsOn(set.right, id),
        .values => |arms| for (arms) |arm| {
            if (dependsOn(arm, id)) break true;
        } else false,
        else => false,
    };
}

fn outerMask(node: *const binding.Node) u32 {
    return switch (node.operation) {
        .outer_ref => |id| @as(u32, 1) << @intCast(id),
        .materialized_ref => |source| outerMask(source),
        .query => |query| outerMask(query.source),
        .join => |join| outerMask(join.left) | outerMask(join.right),
        .apply => |apply| (outerMask(apply.left) | outerMask(apply.right)) & ~(@as(u32, 1) << @intCast(apply.id)),
        .set => |set| outerMask(set.left) | outerMask(set.right),
        .recursive => |part| outerMask(part.seed) | outerMask(part.step),
        .values => |arms| blk: {
            var mask: u32 = 0;
            for (arms) |arm| mask |= outerMask(arm);
            break :blk mask;
        },
        else => 0,
    };
}

const SetTestBackend = struct {
    checkpoints: usize = 0,
    cancel_after: usize = std.math.maxInt(usize),
    fn resolve(_: *anyopaque, _: Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedBackendCall;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
        if (self.checkpoints > self.cancel_after) return error.Canceled;
    }
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

test "SQL row membership executes captured typed relations with NULL-aware truth and query boundaries" {
    const Case = struct { sql: []const u8, rows: []const u8 };
    const Fixture = struct {
        fn run(a: Allocator) !void {
            for ([_]Case{
                .{ .sql = "SELECT (1,2) IN (SELECT 1,2), (1,2) NOT IN (SELECT 1,2)", .rows = "[[true,false]]" },
                .{ .sql = "SELECT (NULL,1) IN (SELECT 2,2), (NULL,1) IN (SELECT 2,1)", .rows = "[[false,null]]" },
                .{ .sql = "SELECT (NULL,NULL) IN (SELECT 1,2 WHERE false), (NULL,NULL) NOT IN (SELECT 1,2 WHERE false)", .rows = "[[false,true]]" },
                .{ .sql = "SELECT (1,2) IN (SELECT a,b FROM (VALUES(1,2),(1,2),(NULL,2)) s(a,b))", .rows = "[[true]]" },
                .{ .sql = "SELECT (1,2) IN (SELECT a,b FROM (VALUES(1,2),(1,3)) s(a,b) ORDER BY b DESC LIMIT 1)", .rows = "[[false]]" },
                .{ .sql = "SELECT (1,2) IN (SELECT a,count(*) FROM (VALUES(1),(1)) s(a) GROUP BY a)", .rows = "[[true]]" },
                .{ .sql = "SELECT (ARRAY[1,NULL]::int8[],1) IN (SELECT ARRAY[1,NULL]::int8[],1)", .rows = "[[true]]" },
                .{ .sql = "SELECT ROW(1) IN (SELECT 1), ROW(1,2) IN (SELECT 1,2)", .rows = "[[true,true]]" },
                .{ .sql = "SELECT ('null'::jsonb,1) IN (SELECT 'null'::jsonb,1), (NULL::jsonb,1) IN (SELECT 'null'::jsonb,1)", .rows = "[[true,null]]" },
                .{ .sql = "SELECT CASE WHEN false THEN (1,2) IN (SELECT 1/0,2) ELSE false END", .rows = "[[false]]" },
                .{ .sql = "SELECT CASE WHEN t.k THEN (t.a,t.b) IN (SELECT a,b FROM (VALUES(1,2),(1,NULL)) s(a,b)) ELSE false END FROM (VALUES(1,2,true),(1,3,true)) t(a,b,k)", .rows = "[[true],[null]]" },
                .{ .sql = "SELECT (t.a,t.b) IN (SELECT s.a,s.b FROM (VALUES(1,2,7),(1,NULL,8),(1,2,NULL)) s(a,b,k) WHERE s.k=t.k) FROM (VALUES(1,2,7),(1,2,8),(1,2,NULL)) t(a,b,k)", .rows = "[[true],[null],[false]]" },
                .{ .sql = "SELECT (t.a,t.b) IN (SELECT 1/s.x,2 FROM (VALUES(1,7),(0,8)) s(x,k) WHERE s.k=t.k) FROM (VALUES(1,2,7)) t(a,b,k)", .rows = "[[true]]" },
                .{ .sql = "SELECT (t.a,t.b) IN (SELECT s.a,s.b FROM (VALUES(1,2,7),(1,3,7)) s(a,b,k) WHERE s.k=t.k ORDER BY b DESC LIMIT 1) FROM (VALUES(1,2,7),(1,3,7)) t(a,b,k)", .rows = "[[false],[true]]" },
            }) |case| {
                var backend: SetTestBackend = .{};
                var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
                defer compiled.deinit();
                var result = try @import("runtime.zig").execute(a, backend.backend(), &compiled, &.{}, .{});
                defer result.deinit();
                const rows = try std.json.Stringify.valueAlloc(a, result.output.rows, .{});
                defer a.free(rows);
                try std.testing.expectEqualStrings(case.rows, rows);
                for (result.output.columns) |column| try std.testing.expectEqual(@import("ast.zig").ColumnType.boolean, column.type);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
}

test "SQL row membership retained Apply builds unwind every allocation failure" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var backend: SetTestBackend = .{};
            var compiled = try @import("compiler.zig").compile(a, "SELECT CASE WHEN t.k THEN (t.a,t.b) IN (SELECT a,b FROM (VALUES(1,2),(1,NULL)) s(a,b)) ELSE false END FROM (VALUES(1,2,true),(1,3,true)) t(a,b,k)", .{});
            defer compiled.deinit();
            var result = try @import("runtime.zig").execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expect(result.output.rows[0][0].bool);
            try std.testing.expect(result.output.sql_nulls.?[1][0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL row membership rejects unequal arity and incompatible operator signatures before opening sources" {
    for ([_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "SELECT (1,2) IN (SELECT 1)", .err = error.InvalidSqlSyntax },
        .{ .sql = "SELECT (1,2) IN (SELECT 1,2,3)", .err = error.InvalidSqlSyntax },
        .{ .sql = "SELECT (ARRAY[1]::int2[],1) IN (SELECT ARRAY[1]::int8[],1)", .err = error.SqlUndefinedOperator },
        .{ .sql = "SELECT (1,2) IN (SELECT true,2)", .err = error.SqlUndefinedOperator },
    }) |case| {
        var backend: SetTestBackend = .{};
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.err, @import("runtime.zig").execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
}

test "SQL typed array rows survive scalar join and grouped relation adapters" {
    const Fixture = struct {
        const State = struct {
            emitted: bool = false,
            fn next(raw: *anyopaque, a: Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.emitted or limit == 0) return .{ .rows = &.{} };
                self.emitted = true;
                const rows = try a.alloc(catalog.Row, 1);
                var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
                rows[0] = try catalog.Row.fromDatums(a, "row", try catalog.Row.TypedLayout.init(a, &.{"a"}), &.{Datum.typedArray(&array)});
                return .{ .rows = rows };
            }
            fn close(_: *anyopaque) void {}
        };
        const Owner = struct {
            arena: std.heap.ArenaAllocator,
            backing: Allocator,
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const a = self.backing;
                self.arena.deinit();
                a.destroy(self);
            }
        };
        fn resolve(_: *anyopaque, _: Allocator, name: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "a", .path = "a", .type = .array, .element_type = .int64 }} };
        }
        fn scan(_: *anyopaque, a: Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
            var state: State = .{};
            return State.next(&state, a, request.limit);
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn capture(_: *anyopaque, a: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const owner = try a.create(Owner);
            owner.* = .{ .arena = .init(a), .backing = a };
            errdefer Owner.close(owner);
            const states = try owner.arena.allocator().alloc(State, scans.len);
            const cursors = try owner.arena.allocator().alloc(catalog.Cursor, scans.len);
            for (states, cursors) |*state, *cursor| {
                state.* = .{};
                cursor.* = .{ .ptr = state, .next = State.next, .close = State.close };
            }
            return .{ .ptr = owner, .cursors = cursors, .close = Owner.close };
        }
        fn run(a: Allocator) !void {
            var dummy: u8 = 0;
            const backend: catalog.Backend = .{ .ptr = &dummy, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = SetTestBackend.mutate, .checkpoint = checkpoint, .open_statement = capture } };
            const Case = struct { sql: []const u8, expected: []const u8 };
            for ([_]Case{
                .{ .sql = "SELECT cardinality(a), array_lower(a, 1), array_upper(a, 1), 9007199254740993 = ANY(a), 9007199254740992 = ANY(a) FROM items", .expected = "[[\"2\",\"-3\",\"-2\",true,null]]" },
                .{ .sql = "SELECT cardinality(t.a), array_lower(t.a, 1), 9007199254740993 = ANY(t.a) FROM items t CROSS JOIN items u", .expected = "[[\"2\",\"-3\",true]]" },
                .{ .sql = "SELECT sum(cardinality(t.a)) FROM items t CROSS JOIN items u", .expected = "[[\"2\"]]" },
            }) |case| {
                var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
                defer compiled.deinit();
                var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{});
                defer result.deinit();
                const encoded = try std.json.Stringify.valueAlloc(a, result.output.rows, .{});
                defer a.free(encoded);
                try std.testing.expectEqualStrings(case.expected, encoded);
            }
            var array_output = try @import("compiler.zig").compile(a, "SELECT a FROM items", .{});
            defer array_output.deinit();
            var result = try @import("runtime.zig").execute(a, backend, &array_output, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            const envelope = result.output.rows[0][0];
            try std.testing.expectEqualStrings("9007199254740993", envelope.object.get("values").?.array.items[0].string);
            try std.testing.expect(envelope.object.get("sql_nulls").?.array.items[1].bool);
            try std.testing.expectEqual(@as(i64, -3), envelope.object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL internal array query boundaries preserve values in memory and spill execution" {
    const Case = struct { sql: []const u8, expected: []const u8 };
    for ([_]bool{ false, true }) |with_io| {
        var fixture: SetTestBackend = .{};
        var backend = fixture.backend();
        backend.execution_io = if (with_io) std.testing.io else null;
        for ([_]Case{
            .{ .sql = "WITH q AS (SELECT ARRAY[1,NULL,3]::bigint[] a) SELECT cardinality(a), array_length(a,1) FROM q", .expected = "[[\"3\",\"3\"]]" },
            .{ .sql = "WITH q AS MATERIALIZED (SELECT ARRAY[1,NULL,3]::bigint[] a) SELECT cardinality(a), array_lower(a,1) FROM q", .expected = "[[\"3\",\"1\"]]" },
            .{ .sql = "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL,3]::bigint[] a ORDER BY 1) q", .expected = "[[\"3\"]]" },
            .{ .sql = "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL,3]::bigint[] a UNION ALL SELECT ARRAY[4]::bigint[]) q ORDER BY 1", .expected = "[[\"1\"],[\"3\"]]" },
            .{ .sql = "SELECT cardinality(p), array_length(p,1), 1.5 = ANY(p), 2.5 = ANY(p), 2.0 = ANY(p) FROM (SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) p FROM (SELECT 1.0 x UNION ALL SELECT 3.0 x) t) q", .expected = "[[\"3\",\"3\",true,true,null]]" },
            .{ .sql = "SELECT cardinality(a), row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,2]::bigint[] a UNION ALL SELECT ARRAY[3]::bigint[]) q ORDER BY 1", .expected = "[[\"1\",\"1\"],[\"2\",\"2\"]]" },
            .{ .sql = "SELECT cardinality((SELECT ARRAY[1,NULL,3]::bigint[]))", .expected = "[[\"3\"]]" },
        }) |case| {
            var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
            defer compiled.deinit();
            var result = try @import("runtime.zig").execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
            defer std.testing.allocator.free(encoded);
            try std.testing.expectEqualStrings(case.expected, encoded);
        }
    }
}

test "SQL typed blocking query boundaries unwind allocation failures" {
    const Scenario = struct {
        fn run(a: Allocator, with_io: bool) !void {
            var fixture: SetTestBackend = .{};
            var backend = fixture.backend();
            backend.execution_io = if (with_io) std.testing.io else null;
            for ([_][]const u8{
                "SELECT cardinality((SELECT ARRAY[1,NULL,3]::bigint[]))",
                "SELECT cardinality(a), row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,2]::bigint[] a UNION ALL SELECT ARRAY[3]::bigint[]) q ORDER BY 1",
            }) |sql| {
                var compiled = try @import("compiler.zig").compile(a, sql, .{});
                defer compiled.deinit();
                var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{ .page_rows = 1 });
                defer result.deinit();
                try std.testing.expect(result.output.rows.len != 0);
            }
        }
    };
    for ([_]bool{ false, true }) |with_io| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{with_io});
}

test "SQL public array results preserve descriptors through constant set window and scalar paths" {
    const arrays = @import("array_value.zig");
    const Case = struct { sql: []const u8, kind: arrays.ElementType, expected: ?[]const u8 };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |with_io| for ([_]Case{
        .{ .sql = "SELECT ARRAY[-9223372036854775808,NULL,9223372036854775807]::bigint[]", .kind = .int64, .expected = "{-9223372036854775808,NULL,9223372036854775807}" },
        .{ .sql = "SELECT '[0:1][3:4]={{1,NULL},{3,4}}'::int4[]", .kind = .int32, .expected = "[0:1][3:4]={{1,NULL},{3,4}}" },
        .{ .sql = "SELECT ARRAY[]::text[]", .kind = .text, .expected = "{}" },
        .{ .sql = "SELECT NULL::int4[]", .kind = .int32, .expected = null },
        .{ .sql = "SELECT ARRAY['null'::jsonb,NULL,'{\"a\":[1,2]}'::jsonb]::jsonb[]", .kind = .jsonb, .expected = "{\"null\",NULL,\"{\\\"a\\\":[1,2]}\"}" },
        .{ .sql = "SELECT a FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q ORDER BY 1", .kind = .int64, .expected = "{1,NULL}" },
        .{ .sql = "SELECT a FROM (VALUES(ARRAY[1,NULL]::int2[]),(ARRAY[1,NULL]::float8[])) q(a) LIMIT 1", .kind = .float64, .expected = "{1,NULL}" },
        .{ .sql = "SELECT a,row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,NULL]::int4[] a) q", .kind = .int32, .expected = "{1,NULL}" },
        .{ .sql = "SELECT (SELECT ARRAY[1,NULL]::int4[])", .kind = .int32, .expected = "{1,NULL}" },
    }) |entry| {
        var fixture: SetTestBackend = .{};
        var backend = fixture.backend();
        backend.execution_io = if (with_io) std.testing.io else null;
        var compiled = try @import("compiler.zig").compile(a, entry.sql, .{});
        defer compiled.deinit();
        var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{ .page_rows = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqual(@import("ast.zig").ColumnType.array, result.output.columns[0].type);
        try std.testing.expectEqual(entry.kind, result.output.columns[0].element_type.?);
        const cell = result.output.rows[0][0];
        const flag = result.output.sql_nulls.?[0][0];
        if (entry.expected) |text| {
            try std.testing.expect(!flag);
            var decoded = try @import("array_wire.zig").decode(a, entry.kind, cell, .{});
            defer decoded.deinit();
            var expected = try @import("array_text.zig").decode(a, entry.kind, text, .{});
            defer expected.deinit();
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try expected.value.compare(decoded.value, &work));
        } else try std.testing.expect(flag and cell == .null);
    };
}

test "SQL common array types are applied before pairwise set identity and VALUES emission" {
    const Case = struct { sql: []const u8, expected: []const u8 };
    for ([_]bool{ false, true }) |with_io| {
        var fixture: SetTestBackend = .{};
        var backend = fixture.backend();
        backend.execution_io = if (with_io) std.testing.io else null;
        for ([_]Case{
            .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q", .expected = "[[\"1\"]]" },
            .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a INTERSECT SELECT ARRAY[1]::float8[]) q", .expected = "[[\"1\"]]" },
            .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a EXCEPT SELECT ARRAY[1]::float8[]) q", .expected = "[[\"0\"]]" },
            .{ .sql = "SELECT cardinality(a),2.5=ANY(a) FROM (VALUES(ARRAY[1]::int2[]),(ARRAY[2.5]::float4[])) q(a) ORDER BY 1,2", .expected = "[[\"1\",false],[\"1\",true]]" },
            .{ .sql = "SELECT count(*) FROM ((SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::int8[]) UNION ALL SELECT ARRAY[1]::float4[]) q", .expected = "[[\"3\"]]" },
            .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::float4[]) q", .expected = "[[\"1\"]]" },
            .{ .sql = "SELECT cardinality(a),1=ANY(a) FROM (SELECT '{1,NULL}' a UNION SELECT ARRAY[1,NULL]::int4[]) q", .expected = "[[\"2\",true]]" },
            .{ .sql = "SELECT count(*) FROM (SELECT NULL::int2[] a UNION SELECT NULL::float8[]) q", .expected = "[[\"1\"]]" },
            .{ .sql = "SELECT x FROM (VALUES(NULL),(NULL),(1)) q(x) ORDER BY x", .expected = "[[\"1\"],[null],[null]]" },
        }) |case| {
            errdefer std.debug.print("common type query: {s}, io: {}\n", .{ case.sql, with_io });
            var compiled = try @import("compiler.zig").compile(std.testing.allocator, case.sql, .{});
            defer compiled.deinit();
            var result = try @import("runtime.zig").execute(std.testing.allocator, backend, &compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
            defer std.testing.allocator.free(encoded);
            try std.testing.expectEqualStrings(case.expected, encoded);
        }
    }
}

test "SQL common array coercions release every partial set and VALUES allocation" {
    const Fixture = struct {
        fn run(a: Allocator, with_io: bool) !void {
            var fixture: SetTestBackend = .{};
            var backend = fixture.backend();
            backend.execution_io = if (with_io) std.testing.io else null;
            for ([_][]const u8{
                "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q",
                "SELECT cardinality(a) FROM (VALUES(ARRAY[1,NULL]::int2[]),(ARRAY[1,NULL]::float8[])) q(a)",
            }) |sql| {
                var compiled = try @import("compiler.zig").compile(a, sql, .{});
                defer compiled.deinit();
                var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{ .page_rows = 1 });
                defer result.deinit();
                try std.testing.expect(result.output.rows.len != 0);
            }
        }
    };
    for ([_]bool{ false, true }) |with_io| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{with_io});
}

test "SQL materialized relation replay spills once and shares a single statement capture" {
    const Fixture = struct {
        offset: usize = 0,
        captures: usize = 0,
        closes: usize = 0,
        cursors: [1]catalog.Cursor = undefined,
        const count = 4096;
        fn resolve(_: *anyopaque, _: Allocator, name: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn next(raw: *anyopaque, a: Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const length = @min(@as(usize, limit), count - self.offset);
            const rows = try a.alloc(catalog.Row, length);
            for (rows, 0..) |*row, i| {
                var object: std.json.ObjectMap = .empty;
                try object.put(a, "n", .{ .integer = @intCast(self.offset + i) });
                row.* = .{ .id = "row", .version = 1, .value = .{ .object = object } };
            }
            self.offset += length;
            return .{ .rows = rows, .after = if (self.offset == count) null else "more" };
        }
        fn capture(raw: *anyopaque, _: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), scans.len);
            self.captures += 1;
            self.cursors[0] = .{ .ptr = self, .next = next, .close = undefined };
            return .{ .ptr = self, .cursors = &self.cursors, .close = close };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |to_disk| {
        var fixture: Fixture = .{};
        var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.checkpoint };
        defer manager.deinit();
        const backend: catalog.Backend = .{ .ptr = &fixture, .spill_manager = if (to_disk) &manager else null, .vtable = &.{ .resolve = Fixture.resolve, .scan = SetTestBackend.scan, .mutate = SetTestBackend.mutate, .checkpoint = Fixture.checkpoint, .open_statement = Fixture.capture } };
        var compiled = try @import("compiler.zig").compile(a, "WITH cached AS MATERIALIZED (SELECT n FROM items) SELECT count(*) FROM cached a JOIN cached b ON a.n=b.n", .{});
        defer compiled.deinit();
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 16384 });
        defer result.deinit();
        try std.testing.expectEqualStrings("4096", result.output.rows[0][0].string);
        try std.testing.expectEqual(@as(usize, 1), fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        try std.testing.expectEqual(@as(usize, Fixture.count), fixture.offset);
        try std.testing.expectEqual(to_disk, manager.written_bytes > 0);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        std.debug.print("SQL materialized replay: input_rows=4096 captures=1 peak_memory={} spill_written={} elapsed_ns={}\n", .{ result.peakMemoryBytes(), manager.written_bytes, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
    }
}

test "SQL set operations preserve multiplicities precedence and output ordering" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    const Case = struct { sql: []const u8, expected: []const i64 };
    var backend: SetTestBackend = .{};
    for ([_]Case{
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY x DESC", .expected = &.{ 2, 1, 1 } },
        .{ .sql = "SELECT 1 AS x UNION SELECT 1 UNION SELECT 2 ORDER BY x", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT 1 AS x UNION SELECT 2 INTERSECT SELECT 2 ORDER BY x", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 EXCEPT ALL SELECT 1", .expected = &.{1} },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 EXCEPT SELECT 1", .expected = &.{} },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 INTERSECT ALL SELECT 1", .expected = &.{ 1, 1 } },
        .{ .sql = "SELECT 1 AS x INTERSECT (SELECT 1 UNION ALL SELECT 1)", .expected = &.{1} },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1) AS l INTERSECT ALL (SELECT 1 UNION ALL SELECT 1)", .expected = &.{ 1, 1 } },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1) AS l INTERSECT (SELECT 1 UNION ALL SELECT 1)", .expected = &.{1} },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) AS l EXCEPT ALL (SELECT 1 UNION ALL SELECT 1)", .expected = &.{2} },
        .{ .sql = "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY x LIMIT 1 OFFSET 1", .expected = &.{2} },
        .{ .sql = "SELECT 1 AS x WHERE TRUE UNION SELECT 2 WHERE FALSE", .expected = &.{1} },
        .{ .sql = "(SELECT 1 AS x UNION SELECT 2 LIMIT 1) UNION SELECT 3 ORDER BY x", .expected = &.{ 1, 3 } },
        .{ .sql = "((SELECT 1 AS x UNION SELECT 2)) EXCEPT SELECT 2", .expected = &.{1} },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (result.output.rows, case.expected) |row, expected| try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, row[0].string, 10));
    }
}

test {
    _ = @import("set_spill.zig");
}

test "SQL small sets preserve the hash and UNION ALL fast paths without temporary files" {
    const a = std.testing.allocator;
    var fixture: SetTestBackend = .{};
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = SetTestBackend.checkpoint };
    defer manager.deinit();
    var backend = fixture.backend();
    backend.spill_manager = &manager;
    for ([_]struct { sql: []const u8, count: usize }{
        .{ .sql = "SELECT 1 n UNION SELECT 1 UNION SELECT 2", .count = 2 },
        .{ .sql = "SELECT 1 n UNION ALL SELECT 1 UNION ALL SELECT 2", .count = 3 },
        .{ .sql = "SELECT 1 n INTERSECT SELECT 1", .count = 1 },
        .{ .sql = "SELECT 1 n EXCEPT SELECT 2", .count = 1 },
    }) |case| {
        var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
        defer compiled.deinit();
        var result = try @import("runtime.zig").execute(a, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.output.rows.len);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.written_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL set promotion bounds high cardinality memory without replaying prior output" {
    const Fixture = struct {
        const count = 16384;
        const Cursor = struct {
            offset: usize = 0,
            fn next(raw: *anyopaque, a: Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const length = @min(@as(usize, limit), count - self.offset);
                const rows = try a.alloc(catalog.Row, length);
                for (rows, 0..) |*row, i| {
                    var object: std.json.ObjectMap = .empty;
                    try object.put(a, "n", .{ .integer = @intCast((self.offset + i) / 2) });
                    row.* = .{ .id = "row", .version = 1, .value = .{ .object = object } };
                }
                self.offset += length;
                return .{ .rows = rows, .after = if (self.offset == count) null else "more" };
            }
        };
        inputs: [2]Cursor = .{ .{}, .{} },
        cursors: [2]catalog.Cursor = undefined,
        captures: usize = 0,
        closes: usize = 0,
        fn resolve(_: *anyopaque, _: Allocator, name: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn capture(raw: *anyopaque, _: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 2), scans.len);
            self.captures += 1;
            for (&self.cursors, &self.inputs) |*cursor, *input| cursor.* = .{ .ptr = input, .next = Cursor.next, .close = undefined };
            return .{ .ptr = self, .cursors = &self.cursors, .close = close };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, expected: []const u8, sum: []const u8, maximum: usize, multiplicity: u16 };
    const profile = try std.json.parseFromSlice(struct { format: u8, input_rows: usize, entries: []const Case }, a, @import("parity_fixtures.zig").set_spill_reference, .{});
    defer profile.deinit();
    try std.testing.expectEqual(@as(u8, 1), profile.value.format);
    try std.testing.expectEqual(@as(usize, Fixture.count), profile.value.input_rows);
    try std.testing.expectEqual(@as(usize, 5), profile.value.entries.len);
    for (profile.value.entries) |case| {
        for ([_]bool{ false, true }) |with_sum| {
            for ([_]bool{ false, true }) |with_spill| {
                if (!with_spill and with_sum) continue;
                var fixture: Fixture = .{};
                var quota: @import("memory_budget.zig") = .{ .backing = a, .limit = 2 * 1024 * 1024 };
                defer std.debug.assert(quota.live == 0);
                var manager: @import("spill.zig").Manager = .{ .alloc = quota.allocator(), .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.checkpoint };
                defer manager.deinit();
                const backend: catalog.Backend = .{ .ptr = &fixture, .spill_manager = if (with_spill) &manager else null, .vtable = &.{ .resolve = Fixture.resolve, .scan = SetTestBackend.scan, .mutate = SetTestBackend.mutate, .checkpoint = Fixture.checkpoint, .open_statement = Fixture.capture } };
                const sql = try std.fmt.allocPrint(a, "SELECT {s} FROM ({s}) q", .{ if (with_sum) "count(*),sum(n)" else "count(*)", case.sql });
                defer a.free(sql);
                var compiled = try @import("compiler.zig").compile(a, sql, .{});
                defer compiled.deinit();
                const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                var result = @import("runtime.zig").execute(quota.allocator(), backend, &compiled, &.{}, .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 65536 }) catch |err| {
                    if (!with_spill and err == error.SqlProgramLimitExceeded) {
                        std.debug.print("SQL set no-spill baseline exceeded 2 MiB: {s}\n", .{case.sql});
                        continue;
                    }
                    std.debug.print("SQL set load failure: {s} peak={} spilled={} consumed={}/{}\n", .{ case.sql, quota.peak, manager.written_bytes, fixture.inputs[0].offset, fixture.inputs[1].offset });
                    return err;
                };
                defer result.deinit();
                try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
                if (with_sum) try std.testing.expectEqualStrings(case.sum, result.output.rows[0][1].string);
                try std.testing.expectEqual(@as(usize, 1), fixture.captures);
                try std.testing.expectEqual(@as(usize, 1), fixture.closes);
                try std.testing.expectEqual(with_spill, manager.written_bytes > 0);
                try std.testing.expect(quota.peak <= 2 * 1024 * 1024);
                try std.testing.expectEqual(@as(usize, 0), manager.files);
                try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
                std.debug.print("SQL bounded set: spill={} input_rows=32768 output_rows={s} peak_memory={} spill_written={} elapsed_ns={}\n", .{ with_spill, case.expected, quota.peak, manager.written_bytes, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
            }
        }
        const Observer = struct {
            counts: [8192]u16 = @splat(0),
            fn append(raw: *anyopaque, values: []const Datum) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try std.testing.expectEqual(@as(usize, 1), values.len);
                try std.testing.expect(!values[0].sql_null);
                try std.testing.expect(values[0].value == .integer);
                const n = std.math.cast(usize, values[0].value.integer) orelse return error.UnexpectedSetValue;
                try std.testing.expect(n < self.counts.len);
                self.counts[n] = std.math.add(u16, self.counts[n], 1) catch return error.UnexpectedSetMultiplicity;
            }
        };
        var observer: Observer = .{};
        var fixture: Fixture = .{};
        var quota: @import("memory_budget.zig") = .{ .backing = a, .limit = 2 * 1024 * 1024 };
        defer std.debug.assert(quota.live == 0);
        var manager: @import("spill.zig").Manager = .{ .alloc = quota.allocator(), .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.checkpoint };
        defer manager.deinit();
        const backend: catalog.Backend = .{ .ptr = &fixture, .spill_manager = &manager, .vtable = &.{ .resolve = Fixture.resolve, .scan = SetTestBackend.scan, .mutate = SetTestBackend.mutate, .checkpoint = Fixture.checkpoint, .open_statement = Fixture.capture } };
        var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(quota.allocator());
        defer arena.deinit();
        const bound = try @import("describe.zig").bind(arena.allocator(), backend, &compiled, &.{});
        const context: @import("runtime.zig").Context = .{ .alloc = quota.allocator(), .arena = arena.allocator(), .backend = backend, .binding = bound, .parameters = &.{}, .spill = &manager, .limits = .{ .retained_bytes = 2 * 1024 * 1024, .scan_rows = 65536, .result_rows = 65536 } };
        try context.selectInto(compiled.statement.select, .{ .ptr = &observer, .append = Observer.append });
        for (observer.counts, 0..) |count, n| try std.testing.expectEqual(if (n <= case.maximum) case.multiplicity else @as(u16, 0), count);
        try std.testing.expectEqual(@as(usize, 1), fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        try std.testing.expect(manager.written_bytes > 0);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    }
}

test "SQL set inference respects pairwise and derived type boundaries" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    var backend: SetTestBackend = .{};
    for ([_][]const u8{
        "SELECT $1 AS x UNION SELECT 1",
        "SELECT 1 AS x UNION SELECT $1",
        "(SELECT $1 AS x) UNION SELECT 1",
        "(SELECT $1 AS x LIMIT 1) UNION SELECT 1",
        "SELECT COALESCE($1::bigint, NULL) AS x UNION SELECT 1",
        "SELECT NULL::bigint AS x UNION SELECT NULL UNION SELECT $1 UNION SELECT 1",
        "SELECT CASE WHEN TRUE THEN SUM($1::bigint) ELSE 0 END AS x UNION SELECT 1",
        "WITH n AS (SELECT NULL::bigint AS x) SELECT COALESCE(n.x,$1) AS x FROM n UNION SELECT 1",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@import("ast.zig").ColumnType.integer, result.output.columns[0].type);
    }
    var predicate_query = try compiler.compile(std.testing.allocator, "SELECT $1 AS x WHERE $1=1 UNION ALL SELECT 1.5", .{});
    defer predicate_query.deinit();
    var predicate_result = try runtime.execute(std.testing.allocator, backend.backend(), &predicate_query, &.{.{ .integer = 1 }}, .{});
    defer predicate_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), predicate_result.output.rows.len);
    try std.testing.expectEqual(@import("ast.zig").ColumnType.number, predicate_result.output.columns[0].type);
    for ([_][]const u8{ "SELECT $1 AS x UNION SELECT 1 UNION SELECT 1.5", "SELECT 1.5 AS x UNION SELECT 1 UNION SELECT $1" }, 0..) |sql, index| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{if (index == 0) .{ .integer = 2 } else .{ .float = 2.5 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@import("ast.zig").ColumnType.number, result.output.columns[0].type);
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    }
}

test "SQL sets distinguish typed JSON null and SQL NULL and release every allocation" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var backend: SetTestBackend = .{};
            var compiled = try compiler.compile(alloc, "SELECT CAST('null' AS JSON) AS x UNION SELECT NULL UNION SELECT CAST('null' AS JSON)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expect(!result.output.sql_nulls.?[0][0]);
            try std.testing.expect(result.output.sql_nulls.?[1][0]);
        }
    };
    try Fixture.run(std.testing.allocator);
    // SafeAllocator can grow the last bucket allocation depending on prior
    // tests. Force allocate/copy growth so every OOM run visits the same sites,
    // while retaining the backing allocator's leak and ownership checks.
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
}

test "SQL set admission rejects incompatible shapes and enforces the shared memory budget" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    var backend: SetTestBackend = .{};
    for ([_][]const u8{
        "SELECT 1 UNION SELECT TRUE",
        "SELECT 1 UNION SELECT 1, 2",
        "SELECT NULL UNION SELECT NULL UNION SELECT 1",
        "SELECT '1'::text UNION SELECT 1",
        "SELECT ARRAY[1]::int4[] UNION SELECT 1",
        "SELECT x FROM (SELECT NULL x) q UNION SELECT 1",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
    for ([_][]const u8{
        "SELECT ARRAY[1]::int4[] UNION SELECT ARRAY[TRUE]::boolean[]",
        "SELECT ARRAY[1]::int4[] UNION SELECT ARRAY['1']::text[]",
    }) |sql| {
        var incompatible = try compiler.compile(std.testing.allocator, sql, .{});
        defer incompatible.deinit();
        try std.testing.expectError(error.SqlCannotCoerce, runtime.execute(std.testing.allocator, backend.backend(), &incompatible, &.{}, .{}));
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT 'long retained payload' AS x UNION SELECT 'another retained payload'", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1024 }));
    backend = .{ .cancel_after = 12 };
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
}

fn Engine(comptime Context: type) type {
    return struct {
        const Self = @This();
        context: Context,
        cursors: []const catalog.Cursor,
        visited: usize = 0,
        work: usize = 0,
        cache_arena: std.heap.ArenaAllocator,
        static_rows: std.AutoHashMapUnmanaged(*const binding.Node, *Replay) = .empty,
        static_hashes: std.AutoHashMapUnmanaged(*const binding.Node, *operators.HashJoin) = .empty,
        static_memberships: std.AutoHashMapUnmanaged(*const binding.Node, *operators.TupleMembership) = .empty,
        recursions: [32]?*Worklist = @splat(null),
        recursion_scopes: [32]?usize = @splat(null),
        outer_values: [32]?[]const Datum = @splat(null),
        outer_replays: [32]std.AutoHashMapUnmanaged(*const binding.Node, *Replay) = @splat(.empty),

        pub fn checkpoint(self: *Self) !void {
            try self.context.checkpoint();
            self.work += 1;
            if (self.work > self.context.limits.scan_rows *| 64) return error.SqlProgramLimitExceeded;
        }

        pub fn deinit(self: *Self) void {
            for (0..self.outer_replays.len) |id| {
                self.clearOuter(id);
                self.outer_replays[id].deinit(self.context.alloc);
            }
            for (0..self.recursions.len) |id| self.closeRecursion(id);
            var hashes = self.static_hashes.valueIterator();
            while (hashes.next()) |join| join.*.deinit();
            var memberships = self.static_memberships.valueIterator();
            while (memberships.next()) |index| index.*.deinit();
            var rows = self.static_rows.valueIterator();
            while (rows.next()) |replay| replay.*.deinit();
            self.cache_arena.deinit();
        }

        fn clearOuter(self: *Self, id: usize) void {
            var entries = self.outer_replays[id].valueIterator();
            while (entries.next()) |replay| replay.*.deinit();
            self.outer_replays[id].clearRetainingCapacity();
            for (self.recursion_scopes, 0..) |scope, recursion_id| if (scope == id) self.closeRecursion(recursion_id);
            self.outer_values[id] = null;
        }

        fn closeRecursion(self: *Self, id: usize) void {
            if (self.recursions[id]) |worklist| {
                worklist.deinit();
                self.context.alloc.destroy(worklist);
                self.recursions[id] = null;
                self.recursion_scopes[id] = null;
            }
        }

        fn staticRows(self: *Self, node: *const binding.Node) anyerror!*Replay {
            const mask = outerMask(node);
            const outer_id: usize = if (mask != 0) 31 - @clz(mask) else 0;
            const cache = if (mask != 0) &self.outer_replays[outer_id] else &self.static_rows;
            if (cache.get(node)) |rows| return rows;
            // Reserve room for consuming operators when spill is available;
            // memory-only backends retain their full statement admission.
            const memory_bytes = if (self.context.spill != null) self.context.limits.retained_bytes / 16 else self.context.limits.retained_bytes;
            const replay = try Replay.create(self.context.alloc, node.columns.len, memory_bytes, self.context.spill);
            errdefer replay.deinit();
            const iterator = try Iterator.createContext(self, node, null, mask != 0);
            defer iterator.deinit();
            var scratch = std.heap.ArenaAllocator.init(self.context.alloc);
            defer scratch.deinit();
            while (try iterator.next(scratch.allocator())) |values| {
                if (replay.count >= self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                try replay.append(values);
                _ = scratch.reset(.free_all);
            }
            try replay.finish();
            try cache.put(if (mask != 0) self.context.alloc else self.cache_arena.allocator(), node, replay);
            return replay;
        }

        fn recursive(self: *Self, node: *const binding.Node) anyerror!*Worklist {
            const plan = node.operation.recursive;
            if (self.recursions[plan.id]) |state| {
                if (!state.complete) return error.UnsupportedSqlShape;
                return state;
            }
            const state = try self.context.alloc.create(Worklist);
            state.* = .init(self.context.alloc, plan.all);
            self.recursions[plan.id] = state;
            const mask = outerMask(node);
            self.recursion_scopes[plan.id] = if (mask != 0) 31 - @clz(mask) else null;
            var scratch = std.heap.ArenaAllocator.init(self.context.alloc);
            defer scratch.deinit();
            {
                const seed = try Iterator.create(self, plan.seed);
                defer seed.deinit();
                while (try seed.next(scratch.allocator())) |values| {
                    const normalized = try normalize(scratch.allocator(), node.columns, values);
                    try state.append(self, normalized, self.context.limits.scan_rows);
                    _ = scratch.reset(.free_all);
                }
            }
            state.end = state.rows.items.len;
            while (state.begin != state.end) {
                try self.checkpoint();
                const step = try Iterator.createRecursive(self, plan.step, plan.id);
                defer step.deinit();
                while (try step.next(scratch.allocator())) |values| {
                    const normalized = try normalize(scratch.allocator(), node.columns, values);
                    try state.append(self, normalized, self.context.limits.scan_rows);
                    _ = scratch.reset(.free_all);
                }
                state.begin = state.end;
                state.end = state.rows.items.len;
            }
            state.complete = true;
            return state;
        }

        fn normalize(alloc: Allocator, columns: []const binding.Column, values: []const Datum) ![]const Datum {
            if (columns.len != values.len) return error.SqlTypeMismatch;
            const result = try alloc.alloc(Datum, values.len);
            for (values, columns, result) |value, column, *out| out.* = try describe.coerceDatum(alloc, value, column.type, column.element_type);
            return result;
        }

        fn estimate(self: *Self, node: *const binding.Node) ?u64 {
            return switch (node.operation) {
                .scan => |scan| self.cursors[scan.index].estimated_rows,
                .singleton => 1,
                .literal_rows => |rows| rows.len,
                .prepared_rows => if (self.context.returning_cursor) |cursor| cursor.count() else self.context.returning_rows.len,
                .query => |query| blk: {
                    const source = self.estimate(query.source);
                    if (query.statement.limit) |limit| {
                        const count = query.statement.capRows(self.context.count(limit, std.math.maxInt(usize)) catch break :blk source);
                        break :blk if (source) |rows| @min(rows, count) else count;
                    }
                    break :blk source;
                },
                .materialized_ref => |source| self.estimate(source),
                else => null,
            };
        }
        fn preferLeftBuild(self: *Self, left: *const binding.Node, right: *const binding.Node) bool {
            if (self.estimate(left)) |l| {
                if (self.estimate(right)) |r| return l < r;
                // A compiler-owned singleton/constant frame is a bounded
                // build even when a physical source has no row estimate.
                // Building the unknown side would eagerly consume the entire
                // source before a scalar/EXISTS/LIMIT consumer can stop it.
                if (l <= 1) return true;
            }
            // Compressed source bytes are a secondary estimate, used only when
            // neither side provides snapshot-local cardinality information.
            if (left.operation == .scan and right.operation == .scan) {
                const l = self.cursors[left.operation.scan.index].estimated_bytes orelse return false;
                const r = self.cursors[right.operation.scan.index].estimated_bytes orelse return false;
                return l < r;
            }
            return false;
        }

        const Iterator = struct {
            engine: *Self,
            node: *const binding.Node,
            arena: std.heap.ArenaAllocator,
            scratch: std.heap.ArenaAllocator,
            left: ?*Iterator = null,
            right: ?*Iterator = null,
            page: ?catalog.Page = null,
            column_page: ?catalog.ColumnPage = null,
            page_index: usize = 0,
            pages: usize = 0,
            eof: bool = false,
            emitted: bool = false,
            hash_join: ?*operators.HashJoin = null,
            membership_index: ?*operators.TupleMembership = null,
            borrowed_membership: bool = false,
            partition_join: ?*@import("partition_join.zig").Join = null,
            join_view: ?@import("parallel_output.zig").Pipe.View = null,
            probe: ?operators.HashJoin.Probe = null,
            probe_arena: std.heap.ArenaAllocator,
            probe_payload: @import("execution_batch.zig").Batch = .{ .rows = &.{} },
            probe_batch: []operators.HashJoin.Probe = &.{},
            probe_index: usize = 0,
            probe_errors: []?anyerror = &.{},
            probe_input_error: ?anyerror = null,
            probe_source_exhausted: bool = false,
            scan_filter: ?*@import("dynamic_filter.zig").Filter = null,
            left_values: ?[]const Datum = null,
            left_matched: bool = false,
            unmatched_index: usize = 0,
            output_index: usize = 0,
            result_cursor: ?*@import("result_cursor.zig").Cursor = null,
            prepared_reader: ?*@import("result_cursor.zig").Cursor.ReplayReader = null,
            query_fields: ?[]const []const u8 = null,
            query_skip: usize = 0,
            query_remaining: usize = 0,
            query_buffer: []const []const Datum = &.{},
            query_buffer_index: usize = 0,
            set_entries: std.ArrayList(SetEntry) = .empty,
            set_heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            set_ready: bool = false,
            set_external: ?*@import("set_spill.zig").State = null,
            set_external_ready: bool = false,
            values_leaves: []const *const binding.Node = &.{},
            values_leaf_index: usize = 0,
            values_leaf: ?*Iterator = null,
            values_leaf_coerce: bool = false,
            recursive_id: ?usize = null,
            apply_mode: bool = false,
            batch_demand: ?usize = null,
            cached_reader: ?*Replay.Reader = null,
            borrowed_hash: bool = false,
            flipped_join: bool = false,

            const SetEntry = struct { values: []const Datum, count: usize, next: ?usize };

            fn create(engine: *Self, node: *const binding.Node) anyerror!*Iterator {
                return createRecursive(engine, node, null);
            }
            fn createRecursive(engine: *Self, node: *const binding.Node, recursive_id: ?usize) anyerror!*Iterator {
                return createContext(engine, node, recursive_id, false);
            }
            fn createContext(engine: *Self, node: *const binding.Node, recursive_id: ?usize, apply_mode: bool) anyerror!*Iterator {
                const alloc = engine.context.alloc;
                const self = try alloc.create(Iterator);
                self.* = .{ .engine = engine, .node = node, .arena = .init(alloc), .scratch = .init(alloc), .probe_arena = .init(alloc), .recursive_id = recursive_id, .apply_mode = apply_mode };
                errdefer self.deinit();
                if (recursive_id) |id| if (!dependsOn(node, id)) {
                    self.cached_reader = try (try engine.staticRows(node)).openReader();
                    return self;
                };
                if (apply_mode and outerMask(node) == 0 and (recursive_id == null or !dependsOn(node, recursive_id.?))) {
                    self.cached_reader = try (try engine.staticRows(node)).openReader();
                    return self;
                }
                switch (node.operation) {
                    .materialized_ref => |source| self.cached_reader = try (try engine.staticRows(source)).openReader(),
                    .join => |join| {
                        // Always probe the delta and build the invariant side.
                        // Keep output ordinals in the original SQL FROM order.
                        self.flipped_join = if (join.membership != null) false else if (apply_mode and outerMask(node) != 0) outerMask(join.left) == 0 and outerMask(join.right) != 0 else if (recursive_id) |id| !dependsOn(join.left, id) and dependsOn(join.right, id) else engine.preferLeftBuild(join.left, join.right);
                        self.left = try createContext(engine, if (self.flipped_join) join.right else join.left, recursive_id, apply_mode);
                        self.right = try createContext(engine, if (self.flipped_join) join.left else join.right, recursive_id, apply_mode);
                    },
                    .apply => |apply| self.left = try createContext(engine, apply.left, recursive_id, apply_mode),
                    .query => |query| self.left = try createContext(engine, query.source, recursive_id, apply_mode),
                    .set => |set| {
                        self.left = try createContext(engine, set.left, recursive_id, apply_mode);
                        self.right = try createContext(engine, set.right, recursive_id, apply_mode);
                    },
                    .values => |arms| self.values_leaves = arms,
                    else => {},
                }
                return self;
            }
            pub fn deinit(self: *Iterator) void {
                if (self.prepared_reader) |reader| reader.close();
                if (self.cached_reader) |reader| reader.close();
                if (self.result_cursor) |cursor| cursor.close();
                if (self.page) |page| page.deinit();
                if (self.left) |left| left.deinit();
                if (self.right) |right| right.deinit();
                if (self.node.operation == .apply) self.engine.clearOuter(self.node.operation.apply.id);
                if (self.values_leaf) |leaf| leaf.deinit();
                if (self.join_view) |view| view.deinit();
                if (self.partition_join) |join| join.close();
                if (self.scan_filter) |filter| filter.close();
                if (!self.borrowed_hash) if (self.hash_join) |join| join.deinit();
                if (!self.borrowed_membership) if (self.membership_index) |index| index.deinit();
                if (self.set_external) |state| state.deinit();
                self.set_entries.deinit(self.engine.context.alloc);
                self.set_heads.deinit(self.engine.context.alloc);
                self.probe_arena.deinit();
                self.arena.deinit();
                self.scratch.deinit();
                self.engine.context.alloc.destroy(self);
            }
            fn nextBatch(self: *Iterator, a: Allocator, maximum: usize, failure: ?*?anyerror) anyerror!@import("execution_batch.zig").Batch {
                const previous_demand = self.batch_demand;
                self.batch_demand = if (previous_demand) |prior| @min(prior, maximum) else maximum;
                defer self.batch_demand = previous_demand;
                try self.engine.checkpoint();
                if (self.join_view) |view| {
                    view.deinit();
                    self.join_view = null;
                }
                if (self.partition_join) |join| if (try join.nextTypedBatch(maximum)) |view| {
                    self.join_view = view;
                    const source = try a.create(@import("execution_batch.zig").Batch);
                    source.* = view.values();
                    if (!self.flipped_join) return source.*;
                    const ordinals = try a.alloc(usize, self.node.columns.len);
                    const kinds = try a.alloc(@import("ast.zig").ColumnType, self.node.columns.len);
                    const selection = try a.alloc(usize, view.count);
                    const width = self.node.operation.join.left.columns.len;
                    for (ordinals, kinds, self.node.columns, 0..) |*ordinal, *kind, column, index| {
                        ordinal.* = if (index < width) self.node.operation.join.right.columns.len + index else index - width;
                        kind.* = column.type;
                    }
                    for (selection, 0..) |*index, i| index.* = i;
                    return .{ .mapped = .{ .source = source, .ordinals = ordinals, .kinds = kinds, .selection = selection } };
                };
                if (self.cached_reader == null and self.left != null and self.node.operation == .query) {
                    const query = self.node.operation.query;
                    const decisions = @import("decision_eval.zig");
                    const eligible = blk: {
                        if (query.binding.aggregate != null or query.binding.window != null or query.statement.count_all or query.binding.order_keys.len != 0) break :blk false;
                        if (query.binding.scalars.predicate) |*program| if (decisions.hasExternal(program)) break :blk false;
                        for (query.binding.scalars.projections) |*optional| if (optional.*) |*program| if (decisions.hasExternal(program)) break :blk false;
                        break :blk true;
                    };
                    if (eligible) return self.nextQueryBatch(a, maximum, query, failure);
                }
                if (self.cached_reader == null and self.node.operation == .scan) {
                    const scan = self.node.operation.scan;
                    const cursor = self.engine.cursors[scan.index];
                    if (cursor.next_columns) |pull| {
                        if (self.eof) return .{ .rows = &.{} };
                        // Never advance a producer while a consumer borrows its page.
                        self.column_page = null;
                        _ = self.arena.reset(.free_all);
                        self.pages += 1;
                        if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                        const page = try pull(cursor.ptr, self.arena.allocator(), @intCast(maximum));
                        try page.validate();
                        if (page.selection.len > maximum or page.selection.len > self.engine.context.limits.scan_rows -| self.engine.visited) return error.SqlProgramLimitExceeded;
                        self.engine.visited += page.selection.len;
                        self.eof = page.after == null;
                        const definitions = try a.alloc(scalar.Column, self.node.columns.len);
                        for (definitions, scan.source_columns, self.node.columns) |*definition, name, column| definition.* = .{ .name = name, .type = column.type };
                        return .{ .columns = .{ .page = page, .definitions = definitions } };
                    }
                }
                var rows: std.ArrayList([]const Datum) = .empty;
                var retained: usize = 0;
                while (rows.items.len < maximum) {
                    const values = (self.nextDemand(a, maximum - rows.items.len) catch |err| blk: {
                        if (failure) |out| {
                            out.* = err;
                            break :blk null;
                        }
                        return err;
                    }) orelse break;
                    const owned = try a.alloc(Datum, values.len);
                    for (values, owned) |value, *out| {
                        out.* = try operators.cloneDatum(a, value);
                        retained +|= try operators.datumBytes(value);
                    }
                    try rows.append(a, owned);
                    if (retained >= self.engine.context.limits.retained_bytes / 32) break;
                }
                return .{ .rows = rows.items };
            }
            fn next(self: *Iterator, alloc: Allocator) anyerror!?[]const Datum {
                try self.engine.checkpoint();
                if (self.cached_reader) |reader| return reader.next();
                return switch (self.node.operation) {
                    .materialized_ref => unreachable,
                    .recursive => blk: {
                        const state = try self.engine.recursive(self.node);
                        if (self.output_index == state.rows.items.len) break :blk null;
                        const values = state.rows.items[self.output_index].values;
                        self.output_index += 1;
                        break :blk values;
                    },
                    .recursive_ref => |id| blk: {
                        const state = self.engine.recursions[id] orelse return error.InvalidSqlBackendResponse;
                        if (state.begin + self.output_index == state.end) break :blk null;
                        const values = state.rows.items[state.begin + self.output_index].values;
                        self.output_index += 1;
                        break :blk values;
                    },
                    .outer_ref => |id| if (self.emitted) null else blk: {
                        const values = self.engine.outer_values[id] orelse return error.InvalidSqlBackendResponse;
                        self.emitted = true;
                        const identity = values.len == self.node.columns.len and for (self.node.columns, 0..) |column, ordinal| {
                            if (column.outer_ordinal != ordinal) break false;
                        } else true;
                        if (identity) break :blk values;
                        const selected = try alloc.alloc(Datum, self.node.columns.len);
                        for (self.node.columns, selected) |column, *out| {
                            const ordinal = column.outer_ordinal orelse return error.InvalidSqlBackendResponse;
                            if (ordinal >= values.len) return error.InvalidSqlBackendResponse;
                            out.* = values[ordinal];
                        }
                        break :blk selected;
                    },
                    .singleton => if (self.emitted) null else blk: {
                        self.emitted = true;
                        break :blk &.{};
                    },
                    .literal_rows => |rows| blk: {
                        if (self.output_index == rows.len) break :blk null;
                        const values = try normalize(alloc, self.node.columns, rows[self.output_index]);
                        self.output_index += 1;
                        break :blk values;
                    },
                    .prepared_rows => |names| blk: {
                        if (self.engine.context.returning_cursor) |cursor| {
                            if (self.prepared_reader == null) self.prepared_reader = try cursor.openReplayReader();
                            const row = (try self.prepared_reader.?.next()) orelse break :blk null;
                            const layout = self.engine.context.returning_layout orelse return error.InvalidSqlBackendResponse;
                            const values = try alloc.alloc(Datum, self.node.columns.len);
                            for (names, self.node.columns, values) |name, column, *out| {
                                const ordinal = layout.ordinals.get(name) orelse return error.InvalidSqlBackendResponse;
                                if (ordinal >= row.len) return error.InvalidSqlBackendResponse;
                                out.* = try operators.cloneDatum(alloc, try describe.coerceDatum(alloc, row[ordinal], column.type, column.element_type));
                            }
                            self.output_index += 1;
                            break :blk values;
                        }
                        if (self.output_index == self.engine.context.returning_rows.len) break :blk null;
                        const row = self.engine.context.returning_rows[self.output_index];
                        self.output_index += 1;
                        const values = try alloc.alloc(Datum, self.node.columns.len);
                        for (names, self.node.columns, values) |name, column, *out| {
                            const cell = try row.cell(name);
                            out.* = try describe.coerceDatum(alloc, cell, column.type, column.element_type);
                        }
                        break :blk values;
                    },
                    .scan => |scan| blk: {
                        const cursor = self.engine.cursors[scan.index];
                        if (cursor.next_columns) |pull| {
                            while (self.column_page == null or self.page_index == self.column_page.?.selection.len) {
                                if (self.eof) break :blk null;
                                self.column_page = null;
                                _ = self.arena.reset(.free_all);
                                self.pages += 1;
                                if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                                const wanted: u32 = @intCast(@min(self.batch_demand orelse std.math.maxInt(usize), @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.node.columns.len * @sizeOf(Datum) * 16)))));
                                self.column_page = try pull(cursor.ptr, self.arena.allocator(), wanted);
                                try self.column_page.?.validate();
                                if (self.column_page.?.selection.len > wanted) return error.InvalidSqlBackendResponse;
                                self.page_index = 0;
                                self.eof = self.column_page.?.after == null;
                            }
                            const page = self.column_page.?;
                            const index = self.page_index;
                            self.page_index += 1;
                            self.engine.visited += 1;
                            if (self.engine.visited > self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                            const values = try alloc.alloc(Datum, self.node.columns.len);
                            for (scan.source_columns, self.node.columns, values) |name, column, *out| {
                                const cell = try page.cell(alloc, index, name);
                                // Values borrow the current page until the next pull.
                                // Retaining operators own typed copies at admission.
                                out.* = try describe.coerceDatum(alloc, cell, column.type, column.element_type);
                            }
                            break :blk values;
                        }
                        while (self.page == null or self.page_index == self.page.?.rows.len) {
                            if (self.eof) break :blk null;
                            if (self.page) |page| page.deinit();
                            self.page = null;
                            _ = self.arena.reset(.free_all);
                            self.pages += 1;
                            if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                            const wanted = @min(self.batch_demand orelse std.math.maxInt(usize), @min(self.engine.context.limits.page_rows, @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.node.columns.len * @sizeOf(Datum) * 16))));
                            self.page = try cursor.next(cursor.ptr, self.arena.allocator(), @intCast(wanted));
                            if (self.page.?.rows.len > wanted) return error.InvalidSqlBackendResponse;
                            self.page_index = 0;
                            self.eof = self.page.?.after == null;
                        }
                        const row = self.page.?.rows[self.page_index];
                        self.page_index += 1;
                        self.engine.visited += 1;
                        if (self.engine.visited > self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                        const values = try alloc.alloc(Datum, self.node.columns.len);
                        for (scan.source_columns, self.node.columns, values) |name, column, *out| {
                            const cell = try @import("joined_mutation.zig").cell(alloc, row, name);
                            out.* = try describe.coerceDatum(alloc, cell, column.type, column.element_type);
                        }
                        break :blk values;
                    },
                    .join => |join| self.nextJoin(alloc, join),
                    .apply => |apply| self.nextApply(alloc, apply),
                    .set => |set| self.nextSet(alloc, set),
                    .values => self.nextValues(alloc),
                    .query => |query| blk: {
                        // Nonblocking nested queries are pipelines, not hidden
                        // materialization boundaries. Blocking sort/aggregate
                        // nodes retain their bounded operator-specific state.
                        if (query.binding.aggregate == null and query.binding.window == null and !query.statement.count_all and query.binding.order_keys.len == 0)
                            break :blk try self.nextQuery(alloc, query);
                        if (self.result_cursor) |cursor| break :blk try cursor.next(alloc);
                        {
                            var adapter: Adapter = .{ .engine = self.engine, .iterator = self.left.?, .table = query.binding.table.? };
                            var context = self.engine.context;
                            context.backend = adapter.iface();
                            context.sink = null;
                            context.binding = query.binding;
                            context.binding.relation = null;
                            context.arena = self.arena.allocator();
                            const constants = try context.arena.alloc(Datum, query.constant_refs.len);
                            for (query.constant_refs, constants) |reference, *constant| {
                                const values = self.engine.outer_values[reference.frame] orelse return error.InvalidSqlBackendResponse;
                                if (reference.ordinal >= values.len) return error.InvalidSqlBackendResponse;
                                constant.* = values[reference.ordinal];
                            }
                            context.invocation_constants = constants;
                            context.limits.result_rows = context.limits.scan_rows;
                            const cursor = try context.typedQuery(query.statement);
                            errdefer cursor.close();
                            const first = try cursor.next(alloc);
                            self.result_cursor = cursor;
                            break :blk first;
                        }
                    },
                };
            }
            /// Carry a pull's prefetch ceiling through row adapters too. This
            /// does not cap total scan work or truncate a join build.
            fn nextDemand(self: *Iterator, alloc: Allocator, maximum: usize) anyerror!?[]const Datum {
                const previous = self.batch_demand;
                self.batch_demand = if (previous) |prior| @min(prior, maximum) else maximum;
                defer self.batch_demand = previous;
                return self.next(alloc);
            }
            fn nextApply(self: *Iterator, alloc: Allocator, apply: @FieldType(@FieldType(binding.Node, "operation"), "apply")) anyerror!?[]const Datum {
                while (true) {
                    try self.engine.checkpoint();
                    if (self.left_values == null) {
                        _ = self.arena.reset(.free_all);
                        const values = (if (apply.kind == .left)
                            try self.left.?.nextDemand(self.arena.allocator(), self.batch_demand orelse std.math.maxInt(usize))
                        else
                            try self.left.?.next(self.arena.allocator())) orelse return null;
                        const owned = try self.arena.allocator().alloc(Datum, values.len);
                        for (values, owned) |value, *out| out.* = try operators.cloneDatum(self.arena.allocator(), value);
                        self.left_values = owned;
                        self.left_matched = false;
                        self.engine.outer_values[apply.id] = owned;
                        if (apply.demand) |program| {
                            const demand = try self.engine.context.evaluate(self.arena.allocator(), program, owned);
                            if (!demand.sql_null and demand.value != .bool) return error.SqlTypeMismatch;
                            if (demand.sql_null or !demand.value.bool) {
                                // Do not create the right iterator: even an
                                // invariant cached producer may fail on first
                                // evaluation. Preserve one NULL-extended row.
                                self.engine.clearOuter(apply.id);
                                self.left_values = null;
                                const output = try alloc.alloc(Datum, self.node.columns.len);
                                @memset(output, .{});
                                for (owned, output[0..owned.len]) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                                return output;
                            }
                        }
                        self.right = try createContext(self.engine, apply.right, self.recursive_id, true);
                    }
                    _ = self.scratch.reset(.free_all);
                    if (try self.right.?.next(self.scratch.allocator())) |right| {
                        // A second pull may retire borrowed cursor/cache pages.
                        // Own the first row before checking the cardinality
                        // witness, and never expose a prefix of an invalid row.
                        const row = if (apply.single_row) blk: {
                            const owned = try self.scratch.allocator().alloc(Datum, right.len);
                            for (right, owned) |value, *out| out.* = try operators.cloneDatum(self.scratch.allocator(), value);
                            _ = self.probe_arena.reset(.{ .retain_with_limit = 16 * 1024 });
                            if (try self.right.?.next(self.probe_arena.allocator()) != null) return error.SqlCardinalityViolation;
                            break :blk owned;
                        } else right;
                        const values = try self.scratch.allocator().alloc(Datum, self.node.columns.len);
                        @memcpy(values[0..self.left_values.?.len], self.left_values.?);
                        @memcpy(values[self.left_values.?.len..], row);
                        if (apply.condition) |program| {
                            const accepted = try self.engine.context.evaluate(self.scratch.allocator(), program, values);
                            if (accepted.sql_null) continue;
                            if (accepted.value != .bool) return error.SqlTypeMismatch;
                            if (!accepted.value.bool) continue;
                        }
                        self.left_matched = true;
                        const output = try alloc.alloc(Datum, values.len);
                        for (values, output) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                        if (apply.single_row) {
                            self.right.?.deinit();
                            self.right = null;
                            self.engine.clearOuter(apply.id);
                            self.left_values = null;
                        }
                        return output;
                    }
                    self.right.?.deinit();
                    self.right = null;
                    self.engine.clearOuter(apply.id);
                    const left = self.left_values.?;
                    self.left_values = null;
                    if (apply.kind == .left and !self.left_matched) {
                        const output = try alloc.alloc(Datum, self.node.columns.len);
                        @memset(output, .{});
                        for (left, output[0..left.len]) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                        return output;
                    }
                }
            }

            fn batchProgram(self: *Iterator, a: Allocator, context: @TypeOf(self.engine.context), program: *const scalar.Program, batch: @import("execution_batch.zig").Batch, errors: []?anyerror) ![]const Datum {
                const vector = @import("vector_eval.zig").evaluateBatch(a, program, batch, context.parameters) catch |err| switch (err) {
                    error.SqlDivisionByZero, error.SqlNumericOutOfRange, error.SqlTypeMismatch, error.InvalidSqlDateTime, error.SqlCardinalityViolation => null,
                    else => return err,
                };
                if (vector) |values| return values;
                const values = try a.alloc(Datum, batch.len());
                for (values, 0..) |*value, index| {
                    value.* = context.evaluate(a, program.*, try batch.row(a, index)) catch |err| blk: {
                        if (err == error.OutOfMemory or err == error.QueryCanceled or err == error.DeadlineExceeded) return err;
                        errors[index] = errors[index] orelse err;
                        break :blk .{};
                    };
                }
                return values;
            }
            fn nextQueryBatch(self: *Iterator, _: Allocator, maximum: usize, query: @FieldType(@FieldType(binding.Node, "operation"), "query"), failure: ?*?anyerror) anyerror!@import("execution_batch.zig").Batch {
                const Batch = @import("execution_batch.zig").Batch;
                var context = self.engine.context;
                context.binding = query.binding;
                context.binding.relation = null;
                if (self.query_fields == null) {
                    const fields = try self.arena.allocator().alloc([]const u8, query.statement.columns.len);
                    for (query.statement.columns, fields) |column, *field| field.* = if (column.expression != null) "" else column.field;
                    self.query_fields = fields;
                    self.query_skip = try context.offsetCount(query.statement.offset);
                    self.query_remaining = query.statement.capRows(try context.count(query.statement.limit, std.math.maxInt(usize)));
                }
                while (self.query_remaining != 0) {
                    _ = self.scratch.reset(.free_all);
                    const a = self.scratch.allocator();
                    const input = try a.create(Batch);
                    var upstream_error: ?anyerror = null;
                    input.* = try self.left.?.nextBatch(a, @min(maximum, self.query_remaining +| self.query_skip), &upstream_error);
                    const errors = try a.alloc(?anyerror, input.len());
                    @memset(errors, null);
                    if (input.len() == 0) {
                        if (upstream_error) |err| {
                            if (failure) |out| out.* = err else return err;
                        }
                        return .{ .rows = &.{} };
                    }
                    const ordinals = try a.alloc(usize, context.binding.scalars.columns.len);
                    const kinds = try a.alloc(@import("ast.zig").ColumnType, ordinals.len);
                    for (context.binding.scalars.columns, ordinals, kinds) |definition, *ordinal, *kind| {
                        ordinal.* = for (query.source.columns, 0..) |column, index| {
                            if (std.mem.eql(u8, column.internal, definition.name)) break index;
                        } else std.math.maxInt(usize);
                        kind.* = definition.type;
                    }
                    const identity = try a.alloc(usize, input.len());
                    for (identity, 0..) |*index, value| index.* = value;
                    var bound: Batch = .{ .mapped = .{ .source = input, .ordinals = ordinals, .kinds = kinds, .selection = identity } };
                    const predicates = if (context.binding.scalars.predicate) |*program| try self.batchProgram(a, context, program, bound, errors) else null;
                    var selected: std.ArrayList(usize) = .empty;
                    for (0..input.len()) |index| {
                        if (selected.items.len == self.query_remaining) {
                            upstream_error = null;
                            break;
                        }
                        if (errors[index]) |err| {
                            upstream_error = err;
                            break;
                        }
                        if (predicates) |values| {
                            if (values[index].sql_null) continue;
                            if (values[index].value != .bool) {
                                upstream_error = error.SqlTypeMismatch;
                                break;
                            }
                            if (!values[index].value.bool) continue;
                        }
                        if (self.query_skip != 0) {
                            self.query_skip -= 1;
                            continue;
                        }
                        try selected.append(a, index);
                    }
                    if (selected.items.len == 0) {
                        if (upstream_error) |err| {
                            if (failure) |out| out.* = err else return err;
                            return .{ .rows = &.{} };
                        }
                        continue;
                    }
                    bound.mapped.selection = selected.items;
                    const output_errors = try a.alloc(?anyerror, selected.items.len);
                    @memset(output_errors, null);
                    const columns = try a.alloc([]const Datum, self.query_fields.?.len);
                    for (self.query_fields.?, context.binding.columns, columns, 0..) |field, definition, *values, column| {
                        const projected = if (column < context.binding.scalars.projections.len) context.binding.scalars.projections[column] else null;
                        if (projected) |*program| {
                            values.* = try self.batchProgram(a, context, program, bound, output_errors);
                        } else {
                            const ordinal = for (query.source.columns, 0..) |source, index| {
                                if (std.mem.eql(u8, source.internal, field)) break index;
                            } else return error.InvalidSqlBackendResponse;
                            const vector = try a.alloc(Datum, selected.items.len);
                            for (selected.items, vector) |index, *value| value.* = try input.cell(a, index, ordinal);
                            values.* = vector;
                        }
                        for (@constCast(values.*), output_errors) |*value, *err| value.* = describe.coerceDatum(a, value.*, definition.type, definition.element_type) catch |cause| blk: {
                            if (cause == error.OutOfMemory) return cause;
                            err.* = err.* orelse cause;
                            break :blk .{};
                        };
                    }
                    var prefix = selected.items.len;
                    for (output_errors, 0..) |cause, index| if (cause) |err| {
                        prefix = index;
                        upstream_error = err;
                        break;
                    };
                    if (prefix == self.query_remaining and prefix == selected.items.len) upstream_error = null;
                    if (upstream_error) |err| {
                        if (failure) |out| out.* = err else return err;
                    }
                    for (columns) |*column| column.* = column.*[0..prefix];
                    self.query_remaining -= prefix;
                    return .{ .vectors = .{ .values = columns, .count = prefix } };
                }
                return .{ .rows = &.{} };
            }
            fn nextQuery(self: *Iterator, alloc: Allocator, query: @FieldType(@FieldType(binding.Node, "operation"), "query")) anyerror!?[]const Datum {
                var context = self.engine.context;
                context.binding = query.binding;
                context.binding.relation = null;
                context.typed_output = true;
                if (self.query_fields == null) {
                    const fields = try self.arena.allocator().alloc([]const u8, query.statement.columns.len);
                    for (query.statement.columns, fields) |column, *field| field.* = if (column.expression != null) "" else column.field;
                    self.query_fields = fields;
                    self.query_skip = try context.offsetCount(query.statement.offset);
                    self.query_remaining = query.statement.capRows(try context.count(query.statement.limit, std.math.maxInt(usize)));
                }
                if (self.query_remaining == 0) return null;
                while (self.query_buffer_index == self.query_buffer.len) {
                    // Keep only one page of input, provider scratch and output.
                    // next() may lend an upstream page until its next pull.
                    // Own buffered cells before advancing across that page.
                    if (!self.scratch.reset(.retain_capacity)) return error.OutOfMemory;
                    const scratch = self.scratch.allocator();
                    self.query_buffer = &.{};
                    self.query_buffer_index = 0;
                    var page_bytes: usize = 0;
                    var rows: std.ArrayList(catalog.Row) = .empty;
                    var cells: std.ArrayList([]const Datum) = .empty;
                    const names = try scratch.alloc([]const u8, query.source.columns.len);
                    for (query.source.columns, names) |column, *name| name.* = column.internal;
                    const layout = try catalog.Row.TypedLayout.init(scratch, names);
                    // Each nested stage leaves room for upstream pages, bindings and spill operators.
                    const wanted = @min(self.batch_demand orelse std.math.maxInt(usize), @min(@min(context.limits.page_rows, @max(@as(usize, 1), context.limits.retained_bytes / (16 * 1024 + query.source.columns.len * @sizeOf(Datum) * 16))), self.query_remaining +| self.query_skip));
                    while (rows.items.len < wanted) {
                        const input = (if (context.binding.scalars.predicate == null)
                            try self.left.?.nextDemand(scratch, wanted - rows.items.len)
                        else
                            try self.left.?.next(scratch)) orelse break;
                        try self.engine.checkpoint();
                        const row = try catalog.Row.fromDatums(scratch, "", layout, input);
                        try rows.append(scratch, row);
                        try cells.append(scratch, try context.binding.scalars.cells(scratch, row));
                        for (input) |cell| page_bytes +|= try operators.datumBytes(cell);
                        if (page_bytes >= context.limits.page_bytes) break;
                    }
                    if (rows.items.len == 0) return null;
                    const predicates = if (context.binding.scalars.predicate) |*program|
                        try @import("decision_eval.zig").evaluateBatchWithLimits(scratch, context.backend.decision_provider, program, cells.items, context.parameters, @import("decision_eval.zig").limitsFor(context.backend))
                    else
                        null;
                    var selected_rows: std.ArrayList(catalog.Row) = .empty;
                    var selected_cells: std.ArrayList([]const Datum) = .empty;
                    for (rows.items, cells.items, 0..) |row, row_cells, index| {
                        if (predicates) |values| {
                            const value = values[index];
                            if (value.sql_null) continue;
                            if (value.value != .bool) return error.SqlTypeMismatch;
                            if (!value.value.bool) continue;
                        }
                        if (self.query_skip != 0) {
                            self.query_skip -= 1;
                            continue;
                        }
                        if (selected_rows.items.len == self.query_remaining) break;
                        try selected_rows.append(scratch, row);
                        try selected_cells.append(scratch, row_cells);
                    }
                    self.query_buffer = try context.projectValuesBatch(scratch, selected_rows.items, self.query_fields.?, selected_cells.items);
                }
                const values = self.query_buffer[self.query_buffer_index];
                const owned = try alloc.alloc(Datum, values.len);
                for (values, owned) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                self.query_buffer_index += 1;
                self.query_remaining -= 1;
                return owned;
            }

            fn setValues(self: *Iterator, alloc: Allocator, values: []const Datum) ![]const Datum {
                const result = try alloc.alloc(Datum, values.len);
                for (values, result, self.node.columns) |value, *out, column| out.* = try describe.coerceDatum(alloc, value, column.type, column.element_type);
                return result;
            }
            fn nextValues(self: *Iterator, alloc: Allocator) anyerror!?[]const Datum {
                while (self.values_leaf_index < self.values_leaves.len) {
                    if (self.values_leaf == null) {
                        const leaf = self.values_leaves[self.values_leaf_index];
                        if (leaf.columns.len != self.node.columns.len) return error.InvalidSqlBackendResponse;
                        self.values_leaf_coerce = false;
                        for (leaf.columns, self.node.columns) |source, target| {
                            if (source.type != target.type) self.values_leaf_coerce = true;
                        }
                        self.values_leaf = try createContext(self.engine, leaf, self.recursive_id, self.apply_mode);
                    }
                    if (try self.values_leaf.?.next(alloc)) |values| {
                        if (values.len != self.node.columns.len) return error.InvalidSqlBackendResponse;
                        return if (self.values_leaf_coerce) try self.setValues(alloc, values) else values;
                    }
                    self.values_leaf.?.deinit();
                    self.values_leaf = null;
                    self.values_leaf_index += 1;
                }
                return null;
            }
            fn setSlot(self: *Iterator, values: []const Datum, insert: bool) !?usize {
                var hasher = std.hash.Wyhash.init(0);
                for (values) |value| {
                    var bytes: [9]u8 = undefined;
                    bytes[0] = @intFromBool(value.sql_null);
                    std.mem.writeInt(u64, bytes[1..9], if (value.sql_null) 0 else try scalar.semanticHashDatum(value), .little);
                    hasher.update(&bytes);
                }
                const hash = hasher.final();
                var cursor = self.set_heads.get(hash);
                while (cursor) |index| {
                    try self.engine.checkpoint();
                    const entry = self.set_entries.items[index];
                    var equal = true;
                    for (entry.values, values) |a, b| if (a.sql_null != b.sql_null or (!a.sql_null and try scalar.compareDatums(a, b) != .eq)) {
                        equal = false;
                        break;
                    };
                    if (equal) return index;
                    cursor = entry.next;
                }
                if (!insert) return null;
                if (self.set_entries.items.len >= self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const owned = self.arena.allocator();
                const copied = try owned.alloc(Datum, values.len);
                for (values, copied) |value, *out| out.* = try operators.cloneDatum(owned, value);
                const index = self.set_entries.items.len;
                try self.set_entries.append(self.engine.context.alloc, .{ .values = copied, .count = 0, .next = self.set_heads.get(hash) });
                try self.set_heads.put(self.engine.context.alloc, hash, index);
                return index;
            }
            fn nextSet(self: *Iterator, alloc: Allocator, set: @FieldType(@FieldType(binding.Node, "operation"), "set")) anyerror!?[]const Datum {
                if (self.set_external_ready) return self.set_external.?.next(alloc);
                const scratch = &self.scratch;
                if (!self.set_ready) {
                    if (set.kind != .@"union") {
                        while (try self.right.?.next(scratch.allocator())) |values| {
                            const normalized = try self.setValues(scratch.allocator(), values);
                            try self.prepareSetCapacity(normalized, set);
                            if (self.set_external) |state| {
                                try state.add(normalized, true, 1);
                            } else {
                                const index = (try self.setSlot(normalized, true)).?;
                                self.set_entries.items[index].count = std.math.add(usize, self.set_entries.items[index].count, 1) catch return error.SqlProgramLimitExceeded;
                            }
                            if (!scratch.reset(.{ .retain_with_limit = 16 * 1024 })) return error.OutOfMemory;
                        }
                        // Retain only distinct keys/counts after the build side
                        // is consumed, not its materialized projection pages.
                        self.right.?.deinit();
                        self.right = null;
                    }
                    self.set_ready = true;
                }
                while (true) {
                    if (!scratch.reset(.{ .retain_with_limit = 16 * 1024 })) return error.OutOfMemory;
                    const source = if (self.eof) self.right.? else self.left.?;
                    const input = try source.next(scratch.allocator()) orelse {
                        if (!self.eof and set.kind == .@"union") {
                            self.left.?.deinit();
                            self.left = null;
                            self.eof = true;
                            continue;
                        }
                        if (self.set_external) |state| {
                            self.set_external_ready = true;
                            return state.next(alloc);
                        }
                        return null;
                    };
                    const values = try self.setValues(scratch.allocator(), input);
                    var emit = false;
                    if (set.kind == .@"union" and set.all) {
                        emit = true;
                    } else {
                        try self.prepareSetCapacity(values, set);
                        if (self.set_external) |state| {
                            try state.add(values, false, 1);
                            continue;
                        }
                    }
                    if (!(set.kind == .@"union" and set.all)) {
                        if (set.kind == .@"union" or (set.kind == .except and !set.all)) {
                            const index = (try self.setSlot(values, true)).?;
                            const entry = &self.set_entries.items[index];
                            emit = entry.count == 0;
                            entry.count = 1;
                        } else if (try self.setSlot(values, false)) |index| {
                            const entry = &self.set_entries.items[index];
                            emit = if (set.kind == .intersect) entry.count != 0 else entry.count == 0;
                            if (entry.count != 0) entry.count = if (set.all) entry.count - 1 else 0;
                        } else emit = set.kind == .except;
                    }
                    if (emit) {
                        const result = try alloc.alloc(Datum, values.len);
                        for (values, result) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                        return result;
                    }
                }
            }

            fn prepareSetCapacity(self: *Iterator, values: []const Datum, set: @FieldType(@FieldType(binding.Node, "operation"), "set")) !void {
                if (self.set_external != null) return;
                const manager = self.engine.context.spill orelse return;
                // Keep headroom for run construction while transferring the
                // hash state. Account for actual arena capacity, not row count.
                const limit = self.engine.context.limits.retained_bytes / 4;
                var needed: usize = @sizeOf(SetEntry) + 128;
                for (values) |value| needed +|= try operators.datumBytes(value);
                const retained = self.arena.queryCapacity() +| (self.set_entries.capacity *| @sizeOf(SetEntry)) +| (self.set_heads.capacity() *| 32);
                if (retained +| needed < limit) return;
                const state = try @import("set_spill.zig").State.create(self.engine.context.alloc, manager, switch (set.kind) {
                    .@"union" => .@"union",
                    .intersect => .intersect,
                    .except => .except,
                }, set.all, self.node.columns.len, limit, self.engine.context.limits.scan_rows);
                errdefer state.deinit();
                for (self.set_entries.items) |entry| {
                    try self.engine.checkpoint();
                    try state.add(entry.values, true, entry.count);
                }
                self.set_entries.deinit(self.engine.context.alloc);
                self.set_entries = .empty;
                self.set_heads.deinit(self.engine.context.alloc);
                self.set_heads = .empty;
                _ = self.arena.reset(.free_all);
                self.set_external = state;
            }

            fn keys(self: *Iterator, alloc: Allocator, programs: []const scalar.Program, values: []const Datum) ![]const Datum {
                const result = try alloc.alloc(Datum, programs.len);
                for (programs, result) |program, *out| out.* = try self.engine.context.evaluate(alloc, program, values);
                return result;
            }
            fn keyBatch(self: *Iterator, a: Allocator, programs: []const scalar.Program, batch: @import("execution_batch.zig").Batch, errors: ?[]?anyerror) ![]const []const Datum {
                const count = batch.len();
                const cells = try a.alloc(Datum, count * programs.len);
                @memset(cells, .{});
                const keys_ = try a.alloc([]const Datum, count);
                for (keys_, 0..) |*row, index| row.* = cells[index * programs.len ..][0..programs.len];
                const native_values = if (batch == .columns) blk: {
                    const pointers = try a.alloc(*const scalar.Program, programs.len);
                    for (programs, pointers) |*program, *pointer| pointer.* = program;
                    break :blk @import("vector_eval.zig").evaluateColumnsManyScheduled(a, pointers, batch.columns.page, batch.columns.definitions, self.engine.context.parameters, self.engine.context.backend.execution_io) catch |err| {
                        if (errors == null) return err;
                        break :blk null;
                    };
                } else null;
                for (programs, 0..) |*program, slot| {
                    const values = (switch (batch) {
                        .rows => |rows| @import("vector_eval.zig").evaluate(a, program, rows, self.engine.context.parameters),
                        .columns => if (native_values) |vectors| vectors[slot] else null,
                        else => @import("vector_eval.zig").evaluateBatch(a, program, batch, self.engine.context.parameters),
                    }) catch |err| blk: {
                        if (errors == null) return err;
                        break :blk null;
                    };
                    for (0..count) |index| {
                        if (errors) |flags| if (flags[index] != null) continue;
                        cells[index * programs.len + slot] = if (values) |vector| vector[index] else self.engine.context.evaluate(a, program.*, try batch.row(a, index)) catch |err| blk: {
                            if (errors) |flags| {
                                flags[index] = err;
                                break :blk .{};
                            }
                            return err;
                        };
                    }
                }
                return keys_;
            }
            fn fillProbes(self: *Iterator, programs: []const scalar.Program) !bool {
                if (self.probe_input_error) |err| return err;
                if (self.probe_source_exhausted) return false;
                _ = self.probe_arena.reset(.free_all);
                const a = self.probe_arena.allocator();
                const count = @min(self.batch_demand orelse std.math.maxInt(usize), @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.left.?.node.columns.len * @sizeOf(Datum) * 16))));
                self.probe_payload = self.left.?.nextBatch(a, count, &self.probe_input_error) catch |err| blk: {
                    self.probe_input_error = err;
                    break :blk .{ .rows = &.{} };
                };
                self.probe_index = 0;
                if (self.probe_payload.len() == 0) {
                    self.probe_source_exhausted = true;
                    if (self.probe_input_error) |err| return err;
                    return false;
                }
                self.probe_errors = try a.alloc(?anyerror, self.probe_payload.len());
                @memset(self.probe_errors, null);
                const keys_ = try self.keyBatch(a, programs, self.probe_payload, self.probe_errors);
                self.probe_batch = self.hash_join.?.probeBatch(a, keys_) catch blk: {
                    const probes = try a.alloc(operators.HashJoin.Probe, keys_.len);
                    for (keys_, probes, self.probe_errors) |keys__, *probe_, *failure| {
                        probe_.* = self.hash_join.?.probe(keys__) catch |err| inner: {
                            failure.* = failure.* orelse err;
                            break :inner .{ .owner = self.hash_join.?, .keys = keys__, .cursor = null };
                        };
                    }
                    break :blk probes;
                };
                return true;
            }
            fn combine(self: *Iterator, alloc: Allocator, left: ?[]const Datum, right: ?[]const Datum) ![]const Datum {
                const width = self.node.operation.join.left.columns.len;
                const values = try alloc.alloc(Datum, self.node.columns.len);
                @memset(values, .{});
                if (if (self.flipped_join) right else left) |cells| @memcpy(values[0..width], cells);
                if (if (self.flipped_join) left else right) |cells| @memcpy(values[width..], cells);
                return values;
            }
            fn nextPartitionJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                const owner = self.partition_join.?;
                while (try owner.next()) |pair| {
                    try self.engine.checkpoint();
                    _ = self.scratch.reset(.free_all);
                    const values = try self.combine(self.scratch.allocator(), pair.left, pair.right);
                    if (pair.match) |index| {
                        if (join.condition) |program| {
                            const accepted = try self.engine.context.evaluate(self.scratch.allocator(), program, values);
                            if (accepted.sql_null) continue;
                            if (accepted.value != .bool) return error.SqlTypeMismatch;
                            if (!accepted.value.bool) continue;
                        }
                        try owner.accept(index);
                    }
                    const result = try alloc.alloc(Datum, values.len);
                    for (values, result) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                    return result;
                }
                return null;
            }
            fn prepareScanFilter(self: *Iterator, programs: []const scalar.Program) !void {
                const probe = self.left.?;
                if (probe.node.operation != .scan or programs.len == 0) return;
                const scan = probe.node.operation.scan;
                const cursor = self.engine.cursors[scan.index];
                if (cursor.set_dynamic_filter == null) return;
                const columns = try self.engine.context.alloc.alloc(@import("dynamic_filter.zig").Column, programs.len);
                defer self.engine.context.alloc.free(columns);
                for (programs, columns) |program, *column| {
                    if (program.instructions.len != 1 or program.instructions[program.root].operation != .column) return;
                    const instruction = program.instructions[program.root];
                    const kind = instruction.type.kind orelse return;
                    switch (kind) {
                        .integer, .number, .string, .boolean => {},
                        else => return,
                    }
                    column.* = .{ .name = scan.source_columns[instruction.operation.column], .type = kind };
                }
                self.scan_filter = try @import("dynamic_filter.zig").Filter.create(self.engine.context.alloc, columns, self.engine.context.limits.retained_bytes / 32);
            }
            fn membershipCheckpoint(raw: *anyopaque) !void {
                const engine: *Self = @ptrCast(@alignCast(raw));
                try engine.checkpoint();
            }
            fn nextMembership(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                const correlations = join.membership.?.correlations;
                const shared = (self.recursive_id != null or self.apply_mode) and outerMask(join.right) == 0 and (self.recursive_id == null or !dependsOn(join.right, self.recursive_id.?));
                if (self.membership_index == null and shared) if (self.engine.static_memberships.get(self.node)) |index| {
                    self.membership_index = index;
                    self.borrowed_membership = true;
                };
                if (self.membership_index == null) {
                    self.membership_index = try operators.TupleMembership.create(self.engine.context.alloc, join.right_keys.len, .{ .rows = self.engine.context.limits.scan_rows, .bytes = self.engine.context.limits.retained_bytes, .work = self.engine.context.limits.scan_rows *| 64 }, .{ .ptr = self.engine, .call = membershipCheckpoint });
                    while (true) {
                        try self.engine.checkpoint();
                        _ = self.scratch.reset(.retain_capacity);
                        const a = self.scratch.allocator();
                        const row = try self.right.?.next(a) orelse break;
                        const keys_ = try self.keys(a, join.right_keys, row);
                        const null_correlation = for (keys_[0..correlations]) |key| {
                            if (key.sql_null) break true;
                        } else false;
                        if (!null_correlation) try self.membership_index.?.add(keys_);
                    }
                    try self.membership_index.?.seal();
                    self.right.?.deinit();
                    self.right = null;
                    if (shared) {
                        try self.engine.static_memberships.put(self.engine.cache_arena.allocator(), self.node, self.membership_index.?);
                        self.borrowed_membership = true;
                    }
                }
                const row = try self.left.?.next(alloc) orelse return null;
                _ = self.scratch.reset(.retain_capacity);
                const keys_ = try self.keys(self.scratch.allocator(), join.left_keys, row);
                const null_correlation = for (keys_[0..correlations]) |key| {
                    if (key.sql_null) break true;
                } else false;
                const truth = if (null_correlation) .no else try self.membership_index.?.probe(keys_);
                const output = try alloc.alloc(Datum, row.len + 1);
                @memcpy(output[0..row.len], row);
                output[row.len] = switch (truth) {
                    .yes => Datum.json(.{ .bool = true }),
                    .no => Datum.json(.{ .bool = false }),
                    .unknown => .{},
                };
                return output;
            }
            fn nextJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                if (join.membership != null) return self.nextMembership(alloc, join);
                const kind = if (self.flipped_join) switch (join.kind) {
                    .right => .left,
                    .left => .right,
                    else => join.kind,
                } else join.kind;
                const build = if (self.flipped_join) join.left else join.right;
                const shared = (self.recursive_id != null or self.apply_mode) and outerMask(build) == 0 and (self.recursive_id == null or !dependsOn(build, self.recursive_id.?)) and kind != .right and kind != .full;
                if (self.hash_join == null and shared) if (self.engine.static_hashes.get(self.node)) |cached| {
                    self.hash_join = cached;
                    self.borrowed_hash = true;
                };
                if (self.partition_join != null) return self.nextPartitionJoin(alloc, join);
                if (self.hash_join == null) {
                    self.hash_join = try operators.HashJoin.create(self.engine.context.alloc, .{ .rows = self.engine.context.limits.scan_rows, .bytes = self.engine.context.limits.retained_bytes, .spill = self.engine.context.spill });
                    if (!shared and kind != .left and kind != .full) try self.prepareScanFilter(if (self.flipped_join) join.right_keys else join.left_keys);
                    var scratch = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                    defer scratch.deinit();
                    while (true) {
                        _ = scratch.reset(.free_all);
                        const a = scratch.allocator();
                        const count = @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.right.?.node.columns.len * @sizeOf(Datum) * 16)));
                        const batch = try self.right.?.nextBatch(a, count, null);
                        if (batch.len() == 0) break;
                        const keys_ = try self.keyBatch(a, if (self.flipped_join) join.left_keys else join.right_keys, batch, null);
                        if (!shared and self.engine.context.spill != null) {
                            if (self.partition_join) |owner| {
                                try owner.addBatch(true, batch, keys_, 0);
                            } else {
                                const consumed = try self.hash_join.?.addBatchUntilFull(a, batch, keys_);
                                if (consumed != batch.len()) {
                                    // The partition workspace coexists with input decode,
                                    // the enclosing operator, and result delivery. Reserve
                                    // a statement lane for those consumers before assigning
                                    // the remaining workspace to serial or parallel builds.
                                    const workspace = self.engine.context.limits.retained_bytes - self.engine.context.limits.retained_bytes / 4;
                                    const owner = try @import("partition_join.zig").Join.create(self.engine.context.alloc, self.engine.context.spill.?, workspace, self.engine.context.limits.scan_rows, self.engine.context.limits.retained_bytes / 8, kind == .left or kind == .full, kind == .right or kind == .full);
                                    self.partition_join = owner;
                                    var transfer = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                                    defer transfer.deinit();
                                    var index: usize = 0;
                                    while (try self.hash_join.?.unmatched(&index)) |match| {
                                        _ = transfer.reset(.retain_capacity);
                                        try owner.add(true, try match.materializeValues(transfer.allocator()), try match.materializeKeys(transfer.allocator()), @intCast(match.index));
                                    }
                                    self.hash_join.?.deinit();
                                    self.hash_join = null;
                                    try owner.addBatch(true, batch, keys_, consumed);
                                }
                            }
                        } else try self.hash_join.?.addBatch(a, batch, keys_);
                        if (self.scan_filter) |filter| for (keys_) |key_values| try filter.add(key_values);
                    }
                    if (self.scan_filter) |filter| {
                        filter.sealed = true;
                        const scan = self.left.?.node.operation.scan;
                        const cursor = self.engine.cursors[scan.index];
                        _ = try cursor.set_dynamic_filter.?(cursor.ptr, filter);
                    }
                    if (self.partition_join) |owner| {
                        if (!Adapter.hasPatterns(self.node, 0) and (join.condition == null or !@import("decision_eval.zig").hasExternal(&join.condition.?))) owner.evaluation = .{
                            .condition = if (self.node.operation.join.condition) |*program| program else null,
                            .parameters = self.engine.context.parameters,
                            .left_width = self.left.?.node.columns.len,
                            .right_width = self.right.?.node.columns.len,
                            .flipped = self.flipped_join,
                        };
                        while (true) {
                            _ = scratch.reset(.retain_capacity);
                            const a = scratch.allocator();
                            const count = @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.left.?.node.columns.len * @sizeOf(Datum) * 16)));
                            const batch = try self.left.?.nextBatch(a, count, null);
                            if (batch.len() == 0) break;
                            const keys_ = try self.keyBatch(a, if (self.flipped_join) join.right_keys else join.left_keys, batch, null);
                            try owner.addBatch(false, batch, keys_, 0);
                        }
                        if (self.hash_join) |hash| hash.deinit();
                        self.hash_join = null;
                        self.partition_join = owner;
                        // Both inputs now belong to the spill files. Release their
                        // decode arena before building or probing a partition;
                        // return expressions execute before deferred cleanup.
                        _ = scratch.reset(.free_all);
                        return self.nextPartitionJoin(alloc, join);
                    }
                    if (shared) {
                        try self.engine.static_hashes.put(self.engine.cache_arena.allocator(), self.node, self.hash_join.?);
                        self.borrowed_hash = true;
                    }
                }
                while (true) {
                    try self.engine.checkpoint();
                    if (self.probe) |*probe| {
                        while (try probe.next()) |match| {
                            try self.engine.checkpoint();
                            // A join iterator owns its candidate scratch for its full
                            // lifetime. Reuse retained pages across output rows;
                            // returned cells still reference the stable probe/build
                            // rows, not this transient candidate allocation.
                            _ = self.scratch.reset(.retain_capacity);
                            if (self.left_values == null) self.left_values = try self.probe_payload.row(self.probe_arena.allocator(), self.probe_index - 1);
                            const values = try self.combine(self.scratch.allocator(), self.left_values, try match.materializeValues(self.scratch.allocator()));
                            if (join.condition) |program| {
                                const accepted = try self.engine.context.evaluate(self.scratch.allocator(), program, values);
                                if (accepted.sql_null) continue;
                                if (accepted.value != .bool) return error.SqlTypeMismatch;
                                if (!accepted.value.bool) continue;
                            }
                            self.left_matched = true;
                            try self.hash_join.?.markMatched(match.index);
                            return try alloc.dupe(Datum, values);
                        }
                        self.probe = null;
                        if (!self.left_matched and (kind == .left or kind == .full)) return try self.combine(alloc, self.left_values orelse try self.probe_payload.row(self.probe_arena.allocator(), self.probe_index - 1), null);
                    }
                    if (self.eof) {
                        if (kind == .right or kind == .full) {
                            if (try self.hash_join.?.unmatched(&self.unmatched_index)) |match| return try self.combine(alloc, null, try match.materializeValues(alloc));
                        }
                        return null;
                    }
                    if (self.probe_index == self.probe_payload.len()) {
                        if (!try self.fillProbes(if (self.flipped_join) join.right_keys else join.left_keys)) {
                            self.eof = true;
                            continue;
                        }
                    }
                    if (self.probe_errors[self.probe_index]) |err| return err;
                    self.left_values = null;
                    self.left_matched = false;
                    self.probe = self.probe_batch[self.probe_index];
                    self.probe_index += 1;
                }
            }
        };

        const Adapter = struct {
            engine: *Self,
            iterator: *Iterator,
            table: catalog.Table,
            ordinal: u64 = 0,
            opened: bool = false,
            pending_failure: ?anyerror = null,

            fn iface(self: *Adapter) catalog.Backend {
                return .{ .regex_execution = self.engine.context.backend.regex_execution, .scalar_control = self.engine.context.backend.scalar_control, .execution_io = self.engine.context.backend.execution_io, .spill_manager = self.engine.context.spill, .ptr = self, .decision_provider = self.engine.context.backend.decision_provider, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = Adapter.checkpoint } };
            }
            fn resolve(ptr: *anyopaque, _: Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                return self.table;
            }
            fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
                return error.InvalidSqlBackendResponse;
            }
            fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
                return error.UnsupportedSqlExecution;
            }
            fn checkpoint(ptr: *anyopaque) !void {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                try self.engine.checkpoint();
            }
            fn hasPatterns(node: *const binding.Node, depth: usize) bool {
                if (depth == 64) return true;
                // The primitive column-page codec cannot carry typed arrays.
                // Native ColumnPage readers carry complete Datums, including
                // typed arrays. Only owner-bound pattern handles need rows.
                return switch (node.operation) {
                    .query => |query| blk: {
                        if (query.binding.aggregate) |aggregate| for (aggregate.specs) |spec| if (spec.kind == .pattern_set) break :blk true;
                        break :blk hasPatterns(query.source, depth + 1);
                    },
                    .join => |join| hasPatterns(join.left, depth + 1) or hasPatterns(join.right, depth + 1),
                    .apply => |apply| hasPatterns(apply.left, depth + 1) or hasPatterns(apply.right, depth + 1),
                    .set => |set| hasPatterns(set.left, depth + 1) or hasPatterns(set.right, depth + 1),
                    .values => |arms| for (arms) |arm| {
                        if (hasPatterns(arm, depth + 1)) break true;
                    } else false,
                    .materialized_ref => |source| hasPatterns(source, depth + 1),
                    .recursive, .recursive_ref => true,
                    else => false,
                };
            }
            /// Preserve columns across a relational adapter instead of building
            /// a JSON object per joined/projected row for the next operator.
            fn nextColumns(ptr: *anyopaque, a: Allocator, limit: u32) anyerror!catalog.ColumnPage {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                if (self.pending_failure) |err| return err;
                const wanted = @min(limit, @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.iterator.node.columns.len * @sizeOf(Datum) * 16)));
                var failure: ?anyerror = null;
                const batch = try self.iterator.nextBatch(a, wanted, &failure);
                self.pending_failure = failure;
                if (batch.len() == 0) if (failure) |err| return err;
                const values = try a.create(@import("execution_batch.zig").Batch);
                values.* = batch;
                const names = try a.alloc([]const u8, self.iterator.node.columns.len);
                for (self.iterator.node.columns, names) |definition, *name| name.* = definition.internal;
                const selection = try a.alloc(usize, batch.len());
                for (selection, 0..) |*index, offset| index.* = offset;
                self.ordinal += batch.len();
                return .{ .native = .{ .values = values, .names = names }, .selection = selection, .after = if (batch.len() != 0) try std.fmt.allocPrint(a, "{d}", .{self.ordinal}) else null };
            }
            fn open(ptr: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                if (self.opened) return error.InvalidSqlBackendResponse;
                self.opened = true;
                return .{ .ptr = self, .next = next, .next_columns = if (hasPatterns(self.iterator.node, 0)) null else nextColumns, .close = close };
            }
            fn close(_: *anyopaque) void {}
            fn next(ptr: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                // Row adapters share the same memory-aware demand ceiling as
                // typed pages; an aggregate does not need oversized row pages.
                const wanted = @min(limit, @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.iterator.node.columns.len * @sizeOf(Datum) * 16)));
                var rows: std.ArrayList(catalog.Row) = .empty;
                var bytes: usize = 0;
                var stopped_for_bytes = false;
                const names = try alloc.alloc([]const u8, self.iterator.node.columns.len);
                for (self.iterator.node.columns, names) |column, *name| name.* = column.internal;
                const layout = try catalog.Row.TypedLayout.init(alloc, names);
                while (rows.items.len < wanted) {
                    const values = try self.iterator.nextDemand(alloc, wanted - rows.items.len) orelse break;
                    for (values) |value| bytes +|= try operators.datumBytes(value);
                    self.ordinal += 1;
                    const id = try std.fmt.allocPrint(alloc, "{d}", .{self.ordinal});
                    try rows.append(alloc, try catalog.Row.fromDatums(alloc, id, layout, values));
                    if (bytes >= self.engine.context.limits.page_bytes) {
                        stopped_for_bytes = true;
                        break;
                    }
                }
                const more = rows.items.len == wanted or stopped_for_bytes;
                return .{ .rows = try rows.toOwnedSlice(alloc), .after = if (more) try std.fmt.allocPrint(alloc, "{d}", .{self.ordinal}) else null };
            }
        };
    };
}

fn Source(comptime Context: type) type {
    return struct {
        const Self = @This();
        alloc: Allocator,
        read: ?catalog.StatementRead = null,
        regex_execution: @import("regex_execution.zig"),
        single: ?catalog.Cursor = null,
        single_list: [1]catalog.Cursor = undefined,
        engine: Engine(Context),
        iterator: ?*Engine(Context).Iterator = null,
        adapter: Engine(Context).Adapter = undefined,

        fn create(context: Context) !*Self {
            const relation = context.binding.relation orelse return error.InvalidSqlBackendResponse;
            const self = try context.alloc.create(Self);
            self.* = .{ .alloc = context.alloc, .regex_execution = .init(context.alloc, @min(16 * 1024 * 1024, context.limits.retained_bytes)), .engine = .{ .context = context, .cursors = &.{}, .cache_arena = .init(context.alloc) } };
            errdefer self.close();
            if (self.engine.context.backend.regex_execution == null) self.engine.context.backend.regex_execution = &self.regex_execution;
            var search_arena = std.heap.ArenaAllocator.init(context.alloc);
            defer search_arena.deinit();
            const scans = try bindSearchScans(search_arena.allocator(), relation.scans, context.parameters);
            if (scans.len != 0) {
                if (context.statement_capture) |capture| {
                    self.engine.cursors = try capture.cursors(relation);
                } else if (context.backend.vtable.open_statement) |open| {
                    self.read = try open(context.backend.ptr, context.alloc, scans);
                    self.engine.cursors = self.read.?.cursors;
                    if (self.engine.cursors.len != relation.scans.len) return error.InvalidSqlBackendResponse;
                } else if (relation.scans.len == 1) {
                    const open = context.backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
                    self.single = (try open(context.backend.ptr, context.alloc, scans[0].table, scans[0].request)) orelse return error.SqlStatementSnapshotRequired;
                    self.single_list[0] = self.single.?;
                    self.engine.cursors = &self.single_list;
                } else return error.SqlStatementSnapshotRequired;
            }
            self.iterator = try Engine(Context).Iterator.create(&self.engine, relation.root);
            self.adapter = .{ .engine = &self.engine, .iterator = self.iterator.?, .table = relation.table };
            return self;
        }
        fn close(self: *Self) void {
            if (self.iterator) |iterator| iterator.deinit();
            self.engine.deinit();
            if (self.read) |read| read.close(read.ptr);
            if (self.single) |cursor| cursor.close(cursor.ptr);
            self.regex_execution.deinit();
            self.alloc.destroy(self);
        }
        fn next(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
            const self: *Self = @ptrCast(@alignCast(raw));
            return Engine(Context).Adapter.next(&self.adapter, alloc, limit);
        }
        fn nextColumns(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.ColumnPage {
            const self: *Self = @ptrCast(@alignCast(raw));
            return Engine(Context).Adapter.nextColumns(&self.adapter, alloc, limit);
        }
        fn closeCursor(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            self.close();
        }
    };
}

pub fn openCursor(context: anytype) !catalog.Cursor {
    const owner = try Source(@TypeOf(context)).create(context);
    return .{ .ptr = owner, .next = @TypeOf(owner.*).next, .next_columns = if (Engine(@TypeOf(context)).Adapter.hasPatterns(owner.iterator.?.node, 0)) null else @TypeOf(owner.*).nextColumns, .close = @TypeOf(owner.*).closeCursor };
}

pub fn execute(context: anytype) anyerror!@import("runtime.zig").Output {
    const owner = try Source(@TypeOf(context)).create(context);
    defer owner.close();
    var lowered = owner.engine.context;
    lowered.backend = owner.adapter.iface();
    lowered.binding.relation = null;
    return lowered.select(context.binding.relation.?.statement);
}
