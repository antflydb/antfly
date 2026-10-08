// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const compiler = @import("antfly_local_sources").sql_compiler;
const runtime = @import("antfly_local_sources").sql_runtime;
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;
const Backend = struct {
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedBackendCall;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

test "SQL ordered-set execution preserves grouped namespaces and shares compatible streams" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, rows: []const u8 }{
        .{ .sql = "SELECT g,percentile_cont(g/10.0) WITHIN GROUP (ORDER BY x) FROM (SELECT 1 AS g,2 AS x UNION ALL SELECT 1,4) t GROUP BY g", .rows = "[[\"1\",2.2]]" },
        .{ .sql = "SELECT mode() WITHIN GROUP (ORDER BY t.x DESC) FILTER (WHERE t.x>0) FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[\"3\"]]" },
        .{ .sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x),percentile_disc(0.5) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x),COUNT(*) FROM (SELECT 1 AS x UNION ALL SELECT 3 UNION ALL SELECT 3 UNION ALL SELECT 7) t", .rows = "[[3,\"3\",\"3\",\"4\"]]" },
        .{ .sql = "SELECT g,percentile_disc(NULL) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x) FROM (SELECT 2 AS g,10 AS x UNION ALL SELECT 1,3 UNION ALL SELECT 2,20 UNION ALL SELECT 1,1) t GROUP BY g ORDER BY g", .rows = "[[\"1\",null,\"1\"],[\"2\",null,\"10\"]]" },
        .{ .sql = "SELECT g,percentile_disc(NULL) WITHIN GROUP (ORDER BY x) FROM (SELECT 2 AS g,10 AS x UNION ALL SELECT 1,3 UNION ALL SELECT 2,20 UNION ALL SELECT 1,1) t GROUP BY g ORDER BY g", .rows = "[[\"1\",null],[\"2\",null]]" },
        .{ .sql = "SELECT g,mode() WITHIN GROUP (ORDER BY x) FILTER (WHERE x>5),percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT 2 AS g,10 AS x UNION ALL SELECT 1,3 UNION ALL SELECT 2,20 UNION ALL SELECT 1,1) t GROUP BY g HAVING COUNT(*)=2 ORDER BY g", .rows = "[[\"1\",null,2],[\"2\",\"10\",15]]" },
        .{ .sql = "SELECT percentile_cont(NULL) WITHIN GROUP (ORDER BY x),mode() WITHIN GROUP (ORDER BY x),COUNT(*) FROM (SELECT 1 AS x) t WHERE false", .rows = "[[null,null,\"0\"]]" },
        .{ .sql = "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM (SELECT 9007199254740993 AS x UNION ALL SELECT 9007199254740995) t", .rows = "[[\"9007199254740993\"]]" },
        .{ .sql = "SELECT array_length(percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x),1) FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[\"3\"]]" },
        .{ .sql = "SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM ARRAY[1.5,NULL,2.5]::float8[] FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[true]]" },
        .{ .sql = "SELECT percentile_disc('[0:2]={0.25,NULL,0.75}'::float8[]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM '[0:2]={1,NULL,3}'::bigint[] FROM (SELECT 1::bigint AS x UNION ALL SELECT 3::bigint) t", .rows = "[[true]]" },
        .{ .sql = "SELECT percentile_cont('{}'::float8[]) WITHIN GROUP (ORDER BY x) IS NOT DISTINCT FROM '{}'::float8[] FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[true]]" },
        .{ .sql = "SELECT array_length(percentile_disc(ARRAY[[0.25,NULL],[0.75,1.0]]) WITHIN GROUP (ORDER BY x),2) FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[\"2\"]]" },
        .{ .sql = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FILTER(WHERE x>10),mode() WITHIN GROUP(ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .rows = "[[null,\"1\"]]" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        const actual = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings(case.rows, actual);
    }
    // Public array results retain their element descriptor and SQL-null flags;
    // they are not approximated by untyped JSON arrays.
    var array = try compiler.compile(std.testing.allocator, "SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 3) t", .{});
    defer array.deinit();
    var array_result = try runtime.execute(std.testing.allocator, backend.backend(), &array, &.{}, .{});
    defer array_result.deinit();
    try std.testing.expectEqual(@import("antfly_local_sources").sql_array_value.ElementType.float64, array_result.output.columns[0].element_type.?);
    const envelope = array_result.output.rows[0][0];
    const values = envelope.object.get("values").?.array.items;
    try std.testing.expectEqual(@as(f64, 1.5), values[0].float);
    try std.testing.expectEqual(@as(f64, 2.5), values[2].float);
    try std.testing.expect(envelope.object.get("sql_nulls").?.array.items[1].bool);
    const invalid = [_]struct { sql: []const u8, code: []const u8 }{
        .{ .sql = "SELECT COUNT(*) WITHIN GROUP (ORDER BY 1)", .code = "42809" },
        .{ .sql = "SELECT percentile_cont(0.5)", .code = "42883" },
        .{ .sql = "SELECT percentile_cont(0.5,1.0)", .code = "42809" },
        .{ .sql = "SELECT mode()", .code = "42883" },
        .{ .sql = "SELECT mode(1)", .code = "42809" },
        .{ .sql = "SELECT percentile_cont(x) WITHIN GROUP (ORDER BY x) FROM (SELECT 1 AS x,2 AS g) t GROUP BY g", .code = "42803" },
        .{ .sql = "SELECT percentile_cont(SUM(x)) WITHIN GROUP (ORDER BY x) FROM (SELECT 1 AS x) t", .code = "42803" },
        .{ .sql = "SELECT mode() WITHIN GROUP (ORDER BY 1) OVER ()", .code = "0A000" },
        .{ .sql = "SELECT percentile_cont(-1) WITHIN GROUP(ORDER BY x) FROM (SELECT 1 AS x) t WHERE false", .code = "22003" },
        .{ .sql = "SELECT percentile_cont(0.5) WITHIN GROUP(ORDER BY true)", .code = "42883" },
    };
    for (invalid) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        if (runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{})) |value| {
            var result = value;
            result.deinit();
            return error.ExpectedOrderedSetBindingFailure;
        } else |err| try std.testing.expectEqualStrings(case.code, @import("antfly_local_sources").sql_errors.describe(err).code);
    }
}

test "SQL masked Apply preserves PostgreSQL conditional subquery demand and NULL truth" {
    const fixture = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, rows: []const std.json.Value },
        errors: []const struct { sql: []const u8, code: []const u8 },
    }, std.testing.allocator, @import("antfly_local_sources").sql_parity_fixtures.conditional_subquery_reference, .{});
    defer fixture.deinit();
    var backend: Backend = .{};
    for (fixture.value.entries) |entry| {
        var compiled = try compiler.compile(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        var result = runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}) catch |err| {
            std.debug.print("masked Apply contract failed: {s} {s}\n", .{ entry.sql, @errorName(err) });
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(entry.rows.len, result.output.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.output.columns.len);
        if (entry.rows.len == 0) continue;
        for (result.output.rows, result.output.sql_nulls.?, entry.rows) |row, nulls, want| {
            try std.testing.expectEqual(@as(usize, 1), row.len);
            try std.testing.expectEqual(want == .null, nulls[0]);
            switch (want) {
                .null => try std.testing.expect(row[0] == .null),
                .bool => |value| try std.testing.expectEqual(value, row[0].bool),
                .integer => |value| try std.testing.expectEqual(value, try std.fmt.parseInt(i64, row[0].string, 10)),
                else => return error.UnexpectedConditionalContractValue,
            }
        }
    }
    for (fixture.value.errors) |entry| {
        var compiled = try compiler.compile(std.testing.allocator, entry.sql, .{});
        defer compiled.deinit();
        if (runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{})) |value| {
            var result = value;
            result.deinit();
            std.debug.print("conditional subquery unexpectedly succeeded: {s}\n", .{entry.sql});
            return error.ExpectedConditionalSubqueryFailure;
        } else |err| try std.testing.expectEqualStrings(entry.code, @import("antfly_local_sources").sql_errors.describe(err).code);
    }
}

test "SQL global aggregate invocation constants preserve parameter types and empty rows" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT COALESCE(o.x,7)+COUNT(*) FROM (SELECT 1 AS y WHERE FALSE) i) FROM (SELECT $1::bigint AS x) o", .{});
    defer compiled.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqual(ast.ColumnType.integer, description.binding.parameter_types[0].?);
    for ([_]std.json.Value{ .null, .{ .integer = 9 }, .{ .integer = -3 } }, [_][]const u8{ "7", "9", "-3" }) |value, expected| {
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{value}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings(expected, result.output.rows[0][0].string);
        try std.testing.expect(!result.output.sql_nulls.?[0][0]);
    }
}

test "SQL global aggregate invocation constants unwind every allocation failure" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(a, "SELECT (SELECT (SELECT o.x+q.z+COUNT(*) FROM (SELECT 1 AS y WHERE FALSE) i) FROM (SELECT 3 AS z) q) FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqualStrings("4", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("5", result.output.rows[1][0].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL global aggregate invocation constants survive spill-backed nested result cursors" {
    var backend: Backend = .{};
    var iface = backend.backend();
    iface.execution_io = std.testing.io;
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT (SELECT o.x+q.z+COUNT(*) FROM (SELECT 1 AS y WHERE FALSE) i) FROM (SELECT 3 AS z) q) FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, iface, &compiled, &.{}, .{ .spill_bytes = 1024 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expectEqualStrings("4", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("5", result.output.rows[1][0].string);
}

test "SQL sorted scalar outputs retain computed aliases and NULL provenance" {
    var backend: Backend = .{};
    for ([_][]const u8{
        "SELECT o.x+1 AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY rank DESC LIMIT 1",
        "SELECT (SELECT o.x+1) AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY rank DESC LIMIT 1",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("rank", result.output.columns[0].name);
        try std.testing.expectEqualStrings("v", result.output.columns[1].name);
        try std.testing.expectEqualStrings("3", result.output.rows[0][0].string);
        try std.testing.expect(result.output.sql_nulls.?[0][1]);
    }
}

test "SQL sorted wildcard scalar outputs bind ordinals before selection" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, rows: []const u8 }{
        .{ .sql = "SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 DESC LIMIT 1", .rows = "[[\"2\",null]]" },
        .{ .sql = "SELECT o.*,o.x+10 AS rank,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1", .rows = "[[\"2\",\"12\",null]]" },
        .{ .sql = "SELECT o.*,(SELECT o.x+10) AS rank FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1", .rows = "[[\"2\",\"12\"]]" },
        .{ .sql = "SELECT o.*,(SELECT o.x+10) AS rank FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 ASC LIMIT 1 OFFSET 1", .rows = "[[\"2\",\"12\"]]" },
        .{ .sql = "SELECT o.*,o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 2 DESC LIMIT 1", .rows = "[[\"2\",\"2\",null]]" },
        .{ .sql = "WITH c(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT c.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE c.x=1) AS v FROM c ORDER BY 1 DESC LIMIT 1", .rows = "[[\"2\",null]]" },
        .{ .sql = "SELECT p.k,l.x,l.v FROM (SELECT 1 AS k) p CROSS JOIN LATERAL (SELECT q.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE q.x=p.k) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) q ORDER BY 1 DESC LIMIT 1) l", .rows = "[[\"1\",\"2\",null]]" },
        .{ .sql = "SELECT * FROM ((SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 DESC LIMIT 1) UNION ALL SELECT 3,CAST(NULL AS BIGINT)) s ORDER BY 1", .rows = "[[\"2\",null],[\"3\",null]]" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        const rows = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(rows);
        try std.testing.expectEqualStrings(case.rows, rows);
    }
    var skipped = try compiler.compile(std.testing.allocator, "SELECT o.*,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE o.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 ASC LIMIT 1 OFFSET 1", .{});
    defer skipped.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &skipped, &.{}, .{}));
}

test "SQL sorted wildcard ordinal staging releases all allocations" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(a, "SELECT o.*,(SELECT o.x+10) AS rank FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY 1 ASC LIMIT $1 OFFSET $2", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{ .{ .integer = 1 }, .{ .integer = 1 } }, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings("12", result.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL window input subqueries retain partition ordering and filter domains" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, rows: []const u8 }{
        .{ .sql = "SELECT t.x,row_number() OVER (PARTITION BY (SELECT t.x%2) ORDER BY (SELECT t.x) DESC) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY t.x", .rows = "[[\"1\",\"2\"],[\"2\",\"1\"],[\"3\",\"1\"]]" },
        .{ .sql = "SELECT t.x,row_number() OVER w AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t WINDOW w AS (PARTITION BY (SELECT t.x%2) ORDER BY (SELECT t.x) DESC) ORDER BY t.x", .rows = "[[\"1\",\"2\"],[\"2\",\"1\"],[\"3\",\"1\"]]" },
        .{ .sql = "SELECT t.x,SUM(t.x) FILTER (WHERE t.x>1) OVER (ORDER BY (SELECT t.x) ROWS UNBOUNDED PRECEDING) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY t.x", .rows = "[[\"1\",null],[\"2\",\"2\"],[\"3\",\"5\"]]" },
        .{ .sql = "SELECT row_number() OVER (ORDER BY CASE WHEN t.x=1 THEN (SELECT t.x) ELSE (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) END) FROM (SELECT 1 AS x) t", .rows = "[[\"1\"]]" },
        .{ .sql = "SELECT COUNT((SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FILTER (WHERE false) OVER (ORDER BY (SELECT t.x)) FROM (SELECT 1 AS x) t", .rows = "[[\"0\"]]" },
        .{ .sql = "SELECT row_number() OVER (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FROM (SELECT 1 AS x) t WHERE false", .rows = "[]" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        const rows = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(rows);
        try std.testing.expectEqualStrings(case.rows, rows);
    }
    var filtered = try compiler.compile(std.testing.allocator, "SELECT COUNT(*) FILTER (WHERE false) OVER (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i)) FROM (SELECT 1 AS x) t", .{});
    defer filtered.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &filtered, &.{}, .{}));
    var unused = try compiler.compile(std.testing.allocator, "SELECT 1 WINDOW unused AS (ORDER BY (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i))", .{});
    defer unused.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &unused, &.{}, .{}));
}

test "SQL window input subquery staging releases every allocation" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(a, "SELECT t.x,row_number() OVER w,rank() OVER w FROM (SELECT 1 AS x UNION ALL SELECT 2) t WINDOW w AS (ORDER BY (SELECT t.x) DESC) ORDER BY t.x", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqualStrings("2", result.output.rows[0][1].string);
            try std.testing.expectEqualStrings("row_number", result.output.columns[1].name);
            try std.testing.expectEqualStrings("rank", result.output.columns[2].name);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL phase outputs evaluate scalar children after grouping and windows" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, rows: []const u8 }{
        .{ .sql = "SELECT t.x,(SELECT t.x+10) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x", .rows = "[[\"1\",\"11\"],[\"2\",\"12\"]]" },
        .{ .sql = "SELECT SUM(t.x),(SELECT 1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) t", .rows = "[[\"3\",\"1\"]]" },
        .{ .sql = "SELECT t.x,row_number() OVER (ORDER BY t.x) AS n,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x DESC LIMIT 1", .rows = "[[\"2\",\"2\",null]]" },
        .{ .sql = "SELECT t.x,COUNT(*)+(SELECT t.x) AS n FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>1 ORDER BY t.x", .rows = "[[\"2\",\"3\"]]" },
        .{ .sql = "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY 1 ORDER BY k", .rows = "[[\"2\",\"2\",\"9\"],[\"3\",\"1\",\"9\"]]" },
        .{ .sql = "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY k ORDER BY k", .rows = "[[\"2\",\"2\",\"9\"],[\"3\",\"1\",\"9\"]]" },
        .{ .sql = "SELECT t.x+1 AS k,COUNT(*),(SELECT 9) AS v FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY x+1 ORDER BY k", .rows = "[[\"2\",\"2\",\"9\"],[\"3\",\"1\",\"9\"]]" },
        .{ .sql = "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY COUNT(*) DESC),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x", .rows = "[[\"1\",\"2\",\"1\",\"1\"],[\"2\",\"1\",\"2\",\"2\"]]" },
        .{ .sql = "SELECT COUNT(*),(SELECT 7) FROM (SELECT 1 AS x) t WHERE false", .rows = "[[\"0\",\"7\"]]" },
        .{ .sql = "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) FROM (SELECT 1 AS x) t WHERE false GROUP BY t.x", .rows = "[]" },
        .{ .sql = "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x DESC LIMIT 1", .rows = "[[\"2\",null]]" },
        .{ .sql = "SELECT t.x,(SELECT t.x FROM (SELECT 3 AS x) t) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x", .rows = "[[\"1\",\"3\"],[\"2\",\"3\"]]" },
        .{ .sql = "SELECT t.x,(SELECT x+10) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x", .rows = "[[\"1\",\"11\"],[\"2\",\"12\"]]" },
        .{ .sql = "SELECT CASE WHEN COUNT(*)=0 THEN (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i) ELSE 1 END FROM (SELECT 1 AS x) t", .rows = "[[\"1\"]]" },
        .{ .sql = "WITH c(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT t.x,(SELECT c.x FROM c WHERE c.x=t.x) FROM c t GROUP BY t.x ORDER BY t.x", .rows = "[[\"1\",\"1\"],[\"2\",\"2\"]]" },
        .{ .sql = "SELECT p.k,l.x,l.v FROM (SELECT 1 AS k) p CROSS JOIN LATERAL (SELECT q.x,(SELECT q.x+p.k) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) q GROUP BY q.x ORDER BY q.x DESC LIMIT 1) l", .rows = "[[\"1\",\"2\",\"3\"]]" },
        .{ .sql = "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY t.x),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x HAVING t.x>1 ORDER BY t.x", .rows = "[[\"2\",\"1\",\"1\",\"2\"]]" },
        .{ .sql = "SELECT t.x,COUNT(*),row_number() OVER (ORDER BY t.x),(SELECT t.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>1 ORDER BY t.x", .rows = "[[\"2\",\"1\",\"1\",\"2\"]]" },
        .{ .sql = "SELECT t.x,CASE WHEN row_number() OVER (ORDER BY t.x)=1 THEN (SELECT t.x+10) ELSE 0 END FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x", .rows = "[[\"1\",\"11\"],[\"2\",\"0\"]]" },
        .{ .sql = "SELECT t.x,SUM((SELECT t.x)) OVER (ORDER BY t.x)+(SELECT 1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY t.x", .rows = "[[\"1\",\"2\"],[\"2\",\"4\"]]" },
        .{ .sql = "SELECT * FROM ((SELECT t.x,(SELECT t.x+10) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x DESC LIMIT 1) UNION ALL SELECT 3,13) s ORDER BY 1", .rows = "[[\"2\",\"12\"],[\"3\",\"13\"]]" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}) catch |err| {
            std.debug.print("phase output failed: {s}: {s}\n", .{ case.sql, @errorName(err) });
            return err;
        };
        defer result.deinit();
        const rows = try std.json.Stringify.valueAlloc(std.testing.allocator, result.output.rows, .{});
        defer std.testing.allocator.free(rows);
        try std.testing.expectEqualStrings(case.rows, rows);
    }
}

test "SQL phase outputs retain aggregate and EXISTS labels" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT COUNT(*),EXISTS(SELECT 1) FROM (SELECT 1 AS x) t", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("count", result.output.columns[0].name);
    try std.testing.expectEqualStrings("exists", result.output.columns[1].name);
    try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
    try std.testing.expect(result.output.rows[0][1].bool);
}

test "SQL phase outputs preserve grouping and child cardinality diagnostics" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, code: []const u8 }{
        .{ .sql = "SELECT COUNT(*),(SELECT t.x) FROM (SELECT 1 AS x) t", .code = "42803" },
        .{ .sql = "SELECT t.x,(SELECT t.y) FROM (SELECT 1 AS x,2 AS y) t GROUP BY t.x", .code = "42803" },
        .{ .sql = "SELECT t.x+1,(SELECT t.x+1) FROM (SELECT 1 AS x) t GROUP BY t.x+1", .code = "42803" },
        .{ .sql = "SELECT t.x,(SELECT t.missing) FROM (SELECT 1 AS x) t GROUP BY t.x", .code = "42703" },
        .{ .sql = "SELECT t.x,(SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 2) i WHERE t.x=1) FROM (SELECT 1 AS x UNION ALL SELECT 2) t GROUP BY t.x ORDER BY t.x ASC LIMIT 1 OFFSET 1", .code = "21000" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        if (runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{})) |value| {
            var result = value;
            result.deinit();
            return error.ExpectedPhaseOutputFailure;
        } else |err| try std.testing.expectEqualStrings(case.code, @import("antfly_local_sources").sql_errors.describe(err).code);
    }
}

test "SQL phase outputs release every allocation with prepared grouped demand" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(a, "SELECT t.x,COUNT(*)+(SELECT t.x+$1) AS n FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t GROUP BY t.x HAVING (SELECT t.x)>$2 ORDER BY n DESC,t.x DESC LIMIT $3 OFFSET $4", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend.backend(), &compiled, &.{ .{ .integer = 2 }, .{ .integer = 0 }, .{ .integer = 1 }, .{ .integer = 1 } }, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("5", result.output.rows[0][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL row bounds validate negative parameters at execution in their own domain" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, code: []const u8 }{
        .{ .sql = "SELECT 1 LIMIT $1", .code = "2201W" },
        .{ .sql = "SELECT 1 OFFSET $1", .code = "2201X" },
        .{ .sql = "SELECT 1 FETCH NEXT $1 ROWS ONLY", .code = "2201W" },
        .{ .sql = "SELECT i.x FROM (SELECT 1 AS x) i LIMIT $1", .code = "2201W" },
        .{ .sql = "SELECT i.x FROM (SELECT 1 AS x) i OFFSET $1", .code = "2201X" },
        .{ .sql = "SELECT (SELECT i.x FROM (SELECT 1 AS x) i LIMIT $1)", .code = "2201W" },
        .{ .sql = "SELECT (SELECT i.x FROM (SELECT 1 AS x) i OFFSET $1)", .code = "2201X" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        if (runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = -1 }}, .{})) |value| {
            var result = value;
            result.deinit();
            return error.ExpectedNegativeRowBoundFailure;
        } else |err| try std.testing.expectEqualStrings(case.code, @import("antfly_local_sources").sql_errors.describe(err).code);
    }
    // Binding negative literal bounds must not reject an undemanded child.
    var compiled = try compiler.compile(std.testing.allocator, "SELECT CASE WHEN FALSE THEN (SELECT 1 LIMIT -1) ELSE 7 END", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("7", result.output.rows[0][0].string);
}

test "SQL scalar cardinality NULL limits retain unbounded result admission" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, parameters: []const std.json.Value = &.{} }{
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2) o LIMIT NULL" },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2) o LIMIT $1", .parameters = &.{.null} },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2) o LIMIT ALL" },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2) o FETCH NEXT NULL ROWS ONLY" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, backend.backend(), &compiled, case.parameters, .{ .result_rows = 1 }));
    }
}

test "SQL nested join buffers own borrowed text and JSON across upstream pulls" {
    const Fixture = struct {
        const Owner = @This();
        const Cursor = struct {
            offset: usize = 0,
            payload: [32]u8 = undefined,
            fn next(ptr: *anyopaque, a: std.mem.Allocator, _: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.offset == 9) return .{ .rows = &.{} };
                const text = try std.fmt.bufPrint(&self.payload, "payload-{d:0>3}", .{self.offset});
                var object: std.json.ObjectMap = .empty;
                var metadata: std.json.ObjectMap = .empty;
                try metadata.put(a, "text", .{ .string = text });
                try object.put(a, "id", .{ .integer = @intCast(self.offset) });
                try object.put(a, "label", .{ .string = text });
                try object.put(a, "metadata", .{ .object = metadata });
                const rows = try a.alloc(catalog.Row, 1);
                rows[0] = .{ .id = text, .version = 1, .value = .{ .object = object } };
                self.offset += 1;
                return .{ .rows = rows, .after = if (self.offset < 9) "more" else null };
            }
        };
        states: [2]Cursor = .{ .{}, .{} },
        cursors: [2]catalog.Cursor = undefined,
        closes: usize = 0,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .integer },
                .{ .name = "label", .path = "label", .type = .string },
                .{ .name = "metadata", .path = "metadata", .type = .json },
            } };
        }
        fn capture(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *Owner = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), scans.len);
            for (&self.states, &self.cursors) |*state, *cursor| cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            return .{ .ptr = self, .cursors = &self.cursors, .close = close };
        }
        fn close(ptr: *anyopaque) void {
            const self: *Owner = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn run(a: std.mem.Allocator) !void {
            var fixture: Owner = .{};
            const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint, .open_statement = capture } };
            var compiled = try compiler.compile(a, "WITH joined_rows AS (SELECT o.id,o.label,o.metadata FROM records o JOIN records c ON o.id=c.id WHERE o.label <> 'discard') SELECT id,label,metadata FROM joined_rows ORDER BY id", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, backend, &compiled, &.{}, .{ .page_rows = 3 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 9), result.output.rows.len);
            for (result.output.rows, 0..) |row, index| {
                var buffer: [32]u8 = undefined;
                const expected = try std.fmt.bufPrint(&buffer, "payload-{d:0>3}", .{index});
                try std.testing.expectEqualStrings(expected, row[1].string);
                try std.testing.expectEqualStrings(expected, row[2].object.get("text").?.string);
            }
            try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL quantified subqueries match three valued comparison truth tables" {
    const Set = struct { sql: []const u8, values: []const ?i64 };
    const sets = [_]Set{
        .{ .sql = "SELECT 1 AS y WHERE false", .values = &.{} },
        .{ .sql = "SELECT CAST(NULL AS INTEGER) AS y", .values = &.{null} },
        .{ .sql = "SELECT 1 AS y", .values = &.{1} },
        .{ .sql = "SELECT 1 AS y UNION ALL SELECT 1", .values = &.{ 1, 1 } },
        .{ .sql = "SELECT 1 AS y UNION ALL SELECT 2", .values = &.{ 1, 2 } },
        .{ .sql = "SELECT 1 AS y UNION ALL SELECT NULL", .values = &.{ 1, null } },
        .{ .sql = "SELECT 1 AS y UNION ALL SELECT 2 UNION ALL SELECT NULL", .values = &.{ 1, 2, null } },
    };
    const operands = [_]?i64{ 0, 1, 2, null };
    var backend: Backend = .{};
    for ([_][]const u8{ "=", "<>", "<", "<=", ">", ">=" }, 0..) |op, op_index| {
        for ([_]bool{ false, true }) |every| for (sets) |set| for ([_]bool{ false, true }) |boundary| {
            const query = try std.fmt.allocPrint(std.testing.allocator, "SELECT x {s} {s} (SELECT y FROM ({s}) i{s}) FROM (SELECT 0 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT NULL) o", .{ op, if (every) "ALL" else "ANY", set.sql, if (boundary) " WHERE o.x IS NULL OR o.x IS NOT NULL ORDER BY i.y LIMIT 100" else "" });
            defer std.testing.allocator.free(query);
            var compiled = try compiler.compile(std.testing.allocator, query, .{});
            defer compiled.deinit();
            var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(operands.len, result.output.rows.len);
            for (operands, 0..) |operand, index| {
                var expected: ?bool = every;
                for (set.values) |value| {
                    if (operand == null or value == null) {
                        expected = null;
                        continue;
                    }
                    const matches = switch (op_index) {
                        0 => operand.? == value.?,
                        1 => operand.? != value.?,
                        2 => operand.? < value.?,
                        3 => operand.? <= value.?,
                        4 => operand.? > value.?,
                        5 => operand.? >= value.?,
                        else => unreachable,
                    };
                    if (matches != every) {
                        expected = matches;
                        break;
                    }
                }
                if (expected) |truth| try std.testing.expectEqual(truth, result.output.rows[index][0].bool) else try std.testing.expect(result.output.sql_nulls.?[index][0]);
            }
        };
    }
}

test "SQL quantified pattern sets preserve empty null and negated truth tables" {
    // sql-1283, sql-1284, sql-1285, sql-1286, sql-1287, sql-1293,
    // sql-1294, sql-1295, sql-1296, sql-1297: the mounted test proves
    // exact corpus output; these cases prove wildcard and NULL truth tables.
    var backend: Backend = .{};
    const Case = struct { sql: []const u8, expected: ?bool };
    const cases = [_]Case{
        .{ .sql = "SELECT 'open' LIKE ANY (SELECT 'x%' WHERE FALSE)", .expected = false },
        .{ .sql = "SELECT 'open' LIKE ALL (SELECT 'x%' WHERE FALSE)", .expected = true },
        .{ .sql = "SELECT CAST(NULL AS STRING) LIKE ANY (SELECT 'x%' WHERE FALSE)", .expected = false },
        .{ .sql = "SELECT CAST(NULL AS STRING) LIKE ALL (SELECT 'x%' WHERE FALSE)", .expected = true },
        .{ .sql = "SELECT 'open' LIKE ANY (SELECT p FROM (SELECT 'op%' AS p UNION ALL SELECT NULL) s)", .expected = true },
        .{ .sql = "SELECT 'open' LIKE ANY (SELECT p FROM (SELECT 'x%' AS p UNION ALL SELECT NULL) s)", .expected = null },
        .{ .sql = "SELECT 'open' LIKE ALL (SELECT p FROM (SELECT 'op%' AS p UNION ALL SELECT NULL) s)", .expected = null },
        .{ .sql = "SELECT 'open' LIKE ALL (SELECT p FROM (SELECT 'x%' AS p UNION ALL SELECT NULL) s)", .expected = false },
        .{ .sql = "SELECT 'Open' ILIKE ANY (SELECT 'op%')", .expected = true },
        .{ .sql = "SELECT 'Open' LIKE SOME (SELECT 'op%')", .expected = false },
        .{ .sql = "SELECT 'open' NOT LIKE ANY (SELECT p FROM (SELECT 'op%' AS p UNION ALL SELECT 'x%') s)", .expected = true },
        .{ .sql = "SELECT 'open' NOT LIKE ALL (SELECT p FROM (SELECT 'op%' AS p UNION ALL SELECT 'x%') s)", .expected = false },
        .{ .sql = "SELECT 'Open' NOT ILIKE ANY (SELECT 'op%')", .expected = false },
        .{ .sql = "SELECT 'Open' NOT ILIKE ALL (SELECT 'x%')", .expected = true },
    };
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        if (case.expected) |expected| {
            if (result.output.rows[0][0] != .bool) std.debug.print("unexpected quantified-pattern null: {s}\n", .{case.sql});
            try std.testing.expect(result.output.rows[0][0] == .bool);
            try std.testing.expectEqual(expected, result.output.rows[0][0].bool);
        } else try std.testing.expect(result.output.sql_nulls.?[0][0]);
    }
}

test "SQL quantified pattern sets correlate once by grouped key" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(
        std.testing.allocator,
        "SELECT o.k, o.v LIKE ANY (SELECT i.p FROM (SELECT 1 AS k, 'a%' AS p UNION ALL SELECT 1, NULL UNION ALL SELECT 2, 'b%') i WHERE i.k = o.k) AS matched " ++
            "FROM (SELECT 1 AS k, 'apple' AS v UNION ALL SELECT 1, 'other' UNION ALL SELECT 2, 'banana' UNION ALL SELECT 3, 'none') o ORDER BY o.k, o.v",
        .{},
    );
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 4), result.output.rows.len);
    try std.testing.expect(result.output.rows[0][1].bool);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expect(result.output.rows[2][1].bool);
    try std.testing.expect(!result.output.rows[3][1].bool);
}

test "SQL quantified pattern parameter infers string type" {
    var backend: Backend = .{};
    for ([_][]const u8{ "SELECT 'apple' LIKE ANY (SELECT $1)", "SELECT $1 LIKE ANY (SELECT 'a%')" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .string = if (std.mem.indexOf(u8, sql, "SELECT $1 LIKE") != null) "apple" else "a%" }}, .{});
        defer result.deinit();
        try std.testing.expect(result.output.rows[0][0].bool);
    }
}

test "SQL quantified summaries correlate once and infer typed operands" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT o.x, $1 < SOME (SELECT i.y FROM (SELECT 1 AS k, 2 AS y UNION ALL SELECT 1,NULL UNION ALL SELECT 2,NULL) i WHERE i.k=o.x) AS found FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) o ORDER BY o.x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
    defer result.deinit();
    try std.testing.expect(result.output.rows[0][1].bool);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expect(!result.output.rows[2][1].bool);
}

test "SQL EXISTS validates discarded expressions without evaluating them" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT EXISTS (SELECT 1 / 0 + length(lower($1)), CAST('invalid' AS INTEGER) FROM (SELECT 1 AS x) i)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{.{ .string = "bound" }}, .{});
            defer result.deinit();
            try std.testing.expect(result.output.rows[0][0].bool);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    for ([_]struct { query: []const u8, err: anyerror }{
        .{ .query = "SELECT EXISTS (SELECT missing + 1 FROM (SELECT 1 AS x) i)", .err = error.UndefinedColumn },
        .{ .query = "SELECT EXISTS (SELECT lower(1) FROM (SELECT 1 AS x) i)", .err = error.SqlTypeMismatch },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.query, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
}

test "SQL IN subqueries preserve full three valued membership and duplicate semantics" {
    var backend: Backend = .{};
    const cases = [_]struct { sql: []const u8, expected: ?bool }{
        .{ .sql = "SELECT 1 IN (SELECT y FROM (SELECT 1 AS y UNION ALL SELECT 1 UNION ALL SELECT NULL) i)", .expected = true },
        .{ .sql = "SELECT 2 IN (SELECT y FROM (SELECT 1 AS y UNION ALL SELECT NULL) i)", .expected = null },
        .{ .sql = "SELECT 2 NOT IN (SELECT y FROM (SELECT 1 AS y UNION ALL SELECT NULL) i)", .expected = null },
        .{ .sql = "SELECT 1 NOT IN (SELECT y FROM (SELECT 1 AS y UNION ALL SELECT NULL) i)", .expected = false },
        .{ .sql = "SELECT 2 NOT IN (SELECT y FROM (SELECT 1 AS y) i)", .expected = true },
        .{ .sql = "SELECT CAST(NULL AS INTEGER) IN (SELECT y FROM (SELECT 1 AS y) i)", .expected = null },
        .{ .sql = "SELECT CAST(NULL AS INTEGER) IN (SELECT y FROM (SELECT 1 AS y) i WHERE y=0)", .expected = false },
        .{ .sql = "SELECT CAST(NULL AS INTEGER) NOT IN (SELECT y FROM (SELECT 1 AS y) i WHERE y=0)", .expected = true },
        .{ .sql = "SELECT 1 IN (SELECT y FROM (SELECT CAST(NULL AS INTEGER) AS y) i)", .expected = null },
        .{ .sql = "SELECT 2 IN (SELECT y FROM (SELECT 1 AS y) i)", .expected = false },
    };
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        if (case.expected) |expected| try std.testing.expectEqual(expected, result.output.rows[0][0].bool) else try std.testing.expect(result.output.sql_nulls.?[0][0]);
    }
}

test "SQL correlated IN groups NULL evidence by lexical correlation keys" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT o.x, o.x IN (SELECT i.y FROM (SELECT 1 AS k, 1 AS y UNION ALL SELECT 1,NULL UNION ALL SELECT 2,NULL) i WHERE i.k=o.x) AS found FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) o ORDER BY o.x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expect(result.output.rows[0][1].bool);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    try std.testing.expect(!result.output.rows[2][1].bool);
}

test "SQL membership unwinds allocations admits parameters and preserves nonkeyed correlation" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT $1 IN (SELECT x FROM (SELECT 1 AS x UNION ALL SELECT NULL) i)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
            defer result.deinit();
            try std.testing.expect(result.output.rows[0][0].bool);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT 1 IN (SELECT x FROM (SELECT 1 AS x) i)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1 }));
    var correlated = try compiler.compile(std.testing.allocator, "SELECT o.x IN (SELECT i.x FROM (SELECT 1 AS x) i WHERE i.x > o.x) FROM (SELECT 1 AS x) o", .{});
    defer correlated.deinit();
    var correlated_result = try runtime.execute(std.testing.allocator, backend.backend(), &correlated, &.{}, .{});
    defer correlated_result.deinit();
    try std.testing.expect(!correlated_result.output.rows[0][0].bool);
    try std.testing.expectError(error.SqlLimitExceeded, compiler.compile(std.testing.allocator, "SELECT 1 IN (SELECT 1 IN (SELECT 1 IN (SELECT 1)))", .{ .max_depth = 2 }));
}

test "SQL membership bounded hash projections share one capture instead of per row reads" {
    const Fixture = struct {
        const Owner = @This();
        const Cursor = struct {
            owner: *Owner,
            offset: usize = 0,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const count = @min(limit, self.owner.count - self.offset);
                const rows = try alloc.alloc(catalog.Row, count);
                for (rows, 0..) |*row, i| {
                    var object: std.json.ObjectMap = .empty;
                    try object.put(alloc, "x", .{ .integer = @intCast((self.offset + i) % self.owner.key_count) });
                    row.* = .{ .id = "id", .version = 1, .value = .{ .object = object } };
                }
                self.offset += count;
                self.owner.rows += count;
                return .{ .rows = rows, .after = if (self.offset < self.owner.count) try std.fmt.allocPrint(alloc, "{d}", .{self.offset}) else null };
            }
        };
        count: usize = 512,
        key_count: usize = 128,
        rows: usize = 0,
        captures: usize = 0,
        closes: usize = 0,
        canceled: bool = false,
        expected_scans: usize = 3,
        states: [3]Cursor = undefined,
        cursors: [3]catalog.Cursor = undefined,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = if (std.mem.eql(u8, name.table, "outer_rows")) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{ .{ .name = "x", .path = "x", .type = .integer }, .{ .name = "cold_payload", .path = "cold_payload", .type = .string } } };
        }
        fn capture(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.expected_scans, scans.len);
            for (scans) |scan| for (scan.request.fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "cold_payload"));
            self.captures += 1;
            for (&self.states, &self.cursors) |*state, *cursor| {
                state.* = .{ .owner = self };
                cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            }
            return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = close };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.QueryCanceled;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = checkpoint, .open_statement = capture } };
        }
    };
    var fixture: Fixture = .{};
    const benchmark = try std.testing.environ.contains(std.testing.allocator, "ANTFLY_SQL_MEMBERSHIP_BENCHMARK");
    // The routine fixture retains leak detection. The opt-in workload uses the
    // production allocator so debug allocator quarantine does not dominate RSS.
    const execution_alloc = if (benchmark) std.heap.smp_allocator else std.testing.allocator;
    if (benchmark) {
        fixture.count = 10000;
        fixture.key_count = 1000;
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM outer_rows o WHERE o.x+1 IN (SELECT i.x+1 FROM inner_rows i)", .{});
    defer compiled.deinit();
    {
        var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, fixture.backend(), &compiled, &.{});
        defer description.deinit();
        const join = description.binding.relation.?.root.operation.join;
        try std.testing.expectEqual(@as(usize, 1), join.left_keys.len);
        try std.testing.expectEqual(@as(usize, 1), join.right_keys.len);
    }
    const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var result = try runtime.execute(execution_alloc, fixture.backend(), &compiled, &.{}, .{ .retained_bytes = 32 * 1024 * 1024 });
    defer result.deinit();
    const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(fixture.count, try std.fmt.parseInt(usize, result.output.rows[0][0].string, 10));
    try std.testing.expectEqual(fixture.count * 3, fixture.rows);
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), fixture.closes);
    std.debug.print("SQL membership: outer_rows={d} inner_rows={d} native_scans=3 captures=1 peak_bytes={d} elapsed_ns={d}\n", .{ fixture.count, fixture.count, result.peakMemoryBytes(), elapsed });
    {
        // An independent ordered child must remain a hash build, not replay
        // its comparison rows for every parent merely because it has LIMIT.
        var independent: Fixture = .{ .count = fixture.count, .key_count = fixture.key_count };
        var ordered = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM outer_rows o WHERE o.x+1 IN (SELECT i.x+1 FROM inner_rows i ORDER BY i.x LIMIT 100000)", .{});
        defer ordered.deinit();
        var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, independent.backend(), &ordered, &.{});
        defer description.deinit();
        const join = description.binding.relation.?.root.operation.join;
        try std.testing.expectEqual(@as(usize, 1), join.left_keys.len);
        var output = try runtime.execute(execution_alloc, independent.backend(), &ordered, &.{}, .{ .retained_bytes = 32 * 1024 * 1024 });
        defer output.deinit();
        try std.testing.expectEqual(independent.count, try std.fmt.parseInt(usize, output.output.rows[0][0].string, 10));
        try std.testing.expectEqual(independent.count * 3, independent.rows);
        try std.testing.expectEqual(@as(usize, 1), independent.captures);
        try std.testing.expectEqual(@as(usize, 1), independent.closes);
    }
    fixture.canceled = true;
    try std.testing.expectError(error.QueryCanceled, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    for ([_][]const u8{
        "SELECT count(*) FROM outer_rows o WHERE o.x <= ANY (SELECT i.x FROM inner_rows i)",
        "SELECT count(*) FROM outer_rows o WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.x >= o.x)",
        "SELECT count(*) FROM outer_rows o WHERE EXISTS (SELECT lower(i.cold_payload), 1/0, CAST('bad' AS BIGINT) FROM inner_rows i)",
    }, 0..) |sql, case_index| {
        fixture.rows = 0;
        fixture.captures = 0;
        fixture.closes = 0;
        fixture.canceled = false;
        fixture.expected_scans = 2;
        var ordered = try compiler.compile(std.testing.allocator, sql, .{});
        defer ordered.deinit();
        const ordered_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        var output = try runtime.execute(execution_alloc, fixture.backend(), &ordered, &.{}, .{ .retained_bytes = 32 * 1024 * 1024 });
        defer output.deinit();
        try std.testing.expectEqual(fixture.count, try std.fmt.parseInt(usize, output.output.rows[0][0].string, 10));
        // An invariant EXISTS needs one witness, not a speculative full page.
        try std.testing.expectEqual(fixture.count + (if (case_index == 2) @as(usize, 1) else fixture.count), fixture.rows);
        try std.testing.expectEqual(@as(usize, 1), fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        std.debug.print("SQL subquery: shape={s} rows={d} native_rows={d} native_scans=2 captures=1 peak_bytes={d} elapsed_ns={d}\n", .{ if (case_index == 2) "uncorrelated-exists" else "ordered", fixture.count, fixture.rows, output.peakMemoryBytes(), std.Io.Clock.awake.now(std.testing.io).nanoseconds - ordered_start });
    }
    fixture.rows = 0;
    fixture.captures = 0;
    fixture.closes = 0;
    fixture.expected_scans = 3;
    var disjunctive = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM outer_rows o WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.x > o.x OR i.x = 0)", .{});
    defer disjunctive.deinit();
    const disjunctive_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var disjunctive_result = try runtime.execute(execution_alloc, fixture.backend(), &disjunctive, &.{}, .{ .retained_bytes = 32 * 1024 * 1024 });
    defer disjunctive_result.deinit();
    try std.testing.expectEqual(fixture.count, try std.fmt.parseInt(usize, disjunctive_result.output.rows[0][0].string, 10));
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), fixture.closes);
    try std.testing.expect(fixture.rows >= fixture.count * 2 and fixture.rows <= fixture.count * 3);
    std.debug.print("SQL subquery: shape=disjunctive outer_rows={d} native_rows={d} native_scans=3 captures=1 peak_bytes={d} elapsed_ns={d}\n", .{ fixture.count, fixture.rows, disjunctive_result.peakMemoryBytes(), std.Io.Clock.awake.now(std.testing.io).nanoseconds - disjunctive_start });
}

test "SQL complete uncorrelated value relations preserve grouped set window and topK semantics" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, expected: []const u8 }{
        .{ .sql = "SELECT (SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 3) t ORDER BY x DESC LIMIT 1)", .expected = "3" },
        .{ .sql = "SELECT (SELECT sum(x) AS total FROM (SELECT 1 AS x UNION ALL SELECT 3) t GROUP BY x HAVING sum(x) > 1)", .expected = "3" },
        .{ .sql = "SELECT (SELECT 1 UNION SELECT 1)", .expected = "1" },
        .{ .sql = "SELECT (WITH t AS (SELECT 3 AS x) SELECT x FROM t)", .expected = "3" },
        .{ .sql = "SELECT (SELECT row_number() OVER (ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 3) t ORDER BY x DESC LIMIT 1)", .expected = "2" },
        .{ .sql = "SELECT 3 IN (SELECT 1 UNION SELECT 3)", .expected = "true" },
        .{ .sql = "SELECT 2 < ALL (SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 3) t ORDER BY x DESC LIMIT 1)", .expected = "true" },
        .{ .sql = "SELECT 3 = ANY (SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT 3) t GROUP BY x)", .expected = "true" },
        .{ .sql = "SELECT 0 = ANY (SELECT count(*) FROM (SELECT 1 AS x) t WHERE false)", .expected = "true" },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        const value = result.output.rows[0][0];
        if (value == .string) try std.testing.expectEqualStrings(case.expected, value.string) else try std.testing.expectEqualStrings(case.expected, if (value.bool) "true" else "false");
    }
    var multiple = try compiler.compile(std.testing.allocator, "SELECT (SELECT 1 UNION ALL SELECT 2)", .{});
    defer multiple.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &multiple, &.{}, .{}));
}

test "SQL equality correlated EXISTS and NOT EXISTS decorrelate without duplicate outer rows" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, expected: i64 }{
        .{ .sql = "SELECT o.x FROM (SELECT 1 AS x UNION ALL SELECT 2) o WHERE EXISTS (SELECT 1 FROM (SELECT 1 AS y UNION ALL SELECT 1) i WHERE i.y=o.x)", .expected = 1 },
        .{ .sql = "SELECT o.x FROM (SELECT 1 AS x UNION ALL SELECT 2) o WHERE NOT EXISTS (SELECT 1 FROM (SELECT 1 AS y UNION ALL SELECT 1) i WHERE i.y=o.x)", .expected = 2 },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqual(case.expected, try std.fmt.parseInt(i64, result.output.rows[0][0].string, 10));
    }
}

test "SQL OR-correlated EXISTS distributes bounded witnesses without per-row scans" {
    // Exact mounted cases sql-1300, sql-1301, sql-1303, sql-1304 and sql-1305
    // share this decorrelation path; these smaller cases isolate its semantics.
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, expected: []const bool }{
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y, 'miss' AS state UNION ALL SELECT 3, 'hit') i WHERE i.y > o.x OR i.state = 'hit') FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, true, true },
        },
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y, 'miss' AS state UNION ALL SELECT 3, 'hit') i WHERE i.y > o.x OR i.state IN (SELECT t.state FROM (SELECT 'hit' AS state) t)) FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, true, true },
        },
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y UNION ALL SELECT 3) i WHERE i.y > o.x OR i.y IN (SELECT z.y FROM (SELECT 3 AS y) z WHERE z.y = i.y)) FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, true, true },
        },
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y UNION ALL SELECT 3) i WHERE i.y > o.x OR i.y IN (SELECT o.x FROM (SELECT 3 AS x) o)) FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, true, true },
        },
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y, 'miss' AS state UNION ALL SELECT 3, 'miss') i WHERE (i.y > o.x AND (i.state = 'miss' OR i.y = 9)) OR i.state = 'hit') FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, false, false },
        },
        .{
            .sql = "SELECT NOT EXISTS (SELECT 1 FROM (SELECT 1 AS y, 'miss' AS state UNION ALL SELECT 3, 'miss') i WHERE i.y > o.x OR i.state = 'hit') FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ false, false, true, true },
        },
        .{
            .sql = "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y, 'miss' AS state UNION ALL SELECT 3, 'miss') i WHERE i.state = 'miss' AND (i.y > o.x OR i.y = o.x)) FROM (SELECT 0 AS x UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT NULL) o",
            .expected = &.{ true, true, false, false },
        },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (case.expected, result.output.rows) |expected, row| try std.testing.expectEqual(expected, row[0].bool);
    }
    var excessive = try compiler.compile(std.testing.allocator, "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y) i WHERE i.y > o.x OR i.y >= o.x OR i.y < o.x OR i.y <= o.x OR i.y = o.x OR i.y > o.x+1 OR i.y > o.x+2 OR i.y > o.x+3 OR i.y > o.x+4) FROM (SELECT 1 AS x) o", .{});
    defer excessive.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &excessive, &.{}, .{}));
    var escaping = try compiler.compile(std.testing.allocator, "SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS y) i WHERE i.y > o.x OR i.y IN (SELECT o.x)) FROM (SELECT 1 AS x) o", .{});
    defer escaping.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, backend.backend(), &escaping, &.{}, .{}));
}

test "SQL ordered correlated EXISTS summarizes range evidence and preserves null groups" {
    var backend: Backend = .{};
    for ([_][]const u8{ "<", "<=", ">", ">=" }, 0..) |op, which| {
        for ([_]bool{ false, true }) |reverse| {
            const query = try std.fmt.allocPrint(std.testing.allocator, "SELECT EXISTS (SELECT 1 / 0 FROM (SELECT 1 AS y UNION ALL SELECT 3 UNION ALL SELECT NULL) i WHERE {s} {s} {s}) FROM (SELECT 0 AS x UNION ALL SELECT 1 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT NULL) o", .{ if (reverse) "o.x" else "i.y", op, if (reverse) "i.y" else "o.x" });
            defer std.testing.allocator.free(query);
            var compiled = try compiler.compile(std.testing.allocator, query, .{});
            defer compiled.deinit();
            var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            for ([_]?i64{ 0, 1, 3, 4, null }, 0..) |x, index| {
                var expected = false;
                if (x) |outer| for ([_]i64{ 1, 3 }) |inner| {
                    const left = if (reverse) outer else inner;
                    const right = if (reverse) inner else outer;
                    expected = expected or switch (which) {
                        0 => left < right,
                        1 => left <= right,
                        2 => left > right,
                        3 => left >= right,
                        else => unreachable,
                    };
                };
                try std.testing.expectEqual(expected, result.output.rows[index][0].bool);
            }
        }
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT NOT EXISTS (SELECT 1 FROM (SELECT 1 AS k, 1 AS y UNION ALL SELECT 2,NULL) i WHERE i.k=o.x AND i.y < o.x+1) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) o", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    for ([_]bool{ false, true, true }, 0..) |expected, i| try std.testing.expectEqual(expected, result.output.rows[i][0].bool);
}

test "SQL scalar subqueries preserve zero one and too many row semantics" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT o.x,(SELECT i.y FROM (SELECT 1 AS y) i WHERE i.y=o.x) AS v FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("1", result.output.rows[0][1].string);
    try std.testing.expect(result.output.sql_nulls.?[1][1]);
    var duplicate = try compiler.compile(std.testing.allocator, "SELECT (SELECT i.y FROM (SELECT 1 AS y UNION ALL SELECT 1) i)", .{});
    defer duplicate.deinit();
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &duplicate, &.{}, .{}));
}

test "SQL correlated aggregates restore empty COUNT and nested query scope" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x,n FROM (SELECT o.x,(SELECT count(*) FROM (SELECT 1 AS y UNION ALL SELECT 1) i WHERE i.y=o.x) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2) o) q ORDER BY x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("2", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("0", result.output.rows[1][1].string);
}

test "SQL composed correlated aggregates retain empty group defaults and computed hash keys" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT o.x, (SELECT COALESCE(SUM(i.y), 10) + COUNT(*) + COUNT(DISTINCT i.y) FILTER (WHERE i.y > 1) FROM (SELECT 1 AS k, 2 AS y UNION ALL SELECT 1,3) i WHERE i.k + 1 = o.x * 2) AS total FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings("9", result.output.rows[0][1].string);
            try std.testing.expectEqualStrings("10", result.output.rows[1][1].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT i.y + SUM(i.y) FROM (SELECT 1 AS y) i)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlGroupingError, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
}

test "SQL subquery lexical shadowing NULL keys and hidden probes preserve semantics" {
    var backend: Backend = .{};
    for ([_]struct { sql: []const u8, rows: usize, columns: usize }{
        .{ .sql = "SELECT * FROM (SELECT 1 AS x) o WHERE EXISTS (SELECT i.y FROM (SELECT 1 AS y) i WHERE i.y=o.x)", .rows = 1, .columns = 1 },
        .{ .sql = "SELECT o.x FROM (SELECT 1 AS x) o WHERE EXISTS (SELECT 1 FROM (SELECT 2 AS x) o WHERE o.x=2)", .rows = 1, .columns = 1 },
        .{ .sql = "SELECT o.x FROM (SELECT CAST(NULL AS INTEGER) AS x) o WHERE EXISTS (SELECT 1 FROM (SELECT CAST(NULL AS INTEGER) AS x) i WHERE i.x=o.x)", .rows = 0, .columns = 1 },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.rows, result.output.rows.len);
        try std.testing.expectEqual(case.columns, result.output.columns.len);
    }
    var missing = try compiler.compile(std.testing.allocator, "SELECT EXISTS (SELECT nope FROM (SELECT 1 AS x) i)", .{});
    defer missing.deinit();
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &missing, &.{}, .{}));
}

test "SQL correlated subquery admits every physical table into one capture before reads" {
    const Capture = struct {
        calls: usize = 0,
        deny_inner: bool = false,
        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(catalog.Action.read, action);
            if (self.deny_inner and std.mem.eql(u8, name.table, "inner_rows")) return error.AccessDenied;
            return .{ .id = if (std.mem.eql(u8, name.table, "outer_rows")) 1 else 2, .physical_name = name.table, .schema_version = 7, .columns = &.{.{ .name = "x", .path = "x", .type = .integer }}, .scope = .{ .database = "d", .namespace = "n", .name = name.table, .revision = 3 } };
        }
        fn capture(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 2), scans.len);
            try std.testing.expectEqualStrings("outer_rows", scans[0].table.scope.?.name);
            try std.testing.expectEqualStrings("inner_rows", scans[1].table.scope.?.name);
            return error.CaptureObserved;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint, .open_statement = capture } };
        }
    };
    var backend: Capture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT o.x FROM outer_rows o WHERE EXISTS (SELECT 1 FROM inner_rows i WHERE i.x=o.x)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.CaptureObserved, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), backend.calls);
    backend.deny_inner = true;
    try std.testing.expectError(error.AccessDenied, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), backend.calls);
}

test "SQL decorrelation unwinds every allocation and enforces shared memory admission" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT o.x,(SELECT count(*) FROM (SELECT 1 AS y UNION ALL SELECT 1) i WHERE i.y=o.x) FROM (SELECT 1 AS x UNION ALL SELECT 2) o", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT EXISTS (SELECT 1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1 }));
}

test "SQL correlated scalar parameter constraints propagate through join and result domains" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT $1::bigint FROM (SELECT 1 AS y) i WHERE i.y=o.x)+1 FROM (SELECT $2::bigint AS x) o WHERE o.x=1", .{});
    defer compiled.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .integer, .integer }, description.binding.parameter_types);
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{ .{ .integer = 7 }, .{ .integer = 1 } }, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("8", result.output.rows[0][0].string);
}

test "SQL scalar subquery nesting cannot bypass query depth admission" {
    try std.testing.expectError(error.SqlLimitExceeded, compiler.compile(std.testing.allocator, "SELECT (SELECT (SELECT (SELECT 1)))", .{ .max_depth = 2 }));
}
