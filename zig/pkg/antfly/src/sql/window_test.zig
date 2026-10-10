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
    checkpoints: usize = 0,
    cancel_after: usize = std.math.maxInt(usize),
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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

test "SQL execution transport types fill polymorphic holes without overriding SQL context" {
    var backend: Backend = .{};
    const parameters = [_]std.json.Value{
        .{ .string = "wrapped" },                 .{ .integer = 9007199254740993 },
        .{ .number_string = "9007199254740993" }, .{ .float = 1.5 },
        .{ .bool = true },
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT to_jsonb($1) AS value", .{});
    defer compiled.deinit();
    for (parameters) |parameter| {
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{parameter}, .{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.json, result.output.columns[0].type);
        try std.testing.expect(!result.output.sql_nulls.?[0][0]);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(arena.allocator(), parameter, .{}), try std.json.Stringify.valueAlloc(arena.allocator(), result.output.rows[0][0], .{}));
    }
    for ([_][]const u8{
        "SELECT to_jsonb($1) AS j,$1+1 AS n",
        "SELECT $1+1 AS n,to_jsonb($1) AS j",
    }, 0..) |sql, index| {
        var typed = try compiler.compile(std.testing.allocator, sql, .{});
        defer typed.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &typed, &.{.{ .string = "4" }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 4), result.output.rows[0][index].integer);
        try std.testing.expectEqualStrings("5", result.output.rows[0][1 - index].string);
    }
    // No supplied value means no invented polymorphic type during Prepare.
    try std.testing.expectError(error.UnknownSqlParameterType, @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{}));
    var prepared = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{.string});
    defer prepared.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{.string}, prepared.binding.parameter_types);
    try std.testing.expectError(error.SqlNumericOutOfRange, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .number_string = "9223372036854775808" }}, .{}));
}

test "SQL JSON numeric scalars retain logical type across relation aggregate and window output" {
    var backend: Backend = .{};
    for ([_][]const u8{
        "SELECT to_jsonb(x) AS j FROM (SELECT 2 AS x UNION ALL SELECT 1) q ORDER BY x",
        "SELECT to_jsonb(row_number() OVER (ORDER BY x)) AS j FROM (SELECT 2 AS x UNION ALL SELECT 1) q ORDER BY x",
        "SELECT to_jsonb(sum(x)) AS j FROM (SELECT 2 AS x UNION ALL SELECT 1) q",
        "SELECT * FROM (SELECT to_jsonb(x) AS j FROM (SELECT 2 AS x UNION ALL SELECT 1) q ORDER BY x) d",
    }, 0..) |sql, index| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(ast.ColumnType.json, result.output.columns[0].type);
        const expected: []const i64 = if (index == 2) &.{3} else &.{ 1, 2 };
        try std.testing.expectEqual(expected.len, result.output.rows.len);
        for (result.output.rows, expected, 0..) |row, number, ordinal| {
            try std.testing.expectEqual(@as(i64, number), row[0].integer);
            try std.testing.expect(!result.output.sql_nulls.?[ordinal][0]);
        }
    }
}

test "SQL window ORDER BY labels are standalone and expressions bind input columns" {
    var backend: Backend = .{};
    const cases = [_][]const u8{
        "SELECT x, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY (x + $1) DESC",
        "SELECT * FROM (SELECT x, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY (x + $1) DESC) q",
        "SELECT x, row_number() OVER (ORDER BY x) AS x FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY (x + $1) DESC",
    };
    for (cases) |text| {
        var compiled = try compiler.compile(std.testing.allocator, text, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        for (result.output.rows, [_][]const u8{ "3", "2", "1" }) |row, expected| {
            try std.testing.expectEqualStrings(expected, row[0].string);
            try std.testing.expectEqualStrings(expected, row[1].string);
        }
    }
    var ambiguous = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER () AS n, rank() OVER () AS n ORDER BY (n+1)", .{});
    defer ambiguous.deinit();
    try std.testing.expectError(error.UnknownColumn, runtime.execute(std.testing.allocator, backend.backend(), &ambiguous, &.{}, .{}));
    var invalid_input = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER () AS n ORDER BY sum(n) OVER ()", .{});
    defer invalid_input.deinit();
    try std.testing.expectError(error.UnknownColumn, runtime.execute(std.testing.allocator, backend.backend(), &invalid_input, &.{}, .{}));
}

test "SQL window ORDER BY rejects aliases nested in source expressions" {
    var backend: Backend = .{};
    for ([_][]const u8{
        "SELECT row_number() OVER () AS n ORDER BY n+1",
        "SELECT row_number() OVER () AS n ORDER BY CAST(n AS bigint)",
        "SELECT row_number() OVER () AS n ORDER BY coalesce(n,0)",
        "SELECT row_number() OVER () AS n ORDER BY CASE WHEN TRUE THEN n ELSE 0 END",
        "SELECT row_number() OVER () ORDER BY row_number+1",
        "SELECT row_number() OVER () AS \"n.total\" ORDER BY \"n.total\"+1",
        "SELECT x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 1 AS x) t ORDER BY n+1",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        if (runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{})) |value| {
            var result = value;
            result.deinit();
            return error.ExpectedUndefinedColumn;
        } else |err| switch (err) {
            error.UnknownColumn, error.UndefinedColumn => {},
            else => return err,
        }
    }
    // A standalone output label takes precedence over the identically named
    // input; placing that name in an expression selects the input instead.
    for ([_][]const u8{ "x", "(x+0)" }, [_][]const u8{ "-1", "-3" }) |key, first| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "SELECT -x AS x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY {s} DESC", .{key});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(first, result.output.rows[0][0].string);
    }
}

test "SQL mixed wildcard projections expand in the pinned visible input domain" {
    var backend: Backend = .{};
    for ([_][]const u8{
        "SELECT *, x+1 AS next FROM (SELECT 2 AS x UNION ALL SELECT 1) t ORDER BY x",
        "SELECT t.*, x+1 AS next FROM (SELECT 2 AS x UNION ALL SELECT 1) t ORDER BY x",
        "WITH data AS (SELECT 2 AS x UNION ALL SELECT 1) SELECT *, x+1 AS next FROM data ORDER BY x",
        "SELECT *, row_number() OVER (ORDER BY x) + 1 AS next FROM (SELECT 2 AS x UNION ALL SELECT 1) t ORDER BY x",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.output.columns.len);
        try std.testing.expectEqualStrings("x", result.output.columns[0].name);
        try std.testing.expectEqualStrings("next", result.output.columns[1].name);
        try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
        for (result.output.rows, [_][]const u8{ "1", "2" }, [_][]const u8{ "2", "3" }) |row, x, next| {
            try std.testing.expectEqualStrings(x, row[0].string);
            try std.testing.expectEqualStrings(next, row[1].string);
        }
    }
    var join = try compiler.compile(std.testing.allocator, "SELECT a.*, b.* FROM (SELECT 1 AS x) a CROSS JOIN (SELECT 2 AS y) b", .{});
    defer join.deinit();
    var joined = try runtime.execute(std.testing.allocator, backend.backend(), &join, &.{}, .{});
    defer joined.deinit();
    try std.testing.expectEqual(@as(usize, 2), joined.output.columns.len);
    try std.testing.expectEqualStrings("1", joined.output.rows[0][0].string);
    try std.testing.expectEqualStrings("2", joined.output.rows[0][1].string);
    var quoted = try compiler.compile(std.testing.allocator, "SELECT \"*\", 2 AS n FROM (SELECT 1 AS \"*\") t", .{});
    defer quoted.deinit();
    var literal = try runtime.execute(std.testing.allocator, backend.backend(), &quoted, &.{}, .{});
    defer literal.deinit();
    try std.testing.expectEqual(@as(usize, 2), literal.output.columns.len);
    var duplicate = try compiler.compile(std.testing.allocator, "SELECT t.*, row_number() OVER () AS n FROM (SELECT 1 AS x,2 AS x) t ORDER BY n", .{});
    defer duplicate.deinit();
    var repeated = try runtime.execute(std.testing.allocator, backend.backend(), &duplicate, &.{}, .{});
    defer repeated.deinit();
    try std.testing.expectEqual(@as(usize, 3), repeated.output.columns.len);
    try std.testing.expectEqualStrings("x", repeated.output.columns[0].name);
    try std.testing.expectEqualStrings("x", repeated.output.columns[1].name);
    try std.testing.expectEqualStrings("1", repeated.output.rows[0][0].string);
    try std.testing.expectEqualStrings("2", repeated.output.rows[0][1].string);
    var missing = try compiler.compile(std.testing.allocator, "SELECT missing.*, 1 FROM (SELECT 1 AS x) t", .{});
    defer missing.deinit();
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &missing, &.{}, .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compiler.compile(std.testing.allocator, "SELECT *, 1", .{}));
    try std.testing.expectError(error.InvalidSqlSyntax, compiler.compile(std.testing.allocator, "SELECT *", .{}));
    // A qualified wildcard can reference a LATERAL parent without a local
    // FROM. Its scope is resolved by binding, not rejected by the parser.
    var no_scope = try compiler.compile(std.testing.allocator, "SELECT t.*", .{});
    defer no_scope.deinit();
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &no_scope, &.{}, .{}));
}

test "SQL mixed wildcard preparation releases allocation failures" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT t.*, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1) t ORDER BY n", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL implicit window labels share the statement output order domain" {
    var backend: Backend = .{};
    for ([_][]const u8{
        "SELECT x, row_number() OVER (ORDER BY x) FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY row_number DESC",
        "SELECT * FROM (SELECT x, row_number() OVER (ORDER BY x) FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t) q ORDER BY (row_number+1) DESC",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        try std.testing.expectEqualStrings("row_number", result.output.columns[1].name);
        for (result.output.rows, [_][]const u8{ "3", "2", "1" }) |row, number| try std.testing.expectEqualStrings(number, row[1].string);
    }
    for ([_][]const u8{
        "SELECT row_number() OVER (ORDER BY 1), row_number() OVER (ORDER BY 2) ORDER BY row_number",
        "SELECT row_number() OVER () AS n, rank() OVER () AS n ORDER BY n",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.AmbiguousSqlColumn, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
    var quoted = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER (ORDER BY x) AS \"n.total\" FROM (SELECT 2 AS x UNION ALL SELECT 1) t ORDER BY \"n.total\" DESC", .{});
    defer quoted.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &quoted, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("n.total", result.output.columns[0].name);
    try std.testing.expectEqualStrings("2", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("1", result.output.rows[1][0].string);
}

test "SQL window aliases expand once across inference and virtual table lowering" {
    var backend: Backend = .{};
    const cases = [_][]const u8{
        "SELECT -x AS x, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY (x + 0) DESC",
        "SELECT * FROM (SELECT -x AS x, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY (x + 0) DESC) q",
    };
    for (cases) |text| {
        var compiled = try compiler.compile(std.testing.allocator, text, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        for (result.output.rows, [_][]const u8{ "-3", "-2", "-1" }) |row, expected| try std.testing.expectEqualStrings(expected, row[0].string);
    }
}

test "SQL window final ordering allocates for actual rows not response headroom" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY n DESC", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = 4096, .retained_bytes = 256 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expect(result.peakMemoryBytes() < 256 * 1024);
    try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .result_rows = 2 }));
}

test "SQL window alias reuse retains one computed slot without merging different windows" {
    var backend: Backend = .{};
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var compiled = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER (ORDER BY 1) AS n, row_number() OVER (ORDER BY 2) AS other ORDER BY n", .{});
    defer compiled.deinit();
    const bound = try @import("antfly_local_sources").sql_describe.bind(alloc, backend.backend(), &compiled, &.{});
    try std.testing.expectEqual(@as(usize, 2), bound.window.?.specs.len);
    try std.testing.expectEqual(@as(usize, 2), bound.window.?.sorts.len);
}

test "SQL GROUPS frames and exclusions share peer domains without row rescans" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW), sum(x) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES), count(*) OVER (ORDER BY x GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP), first_value(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW), nth_value(x,2) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 3) t ORDER BY x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][6]i64{ .{ 1, 2, 1, 0, 1, 3 }, .{ 1, 2, 1, 0, 1, 3 }, .{ 2, 4, 4, 2, 1, 1 }, .{ 3, 8, 5, 1, 1, 1 }, .{ 3, 8, 5, 1, 1, 1 } };
    try std.testing.expectEqual(expected.len, result.output.rows.len);
    for (result.output.rows, expected) |row, want| for (row, want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
}

test "SQL named windows inherit once and preserve query-local scope" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, sum(x) OVER running, rank() OVER base FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 1) t WINDOW base AS (ORDER BY x), running AS (base GROUPS UNBOUNDED PRECEDING) ORDER BY row_number() OVER base", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][3]i64{ .{ 1, 2, 1 }, .{ 1, 2, 1 }, .{ 2, 4, 3 } };
    for (result.output.rows, expected) |row, want| for (row, want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
    // Direct reference admits a frame, copying a framed window does not.
    const rejected = [_][]const u8{
        "SELECT sum(1) OVER missing",
        "SELECT sum(1) OVER (w) WINDOW w AS (ROWS CURRENT ROW)",
        "SELECT sum(1) OVER (w PARTITION BY 1) WINDOW w AS ()",
        "SELECT sum(1) OVER (w ORDER BY 2) WINDOW w AS (ORDER BY 1)",
        "SELECT 1 WINDOW w AS (), w AS ()",
        "SELECT 1 WINDOW w AS (later), later AS ()",
        "SELECT 1 WINDOW w AS (PARTITION BY sum(1) OVER w)",
        "SELECT sum(1) OVER w FROM (SELECT 1 WINDOW w AS ()) t",
    };
    for (rejected) |text| try std.testing.expectError(error.InvalidSqlSyntax, compiler.compile(std.testing.allocator, text, .{}));
}

test "SQL named windows and exclusions release allocation failures" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT sum(x) OVER (w GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES), nth_value(x,2) OVER (w ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 2) t WINDOW w AS (ORDER BY x)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL unused named windows validate without evaluating discarded expressions" {
    var backend: Backend = .{};
    var valid = try compiler.compile(std.testing.allocator, "SELECT 1 WINDOW unused AS (ORDER BY 1/0)", .{});
    defer valid.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &valid, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
    var invalid = try compiler.compile(std.testing.allocator, "SELECT 1 FROM (SELECT 1 AS x) t WINDOW unused AS (ORDER BY missing)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &invalid, &.{}, .{}));
    var quoted = try compiler.compile(std.testing.allocator, "SELECT sum(1) OVER (\"groups\") WINDOW \"groups\" AS ()", .{});
    defer quoted.deinit();
    var quoted_result = try runtime.execute(std.testing.allocator, backend.backend(), &quoted, &.{}, .{});
    defer quoted_result.deinit();
    try std.testing.expectEqualStrings("1", quoted_result.output.rows[0][0].string);
    var missing_order = try compiler.compile(std.testing.allocator, "SELECT 1 WINDOW unused AS (RANGE 1 PRECEDING)", .{});
    defer missing_order.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, backend.backend(), &missing_order, &.{}, .{}));
    var wrong_type = try compiler.compile(std.testing.allocator, "SELECT 1 WINDOW unused AS (ORDER BY 'a' RANGE 1 PRECEDING)", .{});
    defer wrong_type.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, runtime.execute(std.testing.allocator, backend.backend(), &wrong_type, &.{}, .{}));
}

test "SQL exclusions preserve empty frames and ignore peers outside frame" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, count(*) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND CURRENT ROW EXCLUDE CURRENT ROW), first_value(x) OVER (ORDER BY x GROUPS CURRENT ROW EXCLUDE GROUP), sum(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING EXCLUDE TIES) FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t ORDER BY x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    for (result.output.rows, result.output.sql_nulls.?) |row, flags| {
        try std.testing.expectEqualStrings("0", row[1].string);
        try std.testing.expect(flags[2]);
    }
    try std.testing.expect(result.output.sql_nulls.?[0][3]);
    try std.testing.expectEqualStrings("2", result.output.rows[1][3].string);
    try std.testing.expect(result.output.sql_nulls.?[2][3]);
}

test "SQL window ranking partitions and shared sort preserve final ordering" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, row_number() OVER (PARTITION BY x%2 ORDER BY x DESC) AS rn, rank() OVER (ORDER BY x) AS r, dense_rank() OVER (ORDER BY x) AS d FROM (SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 1) t ORDER BY x,rn", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][4]i64{ .{ 1, 2, 1, 1 }, .{ 1, 3, 1, 1 }, .{ 2, 1, 3, 2 }, .{ 3, 1, 4, 3 } };
    try std.testing.expectEqual(expected.len, result.output.rows.len);
    for (result.output.rows, expected) |row, want| for (row, want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
}

test "SQL window RANGE peers differ from ROWS sliding frames" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, sum(x) OVER (ORDER BY x) AS peers, sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sliding, count(*) OVER () AS total FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t ORDER BY x,sliding", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][4]i64{ .{ 1, 2, 1, 3 }, .{ 1, 2, 2, 3 }, .{ 2, 4, 3, 3 } };
    try std.testing.expectEqual(expected.len, result.output.rows.len);
    for (result.output.rows, expected) |row, want| for (row, want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
}

test "SQL windows execute after grouping HAVING and before final LIMIT" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT g, sum(x) AS s, sum(sum(x)) OVER (ORDER BY g ROWS UNBOUNDED PRECEDING) AS running FROM (SELECT 1 AS g,1 AS x UNION ALL SELECT 1,2 UNION ALL SELECT 2,3 UNION ALL SELECT 3,-1) t GROUP BY g HAVING sum(x)>0 ORDER BY g DESC LIMIT 1", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
    for (result.output.rows[0], [_]i64{ 2, 3, 6 }) |value, want| try std.testing.expectEqual(want, try std.fmt.parseInt(i64, value.string, 10));
}

test "SQL window value offsets and empty frames retain SQL NULL" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x, lag(x,1,99) OVER (ORDER BY x) AS p, lead(x) OVER (ORDER BY x) AS n, min(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS m FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY x", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    try std.testing.expectEqualStrings("99", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("1", result.output.rows[1][1].string);
    try std.testing.expect(result.output.sql_nulls.?[1][2]);
    try std.testing.expect(result.output.sql_nulls.?[1][3]);
}

test "SQL mixed wildcard expansion enforces the output budget before binding" {
    const alloc = std.testing.allocator;
    var backend: Backend = .{};
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(alloc);
    try sql.appendSlice(alloc, "SELECT ");
    for (0..1024) |index| {
        if (index != 0) try sql.append(alloc, ',');
        try sql.append(alloc, '*');
    }
    const projection_end = sql.items.len;
    try sql.appendSlice(alloc, " FROM (SELECT 1 AS x) t");
    var boundary = try compiler.compile(alloc, sql.items, .{});
    defer boundary.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(alloc, backend.backend(), &boundary, &.{});
    defer description.deinit();
    try std.testing.expectEqual(@as(usize, 1024), description.binding.columns.len);
    sql.shrinkRetainingCapacity(projection_end);
    try sql.appendSlice(alloc, ",* FROM (SELECT 1 AS x) t");
    var compiled = try compiler.compile(alloc, sql.items, .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, @import("antfly_local_sources").sql_describe.describe(alloc, backend.backend(), &compiled, &.{}));
}

test "SQL window input preparation releases every allocation failure" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), row_number() OVER (ORDER BY x) AS n FROM (SELECT 1 AS x UNION ALL SELECT 2) t ORDER BY n DESC", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL window final permutation owns references through every allocation failure" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT x,lag(x) OVER (ORDER BY x),sum(x) OVER (ORDER BY x DESC) FROM (SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) t ORDER BY x LIMIT 2", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
            try std.testing.expect(result.output.sql_nulls.?[0][1]);
            try std.testing.expectEqualStrings("6", result.output.rows[0][2].string);
            try std.testing.expectEqualStrings("2", result.output.rows[1][0].string);
            try std.testing.expectEqualStrings("1", result.output.rows[1][1].string);
            try std.testing.expectEqualStrings("5", result.output.rows[1][2].string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL window shape infers frame value and offset parameters before execution" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT sum($1) OVER (ORDER BY 0 ROWS BETWEEN $2 PRECEDING AND CURRENT ROW)+1, lag(4,$3,$4) OVER (), ntile($5) OVER ()", .{});
    defer compiled.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{});
    defer description.deinit();
    for (description.binding.parameter_types) |kind| try std.testing.expectEqual(ast.ColumnType.integer, kind.?);
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{ .{ .integer = 7 }, .{ .integer = 2 }, .{ .integer = 1 }, .{ .integer = 9 }, .{ .integer = 2 } }, .{});
    defer result.deinit();
    for (result.output.rows[0], [_]i64{ 8, 9, 1 }) |value, want| try std.testing.expectEqual(want, try std.fmt.parseInt(i64, value.string, 10));
}

test "SQL window invalid function types fail during catalog binding" {
    var backend: Backend = .{};
    for ([_][]const u8{ "SELECT sum('bad') OVER ()", "SELECT bool_and(1) OVER ()", "SELECT ntile('bad') OVER ()", "SELECT lag(1,'bad') OVER ()" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{}));
    }
}

test "SQL window preparation honors cancellation" {
    var backend: Backend = .{ .cancel_after = 0 };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT row_number() OVER () FROM (SELECT 1 UNION ALL SELECT 2) t", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
}

test "SQL window NULL and mixed numeric defaults keep coherent output types" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT lag(1,1,2.5) OVER (), lag(NULL,1,7) OVER (), lag(1,1,NULL) OVER (), sum(NULL) OVER (), bool_and(NULL) OVER ()", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(ast.ColumnType.number, result.output.columns[0].type);
    try std.testing.expectEqual(ast.ColumnType.integer, result.output.columns[1].type);
    try std.testing.expectEqualStrings("7", result.output.rows[0][1].string);
    for (result.output.sql_nulls.?[0][2..]) |is_null| try std.testing.expect(is_null);
}

test "SQL window calls cannot be evaluated in pre-window clauses" {
    var backend: Backend = .{};
    for ([_][]const u8{ "SELECT 1 WHERE row_number() OVER ()>0", "SELECT 1 GROUP BY row_number() OVER ()", "SELECT sum(1) HAVING sum(1) OVER ()>0" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlGroupingError, @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, backend.backend(), &compiled, &.{}));
    }
}

test "SQL prefix window reuse preserves weak peers and navigation tie order" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT x,y,rank() OVER (ORDER BY x),rank() OVER (ORDER BY x,y),sum(y) OVER (ORDER BY x),lag(y) OVER (ORDER BY x) FROM (SELECT 1 AS x,3 AS y UNION ALL SELECT 1,1 UNION ALL SELECT 2,2) t ORDER BY x,y", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][5]i64{ .{ 1, 1, 1, 1, 4 }, .{ 1, 3, 1, 2, 4 }, .{ 2, 2, 3, 3, 6 } };
    for (result.output.rows, expected) |row, want| for (row[0..5], want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
    // LAG still uses arrival order within the x=1 peers, not stronger y order.
    try std.testing.expectEqualStrings("3", result.output.rows[0][5].string);
    try std.testing.expect(result.output.sql_nulls.?[1][5]);
    try std.testing.expectEqualStrings("1", result.output.rows[2][5].string);
}

test "SQL compatible window prefixes share one actual permutation and preserve peer aggregates" {
    const a = std.testing.allocator;
    var backend: Backend = .{};
    var compiled = try compiler.compile(a, "SELECT x,y,rank() OVER (ORDER BY x),rank() OVER (ORDER BY x,y),sum(y) OVER (ORDER BY x) FROM (SELECT 1 AS x,3 AS y UNION ALL SELECT 1,1 UNION ALL SELECT 2,2) t ORDER BY x,y", .{});
    defer compiled.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(a, backend.backend(), &compiled, &.{});
    defer description.deinit();
    const bound = description.binding.window.?;
    try std.testing.expectEqual(@as(usize, 2), bound.sorts.len);
    const roots = try @import("antfly_local_sources").sql_ordering_reuse.plan(a, bound);
    defer a.free(roots);
    var permutations: usize = 0;
    for (roots, 0..) |root, index| permutations += @intFromBool(root == index);
    try std.testing.expectEqual(@as(usize, 1), permutations);
    var result = try runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    const expected = [_][5]i64{ .{ 1, 1, 1, 1, 4 }, .{ 1, 3, 1, 2, 4 }, .{ 2, 2, 3, 3, 6 } };
    for (result.output.rows, expected) |row, want| for (row, want) |value, number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
}
