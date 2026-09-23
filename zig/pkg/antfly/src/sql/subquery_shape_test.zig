// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");

const Backend = struct {
    calls: usize = 0,

    fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return error.UnexpectedBackendCall;
    }
    fn scan(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return error.UnexpectedBackendCall;
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return error.UnexpectedBackendCall;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

test "SQL subquery shapes preserve nullable quantified typed domains" {
    const cases = [_]struct { sql: []const u8, expected: ?bool }{
        .{ .sql = "SELECT NULL = ALL (SELECT 1)", .expected = null },
        .{ .sql = "SELECT NULL < ANY (SELECT 1)", .expected = null },
        .{ .sql = "SELECT NULL <> ALL (SELECT 1 WHERE false)", .expected = true },
        .{ .sql = "SELECT NULL = ANY (SELECT 1 WHERE false)", .expected = false },
        .{ .sql = "SELECT 0 IN (SELECT 0)", .expected = true },
        .{ .sql = "SELECT 3 IN (SELECT 3)", .expected = true },
        .{ .sql = "SELECT 9007199254740993 = ANY (SELECT 9007199254740993)", .expected = true },
        .{ .sql = "SELECT 9007199254740992 <> ALL (SELECT 9007199254740993)", .expected = true },
        .{ .sql = "SELECT 1 < ALL (SELECT CAST(NULL AS BIGINT))", .expected = null },
        .{ .sql = "SELECT 1 = ANY (SELECT CAST(NULL AS BIGINT))", .expected = null },
        .{ .sql = "SELECT 1 = ALL (SELECT 1.0 UNION ALL SELECT 1)", .expected = true },
        .{ .sql = "SELECT 1.5 > ANY (SELECT 1 UNION ALL SELECT 2.0)", .expected = true },
        .{ .sql = "SELECT 1.5 < ALL (SELECT 2 UNION ALL SELECT NULL)", .expected = null },
        .{ .sql = "SELECT 2.0 <> ANY (SELECT 2 UNION ALL SELECT 2.0)", .expected = false },
        .{ .sql = "SELECT 'm' < ALL (SELECT 'z' UNION ALL SELECT 'n')", .expected = true },
        .{ .sql = "SELECT 'm' = ANY (SELECT 'm' UNION ALL SELECT NULL)", .expected = true },
        .{ .sql = "SELECT true = ALL (SELECT true UNION ALL SELECT NULL)", .expected = null },
        .{ .sql = "SELECT false <> ANY (SELECT true UNION ALL SELECT NULL)", .expected = true },
        .{ .sql = "SELECT CAST('null' AS JSON) = ALL (SELECT CAST('null' AS JSON))", .expected = true },
        .{ .sql = "SELECT CAST('null' AS JSON) = ALL (SELECT CAST(NULL AS JSON))", .expected = null },
        .{ .sql = "SELECT CAST(NULL AS JSON) = ANY (SELECT CAST('null' AS JSON))", .expected = null },
    };
    var backend: Backend = .{};
    var failures: usize = 0;
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}) catch |err| {
            std.debug.print("quantified shape failed: {s}: {s}\n", .{ case.sql, @errorName(err) });
            failures += 1;
            continue;
        };
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        if (case.expected) |truth| {
            try std.testing.expectEqual(truth, result.output.rows[0][0].bool);
        } else try std.testing.expect(result.output.sql_nulls.?[0][0]);
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
    try std.testing.expectEqual(@as(usize, 0), backend.calls);
}

test "SQL subquery shapes resolve untyped standalone output to text" {
    // PostgreSQL SELECT output-column typing resolves a bare NULL to text;
    // a numeric comparison requires an explicit cast at that query boundary.
    // https://www.postgresql.org/docs/18/typeconv-select.html
    var backend: Backend = .{};
    for ([_][]const u8{ "SELECT 1 < ALL (SELECT NULL)", "SELECT 1 = ANY (SELECT NULL)" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
}

test "SQL subquery shapes reject correlation across complete value boundaries" {
    const queries = [_][]const u8{
        "SELECT (SELECT i.y FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x ORDER BY i.y LIMIT 1) FROM (SELECT 1 AS x) o",
        "SELECT (SELECT SUM(i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x GROUP BY i.k) FROM (SELECT 1 AS x) o",
        "SELECT (SELECT ROW_NUMBER() OVER (ORDER BY i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x) FROM (SELECT 1 AS x) o",
        "SELECT o.x IN (SELECT i.y FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x ORDER BY i.y LIMIT 1) FROM (SELECT 1 AS x) o",
        "SELECT o.x < ANY (SELECT SUM(i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x GROUP BY i.k) FROM (SELECT 1 AS x) o",
        "SELECT o.x = ALL (SELECT ROW_NUMBER() OVER (ORDER BY i.y) FROM (SELECT 1 AS k, 2 AS y) i WHERE i.k=o.x) FROM (SELECT 1 AS x) o",
    };
    var backend: Backend = .{};
    for (queries) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
    try std.testing.expectEqual(@as(usize, 0), backend.calls);
}
