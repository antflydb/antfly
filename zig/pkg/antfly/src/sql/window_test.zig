// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const Backend = struct {
    checkpoints: usize = 0,
    cancel_after: usize = std.math.maxInt(usize),
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
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

test "SQL window input preparation releases every allocation failure" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), row_number() OVER (ORDER BY x) FROM (SELECT 1 AS x UNION ALL SELECT 2) t", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "SQL window shape infers frame value and offset parameters before execution" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT sum($1) OVER (ORDER BY 0 ROWS BETWEEN $2 PRECEDING AND CURRENT ROW)+1, lag(4,$3,$4) OVER (), ntile($5) OVER ()", .{});
    defer compiled.deinit();
    var description = try @import("describe.zig").describe(std.testing.allocator, backend.backend(), &compiled, &.{});
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
        try std.testing.expectError(error.SqlTypeMismatch, @import("describe.zig").describe(std.testing.allocator, backend.backend(), &compiled, &.{}));
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
        try std.testing.expectError(error.SqlGroupingError, @import("describe.zig").describe(std.testing.allocator, backend.backend(), &compiled, &.{}));
    }
}
