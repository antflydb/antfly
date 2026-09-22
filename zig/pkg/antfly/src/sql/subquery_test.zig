// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
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

test "SQL membership unwinds allocations admits parameters and fails closed outside decorrelation" {
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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT 1 IN (SELECT x FROM (SELECT 1 AS x) i)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1 }));
    var invalid = try compiler.compile(std.testing.allocator, "SELECT o.x IN (SELECT i.x FROM (SELECT 1 AS x) i WHERE i.x > o.x) FROM (SELECT 1 AS x) o", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, backend.backend(), &invalid, &.{}, .{}));
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
        states: [3]Cursor = undefined,
        cursors: [3]catalog.Cursor = undefined,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = if (std.mem.eql(u8, name.table, "outer_rows")) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "x", .path = "x", .type = .integer }} };
        }
        fn capture(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 3), scans.len);
            self.captures += 1;
            for (&self.states, &self.cursors) |*state, *cursor| {
                state.* = .{ .owner = self };
                cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            }
            return .{ .ptr = self, .cursors = &self.cursors, .close = close };
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
    if (try std.testing.environ.contains(std.testing.allocator, "ANTFLY_SQL_MEMBERSHIP_BENCHMARK")) {
        fixture.count = 10000;
        fixture.key_count = 1000;
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM outer_rows o WHERE o.x+1 IN (SELECT i.x+1 FROM inner_rows i)", .{});
    defer compiled.deinit();
    {
        var description = try @import("describe.zig").describe(std.testing.allocator, fixture.backend(), &compiled, &.{});
        defer description.deinit();
        const join = description.binding.relation.?.root.operation.join;
        try std.testing.expectEqual(@as(usize, 1), join.left_keys.len);
        try std.testing.expectEqual(@as(usize, 1), join.right_keys.len);
    }
    const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .retained_bytes = 32 * 1024 * 1024 });
    defer result.deinit();
    const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
    try std.testing.expectEqual(fixture.count, try std.fmt.parseInt(usize, result.output.rows[0][0].string, 10));
    try std.testing.expectEqual(fixture.count * 3, fixture.rows);
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), fixture.closes);
    std.debug.print("SQL membership: outer_rows={d} inner_rows={d} native_scans=3 captures=1 peak_bytes={d} elapsed_ns={d}\n", .{ fixture.count, fixture.count, result.peakMemoryBytes(), elapsed });
    fixture.canceled = true;
    try std.testing.expectError(error.QueryCanceled, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT EXISTS (SELECT 1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1 }));
}

test "SQL correlated scalar parameter constraints propagate through join and result domains" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "SELECT (SELECT $1 FROM (SELECT 1 AS y) i WHERE i.y=o.x)+1 FROM (SELECT $2 AS x) o WHERE o.x=1", .{});
    defer compiled.deinit();
    var description = try @import("describe.zig").describe(std.testing.allocator, backend.backend(), &compiled, &.{});
    defer description.deinit();
    try std.testing.expectEqualSlices(?ast.ColumnType, &.{ .integer, .integer }, description.binding.parameter_types);
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{ .{ .integer = 7 }, .{ .integer = 1 } }, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("8", result.output.rows[0][0].string);
}

test "SQL scalar subquery nesting cannot bypass query depth admission" {
    try std.testing.expectError(error.SqlLimitExceeded, compiler.compile(std.testing.allocator, "SELECT (SELECT (SELECT (SELECT 1)))", .{ .max_depth = 2 }));
}
