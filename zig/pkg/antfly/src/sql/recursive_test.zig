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
const ast = @import("antfly_local_sources").sql_ast;
const compiler = @import("antfly_local_sources").sql_compiler;
const runtime = @import("antfly_local_sources").sql_runtime;
const catalog = @import("antfly_local_sources").sql_catalog;

const Fixture = struct {
    checkpoints: usize = 0,
    cancel_after: usize = std.math.maxInt(usize),
    mutations: usize = 0,
    fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .write or !std.mem.eql(u8, name.table, "items")) return error.UnexpectedBackendCall;
        return .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer, .nullable = false }} };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.mutations += 1;
        try std.testing.expectEqual(@as(usize, 3), mutations.len);
        for (mutations, 1..) |mutation, value| try std.testing.expectEqual(@as(i64, @intCast(value)), mutation.row.?.object.get("n").?.integer);
        return .committed;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
        if (self.checkpoints > self.cancel_after) return error.QueryCanceled;
    }
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

test "SQL recursive physical graph shares one capture and builds static edges once" {
    const Graph = struct {
        count: usize = 256,
        offset: usize = 0,
        rows: usize = 0,
        captures: usize = 0,
        closes: usize = 0,
        checks: usize = 0,
        cursors: [1]catalog.Cursor = undefined,
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, _: catalog.Action) !catalog.Table {
            // The recursive seed is SQL int4; these declared edge columns
            // must have that same identity, not the storage-default int8.
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{ .{ .name = "src", .path = "src", .type = .integer, .element_type = .int32 }, .{ .name = "dst", .path = "dst", .type = .integer, .element_type = .int32 } } };
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const count = @min(@min(limit, 17), self.count - self.offset);
            const rows = try alloc.alloc(catalog.Row, count);
            for (rows, 0..) |*row, i| {
                var object: std.json.ObjectMap = .empty;
                try object.put(alloc, "src", .{ .integer = @intCast(self.offset + i + 1) });
                try object.put(alloc, "dst", .{ .integer = @intCast(self.offset + i + 2) });
                row.* = .{ .id = "edge", .version = 1, .value = .{ .object = object } };
            }
            self.offset += count;
            self.rows += count;
            return .{ .rows = rows, .after = if (self.offset < self.count) "more" else null };
        }
        fn capture(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), scans.len);
            self.captures += 1;
            self.cursors[0] = .{ .ptr = self, .next = next, .close = undefined };
            return .{ .ptr = self, .cursors = &self.cursors, .close = close };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.checks += 1;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = checkpoint, .open_statement = capture } };
        }
    };
    for ([_][]const u8{ "r JOIN edges e ON r.n=e.src", "edges e JOIN r ON r.n=e.src" }) |from| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT e.dst FROM {s}) SELECT n FROM r ORDER BY n", .{from});
        defer std.testing.allocator.free(sql);
        var fixture: Graph = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .scan_rows = 4096, .result_rows = 4096 });
        defer result.deinit();
        try std.testing.expectEqual(fixture.count + 1, result.output.rows.len);
        try std.testing.expectEqual(fixture.count, fixture.rows);
        try std.testing.expectEqual(@as(usize, 1), fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.closes);
        // A re-hashed edge relation per delta exceeds this linear work budget.
        try std.testing.expect(fixture.checks < fixture.count * 100);
        for (result.output.rows, 1..) |row, n| try std.testing.expectEqual(n, try std.fmt.parseInt(usize, row[0].string, 10));
    }
}

test "SQL recursive prefix demand terminates unbounded reads and preserves blocking consumers" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .scan_rows = 5, .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 5), result.output.rows.len);
    try std.testing.expectEqual(@as(?@import("antfly_local_sources").sql_array_value.ElementType, .int32), result.output.columns[0].element_type);
    for (result.output.rows, 1..) |row, value| try std.testing.expectEqual(value, try std.fmt.parseInt(usize, row[0].string, 10));

    var sorted = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r ORDER BY n LIMIT 1", .{});
    defer sorted.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, fixture.backend(), &sorted, &.{}, .{ .scan_rows = 16 }));

    var prefix = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 10/(n-1) FROM r) SELECT n FROM r LIMIT 1", .{});
    defer prefix.deinit();
    var first = try runtime.execute(std.testing.allocator, fixture.backend(), &prefix, &.{}, .{});
    defer first.deinit();
    try std.testing.expectEqualStrings("1", first.output.rows[0][0].string);
    var failing = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 10/(n-1) FROM r) SELECT n FROM r LIMIT 2", .{});
    defer failing.deinit();
    try std.testing.expectError(error.SqlDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), &failing, &.{}, .{}));
}

test "SQL recursive prefix ownership survives paused steps scoped retirement and allocator faults" {
    const Check = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled, expected: []const []const u8) !void {
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), compiled, &.{}, .{ .page_rows = 1 });
            defer result.deinit();
            try std.testing.expectEqual(expected.len, result.output.rows.len * result.output.columns.len);
            for (result.output.columns) |column| {
                try std.testing.expectEqualStrings("n", column.name);
                try std.testing.expectEqual(ast.ColumnType.integer, column.type);
                try std.testing.expectEqual(@as(?@import("antfly_local_sources").sql_array_value.ElementType, .int32), column.element_type);
            }
            var index: usize = 0;
            for (result.output.rows) |row| for (row) |cell| {
                try std.testing.expectEqualStrings(expected[index], cell.string);
                index += 1;
            };
        }
    };
    const Case = struct { sql: []const u8, expected: []const []const u8 };
    for ([_]Case{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 5", .expected = &.{ "1", "2", "3", "4", "5" } },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT n FROM r ORDER BY n DESC LIMIT 1", .expected = &.{"5"} },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT CASE WHEN n=1 THEN 2 ELSE 1 END FROM r) SELECT n FROM r", .expected = &.{ "1", "2" } },
        .{ .sql = "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT n FROM r LIMIT 1) q", .expected = &.{ "1", "1", "10", "10" } },
        .{ .sql = "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT n FROM r LIMIT 2) q", .expected = &.{ "1", "1", "1", "2", "10", "10", "10", "11" } },
        .{ .sql = "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+2), c AS MATERIALIZED (SELECT n FROM r), nested AS MATERIALIZED (SELECT n FROM c) SELECT n FROM nested LIMIT 2) q", .expected = &.{ "1", "1", "1", "2", "10", "10", "10", "11" } },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{ &compiled, case.expected }) catch |err| {
            std.debug.print("recursive prefix ownership case failed: {s}: {t}\n", .{ case.sql, err });
            return err;
        };
    }
    var fixture: Fixture = .{};
    var scoped = try compiler.compile(std.testing.allocator, "SELECT p.n, q.n FROM (VALUES (1),(10)) p(n) CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT n FROM r LIMIT 1) q", .{});
    defer scoped.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &scoped, &.{}, .{ .page_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
    for (result.output.rows, [_][]const u8{ "1", "10" }) |row, expected| {
        try std.testing.expectEqualStrings(expected, row[0].string);
        try std.testing.expectEqualStrings(expected, row[1].string);
    }
}

test "SQL recursive binding pins seed builtin identities and coerces unknown term literals" {
    const compiler_a = std.testing.allocator;
    for ([_]struct { sql: []const u8, error_kind: anyerror }{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+2147483648 FROM r WHERE n<2) SELECT n FROM r", .error_kind = error.SqlRecursiveTypeMismatch },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1::numeric(3,0) UNION ALL SELECT n+1 FROM r WHERE n<2) SELECT n FROM r", .error_kind = error.SqlRecursiveTypeMismatch },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT ARRAY[1] UNION ALL SELECT ARRAY[2::bigint] FROM r WHERE n=ARRAY[1]) SELECT n FROM r", .error_kind = error.SqlRecursiveTypeMismatch },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 'bad' FROM r WHERE n<2) SELECT n FROM r", .error_kind = error.SqlInvalidTextRepresentation },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT '2'::text FROM r WHERE n<2) SELECT n FROM r", .error_kind = error.SqlRecursiveTypeMismatch },
    }) |case| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(compiler_a, case.sql, .{});
        defer compiled.deinit();
        var description = @import("antfly_local_sources").sql_describe.describe(compiler_a, fixture.backend(), &compiled, &.{}) catch |err| {
            try std.testing.expectEqual(case.error_kind, err);
            try std.testing.expectEqualStrings(if (err == error.SqlRecursiveTypeMismatch) "42804" else "22P02", @import("antfly_local_sources").sql_errors.describe(err).code);
            try std.testing.expectEqual(@as(usize, 0), fixture.mutations);
            continue;
        };
        description.deinit();
        return error.TestExpectedError;
    }
    const Check = struct {
        fn run(a: std.mem.Allocator, compiled: *const compiler.Compiled, width: @import("antfly_local_sources").sql_array_value.ElementType) !void {
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expectEqual(@as(?@import("antfly_local_sources").sql_array_value.ElementType, width), result.output.columns[0].element_type);
            try std.testing.expectEqualStrings("1", result.output.rows[0][0].string);
            try std.testing.expectEqualStrings("2", result.output.rows[1][0].string);
        }
    };
    for ([_]struct { sql: []const u8, width: @import("antfly_local_sources").sql_array_value.ElementType }{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT 2::integer FROM r WHERE n<2) SELECT n FROM r", .width = .int64 },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT '2' FROM r WHERE n<2) SELECT n FROM r", .width = .int32 },
    }) |case| {
        var compiled = try compiler.compile(compiler_a, case.sql, .{});
        defer compiled.deinit();
        @import("antfly_platform").allocator.checkAllAllocationFailures(compiler_a, Check.run, .{ &compiled, case.width }) catch |err| {
            std.debug.print("recursive type case failed: {s}: {t}\n", .{ case.sql, err });
            return err;
        };
    }
}

test "SQL recursive integer value overflow remains deferred after type admission" {
    var fixture: Fixture = .{};
    var first = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 2147483647 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 1", .{});
    defer first.deinit();
    var prefix = try runtime.execute(std.testing.allocator, fixture.backend(), &first, &.{}, .{});
    defer prefix.deinit();
    try std.testing.expectEqualStrings("2147483647", prefix.output.rows[0][0].string);
    var next = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 2147483647 UNION ALL SELECT n+1 FROM r) SELECT n FROM r LIMIT 2", .{});
    defer next.deinit();
    var description = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, fixture.backend(), &next, &.{});
    defer description.deinit();
    try std.testing.expectError(error.SqlNumericOutOfRange, runtime.execute(std.testing.allocator, fixture.backend(), &next, &.{}, .{}));
    try std.testing.expectEqualStrings("22003", @import("antfly_local_sources").sql_errors.describe(error.SqlNumericOutOfRange).code);
}

test "SQL recursive worklists preserve delta UNION ALL and distinct cycle semantics" {
    const cases = [_]struct { sql: []const u8, expected: []const i64 }{
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT n FROM r ORDER BY n", .expected = &.{ 1, 2, 3, 4, 5 } },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT CASE WHEN n=1 THEN 2 ELSE 1 END FROM r) SELECT n FROM r ORDER BY n", .expected = &.{ 1, 2 } },
        .{ .sql = "WITH RECURSIVE r(n) AS ((SELECT 1 UNION ALL SELECT 1) UNION SELECT n+1 FROM r WHERE n<2) SELECT n FROM r ORDER BY n", .expected = &.{ 1, 2 } },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r CROSS JOIN (SELECT 1 AS x UNION ALL SELECT 1) b WHERE r.n<3) SELECT n FROM r ORDER BY n", .expected = &.{ 1, 2, 2, 3, 3, 3, 3 } },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM (SELECT 1 AS x) b CROSS JOIN r WHERE r.n<3) SELECT n FROM r", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT a.n FROM r a JOIN r b ON a.n=b.n ORDER BY a.n", .expected = &.{ 1, 2, 3 } },
        .{ .sql = "WITH RECURSIVE seed(n) AS (SELECT 2), r(n) AS (SELECT n FROM seed UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT n FROM r", .expected = &.{ 2, 3 } },
    };
    for (cases) |case| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}) catch |err| {
            std.debug.print("recursive query failed: {s}: {s}\n", .{ case.sql, @errorName(err) });
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (case.expected, result.output.rows) |expected, row| try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, row[0].string, 10));
    }
}

test "SQL recursive typing infers parameters and distinguishes JSON null in visited sets" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT $1::bigint UNION ALL SELECT n+1 FROM r WHERE n<$2) SELECT n FROM r", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{ .{ .integer = 1 }, .{ .integer = 3 } }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    try std.testing.expectEqual(ast.ColumnType.integer, result.output.columns[0].type);
    var nulls = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS ((SELECT CAST('null' AS JSON) UNION ALL SELECT CAST(NULL AS JSON)) UNION SELECT n FROM r) SELECT n FROM r", .{});
    defer nulls.deinit();
    var null_result = try runtime.execute(std.testing.allocator, fixture.backend(), &nulls, &.{}, .{});
    defer null_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), null_result.output.rows.len);
    try std.testing.expect(!null_result.output.sql_nulls.?[0][0]);
    try std.testing.expect(null_result.output.sql_nulls.?[1][0]);
}

test "SQL recursive admission rejects nonlinear scopes and enforces type work memory cancellation limits" {
    for ([_][]const u8{
        "WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT 1) SELECT n FROM r",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT a.n FROM r a JOIN r b ON a.n=b.n) SELECT n FROM r",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT SUM(n) FROM r) SELECT n FROM r",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n FROM (SELECT 1 AS x) a LEFT JOIN r ON a.x=r.n) SELECT n FROM r",
        "WITH RECURSIVE a(n) AS (SELECT n FROM b), b(n) AS (SELECT n FROM a) SELECT n FROM a",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    }
    var fixture: Fixture = .{};
    var typed = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 'text' FROM r) SELECT n FROM r", .{});
    defer typed.deinit();
    try std.testing.expectError(error.SqlInvalidTextRepresentation, runtime.execute(std.testing.allocator, fixture.backend(), &typed, &.{}, .{}));
    var endless = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM r) SELECT n FROM r", .{});
    defer endless.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, fixture.backend(), &endless, &.{}, .{ .scan_rows = 16 }));
    try std.testing.expectError(error.SqlWorkingMemoryLimitExceeded, runtime.execute(std.testing.allocator, fixture.backend(), &endless, &.{}, .{ .retained_bytes = 1024 }));
    fixture.cancel_after = fixture.checkpoints + 32;
    try std.testing.expectError(error.QueryCanceled, runtime.execute(std.testing.allocator, fixture.backend(), &endless, &.{}, .{}));
}

test "SQL recursive INSERT SELECT validates the worklist before one atomic mutation" {
    for ([_][]const u8{
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) INSERT INTO items (_id,n) SELECT CAST(n AS TEXT),n FROM r",
        "INSERT INTO items (_id,n) WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT CAST(n AS TEXT),n FROM r",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.mutations);
    }
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r) INSERT INTO items (_id,n) SELECT CAST(n AS TEXT),n FROM r", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .scan_rows = 16 }));
    try std.testing.expectEqual(@as(usize, 0), fixture.mutations);
}

test "SQL recursive worklist and cached hash allocation failures release ownership" {
    const Case = struct {
        fn run(backing: std.mem.Allocator) !void {
            // In-place arena growth depends on backing addresses. Enumerate
            // every mandatory allocation deterministically via allocate/copy.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const alloc: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var fixture: Fixture = .{};
            var compiled = try compiler.compile(alloc, "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT r.n+1 FROM (SELECT 1 AS x UNION ALL SELECT 2) b JOIN r ON b.x=r.n WHERE r.n<3) SELECT n FROM r", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
