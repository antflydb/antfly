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
const A = std.mem.Allocator;
const Backend = struct {
    fn resolve(_: *anyopaque, _: A, _: @import("antfly_local_sources").sql_ast.Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: A, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedBackendCall;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn backend() catalog.Backend {
        return .{ .ptr = undefined, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

const Case = struct { sql: []const u8, rows: []const []const ?i64 };
const cases: []const Case = &.{
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) p LEFT JOIN LATERAL (SELECT p.n*10 AS x WHERE p.n<>2) l ON true ORDER BY p.n", .rows = &.{ &.{ 1, 10 }, &.{ 2, null }, &.{ 3, 30 } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT p.n+10 AS x UNION ALL SELECT p.n+20 UNION ALL SELECT p.n+30 ORDER BY 1 DESC LIMIT 1 OFFSET 1) l ORDER BY p.n", .rows = &.{ &.{ 1, 21 }, &.{ 2, 22 } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p LEFT JOIN LATERAL (SELECT p.n AS x) l ON false ORDER BY p.n", .rows = &.{ &.{ 1, null }, &.{ 2, null } } },
    .{ .sql = "SELECT p.n,l.n FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT n FROM (SELECT 99 AS n) q) l ORDER BY p.n", .rows = &.{ &.{ 1, 99 }, &.{ 2, 99 } } },
    .{ .sql = "SELECT p.n,l.n FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT * FROM (SELECT 99 AS n) q) l ORDER BY p.n", .rows = &.{ &.{ 1, 99 }, &.{ 2, 99 } } },
    .{ .sql = "SELECT l.n FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT p.*) l ORDER BY l.n", .rows = &.{ &.{1}, &.{2} } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT p.n AS x) SELECT a.x+b.x AS x FROM c a CROSS JOIN c b) l ORDER BY p.n", .rows = &.{ &.{ 1, 2 }, &.{ 2, 4 } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT z.x FROM (SELECT p.n+1 AS x) q CROSS JOIN LATERAL (SELECT q.x+p.n AS x) z) l ORDER BY p.n", .rows = &.{ &.{ 1, 3 }, &.{ 2, 5 } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (SELECT count(*) AS x FROM (SELECT p.n AS x UNION ALL SELECT p.n) q) l ORDER BY p.n", .rows = &.{ &.{ 1, 2 }, &.{ 2, 2 } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p FULL JOIN LATERAL (SELECT 1 AS x) l ON p.n=l.x ORDER BY p.n", .rows = &.{ &.{ 1, 1 }, &.{ 2, null } } },
    .{ .sql = "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) SELECT sum(n) AS x FROM r) l ORDER BY p.n", .rows = &.{ &.{ 1, 3 }, &.{ 2, 5 } } },
};

fn assertRows(output: runtime.Output, expected: []const []const ?i64) !void {
    try std.testing.expectEqual(expected.len, output.rows.len);
    for (output.rows, expected, 0..) |row, values, i| {
        try std.testing.expectEqual(values.len, row.len);
        for (row, values, 0..) |value, wanted, j| {
            const sql_null = if (output.sql_nulls) |flags| flags[i][j] else value == .null;
            try std.testing.expectEqual(wanted == null, sql_null);
            if (wanted) |number| try std.testing.expectEqual(number, try std.fmt.parseInt(i64, value.string, 10));
        }
    }
}

test "SQL LATERAL applies parent scopes limits null extension materialization and recursion" {
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, Backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try assertRows(result.output, case.rows);
    }
}

test "SQL LATERAL rejects invalid sibling and right full correlation scopes before execution" {
    const Failure = struct { sql: []const u8, err: anyerror };
    for ([_]Failure{
        .{ .sql = "SELECT l.x FROM (SELECT 1 AS n) p JOIN (SELECT p.n AS x) l ON true", .err = error.UndefinedColumn },
        .{ .sql = "SELECT l.x FROM (SELECT 1 AS n) p RIGHT JOIN LATERAL (SELECT p.n AS x) l ON true", .err = error.InvalidLateralReference },
        .{ .sql = "SELECT l.x FROM (SELECT 1 AS n) p FULL JOIN LATERAL (SELECT p.n AS x) l ON true", .err = error.InvalidLateralReference },
        .{ .sql = "WITH c AS (SELECT p.n AS x) SELECT l.x FROM (SELECT 1 AS n) p CROSS JOIN LATERAL (SELECT x FROM c) l", .err = error.UndefinedColumn },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, Backend.backend(), &compiled, &.{}, .{}));
    }
}

test "SQL LATERAL parameter scopes and allocations unwind on every failure" {
    const Faults = struct {
        fn run(backing: A) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            var compiled = try compiler.compile(a, "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT p.n+$1 AS x) SELECT a.x+b.x AS x FROM c a CROSS JOIN c b) l ORDER BY p.n", .{});
            defer compiled.deinit();
            var result = try runtime.execute(a, Backend.backend(), &compiled, &.{.{ .integer = 10 }}, .{});
            defer result.deinit();
            try assertRows(result.output, &.{ &.{ 1, 22 }, &.{ 2, 24 } });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL LATERAL shares captured child scans and hash builds across parents" {
    const Source = struct {
        offset: usize = 0,
        captures: usize = 0,
        closes: usize = 0,
        checks: usize = 0,
        cancel_after: usize = std.math.maxInt(usize),
        cursors: [1]catalog.Cursor = undefined,
        fn resolve(_: *anyopaque, _: A, name: @import("antfly_local_sources").sql_ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{ .{ .name = "src", .path = "src", .type = .integer }, .{ .name = "dst", .path = "dst", .type = .integer } } };
        }
        fn next(ptr: *anyopaque, a: A, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const count = @min(@min(limit, 17), 256 - self.offset);
            const rows = try a.alloc(catalog.Row, count);
            for (rows, self.offset..) |*row, index| {
                var object: std.json.ObjectMap = .empty;
                try object.put(a, "src", .{ .integer = @intCast(index / 2 + 1) });
                try object.put(a, "dst", .{ .integer = @intCast((index / 2 + 1) * 10 + index % 2) });
                row.* = .{ .id = "child", .version = 1, .value = .{ .object = object } };
            }
            self.offset += count;
            return .{ .rows = rows, .after = if (self.offset < 256) "more" else null };
        }
        fn capture(ptr: *anyopaque, _: A, scans: []const catalog.StatementScan) !catalog.StatementRead {
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
            if (self.checks > self.cancel_after) return error.QueryCanceled;
        }
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = checkpoint, .open_statement = capture } };
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "WITH RECURSIVE p(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM p WHERE n<128) SELECT p.n,l.x FROM p LEFT JOIN LATERAL (SELECT e.dst AS x FROM edges e WHERE e.src=p.n ORDER BY e.dst DESC LIMIT 1 OFFSET 1) l ON true ORDER BY p.n", .{});
    defer compiled.deinit();
    var source: Source = .{};
    var result = try runtime.execute(std.testing.allocator, source.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 128), result.output.rows.len);
    for (result.output.rows, 1..) |row, n| {
        try std.testing.expectEqual(n, try std.fmt.parseInt(usize, row[0].string, 10));
        try std.testing.expectEqual(n * 10, try std.fmt.parseInt(usize, row[1].string, 10));
    }
    try std.testing.expectEqual(@as(usize, 256), source.offset);
    try std.testing.expectEqual(@as(usize, 1), source.captures);
    try std.testing.expectEqual(@as(usize, 1), source.closes);
    // Rebuilding the 256-row child hash for each parent exceeds this budget.
    try std.testing.expect(source.checks < 128 * 100);
    var canceled: Source = .{ .cancel_after = 100 };
    try std.testing.expectError(error.QueryCanceled, runtime.execute(std.testing.allocator, canceled.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(canceled.captures, canceled.closes);
}

test "SQL LATERAL original campaign binds every source expression" {
    const fixtures = @import("antfly_local_sources").sql_parity_fixtures;
    const Schema = struct {
        fn resolve(_: *anyopaque, _: A, name: @import("antfly_local_sources").sql_ast.Name, _: catalog.Action) !catalog.Table {
            if (!std.mem.eql(u8, name.table, "usage_records") and !std.mem.eql(u8, name.table, "balance_records")) return error.TableNotFound;
            return .{ .id = if (std.mem.eql(u8, name.table, "usage_records")) 7 else 8, .physical_name = name.table, .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "organization_id", .path = "organization_id", .type = .string },
                .{ .name = "name", .path = "name", .type = .string },
                .{ .name = "kind", .path = "kind", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
                .{ .name = "scope", .path = "scope", .type = .string },
                .{ .name = "created_at", .path = "created_at", .type = .string },
                .{ .name = "amount", .path = "amount", .type = .integer },
                .{ .name = "enabled", .path = "enabled", .type = .boolean },
                .{ .name = "metadata", .path = "metadata", .type = .json },
            } };
        }
    };
    const a = std.testing.allocator;
    var corpus = try fixtures.Corpus.init(a);
    defer corpus.deinit();
    const reference = try std.json.parseFromSlice(struct { entries: []const struct { id: []const u8 } }, a, fixtures.lateral_campaign_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 23), reference.value.entries.len);
    const backend: catalog.Backend = .{ .ptr = undefined, .vtable = &.{ .resolve = Schema.resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint } };
    for (reference.value.entries) |entry| {
        const case = try corpus.get(entry.id);
        var compiled = compiler.compile(a, case.sql, .{}) catch |err| {
            std.debug.print("LATERAL original {s}: compile {s}\n", .{ case.id, @errorName(err) });
            return err;
        };
        defer compiled.deinit();
        var description = @import("antfly_local_sources").sql_describe.describe(a, backend, &compiled, &.{}) catch |err| {
            std.debug.print("LATERAL original {s}: bind {s}\n", .{ case.id, @errorName(err) });
            return err;
        };
        defer description.deinit();
    }
    var invalid = try compiler.compile(a, (try corpus.get("sql-1357")).sql, .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UndefinedColumn, @import("antfly_local_sources").sql_describe.describe(a, backend, &invalid, &.{}));
}
