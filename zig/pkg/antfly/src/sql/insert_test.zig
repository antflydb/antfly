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
const catalog = @import("antfly_local_sources").sql_catalog;
const compiler = @import("antfly_local_sources").sql_compiler;
const runtime = @import("antfly_local_sources").sql_runtime;

const Fixture = struct {
    calls: usize = 0,
    rows: usize = 0,
    json_null_fields_expected: usize = 1,
    fn backend(self: *Fixture) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{
            .{ .name = "n", .path = "n", .type = .integer, .nullable = false },
            .{ .name = "j", .path = "j", .type = .json },
        } };
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedScan;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        self.rows = mutations.len;
        for (mutations) |mutation| {
            try std.testing.expectEqual(@as(u64, 0), mutation.expected_version);
            try std.testing.expectEqual(@as(i64, 9007199254740993), mutation.row.?.object.get("n").?.integer);
            if (mutation.row.?.object.get("j")) |json| {
                try std.testing.expect(json == .null);
                try std.testing.expectEqual(self.json_null_fields_expected, mutation.json_null_fields.len);
                if (mutation.json_null_fields.len != 0) try std.testing.expectEqualStrings("j", mutation.json_null_fields[0]);
            }
        }
        return .committed;
    }
};

test "SQL INSERT expressions infer batch parameters and preserve exact integers" {
    var fixture: Fixture = .{};
    // The int4 operator fixes $1 before bigint assignment coercion. An exact
    // bigint input requires a bigint cast; the target cannot silently widen it.
    var narrow = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id, n) VALUES (lower('A'), $1 + 1), ('b', $1 + 1)", .{});
    defer narrow.deinit();
    try std.testing.expectError(error.SqlNumericOutOfRange, runtime.execute(std.testing.allocator, fixture.backend(), &narrow, &.{.{ .number_string = "9007199254740992" }}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id, n) VALUES (lower('A'), $1::bigint + 1), ('b', $1::bigint + 1)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{.{ .number_string = "9007199254740992" }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    try std.testing.expectEqual(@as(usize, 2), fixture.rows);
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
}

test "SQL INSERT validates every expression before atomic mutation" {
    const cases = [_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',1),('b',1/0)", .err = error.SqlDivisionByZero },
        .{ .sql = "INSERT INTO items (_id,n) VALUES (lower('A'),1),('a',2)", .err = error.DuplicateSqlRow },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',n+1)", .err = error.UnknownColumn },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',nullif(1,1))", .err = error.SqlNotNullViolation },
    };
    for (cases) |case| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    }
}

fn expressionAllocationCase(alloc: std.mem.Allocator) !void {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(alloc, "INSERT INTO items (_id,n,j) VALUES (lower('A'),9007199254740992+1,CAST('null' AS json))", .{});
    defer compiled.deinit();
    var result = try runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
}

test "SQL INSERT expression ownership and JSON null survive allocation failures" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, expressionAllocationCase, .{});
}

test "SQL INSERT SELECT retains typed exact integers and JSON null across set sources" {
    const cases = [_][]const u8{
        "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',SUM(9007199254740993),CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',$1,CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',COALESCE($1::bigint,NULL),CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',d.x,CAST('null' AS json) FROM (SELECT $1::bigint AS x) d",
        "INSERT INTO items (_id,n,j) WITH a AS (SELECT $1::bigint AS x), b AS (SELECT x FROM a) SELECT 'a',x,CAST('null' AS json) FROM b",
        "INSERT INTO items (_id,n,j) SELECT 'a',$1::bigint,CAST('null' AS json) UNION ALL SELECT 'b',$1,CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,CAST('null' AS json) UNION ALL SELECT 'b',9007199254740993,CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) WITH q AS (SELECT 'a' AS k,9007199254740993 AS n,CAST('null' AS json) AS j) SELECT k,n,j FROM q",
    };
    for (cases) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, if (compiled.parameter_count == 0) &.{} else &.{.{ .number_string = "9007199254740993" }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(fixture.rows, result.output.rows_affected);
    }
}

test "SQL INSERT source type boundaries reject unknown text before any mutation" {
    for ([_][]const u8{
        "INSERT INTO items (_id,n,j) SELECT 'a',COALESCE($1,NULL),CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',d.x,CAST('null' AS json) FROM (SELECT $1 AS x) d",
        "INSERT INTO items (_id,n,j) SELECT 'a',$1,CAST('null' AS json) UNION ALL SELECT 'b',$1,CAST('null' AS json)",
        "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,NULL UNION ALL SELECT 'b',9007199254740993,NULL",
        "INSERT INTO items (_id,n,j) WITH q AS (SELECT 'a' AS k,9007199254740993 AS n,NULL AS j) SELECT k,n,j FROM q",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, if (compiled.parameter_count == 0) &.{} else &.{.{ .integer = 1 }}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    }
}

test "SQL INSERT SELECT validates complete source and never commits partial data" {
    const cases = [_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a',1 UNION ALL SELECT 'b',1/0", .err = error.SqlDivisionByZero },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a',1 UNION ALL SELECT 'a',2", .err = error.DuplicateSqlRow },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a',NULL", .err = error.SqlNotNullViolation },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a','123'", .err = error.SqlTypeMismatch },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a','123' WHERE FALSE", .err = error.SqlTypeMismatch },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a',1.5", .err = error.SqlTypeMismatch },
        .{ .sql = "INSERT INTO items (_id,n) SELECT 'a'", .err = error.InvalidSqlParameters },
        .{ .sql = "INSERT INTO items (_id,n,j) SELECT 'a',1,CAST(NULL AS TEXT) WHERE FALSE", .err = error.SqlTypeMismatch },
    };
    for (cases) |case| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    }
}

test "SQL INSERT SELECT empty sources do not mutate and untyped NULL takes target context" {
    var fixture: Fixture = .{ .json_null_fields_expected = 0 };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,NULL WHERE FALSE", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    var with_null = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,NULL", .{});
    defer with_null.deinit();
    var inserted = try runtime.execute(std.testing.allocator, fixture.backend(), &with_null, &.{}, .{});
    defer inserted.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    const nested = [_][]const u8{
        "INSERT INTO items (_id,n,j) SELECT 'a',9007199254740993,NULL::json UNION ALL SELECT 'b',9007199254740993,NULL",
        "INSERT INTO items (_id,n,j) WITH q AS (SELECT 'a' AS k,9007199254740993 AS n,NULL::json AS j) SELECT k,n,j FROM q",
    };
    for (nested) |sql| {
        fixture = .{ .json_null_fields_expected = 0 };
        var query = try compiler.compile(std.testing.allocator, sql, .{});
        defer query.deinit();
        var output = try runtime.execute(std.testing.allocator, fixture.backend(), &query, &.{}, .{});
        defer output.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
}

const SourceFixture = struct {
    calls: usize = 0,
    pages: usize = 0,
    source_closed: bool = false,
    fail_second: bool = false,
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = Fixture.scan, .open_scan = open, .mutate = mutate, .checkpoint = Fixture.checkpoint } };
    }
    fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        try std.testing.expectEqual(if (std.mem.eql(u8, name.table, "source")) catalog.Action.read else catalog.Action.write, action);
        const source_columns = [_]catalog.Column{ .{ .name = "n", .path = "n", .type = .integer, .nullable = false }, .{ .name = "j", .path = "j", .type = .json } };
        const target_columns = [_]catalog.Column{ .{ .name = "n", .path = "n", .type = .number, .nullable = false }, .{ .name = "j", .path = "j", .type = .json } };
        return .{ .id = if (action == .read) 2 else 1, .physical_name = name.table, .schema_version = 1, .columns = if (action == .read) &source_columns else &target_columns };
    }
    fn open(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
        try std.testing.expectEqual(@as(u64, 2), table.id);
        return .{ .ptr = ptr, .next = next, .close = close };
    }
    fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.source_closed = true;
    }
    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.pages += 1;
        if (self.pages == 2 and self.fail_second) return error.SourceReadFailed;
        if (self.pages > 2) return .{ .rows = &.{} };
        var object: std.json.ObjectMap = .empty;
        try object.put(alloc, "n", .{ .integer = 42 });
        try object.put(alloc, "j", .null);
        const rows = try alloc.alloc(catalog.Row, 1);
        rows[0] = .{ .id = if (self.pages == 1) "a" else "b", .version = 1, .value = .{ .object = object }, .sql_nulls = if (self.pages == 1) &.{ false, false } else &.{ false, true } };
        return .{ .rows = rows, .after = if (self.pages == 1) "a" else null };
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        try std.testing.expect(self.source_closed);
        try std.testing.expectEqual(@as(u64, 1), table.id);
        try std.testing.expectEqual(@as(usize, 2), mutations.len);
        for (mutations, 0..) |mutation, i| {
            try std.testing.expectEqual(@as(f64, 42), mutation.row.?.object.get("n").?.float);
            try std.testing.expect(mutation.row.?.object.get("j").? == .null);
            try std.testing.expectEqual(@as(usize, if (i == 0) 1 else 0), mutation.json_null_fields.len);
        }
        return .committed;
    }
};

fn sourceAllocationCase(alloc: std.mem.Allocator) !void {
    var fixture: SourceFixture = .{};
    var compiled = try compiler.compile(alloc, "INSERT INTO target (_id,n,j) SELECT _id,n,j FROM source", .{});
    defer compiled.deinit();
    var result = try runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
}

test "SQL INSERT SELECT pages preserve null flags widen numeric types and close before commit" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, sourceAllocationCase, .{});
}

test "SQL INSERT SELECT source failures and row quotas occur before mutation" {
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO target (_id,n,j) SELECT _id,n,j FROM source", .{});
    defer compiled.deinit();
    var fixture: SourceFixture = .{ .fail_second = true };
    try std.testing.expectError(error.SourceReadFailed, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    try std.testing.expect(fixture.source_closed);
    fixture = .{};
    try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    try std.testing.expect(fixture.source_closed);
}

test "SQL original prepared CTE INSERT captures source before one target mutation" {
    // sql-0005: execute the body of the exact original PREPARE statement.
    const alloc = std.testing.allocator;
    const corpus = try std.json.parseFromSlice(std.json.Value, alloc, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
    defer corpus.deinit();
    const original = for (corpus.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0005")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    const separator = std.mem.indexOf(u8, original, " AS ") orelse return error.TestInvalidCorpusCase;
    try std.testing.expectEqualStrings("PREPARE cte_insert_plan", original[0..separator]);
    const PreparedCteFixture = struct {
        pages: usize = 0,
        closes: usize = 0,
        generated: usize = 0,
        commits: usize = 0,
        fail_source: bool = false,

        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .generate_row_id = generate, .mutate = mutate, .checkpoint = checkpoint } };
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const source = std.mem.eql(u8, name.table, "usage_records");
            try std.testing.expectEqual(if (source) catalog.Action.read else catalog.Action.write, action);
            try std.testing.expect(source or std.mem.eql(u8, name.table, "archived_records"));
            return .{ .id = if (source) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "id", .path = "id", .type = .string }} };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedScan;
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            try std.testing.expectEqual(@as(u64, 1), table.id);
            return .{ .ptr = ptr, .next = next, .close = close };
        }
        fn next(ptr: *anyopaque, allocator: std.mem.Allocator, _: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pages += 1;
            if (self.fail_source) return error.SourceReadFailed;
            if (self.pages > 1) return .{ .rows = &.{} };
            const rows = try allocator.alloc(catalog.Row, 2);
            for (rows, 0..) |*row, i| {
                var object: std.json.ObjectMap = .empty;
                try object.put(allocator, "id", .{ .string = if (i == 0) "u1" else "u2" });
                row.* = .{ .id = if (i == 0) "source-1" else "source-2", .version = 1, .value = .{ .object = object } };
            }
            return .{ .rows = rows };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn generate(ptr: *anyopaque, allocator: std.mem.Allocator) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.generated += 1;
            return std.fmt.allocPrint(allocator, "archive-{d}", .{self.generated});
        }
        fn mutate(ptr: *anyopaque, _: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 2), table.id);
            try std.testing.expectEqual(@as(usize, 1), self.closes);
            try std.testing.expectEqual(@as(usize, 2), mutations.len);
            for (mutations, 0..) |mutation, i| {
                try std.testing.expectEqualStrings(if (i == 0) "archive-1" else "archive-2", mutation.key);
                try std.testing.expectEqualStrings(if (i == 0) "u1" else "u2", mutation.row.?.object.get("id").?.string);
            }
            self.commits += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var fixture: PreparedCteFixture = .{};
    var compiled = try compiler.compile(alloc, original[separator + " AS ".len ..], .{});
    defer compiled.deinit();
    var result = try runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("INSERT", result.output.command_tag);
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 2), fixture.generated);
    fixture = .{ .fail_source = true };
    try std.testing.expectError(error.SourceReadFailed, runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), fixture.closes);
    try std.testing.expectEqual(@as(usize, 0), fixture.generated);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
}
