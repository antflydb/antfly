// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const Json = std.json.Value;
const Allocator = std.mem.Allocator;

const Fixture = struct {
    writes: usize = 0,
    prepares: usize = 0,
    rows: usize = 0,
    prepare_failure: ?anyerror = null,
    corrupt_identity: bool = false,
    committed_n: i64 = 0,
    deny_after_commit: ?*std.testing.FailingAllocator = null,
    fn backend(self: *Fixture, preparation: bool) catalog.Backend {
        const full: catalog.Backend.VTable = .{ .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .mutate = mutate, .mutate_prepared = mutate, .checkpoint = checkpoint, .prepare_mutations = prepare };
        const bare: catalog.Backend.VTable = .{ .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint };
        return .{ .ptr = self, .vtable = if (preparation) &full else &bare };
    }
    fn generateRowId(_: *anyopaque, alloc: Allocator) ![]const u8 {
        return alloc.dupe(u8, "generated");
    }
    fn resolve(_: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        if (action != .read_write) return error.UnexpectedAuthorization;
        return .{ .id = 1, .physical_name = "items", .schema_version = 7, .columns = &.{
            .{ .name = "n", .path = "n", .type = .integer, .nullable = false },
            .{ .name = "j", .path = "j", .type = .json },
            .{ .name = "s", .path = "s", .type = .string },
            .{ .name = "label", .path = "label", .type = .string },
            .{ .name = "g", .path = "g", .type = .integer, .generated = true },
        } };
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn scan(_: *anyopaque, alloc: Allocator, _: catalog.Table, request: catalog.Scan) !catalog.Page {
        if (request.after != null) return .{ .rows = &.{} };
        var object: std.json.ObjectMap = .empty;
        var flags: std.ArrayList(bool) = .empty;
        for (request.fields) |field| {
            const value: Json = if (std.mem.eql(u8, field, "n")) .{ .integer = 4 } else if (std.mem.eql(u8, field, "g")) .{ .integer = 8 } else if (std.mem.eql(u8, field, "label")) .{ .string = "old" } else .null;
            try object.put(alloc, field, value);
            try flags.append(alloc, value == .null and !std.mem.eql(u8, field, "j"));
        }
        const rows = try alloc.alloc(catalog.Row, 1);
        rows[0] = .{ .id = "existing", .version = 9, .value = .{ .object = object }, .sql_nulls = flags.items };
        return .{ .rows = rows };
    }
    fn prepare(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.prepares += 1;
        if (self.prepare_failure) |failure| return failure;
        const output = try alloc.dupe(catalog.Mutation, input);
        for (output) |*mutation| {
            var object: std.json.ObjectMap = .empty;
            const original = mutation.row.?.object;
            for (original.keys(), original.values()) |key, value| try object.put(alloc, key, value);
            if (!object.contains("n")) try object.put(alloc, "n", .{ .integer = 4 });
            if (!object.contains("label")) try object.put(alloc, "label", .{ .string = "native" });
            if (!object.contains("s")) try object.put(alloc, "s", .null);
            try object.put(alloc, "g", .{ .integer = object.get("n").?.integer * 2 });
            mutation.row = .{ .object = object };
            if (self.corrupt_identity) mutation.expected_version += 1;
        }
        return output;
    }
    fn mutate(ptr: *anyopaque, _: Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        self.rows = mutations.len;
        for (mutations) |mutation| if (mutation.row) |value| {
            self.committed_n = value.object.get("n").?.integer;
            try std.testing.expectEqual(self.committed_n * 2, value.object.get("g").?.integer);
            try std.testing.expect(value.object.contains("label"));
        } else {
            try std.testing.expectEqual(@as(u64, 9), mutation.expected_version);
            try std.testing.expect(mutation.previous != null);
        };
        if (self.deny_after_commit) |allocator| {
            allocator.fail_index = allocator.alloc_index;
            allocator.resize_fail_index = allocator.resize_index;
        }
        return .committed;
    }
};

test "SQL RETURNING INSERT and SELECT expose exactly prepared defaults generated values and typed nulls" {
    for ([_][]const u8{
        "INSERT INTO items (_id,n,j) VALUES ('a',4,CAST('null' AS json)) RETURNING items._id,items.n,items.g,label,j,s",
        "INSERT INTO items (_id,n,j) SELECT 'a',4,CAST('null' AS json) RETURNING _id,n,g,label,j,s",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(true), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqual(@as(usize, 1), fixture.prepares);
        try std.testing.expectEqualStrings("a", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("4", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings("8", result.output.rows[0][2].string);
        try std.testing.expectEqualStrings("native", result.output.rows[0][3].string);
        try std.testing.expectEqualSlices(bool, &.{ false, false, false, false, false, true }, result.output.sql_nulls.?[0]);
    }
}

test "SQL INSERT DEFAULT cells and DEFAULT VALUES use native row preparation" {
    for ([_][]const u8{
        "INSERT INTO items (_id,n,label) VALUES ('a',4,DEFAULT),('b',5,'set'),('c',6,NULL) RETURNING _id,n,label,g",
        "INSERT INTO items (_id,n,label) VALUES ('a',(SELECT 4),DEFAULT),('b',5,'set'),('c',6,NULL) RETURNING _id,n,label,g",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(true), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 3), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), fixture.prepares);
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqualStrings("a", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("native", result.output.rows[0][2].string);
        try std.testing.expectEqualStrings("8", result.output.rows[0][3].string);
        try std.testing.expectEqualStrings("b", result.output.rows[1][0].string);
        try std.testing.expectEqualStrings("set", result.output.rows[1][2].string);
        try std.testing.expectEqualStrings("10", result.output.rows[1][3].string);
        try std.testing.expectEqualStrings("c", result.output.rows[2][0].string);
        try std.testing.expect(result.output.rows[2][2] == .null);
        try std.testing.expect(result.output.sql_nulls.?[2][2]);
        try std.testing.expectEqualStrings("12", result.output.rows[2][3].string);
    }
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items DEFAULT VALUES RETURNING _id,n,label,g", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(true), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqualStrings("generated", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("4", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("native", result.output.rows[0][2].string);
    try std.testing.expectEqualStrings("8", result.output.rows[0][3].string);
    for ([_][]const u8{
        "INSERT INTO items (_id,n,g) VALUES (DEFAULT,4,DEFAULT) RETURNING _id,g",
        "INSERT INTO items (_id,n) VALUES (DEFAULT,4) RETURNING _id,g",
    }) |sql| {
        var generated_fixture: Fixture = .{};
        var generated_sql = try compiler.compile(std.testing.allocator, sql, .{});
        defer generated_sql.deinit();
        var generated_result = try runtime.execute(std.testing.allocator, generated_fixture.backend(true), &generated_sql, &.{}, .{});
        defer generated_result.deinit();
        try std.testing.expectEqualStrings("generated", generated_result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("8", generated_result.output.rows[0][1].string);
    }
    var rejected_fixture: Fixture = .{};
    var explicit_generated = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n,g) VALUES ('a',4,8) RETURNING g", .{});
    defer explicit_generated.deinit();
    try std.testing.expectError(error.SqlGeneratedColumnWrite, runtime.execute(std.testing.allocator, rejected_fixture.backend(true), &explicit_generated, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), rejected_fixture.writes);
    var no_identity_fixture: Fixture = .{};
    var no_identity_backend = no_identity_fixture.backend(true);
    var no_identity_vtable = no_identity_backend.vtable.*;
    no_identity_vtable.generate_row_id = null;
    no_identity_backend.vtable = &no_identity_vtable;
    try std.testing.expectError(error.SqlRowIdentityRequired, runtime.execute(std.testing.allocator, no_identity_backend, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), no_identity_fixture.writes);
}

test "SQL RETURNING UPDATE uses normalized postimage and DELETE uses versioned preimage" {
    for ([_]bool{ false, true }) |delete| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, if (delete) "DELETE FROM items RETURNING items._id,n,g,j,s" else "UPDATE items SET n=n+1 RETURNING items._id,n,g,j,s", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(!delete), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        try std.testing.expectEqual(@as(usize, if (delete) 0 else 1), fixture.prepares);
        try std.testing.expectEqualStrings("existing", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings(if (delete) "4" else "5", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings(if (delete) "8" else "10", result.output.rows[0][2].string);
        try std.testing.expectEqualSlices(bool, &.{ false, false, false, false, true }, result.output.sql_nulls.?[0]);
    }
}

test "SQL RETURNING wildcard exposes schema columns not implicit row identity" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING *", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(true), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 5), result.output.columns.len);
    try std.testing.expectEqualStrings("n", result.output.columns[0].name);
    try std.testing.expectEqualStrings("g", result.output.columns[4].name);
    try std.testing.expectEqualStrings("8", result.output.rows[0][4].string);
}

test "SQL RETURNING rejects preparation projection and quota failures before commit" {
    const cases = [_]struct { sql: []const u8, failure: anyerror, prepare_failure: ?anyerror = null, corrupt: bool = false, capability: bool = true, limits: runtime.Limits = .{} }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING 1/0", .failure = error.SqlDivisionByZero },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4),('b',4) RETURNING n", .failure = error.SqlResultTooLarge, .limits = .{ .result_rows = 1 } },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING n", .failure = error.SqlWriteConflict, .prepare_failure = error.SqlWriteConflict },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING n", .failure = error.InvalidSqlBackendResponse, .corrupt = true },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING n", .failure = error.UnsupportedSqlExecution, .capability = false },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING n", .failure = error.SqlProgramLimitExceeded, .limits = .{ .retained_bytes = 64 } },
    };
    for (cases) |case| {
        var fixture: Fixture = .{ .prepare_failure = case.prepare_failure, .corrupt_identity = case.corrupt };
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(case.failure, runtime.execute(std.testing.allocator, fixture.backend(case.capability), &compiled, &.{}, case.limits));
        try std.testing.expectEqual(@as(usize, 0), fixture.writes);
    }
}

test "SQL RETURNING commit receipt performs no fallible postcommit allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var fixture: Fixture = .{ .deny_after_commit = &failing };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('a',4) RETURNING _id,n,g,label", .{});
    defer compiled.deinit();
    var result = try runtime.execute(failing.allocator(), fixture.backend(true), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.writes);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(catalog.MutationOutcome.committed, result.output.mutation_outcome.?);
}

test "SQL RETURNING owns partial preparation and projected results under allocation faults" {
    const Check = struct {
        fn run(alloc: Allocator, compiled: *const compiler.Compiled) !void {
            var fixture: Fixture = .{};
            var result = runtime.execute(alloc, fixture.backend(true), compiled, &.{}, .{}) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), fixture.writes);
                return err;
            };
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.writes);
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n,j) VALUES ('a',4,CAST('null' AS json)) RETURNING _id,n+g,label,j,s", .{});
    defer compiled.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{&compiled});
}
