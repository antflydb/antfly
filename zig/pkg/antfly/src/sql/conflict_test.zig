// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const Allocator = std.mem.Allocator;

const Fixture = struct {
    commits: usize = 0,
    affected: usize = 0,
    fences: usize = 0,
    conflicted: bool = false,
    seen_n: i64 = 0,
    generated: usize = 0,
    identity_failure: bool = false,
    guards: usize = 0,
    page_token_bytes: usize = 0,
    empty_pages: usize = 0,
    fn owners(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, columns: []const []const u8, _: []const catalog.ConflictExpression, _: []const catalog.Condition, input: []const catalog.Mutation) ![]const catalog.ConflictOwner {
        if (columns.len != 0) try std.testing.expectEqualStrings("n", columns[0]);
        const result = try alloc.alloc(catalog.ConflictOwner, input.len);
        for (input, result) |mutation, *owner| {
            const number = mutation.row.?.object.get("n").?.integer;
            owner.* = .{ .key = if (number == 3) "existing" else null, .identity = try std.fmt.allocPrint(alloc, "native-tuple-{d}", .{number}), .guard = ptr };
            const identities = try alloc.alloc([]const u8, 1);
            identities[0] = owner.identity.?;
            owner.identities = identities;
        }
        return result;
    }
    fn backend(self: *Fixture) catalog.Backend {
        return .{ .ptr = self, .predicate_only_mutations = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepare, .checkpoint = checkpoint } };
    }
    fn resolve(_: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        try std.testing.expectEqual(catalog.Action.read_write, action);
        return .{ .id = 1, .physical_name = "items", .schema_version = 7, .columns = &.{
            .{ .name = "n", .path = "n", .type = .integer, .nullable = false },
            .{ .name = "g", .path = "g", .type = .integer, .generated = true },
        } };
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn generate(ptr: *anyopaque, alloc: Allocator) ![]const u8 {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (self.identity_failure) return error.EntropyUnavailable;
        self.generated += 1;
        return std.fmt.allocPrint(alloc, "generated-{d}", .{self.generated});
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedUnpinnedScan;
    }
    const Cursor = struct {
        key: []const u8,
        page_token_bytes: usize = 0,
        empty_pages: usize = 0,
        pages_seen: usize = 0,
        fn next(ptr: *anyopaque, alloc: Allocator, _: u32) !catalog.Page {
            const self: *Cursor = @ptrCast(@alignCast(ptr));
            if (!std.mem.startsWith(u8, self.key, "existing")) return .{ .rows = &.{} };
            const token = if (self.page_token_bytes != 0) try alloc.alloc(u8, self.page_token_bytes) else null;
            if (token) |bytes| @memset(bytes, 'x');
            if (self.pages_seen < self.empty_pages) {
                self.pages_seen += 1;
                return .{ .rows = &.{}, .after = token orelse "progress" };
            }
            var object: std.json.ObjectMap = .empty;
            try object.put(alloc, "n", .{ .integer = 4 });
            try object.put(alloc, "g", .{ .integer = 8 });
            const rows = try alloc.alloc(catalog.Row, 1);
            rows[0] = .{ .id = self.key, .version = 9, .value = .{ .object = object } };
            return .{ .rows = rows, .after = token };
        }
        fn close(_: *anyopaque) void {}
    };
    fn open(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        const cursor = try alloc.create(Cursor);
        cursor.* = .{ .key = request.primary_key orelse return error.UnexpectedFullScan, .page_token_bytes = self.page_token_bytes, .empty_pages = self.empty_pages };
        return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
    }
    fn prepare(_: *anyopaque, alloc: Allocator, _: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
        const output = try alloc.dupe(catalog.Mutation, input);
        for (output) |*mutation| {
            var object: std.json.ObjectMap = .empty;
            const original = mutation.row.?.object;
            for (original.keys(), original.values()) |key, value| try object.put(alloc, key, value);
            try object.put(alloc, "g", .{ .integer = object.get("n").?.integer * 2 });
            mutation.row = .{ .object = object };
        }
        return output;
    }
    fn mutate(ptr: *anyopaque, _: Allocator, _: catalog.Table, input: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (self.conflicted) return error.SqlWriteConflict;
        for (input) |mutation| {
            if (mutation.conflict_guard) |guard| {
                try std.testing.expect(guard == ptr);
                self.guards += 1;
            }
            const expected: u64 = if (std.mem.startsWith(u8, mutation.key, "existing")) 9 else 0;
            try std.testing.expectEqual(expected, mutation.expected_version);
            if (mutation.predicate_only) {
                self.fences += 1;
                try std.testing.expect(mutation.row == null);
            } else {
                self.affected += 1;
                self.seen_n = mutation.row.?.object.get("n").?.integer;
            }
        }
        self.commits += 1;
        return .committed;
    }
};

test "SQL targetless conflict arbitrates primary and unique keys without reserving skipped candidates" {
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.resolve_conflict_owners = Fixture.owners;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',7),('new',7),('duplicate',7),('another',3),('another',9),('another',10) ON CONFLICT DO NOTHING RETURNING _id,n", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 2), fixture.fences);
    try std.testing.expectEqual(@as(usize, 4), fixture.guards);
    try std.testing.expectEqualStrings("new", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("another", result.output.rows[1][0].string);
}

test "SQL secondary arbiter retains native owner identity and opaque atomic guards" {
    for ([_][]const u8{ "DO UPDATE SET n=items.n+excluded.n", "DO NOTHING" }) |action| {
        var fixture: Fixture = .{};
        var backend = fixture.backend();
        var vtable = backend.vtable.*;
        vtable.resolve_conflict_owners = Fixture.owners;
        backend.vtable = &vtable;
        const sql = try std.fmt.allocPrint(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('proposed',3),('new',7),('duplicate',7) ON CONFLICT (n) {s} RETURNING _id,n", .{action});
        defer std.testing.allocator.free(sql);
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        if (std.mem.startsWith(u8, action, "DO UPDATE")) {
            try std.testing.expectError(error.DuplicateSqlRow, runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{}));
            try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        } else {
            var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), fixture.guards);
            try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
            try std.testing.expectEqualStrings("new", result.output.rows[0][0].string);
        }
    }
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.resolve_conflict_owners = Fixture.owners;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('proposed',3) ON CONFLICT (n) DO UPDATE SET n=items.n+excluded.n RETURNING _id,n", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("existing", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("7", result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), fixture.guards);
}

test "SQL conflict primary arbiter compiles old and excluded shape with RETURNING" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=items.n+excluded.n+$1 WHERE excluded.g>0 RETURNING n,g", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{.{ .integer = 2 }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(i64, 9), fixture.seen_n);
    try std.testing.expectEqualStrings("9", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("18", result.output.rows[0][1].string);
}

test "SQL conflict assignment subqueries reject before preparing or committing rows" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing') RETURNING n", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.affected);
}

test "SQL conflict point-page scratch is bounded across a batch" {
    var fixture: Fixture = .{ .page_token_bytes = 64 * 1024, .empty_pages = 2 };
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(std.testing.allocator);
    try sql.appendSlice(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ");
    for (0..32) |i| {
        const value = try std.fmt.allocPrint(std.testing.allocator, "{s}('existing-{d}',3)", .{ if (i == 0) "" else ",", i });
        defer std.testing.allocator.free(value);
        try sql.appendSlice(std.testing.allocator, value);
    }
    try sql.appendSlice(std.testing.allocator, " ON CONFLICT (_id) DO UPDATE SET n=excluded.n RETURNING n");
    var compiled = try compiler.compile(std.testing.allocator, sql.items, .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{ .retained_bytes = 512 * 1024 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 32), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 32), fixture.affected);
    try std.testing.expect(result.peakMemoryBytes() <= 512 * 1024);
}

test "SQL conflict skipped rows retain atomic fences but no affected counts or RETURNING" {
    for ([_][]const u8{ "DO NOTHING", "DO UPDATE SET n=excluded.n WHERE false" }) |action| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3),('new',7) ON CONFLICT (_id) {s} RETURNING _id,n", .{action});
        defer std.testing.allocator.free(sql);
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.fences);
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        try std.testing.expectEqualStrings("new", result.output.rows[0][0].string);
    }
}

test "SQL conflict concurrent changes remain definite aborts without replay" {
    var fixture: Fixture = .{ .conflicted = true };
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO NOTHING", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlWriteConflict, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
}

test "SQL conflict DO NOTHING deduplicates same statement only after validation" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('new',3),('new',9) ON CONFLICT (_id) DO NOTHING RETURNING n", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(i64, 3), fixture.seen_n);
    try std.testing.expectEqualStrings("3", result.output.rows[0][0].string);
}

test "SQL conflict allocations cannot partially publish a statement" {
    const Faults = struct {
        fn run(alloc: Allocator) !void {
            var fixture: Fixture = .{};
            var compiled = try compiler.compile(alloc, "INSERT INTO items (_id,n) VALUES ('existing',3),('new',9) ON CONFLICT (_id) DO UPDATE SET n=items.n+excluded.n RETURNING n,g", .{});
            defer compiled.deinit();
            var result = runtime.execute(alloc, fixture.backend(), &compiled, &.{}, .{}) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), fixture.commits);
                return err;
            };
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL targetless conflict allocation failures cannot publish partial arbitration" {
    const Faults = struct {
        fn run(alloc: Allocator) !void {
            var fixture: Fixture = .{};
            var backend = fixture.backend();
            var vtable = backend.vtable.*;
            vtable.resolve_conflict_owners = Fixture.owners;
            backend.vtable = &vtable;
            var compiled = try compiler.compile(alloc, "INSERT INTO items (_id,n) VALUES ('existing',7),('new',7),('duplicate',7) ON CONFLICT DO NOTHING RETURNING n", .{});
            defer compiled.deinit();
            var result = runtime.execute(alloc, backend, &compiled, &.{}, .{}) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), fixture.commits);
                return err;
            };
            defer result.deinit();
            try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL conflict unsupported arbiters and generated assignments fail before reads" {
    for ([_][]const u8{ "ON CONFLICT (n) DO NOTHING", "ON CONFLICT (_id) DO UPDATE SET g=excluded.g", "ON CONFLICT (_id) DO UPDATE SET _id=excluded._id" }) |action| {
        const sql = try std.fmt.allocPrint(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) {s}", .{action});
        defer std.testing.allocator.free(sql);
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnsupportedSqlShape, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

test "SQL conflict binder separates partial arbiter predicates from DO UPDATE filters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.resolve_conflict_owners = Fixture.owners;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (n) VALUES (3) ON CONFLICT (n) WHERE n >= 2 DO UPDATE SET n = excluded.n WHERE items.n < 9", .{});
    defer compiled.deinit();
    const table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{.{ .name = "n", .path = "n", .type = .integer, .nullable = false }} };
    const bound = try @import("conflict.zig").bind(alloc, backend, table, compiled.statement.insert.table, compiled.statement.insert.conflict.?, &.{});
    try std.testing.expectEqual(@as(usize, 1), bound.arbiter_conditions.len);
    try std.testing.expectEqual(catalog.Condition.Op.gte, bound.arbiter_conditions[0].op);
    try std.testing.expectEqual(@as(i64, 2), bound.arbiter_conditions[0].value.integer);
    try std.testing.expect(bound.predicate != null);
}

test "SQL native generated identity VALUES SELECT and explicit identity share prepared boundary" {
    for ([_][]const u8{ "INSERT INTO items (n) VALUES (3),(9) RETURNING _id,n", "INSERT INTO items (n) SELECT 3 UNION ALL SELECT 9 RETURNING _id,n" }) |sql| {
        var fixture: Fixture = .{};
        var backend = fixture.backend();
        var vtable = backend.vtable.*;
        vtable.generate_row_id = Fixture.generate;
        backend.vtable = &vtable;
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), fixture.generated);
        try std.testing.expectEqualStrings("generated-1", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("generated-2", result.output.rows[1][0].string);
    }
    var fixture: Fixture = .{ .identity_failure = true };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.generate_row_id = Fixture.generate;
    backend.vtable = &vtable;
    var explicit = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('explicit',4) RETURNING _id", .{});
    defer explicit.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, &explicit, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("explicit", result.output.rows[0][0].string);
    var generated = try compiler.compile(std.testing.allocator, "INSERT INTO items (n) VALUES (4) RETURNING _id", .{});
    defer generated.deinit();
    try std.testing.expectError(error.EntropyUnavailable, runtime.execute(std.testing.allocator, backend, &generated, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
}
