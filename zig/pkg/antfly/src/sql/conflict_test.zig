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
    guarded: bool = false,
    captures: usize = 0,
    capture_states: [8]Cursor = undefined,
    capture_cursors: [8]catalog.Cursor = undefined,
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
        return .{ .ptr = self, .predicate_only_mutations = true, .atomic_statement_read_set = self.guarded, .coordinated_point_reads = self.guarded, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepare, .checkpoint = checkpoint } };
    }
    fn resolve(_: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        try std.testing.expect(action == .read_write or action == .read);
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
    fn openStatement(ptr: *anyopaque, _: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (!self.guarded) return error.UnexpectedUnpinnedScan;
        if (scans.len > self.capture_cursors.len) return error.SqlProgramLimitExceeded;
        self.captures += 1;
        for (scans, self.capture_states[0..scans.len], self.capture_cursors[0..scans.len]) |scan_request, *state, *cursor| {
            state.* = .{ .key = scan_request.request.primary_key orelse "existing" };
            cursor.* = .{ .ptr = state, .next = Cursor.next, .close = Cursor.close };
        }
        return .{ .ptr = self, .cursors = self.capture_cursors[0..scans.len], .close = Cursor.close };
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

test "SQL conflict assignment subqueries use one guarded INSERT source capture" {
    var fixture: Fixture = .{};
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing') RETURNING n", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlRangeTrackingRequired, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.affected);
    fixture.guarded = true;
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(i64, 4), fixture.seen_n);
    try std.testing.expectEqualStrings("4", result.output.rows[0][0].string);
}

test "SQL original sql-1411 conflict scalar source captures before one guarded commit" {
    const Original = struct {
        const Self = @This();
        source_rows: usize = 1,
        distinct_source: bool = false,
        deny_read: bool = false,
        captures: usize = 0,
        point_reads: usize = 0,
        commits: usize = 0,
        assigned: i64 = 0,
        assigned_second: i64 = 0,
        assigned_status: []const u8 = "",
        generated: usize = 0,
        cursors: [4]catalog.Cursor = undefined,
        states: [4]Cursor = undefined,
        const Cursor = struct {
            owner: *Self,
            point: bool = false,
            secondary: bool = false,
            done: bool = false,
            fn next(ptr: *anyopaque, alloc: Allocator, _: u32) !catalog.Page {
                const self: *Cursor = @ptrCast(@alignCast(ptr));
                if (self.done) return .{ .rows = &.{} };
                self.done = true;
                const count: usize = if (self.point) 1 else self.owner.source_rows;
                const rows = try alloc.alloc(catalog.Row, count);
                for (rows, 0..) |*row, index| {
                    const secondary = self.secondary or (self.owner.distinct_source and !self.point and index == 1);
                    var object: std.json.ObjectMap = .empty;
                    try object.put(alloc, "id", .{ .string = if (secondary) "u2" else "u1" });
                    try object.put(alloc, "status", .{ .string = if (self.point) "old" else "ready" });
                    try object.put(alloc, "quantity", .{ .integer = if (self.point) 4 else if (secondary) 9 else 8 });
                    try object.put(alloc, "amount", .{ .integer = 5 });
                    row.* = .{ .id = if (secondary) "stored-2" else "stored-1", .version = 9, .value = .{ .object = object } };
                }
                return .{ .rows = rows };
            }
            fn close(_: *anyopaque) void {}
        };
        fn resolve(ptr: *anyopaque, _: Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_records", name.table);
            try std.testing.expect(action == .read or action == .read_write);
            if (action == .read and self.deny_read) return error.PermissionDenied;
            return .{ .id = 1, .physical_name = "usage_records", .schema_version = 1, .columns = &.{
                .{ .name = "id", .path = "id", .type = .string },
                .{ .name = "status", .path = "status", .type = .string },
                .{ .name = "quantity", .path = "quantity", .type = .integer, .nullable = false },
                .{ .name = "amount", .path = "amount", .type = .integer },
            } };
        }
        fn generate(ptr: *anyopaque, _: Allocator) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.generated += 1;
            return if (self.generated == 1) "proposed-1" else "proposed-2";
        }
        fn owners(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, columns: []const []const u8, _: []const catalog.ConflictExpression, _: []const catalog.Condition, proposed: []const catalog.Mutation) ![]const catalog.ConflictOwner {
            try std.testing.expectEqualStrings("id", columns[0]);
            const result = try alloc.alloc(catalog.ConflictOwner, proposed.len);
            for (result, proposed) |*owner, mutation| {
                const secondary = std.mem.eql(u8, mutation.row.?.object.get("id").?.string, "u2");
                owner.* = .{ .key = if (secondary) "stored-2" else "stored-1", .identity = if (secondary) "id:u2" else "id:u1", .guard = ptr };
            }
            return result;
        }
        fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedUnpinnedScan;
        }
        fn openStatement(ptr: *anyopaque, _: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(scans.len <= self.cursors.len);
            self.captures += 1;
            for (self.states[0..scans.len], self.cursors[0..scans.len]) |*state, *cursor| {
                state.* = .{ .owner = self };
                cursor.* = .{ .ptr = state, .next = Cursor.next, .close = Cursor.close };
            }
            return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = Cursor.close };
        }
        fn open(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(std.mem.eql(u8, request.primary_key.?, "stored-1") or std.mem.eql(u8, request.primary_key.?, "stored-2"));
            self.point_reads += 1;
            const state = try alloc.create(Cursor);
            state.* = .{ .owner = self, .point = true, .secondary = std.mem.eql(u8, request.primary_key.?, "stored-2") };
            return .{ .ptr = state, .next = Cursor.next, .close = Cursor.close };
        }
        fn prepare(_: *anyopaque, alloc: Allocator, _: catalog.Table, proposed: []const catalog.Mutation) ![]const catalog.Mutation {
            return alloc.dupe(catalog.Mutation, proposed);
        }
        fn mutate(ptr: *anyopaque, _: Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, if (self.distinct_source) 2 else 1), mutations.len);
            for (mutations) |mutation| {
                try std.testing.expectEqual(@as(u64, 9), mutation.expected_version);
                try std.testing.expect(mutation.conflict_guard == ptr);
                const secondary = std.mem.eql(u8, mutation.key, "stored-2");
                try std.testing.expectEqualStrings(if (secondary) "u2" else "u1", mutation.row.?.object.get("id").?.string);
                if (secondary) self.assigned_second = mutation.row.?.object.get("quantity").?.integer else self.assigned = mutation.row.?.object.get("quantity").?.integer;
                self.assigned_status = mutation.row.?.object.get("status").?.string;
            }
            self.commits += 1;
            return .committed;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .predicate_only_mutations = true, .atomic_statement_read_set = true, .coordinated_point_reads = true, .vtable = &.{ .generate_row_id = generate, .resolve_conflict_owners = owners, .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .prepare_mutations = prepare, .mutate = mutate, .mutate_prepared = mutate, .checkpoint = checkpoint } };
        }
    };
    const corpus = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @embedFile("fixtures/sql_parity_inventory.json"), .{});
    defer corpus.deinit();
    const exact_sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-1411")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    var compiled = try compiler.compile(std.testing.allocator, exact_sql, .{});
    defer compiled.deinit();
    // Capability flags alone do not authorize a collection of independently
    // refreshed pages as one statement snapshot. The native provider must
    // supply the coordinated capture entry point before any mutation work.
    var missing_capture_fixture: Original = .{};
    var missing_capture_backend = missing_capture_fixture.backend();
    var missing_capture_vtable = missing_capture_backend.vtable.*;
    missing_capture_vtable.open_statement = null;
    missing_capture_backend.vtable = &missing_capture_vtable;
    missing_capture_backend.pinned_statement_snapshot = true;
    try std.testing.expectError(error.SqlRangeTrackingRequired, runtime.execute(std.testing.allocator, missing_capture_backend, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), missing_capture_fixture.point_reads);
    try std.testing.expectEqual(@as(usize, 0), missing_capture_fixture.commits);
    var fixture: Original = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), fixture.point_reads);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(i64, 8), fixture.assigned);
    try std.testing.expectEqualStrings("old", fixture.assigned_status);
    try std.testing.expectEqualStrings("u1", result.output.rows[0][0].string);
    var selected = try compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) SELECT id,status,quantity FROM usage_records WHERE id='u1' ON CONFLICT (id) DO UPDATE SET quantity=(SELECT quantity FROM usage_records WHERE id='u1') RETURNING id", .{});
    defer selected.deinit();
    var selected_fixture: Original = .{};
    var selected_result = try runtime.execute(std.testing.allocator, selected_fixture.backend(), &selected, &.{}, .{});
    defer selected_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), selected_fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), selected_fixture.commits);
    try std.testing.expectEqual(@as(i64, 8), selected_fixture.assigned);
    const exact_select_sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
        if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-1440")) break entry.object.get("sql").?.string;
    } else return error.TestMissingCorpusCase;
    var original_select = try compiler.compile(std.testing.allocator, exact_select_sql, .{});
    defer original_select.deinit();
    var original_select_fixture: Original = .{};
    var original_select_result = try runtime.execute(std.testing.allocator, original_select_fixture.backend(), &original_select, &.{}, .{});
    defer original_select_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), original_select_fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), original_select_fixture.commits);
    try std.testing.expectEqualStrings("ready", original_select_fixture.assigned_status);
    try std.testing.expectEqualStrings("u1", original_select_result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("ready", original_select_result.output.rows[0][1].string);
    for ([_]struct { sql: []const u8, assigned: i64 }{
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=1+(SELECT quantity FROM usage_records WHERE id='u1')", .assigned = 9 },
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CAST((SELECT quantity FROM usage_records WHERE id='u1') AS INTEGER)+2", .assigned = 10 },
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=(SELECT quantity FROM usage_records WHERE id='u1')+(SELECT quantity FROM usage_records WHERE id='u1')", .assigned = 16 },
    }) |case| {
        var composed = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer composed.deinit();
        var composed_fixture: Original = .{};
        var composed_result = try runtime.execute(std.testing.allocator, composed_fixture.backend(), &composed, &.{}, .{});
        defer composed_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), composed_fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), composed_fixture.commits);
        try std.testing.expectEqual(case.assigned, composed_fixture.assigned);
    }
    var lazy_case = try compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CASE WHEN (SELECT quantity FROM usage_records WHERE id='u1') > 7 THEN 11 ELSE 0 END", .{});
    defer lazy_case.deinit();
    var lazy_fixture: Original = .{};
    var lazy_result = try runtime.execute(std.testing.allocator, lazy_fixture.backend(), &lazy_case, &.{}, .{});
    defer lazy_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), lazy_fixture.captures);
    try std.testing.expectEqual(@as(usize, 1), lazy_fixture.commits);
    try std.testing.expectEqual(@as(i64, 11), lazy_fixture.assigned);
    for ([_]struct { sql: []const u8, expected: i64 }{
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CASE WHEN (SELECT quantity FROM usage_records WHERE id='u1') > 7 THEN quantity+1 ELSE 0 END", .expected = 5 },
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=COALESCE((SELECT quantity FROM usage_records WHERE id='u1'),quantity)", .expected = 8 },
    }) |case| {
        var unconditional = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer unconditional.deinit();
        var unconditional_fixture: Original = .{};
        var unconditional_result = try runtime.execute(std.testing.allocator, unconditional_fixture.backend(), &unconditional, &.{}, .{});
        defer unconditional_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), unconditional_fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), unconditional_fixture.commits);
        try std.testing.expectEqual(case.expected, unconditional_fixture.assigned);
    }
    var cardinality_fixture: Original = .{ .source_rows = 2 };
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, cardinality_fixture.backend(), &lazy_case, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), cardinality_fixture.commits);
    try std.testing.expectError(error.UnsupportedSqlShape, compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CASE WHEN FALSE THEN (SELECT quantity FROM usage_records WHERE id='u1') ELSE quantity END", .{}));
    for ([_][]const u8{
        "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CASE WHEN FALSE AND (SELECT quantity FROM usage_records WHERE id='u1')>0 THEN 1 ELSE quantity END",
        "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=CASE WHEN 1 IN (1,(SELECT quantity FROM usage_records WHERE id='u1')) THEN 1 ELSE quantity END",
    }) |sql| {
        try std.testing.expectError(error.UnsupportedSqlShape, compiler.compile(std.testing.allocator, sql, .{}));
    }
    for ([_]struct { sql: []const u8, assigned: i64 }{
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=quantity+(SELECT quantity FROM usage_records WHERE id='u1')", .assigned = 12 },
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=excluded.quantity+(SELECT quantity FROM usage_records WHERE id='u1')", .assigned = 10 },
        .{ .sql = "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=quantity+(SELECT quantity FROM usage_records WHERE id='u1')+(SELECT quantity FROM usage_records WHERE id='u1')", .assigned = 20 },
    }) |case| {
        var mixed = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer mixed.deinit();
        var mixed_fixture: Original = .{};
        var mixed_result = try runtime.execute(std.testing.allocator, mixed_fixture.backend(), &mixed, &.{}, .{});
        defer mixed_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), mixed_fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), mixed_fixture.commits);
        try std.testing.expectEqual(case.assigned, mixed_fixture.assigned);
    }
    var typed_mixed = try compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET status=CAST((SELECT quantity FROM usage_records WHERE id='u1') AS TEXT)||status", .{});
    defer typed_mixed.deinit();
    var typed_fixture: Original = .{};
    var typed_result = try runtime.execute(std.testing.allocator, typed_fixture.backend(), &typed_mixed, &.{}, .{});
    defer typed_result.deinit();
    try std.testing.expectEqualStrings("8old", typed_fixture.assigned_status);
    var per_row = try compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) SELECT s.id,s.status,s.quantity FROM usage_records s WHERE s.status='ready' ON CONFLICT (id) DO UPDATE SET quantity=(SELECT t.quantity FROM usage_records t WHERE t.id=s.id) RETURNING id,quantity", .{});
    defer per_row.deinit();
    var per_row_fixture: Original = .{ .source_rows = 2, .distinct_source = true };
    var per_row_result = try runtime.execute(std.testing.allocator, per_row_fixture.backend(), &per_row, &.{}, .{});
    defer per_row_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), per_row_fixture.captures);
    try std.testing.expectEqual(@as(usize, 2), per_row_fixture.point_reads);
    try std.testing.expectEqual(@as(usize, 1), per_row_fixture.commits);
    try std.testing.expectEqual(@as(i64, 8), per_row_fixture.assigned);
    try std.testing.expectEqual(@as(i64, 9), per_row_fixture.assigned_second);
    try std.testing.expectEqual(@as(usize, 2), per_row_result.output.rows.len);
    try std.testing.expectError(error.UnsupportedSqlShape, compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=COALESCE(NULL,(SELECT quantity FROM usage_records WHERE id='u1'))", .{}));
    var mixed_rejection = try compiler.compile(std.testing.allocator, "INSERT INTO usage_records (id,status,quantity) VALUES ('u1','new',2) ON CONFLICT (id) DO UPDATE SET quantity=quantity+(SELECT quantity FROM usage_records WHERE id='u1')", .{});
    defer mixed_rejection.deinit();
    for ([_]usize{ 0, 2 }) |count| {
        var rejected: Original = .{ .source_rows = count };
        try std.testing.expectError(if (count == 0) error.SqlNotNullViolation else error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, rejected.backend(), &mixed_rejection, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), rejected.commits);
    }
    for ([_]usize{ 0, 2 }) |count| {
        var rejected: Original = .{ .source_rows = count };
        try std.testing.expectError(if (count == 0) error.SqlNotNullViolation else error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, rejected.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), rejected.commits);
    }
    var denied: Original = .{ .deny_read = true };
    try std.testing.expectError(error.PermissionDenied, runtime.execute(std.testing.allocator, denied.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), denied.captures);
    try std.testing.expectEqual(@as(usize, 0), denied.commits);
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

test "SQL conflict point-page admission is global across the mutation batch" {
    const sql = "INSERT INTO items (_id,n) VALUES ('existing-1',3),('existing-2',3) " ++
        "ON CONFLICT (_id) DO UPDATE SET n=excluded.n RETURNING n";
    var compiled = try compiler.compile(std.testing.allocator, sql, .{});
    defer compiled.deinit();
    var limited: Fixture = .{ .empty_pages = 2 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, limited.backend(), &compiled, &.{}, .{ .scan_pages = 4 }));
    try std.testing.expectEqual(@as(usize, 0), limited.commits);
    var admitted: Fixture = .{ .empty_pages = 2 };
    var result = try runtime.execute(std.testing.allocator, admitted.backend(), &compiled, &.{}, .{ .scan_pages = 6 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), admitted.commits);
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
    const bound = try @import("conflict.zig").bind(alloc, backend, table, compiled.statement.insert.table, compiled.statement.insert.conflict.?, &.{}, &.{});
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
