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
const Allocator = std.mem.Allocator;

test "SQL conflict qualified old cells and INSERT aliases share dependency ordinals" {
    for ([_][]const u8{
        "INSERT INTO public.items(_id,n) VALUES('existing',5) ON CONFLICT(_id) DO UPDATE SET n=public.items.n+excluded.n RETURNING n",
        "INSERT INTO items AS i(_id,n) VALUES('existing',5) ON CONFLICT(_id) DO UPDATE SET n=i.n+excluded.n RETURNING i.n",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var described = try @import("antfly_local_sources").sql_describe.describe(std.testing.allocator, fixture.backend(), &compiled, &.{});
        defer described.deinit();
        const conflict = described.binding.conflict.?;
        try std.testing.expectEqual(@as(usize, 9), conflict.columns.len);
        try std.testing.expectEqualSlices(u32, &.{ 3, 6 }, conflict.assignments[0].?.required_columns);
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        try std.testing.expectEqualStrings("9", result.output.rows[0][0].string);
    }
    for ([_][]const u8{
        "INSERT INTO public.items AS i(_id,n) VALUES('existing',5) ON CONFLICT(_id) DO UPDATE SET n=public.items.n+excluded.n RETURNING i.n",
        "INSERT INTO public.items(_id,n) VALUES('existing',5) ON CONFLICT(_id) DO UPDATE SET n=secret.items.n+excluded.n RETURNING n",
    }) |sql| {
        var fixture: Fixture = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.UnknownColumn, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

const Fixture = struct {
    commits: usize = 0,
    affected: usize = 0,
    fences: usize = 0,
    conflicted: bool = false,
    seen_n: i64 = 0,
    generated: usize = 0,
    generated_collision: bool = false,
    identity_failure: bool = false,
    guards: usize = 0,
    page_token_bytes: usize = 0,
    empty_pages: usize = 0,
    guarded: bool = false,
    dynamic: bool = false,
    scalar_error: bool = false,
    scalar_two_rows: bool = false,
    captures: usize = 0,
    arrays: bool = false,
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
        return .{ .ptr = self, .predicate_only_mutations = true, .atomic_statement_read_set = self.guarded, .coordinated_point_reads = self.guarded, .dynamic_statement_read_set = self.dynamic, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepare, .checkpoint = checkpoint } };
    }
    fn resolve(ptr: *anyopaque, _: Allocator, _: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        try std.testing.expect(action == .read_write or action == .read);
        if (self.arrays) return .{ .id = 1, .physical_name = "items", .schema_version = 7, .columns = &.{
            .{ .name = "n", .path = "n", .type = .integer, .nullable = false },
            .{ .name = "g", .path = "g", .type = .integer, .generated = true },
            .{ .name = "a", .path = "a", .type = .array, .element_type = .int64 },
            .{ .name = "j", .path = "j", .type = .array, .element_type = .jsonb },
        } };
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
        if (self.generated_collision) return alloc.dupe(u8, "existing");
        return std.fmt.allocPrint(alloc, "generated-{d}", .{self.generated});
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedUnpinnedScan;
    }
    const Cursor = struct {
        allocator: ?Allocator = null,
        key: []const u8,
        page_token_bytes: usize = 0,
        empty_pages: usize = 0,
        pages_seen: usize = 0,
        two_rows: bool = false,
        arrays: bool = false,
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
            const rows = try alloc.alloc(catalog.Row, if (self.two_rows) 2 else 1);
            rows[0] = .{ .id = self.key, .version = 9, .value = .{ .object = object } };
            if (self.arrays) {
                const local = @import("antfly_local_sources");
                const integers = try local.sql_array_text.decodeLeaky(alloc, .int64, "[-1:1]={9007199254740993,NULL,2}", .{});
                const json = try local.sql_array_text.decodeLeaky(alloc, .jsonb, "{\"null\",NULL}", .{});
                try object.put(alloc, "a", try local.sql_array_wire.toJsonLeaky(alloc, integers.value, .{}));
                try object.put(alloc, "j", try local.sql_array_wire.toJsonLeaky(alloc, json.value, .{}));
                rows[0].value = .{ .object = object };
                var fixture: Fixture = .{ .arrays = true };
                const projection = try local.sql_document_row.Projection.init(alloc, try resolve(&fixture, alloc, .{ .table = "items" }, .read), &.{ "n", "g", "a", "j" });
                defer projection.deinit(alloc);
                rows[0] = try projection.adaptBorrowed(alloc, try projection.pageLayout(alloc), rows[0]);
            }
            if (self.two_rows) rows[1] = rows[0];
            return .{ .rows = rows, .after = token };
        }
        fn close(_: *anyopaque) void {}
        fn closeOwned(ptr: *anyopaque) void {
            const self: *Cursor = @ptrCast(@alignCast(ptr));
            self.allocator.?.destroy(self);
        }
    };
    fn open(ptr: *anyopaque, alloc: Allocator, _: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (self.dynamic) self.captures += 1;
        if (request.primary_key == null and self.scalar_error) return error.TestScalarReadFailure;
        const key = request.primary_key orelse if (self.dynamic) "existing" else return error.UnexpectedFullScan;
        const cursor = try alloc.create(Cursor);
        cursor.* = .{ .allocator = alloc, .key = key, .page_token_bytes = self.page_token_bytes, .empty_pages = self.empty_pages, .two_rows = request.primary_key == null and self.scalar_two_rows, .arrays = self.arrays };
        return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.closeOwned };
    }
    fn openStatement(ptr: *anyopaque, _: Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        if (!self.guarded) return error.UnexpectedUnpinnedScan;
        if (scans.len > self.capture_cursors.len) return error.SqlProgramLimitExceeded;
        self.captures += 1;
        for (scans, self.capture_states[0..scans.len], self.capture_cursors[0..scans.len]) |scan_request, *state, *cursor| {
            state.* = .{ .key = scan_request.request.primary_key orelse "existing", .arrays = self.arrays };
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
            if (object.get("n") == null) try object.put(alloc, "n", .{ .integer = 3 });
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
            try std.testing.expectEqual(expected == 0, mutation.unique_absence);
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

test "SQL DEFAULT VALUES conflict uses prepared defaults and generated identity" {
    var fixture: Fixture = .{ .generated_collision = true };
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.generate_row_id = Fixture.generate;
    backend.vtable = &vtable;
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items DEFAULT VALUES ON CONFLICT (_id) DO UPDATE SET n=excluded.n RETURNING _id,n,g", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.generated);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 1), fixture.affected);
    try std.testing.expectEqual(@as(i64, 3), fixture.seen_n);
    try std.testing.expectEqualStrings("existing", result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("3", result.output.rows[0][1].string);
    try std.testing.expectEqualStrings("6", result.output.rows[0][2].string);
}

test "SQL conflict assignment subqueries fail closed before owner-side masked Apply" {
    for ([_][]const u8{
        "INSERT INTO items (_id,n) VALUES ('new',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing') RETURNING n",
        "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing') WHERE FALSE RETURNING n",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(@as(usize, 1), compiled.statement.insert.conflict.?.deferred_count);
        var fixture: Fixture = .{ .guarded = true };
        try std.testing.expectError(error.SqlStatementSnapshotRequired, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
    for ([_][]const u8{
        "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN TRUE THEN (SELECT n FROM items WHERE _id='existing') ELSE 0 END",
        "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=COALESCE((SELECT n FROM items WHERE _id='existing'), n)",
        "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN EXISTS(SELECT n FROM items WHERE _id='existing') THEN 1 ELSE 0 END",
        "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET a=coalesce((SELECT a FROM items WHERE _id='existing'),excluded.a) RETURNING a,j",
    }) |sql| try std.testing.expectError(error.UnsupportedSqlShape, compiler.compile(std.testing.allocator, sql, .{}));
}

test "SQL original conflict scalar cases require a deferred owner-side read" {
    const corpus = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
    defer corpus.deinit();
    for ([_][]const u8{ "sql-1411", "sql-1440" }) |id| {
        const sql = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, id)) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(@as(usize, 1), compiled.statement.insert.conflict.?.deferred_count);
    }
}

test "SQL coordinated conflict scalar sources preserve precise arrays and native fences" {
    const local = @import("antfly_local_sources");
    for ([_]struct { sql: []const u8, whole_null: bool = false }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET a=(SELECT a FROM items WHERE _id='existing') RETURNING a,j" },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET a=(SELECT a FROM items WHERE _id='absent') RETURNING a,j", .whole_null = true },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .guarded = true, .dynamic = true, .arrays = true };
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        try std.testing.expectEqual(@as(usize, 1), fixture.affected);
        try std.testing.expectEqual(@as(i64, 4), fixture.seen_n);
        try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
        if (case.whole_null) {
            try std.testing.expect(result.output.rows[0][0] == .null);
            try std.testing.expect(result.output.sql_nulls.?[0][0]);
        } else {
            var integers = try local.sql_array_wire.decode(std.testing.allocator, .int64, result.output.rows[0][0], .{});
            defer integers.deinit();
            try std.testing.expectEqual(@as(i32, -1), integers.value.dimensions[0].lower);
            try std.testing.expectEqual(@as(i64, 9007199254740993), integers.value.elements[0].value.integer);
            try std.testing.expect(integers.value.elements[1].sql_null);
        }
        var json = try local.sql_array_wire.decode(std.testing.allocator, .jsonb, result.output.rows[0][1], .{});
        defer json.deinit();
        try std.testing.expect(!json.value.elements[0].sql_null);
        try std.testing.expect(json.value.elements[1].sql_null);
    }
}

test "SQL direct conflict scalar Apply reads only owner-selected updates" {
    const cases = [_]struct { sql: []const u8, captures: usize, affected: usize, fences: usize, value: i64 }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing')", .captures = 2, .affected = 1, .fences = 0, .value = 4 },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('new',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing')", .captures = 1, .affected = 1, .fences = 0, .value = 3 },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items WHERE _id='existing') WHERE FALSE", .captures = 1, .affected = 0, .fences = 1, .value = 0 },
    };
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .guarded = true, .dynamic = true };
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.captures, fixture.captures);
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        try std.testing.expectEqual(case.affected, fixture.affected);
        try std.testing.expectEqual(case.fences, fixture.fences);
        try std.testing.expectEqual(case.value, fixture.seen_n);
    }
}

test "SQL deferred scalar errors and cardinality occur only on taken conflict updates" {
    const cases = [_]struct { sql: []const u8, demanded: bool }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items)", .demanded = true },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('new',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items)", .demanded = false },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items) WHERE FALSE", .demanded = false },
    };
    for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .guarded = true, .dynamic = true, .scalar_error = true };
        if (case.demanded) {
            try std.testing.expectError(error.TestScalarReadFailure, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
            try std.testing.expectEqual(@as(usize, 0), fixture.commits);
        } else {
            var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
            result.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        }
    }
    var compiled = try compiler.compile(std.testing.allocator, cases[0].sql, .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .guarded = true, .dynamic = true, .scalar_two_rows = true };
    try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
}

test "SQL deferred scalar has no outer-row binding scope" {
    var compiled = try compiler.compile(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT excluded.n FROM items)", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .guarded = true, .dynamic = true };
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
}

test "SQL uncorrelated deferred scalar is computed once for multiple conflict owners" {
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(std.testing.allocator);
    try sql.appendSlice(std.testing.allocator, "INSERT INTO items (_id,n) VALUES ");
    for (0..128) |index| {
        const value = try std.fmt.allocPrint(std.testing.allocator, "{s}('existing-{d}',3)", .{ if (index == 0) "" else ",", index });
        defer std.testing.allocator.free(value);
        try sql.appendSlice(std.testing.allocator, value);
    }
    try sql.appendSlice(std.testing.allocator, " ON CONFLICT (_id) DO UPDATE SET n=(SELECT n FROM items)");
    var compiled = try compiler.compile(std.testing.allocator, sql.items, .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .guarded = true, .dynamic = true };
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 129), fixture.captures); // 128 owners, one scalar read
    try std.testing.expectEqual(@as(usize, 128), fixture.affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
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
            var compiled = try compiler.compile(alloc, "INSERT INTO public.items (_id,n) VALUES ('existing',3),('new',9) ON CONFLICT (_id) DO UPDATE SET n=public.items.n+excluded.n RETURNING public.items.n,public.items.g", .{});
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
    const bound = try @import("antfly_local_sources").sql_conflict.bind(alloc, backend, table, compiled.statement.insert.table, false, compiled.statement.insert.conflict.?, &.{}, &.{});
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

test "SQL decisions in conflict predicates assignments and returning retain atomic fences" {
    const Provider = @import("antfly_local_sources").sql_decision_eval.testing.Provider;
    const cases = [_]struct { sql: []const u8, calls: usize, affected: usize }{
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN ai_probability(CAST(excluded.n AS TEXT),'Refund?','local')>0.8 THEN items.n+excluded.n ELSE 0 END WHERE ai_probability(CAST(items.n AS TEXT),'Refund?','local')>0.8 RETURNING ai_probability(CAST(n AS TEXT),'Refund?','local')", .calls = 3, .affected = 1 },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN ai_probability('unused','Refund?','local')>0.8 THEN 7 ELSE 0 END WHERE FALSE", .calls = 0, .affected = 0 },
        .{ .sql = "INSERT INTO items (_id,n) VALUES ('new',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN ai_probability('unused','Refund?','local')>0.8 THEN 7 ELSE 0 END", .calls = 0, .affected = 1 },
    };
    for (cases) |case| {
        var fixture: Fixture = .{};
        var provider: Provider = .{};
        var backend = fixture.backend();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.calls, provider.calls);
        try std.testing.expectEqual(case.affected, fixture.affected);
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        if (case.calls != 0) {
            try std.testing.expectEqual(@as(i64, 7), fixture.seen_n);
            try std.testing.expectApproxEqAbs(@as(f64, 0.9), result.output.rows[0][0].float, 0.001);
        }
    }
    var fixture: Fixture = .{};
    var provider: Provider = .{ .fail = true };
    var backend = fixture.backend();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(std.testing.allocator, cases[0].sql, .{});
    defer compiled.deinit();
    try std.testing.expectError(error.DecisionProviderUnavailable, runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectError(error.DecisionProviderUnavailable, runtime.execute(std.testing.allocator, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
}

test "SQL conflict decisions batch fenced owner rows across bounded pages" {
    const a = std.testing.allocator;
    var sql: std.ArrayList(u8) = .empty;
    defer sql.deinit(a);
    try sql.appendSlice(a, "INSERT INTO items (_id,n) VALUES ");
    for (0..259) |index| {
        const value = try std.fmt.allocPrint(a, "{s}('existing-{d}',3)", .{ if (index == 0) "" else ",", index });
        defer a.free(value);
        try sql.appendSlice(a, value);
    }
    try sql.appendSlice(a, " ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN ai_probability(CAST(excluded.n AS TEXT),'Refund?','local')>0.8 THEN items.n+excluded.n ELSE 0 END WHERE ai_probability(CAST(items.n AS TEXT),'Refund?','local')>0.8");
    var fixture: Fixture = .{};
    var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
    var backend = fixture.backend();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(a, sql.items, .{});
    defer compiled.deinit();
    var result = try runtime.execute(a, backend, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 259), fixture.affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 518), provider.calls);
    try std.testing.expectEqual(@as(usize, 256), provider.max_batch);
    fixture = .{};
    provider = .{ .fail_after = 256 };
    // A later decision page fails after the first page has been prepared.
    try std.testing.expectError(error.DecisionProviderUnavailable, runtime.execute(a, backend, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 256), provider.calls);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.affected);
}

test "SQL conflict decisions respect configured row and byte pages" {
    const a = std.testing.allocator;
    for ([_]runtime.Limits{ .{ .page_rows = 1 }, .{ .page_bytes = 1 }, .{ .page_rows = 2 } }) |limits| {
        var fixture: Fixture = .{};
        var provider: @import("antfly_local_sources").sql_decision_eval.testing.Provider = .{};
        var backend = fixture.backend();
        backend.decision_provider = provider.provider();
        var compiled = try compiler.compile(a, "INSERT INTO items (_id,n) VALUES ('existing-0',3),('existing-1',3),('existing-2',3) ON CONFLICT (_id) DO UPDATE SET n=CASE WHEN ai_probability(CAST(excluded.n AS TEXT),'Refund?','local')>0.8 THEN items.n+excluded.n ELSE 0 END WHERE ai_probability(CAST(items.n AS TEXT),'Refund?','local')>0.8 RETURNING ai_probability(CAST(n AS TEXT),'Refund?','local')", .{});
        defer compiled.deinit();
        var result = try runtime.execute(a, backend, &compiled, &.{}, limits);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 9), provider.calls);
        try std.testing.expectEqual(@as(usize, if (limits.page_bytes == 1) 1 else limits.page_rows), provider.max_batch);
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
        fixture = .{};
        provider = .{ .fail_after = 2 };
        try std.testing.expectError(error.DecisionProviderUnavailable, runtime.execute(a, backend, &compiled, &.{}, limits));
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

test "SQL EXPLAIN exposes deferred conflict decision queries without owner reads" {
    const Provider = @import("antfly_local_sources").sql_decision_eval.testing.Provider;
    var fixture: Fixture = .{ .guarded = true, .dynamic = true };
    var provider: Provider = .{};
    var backend = fixture.backend();
    backend.decision_provider = provider.provider();
    var compiled = try compiler.compile(std.testing.allocator, "EXPLAIN (FORMAT JSON) INSERT INTO items (_id,n) VALUES ('existing',3) ON CONFLICT (_id) DO UPDATE SET n=(SELECT CAST(ai_probability(CAST(n AS TEXT),'Refund?','local') AS BIGINT) FROM items WHERE _id='existing')", .{});
    defer compiled.deinit();
    var explained = try runtime.execute(std.testing.allocator, backend, &compiled, &.{}, .{});
    defer explained.deinit();
    const plan = explained.output.rows[0][0].string;
    try std.testing.expect(std.mem.indexOf(u8, plan, "Conflict Scalar Subquery") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan, "DecisionEval") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan, "ai_probability") != null);
    try std.testing.expectEqual(@as(usize, 0), fixture.captures);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), provider.calls);
}
