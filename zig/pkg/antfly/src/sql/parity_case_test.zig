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
const sources = @import("antfly_local_sources");
const compiler = sources.sql_compiler;
const fixtures = sources.sql_parity_fixtures;

test "SQL physical column qualification validates pinned namespace and alias boundaries" {
    const Backend = struct {
        fn generateRowId(_: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
            return alloc.dupe(u8, "generated");
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, name: sources.sql_ast.Name, _: sources.sql_catalog.Action) !sources.sql_catalog.Table {
            if (!std.mem.eql(u8, name.table, "items")) return error.TableNotFound;
            if (name.namespace) |namespace| if (!std.mem.eql(u8, namespace, "public")) return error.TableNotFound;
            return .{ .id = 1, .physical_name = "physical-items", .schema_version = 1, .scope = .{ .database = "app", .namespace = "public", .name = "items", .revision = 9 }, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: sources.sql_catalog.Table, _: sources.sql_catalog.Scan) !sources.sql_catalog.Page {
            return error.UnexpectedBackendCall;
        }
        fn mutate(_: *anyopaque, _: std.mem.Allocator, _: sources.sql_catalog.Table, _: []const sources.sql_catalog.Mutation) !sources.sql_catalog.MutationOutcome {
            return error.UnexpectedBackendCall;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    var sentinel: u8 = 0;
    const backend: sources.sql_catalog.Backend = .{ .ptr = &sentinel, .vtable = &.{ .generate_row_id = Backend.generateRowId, .resolve = Backend.resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint } };
    for ([_][]const u8{
        "SELECT public.items.n FROM items",
        "SELECT app.public.items.n FROM public.items",
        "SELECT public.items.*, 1 AS extra FROM items",
        "SELECT app.public.items.* FROM items",
        "SELECT a.n FROM public.items AS a",
        "SELECT items.n FROM public.items AS items",
        "INSERT INTO public.items(n) VALUES(1) RETURNING public.items.n",
        "INSERT INTO public.items AS i(n) VALUES(1) RETURNING i.*",
        "UPDATE public.items SET n=2 RETURNING public.items.*",
        "UPDATE public.items SET n=public.items.n+1 WHERE public.items.n=2 RETURNING public.items.n",
        "DELETE FROM public.items WHERE public.items.n=2 RETURNING public.items.n",
        "DELETE FROM public.items RETURNING app.public.items.n",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        _ = try sources.sql_describe.bind(arena.allocator(), backend, &compiled, &.{});
    }
    for ([_][]const u8{
        "SELECT secret.items.n FROM items",
        "SELECT other.public.items.n FROM items",
        "SELECT app.secret.items.* FROM items",
        "SELECT public.items.n FROM public.items AS a",
        "SELECT public.a.* FROM public.items AS a",
        "SELECT public.items.n FROM public.items AS items",
        "SELECT public.items.n FROM (SELECT n FROM items) AS items",
        "INSERT INTO public.items AS i(n) VALUES(1) RETURNING public.items.n",
        "UPDATE public.items AS i SET n=2 RETURNING public.items.*",
        "DELETE FROM public.items AS i RETURNING app.public.items.n",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        try std.testing.expectError(error.UndefinedColumn, sources.sql_describe.bind(arena.allocator(), backend, &compiled, &.{}));
    }
    for ([_][]const u8{ "SELECT a.b.c.d.e FROM items", "SELECT a.b.c.d.* FROM items" }) |sql|
        try std.testing.expectError(error.InvalidSqlSyntax, compiler.compile(std.testing.allocator, sql, .{}));
    for ([_][]const u8{
        "UPDATE items SET n=n+1 WHERE n=2",
        "UPDATE public.items SET n=public.items.n+1 WHERE public.items.n=2",
    }, 0..) |sql, index| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var described = try sources.sql_describe.describe(std.testing.allocator, backend, &compiled, &.{});
        defer described.deinit();
        try std.testing.expectEqual(@as(usize, 2), described.binding.scalars.columns.len);
        try std.testing.expectEqual(@as(usize, if (index == 0) 0 else 3), described.binding.scalars.columns[0].aliases.len);
        try std.testing.expectEqualSlices(u32, &.{0}, described.binding.scalars.required);
    }
}

test "SQL mutation campaign discovery reports exact compiler admission without parity credit" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const manifest = try std.json.parseFromSlice(struct { entries: []const struct { id: []const u8, profile: []const u8 } }, std.testing.allocator, fixtures.mutation_campaign, .{ .ignore_unknown_fields = true });
    defer manifest.deinit();
    try std.testing.expectEqual(@as(usize, 235), manifest.value.entries.len);
    const verbose = try std.testing.environ.containsUnempty(std.testing.allocator, "ANTFLY_SQL_MUTATION_DISCOVERY");
    var admitted: usize = 0;
    for (manifest.value.entries) |entry| {
        const case = try corpus.get(entry.id);
        var diagnostic: compiler.Diagnostic = .{};
        var compiled = compiler.compileDiagnostic(std.testing.allocator, case.sql, .{}, &diagnostic) catch |err| {
            if (verbose) std.debug.print("MUTATION DISCOVERY {s} {s}: {s} at {d}: {s}\n", .{ case.id, entry.profile, @errorName(err), diagnostic.start, diagnostic.message });
            continue;
        };
        defer compiled.deinit();
        admitted += 1;
    }
    std.debug.print("MUTATION DISCOVERY compiler admitted {d}/235; execution and storage outcomes are separate gates\n", .{admitted});
}

test "SQL catalog campaign discovery separates compiler admission from owner activation" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const verbose = try std.testing.environ.containsUnempty(std.testing.allocator, "ANTFLY_SQL_CATALOG_DISCOVERY");
    var reviewed: usize = 0;
    var admitted: usize = 0;
    var commands: std.StringHashMapUnmanaged(usize) = .empty;
    defer commands.deinit(std.testing.allocator);
    for (corpus.parsed.value.entries) |case| {
        if (!std.mem.eql(u8, case.family, "ddl") and !std.mem.eql(u8, case.family, "unsupported_ddl")) continue;
        reviewed += 1;
        var diagnostic: compiler.Diagnostic = .{};
        var compiled = compiler.compileDiagnostic(std.testing.allocator, case.sql, .{}, &diagnostic) catch |err| {
            if (verbose) std.debug.print("CATALOG DISCOVERY {s} {s}: {s} at {d}: {s}\n", .{ case.id, case.family, @errorName(err), diagnostic.start, diagnostic.message });
            continue;
        };
        defer compiled.deinit();
        admitted += 1;
        const kind = @tagName(compiled.statement);
        const count = try commands.getOrPut(std.testing.allocator, kind);
        if (!count.found_existing) count.value_ptr.* = 0;
        count.value_ptr.* += 1;
        if (verbose) std.debug.print("CATALOG DISCOVERY {s} admitted {s}; native catalog activation not certified\n", .{ case.id, kind });
    }
    // Fixed original source cohort, including deliberate negative contracts.
    // No compiler outcome changes a source classification or disposition.
    try std.testing.expectEqual(@as(usize, 479), reviewed);
    var totals = commands.iterator();
    while (totals.next()) |count| std.debug.print("CATALOG DISCOVERY admitted shape={s} cases={}\n", .{ count.key_ptr.*, count.value_ptr.* });
    std.debug.print("CATALOG DISCOVERY compiler admitted {}/479; authorization, durable publication and storage semantics remain separate gates\n", .{admitted});
}

test "SQL catalog default admission matches PostgreSQL subquery prohibition" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    for (1109..1141) |ordinal| {
        var buffer: [8]u8 = undefined;
        const id = try std.fmt.bufPrint(&buffer, "sql-{d:0>4}", .{ordinal});
        const case = try corpus.get(id);
        var diagnostic: compiler.Diagnostic = .{};
        try std.testing.expectError(error.UnsupportedSqlShape, compiler.compileDiagnostic(std.testing.allocator, case.sql, .{}, &diagnostic));
        try std.testing.expectEqualStrings("subqueries are not allowed in schema defaults", diagnostic.message);
        try std.testing.expectEqualStrings("0A000", sources.sql_errors.describe(error.UnsupportedSqlShape).code);
    }
    for ([_][]const u8{
        "CREATE TABLE items (n bigint DEFAULT $1)",
        "ALTER TABLE items ADD COLUMN n bigint DEFAULT $1",
        "ALTER TABLE items ALTER COLUMN n SET DEFAULT $1",
    }) |sql| {
        var diagnostic: compiler.Diagnostic = .{};
        try std.testing.expectError(error.UnsupportedSqlShape, compiler.compileDiagnostic(std.testing.allocator, sql, .{}, &diagnostic));
        try std.testing.expectEqualStrings("schema defaults cannot contain execution parameters", diagnostic.message);
    }
}

test "SQL original catalog request truncations never access missing tokens" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    var prefixes: usize = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (corpus.parsed.value.entries) |case| {
        if (!std.mem.eql(u8, case.family, "ddl") and !std.mem.eql(u8, case.family, "unsupported_ddl")) continue;
        for (0..case.sql.len + 1) |end| {
            // Include token boundaries and punctuation cuts, including cuts
            // inside quoted bodies. EOF is never a valid token to dereference.
            if (end != case.sql.len and !std.ascii.isWhitespace(case.sql[end]) and std.mem.indexOfScalar(u8, "(),;", case.sql[end]) == null) continue;
            prefixes += 1;
            var diagnostic: compiler.Diagnostic = .{};
            var compiled = compiler.compileDiagnostic(std.testing.allocator, case.sql[0..end], .{}, &diagnostic) catch {
                try std.testing.expect(diagnostic.start <= end);
                try std.testing.expect(diagnostic.end <= end);
                continue;
            };
            compiled.deinit();
        }
    }
    std.debug.print("SQL catalog truncation contracts: source_cases=479 prefixes={} elapsed_ns={}\n", .{ prefixes, std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start });
}

test "SQL exact corpus lookup owns parameters and rejects malformed identities" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const first = try corpus.get("sql-0001");
    try std.testing.expectEqualStrings("ddl", first.family);
    try std.testing.expectEqualStrings("PREPARE usage_plan(text) AS SELECT id FROM usage_records WHERE status = $1", first.sql);
    const parameterized = try corpus.get("sql-0166");
    try std.testing.expectEqual(@as(usize, 3), parameterized.params.len);
    try std.testing.expectEqualStrings("org_1", parameterized.params[0].object.get("string").?.string);
    try std.testing.expectEqualStrings("[\"rated\"]", parameterized.params[2].object.get("json").?.string);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const logical = try fixtures.Corpus.logicalParameters(arena.allocator(), parameterized);
    try std.testing.expectEqualStrings("org_1", logical[0].string);
    try std.testing.expectEqualStrings("rated", logical[2].array.items[0].string);
    const numeric = try fixtures.Corpus.logicalParameters(arena.allocator(), try corpus.get("sql-0229"));
    try std.testing.expectEqual(@as(i64, 10), numeric[0].integer);
    for ([_][]const u8{ "sql-0000", "sql-1587", "sql-1", "sql-+001", "other001" }) |id|
        try std.testing.expectError(error.UnknownParityCase, corpus.get(id));
}

test "SQL exact original rejection contracts fail before backend access" {
    var corpus = try fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    const cases = [_]struct { id: []const u8, expected: compiler.Error }{
        .{ .id = "sql-0378", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0382", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0672", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0673", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0699", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0700", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0701", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0702", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0703", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0776", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0777", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0780", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0781", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0782", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0783", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0784", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0785", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0786", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0787", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0788", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0789", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0790", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0791", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0792", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0793", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0794", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0795", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0796", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0834", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0835", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0839", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0840", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0841", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0842", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0843", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0844", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0845", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0879", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0880", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-0892", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0904", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0905", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-0906", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1034", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1035", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1036", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1037", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1038", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1039", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1040", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1041", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1042", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1043", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1045", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1046", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1183", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1184", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1193", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1194", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1195", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1196", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1197", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1198", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1199", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1200", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1201", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1203", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1204", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1210", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1211", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1212", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1213", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1214", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1215", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1409", .expected = error.InvalidSqlSyntax },
        .{ .id = "sql-1585", .expected = error.UnsupportedSqlShape },
        .{ .id = "sql-1586", .expected = error.UnsupportedSqlShape },
    };
    for (cases) |expected| {
        const case = try corpus.get(expected.id);
        try std.testing.expectEqualStrings("rejection", case.source_expectation);
        var diagnostic: compiler.Diagnostic = .{};
        try std.testing.expectError(expected.expected, compiler.compileDiagnostic(std.testing.allocator, case.sql, .{}, &diagnostic));
        try std.testing.expect(diagnostic.message.len > 0);
        try std.testing.expect(diagnostic.start <= diagnostic.end);
        try std.testing.expect(diagnostic.end <= case.sql.len);
    }
}
