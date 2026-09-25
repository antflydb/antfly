// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const runtime = @import("runtime.zig");
const describe = @import("describe.zig");
const relation_binding = @import("relation_binding.zig");

const Backend = struct {
    const Cursor = struct {
        owner: *Backend,
        request: catalog.StatementScan,
        offset: usize = 0,
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.offset == self.owner.row_count) return .{ .rows = &.{} };
            const count = @min(limit, self.owner.row_count - self.offset);
            const rows = try alloc.alloc(catalog.Row, count);
            for (rows, self.offset..) |*row, i| {
                const target = self.request.table.id == 1;
                const id = if (target or !self.owner.duplicates) (if (i == 0) "a" else if (i == 1) "b" else try std.fmt.allocPrint(alloc, "row{d}", .{i})) else "a";
                var values: std.json.ObjectMap = .empty;
                const fields = self.request.request.fields;
                for (fields) |field| {
                    const value: std.json.Value = if (std.mem.eql(u8, field, "n")) .{ .integer = @intCast(i + 1) } else if (std.mem.eql(u8, field, "delta")) .{ .integer = @intCast((i + 1) * 10) } else if (std.mem.eql(u8, field, "id")) .{ .string = id } else if (std.mem.eql(u8, field, "cold")) .{ .string = "old" } else .null;
                    try values.put(alloc, field, value);
                }
                const flags = try alloc.alloc(bool, fields.len);
                @memset(flags, false); // payload JSON null is not SQL NULL.
                row.* = .{ .id = id, .version = std.math.maxInt(u64) - 1, .expected_content_digest = if (self.request.request.include_primary_digest) @splat(9) else null, .value = .{ .object = values }, .sql_nulls = flags };
                if (self.request.request.include_document) row.document = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "{\"n\":1,\"payload\":null,\"cold\":\"old\",\"undeclared\":42}", .{});
            }
            self.offset += count;
            self.owner.rows_read += count;
            return .{ .rows = rows, .after = if (self.offset == self.owner.row_count) null else rows[rows.len - 1].id };
        }
    };
    duplicates: bool = false,
    document: bool = false,
    default_mode: bool = false,
    default_prepare_failure: bool = false,
    generated_mode: bool = false,
    deny_source: bool = false,
    row_count: usize = 2,
    rows_read: usize = 0,
    checkpoints: usize = 0,
    captures: usize = 0,
    last_scan_count: usize = 0,
    closes: usize = 0,
    commits: usize = 0,
    writes: usize = 0,
    states: [8]Cursor = undefined,
    cursors: [8]catalog.Cursor = undefined,
    fn resolve(ptr: *anyopaque, _: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, name.table, "target")) {
            try std.testing.expect(action == .read_write);
            if (self.generated_mode) return .{ .id = 1, .physical_name = "target", .schema_version = 1, .storage_mode = if (self.document) .document else .relational, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .json }, .{ .name = "cold", .path = "cold", .type = .string }, .{ .name = "g", .path = "g", .type = .integer, .generated = true } } };
            return .{ .id = 1, .physical_name = "target", .schema_version = 1, .storage_mode = if (self.document) .document else .relational, .columns = &.{ .{ .name = "n", .path = "n", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .json }, .{ .name = "cold", .path = "cold", .type = .string } } };
        }
        try std.testing.expectEqual(catalog.Action.read, action);
        if (self.deny_source) return error.Forbidden;
        return .{ .id = 2, .physical_name = "source", .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = .string }, .{ .name = "delta", .path = "delta", .type = .integer } } };
    }
    fn open(ptr: *anyopaque, _: std.mem.Allocator, scans: []const catalog.StatementScan) !catalog.StatementRead {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (scans.len != 2 and scans.len != 3 and !(self.default_mode and scans.len == 1)) return error.TestUnexpectedScanCount;
        self.captures += 1;
        self.last_scan_count = scans.len;
        for (scans, self.states[0..scans.len], self.cursors[0..scans.len]) |scan_, *state, *cursor| {
            state.* = .{ .owner = self, .request = scan_ };
            cursor.* = .{ .ptr = state, .next = Cursor.next, .close = undefined };
            if (scan_.table.id == 1) {
                try std.testing.expect(scan_.request.include_primary_digest);
                for (scan_.request.fields) |field| try std.testing.expect(!std.mem.eql(u8, field, "cold"));
            }
        }
        return .{ .ptr = self, .cursors = self.cursors[0..scans.len], .close = close };
    }
    fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.closes += 1;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedIndependentScan;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
    }
    fn prepare(ptr: *anyopaque, alloc: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) ![]const catalog.Mutation {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.default_mode) return mutations;
        if (self.default_prepare_failure) return error.NativeDefaultFailed;
        const normalized = try alloc.dupe(catalog.Mutation, mutations);
        for (normalized) |*mutation| {
            var object: std.json.ObjectMap = .empty;
            const row = mutation.row orelse return error.TestUnexpectedResult;
            for (row.object.keys(), row.object.values()) |key, value| try object.put(alloc, key, value);
            try std.testing.expect(!object.contains("cold"));
            try object.put(alloc, "cold", .{ .string = "default" });
            if (self.generated_mode) {
                try std.testing.expect(!object.contains("g"));
                try object.put(alloc, "g", .{ .integer = object.get("n").?.integer * 2 });
            }
            mutation.row = .{ .object = object };
        }
        return normalized;
    }
    fn mutate(ptr: *anyopaque, _: std.mem.Allocator, _: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 1), self.closes);
        self.commits += 1;
        self.writes += mutations.len;
        for (mutations) |mutation| {
            try std.testing.expectEqual(std.math.maxInt(u64) - 1, mutation.expected_version);
            try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(9)), &mutation.expected_content_digest.?);
            if (mutation.row) |row| {
                try std.testing.expectEqualStrings(if (self.default_mode) "default" else "new", row.object.get("cold").?.string);
                if (self.generated_mode) try std.testing.expectEqual(row.object.get("n").?.integer * 2, row.object.get("g").?.integer);
                try std.testing.expect(row.object.get("payload").? == .null);
                try std.testing.expectEqualStrings("payload", mutation.json_null_fields[0]);
                if (self.document) try std.testing.expectEqual(@as(i64, 42), row.object.get("undeclared").?.integer);
            }
        }
        return .committed;
    }
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .open_statement = open, .scan = scan, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepare, .checkpoint = checkpoint } };
    }
};

test "SQL UPDATE DEFAULT omits the old cell for native preparation" {
    for ([_]bool{ false, true }) |document| {
        for ([_][]const u8{
            "UPDATE target SET cold=DEFAULT RETURNING cold",
            "UPDATE target SET (n,cold)=ROW(n,DEFAULT) RETURNING cold",
        }) |sql| {
            var backend: Backend = .{ .document = document, .default_mode = true };
            var compiled = try compiler.compile(std.testing.allocator, sql, .{});
            defer compiled.deinit();
            var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
            try std.testing.expectEqual(@as(usize, 1), backend.captures);
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
            try std.testing.expectEqualStrings("default", result.output.rows[0][0].string);
        }
    }
    var failing: Backend = .{ .default_mode = true, .default_prepare_failure = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold=DEFAULT RETURNING cold", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.NativeDefaultFailed, runtime.execute(std.testing.allocator, failing.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), failing.captures);
    try std.testing.expectEqual(@as(usize, 1), failing.closes);
    try std.testing.expectEqual(@as(usize, 0), failing.commits);
    var generated: Backend = .{ .default_mode = true, .generated_mode = true };
    var generated_sql = try compiler.compile(std.testing.allocator, "UPDATE target SET cold=DEFAULT,g=DEFAULT RETURNING cold,g", .{});
    defer generated_sql.deinit();
    var generated_result = try runtime.execute(std.testing.allocator, generated.backend(), &generated_sql, &.{}, .{});
    defer generated_result.deinit();
    try std.testing.expectEqual(@as(u64, 2), generated_result.output.rows_affected);
    try std.testing.expectEqualStrings("default", generated_result.output.rows[0][0].string);
    try std.testing.expectEqualStrings("2", generated_result.output.rows[0][1].string);
    try std.testing.expectEqual(@as(usize, 1), generated.commits);
    var invalid = try compiler.compile(std.testing.allocator, "UPDATE target SET g=3", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.SqlGeneratedColumnWrite, runtime.execute(std.testing.allocator, generated.backend(), &invalid, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 1), generated.commits);
}

test "SQL mutation target binding reuses its pinned identity only for reads" {
    var backend: Backend = .{};
    const target_name: ast.Name = .{ .table = "target" };
    const pinned = try backend.backend().vtable.resolve(&backend, std.testing.allocator, target_name, .read_write);
    var adapter: relation_binding.TargetResolveAdapter = .{ .backend = backend.backend(), .table = pinned, .name = target_name };
    const binding = adapter.iface();
    const target = try binding.vtable.resolve(binding.ptr, std.testing.allocator, target_name, .read);
    try std.testing.expectEqual(pinned.id, target.id);
    try std.testing.expectEqual(pinned.schema_version, target.schema_version);
    const source = try binding.vtable.resolve(binding.ptr, std.testing.allocator, .{ .table = "source" }, .read);
    try std.testing.expectEqual(@as(u64, 2), source.id);
    for ([_]catalog.Action{ .write, .read_write, .admin }) |action| {
        try std.testing.expectError(error.UnsupportedSqlExecution, binding.vtable.resolve(binding.ptr, std.testing.allocator, target_name, action));
        try std.testing.expectError(error.UnsupportedSqlExecution, binding.vtable.resolve(binding.ptr, std.testing.allocator, .{ .table = "source" }, action));
    }
}

test "SQL joined mutations preserve one capture exact fences typed values and document fields" {
    for ([_]bool{ false, true }) |document| {
        var backend: Backend = .{ .document = document };
        var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta, cold='new' FROM source s WHERE t._id=s.id RETURNING t.n", .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("11", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("22", result.output.rows[1][0].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL target-only mutation subqueries share one captured relational plan" {
    // sql-0068: its EXPLAIN remains dry-run, while these ordinary mutations
    // prove the same source-aware plan executes under one snapshot and commit.
    for ([_]struct { sql: []const u8, tag: []const u8 }{
        .{ .sql = "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET cold='new' WHERE EXISTS (SELECT id FROM source WHERE source.id=target._id)", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET n=(SELECT delta FROM source WHERE source.id=target._id),cold='new'", .tag = "UPDATE" },
        .{ .sql = "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source WHERE id='a')", .tag = "UPDATE" },
        .{ .sql = "DELETE FROM target WHERE _id IN (SELECT id FROM source)", .tag = "DELETE" },
    }) |case| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.tag, result.output.command_tag);
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expect(backend.last_scan_count <= 3);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
        try std.testing.expectEqual(@as(usize, 2), backend.writes);
    }
}

test "SQL row subquery assigns positional values after bounded ordered source" {
    for ([_][]const u8{
        "UPDATE target SET (n,cold)=(SELECT delta AS amount,'new' AS label FROM source ORDER BY amount DESC LIMIT 1) RETURNING n,cold",
        "WITH s AS (SELECT delta FROM source) UPDATE target SET (n,cold)=(SELECT delta,'new' FROM s ORDER BY delta DESC LIMIT 1) RETURNING n,cold",
        "WITH \"$update_row_source_0\" AS (SELECT delta FROM source) UPDATE target SET (n,cold)=(SELECT delta,'new' FROM \"$update_row_source_0\" ORDER BY delta DESC LIMIT 1) RETURNING n,cold",
    }) |sql| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("20", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("new", result.output.rows[0][1].string);
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 2), backend.last_scan_count);
        try std.testing.expectEqual(@as(usize, 1), backend.commits);
    }
}

test "SQL row subquery cannot escape its independent derived source" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source WHERE source.id=target._id)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.UndefinedColumn, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.captures);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL mutation membership subquery scales by captured rows" {
    var backend: Backend = .{ .row_count = 1024 };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .page_rows = 17 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1024), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.commits);
    try std.testing.expectEqual(@as(usize, 1024), backend.writes);
    try std.testing.expect(backend.rows_read <= 3 * backend.row_count);
}

test "SQL mutation subquery quota aborts before commit" {
    var backend: Backend = .{};
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expectEqual(@as(usize, 1), backend.closes);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL mutation subquery requires source read authority before capture" {
    var backend: Backend = .{ .deny_source = true };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target SET cold='new' WHERE _id IN (SELECT id FROM source)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.Forbidden, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.captures);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL mutation scalar subquery rejects multiple rows before commit" {
    for ([_][]const u8{
        "UPDATE target SET n=(SELECT delta FROM source WHERE source.id=target._id),cold='new'",
        "UPDATE target SET (n,cold)=ROW((SELECT delta FROM source WHERE source.id=target._id),'new')",
        "UPDATE target SET (n,cold)=(SELECT delta,'new' FROM source)",
    }) |sql| {
        var backend: Backend = .{ .duplicates = true };
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 1), backend.captures);
        try std.testing.expectEqual(@as(usize, 1), backend.closes);
        try std.testing.expectEqual(@as(usize, 0), backend.commits);
    }
}

test "SQL joined FROM mutations choose hash keys and preserve CTE target identity" {
    for ([_][]const u8{
        "UPDATE target t SET n=t.n+s.delta, cold='new' FROM source s WHERE t._id=s.id RETURNING t.n",
        "WITH target AS (SELECT id, delta FROM source) UPDATE target t SET n=t.n+s.delta, cold='new' FROM target s WHERE t._id=s.id RETURNING t.n",
        "WITH RECURSIVE input(id,delta) AS (SELECT id,delta FROM source UNION ALL SELECT id,delta FROM input WHERE FALSE) UPDATE target t SET n=t.n+s.delta,cold='new' FROM input s WHERE t._id=s.id RETURNING t.n",
    }) |sql| {
        var backend: Backend = .{};
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const bound = try describe.bind(arena.allocator(), backend.backend(), &compiled, &.{});
        const relation = bound.joined_mutation.?.input.relation.?;
        try std.testing.expect(relation.root.operation == .join);
        try std.testing.expectEqual(@as(usize, 1), relation.root.operation.join.left_keys.len);
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 2), result.output.rows_affected);
        try std.testing.expectEqualStrings("11", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("22", result.output.rows[1][0].string);
    }
}

test "SQL joined mutations empty matches and limits never partially commit" {
    var backend: Backend = .{};
    var empty = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id AND FALSE", .{});
    defer empty.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &empty, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
    backend = .{};
    var limited = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=s.delta,cold='new' FROM source s WHERE t._id=s.id", .{});
    defer limited.deinit();
    try std.testing.expectError(error.SqlResultTooLarge, runtime.execute(std.testing.allocator, backend.backend(), &limited, &.{}, .{ .mutation_rows = 1 }));
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
}

test "SQL joined mutation equality work scales with inputs not Cartesian candidates" {
    const benchmark = try std.testing.environ.contains(std.testing.allocator, "ANTFLY_SQL_JOINED_MUTATION_BENCHMARK");
    const rows: usize = if (benchmark) 4096 else 1024;
    const execution_alloc = if (benchmark) std.heap.smp_allocator else std.testing.allocator;
    var backend: Backend = .{ .row_count = rows };
    var compiled = try compiler.compile(std.testing.allocator, "UPDATE target t SET n=t.n+s.delta,cold='new' FROM source s WHERE t._id=s.id", .{});
    defer compiled.deinit();
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var result = try runtime.execute(execution_alloc, backend.backend(), &compiled, &.{}, .{ .mutation_rows = rows, .retained_bytes = if (benchmark) 64 << 20 else 8 << 20 });
    defer result.deinit();
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
    try std.testing.expectEqual(@as(u64, rows), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, rows * 2), backend.rows_read);
    try std.testing.expectEqual(@as(usize, 1), backend.captures);
    try std.testing.expect(backend.checkpoints < rows * 100);
    std.debug.print("SQL joined mutation: targets={d} source_rows={d} native_rows={d} captures={d} checkpoints={d} peak_bytes={d} elapsed_ns={d}\n", .{ rows, rows, backend.rows_read, backend.captures, backend.checkpoints, result.peakMemoryBytes(), elapsed });
}

test "SQL DELETE source fanout consumes one target mutation slot" {
    var backend: Backend = .{ .row_count = 256, .duplicates = true };
    var compiled = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id", .{});
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .mutation_rows = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL joined UPDATE rejects ambiguous matches atomically and DELETE deduplicates" {
    var backend: Backend = .{ .duplicates = true };
    var update = try compiler.compile(std.testing.allocator, "UPDATE target t JOIN source s ON t._id=s.id SET t.n=s.delta, cold='new'", .{});
    defer update.deinit();
    try std.testing.expectError(error.SqlMutationCardinalityViolation, runtime.execute(std.testing.allocator, backend.backend(), &update, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), backend.commits);
    backend = .{ .duplicates = true };
    var deletion = try compiler.compile(std.testing.allocator, "DELETE FROM target t USING source s WHERE t._id=s.id", .{});
    defer deletion.deinit();
    var result = try runtime.execute(std.testing.allocator, backend.backend(), &deletion, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), backend.writes);
}

test "SQL joined mutations release every failed allocation without partial commits" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var backend: Backend = .{};
            var compiled = try compiler.compile(alloc, "UPDATE target t SET n=s.delta+$1, cold='new' FROM source s WHERE t._id=s.id", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{.{ .integer = 2 }}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), backend.commits);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
