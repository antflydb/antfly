// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const c = @import("ffi.zig").c;
const p = @import("plan.zig");
const sqlite = @import("sqlite.zig");
const aflite = @import("aflite.zig");
const Value = std.json.Value;
const Cell = struct { value: Value, sql_null: bool };

fn query(a: std.mem.Allocator, table: p.Table) ![]const u8 {
    return std.fmt.allocPrint(a, "SELECT * FROM ({s}) AS migration_source", .{std.mem.trimEnd(u8, table.query, " \t\n\r;")});
}

fn columns(a: std.mem.Allocator, table: p.Table) ![]const u8 {
    var result: std.Io.Writer.Allocating = .init(a);
    for (table.columns, 0..) |col, i| {
        if (i != 0) try result.writer.writeAll(",");
        try result.writer.print("\"{s}\"", .{col.name});
    }
    return result.toOwnedSlice();
}

fn readRow(a: std.mem.Allocator, stmt: *c.sqlite3_stmt, table: p.Table, max_bytes: usize) ![]Cell {
    const row = try a.alloc(Cell, table.columns.len);
    var remaining = max_bytes;
    for (table.columns, row) |col, *value| {
        const index = try sqlite.columnIndex(stmt, col.from);
        value.* = .{ .value = try sqlite.cell(a, stmt, index, col, remaining), .sql_null = c.sqlite3_column_type(stmt, index) == c.SQLITE_NULL };
        const bytes = try std.json.Stringify.valueAlloc(a, value.*, .{});
        if (bytes.len > remaining) return error.RowTooLarge;
        remaining -= bytes.len;
    }
    for (table.key) |key| for (table.columns, row) |col, value| {
        if (std.mem.eql(u8, col.name, key) and value.sql_null) return error.NullKey;
    };
    return row;
}

fn sourceFor(plan: p.Plan, sources: []sqlite.Source, name: []const u8) sqlite.Source {
    for (plan.sources, sources) |definition, source| if (std.mem.eql(u8, definition.name, name)) return source;
    unreachable; // Plan.validate checks all references.
}

fn preflight(a: std.mem.Allocator, scratch: std.mem.Allocator, source: sqlite.Source, table: p.Table, max_bytes: usize) !u64 {
    const q = try query(a, table);
    // A single read-only statement; SQLite executes all joins/projections.
    const stmt = try source.prepare(a, q);
    defer _ = c.sqlite3_finalize(stmt);
    for (table.columns) |col| _ = try sqlite.columnIndex(stmt, col.from);
    var keys: std.Io.Writer.Allocating = .init(a);
    for (table.key, 0..) |key, i| for (table.columns) |col| if (std.mem.eql(u8, col.name, key)) {
        if (i != 0) try keys.writer.writeAll(",");
        try keys.writer.print("\"{s}\"", .{col.from});
    };
    const duplicate_query = try std.fmt.allocPrint(a, "SELECT 1 FROM ({s}) AS source_keys GROUP BY {s} HAVING COUNT(*) > 1 LIMIT 1", .{ q, keys.written() });
    const duplicates = try source.prepare(a, duplicate_query);
    defer _ = c.sqlite3_finalize(duplicates);
    if (try sqlite.next(duplicates)) return error.DuplicateSourceKey;
    var count: u64 = 0;
    while (try sqlite.next(stmt)) {
        var row_arena = std.heap.ArenaAllocator.init(scratch);
        defer row_arena.deinit();
        const row = try readRow(row_arena.allocator(), stmt, table, max_bytes);
        const encoded = try std.json.Stringify.valueAlloc(row_arena.allocator(), row, .{});
        if (encoded.len > max_bytes) return error.RowTooLarge;
        count += 1;
    }
    return count;
}

fn insert(sink: aflite.Sink, a: std.mem.Allocator, table: p.Table, rows: []const []Cell, max_bytes: usize) !void {
    var sql: std.Io.Writer.Allocating = .init(a);
    try sql.writer.print("INSERT INTO \"{s}\" ({s}) VALUES ", .{ table.target, try columns(a, table) });
    var params: std.ArrayList(Value) = .empty;
    for (rows, 0..) |row, ri| {
        if (ri != 0) try sql.writer.writeAll(",");
        try sql.writer.writeAll("(");
        for (table.columns, row, 0..) |col, value, ci| {
            if (ci != 0) try sql.writer.writeAll(",");
            if (col.type == .json and !value.sql_null) {
                try params.append(a, .{ .string = try std.json.Stringify.valueAlloc(a, value.value, .{}) });
                try sql.writer.print("CAST(${d} AS JSONB)", .{params.items.len});
            } else {
                try params.append(a, value.value);
                try sql.writer.print("${d}", .{params.items.len});
            }
        }
        try sql.writer.writeAll(")");
    }
    if (table.write_mode == .upsert) {
        try sql.writer.writeAll(" ON CONFLICT (");
        for (table.key, 0..) |key, i| {
            if (i != 0) try sql.writer.writeAll(",");
            try sql.writer.print("\"{s}\"", .{key});
        }
        try sql.writer.writeAll(") DO UPDATE SET ");
        var assignments: usize = 0;
        for (table.columns) |col| {
            var is_key = false;
            for (table.key) |key| if (std.mem.eql(u8, key, col.name)) {
                is_key = true;
            };
            if (is_key) continue;
            if (assignments != 0) try sql.writer.writeAll(",");
            try sql.writer.print("\"{s}\" = EXCLUDED.\"{s}\"", .{ col.name, col.name });
            assignments += 1;
        }
        // Key-only tables use a no-op assignment while retaining one affected
        // row per source key, avoiding different DO NOTHING count semantics.
        if (assignments == 0) try sql.writer.print("\"{s}\" = EXCLUDED.\"{s}\"", .{ table.key[0], table.key[0] });
    }
    // Includes parameter escaping and statement overhead in the hard limit.
    const request = try std.json.Stringify.valueAlloc(a, .{ .statement = sql.written(), .parameters = params.items }, .{});
    if (request.len > max_bytes) {
        if (rows.len == 1) return error.RowTooLarge;
        const middle = rows.len / 2;
        try insert(sink, a, table, rows[0..middle], max_bytes);
        try insert(sink, a, table, rows[middle..], max_bytes);
        return;
    }
    const result = try sink.sql(a, sql.written(), params.items);
    if (result.object.get("rows_affected").?.integer != rows.len) return error.InsertCountMismatch;
}

fn copy(a: std.mem.Allocator, scratch: std.mem.Allocator, source: sqlite.Source, sink: aflite.Sink, table: p.Table, plan: p.Plan) !void {
    const stmt = try source.prepare(a, try query(a, table));
    defer _ = c.sqlite3_finalize(stmt);
    var done = false;
    while (!done) {
        var batch = std.heap.ArenaAllocator.init(scratch);
        defer batch.deinit();
        const alloc = batch.allocator();
        var rows: std.ArrayList([]Cell) = .empty;
        var bytes: usize = 0;
        const row_limit = @min(plan.batch_rows, @min(aflite.max_parameters, aflite.max_batch_cells) / table.columns.len);
        while (rows.items.len < row_limit and bytes < plan.batch_bytes / 2) {
            if (!try sqlite.next(stmt)) {
                done = true;
                break;
            }
            const row = try readRow(alloc, stmt, table, plan.batch_bytes);
            bytes += (try std.json.Stringify.valueAlloc(alloc, row, .{})).len;
            try rows.append(alloc, row);
        }
        if (rows.items.len != 0) try insert(sink, alloc, table, rows.items, plan.batch_bytes);
    }
}

fn countRows(a: std.mem.Allocator, sink: aflite.Sink, table: p.Table) !u64 {
    const result = try sink.sql(a, try std.fmt.allocPrint(a, "SELECT COUNT(*) FROM \"{s}\"", .{table.target}), &.{});
    return @intCast(try aflite.integer(result.object.get("rows").?.array.items[0].array.items[0]));
}

fn keyIndex(table: p.Table, name: []const u8) usize {
    for (table.columns, 0..) |col, i| if (std.mem.eql(u8, col.name, name)) return i;
    unreachable;
}

fn rowKey(a: std.mem.Allocator, table: p.Table, row: []const Cell) ![]const u8 {
    const values = try a.alloc(Value, table.key.len);
    for (table.key, values) |key, *value| value.* = row[keyIndex(table, key)].value;
    return std.json.Stringify.valueAlloc(a, values, .{});
}

fn predicates(writer: *std.Io.Writer, a: std.mem.Allocator, table: p.Table, rows: []const []Cell, params: *std.ArrayList(Value)) anyerror!void {
    if (rows.len > 1) {
        const middle = rows.len / 2;
        try writer.writeAll("(");
        try predicates(writer, a, table, rows[0..middle], params);
        try writer.writeAll(" OR ");
        try predicates(writer, a, table, rows[middle..], params);
        try writer.writeAll(")");
        return;
    }
    for (table.key, 0..) |key, i| {
        if (i != 0) try writer.writeAll(" AND ");
        try params.append(a, rows[0][keyIndex(table, key)].value);
        try writer.print("\"{s}\" = ${d}", .{ key, params.items.len });
    }
}

fn verify(a: std.mem.Allocator, scratch: std.mem.Allocator, source: sqlite.Source, sink: aflite.Sink, table: p.Table, plan: p.Plan, expected_count: u64) !void {
    const target_count = try countRows(a, sink, table);
    if (if (table.write_mode == .insert) target_count != expected_count else target_count < expected_count) return error.TargetCountMismatch;
    const stmt = try source.prepare(a, try query(a, table));
    defer _ = c.sqlite3_finalize(stmt);
    var count: u64 = 0;
    var done = false;
    while (!done) {
        var arena = std.heap.ArenaAllocator.init(scratch);
        defer arena.deinit();
        const alloc = arena.allocator();
        var rows: std.ArrayList([]Cell) = .empty;
        var expected: std.StringHashMap([]Cell) = .init(alloc);
        var bytes: usize = 0;
        const row_limit = @min(plan.verification_rows, aflite.max_batch_cells / table.key.len);
        while (rows.items.len < row_limit and bytes < plan.batch_bytes / 2) {
            if (!try sqlite.next(stmt)) {
                done = true;
                break;
            }
            const row = try readRow(alloc, stmt, table, plan.batch_bytes);
            bytes += (try std.json.Stringify.valueAlloc(alloc, row, .{})).len;
            try rows.append(alloc, row);
            try expected.put(try rowKey(alloc, table, row), row);
        }
        if (rows.items.len == 0) break;
        var sql: std.Io.Writer.Allocating = .init(alloc);
        try sql.writer.print("SELECT {s} FROM \"{s}\" WHERE ", .{ try columns(alloc, table), table.target });
        var params: std.ArrayList(Value) = .empty;
        try predicates(&sql.writer, alloc, table, rows.items, &params);
        // Fetch one more than expected to detect unexpected duplicate keys.
        const result = try sink.sqlLimit(alloc, sql.written(), params.items, rows.items.len + 1);
        const actual_rows = result.object.get("rows").?.array.items;
        if (actual_rows.len != rows.items.len) return error.TargetKeyMismatch;
        const null_rows = result.object.get("sql_nulls") orelse return error.MissingNullInformation;
        for (actual_rows, 0..) |actual_row, ri| {
            if (actual_row.array.items.len != table.columns.len) return error.TargetColumnsMismatch;
            const decoded = try alloc.alloc(Cell, table.columns.len);
            for (table.columns, actual_row.array.items, decoded, 0..) |col, actual, *cell, ci| {
                const is_null = if (null_rows == .null) actual == .null else null_rows.array.items[ri].array.items[ci].bool;
                cell.* = .{ .value = try aflite.decode(alloc, col.type, actual, is_null), .sql_null = is_null };
            }
            const matched = expected.fetchRemove(try rowKey(alloc, table, decoded)) orelse return error.TargetKeyMismatch;
            for (matched.value, decoded) |e, actual| if (e.sql_null != actual.sql_null or !aflite.equal(e.value, actual.value)) return error.TargetValueMismatch;
        }
        if (expected.count() != 0) return error.TargetKeyMismatch;
        count += rows.items.len;
        if (count % 100 == 0) std.debug.print("verified {s}: {d}/{d} rows\n", .{ table.target, count, expected_count });
    }
    if (count != expected_count) return error.SourceCountChanged;
}

pub fn run(init: std.process.Init, plan: p.Plan, base: []const u8, dry_run: bool, verify_only: bool) !void {
    try plan.validate();
    if (verify_only and plan.schema.seed == null) return error.VerifyRequiresSeed;
    var arena = std.heap.ArenaAllocator.init(init.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const destination = try std.fs.path.resolve(a, &.{ base, plan.destination });
    // Refuse any existing directory entry, including dangling symlinks.
    const dest_z = try a.dupeSentinel(u8, destination, 0);
    if (!verify_only and c.faccessat(c.AT_FDCWD, dest_z, c.F_OK, c.AT_SYMLINK_NOFOLLOW) == 0) return error.DestinationExists;
    const sources = try a.alloc(sqlite.Source, plan.sources.len);
    var opened: usize = 0;
    defer for (sources[0..opened]) |source| source.close();
    for (plan.sources, sources) |definition, *source| {
        source.* = try sqlite.Source.open(a, try std.fs.path.resolve(a, &.{ base, definition.path }));
        opened += 1;
    }
    const counts = try a.alloc(u64, plan.tables.len);
    for (plan.tables, counts) |table, *count| {
        std.debug.print("preflight {s}\n", .{table.target});
        count.* = try preflight(a, init.gpa, sourceFor(plan, sources, table.source), table, plan.batch_bytes);
    }
    var nonce: [16]u8 = undefined;
    init.io.random(&nonce);
    const directory = try std.fmt.allocPrint(a, "{s}.migrate-{s}", .{ destination, std.fmt.bytesToHex(nonce, .lower) });
    try std.Io.Dir.cwd().createDir(init.io, directory, @fromBackingInt(@intCast(0o700)));
    defer std.Io.Dir.cwd().deleteTree(init.io, directory) catch {};
    const stage = try std.fs.path.join(a, &.{ directory, "staged.aflite" });
    const seed = if (plan.schema.seed) |s| try std.fs.path.resolve(a, &.{ base, s }) else null;
    {
        const sink = try aflite.Sink.open(a, stage, seed);
        defer sink.close();
        for (plan.schema.statements) |statement| _ = try sink.sql(a, statement, &.{});
        for (plan.tables) |table| {
            const description = try sink.sql(a, try std.fmt.allocPrint(a, "SELECT {s} FROM \"{s}\" LIMIT 0", .{ try columns(a, table), table.target }), &.{});
            const metadata = description.object.get("columns").?.array.items;
            if (metadata.len != table.columns.len) return error.TargetColumnsMismatch;
            for (table.columns, metadata) |col, target| {
                const expected = switch (col.type) {
                    .text => "string",
                    .integer => "integer",
                    .real => "number",
                    .boolean => "boolean",
                    .json => "json",
                };
                if (!std.mem.eql(u8, expected, target.object.get("type").?.string)) return error.TargetTypeMismatch;
            }
            if (!verify_only and table.write_mode == .insert and try countRows(a, sink, table) != 0) return error.TargetMustBeEmpty;
        }
        if (dry_run) {
            std.debug.print("dry-run passed: source conversions, keys, and target columns validated; destination not published\n", .{});
            return;
        }
        if (!verify_only) for (plan.tables) |table| {
            std.debug.print("copy {s}\n", .{table.target});
            try copy(a, init.gpa, sourceFor(plan, sources, table.source), sink, table, plan);
        };
        for (plan.tables, counts) |table, count| {
            std.debug.print("verify {s}: {d} rows\n", .{ table.target, count });
            try verify(a, init.gpa, sourceFor(plan, sources, table.source), sink, table, plan, count);
        }
    }
    try aflite.checkFile(a, stage);
    if (verify_only) {
        std.debug.print("verification passed; seed and destination unchanged\n", .{});
        return;
    }
    const stage_z = try a.dupeSentinel(u8, stage, 0);
    const fd = c.open(stage_z, c.O_RDONLY);
    if (fd < 0) return error.StageOpenFailed;
    defer _ = c.close(fd);
    if (c.fchmod(fd, 0o600) != 0) return error.StagePermissionsFailed;
    if (c.fsync(fd) != 0) return error.StageSyncFailed;
    // A hard link atomically publishes without replacing a racing creator.
    if (c.link(stage_z, dest_z) != 0) return error.PublishFailed;
    const parent = try a.dupeSentinel(u8, std.fs.path.dirname(destination).?, 0);
    const parent_fd = c.open(parent, c.O_RDONLY);
    if (parent_fd < 0) return error.PublishedDurabilityUnknown;
    defer _ = c.close(parent_fd);
    if (c.fsync(parent_fd) != 0) return error.PublishedDurabilityUnknown;
    std.debug.print("published {s}\n", .{destination});
}

test "preflight releases scratch memory across thousands of rows" {
    var lifetime_buffer: [64 * 1024]u8 = undefined;
    var scratch_buffer: [64 * 1024]u8 = undefined;
    var lifetime = std.heap.FixedBufferAllocator.init(&lifetime_buffer);
    var scratch = std.heap.FixedBufferAllocator.init(&scratch_buffer);
    var arena = std.heap.ArenaAllocator.init(lifetime.allocator());
    defer arena.deinit();
    var db: ?*c.sqlite3 = null;
    try std.testing.expectEqual(c.SQLITE_OK, c.sqlite3_open(":memory:", &db));
    defer _ = c.sqlite3_close(db);
    const table: p.Table = .{
        .source = "fixture",
        .query = "WITH RECURSIVE r(id) AS (VALUES(1) UNION ALL SELECT id+1 FROM r WHERE id<2000) SELECT id, printf('%01024d',id) AS value FROM r",
        .target = "items",
        .key = &.{"id"},
        .columns = &.{
            .{ .name = "id", .from = "id", .type = .integer },
            .{ .name = "value", .from = "value", .type = .text },
        },
    };
    try std.testing.expectEqual(@as(u64, 2000), try preflight(arena.allocator(), scratch.allocator(), .{ .db = db }, table, 4096));
    try std.testing.expectEqual(@as(usize, 0), scratch.end_index);
}
