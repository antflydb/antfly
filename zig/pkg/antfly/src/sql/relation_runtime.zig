// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Typed relational iterator execution. All physical cursors open together;
//! hash join retains one build side and streams the probe side under quota.
const std = @import("std");
const catalog = @import("catalog.zig");
const binding = @import("relation_binding.zig");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const describe = @import("describe.zig");
const Datum = scalar.Datum;
const Allocator = std.mem.Allocator;

const SetTestBackend = struct {
    checkpoints: usize = 0,
    cancel_after: usize = std.math.maxInt(usize),
    fn resolve(_: *anyopaque, _: Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedBackendCall;
    }
    fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedBackendCall;
    }
    fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedBackendCall;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
        if (self.checkpoints > self.cancel_after) return error.Canceled;
    }
    fn backend(self: *@This()) catalog.Backend {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint } };
    }
};

test "SQL set operations preserve multiplicities precedence and output ordering" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    const Case = struct { sql: []const u8, expected: []const i64 };
    var backend: SetTestBackend = .{};
    for ([_]Case{
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY x DESC", .expected = &.{ 2, 1, 1 } },
        .{ .sql = "SELECT 1 AS x UNION SELECT 1 UNION SELECT 2 ORDER BY x", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT 1 AS x UNION SELECT 2 INTERSECT SELECT 2 ORDER BY x", .expected = &.{ 1, 2 } },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 EXCEPT ALL SELECT 1", .expected = &.{1} },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 EXCEPT SELECT 1", .expected = &.{} },
        .{ .sql = "SELECT 1 AS x UNION ALL SELECT 1 INTERSECT ALL SELECT 1", .expected = &.{ 1, 1 } },
        .{ .sql = "SELECT 1 AS x INTERSECT (SELECT 1 UNION ALL SELECT 1)", .expected = &.{1} },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1) AS l INTERSECT ALL (SELECT 1 UNION ALL SELECT 1)", .expected = &.{ 1, 1 } },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1) AS l INTERSECT (SELECT 1 UNION ALL SELECT 1)", .expected = &.{1} },
        .{ .sql = "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 2) AS l EXCEPT ALL (SELECT 1 UNION ALL SELECT 1)", .expected = &.{2} },
        .{ .sql = "SELECT 3 AS x UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY x LIMIT 1 OFFSET 1", .expected = &.{2} },
        .{ .sql = "SELECT 1 AS x WHERE TRUE UNION SELECT 2 WHERE FALSE", .expected = &.{1} },
        .{ .sql = "(SELECT 1 AS x UNION SELECT 2 LIMIT 1) UNION SELECT 3 ORDER BY x", .expected = &.{ 1, 3 } },
        .{ .sql = "((SELECT 1 AS x UNION SELECT 2)) EXCEPT SELECT 2", .expected = &.{1} },
    }) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case.sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.expected.len, result.output.rows.len);
        for (result.output.rows, case.expected) |row, expected| try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, row[0].string, 10));
    }
}

test "SQL set inference is arm-order independent and delays unknown NULL typing" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    var backend: SetTestBackend = .{};
    for ([_][]const u8{
        "SELECT $1 AS x UNION SELECT 1",
        "SELECT 1 AS x UNION SELECT $1",
        "(SELECT $1 AS x) UNION SELECT 1",
        "(SELECT $1 AS x LIMIT 1) UNION SELECT 1",
        "SELECT COALESCE($1, NULL) AS x UNION SELECT 1",
        "SELECT NULL AS x UNION SELECT NULL UNION SELECT $1 UNION SELECT 1",
        "SELECT CASE WHEN TRUE THEN SUM($1) ELSE 0 END AS x UNION SELECT 1",
        "WITH n AS (SELECT NULL AS x) SELECT COALESCE(n.x,$1) AS x FROM n UNION SELECT 1",
    }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .integer = 1 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@import("ast.zig").ColumnType.integer, result.output.columns[0].type);
    }
    var predicate_query = try compiler.compile(std.testing.allocator, "SELECT $1 AS x WHERE $1=1 UNION ALL SELECT 1.5", .{});
    defer predicate_query.deinit();
    var predicate_result = try runtime.execute(std.testing.allocator, backend.backend(), &predicate_query, &.{.{ .integer = 1 }}, .{});
    defer predicate_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), predicate_result.output.rows.len);
    try std.testing.expectEqual(@import("ast.zig").ColumnType.number, predicate_result.output.columns[0].type);
    for ([_][]const u8{ "SELECT $1 AS x UNION SELECT 1 UNION SELECT 1.5", "SELECT 1.5 AS x UNION SELECT 1 UNION SELECT $1" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{.{ .float = 2.5 }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@import("ast.zig").ColumnType.number, result.output.columns[0].type);
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
    }
}

test "SQL sets distinguish typed JSON null and SQL NULL and release every allocation" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var backend: SetTestBackend = .{};
            var compiled = try compiler.compile(alloc, "SELECT CAST('null' AS JSON) AS x UNION SELECT NULL UNION SELECT CAST('null' AS JSON)", .{});
            defer compiled.deinit();
            var result = try runtime.execute(alloc, backend.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.output.rows.len);
            try std.testing.expect(!result.output.sql_nulls.?[0][0]);
            try std.testing.expect(result.output.sql_nulls.?[1][0]);
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL set admission rejects incompatible shapes and enforces the shared memory budget" {
    const runtime = @import("runtime.zig");
    const compiler = @import("compiler.zig");
    var backend: SetTestBackend = .{};
    for ([_][]const u8{ "SELECT 1 UNION SELECT TRUE", "SELECT 1 UNION SELECT 1, 2" }) |sql| {
        var compiled = try compiler.compile(std.testing.allocator, sql, .{});
        defer compiled.deinit();
        try std.testing.expectError(error.SqlTypeMismatch, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
    }
    var compiled = try compiler.compile(std.testing.allocator, "SELECT 'long retained payload' AS x UNION SELECT 'another retained payload'", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.SqlProgramLimitExceeded, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{ .retained_bytes = 1024 }));
    backend = .{ .cancel_after = 12 };
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, backend.backend(), &compiled, &.{}, .{}));
}

fn Engine(comptime Context: type) type {
    return struct {
        const Self = @This();
        context: Context,
        cursors: []const catalog.Cursor,
        visited: usize = 0,
        work: usize = 0,

        fn checkpoint(self: *Self) !void {
            try self.context.checkpoint();
            self.work += 1;
            if (self.work > self.context.limits.scan_rows *| 64) return error.SqlProgramLimitExceeded;
        }

        const Iterator = struct {
            engine: *Self,
            node: *const binding.Node,
            arena: std.heap.ArenaAllocator,
            scratch: std.heap.ArenaAllocator,
            left: ?*Iterator = null,
            right: ?*Iterator = null,
            page: ?catalog.Page = null,
            page_index: usize = 0,
            pages: usize = 0,
            eof: bool = false,
            emitted: bool = false,
            hash_join: ?*operators.HashJoin = null,
            probe: ?operators.HashJoin.Probe = null,
            left_values: ?[]const Datum = null,
            left_matched: bool = false,
            unmatched_index: usize = 0,
            output: ?@import("runtime.zig").Output = null,
            output_index: usize = 0,
            query_fields: ?[]const []const u8 = null,
            query_skip: usize = 0,
            query_remaining: usize = 0,
            set_entries: std.ArrayList(SetEntry) = .empty,
            set_heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
            set_ready: bool = false,

            const SetEntry = struct { values: []const Datum, count: usize, next: ?usize };

            fn create(engine: *Self, node: *const binding.Node) anyerror!*Iterator {
                const alloc = engine.context.alloc;
                const self = try alloc.create(Iterator);
                self.* = .{ .engine = engine, .node = node, .arena = .init(alloc), .scratch = .init(alloc) };
                errdefer self.deinit();
                switch (node.operation) {
                    .join => |join| {
                        self.left = try create(engine, join.left);
                        self.right = try create(engine, join.right);
                    },
                    .query => |query| self.left = try create(engine, query.source),
                    .set => |set| {
                        self.left = try create(engine, set.left);
                        self.right = try create(engine, set.right);
                    },
                    else => {},
                }
                return self;
            }
            fn deinit(self: *Iterator) void {
                if (self.page) |page| page.deinit();
                if (self.left) |left| left.deinit();
                if (self.right) |right| right.deinit();
                if (self.hash_join) |join| join.deinit();
                self.arena.deinit();
                self.scratch.deinit();
                self.engine.context.alloc.destroy(self);
            }
            fn next(self: *Iterator, alloc: Allocator) anyerror!?[]const Datum {
                try self.engine.checkpoint();
                return switch (self.node.operation) {
                    .singleton => if (self.emitted) null else blk: {
                        self.emitted = true;
                        break :blk &.{};
                    },
                    .scan => |scan| blk: {
                        while (self.page == null or self.page_index == self.page.?.rows.len) {
                            if (self.eof) break :blk null;
                            if (self.page) |page| page.deinit();
                            self.page = null;
                            _ = self.arena.reset(.free_all);
                            self.pages += 1;
                            if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                            const cursor = self.engine.cursors[scan.index];
                            self.page = try cursor.next(cursor.ptr, self.arena.allocator(), self.engine.context.limits.page_rows);
                            if (self.page.?.rows.len > self.engine.context.limits.page_rows) return error.InvalidSqlBackendResponse;
                            self.page_index = 0;
                            self.eof = self.page.?.after == null;
                        }
                        const row = self.page.?.rows[self.page_index];
                        self.page_index += 1;
                        self.engine.visited += 1;
                        if (self.engine.visited > self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                        const values = try alloc.alloc(Datum, self.node.columns.len);
                        for (scan.source_columns, self.node.columns, values) |name, column, *out| {
                            const cell = try row.cell(name);
                            out.* = .{ .value = try describe.coerce(cell.value, column.type), .sql_null = cell.sql_null };
                        }
                        break :blk values;
                    },
                    .join => |join| self.nextJoin(alloc, join),
                    .set => |set| self.nextSet(alloc, set),
                    .query => |query| blk: {
                        // Nonblocking nested queries are pipelines, not hidden
                        // materialization boundaries. Blocking sort/aggregate
                        // nodes retain their bounded operator-specific state.
                        if (query.binding.aggregate == null and query.binding.window == null and !query.statement.count_all and query.binding.order_keys.len == 0)
                            break :blk try self.nextQuery(alloc, query);
                        if (self.output == null) {
                            var adapter: Adapter = .{ .engine = self.engine, .iterator = self.left.?, .table = query.binding.table.? };
                            var context = self.engine.context;
                            context.backend = adapter.iface();
                            context.binding = query.binding;
                            context.binding.relation = null;
                            context.arena = self.arena.allocator();
                            context.limits.result_rows = context.limits.scan_rows;
                            self.output = try context.select(query.statement);
                        }
                        const output = self.output.?;
                        if (self.output_index >= output.rows.len) break :blk null;
                        const row = output.rows[self.output_index];
                        const sql_nulls = if (output.sql_nulls) |nulls| nulls[self.output_index] else null;
                        self.output_index += 1;
                        const values = try alloc.alloc(Datum, row.len);
                        for (row, self.node.columns, values, 0..) |value, column, *out, i| out.* = .{ .value = try describe.coerce(value, column.type), .sql_null = if (sql_nulls) |flags| flags[i] else value == .null };
                        break :blk values;
                    },
                };
            }

            fn nextQuery(self: *Iterator, alloc: Allocator, query: @FieldType(@FieldType(binding.Node, "operation"), "query")) anyerror!?[]const Datum {
                var context = self.engine.context;
                context.binding = query.binding;
                context.binding.relation = null;
                context.typed_output = true;
                if (self.query_fields == null) {
                    const fields = try self.arena.allocator().alloc([]const u8, query.statement.columns.len);
                    for (query.statement.columns, fields) |column, *field| field.* = if (column.expression != null) "" else column.field;
                    self.query_fields = fields;
                    self.query_skip = try context.count(query.statement.offset, 0);
                    self.query_remaining = try context.count(query.statement.limit, std.math.maxInt(usize));
                }
                if (self.query_remaining == 0) return null;
                const scratch = &self.scratch;
                _ = scratch.reset(.retain_capacity);
                while (try self.left.?.next(scratch.allocator())) |input| {
                    try self.engine.checkpoint();
                    var object: std.json.ObjectMap = .empty;
                    const nulls = try scratch.allocator().alloc(bool, input.len);
                    for (input, query.source.columns, nulls) |cell, column, *is_null| {
                        try object.put(scratch.allocator(), column.internal, cell.value);
                        is_null.* = cell.sql_null;
                    }
                    const row: catalog.Row = .{ .id = "", .version = 0, .value = .{ .object = object }, .sql_nulls = nulls };
                    const cells = try context.binding.scalars.cells(scratch.allocator(), row);
                    if (try context.binding.scalars.matches(scratch.allocator(), cells, context.parameters)) {
                        if (self.query_skip != 0) self.query_skip -= 1 else {
                            const values = try context.projectValues(scratch.allocator(), row, self.query_fields.?, cells);
                            const owned = try alloc.alloc(Datum, values.len);
                            for (values, owned) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                            self.query_remaining -= 1;
                            return owned;
                        }
                    }
                    _ = scratch.reset(.retain_capacity);
                }
                return null;
            }

            fn setValues(self: *Iterator, alloc: Allocator, values: []const Datum) ![]const Datum {
                const result = try alloc.alloc(Datum, values.len);
                for (values, result, self.node.columns) |value, *out, column| out.* = .{
                    .value = try describe.coerce(value.value, column.type),
                    .sql_null = value.sql_null,
                };
                return result;
            }
            fn setSlot(self: *Iterator, values: []const Datum, insert: bool) !?usize {
                var hasher = std.hash.Wyhash.init(0);
                for (values) |value| {
                    var bytes: [9]u8 = undefined;
                    bytes[0] = @intFromBool(value.sql_null);
                    std.mem.writeInt(u64, bytes[1..9], if (value.sql_null) 0 else try scalar.semanticHash(value.value), .little);
                    hasher.update(&bytes);
                }
                const hash = hasher.final();
                var cursor = self.set_heads.get(hash);
                while (cursor) |index| {
                    try self.engine.checkpoint();
                    const entry = self.set_entries.items[index];
                    var equal = true;
                    for (entry.values, values) |a, b| if (a.sql_null != b.sql_null or (!a.sql_null and try scalar.compare(a.value, b.value) != .eq)) {
                        equal = false;
                        break;
                    };
                    if (equal) return index;
                    cursor = entry.next;
                }
                if (!insert) return null;
                if (self.set_entries.items.len >= self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const owned = self.arena.allocator();
                const copied = try owned.alloc(Datum, values.len);
                for (values, copied) |value, *out| out.* = try operators.cloneDatum(owned, value);
                const index = self.set_entries.items.len;
                try self.set_entries.append(owned, .{ .values = copied, .count = 0, .next = self.set_heads.get(hash) });
                try self.set_heads.put(owned, hash, index);
                return index;
            }
            fn nextSet(self: *Iterator, alloc: Allocator, set: @FieldType(@FieldType(binding.Node, "operation"), "set")) anyerror!?[]const Datum {
                var scratch = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                defer scratch.deinit();
                if (!self.set_ready) {
                    if (set.kind != .@"union") {
                        while (try self.right.?.next(scratch.allocator())) |values| {
                            const normalized = try self.setValues(scratch.allocator(), values);
                            const index = (try self.setSlot(normalized, true)).?;
                            self.set_entries.items[index].count += 1;
                            _ = scratch.reset(.free_all);
                        }
                        // Retain only distinct keys/counts after the build side
                        // is consumed, not its materialized projection pages.
                        self.right.?.deinit();
                        self.right = null;
                    }
                    self.set_ready = true;
                }
                while (true) {
                    _ = scratch.reset(.free_all);
                    const source = if (self.eof) self.right.? else self.left.?;
                    const input = try source.next(scratch.allocator()) orelse {
                        if (!self.eof and set.kind == .@"union") {
                            self.left.?.deinit();
                            self.left = null;
                            self.eof = true;
                            continue;
                        }
                        return null;
                    };
                    const values = try self.setValues(scratch.allocator(), input);
                    var emit = false;
                    if (set.kind == .@"union" and set.all) {
                        emit = true;
                    } else if (set.kind == .@"union" or (set.kind == .except and !set.all)) {
                        const index = (try self.setSlot(values, true)).?;
                        const entry = &self.set_entries.items[index];
                        emit = entry.count == 0;
                        entry.count = 1;
                    } else if (try self.setSlot(values, false)) |index| {
                        const entry = &self.set_entries.items[index];
                        emit = if (set.kind == .intersect) entry.count != 0 else entry.count == 0;
                        if (entry.count != 0) entry.count = if (set.all) entry.count - 1 else 0;
                    } else emit = set.kind == .except;
                    if (emit) {
                        const result = try alloc.alloc(Datum, values.len);
                        for (values, result) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                        return result;
                    }
                }
            }

            fn keys(self: *Iterator, alloc: Allocator, programs: []const scalar.Program, values: []const Datum) ![]const Datum {
                const result = try alloc.alloc(Datum, programs.len);
                for (programs, result) |program, *out| out.* = try program.evaluate(alloc, values, self.engine.context.parameters, .{});
                return result;
            }
            fn combine(self: *Iterator, alloc: Allocator, left: ?[]const Datum, right: ?[]const Datum) ![]const Datum {
                const width = self.node.operation.join.left.columns.len;
                const values = try alloc.alloc(Datum, self.node.columns.len);
                @memset(values, .{});
                if (left) |cells| @memcpy(values[0..width], cells);
                if (right) |cells| @memcpy(values[width..], cells);
                return values;
            }
            fn nextJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                var candidate_arena = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                defer candidate_arena.deinit();
                if (self.hash_join == null) {
                    self.hash_join = try operators.HashJoin.create(self.engine.context.alloc, .{ .rows = self.engine.context.limits.scan_rows, .bytes = self.engine.context.limits.retained_bytes });
                    var scratch = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                    defer scratch.deinit();
                    while (try self.right.?.next(scratch.allocator())) |values| {
                        const key_values = try self.keys(scratch.allocator(), join.right_keys, values);
                        try self.hash_join.?.add(values, key_values);
                        _ = scratch.reset(.free_all);
                    }
                }
                while (true) {
                    try self.engine.checkpoint();
                    if (self.probe) |*probe| {
                        while (try probe.next()) |match| {
                            try self.engine.checkpoint();
                            _ = candidate_arena.reset(.free_all);
                            const values = try self.combine(candidate_arena.allocator(), self.left_values, match.values);
                            if (join.condition) |program| {
                                const accepted = try program.evaluate(candidate_arena.allocator(), values, self.engine.context.parameters, .{});
                                if (accepted.sql_null) continue;
                                if (accepted.value != .bool) return error.SqlTypeMismatch;
                                if (!accepted.value.bool) continue;
                            }
                            self.left_matched = true;
                            self.hash_join.?.markMatched(match.index);
                            return try alloc.dupe(Datum, values);
                        }
                        self.probe = null;
                        if (!self.left_matched and (join.kind == .left or join.kind == .full)) return try self.combine(alloc, self.left_values, null);
                    }
                    if (self.eof) {
                        if (join.kind == .right or join.kind == .full) {
                            if (self.hash_join.?.unmatched(&self.unmatched_index)) |match| return try self.combine(alloc, null, match.values);
                        }
                        return null;
                    }
                    _ = self.arena.reset(.free_all);
                    const left = try self.left.?.next(self.arena.allocator()) orelse {
                        self.eof = true;
                        continue;
                    };
                    const owned = try self.arena.allocator().alloc(Datum, left.len);
                    for (left, owned) |value, *out| out.* = try operators.cloneDatum(self.arena.allocator(), value);
                    self.left_values = owned;
                    self.left_matched = false;
                    self.probe = try self.hash_join.?.probe(try self.keys(self.arena.allocator(), join.left_keys, owned));
                }
            }
        };

        const Adapter = struct {
            engine: *Self,
            iterator: *Iterator,
            table: catalog.Table,
            ordinal: u64 = 0,
            opened: bool = false,

            fn iface(self: *Adapter) catalog.Backend {
                return .{ .ptr = self, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = Adapter.checkpoint } };
            }
            fn resolve(ptr: *anyopaque, _: Allocator, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                return self.table;
            }
            fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
                return error.InvalidSqlBackendResponse;
            }
            fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
                return error.UnsupportedSqlExecution;
            }
            fn checkpoint(ptr: *anyopaque) !void {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                try self.engine.checkpoint();
            }
            fn open(ptr: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                if (self.opened) return error.InvalidSqlBackendResponse;
                self.opened = true;
                return .{ .ptr = self, .next = next, .close = close };
            }
            fn close(_: *anyopaque) void {}
            fn next(ptr: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                var rows: std.ArrayList(catalog.Row) = .empty;
                while (rows.items.len < limit) {
                    const values = try self.iterator.next(alloc) orelse break;
                    var object: std.json.ObjectMap = .empty;
                    const nulls = try alloc.alloc(bool, values.len);
                    for (values, self.iterator.node.columns, nulls) |value, column, *sql_null| {
                        const owned = try operators.cloneDatum(alloc, value);
                        try object.put(alloc, column.internal, owned.value);
                        sql_null.* = owned.sql_null;
                    }
                    self.ordinal += 1;
                    try rows.append(alloc, .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{self.ordinal}), .version = 0, .value = .{ .object = object }, .sql_nulls = nulls });
                }
                const more = rows.items.len == limit;
                return .{ .rows = try rows.toOwnedSlice(alloc), .after = if (more) try std.fmt.allocPrint(alloc, "{d}", .{self.ordinal}) else null };
            }
        };
    };
}

fn Source(comptime Context: type) type {
    return struct {
        const Self = @This();
        alloc: Allocator,
        read: ?catalog.StatementRead = null,
        single: ?catalog.Cursor = null,
        single_list: [1]catalog.Cursor = undefined,
        engine: Engine(Context),
        iterator: ?*Engine(Context).Iterator = null,
        adapter: Engine(Context).Adapter = undefined,

        fn create(context: Context) !*Self {
            const relation = context.binding.relation orelse return error.InvalidSqlBackendResponse;
            const self = try context.alloc.create(Self);
            self.* = .{ .alloc = context.alloc, .engine = .{ .context = context, .cursors = &.{} } };
            errdefer self.close();
            if (relation.scans.len != 0) {
                if (context.backend.vtable.open_statement) |open| {
                    self.read = try open(context.backend.ptr, context.alloc, relation.scans);
                    self.engine.cursors = self.read.?.cursors;
                    if (self.engine.cursors.len != relation.scans.len) return error.InvalidSqlBackendResponse;
                } else if (relation.scans.len == 1) {
                    const open = context.backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
                    self.single = (try open(context.backend.ptr, context.alloc, relation.scans[0].table, relation.scans[0].request)) orelse return error.SqlStatementSnapshotRequired;
                    self.single_list[0] = self.single.?;
                    self.engine.cursors = &self.single_list;
                } else return error.SqlStatementSnapshotRequired;
            }
            self.iterator = try Engine(Context).Iterator.create(&self.engine, relation.root);
            self.adapter = .{ .engine = &self.engine, .iterator = self.iterator.?, .table = relation.table };
            return self;
        }
        fn close(self: *Self) void {
            if (self.iterator) |iterator| iterator.deinit();
            if (self.read) |read| read.close(read.ptr);
            if (self.single) |cursor| cursor.close(cursor.ptr);
            self.alloc.destroy(self);
        }
        fn next(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
            const self: *Self = @ptrCast(@alignCast(raw));
            return Engine(Context).Adapter.next(&self.adapter, alloc, limit);
        }
        fn closeCursor(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            self.close();
        }
    };
}

pub fn openCursor(context: anytype) !catalog.Cursor {
    const owner = try Source(@TypeOf(context)).create(context);
    return .{ .ptr = owner, .next = @TypeOf(owner.*).next, .close = @TypeOf(owner.*).closeCursor };
}

pub fn execute(context: anytype) anyerror!@import("runtime.zig").Output {
    const owner = try Source(@TypeOf(context)).create(context);
    defer owner.close();
    var lowered = context;
    lowered.backend = owner.adapter.iface();
    lowered.binding.relation = null;
    return lowered.select(context.binding.relation.?.statement);
}
