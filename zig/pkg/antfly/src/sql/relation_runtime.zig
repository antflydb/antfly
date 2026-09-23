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
const Worklist = @import("recursive_worklist.zig").Worklist;

fn dependsOn(node: *const binding.Node, id: usize) bool {
    return switch (node.operation) {
        .recursive_ref => |reference| reference == id,
        .materialized_ref => |source| dependsOn(source, id),
        .query => |query| dependsOn(query.source, id),
        .join => |join| dependsOn(join.left, id) or dependsOn(join.right, id),
        .set => |set| dependsOn(set.left, id) or dependsOn(set.right, id),
        .values => |arms| for (arms) |arm| {
            if (dependsOn(arm, id)) break true;
        } else false,
        else => false,
    };
}

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
        cache_arena: std.heap.ArenaAllocator,
        static_rows: std.AutoHashMapUnmanaged(*const binding.Node, []const []const Datum) = .empty,
        static_hashes: std.AutoHashMapUnmanaged(*const binding.Node, *operators.HashJoin) = .empty,
        recursions: [32]?*Worklist = @splat(null),

        pub fn checkpoint(self: *Self) !void {
            try self.context.checkpoint();
            self.work += 1;
            if (self.work > self.context.limits.scan_rows *| 64) return error.SqlProgramLimitExceeded;
        }

        fn deinit(self: *Self) void {
            for (self.recursions) |optional| if (optional) |worklist| worklist.deinit();
            var hashes = self.static_hashes.valueIterator();
            while (hashes.next()) |join| join.*.deinit();
            self.cache_arena.deinit();
        }

        fn staticRows(self: *Self, node: *const binding.Node) anyerror![]const []const Datum {
            if (self.static_rows.get(node)) |rows| return rows;
            const owned = self.cache_arena.allocator();
            const iterator = try Iterator.create(self, node);
            defer iterator.deinit();
            var scratch = std.heap.ArenaAllocator.init(self.context.alloc);
            defer scratch.deinit();
            var rows: std.ArrayList([]const Datum) = .empty;
            while (try iterator.next(scratch.allocator())) |values| {
                if (rows.items.len >= self.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                const cells = try owned.alloc(Datum, values.len);
                for (values, cells) |value, *out| out.* = try operators.cloneDatum(owned, value);
                try rows.append(owned, cells);
                _ = scratch.reset(.free_all);
            }
            const result = try rows.toOwnedSlice(owned);
            try self.static_rows.put(owned, node, result);
            return result;
        }

        fn recursive(self: *Self, node: *const binding.Node) anyerror!*Worklist {
            const plan = node.operation.recursive;
            if (self.recursions[plan.id]) |state| {
                if (!state.complete) return error.UnsupportedSqlShape;
                return state;
            }
            const state = try self.cache_arena.allocator().create(Worklist);
            state.* = .init(self.context.alloc, plan.all);
            self.recursions[plan.id] = state;
            var scratch = std.heap.ArenaAllocator.init(self.context.alloc);
            defer scratch.deinit();
            {
                const seed = try Iterator.create(self, plan.seed);
                defer seed.deinit();
                while (try seed.next(scratch.allocator())) |values| {
                    const normalized = try normalize(scratch.allocator(), node.columns, values);
                    try state.append(self, normalized, self.context.limits.scan_rows);
                    _ = scratch.reset(.free_all);
                }
            }
            state.end = state.rows.items.len;
            while (state.begin != state.end) {
                try self.checkpoint();
                const step = try Iterator.createRecursive(self, plan.step, plan.id);
                defer step.deinit();
                while (try step.next(scratch.allocator())) |values| {
                    const normalized = try normalize(scratch.allocator(), node.columns, values);
                    try state.append(self, normalized, self.context.limits.scan_rows);
                    _ = scratch.reset(.free_all);
                }
                state.begin = state.end;
                state.end = state.rows.items.len;
            }
            state.complete = true;
            return state;
        }

        fn normalize(alloc: Allocator, columns: []const binding.Column, values: []const Datum) ![]const Datum {
            if (columns.len != values.len) return error.SqlTypeMismatch;
            const result = try alloc.alloc(Datum, values.len);
            for (values, columns, result) |value, column, *out| out.* = .{ .value = try describe.coerce(value.value, column.type), .sql_null = value.sql_null };
            return result;
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
            values_leaves: []const *const binding.Node = &.{},
            values_leaf_index: usize = 0,
            values_leaf: ?*Iterator = null,
            values_leaf_coerce: bool = false,
            recursive_id: ?usize = null,
            cached_rows: ?[]const []const Datum = null,
            borrowed_hash: bool = false,
            flipped_join: bool = false,

            const SetEntry = struct { values: []const Datum, count: usize, next: ?usize };

            fn create(engine: *Self, node: *const binding.Node) anyerror!*Iterator {
                return createRecursive(engine, node, null);
            }
            fn createRecursive(engine: *Self, node: *const binding.Node, recursive_id: ?usize) anyerror!*Iterator {
                const alloc = engine.context.alloc;
                const self = try alloc.create(Iterator);
                self.* = .{ .engine = engine, .node = node, .arena = .init(alloc), .scratch = .init(alloc), .recursive_id = recursive_id };
                errdefer self.deinit();
                if (recursive_id) |id| if (!dependsOn(node, id)) {
                    self.cached_rows = try engine.staticRows(node);
                    return self;
                };
                switch (node.operation) {
                    .materialized_ref => |source| self.cached_rows = try engine.staticRows(source),
                    .join => |join| {
                        // Always probe the delta and build the invariant side.
                        // Keep output ordinals in the original SQL FROM order.
                        self.flipped_join = if (recursive_id) |id| !dependsOn(join.left, id) and dependsOn(join.right, id) else false;
                        self.left = try createRecursive(engine, if (self.flipped_join) join.right else join.left, recursive_id);
                        self.right = try createRecursive(engine, if (self.flipped_join) join.left else join.right, recursive_id);
                    },
                    .query => |query| self.left = try createRecursive(engine, query.source, recursive_id),
                    .set => |set| {
                        self.left = try createRecursive(engine, set.left, recursive_id);
                        self.right = try createRecursive(engine, set.right, recursive_id);
                    },
                    .values => |arms| self.values_leaves = arms,
                    else => {},
                }
                return self;
            }
            fn deinit(self: *Iterator) void {
                if (self.page) |page| page.deinit();
                if (self.left) |left| left.deinit();
                if (self.right) |right| right.deinit();
                if (self.values_leaf) |leaf| leaf.deinit();
                if (!self.borrowed_hash) if (self.hash_join) |join| join.deinit();
                self.arena.deinit();
                self.scratch.deinit();
                self.engine.context.alloc.destroy(self);
            }
            fn next(self: *Iterator, alloc: Allocator) anyerror!?[]const Datum {
                try self.engine.checkpoint();
                if (self.cached_rows) |rows| {
                    if (self.output_index == rows.len) return null;
                    const row = rows[self.output_index];
                    self.output_index += 1;
                    return row;
                }
                return switch (self.node.operation) {
                    .materialized_ref => unreachable,
                    .recursive => blk: {
                        const state = try self.engine.recursive(self.node);
                        if (self.output_index == state.rows.items.len) break :blk null;
                        const values = state.rows.items[self.output_index].values;
                        self.output_index += 1;
                        break :blk values;
                    },
                    .recursive_ref => |id| blk: {
                        const state = self.engine.recursions[id] orelse return error.InvalidSqlBackendResponse;
                        if (state.begin + self.output_index == state.end) break :blk null;
                        const values = state.rows.items[state.begin + self.output_index].values;
                        self.output_index += 1;
                        break :blk values;
                    },
                    .singleton => if (self.emitted) null else blk: {
                        self.emitted = true;
                        break :blk &.{};
                    },
                    .literal_rows => |rows| blk: {
                        if (self.output_index == rows.len) break :blk null;
                        const values = try normalize(alloc, self.node.columns, rows[self.output_index]);
                        self.output_index += 1;
                        break :blk values;
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
                            const cell = try @import("joined_mutation.zig").cell(alloc, row, name);
                            out.* = .{ .value = try describe.coerce(cell.value, column.type), .sql_null = cell.sql_null };
                        }
                        break :blk values;
                    },
                    .join => |join| self.nextJoin(alloc, join),
                    .set => |set| self.nextSet(alloc, set),
                    .values => self.nextValues(alloc),
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
            fn nextValues(self: *Iterator, alloc: Allocator) anyerror!?[]const Datum {
                while (self.values_leaf_index < self.values_leaves.len) {
                    if (self.values_leaf == null) {
                        const leaf = self.values_leaves[self.values_leaf_index];
                        if (leaf.columns.len != self.node.columns.len) return error.InvalidSqlBackendResponse;
                        self.values_leaf_coerce = false;
                        for (leaf.columns, self.node.columns) |source, target| {
                            if (source.type != target.type) self.values_leaf_coerce = true;
                        }
                        self.values_leaf = try createRecursive(self.engine, leaf, self.recursive_id);
                    }
                    if (try self.values_leaf.?.next(alloc)) |values| {
                        if (values.len != self.node.columns.len) return error.InvalidSqlBackendResponse;
                        return if (self.values_leaf_coerce) try self.setValues(alloc, values) else values;
                    }
                    self.values_leaf.?.deinit();
                    self.values_leaf = null;
                    self.values_leaf_index += 1;
                }
                return null;
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
                if (if (self.flipped_join) right else left) |cells| @memcpy(values[0..width], cells);
                if (if (self.flipped_join) left else right) |cells| @memcpy(values[width..], cells);
                return values;
            }
            fn nextJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                var candidate_arena = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                defer candidate_arena.deinit();
                const kind = if (self.flipped_join and join.kind == .right) .left else join.kind;
                const shared = self.recursive_id != null;
                if (self.hash_join == null and shared) if (self.engine.static_hashes.get(self.node)) |cached| {
                    self.hash_join = cached;
                    self.borrowed_hash = true;
                };
                if (self.hash_join == null) {
                    self.hash_join = try operators.HashJoin.create(self.engine.context.alloc, .{ .rows = self.engine.context.limits.scan_rows, .bytes = self.engine.context.limits.retained_bytes });
                    var scratch = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                    defer scratch.deinit();
                    while (try self.right.?.next(scratch.allocator())) |values| {
                        const key_values = try self.keys(scratch.allocator(), if (self.flipped_join) join.left_keys else join.right_keys, values);
                        try self.hash_join.?.add(values, key_values);
                        _ = scratch.reset(.free_all);
                    }
                    if (shared) {
                        try self.engine.static_hashes.put(self.engine.cache_arena.allocator(), self.node, self.hash_join.?);
                        self.borrowed_hash = true;
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
                        if (!self.left_matched and (kind == .left or kind == .full)) return try self.combine(alloc, self.left_values, null);
                    }
                    if (self.eof) {
                        if (kind == .right or kind == .full) {
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
                    self.probe = try self.hash_join.?.probe(try self.keys(self.arena.allocator(), if (self.flipped_join) join.right_keys else join.left_keys, owned));
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
            self.* = .{ .alloc = context.alloc, .engine = .{ .context = context, .cursors = &.{}, .cache_arena = .init(context.alloc) } };
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
            self.engine.deinit();
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
