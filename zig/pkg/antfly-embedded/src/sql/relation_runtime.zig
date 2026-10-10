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
    // SafeAllocator can grow the last bucket allocation depending on prior
    // tests. Force allocate/copy growth so every OOM run visits the same sites,
    // while retaining the backing allocator's leak and ownership checks.
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
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

        pub fn deinit(self: *Self) void {
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
            for (values, columns, result) |value, column, *out| out.* = .{ .value = try describe.coerceAlloc(alloc, value.value, column.type), .sql_null = value.sql_null };
            return result;
        }

        fn estimate(self: *Self, node: *const binding.Node) ?u64 {
            return switch (node.operation) {
                .scan => |scan| self.cursors[scan.index].estimated_rows,
                .singleton => 1,
                .literal_rows => |rows| rows.len,
                .query => |query| blk: {
                    const source = self.estimate(query.source);
                    if (query.statement.limit) |limit| {
                        const count = self.context.count(limit, 0) catch break :blk source;
                        break :blk if (source) |rows| @min(rows, count) else count;
                    }
                    break :blk source;
                },
                .materialized_ref => |source| self.estimate(source),
                else => null,
            };
        }
        fn preferLeftBuild(self: *Self, left: *const binding.Node, right: *const binding.Node) bool {
            if (self.estimate(left)) |l| if (self.estimate(right)) |r| return l < r;
            // Compressed source bytes are a secondary estimate, used only when
            // neither side provides snapshot-local cardinality information.
            if (left.operation == .scan and right.operation == .scan) {
                const l = self.cursors[left.operation.scan.index].estimated_bytes orelse return false;
                const r = self.cursors[right.operation.scan.index].estimated_bytes orelse return false;
                return l < r;
            }
            return false;
        }

        const Iterator = struct {
            engine: *Self,
            node: *const binding.Node,
            arena: std.heap.ArenaAllocator,
            scratch: std.heap.ArenaAllocator,
            left: ?*Iterator = null,
            right: ?*Iterator = null,
            page: ?catalog.Page = null,
            column_page: ?catalog.ColumnPage = null,
            page_index: usize = 0,
            pages: usize = 0,
            eof: bool = false,
            emitted: bool = false,
            hash_join: ?*operators.HashJoin = null,
            partition_join: ?*@import("partition_join.zig").Join = null,
            join_view: ?@import("parallel_output.zig").Pipe.View = null,
            probe: ?operators.HashJoin.Probe = null,
            probe_arena: std.heap.ArenaAllocator,
            probe_payload: @import("execution_batch.zig").Batch = .{ .rows = &.{} },
            probe_batch: []operators.HashJoin.Probe = &.{},
            probe_index: usize = 0,
            probe_errors: []?anyerror = &.{},
            probe_input_error: ?anyerror = null,
            probe_source_exhausted: bool = false,
            scan_filter: ?*@import("dynamic_filter.zig").Filter = null,
            left_values: ?[]const Datum = null,
            left_matched: bool = false,
            unmatched_index: usize = 0,
            output: ?@import("runtime.zig").Output = null,
            output_index: usize = 0,
            result_cursor: ?*@import("result_cursor.zig").Cursor = null,
            query_fields: ?[]const []const u8 = null,
            query_skip: usize = 0,
            query_remaining: usize = 0,
            query_buffer: []const []const Datum = &.{},
            query_buffer_index: usize = 0,
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
                self.* = .{ .engine = engine, .node = node, .arena = .init(alloc), .scratch = .init(alloc), .probe_arena = .init(alloc), .recursive_id = recursive_id };
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
                        self.flipped_join = if (recursive_id) |id| !dependsOn(join.left, id) and dependsOn(join.right, id) else engine.preferLeftBuild(join.left, join.right);
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
            pub fn deinit(self: *Iterator) void {
                if (self.result_cursor) |cursor| cursor.close();
                if (self.page) |page| page.deinit();
                if (self.left) |left| left.deinit();
                if (self.right) |right| right.deinit();
                if (self.values_leaf) |leaf| leaf.deinit();
                if (self.join_view) |view| view.deinit();
                if (self.partition_join) |join| join.close();
                if (self.scan_filter) |filter| filter.close();
                if (!self.borrowed_hash) if (self.hash_join) |join| join.deinit();
                self.probe_arena.deinit();
                self.arena.deinit();
                self.scratch.deinit();
                self.engine.context.alloc.destroy(self);
            }
            fn nextBatch(self: *Iterator, a: Allocator, maximum: usize, failure: ?*?anyerror) anyerror!@import("execution_batch.zig").Batch {
                try self.engine.checkpoint();
                if (self.join_view) |view| {
                    view.deinit();
                    self.join_view = null;
                }
                if (self.partition_join) |join| if (try join.nextTypedBatch(maximum)) |view| {
                    self.join_view = view;
                    const source = try a.create(@import("execution_batch.zig").Batch);
                    source.* = view.values();
                    if (!self.flipped_join) return source.*;
                    const ordinals = try a.alloc(usize, self.node.columns.len);
                    const kinds = try a.alloc(@import("ast.zig").ColumnType, self.node.columns.len);
                    const selection = try a.alloc(usize, view.count);
                    const width = self.node.operation.join.left.columns.len;
                    for (ordinals, kinds, self.node.columns, 0..) |*ordinal, *kind, column, index| {
                        ordinal.* = if (index < width) self.node.operation.join.right.columns.len + index else index - width;
                        kind.* = column.type;
                    }
                    for (selection, 0..) |*index, i| index.* = i;
                    return .{ .mapped = .{ .source = source, .ordinals = ordinals, .kinds = kinds, .selection = selection } };
                };
                if (self.cached_rows == null and self.left != null and self.node.operation == .query) {
                    const query = self.node.operation.query;
                    const decisions = @import("decision_eval.zig");
                    const eligible = blk: {
                        if (query.binding.aggregate != null or query.binding.window != null or query.statement.count_all or query.binding.order_keys.len != 0) break :blk false;
                        if (query.binding.scalars.predicate) |*program| if (decisions.hasExternal(program)) break :blk false;
                        for (query.binding.scalars.projections) |*optional| if (optional.*) |*program| if (decisions.hasExternal(program)) break :blk false;
                        break :blk true;
                    };
                    if (eligible) return self.nextQueryBatch(a, maximum, query, failure);
                }
                if (self.cached_rows == null and self.node.operation == .scan) {
                    const scan = self.node.operation.scan;
                    const cursor = self.engine.cursors[scan.index];
                    if (cursor.next_columns) |pull| {
                        if (self.eof) return .{ .rows = &.{} };
                        // Never advance a producer while a consumer borrows its page.
                        self.column_page = null;
                        _ = self.arena.reset(.free_all);
                        self.pages += 1;
                        if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                        const page = try pull(cursor.ptr, self.arena.allocator(), @intCast(maximum));
                        try page.validate();
                        if (page.selection.len > maximum or page.selection.len > self.engine.context.limits.scan_rows -| self.engine.visited) return error.SqlProgramLimitExceeded;
                        self.engine.visited += page.selection.len;
                        self.eof = page.after == null;
                        const definitions = try a.alloc(scalar.Column, self.node.columns.len);
                        for (definitions, scan.source_columns, self.node.columns) |*definition, name, column| definition.* = .{ .name = name, .type = column.type };
                        return .{ .columns = .{ .page = page, .definitions = definitions } };
                    }
                }
                var rows: std.ArrayList([]const Datum) = .empty;
                var retained: usize = 0;
                while (rows.items.len < maximum) {
                    const values = (self.next(a) catch |err| blk: {
                        if (failure) |out| {
                            out.* = err;
                            break :blk null;
                        }
                        return err;
                    }) orelse break;
                    const owned = try a.alloc(Datum, values.len);
                    for (values, owned) |value, *out| {
                        out.* = try operators.cloneDatum(a, value);
                        retained +|= try operators.datumBytes(value);
                    }
                    try rows.append(a, owned);
                    if (retained >= self.engine.context.limits.retained_bytes / 32) break;
                }
                return .{ .rows = rows.items };
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
                        const cursor = self.engine.cursors[scan.index];
                        if (cursor.next_columns) |pull| {
                            while (self.column_page == null or self.page_index == self.column_page.?.selection.len) {
                                if (self.eof) break :blk null;
                                self.column_page = null;
                                _ = self.arena.reset(.free_all);
                                self.pages += 1;
                                if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                                const wanted: u32 = @intCast(@min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.node.columns.len * @sizeOf(Datum) * 16))));
                                self.column_page = try pull(cursor.ptr, self.arena.allocator(), wanted);
                                try self.column_page.?.validate();
                                if (self.column_page.?.selection.len > wanted) return error.InvalidSqlBackendResponse;
                                self.page_index = 0;
                                self.eof = self.column_page.?.after == null;
                            }
                            const page = self.column_page.?;
                            const index = self.page_index;
                            self.page_index += 1;
                            self.engine.visited += 1;
                            if (self.engine.visited > self.engine.context.limits.scan_rows) return error.SqlProgramLimitExceeded;
                            const values = try alloc.alloc(Datum, self.node.columns.len);
                            for (scan.source_columns, self.node.columns, values) |name, column, *out| {
                                const cell = try page.cell(alloc, index, name);
                                // Values borrow the current page until the next pull.
                                // Retaining operators own typed copies at admission.
                                out.* = .{ .value = try describe.coerceAlloc(alloc, cell.value, column.type), .sql_null = cell.sql_null, .patterns = cell.patterns };
                            }
                            break :blk values;
                        }
                        while (self.page == null or self.page_index == self.page.?.rows.len) {
                            if (self.eof) break :blk null;
                            if (self.page) |page| page.deinit();
                            self.page = null;
                            _ = self.arena.reset(.free_all);
                            self.pages += 1;
                            if (self.pages > self.engine.context.limits.scan_pages) return error.SqlProgramLimitExceeded;
                            self.page = try cursor.next(cursor.ptr, self.arena.allocator(), @intCast(@min(self.engine.context.limits.page_rows, @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.node.columns.len * @sizeOf(Datum) * 16)))));
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
                            out.* = .{ .value = try describe.coerceAlloc(alloc, cell.value, column.type), .sql_null = cell.sql_null, .patterns = cell.patterns };
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
                        if (self.result_cursor) |cursor| break :blk try cursor.next(alloc);
                        if (self.output == null) {
                            var adapter: Adapter = .{ .engine = self.engine, .iterator = self.left.?, .table = query.binding.table.? };
                            var context = self.engine.context;
                            context.backend = adapter.iface();
                            context.sink = null;
                            context.binding = query.binding;
                            context.binding.relation = null;
                            context.arena = self.arena.allocator();
                            context.limits.result_rows = context.limits.scan_rows;
                            if (context.spill) |manager| {
                                const Cursor = @import("result_cursor.zig").Cursor;
                                const cursor = try Cursor.create(context.alloc, manager, self.node.columns.len);
                                errdefer cursor.close();
                                context.typed_output = true;
                                context.sink = .{ .ptr = cursor, .append = Cursor.append, .take_sorted = Cursor.takeSorted };
                                const output = try context.select(query.statement);
                                // Constant/count paths can return a small ordinary result.
                                var scratch = std.heap.ArenaAllocator.init(context.alloc);
                                defer scratch.deinit();
                                for (output.rows, 0..) |row, index| {
                                    _ = scratch.reset(.free_all);
                                    const values = try scratch.allocator().alloc(Datum, row.len);
                                    for (row, values, 0..) |value, *cell, column| cell.* = .{
                                        .value = value,
                                        .sql_null = if (output.sql_nulls) |flags| flags[index][column] else value == .null,
                                        .patterns = if (output.pattern_sources) |sources| sources[index][column] else null,
                                    };
                                    try Cursor.append(cursor, values);
                                }
                                const first = try cursor.next(alloc);
                                self.result_cursor = cursor;
                                break :blk first;
                            }
                            self.output = try context.select(query.statement);
                        }
                        const output = self.output.?;
                        if (self.output_index >= output.rows.len) break :blk null;
                        const row = output.rows[self.output_index];
                        const sql_nulls = if (output.sql_nulls) |nulls| nulls[self.output_index] else null;
                        self.output_index += 1;
                        const values = try alloc.alloc(Datum, row.len);
                        for (row, self.node.columns, values, 0..) |value, column, *out, i| out.* = .{ .value = try describe.coerceAlloc(alloc, value, column.type), .sql_null = if (sql_nulls) |flags| flags[i] else value == .null, .patterns = if (output.pattern_sources) |sources| sources[self.output_index - 1][i] else null };
                        break :blk values;
                    },
                };
            }

            fn batchProgram(self: *Iterator, a: Allocator, context: @TypeOf(self.engine.context), program: *const scalar.Program, batch: @import("execution_batch.zig").Batch, errors: []?anyerror) ![]const Datum {
                const vector = @import("vector_eval.zig").evaluateBatch(a, program, batch, context.parameters) catch |err| switch (err) {
                    error.SqlDivisionByZero, error.SqlNumericOutOfRange, error.SqlTypeMismatch, error.InvalidSqlDateTime, error.SqlCardinalityViolation => null,
                    else => return err,
                };
                if (vector) |values| return values;
                const values = try a.alloc(Datum, batch.len());
                for (values, 0..) |*value, index| {
                    value.* = context.evaluate(a, program.*, try batch.row(a, index)) catch |err| blk: {
                        if (err == error.OutOfMemory or err == error.QueryCanceled or err == error.DeadlineExceeded) return err;
                        errors[index] = errors[index] orelse err;
                        break :blk .{};
                    };
                }
                return values;
            }
            fn nextQueryBatch(self: *Iterator, _: Allocator, maximum: usize, query: @FieldType(@FieldType(binding.Node, "operation"), "query"), failure: ?*?anyerror) anyerror!@import("execution_batch.zig").Batch {
                const Batch = @import("execution_batch.zig").Batch;
                var context = self.engine.context;
                context.binding = query.binding;
                context.binding.relation = null;
                if (self.query_fields == null) {
                    const fields = try self.arena.allocator().alloc([]const u8, query.statement.columns.len);
                    for (query.statement.columns, fields) |column, *field| field.* = if (column.expression != null) "" else column.field;
                    self.query_fields = fields;
                    self.query_skip = try context.count(query.statement.offset, 0);
                    self.query_remaining = try context.count(query.statement.limit, std.math.maxInt(usize));
                }
                while (self.query_remaining != 0) {
                    _ = self.scratch.reset(.free_all);
                    const a = self.scratch.allocator();
                    const input = try a.create(Batch);
                    var upstream_error: ?anyerror = null;
                    input.* = try self.left.?.nextBatch(a, @min(maximum, self.query_remaining +| self.query_skip), &upstream_error);
                    const errors = try a.alloc(?anyerror, input.len());
                    @memset(errors, null);
                    if (input.len() == 0) {
                        if (upstream_error) |err| {
                            if (failure) |out| out.* = err else return err;
                        }
                        return .{ .rows = &.{} };
                    }
                    const ordinals = try a.alloc(usize, context.binding.scalars.columns.len);
                    const kinds = try a.alloc(@import("ast.zig").ColumnType, ordinals.len);
                    for (context.binding.scalars.columns, ordinals, kinds) |definition, *ordinal, *kind| {
                        ordinal.* = for (query.source.columns, 0..) |column, index| {
                            if (std.mem.eql(u8, column.internal, definition.name)) break index;
                        } else std.math.maxInt(usize);
                        kind.* = definition.type;
                    }
                    const identity = try a.alloc(usize, input.len());
                    for (identity, 0..) |*index, value| index.* = value;
                    var bound: Batch = .{ .mapped = .{ .source = input, .ordinals = ordinals, .kinds = kinds, .selection = identity } };
                    const predicates = if (context.binding.scalars.predicate) |*program| try self.batchProgram(a, context, program, bound, errors) else null;
                    var selected: std.ArrayList(usize) = .empty;
                    for (0..input.len()) |index| {
                        if (selected.items.len == self.query_remaining) {
                            upstream_error = null;
                            break;
                        }
                        if (errors[index]) |err| {
                            upstream_error = err;
                            break;
                        }
                        if (predicates) |values| {
                            if (values[index].sql_null) continue;
                            if (values[index].value != .bool) {
                                upstream_error = error.SqlTypeMismatch;
                                break;
                            }
                            if (!values[index].value.bool) continue;
                        }
                        if (self.query_skip != 0) {
                            self.query_skip -= 1;
                            continue;
                        }
                        try selected.append(a, index);
                    }
                    if (selected.items.len == 0) {
                        if (upstream_error) |err| {
                            if (failure) |out| out.* = err else return err;
                            return .{ .rows = &.{} };
                        }
                        continue;
                    }
                    bound.mapped.selection = selected.items;
                    const output_errors = try a.alloc(?anyerror, selected.items.len);
                    @memset(output_errors, null);
                    const columns = try a.alloc([]const Datum, self.query_fields.?.len);
                    for (self.query_fields.?, context.binding.columns, columns, 0..) |field, definition, *values, column| {
                        const projected = if (column < context.binding.scalars.projections.len) context.binding.scalars.projections[column] else null;
                        if (projected) |*program| {
                            values.* = try self.batchProgram(a, context, program, bound, output_errors);
                        } else {
                            const ordinal = for (query.source.columns, 0..) |source, index| {
                                if (std.mem.eql(u8, source.internal, field)) break index;
                            } else return error.InvalidSqlBackendResponse;
                            const vector = try a.alloc(Datum, selected.items.len);
                            for (selected.items, vector) |index, *value| value.* = try input.cell(a, index, ordinal);
                            values.* = vector;
                        }
                        for (@constCast(values.*), output_errors) |*value, *err| value.value = describe.coerceAlloc(a, value.value, definition.type) catch |cause| blk: {
                            if (cause == error.OutOfMemory) return cause;
                            err.* = err.* orelse cause;
                            break :blk .null;
                        };
                    }
                    var prefix = selected.items.len;
                    for (output_errors, 0..) |cause, index| if (cause) |err| {
                        prefix = index;
                        upstream_error = err;
                        break;
                    };
                    if (prefix == self.query_remaining and prefix == selected.items.len) upstream_error = null;
                    if (upstream_error) |err| {
                        if (failure) |out| out.* = err else return err;
                    }
                    for (columns) |*column| column.* = column.*[0..prefix];
                    self.query_remaining -= prefix;
                    return .{ .vectors = .{ .values = columns, .count = prefix } };
                }
                return .{ .rows = &.{} };
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
                while (self.query_buffer_index == self.query_buffer.len) {
                    // Keep only one page of input, provider scratch and output.
                    // Every upstream row is copied into this arena by next().
                    if (!self.scratch.reset(.retain_capacity)) return error.OutOfMemory;
                    const scratch = self.scratch.allocator();
                    self.query_buffer = &.{};
                    self.query_buffer_index = 0;
                    var page_bytes: usize = 0;
                    var rows: std.ArrayList(catalog.Row) = .empty;
                    var cells: std.ArrayList([]const Datum) = .empty;
                    // Each nested stage leaves room for upstream pages, bindings and spill operators.
                    const wanted = @min(@min(context.limits.page_rows, @max(@as(usize, 1), context.limits.retained_bytes / (16 * 1024 + query.source.columns.len * @sizeOf(Datum) * 16))), self.query_remaining +| self.query_skip);
                    while (rows.items.len < wanted) {
                        const input = try self.left.?.next(scratch) orelse break;
                        try self.engine.checkpoint();
                        var object: std.json.ObjectMap = .empty;
                        const nulls = try scratch.alloc(bool, input.len);
                        const sources = try scratch.alloc(?*scalar.PatternSet, input.len);
                        for (input, sources) |cell, *source| source.* = cell.patterns;
                        for (input, query.source.columns, nulls) |cell, column, *is_null| {
                            try object.put(scratch, column.internal, cell.value);
                            is_null.* = cell.sql_null;
                        }
                        const row: catalog.Row = .{ .id = "", .version = 0, .value = .{ .object = object }, .sql_nulls = nulls, .pattern_sources = sources };
                        try rows.append(scratch, row);
                        try cells.append(scratch, try context.binding.scalars.cells(scratch, row));
                        for (input) |cell| page_bytes +|= try operators.datumBytes(cell);
                        if (page_bytes >= context.limits.page_bytes) break;
                    }
                    if (rows.items.len == 0) return null;
                    const predicates = if (context.binding.scalars.predicate) |*program|
                        try @import("decision_eval.zig").evaluateBatch(scratch, context.backend.decision_provider, program, cells.items, context.parameters)
                    else
                        null;
                    var selected_rows: std.ArrayList(catalog.Row) = .empty;
                    var selected_cells: std.ArrayList([]const Datum) = .empty;
                    for (rows.items, cells.items, 0..) |row, row_cells, index| {
                        if (predicates) |values| {
                            const value = values[index];
                            if (value.sql_null) continue;
                            if (value.value != .bool) return error.SqlTypeMismatch;
                            if (!value.value.bool) continue;
                        }
                        if (self.query_skip != 0) {
                            self.query_skip -= 1;
                            continue;
                        }
                        if (selected_rows.items.len == self.query_remaining) break;
                        try selected_rows.append(scratch, row);
                        try selected_cells.append(scratch, row_cells);
                    }
                    self.query_buffer = try context.projectValuesBatch(scratch, selected_rows.items, self.query_fields.?, selected_cells.items);
                }
                const values = self.query_buffer[self.query_buffer_index];
                const owned = try alloc.alloc(Datum, values.len);
                for (values, owned) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                self.query_buffer_index += 1;
                self.query_remaining -= 1;
                return owned;
            }

            fn setValues(self: *Iterator, alloc: Allocator, values: []const Datum) ![]const Datum {
                const result = try alloc.alloc(Datum, values.len);
                for (values, result, self.node.columns) |value, *out, column| out.* = .{
                    .value = try describe.coerceAlloc(alloc, value.value, column.type),
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
                for (programs, result) |program, *out| out.* = try self.engine.context.evaluate(alloc, program, values);
                return result;
            }
            fn keyBatch(self: *Iterator, a: Allocator, programs: []const scalar.Program, batch: @import("execution_batch.zig").Batch, errors: ?[]?anyerror) ![]const []const Datum {
                const count = batch.len();
                const cells = try a.alloc(Datum, count * programs.len);
                @memset(cells, .{});
                const keys_ = try a.alloc([]const Datum, count);
                for (keys_, 0..) |*row, index| row.* = cells[index * programs.len ..][0..programs.len];
                const native_values = if (batch == .columns) blk: {
                    const pointers = try a.alloc(*const scalar.Program, programs.len);
                    for (programs, pointers) |*program, *pointer| pointer.* = program;
                    break :blk @import("vector_eval.zig").evaluateColumnsManyScheduled(a, pointers, batch.columns.page, batch.columns.definitions, self.engine.context.parameters, self.engine.context.backend.execution_io) catch |err| {
                        if (errors == null) return err;
                        break :blk null;
                    };
                } else null;
                for (programs, 0..) |*program, slot| {
                    const values = (switch (batch) {
                        .rows => |rows| @import("vector_eval.zig").evaluate(a, program, rows, self.engine.context.parameters),
                        .columns => if (native_values) |vectors| vectors[slot] else null,
                        else => @import("vector_eval.zig").evaluateBatch(a, program, batch, self.engine.context.parameters),
                    }) catch |err| blk: {
                        if (errors == null) return err;
                        break :blk null;
                    };
                    for (0..count) |index| {
                        if (errors) |flags| if (flags[index] != null) continue;
                        cells[index * programs.len + slot] = if (values) |vector| vector[index] else self.engine.context.evaluate(a, program.*, try batch.row(a, index)) catch |err| blk: {
                            if (errors) |flags| {
                                flags[index] = err;
                                break :blk .{};
                            }
                            return err;
                        };
                    }
                }
                return keys_;
            }
            fn fillProbes(self: *Iterator, programs: []const scalar.Program) !bool {
                if (self.probe_input_error) |err| return err;
                if (self.probe_source_exhausted) return false;
                _ = self.probe_arena.reset(.free_all);
                const a = self.probe_arena.allocator();
                const count = @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.left.?.node.columns.len * @sizeOf(Datum) * 16)));
                self.probe_payload = self.left.?.nextBatch(a, count, &self.probe_input_error) catch |err| blk: {
                    self.probe_input_error = err;
                    break :blk .{ .rows = &.{} };
                };
                self.probe_index = 0;
                if (self.probe_payload.len() == 0) {
                    self.probe_source_exhausted = true;
                    if (self.probe_input_error) |err| return err;
                    return false;
                }
                self.probe_errors = try a.alloc(?anyerror, self.probe_payload.len());
                @memset(self.probe_errors, null);
                const keys_ = try self.keyBatch(a, programs, self.probe_payload, self.probe_errors);
                self.probe_batch = self.hash_join.?.probeBatch(a, keys_) catch blk: {
                    const probes = try a.alloc(operators.HashJoin.Probe, keys_.len);
                    for (keys_, probes, self.probe_errors) |keys__, *probe_, *failure| {
                        probe_.* = self.hash_join.?.probe(keys__) catch |err| inner: {
                            failure.* = failure.* orelse err;
                            break :inner .{ .owner = self.hash_join.?, .keys = keys__, .cursor = null };
                        };
                    }
                    break :blk probes;
                };
                return true;
            }
            fn combine(self: *Iterator, alloc: Allocator, left: ?[]const Datum, right: ?[]const Datum) ![]const Datum {
                const width = self.node.operation.join.left.columns.len;
                const values = try alloc.alloc(Datum, self.node.columns.len);
                @memset(values, .{});
                if (if (self.flipped_join) right else left) |cells| @memcpy(values[0..width], cells);
                if (if (self.flipped_join) left else right) |cells| @memcpy(values[width..], cells);
                return values;
            }
            fn nextPartitionJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                const owner = self.partition_join.?;
                while (try owner.next()) |pair| {
                    try self.engine.checkpoint();
                    _ = self.scratch.reset(.free_all);
                    const values = try self.combine(self.scratch.allocator(), pair.left, pair.right);
                    if (pair.match) |index| {
                        if (join.condition) |program| {
                            const accepted = try self.engine.context.evaluate(self.scratch.allocator(), program, values);
                            if (accepted.sql_null) continue;
                            if (accepted.value != .bool) return error.SqlTypeMismatch;
                            if (!accepted.value.bool) continue;
                        }
                        try owner.accept(index);
                    }
                    const result = try alloc.alloc(Datum, values.len);
                    for (values, result) |value, *out| out.* = try operators.cloneDatum(alloc, value);
                    return result;
                }
                return null;
            }
            fn prepareScanFilter(self: *Iterator, programs: []const scalar.Program) !void {
                const probe = self.left.?;
                if (probe.node.operation != .scan or programs.len == 0) return;
                const scan = probe.node.operation.scan;
                const cursor = self.engine.cursors[scan.index];
                if (cursor.set_dynamic_filter == null) return;
                const columns = try self.engine.context.alloc.alloc(@import("dynamic_filter.zig").Column, programs.len);
                defer self.engine.context.alloc.free(columns);
                for (programs, columns) |program, *column| {
                    if (program.instructions.len != 1 or program.instructions[program.root].operation != .column) return;
                    const instruction = program.instructions[program.root];
                    const kind = instruction.type.kind orelse return;
                    switch (kind) {
                        .integer, .number, .string, .boolean => {},
                        else => return,
                    }
                    column.* = .{ .name = scan.source_columns[instruction.operation.column], .type = kind };
                }
                self.scan_filter = try @import("dynamic_filter.zig").Filter.create(self.engine.context.alloc, columns, self.engine.context.limits.retained_bytes / 32);
            }
            fn nextJoin(self: *Iterator, alloc: Allocator, join: @FieldType(@FieldType(binding.Node, "operation"), "join")) anyerror!?[]const Datum {
                const kind = if (self.flipped_join) switch (join.kind) {
                    .right => .left,
                    .left => .right,
                    else => join.kind,
                } else join.kind;
                const shared = self.recursive_id != null;
                if (self.hash_join == null and shared) if (self.engine.static_hashes.get(self.node)) |cached| {
                    self.hash_join = cached;
                    self.borrowed_hash = true;
                };
                if (self.partition_join != null) return self.nextPartitionJoin(alloc, join);
                if (self.hash_join == null) {
                    self.hash_join = try operators.HashJoin.create(self.engine.context.alloc, .{ .rows = self.engine.context.limits.scan_rows, .bytes = self.engine.context.limits.retained_bytes, .spill = self.engine.context.spill });
                    if (!shared and kind != .left and kind != .full) try self.prepareScanFilter(if (self.flipped_join) join.right_keys else join.left_keys);
                    var scratch = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                    defer scratch.deinit();
                    while (true) {
                        _ = scratch.reset(.free_all);
                        const a = scratch.allocator();
                        const count = @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.right.?.node.columns.len * @sizeOf(Datum) * 16)));
                        const batch = try self.right.?.nextBatch(a, count, null);
                        if (batch.len() == 0) break;
                        const keys_ = try self.keyBatch(a, if (self.flipped_join) join.left_keys else join.right_keys, batch, null);
                        if (!shared and self.engine.context.spill != null) {
                            if (self.partition_join) |owner| {
                                try owner.addBatch(true, batch, keys_, 0);
                            } else {
                                const consumed = try self.hash_join.?.addBatchUntilFull(a, batch, keys_);
                                if (consumed != batch.len()) {
                                    // The partition workspace coexists with input decode,
                                    // the enclosing operator, and result delivery. Reserve
                                    // a statement lane for those consumers before assigning
                                    // the remaining workspace to serial or parallel builds.
                                    const workspace = self.engine.context.limits.retained_bytes - self.engine.context.limits.retained_bytes / 4;
                                    const owner = try @import("partition_join.zig").Join.create(self.engine.context.alloc, self.engine.context.spill.?, workspace, self.engine.context.limits.scan_rows, self.engine.context.limits.retained_bytes / 8, kind == .left or kind == .full, kind == .right or kind == .full);
                                    self.partition_join = owner;
                                    var transfer = std.heap.ArenaAllocator.init(self.engine.context.alloc);
                                    defer transfer.deinit();
                                    var index: usize = 0;
                                    while (try self.hash_join.?.unmatched(&index)) |match| {
                                        _ = transfer.reset(.retain_capacity);
                                        try owner.add(true, try match.materializeValues(transfer.allocator()), try match.materializeKeys(transfer.allocator()), @intCast(match.index));
                                    }
                                    self.hash_join.?.deinit();
                                    self.hash_join = null;
                                    try owner.addBatch(true, batch, keys_, consumed);
                                }
                            }
                        } else try self.hash_join.?.addBatch(a, batch, keys_);
                        if (self.scan_filter) |filter| for (keys_) |key_values| try filter.add(key_values);
                    }
                    if (self.scan_filter) |filter| {
                        filter.sealed = true;
                        const scan = self.left.?.node.operation.scan;
                        const cursor = self.engine.cursors[scan.index];
                        _ = try cursor.set_dynamic_filter.?(cursor.ptr, filter);
                    }
                    if (self.partition_join) |owner| {
                        if (!Adapter.hasPatterns(self.node, 0) and (join.condition == null or !@import("decision_eval.zig").hasExternal(&join.condition.?))) owner.evaluation = .{
                            .condition = if (self.node.operation.join.condition) |*program| program else null,
                            .parameters = self.engine.context.parameters,
                            .left_width = self.left.?.node.columns.len,
                            .right_width = self.right.?.node.columns.len,
                            .flipped = self.flipped_join,
                        };
                        while (true) {
                            _ = scratch.reset(.retain_capacity);
                            const a = scratch.allocator();
                            const count = @min(self.engine.context.limits.executionRows(), @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.left.?.node.columns.len * @sizeOf(Datum) * 16)));
                            const batch = try self.left.?.nextBatch(a, count, null);
                            if (batch.len() == 0) break;
                            const keys_ = try self.keyBatch(a, if (self.flipped_join) join.right_keys else join.left_keys, batch, null);
                            try owner.addBatch(false, batch, keys_, 0);
                        }
                        if (self.hash_join) |hash| hash.deinit();
                        self.hash_join = null;
                        self.partition_join = owner;
                        // Both inputs now belong to the spill files. Release their
                        // decode arena before building or probing a partition;
                        // return expressions execute before deferred cleanup.
                        _ = scratch.reset(.free_all);
                        return self.nextPartitionJoin(alloc, join);
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
                            // A join iterator owns its candidate scratch for its full
                            // lifetime. Reuse retained pages across output rows;
                            // returned cells still reference the stable probe/build
                            // rows, not this transient candidate allocation.
                            _ = self.scratch.reset(.retain_capacity);
                            if (self.left_values == null) self.left_values = try self.probe_payload.row(self.probe_arena.allocator(), self.probe_index - 1);
                            const values = try self.combine(self.scratch.allocator(), self.left_values, try match.materializeValues(self.scratch.allocator()));
                            if (join.condition) |program| {
                                const accepted = try self.engine.context.evaluate(self.scratch.allocator(), program, values);
                                if (accepted.sql_null) continue;
                                if (accepted.value != .bool) return error.SqlTypeMismatch;
                                if (!accepted.value.bool) continue;
                            }
                            self.left_matched = true;
                            try self.hash_join.?.markMatched(match.index);
                            return try alloc.dupe(Datum, values);
                        }
                        self.probe = null;
                        if (!self.left_matched and (kind == .left or kind == .full)) return try self.combine(alloc, self.left_values orelse try self.probe_payload.row(self.probe_arena.allocator(), self.probe_index - 1), null);
                    }
                    if (self.eof) {
                        if (kind == .right or kind == .full) {
                            if (try self.hash_join.?.unmatched(&self.unmatched_index)) |match| return try self.combine(alloc, null, try match.materializeValues(alloc));
                        }
                        return null;
                    }
                    if (self.probe_index == self.probe_payload.len()) {
                        if (!try self.fillProbes(if (self.flipped_join) join.right_keys else join.left_keys)) {
                            self.eof = true;
                            continue;
                        }
                    }
                    if (self.probe_errors[self.probe_index]) |err| return err;
                    self.left_values = null;
                    self.left_matched = false;
                    self.probe = self.probe_batch[self.probe_index];
                    self.probe_index += 1;
                }
            }
        };

        const Adapter = struct {
            engine: *Self,
            iterator: *Iterator,
            table: catalog.Table,
            ordinal: u64 = 0,
            opened: bool = false,
            pending_failure: ?anyerror = null,

            fn iface(self: *Adapter) catalog.Backend {
                return .{ .execution_io = self.engine.context.backend.execution_io, .spill_manager = self.engine.context.spill, .ptr = self, .decision_provider = self.engine.context.backend.decision_provider, .pinned_statement_snapshot = true, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = Adapter.checkpoint } };
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
            fn hasPatterns(node: *const binding.Node, depth: usize) bool {
                if (depth == 64) return true;
                return switch (node.operation) {
                    .query => |query| blk: {
                        if (query.binding.aggregate) |aggregate| for (aggregate.specs) |spec| if (spec.kind == .pattern_set) break :blk true;
                        break :blk hasPatterns(query.source, depth + 1);
                    },
                    .join => |join| hasPatterns(join.left, depth + 1) or hasPatterns(join.right, depth + 1),
                    .set => |set| hasPatterns(set.left, depth + 1) or hasPatterns(set.right, depth + 1),
                    .values => |arms| for (arms) |arm| {
                        if (hasPatterns(arm, depth + 1)) break true;
                    } else false,
                    .materialized_ref => |source| hasPatterns(source, depth + 1),
                    .recursive, .recursive_ref => true,
                    else => false,
                };
            }
            /// Preserve columns across a relational adapter instead of building
            /// a JSON object per joined/projected row for the next operator.
            fn nextColumns(ptr: *anyopaque, a: Allocator, limit: u32) anyerror!catalog.ColumnPage {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                if (self.pending_failure) |err| return err;
                const wanted = @min(limit, @max(@as(usize, 1), self.engine.context.limits.retained_bytes / (16 * 1024 + self.iterator.node.columns.len * @sizeOf(Datum) * 16)));
                var failure: ?anyerror = null;
                const batch = try self.iterator.nextBatch(a, wanted, &failure);
                self.pending_failure = failure;
                if (batch.len() == 0) if (failure) |err| return err;
                const values = try a.create(@import("execution_batch.zig").Batch);
                values.* = batch;
                const names = try a.alloc([]const u8, self.iterator.node.columns.len);
                for (self.iterator.node.columns, names) |definition, *name| name.* = definition.internal;
                const selection = try a.alloc(usize, batch.len());
                for (selection, 0..) |*index, offset| index.* = offset;
                self.ordinal += batch.len();
                return .{ .native = .{ .values = values, .names = names }, .selection = selection, .after = if (batch.len() != 0) try std.fmt.allocPrint(a, "{d}", .{self.ordinal}) else null };
            }
            fn open(ptr: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                if (self.opened) return error.InvalidSqlBackendResponse;
                self.opened = true;
                return .{ .ptr = self, .next = next, .next_columns = if (hasPatterns(self.iterator.node, 0)) null else nextColumns, .close = close };
            }
            fn close(_: *anyopaque) void {}
            fn next(ptr: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
                const self: *Adapter = @ptrCast(@alignCast(ptr));
                var rows: std.ArrayList(catalog.Row) = .empty;
                var bytes: usize = 0;
                var stopped_for_bytes = false;
                while (rows.items.len < limit) {
                    const values = try self.iterator.next(alloc) orelse break;
                    for (values) |value| bytes +|= try operators.datumBytes(value);
                    var object: std.json.ObjectMap = .empty;
                    const nulls = try alloc.alloc(bool, values.len);
                    const sources = try alloc.alloc(?*scalar.PatternSet, values.len);
                    for (values, sources) |value, *source| source.* = value.patterns;
                    for (values, self.iterator.node.columns, nulls) |value, column, *sql_null| {
                        const owned = try operators.cloneDatum(alloc, value);
                        try object.put(alloc, column.internal, owned.value);
                        sql_null.* = owned.sql_null;
                    }
                    self.ordinal += 1;
                    try rows.append(alloc, .{ .id = try std.fmt.allocPrint(alloc, "{d}", .{self.ordinal}), .version = 0, .value = .{ .object = object }, .sql_nulls = nulls, .pattern_sources = sources });
                    if (bytes >= self.engine.context.limits.page_bytes) {
                        stopped_for_bytes = true;
                        break;
                    }
                }
                const more = rows.items.len == limit or stopped_for_bytes;
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
            var search_arena = std.heap.ArenaAllocator.init(context.alloc);
            defer search_arena.deinit();
            const has_search = for (relation.scans) |scan| {
                if (scan.request.search != null) break true;
            } else false;
            const scans: []const catalog.StatementScan = if (has_search) bound: {
                const bound_scans = try search_arena.allocator().dupe(catalog.StatementScan, relation.scans);
                for (bound_scans) |*scan| if (scan.request.search) |original| {
                    const search = try search_arena.allocator().create(catalog.Scan.Search);
                    search.* = original.*;
                    scan.request.search = search;
                    const expression = search.expression;
                    const request = if (expression.request == .parameter) context.parameters[expression.request.parameter - 1] else try describe.bindLiteral(context.alloc, expression.request, .string);
                    if (request != .string) return error.InvalidSqlParameters;
                    search.request_text = request.string;
                    if (expression.limit) |input| {
                        const limit = if (input == .parameter) context.parameters[input.parameter - 1] else try describe.bindLiteral(context.alloc, input, .integer);
                        const number = if (limit == .integer) limit.integer else if (limit == .string) std.fmt.parseInt(i64, limit.string, 10) catch return error.InvalidSqlParameters else return error.InvalidSqlParameters;
                        if (number < 1 or number > 10000) return error.InvalidSqlParameters;
                        search.limit = @intCast(number);
                    }
                };
                break :bound bound_scans;
            } else relation.scans;
            if (scans.len != 0) {
                if (context.backend.vtable.open_statement) |open| {
                    self.read = try open(context.backend.ptr, context.alloc, scans);
                    self.engine.cursors = self.read.?.cursors;
                    if (self.engine.cursors.len != relation.scans.len) return error.InvalidSqlBackendResponse;
                } else if (relation.scans.len == 1) {
                    const open = context.backend.vtable.open_scan orelse return error.SqlStatementSnapshotRequired;
                    self.single = (try open(context.backend.ptr, context.alloc, scans[0].table, scans[0].request)) orelse return error.SqlStatementSnapshotRequired;
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
        fn nextColumns(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.ColumnPage {
            const self: *Self = @ptrCast(@alignCast(raw));
            return Engine(Context).Adapter.nextColumns(&self.adapter, alloc, limit);
        }
        fn closeCursor(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            self.close();
        }
    };
}

pub fn openCursor(context: anytype) !catalog.Cursor {
    const owner = try Source(@TypeOf(context)).create(context);
    return .{ .ptr = owner, .next = @TypeOf(owner.*).next, .next_columns = if (Engine(@TypeOf(context)).Adapter.hasPatterns(owner.iterator.?.node, 0)) null else @TypeOf(owner.*).nextColumns, .close = @TypeOf(owner.*).closeCursor };
}

pub fn execute(context: anytype) anyerror!@import("runtime.zig").Output {
    const owner = try Source(@TypeOf(context)).create(context);
    defer owner.close();
    var lowered = context;
    lowered.backend = owner.adapter.iface();
    lowered.binding.relation = null;
    return lowered.select(context.binding.relation.?.statement);
}
