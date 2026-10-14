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
const ast = @import("ast.zig");
const compiler = @import("compiler.zig");
const catalog = @import("catalog.zig");
const v = @import("value.zig");
const backend_mod = @import("backend.zig");
const unicode_string = @import("unicode_string.zig");
const Budget = @import("budget.zig");
const A = std.mem.Allocator;
pub const Row = []const v.Entry;
pub const max_response_bytes: usize = 32 << 20;
pub const Limits = struct { memory_bytes: usize = 64 << 20, scanned_anchors: usize = 1_000_000, explored_edges: usize = 1_000_000, intermediate_states: usize = 1_000_000, path_depth: usize = 1024, recursion_depth: usize = 64 };
pub const Stats = struct { scanned_anchors: usize = 0, explored_edges: usize = 0, intermediate_states: usize = 0, mutations: usize = 0, search_calls: usize = 0, search_candidates: usize = 0, search_approximate: bool = false, memory_bytes: usize = 0 };
pub const Stage = struct { operation: []const u8, span: ast.Span, input_rows: usize, output_rows: usize };
pub const Result = struct {
    arena: std.heap.ArenaAllocator,
    budget: *Budget,
    columns: []const []const u8,
    rows: []const []const v.Value,
    stages: []const Stage,
    stats: Stats,
    read_only: bool = true,
    graph_incarnation: u64,
    graph_revision: u64,
    complete: bool = true,
    pub fn jsonStringify(self: Result, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("wire_version");
        try writer.write(@import("wire.zig").version);
        try writer.objectField("columns");
        try writer.write(self.columns);
        try writer.objectField("rows");
        try writer.beginArray();
        for (self.rows) |row| {
            try writer.beginArray();
            for (row) |cell| try writer.write(@import("wire.zig").Value{ .value = cell });
            try writer.endArray();
        }
        try writer.endArray();
        try writer.objectField("plan");
        try writer.write(self.stages);
        try writer.objectField("read_only");
        try writer.write(self.read_only);
        try writer.objectField("stats");
        try writer.write(self.stats);
        try writer.objectField("graph_incarnation");
        var incarnation: [32]u8 = undefined;
        try writer.write(std.fmt.bufPrint(&incarnation, "{d}", .{self.graph_incarnation}) catch unreachable);
        try writer.objectField("graph_revision");
        var revision: [32]u8 = undefined;
        try writer.write(std.fmt.bufPrint(&revision, "{d}", .{self.graph_revision}) catch unreachable);
        try writer.objectField("complete");
        try writer.write(self.complete);
        try writer.endObject();
    }
    pub fn deinit(self: *Result) void {
        const backing = self.budget.backing;
        self.arena.deinit();
        backing.destroy(self.budget);
        self.* = undefined;
    }
};

pub fn execute(backing: A, backend: backend_mod.Backend, graph: catalog.Graph, compiled: *const compiler.Compiled, parameters: []const v.Entry, limits: Limits) !Result {
    try graph.validate();
    try @import("binder.zig").validate(backing, graph, compiled.query);
    const budget = try backing.create(Budget);
    budget.* = .{ .backing = backing, .limit = limits.memory_bytes };
    errdefer backing.destroy(budget);
    return executeBudget(budget, backend, graph, compiled, parameters, limits) catch |err| return if (budget.exhausted) error.CypherMemoryBudgetExceeded else err;
}

fn executeBudget(budget: *Budget, backend: backend_mod.Backend, graph: catalog.Graph, compiled: *const compiler.Compiled, parameters: []const v.Entry, limits: Limits) !Result {
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    var engine: Engine = .{ .a = arena.allocator(), .backend = backend, .graph = graph, .source = compiled.query.source, .parameters = parameters, .limits = limits, .regex = .{ .a = arena.allocator(), .scratch = std.heap.ArenaAllocator.init(budget.allocator()) } };
    defer engine.regex.deinit();
    for (compiled.parameters) |name| {
        _ = lookup(parameters, name) orelse return error.MissingCypherParameter;
    }
    try backend.vtable.bind(backend.ptr, engine.a, graph);
    const writes = queryHasWrites(compiled.query);
    var transaction = false;
    defer if (transaction) backend.vtable.rollback.?(backend.ptr);
    if (writes and !compiled.query.explain) {
        if (backend.vtable.begin == null or backend.vtable.mutate == null or backend.vtable.commit == null or backend.vtable.rollback == null or backend.vtable.new_identity == null) return error.CypherMutationsUnavailable;
        try backend.vtable.begin.?(backend.ptr);
        transaction = true;
    }
    if (!compiled.query.explain) if (backend.vtable.startup_work) |work| {
        const initial = work(backend.ptr);
        if (initial.scanned_anchors > limits.scanned_anchors) return error.CypherAnchorBudgetExceeded;
        if (initial.explored_edges > limits.explored_edges) return error.CypherEdgeBudgetExceeded;
        engine.stats.scanned_anchors = initial.scanned_anchors;
        engine.stats.explored_edges = initial.explored_edges;
    };
    if (compiled.query.explain) engine.columns = try @import("binder.zig").columns(engine.a, graph, compiled.query);
    const rows = try engine.run(compiled.query.clauses, &.{&.{}}, compiled.query.explain, false);
    var combined: std.ArrayList(Row) = .empty;
    try combined.appendSlice(engine.a, rows);
    for (compiled.query.unions) |arm| {
        const more = try engine.run(arm.clauses, &.{&.{}}, compiled.query.explain, false);
        for (more) |row| {
            if (row.len != engine.columns.len) return error.CypherUnionColumnMismatch;
            for (row, engine.columns) |entry, name| if (!std.mem.eql(u8, entry.name, name)) return error.CypherUnionColumnMismatch;
            try combined.append(engine.a, row);
        }
        if (!arm.all) combined = try engine.distinct(combined.items);
    }
    const output = try engine.a.alloc([]const v.Value, combined.items.len);
    for (combined.items, output) |row, *out| {
        const cells = try engine.a.alloc(v.Value, row.len);
        for (row, cells) |entry, *cell| cell.* = try (try engine.readValue(entry.value)).clone(engine.a);
        out.* = cells;
    }
    const columns = try engine.a.alloc([]const u8, engine.columns.len);
    for (engine.columns, columns) |name, *out| out.* = try engine.a.dupe(u8, name);
    const stages = try engine.stages.toOwnedSlice(engine.a);
    engine.stats.memory_bytes = budget.peak;
    const result: Result = .{ .arena = arena, .budget = budget, .columns = columns, .rows = output, .stages = stages, .stats = engine.stats, .read_only = !writes, .graph_incarnation = graph.incarnation, .graph_revision = graph.revision };
    var checkpoint_backend = backend;
    // Reserve room for the host's durable receipt. Count escaped wire bytes
    // before a write decision, so an oversized RETURN rolls back the overlay.
    _ = try @import("bounded_json.zig").checkSize(result, max_response_bytes - 1024, .{ .ptr = &checkpoint_backend, .call = serializationCheckpoint });
    if (transaction) {
        try backend.vtable.checkpoint(backend.ptr);
        try backend.vtable.commit.?(backend.ptr);
        transaction = false;
    }
    return result;
}

fn serializationCheckpoint(ptr: *anyopaque) !void {
    const backend: *backend_mod.Backend = @ptrCast(@alignCast(ptr));
    try backend.vtable.checkpoint(backend.ptr);
}

fn lookup(row: Row, name: []const u8) ?v.Value {
    for (row) |entry| if (std.mem.eql(u8, entry.name, name)) return entry.value;
    return null;
}
pub fn hasWrites(clauses: []const ast.Clause) bool {
    for (clauses) |clause| switch (clause.operation) {
        .create, .merge, .set, .remove, .delete => return true,
        .subquery => |inner| if (hasWrites(inner)) return true,
        else => {},
    };
    return false;
}
pub fn queryHasWrites(query: ast.Query) bool {
    if (hasWrites(query.clauses)) return true;
    for (query.unions) |arm| if (hasWrites(arm.clauses)) return true;
    return false;
}
const aggregateName = @import("expressions.zig").aggregateName;
const aggregates = @import("expressions.zig").aggregates;
const Engine = struct {
    a: A,
    regex: @import("regex.zig").Runtime,
    backend: backend_mod.Backend,
    graph: catalog.Graph,
    source: []const u8,
    parameters: []const v.Entry,
    limits: Limits,
    stats: Stats = .{},
    columns: []const []const u8 = &.{},
    stages: std.ArrayList(Stage) = .empty,
    recursion: usize = 0,
    deleted: std.ArrayList(v.EntityIdentity) = .empty,
    fn checkpoint(self: *Engine) !void {
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }
    fn regexCheckpoint(ptr: ?*anyopaque) !void {
        const self: *Engine = @ptrCast(@alignCast(ptr.?));
        try self.checkpoint();
    }
    fn state(self: *Engine) !void {
        try self.checkpoint();
        self.stats.intermediate_states += 1;
        if (self.stats.intermediate_states > self.limits.intermediate_states) return error.CypherStateBudgetExceeded;
    }
    fn bind(self: *Engine, row: Row, name: ?[]const u8, value: v.Value) !?Row {
        const alias = name orelse return row;
        if (lookup(row, alias)) |existing| return if (v.Value.eql(existing, value)) row else null;
        const output = try self.a.alloc(v.Entry, row.len + 1);
        @memcpy(output[0..row.len], row);
        output[row.len] = .{ .name = alias, .value = value };
        return output;
    }
    fn replace(self: *Engine, row: Row, name: []const u8, value: v.Value) !Row {
        const output = try self.a.dupe(v.Entry, row);
        for (output) |*entry| if (std.mem.eql(u8, entry.name, name)) {
            entry.value = value;
            return output;
        };
        return (try self.bind(row, name, value)).?;
    }
    fn run(self: *Engine, clauses: []const ast.Clause, input: []const Row, explain: bool, preserve_rows: bool) anyerror![]const Row {
        self.recursion += 1;
        defer self.recursion -= 1;
        if (self.recursion > self.limits.recursion_depth) return error.CypherRecursionBudgetExceeded;
        var rows = input;
        for (clauses, 0..) |clause, clause_index| {
            try self.checkpoint();
            const before = rows.len;
            if (explain) {
                try self.stages.append(self.a, .{ .operation = @tagName(clause.operation), .span = clause.span, .input_rows = 0, .output_rows = 0 });
                continue;
            }
            rows = switch (clause.operation) {
                .match => |m| blk: {
                    if (clause_index == 0 and clauses.len == 2 and clauses[1].operation == .project and clauses[1].operation.project.final) {
                        if (try self.matchNodeGoal(rows, m, clauses[1].operation.project.value)) |bounded| break :blk bounded;
                    }
                    break :blk try self.match(rows, m);
                },
                .project => |p| try self.project(rows, p.value, p.final),
                .unwind => |u| blk: {
                    var output: std.ArrayList(Row) = .empty;
                    for (rows) |row| {
                        const value = try self.eval(u.expression, row, null);
                        if (value == .null) continue;
                        const items = if (value == .list) value.list else &.{value};
                        for (items) |item| {
                            try self.state();
                            try output.append(self.a, try self.replace(row, u.alias, item));
                        }
                    }
                    break :blk try output.toOwnedSlice(self.a);
                },
                .call => |c| try self.call(rows, c),
                .subquery => |inner| blk: {
                    var output: std.ArrayList(Row) = .empty;
                    const columns = self.columns;
                    defer self.columns = columns;
                    for (rows) |row| {
                        const children = try self.run(inner, &.{row}, false, false);
                        const last = inner[inner.len - 1].operation;
                        const unit = last != .project or !last.project.final;
                        if (unit) {
                            try output.append(self.a, row);
                            continue;
                        }
                        for (children) |child| {
                            var merged = row;
                            for (child) |entry| merged = (try self.bind(merged, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                            try output.append(self.a, merged);
                        }
                    }
                    break :blk try output.toOwnedSlice(self.a);
                },
                .create => |paths| try self.create(rows, paths, false),
                .merge => |m| blk: {
                    var output: std.ArrayList(Row) = .empty;
                    for (rows) |row| {
                        // NULL cannot identify a MERGE pattern. Validate both
                        // node and relationship keys before matching or creating.
                        for (m.path.nodes) |node| for (node.properties) |property| {
                            const key = self.eval(property.value, row, null) catch |err| {
                                // Later pattern components can refer to a node
                                // bound earlier in the same MERGE. Check them
                                // again when CREATE evaluates that component.
                                if (err == error.UnboundCypherVariable) continue;
                                return err;
                            };
                            if (key == .null) return error.CypherInvalidPropertyValue;
                        };
                        for (m.path.edges) |edge| for (edge.properties) |property| {
                            const key = self.eval(property.value, row, null) catch |err| {
                                // Later pattern components can refer to a node
                                // bound earlier in the same MERGE. Check them
                                // again when CREATE evaluates that component.
                                if (err == error.UnboundCypherVariable) continue;
                                return err;
                            };
                            if (key == .null) return error.CypherInvalidPropertyValue;
                        };
                        const matched = try self.match(&.{row}, .{ .paths = &.{m.path} });
                        const selected = if (matched.len == 0) try self.create(&.{row}, &.{m.path}, true) else matched;
                        const assigned = try self.assign(selected, if (matched.len == 0) m.on_create else m.on_match);
                        try output.appendSlice(self.a, assigned);
                    }
                    break :blk try output.toOwnedSlice(self.a);
                },
                .set => |assignments| try self.assign(rows, assignments),
                .remove => |expressions| try self.remove(rows, expressions),
                .delete => |d| try self.delete(rows, d.expressions, d.detach),
            };
            try self.stages.append(self.a, .{ .operation = @tagName(clause.operation), .span = clause.span, .input_rows = before, .output_rows = rows.len });
        }
        if (explain or (!preserve_rows and clauses[clauses.len - 1].operation != .project)) return &.{};
        return rows;
    }

    fn nodeMatches(self: *Engine, raw: *const v.Node, pattern: ast.Node, row: Row) !bool {
        if (self.isDeleted(.{ .node = raw })) return false;
        const node = if (self.backend.vtable.project_node) |project_node| (try project_node(self.backend.ptr, self.a, raw)) orelse return false else raw;
        const member = for (self.graph.labels) |label| {
            if (self.graph.admits(node, label.name)) break true;
        } else false;
        if (!member) return false;
        for (pattern.labels) |label| {
            if (self.graph.label(label) == null) return error.UnknownGraphLabel;
            if (!self.graph.admits(node, label)) return false;
        }
        for (pattern.properties) |property| {
            const actual = (v.Value{ .node = node }).property(property.name);
            const expected = try self.eval(property.value, row, null);
            if (actual == .null or expected == .null or !(v.Value.equal(actual, expected) orelse false)) return false;
        }
        return true;
    }
    const MatchState = struct { row: Row, used: []const *const v.Relationship = &.{}, terminal: ?v.Identity = null };
    /// A terminal, unordered node projection can demand only SKIP + LIMIT
    /// matches. Aggregation, DISTINCT, ordering, OPTIONAL and traversals retain
    /// exhaustive semantics. Admission still counts every examined native row.
    fn matchNodeGoal(self: *Engine, rows: []const Row, operation: ast.Match, projection: ast.Projection) anyerror!?[]const Row {
        if (rows.len != 1 or rows[0].len != 0 or operation.optional or operation.paths.len != 1 or operation.paths[0].edges.len != 0 or projection.distinct or projection.order.len != 0 or projection.predicate != null or projection.limit == null) return null;
        for (projection.items) |item| if (aggregates(item.expression)) return null;
        const limit = try self.count(projection.limit.?);
        const skip = if (projection.skip) |expression| try self.count(expression) else 0;
        const goal = std.math.add(usize, skip, limit) catch return error.CypherLimitExceeded;
        if (goal == 0 or limit == 0) return &.{};
        const path = operation.paths[0];
        const first = path.nodes[0];
        if (self.anchorKey(operation.predicate, first.alias, rows[0]) != null) return null;
        var output: std.ArrayList(Row) = .empty;
        var seen: std.ArrayList(v.Identity) = .empty;
        for (self.graph.labels) |label| {
            if (first.labels.len > 0 and !std.mem.eql(u8, first.labels[0], label.name)) continue;
            var cursor: ?[]const u8 = null;
            while (output.items.len < goal) {
                try self.checkpoint();
                const page = try self.backend.vtable.scan(self.backend.ptr, self.a, label.table, cursor, @min(128, goal - output.items.len));
                const examined = page.examined orelse page.items.len;
                if (examined < page.items.len) return error.CypherInvalidBackendPage;
                if (examined > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.stats.scanned_anchors += examined;
                for (page.items) |node| {
                    if (!self.graph.admits(node, label.name)) continue;
                    var duplicate = false;
                    for (seen.items) |identity| if (identity.eql(node.identity)) {
                        duplicate = true;
                        break;
                    };
                    if (duplicate) continue;
                    try seen.append(self.a, node.identity);
                    if (!try self.nodeMatches(node, first, rows[0])) continue;
                    var bound = (try self.bind(rows[0], first.alias, .{ .node = node })) orelse continue;
                    if (path.alias) |alias| bound = (try self.bind(bound, alias, .{ .path = .{ .nodes = try self.a.dupe(*const v.Node, &.{node}), .relationships = &.{} } })) orelse continue;
                    if (operation.predicate) |predicate| if ((try (try self.eval(predicate, bound, null)).truth()) != true) continue;
                    try self.state();
                    try output.append(self.a, bound);
                    if (output.items.len == goal) break;
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                cursor = page.next;
            }
            if (output.items.len == goal) break;
        }
        return try output.toOwnedSlice(self.a);
    }
    fn match(self: *Engine, rows: []const Row, operation: ast.Match) anyerror![]const Row {
        var output: std.ArrayList(Row) = .empty;
        for (rows) |row| {
            var states: []const MatchState = &.{.{ .row = row }};
            for (operation.paths) |path| {
                var next: std.ArrayList(MatchState) = .empty;
                for (states) |input| try self.matchPath(input, path, operation.mode, operation.predicate, &next);
                states = try next.toOwnedSlice(self.a);
            }
            const initial = output.items.len;
            for (states) |matched| {
                if (operation.predicate) |predicate| if ((try (try self.eval(predicate, matched.row, null)).truth()) != true) continue;
                try output.append(self.a, matched.row);
            }
            if (operation.optional and initial == output.items.len) {
                var extended = row;
                for (operation.paths) |path| {
                    if (path.alias) |alias| if (lookup(extended, alias) == null) {
                        extended = (try self.bind(extended, alias, .null)).?;
                    };
                    for (path.nodes) |node| if (node.alias) |alias| if (lookup(extended, alias) == null) {
                        extended = (try self.bind(extended, alias, .null)).?;
                    };
                    for (path.edges) |edge| if (edge.alias) |alias| if (lookup(extended, alias) == null) {
                        extended = (try self.bind(extended, alias, .null)).?;
                    };
                }
                try output.append(self.a, extended);
            }
        }
        return output.toOwnedSlice(self.a);
    }
    fn keyCall(expression: *const ast.Expr, alias: []const u8) bool {
        if (expression.value != .call) return false;
        const function_call = expression.value.call;
        return std.ascii.eqlIgnoreCase(function_call.name, "antfly.documentKey") and function_call.args.len == 1 and function_call.args[0].value == .variable and std.mem.eql(u8, function_call.args[0].value.variable, alias);
    }
    fn constant(self: *Engine, expression: *const ast.Expr, row: Row) ?v.Value {
        return switch (expression.value) {
            .parameter => |name| lookup(self.parameters, name),
            .variable => |name| lookup(row, name),
            .literal => |value| switch (value) {
                .null => blk: {
                    const empty: v.Value = .null;
                    break :blk empty;
                },
                .string => |item| .{ .string = item },
                .integer => |item| .{ .integer = item },
                .number => |item| .{ .number = item },
                .boolean => |item| .{ .boolean = item },
            },
            else => null,
        };
    }
    fn anchorKey(self: *Engine, predicate: ?*const ast.Expr, alias: ?[]const u8, row: Row) ?v.Value {
        const expression = predicate orelse return null;
        const name = alias orelse return null;
        if (expression.value == .binary and expression.value.binary.op == .@"and") return self.anchorKey(expression.value.binary.left, name, row) orelse self.anchorKey(expression.value.binary.right, name, row);
        const operands: [2]*const ast.Expr = switch (expression.value) {
            .binary => |pair| if (pair.op == .eq) .{ pair.left, pair.right } else return null,
            .comparison => |chain| if (chain.operands.len == 2 and chain.operators.len == 1 and chain.operators[0] == .eq) .{ chain.operands[0], chain.operands[1] } else return null,
            else => return null,
        };
        if (keyCall(operands[0], name)) return self.constant(operands[1], row);
        if (keyCall(operands[1], name)) return self.constant(operands[0], row);
        return null;
    }
    fn matchPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, predicate: ?*const ast.Expr, output: *std.ArrayList(MatchState)) anyerror!void {
        const first = path.nodes[0];
        if (first.alias) |alias| if (lookup(input.row, alias)) |bound| {
            if (bound == .null) return;
            if (bound != .node) return error.CypherTypeMismatch;
            if (!try self.nodeMatches(bound.node, first, input.row)) return;
            try self.pathStep(input, path, mode, 0, &.{bound.node}, &.{}, output);
            return;
        };
        if (self.anchorKey(predicate, first.alias, input.row)) |key| {
            if (key != .string) return;
            var tables: std.StringHashMapUnmanaged(void) = .empty;
            for (self.graph.labels) |label| {
                if (first.labels.len > 0 and !std.mem.eql(u8, first.labels[0], label.name)) continue;
                const entry = try tables.getOrPut(self.a, label.table);
                if (entry.found_existing) continue;
                try self.checkpoint();
                if (self.stats.scanned_anchors == self.limits.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.stats.scanned_anchors += 1;
                const node = (try self.backend.vtable.get(self.backend.ptr, self.a, .{ .table = label.table, .key = key.string })) orelse continue;
                if (!try self.nodeMatches(node, first, input.row)) continue;
                const bound = (try self.bind(input.row, first.alias, .{ .node = node })) orelse continue;
                try self.pathStep(.{ .row = bound, .used = input.used }, path, mode, 0, &.{node}, &.{}, output);
            }
            return;
        }
        var seen: std.ArrayList(v.Identity) = .empty;
        for (self.graph.labels) |label| {
            if (first.labels.len > 0 and !std.mem.eql(u8, first.labels[0], label.name)) continue;
            var cursor: ?[]const u8 = null;
            while (true) {
                try self.checkpoint();
                const page = try self.backend.vtable.scan(self.backend.ptr, self.a, label.table, cursor, 128);
                const examined = page.examined orelse page.items.len;
                if (examined < page.items.len) return error.CypherInvalidBackendPage;
                if (examined > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.stats.scanned_anchors += examined;
                for (page.items) |node| {
                    if (!self.graph.admits(node, label.name)) continue;
                    var duplicate = false;
                    for (seen.items) |identity| if (identity.eql(node.identity)) {
                        duplicate = true;
                        break;
                    };
                    if (duplicate) continue;
                    try seen.append(self.a, node.identity);
                    if (!try self.nodeMatches(node, first, input.row)) continue;
                    const bound = (try self.bind(input.row, first.alias, .{ .node = node })) orelse continue;
                    try self.pathStep(.{ .row = bound, .used = input.used }, path, mode, 0, &.{node}, &.{}, output);
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                cursor = page.next;
            }
        }
    }
    fn pathStep(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, edge_index: usize, nodes: []const *const v.Node, relationships: []const *const v.Relationship, output: *std.ArrayList(MatchState)) anyerror!void {
        try self.state();
        if (edge_index == path.edges.len) {
            const row = (try self.bind(input.row, path.alias, .{ .path = .{ .nodes = try self.a.dupe(*const v.Node, nodes), .relationships = try self.a.dupe(*const v.Relationship, relationships) } })) orelse return;
            try output.append(self.a, .{ .row = row, .used = input.used, .terminal = nodes[nodes.len - 1].identity });
            return;
        }
        const before = output.items.len;
        try self.expandPath(input, path, mode, edge_index, nodes, relationships, 0, &.{}, output);
        if (path.selector != .all and output.items.len > before) {
            const candidates = try self.a.dupe(MatchState, output.items[before..]);
            var write = before;
            for (candidates) |candidate| {
                const length = candidate.used.len - input.used.len;
                var minimum = length;
                for (candidates) |other| if (candidate.terminal.?.eql(other.terminal.?)) {
                    minimum = @min(minimum, other.used.len - input.used.len);
                };
                if (length != minimum) continue;
                if (path.selector == .shortest) {
                    var selected = false;
                    for (output.items[before..write]) |other| if (candidate.terminal.?.eql(other.terminal.?)) {
                        selected = true;
                        break;
                    };
                    if (selected) continue;
                }
                output.items[write] = candidate;
                write += 1;
            }
            output.shrinkRetainingCapacity(write);
        }
    }
    fn expandPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, edge_index: usize, nodes: []const *const v.Node, relationships: []const *const v.Relationship, hops: u32, edge_values: []const v.Value, output: *std.ArrayList(MatchState)) anyerror!void {
        const edge = path.edges[edge_index];
        const current = nodes[nodes.len - 1];
        if (hops >= edge.min_hops and try self.nodeMatches(current, path.nodes[edge_index + 1], input.row)) {
            if (try self.bind(input.row, path.nodes[edge_index + 1].alias, .{ .node = current })) |node_row| {
                const edge_value: v.Value = if (edge.variable_length) .{ .list = edge_values } else .{ .relationship = relationships[relationships.len - 1] };
                if (try self.bind(node_row, edge.alias, edge_value)) |edge_row| try self.pathStep(.{ .row = edge_row, .used = input.used }, path, mode, edge_index + 1, nodes, relationships, output);
            }
        }
        if (edge.max_hops) |maximum| if (hops >= maximum) return;
        for (self.graph.relationships) |mapping| {
            var selected = edge.types.len == 0;
            for (edge.types) |type_name| {
                if (self.graph.relationship(type_name) == null) return error.UnknownGraphRelationship;
                if (std.mem.eql(u8, mapping.name, type_name)) selected = true;
            }
            if (!selected) continue;
            const outgoing = edge.direction != .in and self.graph.admits(current, mapping.source_label);
            const incoming = edge.direction != .out and self.graph.admits(current, mapping.target_label);
            if (!outgoing and !incoming) continue;
            var cursor: ?[]const u8 = null;
            while (true) {
                try self.checkpoint();
                const page = try self.backend.vtable.expand(self.backend.ptr, self.a, mapping, current.identity, edge.direction, cursor, 128);
                const examined = page.examined orelse page.items.len;
                if (examined < page.items.len) return error.CypherInvalidBackendPage;
                if (examined > self.limits.explored_edges -| self.stats.explored_edges) return error.CypherEdgeBudgetExceeded;
                self.stats.explored_edges += examined;
                for (page.items) |relationship| {
                    var used = false;
                    if (mode != .repeatable) for (input.used) |previous| if (v.Value.eql(.{ .relationship = previous }, .{ .relationship = relationship })) {
                        used = true;
                        break;
                    };
                    if (used) continue;
                    if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
                    const target = if (outgoing and relationship.source.eql(current.identity)) relationship.target else if (incoming and relationship.target.eql(current.identity)) relationship.source else continue;
                    const reached = (try self.backend.vtable.get(self.backend.ptr, self.a, target)) orelse continue;
                    const target_label = if (relationship.source.eql(current.identity)) mapping.target_label else mapping.source_label;
                    if (!self.graph.admits(reached, target_label)) continue;
                    if (mode == .node_simple) {
                        var revisited = false;
                        for (nodes) |n| if (n.identity.eql(reached.identity)) {
                            revisited = true;
                            break;
                        };
                        if (revisited) continue;
                    }
                    var valid = true;
                    for (edge.properties) |property| {
                        const expected = try self.eval(property.value, input.row, null);
                        const actual = (v.Value{ .relationship = relationship }).property(property.name);
                        if (expected == .null or actual == .null or !(v.Value.equal(expected, actual) orelse false)) {
                            valid = false;
                            break;
                        }
                    }
                    if (!valid) continue;
                    if (relationships.len >= self.limits.path_depth) return error.CypherPathBudgetExceeded;
                    const next_nodes = try self.a.alloc(*const v.Node, nodes.len + 1);
                    @memcpy(next_nodes[0..nodes.len], nodes);
                    next_nodes[nodes.len] = reached;
                    const next_relationships = try self.a.alloc(*const v.Relationship, relationships.len + 1);
                    @memcpy(next_relationships[0..relationships.len], relationships);
                    next_relationships[relationships.len] = relationship;
                    const next_used = try self.a.alloc(*const v.Relationship, input.used.len + 1);
                    @memcpy(next_used[0..input.used.len], input.used);
                    next_used[input.used.len] = relationship;
                    const next_values = try self.a.alloc(v.Value, edge_values.len + 1);
                    @memcpy(next_values[0..edge_values.len], edge_values);
                    next_values[edge_values.len] = .{ .relationship = relationship };
                    try self.state();
                    try self.expandPath(.{ .row = input.row, .used = next_used }, path, mode, edge_index, next_nodes, next_relationships, hops + 1, next_values, output);
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                cursor = page.next;
            }
        }
    }

    // Deletion keeps the binding's identity available for counting/equality,
    // but never permits reading its former contents or reusing its endpoints.
    fn isDeleted(self: *Engine, value: v.Value) bool {
        if (value != .node and value != .relationship) return false;
        const identity = v.EntityIdentity.fromValue(value) catch unreachable;
        for (self.deleted.items) |deleted| {
            if (identity.eql(deleted)) return true;
            if (value == .relationship and deleted == .node and
                (value.relationship.source.eql(deleted.node) or value.relationship.target.eql(deleted.node))) return true;
        }
        return false;
    }
    fn recordDeletion(self: *Engine, identity: v.EntityIdentity) !void {
        for (self.deleted.items) |prior| if (prior.eql(identity)) return;
        try self.deleted.append(self.a, identity);
    }
    fn requireLiveContents(self: *Engine, value: v.Value) !void {
        if (self.isDeleted(value)) return error.CypherDeletedEntity;
        if (value == .relationship) if (self.backend.vtable.get_relationship) |get_relationship| {
            if (try get_relationship(self.backend.ptr, self.a, .{ .identity = value.relationship.identity, .index = value.relationship.index }) == null) return error.CypherDeletedEntity;
        };
    }
    fn readValue(self: *Engine, value: v.Value) anyerror!v.Value {
        const project_node = self.backend.vtable.project_node;
        if (self.isDeleted(value)) return switch (value) {
            .node => |retained| blk: {
                const node = try self.a.create(v.Node);
                node.* = .{ .identity = retained.identity };
                break :blk .{ .node = node };
            },
            .relationship => |retained| blk: {
                const relationship = try self.a.create(v.Relationship);
                relationship.* = retained.*;
                relationship.properties = &.{};
                break :blk .{ .relationship = relationship };
            },
            else => unreachable,
        };
        if (project_node == null and self.backend.vtable.get_relationship == null and self.deleted.items.len == 0) return value;
        return switch (value) {
            .node => |node| if (project_node) |project_read| if (try project_read(self.backend.ptr, self.a, node)) |visible| .{ .node = visible } else .null else value,
            .relationship => |retained| blk: {
                const relationship = if (self.backend.vtable.get_relationship) |get_relationship|
                    (try get_relationship(self.backend.ptr, self.a, .{ .identity = retained.identity, .index = retained.index })) orelse {
                        // A derived contribution can disappear and return with
                        // the same identity. Reads must not record a permanent
                        // deletion; only mutation callbacks establish one.
                        const missing = try self.a.create(v.Relationship);
                        missing.* = retained.*;
                        missing.properties = &.{};
                        break :blk .{ .relationship = missing };
                    }
                else
                    retained;
                if (project_node) |project_read| for ([_]v.Identity{ relationship.source, relationship.target }) |identity| {
                    if (try self.backend.vtable.get(self.backend.ptr, self.a, identity)) |node| {
                        if (try project_read(self.backend.ptr, self.a, node) == null) break :blk .null;
                    }
                };
                break :blk .{ .relationship = relationship };
            },
            .path => |path| blk: {
                const nodes = try self.a.alloc(*const v.Node, path.nodes.len);
                for (path.nodes, nodes) |node, *out| {
                    const visible = try self.readValue(.{ .node = node });
                    if (visible == .null) break :blk .null;
                    out.* = visible.node;
                }
                const relationships = try self.a.alloc(*const v.Relationship, path.relationships.len);
                for (path.relationships, relationships) |relationship, *out| {
                    const visible = try self.readValue(.{ .relationship = relationship });
                    if (visible == .null) break :blk .null;
                    out.* = visible.relationship;
                }
                break :blk .{ .path = .{ .nodes = nodes, .relationships = relationships } };
            },
            .list => |items| blk: {
                const visible = try self.a.alloc(v.Value, items.len);
                for (items, visible) |item, *out| out.* = try self.readValue(item);
                break :blk .{ .list = visible };
            },
            .map => |map_entries| blk: {
                const visible = try self.a.alloc(v.Entry, map_entries.len);
                for (map_entries, visible) |entry, *out| out.* = .{ .name = entry.name, .value = try self.readValue(entry.value) };
                break :blk .{ .map = visible };
            },
            else => value,
        };
    }
    fn mutationEntity(self: *Engine, expression: *const ast.Expr, row: Row) !v.Value {
        // A statement can retain its own opaque write binding even when the
        // new values fail the read policy. Reading RHS expressions still
        // projects them; only a direct mutation target bypasses projection.
        if (expression.value == .variable) return lookup(row, expression.value.variable) orelse error.UnboundCypherVariable;
        return self.eval(expression, row, null);
    }
    fn eval(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) anyerror!v.Value {
        try self.checkpoint();
        return switch (expression.value) {
            .literal => |value| switch (value) {
                .null => .null,
                .boolean => |x| .{ .boolean = x },
                .integer => |x| .{ .integer = x },
                .number => |x| .{ .number = x },
                .string => |x| .{ .string = x },
            },
            .variable => |name| try self.readValue(lookup(row, name) orelse return error.UnboundCypherVariable),
            .parameter => |name| lookup(self.parameters, name) orelse error.MissingCypherParameter,
            .property => |p| blk: {
                const value = try self.eval(p.base, row, group);
                try self.requireLiveContents(value);
                break :blk value.property(p.name);
            },
            .index => |i| blk: {
                const value = try self.eval(i.base, row, group);
                const index = try self.eval(i.index, row, group);
                if (value == .null or index == .null) break :blk .null;
                if ((value == .map or value == .node or value == .relationship) and index == .string) {
                    try self.requireLiveContents(value);
                    break :blk value.property(index.string);
                }
                if (value != .list or index != .integer) return error.CypherTypeMismatch;
                const position = if (index.integer < 0) @as(i64, @intCast(value.list.len)) + index.integer else index.integer;
                break :blk if (position < 0 or position >= value.list.len) .null else value.list[@intCast(position)];
            },
            .slice => |s| blk: {
                const value = try self.eval(s.base, row, group);
                const start = if (s.start) |bound| try self.eval(bound, row, group) else v.Value{ .integer = 0 };
                const end = if (s.end) |bound| try self.eval(bound, row, group) else v.Value{ .integer = if (value == .list) @intCast(value.list.len) else 0 };
                if (value == .null or start == .null or end == .null) break :blk .null;
                if (value != .list or start != .integer or end != .integer) return error.CypherTypeMismatch;
                const length: i128 = value.list.len;
                const first: usize = @intCast(std.math.clamp(if (start.integer < 0) length + start.integer else @as(i128, start.integer), 0, length));
                const last: usize = @intCast(std.math.clamp(if (end.integer < 0) length + end.integer else @as(i128, end.integer), 0, length));
                break :blk .{ .list = value.list[first..@max(first, last)] };
            },
            .unary => |u| blk: {
                const operand = try self.eval(u.operand, row, group);
                if (u.op == .is_null or u.op == .is_not_null) break :blk .{ .boolean = (operand == .null) == (u.op == .is_null) };
                if (operand == .null) break :blk .null;
                break :blk switch (u.op) {
                    .not => .{ .boolean = !(try operand.truth()).? },
                    .positive => if (operand == .integer or operand == .number) operand else return error.CypherTypeMismatch,
                    .negative => if (operand == .integer) .{ .integer = std.math.negate(operand.integer) catch return error.CypherNumericOverflow } else .{ .number = -(try operand.numeric()) },
                    else => unreachable,
                };
            },
            .binary => |b| try self.binary(b.op, try self.eval(b.left, row, group), try self.eval(b.right, row, group)),
            .comparison => |c| blk: {
                var previous = try self.eval(c.operands[0], row, group);
                var result: v.Value = .{ .boolean = true };
                for (c.operators, c.operands[1..]) |op, operand| {
                    const next = try self.eval(operand, row, group);
                    result = try self.binary(.@"and", result, try self.binary(op, previous, next));
                    previous = next;
                }
                break :blk result;
            },
            .list_predicate => |c| blk: {
                const input = try self.eval(c.input, row, group);
                if (input == .null) break :blk .null;
                if (input != .list) return error.CypherTypeMismatch;
                var true_count: usize = 0;
                var unknown = false;
                for (input.list) |item| {
                    try self.state();
                    const inner = try self.replace(row, c.alias, item);
                    const answer = try (try self.eval(c.predicate, inner, null)).truth();
                    if (answer == null) {
                        unknown = true;
                    } else if (answer.?) {
                        true_count += 1;
                        if (c.kind == .any) break :blk .{ .boolean = true };
                        if (c.kind == .none or (c.kind == .single and true_count > 1)) break :blk .{ .boolean = false };
                    } else if (c.kind == .all) break :blk .{ .boolean = false };
                }
                if (unknown) break :blk .null;
                break :blk .{ .boolean = switch (c.kind) {
                    .all, .none => true,
                    .any => false,
                    .single => true_count == 1,
                } };
            },
            .reduce => |c| blk: {
                var accumulator = try self.eval(c.initial, row, group);
                const input = try self.eval(c.input, row, group);
                if (input == .null) break :blk .null;
                if (input != .list) return error.CypherTypeMismatch;
                for (input.list) |item| {
                    try self.state();
                    const inner = try self.replace(try self.replace(row, c.accumulator, accumulator), c.alias, item);
                    accumulator = try self.eval(c.projection, inner, null);
                }
                break :blk accumulator;
            },
            .comprehension => |c| blk: {
                const input = try self.eval(c.input, row, group);
                if (input == .null) break :blk .null;
                if (input != .list) return error.CypherTypeMismatch;
                var values: std.ArrayList(v.Value) = .empty;
                for (input.list) |item| {
                    try self.state();
                    const inner = try self.replace(row, c.alias, item);
                    if (c.predicate) |predicate| if ((try (try self.eval(predicate, inner, null)).truth()) != true) continue;
                    try values.append(self.a, if (c.projection) |projection| try self.eval(projection, inner, null) else item);
                }
                break :blk .{ .list = try values.toOwnedSlice(self.a) };
            },
            .pattern_comprehension => |c| blk: {
                var input = row;
                if (c.path.alias) |alias| {
                    var local: std.ArrayList(v.Entry) = .empty;
                    for (row) |entry| {
                        if (!std.mem.eql(u8, entry.name, alias)) try local.append(self.a, entry);
                    }
                    input = try local.toOwnedSlice(self.a);
                }
                const matched = try self.match(&.{input}, .{ .paths = &.{c.path}, .predicate = c.predicate });
                const values = try self.a.alloc(v.Value, matched.len);
                for (matched, values) |inner, *out| {
                    try self.state();
                    out.* = try self.eval(c.projection, inner, null);
                }
                break :blk .{ .list = values };
            },
            .list => |items| blk: {
                const output = try self.a.alloc(v.Value, items.len);
                for (items, output) |item, *out| out.* = try self.eval(item, row, group);
                break :blk .{ .list = output };
            },
            .map => |items| blk: {
                const output = try self.a.alloc(v.Entry, items.len);
                for (items, output) |item, *out| out.* = .{ .name = item.name, .value = try self.eval(item.value, row, group) };
                break :blk .{ .map = output };
            },
            .map_projection => |p| blk: {
                const base = try self.eval(p.base, row, group);
                if (base == .null) break :blk .null;
                try self.requireLiveContents(base);
                const properties = switch (base) {
                    .map => |map| map,
                    .node => |node| node.properties,
                    .relationship => |relationship| relationship.properties,
                    else => return error.CypherTypeMismatch,
                };
                var output: std.ArrayList(v.Entry) = .empty;
                for (p.items) |item| if (item == .all) {
                    try output.appendSlice(self.a, properties);
                    break;
                };
                for (p.items) |item| {
                    const entry: v.Entry = switch (item) {
                        .all => continue,
                        .property => |name| .{ .name = name, .value = base.property(name) },
                        .variable => |name| .{ .name = name, .value = lookup(row, name) orelse return error.UnboundCypherVariable },
                        .entry => |entry| .{ .name = entry.name, .value = try self.eval(entry.value, row, group) },
                    };
                    var replaced = false;
                    for (output.items) |*existing| if (std.mem.eql(u8, existing.name, entry.name)) {
                        existing.value = entry.value;
                        replaced = true;
                        break;
                    };
                    if (!replaced) try output.append(self.a, entry);
                }
                break :blk .{ .map = try output.toOwnedSlice(self.a) };
            },
            .call => |c| if (aggregateName(c.name)) try self.aggregate(c, group orelse return error.CypherAggregateScope) else try self.function(c.name, c.args, row, group),
            .case_when => |c| blk: {
                const operand = if (c.operand) |x| try self.eval(x, row, group) else null;
                for (c.branches) |branch| {
                    const condition = try self.eval(branch.condition, row, group);
                    const matched = if (operand) |x| v.Value.equal(x, condition) == true else (try condition.truth()) == true;
                    if (matched) break :blk try self.eval(branch.value, row, group);
                }
                break :blk if (c.otherwise) |other| try self.eval(other, row, group) else .null;
            },
            .exists => |clauses| blk: {
                const columns = self.columns;
                defer self.columns = columns;
                const candidates = try self.run(clauses, &.{row}, false, true);
                break :blk .{ .boolean = candidates.len > 0 };
            },
        };
    }
    fn entries(self: *Engine, items: []const ast.Entry, row: Row) ![]const v.Entry {
        const output = try self.a.alloc(v.Entry, items.len);
        for (items, output) |item, *out| out.* = .{ .name = item.name, .value = try self.eval(item.value, row, null) };
        return output;
    }
    fn binary(self: *Engine, op: ast.Binary, left: v.Value, right: v.Value) !v.Value {
        if (op == .@"and" or op == .@"or" or op == .xor) {
            const l = try left.truth();
            const r = try right.truth();
            if (op == .@"and" and (l == false or r == false)) return .{ .boolean = false };
            if (op == .@"or" and (l == true or r == true)) return .{ .boolean = true };
            if (l == null or r == null) return .null;
            return .{ .boolean = if (op == .@"and") l.? and r.? else if (op == .@"or") l.? or r.? else l.? != r.? };
        }
        if (op == .in) {
            if (right == .null) return .null;
            if (right != .list) return error.CypherTypeMismatch;
            var unknown = false;
            for (right.list) |item| {
                if (v.Value.equal(left, item)) |same| {
                    if (same) return .{ .boolean = true };
                } else unknown = true;
            }
            return if (unknown) .null else .{ .boolean = false };
        }
        if (left == .null or right == .null) return .null;
        if (op == .eq or op == .neq) return if (v.Value.equal(left, right)) |same| .{ .boolean = same == (op == .eq) } else .null;
        if (op == .lt or op == .lte or op == .gt or op == .gte) {
            const order_value = v.Value.compare(left, right) orelse return .null;
            return .{ .boolean = switch (op) {
                .lt => order_value == .lt,
                .lte => order_value != .gt,
                .gt => order_value == .gt,
                .gte => order_value != .lt,
                else => unreachable,
            } };
        }
        if (op == .starts_with or op == .ends_with or op == .contains) {
            if (left != .string or right != .string) return .null;
            return .{ .boolean = if (op == .starts_with) std.mem.startsWith(u8, left.string, right.string) else if (op == .ends_with) std.mem.endsWith(u8, left.string, right.string) else std.mem.indexOf(u8, left.string, right.string) != null };
        }
        if (op == .regex) {
            if (left != .string or right != .string) return .null;
            const remaining = self.limits.intermediate_states -| self.stats.intermediate_states;
            var work: @import("regex.zig").Work = .{ .remaining = remaining, .limit_error = error.CypherStateBudgetExceeded, .ptr = self, .checkpoint = regexCheckpoint, .checkpoint_interval = 128 };
            defer self.stats.intermediate_states += remaining - work.remaining;
            return .{ .boolean = try self.regex.matches(left.string, right.string, &work) };
        }
        if (op == .add and left == .string and right == .string) return .{ .string = try std.fmt.allocPrint(self.a, "{s}{s}", .{ left.string, right.string }) };
        if (op == .add and (left == .list or right == .list)) {
            const lhs = if (left == .list) left.list else &.{left};
            const rhs = if (right == .list) right.list else &.{right};
            const items = try self.a.alloc(v.Value, lhs.len + rhs.len);
            @memcpy(items[0..lhs.len], lhs);
            @memcpy(items[lhs.len..], rhs);
            return .{ .list = items };
        }
        if (left == .integer and right == .integer and op != .power) {
            const l = left.integer;
            const r = right.integer;
            const integer = switch (op) {
                .add => std.math.add(i64, l, r),
                .subtract => std.math.sub(i64, l, r),
                .multiply => std.math.mul(i64, l, r),
                .divide => if (r == 0) return error.CypherDivisionByZero else if (l == std.math.minInt(i64) and r == -1) return error.CypherNumericOverflow else @divTrunc(l, r),
                .modulo => if (r == 0) return error.CypherDivisionByZero else if (r == -1) @as(i64, 0) else @rem(l, r),
                else => return error.CypherTypeMismatch,
            } catch return error.CypherNumericOverflow;
            return .{ .integer = integer };
        }
        const l = try left.numeric();
        const r = try right.numeric();
        if ((op == .divide or op == .modulo) and r == 0) return error.CypherDivisionByZero;
        const number = switch (op) {
            .add => l + r,
            .subtract => l - r,
            .multiply => l * r,
            .divide => l / r,
            .modulo => @rem(l, r),
            .power => std.math.pow(f64, l, r),
            else => return error.CypherTypeMismatch,
        };
        if (!std.math.isFinite(number)) return error.CypherNumericOverflow;
        return .{ .number = number };
    }
    fn function(self: *Engine, name: []const u8, args: []const *const ast.Expr, row: Row, group: ?[]const Row) !v.Value {
        if (std.ascii.eqlIgnoreCase(name, "coalesce")) {
            for (args) |arg| {
                const value = try self.eval(arg, row, group);
                if (value != .null) return value;
            }
            return .null;
        }
        if (std.ascii.eqlIgnoreCase(name, "range")) {
            const start = try self.eval(args[0], row, group);
            const end = try self.eval(args[1], row, group);
            const step = if (args.len == 3) try self.eval(args[2], row, group) else v.Value{ .integer = 1 };
            if (start == .null or end == .null or step == .null) return .null;
            if (start != .integer or end != .integer or step != .integer) return error.CypherTypeMismatch;
            if (step.integer == 0) return error.CypherInvalidRange;
            const distance: i128 = @as(i128, end.integer) - start.integer;
            if ((distance > 0 and step.integer < 0) or (distance < 0 and step.integer > 0)) return .{ .list = &.{} };
            const length = std.math.cast(usize, @divTrunc(distance, step.integer) + 1) orelse return error.CypherStateBudgetExceeded;
            if (length > self.limits.intermediate_states -| self.stats.intermediate_states) return error.CypherStateBudgetExceeded;
            self.stats.intermediate_states += length;
            const items = try self.a.alloc(v.Value, length);
            for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(@as(i128, start.integer) + @as(i128, step.integer) * i) };
            return .{ .list = items };
        }
        if (std.ascii.eqlIgnoreCase(name, "substring")) {
            const text = try self.eval(args[0], row, group);
            const start = try self.eval(args[1], row, group);
            const substring_length = if (args.len == 3) try self.eval(args[2], row, group) else v.Value{ .integer = std.math.maxInt(i64) };
            if (text == .null or start == .null or substring_length == .null) return .null;
            if (text != .string or start != .integer or substring_length != .integer) return error.CypherTypeMismatch;
            if (start.integer < 0 or substring_length.integer < 0) return error.CypherInvalidSubstring;
            var iterator = (std.unicode.Utf8View.init(text.string) catch return error.CypherTypeMismatch).iterator();
            var position: usize = 0;
            while (position < start.integer and iterator.nextCodepointSlice() != null) : (position += 1) {}
            const begin = iterator.i;
            position = 0;
            while (position < substring_length.integer and iterator.nextCodepointSlice() != null) : (position += 1) {}
            return .{ .string = try self.a.dupe(u8, text.string[begin..iterator.i]) };
        }
        if (args.len != 1) return error.CypherFunctionArity;
        const value = try self.eval(args[0], row, group);
        if (value == .null) return .null;
        if (std.ascii.eqlIgnoreCase(name, "keys") or std.ascii.eqlIgnoreCase(name, "properties") or std.ascii.eqlIgnoreCase(name, "labels")) try self.requireLiveContents(value);
        if (std.ascii.eqlIgnoreCase(name, "keys")) {
            const properties = switch (value) {
                .map => value.map,
                .node => value.node.properties,
                .relationship => value.relationship.properties,
                else => return error.CypherTypeMismatch,
            };
            const keys = try self.a.alloc(v.Value, properties.len);
            for (properties, keys) |property, *key| key.* = .{ .string = property.name };
            return .{ .list = keys };
        }
        if (std.ascii.eqlIgnoreCase(name, "reverse")) {
            if (value == .list) {
                const items = try self.a.dupe(v.Value, value.list);
                std.mem.reverse(v.Value, items);
                return .{ .list = items };
            }
            if (value != .string) return error.CypherTypeMismatch;
            const output = try self.a.alloc(u8, value.string.len);
            var iterator = (std.unicode.Utf8View.init(value.string) catch return error.CypherTypeMismatch).iterator();
            var end = output.len;
            while (iterator.nextCodepointSlice()) |character| {
                end -= character.len;
                @memcpy(output[end..][0..character.len], character);
            }
            return .{ .string = output };
        }
        if (std.ascii.eqlIgnoreCase(name, "trim") or std.ascii.eqlIgnoreCase(name, "ltrim") or std.ascii.eqlIgnoreCase(name, "rtrim")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try unicode_string.trim(value.string, !std.ascii.eqlIgnoreCase(name, "rtrim"), !std.ascii.eqlIgnoreCase(name, "ltrim")) };
        }
        if (std.ascii.eqlIgnoreCase(name, "toString")) return switch (value) {
            .string => value,
            .boolean => .{ .string = if (value.boolean) "true" else "false" },
            .integer => .{ .string = try std.fmt.allocPrint(self.a, "{d}", .{value.integer}) },
            .number => .{ .string = try std.fmt.allocPrint(self.a, "{d}", .{value.number}) },
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "toBoolean")) return switch (value) {
            .boolean => value,
            .string => if (std.ascii.eqlIgnoreCase(value.string, "true")) .{ .boolean = true } else if (std.ascii.eqlIgnoreCase(value.string, "false")) .{ .boolean = false } else .null,
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "toInteger") or std.ascii.eqlIgnoreCase(name, "toFloat")) {
            const integer = std.ascii.eqlIgnoreCase(name, "toInteger");
            if (integer and value == .integer) return value;
            if (integer and value == .boolean) return .{ .integer = @intFromBool(value.boolean) };
            if (value != .integer and value != .number and value != .string) return error.CypherTypeMismatch;
            if (integer and value == .string) if (std.fmt.parseInt(i64, value.string, 10)) |parsed| {
                return .{ .integer = parsed };
            } else |_| {};
            const number = if (value == .string) std.fmt.parseFloat(f64, value.string) catch return .null else try value.numeric();
            if (!std.math.isFinite(number)) return .null;
            if (!integer) return .{ .number = number };
            if (number >= 9223372036854775808.0 or number < -9223372036854775808.0) return .null;
            return .{ .integer = @intFromFloat(number) };
        }
        if (std.ascii.eqlIgnoreCase(name, "antfly.documentKey")) return switch (value) {
            .node => |n| .{ .string = n.identity.key },
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "elementId")) return .{ .string = switch (value) {
            .node => |n| try std.json.Stringify.valueAlloc(self.a, .{ .kind = "node", .table = n.identity.table, .key = n.identity.key }, .{}),
            .relationship => |r| try std.json.Stringify.valueAlloc(self.a, .{ .kind = "relationship", .table = r.identity.table, .index = r.index, .key = r.identity.key }, .{}),
            else => return error.CypherTypeMismatch,
        } };
        if (std.ascii.eqlIgnoreCase(name, "labels")) {
            if (value != .node) return error.CypherTypeMismatch;
            const labels = try self.a.alloc(v.Value, value.node.labels.len);
            for (value.node.labels, labels) |label, *out| out.* = .{ .string = label };
            return .{ .list = labels };
        }
        if (std.ascii.eqlIgnoreCase(name, "type")) {
            if (value != .relationship) return error.CypherTypeMismatch;
            return .{ .string = value.relationship.type };
        }
        if (std.ascii.eqlIgnoreCase(name, "properties")) return switch (value) {
            .node => |n| .{ .map = n.properties },
            .relationship => |r| .{ .map = r.properties },
            .map => value,
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "nodes")) {
            if (value != .path) return error.CypherTypeMismatch;
            const items = try self.a.alloc(v.Value, value.path.nodes.len);
            for (value.path.nodes, items) |n, *out| out.* = .{ .node = n };
            return .{ .list = items };
        }
        if (std.ascii.eqlIgnoreCase(name, "relationships")) {
            if (value != .path) return error.CypherTypeMismatch;
            const items = try self.a.alloc(v.Value, value.path.relationships.len);
            for (value.path.relationships, items) |r, *out| out.* = .{ .relationship = r };
            return .{ .list = items };
        }
        if (std.ascii.eqlIgnoreCase(name, "length")) {
            if (value != .path) return error.CypherTypeMismatch;
            return .{ .integer = @intCast(value.path.relationships.len) };
        }
        if (std.ascii.eqlIgnoreCase(name, "size")) return .{ .integer = @intCast(switch (value) {
            .list => |l| l.len,
            .string => |s| std.unicode.utf8CountCodepoints(s) catch return error.CypherTypeMismatch,
            else => return error.CypherTypeMismatch,
        }) };
        if (std.ascii.eqlIgnoreCase(name, "head") or std.ascii.eqlIgnoreCase(name, "last")) {
            if (value != .list) return error.CypherTypeMismatch;
            return if (value.list.len == 0) .null else value.list[if (std.ascii.eqlIgnoreCase(name, "head")) 0 else value.list.len - 1];
        }
        if (std.ascii.eqlIgnoreCase(name, "toLower")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try unicode_string.caseConvert(self.a, value.string, false) };
        }
        if (std.ascii.eqlIgnoreCase(name, "toUpper")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try unicode_string.caseConvert(self.a, value.string, true) };
        }
        if (std.ascii.eqlIgnoreCase(name, "abs")) return if (value == .integer) .{ .integer = std.math.cast(i64, @abs(value.integer)) orelse return error.CypherNumericOverflow } else .{ .number = @abs(try value.numeric()) };
        return error.UnknownCypherFunction;
    }
    fn aggregate(self: *Engine, call_value: ast.Function, group: []const Row) !v.Value {
        if (call_value.args.len != (if (call_value.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
        var items: std.ArrayList(v.Value) = .empty;
        for (group) |row| {
            const value = if (call_value.star) v.Value{ .integer = 1 } else try self.eval(call_value.args[0], row, null);
            if (value == .null) continue;
            if (call_value.distinct) {
                var duplicate = false;
                for (items.items) |item| if (v.Value.eql(item, value)) {
                    duplicate = true;
                    break;
                };
                if (duplicate) continue;
            }
            try items.append(self.a, value);
        }
        if (std.ascii.eqlIgnoreCase(call_value.name, "count")) return .{ .integer = @intCast(items.items.len) };
        if (std.ascii.eqlIgnoreCase(call_value.name, "collect")) return .{ .list = try items.toOwnedSlice(self.a) };
        if (items.items.len == 0) return if (std.ascii.eqlIgnoreCase(call_value.name, "sum")) .{ .integer = 0 } else .null;
        if (std.ascii.eqlIgnoreCase(call_value.name, "min") or std.ascii.eqlIgnoreCase(call_value.name, "max")) {
            var best = items.items[0];
            for (items.items[1..]) |item| {
                const order_value = try v.Value.order(item, best);
                if ((order_value == .lt and std.ascii.eqlIgnoreCase(call_value.name, "min")) or (order_value == .gt and std.ascii.eqlIgnoreCase(call_value.name, "max"))) best = item;
            }
            return best;
        }
        var sum: v.Value = .{ .integer = 0 };
        for (items.items) |item| {
            if (item != .integer and item != .number) return error.CypherTypeMismatch;
            sum = try self.binary(.add, sum, item);
        }
        if (std.ascii.eqlIgnoreCase(call_value.name, "avg")) return .{ .number = (try sum.numeric()) / @as(f64, @floatFromInt(items.items.len)) };
        return sum;
    }

    fn expressionName(self: *Engine, expression: *const ast.Expr) ![]const u8 {
        if (expression.value == .map_projection and expression.value.map_projection.base.value == .variable) return expression.value.map_projection.base.value.variable;
        if (expression.value != .variable and expression.value != .property and expression.span.end <= self.source.len) return self.a.dupe(u8, self.source[expression.span.start..expression.span.end]);
        return switch (expression.value) {
            .variable => |n| n,
            .property => |p| std.fmt.allocPrint(self.a, "{s}.{s}", .{ try self.expressionName(p.base), p.name }),
            .call => |c| std.fmt.allocPrint(self.a, "{s}({s})", .{ c.name, if (c.star) "*" else if (c.args.len == 1) try self.expressionName(c.args[0]) else "..." }),
            else => std.fmt.allocPrint(self.a, "expression@{}", .{expression.span.start}),
        };
    }
    const Projected = struct { row: Row, source: Row, group: ?[]const Row, keys: []const v.Value };
    fn project(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool) anyerror![]const Row {
        var grouped = false;
        for (projection.items) |item| grouped = grouped or aggregates(item.expression);
        if (grouped and projection.wildcard) return error.CypherAggregateWildcard;
        var groups: std.ArrayList(std.ArrayList(Row)) = .empty;
        if (grouped) {
            var keys: std.ArrayList([]const v.Value) = .empty;
            for (rows) |row| {
                var values: std.ArrayList(v.Value) = .empty;
                for (projection.items) |item| if (!aggregates(item.expression)) try values.append(self.a, try self.eval(item.expression, row, null));
                var found: ?usize = null;
                for (keys.items, 0..) |key, i| {
                    var equal = key.len == values.items.len;
                    if (equal) for (key, values.items) |x, y| if (!v.Value.eql(x, y)) {
                        equal = false;
                        break;
                    };
                    if (equal) {
                        found = i;
                        break;
                    }
                }
                if (found) |i| try groups.items[i].append(self.a, row) else {
                    try keys.append(self.a, try values.toOwnedSlice(self.a));
                    var group: std.ArrayList(Row) = .empty;
                    try group.append(self.a, row);
                    try groups.append(self.a, group);
                }
            }
            if (groups.items.len == 0) {
                var has_key = false;
                for (projection.items) |item| has_key = has_key or !aggregates(item.expression);
                if (!has_key) try groups.append(self.a, .empty);
            }
        } else for (rows) |row| {
            var group: std.ArrayList(Row) = .empty;
            try group.append(self.a, row);
            try groups.append(self.a, group);
        }
        var projected: std.ArrayList(Projected) = .empty;
        var columns: std.ArrayList([]const u8) = .empty;
        if (projection.wildcard and rows.len > 0) for (rows[0]) |entry| try columns.append(self.a, entry.name);
        for (projection.items) |item| try columns.append(self.a, item.alias orelse try self.expressionName(item.expression));
        for (groups.items) |group| {
            const source: Row = if (group.items.len > 0) group.items[0] else &.{};
            var row: std.ArrayList(v.Entry) = .empty;
            if (projection.wildcard) try row.appendSlice(self.a, source);
            for (projection.items) |item| try row.append(self.a, .{ .name = item.alias orelse try self.expressionName(item.expression), .value = try self.eval(item.expression, source, if (grouped) group.items else null) });
            const out_row = try row.toOwnedSlice(self.a);
            var scope = source;
            for (out_row) |entry| scope = try self.replace(scope, entry.name, entry.value);
            if (projection.predicate) |predicate| if ((try (try self.eval(predicate, if (grouped or projection.distinct) out_row else scope, null)).truth()) != true) continue;
            const order_keys = try self.a.alloc(v.Value, projection.order.len);
            for (projection.order, order_keys) |order_value, *key| key.* = try self.eval(order_value.expression, scope, if (grouped) group.items else null);
            if (projection.distinct) {
                var duplicate = false;
                for (projected.items) |previous| if (rowEqual(previous.row, out_row)) {
                    duplicate = true;
                    break;
                };
                if (duplicate) continue;
            }
            try projected.append(self.a, .{ .row = out_row, .source = source, .group = if (grouped) group.items else null, .keys = order_keys });
        }
        // Stable merge sort with fallible comparisons. Comparisons are computed
        // before moving rows so type errors cannot produce a partial result.
        try self.sort(projected.items, projection.order);
        const skip = if (projection.skip) |expression| try self.count(expression) else 0;
        const limit = if (projection.limit) |expression| try self.count(expression) else projected.items.len;
        const start = @min(skip, projected.items.len);
        const end = start + @min(limit, projected.items.len - start);
        const output = try self.a.alloc(Row, end - start);
        for (projected.items[start..end], output) |item, *row| row.* = item.row;
        if (final) self.columns = try columns.toOwnedSlice(self.a);
        return output;
    }
    fn rowEqual(a: Row, b: Row) bool {
        if (a.len != b.len) return false;
        for (a, b) |x, y| if (!v.Value.eql(x.value, y.value)) return false;
        return true;
    }
    fn distinct(self: *Engine, rows: []const Row) !std.ArrayList(Row) {
        var output: std.ArrayList(Row) = .empty;
        for (rows) |row| {
            var duplicate = false;
            for (output.items) |previous| if (rowEqual(previous, row)) {
                duplicate = true;
                break;
            };
            if (!duplicate) try output.append(self.a, row);
        }
        return output;
    }
    fn count(self: *Engine, expression: *const ast.Expr) !usize {
        const value = try self.eval(expression, &.{}, null);
        if (value != .integer or value.integer < 0) return error.InvalidCypherLimit;
        return @intCast(value.integer);
    }
    fn sort(self: *Engine, items: []Projected, orders: []const ast.Order) !void {
        if (orders.len == 0 or items.len < 2) return;
        const scratch = try self.a.alloc(Projected, items.len);
        var width: usize = 1;
        while (width < items.len) : (width *= 2) {
            try self.checkpoint();
            var start: usize = 0;
            while (start < items.len) : (start += 2 * width) {
                const middle = @min(start + width, items.len);
                const end = @min(start + 2 * width, items.len);
                var l = start;
                var r = middle;
                var out = start;
                while (l < middle or r < end) : (out += 1) {
                    var choose_left = r == end;
                    if (l < middle and r < end) {
                        choose_left = true;
                        for (orders, 0..) |order_value, i| {
                            const ordering = try v.Value.order(items[l].keys[i], items[r].keys[i]);
                            if (ordering != .eq) {
                                choose_left = (ordering == .lt) != order_value.descending;
                                break;
                            }
                        }
                    }
                    if (choose_left and l < middle) {
                        scratch[out] = items[l];
                        l += 1;
                    } else {
                        scratch[out] = items[r];
                        r += 1;
                    }
                }
            }
            @memcpy(items, scratch);
        }
    }

    fn call(self: *Engine, rows: []const Row, operation: ast.Procedure) ![]const Row {
        if (!std.mem.eql(u8, operation.name, "antfly.search")) return error.UnknownCypherProcedure;
        if (operation.args.len != 2) return error.CypherFunctionArity;
        const search = self.backend.vtable.search orelse return error.CypherSearchUnavailable;
        var output: std.ArrayList(Row) = .empty;
        for (rows) |row| {
            const table = try self.eval(operation.args[0], row, null);
            const request = try self.eval(operation.args[1], row, null);
            if ((table != .string and table != .map) or request != .map) return error.CypherTypeMismatch;
            const target = if (table == .string) table else table.property("table");
            const database = if (table == .map and table.property("database") != .null) table.property("database") else v.Value{ .string = "default" };
            const namespace = if (table == .map and table.property("namespace") != .null) table.property("namespace") else v.Value{ .string = "public" };
            if (target != .string or database != .string or namespace != .string) return error.CypherTypeMismatch;

            var physical: ?[]const u8 = null;
            for (self.graph.labels) |label| {
                const logical_match = std.mem.eql(u8, if (label.logical_table.len == 0) label.table else label.logical_table, target.string) and std.mem.eql(u8, label.database, database.string) and std.mem.eql(u8, label.namespace, namespace.string);
                if (!logical_match and !(table == .string and std.mem.eql(u8, label.table, table.string))) continue;
                if (physical) |previous| if (!std.mem.eql(u8, previous, label.table)) return error.CypherAmbiguousSearchTable;
                physical = label.table;
            }
            const selected = physical orelse return error.CypherSearchTableOutsideGraph;
            const page = try search(self.backend.ptr, self.a, selected, .{ .parameters = request, .max_examined = self.limits.scanned_anchors -| self.stats.scanned_anchors });
            if (page.examined > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
            self.stats.scanned_anchors += page.examined;
            self.stats.search_calls += 1;
            self.stats.search_candidates += page.seeds.len;
            self.stats.search_approximate = self.stats.search_approximate or page.approximate;
            for (page.seeds) |seed| {
                if (!std.mem.eql(u8, seed.node.identity.table, selected) or !std.math.isFinite(seed.score)) return error.CypherInvalidSearchResult;
                var result = row;
                for (operation.yields) |yield| {
                    const value: v.Value = if (std.mem.eql(u8, yield.name, "node")) .{ .node = seed.node } else if (std.mem.eql(u8, yield.name, "score")) .{ .number = seed.score } else return error.UnknownCypherYield;
                    result = (try self.bind(result, yield.alias, value)) orelse return error.CypherVariableConflict;
                }
                if (operation.predicate) |predicate| if ((try (try self.eval(predicate, result, null)).truth()) != true) continue;
                try self.state();
                try output.append(self.a, result);
            }
        }
        return output.toOwnedSlice(self.a);
    }
    fn mutate(self: *Engine, mutation: backend_mod.Mutation) !void {
        try self.checkpoint();
        const mutate_fn = self.backend.vtable.mutate orelse return error.CypherMutationsUnavailable;
        try mutate_fn(self.backend.ptr, self.a, mutation);
        const deletion: ?v.EntityIdentity = switch (mutation) {
            .delete_node => |node| .{ .node = node.identity },
            .delete_relationship => |relationship| .{ .relationship = relationship },
            else => null,
        };
        if (deletion) |identity| try self.recordDeletion(identity);
        self.stats.mutations += 1;
    }
    fn creationProperties(self: *Engine, items: []const ast.Entry, row: Row, merge: bool) ![]const v.Entry {
        const properties = try self.entries(items, row);
        if (merge) for (properties) |property| {
            if (property.value == .null) return error.CypherInvalidPropertyValue;
        };
        return properties;
    }
    fn create(self: *Engine, rows: []const Row, paths: []const ast.Path, merge: bool) anyerror![]const Row {
        var output: std.ArrayList(Row) = .empty;
        for (rows) |input| {
            var row = input;
            for (paths) |path| {
                const nodes = try self.a.alloc(*const v.Node, path.nodes.len);
                const relationships = try self.a.alloc(*const v.Relationship, path.edges.len);
                for (path.nodes, nodes) |pattern, *node| {
                    if (pattern.alias) |alias| if (lookup(row, alias)) |bound| {
                        if (bound != .node) return error.CypherTypeMismatch;
                        if (merge) _ = try self.creationProperties(pattern.properties, row, true);
                        try self.requireLiveContents(bound);
                        node.* = (try self.backend.vtable.get(self.backend.ptr, self.a, bound.node.identity)) orelse return error.CypherDeletedEntity;
                        continue;
                    };
                    if (pattern.labels.len == 0) return error.CypherCreateRequiresLabel;
                    const label = self.graph.label(pattern.labels[0]) orelse return error.UnknownGraphLabel;
                    node.* = try self.a.create(v.Node);
                    const mutable: *v.Node = @constCast(node.*);
                    mutable.* = .{ .identity = try self.backend.vtable.new_identity.?(self.backend.ptr, self.a, label.table, false), .labels = pattern.labels, .properties = try self.creationProperties(pattern.properties, row, merge) };
                    try self.mutate(.{ .create_node = mutable });
                    node.* = (try self.backend.vtable.get(self.backend.ptr, self.a, mutable.identity)) orelse return error.CypherDeletedEntity;
                    for (pattern.labels) |name| if (!self.graph.admits(node.*, name)) return error.CypherLabelPredicateViolation;
                    row = (try self.bind(row, pattern.alias, .{ .node = node.* })).?;
                }
                for (path.edges, relationships, 0..) |pattern, *relationship, i| {
                    if (pattern.variable_length or pattern.types.len != 1 or pattern.direction == .both) return error.CypherInvalidCreateRelationship;
                    const mapping = self.graph.relationship(pattern.types[0]) orelse return error.UnknownGraphRelationship;
                    const source = nodes[if (pattern.direction == .out) i else i + 1];
                    const target = nodes[if (pattern.direction == .out) i + 1 else i];
                    if (!self.graph.admits(source, mapping.source_label) or !self.graph.admits(target, mapping.target_label)) return error.CypherRelationshipEndpointMismatch;
                    const rel = try self.a.create(v.Relationship);
                    rel.* = .{ .identity = try self.backend.vtable.new_identity.?(self.backend.ptr, self.a, source.identity.table, true), .index = mapping.index, .type = mapping.name, .source = source.identity, .target = target.identity, .properties = try v.entityProperties(self.a, try self.creationProperties(pattern.properties, row, merge)) };
                    try self.mutate(.{ .create_relationship = rel });
                    relationship.* = if (self.backend.vtable.get_relationship) |get_relationship|
                        (try get_relationship(self.backend.ptr, self.a, .{ .identity = rel.identity, .index = rel.index })) orelse return error.CypherDeletedEntity
                    else
                        rel;
                    row = (try self.bind(row, pattern.alias, .{ .relationship = relationship.* })) orelse return error.CypherVariableConflict;
                }
                row = (try self.bind(row, path.alias, .{ .path = .{ .nodes = nodes, .relationships = relationships } })) orelse return error.CypherVariableConflict;
            }
            try output.append(self.a, row);
        }
        return output.toOwnedSlice(self.a);
    }
    fn appendDeleted(self: *Engine, entities: *std.ArrayList(v.EntityIdentity), value: v.Value) !void {
        if (value == .null) return;
        if (value == .relationship and value.relationship.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
        const target = try v.EntityIdentity.fromValue(value);
        for (entities.items) |prior| if (prior.eql(target)) return;
        try entities.append(self.a, target);
    }
    fn delete(self: *Engine, rows: []const Row, expressions: []const *const ast.Expr, detach: bool) ![]const Row {
        var entities: std.ArrayList(v.EntityIdentity) = .empty;
        for (rows) |row| for (expressions) |expression| {
            const value = try self.mutationEntity(expression, row);
            if (value == .path) {
                for (value.path.relationships) |relationship| try self.appendDeleted(&entities, .{ .relationship = relationship });
                for (value.path.nodes) |node| try self.appendDeleted(&entities, .{ .node = node });
            } else try self.appendDeleted(&entities, value);
        };
        // A clause deleting both an endpoint and its incident relationship
        // removes the relationship before checking the node's detach rule.
        for (entities.items) |entity| if (entity == .relationship) try self.mutate(.{ .delete_relationship = entity.relationship });
        for (entities.items) |entity| if (entity == .node) try self.mutate(.{ .delete_node = .{ .identity = entity.node, .detach = detach } });
        return rows;
    }
    fn updatedProperties(self: *Engine, base: v.Value, updates: []const v.Entry, replace_all: bool) !v.Value {
        var properties: std.ArrayList(v.Entry) = .empty;
        const previous = if (base == .node) base.node.properties else base.relationship.properties;
        if (!replace_all) for (previous) |entry| {
            if (lookup(updates, entry.name) == null) try properties.append(self.a, entry);
        };
        for (updates) |entry| if (entry.value != .null) try properties.append(self.a, entry);
        if (base == .node) {
            const node = try self.a.create(v.Node);
            node.* = base.node.*;
            node.properties = try properties.toOwnedSlice(self.a);
            var labels: std.ArrayList([]const u8) = .empty;
            for (self.graph.labels) |label| if (self.graph.admits(node, label.name)) try labels.append(self.a, label.name);
            node.labels = try labels.toOwnedSlice(self.a);
            return .{ .node = node };
        }
        const relationship = try self.a.create(v.Relationship);
        relationship.* = base.relationship.*;
        relationship.properties = try properties.toOwnedSlice(self.a);
        return .{ .relationship = relationship };
    }
    fn refreshRow(self: *Engine, row: Row, target: v.EntityIdentity, updated: v.Value) !Row {
        const result = try self.a.dupe(v.Entry, row);
        for (result) |*entry| entry.value = try self.refreshValue(entry.value, target, updated);
        return result;
    }
    fn refreshValue(self: *Engine, value: v.Value, target: v.EntityIdentity, updated: v.Value) anyerror!v.Value {
        if (value == .node or value == .relationship) return if ((try v.EntityIdentity.fromValue(value)).eql(target)) updated else value;
        return switch (value) {
            .path => |path| blk: {
                const nodes = try self.a.alloc(*const v.Node, path.nodes.len);
                for (path.nodes, nodes) |node, *out| out.* = (try self.refreshValue(.{ .node = node }, target, updated)).node;
                const relationships = try self.a.alloc(*const v.Relationship, path.relationships.len);
                for (path.relationships, relationships) |relationship, *out| out.* = (try self.refreshValue(.{ .relationship = relationship }, target, updated)).relationship;
                break :blk .{ .path = .{ .nodes = nodes, .relationships = relationships } };
            },
            .list => |items| blk: {
                const out = try self.a.alloc(v.Value, items.len);
                for (items, out) |item, *cell| cell.* = try self.refreshValue(item, target, updated);
                break :blk .{ .list = out };
            },
            .map => |map_entries| .{ .map = try self.refreshRow(map_entries, target, updated) },
            else => value,
        };
    }
    fn assign(self: *Engine, rows: []const Row, assignments: []const ast.Assignment) ![]const Row {
        const output = try self.a.dupe(Row, rows);
        for (output) |*row| for (assignments) |assignment| {
            const base_expression = if (assignment.target.value == .property) assignment.target.value.property.base else assignment.target;
            const base = try self.mutationEntity(base_expression, row.*);
            if (base == .null) continue;
            if (base != .node and base != .relationship) return error.CypherTypeMismatch;
            try self.requireLiveContents(base);
            if (base == .relationship and base.relationship.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
            const value = try self.eval(assignment.value, row.*, null);
            const properties: []const v.Entry = if (assignment.target.value == .property) try self.a.dupe(v.Entry, &.{.{ .name = assignment.target.value.property.name, .value = value }}) else if (value == .map) value.map else return error.CypherTypeMismatch;
            const target = try v.EntityIdentity.fromValue(base);
            const replace_all = assignment.target.value != .property and !assignment.merge;
            try self.mutate(.{ .set_properties = .{ .target = target, .properties = properties, .replace = replace_all } });
            const updated: v.Value = if (base == .node) .{ .node = (try self.backend.vtable.get(self.backend.ptr, self.a, base.node.identity)) orelse return error.CypherDeletedEntity } else try self.updatedProperties(base, properties, replace_all);
            for (output) |*affected| affected.* = try self.refreshRow(affected.*, target, updated);
        };
        return output;
    }
    fn remove(self: *Engine, rows: []const Row, expressions: []const *const ast.Expr) ![]const Row {
        const output = try self.a.dupe(Row, rows);
        for (output) |*row| for (expressions) |expression| {
            if (expression.value != .property) return error.CypherRemoveRequiresProperty;
            const property = expression.value.property;
            const base = try self.mutationEntity(property.base, row.*);
            if (base == .null) continue;
            if (base != .node and base != .relationship) return error.CypherTypeMismatch;
            try self.requireLiveContents(base);
            if (base == .relationship and base.relationship.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
            const target = try v.EntityIdentity.fromValue(base);
            try self.mutate(.{ .remove_properties = .{ .target = target, .names = &.{property.name} } });
            const updated: v.Value = if (base == .node) .{ .node = (try self.backend.vtable.get(self.backend.ptr, self.a, base.node.identity)) orelse return error.CypherDeletedEntity } else try self.updatedProperties(base, &.{.{ .name = property.name, .value = .null }}, false);
            for (output) |*affected| affected.* = try self.refreshRow(affected.*, target, updated);
        };
        return output;
    }
};
