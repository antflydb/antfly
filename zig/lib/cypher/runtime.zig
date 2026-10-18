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
const numbers = @import("numbers.zig");
const Budget = @import("budget.zig");
const value_work = @import("value_work.zig");
const A = std.mem.Allocator;
const Count = @import("multiplicity.zig").Count;
pub const Row = []const v.Entry;
pub const max_response_bytes: usize = 32 << 20;
pub const Limits = struct { memory_bytes: usize = 64 << 20, scanned_anchors: usize = 1_000_000, explored_edges: usize = 1_000_000, intermediate_states: usize = 1_000_000, path_depth: usize = 1024, recursion_depth: usize = 64 };
/// A presence consumer is insensitive to duplicates of an identical source row.
/// It receives unit contributions, including when exact path counts overflow.
/// Capped consumers receive lower bounds at least as large as i64 overflow.
/// Native finite windows count through maxInt(u64), covering SKIP+LIMIT for
/// every signed row goal. Sentinels must never be treated as exact SUM weights.
pub const Multiplicity = enum { exact, presence, capped };
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
    // Scratch frees must reach a reclaiming allocator, even when the host
    // supplied its request arena. Retaining those freed pages in the host
    // would make physical memory grow with every streamed/RPC page.
    const allocator = backend_mod.reclaimingAllocator(backing);
    const budget = try allocator.create(Budget);
    budget.* = .{ .backing = allocator, .limit = limits.memory_bytes };
    errdefer allocator.destroy(budget);
    return executeBudget(budget, backend, graph, compiled, parameters, limits) catch |err| return if (budget.exhausted) error.CypherMemoryBudgetExceeded else err;
}

/// Pull execution for row-local node scans and all-path traversals. Stateful reductions, path
/// selection and nested patterns use the general statement operators. Every
/// pull shares one quota/work meter and immutable read cut. Suspended routes
/// pin their adjacency pages; returned rows belong to the caller.
pub const NodeCursor = struct {
    backing: A,
    budget: *Budget,
    arena: std.heap.ArenaAllocator,
    engine: *Engine,
    query: ast.Query,
    original_match: ast.Match,
    fields: ?[]const []const u8,
    columns: []const []const u8,
    started: bool = false,
    done: bool = false,
    exact_count: ?u64 = null,
    interior_anchors: []const usize = &.{},
    frames: std.ArrayList(*PathFrame) = .empty,
    weighted: bool = false,
    multiplicity: Multiplicity = .exact,
    weighted_seeds: ?Engine.AnchorBatch = null,
    fallback_seed: ?usize = null,
    weighted_arena: std.heap.ArenaAllocator,
    adjacency_cache: Engine.AdjacencyCache,
    pending: std.ArrayList(struct { row: Row, weight: u64 }) = .empty,
    pending_position: usize = 0,
    last_weights: std.ArrayList(u64) = .empty,
    pub const WeightPlan = struct { path: ast.Path, projection: ast.Projection, multiplicity: Multiplicity };
    /// Plan a projected endpoint bag without advancing or rebinding the read
    /// cut. Plans and their AST storage belong to this cursor.
    pub fn planWeightFields(self: *NodeCursor, fields: []const []const u8, multiplicity: Multiplicity) !?WeightPlan {
        if (self.started or self.done or self.exact_count != null) return null;
        const operation = self.original_match;
        if (!self.componentCursorSafe(operation)) return null;
        if (operation.paths.len == 1 and self.interior_anchors[0] != 0) return null;
        const path = (try self.engine.linearComponent(operation.paths)) orelse return null;
        if (path.edges.len == 0 or path.execution_prefix) return null;
        if (path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1) return null;
        var projection = self.query.clauses[1].operation.project.value;
        var items: std.ArrayList(ast.Item) = .empty;
        for (projection.items) |item| {
            const name = item.alias orelse try self.engine.expressionName(item.expression);
            for (fields) |field| if (std.mem.eql(u8, name, field)) {
                try items.append(self.engine.a, item);
                break;
            };
        }
        if (items.items.len != fields.len) return null;
        if (items.items.len == 0) {
            const one = try self.engine.a.create(ast.Expr);
            one.* = .{ .span = .{}, .value = .{ .literal = .{ .integer = 1 } } };
            try items.append(self.engine.a, .{ .expression = one, .alias = "_antfly_weight" });
        }
        projection.items = items.items;
        try self.engine.state();
        const planned = try self.engine.reductionPath(path, &.{}, operation, projection, &.{});
        if (!try self.engine.batchEligible(planned, &.{}, operation, projection, &.{})) return null;
        return .{ .path = planned, .projection = projection, .multiplicity = multiplicity };
    }
    // Ordinary pull fallback traverses the accepted normalized path. Admit
    // only MATCH-wide relationship histories whose depth bound remains valid
    // after concatenation. Per-path node uniqueness and unbounded composite
    // depth retain the original multi-pattern cursor.
    fn componentCursorSafe(self: *NodeCursor, operation: ast.Match) bool {
        if (operation.paths.len == 1) return true;
        if (operation.mode == .node_simple) return false;
        var maximum: usize = 0;
        for (operation.paths) |path| for (path.edges) |edge| {
            maximum = std.math.add(usize, maximum, edge.max_hops orelse return false) catch return false;
            if (maximum > self.engine.limits.path_depth) return false;
        };
        return true;
    }
    /// Serial aggregate passes share every quota, backend capability and raw
    /// adjacency page. Exhaust the preceding pass before changing its shape.
    pub fn startWeightPlan(self: *NodeCursor, plan: WeightPlan) !void {
        if (self.started and !self.done) return error.CypherInvalidBackendPage;
        if (self.frames.items.len != 0) return error.CypherInvalidBackendPage;
        if (self.weighted_seeds) |*seeds| if (seeds.arena) |*arena| arena.deinit();
        self.weighted_seeds = null;
        self.fallback_seed = null;
        _ = self.weighted_arena.reset(.retain_capacity);
        self.pending = .empty;
        self.pending_position = 0;
        self.last_weights = .empty;
        self.started = false;
        self.done = false;
        const clauses = try self.engine.a.dupe(ast.Clause, self.query.clauses);
        clauses[0].operation.match = self.original_match;
        clauses[0].operation.match.paths = try self.engine.a.dupe(ast.Path, &.{plan.path});
        self.interior_anchors = try self.engine.a.dupe(usize, &.{0});
        clauses[1].operation.project.value = plan.projection;
        self.query.clauses = clauses;
        const columns = try self.engine.a.alloc([]const u8, plan.projection.items.len);
        for (plan.projection.items, columns) |item, *name| name.* = item.alias orelse try self.engine.expressionName(item.expression);
        self.columns = columns;
        self.weighted = true;
        self.multiplicity = plan.multiplicity;
        const predicates = try self.engine.batchPredicates(plan.path, &.{}, clauses[0].operation.match.predicate);
        self.weighted_seeds = .{ .admission = predicates.admission, .residual = predicates.residual, .path = plan.path, .input = .{ .row = &.{} }, .mode = clauses[0].operation.match.mode, .budget = .{ .backing = self.budget.allocator(), .limit = @min(8 << 20, self.engine.limits.memory_bytes / 16) } };
    }
    /// Negotiate endpoint bags before the first pull. Route-observing queries
    /// retain the ordinary cursor; every declined per-anchor proof does too.
    pub fn enableWeights(self: *NodeCursor, multiplicity: Multiplicity) !bool {
        if (self.started or self.done or self.exact_count != null) return false;
        const operation = self.original_match;
        if (!self.componentCursorSafe(operation)) return false;
        if (operation.paths.len == 1 and self.interior_anchors[0] != 0) return false;
        const path = (try self.engine.linearComponent(operation.paths)) orelse return false;
        if (path.edges.len == 0 or path.execution_prefix) return false;
        if (path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1) return false;
        if (operation.predicate) |predicate| if (!Engine.endpointExpression(predicate, path, &.{})) return false;
        const projection = self.query.clauses[1].operation.project.value;
        if (projection.predicate) |predicate| if (!Engine.endpointExpression(predicate, path, &.{})) return false;
        for (projection.items) |item| if (!Engine.endpointExpression(item.expression, path, &.{})) return false;
        for (path.nodes) |node| for (node.properties) |entry| if (self.engine.constant(entry.value, &.{}) == null) return false;
        for (path.edges) |edge| for (edge.properties) |entry| if (self.engine.constant(entry.value, &.{}) == null) return false;
        const planned = try self.engine.reductionPath(path, &.{}, operation, projection, &.{});
        const clauses = try self.engine.a.dupe(ast.Clause, self.query.clauses);
        clauses[0].operation.match = self.original_match;
        clauses[0].operation.match.paths = try self.engine.a.dupe(ast.Path, &.{planned});
        self.interior_anchors = try self.engine.a.dupe(usize, &.{0});
        self.query.clauses = clauses;
        if (try self.engine.batchEligible(planned, &.{}, operation, projection, &.{})) {
            const predicates = try self.engine.batchPredicates(planned, &.{}, operation.predicate);
            self.weighted_seeds = .{ .admission = predicates.admission, .residual = predicates.residual, .path = planned, .input = .{ .row = &.{} }, .mode = operation.mode, .budget = .{ .backing = self.budget.allocator(), .limit = @min(8 << 20, self.engine.limits.memory_bytes / 16) } };
        }
        self.weighted = true;
        self.multiplicity = multiplicity;
        return true;
    }
    fn tryWeighted(self: *NodeCursor, input: Engine.MatchState, path: ast.Path, starts: []const *const v.Node) !bool {
        _ = self.weighted_arena.reset(.retain_capacity);
        self.pending = .empty;
        self.pending_position = 0;
        const e = self.engine;
        e.weighted_sink = .{ .ptr = self, .emit = emitWeighted, .multiplicity = self.multiplicity };
        e.stream_predicate = if (self.weighted_seeds != null and input.row.len == 0) self.weighted_seeds.?.residual else self.query.clauses[0].operation.match.predicate;
        defer {
            e.weighted_sink = null;
            e.stream_predicate = null;
        }
        var ignored: std.ArrayList(Engine.MatchState) = .empty;
        return e.countAllPaths(input, path, self.query.clauses[0].operation.match.mode, starts, &ignored);
    }
    fn clearSeeds(self: *NodeCursor) void {
        const seeds = &self.weighted_seeds.?;
        if (seeds.arena) |*arena| arena.deinit();
        seeds.arena = null;
        seeds.nodes = .empty;
        self.fallback_seed = null;
    }
    fn reduceSeeds(self: *NodeCursor) !void {
        const seeds = &self.weighted_seeds.?;
        if (seeds.nodes.items.len == 0) {
            // Admission may have partially cloned the first root. No retained
            // node can borrow this storage, so release it before ordinary pulls.
            self.clearSeeds();
            return;
        }
        if (try self.tryWeighted(seeds.input, seeds.path, seeds.nodes.items)) {
            self.clearSeeds();
        } else {
            // Proofs decline before emitting. Retain owned roots until each
            // ordinary frame has finished, including across caller pulls.
            self.fallback_seed = 0;
        }
    }
    fn emitWeighted(ptr: *anyopaque, row: Row, weight: u64) !void {
        const self: *NodeCursor = @ptrCast(@alignCast(ptr));
        const e = self.engine;
        for (try e.projectUnordered(&.{row}, self.query.clauses[1].operation.project.value, false)) |projected| {
            try e.values.output(.{ .map = projected });
            const a = self.weighted_arena.allocator();
            try self.pending.append(a, .{ .row = (try (v.Value{ .map = projected }).clone(a)).map, .weight = weight });
        }
    }

    pub fn open(backing: A, backend: backend_mod.Backend, graph: catalog.Graph, query: ast.Query, parameters: []const v.Entry, limits: Limits) !?*NodeCursor {
        if (query.explain or query.unions.len != 0 or query.clauses.len != 2 or query.clauses[0].operation != .match or query.clauses[1].operation != .project) return null;
        const operation = query.clauses[0].operation.match;
        const projection = query.clauses[1].operation.project.value;
        if (operation.optional or operation.paths.len == 0 or projection.distinct or projection.wildcard or projection.order.len != 0 or projection.skip != null or projection.limit != null) return null;
        if (operation.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        if (projection.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        for (operation.paths) |path| {
            if (path.selector != .all) return null;
            for (path.nodes) |node| for (node.properties) |entry| if (@import("expressions.zig").containsPattern(entry.value)) return null;
            for (path.edges) |edge| for (edge.properties) |entry| if (@import("expressions.zig").containsPattern(entry.value)) return null;
        }
        for (projection.items) |item| if (aggregates(item.expression) or @import("expressions.zig").containsPattern(item.expression)) return null;
        try graph.validate();
        try @import("binder.zig").validate(backing, graph, query);
        const allocator = backend_mod.reclaimingAllocator(backing);
        const self = try allocator.create(NodeCursor);
        errdefer allocator.destroy(self);
        const budget = try allocator.create(Budget);
        errdefer allocator.destroy(budget);
        budget.* = .{ .backing = allocator, .limit = limits.memory_bytes };
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        var arena_moved = false;
        errdefer if (arena_moved) self.arena.deinit() else arena.deinit();
        const a = arena.allocator();
        const owned_query = try std.json.parseFromSliceLeaky(ast.Query, a, try std.json.Stringify.valueAlloc(a, query, .{}), .{});
        const owned_parameters = try a.alloc(v.Entry, parameters.len);
        for (parameters, owned_parameters) |parameter, *out| out.* = .{ .name = try a.dupe(u8, parameter.name), .value = try parameter.value.clone(a) };
        const engine = try a.create(Engine);
        engine.* = .{ .a = a, .planning_allocator = a, .scratch_allocator = budget.allocator(), .backend = backend, .graph = graph, .source = owned_query.source, .parameters = owned_parameters, .limits = limits, .read_only = true, .cache_reads = true, .regex = .{ .a = a, .scratch = std.heap.ArenaAllocator.init(budget.allocator()) } };
        var aliases: std.ArrayList([]const u8) = .empty;
        for (owned_query.clauses[0].operation.match.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| try aliases.append(a, alias);
            for (path.edges) |edge| if (edge.alias) |alias| try aliases.append(a, alias);
            if (path.alias) |alias| try aliases.append(a, alias);
        }
        engine.match_aliases = try aliases.toOwnedSlice(a);
        engine.values = .{ .a = a, .ptr = engine, .charge = Engine.chargeWork };
        engine.strings = .{ .a = a, .ptr = engine, .charge = Engine.chargeWork };
        errdefer engine.regex.deinit();
        const columns = try a.alloc([]const u8, projection.items.len);
        for (owned_query.clauses[1].operation.project.value.items, columns) |item, *out| out.* = item.alias orelse try engine.expressionName(item.expression);
        const fields = try @import("read_fields.zig").required(a, owned_query, graph);
        self.* = .{ .backing = allocator, .budget = budget, .arena = arena, .engine = engine, .query = owned_query, .original_match = owned_query.clauses[0].operation.match, .weighted_arena = .init(budget.allocator()), .adjacency_cache = .{ .budget = .{ .backing = budget.allocator(), .limit = @min(8 << 20, limits.memory_bytes / 16) } }, .fields = fields, .columns = columns };
        arena_moved = true;
        engine.adjacency_cache = &self.adjacency_cache;
        engine.a = self.arena.allocator();
        engine.planning_allocator = engine.a;
        engine.values.a = engine.a;
        engine.strings.a = engine.a;
        engine.regex.a = engine.a;
        // Binding can retain an allocator and staged graph state. Its arena
        // must have a stable address for the whole cursor, and rebinding must
        // not invalidate routes suspended between pulls.
        try backend.vtable.bind(backend.ptr, self.arena.allocator(), graph);
        if (!try self.planAnchor()) {
            self.close();
            return null;
        }
        if (backend.vtable.startup_work) |work| {
            const initial = work(backend.ptr);
            if (initial.scanned_anchors > limits.scanned_anchors) return error.CypherAnchorBudgetExceeded;
            if (initial.explored_edges > limits.explored_edges) return error.CypherEdgeBudgetExceeded;
            try Engine.chargeWork(engine, initial.intermediate_states);
            engine.startup_accounted = initial;
            engine.stats.scanned_anchors = initial.scanned_anchors;
            engine.stats.explored_edges = initial.explored_edges;
        }
        return self;
    }
    fn planAnchor(self: *NodeCursor) !bool {
        const e = self.engine;
        const operation = self.query.clauses[0].operation.match;
        const paths = try e.a.dupe(ast.Path, operation.paths);
        var shape: Row = &.{};
        const interior_anchors = try e.a.alloc(usize, paths.len);
        @memset(interior_anchors, 0);
        self.interior_anchors = interior_anchors;
        for (paths, 0..) |*planned, path_index| {
            const original = planned.*;
            const input_shape = shape;
            for (original.nodes) |node| if (node.alias) |alias| {
                shape = try e.replace(shape, alias, .null);
            };
            for (original.edges) |edge| if (edge.alias) |alias| {
                shape = try e.replace(shape, alias, .null);
            };
            if (original.alias) |alias| {
                shape = try e.replace(shape, alias, .null);
            }
            var anchor: usize = 0;
            var score = try e.anchorScore(original.nodes[0], operation.predicate, input_shape);
            for (original.nodes[1..], 1..) |node, index| {
                const candidate = try e.anchorScore(node, operation.predicate, input_shape);
                if (candidate > score) {
                    anchor = index;
                    score = candidate;
                }
            }
            if (anchor == 0) continue;
            const nodes = try e.a.dupe(ast.Node, original.nodes);
            const edges = try e.a.dupe(ast.Edge, original.edges);
            std.mem.reverse(ast.Node, nodes[0 .. anchor + 1]);
            std.mem.reverse(ast.Edge, edges[0..anchor]);
            for (edges[0..anchor]) |*edge| edge.direction = switch (edge.direction) {
                .out => .in,
                .in => .out,
                .both => .both,
            };
            var path = original;
            path.nodes = nodes;
            path.edges = edges;
            // Traverse the reversed prefix from the selective anchor, then
            // resume its suffix. Frames retain the complete uniqueness history.
            path.execution_reversed = anchor + 1 == original.nodes.len;
            if (!path.execution_reversed) interior_anchors[path_index] = anchor;
            planned.* = path;
        }
        const clauses = try e.a.dupe(ast.Clause, self.query.clauses);
        clauses[0].operation.match.paths = paths;
        self.query.clauses = clauses;
        return true;
    }
    pub fn next(self: *NodeCursor, a: A, demand: usize) ![]const Row {
        return self.pull(a, demand) catch |err| return if (self.budget.exhausted) error.CypherMemoryBudgetExceeded else err;
    }
    fn pull(self: *NodeCursor, a: A, demand: usize) ![]const Row {
        if (demand == 0 or demand > 4096) return error.InvalidCypherLimit;
        if (self.done) return &.{};
        const e = self.engine;
        const backend = e.backend;
        try backend.vtable.checkpoint(backend.ptr);
        if (backend.vtable.set_work) |configure| configure(backend.ptr, .{ .ptr = e, .charge = Engine.chargeWork, .value = Engine.chargeValue, .remaining = Engine.remainingWork });
        defer if (backend.vtable.set_work) |configure| configure(backend.ptr, null);
        if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, self.fields);
        defer if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, null);
        var output: std.ArrayList(Row) = .empty;
        self.last_weights = .empty;
        if (!self.started) {
            try self.pushScan(.{ .row = &.{} }, 0);
            self.started = true;
        }
        try self.pullPaths(a, demand, &output);
        // Retire known-exhausted control frames without another physical pull.
        // A final nonempty page must not advertise a spurious continuation.
        while (self.frames.items.len != 0) {
            const frame = self.frames.items[self.frames.items.len - 1];
            const path = self.query.clauses[0].operation.match.paths[frame.path_index];
            const exhausted_scan = frame.scanning and frame.anchor_position == frame.anchors.len and frame.label == e.graph.labels.len and
                (self.weighted_seeds == null or self.weighted_seeds.?.nodes.items.len == 0);
            const exhausted_route = !frame.scanning and frame.edge_index < path.edges.len and frame.endpoint_tried and
                (frame.mapping == e.graph.relationships.len or (path.edges[frame.edge_index].max_hops != null and frame.hops >= path.edges[frame.edge_index].max_hops.?));
            if (!frame.complete and !exhausted_scan and !exhausted_route) break;
            self.popFrame();
        }
        self.done = self.frames.items.len == 0 and self.pending_position == self.pending.items.len;
        return output.toOwnedSlice(a);
    }
    // A frame pins its parent's route and current adjacency page. Only the top
    // frame advances; pages are reclaimed once all children have been consumed.
    // Suspension retains the current route and pages, with no replay on the next pull.
    const PathFrame = struct {
        arena: std.heap.ArenaAllocator,
        page_arena: std.heap.ArenaAllocator,
        state: Engine.MatchState,
        path_index: usize = 0,
        scanning: bool = false,
        anchors: []const *const v.Node = &.{},
        anchor_position: usize = 0,
        label: usize = 0,
        continuation: bool = false,
        complete: bool = false,
        nodes: []const *const v.Node,
        relationships: []const *const v.Relationship,
        edge_values: []const v.Value = &.{},
        edge_index: usize = 0,
        hops: u32 = 0,
        endpoint_tried: bool = false,
        mapping: usize = 0,
        items: []const *const v.Relationship = &.{},
        read_nodes: @FieldType(Engine, "read_nodes") = .empty,
        position: usize = 0,
        loaded: bool = false,
        last_page: bool = false,
        after: std.ArrayList(u8) = .empty,
    };
    fn newFrame(self: *NodeCursor) !*PathFrame {
        const a = self.budget.allocator();
        const frame = try a.create(PathFrame);
        frame.* = .{ .arena = std.heap.ArenaAllocator.init(a), .page_arena = std.heap.ArenaAllocator.init(a), .state = .{ .row = &.{} }, .nodes = &.{}, .relationships = &.{} };
        return frame;
    }
    fn freeFrame(self: *NodeCursor, frame: *PathFrame) void {
        frame.after.deinit(self.budget.allocator());
        frame.page_arena.deinit();
        frame.arena.deinit();
        self.budget.allocator().destroy(frame);
    }
    fn popFrame(self: *NodeCursor) void {
        self.freeFrame(self.frames.pop().?);
    }
    fn pushScan(self: *NodeCursor, state: Engine.MatchState, path_index: usize) !void {
        const frame = try self.newFrame();
        errdefer self.freeFrame(frame);
        frame.state = state;
        frame.path_index = path_index;
        frame.scanning = true;
        try self.frames.append(self.budget.allocator(), frame);
    }
    fn scanAnchor(self: *NodeCursor, frame: *PathFrame, demand: usize) !void {
        const e = self.engine;
        const operation = self.query.clauses[0].operation.match;
        const path = operation.paths[frame.path_index];
        const pattern = path.nodes[0];
        if (self.fallback_seed) |position| {
            const seeds = &self.weighted_seeds.?;
            if (position < seeds.nodes.items.len) {
                self.fallback_seed = position + 1;
                try self.pushAnchor(frame, seeds.nodes.items[position], false);
                return;
            }
            self.clearSeeds();
        }
        if (frame.anchor_position < frame.anchors.len) {
            const raw = frame.anchors[frame.anchor_position];
            frame.anchor_position += 1;
            try self.pushAnchor(frame, raw, true);
            return;
        }
        if (frame.label == e.graph.labels.len) {
            if (self.weighted_seeds) |seeds| if (seeds.nodes.items.len != 0) {
                try self.reduceSeeds();
                return;
            };
            self.popFrame();
            return;
        }
        _ = frame.page_arena.reset(.retain_capacity);
        const scope = Engine.EvalScope.enter(e, frame.page_arena.allocator());
        defer scope.leave(e);
        const requested = if (path.edges.len == 0 and frame.path_index + 1 == operation.paths.len) @min(128, demand) else 1;
        const bound = if (pattern.alias) |alias| lookup(frame.state.row, alias) else null;
        const page: backend_mod.Page(*const v.Node) = if (bound) |value| blk: {
            frame.label = e.graph.labels.len;
            break :blk .{ .items = if (value == .node) &.{value.node} else &.{}, .examined = 0 };
        } else blk: {
            if (!e.firstTableLabel(frame.label, pattern)) {
                frame.label += 1;
                return;
            }
            const table = e.graph.labels[frame.label].table;
            if (e.anchorKey(operation.predicate, pattern.alias, frame.state.row)) |key| {
                frame.label += 1;
                if (key != .string) return;
                if (e.stats.scanned_anchors == e.limits.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                const value = try e.getNode(.{ .table = table, .key = key.string });
                break :blk .{ .items = if (value) |node| &.{node} else &.{}, .examined = 1 };
            }
            // Traversal/join anchors stay narrow; terminal node scans retain
            // one bounded page to amortize native and distributed reads.
            const page = try e.scanNodes(table, pattern, operation.predicate, frame.state.row, if (frame.continuation) frame.after.items else null, requested);
            if (page.items.len > requested) return error.CypherInvalidBackendPage;
            if (frame.continuation) if (page.next) |cursor| if (std.mem.eql(u8, cursor, frame.after.items)) return error.CypherInvalidBackendCursor;
            frame.after.clearRetainingCapacity();
            frame.continuation = page.next != null;
            if (page.next) |cursor| try frame.after.appendSlice(self.budget.allocator(), cursor) else frame.label += 1;
            break :blk page;
        };
        const examined = page.examined orelse page.items.len;
        if (bound == null and examined < page.items.len) return error.CypherInvalidBackendPage;
        if (examined > e.limits.scanned_anchors -| e.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
        e.stats.scanned_anchors += examined;
        frame.anchors = page.items;
        frame.anchor_position = 0;
    }
    fn pushAnchor(self: *NodeCursor, frame: *PathFrame, raw: *const v.Node, collect: bool) !void {
        const e = self.engine;
        const pattern = self.query.clauses[0].operation.match.paths[frame.path_index].nodes[0];
        var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
        defer scratch.deinit();
        const scope = Engine.EvalScope.enter(e, scratch.allocator());
        defer scope.leave(e);
        try e.state();
        if (!try e.nodeMatches(raw, pattern)) return;
        const child = try self.newFrame();
        var retained = false;
        defer if (!retained) self.freeFrame(child);
        const child_scope = Engine.EvalScope.enter(e, child.arena.allocator());
        defer child_scope.leave(e);
        const value = try (v.Value{ .node = raw }).clone(e.a);
        const row = (try e.bind(frame.state.row, pattern.alias, value)) orelse return;
        var state = frame.state;
        state.row = row;
        child.state = (try e.constrain(state, value, pattern.properties)) orelse return;
        if (!try e.constraintsMatch(child.state.constraints, row, true)) return;
        if (self.weighted_seeds) |seeds| if (seeds.admission) |predicate| if ((try (try e.eval(predicate, row, null)).truth()) != true) return;
        if (collect) if (self.weighted_seeds) |*seeds| if (!seeds.disabled) {
            seeds.append(value.node) catch |err| {
                if (err != error.OutOfMemory or !seeds.budget.exhausted) return err;
                seeds.disabled = true;
                // This root still belongs to the pinned scan page. Replay it
                // after flushing the retained prefix, without reading it again.
                frame.anchor_position -= 1;
                try self.reduceSeeds();
                return;
            };
            if (seeds.nodes.items.len == 128) try self.reduceSeeds();
            return;
        };
        if (self.weighted) {
            const operation = self.query.clauses[0].operation.match;
            if (try self.tryWeighted(child.state, operation.paths[frame.path_index], &.{value.node})) return;
        }
        child.path_index = frame.path_index;
        child.nodes = try e.a.dupe(*const v.Node, &.{value.node});
        try self.frames.append(self.budget.allocator(), child);
        retained = true;
    }
    fn pullPaths(self: *NodeCursor, a: A, demand: usize, output: *std.ArrayList(Row)) !void {
        const e = self.engine;
        const operation = self.query.clauses[0].operation.match;
        while ((self.frames.items.len != 0 or self.pending_position < self.pending.items.len) and output.items.len < demand) {
            try e.checkpoint();
            if (self.pending_position < self.pending.items.len) {
                const pending = self.pending.items[self.pending_position];
                self.pending_position += 1;
                try output.append(a, (try (v.Value{ .map = pending.row }).clone(a)).map);
                try self.last_weights.append(a, pending.weight);
                continue;
            }
            const frame = self.frames.items[self.frames.items.len - 1];
            if (frame.scanning) {
                try self.scanAnchor(frame, demand - output.items.len);
                continue;
            }
            if (frame.complete) {
                self.popFrame();
                continue;
            }
            const path = operation.paths[frame.path_index];
            var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
            defer scratch.deinit();
            const scope = Engine.EvalScope.enter(e, scratch.allocator());
            defer scope.leave(e);
            if (frame.edge_index == path.edges.len) {
                try e.state();
                const completed_row = blk: {
                    const completed_scope = Engine.EvalScope.enter(e, frame.arena.allocator());
                    defer completed_scope.leave(e);
                    var route: v.Path = .{ .nodes = frame.nodes, .relationships = frame.relationships };
                    if (path.execution_reversed and path.alias != null) {
                        const nodes = try e.a.dupe(*const v.Node, route.nodes);
                        const relationships = try e.a.dupe(*const v.Relationship, route.relationships);
                        std.mem.reverse(*const v.Node, nodes);
                        std.mem.reverse(*const v.Relationship, relationships);
                        route = .{ .nodes = nodes, .relationships = relationships };
                    }
                    break :blk try e.bind(frame.state.row, path.alias, .{ .path = route });
                };
                if (completed_row) |row| {
                    const last = frame.path_index + 1 == operation.paths.len;
                    if (try e.constraintsMatch(frame.state.constraints, row, !last)) {
                        if (!last) {
                            frame.complete = true;
                            var state = frame.state;
                            state.row = row;
                            try self.pushScan(state, frame.path_index + 1);
                            continue;
                        }
                        const accepted = if (operation.predicate) |predicate| (try (try e.eval(predicate, row, null)).truth()) == true else true;
                        if (accepted) for (try e.projectUnordered(&.{row}, self.query.clauses[1].operation.project.value, false)) |projected| {
                            try e.values.output(.{ .map = projected });
                            try output.append(a, (try (v.Value{ .map = projected }).clone(a)).map);
                            if (self.weighted) try self.last_weights.append(a, 1);
                        };
                    }
                }
                self.popFrame();
                continue;
            }
            const edge = path.edges[frame.edge_index];
            const current = frame.nodes[frame.nodes.len - 1];
            if (!frame.endpoint_tried) {
                frame.endpoint_tried = true;
                if (frame.hops >= edge.min_hops and try e.nodeMatches(current, path.nodes[frame.edge_index + 1])) {
                    const child = try self.newFrame();
                    var retained = false;
                    defer if (!retained) self.freeFrame(child);
                    const child_scope = Engine.EvalScope.enter(e, child.arena.allocator());
                    defer child_scope.leave(e);
                    if (try e.bind(frame.state.row, path.nodes[frame.edge_index + 1].alias, .{ .node = current })) |node_row| {
                        var edge_path = path;
                        if (self.interior_anchors[frame.path_index] != 0 and frame.edge_index < self.interior_anchors[frame.path_index]) edge_path.execution_reversed = true;
                        const edge_value = try e.routeEdgeValue(edge_path, edge, frame.edge_values, frame.relationships);
                        if (try e.bind(node_row, edge.alias, edge_value)) |edge_row| {
                            if (try e.constrain(.{ .row = edge_row, .used = frame.state.used, .constraints = frame.state.constraints }, .{ .node = current }, path.nodes[frame.edge_index + 1].properties)) |candidate| {
                                child.state = candidate;
                                child.path_index = frame.path_index;
                                child.nodes = frame.nodes;
                                child.relationships = frame.relationships;
                                child.edge_index = frame.edge_index + 1;
                                if (child.edge_index == self.interior_anchors[frame.path_index]) {
                                    // Restore source order and the anchor as the
                                    // current endpoint before opening the suffix.
                                    const nodes = try e.a.dupe(*const v.Node, child.nodes);
                                    const relationships = try e.a.dupe(*const v.Relationship, child.relationships);
                                    std.mem.reverse(*const v.Node, nodes);
                                    std.mem.reverse(*const v.Relationship, relationships);
                                    child.nodes = nodes;
                                    child.relationships = relationships;
                                }
                                try e.state();
                                try self.frames.append(self.budget.allocator(), child);
                                retained = true;
                                continue;
                            }
                        }
                    }
                }
            }
            if (edge.max_hops) |maximum| if (frame.hops >= maximum) {
                self.popFrame();
                continue;
            };
            if (frame.mapping == e.graph.relationships.len) {
                self.popFrame();
                continue;
            }
            const mapping = e.graph.relationships[frame.mapping];
            const selected = if (edge.types.len == 0) true else for (edge.types) |name| {
                if (std.mem.eql(u8, mapping.name, name)) break true;
            } else false;
            const outgoing = selected and edge.direction != .in and try e.graph.admits(current, mapping.source_label, &e.values);
            const incoming = selected and edge.direction != .out and try e.graph.admits(current, mapping.target_label, &e.values);
            if ((!outgoing and !incoming) or (frame.loaded and frame.position == frame.items.len and frame.last_page)) {
                frame.mapping += 1;
                frame.loaded = false;
                frame.after.clearRetainingCapacity();
                continue;
            }
            if (!frame.loaded or frame.position == frame.items.len) {
                _ = frame.page_arena.reset(.retain_capacity);
                const page_scope = Engine.EvalScope.enter(e, frame.page_arena.allocator());
                defer page_scope.leave(e);
                const requested = @min(128, demand - output.items.len);
                const page = try e.expandPatternEdges(frame.mapping, mapping, current.identity, path.nodes[frame.edge_index + 1], edge, frame.hops, frame.state.row, if (frame.loaded) frame.after.items else null, requested);
                if (page.items.len > requested) return error.CypherInvalidBackendPage;
                if (frame.loaded) if (page.next) |continuation| if (std.mem.eql(u8, frame.after.items, continuation)) return error.CypherInvalidBackendCursor;
                frame.after.clearRetainingCapacity();
                if (page.next) |continuation| try frame.after.appendSlice(self.budget.allocator(), continuation);
                e.cache_reads = true;
                try e.prefetchEndpoints(page.items, current.identity, outgoing, incoming);
                frame.read_nodes = e.read_nodes;
                frame.items = page.items;
                frame.position = 0;
                frame.last_page = page.next == null;
                frame.loaded = true;
                continue;
            }
            const relationship = frame.items[frame.position];
            frame.position += 1;
            var reused = false;
            if (operation.mode != .repeatable) for (frame.state.used) |previous| {
                try e.state();
                if (v.Value.eql(.{ .relationship = previous }, .{ .relationship = relationship })) {
                    reused = true;
                    break;
                }
            };
            if (reused) continue;
            if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
            const traversing_out = outgoing and relationship.source.eql(current.identity);
            const target = if (traversing_out) relationship.target else if (incoming and relationship.target.eql(current.identity)) relationship.source else continue;
            const child = try self.newFrame();
            var retained = false;
            defer if (!retained) self.freeFrame(child);
            const child_scope = Engine.EvalScope.enter(e, child.arena.allocator());
            defer child_scope.leave(e);
            const reached = if (frame.read_nodes.get(target)) |cached| cached orelse continue else (try e.getNode(target)) orelse continue;
            if (!try e.graph.admits(reached, if (traversing_out) mapping.target_label else mapping.source_label, &e.values)) continue;
            if (operation.mode == .node_simple) {
                var visited = false;
                for (frame.nodes) |node| {
                    try e.state();
                    if (node.identity.eql(reached.identity)) {
                        visited = true;
                        break;
                    }
                }
                if (visited) continue;
            }
            const candidate = (try e.constrain(frame.state, .{ .relationship = relationship }, edge.properties)) orelse continue;
            if (frame.relationships.len >= e.limits.path_depth) return error.CypherPathBudgetExceeded;
            const nodes = try e.a.alloc(*const v.Node, frame.nodes.len + 1);
            @memcpy(nodes[0..frame.nodes.len], frame.nodes);
            nodes[frame.nodes.len] = reached;
            const relationships = try e.a.alloc(*const v.Relationship, frame.relationships.len + 1);
            @memcpy(relationships[0..frame.relationships.len], frame.relationships);
            relationships[frame.relationships.len] = relationship;
            const used = try e.a.alloc(*const v.Relationship, frame.state.used.len + 1);
            @memcpy(used[0..frame.state.used.len], frame.state.used);
            used[frame.state.used.len] = relationship;
            const values = try e.a.alloc(v.Value, frame.edge_values.len + 1);
            @memcpy(values[0..frame.edge_values.len], frame.edge_values);
            values[frame.edge_values.len] = .{ .relationship = relationship };
            child.state = .{ .row = frame.state.row, .used = used, .constraints = candidate.constraints };
            child.path_index = frame.path_index;
            child.nodes = nodes;
            child.relationships = relationships;
            child.edge_values = values;
            child.edge_index = frame.edge_index;
            child.hops = frame.hops + 1;
            try e.state();
            try self.frames.append(self.budget.allocator(), child);
            retained = true;
        }
    }
    pub fn countRows(self: *NodeCursor) !?u64 {
        if (self.started or self.query.clauses[1].operation.project.value.predicate != null) return null;
        if (self.exact_count) |count| return count;
        const e = self.engine;
        const backend = e.backend;
        try backend.vtable.checkpoint(backend.ptr);
        if (backend.vtable.set_work) |configure| configure(backend.ptr, .{ .ptr = e, .charge = Engine.chargeWork, .value = Engine.chargeValue, .remaining = Engine.remainingWork });
        defer if (backend.vtable.set_work) |configure| configure(backend.ptr, null);
        if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, self.fields);
        defer if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, null);
        const call: ast.Expr = .{ .span = .{}, .value = .{ .call = .{ .name = "count", .args = &.{}, .star = true } } };
        const clauses = [_]ast.Clause{ .{ .span = self.query.clauses[0].span, .operation = .{ .match = self.original_match } }, .{ .span = .{}, .operation = .{ .project = .{ .final = true, .value = .{ .items = &.{.{ .expression = &call, .alias = "count" }} } } } } };
        const rows = e.run(&clauses, &.{&.{}}, false, false) catch |err| return if (self.budget.exhausted) error.CypherMemoryBudgetExceeded else err;
        self.exact_count = @intCast(rows[0][0].value.integer);
        return self.exact_count;
    }
    pub fn close(self: *NodeCursor) void {
        const allocator = self.backing;
        const budget = self.budget;
        while (self.frames.items.len != 0) self.popFrame();
        self.frames.deinit(budget.allocator());
        self.weighted_arena.deinit();
        if (self.weighted_seeds) |*seeds| if (seeds.arena) |*arena| arena.deinit();
        self.adjacency_cache.deinit();
        self.engine.regex.deinit();
        self.arena.deinit();
        allocator.destroy(budget);
        allocator.destroy(self);
    }
};

fn executeBudget(budget: *Budget, backend: backend_mod.Backend, graph: catalog.Graph, compiled: *const compiler.Compiled, parameters: []const v.Entry, limits: Limits) !Result {
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    errdefer arena.deinit();
    var engine: Engine = .{ .a = arena.allocator(), .planning_allocator = arena.allocator(), .scratch_allocator = budget.allocator(), .backend = backend, .graph = graph, .source = compiled.query.source, .parameters = parameters, .limits = limits, .regex = .{ .a = arena.allocator(), .scratch = std.heap.ArenaAllocator.init(budget.allocator()) } };
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    engine.strings = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    defer engine.regex.deinit();
    for (compiled.parameters) |name| {
        _ = lookup(parameters, name) orelse return error.MissingCypherParameter;
    }
    try backend.vtable.bind(backend.ptr, engine.a, graph);
    if (backend.vtable.set_work) |configure| configure(backend.ptr, .{ .ptr = &engine, .charge = Engine.chargeWork, .value = Engine.chargeValue, .remaining = Engine.remainingWork });
    defer if (backend.vtable.set_work) |configure| configure(backend.ptr, null);
    const writes = queryHasWrites(compiled.query);
    if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, if (writes) null else try @import("read_fields.zig").required(engine.a, compiled.query, graph));
    defer if (backend.vtable.set_read_fields) |configure| configure(backend.ptr, null);
    engine.cache_reads = !writes;
    engine.read_only = !writes;
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
        try Engine.chargeWork(&engine, initial.intermediate_states);
        engine.startup_accounted = initial;
        engine.stats.scanned_anchors = initial.scanned_anchors;
        engine.stats.explored_edges = initial.explored_edges;
    };
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
    // Result shape is bound independently of row production. In particular,
    // RETURN * must retain its columns when every branch produces zero rows.
    engine.columns = try @import("binder.zig").columns(engine.a, graph, compiled.query);
    const output = try engine.a.alloc([]const v.Value, combined.items.len);
    for (combined.items, output) |row, *out| {
        const cells = try engine.a.alloc(v.Value, engine.columns.len);
        for (engine.columns, cells) |name, *cell| {
            const value = try engine.readValue(lookup(row, name) orelse return error.UnboundCypherVariable);
            try engine.values.output(value);
            cell.* = try value.clone(engine.a);
        }
        out.* = cells;
    }
    const columns = try engine.a.alloc([]const u8, engine.columns.len);
    for (engine.columns, columns) |name, *out| out.* = try engine.a.dupe(u8, name);
    const stages = try engine.stages.toOwnedSlice(engine.a);
    engine.stats.memory_bytes = budget.peak;
    var result: Result = .{ .arena = arena, .budget = budget, .columns = columns, .rows = output, .stages = stages, .stats = engine.stats, .read_only = !writes, .graph_incarnation = graph.incarnation, .graph_revision = graph.revision };
    var checkpoint_backend = backend;
    // Reserve room for the host's durable receipt. Count escaped wire bytes
    // before a write decision, so an oversized RETURN rolls back the overlay.
    _ = try @import("bounded_json.zig").checkSize(result, max_response_bytes - 1024, .{ .ptr = &checkpoint_backend, .call = serializationCheckpoint });
    if (transaction) {
        try backend.vtable.checkpoint(backend.ptr);
        try backend.vtable.commit.?(backend.ptr);
        transaction = false;
    }
    engine.stats.memory_bytes = budget.peak;
    result.stats = engine.stats;
    result.arena = arena;
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
    scratch_allocator: A,
    planning_allocator: A = undefined,
    anchor_estimates: std.StringHashMapUnmanaged(?usize) = .empty,
    regex: @import("regex.zig").Runtime,
    backend: backend_mod.Backend,
    graph: catalog.Graph,
    source: []const u8,
    parameters: []const v.Entry,
    limits: Limits,
    stats: Stats = .{},
    startup_accounted: backend_mod.StartupWork = .{},
    columns: []const []const u8 = &.{},
    match_aliases: []const []const u8 = &.{},
    collect_match_paths: bool = false,
    shortest_fast: bool = false,
    stream_fold: ?*StreamFold = null,
    stream_predicate: ?*const ast.Expr = null,
    shortest_count: ?*u64 = null,
    count_endpoints: bool = false,
    component_proof: bool = false,
    component_leaves: []const ComponentLeaf = &.{},
    retain_windows: bool = false,
    match_weight_demand: ?Multiplicity = null,
    terminal_weight_demand: ?Multiplicity = null,
    pending_window: ?WeightedRows = null,
    weighted_sink: ?struct { ptr: *anyopaque, emit: *const fn (*anyopaque, Row, u64) anyerror!void, multiplicity: Multiplicity } = null,
    lower_graph: ?*LowerGraph = null,
    anchor_batch: ?*AnchorBatch = null,
    adjacency_cache: ?*AdjacencyCache = null,
    interior: ?*const Interior = null,
    cache_reads: bool = false,
    read_only: bool = false,
    join: ?*const Join = null,
    lookups: []Lookup = &.{},
    lookup: ?*Lookup = null,
    aggregate_overrides: ?[]const AggregateValue = null,
    projection_overrides: ?struct { items: []const ast.Item, row: Row } = null,
    projection_locals: ?*const @import("expressions.zig").Local = null,
    read_nodes: std.HashMapUnmanaged(v.Identity, ?*const v.Node, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
    stages: std.ArrayList(Stage) = .empty,
    recursion: usize = 0,
    deletions: v.EntityMap(enum { pending, deleted }) = .empty,
    has_deletions: bool = false,
    // Retained aliases keep identity; resolve postimages on access rather than
    // walking every row after each mutation of a shared entity.
    postimages: v.EntityMap(v.Value) = .empty,
    values: value_work.Work = undefined,
    strings: @import("string_work.zig").Runtime = undefined,
    const WeightedRows = struct {
        rows: []const Row,
        weights: []const Count,
        fn count(self: @This()) usize {
            var total: usize = 0;
            for (self.weights) |weight| total +|= @as(usize, @intCast(weight.cap(std.math.maxInt(usize))));
            return total;
        }
    };
    fn chargeValue(ptr: *anyopaque, value: v.Value) !void {
        const self: *Engine = @ptrCast(@alignCast(ptr));
        // Planning validates and encodes properties before the durable decision.
        try self.values.output(value);
        try self.values.traversal(value);
    }
    fn remainingWork(ptr: *anyopaque) usize {
        const self: *Engine = @ptrCast(@alignCast(ptr));
        return self.limits.intermediate_states -| self.stats.intermediate_states;
    }
    fn chargeWork(ptr: *anyopaque, amount: usize) !void {
        const self: *Engine = @ptrCast(@alignCast(ptr));
        try self.checkpoint();
        if (amount > self.limits.intermediate_states -| self.stats.intermediate_states) return error.CypherStateBudgetExceeded;
        self.stats.intermediate_states += amount;
    }
    const AggregateValue = struct { expression: *const ast.Expr, value: v.Value };
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
        if (lookup(row, alias)) |existing| return if ((try self.values.eql(existing, value))) row else null;
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
    const StreamingAggregate = struct {
        function: ast.Function,
        arena: std.heap.ArenaAllocator,
        distinct_arena: std.heap.ArenaAllocator,
        distinct_values: std.ArrayList(v.Value) = .empty,
        distinct_index: value_work.Index = .{},
        count: u64 = 0,
        sum: v.Value = .{ .integer = 0 },
        mean: f64 = 0,
        mean_weight: f64 = 0,
        // Only promoted AVG counts need this state; ordinary aggregate groups
        // retain the inline u64/f64 path and avoid a binary128-sized field.
        mean_scaled: ?*Count.Scaled = null,
        best: v.Value = .null,
        collected: std.ArrayList(v.Value) = .empty,
        fn add(self: *StreamingAggregate, engine: *Engine, row: Row) !void {
            return self.addWeighted(engine, row, Count.one);
        }
        fn addWeighted(self: *StreamingAggregate, engine: *Engine, row: Row, multiplicity: Count) !void {
            const value = if (self.function.star) v.Value{ .integer = 1 } else try engine.eval(self.function.args[0], row, null);
            if (value == .null) return;
            // MIN/MAX are duplicate insensitive and need no distinct set.
            // Other DISTINCT reducers retain only unique arguments, using the
            // same equality and hashing as GROUP BY.
            if (self.function.distinct and !std.ascii.eqlIgnoreCase(self.function.name, "min") and !std.ascii.eqlIgnoreCase(self.function.name, "max")) {
                const hash = (try engine.values.info(value)).hash;
                var candidate = self.distinct_index.head(hash);
                while (candidate) |position| {
                    const link = self.distinct_index.links.items[position];
                    if (try engine.values.eql(self.distinct_values.items[link.index], value)) return;
                    candidate = link.next;
                }
                const retained = self.distinct_arena.allocator();
                const owned = try value.clone(retained);
                try self.distinct_index.insert(retained, hash, self.distinct_values.items.len);
                try self.distinct_values.append(retained, owned);
            }
            const weight = if (self.function.distinct or std.ascii.eqlIgnoreCase(self.function.name, "min") or std.ascii.eqlIgnoreCase(self.function.name, "max")) Count.one else multiplicity;
            const prior_count = self.count;
            self.count +|= weight.cap(std.math.maxInt(u64));
            if (std.ascii.eqlIgnoreCase(self.function.name, "count")) {
                self.count = try addPathCount(prior_count, weight.cap(std.math.maxInt(u64)));
                return;
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "collect")) {
                const retained = self.arena.allocator();
                const owned = if (self.function.distinct) self.distinct_values.items[self.distinct_values.items.len - 1] else try value.clone(retained);
                try Engine.chargeWork(engine, std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded);
                try self.collected.ensureUnusedCapacity(retained, @intCast(weight.exact().?));
                for (0..@intCast(weight.exact().?)) |_| self.collected.appendAssumeCapacity(owned);
                return;
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "sum")) {
                if (value != .integer and value != .number) return error.CypherTypeMismatch;
                if ((value == .integer and value.integer == 0) or (value == .number and value.number == 0)) {
                    self.sum = try engine.binary(.add, self.sum, value);
                    return;
                }
                if (self.sum == .integer and value == .integer) {
                    // A wide intermediate avoids a spurious product overflow
                    // when the current sum offsets a repeated value.
                    const sum = @as(i128, self.sum.integer) + @as(i128, value.integer) * @as(i128, weight.exact() orelse return error.CypherNumericOverflow);
                    self.sum = .{ .integer = std.math.cast(i64, sum) orelse return error.CypherNumericOverflow };
                } else {
                    if (weight.isOne()) {
                        self.sum = try engine.binary(.add, self.sum, value);
                    } else {
                        // A finite current sum can offset an overflowing f64
                        // product. Wider arithmetic keeps that cancellation.
                        const combined = @as(f128, try self.sum.numeric()) + @as(f128, try value.numeric()) * weight.wideNumber();
                        const number: f64 = @floatCast(combined);
                        if (!std.math.isFinite(number)) return error.CypherNumericOverflow;
                        self.sum = .{ .number = number };
                    }
                }
                return;
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "avg")) {
                const number = try value.numeric();
                if (weight.big != null or self.mean_scaled != null) {
                    const added = weight.scaled();
                    const current = if (self.mean_scaled) |scaled| scaled.* else Count.Scaled.from(self.mean_weight);
                    const total = if (prior_count == 0) added else current.plus(added);
                    if (prior_count == 0) self.mean = number else {
                        const fraction = added.ratio(total);
                        self.mean = @floatCast(@as(f128, self.mean) * (1 - fraction) + @as(f128, number) * fraction);
                    }
                    if (self.mean_scaled == null) self.mean_scaled = try self.arena.allocator().create(Count.Scaled);
                    self.mean_scaled.?.* = total;
                    if (!std.math.isFinite(self.mean)) return error.CypherNumericOverflow;
                    return;
                }
                const added: f64 = @floatFromInt(weight.small);
                const total = self.mean_weight + added;
                if (prior_count == 0) self.mean = number else if (weight.isOne() and total < @as(f64, @floatFromInt(std.math.maxInt(usize)))) {
                    self.mean = numbers.nextAverage(self.mean, number, @intFromFloat(total));
                } else {
                    const fraction = added / total;
                    const difference = number - self.mean;
                    self.mean = if (std.math.isFinite(difference)) self.mean + difference * fraction else self.mean * (self.mean_weight / total) + number * fraction;
                }
                self.mean_weight = total;
                if (!std.math.isFinite(self.mean)) return error.CypherNumericOverflow;
                return;
            }
            const order = if (self.count == 1) std.math.Order.eq else blk: {
                var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                defer scratch.deinit();
                var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork };
                break :blk try work.order(value, self.best);
            };
            if (self.count == 1 or (order == .lt and std.ascii.eqlIgnoreCase(self.function.name, "min")) or (order == .gt and std.ascii.eqlIgnoreCase(self.function.name, "max"))) {
                _ = self.arena.reset(.retain_capacity);
                self.best = try value.clone(self.arena.allocator());
            }
        }
        fn result(self: StreamingAggregate) v.Value {
            if (std.ascii.eqlIgnoreCase(self.function.name, "count")) return .{ .integer = @intCast(self.count) };
            if (std.ascii.eqlIgnoreCase(self.function.name, "sum")) return self.sum;
            if (std.ascii.eqlIgnoreCase(self.function.name, "collect")) return .{ .list = self.collected.items };
            if (self.count == 0) return .null;
            if (std.ascii.eqlIgnoreCase(self.function.name, "avg")) return .{ .number = self.mean };
            return self.best;
        }
    };
    fn collectStreamingAggregates(self: *Engine, expression: *const ast.Expr, calls: *std.ArrayList(*const ast.Expr)) !bool {
        // Pattern subqueries own their aggregate scope.
        if (expression.value == .exists or expression.value == .pattern_comprehension) return true;
        if (expression.value == .call and aggregateName(expression.value.call.name)) {
            const call_value = expression.value.call;
            for (call_value.args) |arg| if (aggregates(arg)) return false;
            if (call_value.args.len != (if (call_value.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
            for (calls.items) |prior| if (prior == expression) return true;
            try calls.append(self.a, expression);
            return true;
        }
        return self.collectStreamingChildren(expression.value, calls);
    }
    fn collectStreamingChildren(self: *Engine, value: anytype, calls: *std.ArrayList(*const ast.Expr)) anyerror!bool {
        switch (@typeInfo(@TypeOf(value))) {
            .pointer => |pointer| {
                if (pointer.size == .one and pointer.child == ast.Expr) return self.collectStreamingAggregates(value, calls);
                if (pointer.size == .slice and pointer.child != u8) for (value) |child| {
                    if (!try self.collectStreamingChildren(child, calls)) return false;
                };
            },
            .@"struct" => |structure| inline for (structure.field_names) |name| {
                if (!try self.collectStreamingChildren(@field(value, name), calls)) return false;
            },
            .@"union" => switch (value) {
                inline else => |payload| return self.collectStreamingChildren(payload, calls),
            },
            .optional => if (value) |child| {
                return self.collectStreamingChildren(child, calls);
            },
            else => {},
        }
        return true;
    }
    const StreamGroup = struct { key: []const v.Value, source: Row, states: []StreamingAggregate };
    fn newStreamGroup(self: *Engine, a: A, key: []const v.Value, source: Row, calls: []const *const ast.Expr) !StreamGroup {
        const states = try a.alloc(StreamingAggregate, calls.len);
        for (states, calls) |*state_value, call_value| state_value.* = .{ .function = call_value.value.call, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
        const copied = try (v.Value{ .list = key }).clone(a);
        return .{ .key = copied.list, .source = (try (v.Value{ .map = source }).clone(a)).map, .states = states };
    }
    fn finishStreamGroups(self: *Engine, groups: []const StreamGroup, calls: []const *const ast.Expr, projection: ast.Projection) ![]const Row {
        var projected: std.ArrayList(Projected) = .empty;
        const overrides = try self.a.alloc(AggregateValue, calls.len);
        const previous = self.aggregate_overrides;
        defer self.aggregate_overrides = previous;
        var unique_rows: DistinctRows = .{};
        for (groups) |group| {
            for (overrides, calls, group.states) |*override, expression, state_value| override.* = .{ .expression = expression, .value = try state_value.result().clone(self.a) };
            self.aggregate_overrides = overrides;
            const row = try self.a.alloc(v.Entry, projection.items.len);
            for (projection.items, row) |item, *entry| entry.* = .{ .name = item.alias orelse try self.expressionName(item.expression), .value = try self.eval(item.expression, group.source, &.{}) };
            var scope = group.source;
            for (row) |entry| scope = try self.replace(scope, entry.name, entry.value);
            if (projection.predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) continue;
            if (projection.distinct and !try unique_rows.accept(self, self.a, row)) continue;
            const keys = try self.a.alloc(v.Value, projection.order.len);
            for (projection.order, keys) |order, *key| key.* = try self.orderValue(order.expression, projection, row, scope, &.{});
            try projected.append(self.a, .{ .row = row, .source = group.source, .group = null, .keys = keys });
        }
        self.aggregate_overrides = previous;
        try self.sort(projected.items, projection.order);
        const skip = if (projection.skip) |expression| try self.count(expression) else 0;
        const limit = if (projection.limit) |expression| try self.count(expression) else projected.items.len;
        const start = @min(skip, projected.items.len);
        const end = start + @min(limit, projected.items.len - start);
        const output = try self.a.alloc(Row, end - start);
        for (projected.items[start..end], output) |item, *row| row.* = item.row;
        const columns = try self.a.alloc([]const u8, projection.items.len);
        for (projection.items, columns) |item, *column| column.* = item.alias orelse try self.expressionName(item.expression);
        self.columns = columns;
        return output;
    }
    /// Retained reduction state is independent of the page/route arenas which
    /// feed it. The producer can stop as soon as an unordered demand is met.
    const DistinctRows = struct {
        rows: std.ArrayList(Row) = .empty,
        index: value_work.Index = .{},
        fn accept(self: *@This(), engine: *Engine, retained: A, row: Row) !bool {
            const hash = try engine.rowHash(row);
            var candidate = self.index.head(hash);
            while (candidate) |position| {
                const link = self.index.links.items[position];
                if (try engine.rowEqual(self.rows.items[link.index], row)) return false;
                candidate = link.next;
            }
            const owned = (try (v.Value{ .map = row }).clone(retained)).map;
            try self.index.insert(retained, hash, self.rows.items.len);
            try self.rows.append(retained, owned);
            return true;
        }
    };
    const StreamFold = struct {
        retained: A,
        middle: []const ast.Clause,
        projection: ast.Projection,
        calls: []const *const ast.Expr,
        top: *?TopK,
        groups: *std.ArrayList(StreamGroup),
        group_index: *value_work.Index,
        has_keys: bool,
        stage_counts: []usize,
        matched: *usize,
        distinct: []DistinctRows,
        distinct_goal: ?usize = null,
        had_match: bool = false,
        weight: Count = Count.one,
        active_calls: ?[]const bool = null,
        record_rows: bool = true,
        fn add(fold: *@This(), engine: *Engine, initial: Row) anyerror!void {
            const weight = @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
            fold.had_match = true;
            if (fold.record_rows) fold.stage_counts[0] +|= weight;
            return fold.expand(engine, initial, 0);
        }
        fn expand(fold: *@This(), engine: *Engine, row: Row, index: usize) anyerror!void {
            if (index < fold.middle.len) {
                const clause = fold.middle[index];
                switch (clause.operation) {
                    .unwind => |unwind| {
                        const value = try engine.eval(unwind.expression, row, null);
                        if (value == .null) return;
                        const items = if (value == .list) value.list else &.{value};
                        var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                        defer arena.deinit();
                        for (items) |item| {
                            _ = arena.reset(.retain_capacity);
                            const scope = EvalScope.enter(engine, arena.allocator());
                            defer scope.leave(engine);
                            try engine.state();
                            if (fold.record_rows) fold.stage_counts[index + 1] +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
                            try fold.expand(engine, try engine.replace(row, unwind.alias, item), index + 1);
                        }
                    },
                    .project => |step| {
                        const projected = try engine.projectUnordered(&.{row}, step.value, false);
                        if (projected.len == 0) return;
                        if (step.value.distinct and !try fold.distinct[index].accept(engine, fold.retained, projected[0])) return;
                        const prior_weight = fold.weight;
                        defer fold.weight = prior_weight;
                        if (step.value.distinct) fold.weight = Count.one;
                        if (fold.record_rows) fold.stage_counts[index + 1] +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
                        try fold.expand(engine, projected[0], index + 1);
                    },
                    else => unreachable,
                }
                return;
            }
            try engine.state();
            if (fold.record_rows) fold.matched.* +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
            if (fold.top.*) |*top| {
                try top.addWeighted(row, fold.weight);
                if (fold.projection.limit != null and fold.projection.order.len == 0 and top.total == top.capacity) return error.CypherStreamComplete;
                return;
            }
            var key: std.ArrayList(v.Value) = .empty;
            for (fold.projection.items) |item| if (!aggregates(item.expression)) try key.append(engine.a, try engine.eval(item.expression, row, null));
            const hash = (try engine.values.info(.{ .list = key.items })).hash;
            var found: ?usize = if (!fold.has_keys) 0 else null;
            var candidate = fold.group_index.head(hash);
            while (candidate) |position| {
                const link = fold.group_index.links.items[position];
                if (try engine.values.eql(.{ .list = fold.groups.items[link.index].key }, .{ .list = key.items })) {
                    found = link.index;
                    break;
                }
                candidate = link.next;
            }
            if (found == null) {
                found = fold.groups.items.len;
                try fold.group_index.insert(fold.retained, hash, found.?);
                try fold.groups.append(fold.retained, try engine.newStreamGroup(fold.retained, key.items, row, fold.calls));
            } else if (!fold.has_keys and fold.groups.items[0].source.len == 0) {
                fold.groups.items[0].source = (try (v.Value{ .map = row }).clone(fold.retained)).map;
            }
            for (fold.groups.items[found.?].states, 0..) |*accumulator, i| {
                if (fold.active_calls) |active| if (!active[i]) continue;
                try accumulator.addWeighted(engine, row, fold.weight);
            }
            if (fold.distinct_goal) |goal| if (fold.groups.items.len >= goal) return error.CypherStreamComplete;
        }
        fn demand(fold: *@This()) usize {
            if (fold.top.*) |top| if (fold.projection.order.len == 0) return @min(128, top.capacity - top.total);
            if (fold.distinct_goal) |goal| return @min(128, goal -| fold.groups.items.len);
            return 128;
        }
    };
    /// A deliberately small total-expression proof. It never suppresses
    /// arithmetic, function, parameter or scalar-property errors. Entity shape
    /// is known at MATCH/WITH boundaries; comparison operators return NULL for
    /// incompatible Cypher values rather than raising a type error.
    fn totalExpression(self: *Engine, expression: *const ast.Expr, shape: Row) bool {
        return switch (expression.value) {
            .literal => true,
            .variable => |name| lookup(shape, name) != null,
            .parameter => |name| lookup(self.parameters, name) != null,
            .property => |p| p.base.value == .variable and blk: {
                const known = lookup(shape, p.base.value.variable) orelse break :blk false;
                break :blk known == .boolean and known.boolean;
            },
            .unary => |u| switch (u.op) {
                .is_null, .is_not_null => self.totalExpression(u.operand, shape),
                .not => self.totalPredicate(u.operand, shape),
                else => false,
            },
            .binary => |b| switch (b.op) {
                .eq, .neq, .lt, .lte, .gt, .gte, .starts_with, .ends_with, .contains => self.totalExpression(b.left, shape) and self.totalExpression(b.right, shape),
                .@"and", .@"or", .xor => self.totalPredicate(b.left, shape) and self.totalPredicate(b.right, shape),
                else => false,
            },
            .comparison => |c| blk: {
                for (c.operands) |operand| if (!self.totalExpression(operand, shape)) break :blk false;
                break :blk true;
            },
            else => false,
        };
    }
    fn totalPredicate(self: *Engine, expression: *const ast.Expr, shape: Row) bool {
        return switch (expression.value) {
            .literal => |value| value == .boolean or value == .null,
            .parameter => |name| blk: {
                const value = lookup(self.parameters, name) orelse break :blk false;
                break :blk value == .boolean or value == .null;
            },
            .binary, .comparison, .unary => self.totalExpression(expression, shape),
            else => false,
        };
    }
    fn pathShape(self: *Engine, path: ast.Path, row: Row) !Row {
        var shape: Row = &.{};
        for (row) |entry| shape = try self.replace(shape, entry.name, .{ .boolean = entry.value == .node or entry.value == .map or entry.value == .null });
        for (path.nodes) |node| if (node.alias) |alias| {
            shape = try self.replace(shape, alias, .{ .boolean = true });
        };
        for (path.edges) |edge| if (edge.alias) |alias| {
            shape = try self.replace(shape, alias, .{ .boolean = false });
        };
        if (path.alias) |alias| {
            shape = try self.replace(shape, alias, .{ .boolean = false });
        }
        return shape;
    }
    const BatchPredicates = struct { admission: ?*const ast.Expr = null, residual: ?*const ast.Expr = null };
    fn conjunction(self: *Engine, left: ?*const ast.Expr, right: ?*const ast.Expr) !?*const ast.Expr {
        if (left == null) return right;
        if (right == null) return left;
        const expression = try self.a.create(ast.Expr);
        expression.* = .{ .span = left.?.span, .depth = @max(left.?.depth, right.?.depth) + 1, .value = .{ .binary = .{ .op = .@"and", .left = left.?, .right = right.? } } };
        return expression;
    }
    fn splitAdmission(self: *Engine, path: ast.Path, row: Row, predicate: *const ast.Expr) !BatchPredicates {
        const AnchorScope = struct {
            alias: ?[]const u8,
            outer: Row,
            pub fn permits(scope: @This(), name: []const u8) bool {
                if (scope.alias) |alias| if (std.mem.eql(u8, name, alias)) return true;
                return lookup(scope.outer, name) != null;
            }
        };
        if (@import("expressions.zig").rowLocal(predicate, AnchorScope{ .alias = path.nodes[0].alias, .outer = row })) return .{ .admission = predicate };
        if (predicate.value == .binary and predicate.value.binary.op == .@"and") {
            const left = try self.splitAdmission(path, row, predicate.value.binary.left);
            const right = try self.splitAdmission(path, row, predicate.value.binary.right);
            return .{ .admission = try self.conjunction(left.admission, right.admission), .residual = try self.conjunction(left.residual, right.residual) };
        }
        return .{ .residual = predicate };
    }
    fn batchPredicates(self: *Engine, path: ast.Path, row: Row, predicate: ?*const ast.Expr) !BatchPredicates {
        const expression = predicate orelse return .{};
        // Cypher evaluates both Boolean operands. Moving a conjunct must not
        // conceal an error in another conjunct or in a deleted entity read.
        if (!self.read_only or !self.totalPredicate(expression, try self.pathShape(path, row))) return .{ .residual = expression };
        return self.splitAdmission(path, row, expression);
    }
    const Names = std.StringHashMapUnmanaged(void);
    fn retainReferences(self: *Engine, names: *Names, candidates: Row, expression: *const ast.Expr) !void {
        const Visitor = struct {
            engine: *Engine,
            names: *Names,
            candidates: Row,
            pub fn reference(visitor: @This(), name: []const u8) !void {
                try chargeWork(visitor.engine, 1);
                try visitor.names.put(visitor.engine.a, name, {});
            }
            pub fn pattern(visitor: @This(), expression_value: *const ast.Expr, locals: ?*const @import("expressions.zig").Local) anyerror!void {
                try visitor.engine.retainPattern(visitor.names, visitor.candidates, expression_value, locals);
            }
        };
        try @import("expressions.zig").visitFree(expression, Visitor{ .engine = self, .names = names, .candidates = candidates });
    }
    fn retainPattern(self: *Engine, names: *Names, candidates: Row, expression: *const ast.Expr, locals: ?*const @import("expressions.zig").Local) anyerror!void {
        var visible: std.ArrayList(v.Entry) = .empty;
        for (candidates) |entry| {
            var local = locals;
            const shadowed = while (local) |binding| : (local = binding.parent) {
                if (std.mem.eql(u8, entry.name, binding.name)) break true;
            } else false;
            if (!shadowed) try visible.append(self.a, entry);
        }
        if (expression.value == .exists) return self.retainQueryReferences(names, visible.items, expression.value.exists);
        const comprehension = expression.value.pattern_comprehension;
        if (comprehension.path.alias) |alias| {
            var index: usize = 0;
            while (index < visible.items.len) {
                if (std.mem.eql(u8, visible.items[index].name, alias)) {
                    _ = visible.orderedRemove(index);
                } else index += 1;
            }
        }
        var references: Names = .empty;
        try self.matchReferences(&references, visible.items, ast.Match{ .paths = &.{comprehension.path}, .predicate = comprehension.predicate });
        try self.retainReferences(&references, visible.items, comprehension.projection);
        for (visible.items) |entry| if (references.contains(entry.name)) try names.put(self.a, entry.name, {});
    }
    fn scopeAfter(self: *Engine, active: Row, clause: ast.Clause) !Row {
        switch (clause.operation) {
            .project => |p| {
                var next: std.ArrayList(v.Entry) = .empty;
                if (p.value.wildcard) for (active) |entry| {
                    const shadowed = for (p.value.items) |item| {
                        if (std.mem.eql(u8, entry.name, item.alias orelse try self.expressionName(item.expression))) break true;
                    } else false;
                    if (!shadowed) try next.append(self.a, entry);
                };
                return next.items;
            },
            .unwind => |u| {
                var next: std.ArrayList(v.Entry) = .empty;
                for (active) |entry| if (!std.mem.eql(u8, entry.name, u.alias)) try next.append(self.a, entry);
                return next.items;
            },
            else => return active,
        }
    }
    fn retainQueryReferences(self: *Engine, names: *Names, candidates: Row, clauses: []const ast.Clause) anyerror!void {
        var active = candidates;
        for (clauses) |clause| {
            var references: Names = .empty;
            try self.matchReferences(&references, active, clause);
            for (active) |entry| if (references.contains(entry.name)) try names.put(self.a, entry.name, {});
            active = try self.scopeAfter(active, clause);
        }
    }
    fn discardable(expression: *const ast.Expr, node_shape: Row) bool {
        return switch (expression.value) {
            .literal, .variable => true,
            .property => |p| p.base.value == .variable and blk: {
                const node = lookup(node_shape, p.base.value.variable) orelse break :blk false;
                break :blk node.boolean;
            },
            else => false,
        };
    }
    /// Ordering cannot affect a later COUNT/MIN/MAX when the intervening
    /// pipeline and all sort/argument reads are total. Otherwise retain the
    /// original barrier, including its observable errors and row selection.
    fn elideMiddleOrder(self: *Engine, input: []const Row, operation: ast.Match, middle: []const ast.Clause, projection: ast.Projection) ![]const ast.Clause {
        const ordered = for (middle) |clause| {
            if (clause.operation == .project and clause.operation.project.value.order.len != 0) break true;
        } else false;
        if (!ordered or projection.items.len == 0 or !self.read_only) return middle;
        var calls: std.ArrayList(*const ast.Expr) = .empty;
        for (projection.items) |item| {
            if (!aggregates(item.expression) or !try self.collectStreamingAggregates(item.expression, &calls)) return middle;
        }
        if (calls.items.len == 0) return middle;
        for (calls.items) |expression| {
            const name = expression.value.call.name;
            if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max")) return middle;
        }
        var shape: Row = &.{};
        if (input.len != 0) for (input[0]) |entry| {
            // Outer inputs may have heterogeneous types across rows.
            shape = try self.replace(shape, entry.name, .{ .boolean = false });
        };
        for (operation.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = true });
            };
            for (path.edges) |edge| if (edge.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = false });
            };
            if (path.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = false });
            }
        }
        for (middle) |clause| {
            if (clause.operation != .project) return middle;
            const step = clause.operation.project.value;
            if (step.skip != null or step.limit != null) return middle;
            var next: Row = if (step.wildcard) shape else &.{};
            for (step.items) |item| {
                if (!self.totalExpression(item.expression, shape)) return middle;
                const node = item.expression.value == .variable and (lookup(shape, item.expression.value.variable) orelse v.Value{ .boolean = false }).boolean;
                next = try self.replace(next, item.alias orelse try self.expressionName(item.expression), .{ .boolean = node });
            }
            var scope = shape;
            for (next) |entry| scope = try self.replace(scope, entry.name, entry.value);
            if (step.predicate) |predicate| if (!self.totalPredicate(predicate, scope)) return middle;
            for (step.order) |order| if (!self.totalExpression(order.expression, scope)) return middle;
            shape = next;
        }
        for (calls.items) |expression| for (expression.value.call.args) |arg| if (!self.totalExpression(arg, shape)) return middle;
        const output = try self.a.dupe(ast.Clause, middle);
        for (output) |*clause| clause.operation.project.value.order = &.{};
        return output;
    }
    /// Backward liveness may discard only total, side-effect-free reads. Keep
    /// expressions which could raise an error, even when their result is dead.
    /// DISTINCT retains all of its keys. Forward node lineage proves that a
    /// nullable entity property read is total without guessing scalar types.
    fn pruneMiddle(self: *Engine, input: []const Row, operation: ast.Match, middle: []const ast.Clause, projection: ast.Projection) ![]const ast.Clause {
        if (middle.len == 0) return middle;
        var shape: Row = &.{};
        if (input.len != 0) for (input[0]) |entry| {
            shape = try self.replace(shape, entry.name, .{ .boolean = false });
        };
        for (operation.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = true });
            };
            for (path.edges) |edge| if (edge.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = false });
            };
            if (path.alias) |alias| {
                shape = try self.replace(shape, alias, .{ .boolean = false });
            }
        }
        var candidates = shape;
        const shapes = try self.a.alloc(Row, middle.len);
        for (middle, 0..) |clause, i| {
            shapes[i] = shape;
            switch (clause.operation) {
                .project => |p| {
                    if (p.value.order.len != 0 or p.value.skip != null or p.value.limit != null) return middle;
                    var next: Row = if (p.value.wildcard) shape else &.{};
                    for (p.value.items) |item| {
                        if (aggregates(item.expression) or @import("expressions.zig").containsPattern(item.expression)) return middle;
                        const name = item.alias orelse try self.expressionName(item.expression);
                        const is_node = item.expression.value == .variable and (lookup(shape, item.expression.value.variable) orelse v.Value{ .boolean = false }).boolean;
                        next = try self.replace(next, name, .{ .boolean = is_node });
                        candidates = try self.replace(candidates, name, .{ .boolean = false });
                    }
                    shape = next;
                },
                .unwind => |u| {
                    shape = try self.replace(shape, u.alias, .{ .boolean = false });
                    candidates = try self.replace(candidates, u.alias, .{ .boolean = false });
                },
                else => return middle,
            }
        }
        var live: Names = .empty;
        if (projection.wildcard) for (shape) |entry| {
            try live.put(self.a, entry.name, {});
        };
        for (projection.items) |item| try self.retainReferences(&live, candidates, item.expression);
        for (projection.order) |order| try self.retainReferences(&live, candidates, order.expression);
        if (projection.predicate) |predicate| try self.retainReferences(&live, candidates, predicate);
        const output = try self.a.dupe(ast.Clause, middle);
        var index = middle.len;
        while (index != 0) {
            index -= 1;
            switch (middle[index].operation) {
                .unwind => |u| {
                    _ = live.remove(u.alias);
                    try self.retainReferences(&live, candidates, u.expression);
                },
                .project => |p| {
                    if (p.value.predicate) |predicate| try self.retainReferences(&live, candidates, predicate);
                    var kept: std.ArrayList(ast.Item) = .empty;
                    var previous: Names = if (p.value.wildcard) try live.clone(self.a) else .empty;
                    if (p.value.distinct and p.value.wildcard) for (shapes[index]) |entry| {
                        try previous.put(self.a, entry.name, {});
                    };
                    for (p.value.items) |item| {
                        const name = item.alias orelse try self.expressionName(item.expression);
                        _ = previous.remove(name);
                        if (p.value.distinct or live.contains(name) or !discardable(item.expression, shapes[index])) {
                            try kept.append(self.a, item);
                        }
                    }
                    // Predicate aliases resolve to projected expressions; other
                    // names retain access to the source scope.
                    if (p.value.predicate) |predicate| {
                        var references: Names = .empty;
                        try self.retainReferences(&references, candidates, predicate);
                        var keys = references.keyIterator();
                        while (keys.next()) |name| {
                            const projected = for (p.value.items) |item| {
                                if (std.mem.eql(u8, item.alias orelse try self.expressionName(item.expression), name.*)) break true;
                            } else false;
                            if (!projected) try previous.put(self.a, name.*, {});
                        }
                    }
                    for (kept.items) |item| try self.retainReferences(&previous, candidates, item.expression);
                    output[index].operation.project.value.items = kept.items;
                    live = previous;
                },
                else => unreachable,
            }
        }
        return output;
    }
    const ReductionPass = struct { projection: ast.Projection, active_calls: ?[]const bool = null };
    /// Partition independent aggregate calls by their efficient orientation.
    /// Every pass consumes the same immutable bag; the original expressions
    /// are evaluated once over the completed states. Never rewrite SUM(a+b),
    /// combine arithmetic states, or divide an aggregate across passes.
    fn reductionPasses(self: *Engine, input: []const Row, operation: ast.Match, projection: ast.Projection, middle: []const ast.Clause, calls: []const *const ast.Expr) ![]const ReductionPass {
        const ordinary = try self.a.dupe(ReductionPass, &.{.{ .projection = projection }});
        if (calls.len < 2 or input.len == 0) return ordinary;
        for (projection.items) |item| if (!aggregates(item.expression)) return ordinary;
        const path = operation.paths[0];
        var items: [2]std.ArrayList(ast.Item) = .{ .empty, .empty };
        var active: [2][]bool = undefined;
        for (&active) |*mask| {
            mask.* = try self.a.alloc(bool, calls.len);
            @memset(mask.*, false);
        }
        for (calls, 0..) |expression, index| {
            const single: ast.Projection = .{ .items = &.{.{ .expression = expression }} };
            var direction: ?usize = null;
            for (input) |row| {
                // Planning correlated input rows is statement work too, and
                // must remain cancellable before opening either pass.
                try self.state();
                const planned = try self.reductionPath(path, row, operation, single, middle);
                if (!try self.batchEligible(planned, row, operation, single, middle)) return ordinary;
                const next: usize = if (planned.execution_reversed == path.execution_reversed) 0 else 1;
                if (direction) |prior| if (prior != next) return ordinary;
                direction = next;
            }
            const selected = direction.?;
            try items[selected].append(self.a, .{ .expression = expression });
            active[selected][index] = true;
        }
        if (items[0].items.len == 0 or items[1].items.len == 0) return ordinary;
        const passes = try self.a.alloc(ReductionPass, 2);
        for (passes, items, active) |*pass, group, mask| pass.* = .{ .projection = .{ .items = group.items }, .active_calls = mask };
        return passes;
    }
    fn prefixWindow(self: *Engine, projection: ast.Projection) bool {
        const skip_expression = projection.skip orelse return false;
        const limit_expression = projection.limit orelse return false;
        const skip = self.constant(skip_expression, &.{}) orelse return false;
        const limit = self.constant(limit_expression, &.{}) orelse return false;
        if (skip != .integer or limit != .integer or skip.integer <= 0 or limit.integer <= 0) return false;
        // Route streaming cannot satisfy a prefix larger than its remaining
        // work quota: every candidate charges a state. Smaller goals keep
        // demand-driven traversal rather than discovering the whole closure.
        const goal = @as(u64, @intCast(skip.integer)) + @as(u64, @intCast(limit.integer));
        return goal > self.limits.intermediate_states -| self.stats.intermediate_states;
    }
    fn windowGoal(self: *Engine, projection: ast.Projection) ?usize {
        const limit = self.constant(projection.limit orelse return null, &.{}) orelse return null;
        if (limit != .integer or limit.integer < 0) return null;
        const skip = if (projection.skip) |expression| self.constant(expression, &.{}) orelse return null else v.Value{ .integer = 0 };
        if (skip != .integer or skip.integer < 0) return null;
        return std.math.cast(usize, @as(u64, @intCast(skip.integer)) + @as(u64, @intCast(limit.integer)));
    }
    fn streamMatchProjection(self: *Engine, input: []const Row, operation: ast.Match, projection: ast.Projection, original_middle: []const ast.Clause, match_span: ast.Span, projection_span: ast.Span) anyerror!?[]const Row {
        return self.streamMatchProjectionWeighted(input, null, operation, projection, original_middle, match_span, projection_span);
    }
    fn streamMatchProjectionWeighted(self: *Engine, input: []const Row, input_weights: ?[]const Count, operation: ast.Match, projection: ast.Projection, original_middle: []const ast.Clause, match_span: ast.Span, projection_span: ast.Span) anyerror!?[]const Row {
        // The row pipeline nests only within the statement's stack allowance.
        // Longer clause sequences retain the iterative general executor.
        if (original_middle.len > self.limits.recursion_depth -| self.recursion) return null;
        if (operation.paths.len == 0) return null;
        if (operation.paths.len > 1) for (operation.paths) |candidate| if (candidate.selector != .all) return null;
        const path = operation.paths[0];
        if (operation.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        for (operation.paths) |pattern| {
            for (pattern.nodes) |node| for (node.properties) |entry| {
                if (@import("expressions.zig").containsPattern(entry.value)) return null;
            };
            for (pattern.edges) |edge| for (edge.properties) |entry| {
                if (@import("expressions.zig").containsPattern(entry.value)) return null;
            };
        }
        const middle = try self.pruneMiddle(input, operation, try self.elideMiddleOrder(input, operation, original_middle, projection), projection);
        var grouped = projection.distinct;
        for (projection.items) |item| {
            grouped = grouped or aggregates(item.expression);
            if (@import("expressions.zig").containsPattern(item.expression)) return null;
        }
        for (projection.order) |order| if (@import("expressions.zig").containsPattern(order.expression)) return null;
        if (projection.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        for (middle) |clause| {
            if (clause.operation == .unwind) {
                if (@import("expressions.zig").containsPattern(clause.operation.unwind.expression)) return null;
                continue;
            }
            if (clause.operation != .project) return null;
            const p = clause.operation.project.value;
            if (p.order.len != 0 or p.skip != null or p.limit != null) return null;
            for (p.items) |item| if (aggregates(item.expression) or @import("expressions.zig").containsPattern(item.expression)) return null;
            if (p.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        }
        // This pipeline contains only reads and row-local expressions. Writes
        // remain barriers in run(); no entity cache may cross such a barrier.
        // In particular, a statement containing CREATE/SET can still reduce a
        // subsequent MATCH without materializing its complete route bag.
        const previous_cache = self.cache_reads;
        const previous_reads = self.read_nodes;
        if (!previous_cache) self.read_nodes = .empty;
        self.cache_reads = true;
        defer {
            self.cache_reads = previous_cache;
            if (!previous_cache) self.read_nodes = previous_reads;
        }
        var calls: std.ArrayList(*const ast.Expr) = .empty;
        if (grouped) {
            if (projection.wildcard) return null;
            for (projection.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return null;
            for (projection.order) |order| if (!try self.collectStreamingAggregates(order.expression, &calls)) return null;
        }
        const selected_path = path.selector != .all;
        if (selected_path) {
            if (path.edges.len != 1) return null;
            for (input) |row| {
                if (path.alias) |alias| if (lookup(row, alias) != null) return null;
                if (path.edges[0].alias) |alias| if (lookup(row, alias) != null) return null;
                for (path.nodes) |node| for (node.properties) |entry| {
                    if (self.constant(entry.value, row) == null) return null;
                };
                for (path.edges[0].properties) |entry| if (self.constant(entry.value, row) == null) return null;
            }
        }
        const count_shortest = input_weights == null and !operation.optional and operation.paths.len == 1 and grouped and middle.len == 0 and operation.predicate == null and blk: {
            for (projection.items) |item| if (!aggregates(item.expression)) break :blk false;
            for (calls.items) |expression| if (expression.value.call.distinct or !expression.value.call.star or !std.ascii.eqlIgnoreCase(expression.value.call.name, "count")) break :blk false;
            break :blk true;
        };
        const small_window = if (self.windowGoal(projection)) |goal| goal <= self.limits.intermediate_states -| self.stats.intermediate_states else false;
        const weighted_top = !grouped and (self.match_weight_demand != null or (projection.limit != null and (projection.order.len != 0 or (self.retain_windows and !small_window) or self.prefixWindow(projection))));
        const count_endpoints = !count_shortest and operation.paths.len == 1 and path.selector == .all and (grouped or weighted_top) and blk: {
            // Unordered DISTINCT with a row goal can finish after one route.
            // Preserve that demand rather than capturing the whole closure.
            if (grouped and calls.items.len == 0 and projection.order.len == 0 and projection.predicate == null and projection.limit != null) break :blk false;
            // One fixed hop has no shared prefixes to compress. Keep its
            // page-at-a-time fold instead of retaining the whole adjacency.
            if (path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1) break :blk false;
            for (calls.items) |expression| {
                const name = expression.value.call.name;
                if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "sum") and !std.ascii.eqlIgnoreCase(name, "avg") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max") and !(std.ascii.eqlIgnoreCase(name, "collect") and expression.value.call.distinct)) break :blk false;
            }
            for (input) |row| {
                if (operation.predicate) |predicate| if (!endpointExpression(predicate, path, row)) break :blk false;
                if (!try self.pipelineLocal(path, row, middle, projection, true)) break :blk false;
            }
            break :blk true;
        };
        if (self.component_proof and !count_endpoints and !count_shortest) return null;
        var shape: std.ArrayList(v.Entry) = .empty;
        if (input.len != 0) try shape.appendSlice(self.a, input[0]);
        for (operation.paths) |pattern| {
            for (pattern.nodes) |node| if (node.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            for (pattern.edges) |edge| if (edge.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            if (pattern.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
        }
        var unique_shape: Row = &.{};
        for (shape.items) |entry| unique_shape = try self.replace(unique_shape, entry.name, entry.value);
        shape = .empty;
        try shape.appendSlice(self.a, unique_shape);
        for (middle) |clause| {
            if (clause.operation == .unwind) {
                const next = try self.replace(shape.items, clause.operation.unwind.alias, .null);
                shape = .empty;
                try shape.appendSlice(self.a, next);
                continue;
            }
            const p = clause.operation.project.value;
            var next_shape: std.ArrayList(v.Entry) = .empty;
            if (p.wildcard) try next_shape.appendSlice(self.a, shape.items);
            for (p.items) |item| try next_shape.append(self.a, .{ .name = item.alias orelse try self.expressionName(item.expression), .value = .null });
            shape = next_shape;
        }
        var top: ?TopK = if (grouped) null else try TopK.init(self, projection, shape.items);
        defer if (top) |*value| value.deinit();
        const retained = self.a;
        var groups: std.ArrayList(StreamGroup) = .empty;
        var group_index: value_work.Index = .{};
        defer for (groups.items) |group| for (group.states) |*state_value| {
            state_value.arena.deinit();
            state_value.distinct_arena.deinit();
        };
        var has_keys = false;
        for (projection.items) |item| has_keys = has_keys or !aggregates(item.expression);
        if (grouped and !has_keys) try groups.append(retained, try self.newStreamGroup(retained, &.{}, &.{}, calls.items));
        var matched: usize = 0;
        const stage_counts = try retained.alloc(usize, middle.len + 1);
        @memset(stage_counts, 0);
        const distinct_stages = try retained.alloc(DistinctRows, middle.len);
        @memset(distinct_stages, .{});
        var fold: StreamFold = .{ .distinct = distinct_stages, .retained = retained, .middle = middle, .projection = projection, .calls = calls.items, .top = &top, .groups = &groups, .group_index = &group_index, .has_keys = has_keys, .stage_counts = stage_counts, .matched = &matched };
        if (projection.distinct and calls.items.len == 0 and projection.order.len == 0 and projection.predicate == null and projection.limit != null) {
            fold.distinct_goal = try std.math.add(usize, if (projection.skip) |skip| try self.count(skip) else 0, try self.count(projection.limit.?));
        }
        const saved_lookups = self.lookups;
        const saved_lookup = self.lookup;
        self.lookups = try retained.alloc(Lookup, operation.paths.len);
        for (self.lookups) |*lookup_plan| lookup_plan.* = .{ .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 8) } };
        defer {
            for (self.lookups) |*lookup_plan| if (lookup_plan.arena) |*lookup_arena| lookup_arena.deinit();
            self.lookups = saved_lookups;
            self.lookup = saved_lookup;
        }
        var adjacency_cache: AdjacencyCache = .{ .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 16) } };
        defer adjacency_cache.deinit();
        const saved_adjacency = self.adjacency_cache;
        self.adjacency_cache = &adjacency_cache;
        defer self.adjacency_cache = saved_adjacency;
        const previous_fold = self.stream_fold;
        const previous_predicate = self.stream_predicate;
        const previous_aliases = self.match_aliases;
        const previous_shortest = self.shortest_fast;
        const previous_collect = self.collect_match_paths;
        const previous_count = self.shortest_count;
        const previous_endpoints = self.count_endpoints;
        self.count_endpoints = count_endpoints;
        defer {
            self.stream_fold = previous_fold;
            self.stream_predicate = previous_predicate;
            self.match_aliases = previous_aliases;
            self.shortest_fast = previous_shortest;
            self.collect_match_paths = previous_collect;
            self.shortest_count = previous_count;
            self.count_endpoints = previous_endpoints;
        }
        var aliases: std.ArrayList([]const u8) = .empty;
        for (operation.paths) |pattern| {
            for (pattern.nodes) |node| if (node.alias) |alias| try aliases.append(retained, alias);
            for (pattern.edges) |edge| if (edge.alias) |alias| try aliases.append(retained, alias);
            if (pattern.alias) |alias| try aliases.append(retained, alias);
        }
        self.match_aliases = aliases.items;
        self.shortest_fast = true;
        self.collect_match_paths = false;
        self.stream_fold = if (count_shortest) null else &fold;
        self.stream_predicate = operation.predicate;
        const zero_limit = if (projection.limit) |limit| try self.count(limit) == 0 else false;
        const passes = if (count_endpoints and !zero_limit) try self.reductionPasses(input, operation, projection, middle, calls.items) else try retained.dupe(ReductionPass, &.{.{ .projection = projection }});
        for (passes, 0..) |pass, pass_index| {
            fold.active_calls = pass.active_calls;
            fold.record_rows = pass_index == 0;
            @memset(distinct_stages, .{});
            produce: for (input, 0..) |row, input_index| {
                fold.weight = if (input_weights) |weights| weights[input_index] else Count.one;
                if (fold.distinct_goal == 0 or (projection.limit != null and try self.count(projection.limit.?) == 0)) break :produce;
                if (top) |value| if (value.capacity == 0 and projection.order.len == 0) break :produce;
                var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
                defer scratch.deinit();
                const scope = EvalScope.enter(self, scratch.allocator());
                defer scope.leave(self);
                var ignored: std.ArrayList(MatchState) = .empty;
                fold.had_match = false;
                var count_paths: u64 = 0;
                self.shortest_count = if (count_shortest) &count_paths else null;
                const planned_path = if (count_shortest or count_endpoints) try self.reductionPath(path, row, operation, pass.projection, middle) else path;
                var planned = operation;
                if (count_shortest or count_endpoints) planned.paths = &.{planned_path};
                const predicates = try self.batchPredicates(planned_path, row, operation.predicate);
                var batch: AnchorBatch = .{ .admission = predicates.admission, .residual = predicates.residual, .path = planned_path, .input = .{ .row = row }, .mode = operation.mode, .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 16) } };
                defer if (batch.arena) |*arena| arena.deinit();
                const saved_batch = self.anchor_batch;
                self.anchor_batch = if ((count_shortest or count_endpoints) and try self.batchEligible(planned_path, row, planned, pass.projection, middle)) &batch else null;
                defer self.anchor_batch = saved_batch;
                self.joinPath(.{ .row = row }, planned, 0, &ignored) catch |err| switch (err) {
                    error.CypherStreamComplete => break :produce,
                    else => return err,
                };
                batch.flush(self, &ignored) catch |err| switch (err) {
                    error.CypherStreamComplete => break :produce,
                    else => return err,
                };
                if (operation.optional and !fold.had_match) {
                    var extended = row;
                    for (self.match_aliases) |alias| if (lookup(extended, alias) == null) {
                        extended = (try self.bind(extended, alias, .null)).?;
                    };
                    fold.add(self, extended) catch |err| switch (err) {
                        error.CypherStreamComplete => break :produce,
                        else => return err,
                    };
                }
                if (count_shortest) {
                    matched +|= std.math.cast(usize, count_paths) orelse std.math.maxInt(usize);
                    stage_counts[0] = matched;
                    for (groups.items[0].states) |*accumulator| {
                        accumulator.count = std.math.add(u64, accumulator.count, count_paths) catch return error.CypherNumericOverflow;
                        if (accumulator.count > std.math.maxInt(i64)) return error.CypherNumericOverflow;
                    }
                }
            }
        }
        self.shortest_count = previous_count;
        self.stream_fold = previous_fold;
        const rows = if (top) |*value| try value.finish(true) else try self.finishStreamGroups(groups.items, calls.items, projection);
        try self.stages.append(self.a, .{ .operation = "match", .span = match_span, .input_rows = input.len, .output_rows = stage_counts[0] });
        for (middle, 0..) |clause, index| try self.stages.append(self.a, .{ .operation = @tagName(clause.operation), .span = clause.span, .input_rows = stage_counts[index], .output_rows = stage_counts[index + 1] });
        try self.stages.append(self.a, .{ .operation = "project", .span = projection_span, .input_rows = matched, .output_rows = if (self.pending_window) |weighted| weighted.count() else rows.len });
        return rows;
    }
    // EXISTS consumes one result row. Keep aggregation, DISTINCT, ordering and
    // pagination barriers intact; ordinary terminal projections can carry the
    // demand into MATCH without evaluating or retaining the remaining routes.
    fn existsRows(self: *Engine, clauses: []const ast.Clause, row: Row) ![]const Row {
        const one: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .integer = 1 } } };
        var plan = try self.a.dupe(ast.Clause, clauses);
        if (plan[plan.len - 1].operation == .project and plan[plan.len - 1].operation.project.final) {
            const projection = &plan[plan.len - 1].operation.project.value;
            var has_aggregate = false;
            for (projection.items) |item| has_aggregate = has_aggregate or aggregates(item.expression);
            if (!has_aggregate and !projection.distinct and projection.order.len == 0 and projection.predicate == null) {
                if (projection.limit == null) projection.limit = &one else if (self.constant(projection.limit.?, row)) |limit| {
                    if (limit == .integer and limit.integer > 1) projection.limit = &one;
                }
            }
        } else {
            const extended = try self.a.alloc(ast.Clause, plan.len + 1);
            @memcpy(extended[0..plan.len], plan);
            extended[plan.len] = .{ .span = .{}, .operation = .{ .project = .{ .final = true, .value = .{ .items = &.{.{ .expression = &one }}, .limit = &one } } } };
            plan = extended;
        }
        return self.run(plan, &.{row}, false, true);
    }
    const ComponentResult = struct { rows: []const Row, next: usize };
    fn reverseComponent(self: *Engine, path: ast.Path) !ast.Path {
        var result = path;
        const nodes = try self.a.dupe(ast.Node, path.nodes);
        const edges = try self.a.dupe(ast.Edge, path.edges);
        std.mem.reverse(ast.Node, nodes);
        std.mem.reverse(ast.Edge, edges);
        for (edges) |*edge| edge.direction = switch (edge.direction) {
            .out => .in,
            .in => .out,
            .both => .both,
        };
        result.nodes = nodes;
        result.edges = edges;
        return result;
    }
    fn joinComponent(self: *Engine, left: ast.Path, right: ast.Path) !?ast.Path {
        const end = left.nodes[left.nodes.len - 1];
        const start = right.nodes[0];
        if (end.alias == null or start.alias == null or !std.mem.eql(u8, end.alias.?, start.alias.?)) return null;
        const nodes = try self.a.alloc(ast.Node, left.nodes.len + right.nodes.len - 1);
        @memcpy(nodes[0..left.nodes.len], left.nodes);
        @memcpy(nodes[left.nodes.len..], right.nodes[1..]);
        const labels = try self.a.alloc([]const u8, end.labels.len + start.labels.len);
        @memcpy(labels[0..end.labels.len], end.labels);
        @memcpy(labels[end.labels.len..], start.labels);
        const properties = try self.a.alloc(ast.Entry, end.properties.len + start.properties.len);
        @memcpy(properties[0..end.properties.len], end.properties);
        @memcpy(properties[end.properties.len..], start.properties);
        nodes[left.nodes.len - 1].labels = labels;
        nodes[left.nodes.len - 1].properties = properties;
        const edges = try self.a.alloc(ast.Edge, left.edges.len + right.edges.len);
        @memcpy(edges[0..left.edges.len], left.edges);
        @memcpy(edges[left.edges.len..], right.edges);
        return .{ .span = left.span, .alias = null, .nodes = nodes, .edges = edges };
    }
    /// Canonical path planning shared by native folds and SQL pull cursors.
    /// Keep the original MATCH untouched until a weighted plan is accepted.
    fn linearComponent(self: *Engine, patterns: []const ast.Path) !?ast.Path {
        if (patterns.len == 0) return null;
        if (patterns.len == 1) return patterns[0];
        var paths: std.ArrayList(ast.Path) = .empty;
        try paths.appendSlice(self.a, patterns);
        for (paths.items) |path| if (path.alias != null or path.selector != .all or path.execution_prefix or path.execution_reversed) return null;
        var path = paths.orderedRemove(0);
        while (paths.items.len != 0) {
            var found = false;
            for (paths.items, 0..) |candidate, index| {
                const reverse = try self.reverseComponent(candidate);
                const merged = try self.joinComponent(path, candidate) orelse try self.joinComponent(path, reverse) orelse try self.joinComponent(candidate, path) orelse try self.joinComponent(reverse, path);
                if (merged) |combined| {
                    path = combined;
                    _ = paths.orderedRemove(index);
                    found = true;
                    break;
                }
            }
            if (!found) return null;
        }
        return path;
    }
    const ComponentLeaf = struct { pivot: usize, node: ast.Node, edge: ast.Edge };
    fn aliasUses(patterns: []const ast.Path, alias: []const u8) usize {
        var leaf_count: usize = 0;
        for (patterns) |path| for (path.nodes) |node| if (node.alias) |name| {
            leaf_count += @intFromBool(std.mem.eql(u8, alias, name));
        };
        return leaf_count;
    }
    /// Eliminate unobserved one-edge leaves from a directed linear core.
    /// Acyclicity later proves that a leaf can conflict only with the core's
    /// adjacent outgoing edge. Local matching counts all distinct leaf-edge
    /// assignments instead of retaining complete trunk histories.
    fn branchComponent(self: *Engine, patterns: []const ast.Path, leaves: *std.ArrayList(ComponentLeaf)) !?ast.Path {
        var core: std.ArrayList(ast.Path) = .empty;
        var branches: std.ArrayList(ast.Path) = .empty;
        for (patterns) |pattern| {
            if (pattern.alias != null or pattern.selector != .all or pattern.execution_prefix or pattern.execution_reversed) return null;
            if (pattern.alias == null and pattern.selector == .all and pattern.edges.len == 1 and pattern.edges[0].min_hops == 1 and pattern.edges[0].max_hops == 1) {
                if (pattern.nodes[1].alias) |alias| if (aliasUses(patterns, alias) == 1 and pattern.nodes[0].alias != null and aliasUses(patterns, pattern.nodes[0].alias.?) > 1) {
                    try branches.append(self.a, pattern);
                    continue;
                };
                if (pattern.nodes[0].alias) |alias| if (aliasUses(patterns, alias) == 1 and pattern.nodes[1].alias != null and aliasUses(patterns, pattern.nodes[1].alias.?) > 1) {
                    try branches.append(self.a, try self.reverseComponent(pattern));
                    continue;
                };
            }
            try core.append(self.a, pattern);
        }
        if (branches.items.len == 0 or branches.items.len > @min(16, self.limits.recursion_depth)) return null;
        var path = (try self.linearComponent(core.items)) orelse return null;
        if (path.edges.len == 0) return null;
        const direction = path.edges[0].direction;
        if (direction == .both) return null;
        for (path.edges) |edge| if (edge.direction != direction or edge.min_hops == 0) return null;
        const nodes = try self.a.dupe(ast.Node, path.nodes);
        for (branches.items) |branch| {
            if (branch.edges[0].direction != direction) return null;
            const pivot = for (nodes, 0..) |node, index| {
                if (node.alias) |alias| if (std.mem.eql(u8, alias, branch.nodes[0].alias.?)) break index;
            } else return null;
            const combined = (try self.joinComponent(.{ .span = .{}, .alias = null, .nodes = &.{nodes[pivot]}, .edges = &.{} }, .{ .span = .{}, .alias = null, .nodes = &.{branch.nodes[0]}, .edges = &.{} })).?;
            nodes[pivot] = combined.nodes[0];
            try leaves.append(self.a, .{ .pivot = pivot, .node = branch.nodes[1], .edge = branch.edges[0] });
        }
        path.nodes = nodes;
        return path;
    }
    /// Plan a connected linear component once. Cross-clause fusion is only
    /// committed after the compressed producer proves uniqueness; a declined
    /// proof resumes the original clauses, which reset trail history separately.
    fn tryComponent(self: *Engine, input: []const Row, clauses: []const ast.Clause) anyerror!?ComponentResult {
        if (!self.read_only or self.component_proof or clauses.len < 2 or clauses[0].operation != .match) return null;
        var operation = clauses[0].operation.match;
        var paths: std.ArrayList(ast.Path) = .empty;
        var match_count: usize = 0;
        while (match_count < clauses.len and clauses[match_count].operation == .match) : (match_count += 1) {
            const step = clauses[match_count].operation.match;
            if (step.mode != operation.mode or (match_count != 0 and (step.optional or operation.optional))) return null;
            if (match_count != 0 and operation.predicate != null) return null;
            operation.predicate = step.predicate;
            try paths.appendSlice(self.a, step.paths);
        }
        if (paths.items.len < 2) return null;
        var leaves: std.ArrayList(ComponentLeaf) = .empty;
        const path = (try self.linearComponent(paths.items)) orelse blk: {
            if (match_count != 1 or operation.mode == .node_simple) return null;
            break :blk (try self.branchComponent(paths.items, &leaves)) orelse return null;
        };
        if (leaves.items.len != 0) for (clauses[match_count..]) |clause| {
            if (clause.operation == .project and clause.operation.project.value.wildcard) return null;
        };
        if (path.edges.len == 0 or (path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1)) return null;
        operation.paths = try self.a.dupe(ast.Path, &.{path});
        for (clauses[match_count..], match_count..) |clause, index| {
            if (clause.operation == .unwind) continue;
            if (clause.operation != .project) return null;
            const projection = clause.operation.project.value;
            var reduction = false;
            for (projection.items) |item| reduction = reduction or aggregates(item.expression);
            if (!clause.operation.project.final and !reduction and projection.limit == null) continue;
            const previous = self.component_proof;
            const previous_leaves = self.component_leaves;
            self.component_proof = true;
            self.component_leaves = leaves.items;
            defer self.component_proof = previous;
            defer self.component_leaves = previous_leaves;
            self.retain_windows = self.weightedTail(clauses[index + 1 ..]);
            const result = self.streamMatchProjection(input, operation, projection, clauses[match_count..index], clauses[0].span, clause.span) catch |err| switch (err) {
                error.CypherComponentDeclined => return null,
                else => return err,
            };
            return if (result) |rows| .{ .rows = rows, .next = index + 1 } else null;
        }
        return null;
    }
    // A pure correlated expression is evaluated once per binding; its
    // result keeps that binding's multiplicity. Procedures and writes are
    // execution barriers, including when nested inside EXISTS.
    fn weightedExpression(value: anytype) bool {
        if (@TypeOf(value) == ast.Expr) if (value.value == .call) {
            const function_call = value.value.call;
            if (!aggregateName(function_call.name)) {
                const signature = @import("functions.zig").lookup(function_call.name) orelse return false;
                if (!signature.repeatable) return false;
            }
        };
        if (@TypeOf(value) == ast.Clause) switch (value.operation) {
            .match, .project, .unwind, .subquery => {},
            else => return false,
        };
        switch (@typeInfo(@TypeOf(value))) {
            .pointer => |pointer| {
                if (pointer.size == .one and pointer.child == ast.Expr) return weightedExpression(value.*);
                if (pointer.size == .slice and pointer.child != u8) for (value) |child| {
                    if (!weightedExpression(child)) return false;
                };
            },
            .@"struct" => |structure| inline for (structure.field_names) |name| {
                if (!weightedExpression(@field(value, name))) return false;
            },
            .@"union" => switch (value) {
                inline else => |payload| return weightedExpression(payload),
            },
            .optional => if (value) |child| return weightedExpression(child),
            else => {},
        }
        return true;
    }
    fn hasCorrelatedBoundary(value: anytype) bool {
        if (@TypeOf(value) == ast.Clause) if (value.operation == .subquery) return true;
        if (@TypeOf(value) == *const ast.Expr) return @import("expressions.zig").containsPattern(value);
        switch (@typeInfo(@TypeOf(value))) {
            .pointer => |p| {
                if (p.size == .slice and p.child != u8) for (value) |child| if (hasCorrelatedBoundary(child)) return true;
            },
            .@"struct" => |s| inline for (s.field_names) |name| if (hasCorrelatedBoundary(@field(value, name))) return true,
            .@"union" => switch (value) {
                inline else => |child| return hasCorrelatedBoundary(child),
            },
            .optional => if (value) |child| return hasCorrelatedBoundary(child),
            else => {},
        }
        return false;
    }
    fn separatedMatches(clauses: []const ast.Clause) bool {
        var matched = false;
        var separated = false;
        for (clauses) |clause| switch (clause.operation) {
            .match => {
                if (matched and separated) return true;
                matched = true;
            },
            .project, .unwind => separated = matched,
            else => {},
        };
        return false;
    }
    fn weightedTail(self: *Engine, clauses: []const ast.Clause) bool {
        _ = self;
        if (clauses.len == 0) return false;
        for (clauses) |clause| switch (clause.operation) {
            .project, .match, .unwind, .subquery => if (!weightedExpression(clause)) return false,
            else => return false,
        };
        return true;
    }
    fn matchReferences(self: *Engine, names: *Names, shape: Row, value: anytype) anyerror!void {
        if (@TypeOf(value) == ast.Clause) if (value.operation == .subquery) return self.retainQueryReferences(names, shape, value.operation.subquery);
        if (@TypeOf(value) == ast.Projection) {
            if (value.wildcard) for (shape) |entry| try names.put(self.a, entry.name, {});
        }
        if (@TypeOf(value) == ast.Match) for (value.paths) |path| {
            for (path.nodes) |node| if (node.alias) |name| try names.put(self.a, name, {});
            for (path.edges) |edge| if (edge.alias) |name| try names.put(self.a, name, {});
            if (path.alias) |name| try names.put(self.a, name, {});
        };
        switch (@typeInfo(@TypeOf(value))) {
            .pointer => |pointer| {
                if (pointer.size == .one and pointer.child == ast.Expr) return self.retainReferences(names, shape, value);
                if (pointer.size == .slice and pointer.child != u8) for (value) |child| try self.matchReferences(names, shape, child);
            },
            .@"struct" => |structure| inline for (structure.field_names) |name| try self.matchReferences(names, shape, @field(value, name)),
            .@"union" => switch (value) {
                inline else => |payload| return self.matchReferences(names, shape, payload),
            },
            .optional => if (value) |child| try self.matchReferences(names, shape, child),
            else => {},
        }
    }
    fn matchDemand(self: *Engine, clauses: []const ast.Clause) !Multiplicity {
        for (clauses) |clause| switch (clause.operation) {
            .match, .unwind => {},
            .project => |p| {
                var calls: std.ArrayList(*const ast.Expr) = .empty;
                for (p.value.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return .exact;
                for (p.value.order) |order| if (!try self.collectStreamingAggregates(order.expression, &calls)) return .exact;
                if (calls.items.len != 0) {
                    var demand: Multiplicity = .presence;
                    for (calls.items) |expression| {
                        const aggregate_call = expression.value.call;
                        if (aggregate_call.distinct or std.ascii.eqlIgnoreCase(aggregate_call.name, "min") or std.ascii.eqlIgnoreCase(aggregate_call.name, "max")) continue;
                        if (!std.ascii.eqlIgnoreCase(aggregate_call.name, "count")) return .exact;
                        demand = .capped;
                    }
                    return demand;
                }
                if (p.value.distinct) return .presence;
                if (p.value.limit) |expression| {
                    // Proof-only folding never raises an error early. The
                    // projection remains responsible for validating its goal.
                    const limit = self.constant(expression, &.{}) orelse return .exact;
                    if (limit != .integer or limit.integer < 0) return .exact;
                    if (p.value.skip) |skip_expression| {
                        const skip = self.constant(skip_expression, &.{}) orelse return .exact;
                        if (skip != .integer or skip.integer < 0) return .exact;
                    }
                    return .capped;
                }
                if (p.value.skip != null) return .exact;
            },
            // Pure correlated apply depends on bindings, not their duplicate
            // count. Its body has a separate aggregate and row-goal scope.
            .subquery => {},
            else => return .exact,
        };
        return self.terminal_weight_demand orelse .exact;
    }
    fn weightedMatch(self: *Engine, input: Row, operation: ast.Match, tail: []const ast.Clause) anyerror!WeightedRows {
        if (operation.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate) and weightedExpression(predicate)) {
            // Correlated predicates need the complete binding, not individual
            // routes. Retain their dependencies in the producer, then evaluate
            // once per bag. OPTIONAL extends only when no candidate qualifies.
            var producer = operation;
            producer.predicate = null;
            producer.optional = false;
            const dependencies = try self.a.alloc(ast.Clause, tail.len + 1);
            // Analysis-only stage: retain predicate dependencies without a
            // wildcard making unused path and interior identities observable.
            dependencies[0] = .{ .span = .{}, .operation = .{ .match = .{ .paths = &.{}, .predicate = predicate } } };
            @memcpy(dependencies[1..], tail);
            const candidates = try self.weightedMatch(input, producer, dependencies);
            var rows: std.ArrayList(Row) = .empty;
            var weights: std.ArrayList(Count) = .empty;
            for (candidates.rows, candidates.weights) |row, weight| {
                const accepted = blk: {
                    var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
                    defer scratch.deinit();
                    const scope = EvalScope.enter(self, scratch.allocator());
                    defer scope.leave(self);
                    break :blk (try (try self.eval(predicate, row, null)).truth()) == true;
                };
                if (accepted) {
                    try rows.append(self.a, row);
                    try weights.append(self.a, weight);
                }
            }
            if (operation.optional and rows.items.len == 0) {
                var extended = input;
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
                try rows.append(self.a, extended);
                try weights.append(self.a, Count.one);
            }
            return .{ .rows = rows.items, .weights = weights.items };
        };
        var shape: Row = input;
        for (operation.paths) |path| {
            for (path.nodes) |node| if (node.alias) |name| {
                shape = try self.replace(shape, name, .null);
            };
            for (path.edges) |edge| if (edge.alias) |name| {
                shape = try self.replace(shape, name, .null);
            };
            if (path.alias) |name| {
                shape = try self.replace(shape, name, .null);
            }
        }
        var names: Names = .empty;
        try self.retainQueryReferences(&names, shape, tail);
        var items: std.ArrayList(ast.Item) = .empty;
        for (shape) |entry| if (names.contains(entry.name)) {
            const expression = try self.a.create(ast.Expr);
            expression.* = .{ .span = .{}, .value = .{ .variable = entry.name } };
            try items.append(self.a, .{ .expression = expression, .alias = entry.name });
        };
        const saved_columns = self.columns;
        const saved_stages = self.stages.items.len;
        const saved_demand = self.match_weight_demand;
        const saved_retention = self.retain_windows;
        defer self.columns = saved_columns;
        defer self.stages.shrinkRetainingCapacity(saved_stages);
        defer self.match_weight_demand = saved_demand;
        defer self.retain_windows = saved_retention;
        self.match_weight_demand = try self.matchDemand(tail);
        self.retain_windows = true;
        if (try self.streamMatchProjection(&.{input}, operation, .{ .items = items.items }, &.{}, .{}, .{})) |_| {
            const weighted = self.pending_window orelse return error.CypherInvalidBackendResponse;
            self.pending_window = null;
            return weighted;
        }
        self.match_weight_demand = null;
        self.retain_windows = false;
        const rows = try self.match(&.{input}, operation);
        const weights = try self.a.alloc(Count, rows.len);
        @memset(weights, Count.one);
        return .{ .rows = rows, .weights = weights };
    }
    fn runNext(self: *Engine, clauses: []const ast.Clause, rows: []const Row, preserve_rows: bool) anyerror![]const Row {
        if (self.pending_window) |weighted| {
            self.pending_window = null;
            if (weighted.rows.ptr != rows.ptr) return error.CypherInvalidBackendResponse;
            return self.runWeighted(clauses, weighted, preserve_rows);
        }
        return self.run(clauses, rows, false, preserve_rows);
    }
    fn runWeighted(self: *Engine, clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool) anyerror![]const Row {
        return (try self.runWeightedRows(clauses, initial, preserve_rows, false)).rows;
    }
    // Only a commutative terminal reduction licenses regrouping nonadjacent
    // bindings. Windows, order-sensitive reductions and procedures stay barriers.
    fn consolidationDemand(self: *Engine, clauses: []const ast.Clause) !?Multiplicity {
        for (clauses) |clause| switch (clause.operation) {
            .match, .unwind => {},
            .project => |p| {
                if (p.value.order.len != 0 or p.value.skip != null or p.value.limit != null) return null;
                var calls: std.ArrayList(*const ast.Expr) = .empty;
                for (p.value.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return null;
                if (calls.items.len == 0) continue;
                for (calls.items) |expression| {
                    const name = expression.value.call.name;
                    if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max")) return null;
                }
                return try self.matchDemand(clauses);
            },
            else => return null,
        };
        return null;
    }
    fn consolidateBags(self: *Engine, bags: WeightedRows, demand: Multiplicity) !WeightedRows {
        if (bags.rows.len < 2) return bags;
        var rows: std.ArrayList(Row) = .empty;
        var weights: std.ArrayList(Count) = .empty;
        var index: value_work.Index = .{};
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var bindings: value_work.Work = .{ .a = scratch.allocator(), .ptr = self, .charge = chargeWork, .binding_identity = true };
        for (bags.rows, bags.weights) |row, weight| {
            const hash = (try bindings.info(.{ .map = row })).hash;
            var candidate = index.head(hash);
            const existing = while (candidate) |position| {
                const link = index.links.items[position];
                if (try bindings.eql(.{ .map = rows.items[link.index] }, .{ .map = row })) break link.index;
                candidate = link.next;
            } else null;
            if (existing) |position| {
                weights.items[position] = switch (demand) {
                    .presence => Count.one,
                    .capped => Count.init(mergePathWays(waysCap(weights.items[position]), waysCap(weight))),
                    .exact => try weights.items[position].add(self.a, weight),
                };
            } else {
                try index.insert(scratch.allocator(), hash, rows.items.len);
                try rows.append(self.a, row);
                try weights.append(self.a, if (demand == .presence) Count.one else weight);
            }
        }
        return .{ .rows = rows.items, .weights = weights.items };
    }
    fn callRowGoal(self: *Engine, clauses: []const ast.Clause) ?usize {
        for (clauses) |clause| {
            if (clause.operation != .project) return null;
            const p = clause.operation.project.value;
            if (p.distinct or p.order.len != 0 or p.predicate != null) return null;
            for (p.items) |item| if (aggregates(item.expression)) return null;
            if (p.limit != null) return self.windowGoal(p);
            if (p.skip != null) return null;
        }
        return null;
    }
    fn boundedCallBody(self: *Engine, inner: []const ast.Clause, goal: usize) ![]const ast.Clause {
        const last = inner[inner.len - 1].operation;
        if (last != .project or !last.project.final or @as(u64, goal) > std.math.maxInt(i64)) return inner;
        var bound = goal;
        if (last.project.value.limit) |expression| {
            const existing = self.constant(expression, &.{}) orelse return inner;
            if (existing != .integer or existing.integer < 0) return inner;
            bound = @intCast(@min(@as(u64, bound), @as(u64, @intCast(existing.integer))));
        }
        const copy = try self.a.dupe(ast.Clause, inner);
        const expression = try self.a.create(ast.Expr);
        expression.* = .{ .span = .{}, .value = .{ .literal = .{ .integer = @intCast(bound) } } };
        copy[copy.len - 1].operation.project.value.limit = expression;
        return copy;
    }
    fn runWeightedRows(self: *Engine, clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool, retain_final: bool) anyerror!WeightedRows {
        var bags = initial;
        const saved_retention = self.retain_windows;
        defer self.retain_windows = saved_retention;
        var clause_index: usize = 0;
        while (clause_index < clauses.len) : (clause_index += 1) {
            const clause = clauses[clause_index];
            try self.checkpoint();
            if (clause.operation == .match and bags.rows.len > 1) if (try self.consolidationDemand(clauses[clause_index..])) |demand| {
                bags = try self.consolidateBags(bags, demand);
            };
            const before = bags.count();
            switch (clause.operation) {
                .match => |operation| {
                    // Fuse row-local stages through the next observable window
                    // or reduction. The producer sees the actual row goal and
                    // can stop before capturing an otherwise unbounded closure.
                    var fused = false;
                    for (clauses[clause_index + 1 ..], clause_index + 1..) |following, index| {
                        if (following.operation == .unwind) continue;
                        if (following.operation != .project) break;
                        const projection = following.operation.project.value;
                        var reduction = false;
                        for (projection.items) |item| reduction = reduction or aggregates(item.expression);
                        if (!following.operation.project.final and !reduction and projection.limit == null) continue;
                        // An unbounded row projection must negotiate the
                        // continuation's weight demand through weightedMatch.
                        if (!reduction and !projection.distinct and projection.limit == null) break;
                        self.retain_windows = (retain_final and index + 1 == clauses.len) or (!following.operation.project.final and self.weightedTail(clauses[index + 1 ..]));
                        if (try self.streamMatchProjectionWeighted(bags.rows, bags.weights, operation, projection, clauses[clause_index + 1 .. index], clause.span, following.span)) |output| {
                            if (self.pending_window) |window| {
                                bags = window;
                                self.pending_window = null;
                            } else {
                                const weights = try self.a.alloc(Count, output.len);
                                @memset(weights, Count.one);
                                bags = .{ .rows = output, .weights = weights };
                            }
                            clause_index = index;
                            fused = true;
                        }
                        break;
                    }
                    if (fused) continue;
                    var rows: std.ArrayList(Row) = .empty;
                    var weights: std.ArrayList(Count) = .empty;
                    const demand = try self.matchDemand(clauses[clause_index + 1 ..]);
                    for (bags.rows, bags.weights) |input, weight| {
                        const children = try self.weightedMatch(input, operation, clauses[clause_index + 1 ..]);
                        for (children.rows, children.weights) |output, child_weight| {
                            try rows.append(self.a, output);
                            const combined = switch (demand) {
                                .presence => Count.one,
                                .capped => Count.init(multiplyPathWays(weight.cap(std.math.maxInt(u64)), child_weight.cap(std.math.maxInt(u64)))),
                                .exact => try weight.multiply(self.a, child_weight),
                            };
                            try weights.append(self.a, combined);
                        }
                    }
                    bags = .{ .rows = rows.items, .weights = weights.items };
                },
                .unwind => |u| {
                    var rows: std.ArrayList(Row) = .empty;
                    var weights: std.ArrayList(Count) = .empty;
                    for (bags.rows, bags.weights) |row, weight| {
                        const value = try self.eval(u.expression, row, null);
                        if (value == .null) continue;
                        const items = if (value == .list) value.list else &.{value};
                        for (items) |item| {
                            try self.state();
                            try rows.append(self.a, try self.replace(row, u.alias, item));
                            try weights.append(self.a, weight);
                        }
                    }
                    bags = .{ .rows = rows.items, .weights = weights.items };
                },
                .project => |p| {
                    self.retain_windows = (retain_final and clause_index + 1 == clauses.len) or (!p.final and self.weightedTail(clauses[clause_index + 1 ..]));
                    var projection = p.value;
                    // Resolve DISTINCT * against the complete current scope,
                    // before reducing multiplicities. Explicit items follow
                    // the wildcard prefix just as in ordinary projection.
                    if (projection.distinct and projection.wildcard) {
                        for (projection.items) |item| if (aggregates(item.expression)) return error.CypherAggregateWildcard;
                        const shape: Row = if (bags.rows.len == 0) &.{} else bags.rows[0];
                        const items = try self.a.alloc(ast.Item, shape.len + projection.items.len);
                        for (shape, items[0..shape.len]) |entry, *item| {
                            const expression = try self.a.create(ast.Expr);
                            expression.* = .{ .span = clause.span, .value = .{ .variable = entry.name } };
                            item.* = .{ .expression = expression, .alias = entry.name };
                        }
                        @memcpy(items[shape.len..], projection.items);
                        projection.items = items;
                        projection.wildcard = false;
                    }
                    var calls: std.ArrayList(*const ast.Expr) = .empty;
                    for (projection.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return error.CypherInvalidAggregate;
                    for (projection.order) |order| if (!try self.collectStreamingAggregates(order.expression, &calls)) return error.CypherInvalidAggregate;
                    const grouped = projection.distinct or calls.items.len != 0;
                    if (grouped and projection.wildcard) return error.CypherAggregateWildcard;
                    var top: ?TopK = if (grouped) null else try TopK.init(self, projection, if (bags.rows.len == 0) &.{} else bags.rows[0]);
                    defer if (top) |*value| value.deinit();
                    var groups: std.ArrayList(StreamGroup) = .empty;
                    var group_index: value_work.Index = .{};
                    defer for (groups.items) |group| for (group.states) |*state_value| {
                        state_value.arena.deinit();
                        state_value.distinct_arena.deinit();
                    };
                    // DISTINCT over an empty binding still has one empty
                    // key per input, and no group at all for empty input.
                    var has_keys = projection.distinct and calls.items.len == 0;
                    for (projection.items) |item| has_keys = has_keys or !aggregates(item.expression);
                    if (calls.items.len != 0 and !has_keys) try groups.append(self.a, try self.newStreamGroup(self.a, &.{}, &.{}, calls.items));
                    var matched: usize = 0;
                    var counts = [_]usize{0};
                    var fold: StreamFold = .{ .retained = self.a, .middle = &.{}, .projection = projection, .calls = calls.items, .top = &top, .groups = &groups, .group_index = &group_index, .has_keys = has_keys, .stage_counts = &counts, .matched = &matched, .distinct = &.{} };
                    for (bags.rows, bags.weights) |row, weight| {
                        fold.weight = weight;
                        fold.add(self, row) catch |err| switch (err) {
                            error.CypherStreamComplete => break,
                            else => return err,
                        };
                    }
                    const rows = if (top) |*value| try value.finish(true) else try self.finishStreamGroups(groups.items, calls.items, projection);
                    if (self.pending_window) |weighted| {
                        self.pending_window = null;
                        bags = weighted;
                    } else {
                        const weights = try self.a.alloc(Count, rows.len);
                        @memset(weights, Count.one);
                        bags = .{ .rows = rows, .weights = weights };
                    }
                },
                .subquery => |inner| {
                    self.recursion += 1;
                    defer self.recursion -= 1;
                    if (self.recursion > self.limits.recursion_depth) return error.CypherRecursionBudgetExceeded;
                    var rows: std.ArrayList(Row) = .empty;
                    var weights: std.ArrayList(Count) = .empty;
                    const last = inner[inner.len - 1].operation;
                    const unit = last != .project or !last.project.final;
                    const demand = try self.matchDemand(clauses[clause_index + 1 ..]);
                    const goal = if (unit) null else self.callRowGoal(clauses[clause_index + 1 ..]);
                    var produced: usize = 0;
                    for (bags.rows, bags.weights) |row, weight| {
                        if (goal) |bound| if (produced >= bound) break;
                        const body = if (goal) |bound| blk: {
                            const remaining = bound - produced;
                            const parent: usize = @intCast(@max(1, weight.cap(remaining)));
                            break :blk try self.boundedCallBody(inner, remaining / parent + @intFromBool(remaining % parent != 0));
                        } else inner;
                        const scope = PatternScope.enter(self);
                        const children = blk: {
                            defer scope.leave(self);
                            self.terminal_weight_demand = if (unit) .presence else demand;
                            break :blk try self.runWeightedRows(body, .{ .rows = &.{row}, .weights = &.{Count.one} }, true, true);
                        };
                        if (unit) {
                            try rows.append(self.a, row);
                            try weights.append(self.a, weight);
                            continue;
                        }
                        for (children.rows, children.weights) |child, child_weight| {
                            var merged = row;
                            for (child) |entry| merged = (try self.bind(merged, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                            try rows.append(self.a, merged);
                            try weights.append(self.a, switch (demand) {
                                .presence => Count.one,
                                .capped => Count.init(multiplyPathWays(weight.cap(std.math.maxInt(u64)), child_weight.cap(std.math.maxInt(u64)))),
                                .exact => try weight.multiply(self.a, child_weight),
                            });
                            if (goal) |bound| produced += @intCast(weights.items[weights.items.len - 1].cap(bound - produced));
                        }
                    }
                    bags = .{ .rows = rows.items, .weights = weights.items };
                },
                else => unreachable,
            }
            try self.stages.append(self.a, .{ .operation = @tagName(clause.operation), .span = clause.span, .input_rows = before, .output_rows = bags.count() });
        }
        if (!preserve_rows and clauses.len != 0 and clauses[clauses.len - 1].operation != .project) return .{ .rows = &.{}, .weights = &.{} };
        return bags;
    }
    fn run(self: *Engine, clauses: []const ast.Clause, input: []const Row, explain: bool, preserve_rows: bool) anyerror![]const Row {
        const prior_retention = self.retain_windows;
        defer self.retain_windows = prior_retention;
        self.retain_windows = false;
        const prior_match_demand = self.match_weight_demand;
        self.match_weight_demand = null;
        defer self.match_weight_demand = prior_match_demand;
        const prior_lower_graph = self.lower_graph;
        const prior_join = self.join;
        const prior_lookup = self.lookup;
        self.lower_graph = null;
        self.join = null;
        self.lookup = null;
        defer self.lower_graph = prior_lower_graph;
        defer self.join = prior_join;
        defer self.lookup = prior_lookup;
        self.recursion += 1;
        defer self.recursion -= 1;
        if (self.recursion > self.limits.recursion_depth) return error.CypherRecursionBudgetExceeded;
        if (!explain and self.weightedTail(clauses) and ((clauses[0].operation != .match and hasCorrelatedBoundary(clauses)) or separatedMatches(clauses))) {
            const weights = try self.a.alloc(Count, input.len);
            @memset(weights, Count.one);
            return self.runWeighted(clauses, .{ .rows = input, .weights = weights }, preserve_rows);
        }
        if (!explain and input.len == 1 and input[0].len == 0 and clauses.len >= 2 and clauses[0].operation == .match and clauses[0].operation.match.paths.len == 1) {
            for (clauses[1..], 1..) |clause, index| {
                if (clause.operation == .unwind) continue;
                if (clause.operation != .project) break;
                const projection = clause.operation.project.value;
                var reduction = false;
                for (projection.items) |item| reduction = reduction or aggregates(item.expression);
                if (!clause.operation.project.final and !reduction and projection.limit == null) continue;
                self.retain_windows = self.weightedTail(clauses[index + 1 ..]);
                if (try self.streamMatchProjection(input, clauses[0].operation.match, projection, clauses[1..index], clauses[0].span, clause.span)) |streamed| {
                    if (index + 1 < clauses.len) return self.runNext(clauses[index + 1 ..], streamed, preserve_rows);
                    return streamed;
                }
                break;
            }
        }
        self.retain_windows = false;
        var rows = input;
        for (clauses, 0..) |clause, clause_index| {
            try self.checkpoint();
            const before = rows.len;
            if (explain) {
                try self.stages.append(self.a, .{ .operation = @tagName(clause.operation), .span = clause.span, .input_rows = 0, .output_rows = 0 });
                continue;
            }
            if (clause.operation == .match) {
                const tail = clauses[clause_index + 1 ..];
                const correlated_predicate = if (clause.operation.match.predicate) |predicate| @import("expressions.zig").containsPattern(predicate) and weightedExpression(predicate) else false;
                if (self.weightedTail(tail) and (hasCorrelatedBoundary(tail) or correlated_predicate)) {
                    // Lower the producer before evaluating pure correlated
                    // expressions, once per binding with its bag weight.
                    var rows_out: std.ArrayList(Row) = .empty;
                    var weights: std.ArrayList(Count) = .empty;
                    for (rows) |row| {
                        const children = try self.weightedMatch(row, clause.operation.match, tail);
                        try rows_out.appendSlice(self.a, children.rows);
                        try weights.appendSlice(self.a, children.weights);
                    }
                    const weighted: WeightedRows = .{ .rows = rows_out.items, .weights = weights.items };
                    try self.stages.append(self.a, .{ .operation = "match", .span = clause.span, .input_rows = before, .output_rows = weighted.count() });
                    return self.runWeighted(tail, weighted, preserve_rows);
                }
                if (try self.tryComponent(rows, clauses[clause_index..])) |component| {
                    if (clause_index + component.next < clauses.len) return self.runNext(clauses[clause_index + component.next ..], component.rows, preserve_rows);
                    return component.rows;
                }
                for (clauses[clause_index + 1 ..], clause_index + 1..) |following, index| {
                    if (following.operation == .unwind) continue;
                    if (following.operation != .project) break;
                    const projection = following.operation.project.value;
                    var reduction = false;
                    for (projection.items) |item| reduction = reduction or aggregates(item.expression);
                    if (!following.operation.project.final and !reduction and projection.limit == null) continue;
                    self.retain_windows = self.weightedTail(clauses[index + 1 ..]);
                    if (try self.streamMatchProjection(rows, clause.operation.match, projection, clauses[clause_index + 1 .. index], clause.span, following.span)) |streamed| {
                        if (index + 1 < clauses.len) return self.runNext(clauses[index + 1 ..], streamed, preserve_rows);
                        return streamed;
                    }
                    break;
                }
            }
            self.retain_windows = false;
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

    fn getNode(self: *Engine, identity: v.Identity) anyerror!?*const v.Node {
        try self.checkpoint();
        if (self.lower_graph) |graph| if (graph.positions.get(identity)) |position| return graph.nodes.items[position].node;
        if (!self.cache_reads) return self.backend.vtable.get(self.backend.ptr, self.a, identity);
        if (self.read_nodes.get(identity)) |cached| return cached;
        const node = try self.backend.vtable.get(self.backend.ptr, self.a, identity);
        const owned = if (node) |found| found.identity else v.Identity{ .table = try self.a.dupe(u8, identity.table), .key = try self.a.dupe(u8, identity.key) };
        try self.read_nodes.put(self.a, owned, node);
        return node;
    }
    fn prefetchEndpoints(self: *Engine, relationships: []const *const v.Relationship, identity: v.Identity, outgoing: bool, incoming: bool) !void {
        if (!self.cache_reads) return;
        const batch = self.backend.vtable.get_many orelse return;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        var pending: std.ArrayList(v.Identity) = .empty;
        var seen: v.IdentitySet = .empty;
        for (relationships) |relationship| {
            const target = if (outgoing and relationship.source.eql(identity)) relationship.target else if (incoming and relationship.target.eql(identity)) relationship.source else continue;
            if (self.lower_graph) |graph| if (graph.positions.contains(target)) continue;
            if (self.read_nodes.contains(target)) continue;
            if ((try seen.getOrPut(temporary, target)).found_existing) continue;
            try pending.append(temporary, target);
        }
        if (pending.items.len == 0) return;
        const nodes = try batch(self.backend.ptr, self.a, pending.items);
        if (nodes.len != pending.items.len) return error.CypherInvalidBackendPage;
        for (pending.items, nodes) |identity_key, node| {
            if (node) |value| if (!value.identity.eql(identity_key)) return error.CypherInvalidBackendPage;
            const owned = if (node) |value| value.identity else v.Identity{ .table = try self.a.dupe(u8, identity_key.table), .key = try self.a.dupe(u8, identity_key.key) };
            try self.read_nodes.put(self.a, owned, node);
        }
    }
    fn firstTableLabel(self: *Engine, index: usize, first: ast.Node) bool {
        const label = self.graph.labels[index];
        if (first.labels.len != 0) return std.mem.eql(u8, first.labels[0], label.name);
        for (self.graph.labels[0..index]) |prior| if (std.mem.eql(u8, prior.table, label.table)) return false;
        return true;
    }
    fn nodeMatches(self: *Engine, raw: *const v.Node, pattern: ast.Node) !bool {
        if (self.isDeleted(.{ .node = raw })) return false;
        const node = if (self.backend.vtable.project_node) |project_node| (try project_node(self.backend.ptr, self.a, raw)) orelse return false else raw;
        const member = for (self.graph.labels) |label| {
            if (try self.graph.admits(node, label.name, &self.values)) break true;
        } else false;
        if (!member) return false;
        for (pattern.labels) |label| {
            if (self.graph.label(label) == null) return error.UnknownGraphLabel;
            if (!try self.graph.admits(node, label, &self.values)) return false;
        }
        return true;
    }
    const Constraint = struct { actual: v.Value, expression: *const ast.Expr };
    const MatchState = struct { row: Row, used: []const *const v.Relationship = &.{}, paths: []const v.Path = &.{}, constraints: []const Constraint = &.{} };
    fn samePath(a: v.Path, b: v.Path) bool {
        return v.Value.eql(.{ .path = a }, .{ .path = b });
    }
    fn sameSelectionGroup(a: MatchState, b: MatchState, selected: usize) bool {
        for (a.paths, b.paths, 0..) |left, right, index| {
            if (index == selected) {
                if (!left.nodes[0].identity.eql(right.nodes[0].identity) or !left.nodes[left.nodes.len - 1].identity.eql(right.nodes[right.nodes.len - 1].identity)) return false;
            } else if (!samePath(left, right)) return false;
        }
        return true;
    }
    const SelectionContext = struct {
        states: []const MatchState,
        selected: usize,
        fn textHash(hasher: *std.hash.Wyhash, text: []const u8) void {
            const length: u64 = @intCast(text.len);
            hasher.update(std.mem.asBytes(&length));
            hasher.update(text);
        }
        fn identityHash(hasher: *std.hash.Wyhash, identity: v.Identity) void {
            textHash(hasher, identity.table);
            textHash(hasher, identity.key);
        }
        pub fn hash(self: @This(), index: usize) u64 {
            var hasher = std.hash.Wyhash.init(0);
            for (self.states[index].paths, 0..) |path, position| {
                if (position == self.selected) {
                    identityHash(&hasher, path.nodes[0].identity);
                    identityHash(&hasher, path.nodes[path.nodes.len - 1].identity);
                } else {
                    const length: u64 = @intCast(path.relationships.len);
                    hasher.update(std.mem.asBytes(&length));
                    for (path.nodes) |node| identityHash(&hasher, node.identity);
                    for (path.relationships) |relationship| {
                        identityHash(&hasher, relationship.identity);
                        textHash(&hasher, relationship.index);
                    }
                }
            }
            return hasher.final();
        }
        pub fn eql(self: @This(), left: usize, right: usize) bool {
            return sameSelectionGroup(self.states[left], self.states[right], self.selected);
        }
    };
    // Resolve the complete MATCH scope before selecting, then group by endpoints
    // and the other complete patterns. Two linear passes preserve surrounding
    // bags and all-shortest ties without comparing every pair of result rows.
    fn selectPaths(self: *Engine, states: []const MatchState, patterns: []const ast.Path) ![]const MatchState {
        const Selection = struct { length: usize, path: v.Path };
        var selected = states;
        for (patterns, 0..) |pattern, index| {
            if (pattern.selector == .all) continue;
            const context: SelectionContext = .{ .states = selected, .selected = index };
            var groups: std.HashMapUnmanaged(usize, Selection, SelectionContext, std.hash_map.default_max_load_percentage) = .empty;
            defer groups.deinit(self.a);
            for (selected, 0..) |candidate, position| {
                try self.checkpoint();
                const path = candidate.paths[index];
                const group = try groups.getOrPutContext(self.a, position, context);
                if (!group.found_existing or path.relationships.len < group.value_ptr.length)
                    group.value_ptr.* = .{ .length = path.relationships.len, .path = path };
            }
            var output: std.ArrayList(MatchState) = .empty;
            for (selected, 0..) |candidate, position| {
                try self.checkpoint();
                const winner = groups.getContext(position, context).?;
                const path = candidate.paths[index];
                if (path.relationships.len == winner.length and (pattern.selector == .all_shortest or samePath(path, winner.path)))
                    try output.append(self.a, candidate);
            }
            selected = try output.toOwnedSlice(self.a);
        }
        return selected;
    }
    // Inline properties are predicates over the complete MATCH scope. Filter
    // immediately when possible; retain forward references until their aliases
    // are bound, including anonymous entities and each variable-length hop.
    fn constrain(self: *Engine, input: MatchState, entity: v.Value, properties: []const ast.Entry) !?MatchState {
        var candidate = input;
        if (properties.len == 0) return candidate;
        const visible = try self.readValue(entity);
        for (properties) |property| {
            const actual = try self.values.property(visible, property.name);
            if (!self.matchScopeReady(candidate.row) and @import("expressions.zig").containsPattern(property.value)) {
                candidate = try self.deferConstraint(candidate, actual, property.value);
                continue;
            }
            const expected = self.eval(property.value, candidate.row, null) catch |err| {
                if (err != error.UnboundCypherVariable) return err;
                candidate = try self.deferConstraint(candidate, actual, property.value);
                continue;
            };
            if ((try self.values.equal(actual, expected)) != true) return null;
        }
        return candidate;
    }
    fn deferConstraint(self: *Engine, input: MatchState, actual: v.Value, expression: *const ast.Expr) !MatchState {
        const pending = try self.a.alloc(Constraint, input.constraints.len + 1);
        @memcpy(pending[0..input.constraints.len], input.constraints);
        pending[input.constraints.len] = .{ .actual = actual, .expression = expression };
        var candidate = input;
        candidate.constraints = pending;
        return candidate;
    }
    fn constraintsMatch(self: *Engine, constraints: []const Constraint, row: Row, partial: bool) !bool {
        for (constraints) |constraint| {
            if (partial and !self.matchScopeReady(row) and @import("expressions.zig").containsPattern(constraint.expression)) continue;
            const expected = self.eval(constraint.expression, row, null) catch |err| {
                if (partial and err == error.UnboundCypherVariable) continue;
                return err;
            };
            if ((try self.values.equal(constraint.actual, expected)) != true) return false;
        }
        return true;
    }
    fn matchScopeReady(self: *Engine, row: Row) bool {
        for (self.match_aliases) |name| if (lookup(row, name) == null) return false;
        return true;
    }
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
        var seen: v.IdentitySet = .empty;
        for (self.graph.labels, 0..) |label, label_index| {
            if (!self.firstTableLabel(label_index, first)) continue;
            var cursor: ?[]const u8 = null;
            while (output.items.len < goal) {
                try self.checkpoint();
                const page = try self.scanNodes(label.table, first, operation.predicate, rows[0], cursor, @min(128, goal - output.items.len));
                const examined = page.examined orelse page.items.len;
                if (examined < page.items.len) return error.CypherInvalidBackendPage;
                if (examined > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.stats.scanned_anchors += examined;
                for (page.items) |node| {
                    try chargeWork(self, 1 + node.identity.table.len / 64 + node.identity.key.len / 64);
                    if (self.stream_fold == null and self.shortest_count == null) {
                        if ((try seen.getOrPut(self.a, node.identity)).found_existing) continue;
                    }
                    if (!try self.nodeMatches(node, first)) continue;
                    var bound = (try self.bind(rows[0], first.alias, .{ .node = node })) orelse continue;
                    if (path.alias) |alias| bound = (try self.bind(bound, alias, .{ .path = .{ .nodes = try self.a.dupe(*const v.Node, &.{node}), .relationships = &.{} } })) orelse continue;
                    const candidate = (try self.constrain(.{ .row = bound }, .{ .node = node }, first.properties)) orelse continue;
                    if (!try self.constraintsMatch(candidate.constraints, bound, false)) continue;
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
        // Reuse the route producer with a row collector when downstream
        // operators require materialization. Join planning and early predicate
        // evaluation must not depend on projection/aggregate fusion.
        if (operation.paths.len > 0) {
            const previous_columns = self.columns;
            const stage_count = self.stages.items.len;
            defer self.columns = previous_columns;
            defer self.stages.shrinkRetainingCapacity(stage_count);
            if (try self.streamMatchProjection(rows, operation, .{ .items = &.{}, .wildcard = true }, &.{}, .{}, .{})) |matched| return matched;
        }
        const previous_fold = self.stream_fold;
        const previous_count = self.shortest_count;
        self.stream_fold = null;
        self.shortest_count = null;
        defer {
            self.stream_fold = previous_fold;
            self.shortest_count = previous_count;
        }
        const prior_aliases = self.match_aliases;
        const prior_collect = self.collect_match_paths;
        const prior_shortest = self.shortest_fast;
        defer self.shortest_fast = prior_shortest;
        self.shortest_fast = false;
        defer self.match_aliases = prior_aliases;
        defer self.collect_match_paths = prior_collect;
        self.collect_match_paths = for (operation.paths) |path| {
            if (path.selector != .all) break true;
        } else false;
        var aliases: std.ArrayList([]const u8) = .empty;
        for (operation.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| try aliases.append(self.a, alias);
            for (path.edges) |edge| if (edge.alias) |alias| try aliases.append(self.a, alias);
            if (path.alias) |alias| try aliases.append(self.a, alias);
        }
        self.match_aliases = aliases.items;
        var output: std.ArrayList(Row) = .empty;
        for (rows) |row| {
            var states: []const MatchState = &.{.{ .row = row }};
            for (operation.paths, 0..) |path, path_index| {
                // Only later patterns can invalidate an otherwise shortest
                // route. Earlier bindings are already available to this path.
                self.shortest_fast = path_index + 1 == operation.paths.len;
                var next: std.ArrayList(MatchState) = .empty;
                for (states) |input| try self.matchPath(input, path, operation.mode, operation.predicate, &next);
                states = try next.toOwnedSlice(self.a);
            }
            var eligible: std.ArrayList(MatchState) = .empty;
            for (states) |matched| if (try self.constraintsMatch(matched.constraints, matched.row, false)) try eligible.append(self.a, matched);
            const initial = output.items.len;
            for (try self.selectPaths(eligible.items, operation.paths)) |matched| {
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
    // An indexed inner lookup remains an indexed lookup. A certified full
    // scan can instead build a bounded, statement-local hash relation once.
    // Cache quota exhaustion declines the optimization and reclaims its arena;
    // global statement allocation/cancellation failures still propagate.
    const Lookup = struct {
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        rows: std.ArrayList(*const v.Node) = .empty,
        index: value_work.Index = .{},
        table: []const u8 = "",
        property: ?[]const u8 = null,
        key: v.Value = .null,
        disabled: bool = false,
        complete: bool = false,
        fn decline(self: *@This()) void {
            if (self.arena) |*arena| arena.deinit();
            self.arena = null;
            self.rows = .empty;
            self.index = .{};
            self.disabled = true;
        }
        fn capture(self: *@This(), engine: *Engine, page: backend_mod.Page(*const v.Node)) !void {
            if (self.disabled or self.complete) return;
            if (!page.unfiltered) return self.decline();
            if (self.arena == null) self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
            self.captureRows(engine, page.items) catch |err| {
                if (err == error.OutOfMemory and self.budget.exhausted) return self.decline();
                return err;
            };
            if (!self.disabled and page.next == null) self.complete = true;
        }
        fn captureRows(self: *@This(), engine: *Engine, nodes: []const *const v.Node) !void {
            const a = self.arena.?.allocator();
            for (nodes) |node| {
                const key = (v.Value{ .node = node }).property(self.property.?);
                if (key == .null) continue;
                const hash = (try engine.values.info(key)).hash;
                const owned = (try (v.Value{ .node = node }).clone(a)).node;
                try self.index.insert(a, hash, self.rows.items.len);
                try self.rows.append(a, owned);
            }
        }
        fn cachedPage(self: *@This(), engine: *Engine, after: ?[]const u8, demand: usize) !backend_mod.Page(*const v.Node) {
            const hash = (try engine.values.info(self.key)).hash;
            var candidate = if (after) |cursor| try std.fmt.parseUnsigned(usize, cursor, 10) else self.index.head(hash);
            var nodes: std.ArrayList(*const v.Node) = .empty;
            while (candidate) |position| {
                if (nodes.items.len == demand) break;
                const link = self.index.links.items[position];
                const node = self.rows.items[link.index];
                if ((try engine.values.equal((v.Value{ .node = node }).property(self.property.?), self.key)) == true) try nodes.append(engine.a, node);
                candidate = link.next;
            }
            return .{ .items = try nodes.toOwnedSlice(engine.a), .next = if (candidate) |position| try std.fmt.allocPrint(engine.a, "{d}", .{position}) else null };
        }
    };
    const Join = struct { operation: ast.Match, next: usize };
    fn joinPath(self: *Engine, input: MatchState, operation: ast.Match, index: usize, output: *std.ArrayList(MatchState)) anyerror!void {
        const previous = self.join;
        const previous_lookup = self.lookup;
        self.lookup = null;
        defer self.lookup = previous_lookup;
        const path = operation.paths[index];
        if (index < self.lookups.len and input.row.len != 0 and path.nodes.len == 1 and path.nodes[0].labels.len == 1) {
            const node = path.nodes[0];
            if (node.alias) |alias| {
                var filters: std.ArrayList(backend_mod.PropertyFilter) = .empty;
                if (operation.predicate) |predicate| try self.predicateFilters(predicate, alias, input.row, &filters);
                for (node.properties) |property| if (self.constant(property.value, input.row)) |value| try filters.append(self.a, .{ .property = property.name, .value = value });
                for (filters.items) |filter| if (filter.op == .eq) {
                    const lookup_plan = &self.lookups[index];
                    const table = self.graph.label(node.labels[0]).?.table;
                    if (lookup_plan.property == null) {
                        lookup_plan.table = table;
                        lookup_plan.property = filter.property;
                    }
                    if (std.mem.eql(u8, lookup_plan.property.?, filter.property) and std.mem.eql(u8, lookup_plan.table, table)) {
                        lookup_plan.key = filter.value;
                        self.lookup = lookup_plan;
                    }
                    break;
                };
            }
        }
        const continuation: Join = .{ .operation = operation, .next = index + 1 };
        self.join = if (index + 1 < operation.paths.len) &continuation else null;
        defer self.join = previous;
        try self.matchPath(input, operation.paths[index], operation.mode, operation.predicate, output);
    }
    fn constant(self: *Engine, expression: *const ast.Expr, row: Row) ?v.Value {
        var remaining: usize = 128;
        return self.constantBounded(expression, row, &remaining, 0);
    }
    fn constantBounded(self: *Engine, expression: *const ast.Expr, row: Row, remaining: *usize, depth: usize) ?v.Value {
        if (remaining.* == 0 or depth >= @min(64, self.limits.recursion_depth)) return null;
        remaining.* -= 1;
        return switch (expression.value) {
            .unary => |unary| blk: {
                const operand = self.constantBounded(unary.operand, row, remaining, depth + 1) orelse return null;
                if (unary.op == .is_null or unary.op == .is_not_null) break :blk .{ .boolean = (operand == .null) == (unary.op == .is_null) };
                if (operand == .null) break :blk .null;
                break :blk switch (unary.op) {
                    .positive => if (operand == .integer or operand == .number) operand else null,
                    .negative => if (operand == .integer) .{ .integer = std.math.negate(operand.integer) catch return null } else if (operand == .number) .{ .number = -operand.number } else null,
                    .not => if (operand == .boolean) .{ .boolean = !operand.boolean } else null,
                    else => unreachable,
                };
            },
            .binary => |binary_expression| blk: {
                switch (binary_expression.op) {
                    .add, .subtract, .multiply, .divide, .modulo, .power => {},
                    else => return null,
                }
                const left = self.constantBounded(binary_expression.left, row, remaining, depth + 1) orelse return null;
                const right = self.constantBounded(binary_expression.right, row, remaining, depth + 1) orelse return null;
                // Scalar arithmetic allocates nothing and cannot consume work.
                // Failed folds remain residuals, preserving runtime errors.
                if ((left != .null and left != .integer and left != .number) or (right != .null and right != .integer and right != .number)) return null;
                break :blk self.binary(binary_expression.op, left, right) catch null;
            },
            .property => |property| if (self.read_only) if (self.constantBounded(property.base, row, remaining, depth + 1)) |base| base.property(property.name) else null else null,
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
    // OR hints are atomic: every arm must describe membership of the same
    // property. The original expression remains the authoritative residual.
    fn membershipFilter(self: *Engine, expression: *const ast.Expr, alias: []const u8, row: Row, property: *?[]const u8, items: *std.ArrayList(v.Value)) anyerror!bool {
        if (expression.value == .binary and expression.value.binary.op == .@"or") {
            return try self.membershipFilter(expression.value.binary.left, alias, row, property, items) and
                try self.membershipFilter(expression.value.binary.right, alias, row, property, items);
        }
        var leaf: std.ArrayList(backend_mod.PropertyFilter) = .empty;
        try self.predicateFilters(expression, alias, row, &leaf);
        if (leaf.items.len != 1) return false;
        const filter = leaf.items[0];
        if (filter.op != .eq and filter.op != .in) return false;
        if (property.*) |name| {
            if (!std.mem.eql(u8, name, filter.property)) return false;
        } else property.* = filter.property;
        const values = if (filter.op == .in and filter.value == .list) filter.value.list else if (filter.op == .eq) &.{filter.value} else return false;
        if (values.len > 256 -| items.items.len) return false;
        try items.appendSlice(self.a, values);
        return true;
    }
    fn labelFilters(self: *Engine, node: ast.Node, filters: *std.ArrayList(backend_mod.PropertyFilter)) !void {
        // An unlabeled pattern is a union of catalog labels, not their
        // conjunction. Only explicitly required labels imply these equalities.
        for (node.labels) |name| if (self.graph.label(name)) |label| {
            for (label.predicate) |entry| try filters.append(self.a, .{ .property = entry.name, .value = entry.value });
        };
    }
    fn predicateFilters(self: *Engine, expression: *const ast.Expr, alias: []const u8, row: Row, filters: *std.ArrayList(backend_mod.PropertyFilter)) !void {
        if (expression.value == .binary and expression.value.binary.op == .@"and") {
            try self.predicateFilters(expression.value.binary.left, alias, row, filters);
            return self.predicateFilters(expression.value.binary.right, alias, row, filters);
        }
        if (expression.value == .binary and expression.value.binary.op == .@"or") {
            var property: ?[]const u8 = null;
            var items: std.ArrayList(v.Value) = .empty;
            if (try self.membershipFilter(expression, alias, row, &property, &items)) {
                try filters.append(self.a, .{ .property = property.?, .value = .{ .list = items.items }, .op = .in });
            }
            return;
        }
        if (expression.value == .comparison and expression.value.comparison.operators.len > 1) {
            const chain = expression.value.comparison;
            for (chain.operators, 0..) |op, i| {
                const comparison: ast.Expr = .{ .span = expression.span, .value = .{ .binary = .{ .left = chain.operands[i], .right = chain.operands[i + 1], .op = op } } };
                try self.predicateFilters(&comparison, alias, row, filters);
            }
            return;
        }
        const pair: struct { left: *const ast.Expr, right: *const ast.Expr, op: ast.Binary } = switch (expression.value) {
            .binary => |p| .{ .left = p.left, .right = p.right, .op = p.op },
            .comparison => |chain| if (chain.operands.len == 2 and chain.operators.len == 1)
                .{ .left = chain.operands[0], .right = chain.operands[1], .op = chain.operators[0] }
            else
                return,
            else => return,
        };
        const op: backend_mod.FilterOp = switch (pair.op) {
            .eq => .eq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .in => .in,
            else => return,
        };
        const operands = [_]*const ast.Expr{ pair.left, pair.right };
        for (0..2) |index| {
            if (op == .in and index != 0) continue;
            const property = operands[index];
            if (property.value != .property or property.value.property.base.value != .variable or !std.mem.eql(u8, property.value.property.base.value.variable, alias)) continue;
            var value = self.constant(operands[1 - index], row);
            if (op == .in and operands[1 - index].value == .list) {
                var items: std.ArrayList(v.Value) = .empty;
                for (operands[1 - index].value.list) |item| try items.append(self.a, self.constant(item, row) orelse return);
                value = .{ .list = items.items };
            }
            if (value) |operand| try filters.append(self.a, .{
                .property = property.value.property.name,
                .value = operand,
                .op = if (index == 0) op else switch (op) {
                    .lt => .gt,
                    .lte => .gte,
                    .gt => .lt,
                    .gte => .lte,
                    else => op,
                },
            });
        }
    }

    fn scanNodes(self: *Engine, table: []const u8, first: ast.Node, predicate: ?*const ast.Expr, row: Row, cursor: ?[]const u8, demand: usize) !backend_mod.Page(*const v.Node) {
        const bounded_demand = @min(demand, @max(@as(usize, 1), self.limits.scanned_anchors -| self.stats.scanned_anchors));
        if (self.lookup) |lookup_plan| if (lookup_plan.complete and std.mem.eql(u8, lookup_plan.table, table)) return lookup_plan.cachedPage(self, cursor, bounded_demand);
        if (self.backend.vtable.scan_where) |scan_where| {
            var filters: std.ArrayList(backend_mod.PropertyFilter) = .empty;
            try self.labelFilters(first, &filters);
            for (first.properties) |property| if (self.constant(property.value, row)) |value| try filters.append(self.a, .{ .property = property.name, .value = value });
            if (predicate) |expression| if (first.alias) |alias| try self.predicateFilters(expression, alias, row, &filters);
            if (filters.items.len != 0) {
                const page = try scan_where(self.backend.ptr, self.a, table, filters.items, cursor, bounded_demand);
                if (self.lookup) |lookup_plan| if (std.mem.eql(u8, lookup_plan.table, table)) try lookup_plan.capture(self, page);
                return page;
            }
        }
        const page = try self.backend.vtable.scan(self.backend.ptr, self.a, table, cursor, bounded_demand);
        if (self.lookup) |lookup_plan| if (std.mem.eql(u8, lookup_plan.table, table)) try lookup_plan.capture(self, page);
        return page;
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
    fn anchorScore(self: *Engine, node: ast.Node, predicate: ?*const ast.Expr, row: Row) !usize {
        if (node.alias) |alias| if (lookup(row, alias) != null) return 1_000_000;
        if (self.anchorKey(predicate, node.alias, row) != null) return 100_000;
        var filters: std.ArrayList(backend_mod.PropertyFilter) = .empty;
        try self.labelFilters(node, &filters);
        for (node.properties) |property| if (self.constant(property.value, row)) |value| try filters.append(self.a, .{ .property = property.name, .value = value });
        if (predicate) |expression| if (node.alias) |alias| try self.predicateFilters(expression, alias, row, &filters);
        return (if (self.backend.vtable.scan_where != null) @as(usize, 100) * @min(filters.items.len, 999) else 0) + @intFromBool(node.labels.len > 0);
    }
    fn matchPath(self: *Engine, input: MatchState, original: ast.Path, mode: ast.PathMode, predicate: ?*const ast.Expr, output: *std.ArrayList(MatchState)) anyerror!void {
        var path = original;
        const batch_planned = if (self.anchor_batch) |batch| batch.path.nodes.ptr == path.nodes.ptr else false;
        if (!batch_planned and !path.execution_reversed and !path.execution_prefix and path.nodes.len > 2) {
            var anchor: usize = 0;
            var score = try self.anchorScore(path.nodes[0], predicate, input.row);
            for (path.nodes[1..], 1..) |node, index| {
                const candidate = try self.anchorScore(node, predicate, input.row);
                if (candidate > score) {
                    anchor = index;
                    score = candidate;
                }
            }
            if (anchor != 0 and anchor + 1 != path.nodes.len) return self.matchInterior(input, path, mode, predicate, anchor, output);
        }
        if (!batch_planned and !path.execution_reversed and path.edges.len != 0 and
            try self.anchorScore(path.nodes[path.nodes.len - 1], predicate, input.row) > try self.anchorScore(path.nodes[0], predicate, input.row))
        {
            const nodes = try self.a.dupe(ast.Node, path.nodes);
            const edges = try self.a.dupe(ast.Edge, path.edges);
            std.mem.reverse(ast.Node, nodes);
            std.mem.reverse(ast.Edge, edges);
            for (edges) |*edge| edge.direction = switch (edge.direction) {
                .out => .in,
                .in => .out,
                .both => .both,
            };
            path.nodes = nodes;
            path.edges = edges;
            path.execution_reversed = true;
        }
        const first = path.nodes[0];
        if (first.alias) |alias| if (lookup(input.row, alias)) |retained| {
            const bound = try self.readValue(retained);
            if (bound == .null) return;
            if (bound != .node) return error.CypherTypeMismatch;
            if (!try self.nodeMatches(bound.node, first)) return;
            const candidate = (try self.constrain(input, bound, first.properties)) orelse return;
            try self.startPath(candidate, path, mode, bound.node, output);
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
                const node = (try self.getNode(.{ .table = label.table, .key = key.string })) orelse continue;
                if (!try self.nodeMatches(node, first)) continue;
                const bound = (try self.bind(input.row, first.alias, .{ .node = node })) orelse continue;
                const candidate = (try self.constrain(.{ .row = bound, .used = input.used, .paths = input.paths, .constraints = input.constraints }, .{ .node = node }, first.properties)) orelse continue;
                try self.startPath(candidate, path, mode, node, output);
            }
            return;
        }
        var seen: v.IdentitySet = .empty;
        for (self.graph.labels, 0..) |label, label_index| {
            if (!self.firstTableLabel(label_index, first)) continue;
            var cursor: ?[]const u8 = null;
            var cursor_bytes: std.ArrayList(u8) = .empty;
            defer cursor_bytes.deinit(self.scratch_allocator);
            while (true) {
                var page_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
                defer page_arena.deinit();
                const page_scope = if (self.stream_fold != null or self.shortest_count != null) EvalScope.enter(self, page_arena.allocator()) else null;
                if (page_scope != null) self.cache_reads = true;
                defer if (page_scope) |scope| scope.leave(self);
                try self.checkpoint();
                const page = try self.scanNodes(label.table, first, predicate, input.row, cursor, if (self.stream_fold) |fold| fold.demand() else 128);
                const examined = page.examined orelse page.items.len;
                if (examined < page.items.len) return error.CypherInvalidBackendPage;
                if (examined > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.stats.scanned_anchors += examined;
                for (page.items) |node| {
                    try chargeWork(self, 1 + node.identity.table.len / 64 + node.identity.key.len / 64);
                    if (self.stream_fold == null and self.shortest_count == null) {
                        if ((try seen.getOrPut(self.a, node.identity)).found_existing) continue;
                    }
                    if (!try self.nodeMatches(node, first)) continue;
                    const bound = (try self.bind(input.row, first.alias, .{ .node = node })) orelse continue;
                    const candidate = (try self.constrain(.{ .row = bound, .used = input.used, .paths = input.paths, .constraints = input.constraints }, .{ .node = node }, first.properties)) orelse continue;
                    try self.startPath(candidate, path, mode, node, output);
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                if (self.stream_fold != null or self.shortest_count != null) {
                    cursor_bytes.clearRetainingCapacity();
                    try cursor_bytes.appendSlice(self.scratch_allocator, page.next.?);
                    cursor = cursor_bytes.items;
                } else cursor = page.next;
            }
        }
    }
    const Interior = struct { path: ast.Path, mode: ast.PathMode, anchor: usize };
    fn matchInterior(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, predicate: ?*const ast.Expr, anchor: usize, output: *std.ArrayList(MatchState)) !void {
        const nodes = try self.a.dupe(ast.Node, path.nodes[0 .. anchor + 1]);
        const edges = try self.a.dupe(ast.Edge, path.edges[0..anchor]);
        std.mem.reverse(ast.Node, nodes);
        std.mem.reverse(ast.Edge, edges);
        for (edges) |*edge| edge.direction = switch (edge.direction) {
            .out => .in,
            .in => .out,
            .both => .both,
        };
        const continuation: Interior = .{ .path = path, .mode = mode, .anchor = anchor };
        const previous = self.interior;
        self.interior = &continuation;
        defer self.interior = previous;
        // Complete each prefix directly into its suffix. This retains neither
        // a materialized half-join nor a separate path/selector for the prefix.
        try self.matchPath(input, .{ .span = path.span, .alias = null, .nodes = nodes, .edges = edges, .execution_reversed = true, .execution_prefix = true }, mode, predicate, output);
    }
    const BreadthParent = struct { previous: usize, relationship: *const v.Relationship };
    const BreadthState = struct { node: *const v.Node, parent: ?usize = null, relationship: ?*const v.Relationship = null, alternatives: std.ArrayList(BreadthParent) = .empty, depth: usize = 0, branch: usize = 0, ways: u64 = 1 };
    // Count a regular path using (pattern edge, hop, graph identity) states.
    // Acyclicity proves that merging prefixes preserves both trail and simple
    // path uniqueness. Otherwise retain adjacency and enumerate exact histories.
    fn countAllPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, start: *const v.Node, output: *std.ArrayList(MatchState)) anyerror!bool {
        return self.countAllPaths(input, path, mode, &.{start}, output);
    }
    fn countAllPaths(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, starts: []const *const v.Node, output: *std.ArrayList(MatchState)) anyerror!bool {
        // Native exact consumers keep arbitrary-precision producer counts.
        // Bounded/presence consumers and SQL cursors retain the compact u64 DP.
        if (self.weighted_sink == null and self.exactPathWeights())
            return self.countAllPathsTyped(Count, input, path, mode, starts, output);
        return self.countAllPathsTyped(u64, input, path, mode, starts, output);
    }
    fn exactPathWeights(self: *Engine) bool {
        if (self.match_weight_demand) |demand| return demand == .exact;
        const fold = self.stream_fold orelse return false;
        for (fold.middle) |clause| if (clause.operation == .project and clause.operation.project.value.distinct) return false;
        for (fold.calls, 0..) |expression, index| {
            if (fold.active_calls) |active| if (!active[index]) continue;
            const call_value = expression.value.call;
            if (!call_value.distinct and (std.ascii.eqlIgnoreCase(call_value.name, "sum") or std.ascii.eqlIgnoreCase(call_value.name, "avg"))) return true;
        }
        return false;
    }
    fn waysInit(comptime W: type, value: u64) W {
        return if (W == Count) Count.init(value) else value;
    }
    fn waysCap(value: anytype) u64 {
        return if (@TypeOf(value) == Count) value.cap(std.math.maxInt(u64)) else value;
    }
    fn waysAdd(self: *Engine, left: anytype, right: @TypeOf(left)) !@TypeOf(left) {
        return if (@TypeOf(left) == Count) left.add(self.a, right) else mergePathWays(left, right);
    }
    fn waysMultiply(self: *Engine, left: anytype, right: @TypeOf(left)) !@TypeOf(left) {
        return if (@TypeOf(left) == Count) left.multiply(self.a, right) else multiplyPathWays(left, right);
    }
    fn countAllPathsTyped(self: *Engine, comptime W: type, input: MatchState, path: ast.Path, mode: ast.PathMode, starts: []const *const v.Node, output: *std.ArrayList(MatchState)) anyerror!bool {
        if (path.edges.len == 0 or input.used.len != 0 or input.constraints.len != 0 or path.execution_prefix) return false;
        // A bound single-hop endpoint admits a physical pair seek instead of
        // discovering the anchor's complete reachable adjacency.
        if (path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1)
            if (path.nodes[1].alias) |alias| if (lookup(input.row, alias) != null) return false;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const scope = EvalScope.enter(self, scratch.allocator());
        defer scope.leave(self);
        self.cache_reads = true;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        var maximum: usize = 0;
        var domains: std.ArrayList(ast.Edge) = .empty;
        const edge_domains = try self.a.alloc(usize, path.edges.len);
        for (path.nodes) |node| {
            if (node.alias) |alias| {
                if (names.contains(alias)) return false;
                try names.put(self.a, alias, {});
            }
            for (node.properties) |entry| if (self.constant(entry.value, input.row) == null) return false;
        }
        if (path.alias) |alias| {
            if (lookup(input.row, alias) != null or names.contains(alias)) return false;
            try names.put(self.a, alias, {});
        }
        for (path.edges, 0..) |edge, index| {
            edge_domains[index] = for (domains.items, 0..) |domain, i| {
                if (edge.direction == domain.direction and sameTypeSet(edge.types, domain.types)) break i;
            } else blk: {
                try domains.append(self.a, .{ .span = .{}, .direction = edge.direction, .types = edge.types });
                break :blk domains.items.len - 1;
            };
            if (edge.alias) |alias| {
                if (lookup(input.row, alias) != null or names.contains(alias)) return false;
                try names.put(self.a, alias, {});
            }
            for (edge.properties) |entry| if (self.constant(entry.value, input.row) == null) return false;
            if (edge.max_hops) |bound| maximum = std.math.add(usize, maximum, bound) catch return false;
        }
        if (maximum > self.limits.path_depth) return false;
        for (self.component_leaves) |leaf| {
            if (leaf.edge.alias) |alias| {
                if (lookup(input.row, alias) != null or names.contains(alias)) return error.CypherComponentDeclined;
                try names.put(self.a, alias, {});
            }
            for (leaf.node.properties) |entry| if (self.constant(entry.value, input.row) == null) return error.CypherComponentDeclined;
            for (leaf.edge.properties) |entry| if (self.constant(entry.value, input.row) == null) return error.CypherComponentDeclined;
        }
        // Each product transition retains its own constant property residual.
        const capture_edge = domains.items[0];
        const capture_path: ast.Path = .{ .span = path.span, .alias = null, .nodes = &.{ path.nodes[0], path.nodes[path.nodes.len - 1] }, .edges = &.{capture_edge} };
        var graph: LowerGraph = .{ .input = input, .path = capture_path, .domains = domains.items, .qualified = true, .mode = mode };
        for (starts) |start| {
            const entry = try graph.positions.getOrPut(self.a, start.identity);
            if (!entry.found_existing) {
                entry.value_ptr.* = graph.nodes.items.len;
                try graph.nodes.append(self.a, .{ .node = start, .depth = 0 });
            }
        }
        const Counts = std.AutoHashMapUnmanaged(usize, W);
        var current: Counts = .empty;
        var next: Counts = .empty;
        var endpoints: Counts = .empty;
        defer current.deinit(self.a);
        defer next.deinit(self.a);
        defer endpoints.deinit(self.a);
        for (starts) |start| {
            const entry = try current.getOrPut(self.a, graph.positions.get(start.identity).?);
            entry.value_ptr.* = try self.waysAdd(if (entry.found_existing) entry.value_ptr.* else waysInit(W, 0), waysInit(W, 1));
        }
        var effective_maximum: usize = 0;
        for (path.edges, 0..) |edge, edge_index| {
            if (current.count() == 0) return true;
            const domain = edge_domains[edge_index];
            var topological: ?[]const usize = null;
            const upper = edge.max_hops orelse blk: {
                // Discover only the authorized, property-qualified closure of
                // this segment. A topological proof supplies a finite bound;
                // cyclic trails keep their exact relationship histories.
                var pending: std.ArrayList(usize) = .empty;
                var seen: std.AutoHashMapUnmanaged(usize, void) = .empty;
                var roots = current.keyIterator();
                while (roots.next()) |index| {
                    try seen.put(self.a, index.*, {});
                    try pending.append(self.a, index.*);
                }
                var head: usize = 0;
                while (head < pending.items.len) : (head += 1) {
                    const index = pending.items[head];
                    const range = try self.loadLowerDomain(&graph, index, domain);
                    for (graph.nodes.items[index].arcs.items[range.first..range.last]) |arc| {
                        try self.state();
                        if (!try self.constantProperties(.{ .relationship = arc.relationship }, edge.properties, input.row)) continue;
                        const entry = try seen.getOrPut(self.a, arc.target);
                        if (!entry.found_existing) try pending.append(self.a, arc.target);
                    }
                }
                const depth = try self.lowerDomainDepth(&graph, domain, edge, &current);
                if (depth == null or depth.?.maximum > self.limits.path_depth) {
                    if (self.weighted_sink != null or starts.len != 1) return false;
                    const previous = self.lower_graph;
                    self.lower_graph = &graph;
                    defer self.lower_graph = previous;
                    var resumed = input;
                    resumed.row = (try self.bind(input.row, path.nodes[0].alias, .{ .node = starts[0] })).?;
                    try self.pathStep(resumed, path, mode, 0, &.{starts[0]}, &.{}, output);
                    return true;
                }
                topological = depth.?.order;
                break :blk depth.?.maximum;
            };
            effective_maximum = std.math.add(usize, effective_maximum, upper) catch return false;
            if (effective_maximum > self.limits.path_depth) return false;
            if (topological == null and path.edges.len == 1 and upper > 1 and upper >= starts.len) {
                // A long-range reduction often covers an entire DAG. Probe
                // only nodes reachable within the requested upper bound. If
                // its longest captured route fits, the same linear reduction
                // is exact for bounded ranges too; otherwise retain hop states.
                var head: usize = 0;
                while (head < graph.nodes.items.len) : (head += 1) {
                    if (graph.nodes.items[head].depth >= upper) continue;
                    _ = try self.loadLowerDomain(&graph, head, domain);
                }
                if (try self.lowerDomainDepth(&graph, domain, edge, &current)) |proof| {
                    if (proof.maximum <= upper) topological = proof.order;
                }
            }
            if (edge.min_hops == 1 and upper == 1) {
                // A fixed transition has no zero-hop acceptance. Apply its
                // destination admission while transferring weights directly,
                // without scanning/copying an extra frontier per pattern edge.
                try chargeWork(self, next.capacity());
                try self.advanceLowerTarget(&graph, domain, edge, &current, &next, path.nodes[edge_index + 1], edge_index);
                std.mem.swap(Counts, &current, &next);
                continue;
            }
            try chargeWork(self, endpoints.capacity());
            endpoints.clearRetainingCapacity();
            if (topological != null) {
                // First form exactly the mandatory minimum-hop frontier, then
                // sum every allowed tail length in one topological pass. Never
                // subtract short paths from saturated counts: that can conceal
                // overflow in a qualifying endpoint's bag.
                if (edge.min_hops > upper) {
                    current.clearRetainingCapacity();
                    continue;
                }
                var mandatory: usize = 0;
                while (mandatory < edge.min_hops and current.count() != 0) : (mandatory += 1) {
                    try chargeWork(self, @as(usize, current.capacity()) + next.capacity());
                    try self.advanceLower(&graph, domain, edge, &current, &next, if (mandatory == 0) edge_index else null);
                    std.mem.swap(Counts, &current, &next);
                }
                const reached = try self.a.alloc(W, graph.nodes.items.len);
                @memset(reached, waysInit(W, 0));
                for (topological.?) |index| {
                    try self.state();
                    const seed = current.get(index) orelse waysInit(W, 0);
                    const ways = try self.waysAdd(reached[index], seed);
                    const accepted = ways;
                    const node = graph.nodes.items[index].node;
                    if (waysCap(accepted) != 0 and try self.nodeMatches(node, path.nodes[edge_index + 1]) and
                        boundNodeMatches(node, path.nodes[edge_index + 1], input.row) and try self.constantProperties(.{ .node = node }, path.nodes[edge_index + 1].properties, input.row))
                        try endpoints.put(self.a, index, accepted);
                    if (waysCap(ways) == 0) continue;
                    if (graph.nodes.items[index].domains.get(domain)) |range| for (graph.nodes.items[index].arcs.items[range.first..range.last]) |*arc| {
                        try self.state();
                        if (!try self.constantProperties(.{ .relationship = arc.relationship }, edge.properties, input.row)) continue;
                        arc.eligible = true;
                        reached[arc.target] = try self.waysAdd(reached[arc.target], ways);
                    };
                }
                std.mem.swap(Counts, &current, &endpoints);
                continue;
            }
            var hop: usize = 0;
            while (current.count() != 0 and hop <= upper) : (hop += 1) {
                // Hash-table iteration and clearing consume work even after a
                // wide frontier contracts to a handful of active states.
                try chargeWork(self, @as(usize, current.capacity()) * 2 + next.capacity());
                var candidates = current.iterator();
                while (candidates.next()) |candidate| {
                    try self.state();
                    const index = candidate.key_ptr.*;
                    const ways = candidate.value_ptr.*;
                    const node = graph.nodes.items[index].node;
                    if (hop >= edge.min_hops and try self.nodeMatches(node, path.nodes[edge_index + 1]) and
                        boundNodeMatches(node, path.nodes[edge_index + 1], input.row) and try self.constantProperties(.{ .node = node }, path.nodes[edge_index + 1].properties, input.row))
                    {
                        const entry = try endpoints.getOrPut(self.a, index);
                        entry.value_ptr.* = try self.waysAdd(if (entry.found_existing) entry.value_ptr.* else waysInit(W, 0), ways);
                    }
                }
                if (hop == upper) break;
                try self.advanceLower(&graph, domain, edge, &current, &next, if (hop == 0) edge_index else null);
                std.mem.swap(Counts, &current, &next);
            }
            std.mem.swap(Counts, &current, &endpoints);
        }
        if (self.component_leaves.len != 0) {
            var final = current.iterator();
            while (final.next()) |endpoint| endpoint.value_ptr.* = try self.waysMultiply(endpoint.value_ptr.*, try self.branchWays(W, &graph, endpoint.key_ptr.*, path.nodes.len - 1, null, mode));
        }
        // Saturated counts prove COUNT overflow but are not numeric weights.
        // Never approximate SUM/AVG with the overflow sentinel.
        var exact_weights = true;
        if (self.count_endpoints or self.weighted_sink != null) {
            // An intermediate DISTINCT replaces every surviving bag weight
            // with one before any aggregate consumes it.
            const distinct_barrier = if (self.stream_fold) |fold| for (fold.middle) |clause| {
                if (clause.operation == .project and clause.operation.project.value.distinct) break true;
            } else false else false;
            const needs_weights = if (self.weighted_sink) |sink| sink.multiplicity == .exact else if (self.match_weight_demand) |demand| demand == .exact else !distinct_barrier and for (self.stream_fold.?.calls, 0..) |expression, index| {
                if (self.stream_fold.?.active_calls) |active| if (!active[index]) continue;
                const aggregate_call = expression.value.call;
                if (!aggregate_call.distinct and (std.ascii.eqlIgnoreCase(aggregate_call.name, "sum") or std.ascii.eqlIgnoreCase(aggregate_call.name, "avg"))) break true;
            } else false;
            if (W == u64 and needs_weights) {
                var weights = current.valueIterator();
                while (weights.next()) |weight| if (weight.* == std.math.maxInt(u64)) {
                    exact_weights = false;
                    break;
                };
            }
        }
        const compressible = exact_weights and switch (mode) {
            .repeatable => true,
            .node_simple => try self.lowerAcyclic(&graph),
            .relationship_simple => try self.lowerTrailAcyclic(&graph),
        };
        if (!compressible) {
            if (self.component_proof) return error.CypherComponentDeclined;
            if (self.weighted_sink != null or starts.len != 1) return false;
            const previous = self.lower_graph;
            self.lower_graph = &graph;
            defer self.lower_graph = previous;
            var resumed = input;
            resumed.row = (try self.bind(input.row, path.nodes[0].alias, .{ .node = starts[0] })).?;
            try self.pathStep(resumed, path, mode, 0, &.{starts[0]}, &.{}, output);
            return true;
        }
        var counts = current.iterator();
        while (counts.next()) |endpoint| {
            const ways = endpoint.value_ptr.*;
            if (waysCap(ways) == 0) continue;
            if (self.shortest_count) |count_paths| {
                count_paths.* = try addPathCount(count_paths.*, waysCap(ways));
            } else {
                const node = graph.nodes.items[endpoint.key_ptr.*].node;
                const row = (try self.bind(input.row, path.nodes[path.nodes.len - 1].alias, .{ .node = node })) orelse continue;
                if (self.stream_predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) continue;
                if (self.weighted_sink) |sink| {
                    // SQL's cursor protocol keeps its signed-overflow cap;
                    // native windows retain the wider internal prefix counts.
                    try sink.emit(sink.ptr, row, switch (sink.multiplicity) {
                        .presence => 1,
                        .capped => @min(waysCap(ways), @as(u64, std.math.maxInt(i64)) + 1),
                        .exact => waysCap(ways),
                    });
                    continue;
                }
                const fold = self.stream_fold.?;
                const previous_weight = fold.weight;
                fold.weight = if (self.match_weight_demand == .presence) Count.one else try previous_weight.multiply(fold.retained, if (W == Count) ways else Count.init(ways));
                defer fold.weight = previous_weight;
                try fold.add(self, row);
            }
        }
        return true;
    }
    fn sameRelationship(left: *const v.Relationship, right: *const v.Relationship) bool {
        return left.identity.eql(right.identity) and std.mem.eql(u8, left.index, right.index);
    }
    fn multiplyPathWays(left: u64, right: u64) u64 {
        // Every legal finite window has SKIP+LIMIT < maxInt(u64).
        // Reserve maxInt(u64) as the overflow sentinel for exact consumers.
        return left *| right;
    }
    fn branchWays(self: *Engine, comptime W: type, graph: *LowerGraph, position: usize, pivot: usize, excluded: ?*const v.Relationship, mode: ast.PathMode) anyerror!W {
        var leaf_count: usize = 0;
        for (self.component_leaves) |leaf| leaf_count += @intFromBool(leaf.pivot == pivot);
        if (leaf_count == 0) return waysInit(W, 1);
        const key: BranchKey = .{ .node = position, .pivot = pivot };
        const cached = graph.branch_choices.get(key) orelse blk: {
            const sets = try self.a.alloc([]const *const v.Relationship, leaf_count);
            var slot: usize = 0;
            var eligibility: v.EntityMap(u16) = .empty;
            const node = graph.nodes.items[position].node;
            for (self.component_leaves) |leaf| {
                if (leaf.pivot != pivot) continue;
                var candidates: std.ArrayList(*const v.Relationship) = .empty;
                var seen: v.EntitySet = .empty;
                for (self.graph.relationships, 0..) |mapping, mapping_index| {
                    const selected = leaf.edge.types.len == 0 or for (leaf.edge.types) |name| {
                        if (std.mem.eql(u8, name, mapping.name)) break true;
                    } else false;
                    if (!selected or !try self.graph.admits(node, if (leaf.edge.direction == .out) mapping.source_label else mapping.target_label, &self.values)) continue;
                    var after: ?[]const u8 = null;
                    while (true) {
                        const page = try self.lowerPage(mapping_index, node.identity, leaf.edge.direction, after);
                        try self.prefetchEndpoints(page.items, node.identity, leaf.edge.direction == .out, leaf.edge.direction == .in);
                        for (page.items) |relationship| {
                            try self.state();
                            if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
                            const identity = if (leaf.edge.direction == .out and relationship.source.eql(node.identity)) relationship.target else if (leaf.edge.direction == .in and relationship.target.eql(node.identity)) relationship.source else continue;
                            const target = (try self.getNode(identity)) orelse continue;
                            if (!try self.graph.admits(target, if (leaf.edge.direction == .out) mapping.target_label else mapping.source_label, &self.values) or !try self.nodeMatches(target, leaf.node) or !boundNodeMatches(target, leaf.node, graph.input.row)) continue;
                            if (!try self.constantProperties(.{ .node = target }, leaf.node.properties, graph.input.row) or !try self.constantProperties(.{ .relationship = relationship }, leaf.edge.properties, graph.input.row)) continue;
                            const entry = try seen.getOrPut(self.a, .{ .relationship = .{ .identity = relationship.identity, .index = relationship.index } });
                            if (!entry.found_existing) try candidates.append(self.a, relationship);
                        }
                        const next = page.next orelse break;
                        if (after) |previous| if (std.mem.eql(u8, previous, next)) return error.CypherInvalidBackendCursor;
                        after = next;
                    }
                }
                sets[slot] = candidates.items;
                for (candidates.items) |candidate| {
                    const eligible = try eligibility.getOrPut(self.a, .{ .relationship = .{ .identity = candidate.identity, .index = candidate.index } });
                    if (!eligible.found_existing) eligible.value_ptr.* = 0;
                    eligible.value_ptr.* |= @as(u16, 1) << @intCast(slot);
                }
                slot += 1;
            }
            const result: BranchChoices = .{ .sets = sets, .eligibility = eligibility };
            try graph.branch_choices.put(self.a, key, result);
            break :blk result;
        };
        // Excluding two edges with the same leaf eligibility has the same
        // effect. Cache that equivalence class, not individual trunk histories.
        const excluded_mask: u16 = if (mode != .repeatable and excluded != null)
            cached.eligibility.get(.{ .relationship = .{ .identity = excluded.?.identity, .index = excluded.?.index } }) orelse 0
        else
            0;
        const factor_key: BranchFactorKey = .{ .branch = key, .excluded = excluded_mask };
        if (graph.branch_factors.get(factor_key)) |ways| return if (W == Count) ways else waysCap(ways);
        const ways = try self.branchAssignmentsTyped(W, cached.sets, excluded_mask, mode == .repeatable);
        try graph.branch_factors.put(self.a, factor_key, if (W == Count) ways else Count.init(ways));
        return ways;
    }
    fn branchAssignments(self: *Engine, choices: []const []const *const v.Relationship, excluded_mask: u16, repeatable: bool) anyerror!u64 {
        return self.branchAssignmentsTyped(u64, choices, excluded_mask, repeatable);
    }
    fn branchAssignmentsTyped(self: *Engine, comptime W: type, choices: []const []const *const v.Relationship, excluded_mask: u16, repeatable: bool) anyerror!W {
        if (repeatable) {
            var ways: W = waysInit(W, 1);
            for (choices) |set| ways = try self.waysMultiply(ways, waysInit(W, set.len));
            return ways;
        }
        // Each physical relationship can fill at most one leaf. Group edges
        // by eligibility, then count matchings over leaf subsets, never over
        // permutations of edge identities. Identical leaves have a direct
        // falling-factorial solution.
        const full: u16 = @intCast((@as(u32, 1) << @intCast(choices.len)) - 1);
        var edges: v.EntityMap(usize) = .empty;
        var masks: std.ArrayList(u16) = .empty;
        for (choices, 0..) |set, leaf| for (set) |edge| {
            try self.state();
            const entry = try edges.getOrPut(self.a, .{ .relationship = .{ .identity = edge.identity, .index = edge.index } });
            if (!entry.found_existing) {
                entry.value_ptr.* = masks.items.len;
                try masks.append(self.a, 0);
            }
            const position = entry.value_ptr.*;
            masks.items[position] |= @as(u16, 1) << @intCast(leaf);
        };
        const identical = for (masks.items) |mask| {
            if (mask != full) break false;
        } else true;
        if (identical) {
            var available = masks.items.len - @intFromBool(excluded_mask != 0);
            var ways: W = waysInit(W, 1);
            for (choices) |_| {
                if (available == 0) return waysInit(W, 0);
                ways = try self.waysMultiply(ways, waysInit(W, available));
                available -= 1;
            }
            return ways;
        }
        const counts = try self.a.alloc(W, @as(usize, full) + 1);
        @memset(counts, waysInit(W, 0));
        counts[0] = waysInit(W, 1);
        var removed = excluded_mask == 0;
        for (masks.items) |eligible| {
            try Engine.chargeWork(self, counts.len);
            if (!removed and eligible == excluded_mask) {
                removed = true;
                continue;
            }
            var cursor: usize = full;
            while (cursor != 0) {
                cursor -= 1;
                if (waysCap(counts[cursor]) == 0) continue;
                var available = eligible & ~@as(u16, @intCast(cursor));
                while (available != 0) {
                    try self.state();
                    const bit = available & (0 -% available);
                    available &= available - 1;
                    const next = cursor | bit;
                    counts[next] = try self.waysAdd(counts[next], counts[cursor]);
                }
            }
        }
        return counts[full];
    }
    fn advanceLower(self: *Engine, graph: *LowerGraph, domain: usize, edge: ast.Edge, current: anytype, next: @TypeOf(current), source: ?usize) !void {
        return self.advanceLowerTarget(graph, domain, edge, current, next, null, source);
    }
    fn advanceLowerTarget(self: *Engine, graph: *LowerGraph, domain: usize, edge: ast.Edge, current: anytype, next: @TypeOf(current), target: ?ast.Node, source: ?usize) !void {
        const W = @FieldType(@TypeOf(current.*).KV, "value");
        next.clearRetainingCapacity();
        var candidates = current.iterator();
        while (candidates.next()) |candidate| {
            const index = candidate.key_ptr.*;
            const ways = candidate.value_ptr.*;
            // Discover adjacency only for reachable product states.
            // In particular, a failed intermediate node predicate must
            // not open the arbitrarily large branch behind that node.
            const range = try self.loadLowerDomain(graph, index, domain);
            const node = graph.nodes.items[index].node;
            for (graph.nodes.items[index].arcs.items[range.first..range.last]) |*arc| {
                try self.state();
                const relationship = arc.relationship;
                const mapping = self.graph.relationships[arc.mapping];
                const selected = if (edge.types.len == 0) true else for (edge.types) |name| {
                    if (std.mem.eql(u8, name, mapping.name)) break true;
                } else false;
                if (!selected) continue;
                const outgoing = edge.direction != .in and relationship.source.eql(node.identity) and try self.graph.admits(node, mapping.source_label, &self.values);
                const incoming = edge.direction != .out and relationship.target.eql(node.identity) and try self.graph.admits(node, mapping.target_label, &self.values);
                if (!outgoing and !incoming) continue;
                if (!try self.constantProperties(.{ .relationship = relationship }, edge.properties, graph.input.row)) continue;
                arc.eligible = true;
                if (target) |pattern| {
                    const reached = graph.nodes.items[arc.target].node;
                    if (!try self.nodeMatches(reached, pattern) or !boundNodeMatches(reached, pattern, graph.input.row) or
                        !try self.constantProperties(.{ .node = reached }, pattern.properties, graph.input.row)) continue;
                }
                const entry = try next.getOrPut(self.a, arc.target);
                const weight = if (source) |pivot| try self.waysMultiply(ways, try self.branchWays(W, graph, index, pivot, relationship, graph.mode)) else ways;
                entry.value_ptr.* = try self.waysAdd(if (entry.found_existing) entry.value_ptr.* else waysInit(W, 0), weight);
            }
        }
    }
    const EndpointScope = struct {
        path: ast.Path,
        row: Row,
        include_start: bool = true,
        pub fn permits(self: @This(), name: []const u8) bool {
            for ([_]?[]const u8{ if (self.include_start) self.path.nodes[0].alias else null, self.path.nodes[self.path.nodes.len - 1].alias }) |alias|
                if (alias) |endpoint| if (std.mem.eql(u8, endpoint, name)) return true;
            return lookup(self.row, name) != null;
        }
    };
    fn endpointExpression(expression: *const ast.Expr, path: ast.Path, row: Row) bool {
        return @import("expressions.zig").rowLocal(expression, EndpointScope{ .path = path, .row = row });
    }
    /// Track available bindings through row-local stages without substituting
    /// expression trees or evaluating lazy branches. Unavailable path/interior
    /// bindings stay in wildcard shapes so DISTINCT cannot silently omit keys.
    const PipelineScope = struct {
        bindings: Row,
        pub fn permits(self: @This(), name: []const u8) bool {
            const value = lookup(self.bindings, name) orelse return false;
            return value == .boolean and value.boolean;
        }
    };
    fn pipelineLocal(self: *Engine, path: ast.Path, row: Row, middle: []const ast.Clause, projection: ast.Projection, include_start: bool) !bool {
        const rules = @import("expressions.zig");
        var shape: Row = &.{};
        for (row) |entry| shape = try self.replace(shape, entry.name, .{ .boolean = true });
        for (path.nodes, 0..) |node, i| if (node.alias) |alias| {
            if (lookup(shape, alias) == null) shape = try self.replace(shape, alias, .{ .boolean = (i == 0 and include_start) or i == path.nodes.len - 1 });
        };
        for (path.edges) |edge| if (edge.alias) |alias| {
            if (lookup(shape, alias) == null) shape = try self.replace(shape, alias, .{ .boolean = false });
        };
        if (path.alias) |alias| if (lookup(shape, alias) == null) {
            shape = try self.replace(shape, alias, .{ .boolean = false });
        };
        for (middle) |clause| switch (clause.operation) {
            .unwind => |step| {
                if (!rules.rowLocal(step.expression, PipelineScope{ .bindings = shape })) return false;
                shape = try self.replace(shape, step.alias, .{ .boolean = true });
            },
            .project => |step| {
                const p = step.value;
                if (p.order.len != 0 or p.skip != null or p.limit != null) return false;
                var next: Row = if (p.wildcard) shape else &.{};
                for (p.items) |item| {
                    if (aggregates(item.expression) or !rules.rowLocal(item.expression, PipelineScope{ .bindings = shape })) return false;
                    next = try self.replace(next, item.alias orelse try self.expressionName(item.expression), .{ .boolean = true });
                }
                if (p.distinct) for (next) |entry| if (!entry.value.boolean) return false;
                var predicate_shape = shape;
                for (next) |entry| predicate_shape = try self.replace(predicate_shape, entry.name, entry.value);
                if (p.predicate) |predicate| if (!rules.rowLocal(predicate, PipelineScope{ .bindings = predicate_shape })) return false;
                shape = next;
            },
            else => return false,
        };
        for (projection.items) |item| if (!rules.rowLocal(item.expression, PipelineScope{ .bindings = shape })) return false;
        for (projection.items) |item| if (item.alias) |alias| {
            shape = try self.replace(shape, alias, .{ .boolean = true });
        };
        for (projection.order) |order| if (!rules.rowLocal(order.expression, PipelineScope{ .bindings = shape })) return false;
        if (projection.predicate) |predicate| if (!rules.rowLocal(predicate, PipelineScope{ .bindings = shape })) return false;
        return true;
    }
    fn sameTypeSet(left: []const []const u8, right: []const []const u8) bool {
        for (left) |name| {
            const found = for (right) |other| {
                if (std.mem.eql(u8, name, other)) break true;
            } else false;
            if (!found) return false;
        }
        for (right) |name| {
            const found = for (left) |other| {
                if (std.mem.eql(u8, name, other)) break true;
            } else false;
            if (!found) return false;
        }
        return true;
    }
    fn boundNodeMatches(node: *const v.Node, pattern: ast.Node, row: Row) bool {
        const bound = lookup(row, pattern.alias orelse return true) orelse return true;
        return bound == .node and bound.node.identity.eql(node.identity);
    }
    fn mergePathWays(left: u64, right: u64) u64 {
        return left +| right;
    }
    fn addPathCount(left: u64, right: u64) !u64 {
        const sum = std.math.add(u64, left, right) catch return error.CypherNumericOverflow;
        if (sum > std.math.maxInt(i64)) return error.CypherNumericOverflow;
        return sum;
    }
    fn constantProperties(self: *Engine, entity: v.Value, properties: []const ast.Entry, row: Row) !bool {
        for (properties) |entry| if ((try self.values.equal(entity.property(entry.name), self.constant(entry.value, row).?)) != true) return false;
        return true;
    }
    // Batches bound retention and preserve fallback: a declined uniqueness or
    // exact-weight proof resumes the original per-anchor operator, before any
    // compressed row was emitted. Roots are owned independently of scan pages.
    const AnchorBatch = struct {
        path: ast.Path,
        input: MatchState,
        mode: ast.PathMode,
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        nodes: std.ArrayList(*const v.Node) = .empty,
        disabled: bool = false,
        admission: ?*const ast.Expr = null,
        residual: ?*const ast.Expr = null,
        fn flush(self: *@This(), engine: *Engine, output: *std.ArrayList(MatchState)) anyerror!void {
            if (self.nodes.items.len == 0) {
                if (self.arena) |*arena| arena.deinit();
                self.arena = null;
                return;
            }
            const previous = engine.anchor_batch;
            engine.anchor_batch = null;
            defer engine.anchor_batch = previous;
            const original_predicate = engine.stream_predicate;
            engine.stream_predicate = self.residual;
            const compressed = engine.countAllPaths(self.input, self.path, self.mode, self.nodes.items, output) catch |err| {
                engine.stream_predicate = original_predicate;
                return err;
            };
            engine.stream_predicate = original_predicate;
            if (!compressed) {
                if (engine.component_proof) return error.CypherComponentDeclined;
                for (self.nodes.items) |node| {
                    const row = (try engine.bind(self.input.row, self.path.nodes[0].alias, .{ .node = node })).?;
                    try engine.startPath(.{ .row = row }, self.path, self.mode, node, output);
                }
            }
            self.arena.?.deinit();
            self.arena = null;
            self.nodes = .empty;
        }
        fn append(self: *@This(), node: *const v.Node) !void {
            if (self.arena == null) self.arena = .init(self.budget.allocator());
            const a = self.arena.?.allocator();
            const owned = (try (v.Value{ .node = node }).clone(a)).node;
            try self.nodes.append(a, owned);
        }
    };
    fn reductionPath(self: *Engine, path: ast.Path, row: Row, operation: ast.Match, projection: ast.Projection, middle: []const ast.Clause) !ast.Path {
        if (self.component_leaves.len != 0) return path;
        if (path.selector != .all or path.edges.len == 0 or path.execution_prefix or try self.batchEligible(path, row, operation, projection, middle)) return path;
        // Without cardinality estimates, equal filter scores do not prove a
        // different table or label population costs the same to scan. Only
        // reorient equivalent anchor admissions; selective point/interior
        // anchors still take precedence through batchEligible below.
        const first = path.nodes[0];
        const last = path.nodes[path.nodes.len - 1];
        if (!sameTypeSet(first.labels, last.labels) or first.properties.len != last.properties.len) return path;
        for ([_][]const ast.Entry{ first.properties, last.properties }, [_][]const ast.Entry{ last.properties, first.properties }) |properties, other| {
            for (properties) |entry| {
                const matched = for (other) |candidate| {
                    if (std.mem.eql(u8, entry.name, candidate.name) and @import("expressions.zig").same(entry.value, candidate.value)) break true;
                } else false;
                if (!matched) return path;
            }
        }
        const nodes = try self.a.dupe(ast.Node, path.nodes);
        const edges = try self.a.dupe(ast.Edge, path.edges);
        std.mem.reverse(ast.Node, nodes);
        std.mem.reverse(ast.Edge, edges);
        for (edges) |*edge| edge.direction = switch (edge.direction) {
            .out => .in,
            .in => .out,
            .both => .both,
        };
        var reversed = path;
        reversed.nodes = nodes;
        reversed.edges = edges;
        reversed.execution_reversed = !path.execution_reversed;
        return if (try self.batchEligible(reversed, row, operation, projection, middle)) reversed else path;
    }
    fn batchAnchorScore(self: *Engine, node: ast.Node, predicate: ?*const ast.Expr, row: Row) !usize {
        if (node.alias) |alias| if (lookup(row, alias) != null) return 1_000_000;
        if (self.anchorKey(predicate, node.alias, row) != null) return 100_000;
        var filters: std.ArrayList(backend_mod.PropertyFilter) = .empty;
        try self.labelFilters(node, &filters);
        for (node.properties) |property| if (self.constant(property.value, row)) |value| try filters.append(self.a, .{ .property = property.name, .value = value });
        if (predicate) |expression| if (node.alias) |alias| try self.predicateFilters(expression, alias, row, &filters);
        var seeks: usize = 0;
        for (filters.items) |filter| if (filter.op == .eq or filter.op == .in) {
            seeks += 1;
        };
        var score = (if (self.backend.vtable.scan_where != null) @as(usize, 100) * @min(seeks, 999) else 0) + @intFromBool(node.labels.len > 0);
        // Bounded index cardinality evidence distinguishes a selective range
        // from a broad residual without overriding point/bound admissions.
        if (self.read_only and seeks == 0 and filters.items.len != 0) {
            const estimate = self.backend.vtable.estimate_where orelse return score;
            const key = try std.fmt.allocPrint(self.a, "{s}:{s}", .{ self.graph.name, try std.json.Stringify.valueAlloc(self.a, .{ .table = if (node.labels.len != 0) self.graph.label(node.labels[0]).?.table else "", .filters = filters.items }, .{}) });
            const upper = if (self.anchor_estimates.get(key)) |cached| cached else blk: {
                if (key.len > 4096 or self.anchor_estimates.count() >= 64) return score;
                var total: usize = 0;
                for (self.graph.labels, 0..) |label, index| {
                    if (!self.firstTableLabel(index, node)) continue;
                    if (total == 32) {
                        try self.anchor_estimates.put(self.planning_allocator, try self.planning_allocator.dupe(u8, key), null);
                        return score;
                    }
                    try self.state();
                    const bound = (try estimate(self.backend.ptr, self.a, label.table, filters.items, 32 -| total)) orelse {
                        try self.anchor_estimates.put(self.planning_allocator, try self.planning_allocator.dupe(u8, key), null);
                        return score;
                    };
                    total += bound;
                    if (total > 32) return error.CypherInvalidBackendPage;
                }
                try self.anchor_estimates.put(self.planning_allocator, try self.planning_allocator.dupe(u8, key), total);
                break :blk total;
            };
            if (upper) |bound| score = @max(score, 50 - bound);
        }
        return score;
    }
    fn batchEligible(self: *Engine, path: ast.Path, row: Row, operation: ast.Match, projection: ast.Projection, middle: []const ast.Clause) !bool {
        if (path.selector != .all or path.edges.len == 0 or path.execution_prefix) return false;
        if (path.nodes[0].alias) |alias| if (lookup(row, alias) != null) return false;
        for (path.nodes) |node| for (node.properties) |entry| if (self.constant(entry.value, row) == null) return false;
        for (path.edges) |edge| for (edge.properties) |entry| if (self.constant(entry.value, row) == null) return false;
        const score = try self.batchAnchorScore(path.nodes[0], operation.predicate, row);
        for (path.nodes[1..]) |node| if (try self.batchAnchorScore(node, operation.predicate, row) > score) return false;
        const independent: EndpointScope = .{ .path = path, .row = row, .include_start = false };
        const predicates = try self.batchPredicates(path, row, operation.predicate);
        if (predicates.residual) |predicate| if (!@import("expressions.zig").rowLocal(predicate, independent)) return false;
        return self.pipelineLocal(path, row, middle, projection, false);
    }
    fn startPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, node: *const v.Node, output: *std.ArrayList(MatchState)) anyerror!void {
        if (self.anchor_batch) |batch| {
            if (!batch.disabled and path.nodes.ptr == batch.path.nodes.ptr and input.used.len == 0 and input.constraints.len == 0) {
                if (batch.admission) |predicate| if ((try (try self.eval(predicate, input.row, null)).truth()) != true) return;
                batch.append(node) catch |err| {
                    if (err != error.OutOfMemory or !batch.budget.exhausted) return err;
                    batch.disabled = true;
                    try batch.flush(self, output);
                    return self.startPath(input, path, mode, node, output);
                };
                if (batch.nodes.items.len == 128) try batch.flush(self, output);
                return;
            }
        }
        if ((self.shortest_count != null or self.count_endpoints) and path.selector == .all and try self.countAllPath(input, path, mode, node, output)) return;
        if (self.component_proof) return error.CypherComponentDeclined;
        if (!self.shortest_fast or path.selector == .all or path.edges.len != 1 or input.used.len != 0 or input.constraints.len != 0) {
            return self.pathStep(input, path, mode, 0, &.{node}, &.{}, output);
        }
        // Rebound relationship/path variables constrain the complete route,
        // rather than only its endpoint distance.
        if (path.alias) |alias| if (lookup(input.row, alias) != null)
            return self.pathStep(input, path, mode, 0, &.{node}, &.{}, output);
        if (path.edges[0].alias) |alias| if (lookup(input.row, alias) != null)
            return self.pathStep(input, path, mode, 0, &.{node}, &.{}, output);
        // Forward references, path-dependent predicates and multi-pattern MATCH
        // keep the exhaustive selector. Endpoint-local patterns admit a native
        // breadth-first operator, with one frontier distance per identity.
        for (path.edges[0].properties) |property| if (self.constant(property.value, input.row) == null)
            return self.pathStep(input, path, mode, 0, &.{node}, &.{}, output);
        for (path.nodes[1].properties) |property| if (self.constant(property.value, input.row) == null)
            return self.pathStep(input, path, mode, 0, &.{node}, &.{}, output);
        if (path.edges[0].min_hops > 1) return self.lowerPath(input, path, mode, node, output);
        try self.breadthPath(input, path, mode, node, output);
    }
    // A statement-owned adjacency capture shares physical reads between route
    // prefixes. DAGs and repeatable walks admit distance states indexed by the
    // unsatisfied lower bound; cyclic trails retain exact route histories.
    const LowerArc = struct { target: usize, relationship: *const v.Relationship, mapping: usize, eligible: bool = false };
    const LowerRange = struct { first: usize, last: usize };
    const LowerNode = struct { node: *const v.Node, depth: usize, arcs: std.ArrayList(LowerArc) = .empty, loaded: bool = false, domains: std.AutoHashMapUnmanaged(usize, LowerRange) = .empty };
    const BranchKey = struct { node: usize, pivot: usize };
    const BranchFactorKey = struct { branch: BranchKey, excluded: u16 };
    const BranchChoices = struct { sets: []const []const *const v.Relationship, eligibility: v.EntityMap(u16) };
    const LowerGraph = struct {
        nodes: std.ArrayList(LowerNode) = .empty,
        positions: std.HashMapUnmanaged(v.Identity, usize, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
        input: MatchState,
        path: ast.Path,
        domains: []const ast.Edge = &.{},
        qualified: bool = false,
        mode: ast.PathMode = .relationship_simple,
        branch_factors: std.AutoHashMapUnmanaged(BranchFactorKey, Count) = .empty,
        branch_choices: std.AutoHashMapUnmanaged(BranchKey, BranchChoices) = .empty,
        probing: bool = false,
        probe_matches: bool = false,
        first_only: bool = false,
    };
    fn accountEdges(self: *Engine, page: backend_mod.Page(*const v.Relationship)) !void {
        const examined = page.examined orelse page.items.len;
        if (examined < page.items.len) return error.CypherInvalidBackendPage;
        if (examined > self.limits.explored_edges -| self.stats.explored_edges) return error.CypherEdgeBudgetExceeded;
        self.stats.explored_edges += examined;
    }
    fn arcBound(arcs: []const LowerArc, mapping: usize, upper: bool) usize {
        var low: usize = 0;
        var high = arcs.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (arcs[middle].mapping < mapping or (upper and arcs[middle].mapping == mapping)) low = middle + 1 else high = middle;
        }
        return low;
    }
    fn expandPatternEdges(self: *Engine, mapping_index: usize, mapping: catalog.Relationship, identity: v.Identity, target: ast.Node, edge: ast.Edge, hops: usize, row: Row, cursor: ?[]const u8, demand: usize) !backend_mod.Page(*const v.Relationship) {
        if (self.lower_graph == null and hops == 0 and edge.min_hops == 1 and edge.max_hops == 1) {
            if (self.backend.vtable.expand_between) |between| if (target.alias) |alias| if (lookup(row, alias)) |bound| {
                if (bound != .node) return .{ .items = &.{} };
                var filters: std.ArrayList(backend_mod.PropertyFilter) = .empty;
                for (edge.properties) |property| if (self.constant(property.value, row)) |value| try filters.append(self.a, .{ .property = property.name, .value = value });
                const page = try between(self.backend.ptr, self.a, mapping, identity, bound.node.identity, edge.direction, filters.items, cursor, demand, self.limits.explored_edges -| self.stats.explored_edges);
                try self.accountEdges(page);
                return page;
            };
        }
        return self.expandEdges(mapping_index, mapping, identity, edge.direction, cursor, demand);
    }
    fn expandEdges(self: *Engine, mapping_index: usize, mapping: catalog.Relationship, identity: v.Identity, direction: ast.Direction, cursor: ?[]const u8, demand: usize) !backend_mod.Page(*const v.Relationship) {
        if (self.lower_graph) |graph| if (graph.positions.get(identity)) |position| {
            const range: LowerRange = if (graph.domains.len != 0) blk: {
                for (graph.domains, 0..) |domain, index| {
                    if (domain.direction != direction) continue;
                    const selected = if (domain.types.len == 0) true else for (domain.types) |name| {
                        if (std.mem.eql(u8, name, mapping.name)) break true;
                    } else false;
                    if (selected) if (graph.nodes.items[position].domains.get(index)) |range| break :blk range;
                }
                // Replaying a different overlapping type set may prefer a
                // domain this node never loaded. Reuse any captured matching
                // range; a genuine miss stays page-owned and uncached, rather
                // than retaining page allocations in the statement capture.
                const page = try self.backend.vtable.expand(self.backend.ptr, self.a, mapping, identity, direction, cursor, demand);
                try self.accountEdges(page);
                return page;
            } else blk: {
                if (!graph.nodes.items[position].loaded) try self.loadLowerNode(graph, position);
                break :blk .{ .first = 0, .last = graph.nodes.items[position].arcs.items.len };
            };
            const node = graph.nodes.items[position];
            {
                // Capture appends mappings in catalog order; reachability
                // pruning preserves that order. Seek the matching range rather
                // than scanning every mapping's arcs for each replay.
                if (cursor != null) return error.CypherInvalidBackendCursor;
                const arcs = node.arcs.items[range.first..range.last];
                const first = arcBound(arcs, mapping_index, false);
                const last = arcBound(arcs, mapping_index, true);
                // Physical reads were already charged, but replay still does
                // route work. Keep cyclic enumeration and product-state walks
                // within the statement's work/cancellation budget.
                try chargeWork(self, 1 + last - first);
                const items = try self.a.alloc(*const v.Relationship, last - first);
                for (arcs[first..last], items) |arc, *item| item.* = arc.relationship;
                return .{ .items = items };
            }
        };
        const page = try self.backend.vtable.expand(self.backend.ptr, self.a, mapping, identity, direction, cursor, demand);
        try self.accountEdges(page);
        return page;
    }
    fn loadLowerNode(self: *Engine, graph: *LowerGraph, position: usize) !void {
        graph.nodes.items[position].loaded = true;
        return self.loadLowerAdjacency(graph, position, graph.path.edges[0]);
    }
    fn loadLowerDomain(self: *Engine, graph: *LowerGraph, position: usize, domain: usize) !LowerRange {
        if (graph.nodes.items[position].domains.get(domain)) |range| return range;
        const first = graph.nodes.items[position].arcs.items.len;
        try self.loadLowerAdjacency(graph, position, graph.domains[domain]);
        const range: LowerRange = .{ .first = first, .last = graph.nodes.items[position].arcs.items.len };
        try graph.nodes.items[position].domains.put(self.a, domain, range);
        return range;
    }
    const AdjacencyCache = struct {
        const Page = backend_mod.Page(*const v.Relationship);
        const Key = struct { mapping: usize, node: v.Identity, direction: ast.Direction, cursor: ?[]const u8 };
        const Context = struct {
            pub fn hash(_: @This(), key: Key) u64 {
                var hasher = std.hash.Wyhash.init((v.Identity.Context{}).hash(key.node));
                std.hash.autoHash(&hasher, key.mapping);
                std.hash.autoHash(&hasher, key.direction);
                std.hash.autoHash(&hasher, key.cursor != null);
                if (key.cursor) |cursor| hasher.update(cursor);
                return hasher.final();
            }
            pub fn eql(_: @This(), left: Key, right: Key) bool {
                return left.mapping == right.mapping and left.direction == right.direction and left.node.eql(right.node) and
                    (if (left.cursor) |cursor| right.cursor != null and std.mem.eql(u8, cursor, right.cursor.?) else right.cursor == null);
            }
        };
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        pages: std.HashMapUnmanaged(Key, Page, Context, std.hash_map.default_max_load_percentage) = .empty,
        disabled: bool = false,
        fn put(self: *@This(), key: Key, page: Page) !void {
            if (self.arena == null) self.arena = .init(self.budget.allocator());
            const a = self.arena.?.allocator();
            const items = try a.alloc(*const v.Relationship, page.items.len);
            for (page.items, items) |relationship, *out| out.* = (try (v.Value{ .relationship = relationship }).clone(a)).relationship;
            const owned: Key = .{ .mapping = key.mapping, .direction = key.direction, .node = .{ .table = try a.dupe(u8, key.node.table), .key = try a.dupe(u8, key.node.key) }, .cursor = if (key.cursor) |cursor| try a.dupe(u8, cursor) else null };
            try self.pages.put(a, owned, .{ .items = items, .next = if (page.next) |next| try a.dupe(u8, next) else null });
        }
        fn deinit(self: *@This()) void {
            if (self.arena) |*arena| arena.deinit();
        }
    };
    fn lowerPage(self: *Engine, mapping: usize, node: v.Identity, direction: ast.Direction, cursor: ?[]const u8) !backend_mod.Page(*const v.Relationship) {
        try self.checkpoint();
        const key: AdjacencyCache.Key = .{ .mapping = mapping, .node = node, .direction = direction, .cursor = cursor };
        if (self.adjacency_cache) |cache| if (cache.pages.get(key)) |page| return page;
        const page = try self.backend.vtable.expand(self.backend.ptr, self.a, self.graph.relationships[mapping], node, direction, cursor, 128);
        try self.accountEdges(page);
        if (self.adjacency_cache) |cache| if (!cache.disabled) {
            cache.put(key, page) catch |err| {
                if (err != error.OutOfMemory or !cache.budget.exhausted) return err;
                // Existing captures may borrow cached relationships. Keep their
                // storage alive and stop admission rather than evicting it.
                cache.disabled = true;
            };
        };
        return page;
    }
    fn loadLowerAdjacency(self: *Engine, graph: *LowerGraph, position: usize, edge: ast.Edge) !void {
        const current = graph.nodes.items[position];
        for (self.graph.relationships, 0..) |mapping, mapping_index| {
            var selected = edge.types.len == 0;
            for (edge.types) |name| if (std.mem.eql(u8, name, mapping.name)) {
                selected = true;
            };
            if (!selected) continue;
            const outgoing = edge.direction != .in and try self.graph.admits(current.node, mapping.source_label, &self.values);
            const incoming = edge.direction != .out and try self.graph.admits(current.node, mapping.target_label, &self.values);
            if (!outgoing and !incoming) continue;
            var cursor: ?[]const u8 = null;
            while (true) {
                const page = try self.lowerPage(mapping_index, current.node.identity, edge.direction, cursor);
                try self.prefetchEndpoints(page.items, current.node.identity, outgoing, incoming);
                for (page.items) |relationship| {
                    try self.state();
                    if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
                    const traversing_out = outgoing and relationship.source.eql(current.node.identity);
                    const target = if (traversing_out) relationship.target else if (incoming and relationship.target.eql(current.node.identity)) relationship.source else continue;
                    const reached = (try self.getNode(target)) orelse continue;
                    if (!try self.graph.admits(reached, if (traversing_out) mapping.target_label else mapping.source_label, &self.values)) continue;
                    const eligible = (try self.constrain(graph.input, .{ .relationship = relationship }, edge.properties)) orelse continue;
                    if (!try self.constraintsMatch(eligible.constraints, graph.input.row, false)) continue;
                    const entry = try graph.positions.getOrPut(self.a, reached.identity);
                    if (!entry.found_existing) {
                        entry.value_ptr.* = graph.nodes.items.len;
                        try graph.nodes.append(self.a, .{ .node = reached, .depth = current.depth + 1 });
                    }
                    try graph.nodes.items[position].arcs.append(self.a, .{ .target = entry.value_ptr.*, .relationship = relationship, .mapping = mapping_index });
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                cursor = page.next;
            }
        }
    }
    fn lowerDomainDepth(self: *Engine, graph: *const LowerGraph, domain: usize, edge: ast.Edge, starts: anytype) !?struct { maximum: usize, order: []const usize } {
        const degree = try self.a.alloc(usize, graph.nodes.items.len);
        const depth = try self.a.alloc(?usize, graph.nodes.items.len);
        @memset(degree, 0);
        @memset(depth, null);
        var initial = starts.keyIterator();
        while (initial.next()) |index| depth[index.*] = 0;
        for (graph.nodes.items) |node| if (node.domains.get(domain)) |range| {
            for (node.arcs.items[range.first..range.last]) |arc| {
                try self.state();
                if (try self.constantProperties(.{ .relationship = arc.relationship }, edge.properties, graph.input.row)) degree[arc.target] += 1;
            }
        };
        var queue: std.ArrayList(usize) = .empty;
        for (degree, 0..) |degree_value, index| if (degree_value == 0) {
            try queue.append(self.a, index);
        };
        var head: usize = 0;
        var maximum: usize = 0;
        while (head < queue.items.len) : (head += 1) {
            try self.state();
            const index = queue.items[head];
            const node = graph.nodes.items[index];
            if (depth[index]) |distance| maximum = @max(maximum, distance);
            if (node.domains.get(domain)) |range| for (node.arcs.items[range.first..range.last]) |arc| {
                if (!try self.constantProperties(.{ .relationship = arc.relationship }, edge.properties, graph.input.row)) continue;
                if (depth[index]) |distance| depth[arc.target] = @max(depth[arc.target] orelse 0, distance + 1);
                degree[arc.target] -= 1;
                if (degree[arc.target] == 0) try queue.append(self.a, arc.target);
            };
        }
        return if (queue.items.len == graph.nodes.items.len) .{ .maximum = maximum, .order = queue.items } else null;
    }
    fn lowerAcyclic(self: *Engine, graph: *const LowerGraph) !bool {
        return self.lowerAcyclicMappings(graph, null);
    }
    // A relationship can be reused only inside domains selecting the same
    // physical mapping. Prove acyclicity per connected component of overlapping
    // mapping sets. Disjoint components can revisit nodes, but cannot reuse an
    // edge; node-simple paths deliberately retain the whole-graph proof.
    fn lowerTrailAcyclic(self: *Engine, graph: *const LowerGraph) !bool {
        var components: [64]u64 = @splat(0);
        for (graph.domains) |domain| {
            var selected: u64 = 0;
            for (self.graph.relationships, 0..) |mapping, index| {
                try self.state();
                if (domain.types.len == 0 or for (domain.types) |name| {
                    if (std.mem.eql(u8, name, mapping.name)) break true;
                } else false) selected |= @as(u64, 1) << @intCast(index);
            }
            var joined = selected;
            for (components, 0..) |component, index| {
                if (selected & (@as(u64, 1) << @intCast(index)) != 0) joined |= component;
            }
            for (&components, 0..) |*component, index| {
                if (joined & (@as(u64, 1) << @intCast(index)) != 0) component.* = joined;
            }
        }
        for (components, 0..) |component, index| {
            if (component != 0 and @ctz(component) == index)
                if (!try self.lowerAcyclicMappings(graph, component)) return false;
        }
        return true;
    }
    fn lowerAcyclicMappings(self: *Engine, graph: *const LowerGraph, mappings: ?u64) !bool {
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const indegree = try a.alloc(usize, graph.nodes.items.len);
        @memset(indegree, 0);
        for (graph.nodes.items) |node| {
            try chargeWork(self, 1 + node.arcs.items.len);
            for (node.arcs.items) |arc| {
                if (mappings) |mask| if (mask & (@as(u64, 1) << @intCast(arc.mapping)) == 0) continue;
                if (!graph.qualified or arc.eligible) indegree[arc.target] += 1;
            }
        }
        var queue: std.ArrayList(usize) = .empty;
        for (indegree, 0..) |degree, index| if (degree == 0) {
            try queue.append(a, index);
        };
        var head: usize = 0;
        while (head < queue.items.len) : (head += 1) {
            try self.state();
            for (graph.nodes.items[queue.items[head]].arcs.items) |arc| {
                if (graph.qualified and !arc.eligible) continue;
                if (mappings) |mask| if (mask & (@as(u64, 1) << @intCast(arc.mapping)) == 0) continue;
                indegree[arc.target] -= 1;
                if (indegree[arc.target] == 0) try queue.append(a, arc.target);
            }
        }
        return queue.items.len == graph.nodes.items.len;
    }
    fn lowerPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, node: *const v.Node, output: *std.ArrayList(MatchState)) !void {
        var graph: LowerGraph = .{ .input = input, .path = path, .probing = true };
        try graph.positions.put(self.a, node.identity, 0);
        try graph.nodes.append(self.a, .{ .node = node, .depth = 0 });
        const previous = self.lower_graph;
        self.lower_graph = &graph;
        defer self.lower_graph = previous;
        if (mode == .repeatable) {
            graph.probing = false;
            return self.breadthPath(input, path, mode, node, output);
        }
        if (self.stream_fold) |fold| if (fold.top.*) |top| {
            graph.first_only = top.capacity == 1 and fold.middle.len == 0 and fold.projection.order.len == 0 and fold.projection.predicate == null and self.stream_predicate == null;
        };
        // Probe distance states without emitting or counting speculative walks.
        // Physical adjacency is fetched only as the frontier demands it. Once
        // the winning layer is known, a DAG proof makes state compression safe;
        // cyclic trails instead retain their exact route histories.
        // A repeatable walk is a superset of every uniqueness mode. Its
        // absence proves there is no eligible trail, even in a cyclic graph.
        try self.breadthPath(input, path, .repeatable, node, output);
        graph.probing = false;
        if (!graph.probe_matches) return;
        if (try self.lowerAcyclic(&graph)) return self.breadthPath(input, path, mode, node, output);
        try self.exactLowerPath(input, path, mode, node, output);
    }
    fn exactLowerPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, start: *const v.Node, output: *std.ArrayList(MatchState)) !void {
        const edge = path.edges[0];
        const bound_target: ?v.Identity = if (path.nodes[1].alias) |alias| blk: {
            const bound = lookup(input.row, alias) orelse break :blk null;
            break :blk if (bound == .node) bound.node.identity else null;
        } else null;
        var target_depth: ?usize = null;
        var counted: std.HashMapUnmanaged(v.Identity, usize, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty;
        defer counted.deinit(self.a);
        var queue: std.ArrayList(BreadthState) = .empty;
        try queue.append(self.a, .{ .node = start });
        var head: usize = 0;
        while (head < queue.items.len) : (head += 1) {
            try self.state();
            const current = queue.items[head];
            if (target_depth) |depth| if (current.depth > depth) break;
            if (current.depth >= edge.min_hops and try self.nodeMatches(current.node, path.nodes[1])) {
                if (try self.bind(input.row, path.nodes[1].alias, .{ .node = current.node })) |row| {
                    var candidate = input;
                    candidate.row = row;
                    if (try self.constrain(candidate, .{ .node = current.node }, path.nodes[1].properties)) |constrained| {
                        const entry = try counted.getOrPut(self.a, current.node.identity);
                        const selected = !entry.found_existing or (entry.value_ptr.* == current.depth and path.selector == .all_shortest);
                        if (!entry.found_existing) entry.value_ptr.* = current.depth;
                        if (selected) {
                            if (self.shortest_count) |count_paths| {
                                count_paths.* = std.math.add(u64, count_paths.*, 1) catch return error.CypherNumericOverflow;
                                if (count_paths.* > std.math.maxInt(i64)) return error.CypherNumericOverflow;
                            } else {
                                var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
                                defer scratch.deinit();
                                const scope = if (self.stream_fold != null or self.shortest_count != null) EvalScope.enter(self, scratch.allocator()) else null;
                                defer if (scope) |saved| saved.leave(self);
                                const nodes = try self.a.alloc(*const v.Node, current.depth + 1);
                                const relationships = try self.a.alloc(*const v.Relationship, current.depth);
                                try self.emitBreadthPaths(queue.items, head, constrained, path, mode, nodes, relationships, output);
                            }
                            if (bound_target) |goal| if (goal.eql(current.node.identity)) {
                                target_depth = current.depth;
                            };
                        }
                    }
                }
            }
            if (target_depth) |depth| if (current.depth >= depth) continue;
            if (current.depth >= (edge.max_hops orelse std.math.maxInt(u32))) continue;
            for (self.graph.relationships, 0..) |mapping, mapping_index| {
                var selected = edge.types.len == 0;
                for (edge.types) |name| if (std.mem.eql(u8, name, mapping.name)) {
                    selected = true;
                };
                if (!selected) continue;
                const outgoing = edge.direction != .in and try self.graph.admits(current.node, mapping.source_label, &self.values);
                const incoming = edge.direction != .out and try self.graph.admits(current.node, mapping.target_label, &self.values);
                if (!outgoing and !incoming) continue;
                const page = try self.expandEdges(mapping_index, mapping, current.node.identity, edge.direction, null, 128);
                for (page.items) |relationship| {
                    try self.state();
                    const traversing_out = outgoing and relationship.source.eql(current.node.identity);
                    const target = if (traversing_out) relationship.target else if (incoming and relationship.target.eql(current.node.identity)) relationship.source else continue;
                    var ancestor: ?usize = head;
                    var reused = false;
                    while (ancestor) |index| {
                        try chargeWork(self, 1);
                        const prior = queue.items[index];
                        if (mode == .node_simple and prior.node.identity.eql(target)) reused = true;
                        if (prior.relationship) |previous_edge| if (v.Value.eql(.{ .relationship = previous_edge }, .{ .relationship = relationship })) {
                            reused = true;
                        };
                        if (reused) break;
                        ancestor = prior.parent;
                    }
                    if (reused) continue;
                    if (current.depth >= self.limits.path_depth) return error.CypherPathBudgetExceeded;
                    const reached = (try self.getNode(target)) orelse continue;
                    if (bound_target) |goal| if (target_depth == null and current.depth + 1 >= edge.min_hops and goal.eql(target)) {
                        target_depth = current.depth + 1;
                    };
                    try queue.append(self.a, .{ .node = reached, .parent = head, .relationship = relationship, .depth = current.depth + 1 });
                }
            }
        }
    }
    const BreadthKey = struct {
        identity: v.Identity,
        branch: usize,
        phase: usize = 0,
        const Context = struct {
            pub fn hash(_: @This(), key: BreadthKey) u64 {
                return std.hash.Wyhash.hash(std.hash.Wyhash.hash((v.Identity.Context{}).hash(key.identity), std.mem.asBytes(&key.branch)), std.mem.asBytes(&key.phase));
            }
            pub fn eql(_: @This(), left: BreadthKey, right: BreadthKey) bool {
                return left.phase == right.phase and left.branch == right.branch and left.identity.eql(right.identity);
            }
        };
    };
    fn breadthPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, start: *const v.Node, output: *std.ArrayList(MatchState)) anyerror!void {
        const edge = path.edges[0];
        const bound_target: ?v.Identity = if (path.nodes[1].alias) |alias| blk: {
            const bound = lookup(input.row, alias) orelse break :blk null;
            break :blk if (bound == .node) bound.node.identity else null;
        } else null;
        var target_depth: ?usize = if (bound_target) |identity| if (edge.min_hops == 0 and identity.eql(start.identity)) 0 else null else null;
        var queue: std.ArrayList(BreadthState) = .empty;
        var counted: std.HashMapUnmanaged(v.Identity, usize, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty;
        defer counted.deinit(self.a);
        var distances: std.HashMapUnmanaged(BreadthKey, usize, BreadthKey.Context, std.hash_map.default_max_load_percentage) = .empty;
        try queue.append(self.a, .{ .node = start });
        // A positive-length shortest cycle is distinct from the empty path.
        if (edge.min_hops == 0 or mode == .node_simple) try distances.put(self.a, .{ .identity = start.identity, .branch = 0 }, 0);
        var head: usize = 0;
        while (head < queue.items.len) : (head += 1) {
            try self.state();
            const current = queue.items[head];
            if (target_depth) |depth| if (current.depth > depth) break;
            if (current.depth >= edge.min_hops and try self.nodeMatches(current.node, path.nodes[1])) {
                var candidate = input;
                if (try self.bind(input.row, path.nodes[1].alias, .{ .node = current.node })) |row| {
                    candidate.row = row;
                    if (try self.constrain(candidate, .{ .node = current.node }, path.nodes[1].properties)) |constrained| {
                        var selected = true;
                        if (self.shortest_count != null or self.stream_fold != null) {
                            const entry = try counted.getOrPut(self.a, current.node.identity);
                            if (!entry.found_existing) entry.value_ptr.* = current.depth else selected = entry.value_ptr.* == current.depth and path.selector == .all_shortest;
                        }
                        if (self.lower_graph != null and self.lower_graph.?.probing) {
                            self.lower_graph.?.probe_matches = true;
                            if (self.lower_graph.?.first_only) target_depth = current.depth;
                        } else if (selected) {
                            if (self.shortest_count) |count_paths| {
                                const ways: u64 = if (path.selector == .shortest) 1 else current.ways;
                                count_paths.* = std.math.add(u64, count_paths.*, ways) catch return error.CypherNumericOverflow;
                                if (count_paths.* > std.math.maxInt(i64)) return error.CypherNumericOverflow;
                            } else {
                                var endpoint_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
                                defer endpoint_arena.deinit();
                                const endpoint_scope = if (self.stream_fold != null or self.shortest_count != null) EvalScope.enter(self, endpoint_arena.allocator()) else null;
                                defer if (endpoint_scope) |scope| scope.leave(self);
                                const nodes = try self.a.alloc(*const v.Node, current.depth + 1);
                                const relationships = try self.a.alloc(*const v.Relationship, current.depth);
                                try self.emitBreadthPaths(queue.items, head, constrained, path, mode, nodes, relationships, output);
                            }
                        }
                    }
                }
            }
            // The whole winning layer is already queued when a bound target
            // is discovered. Preserve all ties without expanding deeper layers.
            if (target_depth) |depth| if (current.depth >= depth) continue;
            if (edge.min_hops <= 1 and current.depth != 0 and current.node.identity.eql(start.identity)) continue;
            const maximum = edge.max_hops orelse std.math.maxInt(u32);
            if (current.depth >= maximum) continue;
            for (self.graph.relationships, 0..) |mapping, mapping_index| {
                var selected = edge.types.len == 0;
                for (edge.types) |name| if (std.mem.eql(u8, mapping.name, name)) {
                    selected = true;
                };
                if (!selected) continue;
                const outgoing = edge.direction != .in and try self.graph.admits(current.node, mapping.source_label, &self.values);
                const incoming = edge.direction != .out and try self.graph.admits(current.node, mapping.target_label, &self.values);
                if (!outgoing and !incoming) continue;
                var cursor: ?[]const u8 = null;
                while (true) {
                    const page = try self.expandEdges(mapping_index, mapping, current.node.identity, edge.direction, cursor, 128);
                    try self.prefetchEndpoints(page.items, current.node.identity, outgoing, incoming);
                    for (page.items) |relationship| {
                        try self.checkpoint();
                        if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
                        const traversing_out = outgoing and relationship.source.eql(current.node.identity);
                        const target = if (traversing_out) relationship.target else if (incoming and relationship.target.eql(current.node.identity)) relationship.source else continue;
                        const next_depth = current.depth + 1;
                        // An undirected positive cycle may need a different
                        // first edge from the shortest route to its penultimate
                        // node. Keep one BFS distance per first-edge branch.
                        const branch = if (edge.direction == .both and edge.min_hops == 1 and mode == .relationship_simple and current.depth == 0) queue.items.len + 1 else current.branch;
                        const distance_key: BreadthKey = .{ .identity = target, .branch = branch, .phase = if (edge.min_hops > 1) @min(next_depth, edge.min_hops) else 0 };
                        if (distances.get(distance_key)) |index| if (queue.items[index].depth < next_depth or (queue.items[index].depth == next_depth and path.selector == .shortest)) continue;
                        if (mode != .repeatable and target.eql(start.identity)) {
                            var ancestor: ?usize = head;
                            var reused = false;
                            while (ancestor) |index| {
                                const previous = queue.items[index];
                                if (previous.relationship) |prior| if (v.Value.eql(.{ .relationship = prior }, .{ .relationship = relationship })) {
                                    reused = true;
                                    break;
                                };
                                ancestor = previous.parent;
                            }
                            if (reused) continue;
                        }
                        const reached = (try self.getNode(target)) orelse continue;
                        if (!try self.graph.admits(reached, if (traversing_out) mapping.target_label else mapping.source_label, &self.values)) continue;
                        const eligible = (try self.constrain(input, .{ .relationship = relationship }, edge.properties)) orelse continue;
                        if (!try self.constraintsMatch(eligible.constraints, input.row, false)) continue;
                        if (next_depth > self.limits.path_depth) return error.CypherPathBudgetExceeded;
                        if (distances.get(distance_key)) |index| {
                            // Store every equal-length predecessor, but expand
                            // each vertex/branch only once. BFS has completed
                            // all predecessors before consuming the next layer.
                            queue.items[index].ways = @min(@as(u64, std.math.maxInt(i64)) + 1, queue.items[index].ways +| current.ways);
                            if (self.shortest_count == null) try queue.items[index].alternatives.append(self.a, .{ .previous = head, .relationship = relationship });
                            continue;
                        }
                        try distances.put(self.a, distance_key, queue.items.len);
                        if (bound_target) |goal| if (target_depth == null and next_depth >= edge.min_hops and goal.eql(target)) {
                            target_depth = next_depth;
                        };
                        try queue.append(self.a, .{ .node = reached, .parent = head, .relationship = relationship, .depth = next_depth, .branch = branch, .ways = current.ways });
                    }
                    if (page.next == null) break;
                    if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                    cursor = page.next;
                }
            }
        }
    }
    fn emitBreadthPaths(self: *Engine, queue: []const BreadthState, index: usize, input: MatchState, path: ast.Path, mode: ast.PathMode, nodes: []*const v.Node, relationships: []*const v.Relationship, output: *std.ArrayList(MatchState)) anyerror!void {
        try self.checkpoint();
        const current = queue[index];
        nodes[current.depth] = current.node;
        if (current.parent) |parent| {
            relationships[current.depth - 1] = current.relationship.?;
            try self.emitBreadthPaths(queue, parent, input, path, mode, nodes, relationships, output);
            for (current.alternatives.items) |alternative| {
                relationships[current.depth - 1] = alternative.relationship;
                try self.emitBreadthPaths(queue, alternative.previous, input, path, mode, nodes, relationships, output);
            }
            return;
        }
        var route_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer route_arena.deinit();
        const route_scope = if (self.stream_fold != null or self.shortest_count != null) EvalScope.enter(self, route_arena.allocator()) else null;
        defer if (route_scope) |scope| scope.leave(self);
        const values = try self.a.alloc(v.Value, relationships.len);
        for (relationships, values) |relationship, *value| value.* = .{ .relationship = relationship };
        const edge = path.edges[0];
        const edge_value = try self.routeEdgeValue(path, edge, values, relationships);
        if (try self.bind(input.row, edge.alias, edge_value)) |bound| {
            var completed = input;
            completed.row = bound;
            try self.pathStep(completed, path, mode, 1, nodes, relationships, output);
        }
    }
    fn pathStep(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, edge_index: usize, nodes: []const *const v.Node, relationships: []const *const v.Relationship, output: *std.ArrayList(MatchState)) anyerror!void {
        try self.state();
        if (edge_index == path.edges.len) {
            if (path.execution_prefix) {
                const continuation = self.interior.?;
                const ordered_nodes = try self.a.dupe(*const v.Node, nodes);
                const ordered_relationships = try self.a.dupe(*const v.Relationship, relationships);
                std.mem.reverse(*const v.Node, ordered_nodes);
                std.mem.reverse(*const v.Relationship, ordered_relationships);
                return self.pathStep(input, continuation.path, continuation.mode, continuation.anchor, ordered_nodes, ordered_relationships, output);
            }
            const matched_path: v.Path = if (path.alias != null or self.collect_match_paths)
                .{ .nodes = try self.a.dupe(*const v.Node, nodes), .relationships = try self.a.dupe(*const v.Relationship, relationships) }
            else
                .{ .nodes = &.{}, .relationships = &.{} };
            if (path.execution_reversed) {
                std.mem.reverse(*const v.Node, @constCast(matched_path.nodes));
                std.mem.reverse(*const v.Relationship, @constCast(matched_path.relationships));
            }
            const row = (try self.bind(input.row, path.alias, .{ .path = matched_path })) orelse return;
            if (!try self.constraintsMatch(input.constraints, row, true)) return;
            var paths = input.paths;
            if (self.collect_match_paths) {
                const collected = try self.a.alloc(v.Path, input.paths.len + 1);
                @memcpy(collected[0..input.paths.len], input.paths);
                collected[input.paths.len] = matched_path;
                paths = collected;
            }
            if (self.join) |continuation| {
                return self.joinPath(.{ .row = row, .used = input.used, .paths = paths, .constraints = input.constraints }, continuation.operation, continuation.next, output);
            }
            if (self.shortest_count) |count_paths| {
                if (!try self.constraintsMatch(input.constraints, row, false)) return;
                if (self.stream_predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) return;
                count_paths.* = try addPathCount(count_paths.*, 1);
                return;
            }
            if (self.stream_fold) |fold| {
                if (!try self.constraintsMatch(input.constraints, row, false)) return;
                if (self.stream_predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) return;
                return fold.add(self, row);
            }
            try output.append(self.a, .{ .row = row, .used = input.used, .paths = paths, .constraints = input.constraints });
            return;
        }
        try self.expandPath(input, path, mode, edge_index, nodes, relationships, 0, &.{}, output);
    }

    fn routeEdgeValue(self: *Engine, path: ast.Path, edge: ast.Edge, values: []const v.Value, relationships: []const *const v.Relationship) !v.Value {
        if (!edge.variable_length) return .{ .relationship = relationships[relationships.len - 1] };
        if (!path.execution_reversed) return .{ .list = values };
        const reversed = try self.a.dupe(v.Value, values);
        std.mem.reverse(v.Value, reversed);
        return .{ .list = reversed };
    }
    fn expandPath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, edge_index: usize, nodes: []const *const v.Node, relationships: []const *const v.Relationship, hops: u32, edge_values: []const v.Value, output: *std.ArrayList(MatchState)) anyerror!void {
        const edge = path.edges[edge_index];
        const current = nodes[nodes.len - 1];
        if (hops >= edge.min_hops and try self.nodeMatches(current, path.nodes[edge_index + 1])) {
            if (try self.bind(input.row, path.nodes[edge_index + 1].alias, .{ .node = current })) |node_row| {
                const edge_value = try self.routeEdgeValue(path, edge, edge_values, relationships);
                if (try self.bind(node_row, edge.alias, edge_value)) |edge_row| {
                    if (try self.constrain(.{ .row = edge_row, .used = input.used, .paths = input.paths, .constraints = input.constraints }, .{ .node = current }, path.nodes[edge_index + 1].properties)) |candidate|
                        try self.pathStep(candidate, path, mode, edge_index + 1, nodes, relationships, output);
                }
            }
        }
        if (edge.max_hops) |maximum| if (hops >= maximum) return;
        for (self.graph.relationships, 0..) |mapping, mapping_index| {
            var selected = edge.types.len == 0;
            for (edge.types) |type_name| {
                if (self.graph.relationship(type_name) == null) return error.UnknownGraphRelationship;
                if (std.mem.eql(u8, mapping.name, type_name)) selected = true;
            }
            if (!selected) continue;
            const outgoing = edge.direction != .in and try self.graph.admits(current, mapping.source_label, &self.values);
            const incoming = edge.direction != .out and try self.graph.admits(current, mapping.target_label, &self.values);
            if (!outgoing and !incoming) continue;
            var cursor: ?[]const u8 = null;
            var cursor_bytes: std.ArrayList(u8) = .empty;
            defer cursor_bytes.deinit(self.scratch_allocator);
            while (true) {
                var page_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
                defer page_arena.deinit();
                const page_scope = if (self.stream_fold != null or self.shortest_count != null) EvalScope.enter(self, page_arena.allocator()) else null;
                if (page_scope != null) self.cache_reads = true;
                defer if (page_scope) |scope| scope.leave(self);
                try self.checkpoint();
                const demand = if (self.stream_fold) |fold| fold.demand() else 128;
                const page = try self.expandPatternEdges(mapping_index, mapping, current.identity, path.nodes[edge_index + 1], edge, hops, input.row, cursor, demand);

                try self.prefetchEndpoints(page.items, current.identity, outgoing, incoming);
                for (page.items) |relationship| {
                    var used = false;
                    if (mode != .repeatable) for (input.used) |previous| if (v.Value.eql(.{ .relationship = previous }, .{ .relationship = relationship })) {
                        used = true;
                        break;
                    };
                    if (used) continue;
                    if (!std.mem.eql(u8, relationship.index, mapping.index) or !std.mem.eql(u8, relationship.type, mapping.name)) return error.CypherInvalidBackendRelationship;
                    const traversing_out = outgoing and relationship.source.eql(current.identity);
                    const target = if (traversing_out) relationship.target else if (incoming and relationship.target.eql(current.identity)) relationship.source else continue;
                    const reached = (try self.getNode(target)) orelse continue;
                    const target_label = if (traversing_out) mapping.target_label else mapping.source_label;
                    if (!try self.graph.admits(reached, target_label, &self.values)) continue;
                    if (mode == .node_simple) {
                        var revisited = false;
                        for (nodes) |n| if (n.identity.eql(reached.identity)) {
                            revisited = true;
                            break;
                        };
                        if (revisited) continue;
                    }
                    const candidate = (try self.constrain(input, .{ .relationship = relationship }, edge.properties)) orelse continue;
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
                    try self.expandPath(.{ .row = input.row, .used = next_used, .paths = input.paths, .constraints = candidate.constraints }, path, mode, edge_index, next_nodes, next_relationships, hops + 1, next_values, output);
                }
                if (page.next == null) break;
                if (cursor) |old| if (std.mem.eql(u8, old, page.next.?)) return error.CypherInvalidBackendCursor;
                if (self.stream_fold != null or self.shortest_count != null) {
                    cursor_bytes.clearRetainingCapacity();
                    try cursor_bytes.appendSlice(self.scratch_allocator, page.next.?);
                    cursor = cursor_bytes.items;
                } else cursor = page.next;
            }
        }
    }

    // Deletion keeps the binding's identity available for counting/equality,
    // but never permits reading its former contents or reusing its endpoints.
    fn isDeleted(self: *Engine, value: v.Value) bool {
        if (value != .node and value != .relationship) return false;
        const identity = v.EntityIdentity.fromValue(value) catch unreachable;
        if (self.deletions.get(identity) == .deleted) return true;
        return value == .relationship and (self.deletions.get(.{ .node = value.relationship.source }) == .deleted or self.deletions.get(.{ .node = value.relationship.target }) == .deleted);
    }
    fn recordDeletion(self: *Engine, identity: v.EntityIdentity) !void {
        try chargeWork(self, 1);
        try self.deletions.put(self.a, identity, .deleted);
        self.has_deletions = true;
    }
    fn requireLiveContents(self: *Engine, value: v.Value) !void {
        if (self.isDeleted(value)) return error.CypherDeletedEntity;
        if (value == .relationship) if (self.backend.vtable.get_relationship) |get_relationship| {
            if (try get_relationship(self.backend.ptr, self.a, .{ .identity = value.relationship.identity, .index = value.relationship.index }) == null) return error.CypherDeletedEntity;
        };
    }
    fn readValue(self: *Engine, value: v.Value) anyerror!v.Value {
        if (!(try self.values.info(value)).entities) return value;
        if (self.backend.vtable.project_node != null or self.backend.vtable.get_relationship != null or self.has_deletions or self.postimages.count() != 0) try self.values.entityTraversal(value);
        return self.readValueInner(value);
    }
    fn currentEntity(self: *Engine, value: v.Value) v.Value {
        if (value != .node and value != .relationship) return value;
        return self.postimages.get(v.EntityIdentity.fromValue(value) catch unreachable) orelse value;
    }
    fn readValueInner(self: *Engine, retained_value: v.Value) anyerror!v.Value {
        const value = self.currentEntity(retained_value);
        if (!(try self.values.info(value)).entities) return value;
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
        if (project_node == null and self.backend.vtable.get_relationship == null and !self.has_deletions and self.postimages.count() == 0) return value;
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
                    const visible = try self.readValueInner(.{ .node = node });
                    if (visible == .null) break :blk .null;
                    out.* = visible.node;
                }
                const relationships = try self.a.alloc(*const v.Relationship, path.relationships.len);
                for (path.relationships, relationships) |relationship, *out| {
                    const visible = try self.readValueInner(.{ .relationship = relationship });
                    if (visible == .null) break :blk .null;
                    out.* = visible.relationship;
                }
                break :blk .{ .path = .{ .nodes = nodes, .relationships = relationships } };
            },
            .list => |items| blk: {
                const visible = try self.a.alloc(v.Value, items.len);
                for (items, visible) |item, *out| out.* = try self.readValueInner(item);
                break :blk .{ .list = visible };
            },
            .map => |map_entries| blk: {
                const visible = try self.a.alloc(v.Entry, map_entries.len);
                for (map_entries, visible) |entry, *out| out.* = .{ .name = entry.name, .value = try self.readValueInner(entry.value) };
                break :blk .{ .map = visible };
            },
            else => value,
        };
    }
    fn mutationEntity(self: *Engine, expression: *const ast.Expr, row: Row) !v.Value {
        // A statement can retain its own opaque write binding even when the
        // new values fail the read policy. Reading RHS expressions still
        // projects them; only a direct mutation target bypasses projection.
        if (expression.value == .variable) return self.currentEntity(lookup(row, expression.value.variable) orelse return error.UnboundCypherVariable);
        return self.eval(expression, row, null);
    }
    fn eval(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) anyerror!v.Value {
        const result = try self.evalInner(expression, row, group);
        _ = try self.values.info(result);
        return result;
    }
    fn evalInner(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) anyerror!v.Value {
        if (expression.value != .variable) if (self.projection_overrides) |overrides| for (overrides.items, overrides.row) |item, entry| {
            if (@import("expressions.zig").same(item.expression, expression) and @import("expressions.zig").unshadowed(expression, self.projection_locals)) return entry.value;
        };
        if (self.aggregate_overrides) |overrides| for (overrides) |override| if (override.expression == expression) return override.value;
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
                if (value != .null and value != .node and value != .relationship and value != .map) return error.CypherTypeMismatch;
                break :blk try self.values.property(value, p.name);
            },
            .index => |i| blk: {
                const value = try self.eval(i.base, row, group);
                const index = try self.eval(i.index, row, group);
                if (value == .null or index == .null) break :blk .null;
                if ((value == .map or value == .node or value == .relationship) and index == .string) {
                    try self.requireLiveContents(value);
                    break :blk try self.values.property(value, index.string);
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
                const previous_locals = self.projection_locals;
                defer self.projection_locals = previous_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = previous_locals };
                self.projection_locals = &local;
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
                const previous_locals = self.projection_locals;
                defer self.projection_locals = previous_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = previous_locals };
                self.projection_locals = &local;
                const accumulator_local: @import("expressions.zig").Local = .{ .name = c.accumulator, .parent = &local };
                self.projection_locals = &accumulator_local;
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
                const previous_locals = self.projection_locals;
                defer self.projection_locals = previous_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = previous_locals };
                self.projection_locals = &local;
                for (input.list) |item| {
                    try self.state();
                    const inner = try self.replace(row, c.alias, item);
                    if (c.predicate) |predicate| if ((try (try self.eval(predicate, inner, null)).truth()) != true) continue;
                    try values.append(self.a, if (c.projection) |projection| try self.eval(projection, inner, null) else item);
                }
                break :blk .{ .list = try values.toOwnedSlice(self.a) };
            },
            .pattern_comprehension => |c| blk: {
                const pattern_scope = PatternScope.enter(self);
                defer pattern_scope.leave(self);
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
                var positions: std.StringHashMapUnmanaged(usize) = .empty;
                const indexed = output.items.len > 8 or p.items.len > 8;
                if (indexed) for (output.items, 0..) |entry, index| {
                    try chargeWork(self, 1 + entry.name.len / 64);
                    try positions.put(self.a, entry.name, index);
                };
                for (p.items) |item| {
                    const entry: v.Entry = switch (item) {
                        .all => continue,
                        .property => |name| .{ .name = name, .value = try self.values.property(base, name) },
                        .variable => |name| .{ .name = name, .value = try self.readValue(lookup(row, name) orelse return error.UnboundCypherVariable) },
                        .entry => |entry| .{ .name = entry.name, .value = try self.eval(entry.value, row, group) },
                    };
                    try chargeWork(self, 1 + entry.name.len / 64);
                    if (!indexed) {
                        var replaced = false;
                        for (output.items) |*previous| {
                            try chargeWork(self, 1 + previous.name.len / 64);
                            if (std.mem.eql(u8, previous.name, entry.name)) {
                                previous.value = entry.value;
                                replaced = true;
                                break;
                            }
                        }
                        if (!replaced) try output.append(self.a, entry);
                        continue;
                    }
                    const position = try positions.getOrPut(self.a, entry.name);
                    if (position.found_existing) output.items[position.value_ptr.*].value = entry.value else {
                        position.value_ptr.* = output.items.len;
                        try output.append(self.a, entry);
                    }
                }
                break :blk .{ .map = try output.toOwnedSlice(self.a) };
            },
            .call => |c| if (std.ascii.eqlIgnoreCase(c.name, "size") and c.args.len == 1 and c.args[0].value == .pattern_comprehension and weightedExpression(c.args[0]))
                try self.comprehensionSize(c.args[0], row)
            else if (aggregateName(c.name)) try self.aggregate(c, group orelse return error.CypherAggregateScope) else try self.function(c.name, c.args, row, group),
            .case_when => |c| blk: {
                const operand = if (c.operand) |x| try self.eval(x, row, group) else null;
                for (c.branches) |branch| {
                    const condition = try self.eval(branch.condition, row, group);
                    const matched = if (operand) |x| (try self.values.equal(x, condition)) == true else (try condition.truth()) == true;
                    if (matched) break :blk try self.eval(branch.value, row, group);
                }
                break :blk if (c.otherwise) |other| try self.eval(other, row, group) else .null;
            },
            .exists => |clauses| blk: {
                const pattern_scope = PatternScope.enter(self);
                defer pattern_scope.leave(self);
                const columns = self.columns;
                defer self.columns = columns;
                const candidates = try self.existsRows(clauses, row);
                break :blk .{ .boolean = candidates.len > 0 };
            },
        };
    }
    fn comprehensionSize(self: *Engine, expression: *const ast.Expr, row: Row) anyerror!v.Value {
        const scope = PatternScope.enter(self);
        defer scope.leave(self);
        const comprehension = expression.value.pattern_comprehension;
        var input = row;
        if (comprehension.path.alias) |alias| {
            var local: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (!std.mem.eql(u8, entry.name, alias)) try local.append(self.a, entry);
            input = local.items;
        }
        const count_expression: ast.Expr = .{ .span = expression.span, .value = .{ .call = .{ .name = "count", .args = &.{}, .star = true } } };
        const dependencies = [_]ast.Clause{
            .{ .span = expression.span, .operation = .{ .project = .{ .value = .{ .items = &.{.{ .expression = comprehension.projection, .alias = "_comprehension_value" }} }, .final = false } } },
            .{ .span = expression.span, .operation = .{ .project = .{ .value = .{ .items = &.{.{ .expression = &count_expression }} }, .final = true } } },
        };
        const matched = try self.weightedMatch(input, .{ .paths = &.{comprehension.path}, .predicate = comprehension.predicate }, &dependencies);
        var count_value: u64 = 0;
        for (matched.rows, matched.weights) |binding, weight| {
            // SIZE observes cardinality, but constructing the comprehension
            // would still evaluate its projection, including NULLs and errors.
            var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
            defer scratch.deinit();
            const evaluation = EvalScope.enter(self, scratch.allocator());
            defer evaluation.leave(self);
            _ = try self.eval(comprehension.projection, binding, null);
            count_value = try addPathCount(count_value, weight.cap(std.math.maxInt(u64)));
        }
        return .{ .integer = @intCast(count_value) };
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
                if ((try self.values.equal(left, item))) |same| {
                    if (same) return .{ .boolean = true };
                } else unknown = true;
            }
            return if (unknown) .null else .{ .boolean = false };
        }
        if (left == .null or right == .null) return .null;
        if (op == .eq or op == .neq) return if ((try self.values.equal(left, right))) |same| .{ .boolean = same == (op == .eq) } else .null;
        if (op == .lt or op == .lte or op == .gt or op == .gte) {
            const order_value = (try self.values.compare(left, right)) orelse return .null;
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
            if (op == .contains) return .{ .boolean = try self.strings.contains(left.string, right.string) };
            if (right.string.len > left.string.len) return .{ .boolean = false };
            try self.strings.reserve(right.string.len, 1);
            return .{ .boolean = if (op == .starts_with) std.mem.startsWith(u8, left.string, right.string) else std.mem.endsWith(u8, left.string, right.string) };
        }
        if (op == .regex) {
            if (left != .string or right != .string) return .null;
            const remaining = self.limits.intermediate_states -| self.stats.intermediate_states;
            var work: @import("regex.zig").Work = .{ .remaining = remaining, .limit_error = error.CypherStateBudgetExceeded, .ptr = self, .checkpoint = regexCheckpoint, .checkpoint_interval = 128 };
            defer self.stats.intermediate_states += remaining - work.remaining;
            return .{ .boolean = try self.regex.matches(left.string, right.string, &work) };
        }
        if (op == .add and (left == .string or right == .string) and left != .list and right != .list) {
            const lhs = try self.scalarString(left);
            const rhs = try self.scalarString(right);
            try self.strings.reserve(lhs.len +| rhs.len, 1);
            return .{ .string = try std.fmt.allocPrint(self.a, "{s}{s}", .{ lhs, rhs }) };
        }
        if (op == .add and (left == .list or right == .list)) {
            const lhs = if (left == .list) left.list else &.{left};
            const rhs = if (right == .list) right.list else &.{right};
            try chargeWork(self, lhs.len +| rhs.len);
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
    fn scalarString(self: *Engine, value: v.Value) ![]const u8 {
        return switch (value) {
            .string => value.string,
            .boolean => if (value.boolean) "true" else "false",
            .integer => try std.fmt.allocPrint(self.a, "{d}", .{value.integer}),
            .number => try numbers.floatString(self.a, value.number),
            else => error.CypherTypeMismatch,
        };
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
            return .{ .string = try self.strings.substring(text.string, std.math.cast(usize, start.integer) orelse std.math.maxInt(usize), std.math.cast(usize, substring_length.integer) orelse std.math.maxInt(usize)) };
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
                try chargeWork(self, value.list.len);
                const items = try self.a.dupe(v.Value, value.list);
                std.mem.reverse(v.Value, items);
                return .{ .list = items };
            }
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try self.strings.reverse(value.string) };
        }
        if (std.ascii.eqlIgnoreCase(name, "trim") or std.ascii.eqlIgnoreCase(name, "ltrim") or std.ascii.eqlIgnoreCase(name, "rtrim")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try self.strings.trim(value.string, !std.ascii.eqlIgnoreCase(name, "rtrim"), !std.ascii.eqlIgnoreCase(name, "ltrim")) };
        }
        if (std.ascii.eqlIgnoreCase(name, "toString")) return switch (value) {
            .string => value,
            .boolean => .{ .string = if (value.boolean) "true" else "false" },
            .integer => .{ .string = try std.fmt.allocPrint(self.a, "{d}", .{value.integer}) },
            .number => .{ .string = try numbers.floatString(self.a, value.number) },
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "toBoolean")) return switch (value) {
            .boolean => value,
            .integer => .{ .boolean = value.integer != 0 },
            .string => blk: {
                const text = try self.strings.trim(value.string, true, true);
                break :blk if (std.ascii.eqlIgnoreCase(text, "true")) .{ .boolean = true } else if (std.ascii.eqlIgnoreCase(text, "false")) .{ .boolean = false } else .null;
            },
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "toInteger") or std.ascii.eqlIgnoreCase(name, "toFloat")) {
            const integer = std.ascii.eqlIgnoreCase(name, "toInteger");
            if (integer and value == .integer) return value;
            if (integer and value == .boolean) return .{ .integer = @intFromBool(value.boolean) };
            if (value != .integer and value != .number and value != .string) return error.CypherTypeMismatch;
            if (value == .string) return if (integer) self.strings.integer(value.string) else self.strings.float(value.string);
            const number = try value.numeric();
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
            .string => |s| try self.strings.size(s),
            else => return error.CypherTypeMismatch,
        }) };
        if (std.ascii.eqlIgnoreCase(name, "head") or std.ascii.eqlIgnoreCase(name, "last")) {
            if (value != .list) return error.CypherTypeMismatch;
            return if (value.list.len == 0) .null else value.list[if (std.ascii.eqlIgnoreCase(name, "head")) 0 else value.list.len - 1];
        }
        if (std.ascii.eqlIgnoreCase(name, "toLower")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try self.strings.caseConvert(value.string, false) };
        }
        if (std.ascii.eqlIgnoreCase(name, "toUpper")) {
            if (value != .string) return error.CypherTypeMismatch;
            return .{ .string = try self.strings.caseConvert(value.string, true) };
        }
        if (std.ascii.eqlIgnoreCase(name, "abs")) return if (value == .integer) .{ .integer = std.math.cast(i64, @abs(value.integer)) orelse return error.CypherNumericOverflow } else .{ .number = @abs(try value.numeric()) };
        return error.UnknownCypherFunction;
    }
    fn aggregate(self: *Engine, call_value: ast.Function, group: []const Row) !v.Value {
        if (call_value.args.len != (if (call_value.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
        if (!call_value.distinct and !std.ascii.eqlIgnoreCase(call_value.name, "collect")) {
            var count_values: usize = 0;
            var best: v.Value = .null;
            var sum: v.Value = .{ .integer = 0 };
            var mean: f64 = 0;
            for (group) |row| {
                const value = if (call_value.star) v.Value{ .integer = 1 } else try self.eval(call_value.args[0], row, null);
                if (value == .null) continue;
                count_values += 1;
                if (std.ascii.eqlIgnoreCase(call_value.name, "count")) continue;
                if (std.ascii.eqlIgnoreCase(call_value.name, "avg")) {
                    mean = numbers.nextAverage(mean, try value.numeric(), count_values);
                    if (!std.math.isFinite(mean)) return error.CypherNumericOverflow;
                } else if (std.ascii.eqlIgnoreCase(call_value.name, "sum")) {
                    if (value != .integer and value != .number) return error.CypherTypeMismatch;
                    sum = try self.binary(.add, sum, value);
                } else if (count_values == 1) best = value else {
                    const order = try self.values.order(value, best);
                    if ((order == .lt and std.ascii.eqlIgnoreCase(call_value.name, "min")) or (order == .gt and std.ascii.eqlIgnoreCase(call_value.name, "max"))) best = value;
                }
            }
            if (std.ascii.eqlIgnoreCase(call_value.name, "count")) return .{ .integer = @intCast(count_values) };
            if (std.ascii.eqlIgnoreCase(call_value.name, "sum")) return sum;
            if (count_values == 0) return .null;
            if (std.ascii.eqlIgnoreCase(call_value.name, "avg")) return .{ .number = mean };
            return best;
        }
        var items: std.ArrayList(v.Value) = .empty;
        var distinct_values: value_work.Index = .{};
        for (group) |row| {
            const value = if (call_value.star) v.Value{ .integer = 1 } else try self.eval(call_value.args[0], row, null);
            if (value == .null) continue;
            if (call_value.distinct) {
                const hash = (try self.values.info(value)).hash;
                var candidate = distinct_values.head(hash);
                var duplicate = false;
                while (candidate) |position| {
                    const link = distinct_values.links.items[position];
                    if (try self.values.eql(items.items[link.index], value)) {
                        duplicate = true;
                        break;
                    }
                    candidate = link.next;
                }
                if (duplicate) continue;
                try distinct_values.insert(self.a, hash, items.items.len);
            }
            try items.append(self.a, value);
        }
        if (std.ascii.eqlIgnoreCase(call_value.name, "count")) return .{ .integer = @intCast(items.items.len) };
        if (std.ascii.eqlIgnoreCase(call_value.name, "collect")) return .{ .list = try items.toOwnedSlice(self.a) };
        if (items.items.len == 0) return if (std.ascii.eqlIgnoreCase(call_value.name, "sum")) .{ .integer = 0 } else .null;
        if (std.ascii.eqlIgnoreCase(call_value.name, "min") or std.ascii.eqlIgnoreCase(call_value.name, "max")) {
            var best = items.items[0];
            for (items.items[1..]) |item| {
                const order_value = try self.values.order(item, best);
                if ((order_value == .lt and std.ascii.eqlIgnoreCase(call_value.name, "min")) or (order_value == .gt and std.ascii.eqlIgnoreCase(call_value.name, "max"))) best = item;
            }
            return best;
        }
        if (std.ascii.eqlIgnoreCase(call_value.name, "avg")) {
            var mean: f64 = 0;
            for (items.items, 1..) |item, item_count| {
                mean = numbers.nextAverage(mean, try item.numeric(), item_count);
                if (!std.math.isFinite(mean)) return error.CypherNumericOverflow;
            }
            return .{ .number = mean };
        }
        var sum: v.Value = .{ .integer = 0 };
        for (items.items) |item| {
            if (item != .integer and item != .number) return error.CypherTypeMismatch;
            sum = try self.binary(.add, sum, item);
        }
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
    const Projected = struct { row: Row, source: Row, group: ?[]const Row, keys: []const v.Value, sequence: usize = 0, weight: usize = 1, multiplicity: ?Count = null };
    const PatternScope = struct {
        columns: @FieldType(Engine, "columns"),
        stages: @FieldType(Engine, "stages"),
        retain_windows: bool,
        match_weight_demand: ?Multiplicity,
        terminal_weight_demand: ?Multiplicity,
        pending_window: ?WeightedRows,
        fn enter(engine: *Engine) PatternScope {
            const scope: PatternScope = .{ .columns = engine.columns, .stages = engine.stages, .retain_windows = engine.retain_windows, .match_weight_demand = engine.match_weight_demand, .terminal_weight_demand = engine.terminal_weight_demand, .pending_window = engine.pending_window };
            // A correlated query owns its result/profiling buffers and must
            // produce ordinary expression values. Its caller may itself be
            // retaining a weighted window in a temporary evaluation arena.
            engine.stages = .empty;
            engine.retain_windows = false;
            engine.match_weight_demand = null;
            engine.terminal_weight_demand = null;
            engine.pending_window = null;
            return scope;
        }
        fn leave(scope: PatternScope, engine: *Engine) void {
            engine.columns = scope.columns;
            engine.stages = scope.stages;
            engine.retain_windows = scope.retain_windows;
            engine.match_weight_demand = scope.match_weight_demand;
            engine.terminal_weight_demand = scope.terminal_weight_demand;
            engine.pending_window = scope.pending_window;
        }
    };
    const EvalScope = struct {
        a: A,
        values: value_work.Work,
        strings: @import("string_work.zig").Runtime,
        regex: @import("regex.zig").Runtime,
        reads: @FieldType(Engine, "read_nodes"),
        cache_reads: bool,
        fn enter(engine: *Engine, a: A) EvalScope {
            const scope: EvalScope = .{ .a = engine.a, .values = engine.values, .strings = engine.strings, .regex = engine.regex, .reads = engine.read_nodes, .cache_reads = engine.cache_reads };
            engine.a = a;
            engine.values = .{ .a = a, .ptr = engine, .charge = chargeWork };
            engine.strings = .{ .a = a, .ptr = engine, .charge = chargeWork };
            engine.regex = .{ .a = a, .scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator) };
            engine.read_nodes = .empty;
            engine.cache_reads = false;
            return scope;
        }
        fn leave(scope: EvalScope, engine: *Engine) void {
            engine.a = scope.a;
            engine.values = scope.values;
            engine.strings = scope.strings;
            engine.regex.deinit();
            engine.regex = scope.regex;
            engine.read_nodes = scope.reads;
            engine.cache_reads = scope.cache_reads;
        }
    };
    const Pick = struct { arena: std.heap.ArenaAllocator, value: Projected };
    fn compareProjected(self: *Engine, left: Projected, right: Projected, orders: []const ast.Order) !std.math.Order {
        try chargeWork(self, 1);
        // Heap entries can be evicted and their allocations recycled. Comparison
        // metadata must never enter the statement's allocation-identity cache.
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = self, .charge = chargeWork };
        for (orders, 0..) |order, index| {
            const comparison = try work.order(left.keys[index], right.keys[index]);
            if (comparison != .eq) return if (order.descending) comparison.invert() else comparison;
        }
        return std.math.order(left.sequence, right.sequence);
    }
    fn siftPicks(self: *Engine, picks: []Pick, orders: []const ast.Order) !void {
        var parent: usize = 0;
        while (parent * 2 + 1 < picks.len) {
            var worst = parent * 2 + 1;
            if (worst + 1 < picks.len and try self.compareProjected(picks[worst + 1].value, picks[worst].value, orders) == .gt) worst += 1;
            if (try self.compareProjected(picks[parent].value, picks[worst].value, orders) != .lt) break;
            std.mem.swap(Pick, &picks[parent], &picks[worst]);
            parent = worst;
        }
    }
    fn clonePick(self: *Engine, candidate: Projected) !Pick {
        var arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        const row = (try (v.Value{ .map = candidate.row }).clone(a)).map;
        const keys = (try (v.Value{ .list = candidate.keys }).clone(a)).list;
        return .{ .arena = arena, .value = .{ .row = row, .source = &.{}, .group = null, .keys = keys, .sequence = candidate.sequence, .weight = candidate.weight, .multiplicity = if (candidate.multiplicity) |weight| try weight.clone(a) else null } };
    }
    const TopK = struct {
        engine: *Engine,
        projection: ast.Projection,
        retained: A,
        skip: usize,
        capacity: usize,
        sequence: usize = 0,
        total: usize = 0,
        picks: std.ArrayList(Pick) = .empty,
        columns: std.ArrayList([]const u8) = .empty,
        fn init(engine: *Engine, projection: ast.Projection, first: Row) !TopK {
            const skip = if (projection.skip) |expression| try engine.count(expression) else 0;
            // Without a limit this is a result collector. Its storage grows
            // with emitted rows; it never builds the bounded selection heap.
            const limit = if (projection.limit) |expression| try engine.count(expression) else std.math.maxInt(usize) - skip;
            var result: TopK = .{ .engine = engine, .projection = projection, .retained = engine.a, .skip = skip, .capacity = if (limit == 0) 0 else std.math.add(usize, skip, limit) catch return error.CypherLimitExceeded };
            if (projection.wildcard) for (first) |entry| try result.columns.append(engine.a, entry.name);
            for (projection.items) |item| try result.columns.append(engine.a, item.alias orelse try engine.expressionName(item.expression));
            return result;
        }
        fn deinit(self: *TopK) void {
            for (self.picks.items) |*pick| pick.arena.deinit();
        }
        fn add(self: *TopK, source: Row) !void {
            return self.addWeighted(source, Count.one);
        }
        fn addWeighted(self: *TopK, source: Row, weight: Count) !void {
            const engine = self.engine;
            defer self.sequence += 1;
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            const scope = EvalScope.enter(engine, scratch.allocator());
            defer scope.leave(engine);
            var row: std.ArrayList(v.Entry) = .empty;
            if (self.projection.wildcard) for (self.columns.items[0 .. self.columns.items.len - self.projection.items.len]) |name| {
                try row.append(engine.a, .{ .name = name, .value = lookup(source, name) orelse return error.UnboundCypherVariable });
            };
            for (self.projection.items) |item| try row.append(engine.a, .{ .name = item.alias orelse try engine.expressionName(item.expression), .value = try engine.eval(item.expression, source, null) });
            var context = source;
            for (row.items) |entry| context = try engine.replace(context, entry.name, entry.value);
            if (self.projection.predicate) |predicate| if ((try (try engine.eval(predicate, context, null)).truth()) != true) return;
            const keys = try engine.a.alloc(v.Value, self.projection.order.len);
            for (self.projection.order, keys) |order, *key| key.* = try engine.eval(order.expression, context, null);
            const candidate: Projected = .{ .row = row.items, .source = &.{}, .group = null, .keys = keys, .sequence = self.sequence, .weight = @intCast(weight.cap(self.capacity)), .multiplicity = if (self.projection.limit == null) weight else null };
            if (self.capacity == 0 or weight.cap(1) == 0) return;
            if (self.projection.limit != null and self.total == self.capacity and try engine.compareProjected(candidate, self.picks.items[0].value, self.projection.order) != .lt) return;
            var pick = try engine.clonePick(candidate);
            var owned = true;
            defer if (owned) pick.arena.deinit();
            // One heap entry represents a whole identical projected bag.
            // Trim weights at the worst key rather than inserting K copies.
            try self.picks.append(self.retained, pick);
            owned = false;
            var total = @as(u128, self.total) + candidate.weight;
            if (self.projection.limit != null) {
                var position = self.picks.items.len - 1;
                while (position > 0) {
                    const parent = (position - 1) / 2;
                    if (try engine.compareProjected(self.picks.items[parent].value, self.picks.items[position].value, self.projection.order) != .lt) break;
                    std.mem.swap(Pick, &self.picks.items[parent], &self.picks.items[position]);
                    position = parent;
                }
                while (total > self.capacity) {
                    const discarded: usize = @intCast(@min(total - self.capacity, self.picks.items[0].value.weight));
                    self.picks.items[0].value.weight -= discarded;
                    total -= discarded;
                    if (self.picks.items[0].value.weight == 0) {
                        self.picks.items[0].arena.deinit();
                        const last = self.picks.pop().?;
                        if (self.picks.items.len != 0) {
                            self.picks.items[0] = last;
                            try engine.siftPicks(self.picks.items, self.projection.order);
                        }
                    }
                }
            }
            self.total = if (engine.match_weight_demand != null or engine.retain_windows) @intCast(@min(total, std.math.maxInt(usize))) else std.math.cast(usize, total) orelse return error.CypherNumericOverflow;
        }
        fn finish(self: *TopK, final: bool) ![]const Row {
            const engine = self.engine;
            const projected = try engine.a.alloc(Projected, self.picks.items.len);
            for (self.picks.items, projected) |pick, *item| item.* = .{ .row = (try (v.Value{ .map = pick.value.row }).clone(engine.a)).map, .source = &.{}, .group = null, .keys = (try (v.Value{ .list = pick.value.keys }).clone(engine.a)).list, .sequence = pick.value.sequence, .weight = pick.value.weight, .multiplicity = if (pick.value.multiplicity) |weight| try weight.clone(engine.a) else null };
            // Sort only the retained window, using input sequence for stable ties.
            if (self.projection.order.len == 0 and self.projection.limit != null) std.mem.sort(Projected, projected, {}, struct {
                fn less(_: void, left: Projected, right: Projected) bool {
                    return left.sequence < right.sequence;
                }
            }.less) else if (self.projection.order.len != 0) try engine.sort(projected, self.projection.order);
            var skip = @min(self.skip, self.total);
            if (engine.retain_windows) {
                var rows: std.ArrayList(Row) = .empty;
                var weights: std.ArrayList(Count) = .empty;
                for (projected) |item| {
                    const discarded = @min(skip, item.weight);
                    skip -= discarded;
                    if (item.weight == discarded) continue;
                    try rows.append(engine.a, item.row);
                    try weights.append(engine.a, try (item.multiplicity orelse Count.init(item.weight)).subtract(engine.a, discarded));
                }
                engine.pending_window = .{ .rows = rows.items, .weights = weights.items };
                if (final) engine.columns = try self.columns.toOwnedSlice(engine.a);
                return rows.items;
            }
            const output = try engine.a.alloc(Row, self.total - skip);
            var position: usize = 0;
            for (projected) |item| {
                const discarded = @min(skip, item.weight);
                skip -= discarded;
                for (0..item.weight - discarded) |_| {
                    try engine.state();
                    output[position] = item.row;
                    position += 1;
                }
            }
            if (final) engine.columns = try self.columns.toOwnedSlice(engine.a);
            return output;
        }
    };
    fn orderedTopK(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool) ![]const Row {
        var top = try TopK.init(self, projection, if (rows.len == 0) &.{} else rows[0]);
        defer top.deinit();
        for (rows) |row| try top.add(row);
        return top.finish(final);
    }
    fn orderValue(self: *Engine, expression: *const ast.Expr, projection: ast.Projection, projected: Row, scope: Row, group: ?[]const Row) !v.Value {
        const previous = self.projection_overrides;
        defer self.projection_overrides = previous;
        // DISTINCT sorts its already computed values. Structural references
        // remain valid even if a projection alias shadows the source entity.
        if (projection.distinct) self.projection_overrides = .{ .items = projection.items, .row = projected[projected.len - projection.items.len ..] };
        return self.eval(expression, scope, group);
    }
    // Unordered, non-grouped projection needs one output binding per input.
    // Avoid singleton group buffers, Projected records and unused source/alias
    // scopes. Every input is still evaluated before applying SKIP/LIMIT.
    fn projectUnordered(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool) anyerror![]const Row {
        var columns: std.ArrayList([]const u8) = .empty;
        if (projection.wildcard and rows.len != 0) for (rows[0]) |entry| try columns.append(self.a, entry.name);
        for (projection.items) |item| try columns.append(self.a, item.alias orelse try self.expressionName(item.expression));
        var output: std.ArrayList(Row) = .empty;
        for (rows) |source| {
            const prefix = if (projection.wildcard) source.len else 0;
            const row = try self.a.alloc(v.Entry, prefix + projection.items.len);
            @memcpy(row[0..prefix], source[0..prefix]);
            for (projection.items, row[prefix..], columns.items[prefix..]) |item, *entry, name| entry.* = .{ .name = name, .value = try self.eval(item.expression, source, null) };
            if (projection.predicate) |predicate| {
                var scope = source;
                for (row) |entry| scope = try self.replace(scope, entry.name, entry.value);
                if ((try (try self.eval(predicate, scope, null)).truth()) != true) continue;
            }
            try output.append(self.a, row);
        }
        const skip = if (projection.skip) |expression| try self.count(expression) else 0;
        const limit = if (projection.limit) |expression| try self.count(expression) else output.items.len;
        const start = @min(skip, output.items.len);
        const end = start + @min(limit, output.items.len - start);
        if (final) self.columns = try columns.toOwnedSlice(self.a);
        return output.items[start..end];
    }
    fn project(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool) anyerror![]const Row {
        var grouped = false;
        for (projection.items) |item| grouped = grouped or aggregates(item.expression);
        if (grouped and projection.wildcard) return error.CypherAggregateWildcard;
        if (!grouped and !projection.distinct and projection.order.len == 0) return self.projectUnordered(rows, projection, final);
        if (!grouped and !projection.distinct and projection.limit != null and projection.order.len != 0) top_k: {
            // Pattern/subquery evaluation retains nested execution plans and
            // must use the general scope until it has a streaming operator.
            for (projection.items) |item| if (@import("expressions.zig").containsPattern(item.expression)) break :top_k;
            for (projection.order) |order| if (@import("expressions.zig").containsPattern(order.expression)) break :top_k;
            if (projection.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) break :top_k;
            return self.orderedTopK(rows, projection, final);
        }

        var groups: std.ArrayList(std.ArrayList(Row)) = .empty;
        if (grouped) {
            var keys: std.ArrayList([]const v.Value) = .empty;
            var group_index: value_work.Index = .{};
            for (rows) |row| {
                var values: std.ArrayList(v.Value) = .empty;
                for (projection.items) |item| if (!aggregates(item.expression)) try values.append(self.a, try self.eval(item.expression, row, null));
                const key_values = try values.toOwnedSlice(self.a);
                const hash = (try self.values.info(.{ .list = key_values })).hash;
                var found: ?usize = null;
                var candidate = group_index.head(hash);
                while (candidate) |position| {
                    const link = group_index.links.items[position];
                    if (try self.values.eql(.{ .list = keys.items[link.index] }, .{ .list = key_values })) {
                        found = link.index;
                        break;
                    }
                    candidate = link.next;
                }
                if (found) |i| try groups.items[i].append(self.a, row) else {
                    try group_index.insert(self.a, hash, keys.items.len);
                    try keys.append(self.a, key_values);
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
        var projected_index: value_work.Index = .{};
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
            for (projection.order, order_keys) |order_value, *key| key.* = try self.orderValue(order_value.expression, projection, out_row, scope, if (grouped) group.items else null);
            if (projection.distinct) {
                const hash = try self.rowHash(out_row);
                var candidate = projected_index.head(hash);
                var duplicate = false;
                while (candidate) |position| {
                    const link = projected_index.links.items[position];
                    if (try self.rowEqual(projected.items[link.index].row, out_row)) {
                        duplicate = true;
                        break;
                    }
                    candidate = link.next;
                }
                if (duplicate) continue;
                try projected_index.insert(self.a, hash, projected.items.len);
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
    fn rowHash(self: *Engine, row: Row) !u64 {
        // Columns are named; wildcard and explicit UNION arms can retain a
        // different physical binding order for the same logical row.
        return (try self.values.info(.{ .map = row })).hash;
    }
    fn rowEqual(self: *Engine, a: Row, b: Row) !bool {
        return self.values.eql(.{ .map = a }, .{ .map = b });
    }
    fn distinct(self: *Engine, rows: []const Row) !std.ArrayList(Row) {
        var output: std.ArrayList(Row) = .empty;
        var index: value_work.Index = .{};
        for (rows) |row| {
            try self.checkpoint();
            const hash = try self.rowHash(row);
            var candidate = index.head(hash);
            var duplicate = false;
            while (candidate) |position| {
                const link = index.links.items[position];
                if (try self.rowEqual(output.items[link.index], row)) {
                    duplicate = true;
                    break;
                }
                candidate = link.next;
            }
            if (!duplicate) {
                try index.insert(self.a, hash, output.items.len);
                try output.append(self.a, row);
            }
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
                        choose_left = items[l].sequence <= items[r].sequence;
                        for (orders, 0..) |order_value, i| {
                            const ordering = try self.values.order(items[l].keys[i], items[r].keys[i]);
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
        switch (mutation) {
            .create_node => |node| try self.values.properties(node.properties),
            .create_relationship => |edge| try self.values.properties(edge.properties),
            .set_properties => |update| try self.values.properties(update.properties),
            else => {},
        }
        try mutation.validateProperties();
        try mutate_fn(self.backend.ptr, self.a, mutation);
        if (self.backend.vtable.startup_work) |work| {
            const retained = work(self.backend.ptr);
            const anchors = retained.scanned_anchors -| self.startup_accounted.scanned_anchors;
            const edges = retained.explored_edges -| self.startup_accounted.explored_edges;
            if (anchors > self.limits.scanned_anchors -| self.stats.scanned_anchors) return error.CypherAnchorBudgetExceeded;
            if (edges > self.limits.explored_edges -| self.stats.explored_edges) return error.CypherEdgeBudgetExceeded;
            self.stats.scanned_anchors += anchors;
            self.stats.explored_edges += edges;
            self.startup_accounted = retained;
        }

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
        try self.values.traversal(.{ .map = properties });
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
                    for (pattern.labels) |name| if (!try self.graph.admits(node.*, name, &self.values)) return error.CypherLabelPredicateViolation;
                    row = (try self.bind(row, pattern.alias, .{ .node = node.* })).?;
                }
                for (path.edges, relationships, 0..) |pattern, *relationship, i| {
                    if (pattern.variable_length or pattern.types.len != 1 or pattern.direction == .both) return error.CypherInvalidCreateRelationship;
                    const mapping = self.graph.relationship(pattern.types[0]) orelse return error.UnknownGraphRelationship;
                    const source = nodes[if (pattern.direction == .out) i else i + 1];
                    const target = nodes[if (pattern.direction == .out) i + 1 else i];
                    if (!try self.graph.admits(source, mapping.source_label, &self.values) or !try self.graph.admits(target, mapping.target_label, &self.values)) return error.CypherRelationshipEndpointMismatch;
                    const rel = try self.a.create(v.Relationship);
                    const generated = try self.backend.vtable.new_identity.?(self.backend.ptr, self.a, source.identity.table, true);
                    const identity: v.Identity = .{ .table = source.identity.table, .key = try std.json.Stringify.valueAlloc(self.a, .{ source.identity.key, target.identity.key, mapping.stored_type, generated.key, "" }, .{}) };
                    rel.* = .{ .identity = identity, .index = mapping.index, .type = mapping.name, .source = source.identity, .target = target.identity, .properties = try v.entityProperties(self.a, try self.creationProperties(pattern.properties, row, merge)) };
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
        try chargeWork(self, 1);
        const position = try self.deletions.getOrPut(self.a, target);
        if (position.found_existing) return;
        position.value_ptr.* = .pending;
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
            if (try self.values.lookup(updates, entry.name) == null) try properties.append(self.a, entry);
        };
        for (updates) |entry| if (entry.value != .null) try properties.append(self.a, entry);
        if (base == .node) {
            const node = try self.a.create(v.Node);
            node.* = base.node.*;
            node.properties = try properties.toOwnedSlice(self.a);
            var labels: std.ArrayList([]const u8) = .empty;
            for (self.graph.labels) |label| if (try self.graph.admits(node, label.name, &self.values)) try labels.append(self.a, label.name);
            node.labels = try labels.toOwnedSlice(self.a);
            return .{ .node = node };
        }
        const relationship = try self.a.create(v.Relationship);
        relationship.* = base.relationship.*;
        relationship.properties = try properties.toOwnedSlice(self.a);
        return .{ .relationship = relationship };
    }
    fn assign(self: *Engine, rows: []const Row, assignments: []const ast.Assignment) ![]const Row {
        for (rows) |row| for (assignments) |assignment| {
            const base_expression = if (assignment.target.value == .property) assignment.target.value.property.base else assignment.target;
            const base = try self.mutationEntity(base_expression, row);
            if (base == .null) continue;
            if (base != .node and base != .relationship) return error.CypherTypeMismatch;
            try self.requireLiveContents(base);
            if (base == .relationship and base.relationship.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
            const value = try self.eval(assignment.value, row, null);
            const properties: []const v.Entry = if (assignment.target.value == .property) try self.a.dupe(v.Entry, &.{.{ .name = assignment.target.value.property.name, .value = value }}) else if (value == .map) value.map else return error.CypherTypeMismatch;
            const target = try v.EntityIdentity.fromValue(base);
            const replace_all = assignment.target.value != .property and !assignment.merge;
            try self.mutate(.{ .set_properties = .{ .target = target, .properties = properties, .replace = replace_all } });
            const updated: v.Value = if (base == .node) .{ .node = (try self.backend.vtable.get(self.backend.ptr, self.a, base.node.identity)) orelse return error.CypherDeletedEntity } else try self.updatedProperties(base, properties, replace_all);
            try self.postimages.put(self.a, target, updated);
        };
        return rows;
    }
    fn remove(self: *Engine, rows: []const Row, expressions: []const *const ast.Expr) ![]const Row {
        for (rows) |row| for (expressions) |expression| {
            if (expression.value != .property) return error.CypherRemoveRequiresProperty;
            const property = expression.value.property;
            const base = try self.mutationEntity(property.base, row);
            if (base == .null) continue;
            if (base != .node and base != .relationship) return error.CypherTypeMismatch;
            try self.requireLiveContents(base);
            if (base == .relationship and base.relationship.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
            const target = try v.EntityIdentity.fromValue(base);
            try self.mutate(.{ .remove_properties = .{ .target = target, .names = &.{property.name} } });
            const updated: v.Value = if (base == .node) .{ .node = (try self.backend.vtable.get(self.backend.ptr, self.a, base.node.identity)) orelse return error.CypherDeletedEntity } else try self.updatedProperties(base, &.{.{ .name = property.name, .value = .null }}, false);
            try self.postimages.put(self.a, target, updated);
        };
        return rows;
    }
};

test "weighted numeric reductions keep finite cancellation and checked integer bounds" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var state: u8 = 0;
    var engine: Engine = .{ .a = std.testing.allocator, .scratch_allocator = std.testing.allocator, .regex = undefined, .backend = .{ .ptr = &state, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    var operand: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .number = 1e308 } } };
    var reduction: Engine.StreamingAggregate = .{ .function = .{ .name = "sum", .args = &.{&operand} }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer reduction.arena.deinit();
    defer reduction.distinct_arena.deinit();
    reduction.sum = .{ .number = -1e308 };
    try reduction.addWeighted(&engine, &.{}, Count.init(2));
    try std.testing.expectEqual(@as(f64, 1e308), reduction.result().number);
    operand.value = .{ .literal = .{ .integer = std.math.maxInt(i64) } };
    reduction.sum = .{ .integer = -std.math.maxInt(i64) };
    try reduction.addWeighted(&engine, &.{}, Count.init(2));
    try std.testing.expectEqual(std.math.maxInt(i64), reduction.result().integer);
    reduction.sum = .{ .integer = 0 };
    try std.testing.expectError(error.CypherNumericOverflow, reduction.addWeighted(&engine, &.{}, Count.init(2)));
    operand.value = .{ .literal = .{ .number = 1e308 } };
    reduction.function.name = "avg";
    reduction.count = 1;
    reduction.mean = -1e308;
    reduction.mean_weight = 1;
    try reduction.addWeighted(&engine, &.{}, Count.init(2));
    try std.testing.expectApproxEqRel(@as(f64, 1e308 / 3.0), reduction.result().number, 1e-14);
    var counts = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer counts.deinit();
    const promoted = try Count.init(@as(u64, 1) << 63).multiply(counts.allocator(), Count.init(2));
    reduction.count = 0;
    reduction.mean_weight = 0;
    operand.value = .{ .literal = .{ .number = 9 } };
    try reduction.addWeighted(&engine, &.{}, promoted);
    operand.value = .{ .literal = .{ .number = 3 } };
    try reduction.addWeighted(&engine, &.{}, promoted);
    try std.testing.expectEqual(@as(f64, 6), reduction.result().number);
    try reduction.addWeighted(&engine, &.{}, Count.one);
    try std.testing.expectApproxEqRel(@as(f64, 6), reduction.result().number, 1e-14);
}

test "branch subset counting agrees with explicit distinct edge assignments" {
    const Oracle = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn count(sets: []const []const *const v.Relationship, depth: usize, used: *[4]*const v.Relationship, excluded: ?*const v.Relationship, repeatable: bool) u64 {
            if (depth == sets.len) return 1;
            var total: u64 = 0;
            for (sets[depth]) |edge| {
                if (!repeatable) {
                    if (excluded == edge) continue;
                    if (std.mem.indexOfScalar(*const v.Relationship, used[0..depth], edge) != null) continue;
                }
                used[depth] = edge;
                total += count(sets, depth + 1, used, excluded, repeatable);
            }
            return total;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = a, .scratch_allocator = a, .regex = undefined, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Oracle.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    var edges: [5]v.Relationship = undefined;
    for (&edges, 0..) |*edge, index| edge.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "{d}", .{index / 2}) }, .index = if (index % 2 == 0) "a" else "b", .type = "R", .source = .{ .table = "docs", .key = "s" }, .target = .{ .table = "docs", .key = "t" } };
    var seed: u64 = 42;
    for (0..96) |case| {
        const leaves = 1 + case % 4;
        var sets: [4][]const *const v.Relationship = undefined;
        for (sets[0..leaves]) |*set| {
            var items: std.ArrayList(*const v.Relationship) = .empty;
            for (&edges) |*edge| {
                seed = seed *% 6364136223846793005 +% 1442695040888963407;
                if (seed >> 63 != 0) try items.append(a, edge);
            }
            set.* = items.items;
        }
        const excluded: ?*const v.Relationship = if (case % 6 < 5) &edges[case % 6] else null;
        var mask: u16 = 0;
        for (sets[0..leaves], 0..) |set, leaf| for (set) |edge| {
            if (excluded == edge) mask |= @as(u16, 1) << @intCast(leaf);
        };
        for ([_]bool{ false, true }) |repeatable| {
            var used: [4]*const v.Relationship = undefined;
            const expected = Oracle.count(sets[0..leaves], 0, &used, excluded, repeatable);
            try std.testing.expectEqual(expected, try engine.branchAssignments(sets[0..leaves], if (repeatable) 0 else mask, repeatable));
            try std.testing.expectEqual(expected, (try engine.branchAssignmentsTyped(Count, sets[0..leaves], if (repeatable) 0 else mask, repeatable)).exact().?);
        }
    }
    var wide_edges: [60]v.Relationship = undefined;
    var choices: [60]*const v.Relationship = undefined;
    for (&wide_edges, &choices, 0..) |*edge, *choice, index| {
        edge.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "wide:{d}", .{index}) }, .index = "a", .type = "R", .source = .{ .table = "docs", .key = "s" }, .target = .{ .table = "docs", .key = "t" } };
        choice.* = edge;
    }
    const sets: [12][]const *const v.Relationship = @splat(&choices);
    for ([_]bool{ false, true }) |repeatable| {
        var expected: u128 = 1;
        for (0..sets.len) |index| expected *= if (repeatable) 60 else 60 - index;
        const result = try engine.branchAssignmentsTyped(Count, &sets, 0, repeatable);
        try std.testing.expect(result.exact() == null);
        try std.testing.expectEqual(@as(f128, @floatFromInt(expected)), result.wideNumber());
    }
}
