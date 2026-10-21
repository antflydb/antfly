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
const path_summaries = @import("path_summaries.zig");
const reduction_plan = @import("reduction.zig");
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
    statement_byte_pool: ?*v.BytePool = null,
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
        if (self.statement_byte_pool) |pool| pool.deinit();
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
    summary: ?[]const u8 = null,
    linear_summary: bool = false,
    total_axis: bool = false,
    statistic: ?[]const @import("statistic.zig").Request = null,
    statistic_global: bool = false,
    weighted_seeds: ?Engine.AnchorBatch = null,
    fallback_seed: ?usize = null,
    weighted_arena: std.heap.ArenaAllocator,
    adjacency_cache: Engine.AdjacencyCache,
    pending: std.ArrayList(struct { row: Row, weight: Count, statistic_input: ?@import("statistic.zig").Input = null }) = .empty,
    pending_position: usize = 0,
    /// Compact prefix counts; exact consumers must read last_counts.
    last_weights: std.ArrayList(u64) = .empty,
    last_counts: std.ArrayList(Count) = .empty,
    last_statistic_inputs: std.ArrayList(?@import("statistic.zig").Input) = .empty,
    pub const WeightPlan = struct { path: ast.Path, projection: ast.Projection, multiplicity: Multiplicity, summary: ?[]const u8 = null, linear: bool = false, total_axis: bool = false, statistic: ?[]const @import("statistic.zig").Request = null, statistic_global: bool = false };
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
        const demanded = try path_summaries.prune(self.engine.a, path, .{ projection, operation.predicate });
        const planned = try self.engine.reductionPath(demanded, &.{}, operation, projection, &.{});
        if (!try self.engine.batchEligible(planned, &.{}, operation, projection, &.{})) return null;
        return .{ .path = planned, .projection = projection, .multiplicity = multiplicity };
    }
    /// Certify a marginal-statistic consumer. Other columns, predicates and
    /// path dimensions must not observe the selected length distribution.
    pub fn planSummaryFields(self: *NodeCursor, fields: []const []const u8, multiplicity: Multiplicity, field: []const u8, invariants: []const []const u8) !?WeightPlan {
        return self.planSummaryFieldsJoint(fields, multiplicity, field, invariants, false);
    }
    /// Joint certificates permit several affine projections of the same private
    /// marginal. Invariant observers still cannot read that marginal.
    pub fn planSummaryFieldsJoint(self: *NodeCursor, fields: []const []const u8, multiplicity: Multiplicity, field: []const u8, invariants: []const []const u8, joint: bool) !?WeightPlan {
        return self.planSummaryFieldsChecked(fields, multiplicity, field, invariants, joint, null);
    }
    pub fn planSummaryFieldsChecked(self: *NodeCursor, fields: []const []const u8, multiplicity: Multiplicity, field: []const u8, invariants: []const []const u8, joint: bool, check: ?@import("linear_bounds.zig").Check) !?WeightPlan {
        // Endpoint integer columns also receive affine certificates. Reject
        // them before constructing a reduction plan: long fixed-hop patterns
        // otherwise pay for (and retain) the same geometry twice per reducer.
        const observed = for (self.query.clauses[1].operation.project.value.items) |item| {
            const name = item.alias orelse try self.engine.expressionName(item.expression);
            if (!std.mem.eql(u8, name, field)) continue;
            const reads_summary = reads: {
                for (self.original_match.paths) |path| {
                    if (path.execution_summary.length) |slot| if (path_summaries.references(item.expression, slot)) break :reads true;
                    for (path.edges) |edge| if (edge.execution_length) |slot| if (path_summaries.references(item.expression, slot)) break :reads true;
                }
                break :reads false;
            };
            if (reads_summary) break true;
        } else false;
        if (!observed) return null;
        var plan = (try self.planWeightFields(fields, multiplicity)) orelse return null;
        const targets = try self.engine.summaryTargets(plan.path);
        if (targets.len == 0 or (targets.len != 1 and check == null)) return null;
        if (check) |proof| if (proof.statistic != null) return self.planStatisticFields(plan, targets, proof, invariants);
        const target = targets[0].name;
        var names: Engine.MomentNames = .empty;
        for (targets, 0..) |part, index| {
            var form = Engine.Affine.column(index).bounded(.{ .low = 0, .high = std.math.cast(i64, self.engine.limits.path_depth) orelse std.math.maxInt(i64) });
            form.numeric = if (targets.len == 1) try Engine.Numeric.Form.column(self.engine.a, .{ .low = 0, .high = form.interval.?.high }, 1, null) else try self.engine.summaryNumeric(plan.path, part.edge, .{ .low = 0, .high = form.interval.?.high });
            try names.put(self.engine.a, part.name, .{ .scalar = form });
        }
        var found = false;
        var bounds: std.ArrayList(@import("linear_bounds.zig").Field) = .empty;
        var total_projections = true;
        for (plan.projection.items) |item| {
            const name = item.alias orelse try self.engine.expressionName(item.expression);
            const reads = Engine.referencesAny(item.expression, names);
            const form = try self.engine.momentArgument(item.expression, names);
            if (reads and (form == null or form.?.numeric == null)) total_projections = false;
            if (std.mem.eql(u8, name, field)) {
                const proof = form orelse return null;
                if (proof.field == null) return null;
                found = true;
            } else if (reads and (!joint or form == null or (targets.len == 1 and form.?.multiple))) return null;
            for (invariants) |invariant| if (std.mem.eql(u8, invariant, name) and reads) return null;
            try bounds.append(self.engine.a, .{ .name = name, .bounds = if (form) |proof| proof.interval else null, .numeric = if (form) |proof| proof.numeric else null });
        }
        if (!found or Engine.referencesAny(.{ plan.projection.predicate, self.original_match.predicate }, names)) return null;
        if (check) |proof| {
            // The existing single integer marginal preserves intermediate
            // errors by evaluating its actual extrema. Only new numeric or
            // multi-marginal transports need a totality obligation.
            const floating = for (bounds.items) |bound| {
                if (bound.numeric) |numeric| if (numeric.range.floating) break true;
            } else false;
            if ((targets.len > 1 or proof.numeric_required or floating) and !try proof.accepts(self.engine.a, bounds.items)) return null;
            plan.total_axis = proof.total_only and targets.len > 1;
            if (plan.total_axis and !total_projections) return null;
        }
        if (targets.len > 1) plan.linear = true;
        plan.summary = target;
        return plan;
    }
    fn planStatisticFields(self: *NodeCursor, original: WeightPlan, targets: []const Engine.SummaryTarget, check: @import("linear_bounds.zig").Check, invariants: []const []const u8) !?WeightPlan {
        var plan = original;
        const n = plan.path.edges.len;
        if (n == 0 or n > 32 or self.engine.component_leaves.len != 0) return null;
        var names: Engine.StatisticNames = .empty;
        const depth = std.math.cast(i64, self.engine.limits.path_depth) orelse return null;
        for (targets) |target| try names.put(self.engine.a, target.name, try Engine.Statistic.Form.column(self.engine.a, n, target.edge, depth));
        if (Engine.statisticReferences(.{ plan.projection.predicate, self.original_match.predicate }, names)) return null;
        var fields: std.ArrayList(@import("linear_bounds.zig").Field) = .empty;
        for (plan.projection.items) |item| {
            const name = item.alias orelse try self.engine.expressionName(item.expression);
            const form = try self.engine.statisticArgument(item.expression, names, n);
            const reads = Engine.statisticReferences(item.expression, names);
            if (reads and form == null) return null;
            for (invariants) |invariant| if (reads and std.mem.eql(u8, invariant, name)) return null;
            try fields.append(self.engine.a, .{ .name = name, .bounds = if (form) |f| f.range else null, .polynomial = form });
        }
        const form = (try check.statisticProof(self.engine.a, fields.items)) orelse return null;
        if (!form.eligible(check.statistic_kind)) return null;
        if (!Engine.statisticBenefit(form, check.statistic_kind, plan.path)) return null;
        plan.statistic = try self.engine.a.dupe(Engine.Statistic.Request, &.{.{ .form = form, .kind = check.statistic_kind }});
        plan.statistic_global = check.statistic_global and invariants.len == 0;
        return plan;
    }
    /// Combine certified consumers over the same oriented geometry. Replan
    /// their union of projections so pruning cannot remove another reducer's
    /// private length bindings.
    pub fn mergeStatisticPlans(self: *NodeCursor, left: WeightPlan, right: WeightPlan) !?WeightPlan {
        const l = left.statistic orelse return null;
        const r = right.statistic orelse return null;
        if (left.path.execution_reversed != right.path.execution_reversed or left.path.edges.len != right.path.edges.len) return null;
        var fields: std.ArrayList([]const u8) = .empty;
        for ([_]ast.Projection{ left.projection, right.projection }) |projection| for (projection.items) |item| {
            const name = item.alias orelse try self.engine.expressionName(item.expression);
            const present = for (fields.items) |field| {
                if (std.mem.eql(u8, field, name)) break true;
            } else false;
            if (!present) try fields.append(self.engine.a, name);
        };
        const multiplicity: Multiplicity = if (left.multiplicity == .exact or right.multiplicity == .exact) .exact else if (left.multiplicity == .capped or right.multiplicity == .capped) .capped else .presence;
        var merged = (try self.planWeightFields(fields.items, multiplicity)) orelse return null;
        if (merged.path.execution_reversed != left.path.execution_reversed or merged.path.edges.len != left.path.edges.len) return null;
        const requests = try self.engine.a.alloc(Engine.Statistic.Request, l.len + r.len);
        @memcpy(requests[0..l.len], l);
        @memcpy(requests[l.len..], r);
        merged.statistic = requests;
        merged.statistic_global = left.statistic_global and right.statistic_global;
        return merged;
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
        self.last_counts = .empty;
        self.last_statistic_inputs = .empty;
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
        self.summary = plan.summary;
        self.linear_summary = plan.linear;
        self.total_axis = plan.total_axis;
        self.statistic = plan.statistic;
        self.statistic_global = plan.statistic_global;
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
        e.external_summary = self.summary;
        e.external_linear_summary = self.linear_summary;
        e.external_total_axis = self.total_axis;
        e.external_statistic = self.statistic;
        e.external_statistic_global = self.statistic_global;
        e.stream_predicate = if (self.weighted_seeds != null and input.row.len == 0) self.weighted_seeds.?.residual else self.query.clauses[0].operation.match.predicate;
        defer {
            e.weighted_sink = null;
            e.external_summary = null;
            e.external_linear_summary = false;
            e.external_total_axis = false;
            e.external_statistic = null;
            e.external_statistic_global = false;
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
    fn appendWeighted(self: *NodeCursor, projected: Row, weight: Count, input: ?@import("statistic.zig").Input) !void {
        const a = self.weighted_arena.allocator();
        try self.pending.append(a, .{ .row = (try (v.Value{ .map = projected }).clone(a)).map, .weight = try weight.clone(a), .statistic_input = if (input) |argument| try argument.clone(a) else null });
    }
    fn emitWeighted(ptr: *anyopaque, row: Row, weight: Count) !void {
        const self: *NodeCursor = @ptrCast(@alignCast(ptr));
        const e = self.engine;
        for (try e.projectUnordered(&.{row}, self.query.clauses[1].operation.project.value, false)) |projected| {
            try e.values.output(.{ .map = projected });
            if (e.statistic_packet) |arguments| {
                try self.appendWeighted(projected, weight, .{ .batch = arguments });
            } else try self.appendWeighted(projected, weight, null);
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
        const copied_query = try std.json.parseFromSliceLeaky(ast.Query, a, try std.json.Stringify.valueAlloc(a, query, .{}), .{});
        const lowered_query = try @import("path_summaries.zig").lower(a, try reduction_plan.lowerCardinalityParameters(a, copied_query, parameters));
        const owned_query = try @import("length_constraints.zig").normalize(a, lowered_query, parameters);
        const owned_parameters = try a.alloc(v.Entry, parameters.len);
        for (parameters, owned_parameters) |parameter, *out| out.* = .{ .name = try a.dupe(u8, parameter.name), .value = try parameter.value.clone(a) };
        const engine = try a.create(Engine);
        engine.* = .{ .a = a, .planning_allocator = a, .scratch_allocator = budget.allocator(), .backend = backend, .graph = graph, .source = owned_query.source, .parameters = owned_parameters, .limits = limits, .read_only = true, .cache_reads = true, .regex = .{ .a = a, .scratch = std.heap.ArenaAllocator.init(budget.allocator()) } };
        var aliases: std.ArrayList([]const u8) = .empty;
        for (owned_query.clauses[0].operation.match.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| try aliases.append(a, alias);
            for (path.edges) |edge| if (edge.alias) |alias| try aliases.append(a, alias);
            if (path.alias) |alias| try aliases.append(a, alias);
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| try aliases.append(a, name);
            for (path.edges) |edge| if (edge.execution_length) |name| try aliases.append(a, name);
        }
        engine.match_aliases = try aliases.toOwnedSlice(a);
        engine.values = .{ .a = a, .ptr = engine, .charge = Engine.chargeWork };
        engine.strings = engine.newStringWork(a);
        errdefer engine.regex.deinit();
        errdefer if (engine.invariants) |state_value| state_value.deinit();
        errdefer if (engine.statement_byte_pool) |pool| pool.deinit();
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
        self.last_counts = .empty;
        self.last_statistic_inputs = .empty;
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
                try self.last_weights.append(a, pending.weight.cap(std.math.maxInt(u64)));
                try self.last_counts.append(a, try pending.weight.clone(a));
                try self.last_statistic_inputs.append(a, if (pending.statistic_input) |argument| try argument.clone(a) else null);
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
                    const row = (try e.bind(frame.state.row, path.alias, .{ .path = route })) orelse break :blk null;
                    break :blk try e.pathSummaryRow(row, path, frame.nodes[0], frame.nodes[frame.nodes.len - 1], frame.relationships.len);
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
                            if (self.weighted) {
                                try self.last_weights.append(a, 1);
                                try self.last_counts.append(a, Count.one);
                                try self.last_statistic_inputs.append(a, null);
                            }
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
                            if (try e.constrain(.{ .row = try e.lengthRow(edge_row, edge.execution_length, frame.hops), .used = frame.state.used, .constraints = frame.state.constraints }, .{ .node = current }, path.nodes[frame.edge_index + 1].properties)) |candidate| {
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
        if (self.engine.invariants) |state_value| state_value.deinit();
        if (self.engine.expression_memo) |*memo| memo.deinit();
        if (self.engine.statement_byte_pool) |pool| pool.deinit();
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
    engine.strings = engine.newStringWork(engine.a);
    defer engine.regex.deinit();
    defer if (engine.invariants) |state_value| state_value.deinit();
    defer if (engine.expression_memo) |*memo| memo.deinit();
    errdefer if (engine.statement_byte_pool) |pool| pool.deinit();
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
    const reductions = try reduction_plan.lowerCardinalityParameters(engine.a, compiled.query, parameters);
    const query = if (writes) reductions else try @import("length_constraints.zig").normalize(engine.a, try @import("path_summaries.zig").lower(engine.a, reductions), parameters);
    // Opaque projection callbacks may require complete trusted input content.
    // Writes retain full snapshots across all mutation/postimage boundaries.
    if (!writes and backend.vtable.project_node == null) try @import("entity_retention.zig").plan(engine.a, query.source, query, &engine.entity_retention);
    const rows = try engine.run(query.clauses, &.{&.{}}, compiled.query.explain, false);
    var combined: std.ArrayList(Row) = .empty;
    try combined.appendSlice(engine.a, rows);
    for (query.unions) |arm| {
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
    // Detach the ownership proof before admitting external source addresses to
    // the final clone memo. Nested policy reads must never mistake a borrowed
    // parameter/page address for a statement-owned allocation.
    const output_ownership = try engine.statementOwnership();
    engine.statement_ownership = null;
    const output = try engine.a.alloc([]const v.Value, combined.items.len);
    for (combined.items, output) |row, *out| {
        const cells = try engine.a.alloc(v.Value, engine.columns.len);
        for (engine.columns, cells) |name, *cell| {
            const value = try engine.readValue(lookup(row, name) orelse return error.UnboundCypherVariable);
            try engine.values.output(value);
            cell.* = try output_ownership.clone(value);
        }
        out.* = cells;
    }
    const columns = try engine.a.alloc([]const u8, engine.columns.len);
    for (engine.columns, columns) |name, *out| out.* = try engine.a.dupe(u8, name);
    const stages = try engine.stages.toOwnedSlice(engine.a);
    engine.stats.memory_bytes = budget.peak;
    var result: Result = .{ .arena = arena, .budget = budget, .columns = columns, .rows = output, .stages = stages, .stats = engine.stats, .read_only = !writes, .graph_incarnation = graph.incarnation, .graph_revision = graph.revision, .statement_byte_pool = engine.statement_byte_pool };
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

// Rows and ordering-key arrays are execution containers, not Cypher values.
// Clone each cell at its own depth so retaining a valid depth-64 value does
// not introduce an extra user-visible nesting level.
fn cloneValues(a: A, values: []const v.Value) ![]const v.Value {
    const result = try a.alloc(v.Value, values.len);
    for (values, result) |value, *out| out.* = try value.clone(a);
    return result;
}
fn cloneRow(a: A, row: Row) !Row {
    const result = try a.alloc(v.Entry, row.len);
    for (row, result) |entry, *out| out.* = .{ .name = try a.dupe(u8, entry.name), .value = try entry.value.clone(a) };
    return result;
}
/// Execution rows do not add a level to the user value depth budget.
fn cloneRowContext(row: Row, context: *v.CloneContext) !Row {
    const result = try context.a.alloc(v.Entry, row.len);
    for (row, result) |entry, *out| out.* = .{ .name = try context.a.dupe(u8, entry.name), .value = try context.clone(entry.value) };
    return result;
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
    const Invariants = struct {
        const MappingKey = struct { expression: *const ast.Expr, source: Identity };
        const Identity = struct { address: usize, length: usize };
        work: value_work.Work,
        parent: ?*Invariants = null,
        bytes: ?*v.BytePool = null,
        scalars: std.AutoHashMapUnmanaged(u64, v.Value) = .empty,
        strings: ?@import("string_work.zig").Runtime = null,
        lists: std.AutoHashMapUnmanaged(u64, @import("invariant_membership.zig").Membership) = .empty,
        producer_lists: std.AutoHashMapUnmanaged(u64, *@import("invariant_membership.zig").Producer) = .empty,
        membership_budget: ?*Budget = null,
        captured_lists: ?ProducerMemo = null,
        owned_lists: std.AutoHashMapUnmanaged(Identity, @import("invariant_membership.zig").Membership) = .empty,
        list_counts: std.AutoHashMapUnmanaged(Identity, *ListCountFacts) = .empty,
        mapped_lists: std.AutoHashMapUnmanaged(MappingKey, ?v.Value) = .empty,
        parameter_items: std.AutoHashMapUnmanaged(Identity, *ListType) = .empty,
        fn deinit(self: *@This()) void {
            if (self.bytes) |pool| pool.deinit();
            if (self.captured_lists) |*memo| memo.deinit();
            var producers = self.producer_lists.valueIterator();
            while (producers.next()) |producer| producer.*.deinit();
        }
    };

    a: A,
    scratch_allocator: A,
    planning_allocator: A = undefined,
    invariants: ?*Invariants = null,
    local_invariants: ?*Invariants = null,
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
    expression_memo: ?CallMemo = null,
    expression_memo_scoped: bool = false,
    statement_ownership: ?*v.CloneContext = null,
    statement_byte_pool: ?*v.BytePool = null,
    input_clones: ?*v.CloneContext = null,
    capture_input_clones: ?*v.CloneContext = null,
    producer_sources: std.ArrayList(ProducerSource) = .empty,
    moment_targets: []const SummaryTarget = &.{},
    external_summary: ?[]const u8 = null,
    external_linear_summary: bool = false,
    external_total_axis: bool = false,
    moment_total_axis: bool = false,
    statistic_plan: ?StatisticPlan = null,
    statistic_targets: []const SummaryTarget = &.{},
    external_statistic: ?[]const Statistic.Request = null,
    external_statistic_global: bool = false,
    statistic_packet: ?[]const Statistic.Argument = null,
    moment_input: Row = &.{},
    moment_endpoint: ?[]const u8 = null,
    moment_invariants: std.ArrayList(*const ast.Expr) = .empty,
    moment_representatives: bool = false,
    moment_calls: ?[]const ?DirectMoment = null,
    moment_checks: std.AutoHashMapUnmanaged(*const ast.Expr, Affine) = .empty,
    active_moments: ?struct { values: PathMoments, checks: std.AutoHashMapUnmanaged(*const ast.Expr, Affine) } = null,
    route_pruner: ?*RoutePruner = null,
    route_probe: ?struct { rejected: *usize, fold: *StreamFold } = null,
    numeric_consolidation: bool = false,
    numeric_attempted: bool = false,
    stream_predicate: ?*const ast.Expr = null,
    shortest_count: ?*u64 = null,
    count_endpoints: bool = false,
    component_proof: bool = false,
    component_leaves: []const ComponentLeaf = &.{},
    retain_windows: bool = false,
    match_weight_demand: ?Multiplicity = null,
    terminal_weight_demand: ?Multiplicity = null,
    summary_tail: ?*const SummaryTail = null,
    pending_window: ?WeightedRows = null,
    weighted_sink: ?struct { ptr: *anyopaque, emit: *const fn (*anyopaque, Row, Count) anyerror!void, multiplicity: Multiplicity } = null,
    lower_graph: ?*LowerGraph = null,
    anchor_batch: ?*AnchorBatch = null,
    adjacency_cache: ?*AdjacencyCache = null,
    interior: ?*const Interior = null,
    cache_reads: bool = false,
    read_only: bool = false,
    join: ?*const Join = null,
    lookups: []Lookup = &.{},
    lookup: ?*Lookup = null,
    distinct_sequence: usize = 0,
    distinct_input_token: ?usize = null,
    aggregate_overrides: ?AggregateOverrides = null,
    group_overrides: ?GroupOverrides = null,
    projection_overrides: ?struct { items: []const ast.Item, row: Row } = null,
    projection_locals: ?*const @import("expressions.zig").Local = null,
    read_nodes: std.HashMapUnmanaged(v.Identity, ?*const v.Node, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
    stages: std.ArrayList(Stage) = .empty,
    recursion: usize = 0,
    deletions: v.EntityMap(enum { pending, deleted }) = .empty,
    has_deletions: bool = false,
    // Retained aliases keep identity; resolve postimages on access rather than
    // walking every row after each mutation of a shared entity.
    entity_retention: @import("entity_retention.zig").Plans = .empty,
    postimages: v.EntityMap(v.Value) = .empty,
    values: value_work.Work = undefined,
    strings: @import("string_work.zig").Runtime = undefined,
    scalar_strings: ?*@import("string_work.zig").Runtime = null,
    const SummaryTail = struct { clauses: []const ast.Clause, parent: ?*const SummaryTail = null };
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
    const AggregateOverrides = struct {
        index: *const std.AutoHashMapUnmanaged(*const ast.Expr, usize),
        values: []v.Value,
        lazy: ?struct { ready: []bool, rows: []const Row, leaders: []const usize, inputs: []?[]const v.Value } = null,
        stream: ?struct { ready: []bool, states: []StreamingAggregate } = null,
    };
    const GroupOverrides = struct { index: *const std.AutoHashMapUnmanaged(*const ast.Expr, usize), values: []const v.Value, aliases: Row = &.{} };
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
    // Projection aliases shadow source bindings. Build this environment once;
    // repeated replace() retained every growing prefix in the statement arena.
    fn projectionScope(self: *Engine, source: Row, projected: Row) !Row {
        const output = try self.a.alloc(v.Entry, source.len + projected.len);
        @memcpy(output[0..source.len], source);
        var length = source.len;
        var positions: std.StringHashMapUnmanaged(usize) = .empty;
        defer positions.deinit(self.a);
        try positions.ensureTotalCapacity(self.a, @intCast(output.len));
        for (source, 0..) |entry, index| {
            const slot = positions.getOrPutAssumeCapacity(entry.name);
            if (!slot.found_existing) slot.value_ptr.* = index;
        }
        for (projected) |entry| {
            const slot = positions.getOrPutAssumeCapacity(entry.name);
            if (slot.found_existing) {
                output[slot.value_ptr.*].value = entry.value;
            } else {
                slot.value_ptr.* = length;
                output[length] = entry;
                length += 1;
            }
        }
        return output[0..length];
    }
    // Values and their collision-checked equality index are shared by compatible
    // DISTINCT inputs. Each consumer owns a compact ordinal bitmap: compressed
    // packets can activate only part of a cohort without stealing another
    // reducer's first contribution or collection order.
    const DistinctMembership = struct {
        arena: std.heap.ArenaAllocator,
        values: std.ArrayList(v.Value) = .empty,
        index: value_work.Index = .{},
        retain_payload: bool = true,
        bytes: ?*v.BytePool = null,
        last_token: ?usize = null,
        last_ordinal: usize = 0,
        last_created: bool = false,
        fn deinit(self: *@This()) void {
            if (self.bytes) |pool| pool.deinit();
            self.arena.deinit();
        }
        fn remember(self: *@This(), engine: *Engine, ordinal_value: usize, created: bool) usize {
            self.last_token = engine.distinct_input_token;
            self.last_ordinal = ordinal_value;
            self.last_created = created;
            return ordinal_value;
        }
        fn ordinal(self: *@This(), engine: *Engine, value: v.Value) !usize {
            if (engine.distinct_input_token) |token| if (self.last_token == token) return self.last_ordinal;
            const info = try engine.values.info(value);
            const hash = info.hash;
            var candidate = self.index.head(hash);
            while (candidate) |position| {
                const link = self.index.links.items[position];
                if (try engine.values.eql(self.values.items[link.index], value)) return self.remember(engine, link.index, false);
                candidate = link.next;
            }
            const a = self.arena.allocator();
            const owned = if (engine.read_only and engine.immutableOwner(value) != null) value else try v.cloneMembership(value, a, engine.scratch_allocator, &self.bytes, self.retain_payload);
            const position = self.values.items.len;
            try self.index.insert(a, hash, position);
            try self.values.append(a, owned);
            return self.remember(engine, position, true);
        }
    };
    const DistinctConsumer = struct {
        membership: *DistinctMembership,
        owner: bool = false,
        consumed: std.ArrayList(u64) = .empty,
    };
    const CollectionTail = struct {
        const Run = struct { arena: std.heap.ArenaAllocator, value: v.Value, weight: usize };
        slots: []Run = &.{},
        first: usize = 0,
        length: usize = 0,
        weight: usize = 0,
        fn deinit(self: *@This()) void {
            for (0..self.length) |i| self.slots[(self.first + i) % self.slots.len].arena.deinit();
        }
        fn append(self: *@This(), engine: *Engine, metadata: A, value: v.Value, multiplicity: Count, limit: usize) !void {
            const copies: usize = @intCast(multiplicity.cap(limit));
            if (copies == 0) return;
            var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            var owned = true;
            defer if (owned) arena.deinit();
            const copied = try engine.cloneScoped(value, arena.allocator());
            // A run represents repeated identical values without expanding a
            // promoted path count. Retire payload arenas as soon as evicted.
            const keep = limit - copies;
            while (self.weight > keep) {
                const front = &self.slots[self.first];
                const discarded = @min(front.weight, self.weight - keep);
                front.weight -= discarded;
                self.weight -= discarded;
                if (front.weight == 0) {
                    front.arena.deinit();
                    self.first = (self.first + 1) % self.slots.len;
                    self.length -= 1;
                }
            }
            if (self.length == self.slots.len) {
                const capacity = @min(limit, @max(@as(usize, 4), std.math.mul(usize, self.slots.len, 2) catch std.math.maxInt(usize)));
                const slots = try metadata.alloc(Run, capacity);
                for (0..self.length) |i| slots[i] = self.slots[(self.first + i) % self.slots.len];
                self.slots = slots;
                self.first = 0;
            }
            self.slots[(self.first + self.length) % self.slots.len] = .{ .arena = arena, .value = copied, .weight = copies };
            owned = false;
            self.length += 1;
            self.weight += copies;
        }
    };
    const LoopScope = struct {
        constants: ?std.heap.ArenaAllocator = null,
        owner: ?*Invariants = null,
        fn deinit(self: *@This()) void {
            if (self.owner) |owner| owner.deinit();
            if (self.constants) |*arena| arena.deinit();
        }
        fn enter(self: *@This(), engine: *Engine) !?*Invariants {
            const previous = engine.local_invariants;
            if (!engine.read_only and self.owner == null) {
                // Expressions cannot mutate. Closed computations can be shared
                // within this invocation, but never across mutation stages.
                self.constants = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                const allocator = self.constants.?.allocator();
                const owner = try allocator.create(Invariants);
                const parent = engine.scalarOwner();
                owner.* = .{ .parent = parent, .work = .{ .a = allocator, .ptr = engine, .charge = chargeWork, .parent = if (parent) |outer| &outer.work else null } };
                self.owner = owner;
            }
            if (self.owner) |owner| engine.local_invariants = owner;
            return previous;
        }
    };
    const FoldState = struct {
        initialized: bool = false,
        accumulator: v.Value = .null,
        owned: ?std.heap.ArenaAllocator = null,
        metadata: ?value_work.Work = null,
        invariants: LoopScope = .{},
        fn deinit(self: *@This()) void {
            if (self.owned) |*arena| arena.deinit();
            self.invariants.deinit();
        }
        fn own(self: *@This(), engine: *Engine, value: v.Value) !void {
            if (engine.immutableOwner(value)) |_| {
                // Statement inputs and invariant results outlive this fold.
                // Borrow them rather than making an identical new accumulator
                // allocation on every iteration and losing immutable provenance.
                if (self.owned) |*old| old.deinit();
                self.owned = null;
                self.metadata = null;
                self.accumulator = value;
                self.initialized = true;
                return;
            }
            const metadata = try engine.values.info(value);
            var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            errdefer arena.deinit();
            // Clone once with DAG sharing before releasing the old state.
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            var context: v.CloneContext = .{ .a = arena.allocator(), .scratch = scratch.allocator() };
            const copied = try context.clone(value);
            var owned_metadata: value_work.Work = .{ .a = arena.allocator(), .ptr = engine, .charge = chargeWork, .parent = if (engine.scalarOwner()) |owner| &owner.work else null };
            try owned_metadata.remember(copied, metadata);
            if (self.owned) |*old| old.deinit();
            self.owned = arena;
            self.accumulator = copied;
            self.initialized = true;
            self.metadata = owned_metadata;
            self.metadata.?.a = self.owned.?.allocator();
        }
        fn advance(self: *@This(), engine: *Engine, projection: *const ast.Expr, outer: Row, accumulator: []const u8, alias: []const u8, value: v.Value, weight: Count) !void {
            const previous = try self.invariants.enter(engine);
            defer engine.local_invariants = previous;
            const copies = std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded;
            for (0..copies) |_| {
                try engine.state();
                var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                defer scratch.deinit();
                const scope = EvalScope.enter(engine, scratch.allocator());
                defer scope.leave(engine);
                if (self.metadata) |*metadata| {
                    metadata.parent = if (engine.scalarOwner()) |owner| &owner.work else null;
                    engine.values.parent = metadata;
                }
                const row = try engine.projectionScope(outer, &.{ .{ .name = accumulator, .value = self.accumulator }, .{ .name = alias, .value = value } });
                try self.own(engine, try engine.eval(projection, row, null));
            }
        }
        fn escape(self: *@This(), engine: *Engine) !v.Value {
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            var context: v.CloneContext = .{ .a = engine.a, .scratch = scratch.allocator() };
            return context.clone(self.accumulator);
        }
    };
    /// Facts belong to the statement's immutable input owner. Distinct keys
    /// borrow that owner's values, and can be shared by any number of groups.
    const ListCountFacts = struct {
        input: []const v.Value,
        non_null: u64,
        distinct: ?[]const v.Value = null,
        minimum: ?v.Value = null,
        maximum: ?v.Value = null,
        sum_checked: bool = false,
        integer_sum: ?IntegerListSum = null,
        distinct_sum_checked: bool = false,
        distinct_integer_sum: ?IntegerListSum = null,
    };
    const IntegerListSum = struct {
        total: i128 = 0,
        low: i128 = 0,
        high: i128 = 0,
        positive: bool = false,
        negative: bool = false,
        fn concat(left: @This(), right: @This()) ?@This() {
            return .{
                .total = std.math.add(i128, left.total, right.total) catch return null,
                .low = @min(left.low, std.math.add(i128, left.total, right.low) catch return null),
                .high = @max(left.high, std.math.add(i128, left.total, right.high) catch return null),
                .positive = left.positive or right.positive,
                .negative = left.negative or right.negative,
            };
        }
        fn reversed(self: @This()) ?@This() {
            var result = self;
            result.low = std.math.sub(i128, self.total, self.high) catch return null;
            result.high = std.math.sub(i128, self.total, self.low) catch return null;
            return result;
        }
        fn range(sequence: @import("range.zig").Range) @This() {
            if (sequence.length == 0) return .{};
            const n: i128 = @intCast(sequence.length);
            const Prefix = struct {
                fn at(r: @import("range.zig").Range, k: i128) i128 {
                    return @intCast(@divTrunc(@as(i256, k) * (2 * @as(i256, r.start) + (k - 1) * @as(i256, r.step)), 2));
                }
            };
            const total = Prefix.at(sequence, n);
            var result: @This() = .{ .total = total, .low = @min(0, total), .high = @max(0, total) };
            // Prefix extrema occur at an endpoint or where contributions cross zero.
            const crossing = @divFloor(-@as(i128, sequence.start), @as(i128, sequence.step));
            for ([_]i128{ crossing, crossing + 1, crossing + 2 }) |candidate| {
                const value = Prefix.at(sequence, std.math.clamp(candidate, 0, n));
                result.low = @min(result.low, value);
                result.high = @max(result.high, value);
            }
            const last = sequence.at(sequence.length - 1).integer;
            result.positive = @max(sequence.start, last) > 0;
            result.negative = @min(sequence.start, last) < 0;
            return result;
        }
    };
    fn integerSum(self: *Engine, input: []const v.Value) !?IntegerListSum {
        var summary: IntegerListSum = .{};
        for (input) |value| {
            try self.state();
            if (value == .null) continue;
            if (value != .integer) return null;
            summary.total = std.math.add(i128, summary.total, value.integer) catch return null;
            summary.low = @min(summary.low, summary.total);
            summary.high = @max(summary.high, summary.total);
            summary.positive = summary.positive or value.integer > 0;
            summary.negative = summary.negative or value.integer < 0;
        }
        return summary;
    }
    fn listIntegerSum(self: *Engine, facts: *ListCountFacts, is_distinct: bool) !?IntegerListSum {
        const checked = if (is_distinct) &facts.distinct_sum_checked else &facts.sum_checked;
        const cached = if (is_distinct) &facts.distinct_integer_sum else &facts.integer_sum;
        if (checked.*) return cached.*;
        if (is_distinct) try self.listDistinctFacts(facts);
        cached.* = try self.integerSum(if (is_distinct) facts.distinct.? else facts.input);
        checked.* = true;
        return cached.*;
    }
    /// Only mappings closed over the observed axis and parameters qualify.
    /// Failed scalar probes decline: the ordinary projection then reports the
    /// error in its original row/item order, after any preceding sibling error.
    fn mappedList(self: *Engine, input: v.Value, expression: *const ast.Expr, alias: []const u8) !?v.Value {
        const rules = @import("expressions.zig");
        if (!rules.rowLocal(expression, struct {
            alias: []const u8,
            pub fn permits(context: @This(), name: []const u8) bool {
                return std.mem.eql(u8, context.alias, name);
            }
        }{ .alias = alias })) return null;
        _ = try self.listCountFacts(input) orelse return null;
        const owner = self.invariants.?;
        const key: Invariants.MappingKey = .{ .expression = expression, .source = Invariants.Identity{ .address = @intFromPtr(input.list.ptr), .length = input.list.len } };
        if (owner.mapped_lists.get(key)) |cached| return cached;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const mapped = try scratch.allocator().alloc(v.Value, input.list.len);
        for (input.list, mapped) |item, *output| {
            const scope = EvalScope.enter(self, scratch.allocator());
            defer scope.leave(self);
            try self.state();
            output.* = self.eval(expression, &.{.{ .name = alias, .value = item }}, null) catch |err| {
                if (!StreamingAggregate.consumerFailure(err)) return err;
                try owner.mapped_lists.put(self.planning_allocator, key, null);
                return null;
            };
        }
        const result = self.cloneScoped(.{ .list = mapped }, self.planning_allocator) catch |err| {
            // The synthetic domain adds a list level that the scalar projection
            // does not expose. Decline if that wrapper exceeds value depth.
            if (!StreamingAggregate.consumerFailure(err)) return err;
            try owner.mapped_lists.put(self.planning_allocator, key, null);
            return null;
        };
        _ = owner.work.info(result) catch |err| {
            if (!StreamingAggregate.consumerFailure(err)) return err;
            try owner.mapped_lists.put(self.planning_allocator, key, null);
            return null;
        };
        try owner.mapped_lists.put(self.planning_allocator, key, result);
        return result;
    }
    fn listCountFacts(self: *Engine, value: v.Value) !?*ListCountFacts {
        if (!self.read_only or value != .list) return null;
        const owner = self.immutableOwner(value) orelse return null;
        // Aggregate domains can outlive an element/loop scope. Only the root
        // immutable owner licenses borrowed support keys across those scopes.
        if (owner != self.invariants) return null;
        // Reading an entity can apply authorization or rehydrate its visible
        // value. Raw input cardinalities cannot replace those reads.
        if ((try owner.work.info(value)).entities) return null;
        const identity: Invariants.Identity = .{ .address = @intFromPtr(value.list.ptr), .length = value.list.len };
        if (owner.list_counts.get(identity)) |facts| return facts;
        try chargeWork(self, value.list.len);
        var non_null: u64 = 0;
        for (value.list) |item| non_null += @intFromBool(item != .null);
        const facts = try self.planning_allocator.create(ListCountFacts);
        facts.* = .{ .input = value.list, .non_null = non_null };
        try owner.list_counts.put(self.planning_allocator, identity, facts);
        return facts;
    }
    fn listExtreme(self: *Engine, facts: *ListCountFacts, minimum: bool) !v.Value {
        const cached = if (minimum) &facts.minimum else &facts.maximum;
        if (cached.*) |value| return value;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = self, .charge = chargeWork };
        var best: v.Value = .null;
        for (facts.input) |value| {
            try self.state();
            if (value == .null) continue;
            if (best == .null) {
                best = value;
                continue;
            }
            const order = try work.order(value, best);
            if (order == (if (minimum) std.math.Order.lt else .gt)) best = value;
        }
        // Inputs are owned by the immutable root; ties retain the first
        // representation, exactly as the ordinary MIN/MAX reducer does.
        cached.* = best;
        return best;
    }
    fn listDistinctFacts(self: *Engine, facts: *ListCountFacts) !void {
        if (facts.distinct != null) return;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        var work: value_work.Work = .{ .a = a, .ptr = self, .charge = chargeWork, .parent = &self.invariants.?.work };
        const mutable = self.scratch_allocator;
        var index: value_work.Index = .{};
        defer index.deinit(mutable);
        var keys: std.ArrayList(v.Value) = .empty;
        defer keys.deinit(mutable);
        for (facts.input) |value| {
            if (value == .null) continue;
            const hash = (try work.info(value)).hash;
            var candidate = index.head(hash);
            var found = false;
            while (candidate) |position| {
                const link = index.links.items[position];
                if (try work.eql(keys.items[link.index], value)) {
                    found = true;
                    break;
                }
                candidate = link.next;
            }
            if (found) continue;
            try index.insert(mutable, hash, keys.items.len);
            try keys.append(mutable, value);
        }
        facts.distinct = try self.planning_allocator.dupe(v.Value, keys.items);
    }
    const StreamingAggregate = struct {
        function: ast.Function,
        consumer_row: Row = &.{},
        source_null: bool = false,
        failure: ?anyerror = null,
        released: bool = false,
        arena: std.heap.ArenaAllocator,
        distinct_arena: std.heap.ArenaAllocator,
        distinct_values: std.ArrayList(v.Value) = .empty,
        distinct_bytes: ?*v.BytePool = null,
        distinct_index: value_work.Index = .{},
        distinct_support: ?*Statistic.SupportSet = null,
        list_domain: ?*const ListCountFacts = null,
        list_domains: std.AutoHashMapUnmanaged(*const ListCountFacts, void) = .empty,
        cohort: ?*DistinctConsumer = null,
        count: u64 = 0,
        sum: v.Value = .{ .integer = 0 },
        sum_sign: enum { zero, positive, negative } = .zero,
        mean: f64 = 0,
        mean_weight: f64 = 0,
        // Only promoted AVG counts need this state; ordinary aggregate groups
        // retain the inline u64/f64 path and avoid a binary128-sized field.
        mean_scaled: ?*Count.Scaled = null,
        best: v.Value = .null,
        collected: std.ArrayList(v.Value) = .empty,
        tail: CollectionTail = .{},
        collection_seen: usize = 0,
        filter_states: []FilterState = &.{},
        collection_count: u64 = 0,
        collection_overflow: bool = false,
        const FilterState = struct {
            count: u64 = 0,
            failure: ?anyerror = null,
            unknown: bool = false,
            decided: ?bool = null,
            window: ?*StreamingAggregate = null,
            embedding_failure: bool = false,
            fold: FoldState = .{},
            fn deinit(self: *@This()) void {
                if (self.window) |window| window.deinit();
                self.fold.deinit();
            }
            fn result(self: @This(), kind: ast.CollectionFilter.Kind) v.Value {
                if (kind == .count) return .{ .integer = @intCast(self.count) };
                if (self.decided) |answer| return .{ .boolean = answer };
                if (self.unknown) return .null;
                return .{ .boolean = switch (kind) {
                    .all, .none => true,
                    .any => false,
                    .single => self.count == 1,
                    .count, .window, .fold => unreachable,
                } };
            }
        };
        fn filterItem(engine: *Engine, filter: ast.CollectionFilter, value: v.Value, outer: Row, state_value: *FilterState) !?bool {
            const previous = engine.projection_locals;
            const local: @import("expressions.zig").Local = .{ .name = filter.alias, .parent = previous };
            engine.projection_locals = &local;
            defer engine.projection_locals = previous;
            const row = try engine.replace(outer, filter.alias, value);
            if (filter.kind != .count) return (try engine.eval(filter.predicate.?, row, null)).truth();
            if (filter.predicate) |predicate| if ((try (try engine.eval(predicate, row, null)).truth()) != true) return false;
            // SIZE still observes projection errors and counts NULL projections.
            if (filter.projection) |projection| {
                const projected = try engine.eval(projection, row, null);
                _ = engine.values.info(.{ .list = &.{projected} }) catch |err| blk: {
                    if (err != error.CypherValueDepthExceeded) return err;
                    state_value.embedding_failure = true;
                    break :blk null;
                };
            }
            return true;
        }
        fn ensureFilters(self: *@This()) !void {
            if (self.filter_states.len != 0 or self.function.collect_filters.len == 0) return;
            self.filter_states = try self.arena.allocator().alloc(FilterState, self.function.collect_filters.len);
            @memset(self.filter_states, .{});
        }
        fn consumerFailure(err: anyerror) bool {
            return switch (err) {
                error.CypherTypeMismatch, error.CypherNumericOverflow, error.CypherDivisionByZero, error.MissingCypherParameter, error.UnboundCypherVariable, error.CypherInvalidArgument, error.CypherInvalidRange, error.CypherInvalidSubstring, error.CypherInvalidRegex, error.CypherRegexUnavailable, error.CypherValueDepthExceeded, error.CypherDeletedEntity => true,
                else => false,
            };
        }
        fn initFold(self: *@This(), engine: *Engine, filter: ast.CollectionFilter, state_value: *FilterState) !void {
            if (!state_value.fold.initialized) try state_value.fold.own(engine, try engine.eval(filter.initial.?, self.consumer_row, null));
        }
        fn addConsumer(self: *@This(), engine: *Engine, filter: ast.CollectionFilter, state_value: *FilterState, value: v.Value, weight: Count) !void {
            const previous = engine.projection_locals;
            const local: @import("expressions.zig").Local = .{ .name = filter.alias, .parent = previous };
            engine.projection_locals = &local;
            defer engine.projection_locals = previous;
            if (filter.kind == .fold) {
                try self.initFold(engine, filter, state_value);
                const accumulator_local: @import("expressions.zig").Local = .{ .name = filter.accumulator.?, .parent = &local };
                engine.projection_locals = &accumulator_local;
                try state_value.fold.advance(engine, filter.projection.?, self.consumer_row, filter.accumulator.?, filter.alias, value, weight);
                return;
            }
            const row = try engine.replace(self.consumer_row, filter.alias, value);
            try engine.state();
            if (filter.predicate) |predicate| if ((try (try engine.eval(predicate, row, null)).truth()) != true) return;
            const projected = if (filter.projection) |projection| try engine.eval(projection, row, null) else value;
            // The original comprehension validates its full list, even when a
            // bounded consumer later discards this particular projection.
            _ = engine.values.info(.{ .list = &.{projected} }) catch |err| blk: {
                if (err != error.CypherValueDepthExceeded) return err;
                // List depth is checked after the original comprehension has
                // evaluated every projection. A later scalar failure wins.
                state_value.embedding_failure = true;
                break :blk null;
            };
            if (state_value.window == null) {
                const window = try self.arena.allocator().create(StreamingAggregate);
                window.* = .{ .function = .{ .name = "collect", .args = &.{}, .collect_limit = filter.prefix, .collect_suffix = filter.suffix }, .arena = std.heap.ArenaAllocator.init(engine.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(engine.scratch_allocator) };
                state_value.window = window;
            }
            // Unlike COLLECT, a comprehension retains NULL projections.
            try state_value.window.?.retainCollection(engine, projected, weight, null);
        }
        fn addSummary(self: *@This(), engine: *Engine, value: v.Value, weight: Count) !void {
            if (weight.cap(1) == 0) return;
            if (!self.collection_overflow) self.collection_count = addPathCount(self.collection_count, weight.cap(std.math.maxInt(u64))) catch blk: {
                self.collection_overflow = true;
                break :blk 0;
            };
            if (self.function.collect_filters.len == 0) return;
            try self.ensureFilters();
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            for (self.function.collect_filters, self.filter_states) |filter, *state_value| {
                if (state_value.failure != null or state_value.decided != null) continue;
                _ = scratch.reset(.free_all);
                const scope = EvalScope.enter(engine, scratch.allocator());
                defer scope.leave(engine);
                if (filter.kind == .window or filter.kind == .fold) {
                    self.addConsumer(engine, filter, state_value, value, weight) catch |err| {
                        if (!consumerFailure(err)) return err;
                        state_value.failure = err;
                    };
                    continue;
                }
                const selected = filterItem(engine, filter, value, self.consumer_row, state_value) catch |err| {
                    if (!consumerFailure(err)) return err;
                    state_value.failure = err;
                    continue;
                };
                if (filter.kind == .count) {
                    if (selected == true) state_value.count = addPathCount(state_value.count, weight.cap(std.math.maxInt(u64))) catch |err| blk: {
                        state_value.failure = err;
                        break :blk 0;
                    };
                } else if (selected) |answer| {
                    if (answer) {
                        state_value.count = @min(2, state_value.count + weight.cap(2));
                        switch (filter.kind) {
                            .any => state_value.decided = true,
                            .none => state_value.decided = false,
                            .single => if (state_value.count > 1) {
                                state_value.decided = false;
                            },
                            else => {},
                        }
                    } else if (filter.kind == .all) state_value.decided = false;
                } else state_value.unknown = true;
            }
        }
        fn consumedOrdinal(self: *const @This(), ordinal: usize) bool {
            const word = ordinal / 64;
            return word < self.cohort.?.consumed.items.len and self.cohort.?.consumed.items[word] & (@as(u64, 1) << @intCast(ordinal % 64)) != 0;
        }
        fn consumeOrdinal(self: *@This(), ordinal: usize) !bool {
            const word = ordinal / 64;
            if (self.consumedOrdinal(ordinal)) return false;
            if (word >= self.cohort.?.consumed.items.len) {
                const old = self.cohort.?.consumed.items.len;
                try self.cohort.?.consumed.resize(self.distinct_arena.allocator(), word + 1);
                @memset(self.cohort.?.consumed.items[old..], 0);
            }
            self.cohort.?.consumed.items[word] |= @as(u64, 1) << @intCast(ordinal % 64);
            return true;
        }
        fn deinit(self: *@This()) void {
            if (self.released) return;
            self.released = true;
            self.tail.deinit();
            for (self.filter_states) |*state_value| state_value.deinit();
            self.arena.deinit();
            if (self.distinct_bytes) |pool| pool.deinit();
            self.distinct_arena.deinit();
            if (self.cohort) |cohort| if (cohort.owner) cohort.membership.deinit();
        }
        fn add(self: *StreamingAggregate, engine: *Engine, row: Row) !void {
            return self.addWeighted(engine, row, Count.one);
        }
        fn supportInteger(value: v.Value) ?i64 {
            return switch (value) {
                .integer => |n| n,
                .number => |n| if (std.math.isFinite(n) and @trunc(n) == n and n >= -0x1p63 and n < 0x1p63) @intFromFloat(n) else null,
                else => null,
            };
        }
        fn addSupport(self: *StreamingAggregate, engine: *Engine, support: StatisticSupport) !void {
            try self.materializeListDomain(engine);
            const a = self.distinct_arena.allocator();
            if (self.distinct_support == null) {
                const set = try a.create(Statistic.SupportSet);
                set.* = .{};
                // A previous anchor may have used ordinary enumeration. Import
                // its integral keys without counting those values twice.
                try Engine.chargeWork(engine, if (self.cohort) |cohort| cohort.membership.values.items.len else self.distinct_values.items.len);
                if (self.cohort) |cohort| {
                    for (cohort.membership.values.items, 0..) |value, ordinal| if (self.consumedOrdinal(ordinal)) {
                        if (supportInteger(value)) |integer| _ = try set.insert(a, integer);
                    };
                } else for (self.distinct_values.items) |value| if (supportInteger(value)) |integer| {
                    _ = try set.insert(a, integer);
                };
                self.distinct_support = set;
            }
            try Engine.chargeWork(engine, support.value.words.len * 2);
            self.count = try addPathCount(self.count, try self.distinct_support.?.add(a, support.value, support.offset));
        }
        fn materializeListDomain(self: *StreamingAggregate, engine: *Engine) anyerror!void {
            const domain = self.list_domain orelse return;
            try self.list_domains.put(self.distinct_arena.allocator(), domain, {});
            self.list_domain = null;
            self.count = 0;
            if (std.ascii.eqlIgnoreCase(self.function.name, "collect")) {
                self.collected.clearRetainingCapacity();
                self.tail.deinit();
                self.tail = .{};
                self.collection_seen = 0;
                self.collection_count = 0;
                self.collection_overflow = false;
                for (self.filter_states) |*consumer| consumer.deinit();
                @memset(self.filter_states, .{});
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "sum")) self.sum = .{ .integer = 0 };
            if (std.ascii.eqlIgnoreCase(self.function.name, "avg")) {
                self.mean = 0;
                self.mean_weight = 0;
                self.mean_scaled = null;
            }
            const token = engine.distinct_input_token;
            engine.distinct_input_token = null;
            defer engine.distinct_input_token = token;
            for (domain.distinct.?) |value| try self.addValue(engine, value, Count.one);
        }
        fn addListCount(self: *StreamingAggregate, engine: *Engine, facts: *ListCountFacts, weight: Count) anyerror!void {
            if (std.ascii.eqlIgnoreCase(self.function.name, "sum")) return self.addIntegerListSum(engine, facts, weight);
            if (std.ascii.eqlIgnoreCase(self.function.name, "min") or std.ascii.eqlIgnoreCase(self.function.name, "max"))
                return self.addValue(engine, try engine.listExtreme(facts, std.ascii.eqlIgnoreCase(self.function.name, "min")), Count.one);
            if (!self.function.distinct) return self.addValue(engine, .{ .integer = 1 }, try weight.multiply(engine.a, Count.init(facts.non_null)));
            try engine.listDistinctFacts(facts);
            if (self.list_domain == facts or self.list_domains.contains(facts)) return;
            if (self.list_domain == null and self.count == 0) {
                self.list_domain = facts;
                self.count = @intCast(facts.distinct.?.len);
                return;
            }
            // Different input lists may overlap. Promote the shared first
            // domain once, then merge each additional domain exactly once.
            try self.materializeListDomain(engine);
            try self.list_domains.put(self.distinct_arena.allocator(), facts, {});
            const token = engine.distinct_input_token;
            engine.distinct_input_token = null;
            defer engine.distinct_input_token = token;
            for (facts.distinct.?) |value| try self.addValue(engine, value, Count.one);
        }
        fn addIntegerListSum(self: *StreamingAggregate, engine: *Engine, facts: *ListCountFacts, weight: Count) anyerror!void {
            if (self.function.distinct) {
                if (self.list_domain == facts or self.list_domains.contains(facts)) return;
                if (self.list_domain == null and self.count == 0 and self.distinct_values.items.len == 0 and self.distinct_support == null and self.list_domains.count() == 0 and self.sum == .integer) {
                    try self.addIntegerSummary(engine, facts.distinct_integer_sum.?, Count.one, @intCast(facts.distinct.?.len));
                    self.list_domain = facts;
                    return;
                }
                try self.materializeListDomain(engine);
                // The summary is per ordered support. A different domain can
                // overlap existing keys; merge normally before marking it seen.
                const token = engine.distinct_input_token;
                engine.distinct_input_token = null;
                defer engine.distinct_input_token = token;
                for (facts.distinct.?) |value| try self.addValue(engine, value, Count.one);
                try self.list_domains.put(self.distinct_arena.allocator(), facts, {});
                return;
            }
            if (self.sum != .integer) {
                // A previous domain may have introduced floating arithmetic.
                // Preserve its exact contribution order and rounding.
                for (facts.input) |value| {
                    try engine.state();
                    try self.addValue(engine, value, weight);
                }
                return;
            }
            return self.addIntegerSummary(engine, facts.integer_sum.?, weight, facts.non_null);
        }
        fn addIntegerSummary(self: *StreamingAggregate, engine: *Engine, summary: IntegerListSum, weight: Count, non_null: u64) !void {
            if (engine.numeric_consolidation) {
                if ((summary.positive and summary.negative) or (summary.positive and self.sum_sign == .negative) or (summary.negative and self.sum_sign == .positive)) return error.CypherNumericRegroupingDeclined;
                if (summary.positive) self.sum_sign = .positive;
                if (summary.negative) self.sum_sign = .negative;
            }
            const multiplier = weight.exact() orelse {
                if (summary.low != 0 or summary.high != 0) return error.CypherNumericOverflow;
                self.count +|= if (non_null == 0) 0 else std.math.maxInt(u64);
                return;
            };
            const base: i256 = self.sum.integer;
            const low = base + @as(i256, summary.low) * multiplier;
            const high = base + @as(i256, summary.high) * multiplier;
            if (low < std.math.minInt(i64) or high > std.math.maxInt(i64)) return error.CypherNumericOverflow;
            self.sum = .{ .integer = @intCast(base + @as(i256, summary.total) * multiplier) };
            self.count +|= (try weight.multiply(engine.a, Count.init(non_null))).cap(std.math.maxInt(u64));
        }
        fn addStatisticBin(self: *StreamingAggregate, engine: *Engine, row: Row, value: i64, weight: Count) !void {
            const literal: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .integer = value } } };
            const saved = self.function;
            self.function.args = &.{&literal};
            defer self.function = saved;
            try self.addWeighted(engine, row, weight);
        }
        fn addWeighted(self: *StreamingAggregate, engine: *Engine, row: Row, multiplicity: Count) !void {
            const value = if (self.function.star) v.Value{ .integer = 1 } else try engine.eval(self.function.args[0], row, null);
            return self.addValue(engine, value, multiplicity);
        }
        fn addValue(self: *StreamingAggregate, engine: *Engine, value: v.Value, multiplicity: Count) anyerror!void {
            if (value == .null) return;
            try self.materializeListDomain(engine);
            // Retention is bounded, evaluation is not: every argument above
            // still runs, even after this consumer has received its prefix.
            if (!self.function.collect_summary and self.function.collect_suffix == 0) if (self.function.collect_limit) |limit| if (self.collected.items.len >= limit) return;
            // MIN/MAX are duplicate insensitive and need no distinct set.
            // Other DISTINCT reducers retain only unique arguments, using the
            // same equality and hashing as GROUP BY.
            var distinct_owned: ?v.Value = null;
            if (self.function.distinct and !std.ascii.eqlIgnoreCase(self.function.name, "min") and !std.ascii.eqlIgnoreCase(self.function.name, "max")) {
                const integer = supportInteger(value);
                if (self.distinct_support != null and integer != null) {
                    if (!try self.distinct_support.?.insert(self.distinct_arena.allocator(), integer.?)) return;
                } else if (self.cohort) |cohort| {
                    const membership = cohort.membership;
                    const ordinal = try membership.ordinal(engine, value);
                    if (!try self.consumeOrdinal(ordinal)) return;
                    distinct_owned = membership.values.items[ordinal];
                    // Equality canonicalizes numeric representations. A consumer
                    // activated later must keep its own first representation.
                    if (std.ascii.eqlIgnoreCase(self.function.name, "collect") and self.function.collect_suffix == 0 and !self.function.collect_summary) {
                        if (!membership.retain_payload) {
                            distinct_owned = try value.clone(self.arena.allocator());
                        } else {
                            const canonical = distinct_owned.?;
                            // A value first cloned during this emission came from
                            // this very argument, including its nested representation.
                            const fresh = membership.last_created and engine.distinct_input_token != null and membership.last_token == engine.distinct_input_token;
                            const same_representation = if (fresh) true else if (std.meta.activeTag(value) != std.meta.activeTag(canonical)) false else switch (value) {
                                // Equality has already proved their exact value.
                                .null, .boolean, .integer, .string => true,
                                .number => @as(u64, @bitCast(value.number)) == @as(u64, @bitCast(canonical.number)),
                                else => blk: {
                                    var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                                    defer scratch.deinit();
                                    var identity: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork, .binding_identity = true };
                                    break :blk try identity.eql(value, canonical);
                                },
                            };
                            if (!same_representation) distinct_owned = try value.clone(self.arena.allocator());
                        }
                    }
                } else {
                    const info = if (engine.immutableOwner(value)) |owner| try owner.work.info(value) else try engine.values.info(value);
                    const hash = info.hash;
                    var candidate = self.distinct_index.head(hash);
                    while (candidate) |position| {
                        const link = self.distinct_index.links.items[position];
                        if (try engine.values.eql(self.distinct_values.items[link.index], value)) return;
                        candidate = link.next;
                    }
                    const retained = self.distinct_arena.allocator();
                    const owned = if (engine.read_only and engine.immutableOwner(value) != null) value else try v.cloneMembership(value, retained, engine.scratch_allocator, &self.distinct_bytes, std.ascii.eqlIgnoreCase(self.function.name, "collect") and self.function.collect_suffix == 0 and !self.function.collect_summary);
                    try self.distinct_index.insert(retained, hash, self.distinct_values.items.len);
                    try self.distinct_values.append(retained, owned);
                }
            }
            const weight = if (self.function.distinct or std.ascii.eqlIgnoreCase(self.function.name, "min") or std.ascii.eqlIgnoreCase(self.function.name, "max")) Count.one else multiplicity;
            const prior_count = self.count;
            self.count +|= weight.cap(std.math.maxInt(u64));
            if (std.ascii.eqlIgnoreCase(self.function.name, "count")) {
                self.count = try addPathCount(prior_count, weight.cap(std.math.maxInt(u64)));
                return;
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "collect")) {
                if (self.function.collect_summary) try self.addSummary(engine, value, weight);
                try self.retainCollection(engine, value, weight, distinct_owned);
                return;
            }
            if (std.ascii.eqlIgnoreCase(self.function.name, "sum")) {
                // Each aggregate group certifies one integer sign independently.
                // Zero is neutral. Mixed signs and floating point retain the
                // ordinary contribution order, including intermediate overflow.
                if (engine.numeric_consolidation) {
                    if (value != .integer) return error.CypherNumericRegroupingDeclined;
                    const sign: @TypeOf(self.sum_sign) = if (value.integer == 0) .zero else if (value.integer > 0) .positive else .negative;
                    if (sign != .zero) {
                        if (self.sum_sign != .zero and self.sum_sign != sign) return error.CypherNumericRegroupingDeclined;
                        self.sum_sign = sign;
                    }
                }
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
                self.best = try engine.cloneAggregateScoped(value, self.arena.allocator());
            }
        }
        fn retainCollection(self: *@This(), engine: *Engine, value: v.Value, weight: Count, distinct_owned: ?v.Value) !void {
            const retained = self.arena.allocator();
            const remaining = if (self.function.collect_limit) |limit| limit - self.collected.items.len else std.math.maxInt(usize);
            const copies = if (self.function.collect_limit != null) std.math.cast(usize, weight.cap(remaining)).? else std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded;
            if (copies != 0) {
                const owned = if (self.function.distinct and self.function.collect_suffix == 0 and !self.function.collect_summary) distinct_owned orelse self.distinct_values.items[self.distinct_values.items.len - 1] else try engine.cloneAggregateScoped(value, retained);
                try Engine.chargeWork(engine, copies);
                try self.collected.ensureUnusedCapacity(retained, copies);
                for (0..copies) |_| self.collected.appendAssumeCapacity(owned);
            }
            if (self.function.collect_suffix != 0) {
                self.collection_seen +|= @intCast(weight.cap(std.math.maxInt(usize)));
                try self.tail.append(engine, self.distinct_arena.allocator(), value, weight, self.function.collect_suffix);
            }
        }
        fn addMoments(self: *StreamingAggregate, engine: *Engine, kind: MomentCall, form: Affine, stats: MomentStats, count_value: Count, weight: Count) !void {
            if (kind == .sum) {
                // One-sign affine arguments preserve contribution-order
                // overflow while retaining arbitrary-precision multiplicity.
                const scale: u64 = @intCast(if (form.scale < 0) -@as(i128, form.scale) else form.scale);
                const offset: u64 = @intCast(if (form.offset < 0) -@as(i128, form.offset) else form.offset);
                const magnitude = try (try stats.sum.multiply(engine.a, Count.init(scale))).add(engine.a, try count_value.multiply(engine.a, Count.init(offset)));
                // Row-local CALL memoization can coalesce repeated rows into
                // one weighted contribution. Derive its repetition factor
                // from the current fold weight rather than the producer's.
                const repetitions = try weight.divRem(engine.a, count_value);
                std.debug.assert(repetitions.remainder.cap(1) == 0);
                const contribution = try magnitude.multiply(engine.a, repetitions.quotient);
                const value = contribution.exact() orelse return error.CypherNumericOverflow;
                const signed: i128 = if (form.scale < 0 or form.offset < 0) -@as(i128, value) else value;
                self.sum = .{ .integer = std.math.cast(i64, @as(i128, self.sum.integer) + signed) orelse return error.CypherNumericOverflow };
                return;
            }
            const value: v.Value = switch (kind) {
                .min => .{ .integer = try affineValue(form, if (form.scale < 0) stats.max else stats.min) },
                .max => .{ .integer = try affineValue(form, if (form.scale < 0) stats.min else stats.max) },
                .count => .{ .integer = 0 },
                .avg => .{ .number = @floatCast(@as(f128, @floatFromInt(form.scale)) * (if (stats.sum.cap(1) == 0) @as(f128, 0) else stats.sum.scaled().ratio(count_value.scaled())) + @as(f128, @floatFromInt(form.offset))) },
                .sum => unreachable,
            };
            const literal: ast.Expr = .{ .span = .{}, .value = .{ .literal = switch (value) {
                .integer => |n| .{ .integer = n },
                .number => |n| .{ .number = n },
                else => unreachable,
            } } };
            const saved = self.function;
            self.function.args = &.{&literal};
            defer self.function = saved;
            return self.addWeighted(engine, &.{}, weight);
        }
        fn finish(self: *StreamingAggregate, engine: *Engine) !v.Value {
            const window = try self.finishWindow(engine);
            if (!self.function.collect_summary) return window;
            // This private execution tuple cannot escape: demand planning rewrites
            // every consumer, while full/correlated consumers prohibit summaries.
            try self.ensureFilters();
            const header = 1 + self.function.collect_filters.len * 2;
            var tuple: std.ArrayList(v.Value) = .empty;
            try tuple.resize(engine.a, header + 1 + window.list.len);
            tuple.items[0] = if (self.source_null) .null else .{ .integer = if (self.collection_overflow) -1 else @intCast(self.collection_count) };
            tuple.items[header] = .{ .integer = @intCast(window.list.len) };
            @memcpy(tuple.items[header + 1 ..], window.list);
            for (self.function.collect_filters, 0..) |filter, i| {
                const state_value = &self.filter_states[i];
                if (filter.kind == .fold and state_value.failure == null and !state_value.fold.initialized) {
                    self.initFold(engine, filter, state_value) catch |err| {
                        if (!consumerFailure(err)) return err;
                        state_value.failure = err;
                    };
                }
                if (state_value.failure == null and state_value.embedding_failure) state_value.failure = error.CypherValueDepthExceeded;
                const consumer_value: v.Value = if (state_value.failure != null or self.source_null) .null else switch (filter.kind) {
                    .window => if (state_value.window) |retained| try retained.finishWindow(engine) else .{ .list = &.{} },
                    .fold => state_value.fold.accumulator,
                    else => state_value.result(filter.kind),
                };
                const encoded = if (state_value.failure) |err| @import("collection_summary.zig").Packed{ .value = .null, .code = @intFromError(err) } else try @import("collection_summary.zig").pack(engine.a, &tuple, consumer_value);
                tuple.items[1 + i * 2] = encoded.value;
                tuple.items[2 + i * 2] = .{ .integer = encoded.code };
            }
            return .{ .list = tuple.items };
        }
        fn finishWindow(self: StreamingAggregate, engine: *Engine) !v.Value {
            if (self.function.collect_suffix == 0) return self.result();
            const sum = std.math.add(usize, self.collected.items.len, self.tail.weight) catch return error.CypherStateBudgetExceeded;
            const length = @min(self.collection_seen, sum);
            var overlap = sum - length;
            try Engine.chargeWork(engine, length);
            const output = try engine.a.alloc(v.Value, length);
            @memcpy(output[0..self.collected.items.len], self.collected.items);
            var position = self.collected.items.len;
            for (0..self.tail.length) |i| {
                const tail_run = self.tail.slots[(self.tail.first + i) % self.tail.slots.len];
                const discarded = @min(overlap, tail_run.weight);
                overlap -= discarded;
                const kept = tail_run.weight - discarded;
                @memset(output[position..][0..kept], tail_run.value);
                position += kept;
            }
            return .{ .list = output };
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
    fn collectStreamingAggregates(self: *Engine, expression: *const ast.Expr, calls: *reduction_plan.Calls) !bool {
        // Pattern subqueries own their aggregate scope.
        if (expression.value == .exists or expression.value == .pattern_comprehension) return true;
        if (expression.value == .call and aggregateName(expression.value.call.name)) {
            const call_value = expression.value.call;
            for (call_value.args) |arg| if (aggregates(arg)) return false;
            if (call_value.args.len != (if (call_value.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
            try calls.append(self.a, expression);
            return true;
        }
        return self.collectStreamingChildren(expression.value, calls);
    }
    fn collectStreamingChildren(self: *Engine, value: anytype, calls: *reduction_plan.Calls) anyerror!bool {
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
    const GroupRetention = struct {
        a: A,
        keys: reduction_plan.Index = .{},
        aliases: std.StringHashMapUnmanaged(void) = .empty,
        fields: std.StringHashMapUnmanaged(void) = .empty,
        uses: std.AutoHashMapUnmanaged(*const ast.Expr, usize) = .empty,
        all: bool = false,
        pub fn skip(self: *@This(), expression: *const ast.Expr, locals: ?*const @import("expressions.zig").Local) bool {
            if (expression.value == .call and aggregateName(expression.value.call.name)) return true;
            if (self.keys.find(expression) == null or !@import("expressions.zig").unshadowed(expression, locals)) return false;
            var aliases = self.aliases.keyIterator();
            while (aliases.next()) |alias| if (@import("expressions.zig").references(expression, alias.*)) return false;
            return true;
        }
        pub fn reference(self: *@This(), name: []const u8) !void {
            if (!self.aliases.contains(name)) try self.fields.put(self.a, name, {});
        }
        pub fn pattern(self: *@This(), _: *const ast.Expr, _: ?*const @import("expressions.zig").Local) !void {
            self.all = true;
        }
    };
    fn groupRetention(self: *Engine, projection: ast.Projection) !GroupRetention {
        var plan: GroupRetention = .{ .a = self.a };
        var slot: usize = 0;
        for (projection.items) |item| {
            if (!aggregates(item.expression)) {
                _ = try plan.keys.intern(self.a, item.expression, slot);
                slot += 1;
            }
            try plan.aliases.put(self.a, item.alias orelse try self.expressionName(item.expression), {});
        }
        for (projection.order) |order| try @import("expressions.zig").visitFree(order.expression, &plan);
        try reduction_plan.keyUses(self.a, &plan.keys, projection.items, &plan.uses);
        try reduction_plan.keyUses(self.a, &plan.keys, projection.order, &plan.uses);
        return plan;
    }
    const StreamGroup = struct {
        key: []const v.Value,
        source: Row,
        states: []StreamingAggregate,
        window: ?*GroupWindow.Owned = null,
        fn releaseStates(self: @This()) void {
            for (self.states) |*state_value| state_value.deinit();
        }
        fn deinit(self: *@This()) void {
            self.releaseStates();
            if (self.window) |window| {
                // The ownership record itself lives inside this arena.
                var arena = window.arena;
                arena.deinit();
            }
        }
    };
    /// A group-key ranked window is monotone: an evicted key can never enter
    /// again. Only scalar COUNT reducers are currently licensed. Their count
    /// cannot overflow: each charged transition represents at most
    /// maxInt(i64) / work_budget logical rows. Compressed domains and memo
    /// packets must carry that bound too. Rejected arguments still execute.
    const GroupWindow = struct {
        const Owned = struct { arena: std.heap.ArenaAllocator, rank: []const v.Value, hash: u64, sequence: usize };
        capacity: usize,
        slots: []const usize,
        heap: std.ArrayList(usize) = .empty,
        sequence: usize = 0,
        fn init(engine: *Engine, projection: ast.Projection, scalar: bool) !?@This() {
            // DISTINCT with reducers may collapse different complete groups;
            // only a pure DISTINCT key window licenses candidate-local membership.
            if (projection.distinct) for (projection.items) |item| if (aggregates(item.expression) or !reduction_plan.repeatable(item.expression)) return null;
            if (!scalar or @as(u64, engine.limits.intermediate_states) > std.math.maxInt(i64) or projection.predicate != null or projection.order.len == 0 or !engine.boundedGroupWindow(projection)) return null;
            var keys: std.ArrayList(*const ast.Expr) = .empty;
            for (projection.items) |item| {
                if (aggregates(item.expression)) {
                    if (item.expression.value != .call or !std.ascii.eqlIgnoreCase(item.expression.value.call.name, "count")) return null;
                } else try keys.append(engine.a, item.expression);
            }
            if (keys.items.len == 0) return null;
            const slots = try engine.a.alloc(usize, projection.order.len);
            for (projection.order, slots) |order, *slot| {
                var found: ?usize = null;
                if (order.expression.value == .variable) {
                    var key: usize = 0;
                    for (projection.items) |item| {
                        const grouped = aggregates(item.expression);
                        const name = item.alias orelse try engine.expressionName(item.expression);
                        if (std.mem.eql(u8, name, order.expression.value.variable)) {
                            if (grouped) return null;
                            found = key;
                            break;
                        }
                        if (!grouped) key += 1;
                    }
                }
                if (found == null) {
                    // Aliases shadow input bindings in ORDER BY.
                    for (projection.items) |item| if (@import("expressions.zig").references(order.expression, item.alias orelse try engine.expressionName(item.expression))) return null;
                    for (keys.items, 0..) |key, index| if (reduction_plan.repeatable(key) and @import("expressions.zig").same(key, order.expression)) {
                        found = index;
                        break;
                    };
                }
                slot.* = found orelse return null;
            }
            const limit = try engine.count(projection.limit.?);
            return .{ .capacity = if (limit == 0) 0 else (if (projection.skip) |skip| try engine.count(skip) else 0) + limit, .slots = slots };
        }
        fn contributionLimit(engine: *Engine) u64 {
            return @as(u64, std.math.maxInt(i64)) / @max(1, engine.limits.intermediate_states);
        }
        fn permitsWeight(engine: *Engine, weight: Count) bool {
            return (weight.exact() orelse return false) <= contributionLimit(engine);
        }
        fn permitsProduct(engine: *Engine, weight: Count, length: u128) bool {
            const prior = weight.exact() orelse return false;
            const cardinality = std.math.cast(u64, length) orelse return false;
            return cardinality == 0 or prior <= contributionLimit(engine) / cardinality;
        }
        fn permitsPacket(engine: *Engine, packet: WeightedRows, calls: usize) bool {
            for (packet.rows[1..][0..calls]) |snapshot| {
                if (snapshot[6].value.integer != 0) return false;
                // COUNT's exact non-NULL cardinality is sufficient: a packet
                // can represent arbitrarily many rows whose arguments are NULL.
                const cardinality: u64 = @bitCast(snapshot[0].value.integer);
                if (cardinality > contributionLimit(engine)) return false;
            }
            return true;
        }
        fn projected(group: StreamGroup) Projected {
            return .{ .row = &.{}, .source = &.{}, .group = null, .keys = group.window.?.rank, .sequence = group.window.?.sequence };
        }
        fn compare(engine: *Engine, left: StreamGroup, right: StreamGroup, orders: []const ast.Order) !std.math.Order {
            return engine.compareProjected(projected(left), projected(right), orders);
        }
        fn admit(self: *@This(), engine: *Engine, fold: *StreamFold, key: []const v.Value, row: Row, hash: u64) !?usize {
            const rank = try engine.a.alloc(v.Value, self.slots.len);
            for (self.slots, rank) |slot, *value| value.* = key[slot];
            const candidate: Projected = .{ .row = &.{}, .source = &.{}, .group = null, .keys = rank, .sequence = self.sequence };
            self.sequence += 1;
            const full = self.heap.items.len == self.capacity;
            if (self.capacity == 0 or (full and try engine.compareProjected(candidate, projected(fold.groups.items[self.heap.items[0]]), fold.projection.order) != .lt)) {
                // These values would have been consumed even for discarded
                // groups. Do not hide type/arithmetic/pattern errors in args.
                if (fold.axis_packet == null) for (fold.calls) |expression| {
                    if (!expression.value.call.star) {
                        const value = engine.eval(expression.value.call.args[0], row, null) catch |err| {
                            if (fold.window_retry and StreamingAggregate.consumerFailure(err)) return error.CypherGroupWindowDeclined;
                            return err;
                        };
                        if (expression.value.call.distinct and value != .null) {
                            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                            defer scratch.deinit();
                            var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork };
                            const checked = if (engine.immutableOwner(value)) |owner| owner.work.info(value) else work.info(value);
                            _ = checked catch |err| {
                                if (fold.window_retry and StreamingAggregate.consumerFailure(err)) return error.CypherGroupWindowDeclined;
                                return err;
                            };
                        }
                    }
                };
                // An axis packet already certifies every pure argument in the
                // complete captured domain, including losing group candidates.
                return null;
            }
            var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            var owned = true;
            defer if (owned) arena.deinit();
            var group = try engine.newStreamGroup(arena.allocator(), key, row, fold.retention, fold.projection, fold.calls, fold.membership_leaders);
            const retained_rank = try arena.allocator().alloc(v.Value, self.slots.len);
            for (self.slots, retained_rank) |slot, *value| value.* = group.key[slot];
            const ownership = try arena.allocator().create(Owned);
            ownership.* = .{ .arena = arena, .rank = retained_rank, .hash = hash, .sequence = candidate.sequence };
            group.window = ownership;
            const index = if (full) self.heap.items[0] else fold.groups.items.len;
            if (full) {
                fold.group_index.remove(fold.groups.items[index].window.?.hash, index);
                fold.groups.items[index].deinit();
                fold.groups.items[index] = group;
            } else try fold.groups.append(fold.retained, group);
            owned = false;
            try fold.group_index.insert(engine.scratch_allocator, hash, index);
            if (!full) {
                try self.heap.append(engine.scratch_allocator, index);
                var position = self.heap.items.len - 1;
                while (position > 0) {
                    const parent = (position - 1) / 2;
                    if (try compare(engine, fold.groups.items[self.heap.items[parent]], fold.groups.items[self.heap.items[position]], fold.projection.order) != .lt) break;
                    std.mem.swap(usize, &self.heap.items[parent], &self.heap.items[position]);
                    position = parent;
                }
            } else {
                var position: usize = 0;
                while (position * 2 + 1 < self.heap.items.len) {
                    var worst = position * 2 + 1;
                    if (worst + 1 < self.heap.items.len and try compare(engine, fold.groups.items[self.heap.items[worst + 1]], fold.groups.items[self.heap.items[worst]], fold.projection.order) == .gt) worst += 1;
                    if (try compare(engine, fold.groups.items[self.heap.items[position]], fold.groups.items[self.heap.items[worst]], fold.projection.order) != .lt) break;
                    std.mem.swap(usize, &self.heap.items[position], &self.heap.items[worst]);
                    position = worst;
                }
            }
            return index;
        }
    };
    fn cloneGroupingBinding(self: *Engine, a: A, expression: *const ast.Expr, value: v.Value) !v.Value {
        const fields = self.entity_retention.get(expression) orelse return if (self.read_only and self.immutableOwner(value) != null) value else self.cloneScoped(value, a);
        const properties = switch (value) {
            .node => |node| node.properties,
            .relationship => |edge| edge.properties,
            else => return self.cloneScoped(value, a),
        };
        try chargeWork(self, properties.len);
        var kept: std.ArrayList(v.Entry) = .empty;
        for (properties) |entry| if (fields.contains(entry.name)) try kept.append(self.a, entry);
        return switch (value) {
            .node => |node| blk: {
                var projected = node.*;
                projected.properties = kept.items;
                break :blk self.cloneScoped(.{ .node = &projected }, a);
            },
            .relationship => |edge| blk: {
                var projected = edge.*;
                projected.properties = kept.items;
                break :blk self.cloneScoped(.{ .relationship = &projected }, a);
            },
            else => unreachable,
        };
    }
    fn newStreamGroup(self: *Engine, a: A, key: []const v.Value, input: Row, retention: ?*const GroupRetention, projection: ast.Projection, calls: []const *const ast.Expr, leaders: []const usize) !StreamGroup {
        const states = try a.alloc(StreamingAggregate, calls.len);
        for (states, calls) |*state_value, call_value| state_value.* = .{ .function = call_value.value.call, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
        const copied = try a.alloc(v.Value, key.len);
        var key_slot: usize = 0;
        for (projection.items) |item| if (!aggregates(item.expression)) {
            copied[key_slot] = try self.cloneGroupingBinding(a, item.expression, key[key_slot]);
            key_slot += 1;
        };
        // Grouping validation permits only grouping keys outside aggregates.
        // Bare key variables also serve map selectors, whose names are not Exprs.
        var source: std.ArrayList(v.Entry) = .empty;
        var slot: usize = 0;
        for (projection.items) |item| if (!aggregates(item.expression)) {
            if (item.expression.value == .variable) try source.append(a, .{ .name = item.expression.value.variable, .value = copied[slot] });
            slot += 1;
        };
        if (retention) |plan| for (input) |entry| {
            if ((plan.all or plan.fields.contains(entry.name)) and lookup(source.items, entry.name) == null)
                try source.append(a, .{ .name = try a.dupe(u8, entry.name), .value = try self.cloneScoped(entry.value, a) });
        };
        const retain_payloads = try a.alloc(bool, calls.len);
        @memset(retain_payloads, false);
        for (calls, leaders) |cohort_call, leader| {
            if (std.ascii.eqlIgnoreCase(cohort_call.value.call.name, "collect") and cohort_call.value.call.collect_limit == null) retain_payloads[leader] = true;
        }
        for (leaders, 0..) |leader, index| if (leader != index) {
            if (states[leader].cohort == null) {
                const membership = try a.create(DistinctMembership);
                membership.* = .{ .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .retain_payload = retain_payloads[leader] };
                const consumer = try a.create(DistinctConsumer);
                consumer.* = .{ .membership = membership, .owner = true };
                states[leader].cohort = consumer;
            }
            const consumer = try a.create(DistinctConsumer);
            consumer.* = .{ .membership = states[leader].cohort.?.membership };
            states[index].cohort = consumer;
        };
        for (states) |*state_value| state_value.consumer_row = source.items;
        return .{ .key = copied, .source = source.items, .states = states };
    }
    fn boundedGroupWindow(self: *Engine, projection: ast.Projection) bool {
        // Invalid/unknown window expressions retain the original late error
        // order. A proved nonnegative integer window can select incrementally.
        const limit = self.constant(projection.limit orelse return false, &.{}) orelse return false;
        if (limit != .integer or limit.integer < 0 or std.math.cast(usize, limit.integer) == null) return false;
        if (projection.skip) |expression| {
            const skip = self.constant(expression, &.{}) orelse return false;
            if (skip != .integer or skip.integer < 0 or std.math.cast(usize, skip.integer) == null) return false;
            _ = std.math.add(usize, @intCast(skip.integer), @intCast(limit.integer)) catch return false;
        }
        return true;
    }
    fn finishStreamGroups(self: *Engine, groups: []const StreamGroup, calls: *const reduction_plan.Calls, retention: *const GroupRetention, projection: ast.Projection) ![]const Row {
        var projected: std.ArrayList(Projected) = .empty;
        var window: ?TopK = if (self.boundedGroupWindow(projection)) try TopK.init(self, projection, &.{}) else null;
        defer if (window) |*top| top.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const retained = self.a;
        const previous_keys = self.group_overrides;
        defer self.group_overrides = previous_keys;
        const previous = self.aggregate_overrides;
        defer self.aggregate_overrides = previous;
        var unique_rows: DistinctRows = .{};
        for (groups, 0..) |group, index| {
            // Output owns every escaping value before this iteration ends.
            // Releasing reducers here avoids overlapping all inputs and outputs.
            defer group.releaseStates();
            const sequence = if (group.window) |window_| window_.sequence else index;
            _ = scratch.reset(.free_all);
            // Aggregate arenas stay alive through projection. Borrow their
            // results in scratch and own only values that escape this group.
            const eval_scope = EvalScope.enter(self, scratch.allocator());
            defer eval_scope.leave(self);
            const overrides = try self.a.alloc(v.Value, calls.items.len);
            const ready = try self.a.alloc(bool, calls.items.len);
            @memset(ready, false);
            self.aggregate_overrides = .{ .index = &calls.index.pointers, .values = overrides, .stream = .{ .ready = ready, .states = group.states } };
            self.group_overrides = .{ .index = &retention.uses, .values = group.key };
            const row = try self.a.alloc(v.Entry, projection.items.len);
            for (projection.items, row) |item, *entry| entry.* = .{ .name = item.alias orelse try self.expressionName(item.expression), .value = try self.eval(item.expression, group.source, &.{}) };
            self.group_overrides.?.aliases = row;
            const scope = if (projection.order.len != 0) try self.projectionScope(group.source, row) else group.source;
            if (projection.predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) continue;
            const distinct_hash: ?u64 = if (projection.distinct) try self.rowHash(row) else null;
            if (distinct_hash) |hash| if (try unique_rows.containsHashed(self, row, hash)) continue;
            // Window membership outlives rejected/evicted picks. Unbounded
            // output can instead be the membership's single owned row.
            if (window != null) if (distinct_hash) |hash| try unique_rows.remember(retained, hash, try cloneRow(retained, row));
            const keys = try self.a.alloc(v.Value, projection.order.len);
            for (projection.order, keys) |order, *key| key.* = try self.orderValue(order.expression, projection, row, scope, &.{});
            const candidate: Projected = .{ .row = row, .source = group.source, .group = null, .keys = keys, .sequence = sequence };
            if (window) |*top| {
                // Projection errors are still evaluated for every group,
                // including losing candidates and LIMIT 0.
                try top.acceptProjected(candidate);
            } else {
                const owned = try self.cloneProjected(candidate, retained);
                if (distinct_hash) |hash| try unique_rows.remember(retained, hash, owned.row);
                try projected.append(retained, owned);
            }
        }
        self.aggregate_overrides = previous;
        if (window) |*top| return top.finish(true);
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
        factored: ?*AxisDistinctPartitions = null,
        fn contains(self: *@This(), engine: *Engine, row: Row) !bool {
            return self.containsHashed(engine, row, try engine.rowHash(row));
        }
        fn containsHashed(self: *@This(), engine: *Engine, row: Row, hash: u64) !bool {
            if (self.factored) |domains| if (try domains.contains(engine, row)) return true;
            var candidate = self.index.head(hash);
            while (candidate) |position| {
                const link = self.index.links.items[position];
                if (try engine.rowEqual(self.rows.items[link.index], row)) return true;
                candidate = link.next;
            }
            return false;
        }
        fn accept(self: *@This(), engine: *Engine, retained: A, row: Row) !bool {
            const hash = try engine.rowHash(row);
            if (try self.containsHashed(engine, row, hash)) return false;
            try self.remember(retained, hash, try cloneRow(retained, row));
            return true;
        }
        fn remember(self: *@This(), retained: A, hash: u64, row: Row) !void {
            try self.index.insert(retained, hash, self.rows.items.len);
            try self.rows.append(retained, row);
        }
    };
    fn independentExpressions(value: anytype, alias: []const u8) bool {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) return @import("expressions.zig").rowLocal(value, struct {
            alias: []const u8,
            pub fn permits(scope: @This(), name: []const u8) bool {
                return !std.mem.eql(u8, scope.alias, name);
            }
        }{ .alias = alias });
        return switch (@typeInfo(T)) {
            .pointer => |p| blk: {
                if (p.size == .slice and p.child != u8) for (value) |child| {
                    if (!independentExpressions(child, alias)) break :blk false;
                };
                break :blk true;
            },
            .@"struct" => |s| blk: {
                inline for (s.field_names) |name| if (!independentExpressions(@field(value, name), alias)) break :blk false;
                break :blk true;
            },
            .@"union" => switch (value) {
                inline else => |payload| independentExpressions(payload, alias),
            },
            .optional => if (value) |child| independentExpressions(child, alias) else true,
            else => true,
        };
    }
    fn forwardedAxis(p: ast.Projection, alias: []const u8) ?[]const u8 {
        if (p.wildcard or p.distinct or p.predicate != null or p.order.len != 0 or p.skip != null or p.limit != null) return null;
        var target: ?[]const u8 = null;
        for (p.items) |item| {
            if (item.expression.value != .variable) {
                if (aggregates(item.expression) or !independentExpressions(item.expression, alias)) return null;
                continue;
            }
            if (std.mem.eql(u8, item.expression.value.variable, alias)) {
                if (target != null) return null;
                target = item.alias orelse alias;
            }
        }
        return target;
    }
    fn identityAxis(p: ast.Projection, alias: []const u8) bool {
        if (p.wildcard or p.distinct or p.predicate != null or p.order.len != 0 or p.skip != null or p.limit != null) return false;
        var preserved = false;
        for (p.items) |item| {
            if (item.expression.value != .variable) return false;
            const name = item.expression.value.variable;
            const target = item.alias orelse name;
            if (std.mem.eql(u8, name, alias)) {
                if (!std.mem.eql(u8, target, alias)) return false;
                preserved = true;
            } else if (std.mem.eql(u8, target, alias)) return false;
        }
        return preserved;
    }
    fn axisExpressions(value: anytype, alias: []const u8, slots: *const std.AutoHashMapUnmanaged(*const ast.Expr, usize), counts: []const bool) bool {
        if (@TypeOf(value) == *const ast.Expr) {
            if (slots.get(value)) |slot| if (counts[slot]) return true;
            if (independentExpressions(value, alias)) return true;
            switch (value.value) {
                .variable, .exists, .pattern_comprehension, .comprehension, .list_predicate, .reduce, .map_projection => return false,
                .call => |c| if (!aggregateName(c.name)) {
                    const signature = @import("functions.zig").lookup(c.name) orelse return false;
                    if (!signature.repeatable) return false;
                },
                else => {},
            }
            return axisExpressions(value.value, alias, slots, counts);
        }
        return switch (@typeInfo(@TypeOf(value))) {
            .pointer => |p| blk: {
                if (p.size == .slice and p.child != u8) for (value) |child| if (!axisExpressions(child, alias, slots, counts)) break :blk false;
                break :blk true;
            },
            .@"struct" => |s| blk: {
                inline for (s.field_names) |name| if (!axisExpressions(@field(value, name), alias, slots, counts)) break :blk false;
                break :blk true;
            },
            .@"union" => switch (value) {
                inline else => |child| axisExpressions(child, alias, slots, counts),
            },
            .optional => if (value) |child| axisExpressions(child, alias, slots, counts) else true,
            else => true,
        };
    }
    /// A DISTINCT stage owns partition keys once, while immutable dependent
    /// support and its hash index are shared by every disjoint partition.
    /// Ordinary emissions consult this same membership before retaining rows.
    const AxisDistinctSupport = struct {
        rows: []const Row,
        index: value_work.Index = .{},
        fn contains(self: *@This(), engine: *Engine, row: Row) !bool {
            var candidate = self.index.head(try engine.rowHash(row));
            while (candidate) |position| {
                const link = self.index.links.items[position];
                if (try engine.rowEqual(self.rows[link.index], row)) return true;
                candidate = link.next;
            }
            return false;
        }
    };
    const AxisDistinctPartitions = struct {
        const Partition = struct { key: Row, support: *AxisDistinctSupport };
        dependent: []const bool,
        partitions: std.ArrayList(Partition) = .empty,
        index: value_work.Index = .{},
        fn select(engine: *Engine, row: Row, dependent: []const bool, selected: bool) !Row {
            var output: std.ArrayList(v.Entry) = .empty;
            for (row, dependent) |entry, bit| if (bit == selected) try output.append(engine.a, entry);
            return output.items;
        }
        fn find(self: *@This(), engine: *Engine, key: Row) !?*AxisDistinctSupport {
            var candidate = self.index.head(try engine.rowHash(key));
            while (candidate) |position| {
                const link = self.index.links.items[position];
                const partition = self.partitions.items[link.index];
                if (try engine.rowEqual(partition.key, key)) return partition.support;
                candidate = link.next;
            }
            return null;
        }
        fn contains(self: *@This(), engine: *Engine, row: Row) !bool {
            const key = try select(engine, row, self.dependent, false);
            const support = (try self.find(engine, key)) orelse return false;
            return support.contains(engine, try select(engine, row, self.dependent, true));
        }
        fn put(self: *@This(), engine: *Engine, retained: A, key: Row, support: *AxisDistinctSupport) !void {
            const owned = try engine.cloneRowScoped(key, retained);
            try self.index.insert(retained, try engine.rowHash(key), self.partitions.items.len);
            try self.partitions.append(retained, .{ .key = owned, .support = support });
        }
    };
    /// A bounded, ordered reduction memo. Entries contain exact transition
    /// states, not algebraically regrouped SUM/AVG results. DISTINCT support
    /// stays shared until a group receives another contribution domain.
    const AxisPlan = struct {
        clauses: []const ast.Clause,
        original: []const ast.Clause,
        supports: std.AutoHashMapUnmanaged(usize, *AxisDistinctSupport) = .empty,
        dependent: []const []const bool,
        calls: []const ?AxisCall,
        sample_count: usize,
        ordered: bool,
        captures: Names,
        memo: CallMemo,
    };
    const AxisTrace = struct { reached: []bool, count: Count = Count.init(0), plan: *const AxisPlan, samples: []Row, sampled: []bool, retained: A };
    fn axisHasSource(value: anytype) bool {
        if (@TypeOf(value) == *const ast.Expr) {
            if (value.execution_list_source != null or value.execution_range_binding) return true;
            return axisHasSource(value.value);
        }
        return switch (@typeInfo(@TypeOf(value))) {
            .pointer => |pointer| blk: {
                if (pointer.size == .slice and pointer.child != u8) for (value) |child| if (axisHasSource(child)) break :blk true;
                break :blk false;
            },
            .@"struct" => |structure| blk: {
                inline for (structure.field_names) |name| if (axisHasSource(@field(value, name))) break :blk true;
                break :blk false;
            },
            .@"union" => switch (value) {
                inline else => |payload| axisHasSource(payload),
            },
            .optional => if (value) |child| axisHasSource(child) else false,
            else => false,
        };
    }
    fn axisPure(expression: *const ast.Expr) bool {
        if (axisHasSource(expression)) return false;
        return @import("expressions.zig").rowLocal(expression, struct {
            pub fn permits(_: @This(), _: []const u8) bool {
                return true;
            }
        }{});
    }
    fn axisReferences(self: *Engine, names: *Names, expression: *const ast.Expr) !void {
        try self.retainReferences(names, &.{}, expression);
    }
    fn axisDepends(expression: *const ast.Expr, names: Names) bool {
        var keys = names.keyIterator();
        while (keys.next()) |name| if (@import("expressions.zig").references(expression, name.*)) return true;
        return false;
    }
    // Certificates describe successful results, not total evaluation. Original
    // expressions still execute, including parameter, argument and depth errors.
    // Only primitive variable reads may be elided: entity reads can project
    // authority and containers must still validate structural depth.
    const ScalarType = union(enum) {
        primitive,
        primitive_items,
        sequence: *const ScalarType,
        collection: *const CollectionType,
        items: *ListType,
        element: *ListType,
        derived: *PrimitiveType,
    };
    const CollectionType = struct { input: ?ScalarType, filters: []const ?ScalarType };
    const PrimitiveType = struct { dependencies: []const ScalarType, plain: ?bool = null };
    const ListType = struct { items: []const v.Value, plain: ?bool = null };
    const ScalarTypes = std.StringHashMapUnmanaged(ScalarType);
    fn listType(self: *Engine, items: []const v.Value) !ScalarType {
        // Only immutable parameters enter the statement cache. Other imported
        // lists may belong to a recyclable row arena: their certificate lives
        // solely through this normalization and is never keyed by that address.
        for (self.parameters) |entry| if (entry.value == .list and entry.value.list.ptr == items.ptr and entry.value.list.len == items.len) {
            const owner = try self.rootInvariantState();
            const identity: Invariants.Identity = .{ .address = @intFromPtr(items.ptr), .length = items.len };
            if (owner.parameter_items.get(identity)) |known| return .{ .items = known };
            const known = try self.planning_allocator.create(ListType);
            known.* = .{ .items = items };
            try owner.parameter_items.put(self.planning_allocator, identity, known);
            return .{ .items = known };
        };
        const known = try self.a.create(ListType);
        known.* = .{ .items = items };
        return .{ .items = known };
    }
    fn primitive(value: v.Value) bool {
        return switch (value) {
            .null, .boolean, .integer, .number, .string => true,
            else => false,
        };
    }
    fn plainBinding(self: *Engine, known: ?ScalarType) !bool {
        const kind = known orelse return false;
        if (kind == .primitive) return true;
        if (kind == .derived) {
            // Proofs form an immutable DAG across forwarding projections.
            // Resolve iteratively and memoize each node, avoiding exponential
            // revisits and native-stack growth through long alias chains.
            const Frame = struct { proof: *PrimitiveType, index: usize = 0 };
            var stack: std.ArrayList(Frame) = .empty;
            defer stack.deinit(self.scratch_allocator);
            try stack.append(self.scratch_allocator, .{ .proof = kind.derived });
            while (stack.items.len != 0) {
                const frame = &stack.items[stack.items.len - 1];
                if (frame.proof.plain != null) {
                    _ = stack.pop();
                    continue;
                }
                if (frame.index == frame.proof.dependencies.len) {
                    frame.proof.plain = true;
                    continue;
                }
                const dependency = frame.proof.dependencies[frame.index];
                try self.state();
                if (dependency == .derived and dependency.derived.plain == null) {
                    try stack.append(self.scratch_allocator, .{ .proof = dependency.derived });
                    continue;
                }
                const valid = if (dependency == .derived) dependency.derived.plain.? else try self.plainBinding(dependency);
                if (!valid) frame.proof.plain = false else frame.index += 1;
            }
            return kind.derived.plain.?;
        }
        if (kind != .element) return false;
        const certificate = kind.element;
        if (certificate.plain) |certified| return certified;
        const certified = for (certificate.items) |item| {
            try self.state();
            if (!primitive(item)) break false;
        } else true;
        certificate.plain = certified;
        return certified;
    }
    fn scalarType(self: *Engine, expression: *const ast.Expr, types: ScalarTypes) anyerror!?ScalarType {
        if (expression.execution_list_source) |id| return self.producerScalarType(id);
        return switch (expression.value) {
            .literal => .primitive,
            .variable => |name| types.get(name),
            .parameter => |name| blk: {
                const value = lookup(self.parameters, name) orelse break :blk null;
                if (primitive(value)) break :blk .primitive;
                // Carry a lazy source certificate. Most UNWINDs never need to
                // elide element reads: do not scan their lists just to plan a
                // COUNT(*) or an unreachable child. Resolve only a dead read,
                // once per immutable parameter across all nested CALLs.
                break :blk if (value == .list) try self.listType(value.list) else null;
            },
            .unary, .comparison, .list_predicate, .exists => .primitive,
            .binary => |b| if (b.op == .add) try self.derivedPrimitive(&.{ b.left, b.right }, types) else .primitive,
            .index => |index| elementType(try self.scalarType(index.base, types)),
            .slice => |slice| try self.scalarType(slice.base, types),
            .case_when => |case| blk: {
                var branches: std.ArrayList(ScalarType) = .empty;
                for (case.branches) |branch| try branches.append(self.a, (try self.scalarType(branch.value, types)) orelse break :blk null);
                try branches.append(self.a, if (case.otherwise) |otherwise| (try self.scalarType(otherwise, types)) orelse break :blk null else .primitive);
                break :blk try self.joinScalarTypes(branches.items);
            },
            .call => |call_value| blk: {
                if (call_value.execution_list) {
                    const input = try self.scalarType(call_value.args[0], types);
                    const filters = try self.a.alloc(?ScalarType, call_value.collect_filters.len);
                    for (call_value.collect_filters, filters) |filter, *result| {
                        result.* = switch (filter.kind) {
                            .count, .all, .any, .none, .single => .primitive,
                            .fold => null,
                            .window => window: {
                                if (filter.projection == null) break :window input;
                                var scope = try types.clone(self.a);
                                if (elementType(input)) |element| try scope.put(self.a, filter.alias, element) else _ = scope.remove(filter.alias);
                                break :window if (try self.scalarType(filter.projection.?, scope)) |element| try self.sequenceType(element) else null;
                            },
                        };
                    }
                    const summary = try self.a.create(CollectionType);
                    summary.* = .{ .input = input, .filters = filters };
                    break :blk .{ .collection = summary };
                }
                if (call_value.collection_extract) |extract| {
                    if (extract == .cardinality) break :blk .primitive;
                    const source = (try self.scalarType(call_value.args[0], types)) orelse break :blk null;
                    if (source == .collection) break :blk switch (extract) {
                        .window => source.collection.input,
                        .filtered => |slot| source.collection.filters[slot],
                        .cardinality => unreachable,
                    };
                    // Imported summaries have already executed their producer.
                    // Preserve a lazy element certificate for their owned window.
                    if (source == .items and extract == .window) {
                        const packet = source.items.items;
                        if (packet.len != 0 and packet[0] == .null) break :blk .primitive;
                        const header = extract.window;
                        if (header >= packet.len or packet[header] != .integer or packet[header].integer < 0) break :blk null;
                        const length = std.math.cast(usize, packet[header].integer) orelse break :blk null;
                        if (length > packet.len - header - 1) break :blk null;
                        break :blk try self.listType(packet[header + 1 ..][0..length]);
                    }
                    break :blk null;
                }
                if (@import("functions.zig").lookup(call_value.name)) |signature| switch (signature.result) {
                    .primitive => break :blk .primitive,
                    .primitive_items => break :blk .primitive_items,
                    .arguments => break :blk try self.joinExpressionTypes(call_value.args, types),
                    .element => break :blk elementType(try self.scalarType(call_value.args[0], types)),
                    .preserve => break :blk try self.scalarType(call_value.args[0], types),
                    .unknown => {},
                };
                for ([_][]const u8{ "count", "sum", "avg" }) |name| if (std.ascii.eqlIgnoreCase(call_value.name, name)) break :blk .primitive;
                break :blk null;
            },
            .list => |items| if (try self.joinExpressionTypes(items, types)) |element| try self.sequenceType(element) else null,
            else => null,
        };
    }
    fn elementType(known: ?ScalarType) ?ScalarType {
        return switch (known orelse return null) {
            .primitive, .primitive_items => .primitive,
            .items => |certificate| .{ .element = certificate },
            .sequence => |element| element.*,
            else => null,
        };
    }
    fn sequenceType(self: *Engine, element: ScalarType) !ScalarType {
        if (element == .primitive) return .primitive_items;
        const owned = try self.a.create(ScalarType);
        owned.* = element;
        return .{ .sequence = owned };
    }
    fn joinExpressionTypes(self: *Engine, expressions: []const *const ast.Expr, types: ScalarTypes) anyerror!?ScalarType {
        const domains = try self.a.alloc(ScalarType, expressions.len);
        for (expressions, domains) |expression, *domain| domain.* = (try self.scalarType(expression, types)) orelse return null;
        return self.joinScalarTypes(domains);
    }
    fn joinScalarTypes(self: *Engine, domains: []const ScalarType) anyerror!?ScalarType {
        var resolved = true;
        var scalar = true;
        var sequence = domains.len != 0;
        for (domains) |domain| {
            resolved = resolved and domain == .primitive;
            scalar = scalar and (domain == .primitive or domain == .element or domain == .derived);
            sequence = sequence and (domain == .primitive_items or domain == .items or domain == .sequence);
        }
        if (resolved) return .primitive;
        if (scalar) {
            const proof = try self.a.create(PrimitiveType);
            proof.* = .{ .dependencies = try self.a.dupe(ScalarType, domains) };
            return .{ .derived = proof };
        }
        if (sequence) {
            const elements = try self.a.alloc(ScalarType, domains.len);
            for (domains, elements) |domain, *element| element.* = elementType(domain).?;
            return if (try self.joinScalarTypes(elements)) |element| try self.sequenceType(element) else null;
        }
        return null;
    }
    // Compose success-result proofs without inspecting parameter elements.
    // Resolve the dependencies only when eliminating an actual dead read.
    fn derivedPrimitive(self: *Engine, expressions: []const *const ast.Expr, types: ScalarTypes) !?ScalarType {
        const dependencies = try self.a.alloc(ScalarType, expressions.len);
        var resolved = true;
        for (expressions, dependencies) |expression, *dependency| {
            dependency.* = (try self.scalarType(expression, types)) orelse return null;
            if (dependency.* != .primitive and dependency.* != .element and dependency.* != .derived) return null;
            resolved = resolved and dependency.* == .primitive;
        }
        if (resolved) return .primitive;
        const proof = try self.a.create(PrimitiveType);
        proof.* = .{ .dependencies = dependencies };
        return .{ .derived = proof };
    }
    /// Expand bound wildcard columns without changing expression evaluation
    /// or CALL scope. A single scalar seed gives every stage one static schema.
    fn rowScalarTypes(self: *Engine, initial: Row) !ScalarTypes {
        var plain: ScalarTypes = .empty;
        for (initial) |entry| {
            if (primitive(entry.value)) {
                try plain.put(self.a, entry.name, .primitive);
            } else if (entry.value == .list) {
                try plain.put(self.a, entry.name, try self.listType(entry.value.list));
            }
        }
        return plain;
    }
    fn normalizeScalarPipeline(self: *Engine, initial: Row, clauses: []const ast.Clause) anyerror![]const ast.Clause {
        return self.normalizeScalarPipelineTyped(initial, clauses, try self.rowScalarTypes(initial));
    }
    fn normalizeScalarPipelineTyped(self: *Engine, initial: Row, clauses: []const ast.Clause, imported_plain: ScalarTypes) anyerror![]const ast.Clause {
        var plain = imported_plain;
        var shape = initial;
        const output = try self.a.dupe(ast.Clause, clauses);
        const inputs = try self.a.alloc(Row, output.len);
        // All imports are constant within this invocation. Track variation
        // across child rows separately from captures varying between callers.
        var varying: Names = .empty;
        const variations = try self.a.alloc(Names, output.len);
        const plain_inputs = try self.a.alloc(ScalarTypes, output.len);
        for (output, inputs, variations, plain_inputs) |*clause, *input, *variation, *plain_input| {
            variation.* = varying;
            plain_input.* = plain;
            input.* = shape;
            switch (clause.operation) {
                .unwind => {},
                .project => |*step| if (step.value.wildcard) {
                    var items: std.ArrayList(ast.Item) = .empty;
                    for (shape) |entry| {
                        const expression = try self.a.create(ast.Expr);
                        expression.* = .{ .span = .{}, .value = .{ .variable = entry.name } };
                        try items.append(self.a, .{ .expression = expression, .alias = entry.name });
                    }
                    try items.appendSlice(self.a, step.value.items);
                    step.value.items = try items.toOwnedSlice(self.a);
                    step.value.wildcard = false;
                },
                .subquery => |inner| {
                    if (!axisReturningCall(inner)) return clauses;
                    clause.operation.subquery = try self.normalizeScalarPipelineTyped(shape, inner, plain);
                },
                else => return clauses,
            }
            switch (clause.operation) {
                .unwind => |u| {
                    try varying.put(self.a, u.alias, {});
                    const source_type = try self.scalarType(u.expression, plain);
                    const item_type = elementType(source_type);
                    // Snapshots are immutable, including when UNWIND shadows an
                    // alias. A later type cannot license an earlier dead read.
                    plain = try plain.clone(self.a);
                    if (item_type) |known| try plain.put(self.a, u.alias, known) else _ = plain.remove(u.alias);
                },
                .project => |step| {
                    var next: Names = .empty;
                    var next_plain: ScalarTypes = .empty;
                    for (step.value.items) |item| if (try self.scalarType(item.expression, plain)) |known| {
                        try next_plain.put(self.a, item.alias orelse try self.expressionName(item.expression), known);
                    };
                    plain = next_plain;
                    var varying_keys = false;
                    for (step.value.items) |item| {
                        varying_keys = varying_keys or (!aggregates(item.expression) and axisDepends(item.expression, varying));
                    }
                    for (step.value.items) |item| {
                        const varies = if (aggregates(item.expression)) varying_keys else axisDepends(item.expression, varying);
                        if (varies) try next.put(self.a, item.alias orelse try self.expressionName(item.expression), {});
                    }
                    varying = next;
                },
                .subquery => |inner| {
                    // Conservatively treat exported child values as varying;
                    // recursive normalization still handles its own constants.
                    var child = shape;
                    for (inner) |step| child = try self.streamShape(child, step);
                    plain = try plain.clone(self.a);
                    for (child) |entry| {
                        try varying.put(self.a, entry.name, {});
                        _ = plain.remove(entry.name);
                    }
                },
                else => unreachable,
            }
            shape = try self.streamShape(shape, clause.*);
        }
        if (output.len == 0 or output[output.len - 1].operation != .project or !output[output.len - 1].operation.project.final) return output;
        // A CALL's internal forwarding columns are not observable outputs.
        // Prune only primitive variable copies; entity/container reads and
        // potentially failing scalar expressions execute even when dead.
        var required: Names = .empty;
        for (shape) |entry| try required.put(self.a, entry.name, {});
        var cursor = output.len;
        while (cursor != 0) {
            cursor -= 1;
            switch (output[cursor].operation) {
                .project => |*step| {
                    for (step.value.items) |item| if (!axisPure(item.expression)) return output;
                    if (step.value.predicate) |predicate| {
                        if (!axisPure(predicate)) return output;
                        try self.axisReferences(&required, predicate);
                    }
                    for (step.value.order) |order| try self.axisReferences(&required, order.expression);
                    if (step.value.skip) |expr| try self.axisReferences(&required, expr);
                    if (step.value.limit) |expr| try self.axisReferences(&required, expr);
                    var before: Names = .empty;
                    var kept: std.ArrayList(ast.Item) = .empty;
                    const grouped = for (step.value.items) |projected| {
                        if (aggregates(projected.expression)) break true;
                    } else false;
                    for (step.value.items) |item| {
                        const name = item.alias orelse try self.expressionName(item.expression);
                        // Keep an unobserved invocation-constant grouping slot:
                        // empty grouped inputs still return zero rows, without
                        // capturing the caller's otherwise unobserved value.
                        if (!step.final and !required.contains(name) and (grouped or step.value.distinct) and item.expression.value == .variable and (try self.plainBinding(plain_inputs[cursor].get(item.expression.value.variable))) and !axisDepends(item.expression, variations[cursor])) {
                            const constant_key = try self.a.create(ast.Expr);
                            constant_key.* = .{ .span = item.expression.span, .value = .{ .literal = .null } };
                            try kept.append(self.a, .{ .expression = constant_key, .alias = name });
                            continue;
                        }
                        // Varying keys still determine child multiplicity.
                        if (step.final or step.value.distinct or grouped or item.expression.value != .variable or required.contains(name) or !(try self.plainBinding(plain_inputs[cursor].get(item.expression.value.variable)))) {
                            try kept.append(self.a, item);
                            try self.axisReferences(&before, item.expression);
                        }
                    }
                    var names = required.keyIterator();
                    while (names.next()) |name| {
                        const projected = for (step.value.items) |item| {
                            if (std.mem.eql(u8, name.*, item.alias orelse try self.expressionName(item.expression))) break true;
                        } else false;
                        if (!projected) try before.put(self.a, name.*, {});
                    }
                    step.value.items = try kept.toOwnedSlice(self.a);
                    required = before;
                },
                .unwind => |u| {
                    _ = required.remove(u.alias);
                    try self.axisReferences(&required, u.expression);
                },
                .subquery => |inner| {
                    var shape_after = inputs[cursor];
                    for (inner) |clause| shape_after = try self.streamShape(shape_after, clause);
                    for (shape_after) |entry| {
                        // Even dead returned columns must join against an
                        // existing outer binding before the CALL can succeed.
                        if (lookup(inputs[cursor], entry.name) != null) {
                            try required.put(self.a, entry.name, {});
                        } else _ = required.remove(entry.name);
                    }
                    var captures: Names = .empty;
                    try self.retainQueryReferences(&captures, inputs[cursor], inner);
                    var names = captures.keyIterator();
                    while (names.next()) |name| try required.put(self.a, name.*, {});
                },
                else => return output,
            }
        }
        return output;
    }
    const AxisCall = struct { names: []const []const u8, dependent: []const bool, captures: Names, variable_cardinality: bool, sample: usize = 0 };
    fn axisCall(self: *Engine, initial: Row, incoming: Names, inner: []const ast.Clause) anyerror!?AxisCall {
        if (!axisReturningCall(inner)) return null;
        var shape = initial;
        var dependent: Names = .empty;
        var variable_cardinality = false;
        var outer_names = incoming.keyIterator();
        while (outer_names.next()) |name| try dependent.put(self.a, name.*, {});
        for (inner) |clause| switch (clause.operation) {
            .unwind => |u| {
                // Every expanded value varies inside the domain, including a
                // constant source. It must never become a representative key.
                try dependent.put(self.a, u.alias, {});
                variable_cardinality = true;
                shape = try self.streamShape(shape, clause);
            },
            .project => |step| {
                const p = step.value;
                if (p.wildcard) return null;
                if (p.predicate) |predicate| if (!axisPure(predicate)) return null;
                var next: Names = .empty;
                var grouped = false;
                var keys = false;
                var dependent_keys = false;
                for (p.items) |item| {
                    if (!axisPure(item.expression)) return null;
                    const reducer = aggregates(item.expression);
                    const varies = axisDepends(item.expression, dependent);
                    grouped = grouped or reducer;
                    keys = keys or !reducer;
                    dependent_keys = dependent_keys or (!reducer and varies);
                    // COUNT(*) has no value references, but consumes the row
                    // domain produced by preceding filters/expansions/CALLs.
                    if (varies or (reducer and variable_cardinality)) try next.put(self.a, item.alias orelse try self.expressionName(item.expression), {});
                }
                if (grouped) variable_cardinality = keys and (variable_cardinality or dependent_keys);
                if (p.distinct or p.predicate != null or p.skip != null or p.limit != null) variable_cardinality = true;
                dependent = next;
                shape = try self.streamShape(shape, clause);
            },
            .subquery => |nested| {
                const scalar = (try self.axisCall(shape, dependent, nested)) orelse return null;
                variable_cardinality = variable_cardinality or scalar.variable_cardinality;
                for (scalar.names, scalar.dependent) |name, bit| if (bit) try dependent.put(self.a, name, {});
                shape = try self.streamShape(shape, clause);
            },
            else => return null,
        };
        const names = try self.a.alloc([]const u8, shape.len);
        const bits = try self.a.alloc(bool, shape.len);
        for (shape, names, bits) |entry, *name, *bit| {
            // CALL joins must never turn an outer-binding conflict into an
            // overwrite merely because the probe pruned that outer column.
            if (lookup(initial, entry.name) != null) return null;
            name.* = entry.name;
            bit.* = dependent.contains(entry.name);
        }
        var captures: Names = .empty;
        try self.retainQueryReferences(&captures, initial, inner);
        return .{ .names = names, .dependent = bits, .captures = captures, .variable_cardinality = variable_cardinality };
    }
    fn axisConsumer(self: *Engine, filter: ast.CollectionFilter, required: *Names) !bool {
        const empty: ast.Expr = .{ .span = .{}, .value = .{ .literal = .null } };
        const expression: ast.Expr = .{ .span = .{}, .value = if (filter.kind == .fold) .{ .reduce = .{ .alias = filter.alias, .accumulator = filter.accumulator.?, .initial = filter.initial.?, .input = &empty, .projection = filter.projection.? } } else .{ .comprehension = .{ .alias = filter.alias, .input = &empty, .predicate = filter.predicate, .projection = filter.projection } } };
        if (!axisPure(&expression)) return false;
        try self.axisReferences(required, &expression);
        return true;
    }
    fn axisPlan(self: *Engine, fold: *StreamFold, index: usize, initial: Row) !?*AxisPlan {
        const previous = self.a;
        self.a = fold.retained;
        defer self.a = previous;
        const a = self.a;
        var dependencies: Names = .empty;
        const tail = fold.middle[index..];
        const dependent = try a.alloc([]const bool, tail.len);
        const calls = try a.alloc(?AxisCall, tail.len);
        @memset(calls, null);
        var sample_count: usize = 0;
        var shape = initial;
        for (tail, dependent, calls) |clause, *flags, *call_plan| {
            switch (clause.operation) {
                .unwind => |u| {
                    if (!axisPure(u.expression)) return null;
                    try dependencies.put(a, u.alias, {});
                    flags.* = &.{};
                },
                .project => |step| {
                    const p = step.value;
                    if (p.wildcard or (p.order.len != 0 and !fold.partitioned_order) or p.skip != null or p.limit != null) return null;
                    for (p.order) |order| if (!axisPure(order.expression)) return null;
                    if (p.predicate) |predicate| if (!axisPure(predicate)) return null;
                    var next: Names = .empty;
                    const bits = try a.alloc(bool, p.items.len);
                    for (p.items, bits) |item, *bit| {
                        if (aggregates(item.expression) or !axisPure(item.expression)) return null;
                        bit.* = axisDepends(item.expression, dependencies);
                        if (bit.*) try next.put(a, item.alias orelse try self.expressionName(item.expression), {});
                    }
                    dependencies = next;
                    flags.* = bits;
                },
                .subquery => |inner| {
                    var scalar = (try self.axisCall(shape, dependencies, inner)) orelse return null;
                    scalar.sample = sample_count;
                    sample_count += 1;
                    for (scalar.names, scalar.dependent) |name, bit| if (bit) try dependencies.put(a, name, {});
                    call_plan.* = scalar;
                    flags.* = &.{};
                },
                else => return null,
            }
            shape = try self.streamShape(shape, clause);
        }
        var group_required: Names = .empty;
        for (fold.projection.items) |item| if (!aggregates(item.expression)) {
            if (!axisPure(item.expression) or axisDepends(item.expression, dependencies)) return null;
            try self.axisReferences(&group_required, item.expression);
        };
        var required: Names = .empty;
        for (fold.calls) |expression| {
            const call_value = expression.value.call;
            if (!std.ascii.eqlIgnoreCase(call_value.name, "count") and !std.ascii.eqlIgnoreCase(call_value.name, "sum") and !std.ascii.eqlIgnoreCase(call_value.name, "avg") and !std.ascii.eqlIgnoreCase(call_value.name, "min") and !std.ascii.eqlIgnoreCase(call_value.name, "max") and !(std.ascii.eqlIgnoreCase(call_value.name, "collect") and call_value.collect_limit != null)) return null;
            for (call_value.args) |argument| {
                if (!axisPure(argument)) return null;
                try self.axisReferences(&required, argument);
            }
            for (call_value.collect_filters) |filter| if (!try self.axisConsumer(filter, &required)) return null;
        }
        const clauses = try a.dupe(ast.Clause, tail);
        var cursor = clauses.len;
        while (cursor != 0) {
            cursor -= 1;
            switch (clauses[cursor].operation) {
                .unwind => |u| {
                    _ = required.remove(u.alias);
                    _ = group_required.remove(u.alias);
                    try self.axisReferences(&required, u.expression);
                },
                .project => |*step| {
                    var selected: std.ArrayList(ast.Item) = .empty;
                    // Dependent expressions execute for every domain row even
                    // when their values are dead. Retain them for scalar errors
                    // as well as DISTINCT observation; representatives cannot
                    // evaluate them against placeholder bindings.
                    for (step.value.items, dependent[cursor]) |item, bit| {
                        if (bit) try required.put(a, item.alias orelse try self.expressionName(item.expression), {});
                    }
                    if (step.value.predicate) |predicate| try self.axisReferences(&required, predicate);
                    for (step.value.order) |order| try self.axisReferences(&required, order.expression);
                    var before: Names = .empty;
                    var group_before: Names = .empty;
                    for (step.value.items) |item| if (group_required.contains(item.alias orelse try self.expressionName(item.expression))) try self.axisReferences(&group_before, item.expression);
                    group_required = group_before;
                    for (step.value.items) |item| if (required.contains(item.alias orelse try self.expressionName(item.expression))) {
                        try selected.append(a, item);
                        try self.axisReferences(&before, item.expression);
                    };
                    // WHERE sees both projected aliases and the input scope.
                    // Names absent from the projection still capture input.
                    var needed = required.keyIterator();
                    while (needed.next()) |name| {
                        const projected = for (step.value.items) |item| {
                            if (std.mem.eql(u8, name.*, item.alias orelse try self.expressionName(item.expression))) break true;
                        } else false;
                        if (!projected) try before.put(a, name.*, {});
                    }
                    step.value.items = try selected.toOwnedSlice(a);
                    required = before;
                },
                .subquery => {
                    const scalar = calls[cursor].?;
                    var grouping = false;
                    for (scalar.names) |name| {
                        _ = required.remove(name);
                        grouping = group_required.remove(name) or grouping;
                    }
                    var captures = scalar.captures.keyIterator();
                    while (captures.next()) |name| {
                        try required.put(a, name.*, {});
                        if (grouping) try group_required.put(a, name.*, {});
                    }
                },
                else => unreachable,
            }
        }
        // A cache key determining the group can never initialize a second
        // fresh group. Avoid retaining one-use states for correlated axes.
        var reusable = false;
        var group_names = group_required.keyIterator();
        while (group_names.next()) |name| if (!required.contains(name.*)) {
            reusable = true;
            break;
        };
        if (!reusable) return null;
        const plan = try a.create(AxisPlan);
        plan.* = .{ .clauses = clauses, .original = tail, .dependent = dependent, .calls = calls, .sample_count = sample_count, .ordered = fold.partitioned_order, .captures = required, .memo = .{ .identity_keys = true, .budget = .{ .backing = self.scratch_allocator, .limit = @min(4 << 20, self.limits.memory_bytes / 16) } } };
        return plan;
    }
    fn axisSnapshot(self: *Engine, state_value: *StreamingAggregate) anyerror!Row {
        if (state_value.mean_scaled != null or state_value.distinct_support != null) return error.CypherAxisMemoDeclined;
        var members: v.Value = .null;
        const function_call = state_value.function;
        if (function_call.distinct and state_value.failure == null and !std.ascii.eqlIgnoreCase(function_call.name, "min") and !std.ascii.eqlIgnoreCase(function_call.name, "max")) {
            const keys = if (state_value.list_domain) |facts| facts.distinct.? else if (state_value.cohort) |cohort| blk: {
                if (cohort.membership.values.items.len == state_value.count) break :blk cohort.membership.values.items;
                var kept: std.ArrayList(v.Value) = .empty;
                for (cohort.membership.values.items, 0..) |value, ordinal| if (state_value.consumedOrdinal(ordinal)) try kept.append(self.a, value);
                break :blk kept.items;
            } else state_value.distinct_values.items;
            // Arithmetic cardinality-only proofs do not own an enumerable set.
            if (keys.len != state_value.count) return error.CypherAxisMemoDeclined;
            members = .{ .list = keys };
            if ((try self.values.info(members)).entities) return error.CypherAxisMemoDeclined;
        }
        if ((try self.values.info(state_value.best)).entities) return error.CypherAxisMemoDeclined;
        const collection = if (std.ascii.eqlIgnoreCase(function_call.name, "collect")) try state_value.finishWindow(self) else v.Value.null;
        if ((try self.values.info(collection)).entities) return error.CypherAxisMemoDeclined;
        try state_value.ensureFilters();
        const consumers = try self.a.alloc(v.Value, state_value.filter_states.len);
        for (consumers, state_value.filter_states) |*encoded, *consumer| {
            if ((try self.values.info(consumer.fold.accumulator)).entities) return error.CypherAxisMemoDeclined;
            encoded.* = .{ .map = try self.a.dupe(v.Entry, &.{
                .{ .name = "count", .value = .{ .integer = @bitCast(consumer.count) } },
                .{ .name = "failure", .value = .{ .integer = if (consumer.failure) |err| @intFromError(err) else 0 } },
                .{ .name = "unknown", .value = .{ .boolean = consumer.unknown } },
                .{ .name = "decided", .value = if (consumer.decided) |answer| .{ .boolean = answer } else .null },
                .{ .name = "window", .value = if (consumer.window) |window| .{ .map = try self.axisSnapshot(window) } else .null },
                .{ .name = "embedding", .value = .{ .boolean = consumer.embedding_failure } },
                .{ .name = "initialized", .value = .{ .boolean = consumer.fold.initialized } },
                .{ .name = "accumulator", .value = consumer.fold.accumulator },
            }) };
        }
        return self.a.dupe(v.Entry, &.{
            .{ .name = "count", .value = .{ .integer = @bitCast(state_value.count) } },
            .{ .name = "sum", .value = state_value.sum },
            .{ .name = "mean", .value = .{ .number = state_value.mean } },
            .{ .name = "mean_weight", .value = .{ .number = state_value.mean_weight } },
            .{ .name = "best", .value = state_value.best },
            .{ .name = "members", .value = members },
            .{ .name = "failure", .value = .{ .integer = if (state_value.failure) |err| @intFromError(err) else 0 } },
            .{ .name = "sign", .value = .{ .integer = @backingInt(state_value.sum_sign) } },
            .{ .name = "collection", .value = collection },
            .{ .name = "seen", .value = .{ .integer = @bitCast(@as(u64, state_value.collection_seen)) } },
            .{ .name = "total", .value = .{ .integer = @bitCast(state_value.collection_count) } },
            .{ .name = "overflow", .value = .{ .boolean = state_value.collection_overflow } },
            .{ .name = "source_null", .value = .{ .boolean = state_value.source_null } },
            .{ .name = "consumers", .value = .{ .list = consumers } },
        });
    }
    fn axisRestore(self: *Engine, state_value: *StreamingAggregate, row: Row) anyerror!void {
        state_value.count = @bitCast(row[0].value.integer);
        state_value.sum = row[1].value;
        state_value.mean = row[2].value.number;
        state_value.mean_weight = row[3].value.number;
        state_value.best = row[4].value;
        if (row[5].value != .null) {
            // Read-only memo hits promote their complete rows to statement
            // storage. Register that destination before borrowing its support.
            _ = try (try self.rootInvariantState()).work.info(row[5].value);
            const facts = (try self.listCountFacts(row[5].value)) orelse return error.CypherInvalidBackendPage;
            facts.distinct = facts.input;
            state_value.list_domain = facts;
        }
        state_value.failure = if (row[6].value.integer == 0) null else @errorFromInt(@as(u16, @intCast(row[6].value.integer)));
        state_value.sum_sign = @fromBackingInt(@intCast(row[7].value.integer));
        if (row[8].value == .list) {
            const cells = row[8].value.list;
            const prefix = @min(state_value.function.collect_limit.?, cells.len);
            try state_value.collected.appendSlice(state_value.arena.allocator(), cells[0..prefix]);
            const suffix = @min(state_value.function.collect_suffix, cells.len);
            for (cells[cells.len - suffix ..]) |cell| try state_value.tail.append(self, state_value.distinct_arena.allocator(), cell, Count.one, state_value.function.collect_suffix);
            state_value.collection_seen = @intCast(@as(u64, @bitCast(row[9].value.integer)));
            state_value.collection_count = @bitCast(row[10].value.integer);
            state_value.collection_overflow = row[11].value.boolean;
            state_value.source_null = row[12].value.boolean;
        }
        try state_value.ensureFilters();
        for (state_value.filter_states, state_value.function.collect_filters, row[13].value.list) |*consumer, filter, encoded| {
            const cells = encoded.map;
            consumer.count = @bitCast(cells[0].value.integer);
            consumer.failure = if (cells[1].value.integer == 0) null else @errorFromInt(@as(u16, @intCast(cells[1].value.integer)));
            consumer.unknown = cells[2].value.boolean;
            consumer.decided = if (cells[3].value == .null) null else cells[3].value.boolean;
            if (cells[4].value == .map) {
                const window = try state_value.arena.allocator().create(StreamingAggregate);
                window.* = .{ .function = .{ .name = "collect", .args = &.{}, .collect_limit = filter.prefix, .collect_suffix = filter.suffix }, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
                consumer.window = window;
                try self.axisRestore(window, cells[4].value.map);
            }
            consumer.embedding_failure = cells[5].value.boolean;
            if (cells[6].value.boolean) try consumer.fold.own(self, cells[7].value);
        }
    }
    fn computeAxis(self: *Engine, parent: *StreamFold, plan: *AxisPlan, key: Row) anyerror!WeightedRows {
        const columns = self.columns;
        const stages = self.stages;
        self.stages = .empty;
        const retention = self.retain_windows;
        defer self.columns = columns;
        defer self.stages = stages;
        defer self.retain_windows = retention;
        self.retain_windows = false;
        var groups: std.ArrayList(StreamGroup) = .empty;
        defer for (groups.items) |*group| group.deinit();
        var group_index: value_work.Index = .{};
        var top: ?TopK = null;
        const counts = try self.a.alloc(usize, plan.clauses.len + 1);
        @memset(counts, 0);
        const distinct_rows = try self.a.alloc(DistinctRows, plan.clauses.len);
        @memset(distinct_rows, .{});
        var matched: usize = 0;
        const items = try self.a.alloc(ast.Item, parent.calls.len);
        for (items, parent.calls) |*item, expression| item.* = .{ .expression = expression };
        const projection: ast.Projection = .{ .items = items };
        try groups.append(self.a, try self.newStreamGroup(self.a, &.{}, &.{}, null, projection, parent.calls, parent.membership_leaders));
        for (groups.items[0].states) |*state_value| state_value.consumer_row = key;
        var trace: AxisTrace = .{ .reached = try self.a.alloc(bool, plan.clauses.len), .plan = plan, .samples = try self.a.alloc(Row, plan.sample_count), .sampled = try self.a.alloc(bool, plan.sample_count), .retained = self.a };
        @memset(trace.reached, false);
        @memset(trace.samples, &.{});
        @memset(trace.sampled, false);
        const applies = try self.a.alloc(ApplyMemo, plan.clauses.len);
        for (applies) |*apply| apply.* = .{ .memo = .{ .identity_keys = true, .budget = .{ .backing = self.scratch_allocator, .limit = @min(4 << 20, self.limits.memory_bytes / 32) } } };
        defer for (applies) |*apply| apply.memo.deinit();
        var child: StreamFold = .{ .applies = applies, .retained = self.a, .middle = plan.clauses, .projection = projection, .calls = parent.calls, .membership_leaders = parent.membership_leaders, .call_aliases = parent.call_aliases, .top = &top, .groups = &groups, .group_index = &group_index, .stage_counts = counts, .matched = &matched, .distinct = distinct_rows, .has_keys = false, .single_seed = true, .defer_aggregates = true, .record_rows = true, .axis_enabled = false, .axis_trace = &trace };
        if (plan.ordered) {
            var ordered_rows: []const Row = &.{key};
            for (plan.clauses, 0..) |clause, stage| {
                trace.reached[stage] = ordered_rows.len != 0;
                ordered_rows = if (clause.operation == .project) try self.project(ordered_rows, clause.operation.project.value, false) else try self.run(&.{clause}, ordered_rows, false, true);
                counts[stage + 1] = ordered_rows.len;
                if (clause.operation == .project and clause.operation.project.value.distinct) try distinct_rows[stage].rows.appendSlice(self.a, ordered_rows);
                if (plan.calls[stage]) |scalar| if (ordered_rows.len != 0) {
                    var sample: std.ArrayList(v.Entry) = .empty;
                    for (scalar.names, scalar.dependent) |name, bit| if (!bit) try sample.append(self.a, .{ .name = name, .value = lookup(ordered_rows[0], name) orelse return error.CypherInvalidBackendPage });
                    trace.samples[scalar.sample] = sample.items;
                };
            }
            for (ordered_rows) |row| try child.expand(self, row, plan.clauses.len);
        } else try child.expand(self, key, 0);
        var rows: std.ArrayList(Row) = .empty;
        try rows.resize(self.a, parent.calls.len + 1 + plan.sample_count);
        @memcpy(rows.items[1 + parent.calls.len ..], trace.samples);
        const reached = try self.a.alloc(v.Value, trace.reached.len);
        for (reached, trace.reached) |*value, yes| value.* = .{ .boolean = yes };
        const stage_values = try self.a.alloc(v.Value, counts.len);
        for (stage_values, counts) |*value, stage_count| value.* = .{ .integer = @bitCast(@as(u64, stage_count)) };
        const domains = try self.a.alloc(v.Value, distinct_rows.len);
        @memset(domains, .{ .integer = 0 });
        for (distinct_rows, plan.original, plan.dependent, domains) |set, clause, flags, *length| {
            if (clause.operation != .project or !clause.operation.project.value.distinct) continue;
            length.* = .{ .integer = @intCast(set.rows.items.len) };
            for (set.rows.items) |row| {
                var selected: std.ArrayList(v.Entry) = .empty;
                for (clause.operation.project.value.items, flags) |item, bit| if (bit) {
                    const name = item.alias orelse try self.expressionName(item.expression);
                    try selected.append(self.a, .{ .name = name, .value = lookup(row, name) orelse return error.CypherInvalidBackendPage });
                };
                try rows.append(self.a, selected.items);
            }
        }
        rows.items[0] = try self.a.dupe(v.Entry, &.{ .{ .name = "reached", .value = .{ .list = reached } }, .{ .name = "counts", .value = .{ .list = stage_values } }, .{ .name = "domains", .value = .{ .list = domains } } });
        for (rows.items[1..][0..parent.calls.len], groups.items[0].states) |*row, *state_value| row.* = try self.axisSnapshot(state_value);
        // Clone before reducer arenas close, preserving shared support vectors.
        var copies: v.CloneContext = .{ .a = self.a, .scratch = self.a };
        for (rows.items) |*row| row.* = try cloneRowContext(row.*, &copies);
        const weights = try self.a.alloc(Count, rows.items.len);
        @memset(weights, Count.one);
        weights[0] = trace.count;
        return .{ .rows = rows.items, .weights = weights };
    }

    const AxisObservation = struct {
        slots: []const bool,
        summary: ?IntegerListSum,
        facts: ?*ListCountFacts,
        weight: Count,
        non_null: u64,
        distinct_range: bool,
        copies: std.AutoHashMapUnmanaged(usize, Count) = .empty,
        fn init(fold: *StreamFold, slots: []const bool, summary: ?IntegerListSum, weight: Count, non_null: u64, range: bool, facts: ?*ListCountFacts) @This() {
            var sum = false;
            var range_distinct = false;
            for (slots, fold.calls, 0..) |qualified, expression, index| if (qualified) {
                if (fold.active_calls) |active| if (!active[index]) continue;
                const call_value = expression.value.call;
                sum = sum or (std.ascii.eqlIgnoreCase(call_value.name, "sum") and !call_value.distinct);
                range_distinct = range_distinct or (range and std.ascii.eqlIgnoreCase(call_value.name, "count") and call_value.distinct);
            };
            return .{ .slots = slots, .summary = if (sum) summary.? else null, .facts = facts, .weight = weight, .non_null = non_null, .distinct_range = range_distinct };
        }
        fn record(self: *@This(), engine: *Engine, group: usize, copies: Count) !bool {
            if (self.summary == null and !self.distinct_range) return true;
            const entry = try self.copies.getOrPut(engine.scratch_allocator, group);
            const first = !entry.found_existing;
            if (first) entry.value_ptr.* = copies else if (self.summary != null) entry.value_ptr.* = try entry.value_ptr.add(engine.a, copies);
            return first;
        }
        fn finish(self: *@This(), engine: *Engine, fold: *StreamFold) !void {
            const summary = self.summary orelse return;
            var groups = self.copies.iterator();
            while (groups.next()) |entry| {
                const weight = try self.weight.multiply(engine.a, entry.value_ptr.*);
                for (self.slots, fold.groups.items[entry.key_ptr.*].states, 0..) |qualified, *state_value, index| {
                    if (fold.active_calls) |active| if (!active[index]) continue;
                    if (!qualified or state_value.failure != null or state_value.function.distinct or !std.ascii.eqlIgnoreCase(state_value.function.name, "sum")) continue;
                    // Repeat each axis element by the complete child bag's
                    // multiplicity. Replaying the whole axis per child instead
                    // changes integer prefix overflow for mixed-sign domains.
                    const result = if (self.facts) |facts| state_value.addIntegerListSum(engine, facts, weight) else state_value.addIntegerSummary(engine, summary, weight, self.non_null);
                    result catch |err| {
                        if (!fold.defer_aggregates or !StreamingAggregate.consumerFailure(err)) return err;
                        state_value.failure = err;
                    };
                }
            }
        }
    };

    const StreamFold = struct {
        axis_enabled: bool = true,
        axis_observation: ?*AxisObservation = null,
        axis_copies: Count = Count.one,
        partitioned_order: bool = false,
        ordered_index: ?usize = null,
        axis_plans: std.AutoHashMapUnmanaged(usize, ?*AxisPlan) = .empty,
        axis_trace: ?*AxisTrace = null,
        axis_packet: ?[]const Row = null,
        retained: A,
        middle: []const ast.Clause,
        projection: ast.Projection,
        calls: []const *const ast.Expr,
        group_window: ?*GroupWindow = null,
        window_retry: bool = false,
        membership_leaders: []const usize = &.{},
        call_aliases: ?*const std.AutoHashMapUnmanaged(*const ast.Expr, usize) = null,
        retention: ?*const GroupRetention = null,
        top: *?TopK,
        groups: *std.ArrayList(StreamGroup),
        group_index: *value_work.Index,
        has_keys: bool,
        stage_counts: []usize,
        matched: *usize,
        distinct: []DistinctRows,
        applies: []ApplyMemo = &.{},
        probes: []CallMemo = &.{},
        distinct_goal: ?usize = null,
        had_match: bool = false,
        optional: bool = false,
        probe_tail: bool = true,
        weight: Count = Count.one,
        active_calls: ?[]const bool = null,
        moment_direct: ?struct { calls: []const ?DirectMoment, values: PathMoments } = null,
        record_rows: bool = true,
        consumer: ?*PatternConsumer = null,
        defer_aggregates: bool = false,
        single_seed: bool = false,
        range_counts: ?[]const bool = null,
        range_extrema: ?RangeExtrema = null,
        range_sum: ?struct { value: IntegerListSum, weight: Count, length: u64 } = null,
        range_distinct_count: ?struct { value: Count, weight: Count } = null,
        list_counts: ?struct { facts: *ListCountFacts, slots: []const bool, weight: Count } = null,
        continuation: ?struct { parent: *StreamFold, index: usize, apply: ?Row = null, returned: ?*bool = null, capture: ?*StreamCapture = null, admission: bool = false } = null,
        fn closeAxes(fold: *@This()) void {
            var plans = fold.axis_plans.valueIterator();
            while (plans.next()) |slot| if (slot.*) |plan| plan.memo.deinit();
        }
        fn recordAxisStages(fold: *@This(), summary: WeightedRows, index: usize) void {
            if (!fold.record_rows) return;
            for (summary.rows[0][1].value.list[1..], index..) |stage_count, offset| fold.stage_counts[offset + 1] +|= @intCast(@as(u64, @bitCast(stage_count.integer)));
        }
        const AxisPartition = struct { stage: usize, key: Row, rows: []const Row, dependent: []const bool };
        fn commitAxisPartitions(fold: *@This(), engine: *Engine, plan: *AxisPlan, pending: []const AxisPartition) !void {
            for (pending) |partition| {
                if (partition.rows.len == 0) continue;
                const identity = @intFromPtr(partition.rows.ptr);
                const support = plan.supports.get(identity) orelse blk: {
                    const created = try fold.retained.create(AxisDistinctSupport);
                    created.* = .{ .rows = partition.rows };
                    for (created.rows, 0..) |row, ordinal| try created.index.insert(fold.retained, try engine.rowHash(row), ordinal);
                    try plan.supports.put(fold.retained, identity, created);
                    break :blk created;
                };
                const set = &fold.distinct[partition.stage];
                if (set.factored == null) {
                    const domains = try fold.retained.create(AxisDistinctPartitions);
                    domains.* = .{ .dependent = try fold.retained.dupe(bool, partition.dependent) };
                    set.factored = domains;
                }
                try set.factored.?.put(engine, fold.retained, partition.key, support);
            }
        }
        fn permitsWindowProduct(fold: *@This(), engine: *Engine, length: u128) !bool {
            if (fold.group_window == null or fold.calls.len == 0 or GroupWindow.permitsProduct(engine, fold.weight, length)) return true;
            if (fold.window_retry) return error.CypherGroupWindowDeclined;
            return false;
        }
        fn memoAxis(fold: *@This(), engine: *Engine, row: Row, index: usize) anyerror!bool {
            if (!fold.axis_enabled or !fold.has_keys or !engine.read_only or (!fold.defer_aggregates and fold.group_window == null) or !fold.weight.isOne() or fold.consumer != null or fold.continuation != null or fold.top.* != null or fold.active_calls != null or fold.calls.len == 0 or fold.projection.wildcard) return false;
            if (fold.single_seed) {
                const repeated = for (fold.middle[0..index]) |clause| {
                    if (clause.operation == .unwind) break true;
                } else false;
                if (!repeated) return false;
            }
            const plan = (if (fold.axis_plans.get(index)) |known| known else blk: {
                const computed = try engine.axisPlan(fold, index, row);
                try fold.axis_plans.put(fold.retained, index, computed);
                break :blk computed;
            }) orelse return false;
            if (plan.memo.disabled) return false;
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            const scope = EvalScope.enter(engine, scratch.allocator());
            defer scope.leave(engine);
            var key: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (plan.captures.contains(entry.name)) {
                if ((try engine.values.info(entry.value)).entities) return false;
                try key.append(engine.a, entry);
            };
            var packet = try plan.memo.find(engine, key.items, null);
            if (packet == null) {
                const computed = engine.computeAxis(fold, plan, key.items) catch |err| {
                    // Pruned independent siblings may fail earlier in the real
                    // pipeline. A scalar probe failure must retain that order.
                    if (!StreamingAggregate.consumerFailure(err) and err != error.CypherAxisMemoDeclined) return err;
                    plan.memo.disabled = true;
                    return false;
                };
                plan.memo.put(engine, key.items, null, computed) catch |err| {
                    if (!StreamingAggregate.consumerFailure(err)) return err;
                    plan.memo.disabled = true;
                    return false;
                };
                packet = try plan.memo.find(engine, key.items, null);
                if (packet == null) return false;
            }
            const summary = packet.?;
            if (fold.group_window != null and !GroupWindow.permitsPacket(engine, summary, fold.calls.len)) {
                // No group has been published. Retry without eviction when
                // possible so exact weighted reductions retain their errors.
                if (fold.window_retry) return error.CypherGroupWindowDeclined;
                plan.memo.disabled = true;
                return false;
            }
            var representative = row;
            var pending: std.ArrayList(AxisPartition) = .empty;
            var support_offset = 1 + fold.calls.len + plan.sample_count;
            for (fold.middle[index..], plan.dependent, summary.rows[0][0].value.list, summary.rows[0][2].value.list, index..) |clause, flags, reached, domain_length, stage| {
                const support_rows = summary.rows[support_offset..][0..@intCast(domain_length.integer)];
                support_offset += support_rows.len;
                if (!reached.boolean) break;
                switch (clause.operation) {
                    .unwind => |u| representative = try engine.replace(representative, u.alias, .null),
                    .project => |step| {
                        const projected = try engine.a.alloc(v.Entry, step.value.items.len);
                        for (projected, step.value.items, flags) |*entry, item, dependent| entry.* = .{ .name = item.alias orelse try engine.expressionName(item.expression), .value = if (dependent) .null else try engine.eval(item.expression, representative, null) };
                        representative = projected;
                        if (step.value.distinct) {
                            const set = &fold.distinct[stage];
                            // Mixed domains and repeated partitions enter the
                            // ordinary fold, whose membership includes prior
                            // shared supports. Never replay a DISTINCT twice.
                            if (set.rows.items.len != 0) return false;
                            const partition = try AxisDistinctPartitions.select(engine, projected, flags, false);
                            if (set.factored) |domains| {
                                if (!std.mem.eql(bool, flags, domains.dependent) or try domains.find(engine, partition) != null) return false;
                            }
                            try pending.append(engine.a, .{ .stage = stage, .key = partition, .rows = support_rows, .dependent = flags });
                        }
                    },
                    .subquery => {
                        const scalar = plan.calls[stage - index].?;
                        const sample = summary.rows[1 + fold.calls.len + scalar.sample];
                        for (scalar.names, scalar.dependent) |name, bit| representative = try engine.replace(representative, name, if (bit) .null else lookup(sample, name) orelse .null);
                    },
                    else => unreachable,
                }
            }
            if (summary.weights[0].cap(1) == 0) {
                try fold.commitAxisPartitions(engine, plan, pending.items);
                fold.recordAxisStages(summary, index);
                return true;
            }
            const previous_packet = fold.axis_packet;
            fold.axis_packet = summary.rows[1..][0..fold.calls.len];
            defer fold.axis_packet = previous_packet;
            const previous_weight = fold.weight;
            fold.weight = summary.weights[0];
            defer fold.weight = previous_weight;
            const matched = fold.matched.*;
            fold.expand(engine, representative, fold.middle.len) catch |err| {
                if (err != error.CypherAxisMemoDeclined) return err;
                fold.matched.* = matched;
                return false;
            };
            try fold.commitAxisPartitions(engine, plan, pending.items);
            fold.recordAxisStages(summary, index);
            return true;
        }
        fn add(fold: *@This(), engine: *Engine, initial: Row) anyerror!void {
            const weight = @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
            fold.had_match = true;
            if (fold.record_rows) fold.stage_counts[0] +|= weight;
            return fold.expand(engine, initial, 0);
        }
        fn expand(fold: *@This(), engine: *Engine, row: Row, index: usize) anyerror!void {
            if (fold.axis_trace) |trace| {
                for (row) |entry| if ((try engine.values.info(entry.value)).entities) return error.CypherAxisMemoDeclined;
                if (index < fold.middle.len) trace.reached[index] = true else trace.count = try trace.count.add(engine.a, fold.weight);
                if (index != 0) if (trace.plan.calls[index - 1]) |scalar| if (!trace.sampled[scalar.sample]) {
                    var sample: std.ArrayList(v.Entry) = .empty;
                    for (scalar.names, scalar.dependent) |name, bit| if (!bit) try sample.append(engine.a, .{ .name = name, .value = lookup(row, name) orelse return error.CypherInvalidBackendPage });
                    trace.samples[scalar.sample] = try engine.cloneRowScoped(sample.items, trace.retained);
                    trace.sampled[scalar.sample] = true;
                };
            }
            if (fold.ordered_index == index) {
                if (try fold.memoAxis(engine, row, index)) return;
                return error.CypherInvalidBackendPage;
            }
            if (index < fold.middle.len) {
                const clause = fold.middle[index];
                switch (clause.operation) {
                    .unwind => |unwind| {
                        // Composed arithmetic producers carry exact bag and set
                        // domains, independently of their deferred lexical names.
                        if (projectedSource(unwind.expression) and reduction_plan.repeatable(unwind.expression) and arithmeticDomainShape(engine, unwind.expression, 0)) {
                            if (try fold.rangeCountProof(engine, unwind.alias, index + 1)) |slots| {
                                var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                                defer scratch.deinit();
                                const domain = try engine.arithmeticDomain(unwind.expression, row);
                                if (domain.null_source or domain.length.cap(1) == 0) return;
                                var needs_distinct = false;
                                for (slots, fold.calls) |qualified, call_value| if (qualified and call_value.value.call.distinct and std.ascii.eqlIgnoreCase(call_value.value.call.name, "count")) {
                                    needs_distinct = true;
                                };
                                var needs_sum = false;
                                for (slots, fold.calls) |qualified, call_value| if (qualified and std.ascii.eqlIgnoreCase(call_value.value.call.name, "sum")) {
                                    needs_sum = true;
                                };
                                const unique_count = if (needs_sum and domain.integer_sum == null) @as(?u128, null) else if (needs_distinct) try @import("range.zig").distinctCardinalityWork(scratch.allocator(), domain.ranges, engine, chargeWork) else @as(?u128, 0);
                                if (unique_count) |unique| {
                                    const previous = fold.weight;
                                    const previous_counts = fold.range_counts;
                                    const previous_sum = fold.range_sum;
                                    defer fold.range_sum = previous_sum;
                                    fold.range_sum = if (domain.integer_sum) |summary| .{ .value = summary, .weight = previous, .length = domain.length.cap(std.math.maxInt(u64)) } else null;
                                    const previous_extrema = fold.range_extrema;
                                    defer fold.range_extrema = previous_extrema;
                                    fold.range_extrema = domain.extrema();
                                    const previous_distinct = fold.range_distinct_count;
                                    defer fold.weight = previous;
                                    defer fold.range_counts = previous_counts;
                                    defer fold.range_distinct_count = previous_distinct;
                                    fold.range_counts = slots;
                                    fold.range_distinct_count = .{ .value = try engine.domainCount(unique), .weight = previous };
                                    fold.weight = try previous.multiply(engine.a, domain.length);
                                    try engine.state();
                                    if (fold.record_rows) fold.stage_counts[index + 1] +|= @intCast(fold.weight.cap(std.math.maxInt(usize)));
                                    var observation = AxisObservation.init(fold, slots, domain.integer_sum, previous, domain.length.cap(std.math.maxInt(u64)), true, null);
                                    defer observation.copies.deinit(engine.scratch_allocator);
                                    const prior_observation = fold.axis_observation;
                                    const prior_copies = fold.axis_copies;
                                    fold.axis_observation = &observation;
                                    fold.axis_copies = Count.one;
                                    defer fold.axis_observation = prior_observation;
                                    defer fold.axis_copies = prior_copies;
                                    try fold.expand(engine, try engine.replace(row, unwind.alias, domain.ranges[0].at(0)), index + 1);
                                    try observation.finish(engine, fold);
                                    return;
                                }
                            }
                        }
                        if (projectedSource(unwind.expression) and !arithmeticSource(unwind.expression)) {
                            const independent = try fold.independentTail(engine, unwind.alias, index + 1);
                            if (!independent and try fold.memoAxis(engine, row, index)) return;
                            var consumer: UnwindReceiver = .{ .fold = fold, .row = row, .index = index, .owner = engine.local_invariants, .locals = engine.projection_locals, .weighted = independent };
                            var receiver: ListReceiver = .{ .ptr = &consumer, .emit = UnwindReceiver.add, .complete = UnwindReceiver.isComplete, .destination = fold.retained, .observer = .{ .name = "collect", .args = &.{}, .collect_limit = if (independent) 0 else null } };
                            _ = try engine.produceList(unwind.expression, row, null, &receiver);
                            try receiver.check();
                            if (consumer.complete) |err| return err;
                            return;
                        }
                        var source = (try engine.unwindSource(unwind.expression, row)) orelse return;
                        const counts = if (source.values == .range) try fold.rangeCountProof(engine, unwind.alias, index + 1) else null;
                        const list_counts = if (source.values == .list and !source.reversed) try fold.listCountProof(engine, unwind.alias, index + 1, .{ .list = source.values.list }) else null;
                        // An evaluated list supplies exact bag multiplicity when
                        // its elements are unobserved. Arithmetic ranges also
                        // carry the non-NULL/uniqueness proof for COUNT(alias).
                        // Both proofs cover every remaining binding and consumer.
                        if ((source.values == .range or source.values == .list) and (counts != null or list_counts != null or try fold.independentTail(engine, unwind.alias, index + 1)) and
                            try fold.permitsWindowProduct(engine, source.length()))
                        {
                            const length = source.length();
                            if (length == 0) return;
                            const cardinality = if (std.math.cast(u64, length)) |n| Count.init(n) else try Count.init(std.math.maxInt(u64)).add(engine.a, Count.one);
                            const previous = fold.weight;
                            const previous_counts = fold.range_counts;
                            const previous_sum = fold.range_sum;
                            defer fold.range_sum = previous_sum;
                            fold.range_sum = if (source.values == .range) .{ .value = if (source.reversed) IntegerListSum.range(source.values.range).reversed().? else IntegerListSum.range(source.values.range), .weight = previous, .length = cardinality.cap(std.math.maxInt(u64)) } else null;
                            const previous_extrema = fold.range_extrema;
                            defer fold.range_extrema = previous_extrema;
                            fold.range_extrema = if (source.values == .range) (ArithmeticDomain{ .ranges = &.{source.values.range} }).extrema() else null;
                            const previous_distinct = fold.range_distinct_count;
                            fold.range_distinct_count = if (counts != null) .{ .value = cardinality, .weight = previous } else null;
                            defer fold.range_distinct_count = previous_distinct;
                            const previous_lists = fold.list_counts;
                            fold.list_counts = if (list_counts) |proof| .{ .facts = proof.facts, .slots = proof.slots, .weight = previous } else null;
                            defer fold.list_counts = previous_lists;
                            fold.range_counts = counts;
                            defer fold.range_counts = previous_counts;
                            fold.weight = try previous.multiply(engine.a, cardinality);
                            defer fold.weight = previous;
                            try engine.state();
                            if (fold.record_rows) fold.stage_counts[index + 1] +|= @intCast(fold.weight.cap(std.math.maxInt(usize)));
                            if (counts != null or list_counts != null) {
                                const slots = if (counts) |qualified| qualified else list_counts.?.slots;
                                const summary = if (fold.range_sum) |packet| packet.value else list_counts.?.facts.integer_sum;
                                const non_null = if (fold.range_sum) |packet| packet.length else list_counts.?.facts.non_null;
                                var observation = AxisObservation.init(fold, slots, summary, previous, non_null, counts != null, if (list_counts) |proof| proof.facts else null);
                                defer observation.copies.deinit(engine.scratch_allocator);
                                const prior_observation = fold.axis_observation;
                                const prior_copies = fold.axis_copies;
                                fold.axis_observation = &observation;
                                fold.axis_copies = Count.one;
                                defer fold.axis_observation = prior_observation;
                                defer fold.axis_copies = prior_copies;
                                try fold.expand(engine, try engine.replace(row, unwind.alias, source.at(0)), index + 1);
                                try observation.finish(engine, fold);
                                return;
                            }
                            return fold.expand(engine, try engine.replace(row, unwind.alias, source.at(0)), index + 1);
                        }
                        if (try fold.memoAxis(engine, row, index)) return;
                        var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                        defer arena.deinit();
                        while (source.next()) |item| {
                            _ = arena.reset(.retain_capacity);
                            const scope = EvalScope.enter(engine, arena.allocator());
                            defer scope.leave(engine);
                            try engine.state();
                            if (fold.record_rows) fold.stage_counts[index + 1] +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
                            try fold.expand(engine, try engine.replace(row, unwind.alias, item), index + 1);
                        }
                    },
                    .match => |operation| {
                        // Keep the upstream producer on the stack while the
                        // continuation consumes this binding. Its LIMIT can
                        // stop every enclosing producer without truncating a
                        // prefix whose continuation has no qualifying rows.
                        engine.recursion += 1;
                        defer engine.recursion -= 1;
                        if (engine.recursion > engine.limits.recursion_depth) return error.CypherRecursionBudgetExceeded;
                        const columns = engine.columns;
                        const stages = engine.stages;
                        engine.stages = .empty;
                        const saved_weight_demand = engine.match_weight_demand;
                        const retention = engine.retain_windows;
                        defer engine.columns = columns;
                        defer engine.stages = stages;
                        defer engine.match_weight_demand = saved_weight_demand;
                        defer engine.retain_windows = retention;
                        engine.match_weight_demand = null;
                        engine.retain_windows = false;
                        if (try engine.streamMatchProjectionInto(&.{row}, &.{fold.weight}, operation, .{ .items = &.{}, .wildcard = true }, &.{}, clause.span, clause.span, .{ .parent = fold, .index = index + 1 }, null)) |_| return;
                        // Unproved shapes retain the regular per-binding
                        // operator; no row has been emitted on a decline.
                        for (try engine.match(&.{row}, operation)) |child| try fold.expand(engine, child, index + 1);
                    },
                    .subquery => |inner| {
                        engine.recursion += 1;
                        defer engine.recursion -= 1;
                        if (engine.recursion > engine.limits.recursion_depth) return error.CypherRecursionBudgetExceeded;
                        const columns = engine.columns;
                        const stages = engine.stages;
                        engine.stages = .empty;
                        defer engine.columns = columns;
                        defer engine.stages = stages;
                        const apply = &fold.applies[index];
                        var key_arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                        defer key_arena.deinit();
                        const key = try apply.key(engine, row, inner, fold.retained, key_arena.allocator());
                        const cached = try apply.memo.find(engine, key, null);
                        if (cached == null and streamableCall(inner)) {
                            var child_returned = false;
                            var capture: StreamCapture = .{ .budget = .{ .backing = engine.scratch_allocator, .limit = @min(4 << 20, engine.limits.memory_bytes / 32) }, .disabled = apply.memo.disabled };
                            defer capture.deinit();
                            var input = row;
                            var start: usize = 0;
                            while (start + 1 < inner.len and inner[start].operation == .project) : (start += 1) {
                                const projected = try engine.projectUnordered(&.{input}, inner[start].operation.project.value, false);
                                if (projected.len == 0) {
                                    try apply.memo.put(engine, key, null, .{ .rows = &.{}, .weights = &.{} });
                                    return;
                                }
                                input = projected[0];
                            }
                            if (start + 1 < inner.len and inner[start].operation == .match) {
                                const last = inner[inner.len - 1].operation.project.value;
                                if (try engine.streamMatchProjectionInto(&.{input}, &.{Count.one}, inner[start].operation.match, last, inner[start + 1 .. inner.len - 1], inner[start].span, inner[inner.len - 1].span, .{ .parent = fold, .index = index + 1, .apply = row, .returned = &child_returned, .capture = &capture }, null)) |_| {
                                    if (!child_returned) {
                                        try apply.memo.put(engine, key, null, .{ .rows = &.{}, .weights = &.{} });
                                    } else if (!capture.disabled) {
                                        try apply.memo.put(engine, key, null, .{ .rows = capture.rows.items, .weights = capture.weights.items });
                                    }
                                    return;
                                }
                            }
                        }
                        // Materialized child scopes retain their own windows
                        // and reducers. Empty streamed bodies share this memo.
                        const children = cached orelse blk: {
                            const scope = PatternScope.enter(engine);
                            defer scope.leave(engine);
                            break :blk try engine.runWeightedRows(inner, .{ .rows = &.{row}, .weights = &.{Count.one} }, true, true);
                        };
                        if (cached == null) try apply.memo.put(engine, key, null, children);
                        const prior = fold.weight;
                        const prior_copies = fold.axis_copies;
                        defer fold.weight = prior;
                        defer fold.axis_copies = prior_copies;
                        for (children.rows, children.weights) |child, weight| {
                            var merged = row;
                            for (child) |entry| merged = (try engine.bind(merged, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                            fold.weight = try prior.multiply(engine.a, weight);
                            if (fold.axis_observation != null) fold.axis_copies = try prior_copies.multiply(engine.a, weight);
                            // A child bag may encode many logical rows in one
                            // emission. Certify it before consuming any suffix.
                            _ = try fold.permitsWindowProduct(engine, 1);
                            if (fold.record_rows) fold.stage_counts[index + 1] +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
                            try fold.expand(engine, merged, index + 1);
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
            if (fold.consumer) |consumer| return consumer.add(engine, row, fold.weight);
            if (fold.continuation) |next| {
                if (next.admission) {
                    var output = row;
                    if (next.apply) |parent| {
                        const projected = try engine.projectUnordered(&.{row}, fold.projection, false);
                        if (projected.len == 0) return;
                        output = parent;
                        for (projected[0]) |entry| output = (try engine.bind(output, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                    }
                    if (try next.parent.probe(engine, output, next.index)) return error.CypherAdmissionFound;
                    return;
                }
                const weight = next.parent.weight;
                next.parent.weight = if (next.apply != null) try weight.multiply(engine.a, fold.weight) else fold.weight;
                defer next.parent.weight = weight;
                var output = row;
                if (next.apply) |parent| {
                    const projected = try engine.projectUnordered(&.{row}, fold.projection, false);
                    if (projected.len == 0) return;
                    if (next.capture) |capture| try capture.add(engine, projected[0], fold.weight);
                    output = parent;
                    for (projected[0]) |entry| output = (try engine.bind(output, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                }
                _ = try next.parent.permitsWindowProduct(engine, 1);
                if (next.parent.record_rows) next.parent.stage_counts[next.index] +|= @as(usize, @intCast(next.parent.weight.cap(std.math.maxInt(usize))));
                if (next.returned) |returned| returned.* = true;
                return next.parent.expand(engine, output, next.index);
            }
            try engine.state();
            if (fold.record_rows) fold.matched.* +|= @as(usize, @intCast(fold.weight.cap(std.math.maxInt(usize))));
            if (fold.top.*) |*top| {
                try top.addWeighted(row, fold.weight);
                if (fold.projection.limit != null and fold.projection.order.len == 0 and top.capacity != 0 and top.total == top.capacity) return error.CypherStreamComplete;
                return;
            }
            if (fold.group_window != null and fold.calls.len != 0) {
                // One bounded logical contribution per charged transition.
                // Packet replay carries its own exact COUNT bound.
                try engine.state();
                if (fold.axis_packet == null and !GroupWindow.permitsWeight(engine, fold.weight)) {
                    if (fold.window_retry) return error.CypherGroupWindowDeclined;
                    return error.CypherStateBudgetExceeded;
                }
            }
            // Each expanded input row owns its source-address epoch. Never
            // carry these keys across UNWIND branches or nested scratch frames.
            var input_scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer input_scratch.deinit();
            const saved_values = engine.values;
            if (fold.group_window != null) engine.values = .{ .a = input_scratch.allocator(), .ptr = engine, .charge = chargeWork, .parent = if (engine.scalarOwner()) |owner| &owner.work else null };
            defer if (fold.group_window != null) {
                engine.values = saved_values;
            };
            var input_clones: v.CloneContext = .{ .a = engine.planning_allocator, .scratch = input_scratch.allocator(), .min_shared_string_bytes = 256, .max_shared_allocations = 16384 };
            const saved_input = engine.input_clones;
            const saved_capture = engine.capture_input_clones;
            defer {
                engine.input_clones = saved_input;
                engine.capture_input_clones = saved_capture;
            }
            engine.input_clones = null;
            engine.capture_input_clones = null;
            var key: std.ArrayList(v.Value) = .empty;
            for (fold.projection.items) |item| if (!aggregates(item.expression)) try key.append(engine.a, try engine.eval(item.expression, row, null));
            const hash = (try engine.values.info(.{ .list = key.items })).hash;
            var found: ?usize = if (!fold.has_keys) 0 else null;
            var candidate = fold.group_index.head(hash);
            while (candidate) |position| {
                const link = fold.group_index.links.items[position];
                const equal = if (fold.group_window != null) blk: {
                    // Selected group arenas are recyclable. Their comparison
                    // metadata must have a shorter lifetime than an eviction.
                    var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                    defer scratch.deinit();
                    var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork };
                    break :blk try work.eql(.{ .list = fold.groups.items[link.index].key }, .{ .list = key.items });
                } else try engine.values.eql(.{ .list = fold.groups.items[link.index].key }, .{ .list = key.items });
                if (equal) {
                    found = link.index;
                    break;
                }
                candidate = link.next;
            }
            if (found == null) {
                if (fold.group_window) |window| {
                    found = try window.admit(engine, fold, key.items, row, hash) orelse return;
                } else {
                    found = fold.groups.items.len;
                    try fold.group_index.insert(fold.retained, hash, found.?);
                    engine.capture_input_clones = &input_clones;
                    defer engine.capture_input_clones = null;
                    try fold.groups.append(fold.retained, try engine.newStreamGroup(fold.retained, key.items, row, fold.retention, fold.projection, fold.calls, fold.membership_leaders));
                }
            } else if (!fold.has_keys and fold.groups.items[0].source.len == 0) {
                // Capture only fields needed by an ORDER BY expression behind
                // alias shadowing; global aggregate arguments are already consumed.
                if (fold.retention) |plan| {
                    var retained_source: std.ArrayList(v.Entry) = .empty;
                    for (row) |entry| if (plan.all or plan.fields.contains(entry.name))
                        try retained_source.append(fold.retained, .{ .name = try fold.retained.dupe(u8, entry.name), .value = try engine.cloneScoped(entry.value, fold.retained) });
                    fold.groups.items[0].source = retained_source.items;
                }
            }
            if (fold.axis_packet) |packet| {
                for (fold.groups.items[found.?].states) |state_value| if (state_value.count != 0 or state_value.failure != null) return error.CypherAxisMemoDeclined;
                for (fold.groups.items[found.?].states, packet) |*state_value, snapshot| try engine.axisRestore(state_value, snapshot);
                return;
            }
            input_clones.owned_parent = engine.statement_ownership;
            engine.input_clones = &input_clones;
            // One ordinary fold emission feeds the same repeatable argument to
            // every member. Reuse its ordinal instead of rehashing wide values.
            // Synthetic statistic bins and moment inputs have independent
            // arguments and must not inherit this row-level cache token.
            const previous_token = engine.distinct_input_token;
            defer engine.distinct_input_token = previous_token;
            engine.distinct_input_token = null;
            if (engine.statistic_packet == null and fold.moment_direct == null) {
                engine.distinct_sequence += 1;
                engine.distinct_input_token = engine.distinct_sequence;
            }
            const first_domain = if (fold.axis_observation) |observation| try observation.record(engine, found.?, fold.axis_copies) else true;
            if (engine.statistic_packet) |arguments| {
                for (arguments) |argument| {
                    if (argument.slot >= fold.calls.len) return error.CypherInvalidBackendPage;
                    if (fold.active_calls) |active| if (!active[argument.slot]) return error.CypherInvalidBackendPage;
                    const accumulator = &fold.groups.items[found.?].states[argument.slot];
                    if (argument.support) |support| {
                        try accumulator.addSupport(engine, support);
                    } else {
                        try accumulator.addStatisticBin(engine, row, argument.value, try fold.weight.multiply(engine.a, argument.weight));
                        if (argument.extra) |extra| try accumulator.addStatisticBin(engine, row, extra.value, try fold.weight.multiply(engine.a, extra.weight));
                    }
                }
            } else for (fold.groups.items[found.?].states, 0..) |*accumulator, i| {
                if (fold.active_calls) |active| if (!active[i]) continue;
                if (fold.moment_direct) |direct| if (direct.calls[i]) |input| {
                    try engine.validateMomentInput(input.expression);
                    try accumulator.addMoments(engine, input.kind, input.form, direct.values.part(input.form.field.?), direct.values.count, fold.weight);
                    continue;
                };
                if (accumulator.failure != null) continue;
                if (fold.axis_observation) |observation| if (observation.slots[i] and observation.summary != null and !accumulator.function.distinct and std.ascii.eqlIgnoreCase(accumulator.function.name, "sum")) continue;
                if (fold.list_counts) |proof| if (proof.slots[i]) {
                    accumulator.addListCount(engine, proof.facts, try proof.weight.multiply(engine.a, fold.axis_copies)) catch |err| {
                        if (!fold.defer_aggregates or !StreamingAggregate.consumerFailure(err)) return err;
                        accumulator.failure = err;
                    };
                    continue;
                };
                if (fold.range_counts) |slots| if (slots[i]) {
                    if (std.ascii.eqlIgnoreCase(accumulator.function.name, "sum")) {
                        const packet = fold.range_sum.?;
                        accumulator.addIntegerSummary(engine, packet.value, packet.weight, packet.length) catch |err| {
                            if (!fold.defer_aggregates or !StreamingAggregate.consumerFailure(err)) return err;
                            accumulator.failure = err;
                        };
                        continue;
                    }
                    const minimum = std.ascii.eqlIgnoreCase(accumulator.function.name, "min");
                    if (minimum or std.ascii.eqlIgnoreCase(accumulator.function.name, "max")) {
                        const extrema = fold.range_extrema.?;
                        try accumulator.addValue(engine, .{ .integer = if (minimum) extrema.minimum else extrema.maximum }, Count.one);
                        continue;
                    }
                };
                const saved_function = accumulator.function;
                var multiplicity = fold.weight;
                if (fold.range_counts) |counts| if (counts[i]) {
                    // The range packet proves non-NULL values, and DISTINCT
                    // additionally proves one disjoint axis in this group scope.
                    if (accumulator.function.distinct) if (fold.range_distinct_count) |packet| {
                        if (!first_domain) continue;
                        multiplicity = try packet.weight.multiply(engine.a, packet.value);
                    };
                    accumulator.function.star = true;
                    accumulator.function.distinct = false;
                };
                defer accumulator.function = saved_function;
                accumulator.addWeighted(engine, row, multiplicity) catch |err| {
                    if (fold.group_window != null and fold.window_retry and StreamingAggregate.consumerFailure(err)) return error.CypherGroupWindowDeclined;
                    if (!fold.defer_aggregates or !StreamingAggregate.consumerFailure(err)) return err;
                    accumulator.failure = err;
                };
            }
            if (fold.distinct_goal) |goal| if (fold.groups.items.len >= goal) return error.CypherStreamComplete;
        }
        fn unorderedGoal(fold: *@This(), engine: *Engine) ?usize {
            if (fold.continuation) |next| return next.parent.unorderedGoal(engine);
            if ((fold.top.* == null and fold.distinct_goal == null) or fold.projection.order.len != 0) return null;
            return engine.windowGoal(fold.projection);
        }
        fn independentTail(fold: *@This(), engine: *Engine, alias: []const u8, index: usize) !bool {
            if (fold.consumer != null or fold.continuation != null) return false;
            // Even a homogeneous floating axis must retain sequential
            // rounding. Weighted multiplication is not repeated addition.
            for (fold.calls) |call_value| {
                const name = call_value.value.call.name;
                if (std.ascii.eqlIgnoreCase(name, "sum") or std.ascii.eqlIgnoreCase(name, "avg")) return false;
            }
            var fanout = false;
            for (fold.middle[index..]) |clause| switch (clause.operation) {
                .project => |p| if (!identityAxis(p.value, alias) and (p.value.wildcard or !independentExpressions(p.value, alias))) return false,
                .unwind => |u| {
                    if (!independentExpressions(u.expression, alias)) return false;
                    fanout = true;
                },
                .subquery => |inner| {
                    if (!try engine.independentCall(inner, alias)) return false;
                    fanout = fanout or !unitReturningCall(inner);
                },
                else => return false,
            };
            if (fanout) {
                // Multiplicity distributes through another axis only for
                // commutative observers. COLLECT/top-k must retain XYXY order,
                // and floating SUM/AVG retain contribution order and errors.
                if (fold.top.* != null or fold.calls.len == 0) return false;
                for (fold.calls) |call_value| {
                    const name = call_value.value.call.name;
                    if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max")) return false;
                }
            }
            return !fold.projection.wildcard and independentExpressions(fold.projection, alias);
        }
        fn rangeCountProof(fold: *@This(), engine: *Engine, alias: []const u8, index: usize) !?[]const bool {
            if (fold.consumer != null or fold.continuation != null or fold.group_window != null or fold.projection.wildcard or fold.calls.len == 0) return null;
            const slots = fold.call_aliases orelse return null;
            // Identity projections can drop a captured producer scope. Any
            // filter, rebinding or further axis needs its own domain proof.
            var observed_alias = alias;
            for (fold.middle[index..]) |clause| {
                if (clause.operation == .subquery and try engine.independentCall(clause.operation.subquery, observed_alias)) continue;
                if (clause.operation != .project) return null;
                observed_alias = forwardedAxis(clause.operation.project.value, observed_alias) orelse return null;
            }
            var unique = fold.single_seed and fold.weight.isOne();
            for (fold.middle[0 .. index - 1]) |clause| if (clause.operation != .project) {
                unique = false;
            };
            const counts = try engine.a.alloc(bool, fold.calls.len);
            var observed = false;
            for (fold.calls, counts) |expression, *qualified| {
                const c = expression.value.call;
                if (std.ascii.eqlIgnoreCase(c.name, "avg")) return null;
                const sum = std.ascii.eqlIgnoreCase(c.name, "sum");
                if (sum and (c.distinct or c.star or c.args.len != 1 or c.args[0].value != .variable or !std.mem.eql(u8, c.args[0].value.variable, observed_alias))) return null;
                const is_count = std.ascii.eqlIgnoreCase(c.name, "count");
                const extreme = std.ascii.eqlIgnoreCase(c.name, "min") or std.ascii.eqlIgnoreCase(c.name, "max");
                qualified.* = (is_count or extreme or sum) and !c.star and (extreme or !c.distinct or unique) and c.args.len == 1 and c.args[0].value == .variable and std.mem.eql(u8, c.args[0].value.variable, observed_alias);
                observed = observed or qualified.*;
            }
            if (!observed or !axisExpressions(fold.projection, observed_alias, slots, counts)) return null;
            return counts;
        }
        const ListCountProof = struct { facts: *ListCountFacts, slots: []const bool };
        fn listCountProof(fold: *@This(), engine: *Engine, alias: []const u8, index: usize, input: v.Value) !?ListCountProof {
            if (!engine.read_only or fold.consumer != null or fold.continuation != null or fold.group_window != null or fold.projection.wildcard or fold.calls.len == 0) return null;
            const slots = fold.call_aliases orelse return null;
            for (fold.calls) |expression| {
                const name = expression.value.call.name;
                if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max") and !std.ascii.eqlIgnoreCase(name, "sum")) return null;
            }
            var observed_alias = alias;
            var mappings: std.ArrayList(struct { expression: *const ast.Expr, alias: []const u8 }) = .empty;
            for (fold.middle[index..]) |clause| {
                if (clause.operation == .subquery and try engine.independentCall(clause.operation.subquery, observed_alias)) continue;
                if (clause.operation != .project) return null;
                const p = clause.operation.project.value;
                if (forwardedAxis(p, observed_alias)) |forwarded| {
                    observed_alias = forwarded;
                    continue;
                }
                if (p.wildcard or p.distinct or p.predicate != null or p.order.len != 0 or p.skip != null or p.limit != null) return null;
                var mapping: ?ast.Item = null;
                for (p.items) |item| {
                    if (independentExpressions(item.expression, observed_alias)) continue;
                    if (mapping != null or item.alias == null or aggregates(item.expression)) return null;
                    mapping = item;
                }
                const item = mapping orelse return null;
                try mappings.append(engine.a, .{ .expression = item.expression, .alias = observed_alias });
                observed_alias = item.alias.?;
            }
            const counts = try engine.a.alloc(bool, fold.calls.len);
            var observed = false;
            for (fold.calls, counts) |expression, *qualified| {
                const c = expression.value.call;
                if (std.ascii.eqlIgnoreCase(c.name, "avg")) return null;
                const sum = std.ascii.eqlIgnoreCase(c.name, "sum");
                if (sum and (c.star or c.args.len != 1 or c.args[0].value != .variable or !std.mem.eql(u8, c.args[0].value.variable, observed_alias))) return null;
                qualified.* = (std.ascii.eqlIgnoreCase(c.name, "count") or std.ascii.eqlIgnoreCase(c.name, "min") or std.ascii.eqlIgnoreCase(c.name, "max") or sum) and !c.star and c.args.len == 1 and c.args[0].value == .variable and std.mem.eql(u8, c.args[0].value.variable, observed_alias);
                observed = observed or qualified.*;
            }
            if (!observed or !axisExpressions(fold.projection, observed_alias, slots, counts)) return null;
            var mapped_input = input;
            for (mappings.items) |mapping| mapped_input = (try engine.mappedList(mapped_input, mapping.expression, mapping.alias)) orelse return null;
            const facts = try engine.listCountFacts(mapped_input) orelse return null;
            for (fold.calls, counts) |expression, qualified| if (qualified and std.ascii.eqlIgnoreCase(expression.value.call.name, "sum")) {
                if (try engine.listIntegerSum(facts, expression.value.call.distinct) == null) return null;
            };
            return .{ .facts = facts, .slots = counts };
        }
        fn progress(fold: *@This()) usize {
            if (fold.continuation) |next| return next.parent.progress();
            return if (fold.top.*) |top| top.total else fold.groups.items.len;
        }
        fn generation(fold: *@This()) usize {
            var matched: usize = @intFromBool(fold.optional and fold.had_match);
            for (fold.distinct) |set| {
                matched +|= set.rows.items.len;
                if (set.factored) |domains| matched +|= domains.partitions.items.len;
            }
            if (fold.continuation) |next| return matched + next.parent.generation();
            return matched + if (fold.distinct_goal != null) fold.groups.items.len else @as(usize, 0);
        }
        fn seen(fold: *@This(), engine: *Engine, row: Row) !bool {
            if (fold.distinct_goal == null) return false;
            var key: std.ArrayList(v.Value) = .empty;
            for (fold.projection.items) |item| try key.append(engine.a, try engine.eval(item.expression, row, null));
            const hash = (try engine.values.info(.{ .list = key.items })).hash;
            var candidate = fold.group_index.head(hash);
            while (candidate) |position| {
                const link = fold.group_index.links.items[position];
                if (try engine.values.eql(.{ .list = fold.groups.items[link.index].key }, .{ .list = key.items })) return true;
                candidate = link.next;
            }
            return false;
        }
        fn probeShape(fold: *@This(), engine: *Engine, initial: Row, index: usize) anyerror!?Row {
            var shape = initial;
            const rules = @import("expressions.zig");
            for (fold.middle[index..]) |clause| switch (clause.operation) {
                .project => |step| shape = (try engine.admissionProjectionShape(shape, step.value)) orelse return null,
                .unwind => |u| {
                    if (!rules.rowLocal(u.expression, PipelineScope{ .bindings = shape })) return null;
                    shape = try engine.replace(shape, u.alias, .{ .boolean = true });
                },
                .match => |operation| shape = (try engine.admissionMatchShape(shape, operation)) orelse return null,
                .subquery => |inner| shape = (try engine.admissionCallShape(shape, inner)) orelse return null,
                else => return null,
            };
            if (fold.continuation) |next| {
                if (next.apply) |parent| {
                    shape = (try engine.admissionProjectionShape(shape, fold.projection)) orelse return null;
                    var merged: Row = &.{};
                    for (parent) |entry| merged = try engine.replace(merged, entry.name, .{ .boolean = true });
                    for (shape) |entry| merged = try engine.replace(merged, entry.name, entry.value);
                    shape = merged;
                }
                return next.parent.probeShape(engine, shape, next.index);
            }
            return engine.admissionProjectionShape(shape, fold.projection);
        }
        fn probe(fold: *@This(), engine: *Engine, row: Row, index: usize) anyerror!bool {
            if (index < fold.middle.len) switch (fold.middle[index].operation) {
                .project => |step| {
                    const rows = try engine.projectUnordered(&.{row}, step.value, false);
                    if (rows.len == 0 or (step.value.distinct and try fold.distinct[index].contains(engine, rows[0]))) return false;
                    return fold.probe(engine, rows[0], index + 1);
                },
                .unwind => |u| {
                    var source = (try engine.unwindSource(u.expression, row)) orelse return false;
                    var arena = std.heap.ArenaAllocator.init(engine.scratch_allocator);
                    defer arena.deinit();
                    while (source.next()) |item| {
                        _ = arena.reset(.retain_capacity);
                        const scope = EvalScope.enter(engine, arena.allocator());
                        defer scope.leave(engine);
                        try engine.state();
                        if (try fold.probe(engine, try engine.replace(row, u.alias, item), index + 1)) return true;
                    }
                    return false;
                },
                .match, .subquery => return fold.probeJoin(engine, row, index),
                else => unreachable, // probeShape licenses only pure continuations
            };
            if (fold.continuation) |next| {
                var output = row;
                if (next.apply) |parent| {
                    const rows = try engine.projectUnordered(&.{row}, fold.projection, false);
                    if (rows.len == 0) return false;
                    output = parent;
                    // Conservative cache admission: even a speculative child
                    // row prevents caching an empty bag after parent filtering.
                    if (next.returned) |returned| returned.* = true;
                    // A speculative row may be pruned by the parent. The tee
                    // then cannot certify a complete intrinsic child bag.
                    if (next.capture) |capture| capture.disabled = true;
                    for (rows[0]) |entry| output = (try engine.bind(output, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                }
                return next.parent.probe(engine, output, next.index);
            }
            var projection = fold.projection;
            projection.skip = null;
            projection.limit = null;
            return (try engine.projectUnordered(&.{row}, projection, false)).len != 0 and !try fold.seen(engine, row);
        }
        fn probeJoin(fold: *@This(), engine: *Engine, row: Row, index: usize) anyerror!bool {
            // Probe work owns no parent DISTINCT, weights, row counts or child
            // cache admission. A generation qualifies each presence memo.
            const memo = &fold.probes[index];
            const epoch = fold.generation();
            if (try memo.find(engine, row, epoch)) |cached| return cached.rows.len != 0;
            if (engine.recursion >= engine.limits.recursion_depth) return true;
            engine.recursion += 1;
            defer engine.recursion -= 1;
            const scope = PatternScope.enter(engine);
            defer scope.leave(engine);
            const operation = fold.middle[index].operation;
            var input = row;
            var projection: ast.Projection = .{ .items = &.{}, .wildcard = true };
            var matching: ast.Match = undefined;
            var middle: []const ast.Clause = &.{};
            var apply: ?Row = null;
            if (operation == .match) {
                matching = operation.match;
            } else {
                const inner = operation.subquery;
                if (!streamableCall(inner)) {
                    // Windows and reducers belong to the child, not to each
                    // upstream route. Evaluate its complete weighted result
                    // in this private scope before probing the parent tail.
                    const children = try engine.runWeightedRows(inner, .{ .rows = &.{row}, .weights = &.{Count.one} }, true, true);
                    var possible = false;
                    for (children.rows) |child| {
                        var merged = row;
                        for (child) |entry| merged = (try engine.bind(merged, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                        if (try fold.probe(engine, merged, index + 1)) {
                            possible = true;
                            break;
                        }
                    }
                    try memo.put(engine, row, epoch, if (possible) .{ .rows = &.{&.{}}, .weights = &.{Count.one} } else .{ .rows = &.{}, .weights = &.{} });
                    return possible;
                }
                var start: usize = 0;
                while (start + 1 < inner.len and inner[start].operation == .project) : (start += 1) {
                    const rows = try engine.projectUnordered(&.{input}, inner[start].operation.project.value, false);
                    if (rows.len == 0) {
                        try memo.put(engine, row, epoch, .{ .rows = &.{}, .weights = &.{} });
                        return false;
                    }
                    input = rows[0];
                }
                // Row-local child bodies need no traversal operator.
                if (start + 1 == inner.len) {
                    const rows = try engine.projectUnordered(&.{input}, inner[start].operation.project.value, false);
                    if (rows.len == 0) return false;
                    var merged = row;
                    for (rows[0]) |entry| merged = (try engine.bind(merged, entry.name, entry.value)) orelse return error.CypherVariableConflict;
                    return fold.probe(engine, merged, index + 1);
                }
                if (inner[start].operation != .match) return true;
                matching = inner[start].operation.match;
                projection = inner[inner.len - 1].operation.project.value;
                middle = inner[start + 1 .. inner.len - 1];
                apply = row;
            }
            const result = engine.streamMatchProjectionInto(&.{input}, &.{Count.one}, matching, projection, middle, .{}, .{}, .{ .parent = fold, .index = index + 1, .apply = apply, .admission = true }, null) catch |err| switch (err) {
                error.CypherAdmissionFound => {
                    try memo.put(engine, row, epoch, .{ .rows = &.{&.{}}, .weights = &.{Count.one} });
                    return true;
                },
                else => return err,
            };
            // A declined streaming proof is unknown, never absence.
            const possible = result == null;
            try memo.put(engine, row, epoch, if (possible) .{ .rows = &.{&.{}}, .weights = &.{Count.one} } else .{ .rows = &.{}, .weights = &.{} });
            return possible;
        }
        fn firstOnly(fold: *@This(), index: usize) bool {
            // A shortest-path frontier may stop at one candidate only when
            // every enclosing stage preserves that row without filtering.
            for (fold.middle[index..]) |clause| switch (clause.operation) {
                .project => |p| if (p.value.predicate != null or p.value.distinct or p.value.order.len != 0 or p.value.skip != null or p.value.limit != null) return false,
                else => return false,
            };
            if (fold.projection.predicate != null or fold.projection.distinct or fold.projection.order.len != 0 or fold.calls.len != 0) return false;
            if (fold.continuation) |next| return !next.admission and next.parent.firstOnly(next.index);
            return if (fold.top.*) |top| top.capacity == 1 else false;
        }
        fn demand(fold: *@This()) usize {
            if (fold.continuation) |next| return if (next.admission) 1 else next.parent.demand();
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
        inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
            shape = try self.replace(shape, name, .{ .boolean = std.mem.eql(u8, field, "first") or std.mem.eql(u8, field, "last") });
        };
        for (path.edges) |edge| if (edge.execution_length) |name| {
            shape = try self.replace(shape, name, .{ .boolean = false });
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
            .list => |items| blk: {
                for (items) |item| if (!discardable(item, node_shape)) break :blk false;
                break :blk true;
            },
            .map => |items| blk: {
                for (items) |item| if (!discardable(item.value, node_shape)) break :blk false;
                break :blk true;
            },
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
        var calls: reduction_plan.Calls = .{};
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
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                shape = try self.replace(shape, name, .{ .boolean = std.mem.eql(u8, field, "first") or std.mem.eql(u8, field, "last") });
            };
            for (path.edges) |edge| if (edge.execution_length) |name| {
                shape = try self.replace(shape, name, .{ .boolean = false });
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
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                shape = try self.replace(shape, name, .{ .boolean = std.mem.eql(u8, field, "first") or std.mem.eql(u8, field, "last") });
            };
            for (path.edges) |edge| if (edge.execution_length) |name| {
                shape = try self.replace(shape, name, .{ .boolean = false });
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
    /// Partition reducers by both traversal orientation and certificate family.
    /// Incompatible numeric consumers retain their own exact plan, without
    /// forcing polynomial consumers to retain a length distribution. All
    /// passes share the statement's read cut, adjacency and quotas.
    fn reductionPasses(self: *Engine, input: []const Row, operation: ast.Match, projection: ast.Projection, middle: []const ast.Clause, calls: []const *const ast.Expr) ![]const ReductionPass {
        const ordinary = try self.a.dupe(ReductionPass, &.{.{ .projection = projection }});
        if (calls.len < 2 or input.len == 0) return ordinary;
        const fold = self.stream_fold orelse return ordinary;
        if (fold.continuation != null) return ordinary;
        const prior_active = fold.active_calls;
        const prior_input = self.moment_input;
        const prior_endpoint = self.moment_endpoint;
        defer {
            fold.active_calls = prior_active;
            self.moment_input = prior_input;
            self.moment_endpoint = prior_endpoint;
        }
        const single_active = try self.a.alloc(bool, calls.len);
        var proof_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer proof_arena.deinit();
        var keys: std.ArrayList(ast.Item) = .empty;
        for (projection.items) |item| if (!aggregates(item.expression)) try keys.append(self.a, item);
        const path = operation.paths[0];
        var items: [4]std.ArrayList(ast.Item) = @splat(.empty);
        var active: [4][]bool = undefined;
        for (&active) |*mask| {
            mask.* = try self.a.alloc(bool, calls.len);
            @memset(mask.*, false);
        }
        for (calls, 0..) |expression, index| {
            var observed: std.ArrayList(ast.Item) = .empty;
            try observed.appendSlice(self.a, keys.items);
            try observed.append(self.a, .{ .expression = expression });
            const single: ast.Projection = .{ .items = observed.items };
            @memset(single_active, false);
            single_active[index] = true;
            fold.active_calls = single_active;
            var cohort: ?usize = null;
            for (input) |row| {
                try self.state();
                const planned = try self.reductionPath(path, row, operation, single, middle);
                if (!try self.batchEligible(planned, row, operation, single, middle)) return ordinary;
                self.moment_input = row;
                self.moment_endpoint = planned.nodes[planned.nodes.len - 1].alias;
                const polynomial = proof: {
                    // Classification retains only a boolean. Keep temporary
                    // coefficient arrays bounded across correlated inputs.
                    _ = proof_arena.reset(.free_all);
                    const scope = EvalScope.enter(self, proof_arena.allocator());
                    defer scope.leave(self);
                    break :proof (try self.planStatistics(planned, false)) != null;
                };
                const direction: usize = @intFromBool(planned.execution_reversed != path.execution_reversed);
                const next = direction * 2 + @intFromBool(polynomial);
                if (cohort) |prior| if (prior != next) return ordinary;
                cohort = next;
            }
            const selected = cohort.?;
            if (items[selected].items.len == 0) try items[selected].appendSlice(self.a, keys.items);
            try items[selected].append(self.a, .{ .expression = expression });
            active[selected][index] = true;
        }
        var passes: std.ArrayList(ReductionPass) = .empty;
        for (items, active) |group, mask| {
            if (std.mem.indexOfScalar(bool, mask, true) == null) continue;
            try passes.append(self.a, .{ .projection = .{ .items = group.items }, .active_calls = mask });
        }
        return if (passes.items.len > 1) passes.items else ordinary;
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
        return self.streamMatchProjectionInto(input, input_weights, operation, projection, original_middle, match_span, projection_span, null, null);
    }
    fn streamShape(self: *Engine, initial: Row, clause: ast.Clause) anyerror!Row {
        var shape = initial;
        switch (clause.operation) {
            .match => |operation| for (operation.paths) |pattern| {
                for (pattern.nodes) |node| if (node.alias) |name| {
                    shape = try self.replace(shape, name, .null);
                };
                for (pattern.edges) |edge| if (edge.alias) |name| {
                    shape = try self.replace(shape, name, .null);
                };
                if (pattern.alias) |name| shape = try self.replace(shape, name, .null);
                inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(pattern.execution_summary, field)) |name| {
                    shape = try self.replace(shape, name, .null);
                };
                for (pattern.edges) |edge| if (edge.execution_length) |name| {
                    shape = try self.replace(shape, name, .null);
                };
            },
            .unwind => |u| shape = try self.replace(shape, u.alias, .null),
            .project => |p| {
                if (!p.value.wildcard) shape = &.{};
                for (p.value.items) |item| shape = try self.replace(shape, item.alias orelse try self.expressionName(item.expression), .null);
            },
            .subquery => |inner| {
                var child = initial;
                for (inner) |step| child = try self.streamShape(child, step);
                for (child) |entry| shape = try self.replace(shape, entry.name, .null);
            },
            else => unreachable,
        }
        return shape;
    }
    fn pureReturningCall(inner: []const ast.Clause) bool {
        if (inner.len == 0 or !weightedExpression(inner)) return false;
        const last = inner[inner.len - 1].operation;
        return last == .project and last.project.final;
    }
    fn hasStreamableCall(clauses: []const ast.Clause) bool {
        for (clauses) |clause| if (clause.operation == .subquery and pureReturningCall(clause.operation.subquery)) return true;
        return false;
    }
    /// Complete pure child domains can be evaluated once inside a reducer
    /// probe even when an individual CALL expands, sorts, deduplicates or windows.
    /// Other CALL optimizations still require their narrower cardinality proofs.
    fn axisReturningCall(inner: []const ast.Clause) bool {
        if (inner.len == 0 or inner[inner.len - 1].operation != .project or !inner[inner.len - 1].operation.project.final) return false;
        for (inner) |clause| switch (clause.operation) {
            .unwind => |u| if (!axisPure(u.expression)) return false,
            .project => |step| {
                const p = step.value;
                for (p.items) |item| if (!axisPure(item.expression)) return false;
                if (p.predicate) |expr| if (!axisPure(expr)) return false;
                for (p.order) |order| if (!axisPure(order.expression)) return false;
                if (p.skip) |expr| if (!axisPure(expr)) return false;
                if (p.limit) |expr| if (!axisPure(expr)) return false;
            },
            .subquery => |child| if (!axisReturningCall(child)) return false,
            else => return false,
        };
        return true;
    }
    /// From one unit input these pure bodies return at most one unit row.
    /// Reducers, filtering, DISTINCT and windows cannot expand that input;
    /// nested singleton CALLs preserve the same cardinality bound.
    fn unitReturningCall(inner: []const ast.Clause) bool {
        if (!axisReturningCall(inner)) return false;
        for (inner) |clause| switch (clause.operation) {
            .project => {},
            .subquery => |child| if (!unitReturningCall(child)) return false,
            else => return false,
        };
        return true;
    }
    /// A pure zero-or-one-row CALL independent of the axis has the same result
    /// for each represented input. Evaluate it on a unit child and preserve the
    /// caller's multiplicity; child DISTINCT, windows and global reducers stay
    /// local to that unit input. A returned axis alias would join against the
    /// actual input value even without reading it, so cannot use a representative.
    fn independentUnitCall(self: *Engine, inner: []const ast.Clause, alias: []const u8) anyerror!bool {
        return unitReturningCall(inner) and try self.independentCall(inner, alias);
    }
    fn independentCall(self: *Engine, inner: []const ast.Clause, alias: []const u8) anyerror!bool {
        if (!axisReturningCall(inner) or !independentExpressions(inner, alias)) return false;
        for (inner) |clause| switch (clause.operation) {
            .project => |step| {
                if (step.value.wildcard) return false;
                for (step.value.items) |item| {
                    const name = item.alias orelse try self.expressionName(item.expression);
                    if (std.mem.eql(u8, name, alias)) return false;
                }
            },
            .subquery => |child| if (!try self.independentCall(child, alias)) return false,
            .unwind => |u| if (std.mem.eql(u8, u.alias, alias)) return false,
            else => return false,
        };
        return true;
    }
    fn streamableCall(inner: []const ast.Clause) bool {
        if (inner.len == 0 or !weightedExpression(inner)) return false;
        const last = inner[inner.len - 1].operation;
        if (last != .project or !last.project.final) return false;
        for (inner) |clause| switch (clause.operation) {
            .match, .unwind => {},
            .subquery => |child| if (!streamableCall(child)) return false,
            .project => |step| {
                const p = step.value;
                if (p.distinct or p.order.len != 0 or p.skip != null or p.limit != null) return false;
                for (p.items) |item| if (aggregates(item.expression) or @import("expressions.zig").containsPattern(item.expression)) return false;
                if (p.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return false;
            },
            else => return false,
        };
        return true;
    }
    fn orderedReductionEnd(clauses: []const ast.Clause) ?usize {
        var ordered: ?usize = null;
        for (clauses, 0..) |clause, index| switch (clause.operation) {
            .unwind => {},
            .subquery => |inner| if (!axisReturningCall(inner)) return null,
            .project => |step| {
                var grouped = false;
                for (step.value.items) |item| grouped = grouped or aggregates(item.expression);
                if (grouped) return if (ordered != null) index else null;
                if (step.final or step.value.skip != null or step.value.limit != null) return null;
                if (step.value.order.len != 0) {
                    ordered = index;
                }
            },
            else => return null,
        };
        return null;
    }
    /// Prevalidate fallible independent columns across all outer bindings.
    /// A later partition's pre-sort failure must precede an earlier partition's
    /// post-sort failure. Any scalar failure declines the whole proof, before
    /// publishing groups, so ordinary stage execution chooses the original error.
    fn validateOrderedRepresentatives(self: *Engine, fold: *StreamFold, plan: *const AxisPlan, packet: WeightedRows, input: Row, alias: []const u8, source: ListSource) !void {
        var needed = false;
        for (plan.original, plan.dependent) |clause, flags| if (clause.operation == .project) {
            for (clause.operation.project.value.items, flags) |item, dependent| if (!dependent and item.expression.value != .variable and item.expression.value != .literal) {
                needed = true;
            };
        };
        if (!needed) return;
        var arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer arena.deinit();
        var cursor = source;
        while (cursor.next()) |value| {
            _ = arena.reset(.retain_capacity);
            const scope = EvalScope.enter(self, arena.allocator());
            defer scope.leave(self);
            try self.state();
            var row = try self.replace(input, alias, value);
            for (plan.original, plan.dependent, packet.rows[0][0].value.list, 0..) |clause, flags, reached, stage| {
                if (!reached.boolean) break;
                switch (clause.operation) {
                    .unwind => |u| row = try self.replace(row, u.alias, .null),
                    .project => |step| {
                        const projected = try self.a.alloc(v.Entry, step.value.items.len);
                        for (projected, step.value.items, flags) |*entry, item, dependent| entry.* = .{ .name = item.alias orelse try self.expressionName(item.expression), .value = if (dependent) .null else try self.eval(item.expression, row, null) };
                        row = projected;
                    },
                    .subquery => {
                        const child = plan.calls[stage].?;
                        const sample = packet.rows[1 + fold.calls.len + child.sample];
                        for (child.names, child.dependent) |name, dependent| row = try self.replace(row, name, if (dependent) .null else lookup(sample, name) orelse .null);
                    },
                    else => unreachable,
                }
            }
        }
    }
    /// A unique outer binding retained in every grouping/distinct key makes
    /// partitions disjoint. Identical ordered domains retain outer source order:
    /// stable ties preserve the order of equivalent copies across partitions.
    /// Each partition emits one group, so discovery order and final stable ties
    /// remain unchanged without requiring a final ordering by every group key.
    fn prepareOrderedFold(self: *Engine, fold: *StreamFold, input: Row) !bool {
        if (!self.read_only or !fold.single_seed or !fold.has_keys or fold.calls.len == 0 or fold.consumer != null or fold.continuation != null or fold.top.* != null or fold.middle.len < 2 or fold.middle[0].operation != .unwind or fold.projection.wildcard) return false;
        const outer = fold.middle[0].operation.unwind;
        if (!axisPure(outer.expression)) return false;
        var lineage: Names = .empty;
        try lineage.put(fold.retained, outer.alias, {});
        for (fold.middle[1..]) |clause| switch (clause.operation) {
            .unwind => |u| {
                _ = lineage.remove(u.alias);
            },
            .project => |step| {
                var next: Names = .empty;
                for (step.value.items) |item| if (item.expression.value == .variable and lineage.contains(item.expression.value.variable)) try next.put(fold.retained, item.alias orelse try self.expressionName(item.expression), {});
                if (step.value.distinct and next.count() == 0) return false;
                lineage = next;
            },
            .subquery => {},
            else => return false,
        };
        var partition_key = false;
        for (fold.projection.items) |item| if (!aggregates(item.expression)) {
            partition_key = partition_key or (item.expression.value == .variable and lineage.contains(item.expression.value.variable));
        };
        if (!partition_key) return false;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const scope = EvalScope.enter(self, scratch.allocator());
        defer scope.leave(self);
        const source = (self.unwindSource(outer.expression, input) catch |err| {
            if (StreamingAggregate.consumerFailure(err)) return false;
            return err;
        }) orelse return false;
        switch (source.values) {
            .range => |sequence| if (sequence.length < 2) return false,
            .list => |values| {
                if (values.len < 2) return false;
                var index: value_work.Index = .{};
                for (values, 0..) |value, ordinal| {
                    const info = try self.values.info(value);
                    if (info.entities) return false;
                    var candidate = index.head(info.hash);
                    while (candidate) |position| {
                        const link = index.links.items[position];
                        if (try self.values.eql(values[link.index], value)) return false;
                        candidate = link.next;
                    }
                    try index.insert(self.a, info.hash, ordinal);
                }
            },
            .scalar => return false,
        }
        const shape = try self.streamShape(input, fold.middle[0]);
        const plan = (try self.axisPlan(fold, 1, shape)) orelse return false;
        if (plan.captures.contains(outer.alias)) {
            plan.memo.deinit();
            return false;
        }
        // Register before admission so every failure path closes the cache.
        try fold.axis_plans.put(fold.retained, 1, plan);
        var key: std.ArrayList(v.Entry) = .empty;
        for (input) |entry| if (plan.captures.contains(entry.name)) try key.append(self.a, entry);
        const packet = self.computeAxis(fold, plan, key.items) catch |err| {
            if (StreamingAggregate.consumerFailure(err) or err == error.CypherAxisMemoDeclined) return false;
            return err;
        };
        if (fold.group_window != null and !GroupWindow.permitsPacket(self, packet, fold.calls.len)) {
            if (fold.window_retry) return error.CypherGroupWindowDeclined;
            return false;
        }
        self.validateOrderedRepresentatives(fold, plan, packet, input, outer.alias, source) catch |err| {
            if (StreamingAggregate.consumerFailure(err)) return false;
            return err;
        };
        plan.memo.put(self, key.items, null, packet) catch |err| {
            if (StreamingAggregate.consumerFailure(err)) return false;
            return err;
        };
        const admitted = plan.memo.find(self, key.items, null) catch |err| {
            if (StreamingAggregate.consumerFailure(err)) return false;
            return err;
        };
        if (admitted == null) return false;
        fold.ordered_index = 1;
        return true;
    }
    fn streamMatchProjectionInto(self: *Engine, input: []const Row, input_weights: ?[]const Count, operation: ast.Match, projection: ast.Projection, original_middle: []const ast.Clause, match_span: ast.Span, projection_span: ast.Span, continuation: @FieldType(StreamFold, "continuation"), consumer: ?*PatternConsumer) anyerror!?[]const Row {
        return self.streamMatchProjectionAttempt(input, input_weights, operation, projection, original_middle, match_span, projection_span, continuation, consumer, true) catch |err| {
            if (err != error.CypherGroupWindowDeclined) return err;
            // Only a read-only fold with no downstream observer can request a
            // retry. Its arenas/caches have closed and no stage/result escaped.
            // Keep already charged work; never repeat writes or refund budgets.
            return self.streamMatchProjectionAttempt(input, input_weights, operation, projection, original_middle, match_span, projection_span, continuation, consumer, false);
        };
    }
    fn streamMatchProjectionAttempt(self: *Engine, input: []const Row, input_weights: ?[]const Count, operation: ast.Match, projection: ast.Projection, original_middle: []const ast.Clause, match_span: ast.Span, projection_span: ast.Span, continuation: @FieldType(StreamFold, "continuation"), consumer: ?*PatternConsumer, allow_group_window: bool) anyerror!?[]const Row {
        // The row pipeline nests only within the statement's stack allowance.
        // Longer clause sequences retain the iterative general executor.
        if (original_middle.len > self.limits.recursion_depth -| self.recursion) return null;
        const scalar_seed = operation.paths.len == 0;
        // Invalid/unknown pagination remains late, after source/projection
        // failures. A proved finite window can safely be allocated up front.
        if (scalar_seed) {
            if (projection.limit != null and self.windowGoal(projection) == null) return null;
            if (projection.skip) |expression| {
                const skip = self.constant(expression, &.{}) orelse return null;
                if (skip != .integer or skip.integer < 0) return null;
            }
        }
        if (operation.paths.len > 1) for (operation.paths) |candidate| if (candidate.selector != .all) return null;
        const path: ast.Path = if (scalar_seed) .{ .span = .{}, .alias = null, .nodes = &.{}, .edges = &.{} } else operation.paths[0];
        if (operation.predicate) |predicate| if (@import("expressions.zig").containsPattern(predicate)) return null;
        for (operation.paths) |pattern| {
            for (pattern.nodes) |node| for (node.properties) |entry| {
                if (@import("expressions.zig").containsPattern(entry.value)) return null;
            };
            for (pattern.edges) |edge| for (edge.properties) |entry| {
                if (@import("expressions.zig").containsPattern(entry.value)) return null;
            };
        }
        var middle = if (scalar_seed) original_middle else try self.pruneMiddle(input, operation, try self.elideMiddleOrder(input, operation, original_middle, projection), projection);
        const ordered = for (middle) |clause| {
            if (clause.operation == .project and clause.operation.project.value.order.len != 0) break true;
        } else false;
        if (ordered and (!scalar_seed or input.len != 1)) return null;
        var grouped = projection.distinct;
        var reducers = false;
        for (projection.items) |item| {
            reducers = reducers or aggregates(item.expression);
            grouped = grouped or aggregates(item.expression);
        }
        if (scalar_seed and input.len == 1 and reducers) middle = try self.normalizeScalarPipeline(input[0], middle);
        const consumer_goal = if (continuation) |next| next.parent.unorderedGoal(self) else self.windowGoal(projection);
        // Scalar graph expressions can run in recyclable row scopes. A
        // multihop producer with an unbounded observer must still negotiate
        // symbolic path dependencies through the weighted stage executor;
        // admitting it here would enumerate an exponential route bag merely
        // because its endpoint consumer contains EXISTS/a comprehension.
        var nested_graph = hasCorrelatedBoundary(projection);
        for (middle) |clause| switch (clause.operation) {
            .project => |p| nested_graph = nested_graph or hasCorrelatedBoundary(p.value),
            .unwind => |u| nested_graph = nested_graph or hasCorrelatedBoundary(u.expression),
            else => {},
        };
        const finite_pull = !grouped and projection.order.len == 0 and consumer_goal != null and consumer_goal.? <= self.limits.intermediate_states -| self.stats.intermediate_states;
        const fixed_hop = path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1;
        if (nested_graph and !scalar_seed and path.edges.len != 0 and !fixed_hop and !finite_pull) return null;
        const has_match = for (middle) |clause| {
            if (clause.operation == .match or (clause.operation == .subquery and !axisReturningCall(clause.operation.subquery))) break true;
        } else false;
        // Global reducers/order remain stage barriers. This continuation path
        // is for finite unordered demand; it never enumerates a large count
        // query that the weighted stage executor can reduce compactly.
        if (has_match and (grouped or projection.order.len != 0 or consumer_goal == null or !weightedExpression(middle))) return null;
        if (has_match and consumer_goal.? > self.limits.intermediate_states -| self.stats.intermediate_states) return null;
        for (middle) |clause| {
            if (clause.operation == .match) continue;
            if (clause.operation == .subquery) {
                if (!pureReturningCall(clause.operation.subquery)) return null;
                continue;
            }
            if (clause.operation == .unwind) {
                continue;
            }
            if (clause.operation != .project) return null;
            const p = clause.operation.project.value;
            if ((!ordered and p.order.len != 0) or p.skip != null or p.limit != null) return null;
            for (p.items) |item| if (aggregates(item.expression)) return null;
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
        var calls: reduction_plan.Calls = .{};
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
        const small_window = if (consumer_goal) |goal| goal <= self.limits.intermediate_states -| self.stats.intermediate_states else false;
        const weighted_top = !grouped and (self.match_weight_demand != null or (projection.limit != null and (projection.order.len != 0 or (self.retain_windows and !small_window) or self.prefixWindow(projection))));
        const endpoint_local = !count_shortest and operation.paths.len == 1 and blk: {
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
        const count_endpoints = endpoint_local and (grouped or weighted_top);
        const distinct_window = projection.distinct and calls.items.len == 0 and projection.order.len == 0 and projection.predicate == null and projection.limit != null;
        const adaptive_candidate = path.edges.len != 0 and !(path.edges.len == 1 and path.edges[0].min_hops == 1 and path.edges[0].max_hops == 1) and !count_endpoints and (!grouped or distinct_window) and self.read_only and small_window and operation.paths.len == 1 and path.selector == .all and weightedExpression(operation);
        if (self.component_proof and !count_endpoints and !count_shortest) return null;
        var shape: std.ArrayList(v.Entry) = .empty;
        if (input.len != 0) try shape.appendSlice(self.a, input[0]);
        for (operation.paths) |pattern| {
            for (pattern.nodes) |node| if (node.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            for (pattern.edges) |edge| if (edge.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            if (pattern.alias) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(pattern.execution_summary, field)) |name| try shape.append(self.a, .{ .name = name, .value = .null });
            for (pattern.edges) |edge| if (edge.execution_length) |name| try shape.append(self.a, .{ .name = name, .value = .null });
        }
        var unique_shape: Row = &.{};
        for (shape.items) |entry| unique_shape = try self.replace(unique_shape, entry.name, entry.value);
        shape = .empty;
        try shape.appendSlice(self.a, unique_shape);
        for (middle) |clause| {
            const next = try self.streamShape(shape.items, clause);
            shape = .empty;
            try shape.appendSlice(self.a, next);
        }
        var top: ?TopK = if (grouped) null else try TopK.init(self, projection, shape.items);
        defer if (top) |*value| value.deinit();
        const retained = self.a;
        var groups: std.ArrayList(StreamGroup) = .empty;
        var group_index: value_work.Index = .{};
        defer for (groups.items) |*group| group.deinit();
        const scalar_groups = (scalar_seed or (path.edges.len == 0 and operation.paths.len == 1)) and blk: {
            if (input_weights) |weights| for (weights) |weight| if (!weight.isOne()) break :blk false;
            for (middle) |clause| switch (clause.operation) {
                .project, .unwind => {},
                .subquery => |inner| if (!axisReturningCall(inner)) break :blk false,
                else => break :blk false,
            };
            break :blk true;
        };
        var group_window = if (grouped and allow_group_window) try GroupWindow.init(self, projection, scalar_groups or (projection.distinct and calls.items.len == 0)) else null;
        defer if (group_window) |*window| {
            window.heap.deinit(self.scratch_allocator);
            group_index.deinit(self.scratch_allocator);
        };
        var has_keys = false;
        for (projection.items) |item| has_keys = has_keys or !aggregates(item.expression);
        if (grouped and !has_keys) try groups.append(retained, try self.newStreamGroup(retained, &.{}, &.{}, null, projection, calls.items, calls.distinct_leaders.items));
        var matched: usize = 0;
        const stage_counts = try retained.alloc(usize, middle.len + 1);
        @memset(stage_counts, 0);
        const distinct_stages = try retained.alloc(DistinctRows, middle.len);
        @memset(distinct_stages, .{});
        const applies = try retained.alloc(ApplyMemo, middle.len);
        for (applies, middle) |*apply, clause| apply.* = .{ .memo = .{ .identity_keys = self.read_only, .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 16) }, .disabled = clause.operation != .subquery or !pureReturningCall(clause.operation.subquery) } };
        defer for (applies) |*apply| apply.memo.deinit();
        const probes = try retained.alloc(CallMemo, middle.len);
        for (probes, middle) |*probe, clause| probe.* = .{ .budget = .{ .backing = self.scratch_allocator, .limit = @min(4 << 20, self.limits.memory_bytes / 32) }, .disabled = clause.operation != .match and clause.operation != .subquery };
        defer for (probes) |*probe| probe.deinit();
        const retention: GroupRetention = if (grouped) try self.groupRetention(projection) else .{ .a = self.a };
        var fold: StreamFold = .{ .partitioned_order = ordered, .single_seed = scalar_seed and input.len == 1 and (input_weights == null or input_weights.?[0].isOne()), .defer_aggregates = scalar_seed and group_window == null, .consumer = consumer, .retention = &retention, .probes = probes, .optional = operation.optional, .applies = applies, .continuation = continuation, .distinct = distinct_stages, .retained = retained, .middle = middle, .projection = projection, .calls = calls.items, .membership_leaders = calls.distinct_leaders.items, .group_window = if (group_window) |*window| window else null, .window_retry = self.read_only and continuation == null and consumer == null, .call_aliases = &calls.index.pointers, .top = &top, .groups = &groups, .group_index = &group_index, .has_keys = has_keys, .stage_counts = stage_counts, .matched = &matched };
        defer fold.closeAxes();
        var adaptive_probe = adaptive_candidate;
        if (adaptive_probe) for (input) |row| {
            if (operation.predicate) |predicate| if (!endpointExpression(predicate, path, row)) {
                adaptive_probe = false;
                break;
            };
            const shape_mask = try self.endpointShape(path, row);
            if (try fold.probeShape(self, shape_mask, 0) == null) {
                if (operation.optional) {
                    fold.probe_tail = false;
                } else {
                    adaptive_probe = false;
                    break;
                }
            }
        };
        if (projection.distinct and calls.items.len == 0 and projection.order.len == 0 and projection.predicate == null and projection.limit != null) {
            fold.distinct_goal = try std.math.add(usize, if (projection.skip) |skip| try self.count(skip) else 0, try self.count(projection.limit.?));
            if (scalar_seed and fold.distinct_goal == 0) fold.distinct_goal = null;
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
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(pattern.execution_summary, field)) |name| try aliases.append(retained, name);
            for (pattern.edges) |edge| if (edge.execution_length) |name| try aliases.append(retained, name);
        }
        self.match_aliases = aliases.items;
        self.shortest_fast = true;
        self.collect_match_paths = false;
        self.stream_fold = if (count_shortest) null else &fold;
        self.stream_predicate = operation.predicate;
        const zero_limit = if (projection.limit) |limit| try self.count(limit) == 0 else false;
        const passes = if (count_endpoints and !zero_limit) try self.reductionPasses(input, operation, projection, middle, calls.items) else try retained.dupe(ReductionPass, &.{.{ .projection = projection }});
        if (ordered and (passes.len != 1 or !try self.prepareOrderedFold(&fold, input[0]))) return null;
        for (passes, 0..) |pass, pass_index| {
            fold.active_calls = pass.active_calls;
            fold.record_rows = pass_index == 0;
            @memset(distinct_stages, .{});
            produce: for (input, 0..) |row, input_index| {
                fold.weight = if (input_weights) |weights| weights[input_index] else Count.one;
                if (!scalar_seed and (fold.distinct_goal == 0 or (projection.limit != null and try self.count(projection.limit.?) == 0))) break :produce;
                if (!scalar_seed) if (top) |value| if (value.capacity == 0 and projection.order.len == 0) break :produce;
                var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
                defer scratch.deinit();
                const scope = EvalScope.enter(self, scratch.allocator());
                defer scope.leave(self);
                var ignored: std.ArrayList(MatchState) = .empty;
                fold.had_match = false;
                if (scalar_seed) {
                    fold.add(self, row) catch |err| switch (err) {
                        error.CypherStreamComplete => if (continuation != null) return err else break :produce,
                        else => return err,
                    };
                    continue;
                }
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
                const previous_probe = self.route_probe;
                var rejected: usize = 0;
                self.route_probe = if (adaptive_probe) .{ .rejected = &rejected, .fold = &fold } else null;
                defer self.route_probe = previous_probe;
                self.joinPath(.{ .row = row }, planned, 0, &ignored) catch |err| switch (err) {
                    error.CypherStreamComplete => if (continuation != null) return err else break :produce,
                    else => return err,
                };
                batch.flush(self, &ignored) catch |err| switch (err) {
                    error.CypherStreamComplete => if (continuation != null) return err else break :produce,
                    else => return err,
                };
                if (operation.optional and !fold.had_match) {
                    var extended = row;
                    for (self.match_aliases) |alias| if (lookup(extended, alias) == null) {
                        extended = (try self.bind(extended, alias, .null)).?;
                    };
                    fold.add(self, extended) catch |err| switch (err) {
                        error.CypherStreamComplete => if (continuation != null) return err else break :produce,
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
        if (continuation != null or consumer != null) return &.{};
        const rows = if (top) |*value| try value.finish(true) else try self.finishStreamGroups(groups.items, &calls, &retention, projection);
        if (!scalar_seed) try self.stages.append(self.a, .{ .operation = "match", .span = match_span, .input_rows = input.len, .output_rows = stage_counts[0] });
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
    // Ordered WITH stages own a complete ordered bag. Negotiate the upstream
    // producer's multiplicity instead of falling back to ordinary route rows
    // when the row-local streaming planner declines that stage barrier.
    fn hasOrderedIntermediate(clauses: []const ast.Clause) bool {
        for (clauses) |clause| if (clause.operation == .project) {
            const step = clause.operation.project;
            if (!step.final and step.value.order.len != 0) return true;
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
                var calls: reduction_plan.Calls = .{};
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
                    inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                        extended = try self.replace(extended, name, .null);
                    };
                    for (path.edges) |edge| if (edge.execution_length) |name| {
                        extended = try self.replace(extended, name, .null);
                    };
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
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                shape = try self.replace(shape, name, .null);
            };
            for (path.edges) |edge| if (edge.execution_length) |name| {
                shape = try self.replace(shape, name, .null);
            };
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
    /// A deferred producer is a lexical closure: its source retains the old
    /// scope, while each WITH evaluates ordinary siblings once in its own
    /// input scope. Preflight the entire binding path before evaluating anything.
    const ForwardedUnwind = struct { clauses: []const ast.Clause, row: Row };
    /// A source is eagerly checked at its original WITH/operand position. Its
    /// lexical frame and completed count summary then travel together. Only
    /// repeatable read-only sources may replay into a value-observing consumer.
    const ProducerSource = struct {
        expression: *const ast.Expr,
        row: Row,
        owner: ?*Invariants,
        locals: ?*const @import("expressions.zig").Local,
        summary: v.Value,
        arithmetic: ?@import("range.zig").Range = null,
        arithmetic_shape: ?bool = null,
        arithmetic_domain: ?ArithmeticDomain = null,
        scalar_type: ?ScalarType = null,
        type_checked: bool = false,
    };
    fn producerScalarType(self: *Engine, id: usize) anyerror!?ScalarType {
        const source = &self.producer_sources.items[id];
        if (source.type_checked) return source.scalar_type;
        // A physical producer retains its original lexical frame. Infer from
        // that frame, never its dummy carrier binding. Cache the DAG proof in
        // statement ownership so no recyclable normalization arena escapes.
        const saved = self.a;
        self.a = self.planning_allocator;
        defer self.a = saved;
        source.scalar_type = try self.scalarType(source.expression, try self.rowScalarTypes(source.row));
        source.type_checked = true;
        return source.scalar_type;
    }
    fn captureProducerSource(self: *Engine, expression: *const ast.Expr, row: Row, captured: *std.ArrayList(v.Entry), alias: []const u8) !*const ast.Expr {
        const arithmetic = if (isRange(expression)) try self.rangeInput(expression, row, null) else null;
        const summary = if (isRange(expression)) try self.sourceSummary(if (arithmetic) |sequence| .{ .values = .{ .range = sequence } } else null, .{ .name = "collect", .args = &.{}, .collect_limit = 0 }) else try self.listSummary(.{ .name = "collect", .args = &.{expression}, .collect_limit = 0, .collect_summary = true }, row, null);
        const id = self.producer_sources.items.len;
        try self.producer_sources.append(self.planning_allocator, .{ .expression = expression, .row = try self.a.dupe(v.Entry, row), .owner = self.local_invariants, .locals = self.projection_locals, .summary = summary, .arithmetic = arithmetic });
        const reference = @constCast(try self.captureProducerOperand(captured, .null, expression.span, alias));
        reference.execution_list_source = id;
        reference.execution_range_binding = isRange(expression);
        return reference;
    }
    fn captureAddedProducer(self: *Engine, expression: *const ast.Expr, row: Row, captured: *std.ArrayList(v.Entry), alias: []const u8) !*const ast.Expr {
        if (reduction_plan.knownList(expression) and reduction_plan.repeatable(expression))
            return self.captureProducerSource(expression, row, captured, alias);
        return self.captureProducerOperand(captured, try self.eval(expression, row, null), expression.span, alias);
    }
    fn captureProducerOperand(self: *Engine, captured: *std.ArrayList(v.Entry), value: v.Value, span: ast.Span, alias: []const u8) !*const ast.Expr {
        var suffix = captured.items.len;
        const hidden = while (true) : (suffix += 1) {
            const candidate = try std.fmt.allocPrint(self.a, "_producer_capture_{d}", .{suffix});
            if (lookup(captured.items, candidate) == null and !std.mem.eql(u8, candidate, alias)) break candidate;
        };
        try captured.append(self.a, .{ .name = hidden, .value = value });
        const reference = try self.a.create(ast.Expr);
        reference.* = .{ .span = span, .value = .{ .variable = hidden } };
        return reference;
    }
    const ProducerBinding = struct { name: []const u8, depth: usize, expression: ?*const ast.Expr = null };
    fn producerBinding(bindings: []const ProducerBinding, name: []const u8) ?ProducerBinding {
        for (bindings) |binding| if (std.mem.eql(u8, binding.name, name)) return binding;
        return null;
    }
    fn referencesProducers(self: *Engine, expression: *const ast.Expr, bindings: []const ProducerBinding) !bool {
        for (bindings) |binding| {
            if (!@import("expressions.zig").references(expression, binding.name)) continue;
            var references: Names = .empty;
            try self.retainReferences(&references, &.{.{ .name = binding.name, .value = .null }}, expression);
            if (references.contains(binding.name)) return true;
        }
        return false;
    }
    /// Prove the complete source graph before evaluating any binding. Unknown
    /// consumers retain their WITH barrier; every deferred leaf is repeatable.
    fn producerFrameDepth(self: *Engine, expression: *const ast.Expr, bindings: []const ProducerBinding) anyerror!?usize {
        if (!try self.referencesProducers(expression, bindings))
            return if (reduction_plan.knownList(expression) and reduction_plan.repeatable(expression)) 1 else null;
        if (expression.value == .variable) return (producerBinding(bindings, expression.value.variable) orelse return null).depth;
        if (reduction_plan.reversedInput(expression)) |input| return (try self.producerFrameDepth(input, bindings) orelse return null) +| 1;
        if (expression.value == .binary and expression.value.binary.op == .add) {
            const addition = expression.value.binary;
            const left = if (try self.referencesProducers(addition.left, bindings)) try self.producerFrameDepth(addition.left, bindings) orelse return null else 1;
            const right = if (try self.referencesProducers(addition.right, bindings)) try self.producerFrameDepth(addition.right, bindings) orelse return null else 1;
            return @max(left, right) +| 1;
        }
        return null;
    }
    /// Substitute the whole lexical source frame, capturing ordinary operands
    /// once and preserving left-to-right validation even under NULL absorption.
    fn transformProducerFrame(self: *Engine, expression: *const ast.Expr, bindings: []const ProducerBinding, row: Row, captured: *std.ArrayList(v.Entry), alias: []const u8) anyerror!*const ast.Expr {
        if (expression.value == .variable) if (producerBinding(bindings, expression.value.variable)) |binding| return binding.expression.?;
        if (!try self.referencesProducers(expression, bindings)) return self.captureAddedProducer(expression, row, captured, alias);
        const result = try self.a.create(ast.Expr);
        if (reduction_plan.reversedInput(expression)) |input| {
            const child = try self.transformProducerFrame(input, bindings, row, captured, alias);
            result.* = .{ .span = expression.span, .depth = child.depth + 1, .value = .{ .call = .{ .name = "reverse", .args = try self.a.dupe(*const ast.Expr, &.{child}) } } };
        } else {
            const addition = expression.value.binary;
            const left = try self.transformProducerFrame(addition.left, bindings, row, captured, alias);
            const right = try self.transformProducerFrame(addition.right, bindings, row, captured, alias);
            result.* = .{ .span = expression.span, .depth = @max(left.depth, right.depth) + 1, .value = .{ .binary = .{ .op = .add, .left = left, .right = right } } };
        }
        // A transformed node is itself a validated closure. Later aliases
        // share its source descriptor and summary, rather than recursively
        // expanding the same expression DAG on every validation or proof.
        return self.captureProducerSource(result, captured.items, captured, alias);
    }
    fn forwardedClauses(self: *Engine, clauses: []const ast.Clause, index: usize, source: *const ast.Expr, ordinary: Row, captured: *std.ArrayList(v.Entry)) !ForwardedUnwind {
        const unwind = clauses[index].operation.unwind;
        const output = try self.a.alloc(ast.Clause, 2 + clauses.len - index - 1);
        output[0] = clauses[index];
        output[0].operation.unwind.expression = source;
        var items: std.ArrayList(ast.Item) = .empty;
        const binding = try self.a.create(ast.Expr);
        binding.* = .{ .span = clauses[index].span, .value = .{ .variable = unwind.alias } };
        try items.append(self.a, .{ .expression = binding, .alias = unwind.alias });
        for (ordinary) |entry| {
            if (std.mem.eql(u8, entry.name, unwind.alias)) continue;
            const reference = try self.captureProducerOperand(captured, entry.value, clauses[index].span, unwind.alias);
            try items.append(self.a, .{ .expression = reference, .alias = entry.name });
        }
        output[1] = .{ .span = clauses[0].span, .operation = .{ .project = .{ .final = false, .value = .{ .items = try items.toOwnedSlice(self.a) } } } };
        @memcpy(output[2..], clauses[index + 1 ..]);
        return .{ .clauses = output, .row = try captured.toOwnedSlice(self.a) };
    }
    fn producerEscapes(self: *Engine, bindings: []const ProducerBinding, unwind: anytype, tail: []const ast.Clause) !bool {
        for (bindings) |binding| if (!std.mem.eql(u8, binding.name, unwind.alias)) {
            var references: Names = .empty;
            try self.retainQueryReferences(&references, &.{.{ .name = binding.name, .value = .null }}, tail);
            if (references.contains(binding.name)) return true;
        };
        return false;
    }
    fn forwardUnwind(self: *Engine, clauses: []const ast.Clause, input: Row) !?ForwardedUnwind {
        if (!self.read_only or clauses.len < 2 or clauses[0].operation != .project) return null;
        if (try self.forwardProducerFrame(clauses, input)) |forwarded| return forwarded;
        // Nonrepeatable graph sources can still flow once through pure aliases.
        // They must never acquire an eager validation/replay closure.
        const first = clauses[0].operation.project;
        if (first.final or first.value.wildcard or first.value.distinct or first.value.predicate != null or first.value.order.len != 0 or first.value.skip != null or first.value.limit != null or first.value.items.len != 1) return null;
        const source = first.value.items[0].expression;
        if (!projectedSource(source) or reduction_plan.repeatable(source)) return null;
        var name = first.value.items[0].alias orelse return null;
        var index: usize = 1;
        while (index < clauses.len and clauses[index].operation == .project) : (index += 1) {
            const p = clauses[index].operation.project;
            if (p.final or p.value.wildcard or p.value.distinct or p.value.predicate != null or p.value.order.len != 0 or p.value.skip != null or p.value.limit != null or p.value.items.len != 1) return null;
            const item = p.value.items[0];
            if (item.expression.value != .variable or !std.mem.eql(u8, item.expression.value.variable, name)) return null;
            name = item.alias orelse name;
        }
        if (index + 1 >= clauses.len or clauses[index].operation != .unwind) return null;
        const unwind = clauses[index].operation.unwind;
        if (unwind.expression.value != .variable or !std.mem.eql(u8, unwind.expression.value.variable, name)) return null;
        if (try self.producerEscapes(&.{.{ .name = name, .depth = 1 }}, unwind, clauses[index + 1 ..])) return null;
        var captured: std.ArrayList(v.Entry) = .empty;
        try captured.appendSlice(self.a, input);
        return try self.forwardedClauses(clauses, index, source, &.{}, &captured);
    }
    fn forwardProducerFrame(self: *Engine, clauses: []const ast.Clause, input: Row) !?ForwardedUnwind {
        var bindings: []const ProducerBinding = &.{};
        var plans: std.ArrayList([]const bool) = .empty;
        var index: usize = 0;
        while (index < clauses.len and clauses[index].operation == .project) : (index += 1) {
            const step = clauses[index].operation.project;
            const p = step.value;
            if (step.final or p.wildcard or p.distinct or p.predicate != null or p.order.len != 0 or p.skip != null or p.limit != null or p.items.len == 0) return null;
            const deferred = try self.a.alloc(bool, p.items.len);
            var next: std.ArrayList(ProducerBinding) = .empty;
            for (p.items, deferred) |item, *selected| {
                if (aggregates(item.expression)) return null;
                const depth = try self.producerFrameDepth(item.expression, bindings);
                selected.* = depth != null;
                if (depth) |extent| {
                    if (extent > self.limits.recursion_depth) return null;
                    try next.append(self.a, .{ .name = item.alias orelse try self.expressionName(item.expression), .depth = extent });
                } else if (try self.referencesProducers(item.expression, bindings)) return null;
            }
            try plans.append(self.a, deferred);
            bindings = try next.toOwnedSlice(self.a);
        }
        if (bindings.len == 0 or index + 1 >= clauses.len or clauses[index].operation != .unwind) return null;
        const unwind = clauses[index].operation.unwind;
        const depth = try self.producerFrameDepth(unwind.expression, bindings) orelse return null;
        if (depth > self.limits.recursion_depth or try self.producerEscapes(bindings, unwind, clauses[index + 1 ..])) return null;
        var captured: std.ArrayList(v.Entry) = .empty;
        try captured.appendSlice(self.a, input);
        var current = input;
        bindings = &.{};
        for (plans.items, 0..) |deferred, stage| {
            var ordinary: std.ArrayList(v.Entry) = .empty;
            var next: std.ArrayList(ProducerBinding) = .empty;
            for (clauses[stage].operation.project.value.items, deferred) |item, selected| {
                const name = item.alias orelse try self.expressionName(item.expression);
                if (selected) {
                    const source = try self.transformProducerFrame(item.expression, bindings, current, &captured, unwind.alias);
                    try next.append(self.a, .{ .name = name, .depth = source.depth, .expression = source });
                } else try ordinary.append(self.a, .{ .name = name, .value = try self.eval(item.expression, current, null) });
            }
            current = try ordinary.toOwnedSlice(self.a);
            bindings = try next.toOwnedSlice(self.a);
        }
        const source = try self.transformProducerFrame(unwind.expression, bindings, current, &captured, unwind.alias);
        return try self.forwardedClauses(clauses, index, source, current, &captured);
    }
    fn runWeighted(self: *Engine, clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool) anyerror![]const Row {
        return (try self.runWeightedRows(clauses, initial, preserve_rows, false)).rows;
    }
    // Only a commutative terminal reduction licenses regrouping nonadjacent
    // bindings. Windows, order-sensitive reductions and procedures stay barriers.
    fn consolidatableCall(inner: []const ast.Clause) bool {
        // Identical complete parent bindings invoke the same repeatable child
        // from a unit bag. Child reducers and windows keep that unit scope;
        // only their returned bag is multiplied by the consolidated parent.
        // Unit bodies and effects never license this transformation.
        return pureReturningCall(inner);
    }
    fn consolidationDemand(self: *Engine, clauses: []const ast.Clause) !?Multiplicity {
        for (clauses) |clause| switch (clause.operation) {
            .match, .unwind => {},
            .subquery => |inner| if (!consolidatableCall(inner)) return null,
            .project => |p| {
                if (p.value.order.len != 0 or p.value.skip != null or p.value.limit != null) return null;
                var calls: reduction_plan.Calls = .{};
                for (p.value.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return null;
                if (calls.items.len == 0) continue;
                for (calls.items) |expression| {
                    const name = expression.value.call.name;
                    if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max") and !(self.numeric_consolidation and std.ascii.eqlIgnoreCase(name, "sum"))) return null;
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
    const StreamCapture = struct {
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        rows: std.ArrayList(Row) = .empty,
        weights: std.ArrayList(Count) = .empty,
        disabled: bool = false,
        fn deinit(self: *@This()) void {
            if (self.arena) |*arena| arena.deinit();
        }
        fn add(self: *@This(), engine: *Engine, row: Row, weight: Count) !void {
            if (self.disabled) return;
            self.capture(engine, row, weight) catch |err| {
                if (err != error.OutOfMemory or !self.budget.exhausted) return err;
                self.disabled = true;
            };
        }
        fn capture(self: *@This(), engine: *Engine, row: Row, weight: Count) !void {
            try engine.state();
            if (self.arena == null) self.arena = .init(self.budget.allocator());
            const a = self.arena.?.allocator();
            const owned = (try (v.Value{ .map = row }).clone(a)).map;
            try self.rows.append(a, owned);
            try self.weights.append(a, try weight.clone(a));
        }
    };
    const ApplyMemo = struct {
        memo: CallMemo,
        captures: ?Names = null,
        fn key(self: *@This(), engine: *Engine, row: Row, inner: []const ast.Clause, retained: A, temporary: A) !Row {
            if (self.memo.disabled) return &.{};
            if (self.captures == null) {
                const previous = engine.a;
                engine.a = retained;
                defer engine.a = previous;
                var names: Names = .empty;
                try engine.retainQueryReferences(&names, row, inner);
                self.captures = names;
            }
            var captured: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (self.captures.?.contains(entry.name)) try captured.append(temporary, entry);
            return captured.items;
        }
    };
    // One apply stage owns one bounded, snapshot-local memo. Keys include
    // only captured bindings and the child's row goal. Binding equality keeps
    // numeric representation (including nested 1 versus 1.0) observable.
    const CallMemo = struct {
        // Snapshot-local read-only expressions and CALLs can key entities by
        // identity. Mutable queries retain complete captured bindings.
        const Entry = struct { key: Row, goal: ?usize, children: WeightedRows, shared: ?WeightedRows = null, probe: bool = false };
        identity_keys: bool = false,
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        index: value_work.Index = .{},
        entries: std.ArrayList(Entry) = .empty,
        disabled: bool = false,
        fn deinit(memo: *CallMemo) void {
            if (memo.arena) |*arena| arena.deinit();
        }
        fn find(memo: *CallMemo, engine: *Engine, key: Row, goal: ?usize) !?WeightedRows {
            return memo.findKind(engine, key, goal, false);
        }
        fn findKind(memo: *CallMemo, engine: *Engine, key: Row, goal: ?usize, probe: bool) !?WeightedRows {
            if (memo.disabled) return null;
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork, .binding_identity = true };
            const hash = (try work.info(.{ .map = key })).hash;
            var candidate = memo.index.head(hash);
            while (candidate) |position| {
                const link = memo.index.links.items[position];
                const entry = &memo.entries.items[link.index];
                if (entry.probe == probe and entry.goal == goal and try work.eql(.{ .map = key }, .{ .map = entry.key })) {
                    if (engine.read_only and memo.identity_keys) {
                        if (entry.shared) |shared| return shared;
                        const rows = try engine.planning_allocator.alloc(Row, entry.children.rows.len);
                        const weights = try engine.planning_allocator.alloc(Count, rows.len);
                        const owner = try engine.statementOwnership();
                        var copies: v.CloneContext = .{ .a = engine.planning_allocator, .scratch = scratch.allocator(), .owned_parent = owner, .byte_pool = owner.byte_pool, .byte_allocator = engine.scratch_allocator };
                        for (rows, weights, entry.children.rows, entry.children.weights) |*row, *weight, old_row, old_weight| {
                            row.* = try cloneRowContext(old_row, &copies);
                            weight.* = try old_weight.clone(engine.planning_allocator);
                        }
                        try owner.adoptClones(&copies);
                        if (engine.capture_input_clones) |input| try input.captureInputStrings(&copies);
                        entry.shared = .{ .rows = rows, .weights = weights };
                        return entry.shared;
                    }
                    const rows = try engine.a.alloc(Row, entry.children.rows.len);
                    const weights = try engine.a.alloc(Count, rows.len);
                    for (rows, weights, entry.children.rows, entry.children.weights) |*row, *weight, old_row, old_weight| {
                        row.* = try engine.cloneRowScoped(old_row, engine.a);
                        weight.* = try old_weight.clone(engine.a);
                    }
                    return .{ .rows = rows, .weights = weights };
                }
                candidate = link.next;
            }
            return null;
        }
        fn put(memo: *CallMemo, engine: *Engine, key: Row, goal: ?usize, children: WeightedRows) !void {
            return memo.putKind(engine, key, goal, children, false);
        }
        fn putKind(memo: *CallMemo, engine: *Engine, key: Row, goal: ?usize, children: WeightedRows, probe: bool) !void {
            if (memo.disabled) return;
            memo.capture(engine, key, goal, children, probe) catch |err| {
                if (err != error.OutOfMemory or !memo.budget.exhausted) return err;
                // A cache admission failure must never fail an otherwise valid
                // query or leave partially built entries available to readers.
                memo.disabled = true;
            };
        }
        fn capture(memo: *CallMemo, engine: *Engine, key: Row, goal: ?usize, children: WeightedRows, probe: bool) !void {
            if (memo.arena == null) memo.arena = .init(memo.budget.allocator());
            const a = memo.arena.?.allocator();
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork, .binding_identity = true };
            const hash = (try work.info(.{ .map = key })).hash;
            const rows = try a.alloc(Row, children.rows.len);
            const weights = try a.alloc(Count, rows.len);
            // A read-only cache may borrow proven statement-owned results.
            // Other child values still receive complete memo-owned snapshots.
            // The root registry never retains recyclable memo source addresses.
            var results: v.CloneContext = .{ .a = a, .scratch = scratch.allocator(), .borrowed_parent = if (engine.read_only and memo.identity_keys) try engine.statementOwnership() else null };
            for (rows, weights, children.rows, children.weights) |*row, *weight, old_row, old_weight| {
                row.* = try cloneRowContext(old_row, &results);
                weight.* = try old_weight.clone(a);
            }
            var context: v.CloneContext = .{ .a = a, .scratch = scratch.allocator(), .identity_only = memo.identity_keys };
            const owned_key = (try context.clone(.{ .map = key })).map;
            try memo.entries.ensureUnusedCapacity(a, 1);
            try memo.index.insert(a, hash, memo.entries.items.len);
            memo.entries.appendAssumeCapacity(.{ .key = owned_key, .goal = goal, .children = .{ .rows = rows, .weights = weights }, .probe = probe });
        }
    };
    fn hasConsolidatableSum(self: *Engine, clauses: []const ast.Clause) !bool {
        for (clauses) |clause| switch (clause.operation) {
            .match, .unwind => {},
            .subquery => |inner| if (!consolidatableCall(inner)) return false,
            .project => |p| {
                if (p.value.order.len != 0 or p.value.skip != null or p.value.limit != null) return false;
                var calls: reduction_plan.Calls = .{};
                for (p.value.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return false;
                if (calls.items.len == 0) continue;
                var sum = false;
                for (calls.items) |expression| {
                    const name = expression.value.call.name;
                    if (std.ascii.eqlIgnoreCase(name, "sum")) sum = true else if (!std.ascii.eqlIgnoreCase(name, "count") and !std.ascii.eqlIgnoreCase(name, "min") and !std.ascii.eqlIgnoreCase(name, "max")) return false;
                }
                return sum;
            },
            else => return false,
        };
        return false;
    }
    fn runWeightedRows(self: *Engine, clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool, retain_final: bool) anyerror!WeightedRows {
        if (!self.numeric_attempted and self.read_only and self.stream_fold == null and self.weighted_sink == null and weightedExpression(clauses) and try self.hasConsolidatableSum(clauses)) {
            // A successful integer-only, one-sign-per-group reduction is independent of
            // contribution order, including its overflow outcome. Use a private
            // arena and publish only after all groups certify. Mixed signs,
            // floating point and semantic errors retry the ordinary executor;
            // budgets/cancellation retain their charged work and propagate.
            self.numeric_attempted = true;
            defer self.numeric_attempted = false;
            if (try self.tryNumericConsolidation(clauses, initial, preserve_rows, retain_final)) |result| return result;
        }
        return self.runWeightedRowsImpl(clauses, initial, preserve_rows, retain_final);
    }
    fn tryNumericConsolidation(self: *Engine, clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool, retain_final: bool) anyerror!?WeightedRows {
        var arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer arena.deinit();
        const scope = EvalScope.enter(self, arena.allocator());
        var scope_active = true;
        defer if (scope_active) scope.leave(self);
        const columns = self.columns;
        const stages = self.stages;
        const pending = self.pending_window;
        self.stages = .empty;
        self.pending_window = null;
        self.numeric_consolidation = true;
        defer self.numeric_consolidation = false;
        var published = false;
        defer if (!published) {
            self.columns = columns;
            self.stages = stages;
            self.pending_window = pending;
        };
        const result = self.runWeightedRowsImpl(clauses, initial, preserve_rows, retain_final) catch |err| switch (err) {
            error.CypherNumericRegroupingDeclined, error.CypherNumericOverflow, error.CypherTypeMismatch, error.UnboundCypherVariable, error.MissingCypherParameter, error.CypherInvalidArgument, error.CypherDivisionByZero => return null,
            else => return err,
        };
        const result_columns = self.columns;
        const result_stages = self.stages.items;
        scope.leave(self);
        scope_active = false;
        self.stages = stages;
        self.pending_window = pending;
        const rows = try self.a.alloc(Row, result.rows.len);
        const weights = try self.a.alloc(Count, rows.len);
        for (rows, weights, result.rows, result.weights) |*row, *weight, source, count_value| {
            row.* = (try (v.Value{ .map = source }).clone(self.a)).map;
            weight.* = try count_value.clone(self.a);
        }
        const owned_columns = try self.a.alloc([]const u8, result_columns.len);
        for (owned_columns, result_columns) |*name, old| name.* = try self.a.dupe(u8, old);
        self.columns = owned_columns;
        try self.stages.appendSlice(self.a, result_stages);
        published = true;
        return .{ .rows = rows, .weights = weights };
    }
    fn runWeightedRowsImpl(self: *Engine, original_clauses: []const ast.Clause, initial: WeightedRows, preserve_rows: bool, retain_final: bool) anyerror!WeightedRows {
        const source_mark = self.producer_sources.items.len;
        defer self.producer_sources.shrinkRetainingCapacity(source_mark);
        var clauses = original_clauses;
        var bags = initial;
        const saved_retention = self.retain_windows;
        defer self.retain_windows = saved_retention;
        var clause_index: usize = 0;
        while (clause_index < clauses.len) : (clause_index += 1) {
            var clause = clauses[clause_index];
            try self.checkpoint();
            if (clause.operation == .project and bags.rows.len == 1 and bags.weights[0].isOne()) if (try self.forwardUnwind(clauses[clause_index..], bags.rows[0])) |forwarded| {
                bags.rows = try self.a.dupe(Row, &.{forwarded.row});
                clauses = forwarded.clauses;
                clause_index = 0;
                clause = clauses[0];
            };
            if ((clause.operation == .match or clause.operation == .subquery) and bags.rows.len > 1) if (try self.consolidationDemand(clauses[clause_index..])) |demand| {
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
                        if (following.operation == .unwind or following.operation == .match or (following.operation == .subquery and pureReturningCall(following.operation.subquery))) continue;
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
                    var fused = false;
                    if (orderedReductionEnd(clauses[clause_index..])) |relative| {
                        const index = clause_index + relative;
                        const following = clauses[index];
                        self.retain_windows = (retain_final and index + 1 == clauses.len) or (!following.operation.project.final and self.weightedTail(clauses[index + 1 ..]));
                        if (try self.streamMatchProjectionWeighted(bags.rows, bags.weights, .{ .paths = &.{} }, following.operation.project.value, clauses[clause_index..index], .{}, following.span)) |output| {
                            if (self.pending_window) |window| {
                                bags = window;
                                self.pending_window = null;
                            } else {
                                const weights = try self.a.alloc(Count, output.len);
                                @memset(weights, Count.one);
                                bags = .{ .rows = output, .weights = weights };
                            }
                            clause_index = index;
                            continue;
                        }
                    }
                    for (clauses[clause_index + 1 ..], clause_index + 1..) |following, index| {
                        if (following.operation == .unwind or following.operation == .match or (following.operation == .subquery and pureReturningCall(following.operation.subquery))) continue;
                        if (following.operation != .project) break;
                        const projection = following.operation.project.value;
                        var reduction = false;
                        for (projection.items) |item| reduction = reduction or aggregates(item.expression);
                        if (!following.operation.project.final and !reduction and projection.limit == null and projection.order.len == 0) continue;
                        self.retain_windows = (retain_final and index + 1 == clauses.len) or (!following.operation.project.final and self.weightedTail(clauses[index + 1 ..]));
                        if (try self.streamMatchProjectionWeighted(bags.rows, bags.weights, .{ .paths = &.{} }, projection, clauses[clause_index..index], .{}, following.span)) |output| {
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
                    for (bags.rows, bags.weights) |row, weight| {
                        var source = (try self.unwindSource(u.expression, row)) orelse continue;
                        while (source.next()) |item| {
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
                    var calls: reduction_plan.Calls = .{};
                    for (projection.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return error.CypherInvalidAggregate;
                    for (projection.order) |order| if (!try self.collectStreamingAggregates(order.expression, &calls)) return error.CypherInvalidAggregate;
                    const grouped = projection.distinct or calls.items.len != 0;
                    if (grouped and projection.wildcard) return error.CypherAggregateWildcard;
                    var top: ?TopK = if (grouped) null else try TopK.init(self, projection, if (bags.rows.len == 0) &.{} else bags.rows[0]);
                    defer if (top) |*value| value.deinit();
                    var groups: std.ArrayList(StreamGroup) = .empty;
                    var group_index: value_work.Index = .{};
                    defer for (groups.items) |*group| group.deinit();
                    const scalar_groups = blk: {
                        for (bags.weights) |weight| if (!weight.isOne()) break :blk false;
                        break :blk true;
                    };
                    var group_window = if (grouped) try GroupWindow.init(self, projection, scalar_groups or (projection.distinct and calls.items.len == 0)) else null;
                    defer if (group_window) |*window| {
                        window.heap.deinit(self.scratch_allocator);
                        group_index.deinit(self.scratch_allocator);
                    };
                    // DISTINCT over an empty binding still has one empty
                    // key per input, and no group at all for empty input.
                    var has_keys = projection.distinct and calls.items.len == 0;
                    for (projection.items) |item| has_keys = has_keys or !aggregates(item.expression);
                    if (calls.items.len != 0 and !has_keys) try groups.append(self.a, try self.newStreamGroup(self.a, &.{}, &.{}, null, projection, calls.items, calls.distinct_leaders.items));
                    var matched: usize = 0;
                    var counts = [_]usize{0};
                    const retention: GroupRetention = if (grouped) try self.groupRetention(projection) else .{ .a = self.a };
                    var fold: StreamFold = .{ .retention = &retention, .retained = self.a, .middle = &.{}, .projection = projection, .calls = calls.items, .membership_leaders = calls.distinct_leaders.items, .group_window = if (group_window) |*window| window else null, .call_aliases = &calls.index.pointers, .top = &top, .groups = &groups, .group_index = &group_index, .has_keys = has_keys, .stage_counts = &counts, .matched = &matched, .distinct = &.{} };
                    for (bags.rows, bags.weights) |row, weight| {
                        fold.weight = weight;
                        fold.add(self, row) catch |err| switch (err) {
                            error.CypherStreamComplete => break,
                            else => return err,
                        };
                    }
                    const rows = if (top) |*value| try value.finish(true) else try self.finishStreamGroups(groups.items, &calls, &retention, projection);
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
                    var apply: ApplyMemo = .{ .memo = .{ .identity_keys = self.read_only, .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 16) }, .disabled = !weightedExpression(inner) or unit } };
                    defer apply.memo.deinit();
                    for (bags.rows, bags.weights) |row, weight| {
                        if (goal) |bound| if (produced >= bound) break;
                        const child_goal: ?usize = if (goal) |bound| blk: {
                            const remaining = bound - produced;
                            const parent: usize = @intCast(@max(1, weight.cap(remaining)));
                            break :blk remaining / parent + @intFromBool(remaining % parent != 0);
                        } else null;
                        const body = if (child_goal) |bound| try self.boundedCallBody(inner, bound) else inner;
                        var key_arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
                        defer key_arena.deinit();
                        const key = try apply.key(self, row, inner, self.a, key_arena.allocator());
                        const cached = try apply.memo.find(self, key, child_goal);
                        const scope = PatternScope.enter(self);
                        const children = cached orelse blk: {
                            defer scope.leave(self);
                            self.terminal_weight_demand = if (unit) .presence else demand;
                            const tail: SummaryTail = .{ .clauses = clauses[clause_index + 1 ..], .parent = scope.summary_tail };
                            self.summary_tail = if (unit) null else &tail;
                            break :blk try self.runWeightedRows(body, .{ .rows = &.{row}, .weights = &.{Count.one} }, true, true);
                        };
                        // A cache hit does not enter the child's pattern scope.
                        if (cached != null) scope.leave(self);
                        if (cached == null) try apply.memo.put(self, key, child_goal, children);
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
    fn run(self: *Engine, original_clauses: []const ast.Clause, input: []const Row, explain: bool, preserve_rows: bool) anyerror![]const Row {
        const source_mark = self.producer_sources.items.len;
        defer self.producer_sources.shrinkRetainingCapacity(source_mark);
        var clauses = original_clauses;
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
        if (!explain and self.weightedTail(clauses) and ((clauses[0].operation != .match and hasCorrelatedBoundary(clauses)) or separatedMatches(clauses) or hasStreamableCall(clauses) or hasOrderedIntermediate(clauses))) {
            const weights = try self.a.alloc(Count, input.len);
            @memset(weights, Count.one);
            return self.runWeighted(clauses, .{ .rows = input, .weights = weights }, preserve_rows);
        }
        if (!explain and input.len == 1 and input[0].len == 0 and clauses.len >= 2 and clauses[0].operation == .match and clauses[0].operation.match.paths.len == 1) {
            for (clauses[1..], 1..) |clause, index| {
                if (clause.operation == .unwind or clause.operation == .match or (clause.operation == .subquery and pureReturningCall(clause.operation.subquery))) continue;
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
        var clause_index: usize = 0;
        while (clause_index < clauses.len) : (clause_index += 1) {
            var clause = clauses[clause_index];
            try self.checkpoint();
            if (!explain and clause.operation == .project and rows.len == 1) if (try self.forwardUnwind(clauses[clause_index..], rows[0])) |forwarded| {
                rows = try self.a.dupe(Row, &.{forwarded.row});
                clauses = forwarded.clauses;
                clause_index = 0;
                clause = clauses[0];
            };
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
                    if (following.operation == .unwind or following.operation == .match or (following.operation == .subquery and pureReturningCall(following.operation.subquery))) continue;
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
            if (!explain and clause.operation == .unwind) {
                for (clauses[clause_index + 1 ..], clause_index + 1..) |next, index| {
                    if (next.operation == .unwind or next.operation == .match or (next.operation == .subquery and pureReturningCall(next.operation.subquery))) continue;
                    if (next.operation != .project) break;
                    const projection = next.operation.project.value;
                    var reduction = false;
                    for (projection.items) |item| reduction = reduction or aggregates(item.expression);
                    if (!next.operation.project.final and !reduction and projection.limit == null and projection.order.len == 0) continue;
                    if (try self.streamMatchProjection(rows, .{ .paths = &.{} }, projection, clauses[clause_index..index], .{}, next.span)) |streamed| {
                        if (index + 1 < clauses.len) return self.runNext(clauses[index + 1 ..], streamed, preserve_rows);
                        return streamed;
                    }
                    break;
                }
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
                        var source = (try self.unwindSource(u.expression, row)) orelse continue;
                        while (source.next()) |item| {
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
                    bound = try self.pathSummaryRow(bound, path, node, node, 0);
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
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| try aliases.append(self.a, name);
            for (path.edges) |edge| if (edge.execution_length) |name| try aliases.append(self.a, name);
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
                    inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                        extended = try self.replace(extended, name, .null);
                    };
                    for (path.edges) |edge| if (edge.execution_length) |name| {
                        extended = try self.replace(extended, name, .null);
                    };
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
        if (expression.execution_range_binding) return null; // The carrier is not a logical list constant.
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
    fn countAllPaths(self: *Engine, input: MatchState, original_path: ast.Path, mode: ast.PathMode, starts: []const *const v.Node, output: *std.ArrayList(MatchState)) anyerror!bool {
        const path = if (self.stream_fold) |fold| try path_summaries.prune(self.a, original_path, .{ fold.middle, fold.projection, self.stream_predicate }) else original_path;
        const saved_calls = self.moment_calls;
        self.moment_calls = null;
        defer self.moment_calls = saved_calls;
        const saved_checks = self.moment_checks;
        self.moment_checks = .empty;
        defer self.moment_checks = saved_checks;
        const prior_input = self.moment_input;
        const prior_endpoint = self.moment_endpoint;
        const prior_invariants = self.moment_invariants;
        const prior_representatives = self.moment_representatives;
        const prior_axis = self.moment_total_axis;
        self.moment_input = input.row;
        self.moment_endpoint = path.nodes[path.nodes.len - 1].alias;
        self.moment_invariants = .empty;
        self.moment_representatives = false;
        defer {
            self.moment_input = prior_input;
            self.moment_endpoint = prior_endpoint;
            self.moment_invariants = prior_invariants;
            self.moment_representatives = prior_representatives;
            self.moment_total_axis = prior_axis;
        }
        if (try self.planStatistics(path, true)) |plan| {
            const saved_plan = self.statistic_plan;
            const saved_targets = self.statistic_targets;
            self.statistic_plan = plan;
            self.statistic_targets = try self.summaryTargets(path);
            defer {
                self.statistic_plan = saved_plan;
                self.statistic_targets = saved_targets;
            }
            return self.countAllPathsTyped(Statistic.Value, input, path, mode, starts, output);
        }
        if (try self.momentTargets(path)) |targets| {
            const saved = self.moment_targets;
            self.moment_targets = targets;
            defer self.moment_targets = saved;
            if (self.countAllPathsTyped(PathMoments, input, path, mode, starts, output)) |result| return result else |err| switch (err) {
                // The entire endpoint set is certified before publishing any
                // contribution. Noninteger properties retain overloaded Cypher
                // operations through the exact distribution executor.
                error.CypherMomentTypeDeclined => {},
                else => return err,
            }
            self.moment_calls = null;
        }
        // Native and SQL exact consumers keep arbitrary-precision counts.
        // Bounded and presence consumers retain the compact u64 DP.
        if (if (self.weighted_sink) |sink| sink.multiplicity == .exact else self.exactPathWeights())
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
    const Statistic = @import("statistic.zig");
    const StatisticSupport = Statistic.SupportInput;
    const StatisticNames = std.StringHashMapUnmanaged(Statistic.Form);
    const StatisticPlan = struct { kernel: Statistic.Plan, calls: []const *const ast.Expr = &.{}, call_slots: []const usize = &.{}, global: bool = false };
    fn statisticReferences(value: anytype, names: StatisticNames) bool {
        var keys = names.iterator();
        while (keys.next()) |entry| {
            var varying = false;
            for (entry.value_ptr.coefficients[1..]) |coefficient| varying = varying or coefficient != 0;
            if (varying and path_summaries.references(value, entry.key_ptr.*)) return true;
        }
        return false;
    }
    fn statisticArgument(self: *Engine, expression: *const ast.Expr, names: StatisticNames, dimensions: usize) anyerror!?Statistic.Form {
        return switch (expression.value) {
            .variable => |name| if (names.get(name)) |form| form else if (lookup(self.moment_input, name)) |value| (if (value == .integer) try Statistic.Form.literal(self.a, value.integer, dimensions) else null) else null,
            .parameter => |name| if (lookup(self.parameters, name)) |value| (if (value == .integer) try Statistic.Form.literal(self.a, value.integer, dimensions) else null) else null,
            .literal => |value| if (value == .integer) try Statistic.Form.literal(self.a, value.integer, dimensions) else null,
            .unary => |u| blk: {
                const operand = (try self.statisticArgument(u.operand, names, dimensions)) orelse break :blk null;
                break :blk switch (u.op) {
                    .positive => operand,
                    .negative => try operand.negative(self.a),
                    else => null,
                };
            },
            .binary => |b| blk: {
                const op: Statistic.Op = switch (b.op) {
                    .add => .add,
                    .subtract => .subtract,
                    .multiply => .multiply,
                    else => break :blk null,
                };
                const l = (try self.statisticArgument(b.left, names, dimensions)) orelse break :blk null;
                const r = (try self.statisticArgument(b.right, names, dimensions)) orelse break :blk null;
                break :blk try Statistic.Form.combine(l, r, self.a, op);
            },
            else => null,
        };
    }
    fn statisticProjection(self: *Engine, p: ast.Projection, names: StatisticNames, n: usize, global: *bool) !?StatisticNames {
        if (p.distinct or p.order.len != 0 or p.skip != null or p.limit != null) return null;
        var next: StatisticNames = if (p.wildcard) try names.clone(self.a) else .empty;
        for (p.items) |item| {
            if (aggregates(item.expression)) return null;
            if (try self.statisticArgument(item.expression, names, n)) |f| try next.put(self.a, item.alias orelse try self.expressionName(item.expression), f) else if (statisticReferences(item.expression, names)) return null else global.* = false;
        }
        if (statisticReferences(p.predicate, names) or statisticReferences(p.predicate, next)) return null;
        if (p.predicate) |predicate| if (self.constant(predicate, self.moment_input) == null) {
            global.* = false;
        };
        return next;
    }
    fn statisticMiddle(self: *Engine, clauses: []const ast.Clause, initial: StatisticNames, n: usize, global: *bool) anyerror!?StatisticNames {
        var names = initial;
        for (clauses) |clause| switch (clause.operation) {
            .project => |p| names = (try self.statisticProjection(p.value, names, n, global)) orelse return null,
            .subquery => |inner| {
                const exported = (try self.statisticMiddle(inner, names, n, global)) orelse return null;
                var exports = exported.iterator();
                while (exports.next()) |entry| try names.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
            },
            .unwind => |u| {
                if (statisticReferences(u.expression, names) or self.constant(u.expression, self.moment_input) == null) return null;
                _ = names.remove(u.alias);
            },
            else => return null,
        };
        return names;
    }
    fn statisticBenefit(form: Statistic.Form, kind: Statistic.Kind, path: ast.Path) bool {
        if (form.quadratic() or kind == .distinct) return true;
        if (kind != .min and kind != .max) return false;
        var coefficient: ?i64 = null;
        var different = false;
        var nonzero: usize = 0;
        for (path.edges, 0..) |edge, index| if (fixedHops(edge) == null) {
            const c = form.coefficients[1 + index];
            if (coefficient) |prior| {
                different = different or c != prior;
            } else coefficient = c;
            nonzero += @intFromBool(c != 0);
        };
        // Independent single-marginal extrema already have a cheaper plan.
        return different and nonzero > 1;
    }
    fn planStatistics(self: *Engine, path: ast.Path, require_benefit: bool) !?StatisticPlan {
        if (path.edges.len == 0 or path.edges.len > 32 or self.component_leaves.len != 0 or path.selector != .all or self.shortest_count != null) return null;
        const targets = try self.summaryTargets(path);
        if (targets.len == 0) return null;
        const statistic_depth = std.math.cast(i64, self.limits.path_depth) orelse return null;
        var names: StatisticNames = .empty;
        for (targets) |target| try names.put(self.a, target.name, try Statistic.Form.column(self.a, path.edges.len, target.edge, statistic_depth));
        if (statisticReferences(self.stream_predicate, names)) return null;
        if (self.external_statistic) |requests| {
            var kernel = try Statistic.Plan.init(self.a, requests);
            kernel.ptr = self;
            kernel.charge = chargeWork;
            return .{ .kernel = kernel, .global = self.external_statistic_global };
        }
        if (self.weighted_sink != null) return null;
        const fold = self.stream_fold orelse return null;
        if (fold.continuation != null or fold.calls.len == 0) return null;
        var global = true;
        names = (try self.statisticMiddle(fold.middle, names, path.edges.len, &global)) orelse return null;
        for (fold.projection.items) |item| if (!aggregates(item.expression) and statisticReferences(item.expression, names)) return null;
        if (statisticReferences(fold.projection.predicate, names) or statisticReferences(fold.projection.order, names)) return null;
        var requests: std.ArrayList(Statistic.Request) = .empty;
        var calls: std.ArrayList(*const ast.Expr) = .empty;
        var call_slots: std.ArrayList(usize) = .empty;
        var benefit = false;
        for (fold.calls, 0..) |expression, index| {
            if (fold.active_calls) |active| if (!active[index]) continue;
            const f = expression.value.call;
            if (!f.star and f.args.len != 1) return null;
            const kind: Statistic.Kind = if (f.distinct) (if (std.ascii.eqlIgnoreCase(f.name, "count")) .distinct else return null) else kind: {
                inline for (@typeInfo(Statistic.Kind).@"enum".field_names) |name| {
                    if (std.ascii.eqlIgnoreCase(f.name, name)) break :kind @field(Statistic.Kind, name);
                }
                return null;
            };
            const form = if (f.star and kind == .count) try Statistic.Form.literal(self.a, 0, path.edges.len) else (try self.statisticArgument(f.args[0], names, path.edges.len)) orelse return null;
            if (!form.eligible(kind)) return null;
            try requests.append(self.a, .{ .form = form, .kind = kind });
            try calls.append(self.a, expression);
            try call_slots.append(self.a, index);
            benefit = benefit or statisticBenefit(form, kind, path);
        }
        if (requests.items.len == 0 or (require_benefit and !benefit)) return null;
        var kernel = try Statistic.Plan.init(self.a, requests.items);
        kernel.ptr = self;
        kernel.charge = chargeWork;
        return .{ .kernel = kernel, .calls = calls.items, .call_slots = call_slots.items, .global = !fold.has_keys and global };
    }
    fn emitStatistics(self: *Engine, initial: Row, value: Statistic.Value) !void {
        var row = initial;
        for (self.statistic_targets) |target| row = try self.lengthRow(row, target.name, 0);
        const plan = self.statistic_plan.?;
        const arguments = try self.a.alloc(Statistic.Argument, plan.kernel.requests.len);
        for (plan.kernel.requests, arguments, 0..) |request, *argument, i| {
            argument.* = .{ .slot = if (plan.call_slots.len == 0) i else plan.call_slots[i] };
            switch (request.kind) {
                .count => argument.weight = value.count,
                .min, .max => {
                    const e = value.extreme(plan.kernel, i);
                    argument.value = std.math.cast(i64, @as(i128, request.form.coefficients[0]) + (if (request.kind == .min) e.min else e.max)) orelse return error.CypherStatisticDeclined;
                },
                .distinct => argument.support = .{ .value = value.support(plan.kernel, i), .offset = request.form.coefficients[0] },
                .sum, .avg => {
                    const total = try (try value.total(self.a, plan.kernel, i)).magnitude(self.a);
                    const divided = try total.amount.divRem(self.a, value.count);
                    const q = divided.quotient.exact() orelse return error.CypherNumericOverflow;
                    argument.value = std.math.cast(i64, if (total.negative) -@as(i128, q) else @as(i128, q)) orelse return error.CypherNumericOverflow;
                    argument.weight = try value.count.difference(self.a, divided.remainder);
                    if (divided.remainder.cap(1) != 0) argument.extra = .{ .value = std.math.add(i64, argument.value, if (total.negative) -1 else 1) catch return error.CypherNumericOverflow, .weight = divided.remainder };
                },
            }
        }
        try self.state();
        const prior = self.statistic_packet;
        self.statistic_packet = arguments;
        defer self.statistic_packet = prior;
        if (self.weighted_sink) |sink| return sink.emit(sink.ptr, row, Count.one);
        try self.stream_fold.?.add(self, row);
    }
    const MomentCall = enum { count, sum, avg, min, max };
    const DirectMoment = struct { kind: MomentCall, form: Affine, expression: *const ast.Expr };
    const Affine = @import("integer_affine.zig").Form;
    // Entity identity and opaque shadows are distinct proofs. A transparent
    // alias keeps its original bag binding without reviving a shadowed value.
    const MomentBinding = union(enum) { scalar: Affine, entity: []const u8, blocked };
    const MomentNames = std.StringHashMapUnmanaged(MomentBinding);
    const SummaryTarget = struct { name: []const u8, edge: ?usize = null, zero: bool = false, edges: ?[]const bool = null };
    const MomentStats = struct { sum: Count = Count.init(0), min: usize = 0, max: usize = 0 };
    /// The fields are independent marginals. Their joint distribution is never
    /// synthesized unless every downstream consumer observes only a certified
    /// count, first moment or extremum of each individual field.
    const PathMoments = struct {
        count: Count,
        parts: []const MomentStats = &.{},
        fn init(amount: Count) @This() {
            return .{ .count = amount };
        }
        fn part(self: @This(), i: usize) MomentStats {
            return if (i < self.parts.len) self.parts[i] else .{};
        }
        fn add(left: @This(), a: A, right: @This()) !@This() {
            if (left.count.cap(1) == 0) return right;
            if (right.count.cap(1) == 0) return left;
            const parts = try a.alloc(MomentStats, @max(left.parts.len, right.parts.len));
            for (parts, 0..) |*out, i| {
                const l = left.part(i);
                const r = right.part(i);
                out.* = .{ .sum = try l.sum.add(a, r.sum), .min = @min(l.min, r.min), .max = @max(l.max, r.max) };
            }
            return .{ .count = try left.count.add(a, right.count), .parts = parts };
        }
        fn multiply(left: @This(), a: A, right: @This()) !@This() {
            const parts = try a.alloc(MomentStats, @max(left.parts.len, right.parts.len));
            for (parts, 0..) |*out, i| {
                const l = left.part(i);
                const r = right.part(i);
                out.* = .{ .sum = try (try l.sum.multiply(a, right.count)).add(a, try r.sum.multiply(a, left.count)), .min = l.min + r.min, .max = l.max + r.max };
            }
            return .{ .count = try left.count.multiply(a, right.count), .parts = parts };
        }
        fn advance(self: @This(), a: A, targets: []const SummaryTarget, edge: usize) !@This() {
            const parts = try a.alloc(MomentStats, targets.len);
            for (parts, targets, 0..) |*out, target, i| {
                out.* = self.part(i);
                if (!target.zero and (if (target.edges) |edges| edges[edge] else target.edge == null or target.edge.? == edge)) {
                    out.sum = try out.sum.add(a, self.count);
                    out.min += 1;
                    out.max += 1;
                }
            }
            return .{ .count = self.count, .parts = parts };
        }
    };
    fn referencesAny(value: anytype, names: MomentNames) bool {
        var keys = names.keyIterator();
        while (keys.next()) |key| {
            const binding = names.get(key.*).?;
            if (binding == .scalar and binding.scalar.field != null and path_summaries.references(value, key.*)) return true;
        }
        return false;
    }
    fn momentEntity(self: *Engine, expression: *const ast.Expr, names: MomentNames) ?[]const u8 {
        if (expression.value != .variable) return null;
        const name = expression.value.variable;
        if (names.get(name)) |binding| return if (binding == .entity) binding.entity else null;
        if (lookup(self.moment_input, name)) |value| {
            if (value == .node or value == .map or value == .relationship) return name;
            return null;
        }
        if (self.moment_endpoint) |endpoint| if (std.mem.eql(u8, endpoint, name)) return name;
        return null;
    }
    const Numeric = @import("linear_numeric.zig");
    fn numericLiteral(value: v.Value) ?Numeric.Form {
        return switch (value) {
            .integer => |n| Numeric.Form.literal(.integer(n)),
            .number => |n| Numeric.Form.literal(Numeric.Lattice.number(n) orelse return null),
            else => null,
        };
    }
    fn numericAffine(proof: Numeric.Form, field: ?usize, multiple: bool) Affine {
        return .{ .field = field, .multiple = multiple, .symbolic = true, .floating = proof.range.floating, .numeric = proof, .sign = if (proof.range.low >= 0) .nonnegative else if (proof.range.high <= 0) .nonpositive else .unknown };
    }
    fn momentLiteral(value: v.Value) ?Affine {
        const proof = numericLiteral(value) orelse return null;
        if (value == .integer) {
            var form = Affine.literal(value.integer);
            form.numeric = proof;
            return form;
        }
        return numericAffine(proof, null, false);
    }
    fn momentArgument(self: *Engine, expression: *const ast.Expr, names: MomentNames) anyerror!?Affine {
        return switch (expression.value) {
            .variable => |name| if (names.get(name)) |binding| (if (binding == .scalar) binding.scalar else null) else if (lookup(self.moment_input, name)) |value| momentLiteral(value) else null,
            .parameter => |name| if (lookup(self.parameters, name)) |value| momentLiteral(value) else null,
            .property => |property| if (self.momentEntity(property.base, names) != null) Affine.invariant() else null,
            .literal => |value| switch (value) {
                .integer => |n| momentLiteral(.{ .integer = n }),
                .number => |n| momentLiteral(.{ .number = n }),
                else => null,
            },
            .unary => |u| blk: {
                const operand = (try self.momentArgument(u.operand, names)) orelse return null;
                if (u.op == .positive) break :blk operand;
                if (u.op != .negative) return null;
                const numeric = if (operand.numeric) |n| try n.negative(self.a) else null;
                if (!operand.floating) if (operand.negative()) |original| {
                    var result = original;
                    result.numeric = numeric;
                    break :blk result;
                };
                break :blk numericAffine(numeric orelse return null, operand.field, operand.multiple);
            },
            .binary => |b| blk: {
                const op: Numeric.Op = switch (b.op) {
                    .add => .add,
                    .subtract => .subtract,
                    .multiply => .multiply,
                    .divide => .divide,
                    else => return null,
                };
                const left = (try self.momentArgument(b.left, names)) orelse return null;
                const right = (try self.momentArgument(b.right, names)) orelse return null;
                const numeric = if (left.numeric) |l| (if (right.numeric) |r| try Numeric.Form.combine(l, r, self.a, op) else null) else null;
                if (!left.floating and !right.floating and op != .divide) {
                    const original = switch (op) {
                        .add => Affine.combineMarginals(left, right, .add),
                        .subtract => Affine.combineMarginals(left, right, .subtract),
                        .multiply => Affine.combineMarginals(left, right, .multiply),
                        .divide => unreachable,
                    };
                    if (original) |form| {
                        var result = form;
                        result.numeric = numeric;
                        break :blk result;
                    }
                }
                break :blk numericAffine(numeric orelse return null, left.field orelse right.field, left.multiple or right.multiple or (left.field != null and right.field != null and left.field.? != right.field.?));
            },
            else => null,
        };
    }
    fn retainMomentChecks(self: *Engine, expression: *const ast.Expr, names: MomentNames) anyerror!void {
        const optional = try self.momentArgument(expression, names);
        if (optional) |form| if (form.symbolic) {
            self.moment_representatives = true;
        };
        switch (expression.value) {
            .unary => |u| try self.retainMomentChecks(u.operand, names),
            .binary => |b| {
                try self.retainMomentChecks(b.left, names);
                try self.retainMomentChecks(b.right, names);
            },
            .property => {
                const form = optional orelse return;
                if (form.symbolic) {
                    // Admission uses the producer's binding; evaluation still
                    // uses the original alias and ordinary read projection.
                    const property = expression.value.property;
                    const origin = self.momentEntity(property.base, names).?;
                    for (self.moment_invariants.items) |prior| {
                        const entry = prior.value.property;
                        if (std.mem.eql(u8, entry.base.value.variable, origin) and std.mem.eql(u8, entry.name, property.name)) return;
                    }
                    const base = try self.a.create(ast.Expr);
                    base.* = .{ .span = property.base.span, .value = .{ .variable = origin } };
                    const admitted = try self.a.create(ast.Expr);
                    admitted.* = .{ .span = expression.span, .value = .{ .property = .{ .base = base, .name = property.name } } };
                    try self.moment_invariants.append(self.a, admitted);
                }
                return;
            },
            else => return,
        }
        const form = optional orelse return;
        if (form.field != null) try self.moment_checks.put(self.a, expression, form);
    }
    fn validateMomentNode(self: *Engine, expression: *const ast.Expr) !void {
        const active = self.active_moments orelse return;
        const form = active.checks.get(expression) orelse return;
        const stats = active.values.part(form.field.?);
        _ = try affineValue(form, stats.min);
        _ = try affineValue(form, stats.max);
    }
    fn validateMomentInput(self: *Engine, expression: *const ast.Expr) anyerror!void {
        try self.validateMomentNode(expression);
        switch (expression.value) {
            .unary => |u| try self.validateMomentInput(u.operand),
            .binary => |b| {
                try self.validateMomentInput(b.left);
                try self.validateMomentInput(b.right);
            },
            else => {},
        }
    }
    fn hasMomentCheck(value: anytype, checks: std.AutoHashMapUnmanaged(*const ast.Expr, Affine)) bool {
        const T = @TypeOf(value);
        if (T == *const ast.Expr) if (checks.contains(value)) return true;
        switch (@typeInfo(T)) {
            .pointer => |pointer| {
                if (pointer.size == .one and pointer.child == ast.Expr) return hasMomentCheck(value.*, checks);
                if (pointer.size == .slice and pointer.child != u8) for (value) |child| if (hasMomentCheck(child, checks)) return true;
            },
            .@"struct" => |info| inline for (info.field_names) |field| {
                if (hasMomentCheck(@field(value, field), checks)) return true;
            },
            .@"union" => switch (value) {
                inline else => |child| return hasMomentCheck(child, checks),
            },
            .optional => if (value) |child| return hasMomentCheck(child, checks),
            else => {},
        }
        return false;
    }
    fn momentLocal(self: *Engine, clauses: []const ast.Clause) bool {
        // Alias-only CALLs have no interval-dependent failures. A CALL that
        // computes affine values uses representative bags so memo keys retain
        // every actual extremum and its original lazy error boundary.
        for (clauses) |clause| if (clause.operation == .subquery and hasMomentCheck(clause.operation.subquery, self.moment_checks)) return false;
        return true;
    }
    fn affineValue(form: Affine, input: usize) !i64 {
        std.debug.assert(!form.symbolic);
        return std.math.cast(i64, @as(i128, form.scale) * @as(i128, input) + form.offset) orelse error.CypherNumericOverflow;
    }
    fn momentKind(self: *Engine, aggregate_function: ast.Function, names: MomentNames) !?MomentCall {
        if (aggregate_function.args.len != 1) return null;
        const form = (try self.momentArgument(aggregate_function.args[0], names)) orelse return null;
        if (form.field == null) return null;
        if (self.moment_total_axis) {
            const proof = form.numeric orelse return null;
            if (!proof.total() or (aggregate_function.distinct and !(std.ascii.eqlIgnoreCase(aggregate_function.name, "min") or std.ascii.eqlIgnoreCase(aggregate_function.name, "max")))) return null;
        } else if (form.multiple and ((form.interval == null and form.numeric == null) or aggregate_function.distinct or
            !(std.ascii.eqlIgnoreCase(aggregate_function.name, "avg") or std.ascii.eqlIgnoreCase(aggregate_function.name, "sum") or std.ascii.eqlIgnoreCase(aggregate_function.name, "count")))) return null;
        if (std.ascii.eqlIgnoreCase(aggregate_function.name, "sum") and !form.oneSign()) return null;
        inline for (@typeInfo(MomentCall).@"enum".field_names) |name| {
            if (std.ascii.eqlIgnoreCase(aggregate_function.name, name) and (!aggregate_function.distinct or std.mem.eql(u8, name, "min") or std.mem.eql(u8, name, "max"))) return @field(MomentCall, name);
        }
        return null;
    }
    fn momentSafe(self: *Engine, value: anytype, names: MomentNames) anyerror!bool {
        const T = @TypeOf(value);
        if (T == ast.MapSelector) if (value == .variable) return if (names.get(value.variable)) |proof| (proof != .scalar or proof.scalar.field == null) else true;
        if (T == ast.Expr) {
            if (value.value == .variable) return if (names.get(value.value.variable)) |proof| (proof != .scalar or proof.scalar.field == null) else true;
            if (value.value == .call) {
                const invocation = value.value.call;
                if (std.ascii.eqlIgnoreCase(invocation.name, "collect")) return false;
                if ((try self.momentKind(invocation, names)) != null) {
                    try self.retainMomentChecks(invocation.args[0], names);
                    return true;
                }
            }
        }
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .one and p.child == ast.Expr) {
                    if (self.stream_fold) |fold| if (fold.active_calls) |active| {
                        if (fold.call_aliases) |aliases| if (aliases.get(value)) |slot| if (!active[slot]) return true;
                        for (fold.calls, active) |call_value, enabled| if (!enabled and call_value == value) return true;
                    };
                    return self.momentSafe(value.*, names);
                }
                if (p.size == .slice and p.child != u8) for (value) |child| {
                    if (!try self.momentSafe(child, names)) return false;
                };
            },
            .@"struct" => |info| inline for (info.field_names) |name| {
                if (!try self.momentSafe(@field(value, name), names)) return false;
            },
            .@"union" => switch (value) {
                inline else => |child| return self.momentSafe(child, names),
            },
            .optional => if (value) |child| return self.momentSafe(child, names),
            else => {},
        }
        return true;
    }
    fn momentProjection(self: *Engine, projection: ast.Projection, names: MomentNames) !?MomentNames {
        if (projection.distinct or projection.skip != null or projection.limit != null or projection.order.len != 0) return null;
        var next: MomentNames = if (projection.wildcard) try names.clone(self.a) else .empty;
        for (projection.items) |item| {
            if (try self.momentArgument(item.expression, names)) |form| {
                if (self.moment_total_axis and form.field != null and form.numeric == null) return null;
                try self.retainMomentChecks(item.expression, names);
                try next.put(self.a, item.alias orelse try self.expressionName(item.expression), .{ .scalar = form });
                continue;
            }
            if (referencesAny(item.expression, names)) return null;
            const binding: MomentBinding = if (self.momentEntity(item.expression, names)) |origin| .{ .entity = origin } else .blocked;
            try next.put(self.a, item.alias orelse try self.expressionName(item.expression), binding);
        }
        if (referencesAny(projection.predicate, names) or referencesAny(projection.predicate, next)) return null;
        return next;
    }
    fn momentMiddle(self: *Engine, middle: []const ast.Clause, initial: MomentNames) anyerror!?MomentNames {
        var names = initial;
        for (middle) |clause| switch (clause.operation) {
            .project => |p| names = (try self.momentProjection(p.value, names)) orelse return null,
            .unwind => |u| {
                var form: ?Affine = null;
                var entity: ?[]const u8 = null;
                var proven = true;
                const items = if (u.expression.value == .list) u.expression.value.list else &.{u.expression};
                for (items) |item| {
                    if (try self.momentArgument(item, names)) |candidate| {
                        if (entity != null) {
                            proven = false;
                            break;
                        }
                        if (form) |prior| {
                            if (prior.symbolic or candidate.symbolic or prior.field != candidate.field or prior.scale != candidate.scale or prior.offset != candidate.offset) {
                                form = Affine.choice(prior, candidate) orelse return null;
                                self.moment_representatives = true;
                            }
                        } else form = candidate;
                    } else if (self.momentEntity(item, names)) |origin| {
                        if (form != null or (entity != null and !std.mem.eql(u8, entity.?, origin))) {
                            proven = false;
                            break;
                        }
                        entity = origin;
                    } else {
                        proven = false;
                        break;
                    }
                }
                if (!proven and referencesAny(u.expression, names)) return null;
                if (proven) for (items) |item| try self.retainMomentChecks(item, names);
                names = try names.clone(self.a);
                try names.put(self.a, u.alias, if (!proven) .blocked else if (form) |value| .{ .scalar = value } else if (entity) |origin| .{ .entity = origin } else .blocked);
            },
            .subquery => |inner| {
                if (inner.len == 0 or inner[inner.len - 1].operation != .project or !inner[inner.len - 1].operation.project.final) return null;
                // Row-local CALLs preserve one row per input. Their returned
                // aliases join the caller scope; child-local names do not leak.
                const exported = (try self.momentMiddle(inner, names)) orelse return null;
                var keys = exported.iterator();
                while (keys.next()) |entry| try names.put(self.a, entry.key_ptr.*, entry.value_ptr.*);
            },
            else => return null,
        };
        return names;
    }
    fn momentTail(self: *Engine, initial: MomentNames, first: *const SummaryTail) !bool {
        var names = initial;
        var tail: ?*const SummaryTail = first;
        var depth: usize = 0;
        while (tail) |current| : (tail = current.parent) {
            depth += 1;
            if (depth > self.limits.recursion_depth) return false;
            for (current.clauses) |clause| {
                if (clause.operation == .project) {
                    const projection = clause.operation.project.value;
                    const reduction = for (projection.items) |item| {
                        if (aggregates(item.expression)) break true;
                    } else false;
                    if (reduction) return self.momentSafe(projection, names);
                    names = (try self.momentProjection(projection, names)) orelse return false;
                } else if (clause.operation == .subquery or clause.operation == .unwind) {
                    names = (try self.momentMiddle(&.{clause}, names)) orelse return false;
                } else return false;
            }
        }
        return false;
    }
    fn summaryTargets(self: *Engine, path: ast.Path) ![]const SummaryTarget {
        var targets: std.ArrayList(SummaryTarget) = .empty;
        if (path.execution_summary.length) |name| try targets.append(self.a, .{ .name = name });
        for (path.edges, 0..) |edge, i| if (edge.execution_length) |name| try targets.append(self.a, .{ .name = name, .edge = i });
        return targets.items;
    }
    fn momentTargets(self: *Engine, path: ast.Path) !?[]const SummaryTarget {
        if (self.weighted_sink == null and path.edges.len > 1 and (try self.summaryTargets(path)).len > 1) {
            self.moment_total_axis = true;
            if (try self.planMomentTargets(path)) |targets| return try self.totalTargets(path, targets);
            self.moment_checks = .empty;
            self.moment_invariants = .empty;
            self.moment_representatives = false;
            self.moment_calls = null;
        }
        self.moment_total_axis = self.external_total_axis;
        const targets = (try self.planMomentTargets(path)) orelse return null;
        return if (self.moment_total_axis) try self.totalTargets(path, targets) else targets;
    }
    fn fixedHops(edge: ast.Edge) ?u32 {
        return if (edge.max_hops != null and edge.max_hops.? == edge.min_hops) edge.min_hops else null;
    }
    /// Fixed segments are constants; only variable segments span the geometry.
    /// Use the same normalization for native and external SQL certificates.
    fn summaryNumeric(self: *Engine, path: ast.Path, segment: ?usize, range: Numeric.Lattice) !Numeric.Form {
        var dimensions: usize = 0;
        var index: ?usize = null;
        var offset: i64 = 0;
        for (path.edges, 0..) |edge, i| {
            if (fixedHops(edge)) |hops| {
                if (segment == null or segment.? == i) offset += hops;
            } else {
                if (segment != null and segment.? == i) index = dimensions;
                dimensions += 1;
            }
        }
        if (segment != null and fixedHops(path.edges[segment.?]) != null) return Numeric.Form.literal(Numeric.Lattice.integer(offset));
        var proof = try Numeric.Form.column(self.a, range, dimensions, if (segment == null) null else index.?);
        proof.offset = Numeric.Lattice.integer(offset);
        return proof;
    }
    fn totalTargets(self: *Engine, path: ast.Path, targets: []const SummaryTarget) ![]const SummaryTarget {
        const out = try self.a.dupe(SummaryTarget, targets);
        const variable = try self.a.alloc(bool, path.edges.len);
        var first: ?usize = null;
        for (path.edges, variable, 0..) |edge, *included, i| {
            included.* = fixedHops(edge) == null;
            if (included.* and first == null) first = i;
        }
        for (out) |*target| if (target.edge) |edge| {
            if (fixedHops(path.edges[edge]) != null) continue;
            target.zero = edge != first;
            target.edges = variable;
        };
        // Keep fixed segments at their actual lengths. The first variable
        // segment carries all variable hops and the others carry zero; a whole
        // path target still counts every hop. Certified consumers are invariant
        // under this substitution, including each finite UNWIND branch.
        self.moment_representatives = true;
        self.moment_calls = null;
        return out;
    }
    fn planMomentTargets(self: *Engine, path: ast.Path) !?[]const SummaryTarget {
        if (self.component_leaves.len != 0) return null;
        const targets = try self.summaryTargets(path);
        if (targets.len == 0) return null;
        var names: MomentNames = .empty;
        for (targets, 0..) |target, i| {
            var form = Affine.column(i).bounded(.{ .low = 0, .high = std.math.cast(i64, self.limits.path_depth) orelse std.math.maxInt(i64) });
            form.numeric = try self.summaryNumeric(path, target.edge, .{ .low = 0, .high = form.interval.?.high });
            try names.put(self.a, target.name, .{ .scalar = form });
        }
        if (referencesAny(self.stream_predicate, names)) return null;
        if (self.weighted_sink) |_| {
            // External consumers negotiate this certificate before advancing.
            if (self.external_summary == null or (!self.external_linear_summary and targets.len != 1) or !std.mem.eql(u8, targets[0].name, self.external_summary.?)) return null;
            return targets;
        }
        var fold = self.stream_fold orelse return null;
        var start: usize = 0;
        var depth: usize = 0;
        while (true) {
            depth += 1;
            if (depth > self.limits.recursion_depth) return null;
            names = (try self.momentMiddle(fold.middle[start..], names)) orelse return null;
            if (fold.continuation) |next| {
                if (next.admission) return null;
                if (next.apply != null) names = (try self.momentProjection(fold.projection, names)) orelse return null;
                fold = next.parent;
                start = next.index;
            } else {
                if (fold.calls.len != 0) {
                    if (!try self.momentSafe(fold.projection, names)) return null;
                    if (fold == self.stream_fold.? and self.momentLocal(fold.middle)) {
                        const calls = try self.a.alloc(?DirectMoment, fold.calls.len);
                        for (fold.calls, calls, 0..) |expression, *call_input, i| {
                            if (fold.active_calls) |active| if (!active[i]) {
                                call_input.* = null;
                                continue;
                            };
                            const invocation = expression.value.call;
                            call_input.* = if (try self.momentKind(invocation, names)) |kind| .{ .kind = kind, .form = (try self.momentArgument(invocation.args[0], names)).?, .expression = invocation.args[0] } else null;
                            if (call_input.* != null) try self.retainMomentChecks(invocation.args[0], names);
                        }
                        if (!self.moment_representatives) self.moment_calls = calls;
                    }
                    return targets;
                }
                if (self.summary_tail) |tail| {
                    names = (try self.momentProjection(fold.projection, names)) orelse return null;
                    if (try self.momentTail(names, tail)) return targets;
                }
                return null;
            }
        }
    }
    fn waysCount(value: anytype) Count {
        return if (@TypeOf(value) == Statistic.Value) value.count else if (@TypeOf(value) == PathMoments) value.count else if (@TypeOf(value) == Count) value else Count.init(value);
    }
    fn waysFromCount(comptime W: type, value: Count) W {
        return if (W == Statistic.Value) Statistic.Value.init(value) else if (W == PathMoments) PathMoments.init(value) else if (W == Count) value else waysCap(value);
    }
    fn advanceWays(self: *Engine, graph: *LowerGraph, value: anytype) !@TypeOf(value) {
        return if (@TypeOf(value) == Statistic.Value) value.advance(self.a, graph.segment, self.statistic_plan.?.kernel) else if (@TypeOf(value) == PathMoments) value.advance(self.a, self.moment_targets, graph.segment) else value;
    }
    fn waysInit(comptime W: type, value: u64) W {
        return waysFromCount(W, Count.init(value));
    }
    fn waysCap(value: anytype) u64 {
        return waysCount(value).cap(std.math.maxInt(u64));
    }
    fn waysAdd(self: *Engine, left: anytype, right: @TypeOf(left)) !@TypeOf(left) {
        return if (@TypeOf(left) == Statistic.Value) left.add(self.a, right, self.statistic_plan.?.kernel) else if (@TypeOf(left) == Count or @TypeOf(left) == PathMoments) left.add(self.a, right) else mergePathWays(left, right);
    }
    fn waysMultiply(self: *Engine, left: anytype, right: @TypeOf(left)) !@TypeOf(left) {
        return if (@TypeOf(left) == Statistic.Value) left.multiply(self.a, right, self.statistic_plan.?.kernel) else if (@TypeOf(left) == Count or @TypeOf(left) == PathMoments) left.multiply(self.a, right) else multiplyPathWays(left, right);
    }
    fn countAllPathsTyped(self: *Engine, comptime W: type, input: MatchState, path: ast.Path, mode: ast.PathMode, starts: []const *const v.Node, output: *std.ArrayList(MatchState)) anyerror!bool {
        if (starts.len != 1 and (if (path.execution_reversed) path.execution_summary.last != null else path.execution_summary.first != null)) return false;
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
        var products: SummaryProducts = .{};
        if (W != Statistic.Value and W != PathMoments and (path.execution_summary.length != null or for (path.edges) |edge| {
            if (edge.execution_length != null) break true;
        } else false)) graph.products = &products;
        const Counts = std.AutoHashMapUnmanaged(usize, W);
        var current: Counts = .empty;
        var next: Counts = .empty;
        var endpoints: Counts = .empty;
        defer current.deinit(self.a);
        defer next.deinit(self.a);
        defer endpoints.deinit(self.a);
        for (starts) |start| {
            const entry = try current.getOrPut(self.a, if (graph.products != null) try products.intern(self, .{ .node = graph.positions.get(start.identity).? }) else graph.positions.get(start.identity).?);
            entry.value_ptr.* = try self.waysAdd(if (entry.found_existing) entry.value_ptr.* else waysInit(W, 0), waysInit(W, 1));
        }
        var effective_maximum: usize = 0;
        for (path.edges, 0..) |edge, edge_index| {
            graph.segment = edge_index;
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
                    const node = graph.nodeIndex(index.*);
                    const entry = try seen.getOrPut(self.a, node);
                    if (!entry.found_existing) try pending.append(self.a, node);
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
                if (graph.products == null) topological = depth.?.order;
                break :blk depth.?.maximum;
            };
            effective_maximum = std.math.add(usize, effective_maximum, upper) catch return false;
            if (effective_maximum > self.limits.path_depth) return false;
            if (graph.products == null and topological == null and path.edges.len == 1 and upper > 1 and upper >= starts.len) {
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
            if (graph.products == null and edge.min_hops == 1 and upper == 1) {
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
                        reached[arc.target] = try self.waysAdd(reached[arc.target], try self.advanceWays(&graph, ways));
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
                    const index = graph.nodeIndex(candidate.key_ptr.*);
                    const ways = candidate.value_ptr.*;
                    const node = graph.nodes.items[index].node;
                    if (hop >= edge.min_hops and try self.nodeMatches(node, path.nodes[edge_index + 1]) and
                        boundNodeMatches(node, path.nodes[edge_index + 1], input.row) and try self.constantProperties(.{ .node = node }, path.nodes[edge_index + 1].properties, input.row))
                    {
                        const entry = try endpoints.getOrPut(self.a, try graph.acceptedKey(self, candidate.key_ptr.*, path, edge_index, hop));
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
            while (final.next()) |endpoint| endpoint.value_ptr.* = try self.waysMultiply(endpoint.value_ptr.*, try self.branchWays(W, &graph, graph.nodeIndex(endpoint.key_ptr.*), path.nodes.len - 1, null, mode));
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
        if (W == PathMoments and self.moment_invariants.items.len != 0) {
            // Type admission is effect-free and cannot raise an expression
            // error behind a filter. Check all endpoints before emitting a bag.
            var candidates = current.iterator();
            while (candidates.next()) |endpoint| {
                if (waysCap(endpoint.value_ptr.*) == 0) continue;
                const node = graph.nodes.items[graph.nodeIndex(endpoint.key_ptr.*)].node;
                const row = (try self.bind(input.row, path.nodes[path.nodes.len - 1].alias, .{ .node = node })) orelse continue;
                for (self.moment_invariants.items) |expression| {
                    try self.state();
                    const value = self.constant(expression, row) orelse return error.CypherMomentTypeDeclined;
                    if (value != .integer) return error.CypherMomentTypeDeclined;
                }
            }
        }
        var statistic_total: Statistic.Value = .init(Count.init(0));
        var statistic_row: ?Row = null;
        var counts = current.iterator();
        while (counts.next()) |endpoint| {
            const ways = endpoint.value_ptr.*;
            if (waysCap(ways) == 0) continue;
            if (self.shortest_count) |count_paths| {
                count_paths.* = try addPathCount(count_paths.*, waysCap(ways));
            } else {
                const node = graph.nodes.items[graph.nodeIndex(endpoint.key_ptr.*)].node;
                const bound = (try self.bind(input.row, path.nodes[path.nodes.len - 1].alias, .{ .node = node })) orelse continue;
                var row = try self.pathSummaryRow(bound, path, starts[0], node, if (graph.products != null) products.keys.items[endpoint.key_ptr.*].total else 0);
                if (graph.products != null) {
                    var history = products.keys.items[endpoint.key_ptr.*].history;
                    while (history != 0) {
                        const entry = products.histories.items[history - 1];
                        row = try self.lengthRow(row, path.edges[entry.edge].execution_length, entry.hops);
                        history = entry.parent;
                    }
                }
                if (self.stream_predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) continue;
                if (W == Statistic.Value) {
                    if (self.statistic_plan.?.global) {
                        statistic_total = try statistic_total.add(self.a, ways, self.statistic_plan.?.kernel);
                        statistic_row = row;
                    } else try self.emitStatistics(row, ways);
                    continue;
                }
                if (W == PathMoments) {
                    try self.emitMomentBag(row, ways);
                    continue;
                }
                if (self.weighted_sink) |sink| {
                    // Finite SQL consumers need only the signed-overflow cap;
                    // exact consumers retain the complete promoted count.
                    try sink.emit(sink.ptr, row, switch (sink.multiplicity) {
                        .presence => Count.one,
                        .capped => Count.init(@min(waysCap(ways), @as(u64, std.math.maxInt(i64)) + 1)),
                        .exact => waysCount(ways),
                    });
                    continue;
                }
                const fold = self.stream_fold.?;
                const previous_weight = fold.weight;
                fold.weight = if (self.match_weight_demand == .presence) Count.one else try previous_weight.multiply(fold.retained, waysCount(ways));
                defer fold.weight = previous_weight;
                try fold.add(self, row);
            }
        }
        if (W == Statistic.Value) if (statistic_row) |row| try self.emitStatistics(row, statistic_total);
        return true;
    }
    /// Construct at most four integer bins for each marginal, preserving its
    /// exact count, first moment and both extrema. Couple the bins by splitting
    /// their weights; no Cartesian product or fractional public value is needed.
    /// These bags can be memoized and replayed through ordinary typed cursors.
    fn emitMomentBag(self: *Engine, initial: Row, moments: PathMoments) !void {
        // Local reducers consume the algebra directly. Only escaping bags need
        // integer representatives for CALL memoization or SQL cursor transport.
        if (self.moment_calls) |calls| {
            const active = self.active_moments;
            self.active_moments = .{ .values = moments, .checks = self.moment_checks };
            defer self.active_moments = active;
            const fold = self.stream_fold.?;
            const prior = fold.weight;
            const direct = fold.moment_direct;
            fold.moment_direct = .{ .calls = calls, .values = moments };
            fold.weight = try prior.multiply(fold.retained, moments.count);
            defer {
                fold.weight = prior;
                fold.moment_direct = direct;
            }
            // Transparent projections still evaluate their input bindings.
            // Supply a valid representative for every private marginal even
            // when the terminal reducers consume the algebra directly.
            var row = initial;
            for (self.moment_targets, 0..) |target, i| row = try self.lengthRow(row, target.name, moments.part(i).min);
            return fold.add(self, row);
        }
        const Bin = struct { value: usize, weight: Count };
        const Marginal = struct { bins: [4]Bin = undefined, len: usize = 0, position: usize = 0, remaining: Count = Count.init(0) };
        const marginals = try self.a.alloc(Marginal, self.moment_targets.len);
        for (marginals, 0..) |*marginal, i| {
            marginal.* = .{};
            const part = moments.part(i);
            if (part.min == part.max) {
                marginal.bins[0] = .{ .value = part.min, .weight = moments.count };
                marginal.len = 1;
            } else {
                marginal.bins[0] = .{ .value = part.min, .weight = Count.one };
                marginal.bins[1] = .{ .value = part.max, .weight = Count.one };
                marginal.len = 2;
                const count_left = try moments.count.subtract(self.a, 2);
                if (count_left.cap(1) != 0) {
                    const sum_left = try (try part.sum.subtract(self.a, part.min)).subtract(self.a, part.max);
                    const split = try sum_left.divRem(self.a, count_left);
                    const low = std.math.cast(usize, split.quotient.exact() orelse return error.CypherNumericOverflow) orelse return error.CypherNumericOverflow;
                    const low_weight = try count_left.difference(self.a, split.remainder);
                    if (low_weight.cap(1) != 0) {
                        marginal.bins[marginal.len] = .{ .value = low, .weight = low_weight };
                        marginal.len += 1;
                    }
                    if (split.remainder.cap(1) != 0) {
                        marginal.bins[marginal.len] = .{ .value = low + 1, .weight = split.remainder };
                        marginal.len += 1;
                    }
                }
            }
            marginal.remaining = marginal.bins[0].weight;
        }
        var left = moments.count;
        while (left.cap(1) != 0) {
            try self.state();
            var weight = left;
            var row = initial;
            for (marginals, self.moment_targets) |marginal, target| {
                if (marginal.remaining.order(weight) == .lt) weight = marginal.remaining;
                row = try self.lengthRow(row, target.name, marginal.bins[marginal.position].value);
            }
            if (self.weighted_sink) |sink| {
                try sink.emit(sink.ptr, row, switch (sink.multiplicity) {
                    .presence => Count.one,
                    .capped => Count.init(weight.cap(@as(u64, std.math.maxInt(i64)) + 1)),
                    .exact => weight,
                });
            } else {
                const fold = self.stream_fold.?;
                const prior = fold.weight;
                fold.weight = try prior.multiply(fold.retained, weight);
                defer fold.weight = prior;
                try fold.add(self, row);
            }
            left = try left.difference(self.a, weight);
            for (marginals) |*marginal| {
                marginal.remaining = try marginal.remaining.difference(self.a, weight);
                if (marginal.remaining.cap(1) == 0) {
                    marginal.position += 1;
                    if (marginal.position < marginal.len) marginal.remaining = marginal.bins[marginal.position].weight;
                }
            }
        }
    }
    fn lengthRow(self: *Engine, row: Row, alias: ?[]const u8, length: usize) !Row {
        return if (alias) |name| try self.replace(row, name, .{ .integer = @intCast(length) }) else row;
    }
    fn pathSummaryRow(self: *Engine, initial: Row, path: ast.Path, first: *const v.Node, last: *const v.Node, length: usize) !Row {
        const demand = path.execution_summary;
        var row = try self.lengthRow(initial, demand.presence, 1);
        row = try self.lengthRow(row, demand.length, length);
        if (demand.first) |name| row = try self.replace(row, name, .{ .node = if (path.execution_reversed) last else first });
        if (demand.last) |name| row = try self.replace(row, name, .{ .node = if (path.execution_reversed) first else last });
        return row;
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
        if (graph.branch_factors.get(factor_key)) |ways| return waysFromCount(W, ways);
        const ways = try self.branchAssignmentsTyped(W, cached.sets, excluded_mask, mode == .repeatable);
        try graph.branch_factors.put(self.a, factor_key, waysCount(ways));
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
            const index = graph.nodeIndex(candidate.key_ptr.*);
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
                const entry = try next.getOrPut(self.a, try graph.movedKey(self, candidate.key_ptr.*, arc.target));
                const weight = try self.advanceWays(graph, if (source) |pivot| try self.waysMultiply(ways, try self.branchWays(W, graph, index, pivot, relationship, graph.mode)) else ways);
                entry.value_ptr.* = try self.waysAdd(if (entry.found_existing) entry.value_ptr.* else waysInit(W, 0), weight);
            }
        }
    }
    const EndpointScope = struct {
        path: ast.Path,
        row: Row,
        include_start: bool = true,
        pub fn permits(self: @This(), name: []const u8) bool {
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(self.path.execution_summary, field)) |alias| {
                if (std.mem.eql(u8, name, alias)) return true;
            };
            for (self.path.edges) |edge| if (edge.execution_length) |alias| {
                if (std.mem.eql(u8, name, alias)) return true;
            };
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
    fn endpointShape(self: *Engine, path: ast.Path, row: Row) !Row {
        var shape: Row = &.{};
        for (row) |entry| shape = try self.replace(shape, entry.name, .{ .boolean = true });
        for (path.nodes, 0..) |node, index| if (node.alias) |alias| {
            if (lookup(shape, alias) == null) shape = try self.replace(shape, alias, .{ .boolean = index == 0 or index + 1 == path.nodes.len });
        };
        inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
            shape = try self.replace(shape, name, .{ .boolean = true });
        };
        for (path.edges) |edge| if (edge.execution_length) |name| {
            shape = try self.replace(shape, name, .{ .boolean = true });
        };
        for (path.edges) |edge| if (edge.alias) |alias| {
            if (lookup(shape, alias) == null) shape = try self.replace(shape, alias, .{ .boolean = false });
        };
        if (path.alias) |alias| if (lookup(shape, alias) == null) {
            shape = try self.replace(shape, alias, .{ .boolean = false });
        };
        return shape;
    }
    fn admissionMatchShape(self: *Engine, source: Row, operation: ast.Match) !?Row {
        if (!weightedExpression(operation)) return null;
        const rules = @import("expressions.zig");
        var result = source;
        for (operation.paths) |path| {
            for (path.nodes) |node| if (node.alias) |alias| {
                if (lookup(result, alias)) |known| {
                    if (!known.boolean) return null;
                } else result = try self.replace(result, alias, .{ .boolean = true });
            };
            inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
                result = try self.replace(result, name, .{ .boolean = true });
            };
            for (path.edges) |edge| if (edge.execution_length) |name| {
                result = try self.replace(result, name, .{ .boolean = true });
            };
            for (path.edges) |edge| if (edge.alias) |alias| {
                if (lookup(result, alias)) |known| {
                    if (!known.boolean) return null;
                } else result = try self.replace(result, alias, .{ .boolean = true });
            };
            if (path.alias) |alias| {
                if (lookup(result, alias)) |known| {
                    if (!known.boolean) return null;
                } else result = try self.replace(result, alias, .{ .boolean = true });
            }
        }
        for (operation.paths) |path| {
            for (path.nodes) |node| for (node.properties) |property| if (!rules.rowLocal(property.value, PipelineScope{ .bindings = result })) return null;
            for (path.edges) |edge| for (edge.properties) |property| if (!rules.rowLocal(property.value, PipelineScope{ .bindings = result })) return null;
        }
        if (operation.predicate) |predicate| if (!rules.rowLocal(predicate, PipelineScope{ .bindings = result })) return null;
        return result;
    }
    fn admissionCallShape(self: *Engine, source: Row, inner: []const ast.Clause) anyerror!?Row {
        if (!pureReturningCall(inner)) return null;
        var shape = source;
        const rules = @import("expressions.zig");
        for (inner) |clause| switch (clause.operation) {
            .project => |step| shape = (try self.admissionChildProjectionShape(shape, step.value, true)) orelse return null,
            .match => |matching| shape = (try self.admissionMatchShape(shape, matching)) orelse return null,
            .subquery => |child| shape = (try self.admissionCallShape(shape, child)) orelse return null,
            .unwind => |u| {
                if (!rules.rowLocal(u.expression, PipelineScope{ .bindings = shape })) return null;
                shape = try self.replace(shape, u.alias, .{ .boolean = true });
            },
            else => return null,
        };
        var merged = source;
        for (shape) |entry| merged = try self.replace(merged, entry.name, entry.value);
        return merged;
    }
    fn admissionProjectionShape(self: *Engine, source: Row, projection: ast.Projection) !?Row {
        return self.admissionChildProjectionShape(source, projection, false);
    }
    fn admissionChildProjectionShape(self: *Engine, source: Row, projection: ast.Projection, barriers: bool) !?Row {
        const rules = @import("expressions.zig");
        if (!barriers and projection.order.len != 0) return null;
        if (projection.skip) |skip| if (!rules.rowLocal(skip, PipelineScope{ .bindings = source })) return null;
        if (projection.limit) |limit| if (!rules.rowLocal(limit, PipelineScope{ .bindings = source })) return null;
        var result: Row = if (projection.wildcard) source else &.{};
        for (projection.items) |item| {
            if ((!barriers and aggregates(item.expression)) or !rules.rowLocal(item.expression, PipelineScope{ .bindings = source })) return null;
            result = try self.replace(result, item.alias orelse try self.expressionName(item.expression), .{ .boolean = true });
        }
        var visible = source;
        for (result) |entry| visible = try self.replace(visible, entry.name, entry.value);
        if (projection.predicate) |predicate| if (!rules.rowLocal(predicate, PipelineScope{ .bindings = visible })) return null;
        for (projection.order) |order| if (!rules.rowLocal(order.expression, PipelineScope{ .bindings = visible })) return null;
        return result;
    }
    fn localCallShape(self: *Engine, initial: Row, inner: []const ast.Clause, depth: usize) anyerror!?Row {
        if (depth >= self.limits.recursion_depth or inner.len == 0 or inner[inner.len - 1].operation != .project or !inner[inner.len - 1].operation.project.final) return null;
        const rules = @import("expressions.zig");
        var shape = initial;
        for (inner) |clause| switch (clause.operation) {
            .project => |step| {
                const p = step.value;
                if (p.distinct or p.skip != null or p.limit != null or p.order.len != 0) return null;
                var next: Row = if (p.wildcard) shape else &.{};
                for (p.items) |item| {
                    if (aggregates(item.expression) or !rules.rowLocal(item.expression, PipelineScope{ .bindings = shape })) return null;
                    next = try self.replace(next, item.alias orelse try self.expressionName(item.expression), .{ .boolean = true });
                }
                var visible = shape;
                for (next) |entry| visible = try self.replace(visible, entry.name, entry.value);
                if (p.predicate) |predicate| if (!rules.rowLocal(predicate, PipelineScope{ .bindings = visible })) return null;
                shape = next;
            },
            .subquery => |child| {
                const exported = (try self.localCallShape(shape, child, depth + 1)) orelse return null;
                for (exported) |entry| shape = try self.replace(shape, entry.name, entry.value);
            },
            else => return null,
        };
        return shape;
    }
    fn pipelineLocal(self: *Engine, path: ast.Path, row: Row, middle: []const ast.Clause, projection: ast.Projection, include_start: bool) !bool {
        const rules = @import("expressions.zig");
        var shape: Row = &.{};
        for (row) |entry| shape = try self.replace(shape, entry.name, .{ .boolean = true });
        for (path.nodes, 0..) |node, i| if (node.alias) |alias| {
            if (lookup(shape, alias) == null) shape = try self.replace(shape, alias, .{ .boolean = (i == 0 and include_start) or i == path.nodes.len - 1 });
        };
        inline for (@typeInfo(ast.PathSummary).@"struct".field_names) |field| if (@field(path.execution_summary, field)) |name| {
            shape = try self.replace(shape, name, .{ .boolean = true });
        };
        for (path.edges) |edge| if (edge.execution_length) |name| {
            shape = try self.replace(shape, name, .{ .boolean = true });
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
            .subquery => |inner| {
                const exported = (try self.localCallShape(shape, inner, 0)) orelse return false;
                for (exported) |entry| shape = try self.replace(shape, entry.name, entry.value);
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
        if (path.selector != .all or path.edges.len == 0 or path.execution_prefix or (if (path.execution_reversed) path.execution_summary.last != null else path.execution_summary.first != null)) return false;
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
            return self.routePath(input, path, mode, node, output);
        }
        // Rebound relationship/path variables constrain the complete route,
        // rather than only its endpoint distance.
        if (path.alias) |alias| if (lookup(input.row, alias) != null)
            return self.routePath(input, path, mode, node, output);
        if (path.edges[0].alias) |alias| if (lookup(input.row, alias) != null)
            return self.routePath(input, path, mode, node, output);
        // Forward references, path-dependent predicates and multi-pattern MATCH
        // keep the exhaustive selector. Endpoint-local patterns admit a native
        // breadth-first operator, with one frontier distance per identity.
        for (path.edges[0].properties) |property| if (self.constant(property.value, input.row) == null)
            return self.routePath(input, path, mode, node, output);
        for (path.nodes[1].properties) |property| if (self.constant(property.value, input.row) == null)
            return self.routePath(input, path, mode, node, output);
        if (path.edges[0].min_hops > 1) return self.lowerPath(input, path, mode, node, output);
        try self.breadthPath(input, path, mode, node, output);
    }
    fn routePath(self: *Engine, input: MatchState, path: ast.Path, mode: ast.PathMode, start: *const v.Node, output: *std.ArrayList(MatchState)) anyerror!void {
        const probe = self.route_probe orelse return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
        if (path.execution_prefix or self.interior != null) return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
        var maximum: usize = 0;
        var needs_bound = false;
        for (path.edges) |edge| {
            if (edge.max_hops) |upper| maximum = std.math.add(usize, maximum, upper) catch return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output) else needs_bound = true;
            for (edge.properties) |entry| if (self.constant(entry.value, input.row) == null) return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
        }
        if (maximum > self.limits.path_depth) return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
        for (path.nodes) |node| for (node.properties) |entry| if (self.constant(entry.value, input.row) == null) return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
        var arena = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer arena.deinit();
        const demanded = try path_summaries.prune(arena.allocator(), path, .{ probe.fold.middle, probe.fold.projection, self.stream_predicate });
        var pruner: RoutePruner = .{ .fold = probe.fold, .a = arena.allocator(), .path = demanded, .predicate = self.stream_predicate, .row = input.row, .graph = .{ .input = .{ .row = input.row }, .path = demanded, .qualified = true, .mode = mode }, .start = start, .needs_bound = needs_bound };
        const previous_pruner = self.route_pruner;
        const previous_graph = self.lower_graph;
        self.route_pruner = &pruner;
        defer self.route_pruner = previous_pruner;
        defer self.lower_graph = previous_graph;
        probe.rejected.* = 0;
        // Activate at a suspended DFS frame, without restarting prefixes that
        // have already produced rows or affected DISTINCT/OPTIONAL state.
        return self.pathStep(input, path, mode, 0, &.{start}, &.{}, output);
    }
    const RoutePruner = struct {
        const Key = struct { node: usize, edge: usize, hop: u32, summary: usize = 0 };
        const Frame = struct { key: Key, children: []const Key, next: usize = 0 };
        const Admission = struct { possible: bool, generation: usize };
        a: A,
        path: ast.Path,
        row: Row,
        predicate: ?*const ast.Expr,
        graph: LowerGraph,
        fold: *StreamFold,
        possible: std.AutoHashMapUnmanaged(Key, Admission) = .empty,
        start: ?*const v.Node = null,
        needs_bound: bool = false,
        active: bool = false,
        attempted: bool = false,
        suffix_depth: ?[]const ?usize = null,
        segment_depths: ?[]std.ArrayList(?usize) = null,
        depth_attempted: bool = false,
        frames_built: usize = 0,
        products: SummaryProducts = .{},
        fn needsHops(self: *@This()) bool {
            if (self.path.execution_summary.length != null) return true;
            for (self.path.edges) |edge| if (edge.execution_length != null) return true;
            return false;
        }
        fn activate(self: *@This(), engine: *Engine) anyerror!void {
            if (self.attempted) return;
            self.attempted = true;
            const scope = EvalScope.enter(engine, self.a);
            defer scope.leave(engine);
            const domains = try self.a.alloc(ast.Edge, self.path.edges.len);
            for (domains, self.path.edges) |*domain, edge| domain.* = .{ .span = edge.span, .types = edge.types, .direction = edge.direction };
            self.graph.domains = domains;
            const start = self.start.?;
            try self.graph.positions.put(self.a, start.identity, 0);
            try self.graph.nodes.append(self.a, .{ .node = start, .depth = 0 });
            if (self.needs_bound and !try self.bound(engine)) return;
            self.active = true;
            engine.lower_graph = &self.graph;
        }
        fn get(self: *@This(), key: Key) ?bool {
            const value = self.possible.get(self.memoKey(key)) orelse return null;
            // Seen DISTINCT keys grow monotonically. Negative admissions stay
            // valid; positive ones must be reconsidered after a new group.
            if (value.possible and value.generation != self.fold.generation()) return null;
            return value.possible;
        }
        fn put(self: *@This(), engine: *Engine, key: Key, possible: bool) !void {
            try self.recordDepth(engine, key);
            try self.possible.put(self.a, self.memoKey(key), .{ .possible = possible, .generation = self.fold.generation() });
        }
        fn memoKey(self: *@This(), key: Key) Key {
            const depths = if (self.segment_depths) |segments| segments[key.edge].items else self.suffix_depth orelse return key;
            if (key.node >= depths.len) return key;
            const depth = depths[key.node] orelse return key;
            const edge = self.path.edges[key.edge];
            if (key.hop >= edge.min_hops and edge.max_hops.? -| key.hop >= depth)
                return .{ .node = key.node, .edge = key.edge, .hop = edge.min_hops };
            return key;
        }
        fn prepareDepth(self: *@This(), engine: *Engine) anyerror!void {
            self.depth_attempted = true;
            if (self.needsHops()) return;
            if (self.path.edges.len != 1) {
                // Segment domains retain independent geometry. Postorder
                // completion proves closed acyclic suffixes from captures
                // already read by DFS, without expanding any read horizon.
                const segments = try self.a.alloc(std.ArrayList(?usize), self.path.edges.len);
                @memset(segments, .empty);
                self.segment_depths = segments;
                return;
            }
            const edge = self.path.edges[0];
            if (edge.min_hops == edge.max_hops.?) return;
            var distance: std.AutoHashMapUnmanaged(usize, usize) = .empty;
            var queue: std.ArrayList(usize) = .empty;
            try distance.put(self.a, 0, 0);
            try queue.append(self.a, 0);
            var head: usize = 0;
            while (head < queue.items.len) : (head += 1) {
                const node = queue.items[head];
                const hop = distance.get(node).?;
                if (hop >= edge.max_hops.?) continue;
                const range = try engine.loadLowerDomain(&self.graph, node, 0);
                for (self.graph.nodes.items[node].arcs.items[range.first..range.last]) |arc| {
                    try engine.state();
                    if (!try engine.constantProperties(.{ .relationship = arc.relationship }, edge.properties, self.row)) continue;
                    const found = try distance.getOrPut(self.a, arc.target);
                    if (!found.found_existing) {
                        found.value_ptr.* = hop + 1;
                        try queue.append(self.a, arc.target);
                    }
                }
            }
            const proof = (try engine.lowerDomainDepth(&self.graph, 0, edge, &distance)) orelse return;
            const depths = try self.a.alloc(?usize, self.graph.nodes.items.len);
            @memset(depths, null);
            var index = proof.order.len;
            while (index != 0) {
                index -= 1;
                const node = proof.order[index];
                const range = self.graph.nodes.items[node].domains.get(0) orelse continue;
                var depth: ?usize = 0;
                for (self.graph.nodes.items[node].arcs.items[range.first..range.last]) |arc| {
                    try engine.state();
                    if (!try engine.constantProperties(.{ .relationship = arc.relationship }, edge.properties, self.row)) continue;
                    const child = depths[arc.target] orelse {
                        depth = null;
                        break;
                    };
                    depth = @max(depth.?, child + 1);
                }
                depths[node] = depth;
            }
            self.suffix_depth = depths;
        }
        fn recordDepth(self: *@This(), engine: *Engine, key: Key) !void {
            const segments = self.segment_depths orelse return;
            const depths = &segments[key.edge];
            if (depths.items.len < self.graph.nodes.items.len) {
                const old = depths.items.len;
                try depths.resize(self.a, self.graph.nodes.items.len);
                @memset(depths.items[old..], null);
            }
            if (depths.items[key.node] != null) return;
            const node = self.graph.nodes.items[key.node];
            const range = node.domains.get(key.edge) orelse return;
            var depth: usize = 0;
            for (node.arcs.items[range.first..range.last]) |arc| {
                try engine.state();
                if (!try engine.constantProperties(.{ .relationship = arc.relationship }, self.path.edges[key.edge].properties, self.row)) continue;
                const child = depths.items[arc.target] orelse return;
                depth = @max(depth, child + 1);
            }
            depths.items[key.node] = depth;
        }
        fn simpleBound(self: *@This(), engine: *Engine, domain: ?usize) !usize {
            if (self.graph.mode == .node_simple) return self.graph.nodes.items.len -| 1;
            var edges: v.EntitySet = .empty;
            for (self.graph.nodes.items) |node| {
                const range = if (domain) |d| node.domains.get(d).? else LowerRange{ .first = 0, .last = node.arcs.items.len };
                for (node.arcs.items[range.first..range.last]) |arc| {
                    try engine.state();
                    try edges.put(self.a, try v.EntityIdentity.fromValue(.{ .relationship = arc.relationship }), {});
                }
            }
            return edges.count();
        }
        fn bound(self: *@This(), engine: *Engine) anyerror!bool {
            const scope = EvalScope.enter(engine, self.a);
            defer scope.leave(engine);
            const previous_graph = engine.lower_graph;
            engine.lower_graph = null;
            defer engine.lower_graph = previous_graph;
            // Unbounded segments need a finite proof before quotienting. Load
            // the union closure conservatively, then bound each unbounded
            // domain by DAG depth or a finite simple-path identity bound.
            // Repeatable cyclic walks retain the ordinary depth-limited DFS.
            var head: usize = 0;
            while (head < self.graph.nodes.items.len) : (head += 1) for (self.graph.domains, 0..) |_, domain| {
                _ = try engine.loadLowerDomain(&self.graph, head, domain);
            };
            var roots: std.AutoHashMapUnmanaged(usize, u64) = .empty;
            for (0..self.graph.nodes.items.len) |index| try roots.put(self.a, index, 1);
            const edges = try self.a.dupe(ast.Edge, self.path.edges);
            var maximum: usize = 0;
            for (edges, 0..) |*edge, domain| {
                if (edge.max_hops == null) {
                    const maximum_hops = if (try engine.lowerDomainDepth(&self.graph, domain, edge.*, &roots)) |proof| proof.maximum else if (self.graph.mode != .repeatable) try self.simpleBound(engine, domain) else return false;
                    edge.max_hops = std.math.cast(u32, maximum_hops) orelse return false;
                }
                maximum = std.math.add(usize, maximum, edge.max_hops.?) catch return false;
            }
            if (maximum > engine.limits.path_depth and self.graph.mode != .repeatable) maximum = @min(maximum, try self.simpleBound(engine, null));
            if (maximum > engine.limits.path_depth) return false;
            self.path.edges = edges;
            return true;
        }
        fn admits(self: *@This(), engine: *Engine, identity: v.Identity, edge: usize, hop: u32, row: Row, total: usize) anyerror!bool {
            const scope = EvalScope.enter(engine, self.a);
            defer scope.leave(engine);
            const node = self.graph.positions.get(identity) orelse blk: {
                const captured = (try engine.getNode(identity)) orelse return true;
                const position = self.graph.nodes.items.len;
                try self.graph.positions.put(self.a, captured.identity, position);
                try self.graph.nodes.append(self.a, .{ .node = captured, .depth = 0 });
                break :blk position;
            };
            var summary: usize = 0;
            if (self.needsHops()) {
                summary = try self.products.intern(engine, .{ .node = 0 });
                for (self.path.edges[0..edge], 0..) |segment, index| if (segment.execution_length) |alias| {
                    const length = lookup(row, alias) orelse return true;
                    if (length != .integer or length.integer < 0) return true;
                    summary = try self.products.accepted(engine, summary, self.path, index, @intCast(length.integer));
                };
                var prefix = self.products.keys.items[summary];
                prefix.total = if (self.path.execution_summary.length != null) total - hop else 0;
                summary = try self.products.intern(engine, prefix);
            }
            const key: Key = .{ .node = node, .edge = edge, .hop = hop, .summary = summary };
            if (self.get(key)) |result| return result;
            const previous_graph = engine.lower_graph;
            engine.lower_graph = null;
            defer engine.lower_graph = previous_graph;
            // An explicit postorder stack bounds native stack use by neither
            // graph diameter nor the number of pattern segments.
            var frames: std.ArrayList(Frame) = .empty;
            defer frames.deinit(self.a);
            try frames.append(self.a, try self.frame(engine, key));
            while (frames.items.len != 0) {
                const last = frames.items.len - 1;
                const current = frames.items[last].key;
                if (self.get(current) != null) {
                    _ = frames.pop();
                    continue;
                }
                if (frames.items[last].next == frames.items[last].children.len) {
                    try self.put(engine, current, false);
                    _ = frames.pop();
                    continue;
                }
                const child = frames.items[last].children[frames.items[last].next];
                if (self.get(child)) |result| {
                    if (result) try self.put(engine, current, true) else frames.items[last].next += 1;
                } else try frames.append(self.a, try self.frame(engine, child));
            }
            return self.get(key).?;
        }
        fn accepts(self: *@This(), engine: *Engine, row: Row) anyerror!bool {
            if (self.predicate) |predicate| if ((try (try engine.eval(predicate, row, null)).truth()) != true) return false;
            // OPTIONAL's match-success flag precedes downstream qualification.
            // Only intrinsic MATCH absence licenses NULL extension.
            return (self.fold.optional and (!self.fold.had_match or !self.fold.probe_tail)) or try self.fold.probe(engine, row, 0);
        }
        fn frame(self: *@This(), engine: *Engine, key: Key) anyerror!Frame {
            try engine.state();
            self.frames_built += 1;
            if (!self.depth_attempted and self.frames_built >= 128) try self.prepareDepth(engine);
            if (self.get(key) != null) return .{ .key = key, .children = &.{} };
            const edge = self.path.edges[key.edge];
            const pattern = self.path.nodes[key.edge + 1];
            const node = self.graph.nodes.items[key.node].node;
            var children: std.ArrayList(Key) = .empty;
            if (key.hop >= edge.min_hops and try engine.nodeMatches(node, pattern) and boundNodeMatches(node, pattern, self.row) and try engine.constantProperties(.{ .node = node }, pattern.properties, self.row)) {
                if (key.edge + 1 == self.path.edges.len) {
                    const endpoint_row = (try engine.bind(self.row, pattern.alias, .{ .node = node })) orelse return .{ .key = key, .children = &.{} };
                    var row = try engine.pathSummaryRow(endpoint_row, self.path, self.start.?, node, if (self.needsHops()) self.products.keys.items[key.summary].total + key.hop else 0);
                    if (self.needsHops()) {
                        var history = self.products.keys.items[key.summary].history;
                        while (history != 0) {
                            const entry = self.products.histories.items[history - 1];
                            row = try engine.lengthRow(row, self.path.edges[entry.edge].execution_length, entry.hops);
                            history = entry.parent;
                        }
                        row = try engine.lengthRow(row, edge.execution_length, key.hop);
                    }
                    // A speculative expression error makes this suffix unknown,
                    // never empty. Its error is observed only if DFS reaches it.
                    const accepted = self.accepts(engine, row) catch |err| switch (err) {
                        error.OutOfMemory, error.Canceled, error.Timeout, error.DeadlineExceeded, error.CypherStateBudgetExceeded, error.CypherMemoryBudgetExceeded, error.CypherEdgeBudgetExceeded, error.CypherAnchorBudgetExceeded, error.CypherRecursionBudgetExceeded, error.CypherPathBudgetExceeded => return err,
                        else => true,
                    };
                    if (accepted) {
                        try self.put(engine, key, true);
                        return .{ .key = key, .children = &.{} };
                    }
                } else try children.append(self.a, .{ .node = key.node, .edge = key.edge + 1, .hop = 0, .summary = if (self.needsHops()) try self.products.accepted(engine, key.summary, self.path, key.edge, key.hop) else 0 });
            }
            if (key.hop < edge.max_hops.?) {
                const range = try engine.loadLowerDomain(&self.graph, key.node, key.edge);
                for (self.graph.nodes.items[key.node].arcs.items[range.first..range.last]) |arc| {
                    try engine.state();
                    if (try engine.constantProperties(.{ .relationship = arc.relationship }, edge.properties, self.row)) try children.append(self.a, .{ .node = arc.target, .edge = key.edge, .hop = key.hop + 1, .summary = key.summary });
                }
            }
            return .{ .key = key, .children = children.items };
        }
    };
    // A statement-owned adjacency capture shares physical reads between route
    // prefixes. DAGs and repeatable walks admit distance states indexed by the
    // unsatisfied lower bound; cyclic trails retain exact route histories.
    const LowerArc = struct { target: usize, relationship: *const v.Relationship, mapping: usize, eligible: bool = false };
    const LowerRange = struct { first: usize, last: usize };
    const LowerNode = struct { node: *const v.Node, depth: usize, arcs: std.ArrayList(LowerArc) = .empty, loaded: bool = false, domains: std.AutoHashMapUnmanaged(usize, LowerRange) = .empty };
    const BranchKey = struct { node: usize, pivot: usize };
    const BranchFactorKey = struct { branch: BranchKey, excluded: u16 };
    const BranchChoices = struct { sets: []const []const *const v.Relationship, eligibility: v.EntityMap(u16) };
    const SummaryProducts = struct {
        const Key = struct { node: usize, total: usize = 0, history: usize = 0 };
        const History = struct { parent: usize, edge: usize, hops: usize };
        keys: std.ArrayList(Key) = .empty,
        index: std.AutoHashMapUnmanaged(Key, usize) = .empty,
        histories: std.ArrayList(History) = .empty,
        history_index: std.AutoHashMapUnmanaged(History, usize) = .empty,
        fn intern(self: *@This(), engine: *Engine, key: Key) !usize {
            const entry = try self.index.getOrPut(engine.a, key);
            if (!entry.found_existing) {
                try engine.state();
                entry.value_ptr.* = self.keys.items.len;
                try self.keys.append(engine.a, key);
            }
            return entry.value_ptr.*;
        }
        fn moved(self: *@This(), engine: *Engine, key: usize, node: usize) !usize {
            var next = self.keys.items[key];
            next.node = node;
            return self.intern(engine, next);
        }
        fn accepted(self: *@This(), engine: *Engine, key: usize, path: ast.Path, edge: usize, hops: usize) !usize {
            var next = self.keys.items[key];
            if (path.execution_summary.length != null) next.total += hops;
            if (path.edges[edge].execution_length != null) {
                const history: History = .{ .parent = next.history, .edge = edge, .hops = hops };
                const entry = try self.history_index.getOrPut(engine.a, history);
                if (!entry.found_existing) {
                    try engine.state();
                    entry.value_ptr.* = self.histories.items.len + 1;
                    try self.histories.append(engine.a, history);
                }
                next.history = entry.value_ptr.*;
            }
            return self.intern(engine, next);
        }
    };
    const LowerGraph = struct {
        segment: usize = 0,
        products: ?*SummaryProducts = null,
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
        fn nodeIndex(self: *const @This(), key: usize) usize {
            return if (self.products) |products| products.keys.items[key].node else key;
        }
        fn movedKey(self: *@This(), engine: *Engine, key: usize, node: usize) !usize {
            return if (self.products) |products| products.moved(engine, key, node) else node;
        }
        fn acceptedKey(self: *@This(), engine: *Engine, key: usize, path: ast.Path, edge: usize, hops: usize) !usize {
            return if (self.products) |products| products.accepted(engine, key, path, edge, hops) else key;
        }
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
        if (cursor != null and self.route_pruner != null and self.lower_graph == &self.route_pruner.?.graph) {
            // Lazy reachability can capture a cyclic ancestor while its DFS
            // frame is paging a native reader. Finish that reader with its
            // native token rather than interpreting it as a captured cursor.
            const page = try self.backend.vtable.expand(self.backend.ptr, self.a, mapping, identity, direction, cursor, demand);
            try self.accountEdges(page);
            return page;
        }
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
            // Page values remain pinned in the arena. Index rehashes must
            // reclaim obsolete tables instead of retaining every capacity in
            // that arena and prematurely disabling cross-batch capture.
            try self.pages.put(self.budget.allocator(), owned, .{ .items = items, .next = if (page.next) |next| try a.dupe(u8, next) else null });
        }
        fn deinit(self: *@This()) void {
            self.pages.deinit(self.budget.allocator());
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
        while (initial.next()) |index| depth[graph.nodeIndex(index.*)] = 0;
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
        if (self.stream_fold) |fold| graph.first_only = fold.firstOnly(0) and self.stream_predicate == null;
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
    fn foldShortestSummary(self: *Engine, input: MatchState, path: ast.Path, start: *const v.Node, node: *const v.Node, depth: usize, ways: u64) anyerror!bool {
        if (!self.count_endpoints or ways == std.math.maxInt(u64)) return false;
        const bound = (try self.bind(input.row, path.nodes[1].alias, .{ .node = node })) orelse return true;
        var row = try self.pathSummaryRow(bound, path, start, node, depth);
        row = try self.lengthRow(row, path.edges[0].execution_length, depth);
        if (!try self.constraintsMatch(input.constraints, row, false)) return true;
        if (self.stream_predicate) |predicate| if ((try (try self.eval(predicate, row, null)).truth()) != true) return true;
        const fold = self.stream_fold.?;
        const weight = fold.weight;
        defer fold.weight = weight;
        fold.weight = try weight.multiply(fold.retained, Count.init(if (path.selector == .shortest) 1 else ways));
        try fold.add(self, row);
        return true;
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
                            } else if (!try self.foldShortestSummary(constrained, path, start, current.node, current.depth, current.ways)) {
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
            completed.row = try self.lengthRow(bound, edge.execution_length, relationships.len);
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
            const path_row = (try self.bind(input.row, path.alias, .{ .path = matched_path })) orelse return;
            const row = try self.pathSummaryRow(path_row, path, nodes[0], nodes[nodes.len - 1], relationships.len);
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
                const progress = fold.progress();
                const probe = self.route_probe;
                defer if (probe) |value| {
                    if (value.fold == fold and fold.progress() != progress) value.rejected.* = 0;
                };
                if (probe) |value| if (value.fold == fold) {
                    value.rejected.* +|= 1;
                    if (value.rejected.* >= 64) if (self.route_pruner) |pruner| {
                        if (pruner.fold == fold) try pruner.activate(self);
                    };
                };
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
        if (self.route_pruner) |pruner| if (pruner.active and pruner.path.nodes.ptr == path.nodes.ptr and !try pruner.admits(self, current.identity, edge_index, hops, input.row, relationships.len)) return;
        if (hops >= edge.min_hops and try self.nodeMatches(current, path.nodes[edge_index + 1])) {
            if (try self.bind(input.row, path.nodes[edge_index + 1].alias, .{ .node = current })) |node_row| {
                const edge_value = try self.routeEdgeValue(path, edge, edge_values, relationships);
                if (try self.bind(node_row, edge.alias, edge_value)) |edge_row| {
                    if (try self.constrain(.{ .row = try self.lengthRow(edge_row, edge.execution_length, hops), .used = input.used, .paths = input.paths, .constraints = input.constraints }, .{ .node = current }, path.nodes[edge_index + 1].properties)) |candidate|
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
        const metadata = if (self.read_only and self.statement_ownership != null and self.statement_ownership.?.owns(value))
            try (try self.rootInvariantState()).work.info(value)
        else
            try self.values.info(value);
        if (!metadata.entities) return value;
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
        if (expression.execution_list_source) |id| {
            const source = self.producer_sources.items[id];
            if (expression.execution_range_binding) {
                const sequence = source.arithmetic orelse return .null;
                return self.rangeCells(sequence, 0, sequence.length);
            }
            const previous_owner = self.local_invariants;
            const previous_locals = self.projection_locals;
            self.local_invariants = source.owner;
            self.projection_locals = source.locals;
            defer self.local_invariants = previous_owner;
            defer self.projection_locals = previous_locals;
            return self.eval(source.expression, source.row, null);
        }
        // Validate only reached affine stages. Hoisting this check to the bag
        // producer would incorrectly evaluate errors behind an earlier filter.
        try self.validateMomentNode(expression);
        if (expression.execution_range_binding and expression.value == .call) {
            const range = (try self.rangeInput(expression, row, group)) orelse return .null;
            return .{ .list = try self.a.dupe(v.Value, &.{
                .{ .integer = range.start },                                 .{ .integer = range.step },
                .{ .integer = @bitCast(@as(u64, @truncate(range.length))) }, .{ .integer = @intCast(range.length >> 64) },
            }) };
        }
        // Cardinality/window lowering wraps a graph producer in a private
        // execution-list summary. Cache that bounded summary on its captured
        // bindings just as we cache the original SIZE(pattern) expression.
        const memoizable = self.read_only and self.projection_overrides == null and self.aggregate_overrides == null and (expression.value == .pattern_comprehension or expression.value == .exists or (expression.value == .call and (expression.value.call.execution_list or std.ascii.eqlIgnoreCase(expression.value.call.name, "size")) and expression.value.call.args.len == 1 and expression.value.call.args[0].value == .pattern_comprehension)) and weightedExpression(expression);
        var key: Row = &.{};
        if (memoizable) {
            if (self.expression_memo == null) self.expression_memo = .{ .identity_keys = true, .budget = .{ .backing = self.scratch_allocator, .limit = @min(8 << 20, self.limits.memory_bytes / 16) } };
            var references: Names = .empty;
            try self.retainReferences(&references, row, expression);
            var captured: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (references.contains(entry.name)) try captured.append(self.a, entry);
            key = captured.items;
            if (try self.expression_memo.?.find(self, key, @intFromPtr(expression))) |cached| {
                const value = cached.rows[0][0].value;
                // A nested memo is destroyed when its pattern invocation
                // returns. Values exposed to a containing list/row must own
                // their payloads in that caller's allocation epoch first.
                return if (self.expression_memo_scoped) self.cloneScoped(value, self.a) else value;
            }
        }
        const invariant = (self.read_only or self.local_invariants != null) and expression.execution_invariant != null;
        const previous_strings = self.scalar_strings;
        self.scalar_strings = null;
        defer self.scalar_strings = previous_strings;
        const result = result: {
            if (!invariant) break :result try self.evalInner(expression, row, group);
            const owner = try self.invariantState();
            if (owner.scalars.get(expression.execution_invariant.?)) |cached| break :result cached;
            if (owner.strings == null) owner.strings = self.newStringWork(owner.work.a);
            self.scalar_strings = &owner.strings.?;
            // Closed expressions cannot borrow recycled row values. Allocate
            // their computations directly in statement storage: cloning a
            // materialized RANGE or list here would double its peak memory.
            const scope = EvalScope.enter(self, owner.work.a);
            defer scope.leave(self);
            const owned = try self.evalInner(expression, row, group);
            _ = try owner.work.info(owned);
            try owner.scalars.put(owner.work.a, expression.execution_invariant.?, owned);
            break :result owned;
        };
        if (memoizable) try self.expression_memo.?.put(self, key, @intFromPtr(expression), .{ .rows = &.{&.{.{ .name = "value", .value = result }}}, .weights = &.{Count.one} });
        if (expression.value == .parameter or (expression.value == .literal and result == .string)) {
            _ = try (try self.invariantState()).work.info(result);
        } else if (self.read_only and self.statement_ownership != null and self.statement_ownership.?.owns(result)) {
            // A destination owned by the statement cannot be recycled by a
            // row/producer epoch. Cache its analysis in that same root epoch.
            _ = try (try self.rootInvariantState()).work.info(result);
        } else _ = try self.values.info(result);
        return result;
    }
    fn newStringWork(self: *Engine, allocator: A) @import("string_work.zig").Runtime {
        return .{ .a = allocator, .scratch = self.scratch_allocator, .ptr = self, .charge = chargeWork, .prefix_budget = @min(256 << 10, self.limits.memory_bytes / 64) };
    }
    fn stringWork(self: *Engine) *@import("string_work.zig").Runtime {
        return self.scalar_strings orelse &self.strings;
    }
    fn immutableOwner(self: *Engine, value: v.Value) ?*Invariants {
        // Prefer the longest proven lifetime, even if a loop has also observed
        // this value. A new loop must not discard a root-owned string's analysis.
        // Roots introduced after a loop entered need no parent link to that loop.
        if (self.invariants) |owner| if (owner.work.hasMetadata(value)) return owner;
        return immutableOwnerFrom(self.scalarOwner(), value);
    }
    fn immutableOwnerFrom(start: ?*Invariants, value: v.Value) ?*Invariants {
        var owner = start;
        while (owner) |current| : (owner = current.parent) {
            if (current.work.hasMetadata(value)) return current;
        }
        return null;
    }
    fn stringInputWork(self: *Engine, input: v.Value) *@import("string_work.zig").Runtime {
        if (self.immutableOwner(input)) |owner| {
            if (owner.strings == null) owner.strings = self.newStringWork(owner.work.a);
            return &owner.strings.?;
        }
        return self.stringWork();
    }
    fn stringResult(self: *Engine, input: v.Value, result: v.Value) !v.Value {
        if (self.immutableOwner(input)) |owner| {
            // Cached conversion results and borrowed trims share the immutable
            // input owner's lifetime; dynamic substrings are not registered.
            _ = try owner.work.info(result);
        }
        return result;
    }
    fn scalarOwner(self: *Engine) ?*Invariants {
        return if (self.local_invariants) |owner| owner else self.invariants;
    }
    fn rootInvariantState(self: *Engine) !*Invariants {
        if (self.invariants == null) {
            const owner = try self.planning_allocator.create(Invariants);
            owner.* = .{ .work = .{ .a = self.planning_allocator, .ptr = self, .charge = chargeWork } };
            self.invariants = owner;
        }
        return self.invariants.?;
    }
    fn invariantState(self: *Engine) !*Invariants {
        if (self.scalarOwner() == null) {
            const state_value = try self.planning_allocator.create(Invariants);
            state_value.* = .{ .work = .{ .a = self.planning_allocator, .ptr = self, .charge = chargeWork } };
            self.invariants = state_value;
        }
        const owner = self.scalarOwner().?;
        var parent = &self.values.parent;
        while (parent.*) |current| {
            if (current == &owner.work) return owner;
            // Insert a new child before any shared ancestor. Appending it to
            // that ancestor would invert ownership and create a parent cycle.
            var ancestor = owner.work.parent;
            while (ancestor) |known| : (ancestor = known.parent) if (known == current) {
                parent.* = &owner.work;
                return owner;
            };
            parent = &current.parent;
        }
        parent.* = &owner.work;
        return owner;
    }
    fn membershipBudget(self: *Engine, owner: *Invariants) !*Budget {
        if (owner.membership_budget == null) {
            const budget = try owner.work.a.create(Budget);
            budget.* = .{ .backing = self.scratch_allocator, .limit = @min(1 << 20, self.limits.memory_bytes / 8) };
            owner.membership_budget = budget;
        }
        return owner.membership_budget.?;
    }
    /// Binding-keyed membership state shares the statement's source budget.
    /// Immutable inputs use allocation identity, so forwarding a large parameter
    /// through WITH never rehashes or clones that parameter for each probe.
    const ProducerMemo = struct {
        const Producer = @import("invariant_membership.zig").Producer;
        const Entry = struct { key: Row, expression: *const ast.Expr, producer: *Producer };
        const Allocation = struct { tag: std.meta.Tag(v.Value), address: usize, length: usize };
        budget: Budget,
        arena: ?std.heap.ArenaAllocator = null,
        entries: std.ArrayList(Entry) = .empty,
        index: value_work.Index = .{},
        disabled: bool = false,
        fn deinit(self: *@This()) void {
            for (self.entries.items) |entry| entry.producer.deinit();
            if (self.arena) |*arena| arena.deinit();
        }
        fn allocation(value: v.Value) ?Allocation {
            return switch (value) {
                .list => |x| .{ .tag = .list, .address = @intFromPtr(x.ptr), .length = x.len },
                .map => |x| .{ .tag = .map, .address = @intFromPtr(x.ptr), .length = x.len },
                .string => |x| .{ .tag = .string, .address = @intFromPtr(x.ptr), .length = x.len },
                else => null,
            };
        }
        fn get(self: *@This(), engine: *Engine, expression: *const ast.Expr, key: Row, source_budget: *Budget) !?*Producer {
            if (self.disabled) return null;
            return self.lookup(engine, expression, key, source_budget) catch |err| {
                if (err != error.OutOfMemory or !self.budget.exhausted) return err;
                self.disabled = true;
                return null;
            };
        }
        fn lookup(self: *@This(), engine: *Engine, expression: *const ast.Expr, key: Row, source_budget: *Budget) !*Producer {
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            var work: value_work.Work = .{ .a = scratch.allocator(), .ptr = engine, .charge = chargeWork, .binding_identity = true };
            var hash = std.hash.Wyhash.init(@intFromPtr(expression));
            for (key) |entry| {
                try engine.state();
                hash.update(entry.name);
                if (engine.immutableOwner(entry.value) != null) {
                    std.hash.autoHash(&hash, allocation(entry.value).?);
                } else std.hash.autoHash(&hash, (try work.info(entry.value)).hash);
            }
            const code = hash.final();
            var candidate = self.index.head(code);
            while (candidate) |position| {
                const link = self.index.links.items[position];
                const entry = self.entries.items[link.index];
                if (entry.expression == expression and entry.key.len == key.len) match: {
                    for (key, entry.key) |input, saved| {
                        if (!std.mem.eql(u8, input.name, saved.name)) break :match;
                        if (engine.immutableOwner(saved.value) != null) {
                            if (!std.meta.eql(allocation(input.value), allocation(saved.value))) break :match;
                        } else if (!try work.eql(input.value, saved.value)) break :match;
                    }
                    return entry.producer;
                }
                candidate = link.next;
            }
            if (self.arena == null) self.arena = .init(self.budget.allocator());
            const a = self.arena.?.allocator();
            const owned = try a.alloc(v.Entry, key.len);
            // Read-only entity binding equality uses identity, not properties.
            // A high-degree anchor can carry a large native edge payload; none
            // of that payload belongs in a membership cache's captured key.
            var clone: v.CloneContext = .{ .a = a, .scratch = scratch.allocator(), .identity_only = true };
            for (key, owned) |entry, *out| out.* = .{
                .name = try a.dupe(u8, entry.name),
                .value = if (engine.immutableOwner(entry.value) != null) entry.value else try clone.clone(entry.value),
            };
            const producer = try a.create(Producer);
            producer.* = .{ .budget = source_budget };
            try self.entries.ensureUnusedCapacity(a, 1);
            try self.index.insert(a, code, self.entries.items.len);
            self.entries.appendAssumeCapacity(.{ .key = owned, .expression = expression, .producer = producer });
            return producer;
        }
    };
    fn evalMembership(self: *Engine, left: v.Value, original_right: *const ast.Expr, row: Row, group: ?[]const Row) !v.Value {
        // IN observes equality and NULLs, not encounter order. The producer
        // still completes its original source phase and exposes late errors.
        var right = original_right;
        while (reduction_plan.reversedList(right)) |input| right = input;
        if (isRange(right)) {
            const range = (try self.rangeInput(right, row, group)) orelse return .null;
            return range.contains(left);
        }
        if (reduction_plan.reversedInput(right) != null) {
            // Unknown overloads resolve at runtime. List membership ignores
            // direction and shares the original allocation's equality index.
            // Do not feed an already-owned parameter through a bounded
            // projected-source cache and repeatedly rescan oversized inputs.
            const source = (try self.sequenceSource(right, row, group)) orelse return .null;
            return switch (source.values) {
                .range => |range| range.contains(left),
                .list => |items| self.membershipValue(left, .{ .list = items }),
                .scalar => |value| self.binary(.in, left, value),
            };
        }
        const invariant = self.read_only and right.execution_invariant != null;
        if (projectedSource(right)) {
            var candidate: ?*@import("invariant_membership.zig").Producer = null;
            const stream = if (invariant) blk: {
                const owner = try self.invariantState();
                var existing = owner.producer_lists.get(right.execution_invariant.?);
                if (existing == null) {
                    const cache = try owner.work.a.create(@import("invariant_membership.zig").Producer);
                    cache.* = .{ .budget = try self.membershipBudget(owner) };
                    try owner.producer_lists.put(owner.work.a, right.execution_invariant.?, cache);
                    existing = cache;
                }
                const cache = existing.?;
                if (cache.ready) return cache.membership.contains(&self.values, left);
                // The first Boolean probe pays no retained-source cost. A
                // repeated probe may build an index within the soft budget.
                if (cache.seen and !cache.disabled) candidate = cache;
                cache.seen = true;
                break :blk true;
            } else if (self.read_only and weightedExpression(right) and !aggregates(right)) blk: {
                const owner = try self.invariantState();
                if (owner.captured_lists == null) owner.captured_lists = .{ .budget = .{ .backing = self.scratch_allocator, .limit = @min(1 << 20, self.limits.memory_bytes / 32) } };
                var references: Names = .empty;
                try self.retainReferences(&references, row, right);
                var captured: std.ArrayList(v.Entry) = .empty;
                for (row) |entry| if (references.contains(entry.name)) try captured.append(self.a, entry);
                if (try owner.captured_lists.?.get(self, right, captured.items, try self.membershipBudget(owner))) |cache| {
                    if (cache.ready) return cache.membership.contains(&self.values, left);
                    if (cache.seen and !cache.disabled) candidate = cache;
                    cache.seen = true;
                }
                break :blk true;
            } else true;
            if (stream) {
                var membership: MembershipReceiver = .{ .left = left, .left_info = try self.values.info(left), .candidate = candidate };
                var receiver: ListReceiver = .{ .ptr = &membership, .emit = MembershipReceiver.add, .range = MembershipReceiver.range, .destination = self.a, .observer = .{ .name = "collect", .args = &.{} } };
                const present = try self.produceList(right, row, group, &receiver);
                if (candidate) |cache| cache.finish(present);
                if (!present) return .null;
                try receiver.check();
                return if (membership.found) .{ .boolean = true } else if (membership.unknown) .null else .{ .boolean = false };
            }
        }
        if (!invariant) return self.membershipValue(left, try self.eval(right, row, group));
        const state_value = try self.invariantState();
        if (state_value.lists.getPtr(right.execution_invariant.?)) |cached| return cached.contains(&self.values, left);
        // Evaluate in its original position, then acquire independent ownership
        // before producer pages or nested expression arenas can be recycled.
        const value = try self.eval(right, row, group);
        if (value != .null and value != .list) return error.CypherTypeMismatch;
        const source: ?Invariants.Identity = if (value == .list and state_value.work.hasMetadata(value)) .{ .address = @intFromPtr(value.list.ptr), .length = value.list.len } else null;
        if (source) |id| if (state_value.owned_lists.get(id)) |cached| {
            // Cache headers are immutable views of statement-owned allocations;
            // direct uses and forwarded aliases share one physical membership.
            try state_value.lists.put(self.planning_allocator, right.execution_invariant.?, cached);
            return state_value.lists.getPtr(right.execution_invariant.?).?.contains(&self.values, left);
        };
        const built = try @import("invariant_membership.zig").Membership.init(self.planning_allocator, self.scratch_allocator, &state_value.bytes, &state_value.work, value);
        if (source) |id| try state_value.owned_lists.put(self.planning_allocator, id, built);
        try state_value.lists.put(self.planning_allocator, right.execution_invariant.?, built);
        return state_value.lists.getPtr(right.execution_invariant.?).?.contains(&self.values, left);
    }
    fn membershipValue(self: *Engine, left: v.Value, value: v.Value) !v.Value {
        if (self.read_only and value == .list) if (self.invariants) |owner| if (owner.work.hasMetadata(value)) {
            // Forwarded parameter lists have statement-owned provenance.
            // Key by allocation, since one row variable can alternate lists.
            const id: Invariants.Identity = .{ .address = @intFromPtr(value.list.ptr), .length = value.list.len };
            if (owner.owned_lists.getPtr(id)) |cached| return cached.contains(&self.values, left);
            const built = try @import("invariant_membership.zig").Membership.init(self.planning_allocator, self.scratch_allocator, &owner.bytes, &owner.work, value);
            try owner.owned_lists.put(self.planning_allocator, id, built);
            return owner.owned_lists.getPtr(id).?.contains(&self.values, left);
        };
        return self.binary(.in, left, value);
    }
    /// The source owns its values in the caller's evaluation scope. RANGE is
    /// represented arithmetically; each consumer decides when to stop visiting.
    /// General expressions are evaluated eagerly before the consumer starts, so
    /// their errors cannot be hidden behind a predicate's short circuit.
    const ListSource = struct {
        values: union(enum) { list: []const v.Value, range: @import("range.zig").Range, scalar: v.Value },
        position: u128 = 0,
        reversed: bool = false,
        fn length(self: @This()) u128 {
            return switch (self.values) {
                .scalar => 1,
                .list => |items| items.len,
                .range => |sequence| sequence.length,
            };
        }
        fn at(self: @This(), position: u128) v.Value {
            const ordinal = if (self.reversed) self.length() - position - 1 else position;
            return switch (self.values) {
                .scalar => |value| value,
                .list => |items| items[@intCast(ordinal)],
                .range => |sequence| sequence.at(ordinal),
            };
        }
        fn next(self: *@This()) ?v.Value {
            return self.nextDirection(false);
        }
        fn nextDirection(self: *@This(), reverse: bool) ?v.Value {
            const extent: u128 = switch (self.values) {
                .scalar => 1,
                .list => |x| x.len,
                .range => |x| x.length,
            };
            if (self.position >= extent) return null;
            const ordinal = if (reverse != self.reversed) extent - self.position - 1 else self.position;
            const value = switch (self.values) {
                .scalar => |value| if (self.position == 0) value else return null,
                .list => |items| items[@intCast(ordinal)],
                .range => |sequence| sequence.at(ordinal),
            };
            self.position += 1;
            return value;
        }
    };
    fn listSource(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?ListSource {
        const source = (try self.sequenceSource(expression, row, group)) orelse return null;
        if (source.values == .scalar) return error.CypherTypeMismatch;
        return source;
    }
    fn unwindSource(self: *Engine, expression: *const ast.Expr, row: Row) !?ListSource {
        return self.sequenceSource(expression, row, null);
    }
    /// Resolve overloads once, keeping list direction in a cursor rather than
    /// allocating a reversed copy. Scalar UNWIND and string REVERSE retain
    /// their ordinary behavior; list-only callers check the resulting kind.
    fn sequenceSource(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?ListSource {
        if (isRange(expression)) {
            const sequence = (try self.rangeInput(expression, row, group)) orelse return null;
            return .{ .values = .{ .range = sequence } };
        }
        if (reduction_plan.reversedInput(expression)) |input| {
            try self.state();
            var source = (try self.sequenceSource(input, row, group)) orelse return null;
            switch (source.values) {
                .list, .range => source.reversed = !source.reversed,
                .scalar => |value| {
                    if (value != .string) return error.CypherTypeMismatch;
                    source.values = .{ .scalar = try self.stringResult(value, .{ .string = try self.stringInputWork(value).reverse(value.string) }) };
                },
            }
            return source;
        }
        const value = try self.eval(expression, row, group);
        if (value == .null) return null;
        return .{ .values = if (value == .list) .{ .list = value.list } else .{ .scalar = value } };
    }
    fn cloneScoped(self: *Engine, value: v.Value, allocator: A) !v.Value {
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const parent = if (self.read_only and allocator.ptr == self.planning_allocator.ptr and allocator.vtable == self.planning_allocator.vtable) try self.statementOwnership() else null;
        var context: v.CloneContext = .{ .a = allocator, .scratch = scratch.allocator(), .owned_parent = parent, .byte_pool = if (parent) |proof| proof.byte_pool else null, .byte_allocator = self.scratch_allocator };
        const owned = try context.clone(value);
        if (parent) |proof| {
            try proof.adoptClones(&context);
            if (self.capture_input_clones) |input| try input.captureInputStrings(&context);
        }
        return owned;
    }
    fn cloneRowScoped(self: *Engine, row: Row, allocator: A) !Row {
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const parent = if (self.read_only and allocator.ptr == self.planning_allocator.ptr and allocator.vtable == self.planning_allocator.vtable) try self.statementOwnership() else null;
        var context: v.CloneContext = .{ .a = allocator, .scratch = scratch.allocator(), .owned_parent = parent, .byte_pool = if (parent) |proof| proof.byte_pool else null, .byte_allocator = self.scratch_allocator };
        const owned = try cloneRowContext(row, &context);
        if (parent) |proof| {
            try proof.adoptClones(&context);
            if (self.capture_input_clones) |input| try input.captureInputStrings(&context);
        }
        return owned;
    }
    fn cloneAggregateScoped(self: *Engine, value: v.Value, allocator: A) !v.Value {
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var context: v.CloneContext = .{ .a = allocator, .scratch = scratch.allocator(), .borrowed_parent = if (self.read_only) self.input_clones else null };
        return context.clone(value);
    }
    const ListDemand = union(enum) { all, count, window: reduction_plan.Bounds };
    /// A source finishes before a receiver's scalar failure becomes observable.
    /// This composes eager list phases without retaining their intermediate
    /// values. Resource failures and pipeline control signals remain immediate.
    const ListReceiver = struct {
        ptr: *anyopaque,
        emit: *const fn (*anyopaque, *Engine, v.Value, Count, ?*const value_work.Work) anyerror!void,
        destination: A,
        observer: ast.Function,
        failure: ?anyerror = null,
        range: ?*const fn (*anyopaque, *Engine, @import("range.zig").Range) anyerror!void = null,
        complete: ?*const fn (*anyopaque) bool = null,
        fn isComplete(self: *const @This()) bool {
            return self.failure != null or (if (self.complete) |complete| complete(self.ptr) else false);
        }
        fn accept(self: *@This(), engine: *Engine, value: v.Value, weight: Count, borrowed: ?*const value_work.Work) !void {
            if (self.failure != null) return;
            self.emit(self.ptr, engine, value, weight, borrowed) catch |err| {
                if (!StreamingAggregate.consumerFailure(err)) return err;
                self.failure = err;
            };
        }
        fn check(self: @This()) !void {
            if (self.failure) |err| return err;
        }
    };
    const MembershipReceiver = struct {
        left: v.Value,
        left_info: value_work.Info,
        found: bool = false,
        unknown: bool = false,
        candidate: ?*@import("invariant_membership.zig").Producer = null,
        fn range(ptr: *anyopaque, _: *Engine, sequence: @import("range.zig").Range) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.candidate) |cache| {
                cache.deinit();
                cache.disabled = true;
            }
            const result = sequence.contains(self.left);
            if (result == .null) self.unknown = true else self.found = self.found or result.boolean;
        }
        fn add(ptr: *anyopaque, engine: *Engine, value: v.Value, _: Count, _: ?*const value_work.Work) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.candidate) |cache| try cache.add(value, engine, chargeWork);
            if (self.found) return;
            // Producer pages recycle metadata. Keep the needle's metadata in
            // the receiver and reject unequal, NULL-free hashes before a wide
            // equality traversal. Nullable structures still need three-valued
            // comparison, and every hash match is collision checked.
            const info = try engine.values.info(value);
            if (!self.left_info.unknown and !info.unknown and self.left_info.hash != info.hash) return;
            if (try engine.values.equal(self.left, value)) |same| self.found = same else self.unknown = true;
        }
    };
    const UnwindReceiver = struct {
        fold: *StreamFold,
        row: Row,
        index: usize,
        owner: ?*Invariants,
        locals: ?*const @import("expressions.zig").Local,
        complete: ?anyerror = null,
        weighted: bool,
        fn isComplete(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.complete != null;
        }
        fn add(ptr: *anyopaque, engine: *Engine, value: v.Value, weight: Count, _: ?*const value_work.Work) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const copies = if (self.weighted) 1 else std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded;
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            for (0..copies) |_| {
                if (self.complete != null) return;
                _ = scratch.reset(.retain_capacity);
                const scope = EvalScope.enter(engine, scratch.allocator());
                defer scope.leave(engine);
                try self.addValue(engine, value, if (self.weighted) weight else Count.one);
            }
        }
        fn addValue(self: *@This(), engine: *Engine, value: v.Value, weight: Count) !void {
            // A source comprehension owns temporary lexical bindings and
            // invariant caches. The continuation belongs to its caller.
            const previous_owner = engine.local_invariants;
            const previous_locals = engine.projection_locals;
            engine.local_invariants = self.owner;
            engine.projection_locals = self.locals;
            defer engine.local_invariants = previous_owner;
            defer engine.projection_locals = previous_locals;
            const previous_weight = self.fold.weight;
            self.fold.weight = try previous_weight.multiply(engine.a, weight);
            defer self.fold.weight = previous_weight;
            try engine.state();
            if (self.fold.record_rows) self.fold.stage_counts[self.index + 1] +|= @intCast(self.fold.weight.cap(std.math.maxInt(usize)));
            const unwind = self.fold.middle[self.index].operation.unwind;
            self.fold.expand(engine, try engine.replace(self.row, unwind.alias, value), self.index + 1) catch |err| {
                // LIMIT/admission stops the consumer, but eager source
                // validation must still reach every remaining projection.
                if (err != error.CypherStreamComplete and err != error.CypherAdmissionFound) return err;
                self.complete = err;
            };
        }
    };
    const ListMapper = struct {
        expression: *const ast.Expr,
        row: Row,
        locals: ?*const @import("expressions.zig").Local,
        output: *ListReceiver,
        scratch: std.heap.ArenaAllocator,
        embedding_failure: bool = false,
        repeatable: bool,
        fn isComplete(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.output.isComplete();
        }
        fn add(ptr: *anyopaque, engine: *Engine, value: v.Value, weight: Count, borrowed: ?*const value_work.Work) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const c = self.expression.value.comprehension;
            const copies = if (self.repeatable) 1 else std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded;
            for (0..copies) |_| {
                try engine.state();
                _ = self.scratch.reset(.retain_capacity);
                const scope = EvalScope.enter(engine, self.scratch.allocator());
                defer scope.leave(engine);
                const previous = engine.projection_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = self.locals };
                engine.projection_locals = &local;
                defer engine.projection_locals = previous;
                const row = try engine.replace(self.row, c.alias, value);
                if (c.predicate) |predicate| if ((try (try engine.eval(predicate, row, null)).truth()) != true) continue;
                const projected = if (c.projection) |projection| try engine.eval(projection, row, null) else value;
                _ = engine.values.info(.{ .list = &.{projected} }) catch |err| blk: {
                    if (err != error.CypherValueDepthExceeded) return err;
                    self.embedding_failure = true;
                    break :blk null;
                };
                try self.output.accept(engine, projected, if (self.repeatable) weight else Count.one, borrowed);
            }
        }
    };
    fn producePatternCount(self: *Engine, expression: *const ast.Expr, input: Row, receiver: *ListReceiver) !void {
        const c = expression.value.pattern_comprehension;
        const count_expression: ast.Expr = .{ .span = expression.span, .value = .{ .call = .{ .name = "count", .args = &.{}, .star = true } } };
        // Preserve symbolic path/relationship dependencies without asking for
        // concrete routes. A count-only consumer never observes their order.
        const dependencies = [_]ast.Clause{
            .{ .span = expression.span, .operation = .{ .project = .{ .value = .{ .items = &.{.{ .expression = c.projection, .alias = "_comprehension_value" }} }, .final = false } } },
            .{ .span = expression.span, .operation = .{ .project = .{ .value = .{ .items = &.{.{ .expression = &count_expression }} }, .final = true } } },
        };
        const matched = try self.weightedMatch(input, .{ .paths = &.{c.path}, .predicate = c.predicate }, &dependencies);
        var embedding_failure = false;
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        for (matched.rows, matched.weights) |row, weight| {
            _ = scratch.reset(.retain_capacity);
            const scope = EvalScope.enter(self, scratch.allocator());
            defer scope.leave(self);
            const value = try self.eval(c.projection, row, null);
            _ = self.values.info(.{ .list = &.{value} }) catch |err| blk: {
                if (err != error.CypherValueDepthExceeded) return err;
                embedding_failure = true;
                break :blk null;
            };
            try receiver.accept(self, value, weight, null);
        }
        if (embedding_failure) return error.CypherValueDepthExceeded;
    }
    fn producePattern(self: *Engine, expression: *const ast.Expr, row: Row, receiver: *ListReceiver) !bool {
        if (!self.read_only or !weightedExpression(expression)) return false;
        const scope = PatternScope.enter(self);
        defer scope.leave(self);
        const c = expression.value.pattern_comprehension;
        var input = row;
        if (c.path.alias) |alias| {
            var bindings: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (!std.mem.eql(u8, entry.name, alias)) try bindings.append(self.a, entry);
            input = bindings.items;
        }
        const cardinality = receiver.observer.collect_limit == 0 and receiver.observer.collect_suffix == 0 and receiver.observer.collect_filters.len == 0;
        if (cardinality and !endpointExpression(c.projection, c.path, input)) {
            try self.producePatternCount(expression, input, receiver);
            return true;
        }
        const previous_demand = self.match_weight_demand;
        defer self.match_weight_demand = previous_demand;
        if (cardinality) self.match_weight_demand = .exact;
        var observer_call = receiver.observer;
        observer_call.name = "collect";
        observer_call.execution_list = false;
        observer_call.collection_extract = null;
        observer_call.args = &.{c.projection};
        var consumer: PatternConsumer = .{ .projection = c.projection, .cardinality = cardinality, .receiver = receiver, .reducer = .{
            .function = observer_call,
            .arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
            .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
        } };
        defer consumer.reducer.deinit();
        const observer: ast.Expr = .{ .span = expression.span, .value = .{ .call = observer_call } };
        const columns = self.columns;
        const stages = self.stages.items.len;
        defer self.columns = columns;
        defer self.stages.shrinkRetainingCapacity(stages);
        if ((try self.streamMatchProjectionInto(&.{input}, null, .{ .paths = &.{c.path}, .predicate = c.predicate }, .{ .items = &.{.{ .expression = if (cardinality) c.projection else &observer, .alias = "_list_producer" }} }, &.{}, .{}, .{}, null, &consumer)) == null) return false;
        if (consumer.failure) |err| return err;
        if (consumer.embedding_failure) return error.CypherValueDepthExceeded;
        return true;
    }
    /// Completion propagates only through a producer whose own projections
    /// have finished their eager phase. Validating an input does not validate
    /// an enclosing mapper: that mapper must still visit every source value.
    const ProductionPhase = enum { initial, replay };
    fn produceList(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row, receiver: *ListReceiver) anyerror!bool {
        return self.produceListOrder(expression, row, group, receiver, false, .initial);
    }
    fn produceListOrder(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row, receiver: *ListReceiver, reverse: bool, phase: ProductionPhase) anyerror!bool {
        if (phase == .replay and receiver.isComplete()) return true;
        if (expression.execution_list_source) |id| if (!isRange(expression)) {
            const source = self.producer_sources.items[id];
            if (source.summary.list[0] == .null) return false;
            if (receiver.observer.collect_limit == 0 and receiver.observer.collect_filters.len == 0 and !receiver.observer.distinct and source.summary.list[0].integer >= 0) {
                const length: u64 = @intCast(source.summary.list[0].integer);
                if (length != 0) try receiver.accept(self, .null, Count.init(length), null);
                return true;
            }
            const previous_owner = self.local_invariants;
            const previous_locals = self.projection_locals;
            self.local_invariants = source.owner;
            self.projection_locals = source.locals;
            defer self.local_invariants = previous_owner;
            defer self.projection_locals = previous_locals;
            return self.produceListOrder(source.expression, source.row, null, receiver, reverse, .replay);
        };

        if (self.read_only and reduction_plan.repeatable(expression)) if (reduction_plan.reversedList(expression)) |input| {
            // Reverse changes encounter order only after its eager input phase.
            // Validate repeatable projections in original order, then replay
            // them against a source cursor which walks in the other direction.
            if (phase == .initial) {
                const summary = try self.listSummary(.{ .name = "collect", .args = &.{input}, .collect_limit = 0, .collect_summary = true }, row, group);
                if (summary.list[0] == .null) return false;
            }
            return self.produceListOrder(input, row, group, receiver, !reverse, .replay);
        };
        if (self.read_only and reduction_plan.repeatable(expression)) if (reduction_plan.concatenatedList(expression)) |b| {
            // Addition evaluates both complete operands before exposing a list.
            // Validate into bounded summaries first, including NULL absorption
            // and scalar embedding checks. Repeatable sources can then replay
            // into the consumer without retaining their intermediate payloads.
            if (phase == .initial) {
                const summary = try self.concatSummary(.{ .name = "collect", .args = &.{expression}, .collect_limit = 0, .collect_summary = true }, b.left, b.right, row, group);
                if (summary.list[0] == .null) return false;
            }
            try self.produceAddedOperand(if (reverse) b.right else b.left, row, group, receiver, reverse);
            try self.produceAddedOperand(if (reverse) b.left else b.right, row, group, receiver, reverse);
            return true;
        };
        if (expression.value == .comprehension) {
            var invariants: LoopScope = .{};
            defer invariants.deinit();
            const previous = try invariants.enter(self);
            defer self.local_invariants = previous;
            // Input expressions also run in this producer epoch. Keep its
            // cache chain above the outer owner before evaluating closed input
            // lists; no longer-lived owner may ever acquire a child parent.
            const input_scope = EvalScope.enter(self, self.a);
            defer input_scope.leave(self);
            const c = expression.value.comprehension;
            var mapper: ListMapper = .{ .repeatable = (c.predicate == null or reduction_plan.repeatable(c.predicate.?)) and (c.projection == null or reduction_plan.repeatable(c.projection.?)), .expression = expression, .row = row, .locals = self.projection_locals, .output = receiver, .scratch = std.heap.ArenaAllocator.init(self.scratch_allocator) };
            defer mapper.scratch.deinit();
            // A nested mapping is an ordered full observer of its input. Its
            // own output demand still bounds retention in the next receiver.
            var mapped: ListReceiver = .{ .ptr = &mapper, .emit = ListMapper.add, .complete = if (phase == .replay) ListMapper.isComplete else null, .destination = receiver.destination, .observer = .{ .name = "collect", .args = &.{} } };
            const present = try self.produceListOrder(expression.value.comprehension.input, row, group, &mapped, reverse, phase);
            try mapped.check();
            if (mapper.embedding_failure) return error.CypherValueDepthExceeded;
            return present;
        }
        if (expression.value == .pattern_comprehension and try self.producePattern(expression, row, receiver)) return true;
        var source = (try self.listSource(expression, row, group)) orelse return false;
        if (source.values == .range) if (receiver.range) |consume| {
            try self.state();
            try consume(receiver.ptr, self, source.values.range);
            return true;
        };
        if (source.values == .range and receiver.observer.collect_limit == 0 and receiver.observer.collect_filters.len == 0 and !receiver.observer.distinct) {
            const length = source.values.range.length;
            if (length != 0) {
                const weight = if (std.math.cast(u64, length)) |n| Count.init(n) else try Count.init(std.math.maxInt(u64)).add(self.a, Count.one);
                try self.state();
                try receiver.accept(self, source.values.range.at(0), weight, null);
            }
            return true;
        }
        // Only this caller-owned allocation epoch can be borrowed by escaping
        // values. The snapshot is immutable while receivers enter inner scopes;
        // it is never retained after the source returns or used as a value key.
        const metadata = self.values;
        const borrowed = if (self.a.ptr == receiver.destination.ptr and self.a.vtable == receiver.destination.vtable) &metadata else null;
        while (source.nextDirection(reverse)) |value| {
            if (phase == .replay and receiver.isComplete()) break;
            try self.state();
            try receiver.accept(self, value, Count.one, borrowed);
        }
        return true;
    }
    fn produceAddedOperand(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row, receiver: *ListReceiver, reverse: bool) anyerror!void {
        if (receiver.isComplete()) return;
        // The enclosing summary already validated this entire addition tree.
        // Replay nested additions directly, rather than validating every
        // prefix again (which would make a long concatenation quadratic).
        if (reduction_plan.concatenatedList(expression)) |b| {
            try self.produceAddedOperand(if (reverse) b.right else b.left, row, group, receiver, reverse);
            try self.produceAddedOperand(if (reverse) b.left else b.right, row, group, receiver, reverse);
            return;
        }
        if (isRange(expression) or projectedSource(expression)) {
            _ = try self.produceListOrder(expression, row, group, receiver, reverse, .replay);
            return;
        }
        var source = (try self.sequenceSource(expression, row, group)) orelse return;
        while (source.nextDirection(reverse)) |value| {
            if (receiver.isComplete()) break;
            try self.state();
            try receiver.accept(self, value, Count.one, null);
        }
    }

    const ListWriter = struct {
        demand: ListDemand,
        destination: A,
        caller_owner: ?*Invariants,
        values: std.ArrayList(v.Value) = .empty,
        cardinality: u64 = 0,
        window: StreamingAggregate,
        fn add(ptr: *anyopaque, engine: *Engine, value: v.Value, weight: Count, borrowed: ?*const value_work.Work) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (self.demand) {
                .count => self.cardinality = try addPathCount(self.cardinality, weight.cap(std.math.maxInt(u64))),
                .window => try self.window.retainCollection(engine, value, weight, null),
                .all => {
                    const copies = std.math.cast(usize, weight.exact() orelse return error.CypherStateBudgetExceeded) orelse return error.CypherStateBudgetExceeded;
                    try chargeWork(engine, copies);
                    const owner = if (engine.read_only) engine.scalarOwner() else self.caller_owner;
                    const owned = if ((borrowed != null and borrowed.?.hasMetadata(value)) or immutableOwnerFrom(owner, value) != null) value else try engine.cloneScoped(value, self.destination);
                    try self.values.ensureUnusedCapacity(self.destination, copies);
                    for (0..copies) |_| self.values.appendAssumeCapacity(owned);
                },
            }
        }
    };
    fn evalComprehension(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row, demand: ListDemand) anyerror!v.Value {
        const bounds = if (demand == .window) demand.window else reduction_plan.Bounds{};
        var writer: ListWriter = .{ .demand = demand, .destination = self.a, .caller_owner = self.scalarOwner(), .window = .{
            .function = .{ .name = "collect", .args = &.{}, .collect_limit = if (demand == .all) null else bounds.prefix, .collect_suffix = bounds.suffix },
            .arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
            .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
        } };
        defer writer.window.deinit();
        var receiver: ListReceiver = .{ .ptr = &writer, .emit = ListWriter.add, .destination = self.a, .observer = writer.window.function };
        if (!try self.produceList(expression, row, group, &receiver)) return .null;
        try receiver.check();
        return switch (demand) {
            .all => .{ .list = try writer.values.toOwnedSlice(self.a) },
            .count => .{ .integer = @intCast(writer.cardinality) },
            .window => try self.cloneScoped(try writer.window.finishWindow(self), self.a),
        };
    }
    const SummaryReceiver = struct {
        reducer: *StreamingAggregate,
        owner: ?*Invariants,
        locals: ?*const @import("expressions.zig").Local,
        fn add(ptr: *anyopaque, engine: *Engine, value: v.Value, weight: Count, _: ?*const value_work.Work) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Receiver state outlives the producer's element/constant arenas.
            // Never borrow or parent a fold cache to a shorter producer epoch.
            const previous_owner = engine.local_invariants;
            const previous_locals = engine.projection_locals;
            engine.local_invariants = self.owner;
            engine.projection_locals = self.locals;
            defer engine.local_invariants = previous_owner;
            defer engine.projection_locals = previous_locals;
            try self.reducer.addSummary(engine, value, weight);
            try self.reducer.retainCollection(engine, value, weight, null);
        }
    };
    const RangeExtrema = struct { minimum: i64, maximum: i64 };
    const ArithmeticDomain = struct {
        ranges: []const @import("range.zig").Range = &.{},
        length: Count = Count.init(0),
        null_source: bool = false,
        integer_sum: ?IntegerListSum = .{},
        fn extrema(self: @This()) ?RangeExtrema {
            var minimum: i64 = std.math.maxInt(i64);
            var maximum: i64 = std.math.minInt(i64);
            var present = false;
            for (self.ranges) |range| {
                if (range.length == 0) continue;
                const last = range.at(range.length - 1).integer;
                minimum = @min(minimum, @min(range.start, last));
                maximum = @max(maximum, @max(range.start, last));
                present = true;
            }
            return if (present) .{ .minimum = minimum, .maximum = maximum } else null;
        }
    };
    fn arithmeticDomainShape(self: *Engine, expression: *const ast.Expr, depth: usize) bool {
        if (depth > 64) return false;
        if (expression.execution_list_source) |id| {
            if (self.producer_sources.items[id].arithmetic_shape) |known| return known;
            const shape = self.arithmeticDomainShape(self.producer_sources.items[id].expression, depth + 1);
            self.producer_sources.items[id].arithmetic_shape = shape;
            return shape;
        }
        if (isRange(expression)) return true;
        if (reduction_plan.reversedList(expression)) |input| return self.arithmeticDomainShape(input, depth + 1);
        if (reduction_plan.concatenatedList(expression)) |b| return self.arithmeticDomainShape(b.left, depth + 1) and self.arithmeticDomainShape(b.right, depth + 1);
        return false;
    }
    fn arithmeticDomain(self: *Engine, expression: *const ast.Expr, row: Row) anyerror!ArithmeticDomain {
        try self.state();
        if (expression.execution_list_source) |id| {
            const source = self.producer_sources.items[id];
            if (source.arithmetic_domain) |domain| return domain;
            const owner = self.local_invariants;
            const locals = self.projection_locals;
            self.local_invariants = source.owner;
            self.projection_locals = source.locals;
            defer self.local_invariants = owner;
            defer self.projection_locals = locals;
            const domain = try self.arithmeticDomain(source.expression, source.row);
            self.producer_sources.items[id].arithmetic_domain = domain;
            return domain;
        }
        if (isRange(expression)) {
            const range = (try self.rangeInput(expression, row, null)) orelse return .{ .null_source = true };
            if (range.length == 0) return .{};
            const ranges = try self.planning_allocator.alloc(@import("range.zig").Range, 1);
            ranges[0] = range;
            return .{ .length = try self.domainCount(range.length), .ranges = ranges, .integer_sum = IntegerListSum.range(range) };
        }
        if (reduction_plan.reversedList(expression)) |input| {
            var domain = try self.arithmeticDomain(input, row);
            domain.integer_sum = if (domain.integer_sum) |summary| summary.reversed() else null;
            return domain;
        }
        const b = reduction_plan.concatenatedList(expression).?;
        // Both operands retain eager validation even when one is NULL.
        const left = try self.arithmeticDomain(b.left, row);
        const right = try self.arithmeticDomain(b.right, row);
        var domain: ArithmeticDomain = .{ .length = try left.length.add(self.planning_allocator, right.length), .null_source = left.null_source or right.null_source, .integer_sum = if (left.integer_sum != null and right.integer_sum != null) left.integer_sum.?.concat(right.integer_sum.?) else null };
        if (left.ranges.len == 0) {
            domain.ranges = right.ranges;
            return domain;
        }
        if (right.ranges.len == 0 or (left.ranges.ptr == right.ranges.ptr and left.ranges.len == right.ranges.len)) {
            domain.ranges = left.ranges;
            return domain;
        }
        if (left.ranges.len == 1 and right.ranges.len == 1) if (@import("range.zig").mergeCompatible(left.ranges[0], right.ranges[0])) |merged| {
            const ranges = try self.planning_allocator.alloc(@import("range.zig").Range, 1);
            ranges[0] = merged;
            domain.ranges = ranges;
            return domain;
        };
        // Bag multiplicity is separate from set support. Repeated source DAG
        // branches share their certificate instead of expanding it exponentially.
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var index: std.AutoHashMapUnmanaged(@import("range.zig").Range, void) = .empty;
        var ranges: std.ArrayList(@import("range.zig").Range) = .empty;
        for ([_][]const @import("range.zig").Range{ left.ranges, right.ranges }) |part| for (part) |range| {
            try self.state();
            const entry = try index.getOrPut(scratch.allocator(), range);
            if (!entry.found_existing) try ranges.append(scratch.allocator(), range);
        };
        domain.ranges = try self.planning_allocator.dupe(@import("range.zig").Range, ranges.items);
        return domain;
    }
    fn domainCount(self: *Engine, length: u128) !Count {
        const low = Count.init(@truncate(length));
        const high = Count.init(@intCast(length >> 64));
        return if (high.small == 0) low else (try (try high.multiply(self.planning_allocator, Count.init(std.math.maxInt(u64)))).add(self.planning_allocator, high)).add(self.planning_allocator, low);
    }
    fn arithmeticSource(expression: *const ast.Expr) bool {
        var input = expression;
        while (reduction_plan.reversedInput(input)) |child| input = child;
        return isRange(input);
    }
    fn projectedSource(expression: *const ast.Expr) bool {
        return expression.execution_list_source != null or expression.value == .comprehension or expression.value == .pattern_comprehension or reduction_plan.reversedList(expression) != null or (reduction_plan.concatenatedList(expression) != null and reduction_plan.repeatable(expression));
    }
    fn rangeSummary(self: *Engine, call_value: ast.Function, row: Row, group: ?[]const Row) !v.Value {
        const sequence = try self.rangeInput(call_value.args[0], row, group);
        var reducer: StreamingAggregate = .{ .function = call_value, .consumer_row = row, .source_null = sequence == null, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
        defer reducer.deinit();
        reducer.function.name = "collect";
        reducer.function.execution_list = false;
        // RANGE's source phase is total after checked argument evaluation.
        // Cardinality and root windows are arithmetic; visit only elements
        // still observed by a predicate, mapped consumer, or sequential fold.
        reducer.function.collect_limit = 0;
        reducer.function.collect_suffix = 0;
        try reducer.ensureFilters();
        if (sequence) |range| {
            var summary: SummaryReceiver = .{ .reducer = &reducer, .owner = self.local_invariants, .locals = self.projection_locals };
            var receiver: ListReceiver = .{ .ptr = &summary, .emit = SummaryReceiver.add, .destination = self.a, .observer = reducer.function };
            var source: ListSource = .{ .values = .{ .range = range } };
            while (true) {
                const pending = for (reducer.filter_states) |state_value| {
                    if (state_value.failure == null and state_value.decided == null) break true;
                } else false;
                if (!pending) break;
                const value = source.next() orelse break;
                try self.state();
                try receiver.accept(self, value, Count.one, null);
            }
            try receiver.check();
            reducer.collection_overflow = range.length > std.math.maxInt(i64);
            reducer.collection_count = std.math.cast(u64, range.length) orelse 0;
            const prefix = @min(range.length, call_value.collect_limit orelse 0);
            const suffix = @min(range.length - prefix, call_value.collect_suffix);
            const front = try self.rangeCells(range, 0, prefix);
            const back = try self.rangeCells(range, range.length - suffix, suffix);
            // These non-overlapping arithmetic windows already have their final
            // encounter order; no suffix ring or expanded skipped cells exist.
            try reducer.collected.appendSlice(reducer.arena.allocator(), front.list);
            try reducer.collected.appendSlice(reducer.arena.allocator(), back.list);
        }
        return self.cloneScoped(try reducer.finish(self), self.a);
    }
    fn listSummary(self: *Engine, call_value: ast.Function, row: Row, group: ?[]const Row) !v.Value {
        if (call_value.args[0].execution_list_source) |id| {
            const source = self.producer_sources.items[id];
            if (call_value.collect_filters.len == 0 and call_value.collect_limit == 0 and call_value.collect_suffix == 0) return source.summary;
            if (call_value.args[0].execution_range_binding and call_value.collect_filters.len == 0)
                return self.sourceSummary(if (source.arithmetic) |sequence| .{ .values = .{ .range = sequence } } else null, call_value);
        }

        if (isRange(call_value.args[0])) return self.rangeSummary(call_value, row, group);
        if (call_value.collect_filters.len == 0) if (reduction_plan.concatenatedList(call_value.args[0])) |b| return self.concatSummary(call_value, b.left, b.right, row, group);
        if (call_value.collect_filters.len == 0) if (reduction_plan.reversedList(call_value.args[0])) |input| {
            // Reversal preserves cardinality and exchanges front/back demand.
            // Only the compact owned window changes encounter order. Ordered
            // predicates/folds retain full reversal until independently proved.
            var inner = call_value;
            inner.args = &.{input};
            inner.collect_limit = if (call_value.collect_limit != null) call_value.collect_suffix else null;
            inner.collect_suffix = call_value.collect_limit orelse 0;
            const summary = try self.listSummary(inner, row, group);
            const window = summary.list[2..];
            const reversed = try self.a.dupe(v.Value, window);
            std.mem.reverse(v.Value, reversed);
            const output = try self.a.alloc(v.Value, 2 + reversed.len);
            @memcpy(output[0..2], summary.list[0..2]);
            @memcpy(output[2..], reversed);
            return .{ .list = output };
        };
        if (call_value.collect_filters.len == 0 and call_value.collect_limit != null and reduction_plan.reversedInput(call_value.args[0]) != null) {
            return self.sourceSummary(try self.listSource(call_value.args[0], row, group), call_value);
        }
        var reducer: StreamingAggregate = .{ .function = call_value, .consumer_row = row, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
        defer reducer.deinit();
        reducer.function.name = "collect";
        reducer.function.execution_list = false;
        var summary: SummaryReceiver = .{ .reducer = &reducer, .owner = self.local_invariants, .locals = self.projection_locals };
        var receiver: ListReceiver = .{ .ptr = &summary, .emit = SummaryReceiver.add, .destination = self.a, .observer = reducer.function };
        reducer.source_null = !try self.produceList(call_value.args[0], row, group, &receiver);
        try receiver.check();
        return self.cloneScoped(try reducer.finish(self), self.a);
    }
    const OperandSummary = struct {
        value: v.Value,
        embedding_failure: bool = false,
        fn length(self: @This()) u64 {
            const cardinality = self.value.list[0];
            return if (cardinality == .null) 0 else if (cardinality.integer < 0) std.math.maxInt(u64) else @intCast(cardinality.integer);
        }
        fn item(self: @This(), prefix: usize, index: u64) v.Value {
            const front = @min(prefix, self.length());
            const window = self.value.list[2..];
            if (index < front) return window[@intCast(index)];
            const tail = window.len - front;
            return window[@intCast(front + index - (self.length() - tail))];
        }
    };
    fn operandSummary(self: *Engine, expression: *const ast.Expr, call_value: ast.Function, row: Row, group: ?[]const Row) !OperandSummary {
        var operand = call_value;
        operand.name = "collect";
        operand.execution_list = false;
        operand.args = &.{expression};
        if (isRange(expression) or projectedSource(expression) or reduction_plan.concatenatedList(expression) != null or expression.value == .list) return .{ .value = try self.listSummary(operand, row, group) };
        // List addition also prepends/appends arbitrary scalar values. Unknown
        // operands retain normal evaluation and NULL absorption; embedding
        // depth is checked after both operands, just like ordinary addition.
        const source = try self.sequenceSource(expression, row, group);
        var embedding_failure = false;
        if (source != null and source.?.values == .scalar) _ = self.values.info(.{ .list = &.{source.?.values.scalar} }) catch |err| blk: {
            if (err != error.CypherValueDepthExceeded) return err;
            embedding_failure = true;
            break :blk null;
        };
        return .{ .value = try self.sourceSummary(source, call_value), .embedding_failure = embedding_failure };
    }
    /// Summarize an already evaluated sequence without visiting discarded cells.
    /// No projections or predicates remain here; their eager evaluation happened
    /// before the cursor was created. Direction changes only retained positions.
    fn sourceSummary(self: *Engine, source: ?ListSource, demand: ast.Function) !v.Value {
        const length = if (source) |input| input.length() else 0;
        const prefix = @min(demand.collect_limit orelse return error.CypherInvalidAggregate, length);
        const suffix = @min(demand.collect_suffix, length - prefix);
        const retained = std.math.cast(usize, prefix + suffix) orelse return error.CypherStateBudgetExceeded;
        try chargeWork(self, retained);
        const output = try self.a.alloc(v.Value, std.math.add(usize, retained, 2) catch return error.CypherStateBudgetExceeded);
        output[0] = if (source == null) .null else .{ .integer = std.math.cast(i64, length) orelse -1 };
        output[1] = .{ .integer = @intCast(retained) };
        for (0..@intCast(prefix)) |i| output[2 + i] = source.?.at(i);
        for (0..@intCast(suffix)) |i| output[2 + @as(usize, @intCast(prefix)) + i] = source.?.at(length - suffix + i);
        return .{ .list = output };
    }

    fn concatSummary(self: *Engine, call_value: ast.Function, left: *const ast.Expr, right: *const ast.Expr, row: Row, group: ?[]const Row) !v.Value {
        const lhs = try self.operandSummary(left, call_value, row, group);
        const rhs = try self.operandSummary(right, call_value, row, group);
        if (lhs.value.list[0] == .null or rhs.value.list[0] == .null) return .{ .list = try self.a.dupe(v.Value, &.{ .null, .{ .integer = 0 } }) };
        if (lhs.embedding_failure or rhs.embedding_failure) return error.CypherValueDepthExceeded;
        const length = lhs.length() +| rhs.length();
        const prefix = @min(call_value.collect_limit orelse return error.CypherInvalidAggregate, length);
        const suffix = @min(call_value.collect_suffix, length - prefix);
        const retained = std.math.add(usize, @intCast(prefix), @intCast(suffix)) catch return error.CypherStateBudgetExceeded;
        try chargeWork(self, retained);
        const output = try self.a.alloc(v.Value, std.math.add(usize, retained, 2) catch return error.CypherStateBudgetExceeded);
        output[0] = .{ .integer = if (length > std.math.maxInt(i64) or lhs.value.list[0].integer < 0 or rhs.value.list[0].integer < 0) -1 else @intCast(length) };
        output[1] = .{ .integer = @intCast(retained) };
        const front = call_value.collect_limit.?;
        for (0..@intCast(prefix)) |i| output[2 + i] = if (i < lhs.length()) lhs.item(front, i) else rhs.item(front, i - lhs.length());
        for (0..@intCast(suffix)) |i| {
            const distance = suffix - i;
            output[2 + @as(usize, @intCast(prefix)) + i] = if (distance <= rhs.length()) rhs.item(front, rhs.length() - distance) else lhs.item(front, lhs.length() - (distance - rhs.length()));
        }
        return .{ .list = output };
    }
    fn projectedConsumer(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?v.Value {
        const input = switch (expression.value) {
            .list_predicate => |c| c.input,
            .reduce => |c| c.input,
            else => return null,
        };
        if (!projectedSource(input)) return null;
        const filter: ast.CollectionFilter = switch (expression.value) {
            .list_predicate => |c| .{ .alias = c.alias, .predicate = c.predicate, .projection = null, .kind = switch (c.kind) {
                .all => .all,
                .any => .any,
                .none => .none,
                .single => .single,
            } },
            .reduce => |c| .{ .alias = c.alias, .predicate = null, .projection = c.projection, .kind = .fold, .accumulator = c.accumulator, .initial = c.initial },
            else => unreachable,
        };
        var reducer: StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{}, .collect_limit = 0, .collect_summary = true, .collect_filters = &.{filter} }, .consumer_row = row, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
        defer reducer.deinit();
        if (expression.value == .reduce) {
            // REDUCE evaluates its initial value before reaching its input.
            const initial = try self.eval(expression.value.reduce.initial, row, group);
            try reducer.ensureFilters();
            try reducer.filter_states[0].fold.own(self, initial);
        }
        var summary: SummaryReceiver = .{ .reducer = &reducer, .owner = self.local_invariants, .locals = self.projection_locals };
        var receiver: ListReceiver = .{ .ptr = &summary, .emit = SummaryReceiver.add, .destination = self.a, .observer = reducer.function };
        if (!try self.produceList(input, row, group, &receiver)) return v.Value.null;
        try receiver.check();
        const value = try self.collectionExtract(.{ .filtered = 0 }, try reducer.finish(self));
        return try self.cloneScoped(value, self.a);
    }
    fn rangeInput(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?@import("range.zig").Range {
        if (expression.execution_list_source) |id| return self.producer_sources.items[id].arithmetic;
        if (expression.execution_range_binding and expression.value == .variable) {
            const packet = try self.eval(expression, row, group);
            if (packet == .null) return null;
            if (packet != .list or packet.list.len != 4) return error.CypherInvalidAggregate;
            return .{ .start = packet.list[0].integer, .step = packet.list[1].integer, .length = @as(u128, @intCast(packet.list[3].integer)) << 64 | @as(u64, @bitCast(packet.list[2].integer)) };
        }
        const call_value = expression.value.call;
        const start = try self.eval(call_value.args[0], row, group);
        const end = try self.eval(call_value.args[1], row, group);
        const step = if (call_value.args.len == 3) try self.eval(call_value.args[2], row, group) else v.Value{ .integer = 1 };
        return @import("range.zig").Range.init(start, end, step);
    }
    fn isRange(expression: *const ast.Expr) bool {
        if (expression.execution_range_binding) return true;
        return expression.value == .call and std.ascii.eqlIgnoreCase(expression.value.call.name, "range") and !expression.value.call.star and !expression.value.call.distinct and (expression.value.call.args.len == 2 or expression.value.call.args.len == 3);
    }
    fn rangeCells(self: *Engine, sequence: @import("range.zig").Range, first: u128, length: u128) !v.Value {
        const count_value = std.math.cast(usize, length) orelse return error.CypherStateBudgetExceeded;
        try chargeWork(self, count_value);
        const items = try self.a.alloc(v.Value, count_value);
        for (items, 0..) |*item, i| {
            if (i % 4096 == 0) try self.checkpoint();
            item.* = sequence.at(first + i);
        }
        return .{ .list = items };
    }
    /// Runtime overload dispatch lets list consumers inspect a directional view
    /// while string REVERSE keeps its Unicode validation and allocation rules.
    fn directionalConsumer(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?v.Value {
        const base = switch (expression.value) {
            .call => |call_value| if (!call_value.star and !call_value.distinct and call_value.args.len == 1 and
                (std.ascii.eqlIgnoreCase(call_value.name, "size") or std.ascii.eqlIgnoreCase(call_value.name, "head") or std.ascii.eqlIgnoreCase(call_value.name, "last"))) call_value.args[0] else return null,
            .index => |index| index.base,
            .slice => |slice| slice.base,
            else => return null,
        };
        if (reduction_plan.reversedInput(base) == null or reduction_plan.reversedList(base) != null) return null;
        const source = try self.sequenceSource(base, row, group);
        switch (expression.value) {
            .call => |call_value| {
                const input = source orelse return v.Value.null;
                if (std.ascii.eqlIgnoreCase(call_value.name, "size")) {
                    const length = if (input.values == .scalar) try self.stringInputWork(input.values.scalar).size(input.values.scalar.string) else input.length();
                    return v.Value{ .integer = std.math.cast(i64, length) orelse return error.CypherNumericOverflow };
                }
                if (input.values == .scalar) return error.CypherTypeMismatch;
                return if (input.length() == 0) v.Value.null else input.at(if (std.ascii.eqlIgnoreCase(call_value.name, "head")) 0 else input.length() - 1);
            },
            .index => |index| {
                const position = try self.eval(index.index, row, group);
                if (source == null or position == .null) return v.Value.null;
                if (source.?.values == .scalar or position != .integer) return error.CypherTypeMismatch;
                const length: i128 = @intCast(source.?.length());
                const ordinal = if (position.integer < 0) length + position.integer else position.integer;
                return if (ordinal < 0 or ordinal >= length) v.Value.null else source.?.at(@intCast(ordinal));
            },
            .slice => |slice| {
                const first = if (slice.start) |bound| try self.eval(bound, row, group) else v.Value{ .integer = 0 };
                const last = if (slice.end) |bound| try self.eval(bound, row, group) else v.Value{ .integer = if (source != null and source.?.values != .scalar) std.math.cast(i64, source.?.length()) orelse return error.CypherNumericOverflow else 0 };
                if (source == null or first == .null or last == .null) return v.Value.null;
                if (source.?.values == .scalar or first != .integer or last != .integer) return error.CypherTypeMismatch;
                const length: i128 = @intCast(source.?.length());
                const begin: u128 = @intCast(std.math.clamp(if (first.integer < 0) length + first.integer else first.integer, 0, length));
                const end: u128 = @intCast(std.math.clamp(if (last.integer < 0) length + last.integer else last.integer, 0, length));
                const retained = std.math.cast(usize, @max(begin, end) - begin) orelse return error.CypherStateBudgetExceeded;
                try chargeWork(self, retained);
                const items = try self.a.alloc(v.Value, retained);
                for (items, 0..) |*item, i| item.* = source.?.at(begin + i);
                return v.Value{ .list = items };
            },
            else => unreachable,
        }
    }
    fn rangeConsumer(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) !?v.Value {
        const base = switch (expression.value) {
            .call => |call_value| if (!call_value.star and !call_value.distinct and call_value.args.len == 1 and
                (std.ascii.eqlIgnoreCase(call_value.name, "size") or std.ascii.eqlIgnoreCase(call_value.name, "head") or std.ascii.eqlIgnoreCase(call_value.name, "last"))) call_value.args[0] else return null,
            .index => |index| index.base,
            .slice => |slice| slice.base,
            else => return null,
        };
        if (!isRange(base)) return null;
        const sequence = try self.rangeInput(base, row, group);
        switch (expression.value) {
            .call => |call_value| {
                const range = sequence orelse return v.Value.null;
                if (std.ascii.eqlIgnoreCase(call_value.name, "size")) return v.Value{ .integer = std.math.cast(i64, range.length) orelse return error.CypherNumericOverflow };
                return if (range.length == 0) v.Value.null else range.at(if (std.ascii.eqlIgnoreCase(call_value.name, "head")) 0 else range.length - 1);
            },
            .index => |index| {
                const position = try self.eval(index.index, row, group);
                if (sequence == null or position == .null) return v.Value.null;
                if (position != .integer) return error.CypherTypeMismatch;
                return sequence.?.index(position.integer);
            },
            .slice => |slice| {
                const first = if (slice.start) |bound| try self.eval(bound, row, group) else v.Value{ .integer = 0 };
                const last = if (slice.end) |bound| try self.eval(bound, row, group) else null;
                if (sequence == null or first == .null or (last != null and last.? == .null)) return v.Value.null;
                if (first != .integer or (last != null and last.? != .integer)) return error.CypherTypeMismatch;
                const bounds = sequence.?.slice(first.integer, if (last) |end| end.integer else null);
                return try self.rangeCells(sequence.?, bounds.start, bounds.length);
            },
            else => unreachable,
        }
    }
    fn evalInner(self: *Engine, expression: *const ast.Expr, row: Row, group: ?[]const Row) anyerror!v.Value {
        if (expression.value != .variable) if (self.projection_overrides) |overrides| for (overrides.items, overrides.row) |item, entry| {
            if (@import("expressions.zig").same(item.expression, expression) and @import("expressions.zig").unshadowed(expression, self.projection_locals)) return entry.value;
        };
        if (self.aggregate_overrides) |overrides| if (overrides.index.get(expression)) |index| {
            if (overrides.stream) |stream| if (!stream.ready[index]) {
                if (stream.states[index].failure) |err| return err;
                overrides.values[index] = try stream.states[index].finish(self);
                stream.ready[index] = true;
            };
            if (overrides.lazy) |lazy| if (!lazy.ready[index]) {
                const call_value = expression.value.call;
                const leader = lazy.leaders[index];
                const shareable = call_value.distinct and call_value.args.len == 1 and reduction_plan.repeatable(call_value.args[0]) and
                    !std.ascii.eqlIgnoreCase(call_value.name, "min") and !std.ascii.eqlIgnoreCase(call_value.name, "max");
                var input: ?[]const v.Value = null;
                if (shareable) {
                    if (lazy.inputs[leader] == null) lazy.inputs[leader] = try self.distinctInput(call_value, lazy.rows);
                    input = lazy.inputs[leader];
                }
                overrides.values[index] = try self.aggregate(call_value, lazy.rows, input);
                lazy.ready[index] = true;
            };
            return overrides.values[index];
        };
        if (self.group_overrides) |overrides| if (overrides.index.get(expression)) |index| key: {
            if (!@import("expressions.zig").unshadowed(expression, self.projection_locals)) break :key;
            // ORDER BY aliases shadow input variables, including entity aliases.
            for (overrides.aliases) |alias| if (@import("expressions.zig").references(expression, alias.name)) break :key;
            return overrides.values[index];
        };
        try self.checkpoint();
        if (expression.value == .call and expression.value.call.execution_list) return self.listSummary(expression.value.call, row, group);
        if (try self.directionalConsumer(expression, row, group)) |result| return result;
        if (try self.rangeConsumer(expression, row, group)) |result| return result;
        if (try self.projectedConsumer(expression, row, group)) |result| return result;
        if (expression.value == .call) {
            const c = expression.value.call;
            if (!c.star and !c.distinct and c.args.len == 1 and std.ascii.eqlIgnoreCase(c.name, "size") and reduction_plan.composedList(c.args[0])) {
                const summary = try self.listSummary(.{ .name = "collect", .args = c.args, .collect_limit = 0, .collect_summary = true }, row, group);
                return self.collectionExtract(.cardinality, summary);
            }
        }
        if (reduction_plan.collectionConsumer(expression)) |demand| if (reduction_plan.composedList(demand.input)) {
            const summary = try self.listSummary(.{ .name = "collect", .args = &.{demand.input}, .collect_limit = demand.bounds.prefix, .collect_suffix = demand.bounds.suffix, .collect_summary = true }, row, group);
            const window = try self.collectionExtract(.{ .window = 1 }, summary);
            // Bounds and NULL are handled by the ordinary consumer, after the
            // source has finished evaluating and validating its projections.
            return self.listWindowConsumer(expression, window, row, group);
        };
        if (expression.value == .call) {
            const call_value = expression.value.call;
            if (!call_value.star and !call_value.distinct and call_value.args.len == 1 and std.ascii.eqlIgnoreCase(call_value.name, "size") and call_value.args[0].value == .comprehension)
                return self.evalComprehension(call_value.args[0], row, group, .count);
        }
        if (reduction_plan.collectionConsumer(expression)) |demand| if (demand.input.value == .comprehension) {
            const value = try self.evalComprehension(demand.input, row, group, .{ .window = demand.bounds });
            // Bounds are evaluated after producing the list, even for NULL.
            if (value != .null) return self.listWindowConsumer(expression, value, row, group);
            switch (expression.value) {
                .index => |index| _ = try self.eval(index.index, row, group),
                .slice => |slice| {
                    if (slice.start) |bound| _ = try self.eval(bound, row, group);
                    if (slice.end) |bound| _ = try self.eval(bound, row, group);
                },
                else => {},
            }
            return .null;
        };
        if (reduction_plan.collectionConsumer(expression)) |demand| if (demand.input.value == .pattern_comprehension) {
            if (try self.patternDemand(demand.input, row, demand.bounds)) |value| return self.listWindowConsumer(expression, value, row, group);
        };
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
            .binary => |b| if (b.op == .in) try self.evalMembership(try self.eval(b.left, row, group), b.right, row, group) else try self.binary(b.op, try self.eval(b.left, row, group), try self.eval(b.right, row, group)),
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
                var input = (try self.listSource(c.input, row, group)) orelse break :blk .null;
                var invariants: LoopScope = .{};
                defer invariants.deinit();
                const previous_invariants = try invariants.enter(self);
                defer self.local_invariants = previous_invariants;
                var true_count: usize = 0;
                var unknown = false;
                const previous_locals = self.projection_locals;
                defer self.projection_locals = previous_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = previous_locals };
                self.projection_locals = &local;
                var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
                defer scratch.deinit();
                while (input.next()) |item| {
                    try self.state();
                    _ = scratch.reset(.retain_capacity);
                    const scope = EvalScope.enter(self, scratch.allocator());
                    defer scope.leave(self);
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
                const accumulator = try self.eval(c.initial, row, group);
                var input = (try self.listSource(c.input, row, group)) orelse break :blk .null;
                const previous_locals = self.projection_locals;
                defer self.projection_locals = previous_locals;
                const local: @import("expressions.zig").Local = .{ .name = c.alias, .parent = previous_locals };
                self.projection_locals = &local;
                const accumulator_local: @import("expressions.zig").Local = .{ .name = c.accumulator, .parent = &local };
                self.projection_locals = &accumulator_local;
                var fold: FoldState = .{ .initialized = true, .accumulator = accumulator };
                defer fold.deinit();
                while (input.next()) |item| try fold.advance(self, c.projection, row, c.accumulator, c.alias, item, Count.one);
                break :blk if (input.position == 0) accumulator else try fold.escape(self);
            },
            .comprehension => try self.evalComprehension(expression, row, group, .all),
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
            .call => |c| if (c.execution_list) try self.listSummary(c, row, group) else if (c.collection_extract) |extract|
                try self.collectionExtract(extract, try self.eval(c.args[0], row, group))
            else if (std.ascii.eqlIgnoreCase(c.name, "size") and c.args.len == 1 and c.args[0].value == .pattern_comprehension and weightedExpression(c.args[0]))
                (try self.patternDemand(c.args[0], row, null)) orelse try self.comprehensionSize(c.args[0], row)
            else if (aggregateName(c.name)) try self.aggregate(c, group orelse return error.CypherAggregateScope, null) else try self.function(c.name, c.args, row, group),
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
    fn collectionExtract(self: *Engine, extract: ast.CollectionExtract, value: v.Value) !v.Value {
        return switch (extract) {
            .window => |header| if (value.list[0] == .null) .null else .{ .list = value.list[header + 1 ..][0..@intCast(value.list[header].integer)] },
            .cardinality => if (value.list[0] == .null) .null else if (value.list[0].integer < 0) error.CypherNumericOverflow else value.list[0],
            .filtered => |slot| try @import("collection_summary.zig").unpack(self.a, value.list, .{ .value = value.list[1 + slot * 2], .code = value.list[2 + slot * 2].integer }),
        };
    }
    const PatternConsumer = struct {
        projection: *const ast.Expr,
        cardinality: bool,
        receiver: ?*ListReceiver = null,
        reducer: StreamingAggregate,
        count: u64 = 0,
        failure: ?anyerror = null,
        embedding_failure: bool = false,
        fn add(self: *@This(), engine: *Engine, row: Row, weight: Count) !void {
            if (self.failure != null) return;
            self.addInner(engine, row, weight) catch |err| {
                if (!StreamingAggregate.consumerFailure(err)) return err;
                self.failure = err;
            };
        }
        fn addInner(self: *@This(), engine: *Engine, row: Row, weight: Count) !void {
            try engine.state();
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            const scope = EvalScope.enter(engine, scratch.allocator());
            defer scope.leave(engine);
            const value = try engine.eval(self.projection, row, null);
            if (self.cardinality) {
                // A comprehension counts NULL projections too.
                if (self.receiver) |receiver| {
                    _ = engine.values.info(.{ .list = &.{value} }) catch |err| blk: {
                        if (err != error.CypherValueDepthExceeded) return err;
                        self.embedding_failure = true;
                        break :blk null;
                    };
                    try receiver.accept(engine, value, weight, null);
                } else self.count = try addPathCount(self.count, weight.cap(std.math.maxInt(u64)));
            } else {
                _ = engine.values.info(.{ .list = &.{value} }) catch |err| blk: {
                    if (err != error.CypherValueDepthExceeded) return err;
                    self.embedding_failure = true;
                    break :blk null;
                };
                if (self.receiver) |receiver| try receiver.accept(engine, value, weight, null) else try self.reducer.retainCollection(engine, value, weight, null);
            }
        }
    };
    fn patternDemand(self: *Engine, expression: *const ast.Expr, row: Row, demand: ?reduction_plan.Bounds) !?v.Value {
        if (!self.read_only or !weightedExpression(expression)) return null;
        const scope = PatternScope.enter(self);
        defer scope.leave(self);
        const comprehension = expression.value.pattern_comprehension;
        var input = row;
        if (comprehension.path.alias) |alias| {
            var bindings: std.ArrayList(v.Entry) = .empty;
            for (row) |entry| if (!std.mem.eql(u8, entry.name, alias)) try bindings.append(self.a, entry);
            input = bindings.items;
        }
        const bounds = demand orelse reduction_plan.Bounds{};
        var consumer: PatternConsumer = .{ .projection = comprehension.projection, .cardinality = demand == null, .reducer = .{
            .function = .{ .name = "collect", .args = &.{comprehension.projection}, .collect_limit = bounds.prefix, .collect_suffix = bounds.suffix },
            .arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
            .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator),
        } };
        defer consumer.reducer.deinit();
        // Use the ordinary collection dependency/ordering proof. The sink owns
        // finalization, but compressed routes must retain exactly these values.
        const observer: ast.Expr = .{ .span = expression.span, .value = .{ .call = consumer.reducer.function } };
        // Cardinality is insensitive to encounter order. Keep exact weighted
        // endpoint bags, but decline symbolic/path-observing projections to the
        // existing weighted dependency planner rather than expanding their DAG.
        if (demand == null and !endpointExpression(comprehension.projection, comprehension.path, input)) return null;
        const projection: ast.Projection = .{ .items = &.{.{ .expression = if (demand == null) comprehension.projection else &observer, .alias = "_pattern_consumer" }} };
        const previous_demand = self.match_weight_demand;
        defer self.match_weight_demand = previous_demand;
        if (demand == null) self.match_weight_demand = .exact;
        const columns = self.columns;
        const stages = self.stages.items.len;
        defer self.columns = columns;
        defer self.stages.shrinkRetainingCapacity(stages);
        if ((try self.streamMatchProjectionInto(&.{input}, null, .{ .paths = &.{comprehension.path}, .predicate = comprehension.predicate }, projection, &.{}, .{}, .{}, null, &consumer)) == null) return null;
        // Matching/predicate failures always precede projection failures, as
        // they do when the entire MATCH bag is produced before projection.
        if (consumer.failure) |err| return err;
        if (consumer.embedding_failure) return error.CypherValueDepthExceeded;
        if (consumer.cardinality) return v.Value{ .integer = @intCast(consumer.count) };
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        var context: v.CloneContext = .{ .a = self.a, .scratch = scratch.allocator() };
        return try context.clone(try consumer.reducer.finishWindow(self));
    }
    fn listWindowConsumer(self: *Engine, expression: *const ast.Expr, value: v.Value, row: Row, group: ?[]const Row) !v.Value {
        return switch (expression.value) {
            .call => |call_value| if (value == .null or value.list.len == 0) .null else if (std.ascii.eqlIgnoreCase(call_value.name, "head")) value.list[0] else value.list[value.list.len - 1],
            .index => |index| blk: {
                const position = try self.eval(index.index, row, group);
                if (value == .null or position == .null) break :blk .null;
                if (position != .integer) return error.CypherTypeMismatch;
                const ordinal: i128 = if (position.integer < 0) @as(i128, value.list.len) + position.integer else position.integer;
                break :blk if (ordinal < 0 or ordinal >= value.list.len) .null else value.list[@intCast(ordinal)];
            },
            .slice => |slice| blk: {
                const first = if (slice.start) |bound| try self.eval(bound, row, group) else v.Value{ .integer = 0 };
                const last = if (slice.end) |bound| try self.eval(bound, row, group) else v.Value{ .integer = if (value == .null) 0 else @intCast(value.list.len) };
                if (value == .null or first == .null or last == .null) break :blk .null;
                if (first != .integer or last != .integer) return error.CypherTypeMismatch;
                const length: i128 = value.list.len;
                const begin: usize = @intCast(std.math.clamp(if (first.integer < 0) length + first.integer else first.integer, 0, length));
                const end: usize = @intCast(std.math.clamp(if (last.integer < 0) length + last.integer else last.integer, 0, length));
                break :blk .{ .list = value.list[begin..@max(begin, end)] };
            },
            else => unreachable,
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
            if (op == .contains) return .{ .boolean = try self.stringInputWork(right).contains(left.string, right.string) };
            if (right.string.len > left.string.len) return .{ .boolean = false };
            try self.stringWork().reserve(right.string.len, 1);
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
            try self.stringWork().reserve(lhs.len +| rhs.len, 1);
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
            const range_call: ast.Expr = .{ .span = .{}, .value = .{ .call = .{ .name = "range", .args = args } } };
            const sequence = (try self.rangeInput(&range_call, row, group)) orelse return .null;
            return self.rangeCells(sequence, 0, sequence.length);
        }
        if (std.ascii.eqlIgnoreCase(name, "substring")) {
            const text = try self.eval(args[0], row, group);
            const start = try self.eval(args[1], row, group);
            const substring_length = if (args.len == 3) try self.eval(args[2], row, group) else v.Value{ .integer = std.math.maxInt(i64) };
            if (text == .null or start == .null or substring_length == .null) return .null;
            if (text != .string or start != .integer or substring_length != .integer) return error.CypherTypeMismatch;
            if (start.integer < 0 or substring_length.integer < 0) return error.CypherInvalidSubstring;
            return .{ .string = try self.stringInputWork(text).substring(text.string, std.math.cast(usize, start.integer) orelse std.math.maxInt(usize), std.math.cast(usize, substring_length.integer) orelse std.math.maxInt(usize)) };
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
            return self.stringResult(value, .{ .string = try self.stringInputWork(value).reverse(value.string) });
        }
        if (std.ascii.eqlIgnoreCase(name, "trim") or std.ascii.eqlIgnoreCase(name, "ltrim") or std.ascii.eqlIgnoreCase(name, "rtrim")) {
            if (value != .string) return error.CypherTypeMismatch;
            return self.stringResult(value, .{ .string = try self.stringInputWork(value).trim(value.string, !std.ascii.eqlIgnoreCase(name, "rtrim"), !std.ascii.eqlIgnoreCase(name, "ltrim")) });
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
                const text = try self.stringInputWork(value).trim(value.string, true, true);
                break :blk if (std.ascii.eqlIgnoreCase(text, "true")) .{ .boolean = true } else if (std.ascii.eqlIgnoreCase(text, "false")) .{ .boolean = false } else .null;
            },
            else => error.CypherTypeMismatch,
        };
        if (std.ascii.eqlIgnoreCase(name, "toInteger") or std.ascii.eqlIgnoreCase(name, "toFloat")) {
            const integer = std.ascii.eqlIgnoreCase(name, "toInteger");
            if (integer and value == .integer) return value;
            if (integer and value == .boolean) return .{ .integer = @intFromBool(value.boolean) };
            if (value != .integer and value != .number and value != .string) return error.CypherTypeMismatch;
            if (value == .string) return if (integer) self.stringInputWork(value).integer(value.string) else self.stringInputWork(value).float(value.string);
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
            .string => |s| try self.stringInputWork(value).size(s),
            else => return error.CypherTypeMismatch,
        }) };
        if (std.ascii.eqlIgnoreCase(name, "head") or std.ascii.eqlIgnoreCase(name, "last")) {
            if (value != .list) return error.CypherTypeMismatch;
            return if (value.list.len == 0) .null else value.list[if (std.ascii.eqlIgnoreCase(name, "head")) 0 else value.list.len - 1];
        }
        if (std.ascii.eqlIgnoreCase(name, "toLower")) {
            if (value != .string) return error.CypherTypeMismatch;
            return self.stringResult(value, .{ .string = try self.stringInputWork(value).caseConvert(value.string, false) });
        }
        if (std.ascii.eqlIgnoreCase(name, "toUpper")) {
            if (value != .string) return error.CypherTypeMismatch;
            return self.stringResult(value, .{ .string = try self.stringInputWork(value).caseConvert(value.string, true) });
        }
        if (std.ascii.eqlIgnoreCase(name, "abs")) return if (value == .integer) .{ .integer = std.math.cast(i64, @abs(value.integer)) orelse return error.CypherNumericOverflow } else .{ .number = @abs(try value.numeric()) };
        return error.UnknownCypherFunction;
    }
    fn distinctInput(self: *Engine, call_value: ast.Function, group: []const Row) ![]const v.Value {
        var items: std.ArrayList(v.Value) = .empty;
        var index: value_work.Index = .{};
        for (group) |row| {
            const value = if (call_value.star) v.Value{ .integer = 1 } else try self.eval(call_value.args[0], row, null);
            if (value == .null) continue;
            const hash = (try self.values.info(value)).hash;
            var candidate = index.head(hash);
            var duplicate = false;
            while (candidate) |position| {
                const link = index.links.items[position];
                if (try self.values.eql(items.items[link.index], value)) {
                    duplicate = true;
                    break;
                }
                candidate = link.next;
            }
            if (duplicate) continue;
            try index.insert(self.a, hash, items.items.len);
            try items.append(self.a, value);
        }
        return items.items;
    }
    fn aggregate(self: *Engine, call_value: ast.Function, group: []const Row, prepared: ?[]const v.Value) !v.Value {
        if (call_value.args.len != (if (call_value.star) @as(usize, 0) else 1)) return error.CypherFunctionArity;
        if (call_value.collect_limit) |limit| {
            if (!call_value.collect_summary) if (prepared) |values| {
                const prefix = @min(limit, values.len);
                const tail_start = @max(prefix, values.len - @min(call_value.collect_suffix, values.len));
                const output = try self.a.alloc(v.Value, prefix + values.len - tail_start);
                @memcpy(output[0..prefix], values[0..prefix]);
                @memcpy(output[prefix..], values[tail_start..]);
                return .{ .list = output };
            };
            const retained = self.a;
            var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
            defer scratch.deinit();
            // Membership and rolling payload allocations can be recycled after
            // this lazy call. Their comparison metadata must have that lifetime.
            const scope = EvalScope.enter(self, scratch.allocator());
            defer scope.leave(self);
            var reducer: StreamingAggregate = .{ .function = call_value, .arena = std.heap.ArenaAllocator.init(self.scratch_allocator), .distinct_arena = std.heap.ArenaAllocator.init(self.scratch_allocator) };
            defer reducer.deinit();
            if (prepared) |values| {
                // DISTINCT membership has already selected first representations.
                // Feed literal values without evaluating the original argument twice.
                reducer.function.distinct = false;
                for (values) |value| {
                    const name = "_collection_input";
                    const operand: ast.Expr = .{ .span = .{}, .value = .{ .variable = name } };
                    reducer.function.args = &.{&operand};
                    try reducer.add(self, &.{.{ .name = name, .value = value }});
                }
            } else for (group) |row| try reducer.add(self, row);
            // The temporary collection is an execution container. Its cells
            // can be valid at depth 64 even when HEAD removes the outer list.
            return .{ .list = try cloneValues(retained, (try reducer.finish(self)).list) };
        }
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
        if (call_value.distinct and prepared == null) return self.aggregate(call_value, group, try self.distinctInput(call_value, group));
        var items: std.ArrayList(v.Value) = .empty;
        if (prepared) |values| {
            items.items = @constCast(values);
        } else {
            for (group) |row| {
                const value = if (call_value.star) v.Value{ .integer = 1 } else try self.eval(call_value.args[0], row, null);
                if (value != .null) try items.append(self.a, value);
            }
        }
        if (std.ascii.eqlIgnoreCase(call_value.name, "count")) return .{ .integer = @intCast(items.items.len) };
        if (std.ascii.eqlIgnoreCase(call_value.name, "collect")) return .{ .list = if (prepared) |values| values else try items.toOwnedSlice(self.a) };
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
        return @import("expressions.zig").outputName(self.a, self.source, expression);
    }
    const Projected = struct { row: Row, source: Row, group: ?[]const Row, keys: []const v.Value, sequence: usize = 0, weight: usize = 1, multiplicity: ?Count = null };
    const PatternScope = struct {
        columns: @FieldType(Engine, "columns"),
        stages: @FieldType(Engine, "stages"),
        retain_windows: bool,
        match_weight_demand: ?Multiplicity,
        terminal_weight_demand: ?Multiplicity,
        summary_tail: ?*const SummaryTail,
        pending_window: ?WeightedRows,
        expression_memo: ?CallMemo,
        expression_memo_scoped: bool,
        fn enter(engine: *Engine) PatternScope {
            const scope: PatternScope = .{ .columns = engine.columns, .stages = engine.stages, .retain_windows = engine.retain_windows, .match_weight_demand = engine.match_weight_demand, .terminal_weight_demand = engine.terminal_weight_demand, .summary_tail = engine.summary_tail, .pending_window = engine.pending_window, .expression_memo = engine.expression_memo, .expression_memo_scoped = engine.expression_memo_scoped };
            // A correlated query owns its result/profiling buffers and must
            // produce ordinary expression values. Its caller may itself be
            // retaining a weighted window in a temporary evaluation arena.
            engine.stages = .empty;
            engine.retain_windows = false;
            engine.match_weight_demand = null;
            engine.terminal_weight_demand = null;
            engine.summary_tail = null;
            engine.pending_window = null;
            // Nested rewrites may allocate AST nodes in a recyclable row arena.
            // Their pointer-based memo identities cannot escape this invocation.
            // The caller still memoizes the original expression's owned result.
            engine.expression_memo = null;
            engine.expression_memo_scoped = true;
            return scope;
        }
        fn leave(scope: PatternScope, engine: *Engine) void {
            engine.columns = scope.columns;
            engine.stages = scope.stages;
            engine.retain_windows = scope.retain_windows;
            engine.match_weight_demand = scope.match_weight_demand;
            engine.terminal_weight_demand = scope.terminal_weight_demand;
            engine.summary_tail = scope.summary_tail;
            engine.pending_window = scope.pending_window;
            if (engine.expression_memo) |*memo| memo.deinit();
            engine.expression_memo = scope.expression_memo;
            engine.expression_memo_scoped = scope.expression_memo_scoped;
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
            engine.values = .{ .a = a, .ptr = engine, .charge = chargeWork, .parent = if (engine.scalarOwner()) |state_value| &state_value.work else null };
            engine.strings = engine.newStringWork(a);
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
        const owned = try self.cloneProjected(candidate, arena.allocator());
        return .{ .arena = arena, .value = owned };
    }
    /// One stable ownership boundary for returned values. Metadata lives in
    /// the statement arena; large bytes use bounded slabs to avoid geometric
    /// arena slack. The result/cursor releases the slabs before the arena.
    fn statementOwnership(self: *Engine) !*v.CloneContext {
        if (self.statement_ownership == null) {
            const context = try self.planning_allocator.create(v.CloneContext);
            // Copying tiny scalars is cheaper than retaining a registry entry.
            // Bound metadata independently of total rows; admission exhaustion
            // falls back to an ordinary owned copy without changing results.
            context.* = .{ .a = self.planning_allocator, .scratch = self.planning_allocator, .min_shared_string_bytes = 256, .min_adopted_bytes = 256, .max_shared_allocations = @min(16384, self.limits.memory_bytes / 2048), .byte_pool = &self.statement_byte_pool, .byte_allocator = self.scratch_allocator };
            self.statement_ownership = context;
        }
        return self.statement_ownership.?;
    }
    fn cloneProjected(self: *Engine, candidate: Projected, a: A) !Projected {
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        const enclosing = if (self.read_only) try self.statementOwnership() else null;
        const parent = if (a.ptr == self.planning_allocator.ptr and a.vtable == self.planning_allocator.vtable) enclosing else null;
        // Picks may borrow proven statement-owned values. Their own new
        // allocations remain private and must be copied when the pick escapes.
        var context: v.CloneContext = .{ .a = a, .scratch = scratch.allocator(), .owned_parent = parent, .borrowed_parent = if (parent == null) enclosing else null, .byte_pool = if (parent) |proof| proof.byte_pool else null, .byte_allocator = self.scratch_allocator };
        const row = try a.alloc(v.Entry, candidate.row.len);
        for (candidate.row, row) |entry, *out| out.* = .{ .name = try a.dupe(u8, entry.name), .value = try context.clone(entry.value) };
        const keys = try a.alloc(v.Value, candidate.keys.len);
        for (candidate.keys, keys) |key, *out| out.* = try context.clone(key);
        // Only statement-owned copies can be adopted by the final result.
        // Top-k picks and group scratch arenas must still be copied on escape.
        if (parent) |proof| try proof.adoptClones(&context);
        return .{ .row = row, .source = &.{}, .group = null, .keys = keys, .sequence = candidate.sequence, .weight = candidate.weight, .multiplicity = if (candidate.multiplicity) |weight| try weight.clone(a) else null };
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
            const context = try engine.projectionScope(source, row.items);
            if (self.projection.predicate) |predicate| if ((try (try engine.eval(predicate, context, null)).truth()) != true) return;
            const keys = try engine.a.alloc(v.Value, self.projection.order.len);
            for (self.projection.order, keys) |order, *key| key.* = try engine.eval(order.expression, context, null);
            const candidate: Projected = .{ .row = row.items, .source = &.{}, .group = null, .keys = keys, .sequence = self.sequence, .weight = @intCast(weight.cap(self.capacity)), .multiplicity = if (self.projection.limit == null) weight else null };
            return self.acceptProjected(candidate);
        }
        fn acceptProjected(self: *TopK, candidate: Projected) !void {
            const engine = self.engine;
            if (self.capacity == 0 or candidate.weight == 0) return;
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
            const retained = engine.a;
            var scratch = std.heap.ArenaAllocator.init(engine.scratch_allocator);
            defer scratch.deinit();
            // Picks own their values through this call. Sort borrowed descriptors
            // and apply SKIP before owning the surviving rows. Sort metadata must
            // also die before the pick arenas can be recycled.
            const scope = EvalScope.enter(engine, scratch.allocator());
            defer scope.leave(engine);
            const projected = try engine.a.alloc(Projected, self.picks.items.len);
            for (self.picks.items, projected) |pick, *item| item.* = pick.value;
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
                    try rows.append(retained, try engine.cloneRowScoped(item.row, retained));
                    try weights.append(retained, try (try (item.multiplicity orelse Count.init(item.weight)).clone(retained)).subtract(retained, discarded));
                }
                engine.pending_window = .{ .rows = rows.items, .weights = weights.items };
                if (final) engine.columns = try self.columns.toOwnedSlice(retained);
                return rows.items;
            }
            const output = try retained.alloc(Row, self.total - skip);
            var position: usize = 0;
            for (projected) |item| {
                const discarded = @min(skip, item.weight);
                skip -= discarded;
                if (item.weight == discarded) continue;
                const owned = try engine.cloneRowScoped(item.row, retained);
                for (0..item.weight - discarded) |_| {
                    try engine.state();
                    output[position] = owned;
                    position += 1;
                }
            }
            if (final) engine.columns = try self.columns.toOwnedSlice(retained);
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
    fn projectBoundedCountGroups(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool, plan: GroupWindow) ![]const Row {
        var window = plan;
        defer window.heap.deinit(self.scratch_allocator);
        var calls: reduction_plan.Calls = .{};
        for (projection.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return error.CypherInvalidAggregate;
        var groups: std.ArrayList(StreamGroup) = .empty;
        defer for (groups.items) |*group| group.deinit();
        var index: value_work.Index = .{};
        defer index.deinit(self.scratch_allocator);
        const retention = try self.groupRetention(projection);
        var top: ?TopK = null;
        var matched: usize = 0;
        var counts = [_]usize{0};
        var fold: StreamFold = .{ .retention = &retention, .retained = self.a, .middle = &.{}, .projection = projection, .calls = calls.items, .membership_leaders = calls.distinct_leaders.items, .call_aliases = &calls.index.pointers, .group_window = &window, .top = &top, .groups = &groups, .group_index = &index, .has_keys = true, .stage_counts = &counts, .matched = &matched, .distinct = &.{} };
        var scratch = std.heap.ArenaAllocator.init(self.scratch_allocator);
        defer scratch.deinit();
        for (rows) |row| {
            _ = scratch.reset(.free_all);
            const scope = EvalScope.enter(self, scratch.allocator());
            defer scope.leave(self);
            try fold.add(self, row);
        }
        const columns = self.columns;
        defer if (!final) {
            self.columns = columns;
        };
        return self.finishStreamGroups(groups.items, &calls, &retention, projection);
    }
    fn project(self: *Engine, rows: []const Row, projection: ast.Projection, final: bool) anyerror![]const Row {
        var grouped = false;
        for (projection.items) |item| grouped = grouped or aggregates(item.expression);
        if (grouped and projection.wildcard) return error.CypherAggregateWildcard;
        if (!grouped and projection.distinct and !projection.wildcard) {
            if (try GroupWindow.init(self, projection, true)) |window| return self.projectBoundedCountGroups(rows, projection, final, window);
        }
        if (grouped) bounded: {
            // Materialized reducers normally run group by group. COUNT(*) is
            // total, so switching it to row order cannot reorder argument errors.
            for (projection.items) |item| if (aggregates(item.expression) and (item.expression.value != .call or !item.expression.value.call.star)) break :bounded;
            if (try GroupWindow.init(self, projection, true)) |window| return self.projectBoundedCountGroups(rows, projection, final, window);
        }
        if (!grouped and !projection.distinct and projection.order.len == 0) return self.projectUnordered(rows, projection, final);
        if (!grouped and !projection.distinct and projection.limit != null and projection.order.len != 0) {
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
        // Materialized/write-barrier scopes share pure reducers too. Compute
        // lazily, preserving CASE/coalesce errors and nonrepeatable arguments.
        var calls: reduction_plan.Calls = .{};
        if (grouped) {
            for (projection.items) |item| if (!try self.collectStreamingAggregates(item.expression, &calls)) return error.CypherInvalidAggregate;
            for (projection.order) |order| if (!try self.collectStreamingAggregates(order.expression, &calls)) return error.CypherInvalidAggregate;
        }
        const values = try self.a.alloc(v.Value, calls.items.len);
        const ready = try self.a.alloc(bool, calls.items.len);
        const distinct_inputs = try self.a.alloc(?[]const v.Value, calls.items.len);
        const previous_aggregates = self.aggregate_overrides;
        defer self.aggregate_overrides = previous_aggregates;
        var projected: std.ArrayList(Projected) = .empty;
        var projected_index: value_work.Index = .{};
        var columns: std.ArrayList([]const u8) = .empty;
        if (projection.wildcard and rows.len > 0) for (rows[0]) |entry| try columns.append(self.a, entry.name);
        for (projection.items) |item| try columns.append(self.a, item.alias orelse try self.expressionName(item.expression));
        for (groups.items) |group| {
            @memset(ready, false);
            @memset(distinct_inputs, null);
            if (grouped) self.aggregate_overrides = .{ .index = &calls.index.pointers, .values = values, .lazy = .{ .ready = ready, .rows = group.items, .leaders = calls.distinct_leaders.items, .inputs = distinct_inputs } };
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

test "suffix depth canonicalization preserves unknown horizons and failed allocations" {
    const Campaign = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const retained = arena.allocator();
            var state: u8 = 0;
            var engine: Engine = .{ .a = retained, .scratch_allocator = a, .regex = undefined, .backend = .{ .ptr = &state, .vtable = &.{ .bind = undefined, .checkpoint = checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
            engine.values = .{ .a = retained, .ptr = &engine, .charge = Engine.chargeWork };
            const nodes = [_]v.Node{
                .{ .identity = .{ .table = "docs", .key = "a" } },
                .{ .identity = .{ .table = "docs", .key = "b" } },
                .{ .identity = .{ .table = "docs", .key = "c" } },
                .{ .identity = .{ .table = "docs", .key = "d" } },
            };
            const anonymous: ast.Node = .{ .span = .{}, .alias = null, .labels = &.{}, .properties = &.{} };
            var edges = [_]ast.Edge{.{ .span = .{}, .direction = .out, .min_hops = 1, .max_hops = 8, .variable_length = true }};
            const path: ast.Path = .{ .span = .{}, .alias = null, .nodes = &.{ anonymous, anonymous }, .edges = &edges };
            // Geometry-only proof: no admission evaluator is invoked here.
            var pruner: Engine.RoutePruner = .{ .a = retained, .path = path, .row = &.{}, .predicate = null, .fold = undefined, .graph = .{ .input = .{ .row = &.{} }, .path = path, .domains = &edges } };
            const targets = [_][]const usize{ &.{ 1, 2 }, &.{2}, &.{3}, &.{} };
            for (&nodes, targets, 0..) |*node, outgoing, index| {
                try pruner.graph.nodes.append(retained, .{ .node = node, .depth = index });
                try pruner.graph.positions.put(retained, node.identity, index);
                for (outgoing) |target| {
                    const relationship = try retained.create(v.Relationship);
                    relationship.* = .{ .identity = .{ .table = "docs", .key = "edge" }, .index = "links", .type = "R", .source = node.identity, .target = nodes[target].identity };
                    try pruner.graph.nodes.items[index].arcs.append(retained, .{ .target = target, .relationship = relationship, .mapping = 0 });
                }
                try pruner.graph.nodes.items[index].domains.put(retained, 0, .{ .first = 0, .last = outgoing.len });
            }
            try pruner.prepareDepth(&engine);
            try std.testing.expectEqual(@as(?usize, 2), pruner.suffix_depth.?[1]);
            try std.testing.expectEqual(@as(u32, 1), pruner.memoKey(.{ .node = 1, .edge = 0, .hop = 2 }).hop);
            try std.testing.expectEqual(@as(u32, 7), pruner.memoKey(.{ .node = 1, .edge = 0, .hop = 7 }).hop);
            // A node at the read horizon may have unknown outgoing edges.
            // Missing adjacency never becomes a fabricated zero suffix depth.
            edges[0].max_hops = 2;
            _ = pruner.graph.nodes.items[3].domains.remove(0);
            try pruner.prepareDepth(&engine);
            try std.testing.expectEqual(@as(?usize, null), pruner.suffix_depth.?[1]);
            // Multi-segment geometry is proved only from complete captured
            // domains. It never opens an unknown boundary or merges domains.
            edges[0].max_hops = 8;
            pruner.path.edges = &.{ edges[0], edges[0] };
            try pruner.prepareDepth(&engine);
            try pruner.recordDepth(&engine, .{ .node = 2, .edge = 0, .hop = 2 });
            try std.testing.expectEqual(@as(u32, 2), pruner.memoKey(.{ .node = 2, .edge = 0, .hop = 2 }).hop);
            try pruner.graph.nodes.items[3].domains.put(retained, 0, .{ .first = 0, .last = 0 });
            for ([_]usize{ 3, 2, 1 }) |node| try pruner.recordDepth(&engine, .{ .node = node, .edge = 0, .hop = 2 });
            try std.testing.expectEqual(@as(u32, 1), pruner.memoKey(.{ .node = 1, .edge = 0, .hop = 2 }).hop);
            try std.testing.expectEqual(@as(u32, 2), pruner.memoKey(.{ .node = 1, .edge = 1, .hop = 2 }).hop);
            pruner.graph.mode = .node_simple;
            try std.testing.expectEqual(@as(usize, 3), try pruner.simpleBound(&engine, null));
            pruner.graph.mode = .relationship_simple;
            // Repeated adjacency occurrences of the same physical index/key
            // contribute one relationship to the finite bound.
            try std.testing.expectEqual(@as(usize, 1), try pruner.simpleBound(&engine, 0));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "child barrier admission requires available bound pattern identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var engine: Engine = .{ .a = a, .scratch_allocator = std.testing.allocator, .regex = undefined, .backend = undefined, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    const shape: Row = &.{ .{ .name = "b", .value = .{ .boolean = true } }, .{ .name = "i", .value = .{ .boolean = false } } };
    for ([_][]const u8{
        "WITH * MATCH (i) RETURN count(*) AS n",
        "WITH * MATCH (b)-[i:R]->(c) RETURN count(*) AS n",
        "WITH * MATCH i=(b)-[:R]->(c) RETURN count(*) AS n",
        "WITH * MATCH (b) RETURN count(*) AS n ORDER BY i",
        "WITH * MATCH (b) RETURN count(*) AS n LIMIT i",
    }) |source| {
        var diagnostic: compiler.Diagnostic = .{};
        var compiled = try compiler.compile(std.testing.allocator, source, .{}, &diagnostic);
        defer compiled.deinit();
        engine.source = source;
        try std.testing.expect(try engine.admissionCallShape(shape, compiled.query.clauses) == null);
    }
    const source = "WITH * MATCH (b) RETURN count(*) AS n ORDER BY n LIMIT 1";
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(std.testing.allocator, source, .{}, &diagnostic);
    defer compiled.deinit();
    engine.source = source;
    try std.testing.expect(try engine.admissionCallShape(shape, compiled.query.clauses) != null);
}

test "adjacency page index growth reclaims obsolete hash tables" {
    var cache: Engine.AdjacencyCache = .{ .budget = .{ .backing = std.testing.allocator, .limit = 512 << 10 } };
    defer cache.deinit();
    // Empty pages matter on multi-anchor sparse graphs too. Their identities
    // fit alongside the final index; obsolete rehash tables must not consume
    // the payload arena and disable subsequent capture admission.
    for (0..1000) |i| {
        var key: [40]u8 = undefined;
        const identity: v.Identity = .{ .table = "docs", .key = try std.fmt.bufPrint(&key, "node-{d:0>32}", .{i}) };
        try cache.put(.{ .mapping = 0, .node = identity, .direction = .out, .cursor = null }, .{ .items = &.{} });
    }
    for (0..1000) |i| {
        var key: [40]u8 = undefined;
        const identity: v.Identity = .{ .table = "docs", .key = try std.fmt.bufPrint(&key, "node-{d:0>32}", .{i}) };
        try std.testing.expect(cache.pages.contains(.{ .mapping = 0, .node = identity, .direction = .out, .cursor = null }));
    }
}

test "materialized reductions share pure calls without evaluating unused aggregate branches" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = a, .scratch_allocator = a, .regex = undefined, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    engine.values = .{ .a = a, .ptr = &engine, .charge = Engine.chargeWork };
    var diagnostic: compiler.Diagnostic = .{};
    var compiled = try compiler.compile(a, "RETURN CASE WHEN false THEN sum(1/0) ELSE size(collect(x)) END AS a,size(COLLECT(x)) AS b", .{}, &diagnostic);
    defer compiled.deinit();
    const output = try engine.project(&.{ &.{.{ .name = "x", .value = .{ .integer = 1 } }}, &.{.{ .name = "x", .value = .{ .integer = 2 } }} }, compiled.query.clauses[0].operation.project.value, true);
    try std.testing.expectEqual(@as(usize, 1), output.len);
    try std.testing.expectEqual(@as(i64, 2), output[0][0].value.integer);
    try std.testing.expectEqual(@as(i64, 2), output[0][1].value.integer);
    try std.testing.expect(engine.aggregate_overrides == null);
}

test "shared DISTINCT membership keeps independent consumer ordinals and numeric representations" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = arena.allocator(), .scratch_allocator = std.testing.allocator, .regex = undefined, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    var operand: ast.Expr = .{ .span = .{}, .value = .{ .variable = "x" } };
    var membership: Engine.DistinctMembership = .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer membership.deinit();
    var counter_consumer: Engine.DistinctConsumer = .{ .membership = &membership };
    var collector_consumer: Engine.DistinctConsumer = .{ .membership = &membership };
    var counter: Engine.StreamingAggregate = .{ .function = .{ .name = "count", .args = &.{&operand}, .distinct = true }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator), .cohort = &counter_consumer };
    defer counter.deinit();
    var collector: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&operand}, .distinct = true }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator), .cohort = &collector_consumer };
    defer collector.deinit();
    try counter.add(&engine, &.{.{ .name = "x", .value = .{ .integer = 1 } }});
    try collector.add(&engine, &.{.{ .name = "x", .value = .{ .number = 1.0 } }});
    try collector.add(&engine, &.{.{ .name = "x", .value = .{ .integer = 2 } }});
    try counter.add(&engine, &.{.{ .name = "x", .value = .{ .integer = 2 } }});
    try collector.add(&engine, &.{.{ .name = "x", .value = .{ .integer = 1 } }});
    try std.testing.expectEqual(@as(i64, 2), counter.result().integer);
    try std.testing.expectEqual(@as(usize, 2), membership.values.items.len);
    try std.testing.expectEqual(@as(usize, 2), collector.result().list.len);
    try std.testing.expect(collector.result().list[0] == .number);
    try std.testing.expectEqual(@as(i64, 2), collector.result().list[1].integer);
}

test "bounded collection retention caps promoted bag weights without losing evaluation" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = arena.allocator(), .scratch_allocator = std.testing.allocator, .regex = undefined, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    const operand: ast.Expr = .{ .span = .{}, .value = .{ .variable = "x" } };
    var collector: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&operand}, .collect_limit = 3 }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer collector.deinit();
    const weight = try Count.parse(engine.a, "1180591620717411303424");
    try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, weight);
    try std.testing.expectEqual(@as(usize, 3), collector.result().list.len);
    for (collector.result().list) |item| try std.testing.expectEqual(@as(i64, 7), item.integer);
    // Reaching the retention limit cannot suppress a subsequent binding error.
    try std.testing.expectError(error.UnboundCypherVariable, collector.addWeighted(&engine, &.{}, weight));
}

test "collection suffix runs cap promoted weights and trim overlapping windows" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = arena.allocator(), .scratch_allocator = std.testing.allocator, .regex = undefined, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    const operand: ast.Expr = .{ .span = .{}, .value = .{ .variable = "x" } };
    var collector: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&operand}, .collect_limit = 2, .collect_suffix = 3 }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer collector.deinit();
    try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, Count.init(2));
    try std.testing.expectEqual(@as(usize, 2), (try collector.finish(&engine)).list.len);
    const huge = try Count.parse(engine.a, "1180591620717411303424");
    try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, huge);
    try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 8 } }}, Count.init(2));
    const result = try collector.finish(&engine);
    try std.testing.expectEqual(@as(usize, 5), result.list.len);
    for (result.list, [_]i64{ 7, 7, 7, 8, 8 }) |value, expected| try std.testing.expectEqual(expected, value.integer);
    try std.testing.expectError(error.UnboundCypherVariable, collector.addWeighted(&engine, &.{}, huge));
}

test "boolean collection summaries cap promoted multiplicities and ignore empty runs" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = arena.allocator(), .scratch_allocator = std.testing.allocator, .regex = .{ .a = arena.allocator(), .scratch = std.heap.ArenaAllocator.init(std.testing.allocator) }, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    defer engine.regex.deinit();
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    engine.strings = engine.newStringWork(engine.a);
    const input: ast.Expr = .{ .span = .{}, .value = .{ .variable = "x" } };
    const predicate: ast.Expr = .{ .span = .{}, .value = .{ .variable = "y" } };
    const huge = try Count.parse(engine.a, "1180591620717411303424");
    for ([_]ast.CollectionFilter.Kind{ .all, .any, .none, .single }) |kind| {
        var collector: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&input}, .collect_limit = 0, .collect_summary = true, .collect_filters = &.{.{ .alias = "y", .predicate = &predicate, .projection = null, .kind = kind }} }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
        defer collector.deinit();
        try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .boolean = true } }}, Count.init(0));
        try std.testing.expectEqual(kind == .all or kind == .none, (try collector.finish(&engine)).list[1].boolean);
        try collector.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .boolean = true } }}, huge);
        try std.testing.expectEqual(kind == .all or kind == .any, (try collector.finish(&engine)).list[1].boolean);
    }
}

test "streaming windows cap promoted weights while sequential folds enforce transition budgets" {
    const Noop = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var engine: Engine = .{ .a = arena.allocator(), .scratch_allocator = std.testing.allocator, .regex = .{ .a = arena.allocator(), .scratch = std.heap.ArenaAllocator.init(std.testing.allocator) }, .backend = .{ .ptr = &dummy, .vtable = &.{ .bind = undefined, .checkpoint = Noop.checkpoint, .scan = undefined, .get = undefined, .expand = undefined } }, .graph = undefined, .source = "", .parameters = &.{}, .limits = .{} };
    defer engine.regex.deinit();
    engine.values = .{ .a = engine.a, .ptr = &engine, .charge = Engine.chargeWork };
    engine.strings = engine.newStringWork(engine.a);
    const input: ast.Expr = .{ .span = .{}, .value = .{ .variable = "x" } };
    const item: ast.Expr = .{ .span = .{}, .value = .{ .variable = "y" } };
    const zero: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .integer = 0 } } };
    const accumulator: ast.Expr = .{ .span = .{}, .value = .{ .variable = "s" } };
    const transition: ast.Expr = .{ .span = .{}, .value = .{ .binary = .{ .op = .add, .left = &accumulator, .right = &item } } };
    const huge = try Count.parse(engine.a, "1180591620717411303424");
    var window: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&input}, .collect_limit = 0, .collect_summary = true, .collect_filters = &.{.{ .alias = "y", .predicate = null, .projection = &item, .kind = .window, .prefix = 2, .suffix = 2 }} }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer window.deinit();
    try window.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, huge);
    const cells = (try engine.collectionExtract(.{ .filtered = 0 }, try window.finish(&engine))).list;
    try std.testing.expectEqual(@as(usize, 4), cells.len);
    for (cells) |cell| try std.testing.expectEqual(@as(i64, 7), cell.integer);
    var fold: Engine.StreamingAggregate = .{ .function = .{ .name = "collect", .args = &.{&input}, .collect_limit = 0, .collect_summary = true, .collect_filters = &.{.{ .alias = "y", .predicate = null, .projection = &transition, .kind = .fold, .accumulator = "s", .initial = &zero }} }, .arena = std.heap.ArenaAllocator.init(std.testing.allocator), .distinct_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer fold.deinit();
    try fold.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, Count.init(3));
    try std.testing.expectEqual(@as(i64, 21), (try fold.finish(&engine)).list[1].integer);
    try std.testing.expectError(error.CypherStateBudgetExceeded, fold.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, huge));
    engine.limits.intermediate_states = engine.stats.intermediate_states + 2;
    try std.testing.expectError(error.CypherStateBudgetExceeded, fold.addWeighted(&engine, &.{.{ .name = "x", .value = .{ .integer = 7 } }}, Count.init(100)));
}
