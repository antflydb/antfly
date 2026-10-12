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

//! Graph Query API — structured DSL matching Go antfly's graph_queries.
//!
//! Provides a unified query interface over the graph module's traversal and
//! path-finding primitives:
//!   - traverse / neighbors: BFS via traversal.traverse()
//!   - shortest_path: via paths.findShortestPath()
//!   - k_shortest_paths: via paths.findKShortestPaths()

const std = @import("std");
pub const relationship_filter = @import("relationship_filter.zig");
const Allocator = std.mem.Allocator;
const platform_time = @import("antfly_platform").time;
const graph_mod = @import("graph.zig");
const edge_type_mod = @import("edge_type.zig");
const node_identity = @import("node_identity.zig");
const NodeAdmission = @import("node_admission.zig").NodeAdmission;
const NodeRef = @import("node_admission.zig").NodeRef;
const pattern_mod = @import("pattern.zig");
const traversal_mod = @import("traversal.zig");
const paths_mod = @import("paths.zig");
const work_budget_mod = @import("work_budget.zig");
const algebraic_ir = @import("../storage/db/algebraic/ir.zig");
const algebraic_planner = @import("../storage/db/algebraic/planner.zig");
const algebraic_path_mod = @import("../storage/db/algebraic/path.zig");

pub const max_named_queries: usize = 64;
/// Exact MATCH operations enumerate an independently filtered anchor relation.
/// Keep that scan multiplier bounded while allowing one operation to stream an
/// arbitrarily large relation. Multiple counts over the same pattern belong in
/// one MATCH return object and share its scan.
pub const max_match_queries_per_request: usize = 8;
pub const max_query_name_codepoints: usize = 128;
pub const max_identifier_codepoints: usize = pattern_mod.max_identifier_codepoints;
pub const max_identifier_bytes: usize = pattern_mod.max_identifier_bytes;
pub const max_edge_types: usize = 64;
pub const max_edge_type_bytes: usize = edge_type_mod.max_bytes;

const TraverseResultAdmissionContext = struct {
    alloc: Allocator,
    target_keys: *const std.StringHashMapUnmanaged(void),
    seen: ?*node_identity.Map(void),

    fn admit(raw_ctx: ?*anyopaque, node: NodeRef) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(raw_ctx orelse return error.InvalidArgument));
        if (self.target_keys.count() > 0 and !self.target_keys.contains(node.key)) return false;
        if (self.seen) |seen| {
            return try seen.putIfAbsent(
                self.alloc,
                .{ .table = node.table, .key = node.key },
                {},
            );
        }
        return true;
    }
};

/// Validate execution-cost limits shared by canonical and compatibility graph
/// requests. Public identifier syntax belongs to the dialect-specific API
/// parser: legacy operation names are opaque wire keys and must survive the
/// transition adapter unchanged.
pub fn validateExecutionOperationBudget(queries: anytype) !void {
    if (queries.len > max_named_queries) return error.InvalidQueryRequest;
    var complete_matches: usize = 0;
    for (queries) |named_query| {
        if (named_query.query.match_pattern == null) continue;
        complete_matches += 1;
        if (complete_matches > max_match_queries_per_request)
            return error.GraphMatchOperationLimitExceeded;
    }
}

const ExecutionDependencies = struct {
    start: ?usize,
    target: ?usize,

    fn ready(self: ExecutionDependencies, emitted: []const bool) bool {
        if (self.start) |index| if (!emitted[index]) return false;
        if (self.target) |index| if (!emitted[index]) return false;
        return true;
    }
};

/// Return a stable topological order for named graph operations. JSON object
/// member order is not semantic, so ready operations are selected by their
/// UTF-8 name bytes. That keeps shared request budgets and their diagnostics
/// identical across SDK map implementations and wire encodings.
///
/// Requests contain at most `max_named_queries` operations and each operation
/// has at most two dependencies. A bounded scan therefore avoids another heap
/// and adjacency structure while doing at most 4,096 readiness checks.
pub fn executionOrderAlloc(alloc: Allocator, queries: anytype) ![]usize {
    try validateExecutionOperationBudget(queries);

    var by_name = std.StringHashMapUnmanaged(usize).empty;
    defer by_name.deinit(alloc);
    for (queries, 0..) |query, index| {
        const result = try by_name.getOrPut(alloc, query.name);
        if (result.found_existing) return error.InvalidQueryRequest;
        result.value_ptr.* = index;
    }

    const dependencies = try alloc.alloc(ExecutionDependencies, queries.len);
    defer alloc.free(dependencies);
    for (queries, 0..) |query, index| {
        dependencies[index] = .{
            .start = try dependencyIndex(queries, &by_name, query.query.start_nodes),
            .target = if (query.query.target_nodes) |target|
                try dependencyIndex(queries, &by_name, target)
            else
                null,
        };
    }

    const emitted = try alloc.alloc(bool, queries.len);
    defer alloc.free(emitted);
    @memset(emitted, false);

    const order = try alloc.alloc(usize, queries.len);
    errdefer alloc.free(order);
    for (order) |*slot| {
        var next: ?usize = null;
        for (queries, 0..) |query, index| {
            if (emitted[index] or !dependencies[index].ready(emitted)) continue;
            if (next == null or std.mem.order(u8, query.name, queries[next.?].name) == .lt)
                next = index;
        }
        const index = next orelse return error.InvalidQueryRequest;
        emitted[index] = true;
        slot.* = index;
    }
    return order;
}

fn dependencyIndex(
    queries: anytype,
    by_name: *const std.StringHashMapUnmanaged(usize),
    selector: NodeSelector,
) !?usize {
    const result_ref = switch (selector) {
        .keys, .identities => return null,
        .result_ref => |value| value,
    };
    if (!std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) return null;

    const dep_name = result_ref.ref["$graph_results.".len..];
    const dep_index = by_name.get(dep_name) orelse return error.InvalidQueryRequest;
    const dependency = queries[dep_index].query;
    switch (dependency.query_type) {
        .neighbors, .traverse, .shortest_path, .k_shortest_paths => if (result_ref.binding != null) return error.InvalidQueryRequest,
        .pattern => {
            const binding = result_ref.binding orelse return error.InvalidQueryRequest;
            if (dependency.match_pattern == null or dependency.aggregates.len > 0)
                return error.InvalidQueryRequest;
            for (dependency.return_aliases) |alias| {
                if (std.mem.eql(u8, alias, binding)) return dep_index;
            }
            return error.InvalidQueryRequest;
        },
    }
    return dep_index;
}

test "graph operation execution order is independent of declaration order" {
    const NamedQuery = struct {
        name: []const u8,
        query: GraphQuery,
    };
    const alpha = NamedQuery{ .name = "alpha", .query = .{
        .query_type = .traverse,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{"a"} },
    } };
    const after_alpha = NamedQuery{ .name = "after_alpha", .query = .{
        .query_type = .traverse,
        .index_name = "graph",
        .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.alpha" } },
    } };
    const zulu = NamedQuery{ .name = "zulu", .query = .{
        .query_type = .traverse,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{"z"} },
    } };

    const first = [_]NamedQuery{ zulu, after_alpha, alpha };
    const second = [_]NamedQuery{ alpha, zulu, after_alpha };
    inline for (.{ first, second }) |queries| {
        const order = try executionOrderAlloc(std.testing.allocator, &queries);
        defer std.testing.allocator.free(order);
        try std.testing.expectEqualStrings("alpha", queries[order[0]].name);
        try std.testing.expectEqualStrings("after_alpha", queries[order[1]].name);
        try std.testing.expectEqualStrings("zulu", queries[order[2]].name);
    }
}

test "graph operation execution order rejects cycles" {
    const NamedQuery = struct {
        name: []const u8,
        query: GraphQuery,
    };
    const queries = [_]NamedQuery{
        .{ .name = "alpha", .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.beta" } },
        } },
        .{ .name = "beta", .query = .{
            .query_type = .traverse,
            .index_name = "graph",
            .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.alpha" } },
        } },
    };
    try std.testing.expectError(
        error.InvalidQueryRequest,
        executionOrderAlloc(std.testing.allocator, &queries),
    );
}

pub fn isValidQueryName(name: []const u8) bool {
    if (!isValidIdentifier(name)) return false;
    // `$...` is reserved for typed result namespaces such as
    // `$query_results` and `$graph_results.<name>`. Keeping operation names
    // out of that namespace makes resolution identical in every executor.
    if (name[0] == '$') return false;
    return true;
}

/// Public graph identifiers are copied into execution plans and result rows.
/// Bound both their wire size and Unicode length before any fan-out so a small
/// pattern cannot amplify an oversized alias across intermediate bindings.
pub fn isValidIdentifier(value: []const u8) bool {
    return pattern_mod.isValidIdentifier(value);
}

pub fn validateEdgeTypes(edge_types: []const []const u8) !void {
    if (edge_types.len > max_edge_types) return error.InvalidArgument;
    var total_bytes: usize = 0;
    for (edge_types, 0..) |edge_type, i| {
        if (edge_type.len == 0) return error.InvalidArgument;
        total_bytes = std.math.add(usize, total_bytes, edge_type.len) catch
            return error.InvalidArgument;
        if (total_bytes > max_edge_type_bytes) return error.InvalidArgument;
        for (edge_types[0..i]) |prior| {
            if (std.mem.eql(u8, prior, edge_type)) return error.InvalidArgument;
        }
    }
}

/// Executors collect one extra item to distinguish an exact result whose size
/// equals the public limit from a result that was actually truncated.
pub fn resultCollectionLimit(limit: u32) usize {
    if (limit == 0) return std.math.maxInt(usize);
    return @as(usize, limit) + 1;
}

pub fn resultCountIsTruncated(count: usize, limit: u32) bool {
    return limit > 0 and count > limit;
}

test "graph public operation names and edge filters stay unambiguous and bounded" {
    try std.testing.expect(isValidQueryName("walk"));
    try std.testing.expect(!isValidQueryName("$query_results"));
    try std.testing.expect(!isValidQueryName("$graph_results.walk"));
    try std.testing.expect(isValidIdentifier("author"));
    try std.testing.expect(isValidIdentifier("author name"));
    try std.testing.expect(isValidIdentifier("作者"));
    try std.testing.expect(!isValidIdentifier(""));
    try std.testing.expect(!isValidIdentifier("   \t"));
    try std.testing.expect(!isValidIdentifier(" author"));
    try std.testing.expect(!isValidIdentifier("author "));
    try std.testing.expect(!isValidIdentifier("\u{00a0}author"));
    try std.testing.expect(!isValidIdentifier("author\u{3000}"));
    try std.testing.expect(!isValidIdentifier("author\nname"));
    try std.testing.expect(!isValidIdentifier("author\x00name"));
    try std.testing.expect(!isValidIdentifier("author\u{00a0}name"));
    try std.testing.expect(!isValidIdentifier("author\u{200b}name"));
    try std.testing.expect(!isValidIdentifier("author\u{2028}name"));
    try std.testing.expect(!isValidIdentifier("author\u{202e}name"));
    try std.testing.expect(!isValidIdentifier("*"));
    try std.testing.expect(!isValidIdentifier("$query_results"));
    try std.testing.expect(!isValidIdentifier(&@as([max_identifier_bytes + 1]u8, @splat('a'))));

    try validateEdgeTypes(&.{ "cites", "related" });
    try std.testing.expectError(error.InvalidArgument, validateEdgeTypes(&.{""}));
    try std.testing.expectError(error.InvalidArgument, validateEdgeTypes(&.{ "cites", "cites" }));
    const too_many = @as([max_edge_types + 1][]const u8, @splat("edge"));
    try std.testing.expectError(error.InvalidArgument, validateEdgeTypes(&too_many));
    const too_large = [_][]const u8{&@as([max_edge_type_bytes + 1]u8, @splat('x'))};
    try std.testing.expectError(error.InvalidArgument, validateEdgeTypes(&too_large));

    try std.testing.expectEqual(@as(usize, 101), resultCollectionLimit(100));
    try std.testing.expect(!resultCountIsTruncated(100, 100));
    try std.testing.expect(resultCountIsTruncated(101, 100));
}

// ============================================================================
// Query types
// ============================================================================

pub const QueryType = enum {
    traverse,
    neighbors,
    shortest_path,
    k_shortest_paths,
    pattern,
};

pub const NodeSelector = union(enum) {
    keys: []const []const u8,
    identities: []const NodeIdentity,
    result_ref: ResultRef,
};

pub const NodeIdentity = struct {
    key: []const u8,
    table: ?[]const u8 = null,
};

pub const ResultRef = struct {
    ref: []const u8, // "$query_results" or "$graph_results.<query-name>"
    /// Required when the referenced graph query returns MATCH rows. Selecting
    /// one alias avoids ambiguous flattening of branched bindings.
    binding: ?[]const u8 = null,
    limit: u32 = 0, // 0 = use all results
};

pub const QueryParams = struct {
    edge_filter: relationship_filter.Filter = .{},
    edge_types: []const []const u8 = &.{},
    direction: graph_mod.EdgeDirection = .out,
    max_depth: u32 = 1,
    max_results: u32 = 100,
    min_weight: ?f64 = null,
    max_weight: ?f64 = null,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: paths_mod.PathWeightMode = .min_hops,
    algebraic_semiring: bool = false,
    node_filter: pattern_mod.NodeFilter = .{},
};

/// Exact graph-metric filtering and ordering must observe the full candidate
/// set. Bound that set explicitly so a broad traversal cannot turn a small
/// requested page into unbounded memory and sort work.
pub const graph_metric_candidate_limit: u32 = 100_000;
/// Public and internal graph metric query bounds. These limits keep request
/// parsing, metric materialization, and comparison work predictable even when
/// an internal caller bypasses the OpenAPI layer.
pub const graph_metric_projection_limit: usize = 16;
pub const graph_metric_order_limit: usize = 8;
pub const graph_metric_filter_limit: usize = 32;
pub const graph_metric_dependency_limit: usize = 16;
/// Query-seeded personalization resolves each seed key against the metric
/// topology; bound the per-read seed set so request parsing and seed
/// resolution stay proportional to the query, not the graph.
pub const graph_metric_seed_limit: usize = 128;

/// Backend-independent late-materialization plan. Names borrow the validated
/// query; each stage is deduplicated and ordering remains user-defined.
pub const MetricReadPlan = struct {
    const Names = struct {
        buffer: [graph_metric_dependency_limit][]const u8 = undefined,
        len: usize = 0,

        pub fn slice(self: *const @This()) []const []const u8 {
            return self.buffer[0..self.len];
        }
        fn append(self: *@This(), name: []const u8) void {
            for (self.slice()) |prior| if (std.mem.eql(u8, name, prior)) return;
            std.debug.assert(self.len < self.buffer.len);
            self.buffer[self.len] = name;
            self.len += 1;
        }
    };
    dependencies: Names = .{},
    filters: Names = .{},
    orders: Names = .{},
    projections: Names = .{},
    policies: [graph_metric_dependency_limit]graph_mod.GraphIndex.GraphMetricColumnReadPolicy = @splat(.{}),
    /// Dependency-aligned personalization. A personalized dependency never
    /// reads a published column, so it carries no publication policy; its
    /// column is computed fresh from the current edge snapshot instead.
    personalizations: [graph_metric_dependency_limit]?GraphMetricPersonalization = @splat(null),

    fn require(self: *@This(), name: []const u8, freshness: GraphMetricFreshness, published: bool) void {
        const i = self.dependencyIndex(name);
        self.policies[i].require_published = self.policies[i].require_published or published;
        self.policies[i].require_fresh = self.policies[i].require_fresh or freshness == .fresh;
    }

    fn dependencyIndex(self: *const @This(), name: []const u8) usize {
        for (self.dependencies.slice(), 0..) |dependency, i| {
            if (std.mem.eql(u8, dependency, name)) return i;
        }
        unreachable;
    }

    pub fn personalizationFor(self: *const @This(), name: []const u8) ?GraphMetricPersonalization {
        for (self.dependencies.slice(), 0..) |dependency, i| {
            if (std.mem.eql(u8, dependency, name)) return self.personalizations[i];
        }
        return null;
    }

    pub fn init(query: GraphQuery) !MetricReadPlan {
        try validateGraphMetricQueryShape(query);
        var plan = MetricReadPlan{};
        for (query.metrics) |metric| {
            plan.dependencies.append(metric.name);
            plan.projections.append(metric.name);
            if (metric.seed_nodes.len != 0) {
                plan.personalizations[plan.dependencyIndex(metric.name)] = .{
                    .seed_nodes = metric.seed_nodes,
                    .damping = metric.damping,
                };
                continue;
            }
            plan.require(metric.name, metric.freshness, false);
        }
        for (query.order_by) |order| {
            plan.dependencies.append(order.name);
            plan.orders.append(order.name);
            if (plan.personalizationFor(order.name) != null) {
                // Ordering consumes the personalized column; a published
                // policy here would silently mix global and seeded scores.
                if (order.freshness != .fresh) return error.GraphMetricPersonalizationRequiresFresh;
                continue;
            }
            plan.require(order.name, order.freshness, true);
        }
        for (query.where_metric) |filter| {
            plan.dependencies.append(filter.name);
            plan.filters.append(filter.name);
            if (plan.personalizationFor(filter.name) != null) {
                if (filter.freshness != .fresh) return error.GraphMetricPersonalizationRequiresFresh;
                continue;
            }
            plan.require(filter.name, filter.freshness, true);
        }
        return plan;
    }
};

pub fn nodeFilterActive(filter: pattern_mod.NodeFilter) bool {
    return filter.filter_prefix.len > 0 or filter.filter_query_json != null;
}

pub const AlgebraicTraversalRejectReason = algebraic_path_mod.ExecutionRejectReason;
pub const AlgebraicTraversalProof = algebraic_path_mod.ExecutionProof;

pub fn algebraicTraversalProof(graph_index: *const graph_mod.GraphIndex, params: QueryParams) AlgebraicTraversalProof {
    return algebraic_path_mod.executionProof(.{
        .semiring_enabled = params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal(),
        .deduplicate = params.deduplicate,
        .max_depth = params.max_depth,
        .max_results = params.max_results,
        .min_weight = params.min_weight,
        .max_weight = params.max_weight,
        .min_hops = params.weight_mode == .min_hops,
    });
}

fn algebraicTraversalConsidered(graph_index: *const graph_mod.GraphIndex, params: QueryParams) bool {
    return params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal();
}

pub const GraphQuery = struct {
    query_type: QueryType,
    index_name: []const u8,
    start_nodes: NodeSelector,
    params: QueryParams = .{},
    target_nodes: ?NodeSelector = null,
    k: u32 = 1,
    pattern: []const pattern_mod.PatternStep = &.{},
    match_pattern: ?pattern_mod.ConjunctivePattern = null,
    return_aliases: []const []const u8 = &.{},
    /// Query-wide row limit for canonical MATCH results. Shards may over-fetch
    /// to let the coordinator determine truncation accurately.
    return_limit: u32 = 0,
    evaluation_output_limit: ?u32 = null,
    aggregates: []const NamedCountAggregate = &.{},
    include_documents: bool = false,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
    /// Fetch expression inputs independently of the final document projection.
    defer_document_projection: bool = false,
    metrics: []const GraphMetricRead = &.{},
    order_by: []const GraphMetricOrder = &.{},
    where_metric: []const GraphMetricFilter = &.{},
    include_metric_status: bool = false,

    pub fn documentRetrievalQuery(self: @This()) @This() {
        var retrieval = self;
        if (self.defer_document_projection) {
            retrieval.fields = &.{};
            retrieval.include_all_fields = true;
        }
        return retrieval;
    }
};

pub const NamedCountAggregate = struct {
    name: []const u8,
    of: []const u8,
    distinct: bool = false,
};

pub const ExpandStrategy = enum { @"union", intersection };

pub const GraphMetricFreshness = enum { published, fresh };

pub const GraphMetricRead = struct {
    name: []const u8,
    freshness: GraphMetricFreshness = .published,
    /// Query-seeded personalized PageRank: teleport mass restricted to these
    /// node keys (uniform per seed). The column is computed at query time from
    /// the current edge snapshot, so personalization requires freshness=fresh;
    /// published generations are global-only and reads against them fail
    /// closed. Seed keys absent from the graph are skipped, not errors.
    seed_nodes: []const []const u8 = &.{},
    /// Damping override for personalized reads. Null keeps the metric's
    /// configured damping. Only valid together with seed_nodes.
    damping: ?f64 = null,
};

/// Personalization parameters carried per read-plan dependency. Borrowed from
/// the validated query like every other plan name.
pub const GraphMetricPersonalization = struct {
    seed_nodes: []const []const u8,
    damping: ?f64 = null,
};

pub const GraphMetricOrderDirection = enum { asc, desc };
pub const GraphMetricNullOrder = enum { first, last };

pub const GraphMetricOrder = struct {
    name: []const u8,
    direction: GraphMetricOrderDirection = .desc,
    nulls: GraphMetricNullOrder = .last,
    freshness: GraphMetricFreshness = .published,
};

pub const GraphMetricFilterOp = enum { gt, gte, lt, lte, eq, neq };

pub const GraphMetricFilter = struct {
    name: []const u8,
    op: GraphMetricFilterOp,
    value: f64,
    freshness: GraphMetricFreshness = .published,
};

/// Validate metric cardinality and comparison semantics before graph fan-out.
pub fn validateGraphMetricQueryShape(query: GraphQuery) !void {
    if (query.metrics.len > graph_metric_projection_limit or
        query.order_by.len > graph_metric_order_limit or
        query.where_metric.len > graph_metric_filter_limit)
        return error.InvalidQueryRequest;

    var dependency_names: [graph_metric_dependency_limit][]const u8 = undefined;
    var dependency_count: usize = 0;
    for (query.metrics, 0..) |metric, i| {
        if (metric.name.len == 0) return error.InvalidQueryRequest;
        for (query.metrics[0..i]) |previous|
            if (std.mem.eql(u8, previous.name, metric.name)) return error.InvalidQueryRequest;
        if (metric.seed_nodes.len > graph_metric_seed_limit) return error.InvalidQueryRequest;
        for (metric.seed_nodes) |seed| if (seed.len == 0) return error.InvalidQueryRequest;
        if (metric.damping) |damping| {
            if (metric.seed_nodes.len == 0 or !std.math.isFinite(damping) or damping <= 0 or damping >= 1)
                return error.InvalidQueryRequest;
        }
        // Published generations are global-only; a seeded read against one
        // would silently return unpersonalized scores. Fail closed instead.
        if (metric.seed_nodes.len != 0 and metric.freshness != .fresh)
            return error.GraphMetricPersonalizationRequiresFresh;
        try appendGraphMetricDependencyName(&dependency_names, &dependency_count, metric.name);
    }
    for (query.order_by, 0..) |order, i| {
        if (order.name.len == 0) return error.InvalidQueryRequest;
        for (query.order_by[0..i]) |previous|
            if (std.mem.eql(u8, previous.name, order.name)) return error.InvalidQueryRequest;
        try appendGraphMetricDependencyName(&dependency_names, &dependency_count, order.name);
    }
    for (query.where_metric) |filter| {
        if (filter.name.len == 0 or !std.math.isFinite(filter.value)) return error.InvalidQueryRequest;
        try appendGraphMetricDependencyName(&dependency_names, &dependency_count, filter.name);
    }
}

fn appendGraphMetricDependencyName(
    names: *[graph_metric_dependency_limit][]const u8,
    count: *usize,
    name: []const u8,
) !void {
    for (names[0..count.*]) |existing|
        if (std.mem.eql(u8, existing, name)) return;
    if (count.* == names.len) return error.InvalidQueryRequest;
    names[count.*] = name;
    count.* += 1;
}

// ============================================================================
// Result types
// ============================================================================

pub const PathEdgeInfo = paths_mod.PathEdge;

/// Response-owned column storage. NaN represents a missing score, just as in
/// the native decoder; neither scratch allocators nor transaction leases escape.
pub const GraphMetricColumns = struct {
    names: [][]u8,
    scores: []f64 = &.{},
    rows: usize,
    // Adopted backing remains in its native layout. The bounded directory
    // and optional mapping replace an N*C promotion copy.
    decoded: [graph_metric_projection_limit]?[]f64 = @splat(null),
    pages: ?*@import("metric_pages.zig").ScorePages = null,
    mapping: ?[]usize = null,
    page_columns: [graph_metric_projection_limit]usize = @splat(0),

    // Multiple staged reads may have distinct row mappings. Keep each owner
    // once and route columns through this bounded directory.
    groups: [graph_metric_projection_limit]BackingGroup = @splat(.{}),
    column_groups: [graph_metric_projection_limit]u8 = @splat(0),
    group_count: u8 = 0,

    pub const PageStorage = @import("metric_pages.zig").ScorePages;

    const BackingGroup = struct {
        mapping: ?[]usize = null,
        pages: ?*@import("metric_pages.zig").ScorePages = null,
    };
    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.groups[0..self.group_count]) |group| {
            if (group.mapping) |mapping| alloc.free(mapping);
            if (group.pages) |pages| pages.destroy();
        }
        for (self.names) |name| alloc.free(name);
        alloc.free(self.names);
        alloc.free(self.scores);
        for (self.decoded) |column| if (column) |values| alloc.free(values);
        if (self.pages) |pages| pages.destroy();
        if (self.mapping) |mapping| alloc.free(mapping);
        alloc.destroy(self);
    }
    pub fn backingBytes(self: *const @This()) usize {
        var bytes = self.scores.len * @sizeOf(f64);
        for (self.groups[0..self.group_count]) |group| {
            if (group.mapping) |mapping| bytes += mapping.len * @sizeOf(usize);
            if (group.pages) |pages| bytes += pages.storageBytes();
        }
        for (self.decoded) |column| if (column) |values| {
            bytes += values.len * @sizeOf(f64);
        };
        if (self.pages) |pages| bytes += pages.storageBytes();
        if (self.mapping) |mapping| bytes += mapping.len * @sizeOf(usize);
        return bytes;
    }
    pub fn viewValid(self: *const @This(), row: usize) bool {
        if (row >= self.rows or self.names.len > graph_metric_projection_limit or self.group_count > graph_metric_projection_limit) return false;
        if (self.mapping) |mapping| if (mapping.len < self.rows) return false;
        const count = std.math.mul(usize, self.rows, self.names.len) catch return false;
        for (0..self.names.len) |column| {
            if (self.group_count != 0 and self.column_groups[column] >= self.group_count) return false;
            const group = if (self.group_count != 0) self.groups[self.column_groups[column]] else BackingGroup{ .mapping = self.mapping, .pages = self.pages };
            if (group.mapping) |mapping| if (mapping.len < self.rows) return false;
            const physical = if (group.mapping) |mapping| mapping[row] else row;
            if (group.pages) |pages| {
                if (self.page_columns[column] >= pages.columns) return false;
                if (physical >= pages.count) return false;
            } else if (self.decoded[column]) |values| {
                if (physical >= values.len) return false;
            } else if (self.scores.len != count) return false;
        }
        return true;
    }
    pub fn at(self: *const @This(), row: usize, column: usize) GraphMetricValue {
        const group = if (self.group_count != 0) self.groups[self.column_groups[column]] else BackingGroup{ .mapping = self.mapping, .pages = self.pages };
        const physical = if (group.mapping) |mapping| mapping[row] else row;
        const score = if (group.pages) |pages| pages.at(self.page_columns[column], physical) else if (self.decoded[column]) |values| values[physical] else self.scores[column * self.rows + row];
        return .{ .name = self.names[column], .name_owned = false, .score = if (std.math.isNan(score)) null else score };
    }
};

pub const GraphResultNode = struct {
    key: []const u8,
    depth: u32,
    distance: f64,
    path: ?[]const []const u8 = null,
    /// Table provenance parallel to `path`. Null means every path node belongs
    /// to the query table. When present, null entries still mean query-table
    /// nodes and non-null entries are owned table qualifiers.
    path_tables: ?[]const ?[]const u8 = null,
    path_edges: ?[]const PathEdgeInfo = null,
    provenance: ?[]const []const u8 = null,
    /// Table the node's document lives in, when an edge reaching it declared a
    /// cross-table endpoint (`target_table` in its metadata). Null means the
    /// node is same-table (hydrated locally). Lets the api hydrate a cross-table
    /// entity node from its own table instead of failing closed.
    table: ?[]const u8 = null,
    /// Legacy materialized payload. Read query results through metricCount
    /// and metricAt so compact response ownership stays intact.
    metrics: []GraphMetricValue = &.{},
    /// False when metrics is a row view into GraphQueryResult's contiguous
    /// metric_values_slab. Values still own any promoted names individually.
    metrics_owned: bool = true,

    /// Query results retain compact columns through transport. Use metricCount
    /// and metricAt for both compact and legacy materialized payloads.
    metric_columns: ?*const GraphMetricColumns = null,
    metric_row: u32 = 0,

    pub fn metricViewValid(self: @This()) bool {
        if (self.metric_columns) |columns| {
            return self.metrics.len == 0 and self.metric_row < columns.rows and columns.viewValid(self.metric_row);
        }
        return true;
    }
    pub fn metricCount(self: @This()) usize {
        return if (self.metric_columns) |columns| columns.names.len else self.metrics.len;
    }
    pub fn metricAt(self: @This(), column: usize) GraphMetricValue {
        return if (self.metric_columns) |columns| columns.at(self.metric_row, column) else self.metrics[column];
    }
    pub fn cloneMetrics(self: @This(), alloc: Allocator) ![]GraphMetricValue {
        const values = try alloc.alloc(GraphMetricValue, self.metricCount());
        var initialized: usize = 0;
        errdefer {
            for (values[0..initialized]) |*value| value.deinit(alloc);
            alloc.free(values);
        }
        for (values, 0..) |*value, i| {
            const source = self.metricAt(i);
            value.* = .{ .name = try alloc.dupe(u8, source.name), .score = source.score };
            initialized += 1;
        }
        return values;
    }
    const Wire = struct {
        key: []const u8,
        depth: u32,
        distance: f64,
        path: ?[]const []const u8 = null,
        path_tables: ?[]const ?[]const u8 = null,
        path_edges: ?[]const PathEdgeInfo = null,
        provenance: ?[]const []const u8 = null,
        table: ?[]const u8 = null,
        metrics: []GraphMetricValue = &.{},
        metrics_owned: bool = true,
    };
    fn fromWire(wire: Wire) GraphResultNode {
        return .{ .key = wire.key, .depth = wire.depth, .distance = wire.distance, .path = wire.path, .path_tables = wire.path_tables, .path_edges = wire.path_edges, .provenance = wire.provenance, .table = wire.table, .metrics = wire.metrics };
    }
    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        return fromWire(try std.json.innerParse(Wire, alloc, source, options));
    }
    pub fn jsonParseFromValue(alloc: Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {
        return fromWire(try std.json.innerParseFromValue(Wire, alloc, source, options));
    }
    /// Keep the existing internal wire shape, excluding ownership metadata
    /// added for columnar transport. Scores are emitted directly from columns.
    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        const Metrics = struct {
            node: GraphResultNode,
            pub fn jsonStringify(view: @This(), writer: anytype) !void {
                try writer.beginArray();
                for (0..view.node.metricCount()) |i| try writer.write(view.node.metricAt(i));
                try writer.endArray();
            }
        };
        try jw.write(.{ .key = self.key, .depth = self.depth, .distance = self.distance, .path = self.path, .path_tables = self.path_tables, .path_edges = self.path_edges, .provenance = self.provenance, .table = self.table, .metrics = Metrics{ .node = self }, .metrics_owned = self.metrics_owned });
    }

    pub fn deinit(self: *GraphResultNode, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.table) |t| alloc.free(t);
        for (self.metrics) |*metric| metric.deinit(alloc);
        if (self.metrics_owned and self.metrics.len > 0) alloc.free(self.metrics);
        if (self.path) |p| {
            for (p) |s| alloc.free(s);
            alloc.free(p);
        }
        if (self.path_tables) |tables| {
            for (tables) |table| if (table) |value| alloc.free(value);
            alloc.free(tables);
        }
        if (self.path_edges) |pe| {
            for (pe) |e| {
                alloc.free(e.source);
                alloc.free(e.target);
                alloc.free(e.edge_type);
                if (e.edge_id.len > 0) alloc.free(e.edge_id);
                if (e.owner_document.len > 0) alloc.free(e.owner_document);
                if (e.metadata.len > 0) alloc.free(e.metadata);
            }
            alloc.free(pe);
        }
        if (self.provenance) |items| {
            for (items) |item| alloc.free(item);
            alloc.free(items);
        }
        self.* = undefined;
    }
};

/// Validate the allocation-free, cross-field invariants of a canonical result
/// node. Callers at API boundaries map a false result to their local malformed-
/// response error without exposing graph-internal error names on the wire.
pub fn isCanonicalResultNode(node: GraphResultNode) bool {
    if (!node.metricViewValid()) return false;
    if (node.key.len == 0) return false;
    if (node.table) |table| if (table.len == 0) return false;
    if (node.depth > pattern_mod.max_pattern_hops) return false;
    if (!std.math.isFinite(node.distance) or node.distance < 0) return false;

    const path = node.path orelse {
        // Presence carries meaning in the canonical contract; `[]` is not an
        // alternative spelling for an omitted path or edge list.
        return node.path_tables == null and node.path_edges == null;
    };
    if (path.len == 0 or path.len > pattern_mod.max_pattern_hops + 1) return false;
    if (node.depth != path.len - 1) return false;

    const tables = node.path_tables;
    if (tables) |items| if (items.len != path.len) return false;
    for (path, 0..) |key, i| {
        if (key.len == 0) return false;
        if (tables) |items| if (items[i]) |table| if (table.len == 0) return false;
    }
    if (node.path_edges) |edges| if (edges.len != path.len - 1) return false;

    const terminal_table = if (tables) |items| items[path.len - 1] else null;
    return node_identity.equal(
        .{ .table = terminal_table, .key = path[path.len - 1] },
        .{ .table = node.table, .key = node.key },
    );
}

test "canonical graph result node path is self-consistent" {
    const valid_path: []const []const u8 = &.{ "a", "b" };
    const valid_tables: []const ?[]const u8 = &.{ null, "entities" };
    try std.testing.expect(isCanonicalResultNode(.{
        .key = "b",
        .table = "entities",
        .depth = 1,
        .distance = 1,
        .path = valid_path,
        .path_tables = valid_tables,
    }));
    try std.testing.expect(!isCanonicalResultNode(.{
        .key = "b",
        .table = "entities",
        .depth = 0,
        .distance = 0,
        .path = valid_path,
        .path_tables = valid_tables,
    }));
    try std.testing.expect(!isCanonicalResultNode(.{
        .key = "wrong",
        .table = "entities",
        .depth = 1,
        .distance = 1,
        .path = valid_path,
        .path_tables = valid_tables,
    }));
    try std.testing.expect(!isCanonicalResultNode(.{
        .key = "b",
        .depth = 1,
        .distance = 1,
        .path = valid_path,
        .path_tables = valid_tables,
    }));
    try std.testing.expect(!isCanonicalResultNode(.{
        .key = "b",
        .depth = 0,
        .distance = 0,
        .path_edges = &.{},
    }));
}

pub const GraphMetricValue = struct {
    name: []const u8,
    score: ?f64 = null,
    name_owned: bool = true,

    pub fn ensureNameOwned(self: *GraphMetricValue, alloc: Allocator) !void {
        if (self.name_owned) return;
        self.name = try alloc.dupe(u8, self.name);
        self.name_owned = true;
    }

    pub fn deinit(self: *GraphMetricValue, alloc: Allocator) void {
        if (self.name_owned) alloc.free(self.name);
        self.* = undefined;
    }
};

pub const GraphMetricStatus = struct {
    name: []const u8,
    state: graph_mod.GraphIndex.GraphMetricState = .not_ready,
    phase: graph_mod.GraphIndex.GraphMetricBuildPhase = .idle,
    edge_filter: graph_mod.GraphMetricEdgeFilter = .{},
    metadata_version: u32 = 0,
    config_fingerprint: u64 = 0,
    maintenance_paused: bool = false,
    build_queued: bool = false,
    published_generation: u64 = 0,
    published_edge_generation: u64 = 0,
    edge_generation: u64 = 0,
    target_edge_generation: u64 = 0,
    queued_generation: u64 = 0,
    building_generation: u64 = 0,
    build_job_id: u64 = 0,
    build_started_at_ms: u64 = 0,
    build_iteration: u32 = 0,
    build_lease_expires_at_ms: u64 = 0,
    build_worker_id: []const u8 = "",
    retry_count: u64 = 0,
    last_error: []const u8 = "",
    progress: f64 = 0,
    converged: bool = false,
    iterations_completed: u32 = 0,
    delta: f64 = 0,
    computed_at_ms: u64 = 0,
    last_event: ?graph_mod.GraphIndex.GraphMetricEvent = null,
    recent_events: []graph_mod.GraphIndex.GraphMetricEvent = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.name);
        self.edge_filter.deinit(alloc);
        if (self.build_worker_id.len > 0) alloc.free(self.build_worker_id);
        if (self.last_error.len > 0) alloc.free(self.last_error);
        if (self.recent_events.len > 0) alloc.free(self.recent_events);
        self.* = undefined;
    }
};

pub fn cloneGraphMetricStatus(alloc: Allocator, source: graph_mod.GraphIndex.GraphMetricStatus) !GraphMetricStatus {
    const name = try alloc.dupe(u8, source.name);
    errdefer alloc.free(name);
    var edge_filter = try source.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    const recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        @constCast((&[_]graph_mod.GraphIndex.GraphMetricEvent{})[0..]);
    errdefer if (recent_events.len > 0) alloc.free(recent_events);
    const last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    errdefer if (last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    errdefer if (build_worker_id.len > 0) alloc.free(build_worker_id);
    return .{
        .name = name,
        .state = source.state,
        .phase = source.phase,
        .edge_filter = edge_filter,
        .metadata_version = source.metadata_version,
        .config_fingerprint = source.config_fingerprint,
        .maintenance_paused = source.maintenance_paused,
        .build_queued = source.build_queued,
        .published_generation = source.published_generation,
        .published_edge_generation = source.published_edge_generation,
        .edge_generation = source.edge_generation,
        .target_edge_generation = source.target_edge_generation,
        .queued_generation = source.queued_generation,
        .building_generation = source.building_generation,
        .build_job_id = source.build_job_id,
        .build_started_at_ms = source.build_started_at_ms,
        .build_iteration = source.build_iteration,
        .build_lease_expires_at_ms = source.build_lease_expires_at_ms,
        .build_worker_id = build_worker_id,
        .retry_count = source.retry_count,
        .last_error = last_error,
        .progress = source.progress,
        .converged = source.converged,
        .iterations_completed = source.iterations_completed,
        .delta = source.delta,
        .computed_at_ms = source.computed_at_ms,
        .last_event = source.last_event,
        .recent_events = recent_events,
    };
}

test "graph result node JSON accepts omitted optional path fields" {
    var parsed = try std.json.parseFromSlice(
        GraphResultNode,
        std.testing.allocator,
        "{\"key\":\"doc:z\",\"depth\":1,\"distance\":1}",
        .{},
    );
    defer parsed.deinit();

    try std.testing.expect(parsed.value.path == null);
    try std.testing.expect(parsed.value.path_tables == null);
    try std.testing.expect(parsed.value.path_edges == null);
}

/// Independent metric payload retained by a result rather than individual
/// nodes. A merger keeps one owner per contribution, so row views never move
/// when its node array grows.
pub const GraphMetricOwner = struct {
    values: []GraphMetricValue = &.{},
    columns: ?*GraphMetricColumns = null,
    names: [][]u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.values);
        if (self.columns) |columns| columns.deinit(alloc);
        for (self.names) |name| alloc.free(name);
        alloc.free(self.names);
        self.* = .{};
    }
};

pub const GraphQueryResult = struct {
    nodes: []GraphResultNode,
    matches: []pattern_mod.PatternMatch = &.{},
    metric_status: []GraphMetricStatus = &.{},
    metric_columns: ?*GraphMetricColumns = null,
    metric_values_slab: []GraphMetricValue = &.{},
    metric_value_names: [][]u8 = &.{},

    pub fn deinit(self: *GraphQueryResult, alloc: Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        alloc.free(self.nodes);
        if (self.metric_columns) |columns| columns.deinit(alloc);
        if (self.metric_values_slab.len > 0) alloc.free(self.metric_values_slab);
        for (self.metric_value_names) |name| alloc.free(name);
        if (self.metric_value_names.len > 0) alloc.free(self.metric_value_names);
        pattern_mod.freeMatches(alloc, self.matches);
        for (self.metric_status) |*status| status.deinit(alloc);
        if (self.metric_status.len > 0) alloc.free(self.metric_status);
    }
};

// ============================================================================
// Graph Query Engine
// ============================================================================

/// Per-request scope for the local graph executors, derived by the caller
/// from what it knows about the executing snapshot. It rides the executor
/// vtables so the storage layer, which knows neither its table's name nor
/// its group count, can apply the caller's routing knowledge.
pub const ExecutionScope = struct {
    columnar_metrics: bool = true,
    ttl_now_ns: ?u64 = null,
    /// Physical name of the index-owning table. A `target_table` tag naming
    /// it canonicalizes to the local (null) identity, mirroring the
    /// distributed coordinator's canonicalGraphNodeTable, so a self-table
    /// tag never stops expansion or splits node identity.
    owning_table: []const u8 = "",
    /// True when the executing snapshot holds the graph index's COMPLETE
    /// row set (a single-group table, or an embedded caller): local
    /// traversal, paths, and MATCH may then expand THROUGH cross-table
    /// tagged nodes, because entity-sourced edges are document-owned rows
    /// in this same index (same-snapshot single-index read).
    expand_cross_table_local: bool = false,
};

pub const GraphQueryEngine = struct {
    alloc: Allocator,
    node_admission: ?NodeAdmission = null,
    /// Public request coordinators install one shared budget here. Internal
    /// callers may omit it and retain the graph algorithms' standalone limit.
    work_budget: ?*work_budget_mod.WorkBudget = null,
    /// A coordinator can supply one expiration time for all operations and
    /// shards. A standalone execution captures it for that call.
    ttl_now_ns: ?u64 = null,
    /// See ExecutionScope; defaults keep the historical terminal behavior at
    /// cross-table tagged nodes.
    scope: ExecutionScope = .{},

    /// Execute a graph query. For result_ref node selectors, the caller must
    /// resolve refs to keys and pass them as resolved_keys.
    pub fn execute(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        resolved_keys: []const []const u8,
    ) !GraphQueryResult {
        const supplied_time = self.ttl_now_ns;
        if (supplied_time == null) self.ttl_now_ns = self.scope.ttl_now_ns orelse graph_index.clock.nowRealtimeNs();
        defer self.ttl_now_ns = supplied_time;
        try validateGraphMetricQueryShape(gq);
        const defer_result_limit = graphMetricPostProcessingNeedsFullCandidateSet(gq);
        var execution_params = gq.params;
        if (defer_result_limit) execution_params.max_results = graph_metric_candidate_limit + 1;

        var execution_query = gq;
        execution_query.params = execution_params;

        var result = try switch (execution_query.query_type) {
            .traverse => self.executeTraverse(graph_index, execution_params, resolved_keys, resolveTargetKeys(gq)),
            .neighbors => blk: {
                var params = execution_params;
                params.max_depth = 1;
                break :blk self.executeTraverse(graph_index, params, resolved_keys, resolveTargetKeys(gq));
            },
            .shortest_path => self.executeShortestPath(graph_index, execution_query, resolved_keys),
            .k_shortest_paths => self.executeKShortestPaths(graph_index, execution_query, resolved_keys),
            .pattern => self.executePattern(graph_index, execution_query, resolved_keys),
        };
        errdefer result.deinit(self.alloc);
        if (defer_result_limit and result.nodes.len > graph_metric_candidate_limit) {
            return error.QueryCandidateBudgetExceeded;
        }
        if (gq.metrics.len != 0 or gq.order_by.len != 0 or gq.where_metric.len != 0) {
            try self.applyMetricDependenciesColumnar(graph_index, gq, defer_result_limit, &result);
        }
        if (gq.query_type == .traverse or gq.query_type == .neighbors) {
            for (result.nodes) |*node| if (node.path != null and node.table != null and node.path_tables == null) {
                const bytes = try std.math.add(usize, try std.math.mul(usize, node.path.?.len, @sizeOf(?[]const u8)), if (node.table) |table| table.len else 0);
                if (self.work_budget) |budget| try budget.retainStateBytes(bytes);
                node.path_tables = pathTablesFromTerminalAlloc(self.alloc, node.path.?.len, node.table) catch |err| {
                    if (self.work_budget) |budget| budget.releaseStateBytes(bytes);
                    return err;
                };
            };
        }
        return result;
    }

    fn graphMetricPostProcessingNeedsFullCandidateSet(gq: GraphQuery) bool {
        return gq.where_metric.len > 0 or gq.order_by.len > 0;
    }

    fn metricFilterMatches(score: f64, filter: GraphMetricFilter) bool {
        return switch (filter.op) {
            .gt => score > filter.value,
            .gte => score >= filter.value,
            .lt => score < filter.value,
            .lte => score <= filter.value,
            .eq => score == filter.value,
            .neq => score != filter.value,
        };
    }

    /// Borrowed column view. Selection maps and storage remain valid until
    /// the owning workspace's next select/deinit.
    const ScorePages = @import("metric_pages.zig").ScorePages;
    pub const MetricColumn = struct {
        pub const Paged = struct { owner: *ScorePages, column: usize };
        pub const Source = union(enum) { nullable: []?f64, decoded: []f64, paged: Paged };
        source: Source,
        rows: ?[]const usize = null,
        len: usize,
        pub fn at(self: @This(), row: usize) ?f64 {
            const index = if (self.rows) |mapping| mapping[row] else row;
            return switch (self.source) {
                .nullable => |values| values[index],
                .decoded => |values| if (std.math.isNan(values[index])) null else values[index],
                .paged => |page| blk: {
                    const value = page.owner.at(page.column, index);
                    break :blk if (std.math.isNan(value)) null else value;
                },
            };
        }
    };
    fn columnScore(column: anytype, row: usize) ?f64 {
        return if (comptime @TypeOf(column) == MetricColumn) column.at(row) else column[row];
    }

    const MetricColumnSortContext = struct {
        orders: []const GraphMetricOrder,
        metric_indexes: []const usize,
        score_columns: []const []?f64 = &.{},
        views: []const MetricColumn = &.{},
        row_ordinals: ?[]const usize = null,
        fn score(self: @This(), column: usize, row: usize) ?f64 {
            return if (self.views.len != 0) self.views[column].at(row) else self.score_columns[column][row];
        }
    };

    fn metricColumnCandidateLessThan(context: MetricColumnSortContext, left: usize, right: usize) bool {
        for (context.orders, context.metric_indexes) |order, metric_index| {
            const cmp = compareOptionalMetricScore(
                context.score(metric_index, left),
                context.score(metric_index, right),
                order,
            );
            if (cmp) |less| return less;
        }
        return if (context.row_ordinals) |rows| rows[left] < rows[right] else left < right;
    }

    // Resolve the shared page directory and column offsets once before sorting.
    // The heap owns unmapped rows, so comparisons need neither a source-tag
    // dispatch nor a row-map lookup for every score.
    const PagedMetricSortContext = struct {
        orders: []const GraphMetricOrder,
        offsets: []const usize,
        pages: []const []f64,
        row_shift: std.math.Log2Int(usize),
        row_mask: usize,
        row_ordinals: []const usize,
    };

    fn pagedMetricCandidateLessThan(context: PagedMetricSortContext, left: usize, right: usize) bool {
        const left_page = context.pages[left >> context.row_shift];
        const right_page = context.pages[right >> context.row_shift];
        const left_row = left & context.row_mask;
        const right_row = right & context.row_mask;
        for (context.orders, context.offsets) |order, offset| {
            const l = left_page[offset + left_row];
            const r = right_page[offset + right_row];
            if (compareOptionalMetricScore(
                if (std.math.isNan(l)) null else l,
                if (std.math.isNan(r)) null else r,
                order,
            )) |less| return less;
        }
        return context.row_ordinals[left] < context.row_ordinals[right];
    }

    fn siftWorstMetricCandidateUp(context: MetricColumnSortContext, heap: []usize, start: usize) void {
        var child = start;
        while (child > 0) {
            const parent = (child - 1) / 2;
            if (!metricColumnCandidateLessThan(context, heap[parent], heap[child])) break;
            std.mem.swap(usize, &heap[parent], &heap[child]);
            child = parent;
        }
    }

    fn siftWorstMetricCandidateDown(context: MetricColumnSortContext, heap: []usize, start: usize) void {
        var parent = start;
        while (true) {
            const left = parent * 2 + 1;
            if (left >= heap.len) return;
            const right = left + 1;
            var worse_child = left;
            if (right < heap.len and metricColumnCandidateLessThan(context, heap[left], heap[right])) {
                worse_child = right;
            }
            if (!metricColumnCandidateLessThan(context, heap[parent], heap[worse_child])) return;
            std.mem.swap(usize, &heap[parent], &heap[worse_child]);
            parent = worse_child;
        }
    }

    /// Select and order only the externally observable prefix. This changes a
    /// 100k-candidate query with a 100-row limit from O(N log N) to O(N log K)
    /// while retaining deterministic original-order tie breaking.
    fn retainOrderedMetricCandidatePrefix(
        candidates: []usize,
        keep_count: usize,
        context: MetricColumnSortContext,
    ) []usize {
        const keep = @min(candidates.len, keep_count);
        if (keep == 0) return candidates[0..0];
        if (keep == candidates.len) {
            std.mem.sort(usize, candidates, context, metricColumnCandidateLessThan);
            return candidates;
        }
        const heap = candidates[0..keep];
        for (1..heap.len) |i| siftWorstMetricCandidateUp(context, heap, i);
        for (candidates[keep..]) |candidate| {
            if (!metricColumnCandidateLessThan(context, candidate, heap[0])) continue;
            heap[0] = candidate;
            siftWorstMetricCandidateDown(context, heap, 0);
        }
        std.mem.sort(usize, heap, context, metricColumnCandidateLessThan);
        return heap;
    }

    fn compareOptionalMetricScore(left: ?f64, right: ?f64, order: GraphMetricOrder) ?bool {
        if (left == null and right == null) return null;
        if (left == null) return order.nulls == .first;
        if (right == null) return order.nulls != .first;
        if (left.? == right.?) return null;
        return if (order.direction == .desc) left.? > right.? else left.? < right.?;
    }

    fn metricColumnNameIndex(dependency_names: []const []const u8, name: []const u8) ?usize {
        for (dependency_names, 0..) |dependency_name, i| {
            if (std.mem.eql(u8, dependency_name, name)) return i;
        }
        return null;
    }

    fn metricCandidatePassesFilters(
        candidate_index: usize,
        filters: []const GraphMetricFilter,
        filter_metric_indexes: []const usize,
        score_columns: anytype,
    ) bool {
        for (filters, filter_metric_indexes) |filter, metric_index| {
            const score = columnScore(score_columns[metric_index], candidate_index) orelse return false;
            if (!metricFilterMatches(score, filter)) return false;
        }
        return true;
    }

    /// Returns source row indexes after metric filtering, ordering, and the
    /// optional response limit. Serverless uses this to compact and reuse
    /// already-fetched columns between its filter/order/projection stages.
    pub fn selectLoadedMetricCandidateIndexesAlloc(alloc: Allocator, names: []const []const u8, columns: []const []?f64, query: GraphQuery, apply_limit: bool, count: usize) ![]usize {
        return selectMetricCandidateIndexesAlloc(alloc, names, columns, query, apply_limit, count);
    }

    fn selectMetricCandidateIndexesAlloc(
        alloc: Allocator,
        dependency_names: []const []const u8,
        score_columns: anytype,
        query: GraphQuery,
        apply_result_limit: bool,
        node_count: usize,
    ) ![]usize {
        var selected = try selectMetricCandidates(alloc, dependency_names, score_columns, query, apply_result_limit, node_count);
        defer selected.deinit(alloc);
        if (selected.identity_count) |count| {
            const rows = try alloc.alloc(usize, count);
            for (rows, 0..) |*row, i| row.* = i;
            return rows;
        }
        const rows = selected.rows;
        selected.rows = &.{};
        return rows;
    }

    const MetricSelection = struct {
        rows: []usize = &.{},
        identity_count: ?usize = null,
        fn count(self: @This()) usize {
            return self.identity_count orelse self.rows.len;
        }
        fn at(self: @This(), row: usize) usize {
            return if (self.identity_count != null) row else self.rows[row];
        }
        fn deinit(self: *@This(), a: Allocator) void {
            a.free(self.rows);
        }
    };

    fn selectMetricCandidates(
        alloc: Allocator,
        dependency_names: []const []const u8,
        score_columns: anytype,
        query: GraphQuery,
        apply_result_limit: bool,
        node_count: usize,
    ) !MetricSelection {
        try validateGraphMetricQueryShape(query);
        if (dependency_names.len != score_columns.len) return error.InvalidQueryRequest;
        for (score_columns, dependency_names, 0..) |column, dependency_name, i| {
            if (column.len != node_count) return error.InvalidQueryRequest;
            for (dependency_names[0..i]) |prior_name| {
                if (std.mem.eql(u8, prior_name, dependency_name)) return error.InvalidQueryRequest;
            }
        }

        var filter_index_buffer: [graph_metric_filter_limit]usize = undefined;
        const filter_metric_indexes = filter_index_buffer[0..query.where_metric.len];
        for (query.where_metric, 0..) |filter, i| {
            filter_metric_indexes[i] = metricColumnNameIndex(dependency_names, filter.name) orelse
                return error.InvalidQueryRequest;
        }
        const requested_limit: usize = if (apply_result_limit and query.params.max_results != 0)
            @min(node_count, @as(usize, @intCast(query.params.max_results)))
        else
            node_count;
        var order_index_buffer: [graph_metric_order_limit]usize = undefined;
        const order_metric_indexes = order_index_buffer[0..query.order_by.len];
        for (query.order_by, 0..) |order, i| {
            order_metric_indexes[i] = metricColumnNameIndex(dependency_names, order.name) orelse return error.InvalidQueryRequest;
        }
        const context = MetricColumnSortContext{
            .orders = query.order_by,
            .metric_indexes = order_metric_indexes,
            .score_columns = if (comptime @TypeOf(score_columns[0]) == MetricColumn) &.{} else score_columns,
            .views = if (comptime @TypeOf(score_columns[0]) == MetricColumn) score_columns else &.{},
        };
        if (requested_limit == 0) return .{ .identity_count = 0 };
        var candidates: []usize = &.{};
        errdefer alloc.free(candidates);
        const bounded_order = query.order_by.len != 0 and requested_limit < node_count;
        var count: usize = 0;
        for (0..node_count) |row| {
            if (!metricCandidatePassesFilters(row, query.where_metric, filter_metric_indexes, score_columns)) continue;
            // Keep a passing prefix implicit until a gap or ordering requires
            // storage. All-pass staged selections allocate no row arrays.
            if (query.order_by.len == 0 and candidates.len == 0 and row == count) {
                count += 1;
                if (count == requested_limit) break;
                continue;
            }
            if (candidates.len == 0) {
                candidates = try alloc.alloc(usize, requested_limit);
                for (0..count) |i| candidates[i] = i;
            }
            if (count < candidates.len) {
                candidates[count] = row;
                count += 1;
                if (bounded_order) siftWorstMetricCandidateUp(context, candidates[0..count], count - 1);
            } else if (bounded_order) {
                if (!metricColumnCandidateLessThan(context, row, candidates[0])) continue;
                candidates[0] = row;
                siftWorstMetricCandidateDown(context, candidates, 0);
            } else break;
        }
        if (candidates.len == 0) return .{ .identity_count = count };
        const selected = candidates[0..count];
        if (query.order_by.len != 0) std.mem.sort(usize, selected, context, metricColumnCandidateLessThan);
        return .{ .rows = try alloc.realloc(candidates, count) };
    }

    /// Storage-independent graph-metric post-processing. Callers retain score
    /// columns in their native storage representation until this routine has
    /// filtered and selected the externally visible prefix. Metric objects are
    /// allocated only for surviving rows.
    pub fn applyLoadedMetricColumns(
        alloc: Allocator,
        dependency_names: []const []const u8,
        metric_value_names: []const []const u8,
        score_columns: []const []?f64,
        query: GraphQuery,
        apply_result_limit: bool,
        nodes: *[]GraphResultNode,
    ) ![]GraphMetricValue {
        try validateGraphMetricQueryShape(query);
        if (dependency_names.len != score_columns.len or metric_value_names.len != score_columns.len)
            return error.InvalidQueryRequest;
        for (score_columns, dependency_names, metric_value_names, 0..) |column, dependency_name, metric_value_name, i| {
            if (column.len != nodes.*.len or !std.mem.eql(u8, dependency_name, metric_value_name))
                return error.InvalidQueryRequest;
            for (dependency_names[0..i]) |prior_name| {
                if (std.mem.eql(u8, prior_name, dependency_name)) return error.InvalidQueryRequest;
            }
        }

        const selected = try selectLoadedMetricCandidateIndexesAlloc(
            alloc,
            dependency_names,
            score_columns,
            query,
            apply_result_limit,
            nodes.*.len,
        );
        defer alloc.free(selected);

        return try applySelectedMetricColumns(
            alloc,
            dependency_names,
            metric_value_names,
            score_columns,
            query,
            selected,
            nodes,
        );
    }

    /// Materializes a selection previously produced by
    /// `selectLoadedMetricCandidateIndexesAlloc`. Keeping selection separate
    /// lets staged backends reuse the exact same indexes for node mutation and
    /// score-column compaction instead of repeating filtering and top-k work.
    pub fn applySelectedMetricColumns(
        alloc: Allocator,
        dependency_names: []const []const u8,
        metric_value_names: []const []const u8,
        score_columns: []const []?f64,
        query: GraphQuery,
        selected: []const usize,
        nodes: *[]GraphResultNode,
    ) ![]GraphMetricValue {
        try validateGraphMetricQueryShape(query);
        if (dependency_names.len != score_columns.len or metric_value_names.len != score_columns.len)
            return error.InvalidQueryRequest;
        for (score_columns, dependency_names, metric_value_names, 0..) |column, dependency_name, metric_value_name, i| {
            if (column.len != nodes.*.len or !std.mem.eql(u8, dependency_name, metric_value_name))
                return error.InvalidQueryRequest;
            for (dependency_names[0..i]) |prior_name| {
                if (std.mem.eql(u8, prior_name, dependency_name)) return error.InvalidQueryRequest;
            }
        }

        var projection_index_buffer: [graph_metric_projection_limit]usize = undefined;
        const projection_metric_indexes = projection_index_buffer[0..query.metrics.len];
        for (query.metrics, 0..) |metric, i| {
            projection_metric_indexes[i] = metricColumnNameIndex(dependency_names, metric.name) orelse
                return error.InvalidQueryRequest;
        }

        var selected_mask = try std.DynamicBitSetUnmanaged.initEmpty(alloc, nodes.*.len);
        defer selected_mask.deinit(alloc);
        for (selected) |source_index| {
            if (source_index >= nodes.*.len or selected_mask.isSet(source_index)) return error.InvalidQueryRequest;
            selected_mask.set(source_index);
        }

        const slab_len = std.math.mul(usize, selected.len, query.metrics.len) catch return error.QueryCandidateBudgetExceeded;
        const metric_values_slab = if (slab_len == 0)
            @constCast((&[_]GraphMetricValue{})[0..])
        else
            try alloc.alloc(GraphMetricValue, slab_len);
        errdefer if (metric_values_slab.len > 0) alloc.free(metric_values_slab);
        for (selected, 0..) |source_index, row_index| {
            const row = metric_values_slab[row_index * query.metrics.len ..][0..query.metrics.len];
            for (row, projection_metric_indexes) |*value, metric_index| {
                value.* = .{
                    .name = metric_value_names[metric_index],
                    .score = score_columns[metric_index][source_index],
                    .name_owned = false,
                };
            }
        }

        const final_nodes = try alloc.alloc(GraphResultNode, selected.len);
        for (selected, 0..) |source_index, out_index| {
            var node = nodes.*[source_index];
            for (node.metrics) |*metric| metric.deinit(alloc);
            if (node.metrics_owned and node.metrics.len > 0) alloc.free(node.metrics);
            node.metrics = metric_values_slab[out_index * query.metrics.len ..][0..query.metrics.len];
            node.metrics_owned = false;
            node.metric_columns = null;
            final_nodes[out_index] = node;
        }
        for (nodes.*, 0..) |*node, source_index| if (!selected_mask.isSet(source_index)) node.deinit(alloc);
        alloc.free(nodes.*);
        nodes.* = final_nodes;
        return metric_values_slab;
    }

    /// Installs an already-selected row set whose score columns are aligned
    /// with output order rather than the original candidate array. Staged
    /// backends use this to carry stable source ordinals through filtering and
    /// ordering, then move nodes and materialize projected values exactly once.
    pub fn materializeSelectedMetricColumns(
        alloc: Allocator,
        metric_value_names: []const []const u8,
        aligned_score_columns: []const []?f64,
        selected_source_indexes: []const usize,
        nodes: *[]GraphResultNode,
    ) ![]GraphMetricValue {
        return materializeSelectedMetricColumnsWithAllocators(alloc, alloc, alloc, metric_value_names, aligned_score_columns, selected_source_indexes, nodes);
    }

    fn materializeSelectedMetricColumnsWithAllocators(node_alloc: Allocator, alloc: Allocator, scratch: Allocator, names: []const []const u8, columns: []const []?f64, selected: []const usize, nodes: *[]GraphResultNode) ![]GraphMetricValue {
        return materializeMetricColumnsWithAllocators(node_alloc, alloc, scratch, names, columns, selected, nodes);
    }

    fn materializeMetricColumnsWithAllocators(
        node_alloc: Allocator,
        alloc: Allocator,
        scratch: Allocator,
        metric_value_names: []const []const u8,
        aligned_score_columns: anytype,
        selected_source_indexes: []const usize,
        nodes: *[]GraphResultNode,
    ) ![]GraphMetricValue {
        if (metric_value_names.len != aligned_score_columns.len)
            return error.InvalidQueryRequest;
        for (aligned_score_columns, metric_value_names, 0..) |column, name, i| {
            if (column.len != selected_source_indexes.len or name.len == 0)
                return error.InvalidQueryRequest;
            for (metric_value_names[0..i]) |prior_name| {
                if (std.mem.eql(u8, prior_name, name)) return error.InvalidQueryRequest;
            }
        }

        const full = selected_source_indexes.len == nodes.*.len;
        var identity = full;
        if (full) for (selected_source_indexes, 0..) |source, row| {
            if (source != row) {
                identity = false;
                break;
            }
        };
        var selected_mask: std.DynamicBitSetUnmanaged = .{};
        defer selected_mask.deinit(scratch);
        if (!identity) selected_mask = try std.DynamicBitSetUnmanaged.initEmpty(scratch, nodes.*.len);
        if (!identity) for (selected_source_indexes) |source_index| {
            if (source_index >= nodes.*.len or selected_mask.isSet(source_index))
                return error.InvalidQueryRequest;
            selected_mask.set(source_index);
        };

        const slab_len = std.math.mul(usize, selected_source_indexes.len, metric_value_names.len) catch
            return error.QueryCandidateBudgetExceeded;
        const metric_values_slab = if (slab_len == 0)
            @constCast((&[_]GraphMetricValue{})[0..])
        else
            try alloc.alloc(GraphMetricValue, slab_len);
        errdefer if (metric_values_slab.len > 0) alloc.free(metric_values_slab);
        for (selected_source_indexes, 0..) |_, row_index| {
            const row = metric_values_slab[row_index * metric_value_names.len ..][0..metric_value_names.len];
            for (row, metric_value_names, aligned_score_columns) |*value, name, column| {
                value.* = .{ .name = name, .score = columnScore(column, row_index), .name_owned = false };
            }
        }

        if (full) {
            // All fallible output allocation and permutation validation precedes
            // this ownership commit. Reuse the original exact-length array.
            if (!identity) {
                selected_mask.unsetAll();
                for (0..nodes.*.len) |start_row| {
                    if (selected_mask.isSet(start_row)) continue;
                    const saved = nodes.*[start_row];
                    var target = start_row;
                    while (true) {
                        selected_mask.set(target);
                        const source = selected_source_indexes[target];
                        if (source == start_row) {
                            nodes.*[target] = saved;
                            break;
                        }
                        nodes.*[target] = nodes.*[source];
                        target = source;
                    }
                }
            }
            for (nodes.*, 0..) |*node, row_index| {
                for (node.metrics) |*metric| metric.deinit(node_alloc);
                if (node.metrics_owned and node.metrics.len != 0) node_alloc.free(node.metrics);
                node.metrics = metric_values_slab[row_index * metric_value_names.len ..][0..metric_value_names.len];
                node.metrics_owned = false;
                node.metric_columns = null;
            }
            return metric_values_slab;
        }

        const final_nodes = try alloc.alloc(GraphResultNode, selected_source_indexes.len);
        for (selected_source_indexes, 0..) |source_index, out_index| {
            var node = nodes.*[source_index];
            for (node.metrics) |*metric| metric.deinit(node_alloc);
            if (node.metrics_owned and node.metrics.len > 0) node_alloc.free(node.metrics);
            node.metrics = metric_values_slab[out_index * metric_value_names.len ..][0..metric_value_names.len];
            node.metrics_owned = false;
            node.metric_columns = null;
            final_nodes[out_index] = node;
        }
        for (nodes.*, 0..) |*node, source_index| if (!selected_mask.isSet(source_index)) node.deinit(node_alloc);
        node_alloc.free(nodes.*);
        nodes.* = final_nodes;
        return metric_values_slab;
    }

    /// Peak extra bytes for compact score installation. Full selections move
    /// the existing node array; only nonidentity selections need a bitmap.
    pub fn compactMaterializationBytes(projections: usize, selected: []const usize, node_count: usize) !usize {
        var bytes = try std.math.mul(usize, try std.math.mul(usize, selected.len, projections), @sizeOf(f64));
        if (projections != 0) bytes = try std.math.add(usize, bytes, @sizeOf(GraphMetricColumns));
        if (selected.len != node_count) bytes = try std.math.add(usize, bytes, try std.math.mul(usize, selected.len, @sizeOf(GraphResultNode)));
        var identity = selected.len == node_count;
        if (identity) for (selected, 0..) |row, i| {
            if (row != i) {
                identity = false;
                break;
            }
        };
        if (!identity) bytes = try std.math.add(usize, bytes, try std.math.mul(usize, try std.math.divCeil(usize, node_count, @bitSizeOf(usize)), @sizeOf(usize)));
        return bytes;
    }

    /// Keep metric scores columnar through filtering, ordering, and limiting.
    /// Per-node public metric objects are created only for the surviving result
    /// page, avoiding candidate-count heap fragmentation and needless copies.
    /// Names transfer only on success. All column ownership uses the response
    /// allocator, and every required allocation precedes the node commit.
    pub fn materializeSelectedMetricColumnsCompact(node_alloc: Allocator, output: Allocator, scratch: Allocator, names: [][]u8, columns: anytype, selected: []const usize, nodes: *[]GraphResultNode) !*GraphMetricColumns {
        if (names.len != columns.len or selected.len > std.math.maxInt(u32)) return error.InvalidRequest;
        for (columns, names, 0..) |column, name, index| {
            if (column.len != selected.len or name.len == 0) return error.InvalidQueryRequest;
            for (names[0..index]) |previous| if (std.mem.eql(u8, previous, name)) return error.InvalidQueryRequest;
        }
        const owner = try output.create(GraphMetricColumns);
        errdefer output.destroy(owner);
        const scores = try output.alloc(f64, try std.math.mul(usize, selected.len, names.len));
        errdefer output.free(scores);
        for (columns, 0..) |column, index| {
            for (0..selected.len) |row| scores[index * selected.len + row] = columnScore(column, row) orelse std.math.nan(f64);
        }
        // Promote to the response allocator. The work-budget allocator is
        // stack-bound and must never become a retained column owner.
        _ = try materializeMetricColumnsWithAllocators(node_alloc, output, scratch, &.{}, &.{}, selected, nodes);
        owner.* = .{ .names = names, .scores = scores, .rows = selected.len };
        for (nodes.*, 0..) |*node, row| {
            node.metric_columns = owner;
            node.metric_row = @intCast(row);
        }
        return owner;
    }

    fn applyMetricDependenciesColumnar(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        query: GraphQuery,
        apply_result_limit: bool,
        result: *GraphQueryResult,
    ) !void {
        var scratch_budget = work_budget_mod.RetainedAllocator{ .backing = self.alloc, .budget = self.work_budget };
        var output_budget = work_budget_mod.RetainedAllocator{ .backing = self.alloc, .budget = self.work_budget };
        defer std.debug.assert(scratch_budget.live_bytes == 0 and output_budget.live_bytes == 0);
        self.applyStagedMetricDependencies(graph_index, query, apply_result_limit, result, &scratch_budget, &output_budget) catch |err| {
            if (err == error.OutOfMemory and (scratch_budget.denied or output_budget.denied)) return error.GraphWorkBudgetExceeded;
            return err;
        };
        output_budget.detach();
    }

    fn applyStagedMetricDependencies(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        query: GraphQuery,
        apply_result_limit: bool,
        result: *GraphQueryResult,
        scratch_budget: *work_budget_mod.RetainedAllocator,
        output_budget: *work_budget_mod.RetainedAllocator,
    ) !void {
        const scratch = scratch_budget.allocator();
        const output = output_budget.allocator();
        const plan = try MetricReadPlan.init(query);
        var session = try graph_index.openGraphMetricReadSessionAlloc(scratch, plan.dependencies.slice(), plan.policies[0..plan.dependencies.len]);
        defer session.deinit();
        var workspace = try MetricStageWorkspace.init(scratch, plan, result.nodes.len);
        defer workspace.deinit();
        const fused = try workspace.selectReading(&session, query, apply_result_limit, result.nodes);
        if (!fused and plan.filters.len != 0) {
            try workspace.ensure(&session, plan.filters.slice(), result.nodes);
            var filter_query = query;
            filter_query.metrics = &.{};
            filter_query.order_by = &.{};
            try workspace.select(filter_query, apply_result_limit and plan.orders.len == 0, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
        }
        if (!fused and plan.orders.len != 0) {
            try workspace.ensure(&session, plan.orders.slice(), result.nodes);
            var order_query = query;
            order_query.metrics = &.{};
            order_query.where_metric = &.{};
            try workspace.select(order_query, apply_result_limit, plan.orders.slice(), plan.projections.slice(), &.{});
        }
        try workspace.ensure(&session, plan.projections.slice(), result.nodes);

        const statuses = try output.alloc(GraphMetricStatus, plan.dependencies.len);
        var initialized_statuses: usize = 0;
        errdefer {
            for (statuses[0..initialized_statuses]) |*status| status.deinit(output);
            output.free(statuses);
        }
        for (session.statuses, statuses) |status, *out| {
            out.* = try cloneGraphMetricStatus(output, status);
            initialized_statuses += 1;
        }
        const metric_value_names = try output.alloc([]u8, plan.projections.len);
        var initialized_metric_names: usize = 0;
        errdefer {
            for (metric_value_names[0..initialized_metric_names]) |name| output.free(name);
            output.free(metric_value_names);
        }
        for (plan.projections.slice(), metric_value_names) |name, *out| {
            out.* = try output.dupe(u8, name);
            initialized_metric_names += 1;
        }
        var columns: [graph_metric_dependency_limit]MetricColumn = undefined;
        workspace.columnsFor(plan.projections.slice(), columns[0..plan.projections.len]);
        const selected = try workspace.explicitRows();
        var compact: ?*GraphMetricColumns = null;
        const slab = if (self.scope.columnar_metrics and metric_value_names.len != 0) columnar: {
            compact = try workspace.adoptProjectionColumns(self.alloc, output, scratch, scratch_budget, output_budget, metric_value_names, selected, &result.nodes);
            if (compact == null) compact = try materializeSelectedMetricColumnsCompact(self.alloc, output, scratch, metric_value_names, columns[0..plan.projections.len], selected, &result.nodes);
            break :columnar @as([]GraphMetricValue, &.{});
        } else try materializeMetricColumnsWithAllocators(self.alloc, output, scratch, metric_value_names, columns[0..plan.projections.len], selected, &result.nodes);
        if (result.metric_values_slab.len > 0) self.alloc.free(result.metric_values_slab);
        if (result.metric_columns) |owner| owner.deinit(self.alloc);
        for (result.metric_value_names) |name| self.alloc.free(name);
        if (result.metric_value_names.len > 0) self.alloc.free(result.metric_value_names);
        result.metric_values_slab = slab;
        result.metric_columns = compact;
        result.metric_value_names = if (compact != null) &.{} else metric_value_names;
        if (compact) |owner| for (result.nodes, 0..) |*node, row| {
            node.metric_columns = owner;
            node.metric_row = @intCast(row);
        };
        for (result.metric_status) |*status| status.deinit(self.alloc);
        if (result.metric_status.len > 0) self.alloc.free(result.metric_status);
        result.metric_status = statuses;
    }

    /// In-place MSD partitioning avoids a candidate-sized key descriptor slab
    /// and comparisons that repeatedly chase large GraphResultNode records.
    /// Recurse only into smaller partitions, tail-walking the largest: stack
    /// depth is logarithmic even for arbitrarily long common key prefixes.
    fn sortMetricReadRows(rows: []usize, nodes: []const GraphResultNode) void {
        var local_end = rows.len;
        var i: usize = 0;
        while (i < local_end) {
            if (nodes[rows[i]].table != null) {
                local_end -= 1;
                std.mem.swap(usize, &rows[i], &rows[local_end]);
            } else i += 1;
        }
        radixMetricReadRows(rows[0..local_end], nodes, 0);
    }
    fn metricKeyBucket(nodes: []const GraphResultNode, row: usize, depth: usize) usize {
        const key = nodes[row].key;
        return if (depth < key.len) @as(usize, key[depth]) + 1 else 0;
    }
    fn radixMetricReadRows(input: []usize, nodes: []const GraphResultNode, initial_depth: usize) void {
        var rows = input;
        var depth = initial_depth;
        while (rows.len > 64) {
            var counts: [257]usize = @splat(0);
            for (rows) |row| counts[metricKeyBucket(nodes, row, depth)] += 1;
            if (counts[0] == rows.len) return;
            var largest: usize = 1;
            for (2..counts.len) |bucket| if (counts[bucket] > counts[largest]) {
                largest = bucket;
            };
            if (counts[largest] == rows.len) {
                depth += 1;
                continue;
            }
            var starts: [258]usize = undefined;
            starts[0] = 0;
            for (counts, 0..) |count, bucket| starts[bucket + 1] = starts[bucket] + count;
            var next: [257]usize = starts[0..257].*;
            for (0..counts.len) |bucket| while (next[bucket] < starts[bucket + 1]) {
                const wanted = metricKeyBucket(nodes, rows[next[bucket]], depth);
                if (wanted == bucket) next[bucket] += 1 else {
                    std.mem.swap(usize, &rows[next[bucket]], &rows[next[wanted]]);
                    next[wanted] += 1;
                }
            };
            for (1..counts.len) |bucket| {
                if (bucket == largest or counts[bucket] <= 1) continue;
                radixMetricReadRows(rows[starts[bucket]..starts[bucket + 1]], nodes, depth + 1);
            }
            rows = rows[starts[largest]..starts[largest + 1]];
            depth += 1;
        }
        const Order = struct {
            nodes: []const GraphResultNode,
            fn less(ctx: @This(), left: usize, right: usize) bool {
                return std.mem.order(u8, ctx.nodes[left].key, ctx.nodes[right].key) == .lt;
            }
        };
        std.mem.sort(usize, rows, Order{ .nodes = nodes }, Order.less);
    }

    /// Scores are kept by heap slot; heap maintenance moves only slot indexes.
    /// The stable source ordinal resolves ties across batch boundaries.
    const MetricCandidateHeap = struct {
        alloc: Allocator,
        slots: []usize,
        rows: []usize,
        columns: [graph_metric_dependency_limit]MetricColumn = undefined,
        column_count: usize,
        order_indexes: [graph_metric_order_limit]usize = undefined,
        orders: []const GraphMetricOrder,
        count: usize = 0,
        limit: usize,
        permuted: bool = false,
        paged_owner: ?*ScorePages = null,
        fn allocate(a: Allocator, capacity: usize, limit: usize, column_count: usize, ordered: bool) !@This() {
            const slots = try a.alloc(usize, if (ordered) capacity else 0);
            errdefer a.free(slots);
            const rows = try a.alloc(usize, capacity);
            errdefer a.free(rows);
            var self = @This(){ .alloc = a, .slots = slots, .rows = rows, .column_count = column_count, .orders = &.{}, .limit = limit };
            var initialized: usize = 0;
            errdefer for (self.columns[0..initialized]) |column| a.free(column.source.decoded);
            for (self.columns[0..column_count]) |*column| {
                column.* = .{ .source = .{ .decoded = try a.alloc(f64, capacity) }, .len = capacity };
                initialized += 1;
            }
            return self;
        }
        fn init(a: Allocator, capacity: usize, limit: usize, names: []const []const u8, orders: []const GraphMetricOrder, permuted: bool, paged: bool, projected: []const []const u8) !@This() {
            var self = try allocate(a, capacity, limit, 0, !paged and permuted);
            errdefer self.deinit();
            self.orders = orders;
            self.permuted = permuted;
            var page_columns: usize = 0;
            if (paged) for (names) |name| {
                page_columns += @intFromBool(metricColumnNameIndex(projected, name) != null);
            };
            // A single contiguous owner already has cheap scalar growth.
            if (page_columns > 1) self.paged_owner = try ScorePages.createForRows(a, page_columns, capacity);
            var page_index: usize = 0;
            for (names) |name| {
                const is_paged = self.paged_owner != null and metricColumnNameIndex(projected, name) != null;
                self.columns[self.column_count] = .{ .source = if (is_paged) .{ .paged = .{ .owner = self.paged_owner.?, .column = page_index } } else .{ .decoded = try a.alloc(f64, capacity) }, .len = capacity };
                self.column_count += 1;
                if (is_paged) page_index += 1;
            }
            for (orders, self.order_indexes[0..orders.len]) |order, *index| index.* = metricColumnNameIndex(names, order.name) orelse return error.InvalidQueryRequest;
            return self;
        }
        fn deinit(self: *@This()) void {
            self.alloc.free(self.slots);
            self.alloc.free(self.rows);
            for (self.columns[0..self.column_count]) |column| switch (column.source) {
                .decoded => |values| self.alloc.free(values),
                .paged => {},
                .nullable => unreachable,
            };
            if (self.paged_owner) |owner| owner.destroy();
        }
        fn grow(self: *@This()) !void {
            const capacity = if (self.rows.len >= self.limit - self.rows.len) self.limit else self.rows.len * 2;
            for (self.columns[0..self.column_count]) |*column| {
                switch (column.source) {
                    .decoded => |values| column.source = .{ .decoded = try self.alloc.realloc(values, capacity) },
                    .paged => {},
                    .nullable => unreachable,
                }
                column.len = capacity;
            }
            self.rows = try self.alloc.realloc(self.rows, capacity);
            // Append-only collection needs the permutation only at final sort.
            if (self.slots.len != 0) self.slots = try self.alloc.realloc(self.slots, capacity);
        }
        fn append(self: *@This(), ordinal: usize, columns: []const []f64, row: usize, direct: bool) !void {
            if (self.count == self.rows.len) try self.grow();
            if (self.paged_owner) |owner| try owner.ensureRows(self.count + 1);
            self.rows[self.count] = ordinal;
            for (columns, self.columns[0..self.column_count]) |column, target| switch (target.source) {
                .decoded => |values| values[self.count] = column[row],
                .paged => |page| {
                    if (!direct or column.ptr + row != page.owner.window(page.column, self.count, 1).ptr) page.owner.set(page.column, self.count, column[row]);
                },
                .nullable => unreachable,
            };
            self.count += 1;
            if (self.paged_owner) |owner| owner.count = self.count;
        }
        fn appendDense(self: *@This(), ordinals: ?[]const usize, first_ordinal: usize, columns: []const []f64, count: usize, direct: bool) !void {
            const start = self.count;
            while (self.rows.len - start < count) try self.grow();
            if (self.paged_owner) |owner| try owner.ensureRows(start + count);
            for (columns, self.columns[0..self.column_count]) |column, target| switch (target.source) {
                .decoded => |values| {
                    const out = values[start..][0..count];
                    // A flat column is borrowed only if its whole batch fits
                    // the metadata capacity, so growth cannot invalidate it.
                    if (out.ptr != column.ptr) @memcpy(out, column[0..count]);
                },
                .paged => |page| {
                    if (direct) {
                        const out = page.owner.window(page.column, start, count);
                        if (out.ptr != column.ptr) @memcpy(out, column[0..count]);
                    } else {
                        var copied: usize = 0;
                        while (copied < count) {
                            const span = @min(count - copied, page.owner.row_capacity - ((start + copied) & (page.owner.row_capacity - 1)));
                            @memcpy(page.owner.window(page.column, start + copied, span), column[copied..][0..span]);
                            copied += span;
                        }
                    }
                },
                .nullable => unreachable,
            };
            if (ordinals) |rows| @memcpy(self.rows[start..][0..count], rows[0..count]) else for (self.rows[start..][0..count], first_ordinal..) |*row, ordinal| row.* = ordinal;
            self.count += count;
            if (self.paged_owner) |owner| owner.count = self.count;
        }
        fn context(self: *const @This()) MetricColumnSortContext {
            return .{ .orders = self.orders, .metric_indexes = self.order_indexes[0..self.orders.len], .views = self.columns[0..self.column_count], .row_ordinals = self.rows };
        }
        fn offer(self: *@This(), ordinal: usize, columns: []const []f64, row: usize) !void {
            if (self.count == self.rows.len and self.count < self.limit) try self.grow();
            const full = self.count == self.rows.len;
            std.debug.assert(!full or self.orders.len != 0);
            const slot = if (full) self.slots[0] else self.count;
            if (full) {
                var better = ordinal < self.rows[slot];
                for (self.orders, self.order_indexes[0..self.orders.len]) |order, index| {
                    const value = columns[index][row];
                    if (compareOptionalMetricScore(if (std.math.isNan(value)) null else value, self.columns[index].at(slot), order)) |less| {
                        better = less;
                        break;
                    }
                }
                if (!better) return;
            }
            self.rows[slot] = ordinal;
            for (columns, self.columns[0..self.column_count]) |column, target| target.source.decoded[slot] = column[row];
            if (!full) {
                self.slots[self.count] = slot;
                self.count += 1;
                siftWorstMetricCandidateUp(self.context(), self.slots[0..self.count], self.count - 1);
            } else siftWorstMetricCandidateDown(self.context(), self.slots, 0);
        }
        fn canBeatKnownPrefix(self: *const @This(), ordinal: usize, columns: []const MetricColumn, row: usize, known: []const usize) bool {
            const worst = self.slots[0];
            for (known, 0..) |index, i| {
                if (compareOptionalMetricScore(columns[index].at(row), self.columns[self.order_indexes[i]].at(worst), self.orders[i])) |better| return better;
            }
            return known.len != self.orders.len or ordinal < self.rows[worst];
        }
        fn finish(self: *@This()) !void {
            if (self.permuted) {
                if (self.slots.len == 0) {
                    self.slots = try self.alloc.alloc(usize, self.count);
                    for (self.slots, 0..) |*slot, i| slot.* = i;
                }
                if (self.paged_owner) |owner| {
                    var offsets: [graph_metric_dependency_limit]usize = undefined;
                    var all_paged = true;
                    for (self.order_indexes[0..self.orders.len], 0..) |index, i| {
                        switch (self.columns[index].source) {
                            .paged => |page| {
                                std.debug.assert(page.owner == owner);
                                offsets[i] = page.column * owner.row_capacity;
                            },
                            else => {
                                all_paged = false;
                                break;
                            },
                        }
                    }
                    if (all_paged) {
                        const sort_context: PagedMetricSortContext = .{
                            .orders = self.orders,
                            .offsets = offsets[0..self.orders.len],
                            .pages = owner.pages.items,
                            .row_shift = owner.row_shift,
                            .row_mask = owner.row_capacity - 1,
                            .row_ordinals = self.rows,
                        };
                        std.mem.sort(usize, self.slots[0..self.count], sort_context, pagedMetricCandidateLessThan);
                        return;
                    }
                }
                std.mem.sort(usize, self.slots[0..self.count], self.context(), metricColumnCandidateLessThan);
            }
        }
        fn takeRows(self: *@This()) []usize {
            if (self.permuted) {
                // The heap slots form a permutation of 0..count. Temporarily
                // mark cycles in its spare high bit, preserving the score map.
                const mark = @as(usize, 1) << (@bitSizeOf(usize) - 1);
                for (0..self.count) |start| {
                    if (self.slots[start] & mark != 0) continue;
                    const saved = self.rows[start];
                    var target = start;
                    while (true) {
                        const source = self.slots[target];
                        self.slots[target] |= mark;
                        if (source == start) {
                            self.rows[target] = saved;
                            break;
                        }
                        self.rows[target] = self.rows[source];
                        target = source;
                    }
                }
                for (self.slots[0..self.count]) |*slot| slot.* &= ~mark;
            }
            const rows = self.rows;
            self.rows = &.{};
            return rows;
        }
    };

    /// A stable source-row selection and only the columns needed by future
    /// stages. The reader is snapshot-owned; this executor never opens storage.
    pub const MetricStageWorkspace = struct {
        const ColumnGroup = struct { references: usize = 0, mapping: ?[]usize = null, paged: ?*ScorePages = null };
        const Slot = struct {
            source: MetricColumn.Source,
            group: u8,
            fn deinit(self: @This(), a: Allocator, groups: *[graph_metric_dependency_limit]ColumnGroup) void {
                switch (self.source) {
                    .nullable => |values| a.free(values),
                    .decoded => |values| a.free(values),
                    .paged => {},
                }
                const group = &groups[self.group];
                group.references -= 1;
                if (group.references == 0) {
                    if (group.mapping) |mapping| a.free(mapping);
                    group.mapping = null;
                    if (group.paged) |owner| owner.destroy();
                    group.paged = null;
                }
            }
            fn column(self: @This(), count: usize, groups: *const [graph_metric_dependency_limit]ColumnGroup) MetricColumn {
                return .{ .source = self.source, .rows = groups[self.group].mapping, .len = count };
            }
        };
        alloc: Allocator,
        plan: MetricReadPlan,
        rows: []usize = &.{},
        row_storage: []usize = &.{},
        identity_count: ?usize = null,
        columns: [graph_metric_dependency_limit]?Slot = @splat(null),
        groups: [graph_metric_dependency_limit]ColumnGroup = @splat(.{}),
        pub fn init(alloc: Allocator, plan: MetricReadPlan, node_count: usize) !@This() {
            return .{ .alloc = alloc, .plan = plan, .identity_count = node_count };
        }
        pub fn rowCount(self: *const @This()) usize {
            return self.identity_count orelse self.rows.len;
        }
        pub fn rowAt(self: *const @This(), i: usize) usize {
            return if (self.identity_count != null) i else self.rows[i];
        }
        fn explicitRows(self: *@This()) ![]usize {
            if (self.identity_count) |count| {
                const rows = try self.alloc.alloc(usize, count);
                for (rows, 0..) |*row, i| row.* = i;
                self.rows = rows;
                self.row_storage = rows;
                self.identity_count = null;
            }
            return self.rows;
        }
        pub fn deinit(self: *@This()) void {
            for (self.columns) |column| if (column) |slot| slot.deinit(self.alloc, &self.groups);
            self.alloc.free(self.row_storage);
        }
        fn index(self: *const @This(), name: []const u8) usize {
            return metricColumnNameIndex(self.plan.dependencies.slice(), name).?;
        }
        pub fn columnsFor(self: *const @This(), names: []const []const u8, out: []MetricColumn) void {
            for (names, out) |name, *column| column.* = self.columns[self.index(name)].?.column(self.rowCount(), &self.groups);
        }
        fn adoptProjectionColumns(self: *@This(), node_alloc: Allocator, output: Allocator, scratch: Allocator, source_budget: *work_budget_mod.RetainedAllocator, target_budget: *work_budget_mod.RetainedAllocator, names: [][]u8, selected: []const usize, nodes: *[]GraphResultNode) !?*GraphMetricColumns {
            if (selected.len > std.math.maxInt(u32)) return error.InvalidRequest;
            if (names.len == 0 or names.len > graph_metric_projection_limit) return null;
            var group_counts: [graph_metric_dependency_limit]usize = @splat(0);
            for (names) |name| {
                const slot = self.columns[self.index(name)] orelse return null;
                if (slot.source == .nullable) return null;
                group_counts[slot.group] += 1;
            }
            for (group_counts, self.groups) |count, group| {
                if (count == 0) continue;
                if (group.references != count) return null;
                // Retain only groups whose backing contains projected columns.
                if (group.paged) |pages| if (pages.columns != count) return null;
            }
            const owner = try output.create(GraphMetricColumns);
            errdefer output.destroy(owner);
            owner.* = .{ .names = names, .rows = selected.len };
            var group_ids: [graph_metric_dependency_limit]u8 = undefined;
            for (group_counts, 0..) |count, group| {
                if (count == 0) continue;
                group_ids[group] = owner.group_count;
                owner.groups[owner.group_count] = .{ .mapping = self.groups[group].mapping, .pages = self.groups[group].paged };
                owner.group_count += 1;
            }
            for (names, 0..) |name, column| {
                const slot = self.columns[self.index(name)].?;
                owner.column_groups[column] = group_ids[slot.group];
                switch (slot.source) {
                    .decoded => |values| owner.decoded[column] = values,
                    .paged => |source| owner.page_columns[column] = source.column,
                    .nullable => unreachable,
                }
            }
            const dense_bytes = try std.math.mul(usize, try std.math.mul(usize, selected.len, names.len), @sizeOf(f64));
            if (owner.backingBytes() > dense_bytes +| (256 * 1024)) {
                output.destroy(owner);
                return null;
            }
            // This is the last fallible operation. Before it completes the
            // workspace owns all backing, and failures leave source nodes intact.
            _ = try materializeMetricColumnsWithAllocators(node_alloc, output, scratch, &.{}, &.{}, selected, nodes);
            source_budget.transferTo(target_budget, owner.backingBytes());
            for (owner.groups[0..owner.group_count]) |group| if (group.pages) |pages| {
                pages.alloc = node_alloc;
            };
            for (names) |name| self.columns[self.index(name)] = null;
            for (group_counts, 0..) |count, group| if (count != 0) {
                self.groups[group] = .{};
            };
            for (nodes.*, 0..) |*node, row| {
                node.metric_columns = owner;
                node.metric_row = @intCast(row);
            }
            return owner;
        }
        pub fn ensure(self: *@This(), reader: anytype, names: []const []const u8, nodes: []const GraphResultNode) !void {
            var missing: MetricReadPlan.Names = .{};
            for (names) |name| if (self.columns[self.index(name)] == null) {
                missing.append(name);
            };
            if (missing.len == 0) return;
            var local_count: usize = 0;
            for (0..self.rowCount()) |i| local_count += @intFromBool(nodes[self.rowAt(i)].table == null);
            const decoded = comptime std.meta.hasMethod(@TypeOf(reader), "readDecodedColumns");
            const Score = if (decoded) f64 else ?f64;
            var columns: [graph_metric_dependency_limit][]Score = undefined;
            var initialized: usize = 0;
            errdefer for (columns[0..initialized]) |column| self.alloc.free(column);
            for (columns[0..missing.len]) |*column| {
                column.* = try self.alloc.alloc(Score, self.rowCount());
                @memset(column.*, if (decoded) std.math.nan(f64) else null);
                initialized += 1;
            }
            var published_names: MetricReadPlan.Names = .{};
            var published_columns: [graph_metric_dependency_limit][]Score = undefined;
            for (missing.slice(), columns[0..missing.len]) |name, column| {
                if (self.plan.personalizationFor(name)) |personalization| {
                    if (comptime std.meta.hasMethod(@TypeOf(reader), "readPersonalizedColumn")) {
                        // A seeded kernel consumes the complete local relation
                        // once; only published reads use bounded gathers.
                        const keys = try self.alloc.alloc([]const u8, local_count);
                        defer self.alloc.free(keys);
                        var local: usize = 0;
                        for (0..self.rowCount()) |i| if (nodes[self.rowAt(i)].table == null) {
                            keys[local] = nodes[self.rowAt(i)].key;
                            local += 1;
                        };
                        const nullable = if (decoded) try self.alloc.alloc(?f64, local_count) else column[0..local_count];
                        defer if (decoded) self.alloc.free(nullable);
                        try reader.readPersonalizedColumn(self.alloc, name, personalization.seed_nodes, personalization.damping, keys, nullable);
                        var target = self.rowCount();
                        local = local_count;
                        while (target != 0) {
                            target -= 1;
                            if (nodes[self.rowAt(target)].table == null) {
                                local -= 1;
                                column[target] = if (decoded) nullable[local] orelse std.math.nan(f64) else nullable[local];
                            } else column[target] = if (decoded) std.math.nan(f64) else null;
                        }
                    } else return error.GraphMetricPersonalizationUnsupported;
                } else {
                    published_columns[published_names.len] = column;
                    published_names.append(name);
                }
            }
            if (published_names.len != 0 and local_count != 0) {
                // Prove local key order first. Already ordered relations need
                // only a bounded gather, rather than an N-row permutation.
                var sorted = true;
                var previous: ?[]const u8 = null;
                for (0..self.rowCount()) |i| if (nodes[self.rowAt(i)].table == null) {
                    const key = nodes[self.rowAt(i)].key;
                    if (previous) |before| if (std.mem.order(u8, before, key) == .gt) {
                        sorted = false;
                        break;
                    };
                    previous = key;
                };
                const rows: []usize = if (sorted) &.{} else try self.alloc.alloc(usize, local_count);
                defer self.alloc.free(rows);
                if (!sorted) {
                    var local: usize = 0;
                    for (0..self.rowCount()) |i| if (nodes[self.rowAt(i)].table == null) {
                        rows[local] = i;
                        local += 1;
                    };
                    const Order = struct {
                        workspace: *const MetricStageWorkspace,
                        nodes: []const GraphResultNode,
                        fn less(ctx: @This(), a: usize, b: usize) bool {
                            const order = std.mem.order(u8, ctx.nodes[ctx.workspace.rowAt(a)].key, ctx.nodes[ctx.workspace.rowAt(b)].key);
                            return order == .lt or (order == .eq and a < b);
                        }
                    };
                    std.mem.sort(usize, rows, Order{ .workspace = self, .nodes = nodes }, Order.less);
                }
                const batch_rows = @min(local_count, 4096 / published_names.len);
                const gather: []usize = if (sorted) try self.alloc.alloc(usize, batch_rows) else &.{};
                defer self.alloc.free(gather);
                var next_row: usize = 0;
                const keys = try self.alloc.alloc([]const u8, batch_rows);
                defer self.alloc.free(keys);
                const scores = try self.alloc.alloc(Score, batch_rows * published_names.len);
                defer self.alloc.free(scores);
                const streamed = comptime std.meta.hasMethod(@TypeOf(reader), "openDecodedColumnStream");
                var stream = if (streamed) try reader.openDecodedColumnStream(self.alloc) else {};
                defer if (streamed) stream.deinit();
                const active = if (streamed) &stream else reader;
                var offset: usize = 0;
                while (offset < local_count) {
                    const count = @min(batch_rows, local_count - offset);
                    const batch_indexes = if (sorted) gather[0..count] else rows[offset..][0..count];
                    if (sorted) for (batch_indexes) |*row| {
                        while (nodes[self.rowAt(next_row)].table != null) next_row += 1;
                        row.* = next_row;
                        next_row += 1;
                    };
                    for (batch_indexes, keys[0..count]) |row, *key| key.* = nodes[self.rowAt(row)].key;
                    var batch: [graph_metric_dependency_limit][]Score = undefined;
                    for (batch[0..published_names.len], 0..) |*column, i| column.* = scores[i * batch_rows ..][0..count];
                    if (decoded) try active.readDecodedColumns(self.alloc, published_names.slice(), keys[0..count], batch[0..published_names.len]) else try active.readColumns(self.alloc, published_names.slice(), keys[0..count], batch[0..published_names.len]);
                    for (published_columns[0..published_names.len], batch[0..published_names.len]) |column, values| {
                        for (batch_indexes, values) |row, value| column[row] = value;
                    }
                    offset += count;
                }
            }
            const group = for (self.groups, 0..) |entry, i| {
                if (entry.references == 0) break @as(u8, @intCast(i));
            } else unreachable;
            self.groups[group] = .{ .references = missing.len };
            for (missing.slice(), columns[0..missing.len]) |name, column| self.columns[self.index(name)] = .{ .group = group, .source = if (decoded) .{ .decoded = column } else .{ .nullable = column } };
        }

        /// Stream published filters and ordering through bounded batches. A
        /// batch owns at most 4096 score cells; retained storage grows only with
        /// survivors, up to K when a response limit applies.
        /// Personalization computes a whole graph kernel and stays staged.
        pub fn selectReading(self: *@This(), reader: anytype, query: GraphQuery, apply_limit: bool, nodes: []const GraphResultNode) !bool {
            if (comptime !std.meta.hasMethod(@TypeOf(reader), "readDecodedColumns")) return false;
            const limited = apply_limit and query.params.max_results != 0;
            if (self.plan.filters.len == 0 and (!limited or self.plan.orders.len == 0)) return false;
            var names: MetricReadPlan.Names = .{};
            for (self.plan.filters.slice()) |name| names.append(name);
            for (self.plan.orders.slice()) |name| names.append(name);
            // Unlimited survivors already pass through globally key-ordered
            // batches. Read published projections here instead of gathering
            // the entire relation again after restoring logical row order.
            if (!limited) for (self.plan.projections.slice()) |name| {
                if (self.plan.personalizationFor(name) == null) names.append(name);
            };
            for (names.slice()) |name| if (self.plan.personalizationFor(name) != null) return false;
            for (self.columns) |column| if (column != null) return false;
            // Sorting consumes the existing identity row buffer, with original
            // node ordinals carried by the heap. Selected/reordered workspaces
            // retain the staged path; rollback restores identity on any failure.
            if (self.rowCount() != nodes.len) return false;
            if (self.identity_count == null) for (self.rows, 0..) |row, ordinal| if (row != ordinal) return false;
            const capacity = if (limited) @min(self.rowCount(), @as(usize, @intCast(query.params.max_results))) else self.rowCount();
            const batch_rows = @as(usize, 1) << std.math.log2_int(usize, 4096 / names.len);
            // Small/dense pages already fit the ordinary warmed plan. Fusion
            // is useful when scores can be discarded instead of retained.
            if (self.rowCount() <= batch_rows or (limited and self.plan.orders.len != 0 and capacity > self.rowCount() / 4)) return false;
            const streamed = comptime std.meta.hasMethod(@TypeOf(reader), "openDecodedColumnStream");
            var stream = if (streamed) try reader.openDecodedColumnStream(self.alloc) else {};
            defer if (streamed) stream.deinit();
            const active_reader = if (streamed) &stream else reader;
            const KeyOrder = struct {
                nodes: []const GraphResultNode,
                fn less(ctx: @This(), left: usize, right: usize) bool {
                    const x = ctx.nodes[left];
                    const y = ctx.nodes[right];
                    if ((x.table == null) != (y.table == null)) return x.table == null;
                    if (x.table != null) return left < right;
                    const cmp = std.mem.order(u8, x.key, y.key);
                    return cmp == .lt or (cmp == .eq and left < right);
                }
            };
            const key_order = KeyOrder{ .nodes = nodes };
            var ascending = true;
            if (!limited or query.order_by.len != 0) for (1..self.rowCount()) |i| if (KeyOrder.less(key_order, self.rowAt(i), self.rowAt(i - 1))) {
                ascending = false;
                break;
            };
            const permuted = query.order_by.len != 0 or (!limited and !ascending);
            // Predicate-only scores die with their batch. The candidate owner
            // retains only ordering values and already-read projected values.
            var retained_names: MetricReadPlan.Names = .{};
            for (self.plan.orders.slice()) |name| retained_names.append(name);
            for (names.slice()) |name| if (metricColumnNameIndex(self.plan.projections.slice(), name) != null) {
                retained_names.append(name);
            };
            var heap = try MetricCandidateHeap.init(self.alloc, @min(capacity, batch_rows), capacity, retained_names.slice(), query.order_by, permuted, !limited, self.plan.projections.slice());
            defer heap.deinit();
            const scores = try self.alloc.alloc(f64, batch_rows * names.len);
            defer self.alloc.free(scores);
            const keys = try self.alloc.alloc([]const u8, batch_rows);
            defer self.alloc.free(keys);
            const positions = try self.alloc.alloc(usize, batch_rows);
            defer self.alloc.free(positions);
            const survivors = try self.alloc.alloc(usize, batch_rows);
            defer self.alloc.free(survivors);
            var filter_indexes: [graph_metric_filter_limit]usize = undefined;
            for (query.where_metric, filter_indexes[0..query.where_metric.len]) |filter, *metric_index| metric_index.* = metricColumnNameIndex(names.slice(), filter.name).?;
            var order_names: MetricReadPlan.Names = .{};
            for (names.slice()) |name| if (metricColumnNameIndex(self.plan.filters.slice(), name) == null) {
                order_names.append(name);
            };
            var known_order_indexes: [graph_metric_order_limit]usize = undefined;
            var known_order_count: usize = 0;
            for (query.order_by) |order| {
                if (metricColumnNameIndex(self.plan.filters.slice(), order.name) == null) break;
                known_order_indexes[known_order_count] = metricColumnNameIndex(names.slice(), order.name).?;
                known_order_count += 1;
            }
            const was_implicit = self.identity_count != null;
            const original_count = self.rowCount();
            var committed = false;
            defer if (!committed) {
                if (was_implicit) {
                    self.alloc.free(self.row_storage);
                    self.row_storage = &.{};
                    self.rows = &.{};
                    self.identity_count = original_count;
                } else for (self.rows, 0..) |*row, ordinal| row.* = ordinal;
            };
            // Limited filter-only results are a logical prefix. Unlimited
            // reads retain global key locality and restore surviving ordinals.
            if (permuted and !ascending) sortMetricReadRows(try self.explicitRows(), nodes);
            var offset: usize = 0;
            while (offset < self.rowCount()) {
                const count = @min(batch_rows, self.rowCount() - offset);
                var direct = false;
                if (!limited) {
                    if (heap.paged_owner) |owner| {
                        direct = count <= owner.row_capacity - (heap.count & (owner.row_capacity - 1));
                        if (direct) try owner.ensureRows(heap.count + count);
                    } else direct = heap.count + count <= heap.rows.len;
                }
                var columns: [graph_metric_dependency_limit][]f64 = undefined;
                var views: [graph_metric_dependency_limit]MetricColumn = undefined;
                for (columns[0..names.len], views[0..names.len], 0..) |*column, *view, i| {
                    column.* = scores[i * batch_rows ..][0..count];
                    if (direct) if (metricColumnNameIndex(retained_names.slice(), names.slice()[i])) |retained_index| {
                        switch (heap.columns[retained_index].source) {
                            .paged => |page| column.* = page.owner.window(page.column, heap.count, count),
                            .decoded => |values| if (heap.count + count <= values.len and heap.count + count <= heap.rows.len) {
                                column.* = values[heap.count..][0..count];
                            },
                            .nullable => unreachable,
                        }
                    };
                    @memset(column.*, std.math.nan(f64));
                    view.* = .{ .source = .{ .decoded = column.* }, .len = count };
                }
                var local_count: usize = 0;
                for (0..count) |row| if (nodes[self.rowAt(offset + row)].table == null) {
                    const source = self.rowAt(offset + row);
                    keys[local_count] = nodes[source].key;
                    positions[local_count] = row;
                    local_count += 1;
                };
                var selected_columns: [graph_metric_dependency_limit][]f64 = undefined;
                if (self.plan.filters.len != 0) {
                    for (self.plan.filters.slice(), selected_columns[0..self.plan.filters.len]) |name, *column| column.* = columns[metricColumnNameIndex(names.slice(), name).?][0..local_count];
                    if (local_count != 0) try active_reader.readDecodedColumns(self.alloc, self.plan.filters.slice(), keys[0..local_count], selected_columns[0..self.plan.filters.len]);
                    // Move local scores to logical positions without another slab.
                    for (self.plan.filters.slice()) |name| {
                        const column = columns[metricColumnNameIndex(names.slice(), name).?];
                        var row = count;
                        var local = local_count;
                        while (row != 0) {
                            row -= 1;
                            if (nodes[self.rowAt(offset + row)].table == null) {
                                local -= 1;
                                column[row] = column[local];
                            } else column[row] = std.math.nan(f64);
                        }
                    }
                }
                var survivor_count: usize = 0;
                local_count = 0;
                for (0..count) |row| {
                    if (!metricCandidatePassesFilters(row, query.where_metric, filter_indexes[0..query.where_metric.len], views[0..names.len])) continue;
                    if (limited and order_names.len != 0 and known_order_count != 0 and heap.count == heap.limit and !heap.canBeatKnownPrefix(self.rowAt(offset + row), views[0..names.len], row, known_order_indexes[0..known_order_count])) continue;
                    survivors[survivor_count] = row;
                    survivor_count += 1;
                    const node = nodes[self.rowAt(offset + row)];
                    if (node.table == null) {
                        keys[local_count] = node.key;
                        positions[local_count] = row;
                        local_count += 1;
                    }
                }
                if (order_names.len != 0) {
                    for (order_names.slice(), selected_columns[0..order_names.len]) |name, *column| column.* = columns[metricColumnNameIndex(names.slice(), name).?][0..local_count];
                    if (local_count != 0) try active_reader.readDecodedColumns(self.alloc, order_names.slice(), keys[0..local_count], selected_columns[0..order_names.len]);
                    for (order_names.slice()) |name| {
                        const column = columns[metricColumnNameIndex(names.slice(), name).?];
                        var local = local_count;
                        while (local != 0) {
                            local -= 1;
                            column[positions[local]] = column[local];
                        }
                        // Compact reads may overwrite unrelated positions. Only
                        // surviving qualified rows are consumed, always as null.
                        for (survivors[0..survivor_count]) |row| if (nodes[self.rowAt(offset + row)].table != null) {
                            column[row] = std.math.nan(f64);
                        };
                    }
                }
                var retained_columns: [graph_metric_dependency_limit][]f64 = undefined;
                for (retained_names.slice(), retained_columns[0..retained_names.len]) |name, *column| column.* = columns[metricColumnNameIndex(names.slice(), name).?];
                if (limited and query.order_by.len != 0) {
                    for (survivors[0..survivor_count]) |row| try heap.offer(self.rowAt(offset + row), retained_columns[0..retained_names.len], row);
                } else if (survivor_count == count and count <= capacity - heap.count) {
                    try heap.appendDense(if (self.identity_count == null) self.rows[offset..][0..count] else null, offset, retained_columns[0..retained_names.len], count, direct);
                } else {
                    for (survivors[0..survivor_count]) |row| {
                        if (heap.count == capacity) break;
                        try heap.append(self.rowAt(offset + row), retained_columns[0..retained_names.len], row, direct);
                    }
                }
                if (query.order_by.len == 0 and heap.count == capacity) break;
                offset += count;
            }
            try heap.finish();
            const row_storage = if (heap.count == 0) @as([]usize, &.{}) else heap.takeRows();
            const rows = row_storage[0..heap.count];
            var retained: [graph_metric_dependency_limit]?Slot = @splat(null);
            var retained_count: usize = 0;
            // Transfer selected score storage and its shared heap permutation.
            // No K-by-column output slabs or score copies are needed at commit.
            for (retained_names.slice(), 0..) |name, i| {
                if (metricColumnNameIndex(self.plan.projections.slice(), name) == null) continue;
                if (heap.count == 0) {
                    switch (heap.columns[i].source) {
                        .decoded => |values| self.alloc.free(values),
                        .paged => {},
                        .nullable => unreachable,
                    }
                    heap.columns[i].source = .{ .decoded = &.{} };
                }
                retained[self.index(name)] = .{ .source = heap.columns[i].source, .group = 0 };
                heap.columns[i].source = .{ .decoded = &.{} };
                retained_count += 1;
            }
            self.alloc.free(self.row_storage);
            self.rows = rows;
            self.row_storage = row_storage;
            self.identity_count = null;
            self.columns = retained;
            self.groups[0] = .{ .references = retained_count };
            if (retained_count != 0 and heap.count != 0) {
                self.groups[0].paged = heap.paged_owner;
                heap.paged_owner = null;
            }
            if (retained_count != 0 and heap.count != 0 and permuted) {
                self.groups[0].mapping = heap.slots;
                heap.slots = &.{};
            }
            committed = true;
            return true;
        }

        fn tryCompactGroup(self: *@This(), group: u8, selected: MetricSelection, keep: *const [graph_metric_dependency_limit]bool, compacted: *[graph_metric_dependency_limit]?MetricColumn.Source) bool {
            var complete = false;
            defer if (!complete) {
                for (self.columns, 0..) |maybe, i| {
                    const slot = maybe orelse continue;
                    if (slot.group != group) continue;
                    if (compacted[i]) |source| switch (source) {
                        .nullable => |values| self.alloc.free(values),
                        .decoded => |values| self.alloc.free(values),
                        .paged => {},
                    };
                    compacted[i] = null;
                }
            };
            for (self.columns, 0..) |maybe, i| {
                const slot = maybe orelse continue;
                if (!keep[i] or slot.group != group) continue;
                const column = slot.column(self.rowCount(), &self.groups);
                compacted[i] = switch (slot.source) {
                    .nullable => blk: {
                        const values = self.alloc.alloc(?f64, selected.count()) catch return false;
                        for (values, 0..) |*value, row| value.* = column.at(selected.at(row));
                        break :blk .{ .nullable = values };
                    },
                    .decoded, .paged => blk: {
                        const values = self.alloc.alloc(f64, selected.count()) catch return false;
                        for (values, 0..) |*value, row| value.* = column.at(selected.at(row)) orelse std.math.nan(f64);
                        break :blk .{ .decoded = values };
                    },
                };
            }
            complete = true;
            return true;
        }

        pub fn select(self: *@This(), query: GraphQuery, apply_limit: bool, names: []const []const u8, future: []const []const u8, later: []const []const u8) !void {
            var columns: [graph_metric_dependency_limit]MetricColumn = undefined;
            self.columnsFor(names, columns[0..names.len]);
            var selected = try selectMetricCandidates(self.alloc, names, columns[0..names.len], query, apply_limit, self.rowCount());
            defer selected.deinit(self.alloc);
            var unchanged = selected.count() == self.rowCount();
            for (0..selected.count()) |row| unchanged = unchanged and selected.at(row) == row;
            const prefix = selected.identity_count != null and self.identity_count != null;
            const borrowed_selection = !unchanged and !prefix and selected.identity_count == null;
            const rows = if (unchanged) self.rows else if (prefix) @as([]usize, &.{}) else if (borrowed_selection) selected.rows else try self.alloc.alloc(usize, selected.count());
            errdefer if (!unchanged and !prefix and !borrowed_selection) self.alloc.free(rows);
            var keep: [graph_metric_dependency_limit]bool = @splat(false);
            var groups: [graph_metric_dependency_limit]u8 = undefined;
            var mappings: [graph_metric_dependency_limit]?[]usize = undefined;
            var count: usize = 0;
            var compacted: [graph_metric_dependency_limit]?MetricColumn.Source = @splat(null);
            errdefer for (compacted) |maybe| if (maybe) |source| switch (source) {
                .nullable => |values| self.alloc.free(values),
                .decoded => |values| self.alloc.free(values),
                .paged => {},
            };
            errdefer for (mappings[0..count]) |mapping| if (mapping) |values| self.alloc.free(values);
            for (self.plan.dependencies.slice(), 0..) |name, i| keep[i] = metricColumnNameIndex(future, name) != null or metricColumnNameIndex(later, name) != null;
            var visited: [graph_metric_dependency_limit]bool = @splat(false);
            var sparse_groups: [graph_metric_dependency_limit]bool = @splat(false);
            for (self.columns[0..self.plan.dependencies.len], 0..) |maybe, i| {
                const slot = maybe orelse continue;
                if (!keep[i] or visited[slot.group]) continue;
                visited[slot.group] = true;
                if (unchanged) continue;
                var identity = true;
                for (0..selected.count()) |row| {
                    const parent = selected.at(row);
                    const physical = if (self.groups[slot.group].mapping) |previous| previous[parent] else parent;
                    identity = identity and physical == row;
                }
                // Reserve the fallback before attempting optional compaction.
                // Partial copied groups are discarded together on allocation
                // denial, preserving the original minimum working budget.
                const mapping: ?[]usize = if (identity) null else try self.alloc.alloc(usize, selected.count());
                const source_len = switch (slot.source) {
                    .nullable => |values| values.len,
                    .decoded => |values| values.len,
                    .paged => |page| page.owner.count,
                };
                const item_size: usize = switch (slot.source) {
                    .nullable => @sizeOf(?f64),
                    .decoded, .paged => @sizeOf(f64),
                };
                const sparse = selected.count() == 0 or (selected.count() <= source_len / 4 and (source_len - selected.count()) * item_size >= 16 * 1024);
                sparse_groups[slot.group] = sparse;
                if (mapping) |values| for (values, 0..) |*row, i_row| {
                    const parent = selected.at(i_row);
                    row.* = if (self.groups[slot.group].mapping) |previous| previous[parent] else parent;
                };
                groups[count] = slot.group;
                mappings[count] = mapping;
                count += 1;
            }
            // Reserve every group's mandatory map before any optional copies:
            // a compacted group must not starve another group's fallback.
            for (groups[0..count], mappings[0..count]) |group, *mapping| {
                if (sparse_groups[group] and self.tryCompactGroup(group, selected, &keep, &compacted)) {
                    if (mapping.*) |values| self.alloc.free(values);
                    mapping.* = null;
                } else sparse_groups[group] = false;
            }
            // Every retained column in a read group has the same source length,
            // so a group either shares one selection map or compacts completely.
            // Commit only after all allocations succeed.
            var reset: [graph_metric_dependency_limit]bool = @splat(false);
            for (compacted, 0..) |maybe, i| if (maybe) |source| {
                const slot = &self.columns[i].?;
                switch (slot.source) {
                    .nullable => |values| self.alloc.free(values),
                    .decoded => |values| self.alloc.free(values),
                    .paged => {},
                }
                slot.source = source;
                if (!reset[slot.group]) {
                    if (self.groups[slot.group].mapping) |previous| self.alloc.free(previous);
                    self.groups[slot.group].mapping = null;
                    reset[slot.group] = true;
                }
            };
            for (groups[0..count], mappings[0..count]) |group, mapping| {
                if (sparse_groups[group]) continue;
                if (self.groups[group].mapping) |previous| self.alloc.free(previous);
                self.groups[group].mapping = mapping;
            }
            for (self.columns[0..self.plan.dependencies.len], keep[0..self.plan.dependencies.len]) |*maybe, retained| if (!retained) {
                if (maybe.*) |slot| slot.deinit(self.alloc, &self.groups);
                maybe.* = null;
            };
            for (groups[0..count]) |group| if (sparse_groups[group]) {
                if (self.groups[group].paged) |owner| owner.destroy();
                self.groups[group].paged = null;
            };
            if (!unchanged) {
                // No fallible operations remain; compose in the selection's
                // storage only after every group is prepared atomically.
                if (!prefix) for (rows, 0..) |*row, i| {
                    row.* = self.rowAt(selected.at(i));
                };
                if (borrowed_selection) selected.rows = &.{};
                self.alloc.free(self.row_storage);
                self.rows = rows;
                self.row_storage = rows;
                self.identity_count = if (prefix) selected.count() else null;
            }
        }
    };

    fn executeTraverse(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !GraphQueryResult {
        if (algebraicTraversalConsidered(graph_index, params)) {
            graph_index.noteAlgebraicTraversalAttempt();
            const proof = algebraicTraversalProof(graph_index, params);
            if (proof.safe()) {
                if (try self.executeAlgebraicTraverse(graph_index, params, start_keys, target_keys)) |result| {
                    graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                    return result;
                }
                graph_index.noteAlgebraicTraversalFallback();
            } else {
                graph_index.noteAlgebraicTraversalRejected();
            }
        }

        var target_set = std.StringHashMapUnmanaged(void).empty;
        defer target_set.deinit(self.alloc);
        for (target_keys) |key| try target_set.put(self.alloc, key, {});

        var seen = node_identity.Map(void){};
        defer seen.deinit(self.alloc);
        var result_admission_context = TraverseResultAdmissionContext{
            .alloc = self.alloc,
            .target_keys = &target_set,
            .seen = if (params.deduplicate) &seen else null,
        };
        var rules = traversal_mod.TraversalRules{
            .edge_types = params.edge_types,
            .edge_filter = params.edge_filter,
            .direction = params.direction,
            .max_depth = params.max_depth,
            .min_weight = params.min_weight,
            .max_weight = params.max_weight,
            .max_results = params.max_results,
            .deduplicate = params.deduplicate,
            .include_paths = params.include_paths,
            .node_admission = self.node_admission,
            .work_budget = self.work_budget,
            .ttl_now_ns = self.ttl_now_ns,
            .owning_table = self.scope.owning_table,
            .expand_cross_table_local = self.scope.expand_cross_table_local,
            .result_admission = .{
                .ctx = &result_admission_context,
                .admit_one = TraverseResultAdmissionContext.admit,
            },
        };
        const admitted_starts = try self.admittedStartKeysAlloc(start_keys, params.direction);
        defer if (admitted_starts) |mask| self.alloc.free(mask);

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        var cleanup_results = true;
        defer if (cleanup_results) {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        };

        for (start_keys, 0..) |key, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            if (params.max_results > 0) {
                rules.max_results = params.max_results - @as(u32, @intCast(all_results.items.len));
            }
            const trav_results = try traversal_mod.traverse(self.alloc, graph_index, key, rules);
            defer traversal_mod.freeOwnedResults(self.alloc, trav_results);

            for (trav_results) |*tr| {
                var result_node = try takeTraversalResultNode(tr, self.work_budget);
                errdefer result_node.deinit(self.alloc);
                try all_results.append(self.alloc, result_node);

                if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
            }
            if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        cleanup_results = false;
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeAlgebraicTraverse(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !?GraphQueryResult {
        if (params.edge_filter.active()) return null;
        if (!algebraicTraversalProof(graph_index, params).safe()) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, params, target_keys)) return null;

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        var cleanup_results = true;
        defer if (cleanup_results) {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        };

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |k| self.alloc.free(k.*);
            seen.deinit(self.alloc);
        }

        const admitted_starts = try self.admittedStartKeysAlloc(start_keys, params.direction);
        defer if (admitted_starts) |mask| self.alloc.free(mask);
        if (self.node_admission != null and admitted_starts == null) return null;

        for (start_keys, 0..) |start_key, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, params, self.work_budget);
            defer algebraic_edges.deinit(self.alloc);
            // Algebraic reachability currently keys tensor vertices by document
            // key alone. A cross-table endpoint introduces a distinct namespace,
            // so preserve exact identity by falling back to the table-aware BFS.
            if (algebraic_edges.has_cross_table_edges) return null;
            if (self.node_admission) |admission| {
                try filterAlgebraicReachabilityEdgesWithAdmission(
                    self.alloc,
                    &algebraic_edges,
                    admission,
                );
            }

            const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(self.alloc, start_key, algebraic_edges.items, params.max_depth, .{
                .target_nodes = target_keys,
                .work_budget = self.work_budget,
            });
            defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

            for (reached) |item| {
                if (seen.contains(item.node)) continue;
                const node = if (params.include_paths)
                    (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, params, start_key, item.node, item)) orelse return null
                else
                    try algebraicTraversalResultNodeAlloc(self.alloc, item);
                var node_owned = true;
                errdefer if (node_owned) freeResultNode(self.alloc, node);
                try all_results.append(self.alloc, node);
                node_owned = false;
                try seen.put(self.alloc, try self.alloc.dupe(u8, item.node), {});
                if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
            }
            if (params.max_results > 0 and all_results.items.len >= params.max_results) break;
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        cleanup_results = false;
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeShortestPath(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        const target_keys = resolveTargetKeys(gq);
        if (algebraicTraversalConsidered(graph_index, gq.params)) {
            graph_index.noteAlgebraicTraversalAttempt();
            if (try self.executeAlgebraicShortestPath(graph_index, gq.params, start_keys, target_keys)) |result| {
                graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                return result;
            }
            if (algebraicTraversalProof(graph_index, gq.params).safe()) {
                graph_index.noteAlgebraicTraversalFallback();
            } else {
                graph_index.noteAlgebraicTraversalRejected();
            }
        }

        const opts = paths_mod.PathFindOptions{
            .weight_mode = gq.params.weight_mode,
            .ttl_now_ns = self.ttl_now_ns,
            .edge_types = gq.params.edge_types,
            .edge_filter = gq.params.edge_filter,
            .direction = gq.params.direction,
            .max_depth = gq.params.max_depth,
            .min_weight = gq.params.min_weight,
            .max_weight = gq.params.max_weight,
            .node_admission = self.node_admission,
            .work_budget = self.work_budget,
            .owning_table = self.scope.owning_table,
            .expand_cross_table_local = self.scope.expand_cross_table_local,
        };
        const admitted_starts = try self.admittedStartKeysAlloc(start_keys, gq.params.direction);
        defer if (admitted_starts) |mask| self.alloc.free(mask);

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        errdefer {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        }

        outer: for (start_keys, 0..) |sk, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            for (target_keys) |tk| {
                const path = try paths_mod.findShortestPath(self.alloc, graph_index, sk, tk, opts);
                if (path) |p| {
                    defer paths_mod.freePath(self.alloc, p);
                    var node = try pathToResultNodeRetained(self.alloc, &p, self.work_budget);
                    var node_owned = true;
                    errdefer if (node_owned) node.deinit(self.alloc);
                    try all_results.append(self.alloc, node);
                    node_owned = false;
                    if (gq.params.max_results != 0 and all_results.items.len >= gq.params.max_results) break :outer;
                }
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeAlgebraicShortestPath(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        params: QueryParams,
        start_keys: []const []const u8,
        target_keys: []const []const u8,
    ) !?GraphQueryResult {
        if (params.edge_filter.active()) return null;
        if (!(params.algebraic_semiring or graph_index.supportsAlgebraicSemiringTraversal())) return null;
        if (params.weight_mode != .min_hops) return null;
        if (params.max_depth == 0) return null;
        if (target_keys.len == 0) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, params, target_keys)) return null;

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        var cleanup_results = true;
        defer if (cleanup_results) {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        };

        const admitted_starts = try self.admittedStartKeysAlloc(start_keys, params.direction);
        defer if (admitted_starts) |mask| self.alloc.free(mask);
        if (self.node_admission != null and admitted_starts == null) return null;

        outer: for (start_keys, 0..) |start_key, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            for (target_keys) |target_key| {
                if (std.mem.eql(u8, start_key, target_key)) {
                    try all_results.append(self.alloc, try trivialPathResultNode(self.alloc, start_key));
                    if (params.max_results > 0 and all_results.items.len >= params.max_results) break :outer;
                    continue;
                }

                var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, params, self.work_budget);
                defer algebraic_edges.deinit(self.alloc);
                // Tensor vertex names are key-only; cross-table endpoints must
                // use the exact table-aware path implementation below.
                if (algebraic_edges.has_cross_table_edges) return null;
                if (self.node_admission) |admission| {
                    try filterAlgebraicReachabilityEdgesWithAdmission(
                        self.alloc,
                        &algebraic_edges,
                        admission,
                    );
                }

                const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(
                    self.alloc,
                    start_key,
                    algebraic_edges.items,
                    params.max_depth,
                    .{
                        .target_nodes = &.{target_key},
                        .work_budget = self.work_budget,
                    },
                );
                defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

                if (reached.len == 0) continue;
                if (reached.len != 1) return null;
                const node = (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, params, start_key, target_key, reached[0])) orelse return null;
                try all_results.append(self.alloc, node);
                if (params.max_results > 0 and all_results.items.len >= params.max_results) break :outer;
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        cleanup_results = false;
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executeKShortestPaths(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        const target_keys = resolveTargetKeys(gq);
        if (gq.k == 1) {
            if (try self.executeAlgebraicShortestPath(graph_index, gq.params, start_keys, target_keys)) |result| return result;
        }

        const opts = paths_mod.PathFindOptions{
            .weight_mode = gq.params.weight_mode,
            .ttl_now_ns = self.ttl_now_ns,
            .edge_types = gq.params.edge_types,
            .edge_filter = gq.params.edge_filter,
            .direction = gq.params.direction,
            .max_depth = gq.params.max_depth,
            .min_weight = gq.params.min_weight,
            .max_weight = gq.params.max_weight,
            .node_admission = self.node_admission,
            .work_budget = self.work_budget,
            .owning_table = self.scope.owning_table,
            .expand_cross_table_local = self.scope.expand_cross_table_local,
        };
        const admitted_starts = try self.admittedStartKeysAlloc(start_keys, gq.params.direction);
        defer if (admitted_starts) |mask| self.alloc.free(mask);

        var all_results = std.ArrayListUnmanaged(GraphResultNode).empty;
        errdefer {
            for (all_results.items) |node| freeResultNode(self.alloc, node);
            all_results.deinit(self.alloc);
        }

        outer: for (start_keys, 0..) |sk, start_index| {
            if (admitted_starts) |mask| if (!mask[start_index]) continue;
            for (target_keys) |tk| {
                // Limit search work as well as output allocation. Metric
                // filtering/order supplies its larger candidate bound here.
                const remaining: u32 = if (gq.params.max_results == 0) gq.k else @intCast(gq.params.max_results - all_results.items.len);
                const found = try paths_mod.findKShortestPaths(self.alloc, graph_index, sk, tk, @min(gq.k, remaining), opts);
                defer paths_mod.freePaths(self.alloc, found);

                for (found) |p| {
                    var node = try pathToResultNodeRetained(self.alloc, &p, self.work_budget);
                    var node_owned = true;
                    errdefer if (node_owned) node.deinit(self.alloc);
                    try all_results.append(self.alloc, node);
                    node_owned = false;
                    if (gq.params.max_results != 0 and all_results.items.len >= gq.params.max_results) break :outer;
                }
            }
        }

        const owned = try self.alloc.dupe(GraphResultNode, all_results.items);
        all_results.deinit(self.alloc);
        return .{ .nodes = owned };
    }

    fn executePattern(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !GraphQueryResult {
        if (self.node_admission == null) {
            if (algebraicPatternPlan(gq)) |plan_for_status| {
                if (algebraicTraversalConsidered(graph_index, plan_for_status.params)) {
                    graph_index.noteAlgebraicTraversalAttempt();
                    if (try self.executeAlgebraicPattern(graph_index, gq, start_keys)) |result| {
                        graph_index.noteAlgebraicTraversalProven(result.nodes.len);
                        return result;
                    }
                    if (algebraicTraversalProof(graph_index, plan_for_status.params).safe()) {
                        graph_index.noteAlgebraicTraversalFallback();
                    } else {
                        graph_index.noteAlgebraicTraversalRejected();
                    }
                }
            }
        }

        const matches = try pattern_mod.matchPattern(
            self.alloc,
            graph_index,
            start_keys,
            gq.pattern,
            .{
                .max_results = gq.params.max_results,
                .return_aliases = gq.return_aliases,
                .node_admission = self.node_admission,
                .work_budget = self.work_budget,
                .ttl_now_ns = self.ttl_now_ns,
                .owning_table = self.scope.owning_table,
                .expand_cross_table_local = self.scope.expand_cross_table_local,
            },
        );
        errdefer pattern_mod.freeMatches(self.alloc, matches);

        const owned_nodes = try collectUniqueNodesFromMatches(self.alloc, matches);
        return .{ .nodes = owned_nodes, .matches = matches };
    }

    fn admittedStartKeysAlloc(
        self: *GraphQueryEngine,
        start_keys: []const []const u8,
        direction: graph_mod.EdgeDirection,
    ) !?[]bool {
        const admission = self.node_admission orelse return null;
        // A document-model graph may still contain resolver-produced
        // cross-table targets identified only by edge metadata. Let the graph
        // algorithm classify reverse and bidirectional starts in that case.
        // Bidirectional starts are role-ambiguous even for a statically
        // external target model, so classify them against actual edges.
        if (direction == .both or
            (direction == .in and !admission.external_targets))
        {
            return null;
        }
        return try admission.filterKeysAlloc(
            self.alloc,
            start_keys,
            admission.external_targets and direction == .in,
        );
    }

    fn executeAlgebraicPattern(
        self: *GraphQueryEngine,
        graph_index: *graph_mod.GraphIndex,
        gq: GraphQuery,
        start_keys: []const []const u8,
    ) !?GraphQueryResult {
        const plan = algebraicPatternPlan(gq) orelse return null;
        if (!algebraicTraversalProof(graph_index, plan.params).safe()) return null;
        if (!try algebraicTraversalTensorProgramAccepted(self.alloc, graph_index, plan.params, &.{})) return null;

        var matches = std.ArrayListUnmanaged(pattern_mod.PatternMatch).empty;
        var matches_owned = true;
        defer if (matches_owned) {
            for (matches.items) |*match| match.deinit(self.alloc);
            matches.deinit(self.alloc);
        };

        for (start_keys) |start_key| {
            if (!graphQueryPassesPrefixFilter(start_key, gq.pattern[0].node_filter)) continue;

            var algebraic_edges = try collectAlgebraicReachabilityEdges(self.alloc, graph_index, start_key, plan.params, self.work_budget);
            defer algebraic_edges.deinit(self.alloc);
            if (algebraic_edges.has_cross_table_edges) return null;

            const reached = try algebraic_path_mod.boundedReachabilityWithOptionsAlloc(self.alloc, start_key, algebraic_edges.items, plan.depth, .{
                .work_budget = self.work_budget,
            });
            defer algebraic_path_mod.deinitPathResults(self.alloc, reached);

            for (reached) |item| {
                if (item.depth != plan.depth) continue;
                const node = (try algebraicShortestPathResultNodeAlloc(self.alloc, graph_index, plan.params, start_key, item.node, item)) orelse return null;
                defer freeResultNode(self.alloc, node);

                var match = (try algebraicPatternMatchFromNodeAlloc(self.alloc, gq.pattern, gq.return_aliases, node)) orelse return null;
                var match_owned = true;
                errdefer if (match_owned) match.deinit(self.alloc);
                try matches.append(self.alloc, match);
                match_owned = false;
                if (gq.params.max_results > 0 and matches.items.len >= gq.params.max_results) break;
            }
            if (gq.params.max_results > 0 and matches.items.len >= gq.params.max_results) break;
        }

        const owned_matches = try self.alloc.dupe(pattern_mod.PatternMatch, matches.items);
        matches_owned = false;
        matches.deinit(self.alloc);
        errdefer pattern_mod.freeMatches(self.alloc, owned_matches);
        const owned_nodes = try collectUniqueNodesFromMatches(self.alloc, owned_matches);
        return .{ .nodes = owned_nodes, .matches = owned_matches };
    }
};

/// Collect unique node keys from pattern match bindings into owned GraphResultNodes.
pub fn collectUniqueNodesFromMatches(
    alloc: Allocator,
    matches: []const pattern_mod.PatternMatch,
) ![]GraphResultNode {
    var seen = node_identity.Map(void){};
    defer seen.deinit(alloc);

    var nodes = std.ArrayListUnmanaged(GraphResultNode).empty;
    errdefer {
        for (nodes.items) |*n| n.deinit(alloc);
        nodes.deinit(alloc);
    }

    for (matches) |m| {
        for (m.bindings) |binding| {
            if (!try seen.putIfAbsent(
                alloc,
                .{ .table = binding.table, .key = binding.key },
                {},
            )) continue;
            const key = try alloc.dupe(u8, binding.key);
            errdefer alloc.free(key);
            const table = if (binding.table) |table_name|
                try alloc.dupe(u8, table_name)
            else
                null;
            errdefer if (table) |table_name| alloc.free(table_name);
            try nodes.append(alloc, .{
                .key = key,
                .depth = binding.depth,
                .distance = 0,
                .path = null,
                .path_edges = null,
                .table = table,
            });
        }
    }

    const owned = try alloc.dupe(GraphResultNode, nodes.items);
    nodes.deinit(alloc);
    return owned;
}

/// Move owned traversal payloads instead of cloning every metric candidate.
/// Budget credit moves with the payload; no double reservation is required.
fn takeTraversalResultNode(result: *traversal_mod.TraversalResult, work_budget: ?*work_budget_mod.WorkBudget) !GraphResultNode {
    const bytes = if (work_budget != null) try traversalGraphResultNodeOwnedBytes(result.*) else 0;
    const same_budget = work_budget != null and work_budget == result.retained_budget;
    if (work_budget) |budget| {
        const previous = if (same_budget) result.retained_state_bytes else 0;
        try budget.retainStateBytes(bytes -| previous);
        if (previous > bytes) budget.releaseStateBytes(previous - bytes);
    }
    if (!same_budget) if (result.retained_budget) |budget| budget.releaseStateBytes(result.retained_state_bytes);
    const node = GraphResultNode{ .key = result.key, .table = result.target_table, .depth = result.depth, .distance = result.distance, .path = result.path, .path_edges = result.path_edges };
    result.key = &.{};
    result.target_table = null;
    result.path = null;
    result.path_edges = null;
    result.retained_budget = null;
    result.retained_state_bytes = 0;
    return node;
}

fn traversalGraphResultNodeOwnedBytes(result: traversal_mod.TraversalResult) !usize {
    var total = try std.math.add(usize, @sizeOf(GraphResultNode), result.key.len);
    if (result.target_table) |table| total = try std.math.add(usize, total, table.len);
    if (result.path) |items| {
        total = try std.math.add(usize, total, try std.math.mul(usize, items.len, @sizeOf([]const u8)));
        for (items) |item| total = try std.math.add(usize, total, item.len);
    }
    if (result.path_edges) |edges| for (edges) |edge| {
        total = try std.math.add(usize, total, paths_mod.pathEdgeOwnedBytes(edge));
    };
    return total;
}

fn clonePathNodesAlloc(alloc: Allocator, source: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, source.len);
    var initialized: usize = 0;
    errdefer freePathNodeItems(alloc, out, initialized);
    for (source, 0..) |node, i| {
        out[i] = try alloc.dupe(u8, node);
        initialized += 1;
    }
    return out;
}

const AlgebraicPatternPlan = struct {
    params: QueryParams,
    depth: u32,
};

fn algebraicPatternPlan(gq: GraphQuery) ?AlgebraicPatternPlan {
    if (gq.pattern.len < 2) return null;
    if (gq.target_nodes != null) return null;
    if (gq.params.weight_mode != .min_hops) return null;
    if (!gq.params.deduplicate) return null;
    if (gq.pattern[0].node_filter.filter_query_json != null) return null;
    if (!algebraicPatternAliasesUnique(gq.pattern)) return null;

    const first_edge = gq.pattern[1].edge;
    if (!algebraicPatternStepIsOneHop(first_edge) or first_edge.edge_filter.active()) return null;
    if (gq.pattern[1].node_filter.filter_query_json != null) return null;
    for (gq.pattern[2..]) |step| {
        if (step.node_filter.filter_query_json != null) return null;
        if (!algebraicPatternStepIsOneHop(step.edge)) return null;
        if (step.edge.edge_filter.active()) return null;
        if (step.edge.direction != first_edge.direction) return null;
        if (step.edge.min_weight != first_edge.min_weight or step.edge.max_weight != first_edge.max_weight) return null;
        if (!stringSlicesEqual(step.edge.types, first_edge.types)) return null;
    }

    const depth = std.math.cast(u32, gq.pattern.len - 1) orelse return null;
    return .{
        .params = .{
            .edge_types = first_edge.types,
            .direction = first_edge.direction,
            .max_depth = depth,
            .max_results = gq.params.max_results,
            .min_weight = first_edge.min_weight,
            .max_weight = first_edge.max_weight,
            .deduplicate = true,
            .include_paths = true,
            .weight_mode = .min_hops,
            .algebraic_semiring = gq.params.algebraic_semiring,
        },
        .depth = depth,
    };
}

fn algebraicPatternStepIsOneHop(edge: pattern_mod.PatternEdgeStep) bool {
    const min_hops = if (edge.min_hops == 0) @as(u32, 1) else edge.min_hops;
    const max_hops = if (edge.max_hops == 0) @as(u32, 1) else edge.max_hops;
    return min_hops == 1 and max_hops == 1;
}

fn algebraicPatternAliasesUnique(pattern: []const pattern_mod.PatternStep) bool {
    var left_buf: [32]u8 = undefined;
    var right_buf: [32]u8 = undefined;
    for (pattern, 0..) |left, i| {
        const left_alias = graphQueryEffectiveAlias(left.alias, i, &left_buf);
        for (pattern[i + 1 ..], i + 1..) |right, j| {
            const right_alias = graphQueryEffectiveAlias(right.alias, j, &right_buf);
            if (std.mem.eql(u8, left_alias, right_alias)) return false;
        }
    }
    return true;
}

fn stringSlicesEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |a, b| {
        if (!std.mem.eql(u8, a, b)) return false;
    }
    return true;
}

fn graphQueryEffectiveAlias(alias: []const u8, step_idx: usize, buf: []u8) []const u8 {
    if (alias.len > 0) return alias;
    return std.fmt.bufPrint(buf, "_step{}", .{step_idx}) catch "_step";
}

fn graphQueryPassesPrefixFilter(key: []const u8, filter: pattern_mod.NodeFilter) bool {
    return filter.filter_prefix.len == 0 or std.mem.startsWith(u8, key, filter.filter_prefix);
}

fn algebraicPatternMatchFromNodeAlloc(
    alloc: Allocator,
    pattern: []const pattern_mod.PatternStep,
    return_aliases: []const []const u8,
    node: GraphResultNode,
) !?pattern_mod.PatternMatch {
    const node_path = node.path orelse return null;
    const edge_path = node.path_edges orelse return null;
    if (node_path.len != pattern.len or edge_path.len + 1 != node_path.len) return null;

    var all_bindings = try alloc.alloc(pattern_mod.PatternBinding, pattern.len);
    var initialized: usize = 0;
    defer {
        for (all_bindings[0..initialized]) |*binding| binding.deinit(alloc);
        if (all_bindings.len > 0) alloc.free(all_bindings);
    }

    var table_scratch = traversal_mod.MetadataScratch.init(alloc, null);
    defer table_scratch.deinit();
    for (pattern, 0..) |step, i| {
        if (!graphQueryPassesPrefixFilter(node_path[i], step.node_filter)) return null;
        var alias_buf: [32]u8 = undefined;
        const alias = graphQueryEffectiveAlias(step.alias, i, &alias_buf);
        const table = if (i == 0)
            null
        else if (std.mem.eql(u8, edge_path[i - 1].target, node_path[i]))
            try traversal_mod.metadataTargetTable(&table_scratch, edge_path[i - 1].metadata)
        else
            try traversal_mod.metadataSourceTable(&table_scratch, edge_path[i - 1].metadata);
        all_bindings[i] = try clonePatternBindingAlloc(
            alloc,
            alias,
            node_path[i],
            table,
            std.math.cast(u32, i) orelse return null,
        );
        initialized += 1;
    }

    const filtered_bindings = try graphQueryFilterBindings(alloc, all_bindings, return_aliases);
    errdefer {
        for (filtered_bindings) |*binding| binding.deinit(alloc);
        if (filtered_bindings.len > 0) alloc.free(filtered_bindings);
    }
    return .{
        .bindings = filtered_bindings,
        .path = try clonePatternPathEdgesFromInfoAlloc(alloc, edge_path),
    };
}

fn graphQueryFilterBindings(
    alloc: Allocator,
    bindings: []const pattern_mod.PatternBinding,
    requested: []const []const u8,
) ![]pattern_mod.PatternBinding {
    var count: usize = 0;
    for (bindings) |binding| {
        if (graphQueryShouldReturnAlias(binding.alias, requested)) count += 1;
    }
    const filtered = try alloc.alloc(pattern_mod.PatternBinding, count);
    var out_idx: usize = 0;
    errdefer {
        for (filtered[0..out_idx]) |*binding| binding.deinit(alloc);
        if (filtered.len > 0) alloc.free(filtered);
    }
    for (bindings) |binding| {
        if (!graphQueryShouldReturnAlias(binding.alias, requested)) continue;
        filtered[out_idx] = try clonePatternBindingAlloc(
            alloc,
            binding.alias,
            binding.key,
            binding.table,
            binding.depth,
        );
        out_idx += 1;
    }
    return filtered;
}

fn clonePatternBindingAlloc(
    alloc: Allocator,
    alias: []const u8,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
) !pattern_mod.PatternBinding {
    const owned_alias = try alloc.dupe(u8, alias);
    errdefer alloc.free(owned_alias);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |table_name| try alloc.dupe(u8, table_name) else null;
    errdefer if (owned_table) |table_name| alloc.free(table_name);
    return .{
        .alias = owned_alias,
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
    };
}

fn graphQueryShouldReturnAlias(alias: []const u8, requested: []const []const u8) bool {
    if (requested.len == 0) return true;
    for (requested) |item| {
        if (std.mem.eql(u8, item, alias)) return true;
    }
    return false;
}

fn clonePatternPathEdgesFromInfoAlloc(alloc: Allocator, edges: []const PathEdgeInfo) ![]paths_mod.PathEdge {
    const out = try alloc.alloc(paths_mod.PathEdge, edges.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphPatternPathEdgeItems(alloc, out[0..initialized]);
        if (out.len > 0) alloc.free(out);
    }
    for (edges, 0..) |edge, i| {
        out[i] = try paths_mod.clonePathEdge(alloc, edge);
        initialized += 1;
    }
    return out;
}

fn freeGraphPatternPathEdgeItems(alloc: Allocator, edges: []const paths_mod.PathEdge) void {
    for (edges) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.edge_id.len > 0) alloc.free(edge.edge_id);
        if (edge.owner_document.len > 0) alloc.free(edge.owner_document);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }
}

fn resolveTargetKeys(gq: GraphQuery) []const []const u8 {
    if (gq.target_nodes) |tn| {
        switch (tn) {
            .keys => |k| return k,
            .identities => return &.{}, // cross-table identities require the distributed executor
            .result_ref => return &.{}, // caller should have resolved
        }
    }
    return &.{};
}

pub fn pathToResultNode(alloc: Allocator, path: *const paths_mod.Path) !GraphResultNode {
    return pathToResultNodeRetained(alloc, path, null);
}

fn pathToResultNodeRetained(
    alloc: Allocator,
    path: *const paths_mod.Path,
    work_budget: ?*work_budget_mod.WorkBudget,
) !GraphResultNode {
    if (work_budget) |budget| {
        const retained_bytes = pathGraphResultNodeOwnedBytes(path) catch
            return budget.exhaust(.retained_state_bytes, budget.max_retained_state_bytes);
        try budget.retainStateBytes(retained_bytes);
    }
    // Target is last node
    const target_key = if (path.nodes.len > 0) path.nodes[path.nodes.len - 1] else "";

    // Copy path nodes
    const path_nodes = try alloc.alloc([]const u8, path.nodes.len);
    var initialized_nodes: usize = 0;
    errdefer {
        for (path_nodes[0..initialized_nodes]) |node| alloc.free(node);
        if (path_nodes.len > 0) alloc.free(path_nodes);
    }
    for (path.nodes, 0..) |n, i| {
        path_nodes[i] = try alloc.dupe(u8, n);
        initialized_nodes += 1;
    }
    const path_tables = if (path.node_tables.len > 0)
        try clonePathTables(alloc, path.node_tables)
    else
        null;
    errdefer if (path_tables) |tables| freePathTables(alloc, tables);

    // Copy path edges
    const path_edges = try alloc.alloc(PathEdgeInfo, path.edges.len);
    var initialized_edges: usize = 0;
    errdefer {
        for (path_edges[0..initialized_edges]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.edge_id.len > 0) alloc.free(edge.edge_id);
            if (edge.owner_document.len > 0) alloc.free(edge.owner_document);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        if (path_edges.len > 0) alloc.free(path_edges);
    }
    for (path.edges, 0..) |e, i| {
        path_edges[i] = try pathEdgeInfoFromPathEdge(alloc, e);
        initialized_edges += 1;
    }

    const key = try alloc.dupe(u8, target_key);
    errdefer alloc.free(key);
    const target_table = if (path.node_tables.len > 0) path.node_tables[path.node_tables.len - 1] else null;
    const table = if (target_table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (table) |value| alloc.free(value);
    return .{
        .key = key,
        .table = table,
        .depth = path.length,
        .distance = path.total_weight,
        .path = path_nodes,
        .path_tables = path_tables,
        .path_edges = path_edges,
    };
}

fn pathGraphResultNodeOwnedBytes(path: *const paths_mod.Path) !usize {
    const target_key = if (path.nodes.len > 0) path.nodes[path.nodes.len - 1] else "";
    var total = try std.math.add(usize, @sizeOf(GraphResultNode), target_key.len);
    total = try std.math.add(usize, total, try std.math.mul(usize, path.nodes.len, @sizeOf([]const u8)));
    for (path.nodes) |node| total = try std.math.add(usize, total, node.len);
    if (path.node_tables.len > 0) {
        total = try std.math.add(usize, total, try std.math.mul(usize, path.node_tables.len, @sizeOf(?[]const u8)));
        for (path.node_tables) |table| if (table) |value| {
            total = try std.math.add(usize, total, value.len);
        };
        if (path.node_tables[path.node_tables.len - 1]) |table|
            total = try std.math.add(usize, total, table.len);
    }
    total = try std.math.add(usize, total, try std.math.mul(usize, path.edges.len, @sizeOf(PathEdgeInfo)));
    for (path.edges) |edge| {
        for ([_][]const u8{ edge.source, edge.target, edge.edge_type, edge.edge_id, edge.owner_document, edge.metadata }) |part|
            total = try std.math.add(usize, total, part.len);
    }
    return total;
}

fn pathTablesFromTerminalAlloc(
    alloc: Allocator,
    path_len: usize,
    terminal_table: ?[]const u8,
) !?[]const ?[]const u8 {
    const table = terminal_table orelse return null;
    if (path_len == 0) return null;
    const out = try alloc.alloc(?[]const u8, path_len);
    @memset(out, null);
    errdefer alloc.free(out);
    out[path_len - 1] = try alloc.dupe(u8, table);
    return out;
}

fn clonePathTables(alloc: Allocator, source: []const ?[]const u8) ![]const ?[]const u8 {
    const out = try alloc.alloc(?[]const u8, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |table| if (table) |value| alloc.free(value);
        alloc.free(out);
    }
    for (source, 0..) |table, i| {
        out[i] = if (table) |value| try alloc.dupe(u8, value) else null;
        initialized += 1;
    }
    return out;
}

fn freePathTables(alloc: Allocator, tables: []const ?[]const u8) void {
    for (tables) |table| if (table) |value| alloc.free(value);
    alloc.free(tables);
}

fn pathEdgeInfoFromPathEdge(alloc: Allocator, edge: paths_mod.PathEdge) !PathEdgeInfo {
    const source = try alloc.dupe(u8, edge.source);
    errdefer alloc.free(source);
    const target = try alloc.dupe(u8, edge.target);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, edge.edge_type);
    errdefer alloc.free(edge_type);
    const edge_id = try alloc.dupe(u8, edge.edge_id);
    errdefer alloc.free(edge_id);
    const owner_document = try alloc.dupe(u8, edge.owner_document);
    errdefer alloc.free(owner_document);
    const metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "";
    return .{
        .source = source,
        .target = target,
        .edge_type = edge_type,
        .edge_id = edge_id,
        .owner_document = owner_document,
        .weight = edge.weight,
        .metadata = metadata,
        .traversal_direction = edge.traversal_direction,
    };
}

fn trivialPathResultNode(alloc: Allocator, key: []const u8) !GraphResultNode {
    const nodes = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(nodes);
    nodes[0] = try alloc.dupe(u8, key);
    errdefer alloc.free(nodes[0]);
    return .{
        .key = try alloc.dupe(u8, key),
        .depth = 0,
        .distance = 0,
        .path = nodes,
        .path_edges = try alloc.alloc(PathEdgeInfo, 0),
        .provenance = null,
    };
}

fn algebraicTraversalResultNodeAlloc(
    alloc: Allocator,
    result: algebraic_path_mod.PathResult,
) !GraphResultNode {
    const key = try alloc.dupe(u8, result.node);
    errdefer alloc.free(key);
    const provenance = try algebraic_path_mod.provenanceLabelsAlloc(alloc, result.provenance);
    errdefer freeProvenanceLabels(alloc, provenance);
    for (provenance) |*label| if (provenanceLabelNeedsEncoding(label.*)) {
        const text = try publicProvenanceLabelAlloc(alloc, label.*);
        alloc.free(label.*);
        label.* = text;
    };
    return .{
        .key = key,
        .depth = result.depth,
        .distance = @floatFromInt(result.depth),
        .path = null,
        .path_edges = null,
        .provenance = provenance,
    };
}

const ParsedProvenanceEdge = struct {
    edge_id: []const u8 = "",
    owner_document: []const u8 = "",
    source: []const u8,
    edge_type: []const u8,
    target: []const u8,
};

fn algebraicShortestPathResultNodeAlloc(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    params: QueryParams,
    start_key: []const u8,
    target_key: []const u8,
    result: algebraic_path_mod.PathResult,
) !?GraphResultNode {
    const labels = try algebraic_path_mod.provenanceLabelsAlloc(alloc, result.provenance);
    defer freeProvenanceLabels(alloc, labels);
    if (labels.len != result.depth) return null;

    const path_nodes = try alloc.alloc([]const u8, labels.len + 1);
    var path_node_count: usize = 0;
    var path_nodes_owned = true;
    defer if (path_nodes_owned) freePathNodeItems(alloc, path_nodes, path_node_count);

    const path_edges = try alloc.alloc(PathEdgeInfo, labels.len);
    var path_edge_count: usize = 0;
    var path_edges_owned = true;
    defer if (path_edges_owned) freePathEdgeItems(alloc, path_edges, path_edge_count);

    const used = try alloc.alloc(bool, labels.len);
    defer alloc.free(used);
    @memset(used, false);

    path_nodes[0] = try alloc.dupe(u8, start_key);
    path_node_count = 1;
    var current = path_nodes[0];

    for (0..labels.len) |_| {
        var match_index: ?usize = null;
        var match_edge: ParsedProvenanceEdge = undefined;
        for (labels, 0..) |label, i| {
            if (used[i]) continue;
            const parsed = parseProvenanceEdge(label) orelse return null;
            if (!provenanceEdgeCanAdvance(params.direction, current, parsed)) continue;
            if (match_index != null) return null;
            match_index = i;
            match_edge = parsed;
        }
        const index = match_index orelse return null;
        const next_key = provenanceEdgeNextNode(params.direction, current, match_edge) orelse return null;
        const path_edge = (try resolveUniqueGraphPathEdge(alloc, graph_index, current, match_edge, params.direction)) orelse return null;
        path_edges[path_edge_count] = path_edge;
        path_edge_count += 1;

        path_nodes[path_node_count] = try alloc.dupe(u8, next_key);
        current = path_nodes[path_node_count];
        path_node_count += 1;
        used[index] = true;
    }

    if (!std.mem.eql(u8, current, target_key)) return null;

    const provenance = try cloneProvenanceLabelsAlloc(alloc, labels);
    errdefer freeProvenanceLabels(alloc, provenance);
    const key = try alloc.dupe(u8, target_key);
    errdefer alloc.free(key);

    return .{
        .key = key,
        .depth = result.depth,
        .distance = @floatFromInt(result.depth),
        .path = blk: {
            path_nodes_owned = false;
            break :blk path_nodes;
        },
        .path_edges = blk: {
            path_edges_owned = false;
            break :blk path_edges;
        },
        .provenance = provenance,
    };
}

fn graphEdgeProvenanceLabelAlloc(alloc: Allocator, edge: graph_mod.Edge) ![]u8 {
    // Keep unambiguous legacy tuples readable. A reserved leading byte or
    // an embedded separator needs the same framed identity as explicit edges.
    if (edge.edge_id.len == 0 and (edge.source.len == 0 or edge.source[0] != 2) and
        std.mem.indexOfScalar(u8, edge.source, 0x1f) == null and
        std.mem.indexOfScalar(u8, edge.edge_type, 0x1f) == null and
        std.mem.indexOfScalar(u8, edge.target, 0x1f) == null)
        return std.fmt.allocPrint(alloc, "{s}\x1f{s}\x1f{s}", .{ edge.source, edge.edge_type, edge.target });
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, 2);
    for ([_][]const u8{ edge.source, edge.edge_type, edge.target, edge.edge_id, edge.owner_document }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, @intCast(part.len), .little);
        try out.appendSlice(alloc, &length);
        try out.appendSlice(alloc, part);
    }
    return out.toOwnedSlice(alloc);
}

fn parseProvenanceEdge(label: []const u8) ?ParsedProvenanceEdge {
    if (label.len > 0 and label[0] == 2) {
        var pos: usize = 1;
        var parts: [5][]const u8 = undefined;
        for (&parts) |*part| {
            if (label.len - pos < 8) return null;
            const len = std.math.cast(usize, std.mem.readInt(u64, label[pos..][0..8], .little)) orelse return null;
            pos += 8;
            if (len > label.len - pos) return null;
            part.* = label[pos..][0..len];
            pos += len;
        }
        if (pos != label.len) return null;
        return .{ .source = parts[0], .edge_type = parts[1], .target = parts[2], .edge_id = parts[3], .owner_document = parts[4] };
    }

    var it = std.mem.splitScalar(u8, label, 0x1f);
    const source = it.next() orelse return null;
    const edge_type = it.next() orelse return null;
    const target = it.next() orelse return null;
    if (it.next() != null) return null;
    return .{ .source = source, .edge_type = edge_type, .target = target };
}

fn provenanceEdgeCanAdvance(direction: graph_mod.EdgeDirection, current: []const u8, edge: ParsedProvenanceEdge) bool {
    return provenanceEdgeNextNode(direction, current, edge) != null;
}

fn provenanceEdgeNextNode(direction: graph_mod.EdgeDirection, current: []const u8, edge: ParsedProvenanceEdge) ?[]const u8 {
    return switch (direction) {
        .out => if (std.mem.eql(u8, current, edge.source)) edge.target else null,
        .in => if (std.mem.eql(u8, current, edge.target)) edge.source else null,
        .both => if (std.mem.eql(u8, current, edge.source)) edge.target else if (std.mem.eql(u8, current, edge.target)) edge.source else null,
    };
}

fn resolveUniqueGraphPathEdge(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    current: []const u8,
    provenance_edge: ParsedProvenanceEdge,
    direction: graph_mod.EdgeDirection,
) !?PathEdgeInfo {
    const edges = try graph_index.getEdges(alloc, current, "", direction);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);

    var found: ?graph_mod.Edge = null;
    for (edges) |edge| {
        if (!std.mem.eql(u8, edge.source, provenance_edge.source)) continue;
        if (!std.mem.eql(u8, edge.target, provenance_edge.target)) continue;
        if (!std.mem.eql(u8, edge.edge_type, provenance_edge.edge_type)) continue;
        if (!std.mem.eql(u8, edge.edge_id, provenance_edge.edge_id) or !std.mem.eql(u8, edge.owner_document, provenance_edge.owner_document)) continue;
        if (found != null) return null;
        found = edge;
    }
    const edge = found orelse return null;
    const traversal_direction: ?graph_mod.EdgeDirection = switch (direction) {
        .out => .out,
        .in => .in,
        .both => if (std.mem.eql(u8, edge.source, edge.target)) null else if (std.mem.eql(u8, current, edge.source)) .out else .in,
    };
    return try pathEdgeInfoFromPathEdge(alloc, .{
        .source = edge.source,
        .target = edge.target,
        .edge_type = edge.edge_type,
        .edge_id = edge.edge_id,
        .owner_document = edge.owner_document,
        .weight = edge.weight,
        .metadata = edge.metadata,
        .traversal_direction = traversal_direction,
    });
}

/// Public provenance is opaque text, never the executor's binary identity.
/// Reserve the prefix so even a legacy tuple beginning with it is encoded.
const public_provenance_prefix = "antfly:graph-provenance:v1:";
fn provenanceLabelNeedsEncoding(label: []const u8) bool {
    return (label.len > 0 and label[0] == 2) or !std.unicode.utf8ValidateSlice(label) or
        std.mem.startsWith(u8, label, public_provenance_prefix);
}
fn publicProvenanceLabelAlloc(alloc: Allocator, label: []const u8) ![]u8 {
    if (!provenanceLabelNeedsEncoding(label)) return alloc.dupe(u8, label);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const length = try std.math.add(usize, public_provenance_prefix.len, encoder.calcSize(label.len));
    const out = try alloc.alloc(u8, length);
    @memcpy(out[0..public_provenance_prefix.len], public_provenance_prefix);
    _ = encoder.encode(out[public_provenance_prefix.len..], label);
    return out;
}

fn cloneProvenanceLabelsAlloc(alloc: Allocator, labels: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, labels.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (labels, 0..) |label, i| {
        out[i] = try publicProvenanceLabelAlloc(alloc, label);
        initialized += 1;
    }
    return out;
}

fn freePathNodeItems(alloc: Allocator, nodes: []const []const u8, initialized: usize) void {
    for (nodes[0..initialized]) |node| alloc.free(node);
    alloc.free(nodes);
}

fn freePathEdgeItems(alloc: Allocator, edges: []const PathEdgeInfo, initialized: usize) void {
    for (edges[0..initialized]) |edge| {
        alloc.free(edge.source);
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.edge_id.len > 0) alloc.free(edge.edge_id);
        if (edge.owner_document.len > 0) alloc.free(edge.owner_document);
        if (edge.metadata.len > 0) alloc.free(edge.metadata);
    }
    alloc.free(edges);
}

fn freeResultNode(alloc: Allocator, node: GraphResultNode) void {
    var owned = node;
    owned.deinit(alloc);
}

fn freeProvenanceLabels(alloc: Allocator, labels: []const []const u8) void {
    for (labels) |label| alloc.free(label);
    alloc.free(labels);
}

const AlgebraicReachabilityEdges = struct {
    items: []algebraic_path_mod.Edge,
    /// The algebraic tensor representation is key-scoped. Cross-table graph
    /// identities therefore require the table-aware traversal implementation.
    has_cross_table_edges: bool = false,
    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items) |edge| {
            alloc.free(edge.from);
            alloc.free(edge.to);
            alloc.free(edge.provenance);
        }
        if (self.items.len > 0) alloc.free(self.items);
        self.* = .{ .items = &.{} };
    }
};

const AlgebraicQueueEntry = struct {
    key: []u8,
    depth: u32,
};

fn collectAlgebraicReachabilityEdges(
    alloc: Allocator,
    graph_index: *graph_mod.GraphIndex,
    start_key: []const u8,
    params: QueryParams,
    work_budget: ?*work_budget_mod.WorkBudget,
) !AlgebraicReachabilityEdges {
    var edges = std.ArrayListUnmanaged(algebraic_path_mod.Edge).empty;
    errdefer {
        for (edges.items) |edge| {
            alloc.free(edge.from);
            alloc.free(edge.to);
            alloc.free(edge.provenance);
        }
        edges.deinit(alloc);
    }

    var has_cross_table_edges = false;

    var queue = std.ArrayListUnmanaged(AlgebraicQueueEntry).empty;
    defer {
        for (queue.items) |entry| alloc.free(entry.key);
        queue.deinit(alloc);
    }
    var queue_head: usize = 0;

    var visited = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = visited.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        visited.deinit(alloc);
    }

    try queue.append(alloc, .{
        .key = try alloc.dupe(u8, start_key),
        .depth = 0,
    });
    if (work_budget) |budget| {
        try budget.consumeNode();
        try budget.checkIntermediateStates(queue.items.len, work_budget_mod.default_max_intermediate_states);
    }
    try visited.put(alloc, try alloc.dupe(u8, start_key), {});

    scan: while (queue_head < queue.items.len) {
        const current = queue.items[queue_head];
        queue_head += 1;
        if (current.depth >= params.max_depth) continue;

        const graph_edges = if (work_budget) |budget|
            graph_index.getEdgesByTypesBounded(
                alloc,
                current.key,
                params.edge_types,
                params.direction,
                budget.edgeLimit(),
                budget.edgeByteLimit(),
            ) catch |err| switch (err) {
                error.GraphExploredEdgesBudgetExceeded, error.QueryCandidateBudgetExceeded => return budget.exhaust(.explored_edges, budget.max_edges),
                error.GraphExploredEdgeBytesBudgetExceeded => return budget.exhaust(.explored_edge_bytes, budget.max_edge_bytes),
                else => return err,
            }
        else
            try graph_index.getEdges(alloc, current.key, "", params.direction);
        defer graph_mod.GraphIndex.freeEdges(alloc, graph_edges);
        var table_scratch = traversal_mod.MetadataScratch.init(alloc, work_budget);
        defer table_scratch.deinit();
        if (work_budget) |budget| try budget.consumeMaterializedEdges(graph_edges);
        for (graph_edges) |edge| {
            if (!graphEdgeTypeAllowed(params.edge_types, edge.edge_type)) continue;
            if (!graphEdgeWeightAllowed(params, edge.weight)) continue;
            // Tensor vertices cannot represent either endpoint's namespace,
            // even when only the departure endpoint has an explicit tag.
            if ((try traversal_mod.metadataSourceTable(&table_scratch, edge.metadata)) != null or
                (try traversal_mod.metadataTargetTable(&table_scratch, edge.metadata)) != null)
            {
                has_cross_table_edges = true;
                break :scan;
            }
            const endpoint = try traversal_mod.resolveAdjacent(&table_scratch, edge, current.key, null, null, params.direction);
            if (!endpoint.connected) continue;
            const next_key = endpoint.key;
            if (std.mem.eql(u8, next_key, start_key)) continue;
            const already_visited = visited.contains(next_key);
            if (!already_visited) {
                try visited.put(alloc, try alloc.dupe(u8, next_key), {});
                if (work_budget) |budget| try budget.consumeNode();
            }

            const provenance_label = try graphEdgeProvenanceLabelAlloc(alloc, edge);
            defer alloc.free(provenance_label);
            const provenance = try algebraic_path_mod.provenanceTokenAlloc(alloc, &.{provenance_label});
            var provenance_owned = true;
            errdefer if (provenance_owned) alloc.free(provenance);
            const from = try alloc.dupe(u8, current.key);
            var from_owned = true;
            errdefer if (from_owned) alloc.free(from);
            const to = try alloc.dupe(u8, next_key);
            var to_owned = true;
            errdefer if (to_owned) alloc.free(to);

            try edges.append(alloc, .{
                .from = from,
                .to = to,
                .provenance = provenance,
            });
            provenance_owned = false;
            from_owned = false;
            to_owned = false;
            if (!already_visited) {
                try queue.append(alloc, .{
                    .key = try alloc.dupe(u8, next_key),
                    .depth = current.depth + 1,
                });
                if (work_budget) |budget| {
                    try budget.checkIntermediateStates(queue.items.len, work_budget_mod.default_max_intermediate_states);
                }
            }
        }
    }

    const owned = try edges.toOwnedSlice(alloc);
    return .{
        .items = owned,
        .has_cross_table_edges = has_cross_table_edges,
    };
}

fn filterAlgebraicReachabilityEdgesWithAdmission(
    alloc: Allocator,
    edges: *AlgebraicReachabilityEdges,
    admission: NodeAdmission,
) !void {
    if (edges.items.len == 0) return;
    const refs = try alloc.alloc(NodeRef, edges.items.len);
    defer alloc.free(refs);
    for (edges.items, 0..) |edge, i| {
        refs[i] = .{
            .key = edge.to,
            .table = null,
            .external = false,
        };
    }
    const allowed = try admission.filterAlloc(alloc, refs);
    defer alloc.free(allowed);

    var allowed_count: usize = 0;
    for (allowed) |value| allowed_count += @intFromBool(value);
    if (allowed_count == edges.items.len) return;

    const filtered = try alloc.alloc(algebraic_path_mod.Edge, allowed_count);
    var write_index: usize = 0;
    for (edges.items, allowed) |edge, include| {
        if (include) {
            filtered[write_index] = edge;
            write_index += 1;
        } else {
            alloc.free(edge.from);
            alloc.free(edge.to);
            alloc.free(edge.provenance);
        }
    }
    alloc.free(edges.items);
    edges.items = filtered;
}

fn algebraicTraversalTensorProgramAccepted(
    alloc: Allocator,
    graph_index: *const graph_mod.GraphIndex,
    params: QueryParams,
    target_keys: []const []const u8,
) !bool {
    if (!algebraicTraversalProof(graph_index, params).safe()) return false;

    var plan = (try algebraic_planner.planGraphTraversalTensorProgramAlloc(alloc, graph_index.index_name, target_keys.len > 0)) orelse return false;
    defer plan.deinit(alloc);
    return algebraic_ir.graphTraversalProgramMatchesTarget(plan.asProgram(), graph_index.index_name, target_keys.len > 0) and
        (try algebraic_ir.tensorProgramProof(alloc, plan.access_paths, plan.asProgram())).safe();
}

fn graphEdgeTypeAllowed(allowed: []const []const u8, edge_type: []const u8) bool {
    if (allowed.len == 0) return true;
    for (allowed) |item| {
        if (std.mem.eql(u8, item, edge_type)) return true;
    }
    return false;
}

fn graphEdgeWeightAllowed(params: QueryParams, weight: f64) bool {
    if (params.min_weight) |min_weight| if (weight < min_weight) return false;
    if (params.max_weight) |max_weight| if (weight > max_weight) return false;
    return true;
}

// ============================================================================
// Tests
// ============================================================================

const docstore = @import("../storage/docstore.zig");

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const ns = platform_time.monotonicNs();
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-gq-{s}-{d}\x00", .{ label, ns }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

const TestCtx = struct {
    store: docstore.DocStore,
    graph: graph_mod.GraphIndex,
    sp: [*:0]const u8,
    rp: [*:0]const u8,

    pub fn deinit(self: *TestCtx) void {
        self.graph.close();
        self.store.close();
        cleanupTmp(self.sp);
        cleanupTmp(self.rp);
    }
};

fn setupGraphWithOptions(
    alloc: Allocator,
    store_label: []const u8,
    rev_label: []const u8,
    sb: *[256]u8,
    rb: *[256]u8,
    opts: graph_mod.GraphIndexOptions,
) !*TestCtx {
    const sp = tmpPath(sb, store_label);
    const rp = tmpPath(rb, rev_label);
    const ctx = try alloc.create(TestCtx);
    errdefer alloc.destroy(ctx);
    ctx.store = try docstore.DocStore.open(alloc, sp, .{});
    errdefer ctx.store.close();
    ctx.graph = try graph_mod.GraphIndex.open(alloc, &ctx.store, rp, "test", opts);
    ctx.sp = sp;
    ctx.rp = rp;
    return ctx;
}

fn setupGraph(alloc: Allocator, store_label: []const u8, rev_label: []const u8, sb: *[256]u8, rb: *[256]u8) !*TestCtx {
    return try setupGraphWithOptions(alloc, store_label, rev_label, sb, rb, .{});
}

fn expectAlgebraicTraversalReject(proof: AlgebraicTraversalProof, reason: AlgebraicTraversalRejectReason) !void {
    switch (proof) {
        .proven => return error.TestExpectedEqual,
        .rejected => |actual| try std.testing.expectEqual(reason, actual),
    }
}

// --- Phase 18A tests ---

test "GraphQuery struct construction" {
    const gq_traverse = GraphQuery{
        .query_type = .traverse,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{ "doc1", "doc2" } },
    };
    try std.testing.expectEqual(QueryType.traverse, gq_traverse.query_type);
    try std.testing.expectEqual(@as(u32, 1), gq_traverse.params.max_depth);

    const gq_neighbors = GraphQuery{
        .query_type = .neighbors,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"doc1"} },
    };
    try std.testing.expectEqual(QueryType.neighbors, gq_neighbors.query_type);

    const gq_sp = GraphQuery{
        .query_type = .shortest_path,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"A"} },
        .target_nodes = .{ .keys = &.{"C"} },
    };
    try std.testing.expectEqual(QueryType.shortest_path, gq_sp.query_type);

    const gq_ksp = GraphQuery{
        .query_type = .k_shortest_paths,
        .index_name = "links",
        .start_nodes = .{ .keys = &.{"A"} },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 3,
    };
    try std.testing.expectEqual(@as(u32, 3), gq_ksp.k);
}

test "NodeSelector keys vs result_ref" {
    const keys_sel = NodeSelector{ .keys = &.{ "a", "b", "c" } };
    switch (keys_sel) {
        .keys => |k| try std.testing.expectEqual(@as(usize, 3), k.len),
        .identities => unreachable,
        .result_ref => unreachable,
    }

    const ref_sel = NodeSelector{ .result_ref = .{ .ref = "$query_results", .limit = 10 } };
    switch (ref_sel) {
        .keys => unreachable,
        .identities => unreachable,
        .result_ref => |r| {
            try std.testing.expectEqualStrings("$query_results", r.ref);
            try std.testing.expectEqual(@as(u32, 10), r.limit);
        },
    }
}

test "QueryParams uses the public one-hop default and shared traversal options" {
    const qp = QueryParams{};
    const tr = traversal_mod.TraversalRules{};
    try std.testing.expectEqual(qp.direction, tr.direction);
    try std.testing.expectEqual(@as(u32, 1), qp.max_depth);
    try std.testing.expectEqual(qp.min_weight, tr.min_weight);
    try std.testing.expectEqual(qp.max_weight, tr.max_weight);
    try std.testing.expectEqual(qp.max_results, tr.max_results);
    try std.testing.expectEqual(qp.deduplicate, tr.deduplicate);
    try std.testing.expectEqual(qp.include_paths, tr.include_paths);
}

// --- Phase 18B tests ---

test "traverse: multi-start with depth 2" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq1s", "gq1r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // A -> B -> D, A -> C, X -> Y -> Z
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("X", "Y", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("Y", "Z", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{ "A", "X" };
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 2 },
    }, start_keys);
    defer result.deinit(alloc);

    // Should find B, C, D (from A) + Y, Z (from X) = 5 nodes
    try std.testing.expectEqual(@as(usize, 5), result.nodes.len);
}

test "graph query engine shares traversal work across start nodes" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-budget-s", "gq-budget-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("X", "Y", "e", 1.0, 0, 0, "");

    var work_budget = work_budget_mod.WorkBudget.init(3, 10);
    var engine = GraphQueryEngine{
        .alloc = alloc,
        .work_budget = &work_budget,
    };
    const start_keys: []const []const u8 = &.{ "A", "X" };
    try std.testing.expectError(error.GraphWorkBudgetExceeded, engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 1 },
    }, start_keys));
    try std.testing.expectEqual(work_budget_mod.Dimension.explored_nodes, work_budget.exhaustion().?.dimension);
}

test "traverse preserves table-scoped identities across result dedup and algebraic fallback" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-table-identity-s", "gq-table-identity-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "shared", "local", 1.0, 0, 0, "");
    try ctx.graph.addEdge(
        "A",
        "shared",
        "external",
        1.0,
        0,
        0,
        "{\"target_table\":\"entities\"}",
    );

    var algebraic_probe = try collectAlgebraicReachabilityEdges(alloc, &ctx.graph, "A", .{
        .max_depth = 1,
        .max_results = 0,
        .algebraic_semiring = true,
    }, null);
    defer algebraic_probe.deinit(alloc);
    try std.testing.expect(algebraic_probe.has_cross_table_edges);

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 1,
            .max_results = 0,
            .algebraic_semiring = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    var local_count: usize = 0;
    var external_count: usize = 0;
    for (result.nodes) |node| {
        try std.testing.expectEqualStrings("shared", node.key);
        if (node.table) |table| {
            try std.testing.expectEqualStrings("entities", table);
            external_count += 1;
        } else {
            local_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), local_count);
    try std.testing.expectEqual(@as(usize, 1), external_count);
}

test "engine execution scope expands through cross-table nodes and canonicalizes self-table tags" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-scope-s", "gq-scope-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // The autoschema shape: a mention edge into a resolved cross-table
    // entity, an entity-sourced relation row in THIS index, plus a
    // self-table tag that must canonicalize instead of splitting identity.
    try ctx.graph.addEdge("doc:a", "entity/ada", "mentions", 1.0, 0, 0, "{\"target_table\":\"entities\"}");
    try ctx.graph.addEdge("entity/ada", "event/xyz", "participates_in", 1.0, 0, 0, "{\"source_table\":\"entities\",\"target_table\":\"events\"}");
    try ctx.graph.addEdge("doc:a", "doc:b", "cites", 1.0, 0, 0, "{\"target_table\":\"documents\"}");

    const start_keys: []const []const u8 = &.{"doc:a"};
    const gq = GraphQuery{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 3, .max_results = 0, .deduplicate = true },
    };

    // Default scope keeps the historical terminal behavior.
    var terminal_engine = GraphQueryEngine{ .alloc = alloc };
    var terminal = try terminal_engine.execute(&ctx.graph, gq, start_keys);
    defer terminal.deinit(alloc);
    for (terminal.nodes) |node| {
        try std.testing.expect(!std.mem.eql(u8, node.key, "event/xyz"));
    }

    // A complete-snapshot scope walks doc -> entity -> event in one
    // traversal, and the self-table tag neither stops expansion nor
    // qualifies the node.
    var engine = GraphQueryEngine{
        .alloc = alloc,
        .scope = .{ .owning_table = "documents", .expand_cross_table_local = true },
    };
    var result = try engine.execute(&ctx.graph, gq, start_keys);
    defer result.deinit(alloc);

    var saw_entity = false;
    var saw_event = false;
    var saw_doc_b = false;
    for (result.nodes) |node| {
        if (std.mem.eql(u8, node.key, "entity/ada")) {
            try std.testing.expectEqualStrings("entities", node.table.?);
            saw_entity = true;
        } else if (std.mem.eql(u8, node.key, "event/xyz")) {
            try std.testing.expectEqualStrings("events", node.table.?);
            saw_event = true;
        } else if (std.mem.eql(u8, node.key, "doc:b")) {
            try std.testing.expect(node.table == null);
            saw_doc_b = true;
        }
    }
    try std.testing.expect(saw_entity and saw_event and saw_doc_b);
}

test "traverse can execute through algebraic provenance semiring path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-s", "gq-alg-path-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 0,
            .algebraic_semiring = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[1].depth);
    try std.testing.expectEqualStrings("D", result.nodes[2].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[2].depth);
    const d_provenance = result.nodes[2].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 4), d_provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", d_provenance[0]);
    try std.testing.expectEqualStrings("A\x1fe\x1fC", d_provenance[1]);
    try std.testing.expectEqualStrings("B\x1fe\x1fD", d_provenance[2]);
    try std.testing.expectEqualStrings("C\x1fe\x1fD", d_provenance[3]);
}

test "traverse auto-selects algebraic semiring path from graph index config" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-config-s", "gq-alg-config-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 2, .max_results = 0 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
}

test "traverse algebraic semiring path respects target nodes" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-target-s", "gq-alg-target-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try std.testing.expect(try algebraicTraversalTensorProgramAccepted(alloc, &ctx.graph, .{ .max_depth = 2, .max_results = 0 }, &.{"C"}));
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const target_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{ .max_depth = 2, .max_results = 0 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "traverse algebraic semiring path supports inbound traversal" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-in-s", "gq-alg-in-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .direction = .in,
            .max_depth = 2,
            .max_results = 0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("D", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[1].depth);
    try std.testing.expectEqualStrings("A", result.nodes[2].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[2].depth);
    const provenance = result.nodes[2].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "traverse algebraic semiring path supports bidirectional traversal" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-both-s", "gq-alg-both-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "A", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .direction = .both,
            .max_depth = 1,
            .max_results = 0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    var saw_b = false;
    var saw_c = false;
    for (result.nodes) |node| {
        try std.testing.expectEqual(@as(u32, 1), node.depth);
        const provenance = node.provenance orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(usize, 1), provenance.len);
        if (std.mem.eql(u8, node.key, "B")) {
            saw_b = true;
            try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
        } else if (std.mem.eql(u8, node.key, "C")) {
            saw_c = true;
            try std.testing.expectEqualStrings("C\x1fe\x1fA", provenance[0]);
        }
    }
    try std.testing.expect(saw_b);
    try std.testing.expect(saw_c);
    const algebraic_stats = ctx.graph.algebraicTraversalRuntimeStats();
    try std.testing.expectEqual(@as(u64, 1), algebraic_stats.attempt_count);
    try std.testing.expectEqual(@as(u64, 1), algebraic_stats.proven_count);
    try std.testing.expectEqual(@as(u64, 0), algebraic_stats.rejected_count);
    try std.testing.expectEqual(@as(u64, 0), algebraic_stats.fallback_count);
    try std.testing.expectEqual(@as(u64, 2), algebraic_stats.result_node_count);
}

test "algebraic traversal proof gates exact semiring shapes" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-proof-s", "gq-alg-proof-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{}), .disabled);
    ctx.graph.algebraic_semiring_traversal = true;
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{}).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .include_paths = true, .max_results = 0 }).safe());
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .deduplicate = false, .max_results = 0 }), .non_deduplicated);
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .max_depth = 0, .max_results = 0 }), .zero_depth);
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .min_weight = 0.5, .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .max_weight = 2.5, .max_results = 0 }).safe());
    try expectAlgebraicTraversalReject(algebraicTraversalProof(&ctx.graph, .{ .weight_mode = .min_weight, .max_results = 0 }), .weighted_mode);
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .direction = .in, .max_results = 0 }).safe());
    try std.testing.expect(algebraicTraversalProof(&ctx.graph, .{ .direction = .both, .max_results = 0 }).safe());
}

test "traverse algebraic semiring path applies exact edge weight filters" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-weight-filter-s", "gq-alg-weight-filter-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "fast", 0.5, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("A", "E", "slow", 5.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 0,
            .min_weight = 1.0,
            .max_weight = 3.0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 1), result.nodes[0].depth);
    try std.testing.expectEqualStrings("D", result.nodes[1].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[1].depth);
    const d_provenance = result.nodes[1].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), d_provenance.len);
    try std.testing.expectEqualStrings("A\x1fok\x1fC", d_provenance[0]);
    try std.testing.expectEqualStrings("C\x1fok\x1fD", d_provenance[1]);
}

test "traverse algebraic semiring path supports deterministic result limits" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-limit-s", "gq-alg-limit-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "E", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .max_results = 2,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    try std.testing.expect(result.nodes[0].provenance != null);
    try std.testing.expect(result.nodes[1].provenance != null);
}

test "traverse counts only target-admitted nodes toward result limit" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-target-limit-s", "gq-target-limit-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // BFS encounters the non-target first. It must keep expanding through that
    // node until one admitted result is found instead of consuming the limit.
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "Z", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"Z"} },
        .params = .{
            .max_depth = 2,
            .max_results = 1,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("Z", result.nodes[0].key);
}

test "algebraic traversal intersects query-scoped node admission" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-admission-s", "gq-alg-admission-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    const Admission = struct {
        fn filter(
            _: ?*anyopaque,
            result_alloc: Allocator,
            nodes: []const NodeRef,
        ) ![]bool {
            const out = try result_alloc.alloc(bool, nodes.len);
            for (nodes, 0..) |node, i| {
                out[i] = !std.mem.eql(u8, node.key, "B");
            }
            return out;
        }
    };
    var engine = GraphQueryEngine{
        .alloc = alloc,
        .node_admission = .{
            .ctx = null,
            .filter_many = Admission.filter,
        },
    };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .max_depth = 2 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("D", result.nodes[0].key);
    const stats = ctx.graph.algebraicTraversalRuntimeStats();
    try std.testing.expectEqual(@as(u64, 1), stats.proven_count);
    try std.testing.expectEqual(@as(u64, 0), stats.fallback_count);
}

test "algebraic traversal reconstructs path-returning shapes when provenance is unique" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-return-s", "gq-alg-path-return-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .include_paths = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expect(result.nodes[0].path_edges != null);
    try std.testing.expect(result.nodes[0].provenance != null);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    const c_path = result.nodes[1].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), c_path.len);
    try std.testing.expectEqualStrings("A", c_path[0]);
    try std.testing.expectEqualStrings("B", c_path[1]);
    try std.testing.expectEqualStrings("C", c_path[2]);
    const c_edges = result.nodes[1].path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), c_edges.len);
    try std.testing.expectEqualStrings("A", c_edges[0].source);
    try std.testing.expectEqualStrings("B", c_edges[0].target);
    try std.testing.expectEqualStrings("B", c_edges[1].source);
    try std.testing.expectEqualStrings("C", c_edges[1].target);
}

test "algebraic traversal falls back for ambiguous path-returning provenance" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-path-ambig-s", "gq-alg-path-ambig-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .include_paths = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    for (result.nodes) |node| {
        try std.testing.expect(node.path != null);
        try std.testing.expect(node.provenance == null);
    }
}

test "neighbors: only 1-hop" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq2s", "gq2r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .neighbors,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
    }, start_keys);
    defer result.deinit(alloc);

    // Only B and D (1-hop), not C (2-hop)
    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    for (result.nodes) |node| {
        try std.testing.expectEqual(@as(u32, 1), node.depth);
    }
}

test "shortest_path via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq3s", "gq3r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 10.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .max_depth = 2, .include_paths = true, .weight_mode = .min_weight },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth); // A->B->C (weight 2 < 10)
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expectEqual(@as(usize, 3), result.nodes[0].path.?.len);
}

test "shortest_path can execute through algebraic provenance semiring for unique min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-s", "gq-alg-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    try std.testing.expectEqual(@as(f64, 2.0), result.nodes[0].distance);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), path.len);
    try std.testing.expectEqualStrings("A", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("C", path[2]);
    const path_edges = result.nodes[0].path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), path_edges.len);
    try std.testing.expectEqual(@as(f64, 2.0), path_edges[0].weight);
    try std.testing.expectEqual(@as(f64, 3.0), path_edges[1].weight);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "shortest_path falls back when algebraic provenance is ambiguous" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-fallback-s", "gq-alg-shortest-fallback-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expect(result.nodes[0].provenance == null);
}

test "shortest_path algebraic provenance supports inbound min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-in-s", "gq-alg-shortest-in-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"C"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"A"} },
        .params = .{ .direction = .in, .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("A", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("C", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("A", path[2]);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "shortest_path algebraic provenance supports bidirectional min-hop path" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-both-s", "gq-alg-shortest-both-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "B", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .params = .{ .direction = .both, .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    const path = result.nodes[0].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("A", path[0]);
    try std.testing.expectEqualStrings("B", path[1]);
    try std.testing.expectEqualStrings("C", path[2]);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("C\x1fe\x1fB", provenance[1]);
}

test "shortest_path algebraic provenance applies exact edge weight filters" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-shortest-weight-s", "gq-alg-shortest-weight-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "too-light", 0.5, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "too-heavy", 5.0, 0, 0, "");
    try ctx.graph.addEdge("C", "E", "ok", 2.0, 0, 0, "");
    try ctx.graph.addEdge("E", "D", "ok", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const target_keys: []const []const u8 = &.{"D"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{
            .max_depth = 3,
            .min_weight = 1.0,
            .max_weight = 3.0,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("D", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 3), result.nodes[0].depth);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), provenance.len);
    try std.testing.expectEqualStrings("A\x1fok\x1fC", provenance[0]);
    try std.testing.expectEqualStrings("C\x1fok\x1fE", provenance[1]);
    try std.testing.expectEqualStrings("E\x1fok\x1fD", provenance[2]);
}

test "graph metric query shape bounds clauses and unique dependencies" {
    const names = [_][]const u8{
        "m00", "m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08",
        "m09", "m10", "m11", "m12", "m13", "m14", "m15", "m16",
    };
    var reads: [graph_metric_projection_limit + 1]GraphMetricRead = undefined;
    for (&reads, names) |*read, name| read.* = .{ .name = name };

    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .metrics = &reads,
    }));

    var too_many_orders: [graph_metric_order_limit + 1]GraphMetricOrder = undefined;
    for (&too_many_orders) |*order| order.* = .{ .name = "pagerank" };
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .order_by = &too_many_orders,
    }));

    var too_many_filters: [graph_metric_filter_limit + 1]GraphMetricFilter = undefined;
    for (&too_many_filters) |*filter| filter.* = .{ .name = "pagerank", .op = .gte, .value = 0.1 };
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .where_metric = &too_many_filters,
    }));

    const extra_order = [_]GraphMetricOrder{.{ .name = names[graph_metric_dependency_limit] }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .metrics = reads[0..graph_metric_dependency_limit],
        .order_by = &extra_order,
    }));

    const duplicate_reads = [_]GraphMetricRead{ .{ .name = "pagerank" }, .{ .name = "pagerank" } };
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .metrics = &duplicate_reads,
    }));

    const duplicate_orders = [_]GraphMetricOrder{ .{ .name = "pagerank" }, .{ .name = "pagerank", .direction = .asc } };
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .order_by = &duplicate_orders,
    }));

    const range_filters = [_]GraphMetricFilter{
        .{ .name = "pagerank", .op = .gte, .value = 0.1 },
        .{ .name = "pagerank", .op = .lt, .value = 0.9 },
    };
    try validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .where_metric = &range_filters,
    });

    const invalid_filters = [_]GraphMetricFilter{.{
        .name = "pagerank",
        .op = .gte,
        .value = std.math.nan(f64),
    }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(.{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
        .where_metric = &invalid_filters,
    }));
}

test "borrowed graph metric names do not allocate per node" {
    var value = GraphMetricValue{ .name = "pagerank", .score = 0.5, .name_owned = false };
    try value.ensureNameOwned(std.testing.allocator);
    try std.testing.expect(value.name_owned);
    try std.testing.expectEqualStrings("pagerank", value.name);
    value.deinit(std.testing.allocator);
}

test "graph metric column selection retains deterministic bounded top k" {
    var scores = [_]?f64{ 0.3, null, 0.9, 0.8, 0.9, 0.1 };
    const columns = [_][]?f64{&scores};
    const orders = [_]GraphMetricOrder{.{
        .name = "rank",
        .direction = .desc,
        .nulls = .last,
    }};
    const metric_indexes = [_]usize{0};
    const context = GraphQueryEngine.MetricColumnSortContext{
        .orders = &orders,
        .metric_indexes = &metric_indexes,
        .score_columns = &columns,
    };
    var candidates = [_]usize{ 0, 1, 2, 3, 4, 5 };
    const selected = GraphQueryEngine.retainOrderedMetricCandidatePrefix(&candidates, 3, context);
    try std.testing.expectEqualSlices(usize, &.{ 2, 4, 3 }, selected);

    var ascending_candidates = [_]usize{ 0, 1, 2, 3, 4, 5 };
    const ascending_orders = [_]GraphMetricOrder{.{
        .name = "rank",
        .direction = .asc,
        .nulls = .first,
    }};
    const ascending = GraphQueryEngine.retainOrderedMetricCandidatePrefix(
        &ascending_candidates,
        2,
        .{
            .orders = &ascending_orders,
            .metric_indexes = &metric_indexes,
            .score_columns = &columns,
        },
    );
    try std.testing.expectEqualSlices(usize, &.{ 1, 5 }, ascending);
}

test "graph metric shared column application is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var metric_values_slab: []GraphMetricValue = &.{};
            var nodes = try alloc.alloc(GraphResultNode, 3);
            var initialized: usize = 0;
            defer {
                for (nodes[0..initialized]) |*node| node.deinit(alloc);
                alloc.free(nodes);
                if (metric_values_slab.len > 0) alloc.free(metric_values_slab);
            }
            for (&[_][]const u8{ "a", "b", "c" }, 0..) |key, i| {
                nodes[i] = .{ .key = try alloc.dupe(u8, key), .depth = 1, .distance = 1 };
                initialized += 1;
            }

            var scores = [_]?f64{ 0.1, 0.9, 0.5 };
            const columns = [_][]?f64{&scores};
            const names = [_][]const u8{"rank"};
            const reads = [_]GraphMetricRead{.{ .name = "rank" }};
            const orders = [_]GraphMetricOrder{.{ .name = "rank", .direction = .desc }};
            metric_values_slab = try GraphQueryEngine.applyLoadedMetricColumns(
                alloc,
                &names,
                &names,
                &columns,
                .{
                    .query_type = .neighbors,
                    .index_name = "g",
                    .start_nodes = .{ .keys = &.{"root"} },
                    .params = .{ .max_results = 2 },
                    .metrics = &reads,
                    .order_by = &orders,
                },
                true,
                &nodes,
            );
            initialized = nodes.len;
            try std.testing.expectEqual(@as(usize, 2), nodes.len);
            try std.testing.expectEqualStrings("b", nodes[0].key);
            try std.testing.expectEqual(@as(?f64, 0.9), nodes[0].metrics[0].score);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric stable row materialization moves nodes once and is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var metric_values_slab: []GraphMetricValue = &.{};
            var nodes = try alloc.alloc(GraphResultNode, 3);
            var initialized: usize = 0;
            defer {
                for (nodes[0..initialized]) |*node| node.deinit(alloc);
                alloc.free(nodes);
                if (metric_values_slab.len > 0) alloc.free(metric_values_slab);
            }
            for (&[_][]const u8{ "a", "b", "c" }, 0..) |key, i| {
                nodes[i] = .{ .key = try alloc.dupe(u8, key), .depth = 1, .distance = 1 };
                initialized += 1;
            }

            var aligned_scores = [_]?f64{ 0.8, 0.2 };
            const columns = [_][]?f64{&aligned_scores};
            const names = [_][]const u8{"rank"};
            metric_values_slab = try GraphQueryEngine.materializeSelectedMetricColumns(
                alloc,
                &names,
                &columns,
                &.{ 2, 0 },
                &nodes,
            );
            initialized = nodes.len;
            try std.testing.expectEqual(@as(usize, 2), nodes.len);
            try std.testing.expectEqualStrings("c", nodes[0].key);
            try std.testing.expectEqual(@as(?f64, 0.8), nodes[0].metrics[0].score);
            try std.testing.expectEqualStrings("a", nodes[1].key);
            try std.testing.expectEqual(@as(?f64, 0.2), nodes[1].metrics[0].score);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric staged reads only load display columns for selected rows" {
    const Runner = struct {
        const Reader = struct {
            keys: usize = 0,
            pub fn readColumns(self: *@This(), _: Allocator, names: []const []const u8, keys: []const []const u8, columns: []const []?f64) !void {
                self.keys += names.len * keys.len;
                for (columns) |column| for (keys, column) |key, *value| {
                    value.* = @floatFromInt(key[0] - '0');
                };
            }
        };
        fn run(alloc: Allocator) !void {
            const query = GraphQuery{
                .query_type = .neighbors,
                .index_name = "graph",
                .start_nodes = .{ .keys = &.{"0"} },
                .params = .{ .max_results = 2 },
                .metrics = &.{ .{ .name = "rank" }, .{ .name = "display" } },
                .order_by = &.{.{ .name = "rank" }},
                .where_metric = &.{.{ .name = "rank", .op = .gte, .value = 2 }},
            };
            const nodes = [_]GraphResultNode{
                .{ .key = "0", .depth = 0, .distance = 0 }, .{ .key = "1", .depth = 0, .distance = 0 }, .{ .key = "2", .depth = 0, .distance = 0 },
                .{ .key = "3", .depth = 0, .distance = 0 }, .{ .key = "4", .depth = 0, .distance = 0 }, .{ .key = "5", .depth = 0, .distance = 0 },
            };
            const plan = try MetricReadPlan.init(query);
            var work = try GraphQueryEngine.MetricStageWorkspace.init(alloc, plan, nodes.len);
            defer work.deinit();
            var reader = Reader{};
            try work.ensure(&reader, plan.filters.slice(), &nodes);
            var filter = query;
            filter.metrics = &.{};
            filter.order_by = &.{};
            try work.select(filter, false, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
            try work.ensure(&reader, plan.orders.slice(), &nodes);
            var order = query;
            order.metrics = &.{};
            order.where_metric = &.{};
            try work.select(order, true, plan.orders.slice(), plan.projections.slice(), &.{});
            try work.ensure(&reader, plan.projections.slice(), &nodes);
            try std.testing.expectEqual(@as(usize, 8), reader.keys);
            try std.testing.expectEqualSlices(usize, &.{ 5, 4 }, work.rows);
            for (work.columns[0..2]) |slot| {
                const column = slot.?.column(work.rowCount(), &work.groups);
                try std.testing.expectEqual(@as(?f64, 5), column.at(0));
                try std.testing.expectEqual(@as(?f64, 4), column.at(1));
            }
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric staged columns do not alias qualified node identities" {
    const alloc = std.testing.allocator;
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "graph", .start_nodes = .{ .keys = &.{} }, .metrics = &.{.{ .name = "rank" }} };
    const nodes = [_]GraphResultNode{
        .{ .key = "same", .table = "other", .depth = 0, .distance = 0 },
        .{ .key = "same", .depth = 0, .distance = 0 },
        .{ .key = "same", .table = "other", .depth = 0, .distance = 0 },
    };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(alloc, try MetricReadPlan.init(query), nodes.len);
    defer work.deinit();
    const Reader = struct {
        pub fn readColumns(_: *@This(), _: Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []?f64) !void {
            try std.testing.expectEqual(@as(usize, 1), keys.len);
            columns[0][0] = 7;
        }
    };
    var reader = Reader{};
    try work.ensure(&reader, &.{"rank"}, &nodes);
    const column = work.columns[0].?.column(work.rows.len, &work.groups);
    try std.testing.expect(column.at(0) == null and column.at(2) == null);
    try std.testing.expectEqual(@as(?f64, 7), column.at(1));
}

test "graph metric personalization requires fresh reads and bounded seed sets" {
    const seeds = [_][]const u8{"doc:a"};
    const base = GraphQuery{
        .query_type = .traverse,
        .index_name = "g",
        .start_nodes = .{ .keys = &.{"doc:a"} },
    };

    var published_seeded = base;
    published_seeded.metrics = &.{.{ .name = "pagerank", .seed_nodes = &seeds }};
    try std.testing.expectError(error.GraphMetricPersonalizationRequiresFresh, validateGraphMetricQueryShape(published_seeded));

    var fresh_seeded = base;
    fresh_seeded.metrics = &.{.{ .name = "pagerank", .freshness = .fresh, .seed_nodes = &seeds, .damping = 0.9 }};
    try validateGraphMetricQueryShape(fresh_seeded);

    var damping_without_seeds = base;
    damping_without_seeds.metrics = &.{.{ .name = "pagerank", .freshness = .fresh, .damping = 0.9 }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(damping_without_seeds));

    var damping_out_of_range = base;
    damping_out_of_range.metrics = &.{.{ .name = "pagerank", .freshness = .fresh, .seed_nodes = &seeds, .damping = 1.0 }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(damping_out_of_range));

    var empty_seed_key = base;
    empty_seed_key.metrics = &.{.{ .name = "pagerank", .freshness = .fresh, .seed_nodes = &.{""} }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(empty_seed_key));

    var too_many_seed_keys: [graph_metric_seed_limit + 1][]const u8 = undefined;
    for (&too_many_seed_keys) |*seed| seed.* = "doc:a";
    var too_many_seeds = base;
    too_many_seeds.metrics = &.{.{ .name = "pagerank", .freshness = .fresh, .seed_nodes = &too_many_seed_keys }};
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphMetricQueryShape(too_many_seeds));

    // A personalized dependency carries no publication policy: its column is
    // computed fresh rather than read from a published generation.
    var ordered_fresh = fresh_seeded;
    ordered_fresh.order_by = &.{.{ .name = "pagerank", .freshness = .fresh }};
    const plan = try MetricReadPlan.init(ordered_fresh);
    try std.testing.expect(plan.personalizationFor("pagerank") != null);
    try std.testing.expect(!plan.policies[0].require_published and !plan.policies[0].require_fresh);

    // Ordering or filtering a personalized metric with published freshness
    // would mix global and seeded scores; fail closed.
    var ordered_published = fresh_seeded;
    ordered_published.order_by = &.{.{ .name = "pagerank", .freshness = .published }};
    try std.testing.expectError(error.GraphMetricPersonalizationRequiresFresh, MetricReadPlan.init(ordered_published));
    var filtered_published = fresh_seeded;
    filtered_published.where_metric = &.{.{ .name = "pagerank", .op = .gte, .value = 0.1, .freshness = .published }};
    try std.testing.expectError(error.GraphMetricPersonalizationRequiresFresh, MetricReadPlan.init(filtered_published));
}

test "graph metric personalized reads fail closed on readers without a seeded kernel" {
    const alloc = std.testing.allocator;
    const seeds = [_][]const u8{"a"};
    const query = GraphQuery{
        .query_type = .neighbors,
        .index_name = "graph",
        .start_nodes = .{ .keys = &.{} },
        .metrics = &.{.{ .name = "rank", .freshness = .fresh, .seed_nodes = &seeds }},
    };
    const nodes = [_]GraphResultNode{.{ .key = "a", .depth = 0, .distance = 0 }};
    var work = try GraphQueryEngine.MetricStageWorkspace.init(alloc, try MetricReadPlan.init(query), nodes.len);
    defer work.deinit();
    // Models the serverless published-segment reader: readColumns only.
    const Reader = struct {
        pub fn readColumns(_: *@This(), _: Allocator, _: []const []const u8, _: []const []const u8, _: []const []?f64) !void {
            return error.TestUnexpectedResult;
        }
    };
    var reader = Reader{};
    try std.testing.expectError(error.GraphMetricPersonalizationUnsupported, work.ensure(&reader, &.{"rank"}, &nodes));
}

test "personalized graph metric reads compute seeded pagerank at query time" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const metrics = [_]graph_mod.GraphMetricConfig{.{ .name = "rank", .kind = .pagerank }};
    const ctx = try setupGraphWithOptions(alloc, "gq-metric-ppr-s", "gq-metric-ppr-r", &sb, &rb, .{ .metric_configs = &metrics });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // Seed cluster A<->B bridged to hub H, which dominates global PageRank.
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "A", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "H", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("D", "H", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("E", "H", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("F", "H", "e", 1.0, 0, 0, "");

    // No published generation exists: the personalized column must be
    // computed fresh, and absent seeds must be skipped without failing.
    const seeds = [_][]const u8{ "A", "missing-node" };
    const reads = [_]GraphMetricRead{.{ .name = "rank", .freshness = .fresh, .seed_nodes = &seeds, .damping = 0.9 }};
    const start_keys: []const []const u8 = &.{"A"};
    const query = GraphQuery{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .edge_types = &.{"e"}, .direction = .out, .max_depth = 3, .max_results = 16 },
        .metrics = &reads,
    };

    var engine = GraphQueryEngine{ .alloc = alloc };
    var result = try engine.execute(&ctx.graph, query, start_keys);
    defer result.deinit(alloc);

    var seed_neighbor_score: ?f64 = null;
    var hub_score: ?f64 = null;
    for (result.nodes) |node| {
        try std.testing.expectEqual(@as(usize, 1), node.metricCount());
        if (std.mem.eql(u8, node.key, "B")) seed_neighbor_score = node.metricAt(0).score;
        if (std.mem.eql(u8, node.key, "H")) hub_score = node.metricAt(0).score;
    }
    // Teleport mass restricted to seed A keeps its one-hop neighbor above the
    // globally dominant hub.
    try std.testing.expect(seed_neighbor_score.? > hub_score.?);

    // Deterministic across runs for a fixed seed set.
    var replay = try engine.execute(&ctx.graph, query, start_keys);
    defer replay.deinit(alloc);
    try std.testing.expectEqual(result.nodes.len, replay.nodes.len);
    for (result.nodes, replay.nodes) |expected, actual| {
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqual(expected.metricAt(0).score, actual.metricAt(0).score);
    }

    // Direct column read reports how many seeds resolved.
    var column: [2]?f64 = undefined;
    const resolution = try ctx.graph.personalizedPageRankColumnInto(alloc, "rank", &seeds, 0.9, &.{ "B", "missing-node" }, &column);
    try std.testing.expectEqual(@as(usize, 2), resolution.requested);
    try std.testing.expectEqual(@as(usize, 1), resolution.matched);
    try std.testing.expectEqual(@as(usize, 1), resolution.skipped());
    try std.testing.expect(column[0] != null);
    try std.testing.expectEqual(@as(?f64, null), column[1]);
}

test "graph metric staged query admits scratch and output and pins publication" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const configs = [_]graph_mod.GraphMetricConfig{.{ .name = "degree", .kind = .degree }};
    const ctx = try setupGraphWithOptions(alloc, "gq-staged-budget-s", "gq-staged-budget-r", &sb, &rb, .{ .metric_configs = &configs });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }
    try ctx.graph.addEdge("A", "B", "e", 1, 0, 0, "");
    var published = try ctx.graph.runDegreeMetric("degree");
    published.deinit(alloc);
    var session = try ctx.graph.openGraphMetricReadSession(&.{"degree"}, &.{.{ .require_fresh = true }});
    defer session.deinit();
    var scores: [1]?f64 = undefined;
    try session.readColumns(alloc, &.{"degree"}, &.{"A"}, &.{&scores});
    try std.testing.expectEqual(@as(?f64, 1), scores[0]);
    try ctx.graph.addEdge("A", "C", "e", 1, 0, 0, "");
    published = try ctx.graph.runDegreeMetric("degree");
    published.deinit(alloc);
    try session.readColumns(alloc, &.{"degree"}, &.{"A"}, &.{&scores});
    try std.testing.expectEqual(@as(?f64, 1), scores[0]);

    const Runner = struct {
        fn run(out_alloc: Allocator, index: *graph_mod.GraphIndex, maximum: usize) !void {
            var result = GraphQueryResult{ .nodes = try out_alloc.alloc(GraphResultNode, 1) };
            result.nodes[0] = .{ .key = out_alloc.dupe(u8, "A") catch |err| {
                out_alloc.free(result.nodes);
                return err;
            }, .depth = 0, .distance = 0 };
            defer result.deinit(out_alloc);
            var budget = work_budget_mod.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = maximum });
            var engine = GraphQueryEngine{ .alloc = out_alloc, .work_budget = &budget };
            const query = GraphQuery{ .query_type = .neighbors, .index_name = "graph", .start_nodes = .{ .keys = &.{"A"} }, .metrics = &.{.{ .name = "degree" }} };
            engine.applyMetricDependenciesColumnar(index, query, false, &result) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), budget.retained_state_bytes);
                return err;
            };
            try std.testing.expect(budget.retained_state_bytes > 0 and budget.retained_state_bytes <= maximum);
            try std.testing.expectEqual(@as(?f64, 2), result.nodes[0].metricAt(0).score);
        }
    };
    try std.testing.expectError(error.GraphWorkBudgetExceeded, Runner.run(alloc, &ctx.graph, 1));
    for ([_]usize{ 64, 256, 512, 1024, 2048, 4096, 8192 }) |maximum| {
        Runner.run(alloc, &ctx.graph, maximum) catch |err| {
            try std.testing.expectEqual(error.GraphWorkBudgetExceeded, err);
        };
    }
    try Runner.run(alloc, &ctx.graph, 64 * 1024);
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{ &ctx.graph, @as(usize, 64 * 1024) });
}

test "graph metric order and filter dependencies attach status without projection" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const metrics = [_]graph_mod.GraphMetricConfig{.{
        .name = "degree",
        .kind = .degree,
    }};
    const ctx = try setupGraphWithOptions(alloc, "gq-metric-deps-s", "gq-metric-deps-r", &sb, &rb, .{ .metric_configs = &metrics });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    var degree_status = try ctx.graph.runDegreeMetric("degree");
    degree_status.deinit(alloc);

    const metric_orders = [_]GraphMetricOrder{.{
        .name = "degree",
        .direction = .desc,
        .freshness = .published,
    }};
    const metric_filters = [_]GraphMetricFilter{.{
        .name = "degree",
        .op = .gte,
        .value = 1.0,
        .freshness = .published,
    }};

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .neighbors,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .edge_types = &.{"e"}, .direction = .out, .max_results = 8 },
        .order_by = &metric_orders,
        .where_metric = &metric_filters,
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), result.nodes[0].metricCount());
    try std.testing.expectEqual(@as(usize, 1), result.metric_status.len);
    try std.testing.expectEqualStrings("degree", result.metric_status[0].name);
    try std.testing.expect(result.metric_status[0].published_generation != 0);
}

test "graph metric order and filter apply max results after metric processing" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const metrics = [_]graph_mod.GraphMetricConfig{.{ .name = "degree", .kind = .degree }};
    const ctx = try setupGraphWithOptions(alloc, "gq-metric-limit-s", "gq-metric-limit-r", &sb, &rb, .{ .metric_configs = &metrics });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "X", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "Y", "e", 1.0, 0, 0, "");
    var degree_status = try ctx.graph.runDegreeMetric("degree");
    degree_status.deinit(alloc);

    const metric_orders = [_]GraphMetricOrder{.{ .name = "degree", .direction = .desc, .freshness = .published }};
    const metric_filters = [_]GraphMetricFilter{.{ .name = "degree", .op = .gte, .value = 3.0, .freshness = .published }};
    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .neighbors,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .edge_types = &.{"e"}, .direction = .out, .max_results = 1, .include_paths = true },
        .order_by = &metric_orders,
        .where_metric = &metric_filters,
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expect(isCanonicalResultNode(result.nodes[0]));
    try std.testing.expectEqual(@as(usize, 2), result.nodes[0].path.?.len);
    try std.testing.expect(result.nodes[0].path_tables == null);
}

test "shortest path metric filtering evaluates the complete bounded candidate set" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const metrics = [_]graph_mod.GraphMetricConfig{.{ .name = "degree", .kind = .degree }};
    const ctx = try setupGraphWithOptions(alloc, "gq-metric-shortest-s", "gq-metric-shortest-r", &sb, &rb, .{ .metric_configs = &metrics });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "X", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "Y", "e", 1.0, 0, 0, "");
    var degree_status = try ctx.graph.runDegreeMetric("degree");
    degree_status.deinit(alloc);

    const filters = [_]GraphMetricFilter{.{ .name = "degree", .op = .gte, .value = 3.0, .freshness = .published }};
    var engine = GraphQueryEngine{ .alloc = alloc };
    const starts: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .shortest_path,
        .index_name = "test",
        .start_nodes = .{ .keys = starts },
        .target_nodes = .{ .keys = &.{ "B", "C" } },
        .params = .{ .max_depth = 2, .max_results = 1 },
        .where_metric = &filters,
    }, starts);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
}

test "pattern metric filtering evaluates matches beyond the response limit" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const metrics = [_]graph_mod.GraphMetricConfig{.{ .name = "degree", .kind = .degree }};
    const ctx = try setupGraphWithOptions(alloc, "gq-metric-pattern-s", "gq-metric-pattern-r", &sb, &rb, .{ .metric_configs = &metrics });
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "X", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "Y", "e", 1.0, 0, 0, "");
    var degree_status = try ctx.graph.runDegreeMetric("degree");
    degree_status.deinit(alloc);

    const filters = [_]GraphMetricFilter{.{ .name = "degree", .op = .gte, .value = 3.0, .freshness = .published }};
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "neighbor", .edge = .{ .direction = .out } },
    };
    var engine = GraphQueryEngine{ .alloc = alloc };
    const starts: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = starts },
        .pattern = steps,
        .params = .{ .max_results = 1 },
        .where_metric = &filters,
    }, starts);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
}

test "k_shortest_paths via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq4s", "gq4r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 2,
        .params = .{ .max_depth = 2, .include_paths = true, .weight_mode = .min_weight },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    // First path should be shorter/lighter
    try std.testing.expect(result.nodes[0].distance <= result.nodes[1].distance);

    var limited = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 2,
        .params = .{ .max_depth = 2, .include_paths = true, .weight_mode = .min_weight, .max_results = 1 },
    }, start_keys);
    defer limited.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), limited.nodes.len);
    // The response limit applies across start/target pairs on both ordinary
    // pathfinders, not independently to each pair or only to algebraic paths.
    for ([_]QueryType{ .shortest_path, .k_shortest_paths }) |query_type| {
        var bounded = try engine.execute(&ctx.graph, .{
            .query_type = query_type,
            .index_name = "test",
            .start_nodes = .{ .keys = &.{ "A", "B" } },
            .target_nodes = .{ .keys = &.{ "C", "D" } },
            .k = 64,
            .params = .{ .max_depth = 2, .weight_mode = .min_weight, .max_results = 1 },
        }, &.{ "A", "B" });
        defer bounded.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), bounded.nodes.len);
        try std.testing.expectEqualStrings("C", bounded.nodes[0].key);
    }
}

test "path result node conversion preserves endpoint semantics and allocation safety" {
    var nodes = [_][]const u8{ "A", "B" };
    var edges = [_]paths_mod.PathEdge{.{
        .source = "A",
        .target = "B",
        .edge_type = "links",
        .weight = 2,
        .metadata = "{\"visible\":true}",
    }};
    const path = paths_mod.Path{
        .nodes = &nodes,
        .edges = &edges,
        .total_weight = 2,
        .length = 1,
    };
    const Runner = struct {
        fn run(alloc: Allocator, source: paths_mod.Path) !void {
            var node = try pathToResultNode(alloc, &source);
            defer node.deinit(alloc);
            try std.testing.expectEqualStrings("B", node.key);
            try std.testing.expectEqual(@as(u32, 1), node.depth);
            try std.testing.expectEqualStrings("links", node.path_edges.?[0].edge_type);
            try std.testing.expectEqualStrings("{\"visible\":true}", node.path_edges.?[0].metadata);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{path});
}

test "k_shortest_paths k one can execute through algebraic shortest path proof" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-k1-shortest-s", "gq-alg-k1-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 3.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 1,
        .params = .{ .max_depth = 3 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
    try std.testing.expectEqualStrings("C", result.nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), result.nodes[0].depth);
    try std.testing.expectEqual(@as(f64, 2.0), result.nodes[0].distance);
    const provenance = result.nodes[0].provenance orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("A\x1fe\x1fB", provenance[0]);
    try std.testing.expectEqualStrings("B\x1fe\x1fC", provenance[1]);
}

test "k_shortest_paths k greater than one stays on normal pathfinder" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-k2-shortest-s", "gq-alg-k2-shortest-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 2.0, 0, 0, "");
    try ctx.graph.addEdge("D", "C", "e", 2.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .k_shortest_paths,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = &.{"C"} },
        .k = 2,
        .params = .{ .max_depth = 2 },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expect(result.nodes[0].provenance == null);
    try std.testing.expect(result.nodes[1].provenance == null);
}

test "edge_type filtering in traverse" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq5s", "gq5r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    try ctx.graph.addEdge("A", "B", "knows", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "likes", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "knows", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    const et: []const []const u8 = &.{"knows"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{ .edge_types = et, .max_depth = 1 },
    }, start_keys);
    defer result.deinit(alloc);

    // Only "knows" edges: B and D, not C
    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
}

test "empty result: no edges from start" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq6s", "gq6r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // No edges added

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"lonely"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), result.nodes.len);
}

test "pattern: 3-step linear chain via engine" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gqp1s", "gqp1r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    // A -e-> B -e-> C, A -e-> D (D has no outgoing edges)
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    // 3-step pattern: step0 binds start node, step1 hops out, step2 hops out.
    // So we're looking for start -> 1-hop -> 2-hop chains.
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "hop1", .edge = .{ .direction = .out } },
        .{ .alias = "hop2", .edge = .{ .direction = .out } },
    };
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys);
    defer result.deinit(alloc);

    // Only one 2-hop chain from A: A->B->C. D has no outgoing edges.
    try std.testing.expectEqual(@as(usize, 1), result.matches.len);

    // Match should bind start=A, hop1=B, hop2=C.
    const match = result.matches[0];
    try std.testing.expectEqual(@as(usize, 3), match.bindings.len);

    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (match.bindings) |b| {
        if (std.mem.eql(u8, b.alias, "start") and std.mem.eql(u8, b.key, "A")) found_a = true;
        if (std.mem.eql(u8, b.alias, "hop1") and std.mem.eql(u8, b.key, "B")) found_b = true;
        if (std.mem.eql(u8, b.alias, "hop2") and std.mem.eql(u8, b.key, "C")) found_c = true;
    }
    try std.testing.expect(found_a);
    try std.testing.expect(found_b);
    try std.testing.expect(found_c);

    // Result nodes include all unique keys from match bindings.
    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
}

test "pattern can execute through algebraic provenance semiring for unique linear chain" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-pattern-s", "gq-alg-pattern-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "D", "e", 1.0, 0, 0, "");

    const start_keys: []const []const u8 = &.{"A"};
    const edge_types: []const []const u8 = &.{"e"};
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "hop1", .edge = .{ .direction = .out, .types = edge_types } },
        .{ .alias = "hop2", .edge = .{ .direction = .out, .types = edge_types }, .node_filter = .{ .filter_prefix = "C" } },
    };

    var engine = GraphQueryEngine{ .alloc = alloc };
    var algebraic_result = (try engine.executeAlgebraicPattern(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys)) orelse return error.TestExpectedEqual;
    defer algebraic_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), algebraic_result.matches.len);
    try std.testing.expectEqual(@as(usize, 2), algebraic_result.matches[0].path.len);
    try std.testing.expectEqualStrings("A", algebraic_result.matches[0].path[0].source);
    try std.testing.expectEqualStrings("B", algebraic_result.matches[0].path[0].target);
    try std.testing.expectEqualStrings("C", algebraic_result.matches[0].path[1].target);
    try std.testing.expectEqual(@as(u32, 0), algebraic_result.matches[0].bindings[0].depth);
    try std.testing.expectEqual(@as(u32, 1), algebraic_result.matches[0].bindings[1].depth);
    try std.testing.expectEqual(@as(u32, 2), algebraic_result.matches[0].bindings[2].depth);
    try std.testing.expectEqual(@as(usize, 3), algebraic_result.nodes.len);
}

test "pattern algebraic provenance falls back for ambiguous linear chain" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-alg-pattern-amb-s", "gq-alg-pattern-amb-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = true;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "D", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("A", "C", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("C", "D", "e", 1.0, 0, 0, "");

    const start_keys: []const []const u8 = &.{"A"};
    const edge_types: []const []const u8 = &.{"e"};
    const steps: []const pattern_mod.PatternStep = &.{
        .{ .alias = "start", .edge = .{ .direction = .out } },
        .{ .alias = "mid", .edge = .{ .direction = .out, .types = edge_types } },
        .{ .alias = "end", .edge = .{ .direction = .out, .types = edge_types } },
    };

    var engine = GraphQueryEngine{ .alloc = alloc };
    try std.testing.expect((try engine.executeAlgebraicPattern(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys)) == null);

    var fallback_result = try engine.execute(&ctx.graph, .{
        .query_type = .pattern,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .pattern = steps,
    }, start_keys);
    defer fallback_result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), fallback_result.matches.len);
}

test "graph algebraic provenance preserves binary relationship identity" {
    const alloc = std.testing.allocator;
    const label = try graphEdgeProvenanceLabelAlloc(alloc, .{ .source = "a\x1f", .target = "b", .edge_type = "RELATES_TO", .edge_id = "fact\x00:1", .owner_document = "fact:1", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" });
    defer alloc.free(label);
    const parsed = parseProvenanceEdge(label) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("fact\x00:1", parsed.edge_id);
    try std.testing.expectEqualStrings("a\x1f", parsed.source);
    try std.testing.expectEqualStrings("fact:1", parsed.owner_document);
}

test "graph algebraic provenance public labels are lossless text at length boundaries" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 128, 255, 256, 65536 }) |length| {
        const source = try alloc.alloc(u8, length);
        defer alloc.free(source);
        @memset(source, 'a');
        const label = try graphEdgeProvenanceLabelAlloc(alloc, .{ .source = source, .target = "b", .edge_type = "R", .edge_id = "fact\xff\x00", .owner_document = "fact", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" });
        defer alloc.free(label);
        const text = try publicProvenanceLabelAlloc(alloc, label);
        defer alloc.free(text);
        try std.testing.expect(std.unicode.utf8ValidateSlice(text));
        const json = try std.json.Stringify.valueAlloc(alloc, .{ .provenance = &[_][]const u8{text} }, .{});
        defer alloc.free(json);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(text, parsed.value.object.get("provenance").?.array.items[0].string);
        const decoder = std.base64.url_safe_no_pad.Decoder;
        const payload = text[public_provenance_prefix.len..];
        const decoded = try alloc.alloc(u8, try decoder.calcSizeForSlice(payload));
        defer alloc.free(decoded);
        try decoder.decode(decoded, payload);
        try std.testing.expectEqualSlices(u8, label, decoded);
        const edge = parseProvenanceEdge(decoded).?;
        try std.testing.expectEqualStrings(source, edge.source);
        try std.testing.expectEqualStrings("fact\xff\x00", edge.edge_id);
    }
    // Protect the textual namespace from legacy source names using that prefix.
    const legacy = "antfly:graph-provenance:v1:a\x1fR\x1fb";
    const escaped = try publicProvenanceLabelAlloc(alloc, legacy);
    defer alloc.free(escaped);
    try std.testing.expect(!std.mem.eql(u8, legacy, escaped));
    for ([_][]const u8{ "a\x1fb", "\x02a" }) |source| {
        const label = try graphEdgeProvenanceLabelAlloc(alloc, .{ .source = source, .target = "b", .edge_type = "R", .edge_id = "", .owner_document = "", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" });
        defer alloc.free(label);
        const edge = parseProvenanceEdge(label).?;
        try std.testing.expectEqualStrings(source, edge.source);
        try std.testing.expectEqualStrings("", edge.edge_id);
    }
}

test "graph algebraic provenance traversal and shortest path return textual explicit identities" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-public-provenance-s", "gq-public-provenance-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }
    const source = @as([128]u8, @splat('a'));
    const id = @as([128]u8, @splat('i'));
    try ctx.graph.batchApply(&.{.{ .source = &source, .target = "b", .edge_type = "R", .edge_id = &id, .owner_document = "fact" }}, &.{});
    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{&source};
    for ([_]QueryType{ .traverse, .shortest_path }) |query_type| {
        for ([_]bool{ false, true }) |include_paths| {
            var result = try engine.execute(&ctx.graph, .{
                .query_type = query_type,
                .index_name = "test",
                .start_nodes = .{ .keys = start_keys },
                .target_nodes = .{ .keys = &.{"b"} },
                .params = .{ .max_depth = 1, .algebraic_semiring = true, .include_paths = include_paths },
            }, start_keys);
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), result.nodes.len);
            const labels = result.nodes[0].provenance.?;
            try std.testing.expectEqual(@as(usize, 1), labels.len);
            try std.testing.expect(std.mem.startsWith(u8, labels[0], public_provenance_prefix));
            const json = try std.json.Stringify.valueAlloc(alloc, .{ .provenance = labels }, .{});
            defer alloc.free(json);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(labels[0], parsed.value.object.get("provenance").?.array.items[0].string);
            if (result.nodes[0].path_edges) |edges| try std.testing.expectEqualStrings(&id, edges[0].edge_id);
        }
    }
}

test "graph algebraic provenance public conversion releases partial allocations" {
    const Case = struct {
        fn run(alloc: Allocator) !void {
            const label = try graphEdgeProvenanceLabelAlloc(alloc, .{ .source = "a", .target = "b", .edge_type = "R", .edge_id = "fact", .owner_document = "fact", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" });
            defer alloc.free(label);
            const token = try algebraic_path_mod.provenanceTokenAlloc(alloc, &.{label});
            defer alloc.free(token);
            var node = try algebraicTraversalResultNodeAlloc(alloc, .{ .node = @constCast("b"), .depth = 1, .provenance = token });
            defer node.deinit(alloc);
            const copy = try cloneProvenanceLabelsAlloc(alloc, &.{ label, "a\x1fR\x1fb", label });
            defer freeProvenanceLabels(alloc, copy);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "graph pattern path conversion releases partial relationship allocations" {
    const Case = struct {
        fn run(alloc: Allocator) !void {
            const edges = [_]PathEdgeInfo{.{
                .source = "a",
                .target = "b",
                .edge_type = "RELATES_TO",
                .edge_id = "fact:1",
                .owner_document = "fact:1",
                .weight = 2,
                .metadata = "{\"group_id\":\"g\"}",
                .traversal_direction = .in,
            }};
            const copy = try clonePatternPathEdgesFromInfoAlloc(alloc, &edges);
            defer {
                freeGraphPatternPathEdgeItems(alloc, copy);
                alloc.free(copy);
            }
            try std.testing.expectEqualStrings("fact:1", copy[0].edge_id);
            try std.testing.expectEqual(@as(?graph_mod.EdgeDirection, .in), copy[0].traversal_direction);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "BFS traversal preserves physical edge provenance for distributed paths" {
    const alloc = std.testing.allocator;
    var sb: [256]u8 = undefined;
    var rb: [256]u8 = undefined;
    const ctx = try setupGraph(alloc, "gq-bfs-path-return-s", "gq-bfs-path-return-r", &sb, &rb);
    defer {
        ctx.deinit();
        alloc.destroy(ctx);
    }

    ctx.graph.algebraic_semiring_traversal = false;
    try ctx.graph.addEdge("A", "B", "e", 1.0, 0, 0, "");
    try ctx.graph.addEdge("B", "C", "e", 1.0, 0, 0, "");

    var engine = GraphQueryEngine{ .alloc = alloc };
    const start_keys: []const []const u8 = &.{"A"};
    var result = try engine.execute(&ctx.graph, .{
        .query_type = .traverse,
        .index_name = "test",
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .max_depth = 2,
            .include_paths = true,
        },
    }, start_keys);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqualStrings("B", result.nodes[0].key);
    try std.testing.expect(result.nodes[0].path != null);
    try std.testing.expect(result.nodes[0].path_edges != null);
    try std.testing.expect(result.nodes[0].provenance == null);
    try std.testing.expectEqualStrings("C", result.nodes[1].key);
    const c_path = result.nodes[1].path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 3), c_path.len);
    try std.testing.expectEqualStrings("A", c_path[0]);
    try std.testing.expectEqualStrings("B", c_path[1]);
    try std.testing.expectEqualStrings("C", c_path[2]);
    const c_edges = result.nodes[1].path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), c_edges.len);
    try std.testing.expectEqualStrings("A", c_edges[0].source);
    try std.testing.expectEqualStrings("B", c_edges[0].target);
    try std.testing.expectEqualStrings("B", c_edges[1].source);
    try std.testing.expectEqualStrings("C", c_edges[1].target);
}

test "graph metric staged decoded columns share selection maps without copying scores" {
    const Runner = struct {
        const Reader = struct {
            keys: usize = 0,
            pub fn readDecodedColumns(self: *@This(), _: Allocator, names: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
                self.keys += names.len * keys.len;
                for (columns) |column| for (keys, column) |key, *value| {
                    value.* = @floatFromInt(key[0] - '0');
                };
            }
        };
        fn run(a: Allocator) !void {
            const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{"0"} }, .params = .{ .max_results = 2 }, .metrics = &.{ .{ .name = "rank" }, .{ .name = "display" } }, .order_by = &.{.{ .name = "rank" }}, .where_metric = &.{.{ .name = "rank", .op = .gte, .value = 2 }} };
            const nodes = [_]GraphResultNode{ .{ .key = "0", .depth = 0, .distance = 0 }, .{ .key = "1", .depth = 0, .distance = 0 }, .{ .key = "2", .depth = 0, .distance = 0 }, .{ .key = "3", .depth = 0, .distance = 0 }, .{ .key = "4", .depth = 0, .distance = 0 }, .{ .key = "5", .depth = 0, .distance = 0 }, .{ .key = "same", .table = "foreign", .depth = 0, .distance = 0 } };
            const plan = try MetricReadPlan.init(query);
            var work = try GraphQueryEngine.MetricStageWorkspace.init(a, plan, nodes.len);
            defer work.deinit();
            var reader = Reader{};
            try work.ensure(&reader, plan.filters.slice(), &nodes);
            const storage = work.columns[0].?.source.decoded.ptr;
            var filter = query;
            filter.metrics = &.{};
            filter.order_by = &.{};
            try work.select(filter, false, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
            var order = query;
            order.metrics = &.{};
            order.where_metric = &.{};
            try work.select(order, true, plan.orders.slice(), plan.projections.slice(), &.{});
            try std.testing.expectEqual(storage, work.columns[0].?.source.decoded.ptr);
            try work.ensure(&reader, plan.projections.slice(), &nodes);
            try std.testing.expectEqual(@as(usize, 8), reader.keys);
            try std.testing.expectEqualSlices(usize, &.{ 5, 4 }, work.rows);
            var columns: [2]GraphQueryEngine.MetricColumn = undefined;
            work.columnsFor(plan.projections.slice(), &columns);
            for (columns) |column| {
                try std.testing.expectEqual(@as(?f64, 5), column.at(0));
                try std.testing.expectEqual(@as(?f64, 4), column.at(1));
            }
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric fused selection matches staged missing qualified duplicate and tied rows" {
    const Runner = struct {
        const Reader = struct {
            max_cells: usize = 0,
            cells: usize = 0,
            fail: bool = false,
            pub fn readDecodedColumns(self: *@This(), _: Allocator, names: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
                self.max_cells = @max(self.max_cells, keys.len * names.len);
                self.cells += keys.len * names.len;
                for (names, columns) |name, column| for (keys, column) |key, *value| {
                    const n = try std.fmt.parseInt(usize, key, 10);
                    if (self.fail and n == 4096) return error.InvalidGraphMetricScore;
                    value.* = if ((name[0] != 'f' and n % 11 == 0) or (name[0] == 'f' and n % 17 == 0)) std.math.nan(f64) else @floatFromInt(n % (if (name[0] == 'f') @as(usize, 97) else if (name[0] == 'o') @as(usize, 31) else @as(usize, 19)));
                };
            }
        };
        fn run(a: Allocator, mode: usize) !void {
            const storage = try a.alloc([4]u8, 5000);
            defer a.free(storage);
            const nodes = try a.alloc(GraphResultNode, storage.len);
            defer a.free(nodes);
            for (storage, nodes, 0..) |*key, *node, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>4}", .{i / 2}), .table = if (i % 13 == 0) "foreign" else null, .depth = 0, .distance = 0 };
            var random = std.Random.DefaultPrng.init(0x1080);
            random.random().shuffle(GraphResultNode, nodes);
            const filters = [_]GraphMetricFilter{.{ .name = "f", .op = .gte, .value = 20 }};
            const apply_limit = mode < 6;
            const unlimited = mode >= 4;
            const orders = [_]GraphMetricOrder{ .{ .name = if (mode == 1) "f" else if (mode == 10) "unused" else "o", .direction = if (mode == 3 or mode == 8) .asc else .desc, .nulls = if (mode == 3 or mode >= 8) .first else .last }, .{ .name = if (mode == 10) "o" else if (mode == 11) "unused" else "f", .direction = .asc } };
            const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = if (mode == 4 or mode == 5) 0 else 9 }, .metrics = &.{ .{ .name = "f" }, .{ .name = "o" }, .{ .name = "p" } }, .where_metric = if (mode == 3) &.{} else &filters, .order_by = if (mode == 2 or mode == 4 or mode == 6) &.{} else orders[0..if (mode == 0 or mode >= 8) 2 else 1] };
            const plan = try MetricReadPlan.init(query);
            var staged = try GraphQueryEngine.MetricStageWorkspace.init(a, plan, nodes.len);
            defer staged.deinit();
            var reference = Reader{};
            if (plan.filters.len != 0) {
                try staged.ensure(&reference, plan.filters.slice(), nodes);
                var filter = query;
                filter.metrics = &.{};
                filter.order_by = &.{};
                try staged.select(filter, apply_limit and plan.orders.len == 0, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
            }
            if (plan.orders.len != 0) {
                try staged.ensure(&reference, plan.orders.slice(), nodes);
                var order = query;
                order.metrics = &.{};
                order.where_metric = &.{};
                try staged.select(order, apply_limit, plan.orders.slice(), plan.projections.slice(), &.{});
            }
            try staged.ensure(&reference, plan.projections.slice(), nodes);
            var fused = try GraphQueryEngine.MetricStageWorkspace.init(a, plan, nodes.len);
            defer fused.deinit();
            var reader = Reader{};
            try std.testing.expect(try fused.selectReading(&reader, query, apply_limit, nodes));
            try std.testing.expect(reader.max_cells <= 4096);
            try fused.ensure(&reader, plan.projections.slice(), nodes);
            if (unlimited or query.order_by.len != 0) try std.testing.expectEqual(reference.cells, reader.cells) else try std.testing.expect(reader.cells < reference.cells);
            try std.testing.expectEqualSlices(usize, staged.rows, fused.rows);
            var left: [3]GraphQueryEngine.MetricColumn = undefined;
            var right: [3]GraphQueryEngine.MetricColumn = undefined;
            staged.columnsFor(plan.projections.slice(), &left);
            fused.columnsFor(plan.projections.slice(), &right);
            for (left, right) |x, y| for (0..fused.rows.len) |row| try std.testing.expectEqual(x.at(row), y.at(row));
        }
    };
    for (0..12) |mode| try Runner.run(std.testing.allocator, mode);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{@as(usize, 0)});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{@as(usize, 5)});
    // A failure after successful batches must leave the workspace untouched.
    const a = std.testing.allocator;
    const storage = try a.alloc([4]u8, 5000);
    defer a.free(storage);
    const nodes = try a.alloc(GraphResultNode, storage.len);
    defer a.free(nodes);
    for (storage, nodes, 0..) |*key, *node, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>4}", .{i}), .depth = 0, .distance = 0 };
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 9 }, .metrics = &.{.{ .name = "o" }}, .order_by = &.{.{ .name = "o" }} };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(a, try MetricReadPlan.init(query), nodes.len);
    defer work.deinit();
    var reader = Runner.Reader{ .fail = true };
    try std.testing.expectError(error.InvalidGraphMetricScore, work.selectReading(&reader, query, true, nodes));
    try std.testing.expectEqual(nodes.len, work.rowCount());
    for (0..work.rowCount()) |i| try std.testing.expectEqual(i, work.rowAt(i));
    for (work.columns) |column| try std.testing.expect(column == null);
}

test "graph metric sparse compaction commits atomically and releases empty selections" {
    const Runner = struct {
        const Reader = struct {
            offset: usize = 0,
            pub fn readDecodedColumns(self: *@This(), _: Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
                for (columns) |column| {
                    for (keys, column, 0..) |_, *value, i| value.* = @floatFromInt(self.offset + i);
                }
                self.offset += keys.len;
            }
        };
        fn run(a: Allocator) !void {
            const nodes = try a.alloc(GraphResultNode, 4097);
            defer a.free(nodes);
            for (nodes) |*node| node.* = .{ .key = "node", .depth = 0, .distance = 0 };
            const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 3 }, .metrics = &.{ .{ .name = "a" }, .{ .name = "b" } }, .order_by = &.{.{ .name = "a" }} };
            const plan = try MetricReadPlan.init(query);
            var work = try GraphQueryEngine.MetricStageWorkspace.init(a, plan, nodes.len);
            defer work.deinit();
            var reader = Reader{};
            try work.ensure(&reader, plan.projections.slice(), nodes);
            work.select(query, true, plan.projections.slice(), plan.projections.slice(), &.{}) catch |err| {
                try std.testing.expectEqual(nodes.len, work.rowCount());
                try std.testing.expectEqual(nodes.len, work.columns[0].?.source.decoded.len);
                return err;
            };
            try std.testing.expectEqualSlices(usize, &.{ 4096, 4095, 4094 }, work.rows);
            for (work.columns[0..2]) |slot| {
                const column = slot.?.column(work.rowCount(), &work.groups);
                for (0..3) |row| try std.testing.expectEqual(@as(?f64, @floatFromInt(4096 - row)), column.at(row));
            }
            var empty = query;
            empty.where_metric = &.{.{ .name = "a", .op = .gt, .value = 99999 }};
            try work.select(empty, true, plan.projections.slice(), plan.projections.slice(), &.{});
            try std.testing.expectEqual(@as(usize, 0), work.rows.len);
            for (work.columns[0..2]) |slot| try std.testing.expectEqual(@as(usize, 0), slot.?.source.decoded.len);
        }
    };
    try Runner.run(std.testing.allocator);
    // Compaction may recover from OOM. Empty selection now needs no later
    // allocation, so successful recovery is legitimate; still inventory every
    // failure and require complete cleanup and identical selected scores.
    var stable = @import("../storage/lite/test_allocator.zig").NoResizeAllocator{ .backing = std.testing.allocator };
    var baseline = std.testing.FailingAllocator.init(stable.allocator(), .{});
    try Runner.run(baseline.allocator());
    for (0..baseline.alloc_index) |ordinal| {
        var failing = std.testing.FailingAllocator.init(stable.allocator(), .{ .fail_index = ordinal });
        Runner.run(failing.allocator()) catch |err| {
            if (err != error.OutOfMemory) return err;
        };
        try std.testing.expect(failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "graph metric fused eligibility preserves unlimited personalized and legacy readers" {
    const a = std.testing.allocator;
    const nodes = try a.alloc(GraphResultNode, 5000);
    defer a.free(nodes);
    for (nodes) |*node| node.* = .{ .key = "node", .depth = 0, .distance = 0 };
    var query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 9 }, .metrics = &.{.{ .name = "a" }}, .order_by = &.{.{ .name = "a" }} };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(a, try MetricReadPlan.init(query), nodes.len);
    defer work.deinit();
    const Reader = struct {
        pub fn readDecodedColumns(_: *@This(), _: Allocator, _: []const []const u8, _: []const []const u8, _: []const []f64) !void {
            return error.UnexpectedRead;
        }
    };
    var reader = Reader{};
    try std.testing.expect(!try work.selectReading(&reader, query, false, nodes));
    query.params.max_results = 0;
    try std.testing.expect(!try work.selectReading(&reader, query, true, nodes));
    query.params.max_results = 9;
    work.plan.personalizations[0] = .{ .seed_nodes = &.{"node"}, .damping = null };
    try std.testing.expect(!try work.selectReading(&reader, query, true, nodes));
    work.plan.personalizations[0] = null;
    const Legacy = struct {
        pub fn readColumns(_: *@This(), _: Allocator, _: []const []const u8, _: []const []const u8, _: []const []?f64) !void {
            return error.UnexpectedRead;
        }
    };
    var legacy = Legacy{};
    try std.testing.expect(!try work.selectReading(&legacy, query, true, nodes));
    try std.testing.expectEqual(nodes.len, work.rowCount());
    for (work.columns) |column| try std.testing.expect(column == null);
}

test "graph metric read radix ordering preserves binary keys long prefixes and qualified identities" {
    const a = std.testing.allocator;
    const storage = try a.alloc([96]u8, 4097);
    defer a.free(storage);
    const nodes = try a.alloc(GraphResultNode, storage.len);
    defer a.free(nodes);
    const rows = try a.alloc(usize, storage.len);
    defer a.free(rows);
    const reference = try a.alloc(usize, storage.len);
    defer a.free(reference);
    const seen = try a.alloc(bool, storage.len);
    defer a.free(seen);
    var random = std.Random.DefaultPrng.init(0x1080);
    for (0..3) |mode| {
        for (storage, nodes, 0..) |*bytes, *node, i| {
            random.random().bytes(bytes);
            if (mode != 0) @memset(bytes[0..80], 0);
            const len = if (mode == 0) i % 17 else if (mode == 1) 80 + i % 17 else 80;
            node.* = .{ .key = bytes[0..len], .table = if (i % 13 == 0) "foreign" else null, .depth = 0, .distance = 0 };
            rows[i] = i;
            reference[i] = i;
        }
        random.random().shuffle(usize, rows);
        GraphQueryEngine.sortMetricReadRows(rows, nodes);
        const Order = struct {
            nodes: []const GraphResultNode,
            fn less(ctx: @This(), left: usize, right: usize) bool {
                const x = ctx.nodes[left];
                const y = ctx.nodes[right];
                if ((x.table == null) != (y.table == null)) return x.table == null;
                return std.mem.order(u8, x.key, y.key) == .lt;
            }
        };
        std.mem.sort(usize, reference, Order{ .nodes = nodes }, Order.less);
        @memset(seen, false);
        for (rows, reference) |row, expected| {
            try std.testing.expect(row < nodes.len and !seen[row]);
            seen[row] = true;
            try std.testing.expectEqual(nodes[expected].table == null, nodes[row].table == null);
            if (nodes[row].table == null) try std.testing.expectEqualStrings(nodes[expected].key, nodes[row].key);
        }
    }
}

test "graph metric fused heap grows only with matches and rolls back failed growth" {
    const a = std.testing.allocator;
    const storage = try a.alloc([5]u8, 20000);
    defer a.free(storage);
    const nodes = try a.alloc(GraphResultNode, storage.len);
    defer a.free(nodes);
    for (storage, nodes, 0..) |*key, *node, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>5}", .{i}), .depth = 0, .distance = 0 };
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 5000 }, .metrics = &.{ .{ .name = "a" }, .{ .name = "b" }, .{ .name = "c" }, .{ .name = "d" } }, .where_metric = &.{ .{ .name = "a", .op = .gte, .value = 0 }, .{ .name = "b", .op = .gte, .value = 0 }, .{ .name = "c", .op = .gte, .value = 0 }, .{ .name = "d", .op = .gte, .value = 0 } }, .order_by = &.{.{ .name = "a" }} };
    const plan = try MetricReadPlan.init(query);
    const Reader = struct {
        mode: usize,
        pub fn readDecodedColumns(self: *@This(), _: Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (columns) |column| for (keys, column) |key, *value| {
                const n = try std.fmt.parseInt(usize, key, 10);
                value.* = if (self.mode == 0 or (self.mode == 1 and n != 19999)) std.math.nan(f64) else @floatFromInt(n);
            };
        }
    };
    for (0..3) |mode| {
        var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
        var work = try GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, nodes.len);
        var reader = Reader{ .mode = mode };
        try std.testing.expect(try work.selectReading(&reader, query, true, nodes));
        const expected: usize = if (mode == 0) 0 else if (mode == 1) 1 else 5000;
        try std.testing.expectEqual(expected, work.rows.len);
        var bytes: usize = 0;
        for (work.columns[0..4]) |slot| bytes += slot.?.source.decoded.len * @sizeOf(f64);
        if (expected == 0) {
            try std.testing.expectEqual(@as(usize, 0), bytes);
            try std.testing.expectEqual(@as(usize, 0), counter.live);
        } else {
            try std.testing.expect(bytes <= @max(@as(usize, 32768), expected * 4 * @sizeOf(f64) * 2));
            try std.testing.expectEqual(@as(usize, 19999), work.rows[0]);
            if (mode == 2) try std.testing.expectEqual(@as(usize, 15000), work.rows[4999]);
        }
        work.deinit();
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
    var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 250 * 1024 };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, nodes.len);
    var reader = Reader{ .mode = 2 };
    try std.testing.expectError(error.OutOfMemory, work.selectReading(&reader, query, true, nodes));
    for (0..work.rowCount()) |i| try std.testing.expectEqual(i, work.rowAt(i));
    for (work.columns) |column| try std.testing.expect(column == null);
    work.deinit();
    try std.testing.expectEqual(@as(usize, 0), counter.live);
}

test "graph metric traversal payload transfer is atomic and preserves owner pointers" {
    const a = std.testing.allocator;
    const key = try a.dupe(u8, "b");
    const table = try a.dupe(u8, "entities");
    const path = try clonePathNodesAlloc(a, &.{ "a", "b" });
    const edges = try a.alloc(paths_mod.PathEdge, 1);
    edges[0] = try paths_mod.clonePathEdge(a, .{ .source = "a", .target = "b", .edge_type = "e", .edge_id = "id", .owner_document = "owner", .metadata = "metadata", .weight = 1 });
    var source = traversal_mod.TraversalResult{ .key = key, .target_table = table, .path = path, .path_edges = edges, .depth = 1, .distance = 1, .total_weight = 1 };
    const bytes = try traversalGraphResultNodeOwnedBytes(source);
    var budget = work_budget_mod.WorkBudget.initWithLimits(.{ .max_retained_state_bytes = bytes - 16 });
    try budget.retainStateBytes(bytes - 16);
    source.retained_budget = &budget;
    source.retained_state_bytes = bytes - 16;
    try std.testing.expectError(error.GraphWorkBudgetExceeded, takeTraversalResultNode(&source, &budget));
    try std.testing.expectEqual(key.ptr, source.key.ptr);
    try std.testing.expect(source.retained_budget == &budget);
    budget.max_retained_state_bytes = bytes;
    var node = try takeTraversalResultNode(&source, &budget);
    defer node.deinit(a);
    try std.testing.expectEqual(bytes, budget.retained_state_bytes);
    try std.testing.expectEqual(key.ptr, node.key.ptr);
    try std.testing.expectEqual(table.ptr, node.table.?.ptr);
    try std.testing.expectEqual(path.ptr, node.path.?.ptr);
    try std.testing.expectEqual(edges.ptr, node.path_edges.?.ptr);
    try std.testing.expect(node.path_tables == null);
    traversal_mod.freeResults(a, &.{source});
    try std.testing.expectEqual(bytes, budget.retained_state_bytes);
    node.path_tables = try pathTablesFromTerminalAlloc(a, node.path.?.len, node.table);
    try std.testing.expect(isCanonicalResultNode(node));
}

test "graph metric identity workspace reserves no candidate row storage" {
    var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = std.testing.allocator, .limit = 0 };
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} } };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), try MetricReadPlan.init(query), 100000);
    defer work.deinit();
    try std.testing.expectEqual(@as(usize, 100000), work.rowCount());
    try std.testing.expectEqual(@as(usize, 0), work.rows.len);
    try std.testing.expectEqual(@as(usize, 99999), work.rowAt(99999));
    try std.testing.expectEqual(@as(usize, 0), counter.alloc_attempts);
}
test "graph metric full selection reuses node ownership and bounds final materialization" {
    const a = std.testing.allocator;
    const n = 50000;
    const selected = try a.alloc(usize, n);
    defer a.free(selected);
    const scores = try a.alloc(?f64, n);
    defer a.free(scores);
    @memset(scores, 1);
    for (0..2) |mode| {
        var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
        const alloc = counter.allocator();
        var nodes = try alloc.alloc(GraphResultNode, n);
        for (nodes, selected, 0..) |*node, *row, i| {
            node.* = .{ .key = &.{}, .depth = @intCast(i), .distance = 0 };
            row.* = if (mode == 0) i else n - i - 1;
        }
        const before = counter.live;
        const original_nodes = nodes.ptr;
        const slab = try GraphQueryEngine.materializeSelectedMetricColumns(alloc, &.{ "a", "b", "c", "d" }, &.{ scores, scores, scores, scores }, selected, &nodes);
        try std.testing.expectEqual(original_nodes, nodes.ptr);
        try std.testing.expect(counter.peak < n * (@sizeOf(GraphResultNode) + 4 * @sizeOf(GraphMetricValue)) + 100000);
        for (nodes, selected) |node, row| {
            try std.testing.expectEqual(@as(u32, @intCast(row)), node.depth);
            for (node.metrics) |value| try std.testing.expectEqual(@as(?f64, 1), value.score);
        }
        std.debug.print("REVIEW_MATERIALIZE mode={d} node_size={d} metric_size={d} input_bytes={d} peak_bytes={d} retained_bytes={d} allocations={d}\n", .{ mode, @sizeOf(GraphResultNode), @sizeOf(GraphMetricValue), before, counter.peak, counter.live, counter.alloc_calls });
        for (nodes) |*node| node.deinit(alloc);
        alloc.free(nodes);
        alloc.free(slab);
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "graph metric many predicate pages preserve minimum admission" {
    const a = std.testing.allocator;
    const n = 5000;
    const nodes = try a.alloc(GraphResultNode, n);
    defer a.free(nodes);
    const storage = try a.alloc([5]u8, n);
    defer a.free(storage);
    for (nodes, storage, 0..) |*node, *key, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>5}", .{n - i - 1}), .depth = 0, .distance = 0 };
    const names = [_][]const u8{ "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13", "f14", "f15" };
    var filters: [16]GraphMetricFilter = undefined;
    for (names, &filters) |name, *filter| filter.* = .{ .name = name, .op = .gte, .value = 1 };
    const request = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .where_metric = &filters, .metrics = &.{ .{ .name = "f0" }, .{ .name = "f1" } }, .params = .{ .max_results = 0 } };
    const Reader = struct {
        pub fn readDecodedColumns(_: *@This(), _: Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (columns) |column| {
                for (keys, column) |key, *value| value.* = if (try std.fmt.parseInt(usize, key, 10) == 0) 1 else 0;
            }
        }
    };
    for ([_]usize{ std.math.maxInt(usize), 96 * 1024 }) |limit| {
        var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = limit };
        var work = try GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), try MetricReadPlan.init(request), n);
        var reader = Reader{};
        var failure: ?anyerror = null;
        const fused = work.selectReading(&reader, request, true, nodes) catch |err| blk: {
            failure = err;
            break :blk false;
        };
        try std.testing.expect(failure == null);
        if (failure == null) {
            try std.testing.expect(fused);
            try std.testing.expectEqualSlices(usize, &.{n - 1}, work.rows);
        }
        std.debug.print("REVIEW_ADMISSION limit={d} peak_bytes={d} allocations={d} error={s}\n", .{ limit, counter.peak, counter.alloc_calls, if (failure) |err| @errorName(err) else "none" });
        work.deinit();
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "graph metric full permutation validates and commits after every required allocation" {
    const Runner = struct {
        fn run(a: Allocator, mode: usize) !void {
            const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
            var nodes = try a.alloc(GraphResultNode, names.len);
            var initialized: usize = 0;
            var slab_owner: ?[]GraphMetricValue = null;
            defer {
                for (nodes[0..initialized]) |*node| node.deinit(a);
                a.free(nodes);
                if (slab_owner) |slab| a.free(slab);
            }
            for (names, 0..) |name, i| {
                nodes[i] = .{ .key = try a.dupe(u8, name), .depth = 0, .distance = 0 };
                initialized += 1;
                nodes[i].metrics = try a.alloc(GraphMetricValue, 1);
                nodes[i].metrics[0] = .{ .name = &.{}, .name_owned = false };
                nodes[i].metrics[0] = .{ .name = try a.dupe(u8, "previous"), .score = 9 };
            }
            const original = nodes.ptr;
            var selected: [8]usize = undefined;
            for (&selected, 0..) |*row, i| row.* = if (mode == 0) i else if (mode == 1) 7 - i else (i + 3) % 8;
            var scores: [8]?f64 = undefined;
            for (&scores, selected) |*value, row| value.* = @floatFromInt(row);
            const slab = GraphQueryEngine.materializeSelectedMetricColumns(a, &.{"new"}, &.{&scores}, &selected, &nodes) catch |err| {
                try std.testing.expectEqual(original, nodes.ptr);
                for (nodes, names) |node, name| {
                    try std.testing.expectEqualStrings(name, node.key);
                    try std.testing.expectEqualStrings("previous", node.metrics[0].name);
                }
                return err;
            };
            slab_owner = slab;
            try std.testing.expectEqual(original, nodes.ptr);
            for (nodes, selected) |node, row| {
                try std.testing.expectEqualStrings(names[row], node.key);
                try std.testing.expectEqual(@as(?f64, @floatFromInt(row)), node.metricAt(0).score);
            }
        }
    };
    var stack: [8]GraphResultNode = @splat(.{ .key = "borrowed", .depth = 0, .distance = 0 });
    var nodes: []GraphResultNode = stack[0..];
    try std.testing.expectError(error.InvalidQueryRequest, GraphQueryEngine.materializeSelectedMetricColumns(std.testing.allocator, &.{}, &.{}, &.{ 0, 1, 2, 3, 4, 5, 6, 6 }, &nodes));
    try std.testing.expectEqual(stack[0..].ptr, nodes.ptr);
    for (0..3) |mode| {
        try Runner.run(std.testing.allocator, mode);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{mode});
    }
}

test "graph metric compact responses clone independently and preserve wire scores" {
    const a = std.testing.allocator;
    var names = [_][]u8{ @constCast("rank"), @constCast("display") };
    var scores = [_]f64{ 3, std.math.nan(f64), 4, 9 };
    var columns = GraphMetricColumns{ .names = &names, .scores = &scores, .rows = 2 };
    const nodes = [_]GraphResultNode{
        .{ .key = "a", .depth = 0, .distance = 0, .metric_columns = &columns, .metric_row = 0, .metrics_owned = false },
        .{ .key = "b", .depth = 0, .distance = 0, .metric_columns = &columns, .metric_row = 1, .metrics_owned = false },
    };
    const Runner = struct {
        fn run(alloc: Allocator, source: []const GraphResultNode) !void {
            var clone = try @import("../api/graph.zig").cloneGraphNodesWithMetrics(alloc, source);
            defer clone.deinit(alloc);
            try std.testing.expect(clone.metrics.columns != null);
            try std.testing.expect(clone.metrics.values.len == 0);
            try std.testing.expectEqual(@as(?f64, 3), clone.nodes[0].metricAt(0).score);
            try std.testing.expectEqual(@as(?f64, null), clone.nodes[1].metricAt(0).score);
            clone.metrics.columns.?.scores[0] = 27;
            try std.testing.expectEqual(@as(?f64, 3), source[0].metricAt(0).score);
            try std.testing.expectEqual(@as(?f64, 27), clone.nodes[0].metricAt(0).score);
        }
    };
    try Runner.run(a, &nodes);
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{&nodes});
    const bytes = try @import("../api/graph.zig").jsonStringifyAlloc(a, nodes);
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice([]GraphResultNode, a, bytes, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    try std.testing.expect(parsed.value[0].metric_columns == null);
    for (nodes, parsed.value) |source, restored| {
        try std.testing.expectEqual(source.metricCount(), restored.metricCount());
        for (0..source.metricCount()) |column| {
            try std.testing.expectEqualStrings(source.metricAt(column).name, restored.metricAt(column).name);
            try std.testing.expectEqual(source.metricAt(column).score, restored.metricAt(column).score);
        }
    }
}

test "graph metric compact response cloning allocation measurement" {
    const a = std.testing.allocator;
    const count = 50_000;
    var names = [_][]u8{ @constCast("rank"), @constCast("degree"), @constCast("hub"), @constCast("display") };
    const scores = try a.alloc(f64, count * names.len);
    defer a.free(scores);
    @memset(scores, 1);
    var columns = GraphMetricColumns{ .names = &names, .scores = scores, .rows = count };
    const nodes = try a.alloc(GraphResultNode, count);
    defer a.free(nodes);
    for (nodes, 0..) |*node, row| node.* = .{ .key = "00000", .depth = 0, .distance = 0, .metric_columns = &columns, .metric_row = @intCast(row) };
    var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
    var clone = try @import("../api/graph.zig").cloneGraphNodesWithMetrics(counter.allocator(), nodes);
    try std.testing.expectEqual(@as(usize, 0), clone.metrics.values.len);
    try std.testing.expectEqual(count * names.len * @sizeOf(f64), clone.metrics.columns.?.scores.len * @sizeOf(f64));
    std.debug.print("COMPACT_CLONE nodes={d} width={d} node_bytes={d} peak_bytes={d} allocations={d}\n", .{ count, names.len, @sizeOf(GraphResultNode), counter.peak, counter.alloc_calls });
    clone.deinit(counter.allocator());
    try std.testing.expectEqual(@as(usize, 0), counter.live);
}

test "graph metric compact materialization commits atomically and bounds response storage" {
    const Runner = struct {
        fn run(a: Allocator) !void {
            var result = GraphQueryResult{ .nodes = try a.alloc(GraphResultNode, 3) };
            for (result.nodes, 0..) |*node, row| node.* = .{ .key = &.{}, .depth = @intCast(row), .distance = 0 };
            defer result.deinit(a);
            for (result.nodes) |*node| node.key = try a.dupe(u8, "node");
            const names = try a.alloc([]u8, 1);
            var owned = false;
            defer if (!owned) a.free(names);
            names[0] = try a.dupe(u8, "rank");
            defer if (!owned) a.free(names[0]);
            const values = [_]?f64{ 10, null, 30 };
            const original = result.nodes.ptr;
            const views = [_][]const ?f64{&values};
            result.metric_columns = try GraphQueryEngine.materializeSelectedMetricColumnsCompact(a, a, a, names, &views, &.{ 2, 0, 1 }, &result.nodes);
            owned = true;
            try std.testing.expectEqual(original, result.nodes.ptr);
            for (result.nodes, values, [_]u32{ 2, 0, 1 }) |node, value, depth| {
                try std.testing.expectEqual(depth, node.depth);
                try std.testing.expectEqual(value, node.metricAt(0).score);
                try std.testing.expectEqual(@as(usize, 0), node.metrics.len);
            }
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric result clone and exclusion preserve compact owners and statuses" {
    const api = @import("../api/graph.zig");
    const types = @import("../storage/db/types.zig");
    var names = [_][]u8{@constCast("rank")};
    var scores = [_]f64{ 1, 2 };
    var columns = GraphMetricColumns{ .names = &names, .scores = &scores, .rows = 2 };
    var nodes = [_]GraphResultNode{
        .{ .key = "a", .depth = 0, .distance = 0, .metric_columns = &columns, .metric_row = 0 },
        .{ .key = "b", .depth = 0, .distance = 0, .metric_columns = &columns, .metric_row = 1 },
    };
    var statuses = [_]types.GraphMetricStatus{.{ .name = @constCast("rank"), .published_generation = 7, .last_error = "retained diagnostic" }};
    const source = types.GraphSearchResult{ .name = @constCast("graph"), .nodes = &nodes, .hits = &.{}, .total_hits = 2, .metric_status = &statuses };
    const Runner = struct {
        fn run(a: Allocator, input: types.GraphSearchResult) !void {
            for (0..2) |mode| {
                var result = try api.filterGraphSearchResult(a, "table", input, if (mode == 0) &.{} else &.{.{ .key = @constCast("a") }}, &.{});
                defer result.deinit(a);
                try std.testing.expectEqual(@as(usize, 2 - mode), result.nodes.len);
                try std.testing.expectEqual(@as(usize, 1), result.metric_status.len);
                try std.testing.expectEqual(@as(u64, 7), result.metric_status[0].published_generation);
                try std.testing.expectEqualStrings("retained diagnostic", result.metric_status[0].last_error);
                try std.testing.expectEqual(@as(?f64, @floatFromInt(mode + 1)), result.nodes[0].metricAt(0).score);
                try std.testing.expect(result.metric_columns != null);
                result.metric_columns.?.scores[0] = 17;
                try std.testing.expectEqual(@as(?f64, 1), input.nodes[0].metricAt(0).score);
            }
        }
    };
    try Runner.run(std.testing.allocator, source);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{source});
}

test "graph metric native response adoption transfers flat and paged backing atomically" {
    const Runner = struct {
        fn run(a: Allocator, paged: bool) !void {
            var budget = work_budget_mod.WorkBudget.initWithLimits(.{});
            var scratch_budget = work_budget_mod.RetainedAllocator{ .backing = a, .budget = &budget };
            var output_budget = work_budget_mod.RetainedAllocator{ .backing = a, .budget = &budget };
            defer std.debug.assert(scratch_budget.live_bytes == 0 and output_budget.live_bytes == 0);
            const scratch = scratch_budget.allocator();
            const output = output_budget.allocator();
            var result = GraphQueryResult{ .nodes = try a.alloc(GraphResultNode, 3) };
            for (result.nodes, 0..) |*node, row| node.* = .{ .key = "", .depth = @intCast(row), .distance = 0 };
            defer result.deinit(a);
            const plan = try MetricReadPlan.init(.{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &.{.{ .name = "rank" }}, .params = .{ .max_results = 0 } });
            var workspace = try GraphQueryEngine.MetricStageWorkspace.init(scratch, plan, 3);
            var workspace_live = true;
            defer if (workspace_live) workspace.deinit();
            var original: [*]f64 = undefined;
            if (paged) {
                const pages = try GraphQueryEngine.ScorePages.createForRows(scratch, 1, 4);
                var pages_owned = true;
                errdefer if (pages_owned) pages.destroy();
                try pages.ensureRows(3);
                pages.count = 3;
                for (0..3) |row| pages.set(0, row, @floatFromInt(row));
                original = pages.window(0, 0, 3).ptr;
                workspace.groups[0] = .{ .references = 1, .paged = pages };
                workspace.columns[workspace.index("rank")] = .{ .source = .{ .paged = .{ .owner = pages, .column = 0 } }, .group = 0 };
                pages_owned = false;
            } else {
                const scores = try scratch.alloc(f64, 3);
                for (scores, 0..) |*score, row| score.* = @floatFromInt(row);
                original = scores.ptr;
                workspace.groups[0] = .{ .references = 1 };
                workspace.columns[workspace.index("rank")] = .{ .source = .{ .decoded = scores }, .group = 0 };
            }
            const mapping = try scratch.dupe(usize, &.{ 2, 0, 1 });
            workspace.groups[0].mapping = mapping;
            const names = try output.alloc([]u8, 1);
            var names_owned = true;
            errdefer if (names_owned) output.free(names);
            names[0] = try output.dupe(u8, "rank");
            errdefer if (names_owned) output.free(names[0]);
            result.metric_columns = (try workspace.adoptProjectionColumns(a, output, scratch, &scratch_budget, &output_budget, names, &.{ 2, 0, 1 }, &result.nodes)).?;
            names_owned = false;
            const owner = result.metric_columns.?;
            try std.testing.expectEqual(original, if (owner.groups[0].pages) |pages| pages.window(0, 0, 3).ptr else owner.decoded[0].?.ptr);
            try std.testing.expectEqual(@as(?f64, 2), result.nodes[0].metricAt(0).score);
            try std.testing.expectEqual(@as(?f64, 0), result.nodes[1].metricAt(0).score);
            try std.testing.expect(result.nodes[0].metricViewValid());
            output_budget.detach();
            workspace.deinit();
            workspace_live = false;
        }
    };
    for ([_]bool{ false, true }) |paged| {
        try Runner.run(std.testing.allocator, paged);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{paged});
    }
}

test "graph metric response adoption allocation measurement" {
    const a = std.testing.allocator;
    const n = 50_000;
    const projection_names = [_][]const u8{ "rank", "degree", "hub", "display" };
    var projected: [4]GraphMetricRead = undefined;
    for (&projected, projection_names) |*metric, name| metric.* = .{ .name = name };
    const plan = try MetricReadPlan.init(.{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &projected });
    for (0..2) |mode| {
        var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
        const alloc = counter.allocator();
        var budget = work_budget_mod.WorkBudget.initWithLimits(.{});
        var scratch_budget = work_budget_mod.RetainedAllocator{ .backing = alloc, .budget = &budget };
        var output_budget = work_budget_mod.RetainedAllocator{ .backing = alloc, .budget = &budget };
        const scratch = scratch_budget.allocator();
        const output = output_budget.allocator();
        var result = GraphQueryResult{ .nodes = try alloc.alloc(GraphResultNode, n) };
        for (result.nodes) |*node| node.* = .{ .key = "", .depth = 0, .distance = 0 };
        var workspace = try GraphQueryEngine.MetricStageWorkspace.init(scratch, plan, n);
        workspace.groups[0] = .{ .references = 4 };
        for (projection_names) |name| {
            const scores = try scratch.alloc(f64, n);
            @memset(scores, 3.5);
            workspace.columns[workspace.index(name)] = .{ .source = .{ .decoded = scores }, .group = 0 };
        }
        const selected = try workspace.explicitRows();
        const names = try output.alloc([]u8, 4);
        for (names, projection_names) |*name, value| name.* = try output.dupe(u8, value);
        var views: [4]GraphQueryEngine.MetricColumn = undefined;
        workspace.columnsFor(&projection_names, &views);
        result.metric_columns = if (mode == 0) try GraphQueryEngine.materializeSelectedMetricColumnsCompact(alloc, output, scratch, names, &views, selected, &result.nodes) else (try workspace.adoptProjectionColumns(alloc, output, scratch, &scratch_budget, &output_budget, names, selected, &result.nodes)).?;
        output_budget.detach();
        workspace.deinit();
        try std.testing.expectEqual(@as(usize, 0), scratch_budget.live_bytes);
        try std.testing.expectEqual(@as(?f64, 3.5), result.nodes[n - 1].metricAt(3).score);
        std.debug.print("RESPONSE_ADOPTION mode={d} nodes={d} copied_score_bytes={d} peak_bytes={d} allocations={d} retained_bytes={d}\n", .{ mode, n, result.metric_columns.?.scores.len * @sizeOf(f64), counter.peak, counter.alloc_calls, counter.live });
        result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "graph metric clone keeps wide legacy arrays and rejects invalid compact views" {
    const a = std.testing.allocator;
    var values: [graph_metric_projection_limit + 1]GraphMetricValue = undefined;
    var names: [graph_metric_projection_limit + 1][8]u8 = undefined;
    for (&values, &names, 0..) |*value, *name, column| value.* = .{ .name = try std.fmt.bufPrint(name, "m{d}", .{column}), .score = @floatFromInt(column), .name_owned = false };
    const source = [_]GraphResultNode{.{ .key = "wide", .depth = 0, .distance = 0, .metrics = &values, .metrics_owned = false }};
    const Runner = struct {
        fn run(alloc: Allocator, nodes: []const GraphResultNode) !void {
            var clone = try @import("../api/graph.zig").cloneGraphNodesWithMetrics(alloc, nodes);
            defer clone.deinit(alloc);
            try std.testing.expect(clone.metrics.columns == null);
            try std.testing.expectEqual(@as(usize, graph_metric_projection_limit + 1), clone.nodes[0].metricCount());
            try std.testing.expectEqual(@as(?f64, 16), clone.nodes[0].metricAt(16).score);
        }
    };
    try Runner.run(a, &source);
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{&source});
    var dictionary = [_][]u8{@constCast("rank")};
    var scores = [_]f64{3.5};
    var invalid = GraphMetricColumns{ .names = &dictionary, .scores = &scores, .rows = 2 };
    const malformed = [_]GraphResultNode{.{ .key = "bad", .depth = 0, .distance = 0, .metric_columns = &invalid, .metric_row = 1 }};
    try std.testing.expectError(error.InvalidRemoteResponse, @import("../api/graph.zig").cloneGraphNodesWithMetrics(a, &malformed));
}

test "graph metric native response adoption preserves distinct group mappings" {
    const Runner = struct {
        fn run(a: Allocator) !void {
            var budget = work_budget_mod.WorkBudget.initWithLimits(.{});
            var source = work_budget_mod.RetainedAllocator{ .backing = a, .budget = &budget };
            var target = work_budget_mod.RetainedAllocator{ .backing = a, .budget = &budget };
            defer std.debug.assert(source.live_bytes == 0 and target.live_bytes == 0);
            const scratch = source.allocator();
            const output = target.allocator();
            var result = GraphQueryResult{ .nodes = try a.alloc(GraphResultNode, 3) };
            for (result.nodes) |*node| node.* = .{ .key = "", .depth = 0, .distance = 0 };
            defer result.deinit(a);
            const plan = try MetricReadPlan.init(.{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &.{ .{ .name = "rank" }, .{ .name = "centrality" } }, .params = .{ .max_results = 0 } });
            var workspace = try GraphQueryEngine.MetricStageWorkspace.init(scratch, plan, 3);
            defer workspace.deinit();
            const values = try scratch.dupe(f64, &.{ 0, 1, 2 });
            workspace.groups[0] = .{ .references = 1 };
            workspace.columns[workspace.index("rank")] = .{ .source = .{ .decoded = values }, .group = 0 };
            workspace.groups[0].mapping = try scratch.dupe(usize, &.{ 2, 0, 1 });
            const pages = try GraphQueryEngine.ScorePages.createForRows(scratch, 1, 4);
            workspace.groups[1] = .{ .references = 1, .paged = pages };
            workspace.columns[workspace.index("centrality")] = .{ .source = .{ .paged = .{ .owner = pages, .column = 0 } }, .group = 1 };
            try pages.ensureRows(3);
            pages.count = 3;
            for (0..3) |row| pages.set(0, row, @floatFromInt(10 * row));
            workspace.groups[1].mapping = try scratch.dupe(usize, &.{ 1, 2, 0 });
            const names = try output.alloc([]u8, 2);
            var initialized: usize = 0;
            var owned = true;
            errdefer if (owned) {
                for (names[0..initialized]) |name| output.free(name);
                output.free(names);
            };
            for ([_][]const u8{ "rank", "centrality" }, names) |name, *dest| {
                dest.* = try output.dupe(u8, name);
                initialized += 1;
            }
            result.metric_columns = (try workspace.adoptProjectionColumns(a, output, scratch, &source, &target, names, &.{ 2, 0, 1 }, &result.nodes)).?;
            owned = false;
            const owner = result.metric_columns.?;
            try std.testing.expectEqual(values.ptr, owner.decoded[0].?.ptr);
            try std.testing.expectEqual(pages, owner.groups[owner.column_groups[1]].pages.?);
            try std.testing.expectEqual(@as(?f64, 2), result.nodes[0].metricAt(0).score);
            try std.testing.expectEqual(@as(?f64, 10), result.nodes[0].metricAt(1).score);
            try std.testing.expectEqual(@as(?f64, 20), result.nodes[1].metricAt(1).score);
            for (result.nodes) |node| try std.testing.expect(node.metricViewValid());
            target.detach();
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "graph metric native response adoption multiple group allocation measurement" {
    const a = std.testing.allocator;
    const n = 50_000;
    const plan = try MetricReadPlan.init(.{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &.{ .{ .name = "rank" }, .{ .name = "degree" } } });
    for (0..2) |mode| {
        var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
        const alloc = counter.allocator();
        var budget = work_budget_mod.WorkBudget.initWithLimits(.{});
        var sb = work_budget_mod.RetainedAllocator{ .backing = alloc, .budget = &budget };
        var ob = work_budget_mod.RetainedAllocator{ .backing = alloc, .budget = &budget };
        var result = GraphQueryResult{ .nodes = try alloc.alloc(GraphResultNode, n) };
        for (result.nodes) |*node| node.* = .{ .key = "", .depth = 0, .distance = 0 };
        var work = try GraphQueryEngine.MetricStageWorkspace.init(sb.allocator(), plan, n);
        for ([_][]const u8{ "rank", "degree" }, 0..) |name, column| {
            const values = try sb.allocator().alloc(f64, n);
            @memset(values, @floatFromInt(column + 1));
            const group: u8 = if (mode == 0) 0 else @intCast(column);
            work.groups[group].references += 1;
            work.columns[work.index(name)] = .{ .source = .{ .decoded = values }, .group = group };
        }
        const selected = try work.explicitRows();
        const names = try ob.allocator().alloc([]u8, 2);
        names[0] = try ob.allocator().dupe(u8, "rank");
        names[1] = try ob.allocator().dupe(u8, "degree");
        var views: [2]GraphQueryEngine.MetricColumn = undefined;
        work.columnsFor(&.{ "rank", "degree" }, &views);
        result.metric_columns = try work.adoptProjectionColumns(alloc, ob.allocator(), sb.allocator(), &sb, &ob, names, selected, &result.nodes);
        const adopted = result.metric_columns != null;
        try std.testing.expect(adopted);
        try std.testing.expectEqual(@as(usize, 0), result.metric_columns.?.scores.len);
        if (!adopted) result.metric_columns = try GraphQueryEngine.materializeSelectedMetricColumnsCompact(alloc, ob.allocator(), sb.allocator(), names, &views, selected, &result.nodes);
        ob.detach();
        work.deinit();
        try std.testing.expectEqual(@as(?f64, 2), result.nodes[n - 1].metricAt(1).score);
        std.debug.print("MULTI_GROUP_ADOPTION groups={d} adopted={} copied_score_bytes={d} peak_bytes={d} allocations={d}\n", .{ mode + 1, adopted, result.metric_columns.?.scores.len * 8, counter.peak, counter.alloc_calls });
        result.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "graph metric native response adoption followup staged identity maps" {
    const a = std.testing.allocator;
    const n = 50_000;
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &.{ .{ .name = "rank", .freshness = .fresh, .seed_nodes = &.{"s"} }, .{ .name = "degree" } }, .where_metric = &.{.{ .name = "rank", .op = .gte, .value = 0, .freshness = .fresh }}, .params = .{ .max_results = 0 } };
    const plan = try MetricReadPlan.init(query);
    var source_budget = work_budget_mod.RetainedAllocator{ .backing = a, .budget = null };
    var target_budget = work_budget_mod.RetainedAllocator{ .backing = a, .budget = null };
    var result = GraphQueryResult{ .nodes = try a.alloc(GraphResultNode, n) };
    defer result.deinit(a);
    for (result.nodes) |*node| node.* = .{ .key = "", .depth = 0, .distance = 0 };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(source_budget.allocator(), plan, n);
    defer work.deinit();
    for ([_][]const u8{ "rank", "degree" }, 0..) |name, column| {
        if (column != 0) continue;
        const values = try source_budget.allocator().alloc(f64, n);
        @memset(values, @floatFromInt(column + 1));
        work.groups[column] = .{ .references = 1 };
        work.columns[work.index(name)] = .{ .source = .{ .decoded = values }, .group = @intCast(column) };
    }
    try work.select(query, false, &.{"rank"}, &.{ "rank", "degree" }, &.{});
    const degree = try source_budget.allocator().alloc(f64, n);
    @memset(degree, 2);
    work.groups[1] = .{ .references = 1 };
    work.columns[work.index("degree")] = .{ .source = .{ .decoded = degree }, .group = 1 };
    var map_bytes: usize = 0;
    for (work.groups) |group| if (group.mapping) |mapping| {
        map_bytes += mapping.len * @sizeOf(usize);
    };
    const names = try target_budget.allocator().alloc([]u8, 2);
    names[0] = try target_budget.allocator().dupe(u8, "rank");
    names[1] = try target_budget.allocator().dupe(u8, "degree");
    const selected = try work.explicitRows();
    var views: [2]GraphQueryEngine.MetricColumn = undefined;
    work.columnsFor(&.{ "rank", "degree" }, &views);
    result.metric_columns = try work.adoptProjectionColumns(a, target_budget.allocator(), source_budget.allocator(), &source_budget, &target_budget, names, selected, &result.nodes);
    const adopted = result.metric_columns != null;
    if (!adopted) result.metric_columns = try GraphQueryEngine.materializeSelectedMetricColumnsCompact(a, target_budget.allocator(), source_budget.allocator(), names, &views, selected, &result.nodes);
    target_budget.detach();
    try std.testing.expectEqual(@as(usize, 0), map_bytes);
    try std.testing.expect(adopted);
    try std.testing.expectEqual(@as(usize, 0), result.metric_columns.?.scores.len);
    try std.testing.expectEqual(@as(?f64, 2), result.nodes[n - 1].metricAt(1).score);
    std.debug.print("FRESH_IDENTITY_MAP rows={d} map_bytes={d} adopted={} copied_scores={d}\n", .{ n, map_bytes, adopted, result.metric_columns.?.scores.len * @sizeOf(f64) });
}

test "graph metric native response adoption lazy all pass temporary rows" {
    const a = std.testing.allocator;
    var counter = @import("../storage/lite/test_allocator.zig").BudgetAllocator{ .backing = a };
    const n = 50_000;
    const query = GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .metrics = &.{.{ .name = "rank" }}, .where_metric = &.{.{ .name = "rank", .op = .gte, .value = 0 }}, .params = .{ .max_results = 0 } };
    var work = try GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), try MetricReadPlan.init(query), n);
    const values = try counter.allocator().alloc(f64, n);
    @memset(values, 1);
    work.groups[0] = .{ .references = 1 };
    work.columns[work.index("rank")] = .{ .source = .{ .decoded = values }, .group = 0 };
    const before = counter.live;
    const allocs = counter.alloc_calls;
    try work.select(query, false, &.{"rank"}, &.{"rank"}, &.{});
    try std.testing.expectEqual(n, work.rowCount());
    try std.testing.expect(work.groups[0].mapping == null);
    std.debug.print("REVIEW_ALL_PASS rows={d} extra_peak={d} temporary_allocations={d} retained_rows={d}\n", .{ n, counter.peak - before, counter.alloc_calls - allocs, work.row_storage.len });
    try std.testing.expectEqual(@as(usize, 0), counter.peak - before);
    try std.testing.expectEqual(@as(usize, 0), counter.alloc_calls - allocs);
    work.deinit();
    try std.testing.expectEqual(@as(usize, 0), counter.live);
}
