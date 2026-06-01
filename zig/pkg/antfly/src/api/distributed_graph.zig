// Copyright 2026 Antfly, Inc.
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
const raft_mod = @import("../raft/mod.zig");
const db_mod = @import("../storage/db/mod.zig");
const graph_query_mod = @import("../graph/query.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_paths_mod = @import("../graph/paths.zig");
const doc_set = @import("../storage/db/doc_set.zig");
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;
const algebraic_planner = db_mod.algebraic.planner;
const table_catalog = @import("table_catalog.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const platform_time = @import("../platform/time.zig");
const indexes_api = @import("indexes.zig");
const query_contract = @import("query_contract.zig");
const tables_api = @import("tables.zig");

pub const Worker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute_graph_expand: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!GraphExpandResponse,
        execute_graph_hydrate: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!GraphHydrateResponse,
        execute_graph_get_edges: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphEdgesRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!GraphEdgesResponse = null,
        fanout_io: ?*const fn (ptr: *anyopaque) ?std.Io = null,
        fanout_width_cap: ?*const fn (ptr: *anyopaque) usize = null,
    };

    pub fn executeGraphExpand(
        self: Worker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !GraphExpandResponse {
        return try self.vtable.execute_graph_expand(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn executeGraphHydrate(
        self: Worker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !GraphHydrateResponse {
        return try self.vtable.execute_graph_hydrate(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn executeGraphGetEdges(
        self: Worker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !GraphEdgesResponse {
        const func = self.vtable.execute_graph_get_edges orelse return error.UnsupportedQueryRequest;
        return try func(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn fanoutIo(self: Worker) ?std.Io {
        const func = self.vtable.fanout_io orelse return null;
        return func(self.ptr);
    }

    pub fn fanoutWidthCap(self: Worker) ?usize {
        const func = self.vtable.fanout_width_cap orelse return null;
        return func(self.ptr);
    }
};

const GraphFanoutPlan = struct {
    parallel: bool,
    width: usize,
    reason: Reason,

    const Reason = enum {
        no_io,
        single_batch,
        width_cap,
        parallel,
    };
};

pub const GraphFanoutMetricsSnapshot = struct {
    expand_parallel_total: u64 = 0,
    expand_parallel_ns_total: u64 = 0,
    expand_planned_parallel_total: u64 = 0,
    expand_planned_sequential_total: u64 = 0,
    expand_planned_width_total: u64 = 0,
    expand_plan_no_io_total: u64 = 0,
    expand_plan_single_batch_total: u64 = 0,
    expand_plan_width_cap_total: u64 = 0,
    hydrate_parallel_total: u64 = 0,
    hydrate_parallel_ns_total: u64 = 0,
    hydrate_planned_parallel_total: u64 = 0,
    hydrate_planned_sequential_total: u64 = 0,
    hydrate_planned_width_total: u64 = 0,
    hydrate_plan_no_io_total: u64 = 0,
    hydrate_plan_single_batch_total: u64 = 0,
    hydrate_plan_width_cap_total: u64 = 0,
};

const GraphFanoutPhase = enum {
    expand,
    hydrate,
};

var expand_parallel_total: std.atomic.Value(u64) = .init(0);
var expand_parallel_ns_total: std.atomic.Value(u64) = .init(0);
var expand_planned_parallel_total: std.atomic.Value(u64) = .init(0);
var expand_planned_sequential_total: std.atomic.Value(u64) = .init(0);
var expand_planned_width_total: std.atomic.Value(u64) = .init(0);
var expand_plan_no_io_total: std.atomic.Value(u64) = .init(0);
var expand_plan_single_batch_total: std.atomic.Value(u64) = .init(0);
var expand_plan_width_cap_total: std.atomic.Value(u64) = .init(0);
var hydrate_parallel_total: std.atomic.Value(u64) = .init(0);
var hydrate_parallel_ns_total: std.atomic.Value(u64) = .init(0);
var hydrate_planned_parallel_total: std.atomic.Value(u64) = .init(0);
var hydrate_planned_sequential_total: std.atomic.Value(u64) = .init(0);
var hydrate_planned_width_total: std.atomic.Value(u64) = .init(0);
var hydrate_plan_no_io_total: std.atomic.Value(u64) = .init(0);
var hydrate_plan_single_batch_total: std.atomic.Value(u64) = .init(0);
var hydrate_plan_width_cap_total: std.atomic.Value(u64) = .init(0);

fn recordGraphFanoutPlan(phase: GraphFanoutPhase, plan: GraphFanoutPlan) void {
    switch (phase) {
        .expand => {
            if (plan.parallel) {
                _ = expand_planned_parallel_total.fetchAdd(1, .monotonic);
            } else {
                _ = expand_planned_sequential_total.fetchAdd(1, .monotonic);
            }
            _ = expand_planned_width_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = expand_plan_no_io_total.fetchAdd(1, .monotonic),
                .single_batch => _ = expand_plan_single_batch_total.fetchAdd(1, .monotonic),
                .width_cap => _ = expand_plan_width_cap_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .hydrate => {
            if (plan.parallel) {
                _ = hydrate_planned_parallel_total.fetchAdd(1, .monotonic);
            } else {
                _ = hydrate_planned_sequential_total.fetchAdd(1, .monotonic);
            }
            _ = hydrate_planned_width_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = hydrate_plan_no_io_total.fetchAdd(1, .monotonic),
                .single_batch => _ = hydrate_plan_single_batch_total.fetchAdd(1, .monotonic),
                .width_cap => _ = hydrate_plan_width_cap_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
    }
}

fn recordGraphParallelFanout(phase: GraphFanoutPhase, elapsed_ns: u64) void {
    switch (phase) {
        .expand => {
            _ = expand_parallel_total.fetchAdd(1, .monotonic);
            _ = expand_parallel_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .hydrate => {
            _ = hydrate_parallel_total.fetchAdd(1, .monotonic);
            _ = hydrate_parallel_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
    }
}

pub fn graphFanoutMetricsSnapshot() GraphFanoutMetricsSnapshot {
    return .{
        .expand_parallel_total = expand_parallel_total.load(.monotonic),
        .expand_parallel_ns_total = expand_parallel_ns_total.load(.monotonic),
        .expand_planned_parallel_total = expand_planned_parallel_total.load(.monotonic),
        .expand_planned_sequential_total = expand_planned_sequential_total.load(.monotonic),
        .expand_planned_width_total = expand_planned_width_total.load(.monotonic),
        .expand_plan_no_io_total = expand_plan_no_io_total.load(.monotonic),
        .expand_plan_single_batch_total = expand_plan_single_batch_total.load(.monotonic),
        .expand_plan_width_cap_total = expand_plan_width_cap_total.load(.monotonic),
        .hydrate_parallel_total = hydrate_parallel_total.load(.monotonic),
        .hydrate_parallel_ns_total = hydrate_parallel_ns_total.load(.monotonic),
        .hydrate_planned_parallel_total = hydrate_planned_parallel_total.load(.monotonic),
        .hydrate_planned_sequential_total = hydrate_planned_sequential_total.load(.monotonic),
        .hydrate_planned_width_total = hydrate_planned_width_total.load(.monotonic),
        .hydrate_plan_no_io_total = hydrate_plan_no_io_total.load(.monotonic),
        .hydrate_plan_single_batch_total = hydrate_plan_single_batch_total.load(.monotonic),
        .hydrate_plan_width_cap_total = hydrate_plan_width_cap_total.load(.monotonic),
    };
}

fn planGraphFanout(has_io: bool, width_cap: ?usize, batch_count: usize) GraphFanoutPlan {
    if (!has_io) return .{ .parallel = false, .width = 1, .reason = .no_io };
    if (batch_count <= 1) return .{ .parallel = false, .width = 1, .reason = .single_batch };
    const cap = width_cap orelse batch_count;
    const width = @max(@as(usize, 1), @min(batch_count, @min(cap, @as(usize, 4))));
    return .{
        .parallel = width > 1,
        .width = width,
        .reason = if (width > 1) .parallel else .width_cap,
    };
}

pub const GraphExpandRequest = struct {
    name: []u8,
    index_name: []u8,
    frontier: []GraphFrontierItem,
    exclude_keys: [][]u8,
    exclude_edges: [][]u8,
    target_constraint_keys: [][]u8 = &.{},
    params: graph_query_mod.QueryParams,
    tensor_access_path: ?OwnedGraphTensorAccessPath = null,
    tensor_program: ?query_contract.OwnedAlgebraicTensorProgramEnvelope = null,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_owned: bool = false,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,

    pub fn deinit(self: *GraphExpandRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.index_name);
        for (self.frontier) |*item| item.deinit(alloc);
        if (self.frontier.len > 0) alloc.free(self.frontier);
        for (self.exclude_keys) |key| alloc.free(key);
        if (self.exclude_keys.len > 0) alloc.free(self.exclude_keys);
        for (self.exclude_edges) |edge| alloc.free(edge);
        if (self.exclude_edges.len > 0) alloc.free(self.exclude_edges);
        for (self.target_constraint_keys) |key| alloc.free(key);
        if (self.target_constraint_keys.len > 0) alloc.free(self.target_constraint_keys);
        freeConstStrings(alloc, self.params.edge_types);
        if (self.tensor_access_path) |*path| path.deinit(alloc);
        if (self.tensor_program) |*program| program.deinit(alloc);
        if (self.resolved_doc_filter_owned) {
            if (self.resolved_doc_filter) |ptr| db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, ptr);
        }
        self.* = undefined;
    }
};

pub const OwnedGraphTensorAccessPath = struct {
    owner: []u8,
    layout: algebraic_ir.PhysicalLayout,
    fragments: []algebraic_ir.TensorFragment,
    output_dims: []algebraic_ir.Dimension,
    law_ids: []algebraic_law.Id,

    pub fn deinit(self: *OwnedGraphTensorAccessPath, alloc: std.mem.Allocator) void {
        alloc.free(self.owner);
        if (self.fragments.len > 0) alloc.free(self.fragments);
        if (self.output_dims.len > 0) alloc.free(self.output_dims);
        if (self.law_ids.len > 0) alloc.free(self.law_ids);
        self.* = undefined;
    }

    pub fn asAccessPath(self: *const OwnedGraphTensorAccessPath) algebraic_ir.PhysicalAccessPath {
        return .{
            .owner = self.owner,
            .layout = self.layout,
            .fragments = self.fragments,
            .output_dims = self.output_dims,
            .law_ids = self.law_ids,
        };
    }
};

pub const GraphFrontierItem = struct {
    id: u32,
    key: []u8,
    depth: u32 = 0,
    distance: f64 = 0,

    pub fn deinit(self: *GraphFrontierItem, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const GraphExpandResponse = struct {
    expansions: []GraphExpansion,

    pub fn deinit(self: *GraphExpandResponse, alloc: std.mem.Allocator) void {
        for (self.expansions) |*expansion| expansion.deinit(alloc);
        if (self.expansions.len > 0) alloc.free(self.expansions);
        self.* = undefined;
    }
};

pub const GraphHydrateRequest = struct {
    keys: [][]u8,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_owned: bool = false,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,

    pub fn deinit(self: *GraphHydrateRequest, alloc: std.mem.Allocator) void {
        for (self.keys) |key| alloc.free(key);
        if (self.keys.len > 0) alloc.free(self.keys);
        if (self.resolved_doc_filter_owned) {
            if (self.resolved_doc_filter) |ptr| db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, ptr);
        }
        self.* = undefined;
    }
};

pub const GraphHydrateResponse = struct {
    hits: []db_mod.types.SearchHit,

    pub fn deinit(self: *GraphHydrateResponse, alloc: std.mem.Allocator) void {
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

pub const GraphEdgesRequest = struct {
    index_name: []u8,
    key: []u8,
    direction: graph_mod.EdgeDirection,
    tensor_access_path: ?OwnedGraphTensorAccessPath = null,
    tensor_program: ?query_contract.OwnedAlgebraicTensorProgramEnvelope = null,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,

    pub fn deinit(self: *GraphEdgesRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.key);
        if (self.tensor_access_path) |*path| path.deinit(alloc);
        if (self.tensor_program) |*program| program.deinit(alloc);
        self.* = undefined;
    }
};

pub const GraphEdgesResponse = struct {
    edges: []graph_mod.Edge,

    pub fn deinit(self: *GraphEdgesResponse, alloc: std.mem.Allocator) void {
        for (self.edges) |e| graph_mod.GraphIndex.freeEdge(alloc, e);
        if (self.edges.len > 0) alloc.free(self.edges);
        self.* = undefined;
    }
};

pub const GraphExpansion = struct {
    frontier_id: u32,
    frontier_key: []u8,
    graph_result: db_mod.types.GraphSearchResult,

    pub fn deinit(self: *GraphExpansion, alloc: std.mem.Allocator) void {
        alloc.free(self.frontier_key);
        self.graph_result.deinit(alloc);
        self.* = undefined;
    }
};

const GraphExpandBatchEntry = struct {
    group_id: u64,
    frontier_ids: []const u32,
};

const GraphHydrateBatchEntry = struct {
    group_id: u64,
    keys: []const []const u8,
};

const GraphExpandFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    result: ?GraphExpandResponse = null,
    err: ?anyerror = null,

    fn init() GraphExpandFanoutSlot {
        return .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    }

    fn deinit(self: *GraphExpandFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const GraphHydrateFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    result: ?GraphHydrateResponse = null,
    err: ?anyerror = null,

    fn init() GraphHydrateFanoutSlot {
        return .{ .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator) };
    }

    fn deinit(self: *GraphHydrateFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const GraphExpandRequestJson = struct {
    name: []const u8,
    index_name: []const u8,
    frontier: []const GraphFrontierItemJson,
    exclude_keys: []const []const u8 = &.{},
    exclude_edges: []const []const u8 = &.{},
    target_constraint_keys: []const []const u8 = &.{},
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
    params: GraphExpandParamsJson,
    tensor_access_path: ?GraphTensorAccessPathJson = null,
    tensor_program: ?std.json.Value = null,
};

const GraphFrontierItemJson = struct {
    id: u32,
    key: []const u8,
    depth: u32 = 0,
    distance: f64 = 0,
};

const GraphExpandParamsJson = struct {
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    max_depth: u32 = 1,
    max_results: u32 = 0,
    min_weight: f64 = 0.0,
    max_weight: f64 = 0.0,
    deduplicate: bool = true,
    include_paths: bool = false,
    weight_mode: []const u8 = "min_hops",
    algebraic_semiring: bool = false,
};

const GraphTensorAccessPathJson = struct {
    owner: []const u8,
    layout: []const u8,
    fragments: []const []const u8,
    output_dims: []const []const u8,
    law_ids: []const []const u8,
};

const GraphExpandResponseJson = struct {
    expansions: []const GraphExpansionJson,
};

const GraphExpansionJson = struct {
    frontier_id: u32,
    frontier_key: []const u8,
    name: []const u8,
    total: u32,
    nodes: []const graph_query_mod.GraphResultNode,
    hits: []const db_mod.types.SearchHit = &.{},
};

const GraphHydrateRequestJson = struct {
    keys: []const []const u8,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
};

const GraphHydrateResponseJson = struct {
    hits: []const db_mod.types.SearchHit,
};

const GraphEdgesRequestJson = struct {
    index_name: []const u8,
    key: []const u8,
    direction: []const u8 = "out",
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    tensor_access_path: GraphTensorAccessPathJson,
    tensor_program: std.json.Value,
};

const GraphEdgeJson = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata: []const u8 = "",
};

const GraphEdgesResponseJson = struct {
    edges: []const GraphEdgeJson,
};

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

pub fn supportsCrossRange(req: db_mod.types.SearchRequest) bool {
    if (req.graph_queries.len == 0) return false;
    if (req.expand_strategy != null) return false;

    for (req.graph_queries) |graph_query| {
        if (graph_query.query.params.direction != .out) return false;
        if (!graph_query.query.params.deduplicate) return false;
        if (!supportsSelectorRef(req, graph_query.query.start_nodes)) return false;
        if (graph_query.query.target_nodes) |target_nodes| {
            if (!supportsSelectorRef(req, target_nodes)) return false;
        }

        switch (graph_query.query.query_type) {
            .neighbors, .traverse => {
                if (graph_query.query.params.weight_mode != .min_hops) return false;
            },
            .shortest_path => {
                if (graph_query.query.target_nodes == null) return false;
                if (graph_query.query.k > 1) return false;
            },
            .k_shortest_paths => {
                if (graph_query.query.target_nodes == null) return false;
                if (graph_query.query.k == 0) return false;
            },
            .pattern => {
                if (graph_query.query.pattern.len == 0) return false;
            },
        }
    }
    return true;
}

pub fn rejectUnstampedResultRefs(req: db_mod.types.SearchRequest) !void {
    if (req.identity_read_generation != null) return;
    for (req.graph_queries) |graph_query| {
        if (selectorUsesResultRef(graph_query.query.start_nodes)) return error.UnsupportedQueryRequest;
        if (graph_query.query.target_nodes) |target_nodes| {
            if (selectorUsesResultRef(target_nodes)) return error.UnsupportedQueryRequest;
        }
    }
}

fn selectorUsesResultRef(selector: graph_query_mod.NodeSelector) bool {
    return switch (selector) {
        .keys => false,
        .result_ref => true,
    };
}

fn requireStampedCrossRangeRequest(req: db_mod.types.SearchRequest) !void {
    if (req.identity_read_generation == null) return error.UnsupportedQueryRequest;
}

pub fn executeCrossRange(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.GraphSearchResult {
    if (!supportsCrossRange(req)) return error.UnsupportedQueryRequest;
    try requireStampedCrossRangeRequest(req);
    try rejectUnstampedResultRefs(req);

    var attempts: u32 = 0;
    while (true) : (attempts += 1) {
        return executeCrossRangeOnce(alloc, catalog, worker, table_name, req, base_result, consistency) catch |err| switch (err) {
            error.TopologyChanged, error.UnknownGroup => {
                if (attempts == 0) continue;
                return err;
            },
            else => return err,
        };
    }
}

fn executeCrossRangeOnce(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.GraphSearchResult {
    if (!supportsCrossRange(req)) return error.UnsupportedQueryRequest;
    try table_catalog.validateDocIdentityReadyForTableStrict(alloc, catalog, table_name);

    const results = try alloc.alloc(db_mod.types.GraphSearchResult, req.graph_queries.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    for (req.graph_queries, 0..) |graph_query, i| {
        results[i] = try executeSingleCrossRange(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            results[0..initialized],
            graph_query,
            consistency,
        );
        initialized += 1;
    }
    return results;
}

const QueryState = struct {
    name: []u8,
    nodes: std.ArrayListUnmanaged(graph_query_mod.GraphResultNode) = .empty,
    hits: std.ArrayListUnmanaged(db_mod.types.SearchHit) = .empty,
    path_states: std.ArrayListUnmanaged(PathState) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .empty,

    fn deinit(self: *QueryState, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.nodes.items) |*node| node.deinit(alloc);
        self.nodes.deinit(alloc);
        for (self.hits.items) |*hit| hit.deinit(alloc);
        self.hits.deinit(alloc);
        for (self.path_states.items) |*path_state| path_state.deinit(alloc);
        self.path_states.deinit(alloc);
        var it = self.seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.seen.deinit(alloc);
        self.* = undefined;
    }

    fn deinitTransient(self: *QueryState, alloc: std.mem.Allocator) void {
        for (self.path_states.items) |*path_state| path_state.deinit(alloc);
        self.path_states.deinit(alloc);
        var it = self.seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.seen.deinit(alloc);
        self.path_states = .empty;
        self.seen = .empty;
    }
};

const PathState = struct {
    key: []u8,
    depth: u32,
    distance: f64,
    cost: f64,
    parent: ?u32 = null,
    incoming_edge: ?graph_query_mod.PathEdgeInfo = null,

    fn deinit(self: *PathState, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.incoming_edge) |edge| freeOwnedPathEdge(alloc, edge);
        self.* = undefined;
    }
};

const FrontierState = struct {
    key: []u8,
    depth: u32 = 0,
    distance: f64 = 0,
    cost: f64 = 0,
    path_state_id: ?u32 = null,

    fn deinit(self: *FrontierState, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

fn executeSingleCrossRange(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.GraphSearchResult {
    return switch (graph_query.query.query_type) {
        .neighbors, .traverse => try executeDistributedTraverse(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            prior_results,
            graph_query,
            consistency,
        ),
        .shortest_path => try executeDistributedShortestPath(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            prior_results,
            graph_query,
            consistency,
        ),
        .k_shortest_paths => try executeDistributedKShortestPaths(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            prior_results,
            graph_query,
            consistency,
        ),
        .pattern => try executeDistributedPattern(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            prior_results,
            graph_query,
            consistency,
        ),
    };
}

const DistributedEdgeReader = struct {
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    index_name: []const u8,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    consistency: raft_mod.ReadConsistency,

    pub fn getEdges(self: @This(), a: std.mem.Allocator, key: []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(
            a,
            self.catalog,
            self.table_name,
            key,
            self.topology_epoch,
        )) orelse return error.TableNotFound;

        var req = GraphEdgesRequest{
            .index_name = try a.dupe(u8, self.index_name),
            .key = try a.dupe(u8, key),
            .direction = direction,
            .tensor_access_path = try cloneGraphTensorAccessPathAlloc(a, algebraic_ir.graphEdgeAccessPath(self.index_name)),
            .tensor_program = try graphEdgesTensorProgramEnvelopeAlloc(a, self.index_name),
            .topology_epoch = self.topology_epoch,
            .identity_read_generation = self.identity_read_generation,
        };
        defer req.deinit(a);

        var resp = try self.worker.executeGraphGetEdges(a, group_id, self.table_name, req, self.consistency);

        // Transfer ownership of edges to caller — don't free them in response deinit.
        const edges = resp.edges;
        resp.edges = @constCast((&[_]graph_mod.Edge{})[0..]);
        resp.deinit(a);
        return edges;
    }

    pub fn freeEdges(_: @This(), a: std.mem.Allocator, edges: []graph_mod.Edge) void {
        graph_mod.GraphIndex.freeEdges(a, edges);
    }
};

fn executeDistributedPattern(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.GraphSearchResult {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);

    // Resolve start keys.
    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    errdefer state.deinit(alloc);
    const frontier = try resolveStartFrontier(alloc, &state, req, base_result, prior_results, graph_query.query.start_nodes, false);
    defer freeFrontier(alloc, frontier);

    const start_keys = try alloc.alloc([]const u8, frontier.len);
    defer alloc.free(start_keys);
    for (frontier, 0..) |item, i| start_keys[i] = item.key;

    // Build distributed edge reader.
    const edge_reader = DistributedEdgeReader{
        .catalog = catalog,
        .worker = worker,
        .table_name = table_name,
        .index_name = graph_query.query.index_name,
        .topology_epoch = topology_epoch,
        .identity_read_generation = req.identity_read_generation,
        .consistency = consistency,
    };

    // Run pattern matching with distributed edges.
    const raw_matches = try graph_pattern_mod.matchPatternWithEdgeReader(
        alloc,
        edge_reader,
        start_keys,
        graph_query.query.pattern,
        .{
            .max_results = graph_query.query.params.max_results,
            .return_aliases = graph_query.query.return_aliases,
        },
    );
    defer graph_pattern_mod.freeMatches(alloc, raw_matches);

    // Convert PatternMatch to GraphPatternMatch.
    const matches = try convertPatternMatches(alloc, raw_matches);
    errdefer {
        for (matches) |*m| m.deinit(alloc);
        if (matches.len > 0) alloc.free(matches);
    }

    // Collect unique node keys from all bindings for hydration.
    const unique_nodes = try graph_query_mod.collectUniqueNodesFromMatches(alloc, raw_matches);
    defer {
        for (unique_nodes) |*n| n.deinit(alloc);
        alloc.free(unique_nodes);
    }

    // Hydrate documents if requested.
    const hits = if (graph_query.query.include_documents)
        try hydrateHitsForResultNodes(alloc, catalog, worker, table_name, topology_epoch, req.identity_read_generation, req.resolved_doc_filter, req.resolved_doc_filter_wire_context, unique_nodes, consistency)
    else
        try alloc.alloc(db_mod.types.SearchHit, 0);

    // Transfer ownership of name out of state.
    const name = state.name;
    state.name = try alloc.alloc(u8, 0);
    defer {
        alloc.free(state.name);
        state.deinitTransient(alloc);
    }

    return .{
        .name = name,
        .nodes = &.{},
        .paths = &.{},
        .matches = matches,
        .hits = hits,
        .total_hits = @intCast(raw_matches.len),
    };
}

fn convertPatternMatches(
    alloc: std.mem.Allocator,
    raw_matches: []const graph_pattern_mod.PatternMatch,
) ![]db_mod.types.GraphPatternMatch {
    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, raw_matches.len);
    var initialized: usize = 0;
    errdefer {
        for (matches[0..initialized]) |*m| m.deinit(alloc);
        alloc.free(matches);
    }

    for (raw_matches, 0..) |raw_match, i| {
        const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, raw_match.bindings.len);
        var bindings_init: usize = 0;
        errdefer {
            for (bindings[0..bindings_init]) |*b| b.deinit(alloc);
            alloc.free(bindings);
        }

        for (raw_match.bindings, 0..) |binding, j| {
            bindings[j] = .{
                .alias = try alloc.dupe(u8, binding.alias),
                .node = .{
                    .key = try alloc.dupe(u8, binding.key),
                    .depth = binding.depth,
                    .distance = @floatFromInt(binding.depth),
                    .path = null,
                    .path_edges = null,
                },
            };
            bindings_init += 1;
        }

        const path = try alloc.alloc(graph_query_mod.PathEdgeInfo, raw_match.path.len);
        var path_init: usize = 0;
        errdefer {
            for (path[0..path_init]) |edge| {
                alloc.free(edge.source);
                alloc.free(edge.target);
                alloc.free(edge.edge_type);
            }
            alloc.free(path);
        }

        for (raw_match.path, 0..) |edge, k| {
            path[k] = .{
                .source = try alloc.dupe(u8, edge.source),
                .target = try alloc.dupe(u8, edge.target),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
            };
            path_init += 1;
        }

        matches[i] = .{
            .bindings = bindings,
            .path = path,
        };
        initialized += 1;
    }

    return matches;
}

fn executeDistributedTraverse(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.GraphSearchResult {
    const include_paths = graph_query.query.params.include_paths;
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    const max_depth: u32 = switch (graph_query.query.query_type) {
        .neighbors => 1,
        .traverse => graph_query.query.params.max_depth,
        else => return error.UnsupportedQueryRequest,
    };
    const algebraic_semiring_selected = graph_query.query.params.algebraic_semiring or
        try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, table_name, graph_query.query.index_name);
    var target_keys: [][]u8 = &.{};
    defer freeKeys(alloc, target_keys);
    if (graph_query.query.target_nodes) |selector| {
        target_keys = try resolveSelectorKeys(alloc, req, base_result, prior_results, selector);
    }

    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    errdefer state.deinit(alloc);

    var frontier = try resolveStartFrontier(alloc, &state, req, base_result, prior_results, graph_query.query.start_nodes, include_paths);
    defer freeFrontier(alloc, frontier);

    for (frontier) |item| {
        try state.seen.put(alloc, try alloc.dupe(u8, item.key), {});
    }

    while (frontier.len > 0) {
        var next_frontier = std.ArrayListUnmanaged(FrontierState).empty;
        defer {
            for (next_frontier.items) |*item| item.deinit(alloc);
            next_frontier.deinit(alloc);
        }

        const effective_max_depth = if (max_depth == 0) std.math.maxInt(u32) else max_depth;
        var batches = try batchFrontierByGroup(alloc, catalog, table_name, frontier, effective_max_depth, topology_epoch);
        defer freeFrontierBatches(alloc, &batches);
        const batch_entries = try collectGraphExpandBatchEntries(alloc, &batches);
        defer alloc.free(batch_entries);
        const exclude_keys = try collectSeenKeys(alloc, state.seen);
        defer {
            for (exclude_keys) |key| alloc.free(key);
            if (exclude_keys.len > 0) alloc.free(exclude_keys);
        }

        const fanout_io = worker.fanoutIo();
        const graph_fanout_plan = planGraphFanout(fanout_io != null, worker.fanoutWidthCap(), batch_entries.len);
        recordGraphFanoutPlan(.expand, graph_fanout_plan);
        if (fanout_io) |io| {
            if (!graph_fanout_plan.parallel) {
                var batch_it = batches.iterator();
                while (batch_it.next()) |entry| {
                    var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, frontier, entry.value_ptr.items, exclude_keys, @constCast((&[_][]u8{})[0..]), include_paths, algebraic_semiring_selected);
                    step_req.topology_epoch = topology_epoch;
                    step_req.identity_read_generation = req.identity_read_generation;
                    attachResolvedDocFilterToGraphExpandRequest(&step_req, req);
                    defer step_req.deinit(alloc);

                    var step_result = try worker.executeGraphExpand(alloc, entry.key_ptr.*, table_name, step_req, consistency);
                    defer step_result.deinit(alloc);

                    for (step_result.expansions) |expansion| {
                        const item = frontier[expansion.frontier_id];
                        const step_graph = expansion.graph_result;

                        for (step_graph.nodes) |node| {
                            if (state.seen.contains(node.key)) continue;
                            try state.seen.put(alloc, try alloc.dupe(u8, node.key), {});
                            const return_node = graphTargetKeyAllowed(target_keys, node.key);

                            const path_state_id = if (include_paths)
                                try appendPathStateFromStep(alloc, &state, item, node)
                            else
                                null;
                            var merged_node = try materializeResultNode(alloc, &state, item, node, include_paths, path_state_id);
                            var merged_node_owned = true;
                            errdefer if (merged_node_owned) merged_node.deinit(alloc);

                            if (merged_node.depth < max_depth) {
                                try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                            }
                            if (return_node) {
                                try state.nodes.append(alloc, merged_node);
                                merged_node_owned = false;
                                if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                            } else {
                                merged_node.deinit(alloc);
                                merged_node_owned = false;
                            }
                        }

                        if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                    }

                    if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                }
            } else {
                const fanout_start_ns = platform_time.monotonicNs();
                const slots = try executeGraphExpandBatchesParallel(
                    alloc,
                    io,
                    graph_fanout_plan.width,
                    worker,
                    table_name,
                    graph_query,
                    frontier,
                    batch_entries,
                    exclude_keys,
                    include_paths,
                    algebraic_semiring_selected,
                    topology_epoch,
                    req.identity_read_generation,
                    req.resolved_doc_filter,
                    req.resolved_doc_filter_wire_context,
                    consistency,
                );
                recordGraphParallelFanout(.expand, @intCast(platform_time.monotonicNs() - fanout_start_ns));
                defer deinitGraphExpandFanoutSlots(alloc, slots);

                for (slots) |slot| {
                    const step_result = slot.result.?;
                    for (step_result.expansions) |expansion| {
                        const item = frontier[expansion.frontier_id];
                        const step_graph = expansion.graph_result;

                        for (step_graph.nodes) |node| {
                            if (state.seen.contains(node.key)) continue;
                            try state.seen.put(alloc, try alloc.dupe(u8, node.key), {});
                            const return_node = graphTargetKeyAllowed(target_keys, node.key);

                            const path_state_id = if (include_paths)
                                try appendPathStateFromStep(alloc, &state, item, node)
                            else
                                null;
                            var merged_node = try materializeResultNode(alloc, &state, item, node, include_paths, path_state_id);
                            var merged_node_owned = true;
                            errdefer if (merged_node_owned) merged_node.deinit(alloc);

                            if (merged_node.depth < max_depth) {
                                try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                            }
                            if (return_node) {
                                try state.nodes.append(alloc, merged_node);
                                merged_node_owned = false;
                                if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                            } else {
                                merged_node.deinit(alloc);
                                merged_node_owned = false;
                            }
                        }

                        if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                    }

                    if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                }
            }
        } else {
            var batch_it = batches.iterator();
            while (batch_it.next()) |entry| {
                var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, frontier, entry.value_ptr.items, exclude_keys, @constCast((&[_][]u8{})[0..]), include_paths, algebraic_semiring_selected);
                step_req.topology_epoch = topology_epoch;
                step_req.identity_read_generation = req.identity_read_generation;
                attachResolvedDocFilterToGraphExpandRequest(&step_req, req);
                defer step_req.deinit(alloc);

                var step_result = try worker.executeGraphExpand(alloc, entry.key_ptr.*, table_name, step_req, consistency);
                defer step_result.deinit(alloc);

                for (step_result.expansions) |expansion| {
                    const item = frontier[expansion.frontier_id];
                    const step_graph = expansion.graph_result;

                    for (step_graph.nodes) |node| {
                        if (state.seen.contains(node.key)) continue;
                        try state.seen.put(alloc, try alloc.dupe(u8, node.key), {});
                        const return_node = graphTargetKeyAllowed(target_keys, node.key);

                        const path_state_id = if (include_paths)
                            try appendPathStateFromStep(alloc, &state, item, node)
                        else
                            null;
                        var merged_node = try materializeResultNode(alloc, &state, item, node, include_paths, path_state_id);
                        var merged_node_owned = true;
                        errdefer if (merged_node_owned) merged_node.deinit(alloc);

                        if (merged_node.depth < max_depth) {
                            try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                        }
                        if (return_node) {
                            try state.nodes.append(alloc, merged_node);
                            merged_node_owned = false;
                            if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                        } else {
                            merged_node.deinit(alloc);
                            merged_node_owned = false;
                        }
                    }

                    if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
                }

                if (graph_query.query.params.max_results > 0 and state.nodes.items.len >= graph_query.query.params.max_results) break;
            }
        }

        freeFrontier(alloc, frontier);
        frontier = try next_frontier.toOwnedSlice(alloc);
    }

    state.hits = try adoptHydratedHits(
        alloc,
        state.hits,
        try hydrateHitsForResultNodes(alloc, catalog, worker, table_name, topology_epoch, req.identity_read_generation, req.resolved_doc_filter, req.resolved_doc_filter_wire_context, state.nodes.items, consistency),
    );

    const total_hits: u32 = @intCast(state.nodes.items.len);
    const name = state.name;
    const nodes = try state.nodes.toOwnedSlice(alloc);
    const hits = try state.hits.toOwnedSlice(alloc);
    state.name = try alloc.alloc(u8, 0);
    state.nodes = .empty;
    state.hits = .empty;
    defer {
        alloc.free(state.name);
        state.deinitTransient(alloc);
    }

    return .{
        .name = name,
        .nodes = nodes,
        .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
        .hits = hits,
        .total_hits = total_hits,
    };
}

fn executeDistributedShortestPath(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.GraphSearchResult {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    var path_result = try findDistributedShortestPath(alloc, catalog, worker, table_name, req, base_result, prior_results, graph_query, consistency, topology_epoch, null, null);
    defer if (path_result) |*result| result.deinit(alloc);

    const nodes = if (path_result) |result| blk: {
        const out = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
        errdefer {
            out[0].deinit(alloc);
            alloc.free(out);
        }
        out[0] = try graphPathToResultNode(alloc, result.path);
        break :blk out;
    } else @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]);
    errdefer if (nodes.len > 0) {
        for (nodes) |*node| node.deinit(alloc);
        alloc.free(nodes);
    };

    const hits = try hydrateHitsForResultNodes(alloc, catalog, worker, table_name, topology_epoch, req.identity_read_generation, req.resolved_doc_filter, req.resolved_doc_filter_wire_context, nodes, consistency);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }

    return .{
        .name = try alloc.dupe(u8, graph_query.name),
        .nodes = nodes,
        .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
        .hits = hits,
        .total_hits = if (path_result != null) 1 else 0,
    };
}

fn executeDistributedKShortestPaths(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
) !db_mod.types.GraphSearchResult {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    const start_keys = try resolveSelectorKeys(alloc, req, base_result, prior_results, graph_query.query.start_nodes);
    defer freeKeys(alloc, start_keys);
    const target_keys = try resolveSelectorKeys(alloc, req, base_result, prior_results, graph_query.query.target_nodes.?);
    defer freeKeys(alloc, target_keys);

    if (start_keys.len != 1 or target_keys.len != 1) return error.UnsupportedQueryRequest;

    var results = std.ArrayListUnmanaged(db_mod.types.GraphPath).empty;
    errdefer {
        for (results.items) |path| graph_paths_mod.freePath(alloc, path);
        results.deinit(alloc);
    }

    var seen_paths = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen_paths.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen_paths.deinit(alloc);
    }

    const first = try findDistributedShortestPath(alloc, catalog, worker, table_name, req, base_result, prior_results, graph_query, consistency, topology_epoch, null, null);
    if (first == null) {
        return .{
            .name = try alloc.dupe(u8, graph_query.name),
            .nodes = @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]),
            .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
            .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
            .total_hits = 0,
        };
    }
    var first_result = first.?;
    defer first_result.deinit(alloc);
    try results.append(alloc, try cloneGraphPath(alloc, first_result.path));
    const first_key = try graphPathToKey(alloc, first_result.path);
    try seen_paths.put(alloc, first_key, {});

    var candidates = std.ArrayListUnmanaged(db_mod.types.GraphPath).empty;
    defer {
        for (candidates.items) |path| graph_paths_mod.freePath(alloc, path);
        candidates.deinit(alloc);
    }

    var ki: u32 = 1;
    while (ki < graph_query.query.k) : (ki += 1) {
        const prev_path = &results.items[results.items.len - 1];
        if (prev_path.nodes.len <= 1) break;

        for (0..prev_path.nodes.len - 1) |spur_idx| {
            var excluded_edges = std.StringHashMapUnmanaged(void).empty;
            defer {
                var eit = excluded_edges.keyIterator();
                while (eit.next()) |ek| alloc.free(ek.*);
                excluded_edges.deinit(alloc);
            }

            var excluded_nodes = std.StringHashMapUnmanaged(void).empty;
            defer {
                var nit = excluded_nodes.keyIterator();
                while (nit.next()) |nk| alloc.free(nk.*);
                excluded_nodes.deinit(alloc);
            }

            for (results.items) |result_path| {
                if (result_path.nodes.len <= spur_idx + 1) continue;
                if (!rootPathMatches(prev_path.*, result_path, spur_idx)) continue;

                const edge_key = try allocEdgeExclusionKey(
                    alloc,
                    result_path.nodes[spur_idx],
                    result_path.nodes[spur_idx + 1],
                    if (result_path.edges.len > spur_idx) result_path.edges[spur_idx].edge_type else "",
                );
                if (!excluded_edges.contains(edge_key)) {
                    try excluded_edges.put(alloc, edge_key, {});
                } else {
                    alloc.free(edge_key);
                }
            }

            for (0..spur_idx) |i| {
                const node_key = prev_path.nodes[i];
                if (excluded_nodes.contains(node_key)) continue;
                try excluded_nodes.put(alloc, try alloc.dupe(u8, node_key), {});
            }

            const spur_start = prev_path.nodes[spur_idx];
            const root_prefix = prev_path.nodes[0..spur_idx];
            const root_edges = prev_path.edges[0..spur_idx];

            var spur_query = graph_query;
            spur_query.query.start_nodes = .{ .keys = &.{spur_start} };
            spur_query.query.target_nodes = .{ .keys = &.{target_keys[0]} };
            spur_query.query.query_type = .shortest_path;
            spur_query.query.k = 1;

            var spur = try findDistributedShortestPath(alloc, catalog, worker, table_name, req, base_result, prior_results, spur_query, consistency, topology_epoch, &excluded_nodes, &excluded_edges);
            defer if (spur) |*result| result.deinit(alloc);
            if (spur == null) continue;

            const total_path = try joinDistributedPaths(alloc, root_prefix, root_edges, spur.?.path);
            const pkey = try graphPathToKey(alloc, total_path);
            if (!seen_paths.contains(pkey)) {
                try seen_paths.put(alloc, pkey, {});
                try candidates.append(alloc, total_path);
            } else {
                alloc.free(pkey);
                graph_paths_mod.freePath(alloc, total_path);
            }
        }

        if (candidates.items.len == 0) break;
        const best_idx = bestPathIndex(candidates.items, graph_query.query.params.weight_mode);
        try results.append(alloc, candidates.swapRemove(best_idx));
    }

    const out_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, results.items.len);
    errdefer {
        for (out_nodes) |*node| node.deinit(alloc);
        alloc.free(out_nodes);
    }
    for (results.items, 0..) |path, i| {
        out_nodes[i] = try graphPathToResultNode(alloc, path);
    }

    const out_paths = try alloc.alloc(db_mod.types.GraphPath, results.items.len);
    for (results.items, 0..) |path, i| out_paths[i] = path;
    results.clearRetainingCapacity();

    const hits = try hydrateHitsForResultNodes(alloc, catalog, worker, table_name, topology_epoch, req.identity_read_generation, req.resolved_doc_filter, req.resolved_doc_filter_wire_context, out_nodes, consistency);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }

    return .{
        .name = try alloc.dupe(u8, graph_query.name),
        .nodes = out_nodes,
        .paths = out_paths,
        .hits = hits,
        .total_hits = @intCast(out_nodes.len),
    };
}

const ShortestPathResult = struct {
    path: db_mod.types.GraphPath,

    fn deinit(self: *ShortestPathResult, alloc: std.mem.Allocator) void {
        graph_paths_mod.freePath(alloc, self.path);
        self.* = undefined;
    }
};

fn findDistributedShortestPath(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
    topology_epoch: u64,
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) !?ShortestPathResult {
    const algebraic_semiring_selected = graph_query.query.params.algebraic_semiring or
        try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, table_name, graph_query.query.index_name);
    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    defer state.deinit(alloc);

    const targets = try resolveSelectorKeys(alloc, req, base_result, prior_results, graph_query.query.target_nodes.?);
    defer freeKeys(alloc, targets);
    var target_set = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = target_set.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        target_set.deinit(alloc);
    }
    for (targets) |target| try target_set.put(alloc, try alloc.dupe(u8, target), {});

    var frontier = std.ArrayListUnmanaged(FrontierState).empty;
    defer {
        for (frontier.items) |*item| item.deinit(alloc);
        frontier.deinit(alloc);
    }
    const roots = try resolveStartFrontier(alloc, &state, req, base_result, prior_results, graph_query.query.start_nodes, true);
    defer freeFrontier(alloc, roots);

    var best_cost = std.StringHashMapUnmanaged(f64).empty;
    defer {
        var it = best_cost.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        best_cost.deinit(alloc);
    }

    for (roots) |item| {
        if (excluded_nodes) |set| {
            if (set.contains(item.key)) continue;
        }
        try frontier.append(alloc, .{
            .key = try alloc.dupe(u8, item.key),
            .depth = item.depth,
            .distance = item.distance,
            .cost = item.cost,
            .path_state_id = item.path_state_id,
        });
        try best_cost.put(alloc, try alloc.dupe(u8, item.key), item.cost);
    }

    var settled = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = settled.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        settled.deinit(alloc);
    }

    const max_depth = graph_query.query.params.max_depth;
    while (frontier.items.len > 0) {
        const best_index = popBestFrontierIndex(frontier.items);
        var item = frontier.swapRemove(best_index);
        defer item.deinit(alloc);

        if (settled.contains(item.key)) continue;
        if (excluded_nodes) |set| {
            if (set.contains(item.key) and item.depth > 0) continue;
        }
        try settled.put(alloc, try alloc.dupe(u8, item.key), {});

        if (target_set.contains(item.key) and item.depth > 0) {
            const path_state_id = item.path_state_id orelse return error.InvalidQueryRequest;
            const path = try pathStateToGraphPath(alloc, state.path_states.items, path_state_id);
            errdefer graph_paths_mod.freePath(alloc, path);
            return .{ .path = path };
        }

        if (max_depth > 0 and item.depth >= max_depth) continue;

        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, item.key, topology_epoch)) orelse return error.TableNotFound;
        const frontier_ids = [_]u32{0};
        const exclude_keys = try collectCombinedExcludeKeys(alloc, settled, excluded_nodes);
        const exclude_edge_keys = try collectExcludedEdgeKeys(alloc, excluded_edges);
        var one_frontier = [_]FrontierState{item};
        var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, one_frontier[0..], frontier_ids[0..], exclude_keys, exclude_edge_keys, true, algebraic_semiring_selected);
        step_req.topology_epoch = topology_epoch;
        step_req.identity_read_generation = req.identity_read_generation;
        attachResolvedDocFilterToGraphExpandRequest(&step_req, req);
        defer step_req.deinit(alloc);

        var step_result = try worker.executeGraphExpand(alloc, group_id, table_name, step_req, consistency);
        defer step_result.deinit(alloc);

        if (step_result.expansions.len == 0) continue;
        const step_graph = step_result.expansions[0].graph_result;
        for (step_graph.nodes) |node| {
            if (settled.contains(node.key)) continue;
            if (excluded_nodes) |set| {
                if (set.contains(node.key)) continue;
            }

            const path_state_id = try appendPathStateFromWeightedStep(alloc, &state, item, node, graph_query.query.params.weight_mode);
            const path_state = state.path_states.items[path_state_id];
            const gop = try best_cost.getOrPut(alloc, node.key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, node.key);
                gop.value_ptr.* = path_state.cost;
            } else {
                if (path_state.cost >= gop.value_ptr.*) continue;
                gop.value_ptr.* = path_state.cost;
            }

            try frontier.append(alloc, .{
                .key = try alloc.dupe(u8, path_state.key),
                .depth = path_state.depth,
                .distance = path_state.distance,
                .cost = path_state.cost,
                .path_state_id = path_state_id,
            });
        }
    }

    return null;
}

fn supportsSelectorRef(req: db_mod.types.SearchRequest, selector: graph_query_mod.NodeSelector) bool {
    return switch (selector) {
        .keys => true,
        .result_ref => |result_ref| supportsResultRef(req, result_ref.ref),
    };
}

fn supportsResultRef(req: db_mod.types.SearchRequest, ref: []const u8) bool {
    if (std.mem.startsWith(u8, ref, "$graph_results.")) return true;
    if (!std.mem.eql(u8, ref, "$fused_results") and
        !std.mem.eql(u8, ref, "$full_text_results") and
        !std.mem.eql(u8, ref, "$embeddings_results"))
    {
        return false;
    }
    return req.full_text_queries.len == 0 and req.dense_queries.len == 0 and req.sparse_queries.len == 0 and req.merge_config == null;
}

fn batchFrontierByGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    frontier: []const FrontierState,
    max_depth: u32,
    topology_epoch: u64,
) !std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(u32)) {
    var batches = std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(u32)).empty;
    errdefer freeFrontierBatches(alloc, &batches);

    for (frontier, 0..) |item, i| {
        if (item.depth >= max_depth) continue;
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, item.key, topology_epoch)) orelse return error.TableNotFound;
        const gop = try batches.getOrPut(alloc, group_id);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(alloc, @intCast(i));
    }
    return batches;
}

fn freeFrontierBatches(
    alloc: std.mem.Allocator,
    batches: *std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(u32)),
) void {
    var it = batches.iterator();
    while (it.next()) |entry| entry.value_ptr.deinit(alloc);
    batches.deinit(alloc);
    batches.* = .empty;
}

fn collectGraphExpandBatchEntries(
    alloc: std.mem.Allocator,
    batches: *const std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(u32)),
) ![]GraphExpandBatchEntry {
    const entries = try alloc.alloc(GraphExpandBatchEntry, batches.count());
    var i: usize = 0;
    var it = batches.iterator();
    while (it.next()) |entry| {
        entries[i] = .{
            .group_id = entry.key_ptr.*,
            .frontier_ids = entry.value_ptr.items,
        };
        i += 1;
    }
    return entries;
}

fn initGraphExpandFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]GraphExpandFanoutSlot {
    const slots = try alloc.alloc(GraphExpandFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

fn deinitGraphExpandFanoutSlots(alloc: std.mem.Allocator, slots: []GraphExpandFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

fn executeGraphExpandBatchesParallel(
    alloc: std.mem.Allocator,
    io: std.Io,
    width: usize,
    worker: Worker,
    table_name: []const u8,
    graph_query: db_mod.types.NamedGraphQuery,
    frontier: []const FrontierState,
    entries: []const GraphExpandBatchEntry,
    exclude_keys: [][]u8,
    include_paths: bool,
    algebraic_semiring_selected: bool,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    resolved_doc_filter: ?*const anyopaque,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    consistency: raft_mod.ReadConsistency,
) ![]GraphExpandFanoutSlot {
    const slots = try initGraphExpandFanoutSlots(alloc, entries.len);
    errdefer deinitGraphExpandFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            worker_inner: Worker,
            slot: *GraphExpandFanoutSlot,
            table_name_inner: []const u8,
            graph_query_inner: db_mod.types.NamedGraphQuery,
            frontier_inner: []const FrontierState,
            entry: GraphExpandBatchEntry,
            exclude_keys_inner: [][]u8,
            include_paths_inner: bool,
            algebraic_semiring_selected_inner: bool,
            topology_epoch_inner: u64,
            identity_read_generation_inner: ?u64,
            resolved_doc_filter_inner: ?*const anyopaque,
            resolved_doc_filter_wire_context_inner: ?db_mod.types.ResolvedDocFilterWireContext,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            var step_req = makeGraphExpandRequestWithAlgebraicMode(
                arena,
                graph_query_inner,
                frontier_inner,
                entry.frontier_ids,
                exclude_keys_inner,
                @constCast((&[_][]u8{})[0..]),
                include_paths_inner,
                algebraic_semiring_selected_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
            step_req.topology_epoch = topology_epoch_inner;
            step_req.identity_read_generation = identity_read_generation_inner;
            step_req.resolved_doc_filter = resolved_doc_filter_inner;
            step_req.resolved_doc_filter_wire_context = resolved_doc_filter_wire_context_inner;
            slot.result = worker_inner.executeGraphExpand(arena, entry.group_id, table_name_inner, step_req, consistency_inner) catch |err| {
                slot.err = err;
                return;
            };
        }
    };

    var start: usize = 0;
    while (start < entries.len) : (start += width) {
        const end = @min(start + width, entries.len);
        var group: std.Io.Group = .init;
        for (entries[start..end], start..end) |entry, i| {
            group.async(io, Fiber.run, .{
                worker,
                &slots[i],
                table_name,
                graph_query,
                frontier,
                entry,
                exclude_keys,
                include_paths,
                algebraic_semiring_selected,
                topology_epoch,
                identity_read_generation,
                resolved_doc_filter,
                resolved_doc_filter_wire_context,
                consistency,
            });
        }
        group.await(io) catch {};
    }

    for (slots) |slot| {
        if (slot.err) |err| return err;
    }
    return slots;
}

fn attachResolvedDocFilterToGraphExpandRequest(step_req: *GraphExpandRequest, source_req: db_mod.types.SearchRequest) void {
    step_req.resolved_doc_filter = source_req.resolved_doc_filter;
    step_req.resolved_doc_filter_wire_context = source_req.resolved_doc_filter_wire_context;
}

fn collectSeenKeys(
    alloc: std.mem.Allocator,
    seen: std.StringHashMapUnmanaged(void),
) ![][]u8 {
    const out = try alloc.alloc([]u8, seen.count());
    var i: usize = 0;
    errdefer {
        for (out[0..i]) |key| alloc.free(key);
        alloc.free(out);
    }
    var it = seen.keyIterator();
    while (it.next()) |key| {
        out[i] = try alloc.dupe(u8, key.*);
        i += 1;
    }
    return out;
}

fn collectSettledKeys(
    alloc: std.mem.Allocator,
    settled: std.StringHashMapUnmanaged(void),
) ![][]u8 {
    return collectSeenKeys(alloc, settled);
}

fn collectCombinedExcludeKeys(
    alloc: std.mem.Allocator,
    settled: std.StringHashMapUnmanaged(void),
    excluded_nodes: ?*const std.StringHashMapUnmanaged(void),
) ![][]u8 {
    const extra_count: usize = if (excluded_nodes) |set| set.count() else 0;
    const out = try alloc.alloc([]u8, settled.count() + extra_count);
    var i: usize = 0;
    errdefer {
        for (out[0..i]) |key| alloc.free(key);
        alloc.free(out);
    }

    var settled_it = settled.keyIterator();
    while (settled_it.next()) |key| {
        out[i] = try alloc.dupe(u8, key.*);
        i += 1;
    }
    if (excluded_nodes) |set| {
        var node_it = set.keyIterator();
        while (node_it.next()) |key| {
            out[i] = try alloc.dupe(u8, key.*);
            i += 1;
        }
    }
    return out;
}

fn collectExcludedEdgeKeys(
    alloc: std.mem.Allocator,
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) ![][]u8 {
    const count: usize = if (excluded_edges) |set| set.count() else 0;
    const out = try alloc.alloc([]u8, count);
    var i: usize = 0;
    errdefer {
        for (out[0..i]) |edge| alloc.free(edge);
        alloc.free(out);
    }
    if (excluded_edges) |set| {
        var it = set.keyIterator();
        while (it.next()) |key| {
            out[i] = try alloc.dupe(u8, key.*);
            i += 1;
        }
    }
    return out;
}

fn adoptHydratedHits(
    alloc: std.mem.Allocator,
    old_hits: std.ArrayListUnmanaged(db_mod.types.SearchHit),
    new_hits: []db_mod.types.SearchHit,
) !std.ArrayListUnmanaged(db_mod.types.SearchHit) {
    var owned_old_hits = old_hits;
    for (owned_old_hits.items) |*hit| hit.deinit(alloc);
    owned_old_hits.deinit(alloc);
    var out = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    try out.appendSlice(alloc, new_hits);
    if (new_hits.len > 0) alloc.free(new_hits);
    return out;
}

fn hydrateHitsForResultNodes(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    resolved_doc_filter: ?*const anyopaque,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    nodes: []const graph_query_mod.GraphResultNode,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.SearchHit {
    // Most nodes are hydrated from the query table. Mention/DocRef edges carry a
    // cross-table endpoint (`node.table`, e.g. the canonical "entities" table)
    // that must be routed and hydrated against *that* table's shard topology,
    // not the query table's. Partition by effective table; the single-table
    // common case keeps the original fast path untouched.
    var needs_cross_table = false;
    for (nodes) |node| {
        if (node.table) |t| {
            if (!std.mem.eql(u8, t, table_name)) {
                needs_cross_table = true;
                break;
            }
        }
    }

    if (!needs_cross_table) {
        var keys = try alloc.alloc([]const u8, nodes.len);
        defer alloc.free(keys);
        for (nodes, 0..) |node, i| keys[i] = node.key;
        return try hydrateHitsForKeys(alloc, catalog, worker, table_name, topology_epoch, identity_read_generation, resolved_doc_filter, resolved_doc_filter_wire_context, keys, consistency);
    }

    // Bucket node keys by their effective hydration table.
    var tables = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged([]const u8)).empty;
    defer {
        for (tables.values()) |*list| list.deinit(alloc);
        tables.deinit(alloc);
    }
    for (nodes) |node| {
        const eff_table = node.table orelse table_name;
        const gop = try tables.getOrPut(alloc, eff_table);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(alloc, node.key);
    }

    const HydratedBucket = struct {
        table: []const u8,
        hits: []db_mod.types.SearchHit,

        fn deinit(self: *@This(), a: std.mem.Allocator) void {
            for (self.hits) |*hit| hit.deinit(a);
            if (self.hits.len > 0) a.free(self.hits);
        }
    };
    var hydrated = std.ArrayListUnmanaged(HydratedBucket).empty;
    defer {
        for (hydrated.items) |*bucket| bucket.deinit(alloc);
        hydrated.deinit(alloc);
    }
    var it = tables.iterator();
    while (it.next()) |entry| {
        const eff_table = entry.key_ptr.*;
        const same_table = std.mem.eql(u8, eff_table, table_name);
        // A cross-table endpoint whose table no longer exists fails closed (no
        // hit), matching the same-table path's behavior for a missing key,
        // rather than erroring the whole graph query.
        if (!same_table and !try table_catalog.tableExists(catalog, eff_table)) continue;
        const eff_epoch = if (same_table) topology_epoch else try table_catalog.topologyEpoch(alloc, catalog, eff_table);
        const eff_identity_generation = if (same_table) identity_read_generation else null;
        const eff_resolved_filter = if (same_table) resolved_doc_filter else null;
        const eff_resolved_filter_context = if (same_table) resolved_doc_filter_wire_context else null;
        const hits = try hydrateHitsForKeys(alloc, catalog, worker, eff_table, eff_epoch, eff_identity_generation, eff_resolved_filter, eff_resolved_filter_context, entry.value_ptr.items, consistency);
        errdefer {
            for (hits) |*hit| hit.deinit(alloc);
            if (hits.len > 0) alloc.free(hits);
        }
        if (!same_table) {
            // Ordinals are scoped to the hydrated table's identity namespace.
            // SearchHit has no table/namespace carrier today, so do not export a
            // cross-table ordinal as if it belonged to the query table.
            for (hits) |*hit| hit.doc_ordinal = null;
        }
        try hydrated.append(alloc, .{ .table = eff_table, .hits = hits });
    }

    var out = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (out.items) |*hit| hit.deinit(alloc);
        out.deinit(alloc);
    }
    for (nodes) |node| {
        const eff_table = node.table orelse table_name;
        for (hydrated.items) |bucket| {
            if (!std.mem.eql(u8, bucket.table, eff_table)) continue;
            for (bucket.hits) |hit| {
                if (!std.mem.eql(u8, hit.id, node.key)) continue;
                try out.append(alloc, try hit.clone(alloc));
                break;
            }
            break;
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn hydrateHitsForKeys(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    resolved_doc_filter: ?*const anyopaque,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    keys: []const []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.SearchHit {
    if (keys.len == 0) return @constCast((&[_]db_mod.types.SearchHit{})[0..]);

    var unique = std.StringHashMapUnmanaged(void).empty;
    defer {
        var uit = unique.keyIterator();
        while (uit.next()) |key| alloc.free(key.*);
        unique.deinit(alloc);
    }

    var batches = std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged([]const u8)).empty;
    defer {
        var it = batches.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        batches.deinit(alloc);
    }

    for (keys) |key| {
        const gop = try unique.getOrPut(alloc, key);
        if (gop.found_existing) continue;
        gop.key_ptr.* = try alloc.dupe(u8, key);
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.TableNotFound;
        const batch = try batches.getOrPut(alloc, group_id);
        if (!batch.found_existing) batch.value_ptr.* = .empty;
        try batch.value_ptr.append(alloc, key);
    }

    var out = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (out.items) |*hit| hit.deinit(alloc);
        out.deinit(alloc);
    }
    const cross_group_hydrate = batches.count() > 1;

    const fanout_io = worker.fanoutIo();
    const graph_fanout_plan = planGraphFanout(fanout_io != null, worker.fanoutWidthCap(), batches.count());
    recordGraphFanoutPlan(.hydrate, graph_fanout_plan);
    if (fanout_io) |io| {
        if (!graph_fanout_plan.parallel) {
            var batch_it = batches.iterator();
            while (batch_it.next()) |entry| {
                const owned_keys = try dupKeys(alloc, entry.value_ptr.items);
                var req: GraphHydrateRequest = .{
                    .keys = owned_keys,
                };
                req.topology_epoch = topology_epoch;
                req.identity_read_generation = identity_read_generation;
                req.resolved_doc_filter = resolved_doc_filter;
                req.resolved_doc_filter_wire_context = resolved_doc_filter_wire_context;
                defer req.deinit(alloc);

                var res = try worker.executeGraphHydrate(alloc, entry.key_ptr.*, table_name, req, consistency);
                defer res.deinit(alloc);
                for (res.hits) |hit| {
                    try out.append(alloc, try hit.clone(alloc));
                }
            }
            return try finalizeHydratedHits(alloc, &out, cross_group_hydrate);
        }

        const fanout_start_ns = platform_time.monotonicNs();
        const entries = try alloc.alloc(GraphHydrateBatchEntry, batches.count());
        defer alloc.free(entries);
        var batch_it = batches.iterator();
        var entry_index: usize = 0;
        while (batch_it.next()) |entry| {
            entries[entry_index] = .{
                .group_id = entry.key_ptr.*,
                .keys = entry.value_ptr.items,
            };
            entry_index += 1;
        }

        const slots = try alloc.alloc(GraphHydrateFanoutSlot, entries.len);
        defer {
            for (slots) |*slot| slot.deinit();
            alloc.free(slots);
        }
        for (slots) |*slot| slot.* = .init();

        const Fiber = struct {
            fn run(
                worker_inner: Worker,
                slot: *GraphHydrateFanoutSlot,
                table_name_inner: []const u8,
                entry: GraphHydrateBatchEntry,
                topology_epoch_inner: u64,
                identity_read_generation_inner: ?u64,
                resolved_doc_filter_inner: ?*const anyopaque,
                resolved_doc_filter_wire_context_inner: ?db_mod.types.ResolvedDocFilterWireContext,
                consistency_inner: raft_mod.ReadConsistency,
            ) void {
                const arena = slot.arena.allocator();
                const owned_keys = dupKeys(arena, entry.keys) catch |err| {
                    slot.err = err;
                    return;
                };
                var req: GraphHydrateRequest = .{
                    .keys = owned_keys,
                };
                req.topology_epoch = topology_epoch_inner;
                req.identity_read_generation = identity_read_generation_inner;
                req.resolved_doc_filter = resolved_doc_filter_inner;
                req.resolved_doc_filter_wire_context = resolved_doc_filter_wire_context_inner;
                slot.result = worker_inner.executeGraphHydrate(arena, entry.group_id, table_name_inner, req, consistency_inner) catch |err| {
                    slot.err = err;
                    return;
                };
            }
        };

        var start: usize = 0;
        while (start < entries.len) : (start += graph_fanout_plan.width) {
            const end = @min(start + graph_fanout_plan.width, entries.len);
            var group: std.Io.Group = .init;
            for (entries[start..end], start..end) |entry, i| {
                group.async(io, Fiber.run, .{ worker, &slots[i], table_name, entry, topology_epoch, identity_read_generation, resolved_doc_filter, resolved_doc_filter_wire_context, consistency });
            }
            group.await(io) catch {};
        }
        recordGraphParallelFanout(.hydrate, @intCast(platform_time.monotonicNs() - fanout_start_ns));
        for (slots) |slot| {
            if (slot.err) |err| return err;
        }
        for (slots) |slot| {
            for (slot.result.?.hits) |hit| {
                try out.append(alloc, try hit.clone(alloc));
            }
        }
        return try finalizeHydratedHits(alloc, &out, cross_group_hydrate);
    }

    var batch_it = batches.iterator();
    while (batch_it.next()) |entry| {
        const owned_keys = try dupKeys(alloc, entry.value_ptr.items);
        var req: GraphHydrateRequest = .{
            .keys = owned_keys,
        };
        req.topology_epoch = topology_epoch;
        req.identity_read_generation = identity_read_generation;
        req.resolved_doc_filter = resolved_doc_filter;
        req.resolved_doc_filter_wire_context = resolved_doc_filter_wire_context;
        defer req.deinit(alloc);

        var res = try worker.executeGraphHydrate(alloc, entry.key_ptr.*, table_name, req, consistency);
        defer res.deinit(alloc);
        for (res.hits) |hit| {
            try out.append(alloc, try hit.clone(alloc));
        }
    }

    return try finalizeHydratedHits(alloc, &out, cross_group_hydrate);
}

fn finalizeHydratedHits(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(db_mod.types.SearchHit),
    clear_doc_ordinals: bool,
) ![]db_mod.types.SearchHit {
    const hits = try out.toOwnedSlice(alloc);
    if (clear_doc_ordinals) {
        for (hits) |*hit| hit.doc_ordinal = null;
    }
    return hits;
}

pub fn encodeGraphHydrateRequest(alloc: std.mem.Allocator, req: GraphHydrateRequest) ![]u8 {
    const encoded = try jsonStringifyAlloc(alloc, GraphHydrateRequestJson{
        .keys = req.keys,
        .topology_epoch = req.topology_epoch,
        .identity_read_generation = req.identity_read_generation,
    });
    if (req.resolved_doc_filter == null) return encoded;
    defer alloc.free(encoded);
    return try appendResolvedDocFilterToObjectAlloc(alloc, encoded, req.resolved_doc_filter.?, req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest);
}

pub fn parseGraphHydrateRequest(alloc: std.mem.Allocator, body: []const u8) !GraphHydrateRequest {
    var parsed = try std.json.parseFromSlice(GraphHydrateRequestJson, alloc, body, .{});
    defer parsed.deinit();

    var parsed_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null;
    errdefer if (parsed_filter) |*filter| filter.deinit(alloc);
    if (parsed.value._resolved_doc_filter) |value| {
        parsed_filter = try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, value);
    }
    const identity_read_generation = try identityGenerationFromResolvedFilterEnvelope(parsed.value.identity_read_generation, if (parsed_filter) |*filter| filter else null);

    const out = GraphHydrateRequest{
        .keys = try dupKeys(alloc, parsed.value.keys),
        .topology_epoch = parsed.value.topology_epoch,
        .identity_read_generation = identity_read_generation,
        .resolved_doc_filter = if (parsed_filter) |filter| filter.resolved_doc_filter else null,
        .resolved_doc_filter_owned = parsed_filter != null,
        .resolved_doc_filter_wire_context = if (parsed_filter) |filter| filter.context else null,
    };
    parsed_filter = null;
    return out;
}

pub fn encodeGraphHydrateResponse(alloc: std.mem.Allocator, res: GraphHydrateResponse) ![]u8 {
    return try jsonStringifyAlloc(alloc, GraphHydrateResponseJson{
        .hits = res.hits,
    });
}

pub fn parseGraphHydrateResponse(alloc: std.mem.Allocator, body: []const u8) !GraphHydrateResponse {
    var parsed = try std.json.parseFromSlice(GraphHydrateResponseJson, alloc, body, .{});
    defer parsed.deinit();
    return .{
        .hits = if (parsed.value.hits.len > 0)
            try cloneSearchHits(alloc, parsed.value.hits)
        else
            @constCast((&[_]db_mod.types.SearchHit{})[0..]),
    };
}

fn identityGenerationFromResolvedFilterEnvelope(
    explicit_generation: ?u64,
    parsed_filter: ?*const db_mod.doc_filter_wire.ParsedResolvedDocFilter,
) !?u64 {
    const filter = parsed_filter orelse return explicit_generation;
    if (explicit_generation) |generation| {
        if (generation != filter.context.identity_read_generation) return error.InvalidQueryRequest;
        return generation;
    }
    return filter.context.identity_read_generation;
}

pub fn encodeGraphEdgesRequest(alloc: std.mem.Allocator, req: GraphEdgesRequest) ![]u8 {
    try validateGraphEdgesTensorAccessPath(alloc, req);
    var fragment_names: ?[][]const u8 = null;
    defer if (fragment_names) |names| alloc.free(names);
    var output_dim_names: ?[][]const u8 = null;
    defer if (output_dim_names) |names| alloc.free(names);
    var law_names: ?[][]const u8 = null;
    defer if (law_names) |names| alloc.free(names);
    const tensor_path = req.tensor_access_path orelse return error.InvalidQueryRequest;
    const tensor_program = req.tensor_program orelse return error.InvalidQueryRequest;
    var tensor_program_json = try graphTensorProgramJsonValueAlloc(alloc, &tensor_program);
    defer tensor_program_json.deinit();
    return try jsonStringifyAlloc(alloc, GraphEdgesRequestJson{
        .index_name = req.index_name,
        .key = req.key,
        .direction = switch (req.direction) {
            .out => "out",
            .in => "in",
            .both => "both",
        },
        .topology_epoch = req.topology_epoch,
        .identity_read_generation = req.identity_read_generation,
        .tensor_access_path = try graphTensorAccessPathJsonAlloc(alloc, tensor_path, &fragment_names, &output_dim_names, &law_names),
        .tensor_program = tensor_program_json.value,
    });
}

pub fn parseGraphEdgesRequest(alloc: std.mem.Allocator, body: []const u8) !GraphEdgesRequest {
    var parsed = try std.json.parseFromSlice(GraphEdgesRequestJson, alloc, body, .{});
    defer parsed.deinit();
    var tensor_access_path = try parseGraphTensorAccessPathAlloc(alloc, parsed.value.tensor_access_path);
    errdefer tensor_access_path.deinit(alloc);
    var tensor_program = try parseGraphTensorProgramJsonValueAlloc(alloc, parsed.value.tensor_program);
    errdefer tensor_program.deinit(alloc);
    try validateGraphEdgesTensorAccessPathParts(
        alloc,
        parsed.value.index_name,
        tensor_access_path,
        &tensor_program,
    );
    return .{
        .index_name = try alloc.dupe(u8, parsed.value.index_name),
        .key = try alloc.dupe(u8, parsed.value.key),
        .direction = if (std.mem.eql(u8, parsed.value.direction, "in"))
            .in
        else if (std.mem.eql(u8, parsed.value.direction, "both"))
            .both
        else
            .out,
        .topology_epoch = parsed.value.topology_epoch,
        .identity_read_generation = parsed.value.identity_read_generation,
        .tensor_access_path = tensor_access_path,
        .tensor_program = tensor_program,
    };
}

fn cloneGraphEdge(alloc: std.mem.Allocator, edge: GraphEdgeJson) !graph_mod.Edge {
    return .{
        .source = try alloc.dupe(u8, edge.source),
        .target = try alloc.dupe(u8, edge.target),
        .edge_type = try alloc.dupe(u8, edge.edge_type),
        .weight = edge.weight,
        .created_at = edge.created_at,
        .updated_at = edge.updated_at,
        .metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "",
    };
}

pub fn encodeGraphEdgesResponse(alloc: std.mem.Allocator, res: GraphEdgesResponse) ![]u8 {
    const edges = try alloc.alloc(GraphEdgeJson, res.edges.len);
    defer alloc.free(edges);
    for (res.edges, 0..) |edge, i| {
        edges[i] = .{
            .source = edge.source,
            .target = edge.target,
            .edge_type = edge.edge_type,
            .weight = edge.weight,
            .created_at = edge.created_at,
            .updated_at = edge.updated_at,
            .metadata = edge.metadata,
        };
    }
    return try jsonStringifyAlloc(alloc, GraphEdgesResponseJson{ .edges = edges });
}

pub fn parseGraphEdgesResponse(alloc: std.mem.Allocator, body: []const u8) !GraphEdgesResponse {
    var parsed = try std.json.parseFromSlice(GraphEdgesResponseJson, alloc, body, .{});
    defer parsed.deinit();
    const edges = try alloc.alloc(graph_mod.Edge, parsed.value.edges.len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |edge| graph_mod.GraphIndex.freeEdge(alloc, edge);
        alloc.free(edges);
    }
    for (parsed.value.edges, 0..) |edge, i| {
        edges[i] = try cloneGraphEdge(alloc, edge);
        initialized += 1;
    }
    return .{ .edges = edges };
}

fn popBestFrontierIndex(frontier: []const FrontierState) usize {
    var best_index: usize = 0;
    var i: usize = 1;
    while (i < frontier.len) : (i += 1) {
        if (frontier[i].cost < frontier[best_index].cost) {
            best_index = i;
        } else if (frontier[i].cost == frontier[best_index].cost and frontier[i].depth < frontier[best_index].depth) {
            best_index = i;
        }
    }
    return best_index;
}

fn edgeWeightFromNode(node: graph_query_mod.GraphResultNode) f64 {
    if (node.path_edges) |edges| {
        if (edges.len > 0) return edges[edges.len - 1].weight;
    }
    return node.distance;
}

fn graphPathToResultNode(
    alloc: std.mem.Allocator,
    path: db_mod.types.GraphPath,
) !graph_query_mod.GraphResultNode {
    const target_key = if (path.nodes.len > 0) path.nodes[path.nodes.len - 1] else "";
    return .{
        .key = try alloc.dupe(u8, target_key),
        .depth = path.length,
        .distance = path.total_weight,
        .path = try dupPath(alloc, path.nodes),
        .path_edges = if (path.edges.len > 0) try dupPathEdgesFromGraphPath(alloc, path.edges) else null,
    };
}

fn pathStateToGraphPath(
    alloc: std.mem.Allocator,
    path_states: []const PathState,
    path_state_id: u32,
) !db_mod.types.GraphPath {
    const nodes = try reconstructPath(alloc, path_states, path_state_id);
    errdefer freePathArray(alloc, nodes);
    const edges_info = try reconstructPathEdges(alloc, path_states, path_state_id);
    errdefer freePathEdges(alloc, edges_info);

    const edges = try alloc.alloc(graph_paths_mod.PathEdge, edges_info.len);
    errdefer alloc.free(edges);
    for (edges_info, 0..) |edge, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
    }
    freePathEdges(alloc, edges_info);

    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = path_states[path_state_id].distance,
        .length = @intCast(edges.len),
    };
}

fn cloneGraphPath(
    alloc: std.mem.Allocator,
    source: db_mod.types.GraphPath,
) !db_mod.types.GraphPath {
    const nodes = try dupPath(alloc, source.nodes);
    errdefer freePathArray(alloc, nodes);
    const edges = try alloc.alloc(graph_paths_mod.PathEdge, source.edges.len);
    errdefer alloc.free(edges);
    for (source.edges, 0..) |edge, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
    }
    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = source.total_weight,
        .length = source.length,
    };
}

fn dupPathEdgesFromGraphPath(
    alloc: std.mem.Allocator,
    edges: []const graph_paths_mod.PathEdge,
) ![]graph_query_mod.PathEdgeInfo {
    const out = try alloc.alloc(graph_query_mod.PathEdgeInfo, edges.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |edge| freeOwnedPathEdge(alloc, edge);
        alloc.free(out);
    }
    for (edges, 0..) |edge, i| {
        out[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        initialized += 1;
    }
    return out;
}

fn graphPathToKey(alloc: std.mem.Allocator, path: db_mod.types.GraphPath) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (path.nodes, 0..) |node, i| {
        if (i > 0) try out.appendSlice(alloc, "->");
        try out.appendSlice(alloc, node);
    }
    return try out.toOwnedSlice(alloc);
}

fn rootPathMatches(a: db_mod.types.GraphPath, b: db_mod.types.GraphPath, spur_idx: usize) bool {
    if (a.nodes.len <= spur_idx or b.nodes.len <= spur_idx) return false;
    var i: usize = 0;
    while (i <= spur_idx) : (i += 1) {
        if (!std.mem.eql(u8, a.nodes[i], b.nodes[i])) return false;
    }
    return true;
}

fn joinDistributedPaths(
    alloc: std.mem.Allocator,
    root_nodes: []const []const u8,
    root_edges: []const graph_paths_mod.PathEdge,
    spur_path: db_mod.types.GraphPath,
) !db_mod.types.GraphPath {
    const node_count = root_nodes.len + spur_path.nodes.len;
    const nodes = try alloc.alloc([]const u8, node_count);
    var node_initialized: usize = 0;
    errdefer {
        for (nodes[0..node_initialized]) |node| alloc.free(node);
        alloc.free(nodes);
    }
    for (root_nodes, 0..) |node, i| {
        nodes[i] = try alloc.dupe(u8, node);
        node_initialized += 1;
    }
    for (spur_path.nodes, 0..) |node, i| {
        nodes[root_nodes.len + i] = try alloc.dupe(u8, node);
        node_initialized += 1;
    }

    const edge_count = root_edges.len + spur_path.edges.len;
    const edges = try alloc.alloc(graph_paths_mod.PathEdge, edge_count);
    var edge_initialized: usize = 0;
    errdefer {
        for (edges[0..edge_initialized]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
        alloc.free(edges);
    }
    for (root_edges, 0..) |edge, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        edge_initialized += 1;
    }
    for (spur_path.edges, 0..) |edge, i| {
        edges[root_edges.len + i] = .{
            .source = try alloc.dupe(u8, edge.source),
            .target = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
        };
        edge_initialized += 1;
    }

    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = computeGraphPathScore(root_edges, spur_path.edges),
        .length = @intCast(edge_count),
    };
}

fn bestPathIndex(paths: []const db_mod.types.GraphPath, mode: graph_paths_mod.PathWeightMode) usize {
    var best_idx: usize = 0;
    for (paths[1..], 1..) |path, i| {
        if (comparePathScore(path, paths[best_idx], mode) < 0) best_idx = i;
    }
    return best_idx;
}

fn comparePathScore(a: db_mod.types.GraphPath, b: db_mod.types.GraphPath, mode: graph_paths_mod.PathWeightMode) i8 {
    const sa = graphPathScore(a, mode);
    const sb = graphPathScore(b, mode);
    if (sa < sb) return -1;
    if (sa > sb) return 1;
    if (a.length < b.length) return -1;
    if (a.length > b.length) return 1;
    return 0;
}

fn graphPathScore(path: db_mod.types.GraphPath, mode: graph_paths_mod.PathWeightMode) f64 {
    return switch (mode) {
        .min_hops => @floatFromInt(path.length),
        .min_weight => path.total_weight,
        .max_weight => blk: {
            var score: f64 = 0;
            for (path.edges) |edge| {
                if (edge.weight <= 0.0) return std.math.inf(f64);
                score += -@log(edge.weight);
            }
            break :blk score;
        },
    };
}

fn computeGraphPathScore(
    root_edges: []const graph_paths_mod.PathEdge,
    spur_edges: []const graph_paths_mod.PathEdge,
) f64 {
    var total: f64 = 0;
    for (root_edges) |edge| total += edge.weight;
    for (spur_edges) |edge| total += edge.weight;
    return total;
}

fn allocEdgeExclusionKey(
    alloc: std.mem.Allocator,
    src: []const u8,
    tgt: []const u8,
    edge_type: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}->{s}:{s}", .{ src, tgt, edge_type });
}

fn edgeCost(_: FrontierState, node: graph_query_mod.GraphResultNode, mode: graph_paths_mod.PathWeightMode) f64 {
    return switch (mode) {
        .min_hops => 1.0,
        .min_weight => edgeWeightFromNode(node),
        .max_weight => blk: {
            const weight = edgeWeightFromNode(node);
            if (weight <= 0.0) break :blk std.math.inf(f64);
            break :blk -@log(weight);
        },
    };
}

fn appendPathStateFromWeightedStep(
    alloc: std.mem.Allocator,
    state: *QueryState,
    parent: FrontierState,
    node: graph_query_mod.GraphResultNode,
    mode: graph_paths_mod.PathWeightMode,
) !u32 {
    const parent_id = parent.path_state_id orelse return error.InvalidQueryRequest;
    const local_path_edges = node.path_edges orelse &.{};
    if (local_path_edges.len > 1) return error.InvalidQueryRequest;

    try state.path_states.append(alloc, .{
        .key = try alloc.dupe(u8, node.key),
        .depth = parent.depth + 1,
        .distance = parent.distance + edgeWeightFromNode(node),
        .cost = parent.cost + edgeCost(parent, node, mode),
        .parent = parent_id,
        .incoming_edge = if (local_path_edges.len > 0) try clonePathEdge(alloc, local_path_edges[0]) else null,
    });
    return @intCast(state.path_states.items.len - 1);
}

fn materializePathStateNode(
    alloc: std.mem.Allocator,
    state: *QueryState,
    path_state_id: u32,
) !graph_query_mod.GraphResultNode {
    const path_state = state.path_states.items[path_state_id];
    const path = try reconstructPath(alloc, state.path_states.items, path_state_id);
    errdefer freePathArray(alloc, path);
    const path_edges = try reconstructPathEdges(alloc, state.path_states.items, path_state_id);
    errdefer freePathEdges(alloc, path_edges);

    return .{
        .key = try alloc.dupe(u8, path_state.key),
        .depth = path_state.depth,
        .distance = path_state.distance,
        .path = path,
        .path_edges = if (path_edges.len > 0) path_edges else blk: {
            alloc.free(path_edges);
            break :blk null;
        },
    };
}

fn resolveStartFrontier(
    alloc: std.mem.Allocator,
    state: *QueryState,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    selector: graph_query_mod.NodeSelector,
    include_paths: bool,
) ![]FrontierState {
    const keys = try resolveSelectorKeys(alloc, req, base_result, prior_results, selector);
    defer freeKeys(alloc, keys);

    const out = try alloc.alloc(FrontierState, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (keys, 0..) |key, i| {
        const owned_key = try alloc.dupe(u8, key);
        const path_state_id = if (include_paths) try appendRootPathState(alloc, state, key) else null;
        out[i] = .{
            .key = owned_key,
            .depth = 0,
            .distance = 0,
            .cost = 0,
            .path_state_id = path_state_id,
        };
        initialized += 1;
    }
    return out;
}

fn resolveSelectorKeys(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    selector: graph_query_mod.NodeSelector,
) ![][]u8 {
    return switch (selector) {
        .keys => |keys| dupKeys(alloc, keys),
        .result_ref => |result_ref| resolveResultRefKeys(alloc, req, base_result, prior_results, result_ref),
    };
}

fn graphTargetKeyAllowed(target_keys: []const []const u8, key: []const u8) bool {
    if (target_keys.len == 0) return true;
    for (target_keys) |target_key| {
        if (std.mem.eql(u8, target_key, key)) return true;
    }
    return false;
}

fn resolveResultRefKeys(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    result_ref: graph_query_mod.ResultRef,
) ![][]u8 {
    if (req.identity_read_generation == null) return error.UnsupportedQueryRequest;

    if (std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) {
        const name = result_ref.ref["$graph_results.".len..];
        for (prior_results) |graph_result| {
            if (!std.mem.eql(u8, graph_result.name, name)) continue;
            if (result_ref.limit == 0 and @as(u64, graph_result.total_hits) > graph_result.nodes.len) return error.UnsupportedQueryRequest;
            const count: usize = if (result_ref.limit == 0) graph_result.nodes.len else @min(graph_result.nodes.len, result_ref.limit);
            const out = try alloc.alloc([]u8, count);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |key| alloc.free(key);
                alloc.free(out);
            }
            for (graph_result.nodes[0..count], 0..) |node, i| {
                out[i] = try alloc.dupe(u8, node.key);
                initialized += 1;
            }
            return out;
        }
        return error.GraphResultRefNotImplemented;
    }

    if (!std.mem.eql(u8, result_ref.ref, "$fused_results") and
        !std.mem.eql(u8, result_ref.ref, "$full_text_results") and
        !std.mem.eql(u8, result_ref.ref, "$embeddings_results"))
    {
        return error.GraphResultRefNotImplemented;
    }

    if (result_ref.limit == 0 and baseResultRefMayBeIncomplete(req, base_result)) return error.UnsupportedQueryRequest;
    const count: usize = if (result_ref.limit == 0) base_result.hits.len else @min(base_result.hits.len, result_ref.limit);
    const out = try alloc.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(key);
        alloc.free(out);
    }
    for (base_result.hits[0..count], 0..) |hit, i| {
        out[i] = try alloc.dupe(u8, hit.id);
        initialized += 1;
    }
    return out;
}

fn baseResultRefMayBeIncomplete(req: db_mod.types.SearchRequest, base_result: db_mod.types.SearchResult) bool {
    if (@as(u64, base_result.total_hits) > base_result.hits.len) return true;
    return req.limit > 0 and base_result.hits.len >= req.limit;
}

fn graphTraversalTensorAccessPathAlloc(alloc: std.mem.Allocator, index_name: []const u8) !OwnedGraphTensorAccessPath {
    var plan = (try algebraic_planner.planGraphTraversalTensorProgramAlloc(alloc, index_name, false)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    return try cloneGraphTensorAccessPathAlloc(alloc, plan.access_paths[0]);
}

fn cloneGraphTensorAccessPathAlloc(alloc: std.mem.Allocator, path: algebraic_ir.PhysicalAccessPath) !OwnedGraphTensorAccessPath {
    const owner = try alloc.dupe(u8, path.owner);
    errdefer alloc.free(owner);
    const fragments = try alloc.dupe(algebraic_ir.TensorFragment, path.fragments);
    errdefer alloc.free(fragments);
    const output_dims = try alloc.dupe(algebraic_ir.Dimension, path.output_dims);
    errdefer alloc.free(output_dims);
    const law_ids = try alloc.dupe(algebraic_law.Id, path.law_ids);
    errdefer alloc.free(law_ids);
    return .{
        .owner = owner,
        .layout = path.layout,
        .fragments = fragments,
        .output_dims = output_dims,
        .law_ids = law_ids,
    };
}

fn enumSliceEql(comptime T: type, left: []const T, right: []const T) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn graphTensorAccessPathEql(left: algebraic_ir.PhysicalAccessPath, right: algebraic_ir.PhysicalAccessPath) bool {
    return std.mem.eql(u8, left.owner, right.owner) and
        left.layout == right.layout and
        enumSliceEql(algebraic_ir.TensorFragment, left.fragments, right.fragments) and
        enumSliceEql(algebraic_ir.Dimension, left.output_dims, right.output_dims) and
        enumSliceEql(algebraic_law.Id, left.law_ids, right.law_ids);
}

fn graphTensorProgramJsonValueAlloc(
    alloc: std.mem.Allocator,
    program: *const query_contract.OwnedAlgebraicTensorProgramEnvelope,
) !std.json.Parsed(std.json.Value) {
    var view = try program.asProgramAlloc(alloc);
    defer view.deinit(alloc);
    const encoded = try query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, view.program);
    defer alloc.free(encoded);
    return std.json.parseFromSlice(std.json.Value, alloc, encoded, .{}) catch return error.InvalidQueryRequest;
}

fn parseGraphTensorProgramJsonValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    const encoded = try jsonStringifyAlloc(alloc, value);
    defer alloc.free(encoded);
    return try query_contract.parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
}

pub fn graphTraversalTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    target_constraints: bool,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    var plan = (try algebraic_planner.planGraphTraversalTensorProgramAlloc(alloc, index_name, target_constraints)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    return try cloneGraphTensorProgramEnvelopeAlloc(alloc, plan.asProgram());
}

pub fn graphEdgesTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    var plan = (try algebraic_planner.planGraphEdgesTensorProgramAlloc(alloc, index_name)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    return try cloneGraphTensorProgramEnvelopeAlloc(alloc, plan.asProgram());
}

fn cloneGraphTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    program: algebraic_ir.TensorProgram,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    const encoded = try query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(alloc, program);
    defer alloc.free(encoded);
    return try query_contract.parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
}

pub fn validateGraphExpandTensorAccessPath(alloc: std.mem.Allocator, req: GraphExpandRequest) !void {
    try validateGraphExpandTensorAccessPathParts(
        alloc,
        req.index_name,
        req.params.algebraic_semiring,
        req.target_constraint_keys.len > 0,
        req.tensor_access_path,
        if (req.tensor_program) |*program| program else null,
    );
}

fn validateGraphExpandTensorAccessPathParts(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    algebraic_semiring: bool,
    target_constraints: bool,
    tensor_access_path: ?OwnedGraphTensorAccessPath,
    tensor_program: ?*const query_contract.OwnedAlgebraicTensorProgramEnvelope,
) !void {
    if (!algebraic_semiring) {
        if (target_constraints) return error.InvalidQueryRequest;
        return;
    }
    const selected = tensor_access_path orelse return error.InvalidQueryRequest;
    const selected_path = selected.asAccessPath();
    var plan = (try algebraic_planner.planGraphTraversalTensorProgramAlloc(alloc, index_name, target_constraints)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    if (!graphTensorAccessPathEql(selected_path, plan.access_paths[0])) return error.InvalidQueryRequest;
    const selected_program = tensor_program orelse return error.InvalidQueryRequest;
    var selected_view = try selected_program.asProgramAlloc(alloc);
    defer selected_view.deinit(alloc);
    if (!algebraic_ir.graphTraversalProgramMatchesTarget(selected_view.program, index_name, target_constraints)) return error.InvalidQueryRequest;
    if (!(try algebraic_ir.tensorProgramProof(alloc, &.{selected_path}, selected_view.program)).safe()) return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, selected_program.program_id, plan.program_id)) return error.InvalidQueryRequest;
}

pub fn validateGraphEdgesTensorAccessPath(alloc: std.mem.Allocator, req: GraphEdgesRequest) !void {
    try validateGraphEdgesTensorAccessPathParts(alloc, req.index_name, req.tensor_access_path, if (req.tensor_program) |*program| program else null);
}

fn validateGraphEdgesTensorAccessPathParts(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    tensor_access_path: ?OwnedGraphTensorAccessPath,
    tensor_program: ?*const query_contract.OwnedAlgebraicTensorProgramEnvelope,
) !void {
    const selected = tensor_access_path orelse return error.InvalidQueryRequest;
    const selected_path = selected.asAccessPath();
    var plan = (try algebraic_planner.planGraphEdgesTensorProgramAlloc(alloc, index_name)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    if (!graphTensorAccessPathEql(selected_path, plan.access_paths[0])) return error.InvalidQueryRequest;
    const selected_program = tensor_program orelse return error.InvalidQueryRequest;
    var selected_view = try selected_program.asProgramAlloc(alloc);
    defer selected_view.deinit(alloc);
    if (!algebraic_ir.graphEdgesProgramMatchesTarget(selected_view.program, index_name)) return error.InvalidQueryRequest;
    if (!(try algebraic_ir.tensorProgramProof(alloc, &.{selected_path}, selected_view.program)).safe()) return error.InvalidQueryRequest;
    if (!std.mem.eql(u8, selected_program.program_id, plan.program_id)) return error.InvalidQueryRequest;
}

pub fn makeGraphExpandRequest(
    alloc: std.mem.Allocator,
    named_query: db_mod.types.NamedGraphQuery,
    frontier: []const FrontierState,
    frontier_ids: []const u32,
    exclude_keys: []const []const u8,
    exclude_edges: []const []const u8,
    include_paths: bool,
) !GraphExpandRequest {
    return try makeGraphExpandRequestWithAlgebraicMode(
        alloc,
        named_query,
        frontier,
        frontier_ids,
        exclude_keys,
        exclude_edges,
        include_paths,
        named_query.query.params.algebraic_semiring,
    );
}

fn makeGraphExpandRequestWithAlgebraicMode(
    alloc: std.mem.Allocator,
    named_query: db_mod.types.NamedGraphQuery,
    frontier: []const FrontierState,
    frontier_ids: []const u32,
    exclude_keys: []const []const u8,
    exclude_edges: []const []const u8,
    include_paths: bool,
    algebraic_semiring_selected: bool,
) !GraphExpandRequest {
    return try makeGraphExpandRequestWithAlgebraicModeAndTargetConstraints(
        alloc,
        named_query,
        frontier,
        frontier_ids,
        exclude_keys,
        exclude_edges,
        include_paths,
        algebraic_semiring_selected,
        &.{},
    );
}

fn makeGraphExpandRequestWithAlgebraicModeAndTargetConstraints(
    alloc: std.mem.Allocator,
    named_query: db_mod.types.NamedGraphQuery,
    frontier: []const FrontierState,
    frontier_ids: []const u32,
    exclude_keys: []const []const u8,
    exclude_edges: []const []const u8,
    include_paths: bool,
    algebraic_semiring_selected: bool,
    target_constraint_keys: []const []const u8,
) !GraphExpandRequest {
    var params = named_query.query.params;
    params.edge_types = try dupConstStrings(alloc, named_query.query.params.edge_types);
    params.algebraic_semiring = params.algebraic_semiring or algebraic_semiring_selected;
    params.max_depth = 1;
    params.deduplicate = true;
    params.include_paths = include_paths;
    params.weight_mode = .min_hops;

    const owned_frontier = try alloc.alloc(GraphFrontierItem, frontier_ids.len);
    var frontier_initialized: usize = 0;
    errdefer {
        for (owned_frontier[0..frontier_initialized]) |*item| item.deinit(alloc);
        alloc.free(owned_frontier);
    }
    for (frontier_ids, 0..) |frontier_id, i| {
        const item = frontier[frontier_id];
        owned_frontier[i] = .{
            .id = frontier_id,
            .key = try alloc.dupe(u8, item.key),
            .depth = item.depth,
            .distance = item.distance,
        };
        frontier_initialized += 1;
    }

    var tensor_access_path: ?OwnedGraphTensorAccessPath = null;
    errdefer if (tensor_access_path) |*path| path.deinit(alloc);
    var tensor_program: ?query_contract.OwnedAlgebraicTensorProgramEnvelope = null;
    errdefer if (tensor_program) |*program| program.deinit(alloc);
    if (params.algebraic_semiring) {
        tensor_access_path = try graphTraversalTensorAccessPathAlloc(alloc, named_query.query.index_name);
        tensor_program = try graphTraversalTensorProgramEnvelopeAlloc(alloc, named_query.query.index_name, target_constraint_keys.len > 0);
    }

    return .{
        .name = try alloc.dupe(u8, named_query.name),
        .index_name = try alloc.dupe(u8, named_query.query.index_name),
        .frontier = owned_frontier,
        .exclude_keys = try dupKeys(alloc, exclude_keys),
        .exclude_edges = try dupKeys(alloc, exclude_edges),
        .target_constraint_keys = try dupSortedUniqueKeys(alloc, target_constraint_keys),
        .params = params,
        .tensor_access_path = tensor_access_path,
        .tensor_program = tensor_program,
    };
}

fn catalogGraphIndexEnablesAlgebraicSemiring(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    index_name: []const u8,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return false;
    var lookup = (try indexes_api.lookupSingleIndexConfig(alloc, table.indexes_json, index_name)) orelse return false;
    defer lookup.deinit();
    if (indexes_api.inferIndexType(index_name, lookup.config) != .graph) return false;
    return graphConfigEnablesAlgebraicSemiring(lookup.config);
}

fn graphConfigEnablesAlgebraicSemiring(config: std.json.Value) bool {
    if (config != .object) return false;
    const planning = config.object.get("algebraic_planning") orelse return false;
    if (planning != .object) return false;
    const bounded = planning.object.get("bounded_traversal") orelse return false;
    if (bounded != .object) return false;
    const law = bounded.object.get("law") orelse return false;
    if (law != .string or !std.mem.eql(u8, law.string, "provenance_semiring")) return false;
    if (bounded.object.get("enabled")) |enabled| {
        if (enabled != .bool) return false;
        return enabled.bool;
    }
    return true;
}

pub fn frontierItemToSearchRequest(
    alloc: std.mem.Allocator,
    req: GraphExpandRequest,
    item: GraphFrontierItem,
) !db_mod.types.SearchRequest {
    try validateGraphExpandTensorAccessPath(alloc, req);

    const frontier_keys = try alloc.alloc([]const u8, 1);
    frontier_keys[0] = "";
    errdefer {
        if (frontier_keys[0].len > 0) alloc.free(frontier_keys[0]);
        alloc.free(frontier_keys);
    }
    frontier_keys[0] = try alloc.dupe(u8, item.key);

    var params = req.params;
    params.edge_types = try dupConstStrings(alloc, req.params.edge_types);
    errdefer freeConstStrings(alloc, params.edge_types);

    const name = try alloc.dupe(u8, req.name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, req.index_name);
    errdefer alloc.free(index_name);

    const graph_queries = try alloc.alloc(db_mod.types.NamedGraphQuery, 1);
    errdefer alloc.free(graph_queries);
    graph_queries[0] = .{
        .name = name,
        .query = .{
            .query_type = .neighbors,
            .index_name = index_name,
            .start_nodes = .{ .keys = frontier_keys },
            .params = params,
        },
    };

    return .{
        .query = .{ .match_none = {} },
        .graph_queries = graph_queries,
        .limit = 0,
        .include_stored = true,
        .identity_read_generation = req.identity_read_generation,
        .resolved_doc_filter = req.resolved_doc_filter,
        .resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
    };
}

fn enumNameArrayAlloc(comptime T: type, alloc: std.mem.Allocator, values: []const T) ![][]const u8 {
    const out = try alloc.alloc([]const u8, values.len);
    for (values, 0..) |value, i| out[i] = @tagName(value);
    return out;
}

fn graphTensorAccessPathJsonAlloc(
    alloc: std.mem.Allocator,
    path: OwnedGraphTensorAccessPath,
    fragment_names: *?[][]const u8,
    output_dim_names: *?[][]const u8,
    law_names: *?[][]const u8,
) !GraphTensorAccessPathJson {
    fragment_names.* = try enumNameArrayAlloc(algebraic_ir.TensorFragment, alloc, path.fragments);
    output_dim_names.* = try enumNameArrayAlloc(algebraic_ir.Dimension, alloc, path.output_dims);
    law_names.* = try enumNameArrayAlloc(algebraic_law.Id, alloc, path.law_ids);
    return .{
        .owner = path.owner,
        .layout = @tagName(path.layout),
        .fragments = fragment_names.*.?,
        .output_dims = output_dim_names.*.?,
        .law_ids = law_names.*.?,
    };
}

fn parseGraphTensorAccessPathAlloc(
    alloc: std.mem.Allocator,
    input: GraphTensorAccessPathJson,
) !OwnedGraphTensorAccessPath {
    const layout = std.meta.stringToEnum(algebraic_ir.PhysicalLayout, input.layout) orelse return error.InvalidQueryRequest;
    const owner = try alloc.dupe(u8, input.owner);
    errdefer alloc.free(owner);
    const fragments = try alloc.alloc(algebraic_ir.TensorFragment, input.fragments.len);
    errdefer alloc.free(fragments);
    for (input.fragments, 0..) |fragment, i| {
        fragments[i] = std.meta.stringToEnum(algebraic_ir.TensorFragment, fragment) orelse return error.InvalidQueryRequest;
    }
    const output_dims = try alloc.alloc(algebraic_ir.Dimension, input.output_dims.len);
    errdefer alloc.free(output_dims);
    for (input.output_dims, 0..) |dim, i| {
        output_dims[i] = std.meta.stringToEnum(algebraic_ir.Dimension, dim) orelse return error.InvalidQueryRequest;
    }
    const law_ids = try alloc.alloc(algebraic_law.Id, input.law_ids.len);
    errdefer alloc.free(law_ids);
    for (input.law_ids, 0..) |law_id, i| {
        law_ids[i] = algebraic_law.Id.parse(law_id) orelse return error.InvalidQueryRequest;
    }
    return .{
        .owner = owner,
        .layout = layout,
        .fragments = fragments,
        .output_dims = output_dims,
        .law_ids = law_ids,
    };
}

pub fn freeExpandSearchRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) void {
    for (req.graph_queries) |graph_query| {
        alloc.free(@constCast(graph_query.name));
        alloc.free(@constCast(graph_query.query.index_name));
        switch (graph_query.query.start_nodes) {
            .keys => |keys| {
                for (keys) |key| alloc.free(@constCast(key));
                alloc.free(keys);
            },
            .result_ref => {},
        }
        if (graph_query.query.target_nodes) |target_nodes| {
            switch (target_nodes) {
                .keys => |keys| {
                    for (keys) |key| alloc.free(@constCast(key));
                    alloc.free(keys);
                },
                .result_ref => {},
            }
        }
        freeConstStrings(alloc, graph_query.query.params.edge_types);
    }
    if (req.graph_queries.len > 0) alloc.free(req.graph_queries);
}

pub fn encodeGraphExpandRequest(alloc: std.mem.Allocator, req: GraphExpandRequest) ![]u8 {
    try validateGraphExpandTensorAccessPath(alloc, req);
    var fragment_names: ?[][]const u8 = null;
    defer if (fragment_names) |names| alloc.free(names);
    var output_dim_names: ?[][]const u8 = null;
    defer if (output_dim_names) |names| alloc.free(names);
    var law_names: ?[][]const u8 = null;
    defer if (law_names) |names| alloc.free(names);
    const tensor_access_path: ?GraphTensorAccessPathJson = if (req.tensor_access_path) |path|
        try graphTensorAccessPathJsonAlloc(alloc, path, &fragment_names, &output_dim_names, &law_names)
    else
        null;
    var tensor_program_json: ?std.json.Parsed(std.json.Value) = if (req.tensor_program) |*program|
        try graphTensorProgramJsonValueAlloc(alloc, program)
    else
        null;
    defer if (tensor_program_json) |*program| program.deinit();

    const frontier = try alloc.alloc(GraphFrontierItemJson, req.frontier.len);
    defer alloc.free(frontier);
    for (req.frontier, 0..) |item, i| {
        frontier[i] = .{
            .id = item.id,
            .key = item.key,
            .depth = item.depth,
            .distance = item.distance,
        };
    }
    const encoded = try jsonStringifyAlloc(alloc, GraphExpandRequestJson{
        .name = req.name,
        .index_name = req.index_name,
        .frontier = frontier,
        .exclude_keys = req.exclude_keys,
        .exclude_edges = req.exclude_edges,
        .target_constraint_keys = req.target_constraint_keys,
        .topology_epoch = req.topology_epoch,
        .identity_read_generation = req.identity_read_generation,
        .params = .{
            .edge_types = req.params.edge_types,
            .direction = switch (req.params.direction) {
                .out => "out",
                .in => "in",
                .both => "both",
            },
            .max_depth = req.params.max_depth,
            .max_results = req.params.max_results,
            .min_weight = req.params.min_weight,
            .max_weight = req.params.max_weight,
            .deduplicate = req.params.deduplicate,
            .include_paths = req.params.include_paths,
            .weight_mode = switch (req.params.weight_mode) {
                .min_hops => "min_hops",
                .min_weight => "min_weight",
                .max_weight => "max_weight",
            },
            .algebraic_semiring = req.params.algebraic_semiring,
        },
        .tensor_access_path = tensor_access_path,
        .tensor_program = if (tensor_program_json) |program| program.value else null,
    });
    if (req.resolved_doc_filter == null) return encoded;
    defer alloc.free(encoded);
    return try appendResolvedDocFilterToObjectAlloc(alloc, encoded, req.resolved_doc_filter.?, req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest);
}

fn appendResolvedDocFilterToObjectAlloc(
    alloc: std.mem.Allocator,
    encoded_object: []const u8,
    resolved_doc_filter: *const anyopaque,
    context: db_mod.types.ResolvedDocFilterWireContext,
) ![]u8 {
    if (encoded_object.len == 0 or encoded_object[encoded_object.len - 1] != '}') return error.InvalidQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, encoded_object[0 .. encoded_object.len - 1]);
    var first = false;
    try db_mod.doc_filter_wire.appendFilterFieldAlloc(alloc, &out, &first, resolved_doc_filter, context);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn parseGraphExpandRequest(alloc: std.mem.Allocator, body: []const u8) !GraphExpandRequest {
    var parsed = try std.json.parseFromSlice(GraphExpandRequestJson, alloc, body, .{});
    defer parsed.deinit();

    if (parsed.value.params.algebraic_semiring and parsed.value.tensor_access_path == null) return error.InvalidQueryRequest;
    if (parsed.value.params.algebraic_semiring and parsed.value.tensor_program == null) return error.InvalidQueryRequest;
    var tensor_access_path: ?OwnedGraphTensorAccessPath = if (parsed.value.tensor_access_path) |path|
        try parseGraphTensorAccessPathAlloc(alloc, path)
    else
        null;
    errdefer if (tensor_access_path) |*path| path.deinit(alloc);
    var tensor_program: ?query_contract.OwnedAlgebraicTensorProgramEnvelope = if (parsed.value.tensor_program) |program|
        try parseGraphTensorProgramJsonValueAlloc(alloc, program)
    else
        null;
    errdefer if (tensor_program) |*program| program.deinit(alloc);
    const target_constraint_keys = try dupSortedUniqueKeys(alloc, parsed.value.target_constraint_keys);
    errdefer {
        for (target_constraint_keys) |key| alloc.free(key);
        if (target_constraint_keys.len > 0) alloc.free(target_constraint_keys);
    }
    try validateGraphExpandTensorAccessPathParts(
        alloc,
        parsed.value.index_name,
        parsed.value.params.algebraic_semiring,
        target_constraint_keys.len > 0,
        tensor_access_path,
        if (tensor_program) |*program| program else null,
    );

    const frontier = try alloc.alloc(GraphFrontierItem, parsed.value.frontier.len);
    var frontier_initialized: usize = 0;
    errdefer {
        for (frontier[0..frontier_initialized]) |*item| item.deinit(alloc);
        alloc.free(frontier);
    }
    for (parsed.value.frontier, 0..) |item, i| {
        frontier[i] = .{
            .id = item.id,
            .key = try alloc.dupe(u8, item.key),
            .depth = item.depth,
            .distance = item.distance,
        };
        frontier_initialized += 1;
    }

    var parsed_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null;
    errdefer if (parsed_filter) |*filter| filter.deinit(alloc);
    if (parsed.value._resolved_doc_filter) |value| {
        parsed_filter = try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, value);
    }
    const identity_read_generation = try identityGenerationFromResolvedFilterEnvelope(parsed.value.identity_read_generation, if (parsed_filter) |*filter| filter else null);

    const out = GraphExpandRequest{
        .name = try alloc.dupe(u8, parsed.value.name),
        .index_name = try alloc.dupe(u8, parsed.value.index_name),
        .frontier = frontier,
        .exclude_keys = try dupKeys(alloc, parsed.value.exclude_keys),
        .exclude_edges = try dupKeys(alloc, parsed.value.exclude_edges),
        .target_constraint_keys = target_constraint_keys,
        .topology_epoch = parsed.value.topology_epoch,
        .identity_read_generation = identity_read_generation,
        .resolved_doc_filter = if (parsed_filter) |filter| filter.resolved_doc_filter else null,
        .resolved_doc_filter_owned = parsed_filter != null,
        .resolved_doc_filter_wire_context = if (parsed_filter) |filter| filter.context else null,
        .params = .{
            .edge_types = try dupConstStrings(alloc, parsed.value.params.edge_types),
            .direction = if (std.mem.eql(u8, parsed.value.params.direction, "in"))
                .in
            else if (std.mem.eql(u8, parsed.value.params.direction, "both"))
                .both
            else
                .out,
            .max_depth = parsed.value.params.max_depth,
            .max_results = parsed.value.params.max_results,
            .min_weight = parsed.value.params.min_weight,
            .max_weight = parsed.value.params.max_weight,
            .deduplicate = parsed.value.params.deduplicate,
            .include_paths = parsed.value.params.include_paths,
            .weight_mode = if (std.mem.eql(u8, parsed.value.params.weight_mode, "min_weight"))
                .min_weight
            else if (std.mem.eql(u8, parsed.value.params.weight_mode, "max_weight"))
                .max_weight
            else
                .min_hops,
            .algebraic_semiring = parsed.value.params.algebraic_semiring,
        },
        .tensor_access_path = tensor_access_path,
        .tensor_program = tensor_program,
    };
    parsed_filter = null;
    return out;
}

pub fn encodeGraphExpandResponse(alloc: std.mem.Allocator, res: GraphExpandResponse) ![]u8 {
    const expansions = try alloc.alloc(GraphExpansionJson, res.expansions.len);
    defer alloc.free(expansions);
    for (res.expansions, 0..) |expansion, i| {
        expansions[i] = .{
            .frontier_id = expansion.frontier_id,
            .frontier_key = expansion.frontier_key,
            .name = expansion.graph_result.name,
            .total = @intCast(expansion.graph_result.total_hits),
            .nodes = expansion.graph_result.nodes,
            .hits = expansion.graph_result.hits,
        };
    }
    return try jsonStringifyAlloc(alloc, GraphExpandResponseJson{ .expansions = expansions });
}

pub fn parseGraphExpandResponse(alloc: std.mem.Allocator, body: []const u8) !GraphExpandResponse {
    var parsed = try std.json.parseFromSlice(GraphExpandResponseJson, alloc, body, .{});
    defer parsed.deinit();

    const expansions = try alloc.alloc(GraphExpansion, parsed.value.expansions.len);
    var initialized: usize = 0;
    errdefer {
        for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc);
        alloc.free(expansions);
    }
    for (parsed.value.expansions, 0..) |expansion, i| {
        const nodes = if (expansion.nodes.len > 0)
            try cloneGraphNodes(alloc, expansion.nodes)
        else
            @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]);
        errdefer if (nodes.len > 0) {
            for (nodes) |*node| node.deinit(alloc);
            alloc.free(nodes);
        };

        const hits = if (expansion.hits.len > 0)
            try cloneSearchHits(alloc, expansion.hits)
        else
            @constCast((&[_]db_mod.types.SearchHit{})[0..]);
        errdefer if (hits.len > 0) {
            for (hits) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        };

        expansions[i] = .{
            .frontier_id = expansion.frontier_id,
            .frontier_key = try alloc.dupe(u8, expansion.frontier_key),
            .graph_result = .{
                .name = try alloc.dupe(u8, expansion.name),
                .nodes = nodes,
                .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                .hits = hits,
                .total_hits = expansion.total,
            },
        };
        initialized += 1;
    }

    return .{ .expansions = expansions };
}

pub fn cloneGraphSearchResult(
    alloc: std.mem.Allocator,
    src: db_mod.types.GraphSearchResult,
) !db_mod.types.GraphSearchResult {
    const nodes = if (src.nodes.len > 0)
        try cloneGraphNodes(alloc, src.nodes)
    else
        @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]);
    errdefer if (nodes.len > 0) {
        for (nodes) |*node| node.deinit(alloc);
        alloc.free(nodes);
    };

    const hits = if (src.hits.len > 0)
        try cloneSearchHits(alloc, src.hits)
    else
        @constCast((&[_]db_mod.types.SearchHit{})[0..]);
    errdefer if (hits.len > 0) {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    };

    const paths = if (src.paths.len > 0)
        try cloneGraphPaths(alloc, src.paths)
    else
        @constCast((&[_]db_mod.types.GraphPath{})[0..]);
    errdefer if (paths.len > 0) {
        for (paths) |path| graph_paths_mod.freePath(alloc, path);
        alloc.free(paths);
    };

    const matches = if (src.matches.len > 0)
        try cloneGraphPatternMatches(alloc, src.matches)
    else
        @constCast((&[_]db_mod.types.GraphPatternMatch{})[0..]);
    errdefer if (matches.len > 0) {
        for (matches) |*match| match.deinit(alloc);
        alloc.free(matches);
    };

    return .{
        .name = try alloc.dupe(u8, src.name),
        .nodes = nodes,
        .paths = paths,
        .matches = matches,
        .hits = hits,
        .total_hits = src.total_hits,
    };
}

pub fn filterGraphSearchResult(
    alloc: std.mem.Allocator,
    src: db_mod.types.GraphSearchResult,
    exclude_keys: []const []const u8,
    exclude_edges: []const []const u8,
) !db_mod.types.GraphSearchResult {
    if (exclude_keys.len == 0 and exclude_edges.len == 0) return try cloneGraphSearchResult(alloc, src);

    var exclude = std.StringHashMapUnmanaged(void).empty;
    defer exclude.deinit(alloc);
    for (exclude_keys) |key| try exclude.put(alloc, key, {});

    var exclude_edge_set = std.StringHashMapUnmanaged(void).empty;
    defer exclude_edge_set.deinit(alloc);
    for (exclude_edges) |edge| try exclude_edge_set.put(alloc, edge, {});

    var nodes = std.ArrayListUnmanaged(graph_query_mod.GraphResultNode).empty;
    defer {
        for (nodes.items) |*node| node.deinit(alloc);
        nodes.deinit(alloc);
    }
    for (src.nodes) |node| {
        if (exclude.contains(node.key)) continue;
        if (exclude_edge_set.count() > 0) {
            if (node.path_edges) |path_edges| {
                if (path_edges.len > 0) {
                    const edge_key = try allocEdgeExclusionKey(alloc, path_edges[0].source, path_edges[0].target, path_edges[0].edge_type);
                    defer alloc.free(edge_key);
                    if (exclude_edge_set.contains(edge_key)) continue;
                }
            }
        }
        try nodes.append(alloc, try cloneGraphNode(alloc, node));
    }

    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    defer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }
    for (src.hits) |hit| {
        if (exclude.contains(hit.id)) continue;
        try hits.append(alloc, try hit.clone(alloc));
    }

    var paths = std.ArrayListUnmanaged(db_mod.types.GraphPath).empty;
    defer {
        for (paths.items) |path| graph_paths_mod.freePath(alloc, path);
        paths.deinit(alloc);
    }
    for (src.paths) |path| {
        if (try graphPathIsExcluded(alloc, path, &exclude, &exclude_edge_set)) continue;
        try paths.append(alloc, try cloneGraphPath(alloc, path));
    }

    var matches = std.ArrayListUnmanaged(db_mod.types.GraphPatternMatch).empty;
    defer {
        for (matches.items) |*match| match.deinit(alloc);
        matches.deinit(alloc);
    }
    for (src.matches) |match| {
        if (try graphPatternMatchIsExcluded(alloc, match, &exclude, &exclude_edge_set)) continue;
        try matches.append(alloc, try cloneGraphPatternMatch(alloc, match));
    }

    return .{
        .name = try alloc.dupe(u8, src.name),
        .nodes = try nodes.toOwnedSlice(alloc),
        .paths = try paths.toOwnedSlice(alloc),
        .matches = try matches.toOwnedSlice(alloc),
        .hits = try hits.toOwnedSlice(alloc),
        .total_hits = @intCast(nodes.items.len),
    };
}

pub fn emptyGraphSearchResult(
    alloc: std.mem.Allocator,
    name: []const u8,
) !db_mod.types.GraphSearchResult {
    return .{
        .name = try alloc.dupe(u8, name),
        .nodes = @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]),
        .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
        .matches = @constCast((&[_]db_mod.types.GraphPatternMatch{})[0..]),
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
    };
}

fn appendRootPathState(
    alloc: std.mem.Allocator,
    state: *QueryState,
    key: []const u8,
) !u32 {
    try state.path_states.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .depth = 0,
        .distance = 0,
        .cost = 0,
        .parent = null,
        .incoming_edge = null,
    });
    return @intCast(state.path_states.items.len - 1);
}

fn appendPathStateFromStep(
    alloc: std.mem.Allocator,
    state: *QueryState,
    parent: FrontierState,
    node: graph_query_mod.GraphResultNode,
) !u32 {
    const parent_id = parent.path_state_id orelse return error.InvalidQueryRequest;
    const local_path_edges = node.path_edges orelse &.{};
    if (local_path_edges.len > 1) return error.InvalidQueryRequest;

    try state.path_states.append(alloc, .{
        .key = try alloc.dupe(u8, node.key),
        .depth = parent.depth + 1,
        .distance = parent.distance + node.distance,
        .cost = parent.cost + edgeCost(parent, node, .min_hops),
        .parent = parent_id,
        .incoming_edge = if (local_path_edges.len > 0) try clonePathEdge(alloc, local_path_edges[0]) else null,
    });
    return @intCast(state.path_states.items.len - 1);
}

fn materializeResultNode(
    alloc: std.mem.Allocator,
    state: *QueryState,
    parent: FrontierState,
    node: graph_query_mod.GraphResultNode,
    include_paths: bool,
    path_state_id: ?u32,
) !graph_query_mod.GraphResultNode {
    if (!include_paths) {
        return .{
            .key = try alloc.dupe(u8, node.key),
            .depth = parent.depth + 1,
            .distance = parent.distance + node.distance,
            .path = null,
            .path_edges = null,
            .table = if (node.table) |t| try alloc.dupe(u8, t) else null,
        };
    }

    const id = path_state_id orelse return error.InvalidQueryRequest;
    const path_state = state.path_states.items[id];
    const path = try reconstructPath(alloc, state.path_states.items, id);
    errdefer freePathArray(alloc, path);
    const path_edges = try reconstructPathEdges(alloc, state.path_states.items, id);
    errdefer freePathEdges(alloc, path_edges);

    return .{
        .key = try alloc.dupe(u8, path_state.key),
        .depth = path_state.depth,
        .distance = path_state.distance,
        .path = path,
        .path_edges = if (path_edges.len > 0) path_edges else blk: {
            alloc.free(path_edges);
            break :blk null;
        },
        .table = if (node.table) |t| try alloc.dupe(u8, t) else null,
    };
}

fn frontierFromState(
    alloc: std.mem.Allocator,
    state: *QueryState,
    node: graph_query_mod.GraphResultNode,
    path_state_id: ?u32,
) !FrontierState {
    if (path_state_id) |id| {
        const path_state = state.path_states.items[id];
        return .{
            .key = try alloc.dupe(u8, path_state.key),
            .depth = path_state.depth,
            .distance = path_state.distance,
            .cost = path_state.cost,
            .path_state_id = id,
        };
    }

    return .{
        .key = try alloc.dupe(u8, node.key),
        .depth = node.depth,
        .distance = node.distance,
        .cost = node.distance,
        .path_state_id = null,
    };
}

fn dupKeys(alloc: std.mem.Allocator, keys: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(key);
        alloc.free(out);
    }
    for (keys, 0..) |key, i| {
        out[i] = try alloc.dupe(u8, key);
        initialized += 1;
    }
    return out;
}

fn dupSortedUniqueKeys(alloc: std.mem.Allocator, keys: []const []const u8) ![][]u8 {
    if (keys.len == 0) return &.{};
    var out = try dupKeys(alloc, keys);
    std.mem.sort([]u8, out, {}, stringSliceLessThan);

    var write: usize = 0;
    for (out, 0..) |key, read| {
        if (read > 0 and std.mem.eql(u8, key, out[read - 1])) {
            alloc.free(key);
            continue;
        }
        out[write] = key;
        write += 1;
    }
    if (write == out.len) return out;
    return alloc.realloc(out, write) catch |err| {
        for (out[0..write]) |key| alloc.free(key);
        alloc.free(out);
        return err;
    };
}

fn stringSliceLessThan(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.lessThan(u8, left, right);
}

fn freeKeys(alloc: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn testResultRefFailClosedGuards(alloc: std.mem.Allocator) !void {
    {
        const hit = db_mod.types.SearchHit{ .id = @constCast("doc:a") };
        const base_result = db_mod.types.SearchResult{
            .alloc = alloc,
            .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
            .total_hits = 2,
            .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
        };
        const req = db_mod.types.SearchRequest{ .limit = 10, .identity_read_generation = 9 };

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            req,
            base_result,
            &.{},
            .{ .ref = "$full_text_results", .limit = 0 },
        ));

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            .{ .limit = 10 },
            base_result,
            &.{},
            .{ .ref = "$full_text_results", .limit = 1 },
        ));

        const limited = try resolveResultRefKeys(
            alloc,
            req,
            base_result,
            &.{},
            .{ .ref = "$full_text_results", .limit = 1 },
        );
        defer freeKeys(alloc, limited);
        try std.testing.expectEqual(@as(usize, 1), limited.len);
        try std.testing.expectEqualStrings("doc:a", limited[0]);
    }

    {
        const hit = db_mod.types.SearchHit{ .id = @constCast("doc:a") };
        const base_result = db_mod.types.SearchResult{
            .alloc = alloc,
            .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
            .total_hits = 1,
            .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
        };
        const req = db_mod.types.SearchRequest{ .limit = 1, .identity_read_generation = 9 };

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            req,
            base_result,
            &.{},
            .{ .ref = "$fused_results", .limit = 0 },
        ));
    }

    {
        const node = graph_query_mod.GraphResultNode{ .key = "doc:b", .depth = 0, .distance = 0, .path = null, .path_edges = null };
        const graph_result = db_mod.types.GraphSearchResult{
            .name = @constCast("first_hop"),
            .nodes = @constCast((&[_]graph_query_mod.GraphResultNode{node})[0..]),
            .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
            .total_hits = 2,
        };
        const base_result = db_mod.types.SearchResult{
            .alloc = alloc,
            .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
            .total_hits = 0,
            .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
        };

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            .{ .identity_read_generation = 9 },
            base_result,
            &.{graph_result},
            .{ .ref = "$graph_results.first_hop", .limit = 0 },
        ));

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            .{},
            base_result,
            &.{graph_result},
            .{ .ref = "$graph_results.first_hop", .limit = 1 },
        ));

        const limited = try resolveResultRefKeys(
            alloc,
            .{ .identity_read_generation = 9 },
            base_result,
            &.{graph_result},
            .{ .ref = "$graph_results.first_hop", .limit = 1 },
        );
        defer freeKeys(alloc, limited);
        try std.testing.expectEqual(@as(usize, 1), limited.len);
        try std.testing.expectEqualStrings("doc:b", limited[0]);
    }
}

pub fn testHydrateIdentityGenerationAndCrossRangeOrdinalBoundary(alloc: std.mem.Allocator) !void {
    const TestState = struct {
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 22, .table_id = 7, .start_key = "doc:m", .end_key = null },
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11 },
            .{ .group_id = 22 },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            state.expand_calls += 1;
            const node_key = if (group_id == 11) "doc:b" else "doc:o";
            const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc_inner.dupe(u8, node_key),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            const expansions = try alloc_inner.alloc(GraphExpansion, 1);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = try alloc_inner.dupe(u8, req.frontier[0].key),
                .graph_result = .{
                    .name = try alloc_inner.dupe(u8, req.name),
                    .nodes = nodes,
                    .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                    .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            _: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);
            state.hydrate_calls += 1;
            const hits = try alloc_inner.alloc(db_mod.types.SearchHit, req.keys.len);
            var initialized: usize = 0;
            errdefer {
                for (hits[0..initialized]) |*hit| hit.deinit(alloc_inner);
                alloc_inner.free(hits);
            }
            for (req.keys, 0..) |key, i| {
                hits[i] = .{
                    .id = try alloc_inner.dupe(u8, key),
                    .doc_ordinal = 1,
                    .stored_data = try alloc_inner.dupe(u8, "{}"),
                };
                initialized += 1;
            }
            return .{ .hits = hits };
        }
    };

    var state = TestState{};
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{ "doc:a", "doc:n" } },
                    .params = .{},
                },
            },
        },
        .identity_read_generation = 77,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    const results = try executeCrossRange(
        alloc,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    );
    defer {
        for (results) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
    try std.testing.expectEqual(@as(u32, 2), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 2), results[0].hits.len);
    for (results[0].hits) |hit| try std.testing.expect(hit.doc_ordinal == null);
}

pub fn testCrossTableHydrateClearsQueryScopedFilterAndOrdinals(alloc: std.mem.Allocator) !void {
    const TestState = struct {
        filter_ptr: *const anyopaque,
        same_table_calls: u32 = 0,
        cross_table_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
            .{ .table_id = 8, .name = "entities", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null },
            .{ .group_id = 22, .table_id = 8, .start_key = "", .end_key = null },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            return error.UnsupportedQueryRequest;
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), req.keys.len);
            if (std.mem.eql(u8, table_name, "docs")) {
                state.same_table_calls += 1;
                try std.testing.expectEqual(@as(u64, 11), group_id);
                try std.testing.expectEqualStrings("doc:a", req.keys[0]);
                try std.testing.expectEqual(@as(?u64, 44), req.identity_read_generation);
                try std.testing.expect(req.resolved_doc_filter == state.filter_ptr);
                try std.testing.expect(req.resolved_doc_filter_wire_context != null);
            } else if (std.mem.eql(u8, table_name, "entities")) {
                state.cross_table_calls += 1;
                try std.testing.expectEqual(@as(u64, 22), group_id);
                try std.testing.expectEqualStrings("person/ada", req.keys[0]);
                try std.testing.expect(req.identity_read_generation == null);
                try std.testing.expect(req.resolved_doc_filter == null);
                try std.testing.expect(req.resolved_doc_filter_wire_context == null);
            } else {
                return error.UnexpectedTable;
            }

            const hits = try alloc_inner.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{
                .id = try alloc_inner.dupe(u8, req.keys[0]),
                .doc_ordinal = if (std.mem.eql(u8, table_name, "docs")) 7 else 99,
                .stored_data = try alloc_inner.dupe(u8, "{}"),
            };
            return .{ .hits = hits };
        }
    };

    var filter_sentinel: u8 = 0;
    const filter_ptr: *const anyopaque = &filter_sentinel;
    var state = TestState{ .filter_ptr = filter_ptr };
    const context = db_mod.types.ResolvedDocFilterWireContext{
        .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 11 },
        .identity_read_generation = 44,
    };
    const nodes = [_]graph_query_mod.GraphResultNode{
        .{
            .key = "doc:a",
            .depth = 0,
            .distance = 0,
            .path = null,
            .path_edges = null,
        },
        .{
            .key = "person/ada",
            .depth = 1,
            .distance = 1,
            .path = null,
            .path_edges = null,
            .table = "entities",
        },
    };

    const hits = try hydrateHitsForResultNodes(
        alloc,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        0,
        44,
        filter_ptr,
        context,
        nodes[0..],
        .read_index,
    );
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }

    try std.testing.expectEqual(@as(u32, 1), state.same_table_calls);
    try std.testing.expectEqual(@as(u32, 1), state.cross_table_calls);
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("doc:a", hits[0].id);
    try std.testing.expectEqual(@as(?u32, 7), hits[0].doc_ordinal);
    try std.testing.expectEqualStrings("person/ada", hits[1].id);
    try std.testing.expect(hits[1].doc_ordinal == null);
}

fn freeFrontier(alloc: std.mem.Allocator, items: []FrontierState) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn dupConstStrings(alloc: std.mem.Allocator, items: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn freeConstStrings(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(item);
    if (items.len > 0) alloc.free(items);
}

fn freePathArray(alloc: std.mem.Allocator, path: [][]const u8) void {
    for (path) |item| alloc.free(item);
    if (path.len > 0) alloc.free(path);
}

fn reconstructPath(
    alloc: std.mem.Allocator,
    path_states: []const PathState,
    id: u32,
) ![][]const u8 {
    const out = try alloc.alloc([]const u8, pathLength(path_states, id));
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }

    var cursor: ?u32 = id;
    var write_index: usize = out.len;
    while (cursor) |current_id| {
        write_index -= 1;
        out[write_index] = try alloc.dupe(u8, path_states[current_id].key);
        initialized += 1;
        cursor = path_states[current_id].parent;
    }
    return out;
}

fn reconstructPathEdges(
    alloc: std.mem.Allocator,
    path_states: []const PathState,
    id: u32,
) ![]graph_query_mod.PathEdgeInfo {
    const out = try alloc.alloc(graph_query_mod.PathEdgeInfo, pathEdgeLength(path_states, id));
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |edge| freeOwnedPathEdge(alloc, edge);
        alloc.free(out);
    }

    var cursor: ?u32 = id;
    var write_index: usize = out.len;
    while (cursor) |current_id| {
        if (path_states[current_id].incoming_edge) |edge| {
            write_index -= 1;
            out[write_index] = try clonePathEdge(alloc, edge);
            initialized += 1;
        }
        cursor = path_states[current_id].parent;
    }
    return out;
}

fn freePathEdges(alloc: std.mem.Allocator, edges: []graph_query_mod.PathEdgeInfo) void {
    for (edges) |edge| freeOwnedPathEdge(alloc, edge);
    if (edges.len > 0) alloc.free(edges);
}

fn pathLength(path_states: []const PathState, id: u32) usize {
    var len: usize = 0;
    var cursor: ?u32 = id;
    while (cursor) |current_id| {
        len += 1;
        cursor = path_states[current_id].parent;
    }
    return len;
}

fn pathEdgeLength(path_states: []const PathState, id: u32) usize {
    var len: usize = 0;
    var cursor: ?u32 = id;
    while (cursor) |current_id| {
        if (path_states[current_id].incoming_edge != null) len += 1;
        cursor = path_states[current_id].parent;
    }
    return len;
}

fn cloneGraphNodes(
    alloc: std.mem.Allocator,
    nodes: []const graph_query_mod.GraphResultNode,
) ![]graph_query_mod.GraphResultNode {
    const out = try alloc.alloc(graph_query_mod.GraphResultNode, nodes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(out);
    }
    for (nodes, 0..) |node, i| {
        out[i] = try cloneGraphNode(alloc, node);
        initialized += 1;
    }
    return out;
}

fn graphPathIsExcluded(
    alloc: std.mem.Allocator,
    path: db_mod.types.GraphPath,
    exclude: *std.StringHashMapUnmanaged(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    for (path.nodes) |node| {
        if (exclude.contains(node)) return true;
    }
    if (exclude_edge_set.count() == 0) return false;
    for (path.edges) |edge| {
        const edge_key = try allocEdgeExclusionKey(alloc, edge.source, edge.target, edge.edge_type);
        defer alloc.free(edge_key);
        if (exclude_edge_set.contains(edge_key)) return true;
    }
    return false;
}

fn graphPatternMatchIsExcluded(
    alloc: std.mem.Allocator,
    match: db_mod.types.GraphPatternMatch,
    exclude: *std.StringHashMapUnmanaged(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    for (match.bindings) |binding| {
        if (exclude.contains(binding.node.key)) return true;
        if (binding.node.path) |path| {
            for (path) |node| {
                if (exclude.contains(node)) return true;
            }
        }
        if (exclude_edge_set.count() > 0) {
            if (binding.node.path_edges) |edges| {
                for (edges) |edge| {
                    const edge_key = try allocEdgeExclusionKey(alloc, edge.source, edge.target, edge.edge_type);
                    defer alloc.free(edge_key);
                    if (exclude_edge_set.contains(edge_key)) return true;
                }
            }
        }
    }
    if (exclude_edge_set.count() == 0) return false;
    for (match.path) |edge| {
        const edge_key = try allocEdgeExclusionKey(alloc, edge.source, edge.target, edge.edge_type);
        defer alloc.free(edge_key);
        if (exclude_edge_set.contains(edge_key)) return true;
    }
    return false;
}

fn cloneGraphPaths(
    alloc: std.mem.Allocator,
    paths: []const db_mod.types.GraphPath,
) ![]db_mod.types.GraphPath {
    const out = try alloc.alloc(db_mod.types.GraphPath, paths.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |path| graph_paths_mod.freePath(alloc, path);
        alloc.free(out);
    }
    for (paths, 0..) |path, i| {
        out[i] = try cloneGraphPath(alloc, path);
        initialized += 1;
    }
    return out;
}

fn cloneGraphPatternMatches(
    alloc: std.mem.Allocator,
    matches: []const db_mod.types.GraphPatternMatch,
) ![]db_mod.types.GraphPatternMatch {
    const out = try alloc.alloc(db_mod.types.GraphPatternMatch, matches.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*match| match.deinit(alloc);
        alloc.free(out);
    }
    for (matches, 0..) |match, i| {
        out[i] = try cloneGraphPatternMatch(alloc, match);
        initialized += 1;
    }
    return out;
}

fn cloneGraphPatternMatch(
    alloc: std.mem.Allocator,
    match: db_mod.types.GraphPatternMatch,
) !db_mod.types.GraphPatternMatch {
    const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, match.bindings.len);
    var bindings_initialized: usize = 0;
    errdefer {
        for (bindings[0..bindings_initialized]) |*binding| binding.deinit(alloc);
        alloc.free(bindings);
    }
    for (match.bindings, 0..) |binding, i| {
        bindings[i] = .{
            .alias = try alloc.dupe(u8, binding.alias),
            .node = try cloneGraphNode(alloc, binding.node),
        };
        bindings_initialized += 1;
    }

    const path = try dupPathEdges(alloc, match.path);
    errdefer freePathEdges(alloc, path);

    return .{
        .bindings = bindings,
        .path = path,
    };
}

fn cloneSearchHits(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
) ![]db_mod.types.SearchHit {
    const out = try alloc.alloc(db_mod.types.SearchHit, hits.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(out);
    }
    for (hits, 0..) |hit, i| {
        out[i] = try hit.clone(alloc);
        initialized += 1;
    }
    return out;
}

fn cloneGraphNode(
    alloc: std.mem.Allocator,
    node: graph_query_mod.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    return .{
        .key = try alloc.dupe(u8, node.key),
        .depth = node.depth,
        .distance = node.distance,
        .path = if (node.path) |path| try dupPath(alloc, path) else null,
        .path_edges = if (node.path_edges) |edges| try dupPathEdges(alloc, edges) else null,
        .provenance = if (node.provenance) |items| try dupPath(alloc, items) else null,
        .table = if (node.table) |t| try alloc.dupe(u8, t) else null,
    };
}

fn dupPath(alloc: std.mem.Allocator, path: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, path.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (path, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn dupPathEdges(alloc: std.mem.Allocator, edges: []const graph_query_mod.PathEdgeInfo) ![]graph_query_mod.PathEdgeInfo {
    const out = try alloc.alloc(graph_query_mod.PathEdgeInfo, edges.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |edge| freeOwnedPathEdge(alloc, edge);
        alloc.free(out);
    }
    for (edges, 0..) |edge, i| {
        out[i] = try clonePathEdge(alloc, edge);
        initialized += 1;
    }
    return out;
}

fn clonePathEdge(
    alloc: std.mem.Allocator,
    edge: graph_query_mod.PathEdgeInfo,
) !graph_query_mod.PathEdgeInfo {
    return .{
        .source = try alloc.dupe(u8, edge.source),
        .target = try alloc.dupe(u8, edge.target),
        .edge_type = try alloc.dupe(u8, edge.edge_type),
        .weight = edge.weight,
    };
}

fn freeOwnedPathEdge(alloc: std.mem.Allocator, edge: graph_query_mod.PathEdgeInfo) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
}

fn findHit(hits: []const db_mod.types.SearchHit, key: []const u8) ?db_mod.types.SearchHit {
    for (hits) |hit| {
        if (std.mem.eql(u8, hit.id, key)) return hit;
    }
    return null;
}

test "distributed graph expand request preserves algebraic semiring planning flag" {
    const alloc = std.testing.allocator;
    var frontier = [_]FrontierState{.{
        .key = try alloc.dupe(u8, "doc:a"),
    }};
    defer frontier[0].deinit(alloc);
    const frontier_ids = [_]u32{0};
    var req = try makeGraphExpandRequest(alloc, .{
        .name = "walk",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{
                .edge_types = &.{"links"},
                .max_depth = 3,
                .max_results = 0,
                .min_weight = 1.25,
                .max_weight = 9.5,
                .algebraic_semiring = true,
            },
        },
    }, frontier[0..], frontier_ids[0..], &.{}, &.{}, false);
    defer req.deinit(alloc);
    req.identity_read_generation = 12345;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 3, 5 }) };
    defer filter.deinit(alloc);
    req.resolved_doc_filter = &filter;
    req.resolved_doc_filter_wire_context = .{
        .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .identity_read_generation = 12345,
    };
    try std.testing.expect(req.params.algebraic_semiring);
    try std.testing.expectEqual(@as(f64, 1.25), req.params.min_weight);
    try std.testing.expectEqual(@as(f64, 9.5), req.params.max_weight);
    try std.testing.expect(req.tensor_access_path != null);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.graph_edges, req.tensor_access_path.?.layout);
    try std.testing.expectEqualStrings("graph_idx", req.tensor_access_path.?.owner);
    try std.testing.expectEqual(@as(usize, 1), req.tensor_access_path.?.law_ids.len);
    try std.testing.expectEqual(algebraic_law.Id.provenance_semiring, req.tensor_access_path.?.law_ids[0]);
    var expected_program = try graphTraversalTensorProgramEnvelopeAlloc(alloc, "graph_idx", false);
    defer expected_program.deinit(alloc);
    const expected_program_id = expected_program.program_id;
    try std.testing.expect(req.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, req.tensor_program.?.program_id);

    const encoded = try encodeGraphExpandRequest(alloc, req);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"_resolved_doc_filter\"") != null);
    const tampered_expand = try alloc.dupe(u8, encoded);
    defer alloc.free(tampered_expand);
    const expand_program_id_pos = std.mem.indexOf(u8, tampered_expand, expected_program_id) orelse return error.TestUnexpectedResult;
    tampered_expand[expand_program_id_pos] = if (tampered_expand[expand_program_id_pos] == '0') '1' else '0';
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphExpandRequest(alloc, tampered_expand));
    var parsed = try parseGraphExpandRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 12345), parsed.identity_read_generation);
    try std.testing.expect(parsed.resolved_doc_filter != null);
    try std.testing.expect(parsed.resolved_doc_filter_owned);
    try std.testing.expect(parsed.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
    try std.testing.expect(parsed.params.algebraic_semiring);
    try std.testing.expectEqual(@as(f64, 1.25), parsed.params.min_weight);
    try std.testing.expectEqual(@as(f64, 9.5), parsed.params.max_weight);
    try std.testing.expect(parsed.tensor_access_path != null);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.graph_edges, parsed.tensor_access_path.?.layout);
    try std.testing.expectEqualStrings("graph_idx", parsed.tensor_access_path.?.owner);
    try std.testing.expectEqual(@as(usize, 2), parsed.tensor_access_path.?.fragments.len);
    try std.testing.expectEqual(algebraic_ir.TensorFragment.graph_traverse, parsed.tensor_access_path.?.fragments[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_path.?.output_dims.len);
    try std.testing.expectEqual(algebraic_ir.Dimension.doc, parsed.tensor_access_path.?.output_dims[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_path.?.law_ids.len);
    try std.testing.expectEqual(algebraic_law.Id.provenance_semiring, parsed.tensor_access_path.?.law_ids[0]);
    try std.testing.expect(parsed.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, parsed.tensor_program.?.program_id);
    try std.testing.expectEqual(@as(u32, 1), parsed.params.max_depth);
    try std.testing.expectEqual(@as(usize, 1), parsed.params.edge_types.len);
    try std.testing.expectEqualStrings("links", parsed.params.edge_types[0]);
    const search_req = try frontierItemToSearchRequest(alloc, parsed, parsed.frontier[0]);
    defer freeExpandSearchRequest(alloc, search_req);
    try std.testing.expectEqual(@as(?u64, 12345), search_req.identity_read_generation);
    try std.testing.expect(search_req.resolved_doc_filter != null);
    try std.testing.expect(search_req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
    try std.testing.expectEqual(@as(f64, 1.25), search_req.graph_queries[0].query.params.min_weight);
    try std.testing.expectEqual(@as(f64, 9.5), search_req.graph_queries[0].query.params.max_weight);
    try std.testing.expect(search_req.graph_queries[0].query.params.algebraic_semiring);

    const generation_field = "\"identity_read_generation\":12345,";
    const expand_generation_pos = std.mem.indexOf(u8, encoded, generation_field) orelse return error.TestUnexpectedResult;
    const expand_without_top_generation = try alloc.alloc(u8, encoded.len - generation_field.len);
    defer alloc.free(expand_without_top_generation);
    @memcpy(expand_without_top_generation[0..expand_generation_pos], encoded[0..expand_generation_pos]);
    @memcpy(expand_without_top_generation[expand_generation_pos..], encoded[expand_generation_pos + generation_field.len ..]);
    var parsed_expand_from_envelope = try parseGraphExpandRequest(alloc, expand_without_top_generation);
    defer parsed_expand_from_envelope.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 12345), parsed_expand_from_envelope.identity_read_generation);

    const mismatch_pos = std.mem.indexOf(u8, encoded, "\"identity_read_generation\":12345") orelse return error.TestUnexpectedResult;
    const mismatched_expand = try alloc.dupe(u8, encoded);
    defer alloc.free(mismatched_expand);
    mismatched_expand[mismatch_pos + "\"identity_read_generation\":1234".len] = '6';
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphExpandRequest(alloc, mismatched_expand));

    var hydrate_req = GraphHydrateRequest{
        .keys = try dupKeys(alloc, &.{"doc:b"}),
        .topology_epoch = 11,
        .identity_read_generation = 12345,
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
    };
    defer hydrate_req.deinit(alloc);
    const hydrate_encoded = try encodeGraphHydrateRequest(alloc, hydrate_req);
    defer alloc.free(hydrate_encoded);
    try std.testing.expect(std.mem.indexOf(u8, hydrate_encoded, "\"_resolved_doc_filter\"") != null);
    var parsed_hydrate = try parseGraphHydrateRequest(alloc, hydrate_encoded);
    defer parsed_hydrate.deinit(alloc);
    try std.testing.expect(parsed_hydrate.resolved_doc_filter != null);
    try std.testing.expect(parsed_hydrate.resolved_doc_filter_owned);
    try std.testing.expect(parsed_hydrate.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
    try std.testing.expectEqual(@as(?u64, 12345), parsed_hydrate.identity_read_generation);

    const hydrate_generation_pos = std.mem.indexOf(u8, hydrate_encoded, generation_field) orelse return error.TestUnexpectedResult;
    const hydrate_without_top_generation = try alloc.alloc(u8, hydrate_encoded.len - generation_field.len);
    defer alloc.free(hydrate_without_top_generation);
    @memcpy(hydrate_without_top_generation[0..hydrate_generation_pos], hydrate_encoded[0..hydrate_generation_pos]);
    @memcpy(hydrate_without_top_generation[hydrate_generation_pos..], hydrate_encoded[hydrate_generation_pos + generation_field.len ..]);
    var parsed_hydrate_from_envelope = try parseGraphHydrateRequest(alloc, hydrate_without_top_generation);
    defer parsed_hydrate_from_envelope.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 12345), parsed_hydrate_from_envelope.identity_read_generation);

    const mismatched_hydrate_pos = std.mem.indexOf(u8, hydrate_encoded, "\"identity_read_generation\":12345") orelse return error.TestUnexpectedResult;
    const mismatched_hydrate = try alloc.dupe(u8, hydrate_encoded);
    defer alloc.free(mismatched_hydrate);
    mismatched_hydrate[mismatched_hydrate_pos + "\"identity_read_generation\":1234".len] = '6';
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphHydrateRequest(alloc, mismatched_hydrate));

    parsed.tensor_access_path.?.law_ids[0] = .count;
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphExpandTensorAccessPath(alloc, parsed));
    try std.testing.expectError(error.InvalidQueryRequest, encodeGraphExpandRequest(alloc, parsed));
    parsed.tensor_access_path.?.law_ids[0] = .provenance_semiring;
    parsed.tensor_program.?.program_id[parsed.tensor_program.?.program_id.len - 1] = if (parsed.tensor_program.?.program_id[parsed.tensor_program.?.program_id.len - 1] == '0') '1' else '0';
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphExpandTensorAccessPath(alloc, parsed));
    try std.testing.expectError(error.InvalidQueryRequest, encodeGraphExpandRequest(alloc, parsed));
}

test "distributed graph expand request can select semiring from graph index config" {
    const alloc = std.testing.allocator;
    var frontier = [_]FrontierState{.{
        .key = try alloc.dupe(u8, "doc:a"),
    }};
    defer frontier[0].deinit(alloc);
    const frontier_ids = [_]u32{0};

    var req = try makeGraphExpandRequestWithAlgebraicMode(alloc, .{
        .name = "walk",
        .query = .{
            .query_type = .traverse,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .params = .{
                .edge_types = &.{"links"},
                .max_depth = 3,
                .max_results = 0,
            },
        },
    }, frontier[0..], frontier_ids[0..], &.{}, &.{}, false, true);
    defer req.deinit(alloc);
    try std.testing.expect(req.params.algebraic_semiring);
    try std.testing.expect(req.tensor_access_path != null);
    try std.testing.expect(req.tensor_program != null);
    try std.testing.expectEqualStrings("graph_idx", req.tensor_access_path.?.owner);
    try validateGraphExpandTensorAccessPath(alloc, req);
}

test "distributed graph expand request carries constrained semiring target program without pruning one-hop expansion" {
    const alloc = std.testing.allocator;
    var frontier = [_]FrontierState{.{
        .key = try alloc.dupe(u8, "doc:a"),
    }};
    defer frontier[0].deinit(alloc);
    const frontier_ids = [_]u32{0};

    var req = try makeGraphExpandRequestWithAlgebraicModeAndTargetConstraints(alloc, .{
        .name = "shortest",
        .query = .{
            .query_type = .shortest_path,
            .index_name = "graph_idx",
            .start_nodes = .{ .keys = &.{"doc:a"} },
            .target_nodes = .{ .keys = &.{"doc:z"} },
            .params = .{
                .algebraic_semiring = true,
            },
        },
    }, frontier[0..], frontier_ids[0..], &.{}, &.{}, true, true, &.{ "doc:z", "doc:m", "doc:z" });
    defer req.deinit(alloc);
    try std.testing.expect(req.params.algebraic_semiring);
    try std.testing.expectEqual(@as(usize, 2), req.target_constraint_keys.len);
    try std.testing.expectEqualStrings("doc:m", req.target_constraint_keys[0]);
    try std.testing.expectEqualStrings("doc:z", req.target_constraint_keys[1]);
    var expected_program = try graphTraversalTensorProgramEnvelopeAlloc(alloc, "graph_idx", true);
    defer expected_program.deinit(alloc);
    const expected_program_id = expected_program.program_id;
    try std.testing.expect(req.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, req.tensor_program.?.program_id);
    try validateGraphExpandTensorAccessPath(alloc, req);

    const encoded = try encodeGraphExpandRequest(alloc, req);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"target_constraint_keys\":[\"doc:m\",\"doc:z\"]") != null);
    var parsed = try parseGraphExpandRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), parsed.target_constraint_keys.len);
    try std.testing.expectEqualStrings("doc:m", parsed.target_constraint_keys[0]);
    try std.testing.expectEqualStrings("doc:z", parsed.target_constraint_keys[1]);
    try std.testing.expect(parsed.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, parsed.tensor_program.?.program_id);
    const search_req = try frontierItemToSearchRequest(alloc, parsed, parsed.frontier[0]);
    defer freeExpandSearchRequest(alloc, search_req);
    try std.testing.expect(search_req.graph_queries[0].query.target_nodes == null);

    parsed.params.algebraic_semiring = false;
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphExpandTensorAccessPath(alloc, parsed));
}

test "distributed graph detects semiring-enabled graph index config" {
    const alloc = std.testing.allocator;
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{
                .table_id = 7,
                .name = "docs",
                .indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}},\"plain_graph\":{\"type\":\"graph\"},\"disabled_graph\":{\"type\":\"graph\",\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\",\"enabled\":false}}}}",
            },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const catalog = FakeCatalog.iface();
    try std.testing.expect(try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, "docs", "graph_idx"));
    try std.testing.expect(!try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, "docs", "plain_graph"));
    try std.testing.expect(!try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, "docs", "disabled_graph"));
    try std.testing.expect(!try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, "docs", "missing_graph"));
}

test "distributed graph expand request rejects semiring flag without typed access path" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "name": "walk",
        \\  "index_name": "graph_idx",
        \\  "frontier": [{"id": 0, "key": "doc:a"}],
        \\  "params": {"algebraic_semiring": true}
        \\}
    ;
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphExpandRequest(alloc, body));
}

test "distributed graph edges request preserves typed graph edge access path" {
    const alloc = std.testing.allocator;
    var expected_program = try graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_idx");
    defer expected_program.deinit(alloc);
    const expected_program_id = expected_program.program_id;

    var req = GraphEdgesRequest{
        .index_name = try alloc.dupe(u8, "graph_idx"),
        .key = try alloc.dupe(u8, "doc:a"),
        .direction = .both,
        .topology_epoch = 42,
        .identity_read_generation = 12345,
        .tensor_access_path = try cloneGraphTensorAccessPathAlloc(alloc, algebraic_ir.graphEdgeAccessPath("graph_idx")),
        .tensor_program = try graphEdgesTensorProgramEnvelopeAlloc(alloc, "graph_idx"),
    };
    defer req.deinit(alloc);
    try validateGraphEdgesTensorAccessPath(alloc, req);
    try std.testing.expect(req.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, req.tensor_program.?.program_id);

    const encoded = try encodeGraphEdgesRequest(alloc, req);
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"tensor_program\"") != null);
    const tampered_edges = try alloc.dupe(u8, encoded);
    defer alloc.free(tampered_edges);
    const edges_program_id_pos = std.mem.indexOf(u8, tampered_edges, expected_program_id) orelse return error.TestUnexpectedResult;
    tampered_edges[edges_program_id_pos] = if (tampered_edges[edges_program_id_pos] == '0') '1' else '0';
    try std.testing.expectError(error.InvalidQueryRequest, parseGraphEdgesRequest(alloc, tampered_edges));
    var parsed = try parseGraphEdgesRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(graph_mod.EdgeDirection.both, parsed.direction);
    try std.testing.expectEqual(@as(u64, 42), parsed.topology_epoch);
    try std.testing.expectEqual(@as(?u64, 12345), parsed.identity_read_generation);
    try std.testing.expect(parsed.tensor_access_path != null);
    try std.testing.expect(parsed.tensor_program != null);
    try std.testing.expectEqualStrings(expected_program_id, parsed.tensor_program.?.program_id);
    try std.testing.expectEqual(algebraic_ir.PhysicalLayout.graph_edges, parsed.tensor_access_path.?.layout);
    try std.testing.expectEqualStrings("graph_idx", parsed.tensor_access_path.?.owner);
    try std.testing.expectEqual(@as(usize, 2), parsed.tensor_access_path.?.output_dims.len);
    try std.testing.expectEqual(algebraic_ir.Dimension.src, parsed.tensor_access_path.?.output_dims[0]);
    try std.testing.expectEqual(algebraic_ir.Dimension.dst, parsed.tensor_access_path.?.output_dims[1]);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensor_access_path.?.law_ids.len);
    try std.testing.expectEqual(algebraic_law.Id.provenance_semiring, parsed.tensor_access_path.?.law_ids[0]);

    parsed.tensor_access_path.?.output_dims[0] = .doc;
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphEdgesTensorAccessPath(alloc, parsed));
    try std.testing.expectError(error.InvalidQueryRequest, encodeGraphEdgesRequest(alloc, parsed));
    parsed.tensor_access_path.?.output_dims[0] = .src;

    parsed.tensor_program.?.program_id[parsed.tensor_program.?.program_id.len - 1] = if (parsed.tensor_program.?.program_id[parsed.tensor_program.?.program_id.len - 1] == '0') '1' else '0';
    try std.testing.expectError(error.InvalidQueryRequest, validateGraphEdgesTensorAccessPath(alloc, parsed));
    try std.testing.expectError(error.InvalidQueryRequest, encodeGraphEdgesRequest(alloc, parsed));
}

test "distributed graph edge reader carries identity generation" {
    const alloc = std.testing.allocator;

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .schema_json = "",
            .read_schema_json = "",
            .indexes_json = tables_api.default_indexes_json,
            .replication_sources_json = "[]",
            .placement_role = "data",
        }};
        const ranges = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 11,
            .table_id = 7,
            .range_id = 11,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const TestState = struct {
        calls: u32 = 0,
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                    .execute_graph_get_edges = executeGraphGetEdges,
                },
            };
        }

        fn executeGraphExpand(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            return error.UnsupportedQueryRequest;
        }

        fn executeGraphHydrate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            return error.UnsupportedQueryRequest;
        }

        fn executeGraphGetEdges(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphEdgesRequest,
            consistency: raft_mod.ReadConsistency,
        ) !GraphEdgesResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.calls += 1;
            try std.testing.expectEqual(@as(u64, 11), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqualStrings("graph_idx", req.index_name);
            try std.testing.expectEqualStrings("doc:a", req.key);
            try std.testing.expectEqual(graph_mod.EdgeDirection.out, req.direction);
            try std.testing.expectEqual(@as(u64, 0), req.topology_epoch);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            return .{ .edges = @constCast((&[_]graph_mod.Edge{})[0..]) };
        }
    };

    var state = TestState{};
    const reader = DistributedEdgeReader{
        .catalog = FakeCatalog.iface(),
        .worker = FakeWorker.iface(&state),
        .table_name = "docs",
        .index_name = "graph_idx",
        .topology_epoch = 0,
        .identity_read_generation = 12345,
        .consistency = .read_index,
    };

    const edges = try reader.getEdges(alloc, "doc:a", .out);
    defer reader.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);
    try std.testing.expectEqual(@as(u32, 1), state.calls);
}

test "distributed graph edges response round trips owned edges" {
    const alloc = std.testing.allocator;
    var edges = [_]graph_mod.Edge{.{
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "links",
        .weight = 2.5,
        .created_at = 11,
        .updated_at = 12,
        .metadata = "{\"p\":1}",
    }};
    const encoded = try encodeGraphEdgesResponse(alloc, .{ .edges = edges[0..] });
    defer alloc.free(encoded);

    var parsed = try parseGraphEdgesResponse(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.edges.len);
    try std.testing.expectEqualStrings("doc:a", parsed.edges[0].source);
    try std.testing.expectEqualStrings("doc:b", parsed.edges[0].target);
    try std.testing.expectEqualStrings("links", parsed.edges[0].edge_type);
    try std.testing.expectEqual(@as(f64, 2.5), parsed.edges[0].weight);
    try std.testing.expectEqualStrings("{\"p\":1}", parsed.edges[0].metadata);
}

test "distributed graph result cloning preserves paths matches and provenance" {
    const alloc = std.testing.allocator;
    var node_path = [_][]const u8{ "doc:a", "doc:b" };
    var provenance = [_][]const u8{"edge:a:b"};
    var path_edges = [_]graph_query_mod.PathEdgeInfo{.{
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "links",
        .weight = 1.5,
    }};
    var result_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "doc:b",
        .depth = 1,
        .distance = 1.5,
        .path = node_path[0..],
        .path_edges = path_edges[0..],
        .provenance = provenance[0..],
    }};
    var graph_path_nodes = [_][]const u8{ "doc:a", "doc:b" };
    var graph_path_edges = [_]graph_paths_mod.PathEdge{.{
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "links",
        .weight = 1.5,
    }};
    var paths = [_]db_mod.types.GraphPath{.{
        .nodes = graph_path_nodes[0..],
        .edges = graph_path_edges[0..],
        .total_weight = 1.5,
        .length = 1,
    }};
    var bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("target"),
        .node = .{
            .key = "doc:b",
            .depth = 1,
            .distance = 1.5,
            .path = node_path[0..],
            .path_edges = path_edges[0..],
            .provenance = provenance[0..],
        },
    }};
    var match_path = [_]graph_query_mod.PathEdgeInfo{path_edges[0]};
    var matches = [_]db_mod.types.GraphPatternMatch{.{
        .bindings = bindings[0..],
        .path = match_path[0..],
    }};
    const src = db_mod.types.GraphSearchResult{
        .name = @constCast("graph"),
        .nodes = result_nodes[0..],
        .paths = paths[0..],
        .matches = matches[0..],
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    };

    var cloned = try cloneGraphSearchResult(alloc, src);
    defer cloned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), cloned.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), cloned.paths.len);
    try std.testing.expectEqual(@as(usize, 1), cloned.matches.len);
    try std.testing.expectEqualStrings("doc:b", cloned.nodes[0].key);
    try std.testing.expectEqualStrings("edge:a:b", cloned.nodes[0].provenance.?[0]);
    try std.testing.expectEqualStrings("doc:a", cloned.paths[0].nodes[0]);
    try std.testing.expectEqualStrings("links", cloned.paths[0].edges[0].edge_type);
    try std.testing.expectEqualStrings("target", cloned.matches[0].bindings[0].alias);
    try std.testing.expectEqualStrings("doc:b", cloned.matches[0].bindings[0].node.key);
    try std.testing.expectEqualStrings("links", cloned.matches[0].path[0].edge_type);
}

test "distributed graph filtering preserves non-excluded paths matches and provenance" {
    const alloc = std.testing.allocator;
    var node_path = [_][]const u8{ "doc:a", "doc:b" };
    var provenance = [_][]const u8{"edge:a:b"};
    var path_edges = [_]graph_query_mod.PathEdgeInfo{.{
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "links",
        .weight = 1.5,
    }};
    var result_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "doc:b",
        .depth = 1,
        .distance = 1.5,
        .path = node_path[0..],
        .path_edges = path_edges[0..],
        .provenance = provenance[0..],
    }};
    var graph_path_nodes = [_][]const u8{ "doc:a", "doc:b" };
    var graph_path_edges = [_]graph_paths_mod.PathEdge{.{
        .source = "doc:a",
        .target = "doc:b",
        .edge_type = "links",
        .weight = 1.5,
    }};
    var paths = [_]db_mod.types.GraphPath{.{
        .nodes = graph_path_nodes[0..],
        .edges = graph_path_edges[0..],
        .total_weight = 1.5,
        .length = 1,
    }};
    var bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("target"),
        .node = .{
            .key = "doc:b",
            .depth = 1,
            .distance = 1.5,
            .path = node_path[0..],
            .path_edges = path_edges[0..],
            .provenance = provenance[0..],
        },
    }};
    var match_path = [_]graph_query_mod.PathEdgeInfo{path_edges[0]};
    var matches = [_]db_mod.types.GraphPatternMatch{.{
        .bindings = bindings[0..],
        .path = match_path[0..],
    }};
    const src = db_mod.types.GraphSearchResult{
        .name = @constCast("graph"),
        .nodes = result_nodes[0..],
        .paths = paths[0..],
        .matches = matches[0..],
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    };

    var kept = try filterGraphSearchResult(alloc, src, &.{"doc:z"}, &.{});
    defer kept.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), kept.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), kept.paths.len);
    try std.testing.expectEqual(@as(usize, 1), kept.matches.len);
    try std.testing.expectEqualStrings("edge:a:b", kept.nodes[0].provenance.?[0]);
    try std.testing.expectEqualStrings("doc:a", kept.paths[0].nodes[0]);
    try std.testing.expectEqualStrings("target", kept.matches[0].bindings[0].alias);

    var dropped_node = try filterGraphSearchResult(alloc, src, &.{"doc:b"}, &.{});
    defer dropped_node.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.paths.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.matches.len);

    var dropped_edge = try filterGraphSearchResult(alloc, src, &.{}, &.{"doc:a->doc:b:links"});
    defer dropped_edge.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.paths.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.matches.len);
}

test "distributed graph result_ref fails closed for unbounded paged base results" {
    const alloc = std.testing.allocator;

    const hit = db_mod.types.SearchHit{ .id = @constCast("doc:a") };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
        .total_hits = 2,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };
    const req = db_mod.types.SearchRequest{ .limit = 10, .identity_read_generation = 9 };

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        req,
        base_result,
        &.{},
        .{ .ref = "$full_text_results", .limit = 0 },
    ));

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        .{ .limit = 10 },
        base_result,
        &.{},
        .{ .ref = "$full_text_results", .limit = 1 },
    ));

    const limited = try resolveResultRefKeys(
        alloc,
        req,
        base_result,
        &.{},
        .{ .ref = "$full_text_results", .limit = 1 },
    );
    defer freeKeys(alloc, limited);
    try std.testing.expectEqual(@as(usize, 1), limited.len);
    try std.testing.expectEqualStrings("doc:a", limited[0]);
}

test "distributed graph result_ref fails closed for saturated base result page" {
    const alloc = std.testing.allocator;

    const hit = db_mod.types.SearchHit{ .id = @constCast("doc:a") };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{hit})[0..]),
        .total_hits = 1,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };
    const req = db_mod.types.SearchRequest{ .limit = 1, .identity_read_generation = 9 };

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        req,
        base_result,
        &.{},
        .{ .ref = "$fused_results", .limit = 0 },
    ));
}

test "distributed graph result_ref fails closed for unbounded paged graph results" {
    const alloc = std.testing.allocator;

    const node = graph_query_mod.GraphResultNode{ .key = "doc:b", .depth = 0, .distance = 0, .path = null, .path_edges = null };
    const graph_result = db_mod.types.GraphSearchResult{
        .name = @constCast("first_hop"),
        .nodes = @constCast((&[_]graph_query_mod.GraphResultNode{node})[0..]),
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 2,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        .{ .identity_read_generation = 9 },
        base_result,
        &.{graph_result},
        .{ .ref = "$graph_results.first_hop", .limit = 0 },
    ));

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        .{},
        base_result,
        &.{graph_result},
        .{ .ref = "$graph_results.first_hop", .limit = 1 },
    ));

    const limited = try resolveResultRefKeys(
        alloc,
        .{ .identity_read_generation = 9 },
        base_result,
        &.{graph_result},
        .{ .ref = "$graph_results.first_hop", .limit = 1 },
    );
    defer freeKeys(alloc, limited);
    try std.testing.expectEqual(@as(usize, 1), limited.len);
    try std.testing.expectEqualStrings("doc:b", limited[0]);
}

test "distributed graph rejects unstamped result refs before cross-range fanout" {
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "seed",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .result_ref = .{ .ref = "$full_text_results", .limit = 1 } },
                    .params = .{},
                },
            },
        },
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, rejectUnstampedResultRefs(req));

    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };
    try std.testing.expect(supportsCrossRange(req));

    const DummyWorker = struct {
        fn expand(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            return error.TestUnexpectedResult;
        }

        fn hydrate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            return error.TestUnexpectedResult;
        }

        fn iface() Worker {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute_graph_expand = expand,
                    .execute_graph_hydrate = hydrate,
                },
            };
        }
    };

    try std.testing.expectError(error.UnsupportedQueryRequest, executeCrossRange(
        std.testing.allocator,
        table_catalog.emptyCatalogSource(),
        DummyWorker.iface(),
        "docs",
        req,
        base_result,
        .read_index,
    ));

    var stamped = req;
    stamped.identity_read_generation = 9;
    try rejectUnstampedResultRefs(stamped);

    const explicit_keys = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "seed",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .params = .{},
                },
            },
        },
    };
    try rejectUnstampedResultRefs(explicit_keys);
    try std.testing.expectError(error.UnsupportedQueryRequest, executeCrossRange(
        std.testing.allocator,
        table_catalog.emptyCatalogSource(),
        DummyWorker.iface(),
        "docs",
        explicit_keys,
        base_result,
        .read_index,
    ));
}

test "distributed graph supports cross-range traverse target selectors" {
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .target_nodes = .{ .keys = &[_][]const u8{"doc:c"} },
                    .params = .{ .max_depth = 2 },
                },
            },
        },
        .identity_read_generation = 9,
    };
    try std.testing.expect(supportsCrossRange(req));

    const unsupported_weight_mode = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .target_nodes = .{ .keys = &[_][]const u8{"doc:c"} },
                    .params = .{
                        .max_depth = 2,
                        .weight_mode = .min_weight,
                    },
                },
            },
        },
        .identity_read_generation = 9,
    };
    try std.testing.expect(!supportsCrossRange(unsupported_weight_mode));
}

test "distributed graph rejects doc identity rebuild before cross-range fanout" {
    const TestState = struct {
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null },
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11, .doc_identity = .{ .rebuild_required = true } },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.expand_calls += 1;
            return .{ .expansions = @constCast((&[_]GraphExpansion{})[0..]) };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.hydrate_calls += 1;
            return .{ .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]) };
        }
    };

    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .params = .{},
                },
            },
        },
        .identity_read_generation = 9,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    var state = TestState{};
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    ));
    try std.testing.expectEqual(@as(u32, 0), state.expand_calls);
    try std.testing.expectEqual(@as(u32, 0), state.hydrate_calls);
}

test "distributed graph traverse target nodes filter returned nodes without pruning frontier" {
    const TestState = struct {
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null },
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11 },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 11), group_id);
            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);
            state.expand_calls += 1;

            const next_key = if (std.mem.eql(u8, req.frontier[0].key, "doc:a"))
                "doc:b"
            else if (std.mem.eql(u8, req.frontier[0].key, "doc:b"))
                "doc:c"
            else
                return .{ .expansions = @constCast((&[_]GraphExpansion{})[0..]) };
            const next_depth: u32 = if (std.mem.eql(u8, next_key, "doc:b")) 1 else 2;

            const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc.dupe(u8, next_key),
                .depth = next_depth,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            const expansions = try alloc.alloc(GraphExpansion, 1);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = try alloc.dupe(u8, req.frontier[0].key),
                .graph_result = .{
                    .name = try alloc.dupe(u8, req.name),
                    .nodes = nodes,
                    .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                    .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 11), group_id);
            try std.testing.expectEqual(@as(usize, 1), req.keys.len);
            try std.testing.expectEqualStrings("doc:c", req.keys[0]);
            state.hydrate_calls += 1;
            const hits = try alloc.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{
                .id = try alloc.dupe(u8, "doc:c"),
                .stored_data = try alloc.dupe(u8, "{\"title\":\"target\"}"),
            };
            return .{ .hits = hits };
        }
    };

    var state = TestState{};
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .target_nodes = .{ .keys = &[_][]const u8{"doc:c"} },
                    .params = .{ .max_depth = 2 },
                },
            },
        },
        .identity_read_generation = 9,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    const results = try executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    );
    defer {
        for (results) |*result| result.deinit(std.testing.allocator);
        std.testing.allocator.free(results);
    }

    try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
    try std.testing.expectEqual(@as(u32, 1), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 1), results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:c", results[0].nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), results[0].nodes[0].depth);
    try std.testing.expectEqual(@as(usize, 1), results[0].hits.len);
    try std.testing.expectEqualStrings("doc:c", results[0].hits[0].id);
}

test "distributed graph retries once on topology change and succeeds" {
    const TestState = struct {
        phase: u32 = 0,
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const phase0_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null },
        };
        const phase1_ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 22, .table_id = 7, .start_key = "", .end_key = null },
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11 },
            .{ .group_id = 22 },
        };

        fn iface(state: *TestState) table_catalog.CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(if (state.phase == 0) phase0_ranges[0..] else phase1_ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);
            state.expand_calls += 1;
            if (state.expand_calls == 1) {
                state.phase = 1;
                return error.TopologyChanged;
            }
            try std.testing.expectEqual(@as(u64, 22), group_id);
            try std.testing.expect(req.topology_epoch != 0);

            const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc.dupe(u8, "doc:b"),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            const expansions = try alloc.alloc(GraphExpansion, 1);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = try alloc.dupe(u8, req.frontier[0].key),
                .graph_result = .{
                    .name = try alloc.dupe(u8, req.name),
                    .nodes = nodes,
                    .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                    .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 22), group_id);
            try std.testing.expect(req.topology_epoch != 0);
            state.hydrate_calls += 1;
            const hits = try alloc.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{
                .id = try alloc.dupe(u8, "doc:b"),
                .stored_data = try alloc.dupe(u8, "{\"title\":\"beta\"}"),
            };
            return .{ .hits = hits };
        }
    };

    var state = TestState{};
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .params = .{},
                },
            },
        },
        .identity_read_generation = 9,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    const results = try executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    );
    defer {
        for (results) |*result| result.deinit(std.testing.allocator);
        std.testing.allocator.free(results);
    }

    try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
    try std.testing.expectEqual(@as(u32, 1), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 1), results[0].nodes.len);
    try std.testing.expectEqualStrings("doc:b", results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), results[0].hits.len);
    try std.testing.expectEqualStrings("doc:b", results[0].hits[0].id);
}

test "distributed graph stops after single retry on repeated topology churn" {
    const TestState = struct {
        phase: u32 = 0,
        expand_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_][1]metadata_table_manager.RangeRecord{
            .{.{ .group_id = 31, .table_id = 7, .start_key = "", .end_key = null }},
            .{.{ .group_id = 32, .table_id = 7, .start_key = "", .end_key = null }},
            .{.{ .group_id = 33, .table_id = 7, .start_key = "", .end_key = null }},
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 31 },
            .{ .group_id = 32 },
            .{ .group_id = 33 },
        };

        fn iface(state: *TestState) table_catalog.CatalogSource {
            return .{
                .ptr = state,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            const range_index: usize = @intCast(@min(state.phase, ranges.len - 1));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[range_index][0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                },
            };
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.expand_calls += 1;
            state.phase += 1;
            return error.TopologyChanged;
        }

        fn executeGraphHydrate(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            return .{ .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]) };
        }
    };

    var state = TestState{};
    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{"doc:a"} },
                    .params = .{},
                },
            },
        },
        .identity_read_generation = 9,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    try std.testing.expectError(error.TopologyChanged, executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(&state),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    ));
    try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
}

test "distributed graph fans out per-group expand and hydrate with worker io" {
    const TestState = struct {
        io_impl: *std.Io.Threaded,
        expand_calls: std.atomic.Value(u32) = .init(0),
        hydrate_calls: std.atomic.Value(u32) = .init(0),
        expand_active: std.atomic.Value(u32) = .init(0),
        hydrate_active: std.atomic.Value(u32) = .init(0),
        max_expand_active: std.atomic.Value(u32) = .init(0),
        max_hydrate_active: std.atomic.Value(u32) = .init(0),

        fn updateMax(max_value: *std.atomic.Value(u32), current: u32) void {
            var observed = max_value.load(.monotonic);
            while (current > observed) {
                observed = max_value.cmpxchgWeak(observed, current, .monotonic, .monotonic) orelse return;
            }
        }
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = "doc:m" },
            .{ .group_id = 22, .table_id = 7, .start_key = "doc:m", .end_key = null },
        };
        const statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{ .group_id = 11 },
            .{ .group_id = 22 },
        };

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast(statuses[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeWorker = struct {
        fn iface(state: *TestState) Worker {
            return .{
                .ptr = state,
                .vtable = &.{
                    .execute_graph_expand = executeGraphExpand,
                    .execute_graph_hydrate = executeGraphHydrate,
                    .fanout_io = fanoutIo,
                },
            };
        }

        fn fanoutIo(ptr: *anyopaque) ?std.Io {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            return state.io_impl.io();
        }

        fn executeGraphExpand(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            _ = state.expand_calls.fetchAdd(1, .monotonic);
            const active = state.expand_active.fetchAdd(1, .monotonic) + 1;
            defer _ = state.expand_active.fetchSub(1, .monotonic);
            TestState.updateMax(&state.max_expand_active, active);
            try std.Io.Clock.Duration.sleep(.{
                .clock = .awake,
                .raw = .fromNanoseconds(10 * std.time.ns_per_ms),
            }, state.io_impl.io());

            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);
            const node_key = if (group_id == 11) "doc:b" else "doc:o";
            const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc.dupe(u8, node_key),
                .depth = 1,
                .distance = 1.0,
                .path = null,
                .path_edges = null,
            };
            const expansions = try alloc.alloc(GraphExpansion, 1);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = try alloc.dupe(u8, req.frontier[0].key),
                .graph_result = .{
                    .name = try alloc.dupe(u8, req.name),
                    .nodes = nodes,
                    .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                    .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            _ = state.hydrate_calls.fetchAdd(1, .monotonic);
            const active = state.hydrate_active.fetchAdd(1, .monotonic) + 1;
            defer _ = state.hydrate_active.fetchSub(1, .monotonic);
            TestState.updateMax(&state.max_hydrate_active, active);
            try std.Io.Clock.Duration.sleep(.{
                .clock = .awake,
                .raw = .fromNanoseconds(10 * std.time.ns_per_ms),
            }, state.io_impl.io());
            try std.testing.expectEqual(@as(?u64, 77), req.identity_read_generation);

            const hits = try alloc.alloc(db_mod.types.SearchHit, req.keys.len);
            var initialized: usize = 0;
            errdefer {
                for (hits[0..initialized]) |*hit| hit.deinit(alloc);
                alloc.free(hits);
            }
            for (req.keys, 0..) |key, i| {
                hits[i] = .{
                    .id = try alloc.dupe(u8, key),
                    .doc_ordinal = if (group_id == 11) 1 else 1,
                    .stored_data = try std.fmt.allocPrint(alloc, "{{\"title\":\"{s}\"}}", .{key}),
                };
                initialized += 1;
            }
            return .{ .hits = hits };
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var state = TestState{ .io_impl = &io_impl };

    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{ "doc:a", "doc:n" } },
                    .params = .{},
                },
            },
        },
        .identity_read_generation = 77,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };

    const results = try executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    );
    defer {
        for (results) |*result| result.deinit(std.testing.allocator);
        std.testing.allocator.free(results);
    }

    try std.testing.expectEqual(@as(u32, 2), state.expand_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 2), state.hydrate_calls.load(.monotonic));
    try std.testing.expect(state.max_expand_active.load(.monotonic) >= 2);
    try std.testing.expect(state.max_hydrate_active.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 2), results[0].nodes.len);
    try std.testing.expectEqual(@as(usize, 2), results[0].hits.len);
    for (results[0].hits) |hit| {
        try std.testing.expect(hit.doc_ordinal == null);
    }
}
