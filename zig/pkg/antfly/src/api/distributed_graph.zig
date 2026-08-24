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
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const raft_mod = struct {
    pub const ReadConsistency = @import("../raft/read_gate.zig").ReadConsistency;
};
const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
    pub const doc_filter_wire = @import("../storage/db/doc_filter_wire.zig");
    pub const algebraic = @import("../storage/db/algebraic/mod.zig");
};
const graph_query_mod = @import("../graph/query.zig");
const graph_mod = @import("../graph/graph.zig");
const graph_node_admission = @import("../graph/node_admission.zig");
const graph_node_identity = @import("../graph/node_identity.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_paths_mod = @import("../graph/paths.zig");
const graph_traversal_mod = @import("../graph/traversal.zig");
const doc_set = @import("../storage/db/doc_set.zig");
const graph_exec = @import("../storage/db/query/graph_exec.zig");
const algebraic_ir = db_mod.algebraic.ir;
const algebraic_law = db_mod.algebraic.law;
const algebraic_planner = db_mod.algebraic.planner;
const table_catalog = @import("table_catalog.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const platform_time = @import("antfly_platform").time;
const indexes_api = @import("indexes.zig");
const query_contract = @import("query_contract.zig");
const tables_api = @import("tables.zig");

pub const Worker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    execution_deadline_ns: ?u64 = null,
    cancellation: ?CancellationToken = null,

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
        try self.ensureActive();
        var controlled = req;
        controlled.timeout_ms = try self.remainingTimeoutMs();
        controlled.cancellation = self.cancellation;
        var response = try self.vtable.execute_graph_expand(self.ptr, alloc, group_id, table_name, controlled, consistency);
        errdefer response.deinit(alloc);
        try self.ensureActive();
        return response;
    }

    pub fn executeGraphHydrate(
        self: Worker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !GraphHydrateResponse {
        try self.ensureActive();
        var controlled = req;
        controlled.timeout_ms = try self.remainingTimeoutMs();
        controlled.cancellation = self.cancellation;
        var response = try self.vtable.execute_graph_hydrate(self.ptr, alloc, group_id, table_name, controlled, consistency);
        errdefer response.deinit(alloc);
        try self.ensureActive();
        return response;
    }

    pub fn executeGraphGetEdges(
        self: Worker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !GraphEdgesResponse {
        try self.ensureActive();
        const func = self.vtable.execute_graph_get_edges orelse return error.UnsupportedQueryRequest;
        var controlled = req;
        controlled.timeout_ms = try self.remainingTimeoutMs();
        controlled.cancellation = self.cancellation;
        var response = try func(self.ptr, alloc, group_id, table_name, controlled, consistency);
        errdefer response.deinit(alloc);
        try self.ensureActive();
        return response;
    }

    pub fn fanoutIo(self: Worker) ?std.Io {
        const func = self.vtable.fanout_io orelse return null;
        return func(self.ptr);
    }

    pub fn fanoutWidthCap(self: Worker) ?usize {
        const func = self.vtable.fanout_width_cap orelse return null;
        return func(self.ptr);
    }

    fn ensureActive(self: Worker) !void {
        if (self.cancellation) |value| {
            if (value.isCancelled()) return error.Cancelled;
        }
        if (self.execution_deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
    }

    fn remainingTimeoutMs(self: Worker) !?u32 {
        const deadline_ns = self.execution_deadline_ns orelse return null;
        const now_ns = platform_time.monotonicNs();
        if (now_ns >= deadline_ns) return error.Timeout;
        const remaining_ns = deadline_ns - now_ns;
        const rounded_ms = @max(@as(u64, 1), std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1);
        return @intCast(@min(rounded_ms, @as(u64, std.math.maxInt(u32))));
    }
};

/// Reconstitutes a node-local absolute deadline from the coordinator's
/// remaining transport budget. Saturating addition keeps maliciously large
/// values from wrapping into an already-expired deadline.
pub fn executionDeadlineFromTimeoutMs(timeout_ms: ?u32) ?u64 {
    const value = timeout_ms orelse return null;
    const duration_ns = std.math.mul(u64, value, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return std.math.add(u64, platform_time.monotonicNs(), duration_ns) catch std.math.maxInt(u64);
}

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
    exclude_nodes: []GraphNodeIdentity,
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
    /// Local request controls. They are deliberately excluded from the graph
    /// JSON contract; remote workers receive the deadline as their transport
    /// timeout and cancellation by connection interruption.
    timeout_ms: ?u32 = null,
    cancellation: ?CancellationToken = null,

    pub fn deinit(self: *GraphExpandRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.index_name);
        for (self.frontier) |*item| item.deinit(alloc);
        if (self.frontier.len > 0) alloc.free(self.frontier);
        for (self.exclude_nodes) |*identity| identity.deinit(alloc);
        if (self.exclude_nodes.len > 0) alloc.free(self.exclude_nodes);
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

pub const GraphNodeIdentity = struct {
    key: []u8,
    table: ?[]u8 = null,

    pub fn ref(self: GraphNodeIdentity) graph_node_identity.Ref {
        return .{ .table = self.table, .key = self.key };
    }

    pub fn deinit(self: *GraphNodeIdentity, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        self.* = undefined;
    }
};

pub const GraphFrontierItem = struct {
    id: u32,
    key: []u8,
    table: ?[]u8 = null,
    depth: u32 = 0,
    distance: f64 = 0,

    pub fn deinit(self: *GraphFrontierItem, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
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
    filter_query_json: []const u8 = "",
    filter_query_json_owned: bool = false,
    exclusion_query_json: []const u8 = "",
    exclusion_query_json_owned: bool = false,
    include_stored: bool = true,
    fields: []const []const u8 = &.{},
    fields_owned: bool = false,
    include_all_fields: bool = true,
    include_hits: bool = true,
    incoming_index_name: []const u8 = "",
    incoming_index_name_owned: bool = false,
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_owned: bool = false,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,
    timeout_ms: ?u32 = null,
    cancellation: ?CancellationToken = null,

    pub fn deinit(self: *GraphHydrateRequest, alloc: std.mem.Allocator) void {
        for (self.keys) |key| alloc.free(key);
        if (self.keys.len > 0) alloc.free(self.keys);
        if (self.fields_owned) freeConstStrings(alloc, self.fields);
        if (self.filter_query_json_owned and self.filter_query_json.len > 0) {
            alloc.free(@constCast(self.filter_query_json));
        }
        if (self.exclusion_query_json_owned and self.exclusion_query_json.len > 0) {
            alloc.free(@constCast(self.exclusion_query_json));
        }
        if (self.incoming_index_name_owned and self.incoming_index_name.len > 0) {
            alloc.free(@constCast(self.incoming_index_name));
        }
        if (self.resolved_doc_filter_owned) {
            if (self.resolved_doc_filter) |ptr| db_mod.doc_filter_wire.destroyResolvedDocFilter(alloc, ptr);
        }
        self.* = undefined;
    }
};

pub const GraphHydrateResponse = struct {
    hits: []db_mod.types.SearchHit = &.{},
    has_incoming: []bool = &.{},

    pub fn deinit(self: *GraphHydrateResponse, alloc: std.mem.Allocator) void {
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
        if (self.has_incoming.len > 0) alloc.free(self.has_incoming);
        self.* = undefined;
    }
};

pub const GraphEdgesRequest = struct {
    index_name: []u8,
    key: []u8,
    edge_types: [][]const u8 = &.{},
    direction: graph_mod.EdgeDirection,
    tensor_access_path: ?OwnedGraphTensorAccessPath = null,
    tensor_program: ?query_contract.OwnedAlgebraicTensorProgramEnvelope = null,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    timeout_ms: ?u32 = null,
    cancellation: ?CancellationToken = null,

    pub fn deinit(self: *GraphEdgesRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.key);
        freeConstStrings(alloc, self.edge_types);
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
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    frontier_ids: []const u32,
};

const GraphExpandBatchKey = struct {
    table_name: []const u8,
    group_id: u64,
};

const GraphExpandBatchKeyContext = struct {
    pub fn hash(_: @This(), key: GraphExpandBatchKey) u64 {
        var hasher = std.hash.Wyhash.init(0x4146_4752_4150_4842);
        const table_len: u64 = @intCast(key.table_name.len);
        hasher.update(std.mem.asBytes(&table_len));
        hasher.update(key.table_name);
        hasher.update(std.mem.asBytes(&key.group_id));
        return hasher.final();
    }

    pub fn eql(_: @This(), left: GraphExpandBatchKey, right: GraphExpandBatchKey) bool {
        return left.group_id == right.group_id and
            std.mem.eql(u8, left.table_name, right.table_name);
    }
};

const GraphExpandBatch = struct {
    topology_epoch: u64,
    identity_read_generation: ?u64,
    frontier_ids: std.ArrayListUnmanaged(u32) = .empty,
};

const GraphExpandBatches = std.HashMapUnmanaged(
    GraphExpandBatchKey,
    GraphExpandBatch,
    GraphExpandBatchKeyContext,
    std.hash_map.default_max_load_percentage,
);

const GraphHydrateBatchEntry = struct {
    group_id: u64,
    identity_read_generation: ?u64,
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
    exclude_nodes: []const GraphNodeIdentityJson = &.{},
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
    table: ?[]const u8 = null,
    depth: u32 = 0,
    distance: f64 = 0,
};

const GraphNodeIdentityJson = struct {
    key: []const u8,
    table: ?[]const u8 = null,
};

const GraphExpandParamsJson = struct {
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    max_depth: u32 = 1,
    max_results: u32 = 0,
    min_weight: ?f64 = null,
    max_weight: ?f64 = null,
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
    _filter_query_json: []const u8 = "",
    _exclusion_query_json: []const u8 = "",
    include_stored: bool = true,
    fields: []const []const u8 = &.{},
    include_all_fields: bool = true,
    include_hits: bool = true,
    incoming_index_name: []const u8 = "",
    _resolved_doc_filter: ?std.json.Value = null,
};

const GraphHydrateResponseJson = struct {
    hits: []const db_mod.types.SearchHit = &.{},
    has_incoming: []const bool = &.{},
};

const GraphEdgesRequestJson = struct {
    index_name: []const u8,
    key: []const u8,
    edge_types: []const []const u8 = &.{},
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
                if (graph_query.query.pattern.len == 0 and graph_query.query.match_pattern == null) return false;
            },
        }
    }
    return true;
}

pub fn requiresCompleteMatchAnchors(req: db_mod.types.SearchRequest) bool {
    for (req.graph_queries) |graph_query| {
        if (graph_query.query.match_pattern != null) return true;
    }
    return false;
}

/// Storage-backed canonical MATCH anchors are consumed as stable `_id` cursor
/// pages. The page bound controls transient memory only; it is deliberately not
/// a semantic limit on exact aggregation.
pub const complete_match_anchor_page_size: u32 = 4096;

pub const MatchAnchorPageSource = struct {
    ctx: *anyopaque,
    fetch_fn: *const fn (
        ctx: *anyopaque,
        alloc: std.mem.Allocator,
        graph_query: db_mod.types.NamedGraphQuery,
        search_after: []const std.json.Value,
    ) anyerror!db_mod.types.SearchResult,

    fn fetch(
        self: MatchAnchorPageSource,
        alloc: std.mem.Allocator,
        graph_query: db_mod.types.NamedGraphQuery,
        search_after: []const std.json.Value,
    ) !db_mod.types.SearchResult {
        return self.fetch_fn(self.ctx, alloc, graph_query, search_after);
    }
};

pub const MatchAnchorSource = union(enum) {
    /// Compatibility path for embedded callers and focused tests. Production
    /// table reads use `paged` so independent MATCH operations never share a
    /// materialized candidate ceiling.
    materialized: db_mod.types.SearchResult,
    paged: MatchAnchorPageSource,
};

fn resultHasIdentitySnapshot(
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
) bool {
    return req.identity_read_generation != null or
        base_result.identity_read_generation != null or
        base_result.shard_identity_read_generations.len > 0;
}

fn rejectUnstampedResultRefs(
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
) !void {
    if (resultHasIdentitySnapshot(req, base_result)) return;
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
        .identities => false,
        .result_ref => true,
    };
}

fn requireStampedCrossRangeRequest(
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
) !void {
    if (req.identity_read_generation != null) return;
    if (req.resolved_doc_filter != null or req.resolved_doc_filter_wire_context != null) return error.UnsupportedQueryRequest;
    if (base_result.identity_read_generation != null or
        base_result.shard_identity_read_generations.len > 0) return;
    for (req.graph_queries) |graph_query| {
        if (selectorUsesResultRef(graph_query.query.start_nodes)) return error.UnsupportedQueryRequest;
        if (graph_query.query.target_nodes) |target_nodes| {
            if (selectorUsesResultRef(target_nodes)) return error.UnsupportedQueryRequest;
        }
    }
}

fn identityReadGenerationForGroup(
    identity_read_generation: ?u64,
    identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration,
    group_id: u64,
) !?u64 {
    if (identity_read_generations.len == 0) return identity_read_generation;

    var generation: ?u64 = null;
    for (identity_read_generations) |token| {
        if (token.group_id != group_id) continue;
        if (generation != null) return error.InvalidQueryResult;
        generation = token.generation;
    }
    return generation orelse error.TopologyChanged;
}

fn validateSourceSnapshotGroupSet(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    base_result: db_mod.types.SearchResult,
) !void {
    const tokens = base_result.shard_identity_read_generations;
    if (tokens.len == 0) return;

    const group_ids = try table_catalog.resolveGroupsForSpan(alloc, catalog, table_name, "", "");
    defer alloc.free(group_ids);
    if (group_ids.len != tokens.len) return error.TopologyChanged;
    for (group_ids) |group_id| {
        _ = try identityReadGenerationForGroup(null, tokens, group_id);
    }
}

fn validateMatchingSourceSnapshots(
    base_result: db_mod.types.SearchResult,
    match_anchor_result: db_mod.types.SearchResult,
) !void {
    const base_tokens = base_result.shard_identity_read_generations;
    const anchor_tokens = match_anchor_result.shard_identity_read_generations;
    if (base_tokens.len > 0 or anchor_tokens.len > 0) {
        if (base_tokens.len != anchor_tokens.len) return error.TopologyChanged;
        for (base_tokens) |base_token| {
            const anchor_generation = try identityReadGenerationForGroup(
                null,
                anchor_tokens,
                base_token.group_id,
            );
            if (anchor_generation.? != base_token.generation) return error.TopologyChanged;
        }
        return;
    }
    if (base_result.identity_read_generation != match_anchor_result.identity_read_generation)
        return error.TopologyChanged;
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
    // Embedded/direct callers do not have an outer table-read coordinator to
    // refresh routing. Preserve their single bounded retry here. Production
    // table reads use executeCrossRangeWithMatchAnchors and own the retry so a
    // fresh attempt also refreshes the base scan and MATCH anchor snapshots.
    var attempts: u32 = 0;
    while (true) : (attempts += 1) {
        return executeCrossRangeWithMatchAnchors(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            if (requiresCompleteMatchAnchors(req)) MatchAnchorSource{ .materialized = base_result } else null,
            consistency,
        ) catch |err| switch (err) {
            error.TopologyChanged => {
                if (attempts == 0) continue;
                return err;
            },
            else => return err,
        };
    }
}

pub fn executeCrossRangeWithMatchAnchors(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    match_anchor_source: ?MatchAnchorSource,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.GraphSearchResult {
    if (!supportsCrossRange(req)) return error.UnsupportedQueryRequest;
    try requireStampedCrossRangeRequest(req, base_result);
    try rejectUnstampedResultRefs(req, base_result);
    if (requiresCompleteMatchAnchors(req) and match_anchor_source == null)
        return error.UnsupportedQueryRequest;
    if (match_anchor_source) |source| switch (source) {
        .materialized => |anchors| {
            if (anchors.total_hits_relation != .exact or @as(u64, anchors.total_hits) != anchors.hits.len)
                return error.InvalidQueryResult;
            try validateMatchingSourceSnapshots(base_result, anchors);
        },
        .paged => {},
    };

    var request_worker = worker;
    request_worker.execution_deadline_ns = req.execution_deadline_ns;
    request_worker.cancellation = req.cancellation;
    try request_worker.ensureActive();

    try request_worker.ensureActive();
    return executeCrossRangeOnce(alloc, catalog, request_worker, table_name, req, base_result, match_anchor_source, consistency) catch |err| switch (err) {
        // UnknownGroup is topology churn from the coordinator's perspective.
        // Let the outer table-read attempt refresh the complete routing and
        // snapshot state instead of retrying expensive graph work in place.
        error.UnknownGroup => error.TopologyChanged,
        else => err,
    };
}

fn executeCrossRangeOnce(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    match_anchor_source: ?MatchAnchorSource,
    consistency: raft_mod.ReadConsistency,
) ![]db_mod.types.GraphSearchResult {
    if (!supportsCrossRange(req)) return error.UnsupportedQueryRequest;
    // The coordinator also serves single-node and standalone deployments,
    // where merged runtime status may not be projected even though the
    // catalog range and the opened DB carry a valid identity namespace.
    // Reject every reported rebuild/conflict/reassignment, while allowing the
    // existing range + generation checks below to validate an unstamped
    // standalone catalog without weakening cross-shard snapshot fencing.
    try table_catalog.validateDocIdentityReadyForTable(alloc, catalog, table_name);
    try validateSourceSnapshotGroupSet(alloc, catalog, table_name, base_result);
    if (match_anchor_source) |source| switch (source) {
        .materialized => |anchors| try validateSourceSnapshotGroupSet(alloc, catalog, table_name, anchors),
        .paged => {},
    };

    const results = try alloc.alloc(db_mod.types.GraphSearchResult, req.graph_queries.len);
    const sorted_query_indexes = try graph_exec.sortGraphQueriesByDependencies(alloc, req.graph_queries);
    defer alloc.free(sorted_query_indexes);
    // These are HTTP-query budgets, not per-operation budgets. A request may
    // contain many named graph operations, but it must not multiply expansion
    // work or retained distinct-identity memory by that operation count.
    var request_work_budget = graph_pattern_mod.WorkBudget.init(
        graph_pattern_mod.default_max_explored_nodes,
        graph_pattern_mod.default_max_explored_edges,
    );
    var request_distinct_budget = graph_pattern_mod.DistinctBudget.init(
        graph_pattern_mod.default_max_distinct_identities,
        graph_pattern_mod.default_max_distinct_identity_bytes,
    );
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*result| result.deinit(alloc);
        alloc.free(results);
    }

    for (sorted_query_indexes, 0..) |query_index, i| {
        const graph_query = req.graph_queries[query_index];
        results[i] = try executeSingleCrossRange(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            match_anchor_source,
            results[0..initialized],
            graph_query,
            consistency,
            &request_work_budget,
            &request_distinct_budget,
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
    seen: graph_node_identity.Map(void) = .{},

    fn deinit(self: *QueryState, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        for (self.nodes.items) |*node| node.deinit(alloc);
        self.nodes.deinit(alloc);
        for (self.hits.items) |*hit| hit.deinit(alloc);
        self.hits.deinit(alloc);
        for (self.path_states.items) |*path_state| path_state.deinit(alloc);
        self.path_states.deinit(alloc);
        self.seen.deinit(alloc);
        self.* = undefined;
    }

    fn deinitTransient(self: *QueryState, alloc: std.mem.Allocator) void {
        for (self.path_states.items) |*path_state| path_state.deinit(alloc);
        self.path_states.deinit(alloc);
        self.seen.deinit(alloc);
        self.path_states = .empty;
        self.seen = .{};
    }
};

const TargetNodeSet = struct {
    exact: graph_node_identity.Map(void) = .{},
    wildcard_keys: std.StringHashMapUnmanaged(void) = .empty,

    fn init(
        alloc: std.mem.Allocator,
        source_table: []const u8,
        req: db_mod.types.SearchRequest,
        base_result: db_mod.types.SearchResult,
        prior_results: []const db_mod.types.GraphSearchResult,
        selector: graph_query_mod.NodeSelector,
    ) !TargetNodeSet {
        const nodes = try resolveSelectorNodes(
            alloc,
            source_table,
            req,
            base_result,
            prior_results,
            selector,
        );
        defer freeNodeIdentities(alloc, nodes);

        var out = TargetNodeSet{};
        errdefer out.deinit(alloc);
        switch (selector) {
            .keys => {
                for (nodes) |node| {
                    if (out.wildcard_keys.contains(node.key)) continue;
                    const key = try alloc.dupe(u8, node.key);
                    out.wildcard_keys.putNoClobber(alloc, key, {}) catch |err| {
                        alloc.free(key);
                        return err;
                    };
                }
            },
            .identities, .result_ref => {
                for (nodes) |node| {
                    _ = try out.exact.putIfAbsent(alloc, node.ref(), {});
                }
            },
        }
        return out;
    }

    fn contains(
        self: *const TargetNodeSet,
        table: ?[]const u8,
        key: []const u8,
    ) bool {
        return self.wildcard_keys.contains(key) or
            self.exact.contains(.{ .table = table, .key = key });
    }

    fn deinit(self: *TargetNodeSet, alloc: std.mem.Allocator) void {
        self.exact.deinit(alloc);
        var it = self.wildcard_keys.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.wildcard_keys.deinit(alloc);
        self.* = .{};
    }
};

const GraphAdmissionTableState = struct {
    table_name: []u8,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration = &.{},
    filter_query_json: []u8 = &.{},
    exclusion_query_json: []u8 = &.{},
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,
    allowed: bool,
    requires_admission: bool,
    graph_index_available: ?bool = null,
    requires_hydration: bool,
    decisions: std.StringHashMapUnmanaged(bool) = .empty,

    fn deinit(self: *GraphAdmissionTableState, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.filter_query_json.len > 0) alloc.free(self.filter_query_json);
        if (self.exclusion_query_json.len > 0) alloc.free(self.exclusion_query_json);
        var it = self.decisions.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        self.decisions.deinit(alloc);
        self.* = undefined;
    }

    fn generationForGroup(self: *const GraphAdmissionTableState, group_id: u64) !?u64 {
        return identityReadGenerationForGroup(
            self.identity_read_generation,
            self.identity_read_generations,
            group_id,
        );
    }
};

/// Query-scoped graph document admission. Edge expansion remains on the shard
/// that owns the graph edge, while document predicates are evaluated in
/// batches on the shard that owns each reached document.
const GraphNodeAdmissionContext = struct {
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    source_table: []const u8,
    source_topology_epoch: u64,
    source_identity_read_generation: ?u64,
    source_identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration,
    source_filter_query_json: []const u8,
    source_exclusion_query_json: []const u8,
    source_resolved_doc_filter: ?*const anyopaque,
    source_resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    table_authorizer: ?db_mod.types.GraphTableReadAuthorizer,
    node_filter: graph_pattern_mod.NodeFilter,
    consistency: raft_mod.ReadConsistency,
    tables: std.StringArrayHashMapUnmanaged(GraphAdmissionTableState) = .empty,

    fn init(
        alloc: std.mem.Allocator,
        catalog: table_catalog.CatalogSource,
        worker: Worker,
        source_table: []const u8,
        source_topology_epoch: u64,
        req: db_mod.types.SearchRequest,
        source_result_identity_read_generation: ?u64,
        source_identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration,
        node_filter: graph_pattern_mod.NodeFilter,
        consistency: raft_mod.ReadConsistency,
    ) GraphNodeAdmissionContext {
        return .{
            .alloc = alloc,
            .catalog = catalog,
            .worker = worker,
            .source_table = source_table,
            .source_topology_epoch = source_topology_epoch,
            .source_identity_read_generation = req.identity_read_generation orelse source_result_identity_read_generation,
            .source_identity_read_generations = source_identity_read_generations,
            .source_filter_query_json = req.filter_query_json,
            .source_exclusion_query_json = req.exclusion_query_json,
            .source_resolved_doc_filter = req.resolved_doc_filter,
            .source_resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
            .table_authorizer = req.graph_table_read_authorizer,
            .node_filter = node_filter,
            .consistency = consistency,
        };
    }

    fn deinit(self: *GraphNodeAdmissionContext) void {
        for (self.tables.values()) |*state| state.deinit(self.alloc);
        self.tables.deinit(self.alloc);
        self.* = undefined;
    }

    fn iface(self: *GraphNodeAdmissionContext) graph_node_admission.NodeAdmission {
        return .{
            .ctx = self,
            .external_targets = false,
            .filter_many = filterMany,
        };
    }

    fn ensureTable(
        self: *GraphNodeAdmissionContext,
        table_name: []const u8,
    ) !*GraphAdmissionTableState {
        if (self.tables.getPtr(table_name)) |state| return state;

        const owned_name = try self.alloc.dupe(u8, table_name);
        var owned_name_live = true;
        errdefer if (owned_name_live) self.alloc.free(owned_name);

        var filter_query_json: []u8 = &.{};
        var filter_query_json_live = false;
        errdefer if (filter_query_json_live) self.alloc.free(filter_query_json);
        var exclusion_query_json: []u8 = &.{};
        var exclusion_query_json_live = false;
        errdefer if (exclusion_query_json_live) self.alloc.free(exclusion_query_json);
        var topology_epoch: u64 = 0;
        var identity_read_generation: ?u64 = null;
        var identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration = &.{};
        var resolved_doc_filter: ?*const anyopaque = null;
        var resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null;
        var allowed = false;
        var requires_admission = false;
        var requires_hydration = false;

        if (std.mem.eql(u8, table_name, self.source_table)) {
            topology_epoch = self.source_topology_epoch;
            identity_read_generation = self.source_identity_read_generation;
            identity_read_generations = self.source_identity_read_generations;
            resolved_doc_filter = self.source_resolved_doc_filter;
            resolved_doc_filter_wire_context = self.source_resolved_doc_filter_wire_context;
            allowed = true;
            requires_hydration = self.source_filter_query_json.len > 0 or
                self.source_exclusion_query_json.len > 0 or
                self.source_resolved_doc_filter != null;
            if (self.source_filter_query_json.len > 0) {
                filter_query_json = try self.alloc.dupe(u8, self.source_filter_query_json);
                filter_query_json_live = true;
            }
            if (self.source_exclusion_query_json.len > 0) {
                exclusion_query_json = try self.alloc.dupe(u8, self.source_exclusion_query_json);
                exclusion_query_json_live = true;
            }
        } else {
            var authorization = if (self.table_authorizer) |authorizer|
                try authorizer.authorize(self.alloc, table_name)
            else
                db_mod.types.GraphTableReadAuthorization{ .allowed = true };
            defer authorization.deinit(self.alloc);
            const exists = authorization.allowed and
                try table_catalog.tableExists(self.catalog, table_name);
            topology_epoch = if (exists)
                try table_catalog.topologyEpoch(self.alloc, self.catalog, table_name)
            else
                0;
            allowed = exists;
            requires_hydration = self.table_authorizer != null;
            if (authorization.filter_query_json) |filter| {
                filter_query_json = filter;
                filter_query_json_live = true;
                authorization.filter_query_json = null;
            }
        }

        if (self.node_filter.filter_query_json) |node_filter_json| {
            if (filter_query_json.len == 0) {
                filter_query_json = try self.alloc.dupe(u8, node_filter_json);
                filter_query_json_live = true;
            } else {
                const combined = try std.fmt.allocPrint(
                    self.alloc,
                    "{{\"conjuncts\":[{s},{s}]}}",
                    .{ filter_query_json, node_filter_json },
                );
                self.alloc.free(filter_query_json);
                filter_query_json = combined;
            }
            requires_hydration = true;
        }
        requires_admission = requires_hydration or
            self.node_filter.filter_prefix.len > 0;

        var state = GraphAdmissionTableState{
            .table_name = owned_name,
            .topology_epoch = topology_epoch,
            .identity_read_generation = identity_read_generation,
            .identity_read_generations = identity_read_generations,
            .filter_query_json = filter_query_json,
            .exclusion_query_json = exclusion_query_json,
            .resolved_doc_filter = resolved_doc_filter,
            .resolved_doc_filter_wire_context = resolved_doc_filter_wire_context,
            .allowed = allowed,
            .requires_admission = requires_admission,
            .requires_hydration = requires_hydration,
        };
        owned_name_live = false;
        filter_query_json_live = false;
        exclusion_query_json_live = false;
        errdefer state.deinit(self.alloc);
        try self.tables.put(self.alloc, state.table_name, state);
        return self.tables.getPtr(table_name).?;
    }

    fn graphIndexAvailable(
        self: *GraphNodeAdmissionContext,
        state: *GraphAdmissionTableState,
        index_name: []const u8,
    ) !bool {
        if (std.mem.eql(u8, state.table_name, self.source_table)) return true;
        if (state.graph_index_available) |available| return available;
        const available = try catalogTableHasGraphIndex(
            self.alloc,
            self.catalog,
            state.table_name,
            index_name,
        );
        state.graph_index_available = available;
        return available;
    }

    fn filterMany(
        ctx: ?*anyopaque,
        result_alloc: std.mem.Allocator,
        nodes: []const graph_node_admission.NodeRef,
    ) anyerror![]bool {
        const self: *GraphNodeAdmissionContext = @ptrCast(@alignCast(ctx orelse {
            return error.InvalidArgument;
        }));
        const result = try result_alloc.alloc(bool, nodes.len);
        errdefer result_alloc.free(result);
        @memset(result, false);
        if (nodes.len == 0) return result;

        for (nodes) |node| {
            _ = try self.ensureTable(node.table orelse self.source_table);
        }

        const Pending = struct {
            keys: std.ArrayListUnmanaged([]const u8) = .empty,
            seen: std.StringHashMapUnmanaged(void) = .empty,

            fn deinit(self_inner: *@This(), alloc_inner: std.mem.Allocator) void {
                self_inner.keys.deinit(alloc_inner);
                self_inner.seen.deinit(alloc_inner);
            }
        };
        var pending = std.AutoHashMapUnmanaged(*GraphAdmissionTableState, Pending).empty;
        defer {
            var it = pending.valueIterator();
            while (it.next()) |item| item.deinit(result_alloc);
            pending.deinit(result_alloc);
        }

        for (nodes) |node| {
            const state = self.tables.getPtr(node.table orelse self.source_table).?;
            if (!state.allowed or !state.requires_admission or state.decisions.contains(node.key)) continue;
            if (self.node_filter.filter_prefix.len > 0 and
                !std.mem.startsWith(u8, node.key, self.node_filter.filter_prefix))
            {
                try self.cacheDecision(state, node.key, false);
                continue;
            }
            if (!state.requires_hydration) {
                try self.cacheDecision(state, node.key, true);
                continue;
            }
            const entry = try pending.getOrPut(result_alloc, state);
            if (!entry.found_existing) entry.value_ptr.* = .{};
            const seen = try entry.value_ptr.seen.getOrPut(result_alloc, node.key);
            if (seen.found_existing) continue;
            seen.value_ptr.* = {};
            try entry.value_ptr.keys.append(result_alloc, node.key);
        }

        var pending_it = pending.iterator();
        while (pending_it.next()) |entry| {
            const state = entry.key_ptr.*;
            const hits = try hydrateHitsForKeys(
                result_alloc,
                self.catalog,
                self.worker,
                state.table_name,
                state.topology_epoch,
                state.identity_read_generation,
                state.identity_read_generations,
                state.filter_query_json,
                state.exclusion_query_json,
                state.resolved_doc_filter,
                state.resolved_doc_filter_wire_context,
                false,
                true,
                &.{},
                entry.value_ptr.keys.items,
                self.consistency,
            );
            defer {
                for (hits) |*hit| hit.deinit(result_alloc);
                if (hits.len > 0) result_alloc.free(hits);
            }
            var allowed = std.StringHashMapUnmanaged(void).empty;
            defer allowed.deinit(result_alloc);
            for (hits) |hit| try allowed.put(result_alloc, hit.id, {});
            for (entry.value_ptr.keys.items) |key| {
                try self.cacheDecision(state, key, allowed.contains(key));
            }
        }

        for (nodes, 0..) |node, i| {
            const state = self.tables.getPtr(node.table orelse self.source_table).?;
            result[i] = state.allowed and
                (!state.requires_admission or
                    (state.decisions.get(node.key) orelse return error.InvalidGraphNodeAdmissionResult));
        }
        return result;
    }

    fn cacheDecision(
        self: *GraphNodeAdmissionContext,
        state: *GraphAdmissionTableState,
        key: []const u8,
        allowed: bool,
    ) !void {
        if (state.decisions.contains(key)) return;
        const owned_key = try self.alloc.dupe(u8, key);
        state.decisions.putNoClobber(self.alloc, owned_key, allowed) catch |err| {
            self.alloc.free(owned_key);
            return err;
        };
    }
};

const PathState = struct {
    key: []u8,
    table: ?[]u8 = null,
    depth: u32,
    distance: f64,
    cost: f64,
    parent: ?u32 = null,
    incoming_edge: ?graph_query_mod.PathEdgeInfo = null,

    fn deinit(self: *PathState, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        if (self.incoming_edge) |edge| freeOwnedPathEdge(alloc, edge);
        self.* = undefined;
    }
};

const FrontierState = struct {
    key: []u8,
    table: ?[]u8 = null,
    depth: u32 = 0,
    distance: f64 = 0,
    cost: f64 = 0,
    path_state_id: ?u32 = null,

    fn deinit(self: *FrontierState, alloc: std.mem.Allocator) void {
        alloc.free(self.key);
        if (self.table) |table| alloc.free(table);
        self.* = undefined;
    }
};

fn graphNodeAdmissionRequest(
    req: db_mod.types.SearchRequest,
    graph_query: db_mod.types.NamedGraphQuery,
) db_mod.types.SearchRequest {
    if (graph_query.query.match_pattern == null) return req;
    var admission_req = req;
    admission_req.filter_query_json = req.authorization_filter_query_json;
    admission_req.exclusion_query_json = "";
    // The resolved filter belongs to the combined retrieval predicate and
    // cannot safely be reused for the authorization-only MATCH relation.
    admission_req.resolved_doc_filter = null;
    admission_req.resolved_doc_filter_owned = false;
    admission_req.resolved_doc_filter_wire_context = null;
    return admission_req;
}

fn executeSingleCrossRange(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    match_anchor_source: ?MatchAnchorSource,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
    request_work_budget: *graph_pattern_mod.WorkBudget,
    request_distinct_budget: *graph_pattern_mod.DistinctBudget,
) !db_mod.types.GraphSearchResult {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    const admission_req = graphNodeAdmissionRequest(req, graph_query);
    var admission = GraphNodeAdmissionContext.init(
        alloc,
        catalog,
        worker,
        table_name,
        topology_epoch,
        admission_req,
        base_result.identity_read_generation,
        base_result.shard_identity_read_generations,
        graph_query.query.params.node_filter,
        consistency,
    );
    defer admission.deinit();
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
            &admission,
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
            &admission,
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
            &admission,
        ),
        .pattern => try executeDistributedPattern(
            alloc,
            catalog,
            worker,
            table_name,
            req,
            base_result,
            match_anchor_source,
            prior_results,
            graph_query,
            consistency,
            &admission,
            request_work_budget,
            request_distinct_budget,
        ),
    };
}

const DistributedEdgeReader = struct {
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    source_table: []const u8,
    index_name: []const u8,
    consistency: raft_mod.ReadConsistency,
    admission: *GraphNodeAdmissionContext,

    pub fn getEdges(
        self: @This(),
        a: std.mem.Allocator,
        table: ?[]const u8,
        key: []const u8,
        edge_types: []const []const u8,
        direction: graph_mod.EdgeDirection,
    ) ![]graph_mod.Edge {
        const table_name = table orelse self.source_table;
        const table_state = try self.admission.ensureTable(table_name);
        if (!table_state.allowed) return try a.alloc(graph_mod.Edge, 0);
        // Cross-table endpoints remain useful result nodes even when their table
        // does not expose the same graph index. Treat those nodes as terminals
        // instead of turning an explicit multi-hop traversal into IndexNotFound.
        if (!(try self.admission.graphIndexAvailable(table_state, self.index_name)))
            return try a.alloc(graph_mod.Edge, 0);
        const group_id = (try table_catalog.resolveGroupForKeyPinned(
            a,
            self.catalog,
            table_name,
            key,
            table_state.topology_epoch,
        )) orelse return error.TableNotFound;

        const index_name = try a.dupe(u8, self.index_name);
        errdefer a.free(index_name);
        const owned_key = try a.dupe(u8, key);
        errdefer a.free(owned_key);
        var tensor_access_path = try cloneGraphTensorAccessPathAlloc(
            a,
            algebraic_ir.graphEdgeAccessPath(self.index_name),
        );
        errdefer tensor_access_path.deinit(a);
        var tensor_program = try graphEdgesTensorProgramEnvelopeAlloc(a, self.index_name);
        errdefer tensor_program.deinit(a);

        var req = GraphEdgesRequest{
            .index_name = index_name,
            .key = owned_key,
            .edge_types = try dupConstStrings(a, edge_types),
            .direction = direction,
            .tensor_access_path = tensor_access_path,
            .tensor_program = tensor_program,
            .topology_epoch = table_state.topology_epoch,
            .identity_read_generation = try table_state.generationForGroup(group_id),
        };
        defer req.deinit(a);

        var resp = try self.worker.executeGraphGetEdges(a, group_id, table_name, req, self.consistency);

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
    match_anchor_source: ?MatchAnchorSource,
    prior_results: []const db_mod.types.GraphSearchResult,
    graph_query: db_mod.types.NamedGraphQuery,
    consistency: raft_mod.ReadConsistency,
    admission: *GraphNodeAdmissionContext,
    request_work_budget: *graph_pattern_mod.WorkBudget,
    request_distinct_budget: *graph_pattern_mod.DistinctBudget,
) !db_mod.types.GraphSearchResult {
    // Resolve start keys.
    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    errdefer state.deinit(alloc);
    const edge_reader = DistributedEdgeReader{
        .catalog = catalog,
        .worker = worker,
        .source_table = table_name,
        .index_name = graph_query.query.index_name,
        .consistency = consistency,
        .admission = admission,
    };
    const canonical_match = graph_query.query.match_pattern != null;
    if (canonical_match) {
        return try executeDistributedConjunctivePattern(
            alloc,
            edge_reader,
            req,
            graph_query,
            match_anchor_source orelse return error.UnsupportedQueryRequest,
            base_result,
            admission,
            &state,
            request_work_budget,
            request_distinct_budget,
        );
    }
    const selector_result = base_result;
    var selector_req = req;
    if (canonical_match) selector_req.limit = 0;
    const frontier = try resolveStartFrontier(alloc, &state, table_name, selector_req, selector_result, prior_results, graph_query.query.start_nodes, false);
    defer freeFrontier(alloc, frontier);

    const start_nodes = try alloc.alloc(graph_node_identity.Ref, frontier.len);
    defer alloc.free(start_nodes);
    for (frontier, 0..) |item, i| {
        start_nodes[i] = .{ .table = item.table, .key = item.key };
    }

    const target_frontier = if (graph_query.query.target_nodes) |selector|
        try resolveStartFrontier(alloc, &state, table_name, req, base_result, prior_results, selector, false)
    else
        try alloc.alloc(FrontierState, 0);
    defer freeFrontier(alloc, target_frontier);
    const target_nodes = try alloc.alloc(graph_node_identity.Ref, target_frontier.len);
    defer alloc.free(target_nodes);
    for (target_frontier, 0..) |item, i| {
        target_nodes[i] = .{ .table = item.table, .key = item.key };
    }

    // Run pattern matching with distributed edges.
    const raw_matches = try graph_pattern_mod.matchPatternFromRefsWithEdgeReader(
        alloc,
        edge_reader,
        start_nodes,
        graph_query.query.pattern,
        .{
            .max_results = graph_query.query.params.max_results,
            .return_aliases = graph_query.query.return_aliases,
            .target_nodes = target_nodes,
            .target_required = graph_query.query.target_nodes != null,
            .include_paths = graph_query.query.params.include_paths,
            .node_admission = admission.iface(),
            .work_budget = request_work_budget,
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
    const hits = if (graphResultHydrationRequested(req, graph_query.query))
        try hydrateHitsForResultNodes(alloc, admission, unique_nodes, graph_query.query.include_all_fields, graph_query.query.fields)
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

const AggregatePageAccumulator = struct {
    distinct: bool,
    value: u128 = 0,
    seen: graph_node_identity.Map(void) = .{},

    fn observe(
        self: *@This(),
        alloc: std.mem.Allocator,
        page: graph_pattern_mod.CountAggregateResult,
        distinct_budget: *graph_pattern_mod.DistinctBudget,
    ) !void {
        if (!self.distinct) {
            self.value = std.math.add(u128, self.value, page.value) catch return error.QueryCandidateBudgetExceeded;
            return;
        }
        for (page.distinct_values) |identity| {
            if (self.seen.contains(identity)) continue;
            try distinct_budget.consume(identity);
            _ = try self.seen.putIfAbsent(alloc, identity, {});
        }
        self.value = @intCast(self.seen.count());
    }

    fn cloneDistinctValues(self: *@This(), alloc: std.mem.Allocator) ![]graph_node_identity.Ref {
        if (!self.distinct) return &.{};
        const values = try alloc.alloc(graph_node_identity.Ref, self.seen.count());
        var initialized: usize = 0;
        errdefer {
            for (values[0..initialized]) |value| {
                if (value.table) |table| alloc.free(table);
                alloc.free(value.key);
            }
            if (values.len > 0) alloc.free(values);
        }
        var it = self.seen.keyIterator();
        while (it.next()) |identity| {
            const table = if (identity.table()) |table_name| try alloc.dupe(u8, table_name) else null;
            errdefer if (table) |table_name| alloc.free(table_name);
            values[initialized] = .{ .table = table, .key = try alloc.dupe(u8, identity.key()) };
            initialized += 1;
        }
        return values;
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.seen.deinit(alloc);
        self.* = undefined;
    }
};

fn materializeAggregateResults(
    alloc: std.mem.Allocator,
    requested: []const graph_query_mod.NamedCountAggregate,
    aggregate_indexes: []const usize,
    accumulators: []AggregatePageAccumulator,
) ![]db_mod.types.GraphAggregateResult {
    if (requested.len != aggregate_indexes.len) return error.InvalidArgument;
    const aggregates = try alloc.alloc(db_mod.types.GraphAggregateResult, requested.len);
    const distinct_value_owners = try alloc.alloc(?usize, accumulators.len);
    defer alloc.free(distinct_value_owners);
    @memset(distinct_value_owners, null);
    var initialized: usize = 0;
    errdefer {
        for (aggregates[0..initialized]) |*aggregate| aggregate.deinit(alloc);
        if (aggregates.len > 0) alloc.free(aggregates);
    }
    for (requested, 0..) |aggregate, i| {
        const accumulator_index = aggregate_indexes[i];
        if (accumulator_index >= accumulators.len) return error.InvalidArgument;
        const accumulator = &accumulators[accumulator_index];
        aggregates[i] = blk: {
            const name = try alloc.dupe(u8, aggregate.name);
            errdefer alloc.free(name);
            if (accumulator.distinct) {
                if (distinct_value_owners[accumulator_index]) |owner| {
                    break :blk .{
                        .name = name,
                        .value = accumulator.value,
                        .distinct_values = aggregates[owner].distinct_values,
                        .distinct_values_owned = false,
                    };
                }
                const values = try accumulator.cloneDistinctValues(alloc);
                errdefer {
                    for (values) |value| {
                        if (value.table) |table| alloc.free(table);
                        alloc.free(value.key);
                    }
                    if (values.len > 0) alloc.free(values);
                }
                distinct_value_owners[accumulator_index] = i;
                break :blk .{
                    .name = name,
                    .value = accumulator.value,
                    .distinct_values = values,
                };
            }
            break :blk .{
                .name = name,
                .value = accumulator.value,
            };
        };
        initialized += 1;
    }
    return aggregates;
}

fn validateMatchAnchorPageOrder(
    prior_cursor: ?[]const u8,
    hits: []const db_mod.types.SearchHit,
) !void {
    var prior_id = prior_cursor;
    for (hits) |hit| {
        if (prior_id) |prior| {
            if (std.mem.order(u8, prior, hit.id) != .lt) return error.InvalidQueryResult;
        }
        prior_id = hit.id;
    }
}

/// `_id` seek totals are cursor-relative and may be lower bounds when a page
/// fills, so they cannot define completion for a streamed relation. Snapshot
/// pinning and strict cursor order protect consistency; an exact short page is
/// the storage iterator's end-of-stream signal. An exact multiple deliberately
/// performs one final empty fetch, while a short lower-bound page fails closed.
fn matchAnchorPageIsTerminal(
    source: MatchAnchorSource,
    hit_count: usize,
    total_hits: u32,
    total_hits_relation: db_mod.types.TotalHitsRelation,
) !bool {
    return switch (source) {
        .materialized => {
            if (total_hits_relation != .exact or @as(u64, total_hits) != hit_count)
                return error.InvalidQueryResult;
            return true;
        },
        .paged => {
            if (hit_count > complete_match_anchor_page_size)
                return error.InvalidQueryResult;
            if (@as(u64, total_hits) < hit_count) return error.InvalidQueryResult;
            if (hit_count == complete_match_anchor_page_size) return false;
            // A short lower-bound page could reflect an early storage stop;
            // accepting it would silently turn an incomplete scan into an
            // exact result.
            if (total_hits_relation != .exact) return error.InvalidQueryResult;
            return true;
        },
    };
}

fn executeDistributedConjunctivePattern(
    alloc: std.mem.Allocator,
    edge_reader: DistributedEdgeReader,
    req: db_mod.types.SearchRequest,
    graph_query: db_mod.types.NamedGraphQuery,
    anchor_source: MatchAnchorSource,
    base_result: db_mod.types.SearchResult,
    admission: *GraphNodeAdmissionContext,
    state: *QueryState,
    request_work_budget: *graph_pattern_mod.WorkBudget,
    request_distinct_budget: *graph_pattern_mod.DistinctBudget,
) !db_mod.types.GraphSearchResult {
    const pattern = graph_query.query.match_pattern orelse return error.InvalidArgument;
    var filter_evaluator = DistributedPatternFilterEvaluator{ .admission = admission };
    defer filter_evaluator.deinit();
    // Source anchors are storage-scan input and do not consume this budget.
    // Reached graph nodes and examined edges do, across every cursor page, so
    // exact MATCH fails closed under bounded work without imposing an anchor
    // cardinality ceiling.
    const base_opts = graph_pattern_mod.MatchOptions{
        .max_results = if (graph_query.query.return_limit > 0)
            std.math.add(u32, graph_query.query.return_limit, 1) catch graph_query.query.return_limit
        else
            graph_query.query.params.max_results,
        .return_aliases = graph_query.query.return_aliases,
        .include_paths = false,
        .evaluator = .{
            .ctx = &filter_evaluator,
            .func = DistributedPatternFilterEvaluator.evaluate,
            .batch_func = DistributedPatternFilterEvaluator.evaluateMany,
        },
        .node_admission = admission.iface(),
        // Canonical paged anchors were produced by the same snapshot-pinned
        // source scan with authorization and the explicit anchor filter
        // already applied. Materialized/legacy starts do not carry that proof.
        .start_validation = switch (anchor_source) {
            .paged => .prevalidated,
            .materialized => .required,
        },
        .work_budget = request_work_budget,
    };

    const requested_specs = try alloc.alloc(graph_pattern_mod.CountAggregateSpec, graph_query.query.aggregates.len);
    defer if (requested_specs.len > 0) alloc.free(requested_specs);
    for (graph_query.query.aggregates, 0..) |aggregate, output_index| {
        requested_specs[output_index] = .{
            .alias = if (std.mem.eql(u8, aggregate.of, "*")) null else aggregate.of,
            .distinct = aggregate.distinct,
        };
    }
    var aggregate_plan = try graph_pattern_mod.CountAggregatePlan.init(alloc, requested_specs);
    defer aggregate_plan.deinit(alloc);
    const aggregate_accumulators = try alloc.alloc(AggregatePageAccumulator, aggregate_plan.unique_specs.len);
    defer if (aggregate_accumulators.len > 0) alloc.free(aggregate_accumulators);
    var initialized_accumulators: usize = 0;
    defer for (aggregate_accumulators[0..initialized_accumulators]) |*accumulator| accumulator.deinit(alloc);
    for (aggregate_plan.unique_specs, 0..) |spec, i| {
        aggregate_accumulators[i] = .{ .distinct = spec.distinct };
        initialized_accumulators += 1;
    }
    var collected_matches = std.ArrayListUnmanaged(graph_pattern_mod.PatternMatch).empty;
    defer {
        for (collected_matches.items) |*match| match.deinit(alloc);
        collected_matches.deinit(alloc);
    }
    var cursor_key: ?[]u8 = null;
    defer if (cursor_key) |key| alloc.free(key);
    var materialized_consumed = false;
    var truncated = false;

    while (true) {
        const cursor = if (cursor_key) |key| &[_]std.json.Value{.{ .string = key }} else &.{};
        var page_owned = false;
        var page = switch (anchor_source) {
            .materialized => |result| blk: {
                if (materialized_consumed) break;
                materialized_consumed = true;
                break :blk result;
            },
            .paged => |source| blk: {
                page_owned = true;
                break :blk try source.fetch(alloc, graph_query, cursor);
            },
        };
        defer if (page_owned) page.deinit();

        const terminal_page = try matchAnchorPageIsTerminal(
            anchor_source,
            page.hits.len,
            page.total_hits,
            page.total_hits_relation,
        );
        try validateSourceSnapshotGroupSet(alloc, edge_reader.catalog, edge_reader.source_table, page);
        try validateMatchingSourceSnapshots(base_result, page);
        try validateMatchAnchorPageOrder(cursor_key, page.hits);
        if (page.hits.len == 0) break;

        const start_keys = try alloc.alloc([]const u8, page.hits.len);
        defer alloc.free(start_keys);
        for (page.hits, 0..) |hit, i| start_keys[i] = hit.id;

        if (aggregate_plan.unique_specs.len > 0) {
            const computed = try graph_pattern_mod.aggregateConjunctivePatternWithEdgeReader(
                alloc,
                edge_reader,
                start_keys,
                pattern,
                aggregate_plan.unique_specs,
                base_opts,
            );
            defer {
                for (computed) |*aggregate| aggregate.deinit(alloc);
                if (computed.len > 0) alloc.free(computed);
            }
            for (computed, 0..) |aggregate, i| try aggregate_accumulators[i].observe(
                alloc,
                aggregate,
                request_distinct_budget,
            );
        } else {
            var page_opts = base_opts;
            const desired = std.math.add(usize, graph_query.query.return_limit, 1) catch return error.QueryCandidateBudgetExceeded;
            const remaining = desired -| collected_matches.items.len;
            page_opts.max_results = @intCast(remaining);
            const page_matches = try graph_pattern_mod.matchConjunctivePatternWithEdgeReader(
                alloc,
                edge_reader,
                start_keys,
                pattern,
                page_opts,
            );
            defer graph_pattern_mod.freeMatches(alloc, page_matches);
            try collected_matches.ensureUnusedCapacity(alloc, page_matches.len);
            for (page_matches) |*match| {
                collected_matches.appendAssumeCapacity(match.take());
            }
            if (collected_matches.items.len >= desired) {
                truncated = true;
                break;
            }
        }

        if (terminal_page) break;
        const next_cursor = try alloc.dupe(u8, page.hits[page.hits.len - 1].id);
        if (cursor_key) |prior| alloc.free(prior);
        cursor_key = next_cursor;
    }

    if (aggregate_plan.unique_specs.len > 0) {
        const aggregates = try materializeAggregateResults(
            alloc,
            graph_query.query.aggregates,
            aggregate_plan.output_indexes,
            aggregate_accumulators,
        );
        const name = state.name;
        state.name = &.{};
        return .{ .name = name, .aggregates = aggregates, .hits = &.{}, .total_hits = 0 };
    }

    const result_len = @min(collected_matches.items.len, graph_query.query.return_limit);
    const matches = try convertPatternMatches(alloc, collected_matches.items[0..result_len]);
    errdefer {
        for (matches) |*match| match.deinit(alloc);
        if (matches.len > 0) alloc.free(matches);
    }
    const unique_nodes = try graph_query_mod.collectUniqueNodesFromMatches(alloc, collected_matches.items[0..result_len]);
    defer {
        for (unique_nodes) |*node| node.deinit(alloc);
        if (unique_nodes.len > 0) alloc.free(unique_nodes);
    }
    const hits = if (graphResultHydrationRequested(req, graph_query.query))
        try hydrateHitsForResultNodes(alloc, admission, unique_nodes, graph_query.query.include_all_fields, graph_query.query.fields)
    else
        try alloc.alloc(db_mod.types.SearchHit, 0);
    const name = state.name;
    state.name = &.{};
    return .{
        .name = name,
        .matches = matches,
        .hits = hits,
        .total_hits = @intCast(collected_matches.items.len),
        .truncated = truncated,
    };
}

const DistributedPatternFilterEvaluator = struct {
    admission: *GraphNodeAdmissionContext,
    contexts: std.StringArrayHashMapUnmanaged(GraphNodeAdmissionContext) = .empty,

    fn evaluate(ctx: ?*anyopaque, node: graph_node_identity.Ref, filter: graph_pattern_mod.NodeFilter) anyerror!bool {
        const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        if (filter.filter_query_json == null) return true;
        const scoped = try self.contextFor(filter);
        const nodes = [_]graph_node_admission.NodeRef{.{ .table = node.table, .key = node.key }};
        const decisions = try GraphNodeAdmissionContext.filterMany(scoped, self.admission.alloc, &nodes);
        defer self.admission.alloc.free(decisions);
        return decisions[0];
    }

    fn evaluateMany(
        ctx: ?*anyopaque,
        alloc: std.mem.Allocator,
        nodes: []const graph_node_identity.Ref,
        filter: graph_pattern_mod.NodeFilter,
    ) anyerror![]bool {
        const self: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        if (filter.filter_query_json == null) {
            const decisions = try alloc.alloc(bool, nodes.len);
            @memset(decisions, true);
            return decisions;
        }
        const scoped = try self.contextFor(filter);
        const refs = try alloc.alloc(graph_node_admission.NodeRef, nodes.len);
        defer alloc.free(refs);
        for (nodes, 0..) |node, i| refs[i] = .{ .table = node.table, .key = node.key };
        return try GraphNodeAdmissionContext.filterMany(scoped, alloc, refs);
    }

    fn contextFor(self: *@This(), filter: graph_pattern_mod.NodeFilter) !*GraphNodeAdmissionContext {
        const encoded = filter.filter_query_json orelse return error.InvalidArgument;
        if (self.contexts.getPtr(encoded)) |context| return context;
        var request = db_mod.types.SearchRequest{};
        request.graph_table_read_authorizer = self.admission.table_authorizer;
        var scoped = GraphNodeAdmissionContext.init(
            self.admission.alloc,
            self.admission.catalog,
            self.admission.worker,
            self.admission.source_table,
            self.admission.source_topology_epoch,
            request,
            self.admission.source_identity_read_generation,
            self.admission.source_identity_read_generations,
            filter,
            self.admission.consistency,
        );
        errdefer scoped.deinit();
        try self.contexts.put(self.admission.alloc, encoded, scoped);
        return self.contexts.getPtr(encoded).?;
    }

    fn deinit(self: *@This()) void {
        for (self.contexts.values()) |*context| context.deinit();
        self.contexts.deinit(self.admission.alloc);
        self.* = undefined;
    }
};

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
            bindings[j] = try convertPatternBinding(alloc, binding);
            bindings_init += 1;
        }

        const path = try alloc.alloc(graph_query_mod.PathEdgeInfo, raw_match.path.len);
        var path_init: usize = 0;
        errdefer {
            for (path[0..path_init]) |edge| {
                alloc.free(edge.source);
                alloc.free(edge.target);
                alloc.free(edge.edge_type);
                if (edge.metadata.len > 0) alloc.free(edge.metadata);
            }
            alloc.free(path);
        }

        for (raw_match.path, 0..) |edge, k| {
            path[k] = try clonePatternPathEdge(alloc, edge);
            path_init += 1;
        }

        const null_aliases = try cloneOwnedStrings(alloc, raw_match.null_aliases);
        errdefer {
            for (null_aliases) |alias| alloc.free(alias);
            if (null_aliases.len > 0) alloc.free(null_aliases);
        }

        matches[i] = .{
            .bindings = bindings,
            .path = path,
            .null_aliases = null_aliases,
        };
        initialized += 1;
    }

    return matches;
}

fn convertPatternBinding(
    alloc: std.mem.Allocator,
    binding: graph_pattern_mod.PatternBinding,
) !db_mod.types.GraphPatternBinding {
    const alias = try alloc.dupe(u8, binding.alias);
    errdefer alloc.free(alias);
    const key = try alloc.dupe(u8, binding.key);
    errdefer alloc.free(key);
    const table = if (binding.table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (table) |value| alloc.free(value);

    return .{
        .alias = alias,
        .node = .{
            .key = key,
            .depth = binding.depth,
            .distance = @floatFromInt(binding.depth),
            .path = null,
            .path_edges = null,
            .table = table,
        },
    };
}

fn clonePatternPathEdge(
    alloc: std.mem.Allocator,
    edge: graph_paths_mod.PathEdge,
) !graph_query_mod.PathEdgeInfo {
    return clonePathEdge(alloc, .{
        .source = edge.source,
        .target = edge.target,
        .edge_type = edge.edge_type,
        .weight = edge.weight,
        .metadata = edge.metadata,
    });
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
    admission: *GraphNodeAdmissionContext,
) !db_mod.types.GraphSearchResult {
    const include_paths = graph_query.query.params.include_paths;
    const result_limit = graph_query.query.params.max_results;
    const collection_limit = graph_query_mod.resultCollectionLimit(result_limit);
    const max_depth: u32 = switch (graph_query.query.query_type) {
        .neighbors => 1,
        .traverse => graph_query.query.params.max_depth,
        else => return error.UnsupportedQueryRequest,
    };
    const algebraic_semiring_selected = graph_query.query.params.algebraic_semiring or
        try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, table_name, graph_query.query.index_name);
    var target_nodes: ?TargetNodeSet = null;
    defer if (target_nodes) |*nodes| nodes.deinit(alloc);
    if (graph_query.query.target_nodes) |selector| {
        target_nodes = try TargetNodeSet.init(
            alloc,
            table_name,
            req,
            base_result,
            prior_results,
            selector,
        );
    }

    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    errdefer state.deinit(alloc);

    var frontier = try resolveStartFrontier(alloc, &state, table_name, req, base_result, prior_results, graph_query.query.start_nodes, include_paths);
    defer freeFrontier(alloc, frontier);
    frontier = try retainAdmittedFrontier(alloc, admission, frontier);

    for (frontier) |item| {
        _ = try state.seen.putIfAbsent(
            alloc,
            .{ .table = item.table, .key = item.key },
            {},
        );
    }

    while (frontier.len > 0 and state.nodes.items.len < collection_limit) {
        var next_frontier = std.ArrayListUnmanaged(FrontierState).empty;
        defer {
            for (next_frontier.items) |*item| item.deinit(alloc);
            next_frontier.deinit(alloc);
        }

        const effective_max_depth = if (max_depth == 0) std.math.maxInt(u32) else max_depth;
        var batches = try batchFrontierByGroup(
            alloc,
            catalog,
            table_name,
            frontier,
            effective_max_depth,
            admission,
            graph_query.query.index_name,
        );
        defer freeFrontierBatches(alloc, &batches);
        const batch_entries = try collectGraphExpandBatchEntries(alloc, &batches);
        defer alloc.free(batch_entries);
        const exclude_nodes = try collectSeenNodes(alloc, &state.seen);
        defer {
            for (exclude_nodes) |*identity| identity.deinit(alloc);
            if (exclude_nodes.len > 0) alloc.free(exclude_nodes);
        }

        const fanout_io = worker.fanoutIo();
        const graph_fanout_plan = planGraphFanout(fanout_io != null, worker.fanoutWidthCap(), batch_entries.len);
        recordGraphFanoutPlan(.expand, graph_fanout_plan);
        if (fanout_io) |io| {
            if (!graph_fanout_plan.parallel) {
                var batch_it = batches.iterator();
                while (batch_it.next()) |entry| {
                    const batch_table = entry.key_ptr.table_name;
                    var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, frontier, entry.value_ptr.frontier_ids.items, exclude_nodes, @constCast((&[_][]u8{})[0..]), include_paths, algebraic_semiring_selected);
                    step_req.topology_epoch = entry.value_ptr.topology_epoch;
                    step_req.identity_read_generation = entry.value_ptr.identity_read_generation;
                    defer step_req.deinit(alloc);

                    var step_result = try worker.executeGraphExpand(alloc, entry.key_ptr.group_id, batch_table, step_req, consistency);
                    defer step_result.deinit(alloc);

                    const admitted = try graphExpansionNodeAdmissionMaskAlloc(
                        alloc,
                        admission,
                        table_name,
                        batch_table,
                        step_result.expansions,
                    );
                    defer alloc.free(admitted);
                    var admitted_index: usize = 0;
                    for (step_result.expansions) |expansion| {
                        const item = frontier[expansion.frontier_id];
                        const step_graph = expansion.graph_result;

                        for (step_graph.nodes) |node| {
                            const allowed = admitted[admitted_index];
                            admitted_index += 1;
                            if (!allowed) continue;
                            const node_table = canonicalExpandedNodeTable(
                                table_name,
                                batch_table,
                                node.table,
                            );
                            if (!try state.seen.putIfAbsent(
                                alloc,
                                .{
                                    .table = node_table,
                                    .key = node.key,
                                },
                                {},
                            )) continue;
                            const return_node = if (target_nodes) |*targets|
                                targets.contains(node_table, node.key)
                            else
                                true;

                            const path_state_id = if (include_paths)
                                try appendPathStateFromStep(alloc, &state, item, node, node_table)
                            else
                                null;
                            var merged_node = try materializeResultNode(alloc, &state, item, node, node_table, include_paths, path_state_id);
                            var merged_node_owned = true;
                            errdefer if (merged_node_owned) merged_node.deinit(alloc);

                            if (merged_node.depth < max_depth) {
                                try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                            }
                            if (return_node) {
                                try state.nodes.append(alloc, merged_node);
                                merged_node_owned = false;
                                if (state.nodes.items.len >= collection_limit) break;
                            } else {
                                merged_node.deinit(alloc);
                                merged_node_owned = false;
                            }
                        }

                        if (state.nodes.items.len >= collection_limit) break;
                    }

                    if (state.nodes.items.len >= collection_limit) break;
                }
            } else {
                const fanout_start_ns = platform_time.monotonicNs();
                const slots = try executeGraphExpandBatchesParallel(
                    alloc,
                    io,
                    graph_fanout_plan.width,
                    worker,
                    graph_query,
                    frontier,
                    batch_entries,
                    exclude_nodes,
                    include_paths,
                    algebraic_semiring_selected,
                    consistency,
                );
                recordGraphParallelFanout(.expand, @intCast(platform_time.monotonicNs() - fanout_start_ns));
                defer deinitGraphExpandFanoutSlots(alloc, slots);

                for (slots, batch_entries) |slot, batch_entry| {
                    const step_result = slot.result.?;
                    const admitted = try graphExpansionNodeAdmissionMaskAlloc(
                        alloc,
                        admission,
                        table_name,
                        batch_entry.table_name,
                        step_result.expansions,
                    );
                    defer alloc.free(admitted);
                    var admitted_index: usize = 0;
                    for (step_result.expansions) |expansion| {
                        const item = frontier[expansion.frontier_id];
                        const step_graph = expansion.graph_result;

                        for (step_graph.nodes) |node| {
                            const allowed = admitted[admitted_index];
                            admitted_index += 1;
                            if (!allowed) continue;
                            const node_table = canonicalExpandedNodeTable(
                                table_name,
                                batch_entry.table_name,
                                node.table,
                            );
                            if (!try state.seen.putIfAbsent(
                                alloc,
                                .{
                                    .table = node_table,
                                    .key = node.key,
                                },
                                {},
                            )) continue;
                            const return_node = if (target_nodes) |*targets|
                                targets.contains(node_table, node.key)
                            else
                                true;

                            const path_state_id = if (include_paths)
                                try appendPathStateFromStep(alloc, &state, item, node, node_table)
                            else
                                null;
                            var merged_node = try materializeResultNode(alloc, &state, item, node, node_table, include_paths, path_state_id);
                            var merged_node_owned = true;
                            errdefer if (merged_node_owned) merged_node.deinit(alloc);

                            if (merged_node.depth < max_depth) {
                                try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                            }
                            if (return_node) {
                                try state.nodes.append(alloc, merged_node);
                                merged_node_owned = false;
                                if (state.nodes.items.len >= collection_limit) break;
                            } else {
                                merged_node.deinit(alloc);
                                merged_node_owned = false;
                            }
                        }

                        if (state.nodes.items.len >= collection_limit) break;
                    }

                    if (state.nodes.items.len >= collection_limit) break;
                }
            }
        } else {
            var batch_it = batches.iterator();
            while (batch_it.next()) |entry| {
                const batch_table = entry.key_ptr.table_name;
                var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, frontier, entry.value_ptr.frontier_ids.items, exclude_nodes, @constCast((&[_][]u8{})[0..]), include_paths, algebraic_semiring_selected);
                step_req.topology_epoch = entry.value_ptr.topology_epoch;
                step_req.identity_read_generation = entry.value_ptr.identity_read_generation;
                defer step_req.deinit(alloc);

                var step_result = try worker.executeGraphExpand(alloc, entry.key_ptr.group_id, batch_table, step_req, consistency);
                defer step_result.deinit(alloc);

                const admitted = try graphExpansionNodeAdmissionMaskAlloc(
                    alloc,
                    admission,
                    table_name,
                    batch_table,
                    step_result.expansions,
                );
                defer alloc.free(admitted);
                var admitted_index: usize = 0;
                for (step_result.expansions) |expansion| {
                    const item = frontier[expansion.frontier_id];
                    const step_graph = expansion.graph_result;

                    for (step_graph.nodes) |node| {
                        const allowed = admitted[admitted_index];
                        admitted_index += 1;
                        if (!allowed) continue;
                        const node_table = canonicalExpandedNodeTable(
                            table_name,
                            batch_table,
                            node.table,
                        );
                        if (!try state.seen.putIfAbsent(
                            alloc,
                            .{
                                .table = node_table,
                                .key = node.key,
                            },
                            {},
                        )) continue;
                        const return_node = if (target_nodes) |*targets|
                            targets.contains(node_table, node.key)
                        else
                            true;

                        const path_state_id = if (include_paths)
                            try appendPathStateFromStep(alloc, &state, item, node, node_table)
                        else
                            null;
                        var merged_node = try materializeResultNode(alloc, &state, item, node, node_table, include_paths, path_state_id);
                        var merged_node_owned = true;
                        errdefer if (merged_node_owned) merged_node.deinit(alloc);

                        if (merged_node.depth < max_depth) {
                            try next_frontier.append(alloc, try frontierFromState(alloc, &state, merged_node, path_state_id));
                        }
                        if (return_node) {
                            try state.nodes.append(alloc, merged_node);
                            merged_node_owned = false;
                            if (state.nodes.items.len >= collection_limit) break;
                        } else {
                            merged_node.deinit(alloc);
                            merged_node_owned = false;
                        }
                    }

                    if (state.nodes.items.len >= collection_limit) break;
                }

                if (state.nodes.items.len >= collection_limit) break;
            }
        }

        freeFrontier(alloc, frontier);
        frontier = try next_frontier.toOwnedSlice(alloc);
    }

    const truncated = graph_query_mod.resultCountIsTruncated(state.nodes.items.len, result_limit);
    if (truncated) {
        const public_len: usize = @intCast(result_limit);
        for (state.nodes.items[public_len..]) |*node| node.deinit(alloc);
        state.nodes.items.len = public_len;
    }

    const hydrated_hits = if (graphResultHydrationRequested(req, graph_query.query))
        try hydrateHitsForResultNodes(alloc, admission, state.nodes.items, graph_query.query.include_all_fields, graph_query.query.fields)
    else
        try alloc.alloc(db_mod.types.SearchHit, 0);
    state.hits = try adoptHydratedHits(
        alloc,
        state.hits,
        hydrated_hits,
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
        .truncated = truncated,
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
    admission: *GraphNodeAdmissionContext,
) !db_mod.types.GraphSearchResult {
    var path_result = try findDistributedShortestPath(alloc, catalog, worker, table_name, req, base_result, prior_results, graph_query, consistency, admission, null, null, null);
    defer if (path_result) |*result| result.deinit(alloc);

    const nodes = if (path_result) |result| blk: {
        var node = try graphPathToResultNode(alloc, result.path);
        errdefer node.deinit(alloc);
        const out = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
        out[0] = node;
        break :blk out;
    } else @constCast((&[_]graph_query_mod.GraphResultNode{})[0..]);
    errdefer if (nodes.len > 0) {
        for (nodes) |*node| node.deinit(alloc);
        alloc.free(nodes);
    };

    const paths = if (path_result) |result| blk: {
        const out = try alloc.alloc(db_mod.types.GraphPath, 1);
        errdefer alloc.free(out);
        out[0] = try cloneGraphPath(alloc, result.path);
        break :blk out;
    } else @constCast((&[_]db_mod.types.GraphPath{})[0..]);
    errdefer {
        for (paths) |path| graph_paths_mod.freePath(alloc, path);
        if (paths.len > 0) alloc.free(paths);
    }

    const hits = if (graphResultHydrationRequested(req, graph_query.query))
        try hydrateHitsForResultNodes(alloc, admission, nodes, graph_query.query.include_all_fields, graph_query.query.fields)
    else
        try alloc.alloc(db_mod.types.SearchHit, 0);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }

    return .{
        .name = try alloc.dupe(u8, graph_query.name),
        .nodes = nodes,
        .paths = paths,
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
    admission: *GraphNodeAdmissionContext,
) !db_mod.types.GraphSearchResult {
    const start_nodes = try resolveSelectorNodes(
        alloc,
        table_name,
        req,
        base_result,
        prior_results,
        graph_query.query.start_nodes,
    );
    defer freeNodeIdentities(alloc, start_nodes);
    const target_nodes = try resolveSelectorNodes(
        alloc,
        table_name,
        req,
        base_result,
        prior_results,
        graph_query.query.target_nodes.?,
    );
    defer freeNodeIdentities(alloc, target_nodes);

    if (start_nodes.len != 1 or target_nodes.len != 1)
        return error.UnsupportedQueryRequest;

    var results = std.ArrayListUnmanaged(db_mod.types.GraphPath).empty;
    defer results.deinit(alloc);
    errdefer {
        for (results.items) |path| graph_paths_mod.freePath(alloc, path);
    }

    var seen_paths = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = seen_paths.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen_paths.deinit(alloc);
    }

    const first = try findDistributedShortestPath(alloc, catalog, worker, table_name, req, base_result, prior_results, graph_query, consistency, admission, null, null, null);
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
    const first_path = try cloneGraphPath(alloc, first_result.path);
    results.append(alloc, first_path) catch |err| {
        graph_paths_mod.freePath(alloc, first_path);
        return err;
    };
    const first_key = try graphPathToKey(alloc, first_result.path);
    seen_paths.put(alloc, first_key, {}) catch |err| {
        alloc.free(first_key);
        return err;
    };

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

            var excluded_nodes = graph_node_identity.Map(void){};
            defer excluded_nodes.deinit(alloc);

            for (results.items) |result_path| {
                if (result_path.nodes.len <= spur_idx + 1) continue;
                if (!rootPathMatches(prev_path.*, result_path, spur_idx)) continue;

                const edge_key = try allocEdgeExclusionKey(
                    alloc,
                    graphPathNodeTable(result_path, spur_idx) orelse table_name,
                    result_path.nodes[spur_idx],
                    result_path.nodes[spur_idx + 1],
                    if (result_path.edges.len > spur_idx) result_path.edges[spur_idx].edge_type else "",
                );
                if (!excluded_edges.contains(edge_key)) {
                    excluded_edges.put(alloc, edge_key, {}) catch |err| {
                        alloc.free(edge_key);
                        return err;
                    };
                } else {
                    alloc.free(edge_key);
                }
            }

            for (0..spur_idx) |i| {
                const node_key = prev_path.nodes[i];
                _ = try excluded_nodes.putIfAbsent(
                    alloc,
                    .{
                        .table = graphPathNodeTable(prev_path.*, i),
                        .key = node_key,
                    },
                    {},
                );
            }

            const spur_start = prev_path.nodes[spur_idx];
            const root_prefix = prev_path.nodes[0..spur_idx];
            const root_table_prefix = if (prev_path.node_tables.len == prev_path.nodes.len)
                prev_path.node_tables[0..spur_idx]
            else
                &.{};
            const root_edges = prev_path.edges[0..spur_idx];

            var spur_query = graph_query;
            spur_query.query.start_nodes = .{ .keys = &.{spur_start} };
            spur_query.query.query_type = .shortest_path;
            spur_query.query.k = 1;

            var spur = try findDistributedShortestPath(
                alloc,
                catalog,
                worker,
                table_name,
                req,
                base_result,
                prior_results,
                spur_query,
                consistency,
                admission,
                graphPathNodeTable(prev_path.*, spur_idx),
                &excluded_nodes,
                &excluded_edges,
            );
            defer if (spur) |*result| result.deinit(alloc);
            if (spur == null) continue;

            const total_path = try joinDistributedPaths(
                alloc,
                root_prefix,
                root_table_prefix,
                root_edges,
                spur.?.path,
            );
            var total_path_owned = true;
            errdefer if (total_path_owned) graph_paths_mod.freePath(alloc, total_path);
            const pkey = try graphPathToKey(alloc, total_path);
            if (!seen_paths.contains(pkey)) {
                seen_paths.put(alloc, pkey, {}) catch |err| {
                    alloc.free(pkey);
                    return err;
                };
                candidates.append(alloc, total_path) catch |err| {
                    _ = seen_paths.remove(pkey);
                    alloc.free(pkey);
                    return err;
                };
                total_path_owned = false;
            } else {
                alloc.free(pkey);
                graph_paths_mod.freePath(alloc, total_path);
                total_path_owned = false;
            }
        }

        if (candidates.items.len == 0) break;
        const best_idx = bestPathIndex(candidates.items, graph_query.query.params.weight_mode);
        const best = candidates.swapRemove(best_idx);
        results.append(alloc, best) catch |err| {
            graph_paths_mod.freePath(alloc, best);
            return err;
        };
    }

    const out_nodes = try alloc.alloc(graph_query_mod.GraphResultNode, results.items.len);
    var out_nodes_initialized: usize = 0;
    errdefer {
        for (out_nodes[0..out_nodes_initialized]) |*node| node.deinit(alloc);
        alloc.free(out_nodes);
    }
    for (results.items, 0..) |path, i| {
        out_nodes[i] = try graphPathToResultNode(alloc, path);
        out_nodes_initialized += 1;
    }

    const out_paths = try alloc.alloc(db_mod.types.GraphPath, results.items.len);
    for (results.items, 0..) |path, i| out_paths[i] = path;
    results.clearRetainingCapacity();
    errdefer {
        for (out_paths) |path| graph_paths_mod.freePath(alloc, path);
        alloc.free(out_paths);
    }

    const hits = if (graphResultHydrationRequested(req, graph_query.query))
        try hydrateHitsForResultNodes(alloc, admission, out_nodes, graph_query.query.include_all_fields, graph_query.query.fields)
    else
        try alloc.alloc(db_mod.types.SearchHit, 0);
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
    admission: *GraphNodeAdmissionContext,
    start_table_override: ?[]const u8,
    excluded_nodes: ?*graph_node_identity.Map(void),
    excluded_edges: ?*const std.StringHashMapUnmanaged(void),
) !?ShortestPathResult {
    const algebraic_semiring_selected = graph_query.query.params.algebraic_semiring or
        try catalogGraphIndexEnablesAlgebraicSemiring(alloc, catalog, table_name, graph_query.query.index_name);
    var state = QueryState{ .name = try alloc.dupe(u8, graph_query.name) };
    defer state.deinit(alloc);

    var target_set = try TargetNodeSet.init(
        alloc,
        table_name,
        req,
        base_result,
        prior_results,
        graph_query.query.target_nodes.?,
    );
    defer target_set.deinit(alloc);

    var frontier = std.ArrayListUnmanaged(FrontierState).empty;
    defer {
        for (frontier.items) |*item| item.deinit(alloc);
        frontier.deinit(alloc);
    }
    const roots = try resolveStartFrontier(alloc, &state, table_name, req, base_result, prior_results, graph_query.query.start_nodes, true);
    defer freeFrontier(alloc, roots);
    if (start_table_override) |table| {
        try overrideFrontierTables(alloc, &state, roots, table);
    }
    const admitted_roots = try frontierAdmissionMaskAlloc(alloc, admission, roots);
    defer alloc.free(admitted_roots);

    var best_cost = graph_node_identity.Map(f64){};
    defer best_cost.deinit(alloc);

    for (roots, admitted_roots) |item, allowed| {
        if (!allowed) continue;
        if (excluded_nodes) |set| {
            if (set.contains(.{ .table = item.table, .key = item.key })) continue;
        }
        var owned_item = try initFrontierState(
            alloc,
            item.key,
            item.table,
            item.depth,
            item.distance,
            item.cost,
            item.path_state_id,
        );
        errdefer owned_item.deinit(alloc);
        try frontier.append(alloc, owned_item);
        _ = try best_cost.putIfAbsent(
            alloc,
            .{ .table = item.table, .key = item.key },
            item.cost,
        );
    }

    var settled = graph_node_identity.Map(void){};
    defer settled.deinit(alloc);

    const max_depth = graph_query.query.params.max_depth;
    while (frontier.items.len > 0) {
        const best_index = popBestFrontierIndex(frontier.items);
        var item = frontier.swapRemove(best_index);
        defer item.deinit(alloc);

        const item_ref = graph_node_identity.Ref{ .table = item.table, .key = item.key };
        if (settled.contains(item_ref)) continue;
        if (excluded_nodes) |set| {
            if (set.contains(item_ref) and item.depth > 0) continue;
        }
        _ = try settled.putIfAbsent(alloc, item_ref, {});

        if (target_set.contains(item.table, item.key) and item.depth > 0) {
            const path_state_id = item.path_state_id orelse return error.InvalidQueryRequest;
            const path = try pathStateToGraphPath(alloc, state.path_states.items, path_state_id);
            errdefer graph_paths_mod.freePath(alloc, path);
            return .{ .path = path };
        }

        if (max_depth > 0 and item.depth >= max_depth) continue;

        const expansion_table = item.table orelse table_name;
        const table_state = try admission.ensureTable(expansion_table);
        if (!table_state.allowed) return error.TableNotFound;
        if (!(try admission.graphIndexAvailable(table_state, graph_query.query.index_name))) continue;
        const group_id = (try table_catalog.resolveGroupForKeyPinned(
            alloc,
            catalog,
            expansion_table,
            item.key,
            table_state.topology_epoch,
        )) orelse return error.TableNotFound;
        const frontier_ids = [_]u32{0};
        const exclude_node_refs = try collectCombinedExcludeNodes(alloc, &settled, excluded_nodes);
        defer {
            for (exclude_node_refs) |*identity| identity.deinit(alloc);
            if (exclude_node_refs.len > 0) alloc.free(exclude_node_refs);
        }
        const exclude_edge_keys = try collectExcludedEdgeKeys(alloc, excluded_edges);
        defer freeKeys(alloc, exclude_edge_keys);
        var one_frontier = [_]FrontierState{item};
        var step_req = try makeGraphExpandRequestWithAlgebraicMode(alloc, graph_query, one_frontier[0..], frontier_ids[0..], exclude_node_refs, exclude_edge_keys, true, algebraic_semiring_selected);
        step_req.topology_epoch = table_state.topology_epoch;
        step_req.identity_read_generation = try table_state.generationForGroup(group_id);
        defer step_req.deinit(alloc);

        var step_result = try worker.executeGraphExpand(alloc, group_id, expansion_table, step_req, consistency);
        defer step_result.deinit(alloc);

        if (step_result.expansions.len == 0) continue;
        const step_graph = step_result.expansions[0].graph_result;
        const admitted_nodes = try graphResultNodeAdmissionMaskAlloc(
            alloc,
            admission,
            table_name,
            expansion_table,
            step_graph.nodes,
        );
        defer alloc.free(admitted_nodes);
        for (step_graph.nodes, admitted_nodes) |node, allowed| {
            if (!allowed) continue;
            const node_table = canonicalExpandedNodeTable(
                table_name,
                expansion_table,
                node.table,
            );
            const node_ref = graph_node_identity.Ref{ .table = node_table, .key = node.key };
            if (settled.contains(node_ref)) continue;
            if (excluded_nodes) |set| {
                if (set.contains(node_ref)) continue;
            }

            const candidate_cost = item.cost + edgeCost(
                item,
                node,
                graph_query.query.params.weight_mode,
            );
            if (best_cost.get(node_ref)) |cost| {
                if (candidate_cost >= cost) continue;
            }
            const path_state_id = try appendPathStateFromWeightedStep(
                alloc,
                &state,
                item,
                node,
                node_table,
                graph_query.query.params.weight_mode,
            );
            const path_state = state.path_states.items[path_state_id];
            if (best_cost.getPtr(node_ref)) |cost| {
                cost.* = path_state.cost;
            } else {
                _ = try best_cost.putIfAbsent(alloc, node_ref, path_state.cost);
            }

            var next_item = try initFrontierState(
                alloc,
                path_state.key,
                path_state.table,
                path_state.depth,
                path_state.distance,
                path_state.cost,
                path_state_id,
            );
            errdefer next_item.deinit(alloc);
            try frontier.append(alloc, next_item);
        }
    }

    return null;
}

fn supportsSelectorRef(req: db_mod.types.SearchRequest, selector: graph_query_mod.NodeSelector) bool {
    return switch (selector) {
        .keys => true,
        .identities => true,
        .result_ref => |result_ref| supportsResultRef(req, result_ref.ref),
    };
}

fn supportsResultRef(req: db_mod.types.SearchRequest, ref: []const u8) bool {
    _ = req;
    if (std.mem.startsWith(u8, ref, "$graph_results.")) return true;
    return std.mem.eql(u8, ref, "$query_results");
}

fn batchFrontierByGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    source_table: []const u8,
    frontier: []const FrontierState,
    max_depth: u32,
    admission: *GraphNodeAdmissionContext,
    index_name: []const u8,
) !GraphExpandBatches {
    var batches = GraphExpandBatches.empty;
    errdefer freeFrontierBatches(alloc, &batches);

    for (frontier, 0..) |item, i| {
        if (item.depth >= max_depth) continue;
        const table_name = item.table orelse source_table;
        const table_state = try admission.ensureTable(table_name);
        if (!table_state.allowed) return error.TableNotFound;
        if (!(try admission.graphIndexAvailable(table_state, index_name))) continue;
        const group_id = (try table_catalog.resolveGroupForKeyPinned(
            alloc,
            catalog,
            table_name,
            item.key,
            table_state.topology_epoch,
        )) orelse return error.TableNotFound;
        const gop = try batches.getOrPut(alloc, .{
            .table_name = table_state.table_name,
            .group_id = group_id,
        });
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .topology_epoch = table_state.topology_epoch,
                .identity_read_generation = try table_state.generationForGroup(group_id),
            };
        } else if (gop.value_ptr.topology_epoch != table_state.topology_epoch or
            gop.value_ptr.identity_read_generation != try table_state.generationForGroup(group_id))
        {
            return error.InvalidQueryResult;
        }
        try gop.value_ptr.frontier_ids.append(alloc, @intCast(i));
    }
    return batches;
}

fn freeFrontierBatches(
    alloc: std.mem.Allocator,
    batches: *GraphExpandBatches,
) void {
    var it = batches.iterator();
    while (it.next()) |entry| entry.value_ptr.frontier_ids.deinit(alloc);
    batches.deinit(alloc);
    batches.* = .empty;
}

fn collectGraphExpandBatchEntries(
    alloc: std.mem.Allocator,
    batches: *const GraphExpandBatches,
) ![]GraphExpandBatchEntry {
    const entries = try alloc.alloc(GraphExpandBatchEntry, batches.count());
    var i: usize = 0;
    var it = batches.iterator();
    while (it.next()) |entry| {
        entries[i] = .{
            .table_name = entry.key_ptr.table_name,
            .group_id = entry.key_ptr.group_id,
            .topology_epoch = entry.value_ptr.topology_epoch,
            .identity_read_generation = entry.value_ptr.identity_read_generation,
            .frontier_ids = entry.value_ptr.frontier_ids.items,
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
    graph_query: db_mod.types.NamedGraphQuery,
    frontier: []const FrontierState,
    entries: []const GraphExpandBatchEntry,
    exclude_nodes: []GraphNodeIdentity,
    include_paths: bool,
    algebraic_semiring_selected: bool,
    consistency: raft_mod.ReadConsistency,
) ![]GraphExpandFanoutSlot {
    const slots = try initGraphExpandFanoutSlots(alloc, entries.len);
    errdefer deinitGraphExpandFanoutSlots(alloc, slots);

    const Fiber = struct {
        fn run(
            worker_inner: Worker,
            slot: *GraphExpandFanoutSlot,
            graph_query_inner: db_mod.types.NamedGraphQuery,
            frontier_inner: []const FrontierState,
            entry: GraphExpandBatchEntry,
            exclude_nodes_inner: []GraphNodeIdentity,
            include_paths_inner: bool,
            algebraic_semiring_selected_inner: bool,
            consistency_inner: raft_mod.ReadConsistency,
        ) void {
            const arena = slot.arena.allocator();
            var step_req = makeGraphExpandRequestWithAlgebraicMode(
                arena,
                graph_query_inner,
                frontier_inner,
                entry.frontier_ids,
                exclude_nodes_inner,
                @constCast((&[_][]u8{})[0..]),
                include_paths_inner,
                algebraic_semiring_selected_inner,
            ) catch |err| {
                slot.err = err;
                return;
            };
            step_req.topology_epoch = entry.topology_epoch;
            step_req.identity_read_generation = entry.identity_read_generation;
            slot.result = worker_inner.executeGraphExpand(arena, entry.group_id, entry.table_name, step_req, consistency_inner) catch |err| {
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
                graph_query,
                frontier,
                entry,
                exclude_nodes,
                include_paths,
                algebraic_semiring_selected,
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

fn collectSeenNodes(
    alloc: std.mem.Allocator,
    seen: *graph_node_identity.Map(void),
) ![]GraphNodeIdentity {
    const out = try alloc.alloc(GraphNodeIdentity, seen.count());
    var i: usize = 0;
    errdefer {
        for (out[0..i]) |*identity| identity.deinit(alloc);
        alloc.free(out);
    }
    var it = seen.iterator();
    while (it.next()) |key| {
        out[i] = try cloneGraphNodeIdentityParts(
            alloc,
            key.key_ptr.key(),
            key.key_ptr.table(),
        );
        i += 1;
    }
    return out;
}

fn collectCombinedExcludeNodes(
    alloc: std.mem.Allocator,
    settled: *graph_node_identity.Map(void),
    excluded_nodes: ?*graph_node_identity.Map(void),
) ![]GraphNodeIdentity {
    const extra_count = if (excluded_nodes) |set| set.count() else 0;
    const out = try alloc.alloc(GraphNodeIdentity, settled.count() + extra_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*identity| identity.deinit(alloc);
        alloc.free(out);
    }

    var settled_it = settled.iterator();
    while (settled_it.next()) |entry| {
        out[initialized] = try cloneGraphNodeIdentityParts(
            alloc,
            entry.key_ptr.key(),
            entry.key_ptr.table(),
        );
        initialized += 1;
    }
    if (excluded_nodes) |set| {
        var node_it = set.iterator();
        while (node_it.next()) |entry| {
            out[initialized] = try cloneGraphNodeIdentityParts(
                alloc,
                entry.key_ptr.key(),
                entry.key_ptr.table(),
            );
            initialized += 1;
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
    errdefer {
        for (new_hits) |*hit| hit.deinit(alloc);
        if (new_hits.len > 0) alloc.free(new_hits);
    }
    try out.appendSlice(alloc, new_hits);
    if (new_hits.len > 0) alloc.free(new_hits);
    return out;
}

fn graphResultHydrationRequested(
    req: db_mod.types.SearchRequest,
    query: graph_query_mod.GraphQuery,
) bool {
    // Expansion consumes hydrated graph hits internally. Otherwise the public
    // include_documents flag is the sole authority for stored-data reads and
    // response documents.
    return query.include_documents or req.expand_strategy != null;
}

fn hydrateHitsForResultNodes(
    alloc: std.mem.Allocator,
    admission: *GraphNodeAdmissionContext,
    nodes: []const graph_query_mod.GraphResultNode,
    include_all_fields: bool,
    fields: []const []const u8,
) ![]db_mod.types.SearchHit {
    // Most nodes are hydrated from the query table. Mention/DocRef edges carry a
    // cross-table endpoint (`node.table`, e.g. the canonical "entities" table)
    // that must be routed and hydrated against *that* table's shard topology,
    // not the query table's. Partition by effective table; the single-table
    // common case keeps the original fast path untouched.
    var needs_cross_table = false;
    for (nodes) |node| {
        if (node.table) |t| {
            if (!std.mem.eql(u8, t, admission.source_table)) {
                needs_cross_table = true;
                break;
            }
        }
    }

    if (!needs_cross_table) {
        var keys = try alloc.alloc([]const u8, nodes.len);
        defer alloc.free(keys);
        for (nodes, 0..) |node, i| keys[i] = node.key;
        const state = try admission.ensureTable(admission.source_table);
        return try hydrateHitsForKeys(
            alloc,
            admission.catalog,
            admission.worker,
            state.table_name,
            state.topology_epoch,
            state.identity_read_generation,
            state.identity_read_generations,
            state.filter_query_json,
            state.exclusion_query_json,
            state.resolved_doc_filter,
            state.resolved_doc_filter_wire_context,
            true,
            include_all_fields,
            fields,
            keys,
            admission.consistency,
        );
    }

    // Bucket node keys by their effective hydration table.
    var tables = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged([]const u8)).empty;
    defer {
        for (tables.values()) |*list| list.deinit(alloc);
        tables.deinit(alloc);
    }
    for (nodes) |node| {
        const eff_table = node.table orelse admission.source_table;
        const gop = try tables.getOrPut(alloc, eff_table);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(alloc, node.key);
    }

    const HydratedBucket = struct {
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
        const same_table = std.mem.eql(u8, eff_table, admission.source_table);
        // A cross-table endpoint whose table no longer exists fails closed (no
        // hit), matching the same-table path's behavior for a missing key,
        // rather than erroring the whole graph query.
        const state = try admission.ensureTable(eff_table);
        if (!state.allowed) continue;
        const hits = try hydrateHitsForKeys(
            alloc,
            admission.catalog,
            admission.worker,
            state.table_name,
            state.topology_epoch,
            state.identity_read_generation,
            state.identity_read_generations,
            state.filter_query_json,
            state.exclusion_query_json,
            state.resolved_doc_filter,
            state.resolved_doc_filter_wire_context,
            true,
            include_all_fields,
            fields,
            entry.value_ptr.items,
            admission.consistency,
        );
        errdefer {
            for (hits) |*hit| hit.deinit(alloc);
            if (hits.len > 0) alloc.free(hits);
        }
        if (!same_table) {
            // Ordinals are scoped to the hydrated table's identity namespace.
            // Keep internal table provenance so equal keys from different
            // tables cannot be associated with the wrong graph node.
            for (hits) |*hit| {
                hit.doc_ordinal = null;
                const source_table = try alloc.dupe(u8, eff_table);
                if (hit.source_table) |old| alloc.free(old);
                hit.source_table = source_table;
            }
        }
        try hydrated.append(alloc, .{ .hits = hits });
    }

    var hit_count: usize = 0;
    for (hydrated.items) |bucket| {
        hit_count = std.math.add(usize, hit_count, bucket.hits.len) catch
            return error.InvalidQueryResult;
    }
    const out = try alloc.alloc(db_mod.types.SearchHit, hit_count);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (hydrated.items) |*bucket| {
        for (bucket.hits) |hit| {
            out[initialized] = hit;
            initialized += 1;
        }
        if (bucket.hits.len > 0) alloc.free(bucket.hits);
        bucket.hits = &.{};
    }
    return out;
}

fn hydrateHitsForKeys(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    identity_read_generations: []const db_mod.types.ShardIdentityReadGeneration,
    filter_query_json: []const u8,
    exclusion_query_json: []const u8,
    resolved_doc_filter: ?*const anyopaque,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext,
    include_stored: bool,
    include_all_fields: bool,
    fields: []const []const u8,
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
        gop.key_ptr.* = alloc.dupe(u8, key) catch |err| {
            _ = unique.remove(key);
            return err;
        };
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
                req.identity_read_generation = try identityReadGenerationForGroup(
                    identity_read_generation,
                    identity_read_generations,
                    entry.key_ptr.*,
                );
                req.filter_query_json = filter_query_json;
                req.exclusion_query_json = exclusion_query_json;
                req.include_stored = include_stored;
                req.include_all_fields = include_all_fields;
                req.fields = fields;
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
                .identity_read_generation = try identityReadGenerationForGroup(
                    identity_read_generation,
                    identity_read_generations,
                    entry.key_ptr.*,
                ),
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
                filter_query_json_inner: []const u8,
                exclusion_query_json_inner: []const u8,
                resolved_doc_filter_inner: ?*const anyopaque,
                resolved_doc_filter_wire_context_inner: ?db_mod.types.ResolvedDocFilterWireContext,
                include_stored_inner: bool,
                include_all_fields_inner: bool,
                fields_inner: []const []const u8,
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
                req.identity_read_generation = entry.identity_read_generation;
                req.filter_query_json = filter_query_json_inner;
                req.exclusion_query_json = exclusion_query_json_inner;
                req.include_stored = include_stored_inner;
                req.include_all_fields = include_all_fields_inner;
                req.fields = fields_inner;
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
                group.async(io, Fiber.run, .{ worker, &slots[i], table_name, entry, topology_epoch, filter_query_json, exclusion_query_json, resolved_doc_filter, resolved_doc_filter_wire_context, include_stored, include_all_fields, fields, consistency });
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
        req.identity_read_generation = try identityReadGenerationForGroup(
            identity_read_generation,
            identity_read_generations,
            entry.key_ptr.*,
        );
        req.filter_query_json = filter_query_json;
        req.exclusion_query_json = exclusion_query_json;
        req.include_stored = include_stored;
        req.include_all_fields = include_all_fields;
        req.fields = fields;
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

/// Probe graph in-degree at the owner of each key. Keys are partitioned by a
/// topology epoch and each shard receives one batched reverse-index request.
pub fn probeIncomingEdgesForKeys(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: Worker,
    table_name: []const u8,
    topology_epoch: u64,
    identity_read_generation: ?u64,
    index_name: []const u8,
    keys: []const []const u8,
    consistency: raft_mod.ReadConsistency,
) ![]bool {
    const result = try alloc.alloc(bool, keys.len);
    errdefer alloc.free(result);
    @memset(result, false);
    if (keys.len == 0) return result;

    const Batch = struct {
        keys: std.ArrayListUnmanaged([]const u8) = .empty,
        indexes: std.ArrayListUnmanaged(usize) = .empty,

        fn deinit(self: *@This(), a: std.mem.Allocator) void {
            self.keys.deinit(a);
            self.indexes.deinit(a);
        }
    };
    var batches = std.AutoHashMapUnmanaged(u64, Batch).empty;
    defer {
        var it = batches.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        batches.deinit(alloc);
    }
    for (keys, 0..) |key, key_index| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(
            alloc,
            catalog,
            table_name,
            key,
            topology_epoch,
        )) orelse return error.TableNotFound;
        const batch = try batches.getOrPut(alloc, group_id);
        if (!batch.found_existing) batch.value_ptr.* = .{};
        try batch.value_ptr.keys.append(alloc, key);
        try batch.value_ptr.indexes.append(alloc, key_index);
    }

    const Entry = struct {
        group_id: u64,
        keys: []const []const u8,
        indexes: []const usize,
    };
    const entries = try alloc.alloc(Entry, batches.count());
    defer alloc.free(entries);
    var batch_it = batches.iterator();
    var entry_index: usize = 0;
    while (batch_it.next()) |entry| {
        entries[entry_index] = .{
            .group_id = entry.key_ptr.*,
            .keys = entry.value_ptr.keys.items,
            .indexes = entry.value_ptr.indexes.items,
        };
        entry_index += 1;
    }

    const Probe = struct {
        fn run(
            a: std.mem.Allocator,
            worker_inner: Worker,
            table_name_inner: []const u8,
            topology_epoch_inner: u64,
            identity_generation_inner: ?u64,
            index_name_inner: []const u8,
            entry: Entry,
            consistency_inner: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            var req = GraphHydrateRequest{
                .keys = try dupKeys(a, entry.keys),
                .topology_epoch = topology_epoch_inner,
                .identity_read_generation = identity_generation_inner,
                .include_stored = false,
                .include_hits = false,
                .incoming_index_name = index_name_inner,
            };
            defer req.deinit(a);
            return try worker_inner.executeGraphHydrate(
                a,
                entry.group_id,
                table_name_inner,
                req,
                consistency_inner,
            );
        }

        fn copy(mask: []const bool, entry: Entry, output: []bool) !void {
            if (mask.len != entry.indexes.len) return error.InvalidGraphNodeAdmissionResult;
            for (entry.indexes, mask) |output_index, has_incoming| {
                output[output_index] = has_incoming;
            }
        }
    };

    const fanout_io = worker.fanoutIo();
    const plan = planGraphFanout(fanout_io != null, worker.fanoutWidthCap(), entries.len);
    recordGraphFanoutPlan(.hydrate, plan);
    if (fanout_io) |io| {
        if (plan.parallel) {
            const start_ns = platform_time.monotonicNs();
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
                    topology_epoch_inner: u64,
                    identity_generation_inner: ?u64,
                    index_name_inner: []const u8,
                    entry: Entry,
                    consistency_inner: raft_mod.ReadConsistency,
                ) void {
                    slot.result = Probe.run(
                        slot.arena.allocator(),
                        worker_inner,
                        table_name_inner,
                        topology_epoch_inner,
                        identity_generation_inner,
                        index_name_inner,
                        entry,
                        consistency_inner,
                    ) catch |err| {
                        slot.err = err;
                        return;
                    };
                }
            };

            var start: usize = 0;
            while (start < entries.len) : (start += plan.width) {
                const end = @min(start + plan.width, entries.len);
                var group: std.Io.Group = .init;
                for (entries[start..end], start..end) |entry, i| {
                    group.async(io, Fiber.run, .{
                        worker,
                        &slots[i],
                        table_name,
                        topology_epoch,
                        identity_read_generation,
                        index_name,
                        entry,
                        consistency,
                    });
                }
                group.await(io) catch {};
            }
            recordGraphParallelFanout(.hydrate, @intCast(platform_time.monotonicNs() - start_ns));
            for (slots) |slot| {
                if (slot.err) |err| return err;
            }
            for (slots, entries) |slot, entry| {
                try Probe.copy(slot.result.?.has_incoming, entry, result);
            }
            return result;
        }
    }

    for (entries) |entry| {
        var response = try Probe.run(
            alloc,
            worker,
            table_name,
            topology_epoch,
            identity_read_generation,
            index_name,
            entry,
            consistency,
        );
        defer response.deinit(alloc);
        try Probe.copy(response.has_incoming, entry, result);
    }
    return result;
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
        ._filter_query_json = req.filter_query_json,
        ._exclusion_query_json = req.exclusion_query_json,
        .include_stored = req.include_stored,
        .fields = req.fields,
        .include_all_fields = req.include_all_fields,
        .include_hits = req.include_hits,
        .incoming_index_name = req.incoming_index_name,
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

    const keys = try dupKeys(alloc, parsed.value.keys);
    errdefer freeKeys(alloc, keys);
    const filter_query_json = if (parsed.value._filter_query_json.len > 0)
        try alloc.dupe(u8, parsed.value._filter_query_json)
    else
        &.{};
    errdefer if (filter_query_json.len > 0) alloc.free(filter_query_json);
    const exclusion_query_json = if (parsed.value._exclusion_query_json.len > 0)
        try alloc.dupe(u8, parsed.value._exclusion_query_json)
    else
        &.{};
    errdefer if (exclusion_query_json.len > 0) alloc.free(exclusion_query_json);
    const incoming_index_name = if (parsed.value.incoming_index_name.len > 0)
        try alloc.dupe(u8, parsed.value.incoming_index_name)
    else
        &.{};
    errdefer if (incoming_index_name.len > 0) alloc.free(incoming_index_name);
    const fields = try dupConstStrings(alloc, parsed.value.fields);
    errdefer freeConstStrings(alloc, fields);

    const out = GraphHydrateRequest{
        .keys = keys,
        .topology_epoch = parsed.value.topology_epoch,
        .identity_read_generation = identity_read_generation,
        .filter_query_json = filter_query_json,
        .filter_query_json_owned = filter_query_json.len > 0,
        .exclusion_query_json = exclusion_query_json,
        .exclusion_query_json_owned = exclusion_query_json.len > 0,
        .include_stored = parsed.value.include_stored,
        .fields = fields,
        .fields_owned = fields.len > 0,
        .include_all_fields = parsed.value.include_all_fields,
        .include_hits = parsed.value.include_hits,
        .incoming_index_name = incoming_index_name,
        .incoming_index_name_owned = incoming_index_name.len > 0,
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
        .has_incoming = res.has_incoming,
    });
}

pub fn parseGraphHydrateResponse(alloc: std.mem.Allocator, body: []const u8) !GraphHydrateResponse {
    var parsed = try std.json.parseFromSlice(GraphHydrateResponseJson, alloc, body, .{});
    defer parsed.deinit();
    const hits = if (parsed.value.hits.len > 0)
        try cloneSearchHits(alloc, parsed.value.hits)
    else
        @constCast((&[_]db_mod.types.SearchHit{})[0..]);
    errdefer {
        for (hits) |*hit| hit.deinit(alloc);
        if (hits.len > 0) alloc.free(hits);
    }
    return .{
        .hits = hits,
        .has_incoming = if (parsed.value.has_incoming.len > 0)
            try alloc.dupe(bool, parsed.value.has_incoming)
        else
            @constCast((&[_]bool{})[0..]),
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
        .edge_types = req.edge_types,
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
        .edge_types = try dupConstStrings(alloc, parsed.value.edge_types),
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
    const target_table = if (path.nodes.len > 0 and path.node_tables.len == path.nodes.len)
        path.node_tables[path.node_tables.len - 1]
    else if (path.edges.len > 0 and
        std.mem.eql(u8, path.edges[path.edges.len - 1].target, target_key))
        graph_traversal_mod.metadataTargetTable(path.edges[path.edges.len - 1].metadata)
    else
        null;
    const nodes = try dupPath(alloc, path.nodes);
    errdefer freePathArray(alloc, nodes);
    const node_tables = if (path.node_tables.len > 0)
        try dupOptionalStrings(alloc, path.node_tables)
    else
        null;
    errdefer if (node_tables) |value| freeOptionalStrings(alloc, value);
    const edges = if (path.edges.len > 0)
        try dupPathEdgesFromGraphPath(alloc, path.edges)
    else
        null;
    errdefer if (edges) |value| freePathEdges(alloc, value);

    return try initGraphResultNode(
        alloc,
        target_key,
        target_table,
        path.length,
        path.total_weight,
        nodes,
        node_tables,
        edges,
    );
}

fn pathStateToGraphPath(
    alloc: std.mem.Allocator,
    path_states: []const PathState,
    path_state_id: u32,
) !db_mod.types.GraphPath {
    const nodes = try reconstructPath(alloc, path_states, path_state_id);
    errdefer freePathArray(alloc, nodes);
    const node_tables = try reconstructPathTables(alloc, path_states, path_state_id);
    errdefer freeOptionalStrings(alloc, node_tables);
    const edges_info = try reconstructPathEdges(alloc, path_states, path_state_id);
    errdefer freePathEdges(alloc, edges_info);

    const edges = try alloc.alloc(graph_paths_mod.PathEdge, edges_info.len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |edge| freeOwnedGraphPathEdge(alloc, edge);
        alloc.free(edges);
    }
    for (edges_info, 0..) |edge, i| {
        edges[i] = try dupeGraphPathEdge(alloc, edge);
        initialized += 1;
    }
    freePathEdges(alloc, edges_info);

    return .{
        .nodes = nodes,
        .node_tables = node_tables,
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
    const node_tables = try dupOptionalStrings(alloc, source.node_tables);
    errdefer freeOptionalStrings(alloc, node_tables);
    const edges = try alloc.alloc(graph_paths_mod.PathEdge, source.edges.len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |edge| freeOwnedGraphPathEdge(alloc, edge);
        alloc.free(edges);
    }
    for (source.edges, 0..) |edge, i| {
        edges[i] = try dupeGraphPathEdge(alloc, edge);
        initialized += 1;
    }
    return .{
        .nodes = nodes,
        .node_tables = node_tables,
        .edges = edges,
        .total_weight = source.total_weight,
        .length = source.length,
    };
}

fn dupeGraphPathEdge(
    alloc: std.mem.Allocator,
    edge: anytype,
) !graph_paths_mod.PathEdge {
    const source = try alloc.dupe(u8, edge.source);
    errdefer alloc.free(source);
    const target = try alloc.dupe(u8, edge.target);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, edge.edge_type);
    errdefer alloc.free(edge_type);
    const metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "";
    errdefer if (metadata.len > 0) alloc.free(metadata);
    return .{
        .source = source,
        .target = target,
        .edge_type = edge_type,
        .weight = edge.weight,
        .metadata = metadata,
    };
}

fn freeOwnedGraphPathEdge(
    alloc: std.mem.Allocator,
    edge: graph_paths_mod.PathEdge,
) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
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
        out[i] = try clonePatternPathEdge(alloc, edge);
        initialized += 1;
    }
    return out;
}

fn graphPathToKey(alloc: std.mem.Allocator, path: db_mod.types.GraphPath) ![]u8 {
    var total_len: usize = 0;
    for (path.nodes, 0..) |node, i| {
        const table = graphPathNodeTable(path, i);
        total_len = std.math.add(usize, total_len, 1 + @sizeOf(u64)) catch
            return error.PathIdentityTooLarge;
        if (table) |value| {
            total_len = std.math.add(usize, total_len, value.len) catch
                return error.PathIdentityTooLarge;
        }
        total_len = std.math.add(usize, total_len, @sizeOf(u64)) catch
            return error.PathIdentityTooLarge;
        total_len = std.math.add(usize, total_len, node.len) catch
            return error.PathIdentityTooLarge;
    }

    const out = try alloc.alloc(u8, total_len);
    var pos: usize = 0;
    for (path.nodes, 0..) |node, i| {
        const table = graphPathNodeTable(path, i);
        out[pos] = if (table == null) 0 else 1;
        pos += 1;
        const table_len: u64 = if (table) |value| @intCast(value.len) else 0;
        std.mem.writeInt(u64, out[pos..][0..8], table_len, .little);
        pos += 8;
        if (table) |value| {
            @memcpy(out[pos..][0..value.len], value);
            pos += value.len;
        }
        std.mem.writeInt(u64, out[pos..][0..8], @intCast(node.len), .little);
        pos += 8;
        @memcpy(out[pos..][0..node.len], node);
        pos += node.len;
    }
    return out;
}

fn rootPathMatches(a: db_mod.types.GraphPath, b: db_mod.types.GraphPath, spur_idx: usize) bool {
    if (a.nodes.len <= spur_idx or b.nodes.len <= spur_idx) return false;
    var i: usize = 0;
    while (i <= spur_idx) : (i += 1) {
        if (!std.mem.eql(u8, a.nodes[i], b.nodes[i])) return false;
        if (!optionalGraphTableEql(
            graphPathNodeTable(a, i),
            graphPathNodeTable(b, i),
        )) return false;
    }
    return true;
}

fn joinDistributedPaths(
    alloc: std.mem.Allocator,
    root_nodes: []const []const u8,
    root_node_tables: []const ?[]const u8,
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

    const has_root_tables = root_node_tables.len == root_nodes.len and
        optionalStringsContainValue(root_node_tables);
    const has_spur_tables = spur_path.node_tables.len == spur_path.nodes.len and
        optionalStringsContainValue(spur_path.node_tables);
    var node_tables: []?[]const u8 = &.{};
    var tables_initialized: usize = 0;
    errdefer {
        for (node_tables[0..tables_initialized]) |table| {
            if (table) |value| alloc.free(value);
        }
        if (node_tables.len > 0) alloc.free(node_tables);
    }
    if (has_root_tables or has_spur_tables) {
        node_tables = try alloc.alloc(?[]const u8, node_count);
        for (root_nodes, 0..) |_, i| {
            const table = if (root_node_tables.len == root_nodes.len)
                root_node_tables[i]
            else
                null;
            node_tables[i] = if (table) |value| try alloc.dupe(u8, value) else null;
            tables_initialized += 1;
        }
        for (spur_path.nodes, 0..) |_, i| {
            const table = graphPathNodeTable(spur_path, i);
            node_tables[root_nodes.len + i] = if (table) |value|
                try alloc.dupe(u8, value)
            else
                null;
            tables_initialized += 1;
        }
    }

    const edge_count = root_edges.len + spur_path.edges.len;
    const edges = try alloc.alloc(graph_paths_mod.PathEdge, edge_count);
    var edge_initialized: usize = 0;
    errdefer {
        for (edges[0..edge_initialized]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.metadata.len > 0) alloc.free(edge.metadata);
        }
        alloc.free(edges);
    }
    for (root_edges, 0..) |edge, i| {
        edges[i] = try dupeGraphPathEdge(alloc, edge);
        edge_initialized += 1;
    }
    for (spur_path.edges, 0..) |edge, i| {
        edges[root_edges.len + i] = try dupeGraphPathEdge(alloc, edge);
        edge_initialized += 1;
    }

    return .{
        .nodes = nodes,
        .node_tables = node_tables,
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
    table: []const u8,
    src: []const u8,
    tgt: []const u8,
    edge_type: []const u8,
) ![]u8 {
    return try compositeIdentityAlloc(alloc, &.{ table, src, tgt, edge_type });
}

fn compositeIdentityAlloc(
    alloc: std.mem.Allocator,
    parts: []const []const u8,
) ![]u8 {
    var encoded_len: usize = 0;
    for (parts) |part| {
        encoded_len = std.math.add(usize, encoded_len, @sizeOf(u64)) catch
            return error.GraphIdentityTooLarge;
        encoded_len = std.math.add(usize, encoded_len, part.len) catch
            return error.GraphIdentityTooLarge;
    }

    const encoded = try alloc.alloc(u8, encoded_len);
    var cursor: usize = 0;
    for (parts) |part| {
        std.mem.writeInt(u64, encoded[cursor..][0..8], @intCast(part.len), .little);
        cursor += 8;
        @memcpy(encoded[cursor..][0..part.len], part);
        cursor += part.len;
    }
    return encoded;
}

test "distributed graph identities are length framed" {
    const alloc = std.testing.allocator;
    const path_a = try compositeIdentityAlloc(alloc, &.{ "A", "B->C", "D" });
    defer alloc.free(path_a);
    const path_b = try compositeIdentityAlloc(alloc, &.{ "A", "B", "C", "D" });
    defer alloc.free(path_b);
    try std.testing.expect(!std.mem.eql(u8, path_a, path_b));

    const edge_a = try compositeIdentityAlloc(alloc, &.{ "A->B", "C", "kind" });
    defer alloc.free(edge_a);
    const edge_b = try compositeIdentityAlloc(alloc, &.{ "A", "B->C", "kind" });
    defer alloc.free(edge_b);
    try std.testing.expect(!std.mem.eql(u8, edge_a, edge_b));
}

test "distributed shortest path identity and endpoint retain table provenance" {
    const alloc = std.testing.allocator;
    var nodes = [_][]const u8{ "root", "same" };
    var source_tables = [_]?[]const u8{ null, null };
    var entity_tables = [_]?[]const u8{ null, "entities" };
    const source_path = db_mod.types.GraphPath{
        .nodes = nodes[0..],
        .node_tables = source_tables[0..],
        .edges = @constCast((&[_]graph_paths_mod.PathEdge{})[0..]),
        .total_weight = 1,
        .length = 1,
    };
    const entity_path = db_mod.types.GraphPath{
        .nodes = nodes[0..],
        .node_tables = entity_tables[0..],
        .edges = @constCast((&[_]graph_paths_mod.PathEdge{})[0..]),
        .total_weight = 1,
        .length = 1,
    };

    const source_key = try graphPathToKey(alloc, source_path);
    defer alloc.free(source_key);
    const entity_key = try graphPathToKey(alloc, entity_path);
    defer alloc.free(entity_key);
    try std.testing.expect(!std.mem.eql(u8, source_key, entity_key));

    var result_node = try graphPathToResultNode(alloc, entity_path);
    defer result_node.deinit(alloc);
    try std.testing.expectEqualStrings("same", result_node.key);
    try std.testing.expectEqualStrings("entities", result_node.table.?);
}

test "distributed graph result selectors retain root table provenance" {
    const alloc = std.testing.allocator;
    var prior_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "shared",
        .table = "entities",
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
    }};
    var prior_results = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("prior"),
        .nodes = prior_nodes[0..],
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    }};
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
    };
    var state = QueryState{ .name = try alloc.dupe(u8, "next") };
    defer state.deinit(alloc);

    const frontier = try resolveStartFrontier(
        alloc,
        &state,
        "docs",
        .{ .identity_read_generation = 7 },
        base_result,
        prior_results[0..],
        .{ .result_ref = .{ .ref = "$graph_results.prior" } },
        true,
    );
    defer freeFrontier(alloc, frontier);

    try std.testing.expectEqual(@as(usize, 1), frontier.len);
    try std.testing.expectEqualStrings("shared", frontier[0].key);
    try std.testing.expectEqualStrings("entities", frontier[0].table.?);
    const root_state = state.path_states.items[frontier[0].path_state_id.?];
    try std.testing.expectEqualStrings("entities", root_state.table.?);
}

test "distributed graph target refs are table exact while raw keys remain wildcard" {
    const alloc = std.testing.allocator;
    var prior_nodes = [_]graph_query_mod.GraphResultNode{.{
        .key = "shared",
        .table = "entities",
        .depth = 1,
        .distance = 1,
        .path = null,
        .path_edges = null,
    }};
    var prior_results = [_]db_mod.types.GraphSearchResult{.{
        .name = @constCast("prior"),
        .nodes = prior_nodes[0..],
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 1,
    }};
    const req = db_mod.types.SearchRequest{ .identity_read_generation = 7 };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
    };

    var exact = try TargetNodeSet.init(
        alloc,
        "docs",
        req,
        base_result,
        prior_results[0..],
        .{ .result_ref = .{ .ref = "$graph_results.prior" } },
    );
    defer exact.deinit(alloc);
    try std.testing.expect(exact.contains("entities", "shared"));
    try std.testing.expect(!exact.contains(null, "shared"));

    var wildcard = try TargetNodeSet.init(
        alloc,
        "docs",
        req,
        base_result,
        prior_results[0..],
        .{ .keys = &.{"shared"} },
    );
    defer wildcard.deinit(alloc);
    try std.testing.expect(wildcard.contains(null, "shared"));
    try std.testing.expect(wildcard.contains("entities", "shared"));

    var same_table = try TargetNodeSet.init(
        alloc,
        "docs",
        req,
        base_result,
        &.{},
        .{ .identities = &.{.{ .key = "doc:b", .table = "docs" }} },
    );
    defer same_table.deinit(alloc);
    try std.testing.expect(same_table.contains(null, "doc:b"));
    try std.testing.expect(!same_table.contains("docs", "doc:b"));
}

test "distributed graph MATCH binding refs preserve table identity and deduplicate" {
    const alloc = std.testing.allocator;
    var first_bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("post"),
        .node = .{ .key = "post:1", .table = "docs", .depth = 0, .distance = 0, .path = null, .path_edges = null },
    }};
    var second_bindings = [_]db_mod.types.GraphPatternBinding{.{
        .alias = @constCast("post"),
        .node = .{ .key = "post:1", .table = "docs", .depth = 0, .distance = 0, .path = null, .path_edges = null },
    }};
    const matches = [_]db_mod.types.GraphPatternMatch{
        .{ .bindings = &first_bindings, .path = &.{} },
        .{ .bindings = &second_bindings, .path = &.{} },
    };
    const nodes = try resolveMatchBindingNodes(alloc, "docs", &matches, "post", 0);
    defer freeNodeIdentities(alloc, nodes);
    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(nodes[0].table == null);
    try std.testing.expectEqualStrings("post:1", nodes[0].key);
}

test "distributed graph node refs deduplicate typed identities before limit" {
    const alloc = std.testing.allocator;
    const result_nodes = [_]graph_query_mod.GraphResultNode{
        .{ .key = "shared", .table = "docs", .depth = 0, .distance = 0, .path = null, .path_edges = null },
        .{ .key = "shared", .table = "docs", .depth = 1, .distance = 1, .path = null, .path_edges = null },
        .{ .key = "shared", .table = "people", .depth = 1, .distance = 1, .path = null, .path_edges = null },
        .{ .key = "other", .table = "docs", .depth = 1, .distance = 1, .path = null, .path_edges = null },
    };
    const nodes = try resolveUniqueGraphResultNodes(alloc, "docs", &result_nodes, 3);
    defer freeNodeIdentities(alloc, nodes);

    try std.testing.expectEqual(@as(usize, 3), nodes.len);
    try std.testing.expect(nodes[0].table == null);
    try std.testing.expectEqualStrings("shared", nodes[0].key);
    try std.testing.expectEqualStrings("people", nodes[1].table.?);
    try std.testing.expectEqualStrings("shared", nodes[1].key);
    try std.testing.expect(nodes[2].table == null);
    try std.testing.expectEqualStrings("other", nodes[2].key);
}

test "distributed graph complete anchors require the source snapshot" {
    const source_tokens = [_]db_mod.types.ShardIdentityReadGeneration{
        .{ .group_id = 11, .generation = 101 },
        .{ .group_id = 22, .generation = 202 },
    };
    const same_tokens_reordered = [_]db_mod.types.ShardIdentityReadGeneration{
        .{ .group_id = 22, .generation = 202 },
        .{ .group_id = 11, .generation = 101 },
    };
    const stale_tokens = [_]db_mod.types.ShardIdentityReadGeneration{
        .{ .group_id = 11, .generation = 100 },
        .{ .group_id = 22, .generation = 202 },
    };
    const empty_hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]);
    const source = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = empty_hits,
        .total_hits = 0,
        .shard_identity_read_generations = @constCast(source_tokens[0..]),
    };
    const same_snapshot = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = empty_hits,
        .total_hits = 0,
        .shard_identity_read_generations = @constCast(same_tokens_reordered[0..]),
    };
    try validateMatchingSourceSnapshots(source, same_snapshot);

    const stale_snapshot = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = empty_hits,
        .total_hits = 0,
        .shard_identity_read_generations = @constCast(stale_tokens[0..]),
    };
    try std.testing.expectError(error.TopologyChanged, validateMatchingSourceSnapshots(source, stale_snapshot));
}

test "distributed graph complete anchor pages require strict cursor order" {
    const ordered = [_]db_mod.types.SearchHit{
        .{ .id = @constCast("doc:b") },
        .{ .id = @constCast("doc:c") },
    };
    try validateMatchAnchorPageOrder("doc:a", &ordered);

    const repeated_cursor = [_]db_mod.types.SearchHit{.{ .id = @constCast("doc:a") }};
    try std.testing.expectError(error.InvalidQueryResult, validateMatchAnchorPageOrder("doc:a", &repeated_cursor));

    const out_of_order = [_]db_mod.types.SearchHit{
        .{ .id = @constCast("doc:c") },
        .{ .id = @constCast("doc:b") },
    };
    try std.testing.expectError(error.InvalidQueryResult, validateMatchAnchorPageOrder(null, &out_of_order));
}

test "distributed graph paged anchors use page completion instead of cursor-relative totals" {
    const paged = MatchAnchorSource{ .paged = undefined };

    // A full `_id` seek page commonly reports only a lower bound for the
    // cursor-relative scan. It must advance instead of rejecting the page.
    try std.testing.expect(!try matchAnchorPageIsTerminal(
        paged,
        complete_match_anchor_page_size,
        complete_match_anchor_page_size,
        .gte,
    ));

    // A short page completes the stream even though its cursor-relative total
    // differs from every prior page.
    try std.testing.expect(try matchAnchorPageIsTerminal(paged, 17, 17, .exact));
    try std.testing.expectError(
        error.InvalidQueryResult,
        matchAnchorPageIsTerminal(paged, 17, 17, .gte),
    );

    // Exact multiples fetch one final empty page.
    try std.testing.expect(!try matchAnchorPageIsTerminal(
        paged,
        complete_match_anchor_page_size,
        complete_match_anchor_page_size,
        .exact,
    ));
    try std.testing.expect(try matchAnchorPageIsTerminal(paged, 0, 0, .exact));
    try std.testing.expectError(
        error.InvalidQueryResult,
        matchAnchorPageIsTerminal(paged, complete_match_anchor_page_size + 1, 0, .gte),
    );

    const materialized = MatchAnchorSource{ .materialized = undefined };
    try std.testing.expectError(
        error.InvalidQueryResult,
        matchAnchorPageIsTerminal(materialized, 2, 1, .exact),
    );
}

test "distributed graph paged execution trusts only source-filtered anchors across cursor pages" {
    const alloc = std.testing.allocator;
    const Pager = struct {
        fetches: usize = 0,
        authorization_rejections: usize = 0,
        match_filter_rejections: usize = 0,

        fn fetch(
            ptr: *anyopaque,
            page_alloc: std.mem.Allocator,
            _: db_mod.types.NamedGraphQuery,
            search_after: []const std.json.Value,
        ) !db_mod.types.SearchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fetches += 1;
            var candidate_index: usize = 0;
            if (search_after.len > 0) {
                if (search_after.len != 1) return error.InvalidQueryResult;
                const cursor = switch (search_after[0]) {
                    .string => |value| value,
                    else => return error.InvalidQueryResult,
                };
                if (!std.mem.startsWith(u8, cursor, "anchor:")) return error.InvalidQueryResult;
                candidate_index = try std.fmt.parseInt(usize, cursor["anchor:".len..], 10);
                candidate_index += 1;
            }

            var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            errdefer {
                for (hits.items) |*hit| hit.deinit(page_alloc);
                hits.deinit(page_alloc);
            }
            while (candidate_index < 4099 and hits.items.len < complete_match_anchor_page_size) : (candidate_index += 1) {
                if (candidate_index == 1) {
                    self.authorization_rejections += 1;
                    continue;
                }
                if (candidate_index == 4097) {
                    self.match_filter_rejections += 1;
                    continue;
                }
                try hits.append(page_alloc, .{
                    .id = try std.fmt.allocPrint(page_alloc, "anchor:{d:0>5}", .{candidate_index}),
                });
            }
            const owned_hits = try hits.toOwnedSlice(page_alloc);
            return .{
                .alloc = page_alloc,
                .hits = owned_hits,
                .total_hits = @intCast(owned_hits.len),
                .total_hits_relation = if (owned_hits.len == complete_match_anchor_page_size) .gte else .exact,
            };
        }
    };

    const nodes = [_]graph_pattern_mod.MatchNode{.{
        .alias = "anchor",
        .filter = .{ .filter_query_json = "{\"term\":{\"kind\":\"allowed\"}}" },
    }};
    const pattern = graph_pattern_mod.ConjunctivePattern{
        .anchor_alias = "anchor",
        .nodes = &nodes,
        .edges = &.{},
    };
    const source_req = db_mod.types.SearchRequest{
        .filter_query_json = "{\"term\":{\"tenant\":\"authorized\"}}",
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
    };

    var pager = Pager{};
    var admission = GraphNodeAdmissionContext.init(
        alloc,
        undefined,
        undefined,
        "docs",
        0,
        source_req,
        null,
        &.{},
        .{},
        .read_index,
    );
    defer admission.deinit();
    const edge_reader = DistributedEdgeReader{
        .catalog = undefined,
        .worker = undefined,
        .source_table = "docs",
        .index_name = "graph",
        .consistency = .read_index,
        .admission = &admission,
    };

    const row_query = db_mod.types.NamedGraphQuery{
        .name = "rows",
        .query = .{
            .query_type = .pattern,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
            .match_pattern = pattern,
            .return_aliases = &.{"anchor"},
            .return_limit = 5000,
        },
    };
    var request_work_budget = graph_pattern_mod.WorkBudget.init(
        graph_pattern_mod.default_max_explored_nodes,
        graph_pattern_mod.default_max_explored_edges,
    );
    var request_distinct_budget = graph_pattern_mod.DistinctBudget.init(
        graph_pattern_mod.default_max_distinct_identities,
        graph_pattern_mod.default_max_distinct_identity_bytes,
    );
    var row_state = QueryState{ .name = try alloc.dupe(u8, row_query.name) };
    var rows = try executeDistributedConjunctivePattern(
        alloc,
        edge_reader,
        source_req,
        row_query,
        .{ .paged = .{ .ctx = &pager, .fetch_fn = Pager.fetch } },
        base_result,
        &admission,
        &row_state,
        &request_work_budget,
        &request_distinct_budget,
    );
    defer rows.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4097), rows.matches.len);
    try std.testing.expectEqual(@as(usize, 2), pager.fetches);
    try std.testing.expectEqual(@as(usize, 1), pager.authorization_rejections);
    try std.testing.expectEqual(@as(usize, 1), pager.match_filter_rejections);

    pager = .{};
    const aggregates = [_]graph_query_mod.NamedCountAggregate{.{
        .name = "matches",
        .of = "*",
    }};
    const aggregate_query = db_mod.types.NamedGraphQuery{
        .name = "counts",
        .query = .{
            .query_type = .pattern,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
            .match_pattern = pattern,
            .aggregates = &aggregates,
        },
    };
    var aggregate_state = QueryState{ .name = try alloc.dupe(u8, aggregate_query.name) };
    var counts = try executeDistributedConjunctivePattern(
        alloc,
        edge_reader,
        source_req,
        aggregate_query,
        .{ .paged = .{ .ctx = &pager, .fetch_fn = Pager.fetch } },
        base_result,
        &admission,
        &aggregate_state,
        &request_work_budget,
        &request_distinct_budget,
    );
    defer counts.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), counts.aggregates.len);
    try std.testing.expectEqual(@as(u128, 4097), counts.aggregates[0].value);
    try std.testing.expectEqual(@as(usize, 2), pager.fetches);
    try std.testing.expectEqual(@as(usize, 1), pager.authorization_rejections);
    try std.testing.expectEqual(@as(usize, 1), pager.match_filter_rejections);
}

test "distributed graph duplicate distinct aggregates share one merge payload" {
    const alloc = std.testing.allocator;
    var accumulators = [_]AggregatePageAccumulator{.{ .distinct = true, .value = 1 }};
    defer accumulators[0].deinit(alloc);
    _ = try accumulators[0].seen.putIfAbsent(alloc, .{ .table = "users", .key = "user:1" }, {});

    const requested = [_]graph_query_mod.NamedCountAggregate{
        .{ .name = "authors", .of = "author", .distinct = true },
        .{ .name = "unique_authors", .of = "author", .distinct = true },
    };
    const indexes = [_]usize{ 0, 0 };
    const aggregates = try materializeAggregateResults(alloc, &requested, &indexes, &accumulators);
    defer {
        for (aggregates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    }

    try std.testing.expectEqual(@as(usize, 2), aggregates.len);
    try std.testing.expectEqual(@as(u128, 1), aggregates[0].value);
    try std.testing.expectEqual(@as(u128, 1), aggregates[1].value);
    try std.testing.expect(aggregates[0].distinct_values_owned);
    try std.testing.expect(!aggregates[1].distinct_values_owned);
    try std.testing.expectEqual(aggregates[0].distinct_values.ptr, aggregates[1].distinct_values.ptr);
}

test "distributed graph result hydration follows the public document option" {
    var req = db_mod.types.SearchRequest{};
    var query = graph_query_mod.GraphQuery{
        .query_type = .traverse,
        .index_name = "graph_idx",
        .start_nodes = .{ .keys = &.{"doc:a"} },
    };
    try std.testing.expect(!graphResultHydrationRequested(req, query));
    query.include_documents = true;
    try std.testing.expect(graphResultHydrationRequested(req, query));
    query.include_documents = false;
    req.expand_strategy = .@"union";
    try std.testing.expect(graphResultHydrationRequested(req, query));
}

test "distributed graph exact distinct merge budget spans cursor pages" {
    const alloc = std.testing.allocator;
    var accumulator = AggregatePageAccumulator{ .distinct = true };
    defer accumulator.deinit(alloc);
    var budget = graph_pattern_mod.DistinctBudget.init(1, 1024);

    const first_values = [_]graph_node_identity.Ref{.{ .table = "users", .key = "user:1" }};
    try accumulator.observe(alloc, .{
        .value = 1,
        .distinct_values = @constCast(first_values[0..]),
    }, &budget);
    try std.testing.expectEqual(@as(u128, 1), accumulator.value);
    try std.testing.expectEqual(@as(usize, 0), budget.remaining_identities);

    // A duplicate on a later page neither grows the exact set nor consumes
    // another unit of request memory.
    try accumulator.observe(alloc, .{
        .value = 1,
        .distinct_values = @constCast(first_values[0..]),
    }, &budget);
    try std.testing.expectEqual(@as(u128, 1), accumulator.value);

    const next_values = [_]graph_node_identity.Ref{.{ .table = "users", .key = "user:2" }};
    try std.testing.expectError(
        error.GraphDistinctBudgetExceeded,
        accumulator.observe(alloc, .{
            .value = 1,
            .distinct_values = @constCast(next_values[0..]),
        }, &budget),
    );
    try std.testing.expectEqual(@as(u128, 1), accumulator.value);
}

test "distributed graph exact distinct budget spans named operations" {
    const alloc = std.testing.allocator;
    var first = AggregatePageAccumulator{ .distinct = true };
    defer first.deinit(alloc);
    var second = AggregatePageAccumulator{ .distinct = true };
    defer second.deinit(alloc);
    var request_budget = graph_pattern_mod.DistinctBudget.init(1, 1024);

    const first_values = [_]graph_node_identity.Ref{.{ .table = "users", .key = "user:1" }};
    try first.observe(alloc, .{
        .value = 1,
        .distinct_values = @constCast(first_values[0..]),
    }, &request_budget);

    const second_values = [_]graph_node_identity.Ref{.{ .table = "users", .key = "user:2" }};
    try std.testing.expectError(
        error.GraphDistinctBudgetExceeded,
        second.observe(alloc, .{
            .value = 1,
            .distinct_values = @constCast(second_values[0..]),
        }, &request_budget),
    );
}

test "distributed graph canonical MATCH admission excludes retrieval predicates" {
    const nodes = [_]graph_pattern_mod.MatchNode{.{ .alias = "anchor" }};
    const named = db_mod.types.NamedGraphQuery{
        .name = "matches",
        .query = .{
            .query_type = .pattern,
            .index_name = "graph",
            .start_nodes = .{ .keys = &.{} },
            .match_pattern = .{ .anchor_alias = "anchor", .nodes = &nodes, .edges = &.{} },
        },
    };
    const resolved: *const anyopaque = @ptrFromInt(1);
    const admission = graphNodeAdmissionRequest(.{
        .filter_query_json = "{\"term\":{\"retrieval\":true}}",
        .exclusion_query_json = "{\"term\":{\"hidden\":true}}",
        .authorization_filter_query_json = "{\"term\":{\"tenant\":\"one\"}}",
        .resolved_doc_filter = resolved,
        .resolved_doc_filter_owned = true,
        .resolved_doc_filter_wire_context = .{ .namespace = .{}, .identity_read_generation = 7 },
    }, named);

    try std.testing.expectEqualStrings(
        "{\"term\":{\"tenant\":\"one\"}}",
        admission.filter_query_json,
    );
    try std.testing.expectEqualStrings("", admission.exclusion_query_json);
    try std.testing.expect(admission.resolved_doc_filter == null);
    try std.testing.expect(!admission.resolved_doc_filter_owned);
    try std.testing.expect(admission.resolved_doc_filter_wire_context == null);
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
    node_table: ?[]const u8,
    mode: graph_paths_mod.PathWeightMode,
) !u32 {
    const parent_id = parent.path_state_id orelse return error.InvalidQueryRequest;
    const local_path_edges = node.path_edges orelse &.{};
    if (local_path_edges.len > 1) return error.InvalidQueryRequest;

    var path_state = try initPathState(
        alloc,
        node.key,
        node_table,
        parent.depth + 1,
        parent.distance + edgeWeightFromNode(node),
        parent.cost + edgeCost(parent, node, mode),
        parent_id,
        if (local_path_edges.len > 0) local_path_edges[0] else null,
    );
    errdefer path_state.deinit(alloc);
    try state.path_states.append(alloc, path_state);
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
    const path_tables_raw = try reconstructPathTables(alloc, state.path_states.items, path_state_id);
    errdefer freeOptionalStrings(alloc, path_tables_raw);
    const path_tables: ?[]const ?[]const u8 = if (path_tables_raw.len > 0)
        path_tables_raw
    else
        null;
    const path_edges = try reconstructPathEdges(alloc, state.path_states.items, path_state_id);
    errdefer freePathEdges(alloc, path_edges);
    const owned_path_edges: ?[]const graph_query_mod.PathEdgeInfo = if (path_edges.len > 0)
        path_edges
    else blk: {
        alloc.free(path_edges);
        break :blk null;
    };

    return try initGraphResultNode(
        alloc,
        path_state.key,
        path_state.table,
        path_state.depth,
        path_state.distance,
        path,
        path_tables,
        owned_path_edges,
    );
}

fn resolveStartFrontier(
    alloc: std.mem.Allocator,
    state: *QueryState,
    source_table: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    selector: graph_query_mod.NodeSelector,
    include_paths: bool,
) ![]FrontierState {
    const nodes = try resolveSelectorNodes(alloc, source_table, req, base_result, prior_results, selector);
    var transferred: usize = 0;
    defer {
        for (nodes[transferred..]) |*node| node.deinit(alloc);
        if (nodes.len > 0) alloc.free(nodes);
    }

    const out = try alloc.alloc(FrontierState, nodes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (nodes, 0..) |node, i| {
        const path_state_id = if (include_paths)
            try appendRootPathState(alloc, state, node.key, node.table)
        else
            null;
        out[i] = .{
            .key = node.key,
            .table = node.table,
            .depth = 0,
            .distance = 0,
            .cost = 0,
            .path_state_id = path_state_id,
        };
        transferred += 1;
        initialized += 1;
    }
    return out;
}

fn resolveSelectorNodes(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    selector: graph_query_mod.NodeSelector,
) ![]GraphNodeIdentity {
    return switch (selector) {
        .keys => |keys| blk: {
            const out = try alloc.alloc(GraphNodeIdentity, keys.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |*node| node.deinit(alloc);
                alloc.free(out);
            }
            for (keys, 0..) |key, i| {
                out[i] = try cloneGraphNodeIdentityParts(alloc, key, null);
                initialized += 1;
            }
            break :blk out;
        },
        .identities => |identities| blk: {
            const out = try alloc.alloc(GraphNodeIdentity, identities.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |*node| node.deinit(alloc);
                alloc.free(out);
            }
            for (identities, 0..) |identity, i| {
                out[i] = try cloneGraphNodeIdentityParts(
                    alloc,
                    identity.key,
                    canonicalGraphNodeTable(source_table, identity.table),
                );
                initialized += 1;
            }
            break :blk out;
        },
        .result_ref => |result_ref| resolveResultRefNodes(
            alloc,
            source_table,
            req,
            base_result,
            prior_results,
            result_ref,
        ),
    };
}

fn resolveSelectorKeys(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    selector: graph_query_mod.NodeSelector,
) ![][]u8 {
    return switch (selector) {
        .keys => |keys| dupKeys(alloc, keys),
        .identities => |identities| blk: {
            const refs = try alloc.alloc([]const u8, identities.len);
            defer alloc.free(refs);
            for (identities, 0..) |identity, i| refs[i] = identity.key;
            break :blk try dupKeys(alloc, refs);
        },
        .result_ref => |result_ref| resolveResultRefKeys(alloc, source_table, req, base_result, prior_results, result_ref),
    };
}

fn graphResultNodeAdmissionMaskAlloc(
    alloc: std.mem.Allocator,
    admission: *GraphNodeAdmissionContext,
    source_table: []const u8,
    expansion_table: []const u8,
    nodes: []const graph_query_mod.GraphResultNode,
) ![]bool {
    const refs = try alloc.alloc(graph_node_admission.NodeRef, nodes.len);
    defer alloc.free(refs);
    for (nodes, 0..) |node, i| {
        const table = canonicalExpandedNodeTable(
            source_table,
            expansion_table,
            node.table,
        );
        refs[i] = .{
            .key = node.key,
            .table = table,
            .external = table != null,
        };
    }
    return try admission.iface().filterAlloc(alloc, refs);
}

fn graphExpansionNodeAdmissionMaskAlloc(
    alloc: std.mem.Allocator,
    admission: *GraphNodeAdmissionContext,
    source_table: []const u8,
    expansion_table: []const u8,
    expansions: []const GraphExpansion,
) ![]bool {
    var node_count: usize = 0;
    for (expansions) |expansion| {
        node_count = std.math.add(
            usize,
            node_count,
            expansion.graph_result.nodes.len,
        ) catch return error.InvalidQueryResult;
    }

    const refs = try alloc.alloc(graph_node_admission.NodeRef, node_count);
    defer alloc.free(refs);
    var ref_index: usize = 0;
    for (expansions) |expansion| {
        for (expansion.graph_result.nodes) |node| {
            const table = canonicalExpandedNodeTable(
                source_table,
                expansion_table,
                node.table,
            );
            refs[ref_index] = .{
                .key = node.key,
                .table = table,
                .external = table != null,
            };
            ref_index += 1;
        }
    }
    return try admission.iface().filterAlloc(alloc, refs);
}

fn frontierAdmissionMaskAlloc(
    alloc: std.mem.Allocator,
    admission: *GraphNodeAdmissionContext,
    frontier: []const FrontierState,
) ![]bool {
    const refs = try alloc.alloc(graph_node_admission.NodeRef, frontier.len);
    defer alloc.free(refs);
    for (frontier, 0..) |item, i| {
        refs[i] = .{
            .key = item.key,
            .table = item.table,
            .external = item.table != null,
        };
    }
    return try admission.iface().filterAlloc(alloc, refs);
}

fn retainAdmittedFrontier(
    alloc: std.mem.Allocator,
    admission: *GraphNodeAdmissionContext,
    frontier: []FrontierState,
) ![]FrontierState {
    const mask = try frontierAdmissionMaskAlloc(alloc, admission, frontier);
    defer alloc.free(mask);
    var count: usize = 0;
    for (mask) |allowed| count += @intFromBool(allowed);
    if (count == frontier.len) return frontier;

    const out = try alloc.alloc(FrontierState, count);
    var out_index: usize = 0;
    for (frontier, mask) |*item, allowed| {
        if (allowed) {
            out[out_index] = item.*;
            out_index += 1;
        } else {
            item.deinit(alloc);
        }
    }
    alloc.free(frontier);
    return out;
}

fn resolveResultRefNodes(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    result_ref: graph_query_mod.ResultRef,
) ![]GraphNodeIdentity {
    if (!resultHasIdentitySnapshot(req, base_result)) return error.UnsupportedQueryRequest;

    if (std.mem.startsWith(u8, result_ref.ref, "$graph_results.")) {
        const name = result_ref.ref["$graph_results.".len..];
        for (prior_results) |graph_result| {
            if (!std.mem.eql(u8, graph_result.name, name)) continue;
            if (result_ref.binding) |binding| {
                if (result_ref.limit == 0 and
                    (graph_result.truncated or @as(u64, graph_result.total_hits) > graph_result.matches.len))
                    return error.UnsupportedQueryRequest;
                return try resolveMatchBindingNodes(
                    alloc,
                    source_table,
                    graph_result.matches,
                    binding,
                    result_ref.limit,
                );
            }
            if (result_ref.limit == 0 and
                (graph_result.truncated or @as(u64, graph_result.total_hits) > graph_result.nodes.len))
            {
                return error.UnsupportedQueryRequest;
            }
            return try resolveUniqueGraphResultNodes(
                alloc,
                source_table,
                graph_result.nodes,
                result_ref.limit,
            );
        }
        return error.GraphResultRefNotImplemented;
    }

    if (!std.mem.eql(u8, result_ref.ref, "$query_results") or result_ref.binding != null) {
        return error.GraphResultRefNotImplemented;
    }

    if (result_ref.limit == 0 and baseResultRefMayBeIncomplete(req, base_result))
        return error.UnsupportedQueryRequest;
    return try resolveUniqueSearchHitNodes(
        alloc,
        source_table,
        base_result.hits,
        result_ref.limit,
    );
}

fn resolveUniqueGraphResultNodes(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    result_nodes: []const graph_query_mod.GraphResultNode,
    limit: u32,
) ![]GraphNodeIdentity {
    var nodes = std.ArrayListUnmanaged(GraphNodeIdentity).empty;
    errdefer {
        for (nodes.items) |*node| node.deinit(alloc);
        nodes.deinit(alloc);
    }
    var seen = graph_node_identity.Map(void){};
    defer seen.deinit(alloc);

    for (result_nodes) |result_node| {
        const table = canonicalGraphNodeTable(source_table, result_node.table);
        const ref = graph_node_identity.Ref{ .table = table, .key = result_node.key };
        if (seen.contains(ref)) continue;
        var node = try cloneGraphNodeIdentityParts(alloc, result_node.key, table);
        errdefer node.deinit(alloc);
        _ = try seen.putIfAbsent(alloc, node.ref(), {});
        try nodes.append(alloc, node);
        if (limit > 0 and nodes.items.len >= limit) break;
    }
    return try nodes.toOwnedSlice(alloc);
}

fn resolveUniqueSearchHitNodes(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    hits: []const db_mod.types.SearchHit,
    limit: u32,
) ![]GraphNodeIdentity {
    var nodes = std.ArrayListUnmanaged(GraphNodeIdentity).empty;
    errdefer {
        for (nodes.items) |*node| node.deinit(alloc);
        nodes.deinit(alloc);
    }
    var seen = graph_node_identity.Map(void){};
    defer seen.deinit(alloc);

    for (hits) |hit| {
        const table = canonicalGraphNodeTable(source_table, hit.source_table);
        const ref = graph_node_identity.Ref{ .table = table, .key = hit.id };
        if (seen.contains(ref)) continue;
        var node = try cloneGraphNodeIdentityParts(alloc, hit.id, table);
        errdefer node.deinit(alloc);
        _ = try seen.putIfAbsent(alloc, node.ref(), {});
        try nodes.append(alloc, node);
        if (limit > 0 and nodes.items.len >= limit) break;
    }
    return try nodes.toOwnedSlice(alloc);
}

fn resolveMatchBindingNodes(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    matches: []const db_mod.types.GraphPatternMatch,
    alias: []const u8,
    limit: u32,
) ![]GraphNodeIdentity {
    var nodes = std.ArrayListUnmanaged(GraphNodeIdentity).empty;
    errdefer {
        for (nodes.items) |*node| node.deinit(alloc);
        nodes.deinit(alloc);
    }
    var seen = graph_node_identity.Map(void){};
    defer seen.deinit(alloc);

    for (matches) |match| {
        for (match.bindings) |binding| {
            if (!std.mem.eql(u8, binding.alias, alias)) continue;
            const table = canonicalGraphNodeTable(source_table, binding.node.table);
            const ref = graph_node_identity.Ref{ .table = table, .key = binding.node.key };
            if (seen.contains(ref)) break;
            var node = try cloneGraphNodeIdentityParts(alloc, binding.node.key, table);
            errdefer node.deinit(alloc);
            _ = try seen.putIfAbsent(alloc, node.ref(), {});
            try nodes.append(alloc, node);
            if (limit > 0 and nodes.items.len >= limit) return try nodes.toOwnedSlice(alloc);
            break;
        }
    }
    return try nodes.toOwnedSlice(alloc);
}

fn resolveResultRefKeys(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    req: db_mod.types.SearchRequest,
    base_result: db_mod.types.SearchResult,
    prior_results: []const db_mod.types.GraphSearchResult,
    result_ref: graph_query_mod.ResultRef,
) ![][]u8 {
    const nodes = try resolveResultRefNodes(
        alloc,
        source_table,
        req,
        base_result,
        prior_results,
        result_ref,
    );
    defer freeNodeIdentities(alloc, nodes);
    const out = try alloc.alloc([]u8, nodes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |key| alloc.free(key);
        alloc.free(out);
    }
    for (nodes, 0..) |node, i| {
        out[i] = try alloc.dupe(u8, node.key);
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
    exclude_nodes: []const GraphNodeIdentity,
    exclude_edges: []const []const u8,
    include_paths: bool,
) !GraphExpandRequest {
    return try makeGraphExpandRequestWithAlgebraicMode(
        alloc,
        named_query,
        frontier,
        frontier_ids,
        exclude_nodes,
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
    exclude_nodes: []const GraphNodeIdentity,
    exclude_edges: []const []const u8,
    include_paths: bool,
    algebraic_semiring_selected: bool,
) !GraphExpandRequest {
    return try makeGraphExpandRequestWithAlgebraicModeAndTargetConstraints(
        alloc,
        named_query,
        frontier,
        frontier_ids,
        exclude_nodes,
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
    exclude_nodes: []const GraphNodeIdentity,
    exclude_edges: []const []const u8,
    include_paths: bool,
    algebraic_semiring_selected: bool,
    target_constraint_keys: []const []const u8,
) !GraphExpandRequest {
    var params = named_query.query.params;
    params.edge_types = try dupConstStrings(alloc, named_query.query.params.edge_types);
    errdefer freeConstStrings(alloc, params.edge_types);
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
        owned_frontier[i] = try cloneGraphFrontierItemParts(
            alloc,
            frontier_id,
            item.key,
            item.table,
            item.depth,
            item.distance,
        );
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

    const name = try alloc.dupe(u8, named_query.name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, named_query.query.index_name);
    errdefer alloc.free(index_name);
    const owned_exclude_nodes = try dupNodeIdentities(alloc, exclude_nodes);
    errdefer {
        for (owned_exclude_nodes) |*identity| identity.deinit(alloc);
        if (owned_exclude_nodes.len > 0) alloc.free(owned_exclude_nodes);
    }
    const owned_exclude_edges = try dupKeys(alloc, exclude_edges);
    errdefer freeKeys(alloc, owned_exclude_edges);
    const owned_target_constraint_keys = try dupSortedUniqueKeys(alloc, target_constraint_keys);
    errdefer freeKeys(alloc, owned_target_constraint_keys);

    return .{
        .name = name,
        .index_name = index_name,
        .frontier = owned_frontier,
        .exclude_nodes = owned_exclude_nodes,
        .exclude_edges = owned_exclude_edges,
        .target_constraint_keys = owned_target_constraint_keys,
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

fn catalogTableHasGraphIndex(
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
    return indexes_api.inferIndexType(index_name, lookup.config) == .graph;
}

test "cross-table graph index availability fails closed for terminal expansion" {
    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "with_graph", .placement_role = "data", .indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}" },
            .{ .table_id = 8, .name = "without_index", .placement_role = "data" },
            .{ .table_id = 9, .name = "wrong_kind", .placement_role = "data", .indexes_json = "{\"graph_idx\":{\"type\":\"full_text\"}}" },
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
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const alloc = std.testing.allocator;
    try std.testing.expect(try catalogTableHasGraphIndex(alloc, FakeCatalog.iface(), "with_graph", "graph_idx"));
    try std.testing.expect(!try catalogTableHasGraphIndex(alloc, FakeCatalog.iface(), "without_index", "graph_idx"));
    try std.testing.expect(!try catalogTableHasGraphIndex(alloc, FakeCatalog.iface(), "wrong_kind", "graph_idx"));
    try std.testing.expect(!try catalogTableHasGraphIndex(alloc, FakeCatalog.iface(), "missing_table", "graph_idx"));
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
        .query = .{ .match_all = {} },
        .graph_queries = graph_queries,
        .limit = 0,
        .include_stored = true,
        .identity_read_generation = req.identity_read_generation,
        .resolved_doc_filter = req.resolved_doc_filter,
        .resolved_doc_filter_wire_context = req.resolved_doc_filter_wire_context,
        .execution_deadline_ns = executionDeadlineFromTimeoutMs(req.timeout_ms),
        .cancellation = req.cancellation,
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
            .identities => |identities| {
                for (identities) |identity| {
                    alloc.free(@constCast(identity.key));
                    if (identity.table) |table| alloc.free(@constCast(table));
                }
                alloc.free(identities);
            },
            .result_ref => {},
        }
        if (graph_query.query.target_nodes) |target_nodes| {
            switch (target_nodes) {
                .keys => |keys| {
                    for (keys) |key| alloc.free(@constCast(key));
                    alloc.free(keys);
                },
                .identities => |identities| {
                    for (identities) |identity| {
                        alloc.free(@constCast(identity.key));
                        if (identity.table) |table| alloc.free(@constCast(table));
                    }
                    alloc.free(identities);
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
            .table = item.table,
            .depth = item.depth,
            .distance = item.distance,
        };
    }
    const exclude_nodes = try alloc.alloc(GraphNodeIdentityJson, req.exclude_nodes.len);
    defer alloc.free(exclude_nodes);
    for (req.exclude_nodes, 0..) |identity, i| {
        exclude_nodes[i] = .{ .key = identity.key, .table = identity.table };
    }
    const encoded = try jsonStringifyAlloc(alloc, GraphExpandRequestJson{
        .name = req.name,
        .index_name = req.index_name,
        .frontier = frontier,
        .exclude_nodes = exclude_nodes,
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
        frontier[i] = try cloneGraphFrontierItemParts(
            alloc,
            item.id,
            item.key,
            item.table,
            item.depth,
            item.distance,
        );
        frontier_initialized += 1;
    }

    var parsed_filter: ?db_mod.doc_filter_wire.ParsedResolvedDocFilter = null;
    errdefer if (parsed_filter) |*filter| filter.deinit(alloc);
    if (parsed.value._resolved_doc_filter) |value| {
        parsed_filter = try db_mod.doc_filter_wire.parseFilterEnvelopeAlloc(alloc, value);
    }
    const identity_read_generation = try identityGenerationFromResolvedFilterEnvelope(parsed.value.identity_read_generation, if (parsed_filter) |*filter| filter else null);

    const exclude_nodes = try alloc.alloc(GraphNodeIdentity, parsed.value.exclude_nodes.len);
    var exclude_nodes_initialized: usize = 0;
    errdefer {
        for (exclude_nodes[0..exclude_nodes_initialized]) |*identity| identity.deinit(alloc);
        alloc.free(exclude_nodes);
    }
    for (parsed.value.exclude_nodes, 0..) |identity, i| {
        exclude_nodes[i] = try cloneGraphNodeIdentityParts(
            alloc,
            identity.key,
            identity.table,
        );
        exclude_nodes_initialized += 1;
    }

    const name = try alloc.dupe(u8, parsed.value.name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, parsed.value.index_name);
    errdefer alloc.free(index_name);
    const exclude_edges = try dupKeys(alloc, parsed.value.exclude_edges);
    errdefer freeKeys(alloc, exclude_edges);
    const edge_types = try dupConstStrings(alloc, parsed.value.params.edge_types);
    errdefer freeConstStrings(alloc, edge_types);

    const out = GraphExpandRequest{
        .name = name,
        .index_name = index_name,
        .frontier = frontier,
        .exclude_nodes = exclude_nodes,
        .exclude_edges = exclude_edges,
        .target_constraint_keys = target_constraint_keys,
        .topology_epoch = parsed.value.topology_epoch,
        .identity_read_generation = identity_read_generation,
        .resolved_doc_filter = if (parsed_filter) |filter| filter.resolved_doc_filter else null,
        .resolved_doc_filter_owned = parsed_filter != null,
        .resolved_doc_filter_wire_context = if (parsed_filter) |filter| filter.context else null,
        .params = .{
            .edge_types = edge_types,
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

        const frontier_key = try alloc.dupe(u8, expansion.frontier_key);
        errdefer alloc.free(frontier_key);
        const name = try alloc.dupe(u8, expansion.name);
        errdefer alloc.free(name);
        expansions[i] = .{
            .frontier_id = expansion.frontier_id,
            .frontier_key = frontier_key,
            .graph_result = .{
                .name = name,
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

    const aggregates = if (src.aggregates.len > 0)
        try cloneGraphAggregates(alloc, src.aggregates)
    else
        @constCast((&[_]db_mod.types.GraphAggregateResult{})[0..]);
    errdefer if (aggregates.len > 0) {
        for (aggregates) |*aggregate| aggregate.deinit(alloc);
        alloc.free(aggregates);
    };

    return .{
        .name = try alloc.dupe(u8, src.name),
        .nodes = nodes,
        .paths = paths,
        .matches = matches,
        .aggregates = aggregates,
        .hits = hits,
        .total_hits = src.total_hits,
        .truncated = src.truncated,
    };
}

pub fn filterGraphSearchResult(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    src: db_mod.types.GraphSearchResult,
    exclude_nodes: []const GraphNodeIdentity,
    exclude_edges: []const []const u8,
) !db_mod.types.GraphSearchResult {
    if (exclude_nodes.len == 0 and exclude_edges.len == 0) return try cloneGraphSearchResult(alloc, src);

    var exclude = graph_node_identity.Map(void){};
    defer exclude.deinit(alloc);
    var source_exclude = std.StringHashMapUnmanaged(void).empty;
    defer source_exclude.deinit(alloc);
    for (exclude_nodes) |identity| {
        _ = try exclude.putIfAbsent(alloc, identity.ref(), {});
        if (identity.table == null) try source_exclude.put(alloc, identity.key, {});
    }

    var exclude_edge_set = std.StringHashMapUnmanaged(void).empty;
    defer exclude_edge_set.deinit(alloc);
    for (exclude_edges) |edge| try exclude_edge_set.put(alloc, edge, {});

    var nodes = std.ArrayListUnmanaged(graph_query_mod.GraphResultNode).empty;
    defer {
        for (nodes.items) |*node| node.deinit(alloc);
        nodes.deinit(alloc);
    }
    for (src.nodes) |node| {
        if (exclude.contains(.{
            .table = canonicalGraphNodeTable(source_table, node.table),
            .key = node.key,
        })) continue;
        if (exclude_edge_set.count() > 0) {
            if (node.path_edges) |path_edges| {
                if (path_edges.len > 0) {
                    const edge_key = try allocEdgeExclusionKey(
                        alloc,
                        source_table,
                        path_edges[0].source,
                        path_edges[0].target,
                        path_edges[0].edge_type,
                    );
                    defer alloc.free(edge_key);
                    if (exclude_edge_set.contains(edge_key)) continue;
                }
            }
        }
        var owned_node = try cloneGraphNode(alloc, node);
        nodes.append(alloc, owned_node) catch |err| {
            owned_node.deinit(alloc);
            return err;
        };
    }

    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    defer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }
    for (src.hits) |hit| {
        if (exclude.contains(.{ .table = hit.source_table, .key = hit.id })) continue;
        var owned_hit = try hit.clone(alloc);
        hits.append(alloc, owned_hit) catch |err| {
            owned_hit.deinit(alloc);
            return err;
        };
    }

    var paths = std.ArrayListUnmanaged(db_mod.types.GraphPath).empty;
    defer {
        for (paths.items) |path| graph_paths_mod.freePath(alloc, path);
        paths.deinit(alloc);
    }
    for (src.paths) |path| {
        if (try graphPathIsExcluded(
            alloc,
            source_table,
            path,
            &exclude,
            &exclude_edge_set,
        )) continue;
        const owned_path = try cloneGraphPath(alloc, path);
        paths.append(alloc, owned_path) catch |err| {
            graph_paths_mod.freePath(alloc, owned_path);
            return err;
        };
    }

    var matches = std.ArrayListUnmanaged(db_mod.types.GraphPatternMatch).empty;
    defer {
        for (matches.items) |*match| match.deinit(alloc);
        matches.deinit(alloc);
    }
    for (src.matches) |match| {
        if (try graphPatternMatchIsExcluded(
            alloc,
            source_table,
            match,
            &exclude,
            &source_exclude,
            &exclude_edge_set,
        )) continue;
        var owned_match = try cloneGraphPatternMatch(alloc, match);
        matches.append(alloc, owned_match) catch |err| {
            owned_match.deinit(alloc);
            return err;
        };
    }

    const total_hits: u32 = @intCast(nodes.items.len);
    const name = try alloc.dupe(u8, src.name);
    errdefer alloc.free(name);
    const owned_nodes = try nodes.toOwnedSlice(alloc);
    errdefer {
        for (owned_nodes) |*node| node.deinit(alloc);
        if (owned_nodes.len > 0) alloc.free(owned_nodes);
    }
    const owned_paths = try paths.toOwnedSlice(alloc);
    errdefer {
        for (owned_paths) |path| graph_paths_mod.freePath(alloc, path);
        if (owned_paths.len > 0) alloc.free(owned_paths);
    }
    const owned_matches = try matches.toOwnedSlice(alloc);
    errdefer {
        for (owned_matches) |*match| match.deinit(alloc);
        if (owned_matches.len > 0) alloc.free(owned_matches);
    }
    const owned_hits = try hits.toOwnedSlice(alloc);
    errdefer {
        for (owned_hits) |*hit| hit.deinit(alloc);
        if (owned_hits.len > 0) alloc.free(owned_hits);
    }

    return .{
        .name = name,
        .nodes = owned_nodes,
        .paths = owned_paths,
        .matches = owned_matches,
        .hits = owned_hits,
        .total_hits = total_hits,
    };
}

fn canonicalGraphNodeTable(source_table: []const u8, table: ?[]const u8) ?[]const u8 {
    const table_name = table orelse return null;
    if (std.mem.eql(u8, source_table, table_name)) return null;
    return table_name;
}

fn canonicalExpandedNodeTable(
    source_table: []const u8,
    expansion_table: []const u8,
    declared_table: ?[]const u8,
) ?[]const u8 {
    return canonicalGraphNodeTable(
        source_table,
        declared_table orelse expansion_table,
    );
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
    table: ?[]const u8,
) !u32 {
    var path_state = try initPathState(alloc, key, table, 0, 0, 0, null, null);
    errdefer path_state.deinit(alloc);
    try state.path_states.append(alloc, path_state);
    return @intCast(state.path_states.items.len - 1);
}

fn initPathState(
    alloc: std.mem.Allocator,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
    distance: f64,
    cost: f64,
    parent: ?u32,
    incoming_edge: ?graph_query_mod.PathEdgeInfo,
) !PathState {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_table) |value| alloc.free(value);
    const owned_edge = if (incoming_edge) |value| try clonePathEdge(alloc, value) else null;
    errdefer if (owned_edge) |value| freeOwnedPathEdge(alloc, value);

    return .{
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .distance = distance,
        .cost = cost,
        .parent = parent,
        .incoming_edge = owned_edge,
    };
}

fn appendPathStateFromStep(
    alloc: std.mem.Allocator,
    state: *QueryState,
    parent: FrontierState,
    node: graph_query_mod.GraphResultNode,
    node_table: ?[]const u8,
) !u32 {
    const parent_id = parent.path_state_id orelse return error.InvalidQueryRequest;
    const local_path_edges = node.path_edges orelse &.{};
    if (local_path_edges.len > 1) return error.InvalidQueryRequest;

    var path_state = try initPathState(
        alloc,
        node.key,
        node_table,
        parent.depth + 1,
        parent.distance + node.distance,
        parent.cost + edgeCost(parent, node, .min_hops),
        parent_id,
        if (local_path_edges.len > 0) local_path_edges[0] else null,
    );
    errdefer path_state.deinit(alloc);
    try state.path_states.append(alloc, path_state);
    return @intCast(state.path_states.items.len - 1);
}

fn materializeResultNode(
    alloc: std.mem.Allocator,
    state: *QueryState,
    parent: FrontierState,
    node: graph_query_mod.GraphResultNode,
    node_table: ?[]const u8,
    include_paths: bool,
    path_state_id: ?u32,
) !graph_query_mod.GraphResultNode {
    if (!include_paths) {
        return try initGraphResultNode(
            alloc,
            node.key,
            node_table,
            parent.depth + 1,
            parent.distance + node.distance,
            null,
            null,
            null,
        );
    }

    const id = path_state_id orelse return error.InvalidQueryRequest;
    const path_state = state.path_states.items[id];
    const path = try reconstructPath(alloc, state.path_states.items, id);
    errdefer freePathArray(alloc, path);
    const path_tables_raw = try reconstructPathTables(alloc, state.path_states.items, id);
    errdefer freeOptionalStrings(alloc, path_tables_raw);
    const path_tables: ?[]const ?[]const u8 = if (path_tables_raw.len > 0)
        path_tables_raw
    else
        null;
    const path_edges = try reconstructPathEdges(alloc, state.path_states.items, id);
    errdefer freePathEdges(alloc, path_edges);
    const owned_path_edges: ?[]const graph_query_mod.PathEdgeInfo = if (path_edges.len > 0)
        path_edges
    else blk: {
        alloc.free(path_edges);
        break :blk null;
    };

    return try initGraphResultNode(
        alloc,
        path_state.key,
        path_state.table,
        path_state.depth,
        path_state.distance,
        path,
        path_tables,
        owned_path_edges,
    );
}

fn frontierFromState(
    alloc: std.mem.Allocator,
    state: *QueryState,
    node: graph_query_mod.GraphResultNode,
    path_state_id: ?u32,
) !FrontierState {
    if (path_state_id) |id| {
        const path_state = state.path_states.items[id];
        return try initFrontierState(
            alloc,
            path_state.key,
            path_state.table,
            path_state.depth,
            path_state.distance,
            path_state.cost,
            id,
        );
    }

    return try initFrontierState(
        alloc,
        node.key,
        node.table,
        node.depth,
        node.distance,
        node.distance,
        null,
    );
}

fn overrideFrontierTables(
    alloc: std.mem.Allocator,
    state: *QueryState,
    frontier: []FrontierState,
    table: []const u8,
) !void {
    for (frontier) |*item| {
        const frontier_table = try alloc.dupe(u8, table);
        errdefer alloc.free(frontier_table);
        var path_table: ?[]u8 = null;
        if (item.path_state_id != null) {
            path_table = try alloc.dupe(u8, table);
        }

        if (item.table) |old| alloc.free(old);
        item.table = frontier_table;
        if (item.path_state_id) |id| {
            if (state.path_states.items[id].table) |old| alloc.free(old);
            state.path_states.items[id].table = path_table;
        }
    }
}

fn initGraphResultNode(
    alloc: std.mem.Allocator,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
    distance: f64,
    path: ?[]const []const u8,
    path_tables: ?[]const ?[]const u8,
    path_edges: ?[]const graph_query_mod.PathEdgeInfo,
) !graph_query_mod.GraphResultNode {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_table) |value| alloc.free(value);

    return .{
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .distance = distance,
        .path = path,
        .path_tables = path_tables,
        .path_edges = path_edges,
    };
}

fn initFrontierState(
    alloc: std.mem.Allocator,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
    distance: f64,
    cost: f64,
    path_state_id: ?u32,
) !FrontierState {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_table) |value| alloc.free(value);

    return .{
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .distance = distance,
        .cost = cost,
        .path_state_id = path_state_id,
    };
}

fn cloneGraphFrontierItemParts(
    alloc: std.mem.Allocator,
    id: u32,
    key: []const u8,
    table: ?[]const u8,
    depth: u32,
    distance: f64,
) !GraphFrontierItem {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_table) |value| alloc.free(value);

    return .{
        .id = id,
        .key = owned_key,
        .table = owned_table,
        .depth = depth,
        .distance = distance,
    };
}

fn cloneGraphNodeIdentityParts(
    alloc: std.mem.Allocator,
    key: []const u8,
    table: ?[]const u8,
) !GraphNodeIdentity {
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_table = if (table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (owned_table) |value| alloc.free(value);

    return .{ .key = owned_key, .table = owned_table };
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

fn dupNodeIdentities(
    alloc: std.mem.Allocator,
    identities: []const GraphNodeIdentity,
) ![]GraphNodeIdentity {
    const out = try alloc.alloc(GraphNodeIdentity, identities.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*identity| identity.deinit(alloc);
        alloc.free(out);
    }
    for (identities, 0..) |identity, i| {
        out[i] = try cloneGraphNodeIdentityParts(alloc, identity.key, identity.table);
        initialized += 1;
    }
    return out;
}

fn freeNodeIdentities(
    alloc: std.mem.Allocator,
    identities: []GraphNodeIdentity,
) void {
    for (identities) |*identity| identity.deinit(alloc);
    if (identities.len > 0) alloc.free(identities);
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
            "docs",
            req,
            base_result,
            &.{},
            .{ .ref = "$query_results", .limit = 0 },
        ));

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            "docs",
            .{ .limit = 10 },
            base_result,
            &.{},
            .{ .ref = "$query_results", .limit = 1 },
        ));

        const limited = try resolveResultRefKeys(
            alloc,
            "docs",
            req,
            base_result,
            &.{},
            .{ .ref = "$query_results", .limit = 1 },
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
            "docs",
            req,
            base_result,
            &.{},
            .{ .ref = "$query_results", .limit = 0 },
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
            "docs",
            .{ .identity_read_generation = 9 },
            base_result,
            &.{graph_result},
            .{ .ref = "$graph_results.first_hop", .limit = 0 },
        ));

        try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
            alloc,
            "docs",
            .{},
            base_result,
            &.{graph_result},
            .{ .ref = "$graph_results.first_hop", .limit = 1 },
        ));

        const limited = try resolveResultRefKeys(
            alloc,
            "docs",
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
            const expansions = try alloc_inner.alloc(GraphExpansion, req.frontier.len);
            var initialized: usize = 0;
            errdefer {
                for (expansions[0..initialized]) |*expansion| expansion.deinit(alloc_inner);
                alloc_inner.free(expansions);
            }
            for (req.frontier, 0..) |frontier, i| {
                const node_key = if (group_id == 22)
                    "doc:o"
                else if (std.mem.eql(u8, frontier.key, "doc:a"))
                    "doc:b"
                else
                    "doc:d";
                const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 1);
                var node_initialized = false;
                errdefer {
                    if (node_initialized) nodes[0].deinit(alloc_inner);
                    alloc_inner.free(nodes);
                }
                nodes[0] = .{
                    .key = try alloc_inner.dupe(u8, node_key),
                    .depth = 1,
                    .distance = 1.0,
                    .path = null,
                    .path_edges = null,
                };
                node_initialized = true;
                const frontier_key = try alloc_inner.dupe(u8, frontier.key);
                errdefer alloc_inner.free(frontier_key);
                const result_name = try alloc_inner.dupe(u8, req.name);
                errdefer alloc_inner.free(result_name);
                expansions[i] = .{
                    .frontier_id = frontier.id,
                    .frontier_key = frontier_key,
                    .graph_result = .{
                        .name = result_name,
                        .nodes = nodes,
                        .paths = @constCast((&[_]db_mod.types.GraphPath{})[0..]),
                        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
                        .total_hits = 1,
                    },
                };
                initialized += 1;
            }
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
                    .start_nodes = .{ .keys = &[_][]const u8{ "doc:a", "doc:c", "doc:n" } },
                    .params = .{},
                    .include_documents = true,
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
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
    // Two start-admission batches, two expansion-admission batches, and two
    // final document hydration batches. Per-frontier admission would make
    // seven calls.
    try std.testing.expectEqual(@as(u32, 6), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 3), results[0].hits.len);
    for (results[0].hits) |hit| try std.testing.expect(hit.doc_ordinal == null);
}

pub fn testCrossTableHydrateAppliesTargetAuthorizationAndClearsOrdinals(alloc: std.mem.Allocator) !void {
    const TestState = struct {
        filter_ptr: *const anyopaque,
        same_table_calls: u32 = 0,
        cross_table_calls: u32 = 0,
    };
    const target_filter = "{\"term\":{\"tenant\":\"acme\"}}";
    const Authorizer = struct {
        allow_entities: bool,

        fn authorize(
            ctx: ?*const anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
        ) !db_mod.types.GraphTableReadAuthorization {
            const self: *const @This() = @ptrCast(@alignCast(ctx orelse {
                return .{ .allowed = false };
            }));
            if (!self.allow_entities or !std.mem.eql(u8, table_name, "entities")) {
                return .{ .allowed = false };
            }
            return .{
                .allowed = true,
                .filter_query_json = try alloc_inner.dupe(u8, target_filter),
            };
        }

        fn iface(self: *const @This()) db_mod.types.GraphTableReadAuthorizer {
            return .{
                .ctx = self,
                .authorize_table = authorize,
            };
        }
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
                try std.testing.expectEqualStrings(target_filter, req.filter_query_json);
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

    const authorizer = Authorizer{ .allow_entities = true };
    var admission = GraphNodeAdmissionContext.init(
        alloc,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        0,
        .{
            .identity_read_generation = 44,
            .resolved_doc_filter = filter_ptr,
            .resolved_doc_filter_wire_context = context,
            .graph_table_read_authorizer = authorizer.iface(),
        },
        null,
        &.{},
        .{},
        .read_index,
    );
    defer admission.deinit();
    const hits = try hydrateHitsForResultNodes(alloc, &admission, nodes[0..], true, &.{});
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

    const denying_authorizer = Authorizer{ .allow_entities = false };
    var denied_admission = GraphNodeAdmissionContext.init(
        alloc,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        0,
        .{ .graph_table_read_authorizer = denying_authorizer.iface() },
        null,
        &.{},
        .{},
        .read_index,
    );
    defer denied_admission.deinit();
    const denied_nodes = [_]graph_node_admission.NodeRef{
        .{ .key = "person/ada", .table = "entities", .external = true },
    };
    const denied = try denied_admission.iface().filterAlloc(alloc, &denied_nodes);
    defer alloc.free(denied);
    try std.testing.expect(!denied[0]);
    try std.testing.expectEqual(@as(u32, 1), state.cross_table_calls);
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
    var write_index: usize = out.len;
    errdefer {
        for (out[write_index..]) |item| alloc.free(item);
        alloc.free(out);
    }

    var cursor: ?u32 = id;
    while (cursor) |current_id| {
        write_index -= 1;
        out[write_index] = try alloc.dupe(u8, path_states[current_id].key);
        cursor = path_states[current_id].parent;
    }
    return out;
}

fn reconstructPathTables(
    alloc: std.mem.Allocator,
    path_states: []const PathState,
    id: u32,
) ![]?[]const u8 {
    var probe: ?u32 = id;
    while (probe) |current_id| {
        if (path_states[current_id].table != null) break;
        probe = path_states[current_id].parent;
    }
    if (probe == null) return &.{};

    const out = try alloc.alloc(?[]const u8, pathLength(path_states, id));
    var write_index: usize = out.len;
    errdefer {
        for (out[write_index..]) |table| if (table) |value| alloc.free(value);
        alloc.free(out);
    }

    var cursor: ?u32 = id;
    while (cursor) |current_id| {
        write_index -= 1;
        out[write_index] = if (path_states[current_id].table) |table|
            try alloc.dupe(u8, table)
        else
            null;
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
    var write_index: usize = out.len;
    errdefer {
        for (out[write_index..]) |edge| freeOwnedPathEdge(alloc, edge);
        alloc.free(out);
    }

    var cursor: ?u32 = id;
    while (cursor) |current_id| {
        if (path_states[current_id].incoming_edge) |edge| {
            write_index -= 1;
            out[write_index] = try clonePathEdge(alloc, edge);
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
    source_table: []const u8,
    path: db_mod.types.GraphPath,
    exclude: *graph_node_identity.Map(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    for (path.nodes, 0..) |node, i| {
        if (exclude.contains(.{
            .table = graphPathNodeTable(path, i),
            .key = node,
        })) return true;
    }
    if (exclude_edge_set.count() == 0) return false;
    for (path.edges, 0..) |edge, i| {
        const edge_key = try allocEdgeExclusionKey(
            alloc,
            graphPathNodeTable(path, i) orelse source_table,
            edge.source,
            edge.target,
            edge.edge_type,
        );
        defer alloc.free(edge_key);
        if (exclude_edge_set.contains(edge_key)) return true;
    }
    return false;
}

fn graphPatternMatchIsExcluded(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    match: db_mod.types.GraphPatternMatch,
    exclude: *graph_node_identity.Map(void),
    source_exclude: *std.StringHashMapUnmanaged(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    for (match.bindings) |binding| {
        if (exclude.contains(.{
            .table = canonicalGraphNodeTable(source_table, binding.node.table),
            .key = binding.node.key,
        })) return true;
        if (binding.node.path) |path| {
            for (path) |node| {
                if (source_exclude.contains(node)) return true;
            }
        }
        if (exclude_edge_set.count() > 0) {
            if (binding.node.path_edges) |edges| {
                for (edges) |edge| {
                    const edge_key = try allocEdgeExclusionKey(
                        alloc,
                        source_table,
                        edge.source,
                        edge.target,
                        edge.edge_type,
                    );
                    defer alloc.free(edge_key);
                    if (exclude_edge_set.contains(edge_key)) return true;
                }
            }
        }
    }
    if (exclude_edge_set.count() == 0) return false;
    for (match.path) |edge| {
        const edge_key = try allocEdgeExclusionKey(
            alloc,
            source_table,
            edge.source,
            edge.target,
            edge.edge_type,
        );
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

fn cloneGraphAggregates(
    alloc: std.mem.Allocator,
    aggregates: []const db_mod.types.GraphAggregateResult,
) ![]db_mod.types.GraphAggregateResult {
    const out = try alloc.alloc(db_mod.types.GraphAggregateResult, aggregates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*aggregate| aggregate.deinit(alloc);
        alloc.free(out);
    }
    for (aggregates, 0..) |aggregate, i| {
        const distinct_values = try alloc.alloc(graph_node_identity.Ref, aggregate.distinct_values.len);
        var distinct_initialized: usize = 0;
        errdefer {
            for (distinct_values[0..distinct_initialized]) |identity| {
                if (identity.table) |table| alloc.free(table);
                alloc.free(identity.key);
            }
            if (distinct_values.len > 0) alloc.free(distinct_values);
        }
        for (aggregate.distinct_values, 0..) |identity, identity_index| {
            const table = if (identity.table) |table_name| try alloc.dupe(u8, table_name) else null;
            errdefer if (table) |table_name| alloc.free(table_name);
            distinct_values[identity_index] = .{
                .table = table,
                .key = try alloc.dupe(u8, identity.key),
            };
            distinct_initialized += 1;
        }
        out[i] = .{
            .name = try alloc.dupe(u8, aggregate.name),
            .value = aggregate.value,
            .exact = aggregate.exact,
            .distinct_values = distinct_values,
        };
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
        bindings[i] = try cloneGraphPatternBinding(alloc, binding);
        bindings_initialized += 1;
    }

    const path = try dupPathEdges(alloc, match.path);
    errdefer freePathEdges(alloc, path);

    const null_aliases = try cloneOwnedStrings(alloc, match.null_aliases);
    errdefer {
        for (null_aliases) |alias| alloc.free(alias);
        if (null_aliases.len > 0) alloc.free(null_aliases);
    }

    return .{
        .bindings = bindings,
        .path = path,
        .null_aliases = null_aliases,
    };
}

fn cloneGraphPatternBinding(
    alloc: std.mem.Allocator,
    binding: db_mod.types.GraphPatternBinding,
) !db_mod.types.GraphPatternBinding {
    const alias = try alloc.dupe(u8, binding.alias);
    errdefer alloc.free(alias);
    const node = try cloneGraphNode(alloc, binding.node);
    return .{ .alias = alias, .node = node };
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
    const key = try alloc.dupe(u8, node.key);
    errdefer alloc.free(key);
    const path = if (node.path) |value| try dupPath(alloc, value) else null;
    errdefer if (path) |value| freePathArray(alloc, value);
    const path_tables = if (node.path_tables) |value| try dupOptionalStrings(alloc, value) else null;
    errdefer if (path_tables) |value| freeOptionalStrings(alloc, value);
    const path_edges = if (node.path_edges) |value| try dupPathEdges(alloc, value) else null;
    errdefer if (path_edges) |value| freePathEdges(alloc, value);
    const provenance = if (node.provenance) |value| try dupPath(alloc, value) else null;
    errdefer if (provenance) |value| freePathArray(alloc, value);
    const table = if (node.table) |value| try alloc.dupe(u8, value) else null;
    errdefer if (table) |value| alloc.free(value);

    return .{
        .key = key,
        .depth = node.depth,
        .distance = node.distance,
        .path = path,
        .path_tables = path_tables,
        .path_edges = path_edges,
        .provenance = provenance,
        .table = table,
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

fn cloneOwnedStrings(alloc: std.mem.Allocator, items: []const []const u8) ![][]u8 {
    const out = try alloc.alloc([]u8, items.len);
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

fn dupOptionalStrings(
    alloc: std.mem.Allocator,
    items: []const ?[]const u8,
) ![]?[]const u8 {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(?[]const u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| if (item) |value| alloc.free(value);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = if (item) |value| try alloc.dupe(u8, value) else null;
        initialized += 1;
    }
    return out;
}

fn freeOptionalStrings(
    alloc: std.mem.Allocator,
    items: []const ?[]const u8,
) void {
    for (items) |item| if (item) |value| alloc.free(value);
    if (items.len > 0) alloc.free(items);
}

fn graphPathNodeTable(
    path: db_mod.types.GraphPath,
    index: usize,
) ?[]const u8 {
    if (path.node_tables.len != path.nodes.len) return null;
    return path.node_tables[index];
}

fn optionalGraphTableEql(
    left: ?[]const u8,
    right: ?[]const u8,
) bool {
    if ((left == null) != (right == null)) return false;
    if (left) |value| return std.mem.eql(u8, value, right.?);
    return true;
}

fn optionalStringsContainValue(items: []const ?[]const u8) bool {
    for (items) |item| if (item != null) return true;
    return false;
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
    const source = try alloc.dupe(u8, edge.source);
    errdefer alloc.free(source);
    const target = try alloc.dupe(u8, edge.target);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, edge.edge_type);
    errdefer alloc.free(edge_type);
    const metadata = if (edge.metadata.len > 0) try alloc.dupe(u8, edge.metadata) else "";
    errdefer if (metadata.len > 0) alloc.free(metadata);

    return .{
        .source = source,
        .target = target,
        .edge_type = edge_type,
        .weight = edge.weight,
        .metadata = metadata,
    };
}

fn freeOwnedPathEdge(alloc: std.mem.Allocator, edge: graph_query_mod.PathEdgeInfo) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
}

test "distributed graph expand request preserves algebraic semiring planning flag" {
    const alloc = std.testing.allocator;
    var frontier = [_]FrontierState{.{
        .key = try alloc.dupe(u8, "doc:a"),
        .table = try alloc.dupe(u8, "entities"),
    }};
    defer frontier[0].deinit(alloc);
    const frontier_ids = [_]u32{0};
    const excluded_nodes = [_]GraphNodeIdentity{.{
        .key = @constCast("doc:seen"),
        .table = @constCast("entities"),
    }};
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
    }, frontier[0..], frontier_ids[0..], &excluded_nodes, &.{}, false);
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
    try std.testing.expectEqualStrings("entities", req.frontier[0].table.?);
    try std.testing.expectEqualStrings("doc:seen", req.exclude_nodes[0].key);
    try std.testing.expectEqualStrings("entities", req.exclude_nodes[0].table.?);
    try std.testing.expectEqual(@as(?f64, 1.25), req.params.min_weight);
    try std.testing.expectEqual(@as(?f64, 9.5), req.params.max_weight);
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
    try std.testing.expectEqualStrings("entities", parsed.frontier[0].table.?);
    try std.testing.expectEqualStrings("doc:seen", parsed.exclude_nodes[0].key);
    try std.testing.expectEqualStrings("entities", parsed.exclude_nodes[0].table.?);
    try std.testing.expectEqual(@as(?u64, 12345), parsed.identity_read_generation);
    try std.testing.expect(parsed.resolved_doc_filter != null);
    try std.testing.expect(parsed.resolved_doc_filter_owned);
    try std.testing.expect(parsed.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
    try std.testing.expect(parsed.params.algebraic_semiring);
    try std.testing.expectEqual(@as(?f64, 1.25), parsed.params.min_weight);
    try std.testing.expectEqual(@as(?f64, 9.5), parsed.params.max_weight);
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
    try std.testing.expect(search_req.query == .match_all);
    try std.testing.expectEqual(@as(?f64, 1.25), search_req.graph_queries[0].query.params.min_weight);
    try std.testing.expectEqual(@as(?f64, 9.5), search_req.graph_queries[0].query.params.max_weight);
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
        .include_stored = false,
        .fields = &.{ "title", "summary" },
        .include_all_fields = false,
        .include_hits = false,
        .incoming_index_name = "graph_idx",
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
    try std.testing.expect(!parsed_hydrate.include_stored);
    try std.testing.expect(!parsed_hydrate.include_all_fields);
    try std.testing.expectEqual(@as(usize, 2), parsed_hydrate.fields.len);
    try std.testing.expectEqualStrings("title", parsed_hydrate.fields[0]);
    try std.testing.expectEqualStrings("summary", parsed_hydrate.fields[1]);
    try std.testing.expect(!parsed_hydrate.include_hits);
    try std.testing.expectEqualStrings("graph_idx", parsed_hydrate.incoming_index_name);

    var hydrate_response = GraphHydrateResponse{
        .has_incoming = try alloc.dupe(bool, &.{true}),
    };
    defer hydrate_response.deinit(alloc);
    const hydrate_response_encoded = try encodeGraphHydrateResponse(alloc, hydrate_response);
    defer alloc.free(hydrate_response_encoded);
    var parsed_hydrate_response = try parseGraphHydrateResponse(alloc, hydrate_response_encoded);
    defer parsed_hydrate_response.deinit(alloc);
    try std.testing.expectEqualSlices(bool, &.{true}, parsed_hydrate_response.has_incoming);

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
        .edge_types = try dupConstStrings(alloc, &.{ "links", "mentions" }),
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
    try std.testing.expectEqual(@as(usize, 2), parsed.edge_types.len);
    try std.testing.expectEqualStrings("links", parsed.edge_types[0]);
    try std.testing.expectEqualStrings("mentions", parsed.edge_types[1]);
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
            try std.testing.expectEqual(@as(usize, 1), req.edge_types.len);
            try std.testing.expectEqualStrings("links", req.edge_types[0]);
            try std.testing.expectEqual(graph_mod.EdgeDirection.out, req.direction);
            try std.testing.expectEqual(@as(u64, 0), req.topology_epoch);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            return .{ .edges = @constCast((&[_]graph_mod.Edge{})[0..]) };
        }
    };

    var state = TestState{};
    var admission = GraphNodeAdmissionContext.init(
        alloc,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        0,
        .{ .identity_read_generation = 12345 },
        null,
        &.{},
        .{},
        .read_index,
    );
    defer admission.deinit();

    const reader = DistributedEdgeReader{
        .catalog = FakeCatalog.iface(),
        .worker = FakeWorker.iface(&state),
        .source_table = "docs",
        .index_name = "graph_idx",
        .consistency = .read_index,
        .admission = &admission,
    };

    const edges = try reader.getEdges(alloc, null, "doc:a", &.{"links"}, .out);
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

    var kept = try filterGraphSearchResult(
        alloc,
        "docs",
        src,
        &.{.{ .key = @constCast("doc:z") }},
        &.{},
    );
    defer kept.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), kept.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), kept.paths.len);
    try std.testing.expectEqual(@as(usize, 1), kept.matches.len);
    try std.testing.expectEqualStrings("edge:a:b", kept.nodes[0].provenance.?[0]);
    try std.testing.expectEqualStrings("doc:a", kept.paths[0].nodes[0]);
    try std.testing.expectEqualStrings("target", kept.matches[0].bindings[0].alias);

    var dropped_node = try filterGraphSearchResult(
        alloc,
        "docs",
        src,
        &.{.{ .key = @constCast("doc:b") }},
        &.{},
    );
    defer dropped_node.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.paths.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_node.matches.len);

    const excluded_edge = try allocEdgeExclusionKey(
        alloc,
        "docs",
        "doc:a",
        "doc:b",
        "links",
    );
    defer alloc.free(excluded_edge);
    var dropped_edge = try filterGraphSearchResult(
        alloc,
        "docs",
        src,
        &.{},
        &.{excluded_edge},
    );
    defer dropped_edge.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.paths.len);
    try std.testing.expectEqual(@as(usize, 0), dropped_edge.matches.len);
}

test "distributed graph filtering scopes equal keys by table" {
    const alloc = std.testing.allocator;
    var nodes = [_]graph_query_mod.GraphResultNode{
        .{
            .key = "shared",
            .depth = 1,
            .distance = 1,
            .path = null,
            .path_edges = null,
        },
        .{
            .key = "shared",
            .depth = 1,
            .distance = 1,
            .path = null,
            .path_edges = null,
            .table = "entities",
        },
    };
    var hits = [_]db_mod.types.SearchHit{
        .{ .id = @constCast("shared") },
        .{
            .id = @constCast("shared"),
            .source_table = @constCast("entities"),
        },
    };
    const src = db_mod.types.GraphSearchResult{
        .name = @constCast("graph"),
        .nodes = nodes[0..],
        .paths = &.{},
        .matches = &.{},
        .hits = hits[0..],
        .total_hits = 2,
    };

    var source_excluded = try filterGraphSearchResult(
        alloc,
        "docs",
        src,
        &.{.{ .key = @constCast("shared") }},
        &.{},
    );
    defer source_excluded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), source_excluded.nodes.len);
    try std.testing.expectEqualStrings("entities", source_excluded.nodes[0].table.?);
    try std.testing.expectEqual(@as(usize, 1), source_excluded.hits.len);
    try std.testing.expectEqualStrings("entities", source_excluded.hits[0].source_table.?);

    var external_excluded = try filterGraphSearchResult(
        alloc,
        "docs",
        src,
        &.{.{
            .key = @constCast("shared"),
            .table = @constCast("entities"),
        }},
        &.{},
    );
    defer external_excluded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), external_excluded.nodes.len);
    try std.testing.expect(external_excluded.nodes[0].table == null);
    try std.testing.expectEqual(@as(usize, 1), external_excluded.hits.len);
    try std.testing.expect(external_excluded.hits[0].source_table == null);
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
        "docs",
        req,
        base_result,
        &.{},
        .{ .ref = "$query_results", .limit = 0 },
    ));

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        "docs",
        .{ .limit = 10 },
        base_result,
        &.{},
        .{ .ref = "$query_results", .limit = 1 },
    ));

    const limited = try resolveResultRefKeys(
        alloc,
        "docs",
        req,
        base_result,
        &.{},
        .{ .ref = "$query_results", .limit = 1 },
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
        "docs",
        req,
        base_result,
        &.{},
        .{ .ref = "$query_results", .limit = 0 },
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
        "docs",
        .{ .identity_read_generation = 9 },
        base_result,
        &.{graph_result},
        .{ .ref = "$graph_results.first_hop", .limit = 0 },
    ));

    try std.testing.expectError(error.UnsupportedQueryRequest, resolveResultRefKeys(
        alloc,
        "docs",
        .{},
        base_result,
        &.{graph_result},
        .{ .ref = "$graph_results.first_hop", .limit = 1 },
    ));

    const limited = try resolveResultRefKeys(
        alloc,
        "docs",
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
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .params = .{},
                },
            },
        },
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = @constCast((&[_]db_mod.types.SearchHit{})[0..]),
        .total_hits = 0,
        .graph_results = @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]),
    };
    try std.testing.expectError(error.UnsupportedQueryRequest, rejectUnstampedResultRefs(req, base_result));
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
    try rejectUnstampedResultRefs(stamped, base_result);

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
    try rejectUnstampedResultRefs(explicit_keys, base_result);
    try requireStampedCrossRangeRequest(explicit_keys, base_result);
}

pub fn testPerShardSnapshotsAcrossGraphPhases(alloc: std.mem.Allocator) !void {
    const TestState = struct {
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = "m" },
            .{ .group_id = 22, .table_id = 7, .start_key = "m", .end_key = null },
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
            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);
            state.expand_calls += 1;

            if (group_id == 11) {
                try std.testing.expectEqual(@as(?u64, 101), req.identity_read_generation);
                try std.testing.expectEqualStrings("a", req.frontier[0].key);
                const nodes = try alloc_inner.alloc(graph_query_mod.GraphResultNode, 1);
                nodes[0] = try initGraphResultNode(alloc_inner, "z", null, 1, 1, null, null, null);
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

            try std.testing.expectEqual(@as(u64, 22), group_id);
            try std.testing.expectEqual(@as(?u64, 202), req.identity_read_generation);
            try std.testing.expectEqualStrings("z", req.frontier[0].key);
            return .{ .expansions = @constCast((&[_]GraphExpansion{})[0..]) };
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
            state.hydrate_calls += 1;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 22), group_id);
            try std.testing.expectEqual(@as(?u64, 202), req.identity_read_generation);
            try std.testing.expectEqual(@as(usize, 1), req.keys.len);
            try std.testing.expectEqualStrings("z", req.keys[0]);
            const hits = try alloc_inner.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{
                .id = try alloc_inner.dupe(u8, "z"),
                .stored_data = try alloc_inner.dupe(u8, "{\"title\":\"snapshot target\"}"),
            };
            return .{ .hits = hits };
        }
    };

    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .result_ref = .{ .ref = "$query_results", .limit = 1 } },
                    .include_documents = true,
                    .params = .{ .max_depth = 2 },
                },
            },
        },
    };
    var base_hits = [_]db_mod.types.SearchHit{.{ .id = @constCast("a") }};
    const tokens = [_]db_mod.types.ShardIdentityReadGeneration{
        .{ .group_id = 11, .generation = 101 },
        .{ .group_id = 22, .generation = 202 },
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = base_hits[0..],
        .total_hits = 1,
        .shard_identity_read_generations = @constCast(tokens[0..]),
    };
    var incomplete_result = base_result;
    incomplete_result.shard_identity_read_generations = @constCast(tokens[0..1]);
    try std.testing.expectError(
        error.TopologyChanged,
        validateSourceSnapshotGroupSet(alloc, FakeCatalog.iface(), "docs", incomplete_result),
    );

    var state = TestState{};
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
    try std.testing.expectEqual(@as(u32, 1), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 1), results[0].nodes.len);
    try std.testing.expectEqualStrings("z", results[0].nodes[0].key);
    try std.testing.expectEqual(@as(usize, 1), results[0].hits.len);
    try std.testing.expectEqualStrings("z", results[0].hits[0].id);
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
                    .include_documents = true,
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

test "distributed graph executes result dependencies before declaration order" {
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
            _: u64,
            _: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.expand_calls += 1;
            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);

            const next_key = if (std.mem.eql(u8, req.name, "seed")) blk: {
                try std.testing.expectEqualStrings("doc:a", req.frontier[0].key);
                break :blk "doc:b";
            } else if (std.mem.eql(u8, req.name, "dependent")) blk: {
                try std.testing.expectEqualStrings("doc:b", req.frontier[0].key);
                break :blk "doc:c";
            } else return error.TestUnexpectedResult;

            const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
            nodes[0] = .{
                .key = try alloc.dupe(u8, next_key),
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
                    .hits = &.{},
                    .total_hits = 1,
                },
            };
            return .{ .expansions = expansions };
        }

        fn executeGraphHydrate(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: u64,
            _: []const u8,
            req: GraphHydrateRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphHydrateResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            state.hydrate_calls += 1;
            const hits = try alloc.alloc(db_mod.types.SearchHit, req.keys.len);
            for (req.keys, 0..) |key, i| hits[i] = .{ .id = try alloc.dupe(u8, key) };
            return .{ .hits = hits };
        }
    };

    var state = TestState{};
    const req = db_mod.types.SearchRequest{
        .graph_queries = &.{
            .{
                .name = "dependent",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .result_ref = .{ .ref = "$graph_results.seed", .limit = 1 } },
                    .params = .{ .max_depth = 1 },
                },
            },
            .{
                .name = "seed",
                .query = .{
                    .query_type = .traverse,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .max_depth = 1 },
                },
            },
        },
        .identity_read_generation = 9,
    };
    const base_result = db_mod.types.SearchResult{
        .alloc = std.testing.allocator,
        .hits = &.{},
        .total_hits = 0,
        .identity_read_generation = 9,
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

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("seed", results[0].name);
    try std.testing.expectEqualStrings("doc:b", results[0].nodes[0].key);
    try std.testing.expectEqualStrings("dependent", results[1].name);
    try std.testing.expectEqualStrings("doc:c", results[1].nodes[0].key);
    try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
    try std.testing.expectEqual(@as(u32, 0), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 0), results[0].hits.len);
    try std.testing.expectEqual(@as(usize, 0), results[1].hits.len);
}

test "distributed graph traverse routes cross-table frontier by table generation" {
    const TestState = struct {
        expand_calls: u32 = 0,
        hydrate_calls: u32 = 0,
    };

    const FakeCatalog = struct {
        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 7, .name = "docs", .placement_role = "data" },
            .{
                .table_id = 8,
                .name = "entities",
                .placement_role = "data",
                .indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}",
            },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null },
            .{ .group_id = 22, .table_id = 8, .start_key = "", .end_key = null },
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
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: GraphExpandRequest,
            _: raft_mod.ReadConsistency,
        ) !GraphExpandResponse {
            const state: *TestState = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), req.frontier.len);
            try std.testing.expect(req.topology_epoch != 0);
            state.expand_calls += 1;

            const next_key: []const u8 = if (state.expand_calls == 1) blk: {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(u64, 11), group_id);
                try std.testing.expectEqualStrings("doc:a", req.frontier[0].key);
                try std.testing.expect(req.frontier[0].table == null);
                break :blk "shared";
            } else blk: {
                try std.testing.expectEqual(@as(u32, 2), state.expand_calls);
                try std.testing.expectEqualStrings("entities", table_name);
                try std.testing.expectEqual(@as(u64, 22), group_id);
                try std.testing.expectEqualStrings("shared", req.frontier[0].key);
                try std.testing.expectEqualStrings("entities", req.frontier[0].table.?);
                break :blk "doc:c";
            };
            const next_table: ?[]const u8 = if (state.expand_calls == 1)
                "entities"
            else
                null;

            const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, 1);
            errdefer alloc.free(nodes);
            nodes[0] = try initGraphResultNode(
                alloc,
                next_key,
                next_table,
                state.expand_calls,
                1,
                null,
                null,
                null,
            );
            errdefer nodes[0].deinit(alloc);

            const expansions = try alloc.alloc(GraphExpansion, 1);
            errdefer alloc.free(expansions);
            const frontier_key = try alloc.dupe(u8, req.frontier[0].key);
            errdefer alloc.free(frontier_key);
            const name = try alloc.dupe(u8, req.name);
            errdefer alloc.free(name);
            expansions[0] = .{
                .frontier_id = req.frontier[0].id,
                .frontier_key = frontier_key,
                .graph_result = .{
                    .name = name,
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
            state.hydrate_calls += 1;
            try std.testing.expectEqualStrings("entities", table_name);
            try std.testing.expectEqual(@as(u64, 22), group_id);
            try std.testing.expectEqual(@as(usize, 1), req.keys.len);
            try std.testing.expectEqualStrings("doc:c", req.keys[0]);

            const id = try alloc.dupe(u8, "doc:c");
            errdefer alloc.free(id);
            const stored_data = try alloc.dupe(u8, "{\"kind\":\"entity\"}");
            errdefer alloc.free(stored_data);
            const hits = try alloc.alloc(db_mod.types.SearchHit, 1);
            hits[0] = .{ .id = id, .stored_data = stored_data };
            return .{ .hits = hits };
        }
    };

    const alloc = std.testing.allocator;
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
                    .include_documents = true,
                },
            },
        },
        .identity_read_generation = 9,
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
    try std.testing.expectEqual(@as(u32, 1), state.hydrate_calls);
    try std.testing.expectEqual(@as(usize, 1), results[0].nodes.len);
    try std.testing.expectEqualStrings("entities", results[0].nodes[0].table.?);
    try std.testing.expectEqual(@as(usize, 1), results[0].hits.len);
    try std.testing.expectEqualStrings("entities", results[0].hits[0].source_table.?);
}

test "distributed graph path materialization preserves table provenance" {
    const alloc = std.testing.allocator;
    var state = QueryState{ .name = try alloc.dupe(u8, "walk") };
    defer state.deinit(alloc);

    const root = try appendRootPathState(alloc, &state, "doc:a", null);
    var shared = try initPathState(alloc, "shared", "entities", 1, 1, 1, root, null);
    var shared_owned = true;
    errdefer if (shared_owned) shared.deinit(alloc);
    try state.path_states.append(alloc, shared);
    shared_owned = false;
    const shared_id: u32 = @intCast(state.path_states.items.len - 1);

    var target = try initPathState(alloc, "doc:c", "entities", 2, 2, 2, shared_id, null);
    var target_owned = true;
    errdefer if (target_owned) target.deinit(alloc);
    try state.path_states.append(alloc, target);
    target_owned = false;
    const target_id: u32 = @intCast(state.path_states.items.len - 1);

    var node = try materializePathStateNode(alloc, &state, target_id);
    defer node.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), node.path.?.len);
    try std.testing.expectEqualStrings("doc:a", node.path.?[0]);
    try std.testing.expect(node.path_tables.?[0] == null);
    try std.testing.expectEqualStrings("entities", node.path_tables.?[1].?);
    try std.testing.expectEqualStrings("entities", node.path_tables.?[2].?);
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
                    .include_documents = true,
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
            try std.testing.expect(req.timeout_ms != null);
            try std.testing.expect(req.cancellation != null);
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
            try std.testing.expect(req.timeout_ms != null);
            try std.testing.expect(req.cancellation != null);
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
    var cancellation = std.atomic.Value(bool).init(false);

    const req = db_mod.types.SearchRequest{
        .graph_queries = &[_]db_mod.types.NamedGraphQuery{
            .{
                .name = "walk",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "graph_idx",
                    .start_nodes = .{ .keys = &[_][]const u8{ "doc:a", "doc:n" } },
                    .params = .{},
                    .include_documents = true,
                },
            },
        },
        .identity_read_generation = 77,
        .execution_deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s,
        .cancellation = CancellationToken.fromAtomic(&cancellation),
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

    cancellation.store(true, .release);
    try std.testing.expectError(error.Cancelled, executeCrossRange(
        std.testing.allocator,
        FakeCatalog.iface(),
        FakeWorker.iface(&state),
        "docs",
        req,
        base_result,
        .read_index,
    ));
    try std.testing.expectEqual(@as(u32, 2), state.expand_calls.load(.monotonic));
}
