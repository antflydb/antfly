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

pub const std = @import("std");
pub const CancellationToken = @import("antfly_cancellation").CancellationToken;
pub const db_mod = struct {
    pub const types = @import("../storage/db/types.zig");
    pub const doc_filter_wire = @import("../storage/db/doc_filter_wire.zig");
    pub const algebraic = @import("../storage/db/algebraic/mod.zig");
};
pub const graph_query_mod = @import("../graph/query.zig");
pub const graph_mod = @import("../graph/graph.zig");
pub const graph_node_identity = @import("../graph/node_identity.zig");
pub const graph_pattern_mod = @import("../graph/pattern.zig");
pub const graph_paths_mod = @import("../graph/paths.zig");
pub const algebraic_ir = db_mod.algebraic.ir;
pub const algebraic_law = db_mod.algebraic.law;
pub const algebraic_planner = db_mod.algebraic.planner;
pub const platform_time = @import("antfly_platform").time;
pub const query_contract = @import("query_contract.zig");
pub const GraphIndexIdentity = struct {
    incarnation: u64 = 0,
    config_hash: u64 = 0,

    pub fn valid(self: @This()) bool {
        return self.incarnation != 0 and self.config_hash != 0;
    }

    pub fn eql(self: @This(), other: @This()) bool {
        return self.incarnation == other.incarnation and self.config_hash == other.config_hash;
    }
};

pub fn executionDeadlineFromTimeoutMs(timeout_ms: ?u32) ?u64 {
    const value = timeout_ms orelse return null;
    const duration_ns = std.math.mul(u64, value, std.time.ns_per_ms) catch std.math.maxInt(u64);
    return std.math.add(u64, platform_time.monotonicNs(), duration_ns) catch std.math.maxInt(u64);
}

pub const GraphExpandRequest = struct {
    name: []u8,
    index_name: []u8,
    frontier: []GraphFrontierItem,
    exclude_nodes: []GraphNodeIdentity,
    exclude_edges: [][]u8,
    target_constraint_keys: [][]u8 = &.{},
    params: graph_query_mod.QueryParams,
    metrics: []graph_query_mod.GraphMetricRead = &.{},
    include_metric_status: bool = false,
    defer_result_limit: bool = false,
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
    execution_deadline_ns: ?u64 = null,
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
        freeGraphMetricReads(alloc, self.metrics);
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
    incoming_index_identity: GraphIndexIdentity = .{},
    incoming_index_name_owned: bool = false,
    resolved_doc_filter: ?*const anyopaque = null,
    resolved_doc_filter_owned: bool = false,
    resolved_doc_filter_wire_context: ?db_mod.types.ResolvedDocFilterWireContext = null,
    timeout_ms: ?u32 = null,
    execution_deadline_ns: ?u64 = null,
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
    incoming_index_identity: GraphIndexIdentity = .{},

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
    max_edges: u32 = graph_pattern_mod.default_max_explored_edges,
    max_owned_bytes: u32 = graph_pattern_mod.default_max_explored_edge_bytes,
    timeout_ms: ?u32 = null,
    execution_deadline_ns: ?u64 = null,
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

pub const GraphExpandRequestJson = struct {
    name: []const u8,
    index_name: []const u8,
    frontier: []const GraphFrontierItemJson,
    exclude_nodes: []const GraphNodeIdentityJson = &.{},
    exclude_edges: []const []const u8 = &.{},
    target_constraint_keys: []const []const u8 = &.{},
    metrics: []const GraphMetricReadJson = &.{},
    include_metric_status: bool = false,
    defer_result_limit: bool = false,
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    _resolved_doc_filter: ?std.json.Value = null,
    params: GraphExpandParamsJson,
    tensor_access_path: ?GraphTensorAccessPathJson = null,
    tensor_program: ?std.json.Value = null,
};

pub const GraphFrontierItemJson = struct {
    id: u32,
    key: []const u8,
    table: ?[]const u8 = null,
    depth: u32 = 0,
    distance: f64 = 0,
};

pub const GraphNodeIdentityJson = struct {
    key: []const u8,
    table: ?[]const u8 = null,
};

pub const GraphExpandParamsJson = struct {
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

pub const GraphMetricReadJson = struct {
    name: []const u8,
    freshness: []const u8 = "published",
};

pub const GraphTensorAccessPathJson = struct {
    owner: []const u8,
    layout: []const u8,
    fragments: []const []const u8,
    output_dims: []const []const u8,
    law_ids: []const []const u8,
};

pub const GraphExpandResponseJson = struct {
    expansions: []const GraphExpansionJson,
};

pub const GraphExpansionJson = struct {
    frontier_id: u32,
    frontier_key: []const u8,
    name: []const u8,
    total: u32,
    nodes: []const graph_query_mod.GraphResultNode,
    hits: []const db_mod.types.SearchHit = &.{},
    metric_status: []const db_mod.types.GraphMetricStatus = &.{},
};

pub const GraphHydrateRequestJson = struct {
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
    incoming_index_incarnation: u64 = 0,
    incoming_index_config_hash: u64 = 0,
    _resolved_doc_filter: ?std.json.Value = null,
};

pub const GraphHydrateResponseJson = struct {
    hits: []const db_mod.types.SearchHit = &.{},
    has_incoming: []const bool = &.{},
    incoming_index_incarnation: u64 = 0,
    incoming_index_config_hash: u64 = 0,
};

pub const GraphEdgesRequestJson = struct {
    index_name: []const u8,
    key: []const u8,
    edge_types: []const []const u8 = &.{},
    direction: []const u8 = "out",
    topology_epoch: u64 = 0,
    identity_read_generation: ?u64 = null,
    max_edges: u32 = graph_pattern_mod.default_max_explored_edges,
    max_owned_bytes: u32 = graph_pattern_mod.default_max_explored_edge_bytes,
    tensor_access_path: GraphTensorAccessPathJson,
    tensor_program: std.json.Value,
};

pub const GraphEdgeJson = struct {
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata: []const u8 = "",
};

pub const GraphEdgesResponseJson = struct {
    edges: []const GraphEdgeJson,
};

pub fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
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
        .incoming_index_identity = .{
            .incarnation = parsed.value.incoming_index_incarnation,
            .config_hash = parsed.value.incoming_index_config_hash,
        },
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
        .incoming_index_incarnation = res.incoming_index_identity.incarnation,
        .incoming_index_config_hash = res.incoming_index_identity.config_hash,
    });
}

pub fn identityGenerationFromResolvedFilterEnvelope(
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
    try validateGraphEdgesReadLimits(parsed.value.max_edges, parsed.value.max_owned_bytes);
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
        .max_edges = parsed.value.max_edges,
        .max_owned_bytes = parsed.value.max_owned_bytes,
        .tensor_access_path = tensor_access_path,
        .tensor_program = tensor_program,
    };
}

pub fn validateGraphEdgesReadLimits(max_edges: u32, max_owned_bytes: u32) !void {
    if (max_edges == 0 or max_edges > graph_pattern_mod.default_max_explored_edges or
        max_owned_bytes == 0 or max_owned_bytes > graph_pattern_mod.default_max_explored_edge_bytes)
        return error.InvalidQueryRequest;
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

pub fn cloneGraphPath(
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

pub fn dupeGraphPathEdge(
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
        .traversal_direction = edge.traversal_direction,
    };
}

pub fn freeOwnedGraphPathEdge(
    alloc: std.mem.Allocator,
    edge: graph_paths_mod.PathEdge,
) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
}

pub fn graphPathTraversalDirectionTag(direction: ?graph_mod.EdgeDirection) u8 {
    return if (direction) |value| switch (value) {
        .out => 1,
        .in => 2,
        .both => 3,
    } else 0;
}

pub fn allocEdgeExclusionKey(
    alloc: std.mem.Allocator,
    from: graph_node_identity.Ref,
    to: graph_node_identity.Ref,
    direction: ?graph_mod.EdgeDirection,
    edge_type: []const u8,
) ![]u8 {
    const from_table = from.table orelse return error.InvalidGraphPath;
    const to_table = to.table orelse return error.InvalidGraphPath;
    const direction_tag = [_]u8{graphPathTraversalDirectionTag(direction)};
    return try compositeIdentityAlloc(alloc, &.{
        "path-edge-v1",
        from_table,
        from.key,
        to_table,
        to.key,
        direction_tag[0..],
        edge_type,
    });
}

pub fn compositeIdentityAlloc(
    alloc: std.mem.Allocator,
    parts: []const []const u8,
) ![]u8 {
    const encoded_len = try compositeIdentityEncodedLen(parts);

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

pub fn compositeIdentityEncodedLen(parts: []const []const u8) !usize {
    var encoded_len: usize = 0;
    for (parts) |part| {
        encoded_len = std.math.add(usize, encoded_len, @sizeOf(u64)) catch
            return error.GraphIdentityTooLarge;
        encoded_len = std.math.add(usize, encoded_len, part.len) catch
            return error.GraphIdentityTooLarge;
    }
    return encoded_len;
}

pub fn enumSliceEql(comptime T: type, left: []const T, right: []const T) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (l != r) return false;
    }
    return true;
}

pub fn graphTensorAccessPathEql(left: algebraic_ir.PhysicalAccessPath, right: algebraic_ir.PhysicalAccessPath) bool {
    return std.mem.eql(u8, left.owner, right.owner) and
        left.layout == right.layout and
        enumSliceEql(algebraic_ir.TensorFragment, left.fragments, right.fragments) and
        enumSliceEql(algebraic_ir.Dimension, left.output_dims, right.output_dims) and
        enumSliceEql(algebraic_law.Id, left.law_ids, right.law_ids);
}

pub fn parseGraphTensorProgramJsonValueAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    const encoded = try jsonStringifyAlloc(alloc, value);
    defer alloc.free(encoded);
    return try query_contract.parseAlgebraicTensorProgramEnvelopeAlloc(alloc, encoded);
}

pub fn graphEdgesTensorProgramEnvelopeAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
) !query_contract.OwnedAlgebraicTensorProgramEnvelope {
    var plan = (try algebraic_planner.planGraphEdgesTensorProgramAlloc(alloc, index_name)) orelse return error.InvalidQueryRequest;
    defer plan.deinit(alloc);
    return try cloneGraphTensorProgramEnvelopeAlloc(alloc, plan.asProgram());
}

pub fn cloneGraphTensorProgramEnvelopeAlloc(
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

pub fn validateGraphExpandTensorAccessPathParts(
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

pub fn validateGraphEdgesTensorAccessPathParts(
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
    if (req.defer_result_limit) params.max_results = graph_query_mod.graph_metric_candidate_limit + 1;

    const name = try alloc.dupe(u8, req.name);
    errdefer alloc.free(name);
    const index_name = try alloc.dupe(u8, req.index_name);
    errdefer alloc.free(index_name);
    const metrics = try dupGraphMetricReads(alloc, req.metrics);
    errdefer freeGraphMetricReads(alloc, metrics);

    const graph_queries = try alloc.alloc(db_mod.types.NamedGraphQuery, 1);
    errdefer alloc.free(graph_queries);
    graph_queries[0] = .{
        .name = name,
        .query = .{
            .query_type = .neighbors,
            .index_name = index_name,
            .start_nodes = .{ .keys = frontier_keys },
            .params = params,
            .metrics = metrics,
            .include_metric_status = req.include_metric_status,
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
        .execution_deadline_ns = req.execution_deadline_ns orelse executionDeadlineFromTimeoutMs(req.timeout_ms),
        .cancellation = req.cancellation,
    };
}

pub fn parseGraphTensorAccessPathAlloc(
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
        freeGraphMetricReads(alloc, graph_query.query.metrics);
    }
    if (req.graph_queries.len > 0) alloc.free(req.graph_queries);
}

pub fn parseGraphMetricReads(
    alloc: std.mem.Allocator,
    metrics: []const GraphMetricReadJson,
) ![]graph_query_mod.GraphMetricRead {
    if (metrics.len == 0) return @constCast((&[_]graph_query_mod.GraphMetricRead{})[0..]);
    const out = try alloc.alloc(graph_query_mod.GraphMetricRead, metrics.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |metric| alloc.free(@constCast(metric.name));
        alloc.free(out);
    }
    for (metrics, 0..) |metric, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, metric.name),
            .freshness = if (std.mem.eql(u8, metric.freshness, "fresh"))
                .fresh
            else if (std.mem.eql(u8, metric.freshness, "published"))
                .published
            else
                return error.InvalidQueryRequest,
        };
        initialized += 1;
    }
    return out;
}

pub fn dupGraphMetricReads(
    alloc: std.mem.Allocator,
    metrics: []const graph_query_mod.GraphMetricRead,
) ![]graph_query_mod.GraphMetricRead {
    if (metrics.len == 0) return @constCast((&[_]graph_query_mod.GraphMetricRead{})[0..]);
    const out = try alloc.alloc(graph_query_mod.GraphMetricRead, metrics.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |metric| alloc.free(@constCast(metric.name));
        alloc.free(out);
    }
    for (metrics, 0..) |metric, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, metric.name),
            .freshness = metric.freshness,
        };
        initialized += 1;
    }
    return out;
}

pub fn freeGraphMetricReads(
    alloc: std.mem.Allocator,
    metrics: []const graph_query_mod.GraphMetricRead,
) void {
    for (metrics) |metric| alloc.free(@constCast(metric.name));
    if (metrics.len > 0) alloc.free(@constCast(metrics));
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
        .metrics = try parseGraphMetricReads(alloc, parsed.value.metrics),
        .include_metric_status = parsed.value.include_metric_status,
        .defer_result_limit = parsed.value.defer_result_limit,
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
            .metric_status = expansion.graph_result.metric_status,
        };
    }
    return try jsonStringifyAlloc(alloc, GraphExpandResponseJson{ .expansions = expansions });
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
    for (exclude_nodes) |identity| {
        _ = try exclude.putIfAbsent(alloc, .{
            .table = identity.table orelse source_table,
            .key = identity.key,
        }, {});
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
            .table = canonicalGraphNodeTable(source_table, node.table) orelse source_table,
            .key = node.key,
        })) continue;
        if (exclude_edge_set.count() > 0) {
            if (try graphResultNodeHasExcludedEdge(alloc, source_table, node, &exclude_edge_set)) continue;
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
        if (exclude.contains(.{ .table = hit.source_table orelse source_table, .key = hit.id })) continue;
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

    const metric_status = try cloneGraphMetricStatuses(alloc, src.metric_status);
    errdefer {
        for (metric_status) |*status| status.deinit(alloc);
        if (metric_status.len > 0) alloc.free(metric_status);
    }

    return .{
        .name = name,
        .nodes = owned_nodes,
        .paths = owned_paths,
        .matches = owned_matches,
        .hits = owned_hits,
        .total_hits = total_hits,
        .metric_status = metric_status,
    };
}

pub fn canonicalGraphNodeTable(source_table: []const u8, table: ?[]const u8) ?[]const u8 {
    const table_name = table orelse return null;
    if (std.mem.eql(u8, source_table, table_name)) return null;
    return table_name;
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

pub fn cloneGraphFrontierItemParts(
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

pub fn cloneGraphNodeIdentityParts(
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

pub fn dupKeys(alloc: std.mem.Allocator, keys: []const []const u8) ![][]u8 {
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

pub fn dupSortedUniqueKeys(alloc: std.mem.Allocator, keys: []const []const u8) ![][]u8 {
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

pub fn stringSliceLessThan(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.lessThan(u8, left, right);
}

pub fn freeKeys(alloc: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn dupConstStrings(alloc: std.mem.Allocator, items: []const []const u8) ![][]const u8 {
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

pub fn freeConstStrings(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(item);
    if (items.len > 0) alloc.free(items);
}

pub fn freePathArray(alloc: std.mem.Allocator, path: [][]const u8) void {
    for (path) |item| alloc.free(item);
    if (path.len > 0) alloc.free(path);
}

pub fn freePathEdges(alloc: std.mem.Allocator, edges: []graph_query_mod.PathEdgeInfo) void {
    for (edges) |edge| freeOwnedPathEdge(alloc, edge);
    if (edges.len > 0) alloc.free(edges);
}

pub fn cloneGraphNodes(
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

pub fn graphPathIsExcluded(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    path: db_mod.types.GraphPath,
    exclude: *graph_node_identity.Map(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    if (path.nodes.len == 0 or path.edges.len != path.nodes.len - 1)
        return error.InvalidGraphPath;
    for (path.nodes, 0..) |node, i| {
        if (exclude.contains(.{
            .table = graphPathNodeTable(path, i) orelse source_table,
            .key = node,
        })) return true;
    }
    if (exclude_edge_set.count() == 0) return false;
    for (path.edges, 0..) |edge, i| {
        const edge_key = try allocEdgeExclusionKey(
            alloc,
            .{
                .table = graphPathNodeTable(path, i) orelse source_table,
                .key = path.nodes[i],
            },
            .{
                .table = graphPathNodeTable(path, i + 1) orelse source_table,
                .key = path.nodes[i + 1],
            },
            edge.traversal_direction,
            edge.edge_type,
        );
        defer alloc.free(edge_key);
        if (exclude_edge_set.contains(edge_key)) return true;
    }
    return false;
}

pub fn graphResultNodePathTable(
    source_table: []const u8,
    node: graph_query_mod.GraphResultNode,
    index: usize,
) []const u8 {
    if (node.path_tables) |tables| {
        if (index < tables.len) return tables[index] orelse source_table;
    }
    return source_table;
}

pub fn graphResultNodeTouchesExcludedNode(
    source_table: []const u8,
    node: graph_query_mod.GraphResultNode,
    exclude: *graph_node_identity.Map(void),
) !bool {
    const path = node.path orelse return false;
    if (node.path_tables) |tables| {
        if (tables.len != path.len) return error.InvalidGraphPath;
    }
    for (path, 0..) |key, index| {
        if (exclude.contains(.{
            .table = graphResultNodePathTable(source_table, node, index),
            .key = key,
        })) return true;
    }
    return false;
}

pub fn graphResultNodeHasExcludedEdge(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    node: graph_query_mod.GraphResultNode,
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    const edges = node.path_edges orelse return false;
    if (edges.len == 0) return false;
    const path = node.path orelse return error.InvalidGraphPath;
    if (path.len != edges.len + 1) return error.InvalidGraphPath;
    if (node.path_tables) |tables| {
        if (tables.len != path.len) return error.InvalidGraphPath;
    }
    for (edges, 0..) |edge, index| {
        const edge_key = try allocEdgeExclusionKey(
            alloc,
            .{
                .table = graphResultNodePathTable(source_table, node, index),
                .key = path[index],
            },
            .{
                .table = graphResultNodePathTable(source_table, node, index + 1),
                .key = path[index + 1],
            },
            edge.traversal_direction,
            edge.edge_type,
        );
        defer alloc.free(edge_key);
        if (exclude_edge_set.contains(edge_key)) return true;
    }
    return false;
}

pub fn graphPatternMatchIsExcluded(
    alloc: std.mem.Allocator,
    source_table: []const u8,
    match: db_mod.types.GraphPatternMatch,
    exclude: *graph_node_identity.Map(void),
    exclude_edge_set: *std.StringHashMapUnmanaged(void),
) !bool {
    for (match.bindings) |binding| {
        if (exclude.contains(.{
            .table = canonicalGraphNodeTable(source_table, binding.node.table) orelse source_table,
            .key = binding.node.key,
        })) return true;
        if (try graphResultNodeTouchesExcludedNode(source_table, binding.node, exclude)) return true;
        if (exclude_edge_set.count() > 0 and
            try graphResultNodeHasExcludedEdge(alloc, source_table, binding.node, exclude_edge_set)) return true;
    }
    return false;
}

pub fn cloneGraphPaths(
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

pub fn cloneGraphPatternMatches(
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

pub fn cloneGraphAggregates(
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

pub fn cloneGraphPatternMatch(
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

pub fn cloneGraphPatternBinding(
    alloc: std.mem.Allocator,
    binding: db_mod.types.GraphPatternBinding,
) !db_mod.types.GraphPatternBinding {
    const alias = try alloc.dupe(u8, binding.alias);
    errdefer alloc.free(alias);
    const node = try cloneGraphNode(alloc, binding.node);
    return .{ .alias = alias, .node = node };
}

pub fn cloneSearchHits(
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

pub fn cloneGraphMetricValues(
    alloc: std.mem.Allocator,
    values: []const graph_query_mod.GraphMetricValue,
) ![]graph_query_mod.GraphMetricValue {
    if (values.len == 0) return @constCast((&[_]graph_query_mod.GraphMetricValue{})[0..]);
    const out = try alloc.alloc(graph_query_mod.GraphMetricValue, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*value| value.deinit(alloc);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, value.name),
            .score = value.score,
        };
        initialized += 1;
    }
    return out;
}

pub fn cloneGraphMetricStatuses(
    alloc: std.mem.Allocator,
    statuses: []const db_mod.types.GraphMetricStatus,
) ![]db_mod.types.GraphMetricStatus {
    return db_mod.types.cloneGraphMetricStatuses(alloc, statuses);
}

pub fn cloneGraphNode(
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
        .metrics = try cloneGraphMetricValues(alloc, node.metrics),
    };
}

pub fn dupPath(alloc: std.mem.Allocator, path: []const []const u8) ![][]const u8 {
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

pub fn cloneOwnedStrings(alloc: std.mem.Allocator, items: []const []const u8) ![][]u8 {
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

pub fn dupOptionalStrings(
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

pub fn freeOptionalStrings(
    alloc: std.mem.Allocator,
    items: []const ?[]const u8,
) void {
    for (items) |item| if (item) |value| alloc.free(value);
    if (items.len > 0) alloc.free(items);
}

pub fn graphPathNodeTable(
    path: db_mod.types.GraphPath,
    index: usize,
) ?[]const u8 {
    if (path.node_tables.len != path.nodes.len) return null;
    return path.node_tables[index];
}

pub fn dupPathEdges(alloc: std.mem.Allocator, edges: []const graph_query_mod.PathEdgeInfo) ![]graph_query_mod.PathEdgeInfo {
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

pub fn clonePathEdge(
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
        .traversal_direction = edge.traversal_direction,
    };
}

pub fn freeOwnedPathEdge(alloc: std.mem.Allocator, edge: graph_query_mod.PathEdgeInfo) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
}
