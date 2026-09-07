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

//! Lake-native graph sidecar builders over RowSource batches.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const graph_segment = @import("../graph_segment/mod.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const GraphSidecarBuildOptions = struct {
    name: []const u8,
    graph_column: []const u8,
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
    cancellation: CancellationToken = .none,
};

pub const GraphSidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *GraphSidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const GraphSidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *GraphSidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub fn buildGraphSidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: GraphSidecarBuildOptions,
) !GraphSidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildGraphSidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildGraphSidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: GraphSidecarBuildOptions,
) !GraphSidecarBuildResult {
    try validateOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);

    var node_map = std.StringArrayHashMapUnmanaged(NodeEdges).empty;
    defer deinitNodeMap(alloc, &node_map);
    var neighbor_tables = std.StringArrayHashMapUnmanaged(void).empty;
    defer deinitNeighborTableMap(alloc, &neighbor_tables);
    var total_edges: usize = 0;

    while (true) {
        try options.cancellation.check();
        const batch = try source.next(alloc) orelse break;
        try budget.admitBatch(batch);
        try sidecar_manifest.validateBatchAgainstDeclaredArtifact(.{
            .name = options.name,
            .binding = binding,
            .artifact = .{
                .kind = .graph_segment,
                .name = options.name,
                .artifact_id = "pending",
                .byte_len = 1,
                .checksum = "pending",
            },
        }, batch);

        const column = batch.findColumn(options.graph_column).?;
        total_edges = std.math.add(usize, total_edges, try appendBatchGraph(alloc, &node_map, &neighbor_tables, batch, column)) catch
            return error.LakeSidecarBuildBudgetExceeded;
        const retained_items = std.math.add(usize, node_map.count(), total_edges) catch
            return error.LakeSidecarBuildBudgetExceeded;
        try budget.checkRetainedItems(retained_items);
    }

    if (total_edges == 0) return error.EmptyLakeSidecarGraphSegment;

    var segment = try nodeMapToSegmentAlloc(alloc, &node_map, &neighbor_tables);
    defer graph_segment.freeSegment(alloc, &segment);

    const encoded_size = try graph_segment.encodedSize(segment);
    try budget.checkOutputBytes(encoded_size);
    const payload = try graph_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);
    std.debug.assert(payload.len == encoded_size);

    var declaration = try declaredArtifactAlloc(alloc, binding, options, payload.len);
    errdefer freeOwnedDeclaration(alloc, declaration);
    try declaration.validate();

    return .{
        .payload = payload,
        .declaration = declaration,
    };
}

pub fn publishGraphSidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: GraphSidecarBuildOptions,
) !GraphSidecarPublishResult {
    var built = try buildGraphSidecarFromRowSourceAlloc(alloc, source, binding, options);
    defer alloc.free(built.payload);
    errdefer freeOwnedDeclaration(alloc, built.declaration);

    var metadata = try artifacts.putWithCancellation(built.payload, options.cancellation);
    var metadata_owned = true;
    errdefer if (metadata_owned) metadata.deinit(alloc);

    alloc.free(built.declaration.artifact.artifact_id);
    alloc.free(built.declaration.artifact.checksum);
    built.declaration.artifact.artifact_id = metadata.artifact_id;
    built.declaration.artifact.byte_len = metadata.byte_len;
    built.declaration.artifact.checksum = metadata.checksum;
    metadata_owned = false;

    try built.declaration.validate();
    return .{ .declaration = built.declaration };
}

fn validateOptions(
    binding: source_binding.Binding,
    source_kind: rowsource.SourceKind,
    options: GraphSidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .graph) return error.InvalidLakeSidecarGraphBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.graph_column.len == 0) {
        return error.InvalidLakeSidecarGraphBuildOptions;
    }
    if (binding.column_bindings.len != 1) return error.InvalidLakeSidecarGraphBuildOptions;
    if (!std.mem.eql(u8, binding.column_bindings[0], options.graph_column)) {
        return error.SidecarSourceBindingMismatch;
    }
}

fn appendBatchGraph(
    alloc: Allocator,
    node_map: *std.StringArrayHashMapUnmanaged(NodeEdges),
    neighbor_tables: *std.StringArrayHashMapUnmanaged(void),
    batch: rowsource.ColumnBatch,
    column: rowsource.ColumnVector,
) !usize {
    var total_edges: usize = 0;
    switch (column.values) {
        .bytes => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                total_edges = std.math.add(usize, total_edges, try appendGraphDocument(alloc, node_map, neighbor_tables, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        .json => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                total_edges = std.math.add(usize, total_edges, try appendGraphDocument(alloc, node_map, neighbor_tables, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        else => return error.UnsupportedLakeSidecarGraphColumn,
    }
    return total_edges;
}

fn appendGraphDocument(
    alloc: Allocator,
    node_map: *std.StringArrayHashMapUnmanaged(NodeEdges),
    neighbor_tables: *std.StringArrayHashMapUnmanaged(void),
    row_ref: rowsource.RowRef,
    source_value: []const u8,
) !usize {
    const node_id = try source_binding.rowRefKeyAlloc(alloc, row_ref);
    defer alloc.free(node_id);
    _ = try ensureNode(alloc, node_map, node_id);

    const edges = try parseGraphEdgesAlloc(alloc, source_value);
    defer freeParsedGraphEdges(alloc, edges);
    for (edges) |edge| {
        const src = try ensureNode(alloc, node_map, node_id);
        const neighbor_table_id = if (edge.target_table) |table|
            try internNeighborTable(alloc, neighbor_tables, table)
        else
            null;
        try src.out_edges.append(alloc, .{
            .neighbor_id = try alloc.dupe(u8, edge.target),
            .edge_type = try alloc.dupe(u8, edge.edge_type),
            .weight = edge.weight,
            .neighbor_table_id = neighbor_table_id,
        });
        if (edge.target_table == null) {
            const dst = try ensureNode(alloc, node_map, edge.target);
            try dst.in_edges.append(alloc, .{
                .neighbor_id = try alloc.dupe(u8, node_id),
                .edge_type = try alloc.dupe(u8, edge.edge_type),
                .weight = edge.weight,
            });
        }
    }
    return edges.len;
}

const ParsedGraphEdge = struct {
    target: []u8,
    edge_type: []u8,
    weight: f32,
    target_table: ?[]u8,
};

const NodeEdges = struct {
    out_edges: std.ArrayListUnmanaged(graph_segment.Edge) = .empty,
    in_edges: std.ArrayListUnmanaged(graph_segment.Edge) = .empty,
};

fn ensureNode(
    alloc: Allocator,
    node_map: *std.StringArrayHashMapUnmanaged(NodeEdges),
    node_id: []const u8,
) !*NodeEdges {
    const gop = try node_map.getOrPut(alloc, node_id);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, node_id);
        gop.value_ptr.* = .{};
    }
    return gop.value_ptr;
}

fn parseGraphEdgesAlloc(alloc: Allocator, value: []const u8) ![]ParsedGraphEdge {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return try alloc.alloc(ParsedGraphEdge, 0);
    defer parsed.deinit();
    const raw_edges: std.json.Array = switch (parsed.value) {
        .array => |items| items,
        .object => |obj| blk: {
            const graph_edges = obj.get("graph_edges") orelse return try alloc.alloc(ParsedGraphEdge, 0);
            if (graph_edges != .array) return try alloc.alloc(ParsedGraphEdge, 0);
            break :blk graph_edges.array;
        },
        else => return try alloc.alloc(ParsedGraphEdge, 0),
    };

    var out = std.ArrayListUnmanaged(ParsedGraphEdge).empty;
    errdefer {
        for (out.items) |edge| {
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
            if (edge.target_table) |table| alloc.free(table);
        }
        out.deinit(alloc);
    }

    for (raw_edges.items) |item| {
        if (item != .object) continue;
        const target = jsonObjectStringAny(item.object, &[_][]const u8{ "target", "to", "neighbor_id", "neighbor" }) orelse continue;
        if (target.len == 0) continue;
        const edge_type = jsonObjectStringAny(item.object, &[_][]const u8{ "edge_type", "type" }) orelse "";
        const target_table = jsonObjectStringAny(item.object, &.{"target_table"});
        try out.append(alloc, .{
            .target = try alloc.dupe(u8, target),
            .edge_type = try alloc.dupe(u8, edge_type),
            .weight = if (item.object.get("weight")) |weight| jsonValueAsF32(weight) catch 1.0 else 1.0,
            .target_table = if (target_table) |table|
                if (table.len > 0) try alloc.dupe(u8, table) else null
            else
                null,
        });
    }
    const edges = try out.toOwnedSlice(alloc);
    sortParsedGraphEdges(edges);
    return edges;
}

fn freeParsedGraphEdges(alloc: Allocator, edges: []ParsedGraphEdge) void {
    for (edges) |edge| {
        alloc.free(edge.target);
        alloc.free(edge.edge_type);
        if (edge.target_table) |table| alloc.free(table);
    }
    alloc.free(edges);
}

fn sortParsedGraphEdges(edges: []ParsedGraphEdge) void {
    std.mem.sort(ParsedGraphEdge, edges, {}, lessParsedGraphEdge);
}

fn lessParsedGraphEdge(_: void, lhs: ParsedGraphEdge, rhs: ParsedGraphEdge) bool {
    const table_order = optionalStringOrder(lhs.target_table, rhs.target_table);
    if (table_order != .eq) return table_order == .lt;
    const edge_type_order = std.mem.order(u8, lhs.edge_type, rhs.edge_type);
    if (edge_type_order != .eq) return edge_type_order == .lt;
    const target_order = std.mem.order(u8, lhs.target, rhs.target);
    if (target_order != .eq) return target_order == .lt;
    return lhs.weight < rhs.weight;
}

fn nodeMapToSegmentAlloc(
    alloc: Allocator,
    node_map: *std.StringArrayHashMapUnmanaged(NodeEdges),
    neighbor_table_map: *const std.StringArrayHashMapUnmanaged(void),
) !graph_segment.Segment {
    const neighbor_tables = try alloc.alloc([]u8, neighbor_table_map.count());
    errdefer if (neighbor_tables.len > 0) alloc.free(neighbor_tables);
    var initialized_tables: usize = 0;
    errdefer for (neighbor_tables[0..initialized_tables]) |table| alloc.free(table);
    for (neighbor_table_map.keys(), 0..) |table, idx| {
        neighbor_tables[idx] = try alloc.dupe(u8, table);
        initialized_tables += 1;
    }
    const adjacencies = try alloc.alloc(graph_segment.Adjacency, node_map.count());
    errdefer alloc.free(adjacencies);
    var initialized: usize = 0;
    errdefer {
        for (adjacencies[0..initialized]) |*adjacency| adjacency.deinit(alloc);
    }

    for (node_map.keys(), node_map.values(), 0..) |node_id, *node_edges, idx| {
        sortGraphEdges(node_edges.out_edges.items);
        sortGraphEdges(node_edges.in_edges.items);
        adjacencies[idx] = .{
            .node_id = try alloc.dupe(u8, node_id),
            .out_edges = try node_edges.out_edges.toOwnedSlice(alloc),
            .in_edges = try node_edges.in_edges.toOwnedSlice(alloc),
        };
        initialized += 1;
    }
    std.mem.sort(graph_segment.Adjacency, adjacencies, {}, lessGraphAdjacency);
    return .{ .neighbor_tables = neighbor_tables, .adjacencies = adjacencies };
}

fn internNeighborTable(
    alloc: Allocator,
    tables: *std.StringArrayHashMapUnmanaged(void),
    table: []const u8,
) !u32 {
    if (tables.getIndex(table)) |index| return std.math.cast(u32, index) orelse error.GraphSegmentTooLarge;
    const next_id = std.math.cast(u32, tables.count()) orelse return error.GraphSegmentTooLarge;
    const owned = try alloc.dupe(u8, table);
    errdefer alloc.free(owned);
    const gop = try tables.getOrPut(alloc, owned);
    std.debug.assert(!gop.found_existing);
    std.debug.assert(gop.index == @as(usize, next_id));
    return next_id;
}

fn deinitNeighborTableMap(alloc: Allocator, tables: *std.StringArrayHashMapUnmanaged(void)) void {
    for (tables.keys()) |table| alloc.free(table);
    tables.deinit(alloc);
}

fn optionalStringOrder(lhs: ?[]const u8, rhs: ?[]const u8) std.math.Order {
    if (lhs == null and rhs == null) return .eq;
    if (lhs == null) return .lt;
    if (rhs == null) return .gt;
    return std.mem.order(u8, lhs.?, rhs.?);
}

fn deinitNodeMap(alloc: Allocator, node_map: *std.StringArrayHashMapUnmanaged(NodeEdges)) void {
    for (node_map.keys(), node_map.values()) |key, *value| {
        alloc.free(key);
        for (value.out_edges.items) |*edge| edge.deinit(alloc);
        value.out_edges.deinit(alloc);
        for (value.in_edges.items) |*edge| edge.deinit(alloc);
        value.in_edges.deinit(alloc);
    }
    node_map.deinit(alloc);
}

fn sortGraphEdges(edges: []graph_segment.Edge) void {
    std.mem.sort(graph_segment.Edge, edges, {}, lessGraphEdge);
}

fn lessGraphEdge(_: void, lhs: graph_segment.Edge, rhs: graph_segment.Edge) bool {
    const lookup_order = graph_segment.edgeLookupOrder(lhs.edge_type, lhs.neighbor_id, rhs.edge_type, rhs.neighbor_id);
    if (lookup_order != .eq) return lookup_order == .lt;
    return lhs.weight < rhs.weight;
}

fn lessGraphAdjacency(_: void, lhs: graph_segment.Adjacency, rhs: graph_segment.Adjacency) bool {
    return std.mem.order(u8, lhs.node_id, rhs.node_id) == .lt;
}

fn jsonObjectStringAny(obj: std.json.ObjectMap, keys: []const []const u8) ?[]const u8 {
    for (keys) |key| {
        const value = obj.get(key) orelse continue;
        if (value == .string) return value.string;
    }
    return null;
}

fn jsonValueAsF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        .number_string => try std.fmt.parseFloat(f32, value.number_string),
        else => error.InvalidLakeSidecarGraphFeatures,
    };
}

fn declaredArtifactAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    options: GraphSidecarBuildOptions,
    payload_len: usize,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (options.artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "lake-graph:{d}:{s}:{d}:{s}:{d}",
            .{ options.name.len, options.name, binding.snapshot_id.len, binding.snapshot_id, payload_len },
        )
    else
        try alloc.dupe(u8, options.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{payload_len});
    errdefer alloc.free(checksum);
    const owned_binding = try cloneBindingAlloc(alloc, binding);
    errdefer freeOwnedBinding(alloc, owned_binding);

    return .{
        .name = name,
        .binding = owned_binding,
        .artifact = artifact_ref.ArtifactRef{
            .kind = .graph_segment,
            .name = artifact_name,
            .artifact_id = artifact_id,
            .byte_len = @intCast(payload_len),
            .checksum = checksum,
        },
    };
}

fn cloneBindingAlloc(alloc: Allocator, binding: source_binding.Binding) !source_binding.Binding {
    return try source_binding.cloneAlloc(alloc, binding);
}

fn freeOwnedDeclaration(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) void {
    alloc.free(declaration.name);
    freeOwnedBinding(alloc, declaration.binding);
    if (declaration.artifact.name.len > 0) alloc.free(declaration.artifact.name);
    alloc.free(declaration.artifact.artifact_id);
    alloc.free(declaration.artifact.checksum);
}

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    source_binding.freeOwned(alloc, binding);
}

const MemoryArtifactStore = struct {
    alloc: Allocator,
    bytes: ?[]u8 = null,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = try self.alloc.dupe(u8, contents);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:graph-sidecar"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:graph-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        if (!std.mem.eql(u8, artifact_id, "mem:graph-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:graph-sidecar"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:graph-sidecar")) return error.ArtifactNotFound;
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = null;
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_range_alloc = erasedGetRangeAlloc,
        .stat = erasedStat,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

test "lake graph sidecar builder consumes external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-230",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const target_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(target_key);
    const graph_a = try std.fmt.allocPrint(alloc, "{{\"graph_edges\":[{{\"target\":{f},\"edge_type\":\"cites\",\"weight\":2.0}}]}}", .{std.json.fmt(target_key, .{})});
    defer alloc.free(graph_a);
    const graph_values = [_][]const u8{
        graph_a,
        "{\"graph_edges\":[]}",
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "graph_edges", .values = .{ .json = &graph_values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .graph,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"graph_edges"},
        "sha256:graph:v1",
    );

    var result = try buildGraphSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "events.links.graph",
        .graph_column = "graph_edges",
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);
    try std.testing.expectEqualStrings("events.links.graph", result.declaration.name);
    try std.testing.expectEqual(@as(u64, result.payload.len), result.declaration.artifact.byte_len);

    var segment = try graph_segment.decodeAlloc(alloc, result.payload);
    defer graph_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 2), segment.adjacencies.len);

    const source_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(source_key);
    const source_adj = findAdjacency(segment.adjacencies, source_key).?;
    try std.testing.expectEqual(@as(usize, 1), source_adj.out_edges.len);
    try std.testing.expectEqualStrings(target_key, source_adj.out_edges[0].neighbor_id);
    try std.testing.expectEqualStrings("cites", source_adj.out_edges[0].edge_type);
    try std.testing.expectEqual(@as(f32, 2.0), source_adj.out_edges[0].weight);

    const target_adj = findAdjacency(segment.adjacencies, target_key).?;
    try std.testing.expectEqual(@as(usize, 1), target_adj.in_edges.len);
    try std.testing.expectEqualStrings(source_key, target_adj.in_edges[0].neighbor_id);
}

test "lake graph sidecar builder consumes direct graph edge arrays" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-231",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const graph_values = [_][]const u8{
        "[{\"to\":\"node-b\",\"type\":\"related\"}]",
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "graph_edges", .values = .{ .bytes = &graph_values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .graph,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"graph_edges"},
        "sha256:graph:v1",
    );

    var result = try buildGraphSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "events.links.graph",
        .graph_column = "graph_edges",
    });
    defer result.deinit(alloc);

    var segment = try graph_segment.decodeAlloc(alloc, result.payload);
    defer graph_segment.freeSegment(alloc, &segment);
    const source_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(source_key);
    const source_adj = findAdjacency(segment.adjacencies, source_key).?;
    try std.testing.expectEqualStrings("node-b", source_adj.out_edges[0].neighbor_id);
    try std.testing.expectEqualStrings("related", source_adj.out_edges[0].edge_type);
    try std.testing.expectEqual(@as(f32, 1.0), source_adj.out_edges[0].weight);
}

test "lake graph sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-232",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const graph_values = [_][]const u8{
        "{\"graph_edges\":[{\"target\":\"node-b\",\"edge_type\":\"cites\",\"weight\":1.5}]}",
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "graph_edges", .values = .{ .json = &graph_values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .graph,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"graph_edges"},
        "sha256:graph:v1",
    );

    var result = try publishGraphSidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "events.links.graph",
            .graph_column = "graph_edges",
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:graph-sidecar", result.declaration.artifact.artifact_id);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    try std.testing.expectEqual(@as(usize, @intCast(result.declaration.artifact.byte_len)), stored.len);

    var segment = try graph_segment.decodeAlloc(alloc, stored);
    defer graph_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 2), segment.adjacencies.len);
}

test "lake graph sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-233",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const graph_values = [_][]const u8{
        "{\"graph_edges\":[{\"target\":\"node-b\"}]}",
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "graph_edges", .values = .{ .json = &graph_values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "events", .snapshot_id = "iceberg-232" };
    const binding = source_binding.bindingFromSnapshot(
        .graph,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"graph_edges"},
        "sha256:graph:v1",
    );

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildGraphSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "events.links.graph",
            .graph_column = "graph_edges",
        }),
    );
}

fn findAdjacency(adjacencies: []const graph_segment.Adjacency, node_id: []const u8) ?graph_segment.Adjacency {
    for (adjacencies) |adjacency| {
        if (std.mem.eql(u8, adjacency.node_id, node_id)) return adjacency;
    }
    return null;
}
