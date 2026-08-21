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

//! Candidate production from lake sidecar artifacts.
//!
//! Sidecar segments use the existing Antfly document-id field to store
//! `source_binding.rowRefKeyAlloc` keys. This module is the query-side bridge
//! from sidecar hits back into validated lake `RowRef` candidates.

const std = @import("std");
const Allocator = std.mem.Allocator;
const text_segment = @import("../text_segment/mod.zig");
const sparse_segment = @import("../sparse_segment/mod.zig");
const vector_segment = @import("../vector_segment/mod.zig");
const graph_segment = @import("../graph_segment/mod.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const indexed_reader = @import("indexed_reader.zig");
const query_request = @import("request.zig");
const lake_rows = @import("lake_rows.zig");

pub const TextCandidateRequest = struct {
    text: []const u8,
    operator: query_request.QueryOperator = .any_terms,
    offset: usize = 0,
    limit: usize = std.math.maxInt(usize),
    min_score: u32 = 0,
};

pub const TextCandidatePlan = struct {
    request: TextCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const SparseCandidateRequest = struct {
    terms: []const query_request.SparseTermWeight,
    offset: usize = 0,
    limit: usize = std.math.maxInt(usize),
    min_score: u32 = 0,
};

pub const SparseCandidatePlan = struct {
    request: SparseCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const VectorCandidateRequest = struct {
    vector: []const f32,
    offset: usize = 0,
    limit: usize = std.math.maxInt(usize),
    min_score: u32 = 0,
    num_probes: u32 = 2,
    search_effort: ?f32 = null,
};

pub const VectorCandidatePlan = struct {
    request: VectorCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const GraphCandidateMode = enum {
    neighbors,
    traverse,
};

pub const GraphCandidateRequest = struct {
    start_node_id: []const u8,
    mode: GraphCandidateMode = .neighbors,
    direction: query_request.GraphQueryDirection = .out,
    edge_types: ?[]const []const u8 = null,
    max_depth: u32 = 3,
    limit: usize = 100,
    include_start: bool = false,
};

pub const GraphCandidatePlan = struct {
    request: GraphCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const CandidatePlanSet = struct {
    text_plans: []const TextCandidatePlan = &.{},
    sparse_plans: []const SparseCandidatePlan = &.{},
    vector_plans: []const VectorCandidatePlan = &.{},
    graph_plans: []const GraphCandidatePlan = &.{},
    vector_stats: ?*indexed_reader.SearchExecutionStats = null,
};

pub const OwnedCandidateSet = struct {
    sidecar_name: []u8,
    row_refs: []rowsource.RowRef,

    pub fn asLakeRowsCandidateSet(self: OwnedCandidateSet) lake_rows.SidecarCandidateSet {
        return .{
            .sidecar_name = self.sidecar_name,
            .row_refs = self.row_refs,
        };
    }

    pub fn deinit(self: *OwnedCandidateSet, alloc: Allocator) void {
        alloc.free(self.sidecar_name);
        source_binding.freeOwnedRowRefs(alloc, self.row_refs);
        self.* = undefined;
    }
};

pub const OwnedCandidateSets = struct {
    sets: []OwnedCandidateSet,

    pub fn asLakeRowsCandidateSetsAlloc(self: OwnedCandidateSets, alloc: Allocator) ![]lake_rows.SidecarCandidateSet {
        const out = try alloc.alloc(lake_rows.SidecarCandidateSet, self.sets.len);
        for (self.sets, 0..) |set, idx| out[idx] = set.asLakeRowsCandidateSet();
        return out;
    }

    pub fn deinit(self: *OwnedCandidateSets, alloc: Allocator) void {
        for (self.sets) |*set| set.deinit(alloc);
        if (self.sets.len > 0) alloc.free(self.sets);
        self.* = undefined;
    }
};

pub fn textCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .text) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.text_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try text_segment.decodeAlloc(alloc, payload);
    defer text_segment.freeSegment(alloc, &segment);

    const hits = try indexed_reader.searchTextSegmentDocIdsAlloc(
        alloc,
        segment,
        request.text,
        request.operator,
        request.offset,
        request.limit,
        request.min_score,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| keys[idx] = hit.doc_id;

    const refs = try source_binding.rowRefsFromKeysAlloc(alloc, declaration.binding, keys);
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

pub fn textCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: TextCandidatePlan,
) !OwnedCandidateSets {
    if (plan.request.text.len == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedTextDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try textCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn sparseCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .sparse) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.sparse_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try sparse_segment.decodeAlloc(alloc, payload);
    defer sparse_segment.freeSegment(alloc, &segment);

    const hits = try indexed_reader.searchSparseSegmentDocIdsAlloc(
        alloc,
        segment,
        request.terms,
        request.offset,
        request.limit,
        request.min_score,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| keys[idx] = hit.doc_id;

    const refs = try source_binding.rowRefsFromKeysAlloc(alloc, declaration.binding, keys);
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

pub fn sparseCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: SparseCandidatePlan,
) !OwnedCandidateSets {
    if (plan.request.terms.len == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedSparseDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try sparseCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn vectorCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: VectorCandidateRequest,
    stats: *indexed_reader.SearchExecutionStats,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .vector) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.vector_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try vector_segment.decodeAlloc(alloc, payload);
    defer vector_segment.freeSegment(alloc, &segment);

    const hits = try indexed_reader.searchVectorSegmentDocIdsAlloc(
        alloc,
        segment,
        request.vector,
        request.offset,
        request.limit,
        request.min_score,
        request.num_probes,
        request.search_effort,
        stats,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| keys[idx] = hit.doc_id;

    const refs = try source_binding.rowRefsFromKeysAlloc(alloc, declaration.binding, keys);
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

pub fn vectorCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: VectorCandidatePlan,
    stats: *indexed_reader.SearchExecutionStats,
) !OwnedCandidateSets {
    if (plan.request.vector.len == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedVectorDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try vectorCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request, stats));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn graphCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
) !OwnedCandidateSet {
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .graph) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.graph_segment) return error.UnsupportedLakeSidecarCandidateSource;

    const start_ref = try source_binding.rowRefFromKeyAlloc(alloc, request.start_node_id);
    defer source_binding.freeOwnedRowRef(alloc, start_ref);
    try source_binding.validateCandidateRowRefsAgainstBinding(declaration.binding, &[_]rowsource.RowRef{start_ref});

    var segment = try graph_segment.decodeAlloc(alloc, payload);
    defer graph_segment.freeSegment(alloc, &segment);

    const keys = switch (request.mode) {
        .neighbors => try graphNeighborCandidateKeysAlloc(alloc, segment, request),
        .traverse => try graphTraversalCandidateKeysAlloc(alloc, segment, request),
    };
    defer {
        for (keys) |key| alloc.free(@constCast(key));
        if (keys.len > 0) alloc.free(keys);
    }

    const refs = try source_binding.rowRefsFromKeysAlloc(alloc, declaration.binding, keys);
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

pub fn graphCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: GraphCandidatePlan,
) !OwnedCandidateSets {
    if (plan.request.start_node_id.len == 0 or plan.request.limit == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };

    const selected = try selectedGraphDeclarationsAlloc(alloc, declarations, plan.sidecar_names);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try artifacts.getAlloc(declaration.artifact.artifact_id);
        defer artifacts.allocator.free(payload);
        sets.appendAssumeCapacity(try graphCandidateSetFromPayloadAlloc(alloc, declaration, payload, plan.request));
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn candidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plans: CandidatePlanSet,
) !OwnedCandidateSets {
    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer deinitOwnedCandidateSetList(alloc, &sets);

    for (plans.text_plans) |plan| {
        var produced = try textCandidateSetsFromArtifactStoreAlloc(alloc, artifacts, declarations, plan);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.sets);
    }
    for (plans.sparse_plans) |plan| {
        var produced = try sparseCandidateSetsFromArtifactStoreAlloc(alloc, artifacts, declarations, plan);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.sets);
    }
    var local_vector_stats = indexed_reader.SearchExecutionStats{};
    const vector_stats = plans.vector_stats orelse &local_vector_stats;
    for (plans.vector_plans) |plan| {
        var produced = try vectorCandidateSetsFromArtifactStoreAlloc(alloc, artifacts, declarations, plan, vector_stats);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.sets);
    }
    for (plans.graph_plans) |plan| {
        var produced = try graphCandidateSetsFromArtifactStoreAlloc(alloc, artifacts, declarations, plan);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.sets);
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

fn selectedTextDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findTextDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isTextDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn selectedGraphDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findGraphDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isGraphDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn selectedVectorDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findVectorDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isVectorDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn selectedSparseDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_names: ?[]const []const u8,
) ![]sidecar_manifest.DeclaredArtifact {
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);

    if (maybe_names) |names| {
        for (names) |name| {
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            const declaration = findSparseDeclaration(declarations, name) orelse return error.MissingLakeSidecarCandidateSource;
            try selected.append(alloc, declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    for (declarations) |declaration| {
        if (!isSparseDeclaration(declaration)) continue;
        if (selected.items.len != 0) return error.AmbiguousLakeSidecarCandidateSource;
        try selected.append(alloc, declaration);
    }
    return try selected.toOwnedSlice(alloc);
}

fn findTextDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isTextDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn findGraphDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isGraphDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn findVectorDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isVectorDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn findSparseDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| {
        if (!isSparseDeclaration(declaration)) continue;
        if (std.mem.eql(u8, declaration.name, name)) return declaration;
    }
    return null;
}

fn isTextDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .text and declaration.artifact.kind == artifact_ref.ArtifactKind.text_segment;
}

fn isGraphDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .graph and declaration.artifact.kind == artifact_ref.ArtifactKind.graph_segment;
}

fn isVectorDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .vector and declaration.artifact.kind == artifact_ref.ArtifactKind.vector_segment;
}

fn isSparseDeclaration(declaration: sidecar_manifest.DeclaredArtifact) bool {
    return declaration.binding.sidecar_kind == .sparse and declaration.artifact.kind == artifact_ref.ArtifactKind.sparse_segment;
}

fn graphNeighborCandidateKeysAlloc(
    alloc: Allocator,
    segment: graph_segment.Segment,
    request: GraphCandidateRequest,
) ![]const []const u8 {
    const adjacency = findGraphAdjacency(segment, request.start_node_id) orelse return try alloc.alloc([]const u8, 0);
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }
    if (request.direction == .out or request.direction == .both) {
        try appendGraphNeighborKeysAlloc(alloc, &keys, adjacency.out_edges, request);
    }
    if (request.direction == .in or request.direction == .both) {
        try appendGraphNeighborKeysAlloc(alloc, &keys, adjacency.in_edges, request);
    }
    return try keys.toOwnedSlice(alloc);
}

fn appendGraphNeighborKeysAlloc(
    alloc: Allocator,
    keys: *std.ArrayListUnmanaged([]const u8),
    edges: []const graph_segment.Edge,
    request: GraphCandidateRequest,
) !void {
    for (edges) |edge| {
        if (keys.items.len >= request.limit) return;
        if (!graphEdgeTypeMatches(request.edge_types, edge.edge_type)) continue;
        try keys.append(alloc, try alloc.dupe(u8, edge.neighbor_id));
    }
}

const GraphQueueItem = struct {
    node_id: []const u8,
    depth: u32,
};

fn graphTraversalCandidateKeysAlloc(
    alloc: Allocator,
    segment: graph_segment.Segment,
    request: GraphCandidateRequest,
) ![]const []const u8 {
    if (findGraphAdjacency(segment, request.start_node_id) == null) return try alloc.alloc([]const u8, 0);

    var queue = std.ArrayListUnmanaged(GraphQueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .node_id = request.start_node_id, .depth = 0 });

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, request.start_node_id, {});

    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }

    var cursor: usize = 0;
    while (cursor < queue.items.len and keys.items.len < request.limit) : (cursor += 1) {
        const item = queue.items[cursor];
        if (item.depth > request.max_depth) continue;
        if (item.depth > 0 or request.include_start) {
            try keys.append(alloc, try alloc.dupe(u8, item.node_id));
            if (keys.items.len >= request.limit) break;
        }
        if (item.depth == request.max_depth) continue;
        const adjacency = findGraphAdjacency(segment, item.node_id) orelse continue;
        if (request.direction == .out or request.direction == .both) {
            try enqueueGraphTraversalEdgesAlloc(alloc, &queue, &seen, adjacency.out_edges, item, request);
        }
        if (request.direction == .in or request.direction == .both) {
            try enqueueGraphTraversalEdgesAlloc(alloc, &queue, &seen, adjacency.in_edges, item, request);
        }
    }

    return try keys.toOwnedSlice(alloc);
}

fn enqueueGraphTraversalEdgesAlloc(
    alloc: Allocator,
    queue: *std.ArrayListUnmanaged(GraphQueueItem),
    seen: *std.StringHashMapUnmanaged(void),
    edges: []const graph_segment.Edge,
    current: GraphQueueItem,
    request: GraphCandidateRequest,
) !void {
    for (edges) |edge| {
        if (!graphEdgeTypeMatches(request.edge_types, edge.edge_type)) continue;
        const gop = try seen.getOrPut(alloc, edge.neighbor_id);
        if (gop.found_existing) continue;
        try queue.append(alloc, .{
            .node_id = edge.neighbor_id,
            .depth = current.depth + 1,
        });
    }
}

fn findGraphAdjacency(segment: graph_segment.Segment, node_id: []const u8) ?graph_segment.Adjacency {
    for (segment.adjacencies) |adjacency| {
        if (std.mem.eql(u8, adjacency.node_id, node_id)) return adjacency;
    }
    return null;
}

fn graphEdgeTypeMatches(edge_types: ?[]const []const u8, candidate: []const u8) bool {
    const values = edge_types orelse return true;
    for (values) |edge_type| {
        if (std.mem.eql(u8, edge_type, candidate)) return true;
    }
    return false;
}

fn appendOwnedCandidateSetsIntersectingDuplicatesAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(OwnedCandidateSet),
    incoming: []const OwnedCandidateSet,
) !void {
    for (incoming) |candidate_set| {
        if (ownedCandidateSetIndexByName(out.items, candidate_set.sidecar_name)) |existing_idx| {
            const intersected = try intersectOwnedRowRefsAlloc(
                alloc,
                out.items[existing_idx].row_refs,
                candidate_set.row_refs,
            );
            source_binding.freeOwnedRowRefs(alloc, out.items[existing_idx].row_refs);
            out.items[existing_idx].row_refs = intersected;
            continue;
        }
        var cloned = try cloneOwnedCandidateSetAlloc(alloc, candidate_set);
        errdefer cloned.deinit(alloc);
        try out.append(alloc, cloned);
    }
}

fn deinitOwnedCandidateSetList(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged(OwnedCandidateSet),
) void {
    for (list.items) |*set| set.deinit(alloc);
    list.deinit(alloc);
    list.* = .empty;
}

fn ownedCandidateSetIndexByName(values: []const OwnedCandidateSet, name: []const u8) ?usize {
    for (values, 0..) |value, idx| {
        if (std.mem.eql(u8, value.sidecar_name, name)) return idx;
    }
    return null;
}

fn cloneOwnedCandidateSetAlloc(
    alloc: Allocator,
    source: OwnedCandidateSet,
) !OwnedCandidateSet {
    const sidecar_name = try alloc.dupe(u8, source.sidecar_name);
    errdefer alloc.free(sidecar_name);
    return .{
        .sidecar_name = sidecar_name,
        .row_refs = try cloneRowRefsAlloc(alloc, source.row_refs),
    };
}

fn cloneRowRefsAlloc(alloc: Allocator, row_refs: []const rowsource.RowRef) ![]rowsource.RowRef {
    const out = try alloc.alloc(rowsource.RowRef, row_refs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |row_ref| source_binding.freeOwnedRowRef(alloc, row_ref);
        alloc.free(out);
    }
    for (row_refs, 0..) |row_ref, idx| {
        out[idx] = try cloneRowRefAlloc(alloc, row_ref);
        initialized += 1;
    }
    return out;
}

fn intersectOwnedRowRefsAlloc(
    alloc: Allocator,
    lhs: []const rowsource.RowRef,
    rhs: []const rowsource.RowRef,
) ![]rowsource.RowRef {
    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer {
        for (out.items) |row_ref| source_binding.freeOwnedRowRef(alloc, row_ref);
        out.deinit(alloc);
    }
    for (lhs) |row_ref| {
        if (!rowRefSliceContains(rhs, row_ref)) continue;
        if (rowRefSliceContains(out.items, row_ref)) continue;
        try out.append(alloc, try cloneRowRefAlloc(alloc, row_ref));
    }
    return try out.toOwnedSlice(alloc);
}

fn cloneRowRefAlloc(alloc: Allocator, row_ref: rowsource.RowRef) !rowsource.RowRef {
    const key = try source_binding.rowRefKeyAlloc(alloc, row_ref);
    defer alloc.free(key);
    return try source_binding.rowRefFromKeyAlloc(alloc, key);
}

fn rowRefSliceContains(values: []const rowsource.RowRef, needle: rowsource.RowRef) bool {
    for (values) |value| {
        if (rowRefsEqual(value, needle)) return true;
    }
    return false;
}

fn rowRefsEqual(lhs: rowsource.RowRef, rhs: rowsource.RowRef) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;
    return switch (lhs) {
        .relational_key => |lhs_key| std.mem.eql(u8, lhs_key, rhs.relational_key),
        .serverless => |lhs_ref| std.mem.eql(u8, lhs_ref.fragment_id, rhs.serverless.fragment_id) and
            lhs_ref.row_ordinal == rhs.serverless.row_ordinal,
        .external => |lhs_ref| std.mem.eql(u8, lhs_ref.source_id, rhs.external.source_id) and
            std.mem.eql(u8, lhs_ref.snapshot_id, rhs.external.snapshot_id) and
            std.mem.eql(u8, lhs_ref.file_id, rhs.external.file_id) and
            lhs_ref.row_group_ordinal == rhs.external.row_group_ordinal and
            lhs_ref.row_ordinal == rhs.external.row_ordinal,
    };
}

test "lake text sidecar candidate producer decodes external row refs from hits" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    const docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = key_a, .normalized_text = @constCast("alpha beta"), .token_count = 2 },
        .{ .doc_id = key_b, .normalized_text = @constCast("beta gamma"), .token_count = 2 },
    };
    const beta_postings = [_]text_segment.Posting{
        .{ .doc_index = 0, .term_freq = 1 },
        .{ .doc_index = 1, .term_freq = 1 },
    };
    const gamma_postings = [_]text_segment.Posting{.{ .doc_index = 1, .term_freq = 1 }};
    const terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(gamma_postings[0..]) },
    };
    const segment = text_segment.Segment{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try text_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = binding,
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = "artifact:text",
            .byte_len = payload.len,
            .checksum = "sha256:text",
        },
    };

    var candidates = try textCandidateSetFromPayloadAlloc(alloc, declaration, payload, .{
        .text = "gamma",
        .operator = .any_terms,
        .limit = 10,
    });
    defer candidates.deinit(alloc);

    try std.testing.expectEqualStrings("events.body.text", candidates.sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), candidates.row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", candidates.row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), candidates.row_refs[0].external.row_ordinal);
    try source_binding.validateCandidateRowRefsAgainstBinding(binding, candidates.asLakeRowsCandidateSet().row_refs);
}

test "lake text sidecar candidate producer rejects stale sidecar doc ids" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const stale_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } };
    const stale_key = try source_binding.rowRefKeyAlloc(alloc, stale_ref);
    defer alloc.free(stale_key);
    const docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = stale_key, .normalized_text = @constCast("alpha"), .token_count = 1 },
    };
    const postings = [_]text_segment.Posting{.{ .doc_index = 0, .term_freq = 1 }};
    const terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("alpha"), .postings = @constCast(postings[0..]) },
    };
    const segment = text_segment.Segment{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try text_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        textCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.body.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = "artifact:text",
                .byte_len = payload.len,
                .checksum = "sha256:text",
            },
        }, payload, .{ .text = "alpha" }),
    );
}

test "lake text sidecar candidate producer loads payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    const docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = key_a, .normalized_text = @constCast("alpha beta"), .token_count = 2 },
        .{ .doc_id = key_b, .normalized_text = @constCast("beta gamma"), .token_count = 2 },
    };
    const beta_postings = [_]text_segment.Posting{
        .{ .doc_index = 0, .term_freq = 1 },
        .{ .doc_index = 1, .term_freq = 1 },
    };
    const gamma_postings = [_]text_segment.Posting{.{ .doc_index = 1, .term_freq = 1 }};
    const terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(gamma_postings[0..]) },
    };
    const segment = text_segment.Segment{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try text_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = binding,
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const sidecar_names = [_][]const u8{"events.body.text"};
    var candidates = try textCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .text = "gamma",
            .operator = .any_terms,
            .limit = 10,
        },
        .sidecar_names = &sidecar_names,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.body.text", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
}

test "lake sidecar candidate planner combines text and sparse plans" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);

    const text_docs = [_]text_segment.DocumentEntry{
        .{ .doc_id = key_a, .normalized_text = @constCast("alpha beta"), .token_count = 2 },
        .{ .doc_id = key_b, .normalized_text = @constCast("beta gamma"), .token_count = 2 },
    };
    const text_beta_postings = [_]text_segment.Posting{
        .{ .doc_index = 0, .term_freq = 1 },
        .{ .doc_index = 1, .term_freq = 1 },
    };
    const text_gamma_postings = [_]text_segment.Posting{.{ .doc_index = 1, .term_freq = 1 }};
    const text_terms = [_]text_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(text_beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(text_gamma_postings[0..]) },
    };
    const text_payload = try text_segment.encodeAlloc(alloc, .{
        .index_name = @constCast("events.body.text"),
        .source_name = @constCast("body"),
        .config_json = @constCast("{}"),
        .docs = @constCast(text_docs[0..]),
        .terms = @constCast(text_terms[0..]),
    });
    defer alloc.free(text_payload);
    var text_meta = try store.put(text_payload);
    defer text_meta.deinit(alloc);

    const sparse_docs = [_]sparse_segment.DocumentEntry{
        .{ .doc_id = key_a, .feature_count = 1 },
        .{ .doc_id = key_b, .feature_count = 1 },
    };
    const sparse_gamma_postings = [_]sparse_segment.Posting{.{ .doc_index = 1, .weight = 1.0 }};
    const sparse_terms = [_]sparse_segment.TermEntry{.{
        .term = @constCast("gamma"),
        .postings = @constCast(sparse_gamma_postings[0..]),
    }};
    const sparse_payload = try sparse_segment.encodeAlloc(alloc, .{
        .docs = @constCast(sparse_docs[0..]),
        .terms = @constCast(sparse_terms[0..]),
    });
    defer alloc.free(sparse_payload);
    var sparse_meta = try store.put(sparse_payload);
    defer sparse_meta.deinit(alloc);

    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        .{
            .name = "events.body.text",
            .binding = .{
                .sidecar_kind = .text,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-7",
                .schema_fingerprint = "schema-v1",
                .column_bindings = &[_][]const u8{"body"},
                .index_config_hash = "sha256:text",
            },
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = text_meta.artifact_id,
                .byte_len = text_meta.byte_len,
                .checksum = text_meta.checksum,
            },
        },
        .{
            .name = "events.features.sparse",
            .binding = .{
                .sidecar_kind = .sparse,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-7",
                .schema_fingerprint = "schema-v1",
                .column_bindings = &[_][]const u8{"features"},
                .index_config_hash = "sha256:sparse",
            },
            .artifact = .{
                .kind = .sparse_segment,
                .name = "events.features.sparse",
                .artifact_id = sparse_meta.artifact_id,
                .byte_len = sparse_meta.byte_len,
                .checksum = sparse_meta.checksum,
            },
        },
    };
    const text_sidecar_names = [_][]const u8{"events.body.text"};
    const sparse_sidecar_names = [_][]const u8{"events.features.sparse"};
    const text_plans = [_]TextCandidatePlan{
        .{
            .request = .{ .text = "beta", .limit = 10 },
            .sidecar_names = &text_sidecar_names,
        },
        .{
            .request = .{ .text = "gamma", .limit = 10 },
            .sidecar_names = &text_sidecar_names,
        },
    };
    const sparse_terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};
    const sparse_plans = [_]SparseCandidatePlan{.{
        .request = .{ .terms = &sparse_terms_query, .limit = 10 },
        .sidecar_names = &sparse_sidecar_names,
    }};

    var candidates = try candidateSetsFromArtifactStoreAlloc(alloc, &store, &declarations, .{
        .text_plans = &text_plans,
        .sparse_plans = &sparse_plans,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 2), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.body.text", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
    try std.testing.expectEqualStrings("events.features.sparse", lake_candidate_sets[1].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[1].row_refs.len);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[1].row_refs[0].external.row_ordinal);
}

test "lake text sidecar candidate producer rejects ambiguous implicit sidecars" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        .{
            .name = "events.body.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = "artifact:text-a",
                .byte_len = 1,
                .checksum = "sha256:text-a",
            },
        },
        .{
            .name = "events.title.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.title.text",
                .artifact_id = "artifact:text-b",
                .byte_len = 1,
                .checksum = "sha256:text-b",
            },
        },
    };

    try std.testing.expectError(
        error.AmbiguousLakeSidecarCandidateSource,
        textCandidateSetsFromArtifactStoreAlloc(alloc, &store, &declarations, .{
            .request = .{ .text = "gamma" },
        }),
    );
}

test "lake sparse sidecar candidate producer loads payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"features"},
        .index_config_hash = "sha256:sparse",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    const docs = [_]sparse_segment.DocumentEntry{
        .{ .doc_id = key_a, .feature_count = 1 },
        .{ .doc_id = key_b, .feature_count = 2 },
    };
    const beta_postings = [_]sparse_segment.Posting{
        .{ .doc_index = 0, .weight = 0.25 },
        .{ .doc_index = 1, .weight = 0.5 },
    };
    const gamma_postings = [_]sparse_segment.Posting{.{ .doc_index = 1, .weight = 1.0 }};
    const terms = [_]sparse_segment.TermEntry{
        .{ .term = @constCast("beta"), .postings = @constCast(beta_postings[0..]) },
        .{ .term = @constCast("gamma"), .postings = @constCast(gamma_postings[0..]) },
    };
    const segment = sparse_segment.Segment{
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try sparse_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.features.sparse",
        .binding = binding,
        .artifact = .{
            .kind = .sparse_segment,
            .name = "events.features.sparse",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};
    const sidecar_names = [_][]const u8{"events.features.sparse"};
    var candidates = try sparseCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .terms = &terms_query,
            .limit = 10,
        },
        .sidecar_names = &sidecar_names,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.features.sparse", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
}

test "lake sparse sidecar candidate producer rejects stale sidecar doc ids" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"features"},
        .index_config_hash = "sha256:sparse",
    };
    const stale_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } };
    const stale_key = try source_binding.rowRefKeyAlloc(alloc, stale_ref);
    defer alloc.free(stale_key);
    const docs = [_]sparse_segment.DocumentEntry{
        .{ .doc_id = stale_key, .feature_count = 1 },
    };
    const postings = [_]sparse_segment.Posting{.{ .doc_index = 0, .weight = 1.0 }};
    const terms = [_]sparse_segment.TermEntry{
        .{ .term = @constCast("gamma"), .postings = @constCast(postings[0..]) },
    };
    const segment = sparse_segment.Segment{
        .docs = @constCast(docs[0..]),
        .terms = @constCast(terms[0..]),
    };
    const payload = try sparse_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    const terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        sparseCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.features.sparse",
            .binding = binding,
            .artifact = .{
                .kind = .sparse_segment,
                .name = "events.features.sparse",
                .artifact_id = "artifact:sparse",
                .byte_len = payload.len,
                .checksum = "sha256:sparse",
            },
        }, payload, .{ .terms = &terms_query }),
    );
}

test "lake vector sidecar candidate producer loads payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    var exact_entries = [_]vector_segment.Entry{
        .{ .doc_id = key_a, .vector = @constCast(&[_]f32{ 1.0, 0.0 }) },
        .{ .doc_id = key_b, .vector = @constCast(&[_]f32{ 0.0, 1.0 }) },
    };
    const exact_block = try vector_segment.encodeExactEntriesAlloc(alloc, &exact_entries);
    defer alloc.free(exact_block);
    var segment = vector_segment.Segment{
        .dims = 2,
        .base_probe_count = 1,
        .shortlist_multiplier = 2,
        .clusters = try alloc.alloc(vector_segment.Cluster, 1),
        .entries = try alloc.alloc(vector_segment.Entry, 2),
    };
    defer vector_segment.freeSegment(alloc, &segment);
    segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 0.0, 1.0 }),
        .start_index = 0,
        .entry_count = 2,
        .routing_distance_min = 0,
        .routing_distance_max = 1,
        .routing_distance_avg = 0.5,
        .quantized_set = try alloc.alloc(u8, 0),
        .exact_entries = try alloc.dupe(u8, exact_block),
    };
    segment.entries[0] = .{
        .doc_id = try alloc.dupe(u8, key_a),
        .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
    };
    segment.entries[1] = .{
        .doc_id = try alloc.dupe(u8, key_b),
        .vector = try alloc.dupe(f32, &.{ 0.0, 1.0 }),
    };
    const payload = try vector_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = binding,
        .artifact = .{
            .kind = .vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const sidecar_names = [_][]const u8{"events.embedding.vector"};
    var stats = indexed_reader.SearchExecutionStats{};
    var candidates = try vectorCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .vector = &[_]f32{ 0.0, 1.0 },
            .limit = 1,
            .num_probes = 1,
        },
        .sidecar_names = &sidecar_names,
    }, &stats);
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.embedding.vector", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
    try std.testing.expectEqual(@as(usize, 1), stats.actual_probe_count);
}

test "lake vector sidecar candidate producer rejects stale sidecar doc ids" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const stale_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } };
    const stale_key = try source_binding.rowRefKeyAlloc(alloc, stale_ref);
    defer alloc.free(stale_key);
    var exact_entries = [_]vector_segment.Entry{
        .{ .doc_id = stale_key, .vector = @constCast(&[_]f32{ 1.0, 0.0 }) },
    };
    const exact_block = try vector_segment.encodeExactEntriesAlloc(alloc, &exact_entries);
    defer alloc.free(exact_block);
    var segment = vector_segment.Segment{
        .dims = 2,
        .base_probe_count = 1,
        .shortlist_multiplier = 2,
        .clusters = try alloc.alloc(vector_segment.Cluster, 1),
        .entries = try alloc.alloc(vector_segment.Entry, 1),
    };
    defer vector_segment.freeSegment(alloc, &segment);
    segment.clusters[0] = .{
        .centroid = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
        .start_index = 0,
        .entry_count = 1,
        .routing_distance_min = 0,
        .routing_distance_max = 1,
        .routing_distance_avg = 0.5,
        .quantized_set = try alloc.alloc(u8, 0),
        .exact_entries = try alloc.dupe(u8, exact_block),
    };
    segment.entries[0] = .{
        .doc_id = try alloc.dupe(u8, stale_key),
        .vector = try alloc.dupe(f32, &.{ 1.0, 0.0 }),
    };
    const payload = try vector_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var stats = indexed_reader.SearchExecutionStats{};

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        vectorCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.embedding.vector",
            .binding = binding,
            .artifact = .{
                .kind = .vector_segment,
                .name = "events.embedding.vector",
                .artifact_id = "artifact:vector",
                .byte_len = payload.len,
                .checksum = "sha256:vector",
            },
        }, payload, .{
            .vector = &[_]f32{ 1.0, 0.0 },
            .limit = 1,
            .num_probes = 1,
        }, &stats),
    );
}

test "lake graph sidecar candidate producer loads neighbor payloads from artifact store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const artifact_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifacts", .{tmp.sub_path});
    defer alloc.free(artifact_path);
    var fs = try artifacts_mod.FsStore.init(alloc, artifact_path);
    var store = fs.artifactStore();
    defer store.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"graph_edges"},
        .index_config_hash = "sha256:graph",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    var segment = graph_segment.Segment{
        .adjacencies = try alloc.alloc(graph_segment.Adjacency, 2),
    };
    defer graph_segment.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, key_a),
        .out_edges = try alloc.alloc(graph_segment.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, key_b),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 2.0,
    };
    segment.adjacencies[1] = .{
        .node_id = try alloc.dupe(u8, key_b),
        .out_edges = try alloc.alloc(graph_segment.Edge, 0),
        .in_edges = try alloc.alloc(graph_segment.Edge, 1),
    };
    segment.adjacencies[1].in_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, key_a),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 2.0,
    };
    const payload = try graph_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var meta = try store.put(payload);
    defer meta.deinit(alloc);

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.graph",
        .binding = binding,
        .artifact = .{
            .kind = .graph_segment,
            .name = "events.graph",
            .artifact_id = meta.artifact_id,
            .byte_len = meta.byte_len,
            .checksum = meta.checksum,
        },
    };
    const sidecar_names = [_][]const u8{"events.graph"};
    var candidates = try graphCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .start_node_id = key_a,
            .mode = .neighbors,
            .direction = .out,
            .edge_types = &[_][]const u8{"cites"},
            .limit = 10,
        },
        .sidecar_names = &sidecar_names,
    });
    defer candidates.deinit(alloc);
    const lake_candidate_sets = try candidates.asLakeRowsCandidateSetsAlloc(alloc);
    defer alloc.free(lake_candidate_sets);

    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets.len);
    try std.testing.expectEqualStrings("events.graph", lake_candidate_sets[0].sidecar_name);
    try std.testing.expectEqual(@as(usize, 1), lake_candidate_sets[0].row_refs.len);
    try std.testing.expectEqualStrings("file-a.parquet", lake_candidate_sets[0].row_refs[0].external.file_id);
    try std.testing.expectEqual(@as(u64, 1), lake_candidate_sets[0].row_refs[0].external.row_ordinal);
}

test "lake graph sidecar candidate producer traverses external row refs" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"graph_edges"},
        .index_config_hash = "sha256:graph",
    };
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 0,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 1,
        } },
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 0,
            .row_ordinal = 2,
        } },
    };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(key_a);
    const key_b = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(key_b);
    const key_c = try source_binding.rowRefKeyAlloc(alloc, row_refs[2]);
    defer alloc.free(key_c);
    var segment = graph_segment.Segment{
        .adjacencies = try alloc.alloc(graph_segment.Adjacency, 3),
    };
    defer graph_segment.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, key_a),
        .out_edges = try alloc.alloc(graph_segment.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, key_b),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[1] = .{
        .node_id = try alloc.dupe(u8, key_b),
        .out_edges = try alloc.alloc(graph_segment.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    segment.adjacencies[1].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, key_c),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    segment.adjacencies[2] = .{
        .node_id = try alloc.dupe(u8, key_c),
        .out_edges = try alloc.alloc(graph_segment.Edge, 0),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    const payload = try graph_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    var candidates = try graphCandidateSetFromPayloadAlloc(alloc, .{
        .name = "events.graph",
        .binding = binding,
        .artifact = .{
            .kind = .graph_segment,
            .name = "events.graph",
            .artifact_id = "artifact:graph",
            .byte_len = payload.len,
            .checksum = "sha256:graph",
        },
    }, payload, .{
        .start_node_id = key_a,
        .mode = .traverse,
        .direction = .out,
        .edge_types = &[_][]const u8{"cites"},
        .max_depth = 2,
        .limit = 10,
    });
    defer candidates.deinit(alloc);

    try std.testing.expectEqualStrings("events.graph", candidates.sidecar_name);
    try std.testing.expectEqual(@as(usize, 2), candidates.row_refs.len);
    try std.testing.expectEqual(@as(u64, 1), candidates.row_refs[0].external.row_ordinal);
    try std.testing.expectEqual(@as(u64, 2), candidates.row_refs[1].external.row_ordinal);
}

test "lake graph sidecar candidate producer rejects stale edge row refs" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"graph_edges"},
        .index_config_hash = "sha256:graph",
    };
    const start_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-7",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } };
    const stale_ref = rowsource.RowRef{ .external = .{
        .source_id = "events",
        .snapshot_id = "iceberg-8",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 1,
    } };
    const key_a = try source_binding.rowRefKeyAlloc(alloc, start_ref);
    defer alloc.free(key_a);
    const stale_key = try source_binding.rowRefKeyAlloc(alloc, stale_ref);
    defer alloc.free(stale_key);
    var segment = graph_segment.Segment{
        .adjacencies = try alloc.alloc(graph_segment.Adjacency, 1),
    };
    defer graph_segment.freeSegment(alloc, &segment);
    segment.adjacencies[0] = .{
        .node_id = try alloc.dupe(u8, key_a),
        .out_edges = try alloc.alloc(graph_segment.Edge, 1),
        .in_edges = try alloc.alloc(graph_segment.Edge, 0),
    };
    segment.adjacencies[0].out_edges[0] = .{
        .neighbor_id = try alloc.dupe(u8, stale_key),
        .edge_type = try alloc.dupe(u8, "cites"),
        .weight = 1.0,
    };
    const payload = try graph_segment.encodeAlloc(alloc, segment);
    defer alloc.free(payload);

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        graphCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.graph",
            .binding = binding,
            .artifact = .{
                .kind = .graph_segment,
                .name = "events.graph",
                .artifact_id = "artifact:graph",
                .byte_len = payload.len,
                .checksum = "sha256:graph",
            },
        }, payload, .{
            .start_node_id = key_a,
            .mode = .neighbors,
            .direction = .out,
            .limit = 10,
        }),
    );
}
