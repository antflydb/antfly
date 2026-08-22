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
const bounded_decode = @import("../bounded_decode.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const indexed_reader = @import("indexed_reader.zig");
const query_request = @import("request.zig");
const lake_rows = @import("lake_rows.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const default_candidate_limit: usize = 100;

pub const CandidateServingLimits = struct {
    max_candidate_window: usize = 1_000_000,
    max_plans: usize = 128,
    max_declarations: usize = 16_384,
    max_query_items: usize = 1_000_000,
    max_query_bytes: usize = 16 * 1024 * 1024,
    max_graph_depth: u32 = 64,
    /// Bounds filter construction independently of the general query-shape
    /// ceiling and matches the public graph-query serving contract.
    max_graph_edge_types: usize = 64,
    max_graph_edge_type_bytes: usize = 64 * 1024,
    /// Aggregate edge inspections across every graph plan and artifact in one
    /// candidate request. This bounds work even when no edge matches.
    max_graph_edges_scanned: usize = 1_000_000,
    max_artifacts: usize = 128,
    max_total_artifact_bytes: usize = 1024 * 1024 * 1024,
    max_total_candidates: usize = 1_000_000,
    max_total_candidate_bytes: usize = 256 * 1024 * 1024,
    decode: bounded_decode.Limits = .{},

    pub fn validate(self: CandidateServingLimits) !void {
        try self.decode.validate();
        if (self.max_candidate_window == 0 or self.max_plans == 0 or self.max_declarations == 0 or self.max_query_items == 0 or self.max_query_bytes == 0 or self.max_artifacts == 0 or
            self.max_total_artifact_bytes == 0 or self.max_total_candidates == 0 or self.max_total_candidate_bytes == 0 or self.max_graph_depth == 0 or
            self.max_graph_edge_types == 0 or self.max_graph_edge_type_bytes == 0 or self.max_graph_edges_scanned == 0)
        {
            return error.InvalidLakeSidecarCandidateLimits;
        }
    }
};

/// Execution controls shared by direct single-sidecar APIs. The limits remain
/// value-owned while the cancellation token borrows its callback context for
/// the duration of the synchronous call.
pub const CandidateServingOptions = struct {
    limits: CandidateServingLimits = .{},
    cancellation: CancellationToken = .none,
};

pub const TextCandidateRequest = struct {
    text: []const u8,
    operator: query_request.QueryOperator = .any_terms,
    offset: usize = 0,
    limit: usize = default_candidate_limit,
    min_score: u32 = 0,
};

pub const TextCandidatePlan = struct {
    request: TextCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const SparseCandidateRequest = struct {
    terms: []const query_request.SparseTermWeight,
    offset: usize = 0,
    limit: usize = default_candidate_limit,
    min_score: u32 = 0,
};

pub const SparseCandidatePlan = struct {
    request: SparseCandidateRequest,
    sidecar_names: ?[]const []const u8 = null,
};

pub const VectorCandidateRequest = struct {
    vector: []const f32,
    offset: usize = 0,
    limit: usize = default_candidate_limit,
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
    limits: CandidateServingLimits = .{},
    /// Borrowed for the synchronous execution of this plan set. Cancellation
    /// is checked before artifact I/O and periodically during graph scans.
    cancellation: CancellationToken = .none,
};

const CandidateServingBudget = struct {
    limits: CandidateServingLimits,
    cancellation: CancellationToken = .none,
    plans: usize = 0,
    query_items: usize = 0,
    query_bytes: usize = 0,
    artifacts: usize = 0,
    total_artifact_bytes: usize = 0,
    total_candidates: usize = 0,
    total_candidate_bytes: usize = 0,
    graph_edges_scanned: usize = 0,

    fn init(limits: CandidateServingLimits) !CandidateServingBudget {
        return try initWithCancellation(limits, .none);
    }

    fn initWithCancellation(limits: CandidateServingLimits, cancellation: CancellationToken) !CandidateServingBudget {
        try limits.validate();
        try cancellation.check();
        return .{ .limits = limits, .cancellation = cancellation };
    }

    fn admitArtifact(self: *CandidateServingBudget, byte_len: u64) !usize {
        const len = std.math.cast(usize, byte_len) orelse return error.LakeSidecarCandidateBudgetExceeded;
        if (len == 0 or len > self.limits.decode.max_artifact_bytes) {
            return error.LakeSidecarCandidateBudgetExceeded;
        }
        const next_artifacts = std.math.add(usize, self.artifacts, 1) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_artifacts > self.limits.max_artifacts) return error.LakeSidecarCandidateBudgetExceeded;
        const next_artifact_bytes = std.math.add(usize, self.total_artifact_bytes, len) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_artifact_bytes > self.limits.max_total_artifact_bytes) {
            return error.LakeSidecarCandidateBudgetExceeded;
        }
        self.artifacts = next_artifacts;
        self.total_artifact_bytes = next_artifact_bytes;
        return len;
    }

    fn admitQueryShape(self: *CandidateServingBudget, items: usize, byte_len: usize) !void {
        const next_items = std.math.add(usize, self.query_items, items) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_items > self.limits.max_query_items) return error.LakeSidecarCandidateBudgetExceeded;
        const next_bytes = std.math.add(usize, self.query_bytes, byte_len) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_bytes > self.limits.max_query_bytes) return error.LakeSidecarCandidateBudgetExceeded;
        self.query_items = next_items;
        self.query_bytes = next_bytes;
    }

    fn admitPlans(self: *CandidateServingBudget, count: usize) !void {
        const next_plans = std.math.add(usize, self.plans, count) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_plans > self.limits.max_plans) return error.LakeSidecarCandidateBudgetExceeded;
        self.plans = next_plans;
    }

    fn admitGraphEdge(self: *CandidateServingBudget) !void {
        const next_edges = std.math.add(usize, self.graph_edges_scanned, 1) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_edges > self.limits.max_graph_edges_scanned) {
            return error.LakeSidecarCandidateBudgetExceeded;
        }
        self.graph_edges_scanned = next_edges;
        if (next_edges % 64 == 0) try self.cancellation.check();
    }

    fn checkCancellation(self: CandidateServingBudget) !void {
        try self.cancellation.check();
    }

    fn remainingArtifacts(self: CandidateServingBudget) usize {
        std.debug.assert(self.artifacts <= self.limits.max_artifacts);
        return self.limits.max_artifacts - self.artifacts;
    }

    fn admitCandidateOutput(self: *CandidateServingBudget, count: usize, byte_len: usize) !void {
        const next_candidates = std.math.add(usize, self.total_candidates, count) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_candidates > self.limits.max_total_candidates) {
            return error.LakeSidecarCandidateBudgetExceeded;
        }
        const next_bytes = std.math.add(usize, self.total_candidate_bytes, byte_len) catch
            return error.LakeSidecarCandidateBudgetExceeded;
        if (next_bytes > self.limits.max_total_candidate_bytes) {
            return error.LakeSidecarCandidateBudgetExceeded;
        }
        self.total_candidates = next_candidates;
        self.total_candidate_bytes = next_bytes;
    }

    fn releaseCandidateOutput(self: *CandidateServingBudget, count: usize, byte_len: usize) void {
        std.debug.assert(count <= self.total_candidates);
        std.debug.assert(byte_len <= self.total_candidate_bytes);
        self.total_candidates -= count;
        self.total_candidate_bytes -= byte_len;
    }

    fn remainingCandidates(self: CandidateServingBudget) usize {
        std.debug.assert(self.total_candidates <= self.limits.max_total_candidates);
        return self.limits.max_total_candidates - self.total_candidates;
    }

    fn remainingCandidateBytes(self: CandidateServingBudget) usize {
        std.debug.assert(self.total_candidate_bytes <= self.limits.max_total_candidate_bytes);
        return self.limits.max_total_candidate_bytes - self.total_candidate_bytes;
    }

    /// Bound one producer to the remaining global budget plus a single
    /// sentinel. The sentinel distinguishes "exactly exhausted" from "more
    /// results existed" without allowing a producer to allocate an entire
    /// over-budget result set first.
    fn generationLimit(self: CandidateServingBudget, requested: usize) usize {
        const count_sentinel = std.math.add(usize, self.remainingCandidates(), 1) catch
            std.math.maxInt(usize);
        const minimum_candidate_bytes = @sizeOf(rowsource.RowRef) + 1;
        const byte_capacity = self.remainingCandidateBytes() / minimum_candidate_bytes;
        const byte_sentinel = std.math.add(usize, byte_capacity, 1) catch
            std.math.maxInt(usize);
        return @min(requested, count_sentinel, byte_sentinel);
    }
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

    fn takeSets(self: *OwnedCandidateSets) []OwnedCandidateSet {
        const owned = self.sets;
        self.sets = self.sets[0..0];
        return owned;
    }
};

fn candidateSetFromKeysBudgetAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    keys: []const []const u8,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    const row_ref_bytes = try source_binding.rowRefsOwnedAllocationBytesFromKeysWithCancellation(
        declaration.binding,
        keys,
        budget.cancellation,
    );
    const output_bytes = std.math.add(usize, row_ref_bytes, declaration.name.len) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    try budget.admitCandidateOutput(keys.len, output_bytes);
    errdefer budget.releaseCandidateOutput(keys.len, output_bytes);

    const refs = try source_binding.rowRefsFromKeysWithCancellationAlloc(
        alloc,
        declaration.binding,
        keys,
        budget.cancellation,
    );
    errdefer source_binding.freeOwnedRowRefs(alloc, refs);
    return .{
        .sidecar_name = try alloc.dupe(u8, declaration.name),
        .row_refs = refs,
    };
}

pub fn textCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
) !OwnedCandidateSet {
    return try textCandidateSetFromPayloadWithLimitsAlloc(alloc, declaration, payload, request, .{});
}

pub fn textCandidateSetFromPayloadWithLimitsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
    limits: CandidateServingLimits,
) !OwnedCandidateSet {
    return try textCandidateSetFromPayloadWithOptionsAlloc(alloc, declaration, payload, request, .{ .limits = limits });
}

pub fn textCandidateSetFromPayloadWithOptionsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
    options: CandidateServingOptions,
) !OwnedCandidateSet {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try validateTextCandidateRequest(request, options.limits);
    try budget.checkCancellation();
    try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, options.limits, options.cancellation);
    var bounded_request = request;
    bounded_request.limit = budget.generationLimit(request.limit);
    return try textCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, bounded_request, &budget);
}

fn textCandidateSetFromPayloadWithBudgetAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, budget.limits.decode.max_allocation_bytes);
    return textCandidateSetFromPayloadBoundedAlloc(limiter.allocator(), declaration, payload, request, budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.LakeSidecarCandidateBudgetExceeded;
        return err;
    };
}

fn textCandidateSetFromPayloadBoundedAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: TextCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .text) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.text_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try text_segment.decodeAllocWithLimits(alloc, payload, budget.limits.decode);
    defer text_segment.freeSegment(alloc, &segment);
    try budget.checkCancellation();

    const hits = try indexed_reader.searchTextSegmentDocIdsWithCancellationAlloc(
        alloc,
        segment,
        request.text,
        request.operator,
        request.offset,
        request.limit,
        request.min_score,
        budget.cancellation,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);
    try budget.checkCancellation();

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| {
        if (idx % 64 == 0) try budget.checkCancellation();
        keys[idx] = hit.doc_id;
    }

    return try candidateSetFromKeysBudgetAlloc(alloc, declaration, keys, budget);
}

pub fn textCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: TextCandidatePlan,
) !OwnedCandidateSets {
    return try textCandidateSetsFromArtifactStoreWithOptionsAlloc(alloc, artifacts, declarations, plan, .{});
}

pub fn textCandidateSetsFromArtifactStoreWithOptionsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: TextCandidatePlan,
    options: CandidateServingOptions,
) !OwnedCandidateSets {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try budget.admitPlans(1);
    return try textCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, options.limits, &budget, null);
}

fn textCandidateSetsFromArtifactStoreBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: TextCandidatePlan,
    limits: CandidateServingLimits,
    budget: *CandidateServingBudget,
    declaration_index: ?*const SidecarDeclarationIndex,
) !OwnedCandidateSets {
    try validateTextCandidateRequest(plan.request, limits);
    if (plan.request.text.len == 0 or plan.request.limit == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };
    const query_shape = try textCandidateQueryShape(plan.request);
    try budget.admitQueryShape(query_shape.items, query_shape.bytes);

    const selected = try selectedTextDeclarationsAlloc(alloc, declarations, declaration_index, plan.sidecar_names, budget);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try loadVerifiedArtifactAlloc(artifacts, declaration, budget);
        defer artifacts.allocator.free(payload);
        var request = plan.request;
        request.limit = budget.generationLimit(request.limit);
        const produced = try textCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, request, budget);
        sets.appendAssumeCapacity(produced);
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn sparseCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
) !OwnedCandidateSet {
    return try sparseCandidateSetFromPayloadWithLimitsAlloc(alloc, declaration, payload, request, .{});
}

pub fn sparseCandidateSetFromPayloadWithLimitsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
    limits: CandidateServingLimits,
) !OwnedCandidateSet {
    return try sparseCandidateSetFromPayloadWithOptionsAlloc(alloc, declaration, payload, request, .{ .limits = limits });
}

pub fn sparseCandidateSetFromPayloadWithOptionsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
    options: CandidateServingOptions,
) !OwnedCandidateSet {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try validateSparseCandidateRequestWithCancellation(request, options.limits, options.cancellation);
    try budget.checkCancellation();
    try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, options.limits, options.cancellation);
    var bounded_request = request;
    bounded_request.limit = budget.generationLimit(request.limit);
    return try sparseCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, bounded_request, &budget);
}

fn sparseCandidateSetFromPayloadWithBudgetAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, budget.limits.decode.max_allocation_bytes);
    return sparseCandidateSetFromPayloadBoundedAlloc(limiter.allocator(), declaration, payload, request, budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.LakeSidecarCandidateBudgetExceeded;
        return err;
    };
}

fn sparseCandidateSetFromPayloadBoundedAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: SparseCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .sparse) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.sparse_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try sparse_segment.decodeAllocWithLimits(alloc, payload, budget.limits.decode);
    defer sparse_segment.freeSegment(alloc, &segment);
    try budget.checkCancellation();

    const hits = try indexed_reader.searchSparseSegmentDocIdsWithCancellationAlloc(
        alloc,
        segment,
        request.terms,
        request.offset,
        request.limit,
        request.min_score,
        budget.cancellation,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);
    try budget.checkCancellation();

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| {
        if (idx % 64 == 0) try budget.checkCancellation();
        keys[idx] = hit.doc_id;
    }

    return try candidateSetFromKeysBudgetAlloc(alloc, declaration, keys, budget);
}

pub fn sparseCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: SparseCandidatePlan,
) !OwnedCandidateSets {
    return try sparseCandidateSetsFromArtifactStoreWithOptionsAlloc(alloc, artifacts, declarations, plan, .{});
}

pub fn sparseCandidateSetsFromArtifactStoreWithOptionsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: SparseCandidatePlan,
    options: CandidateServingOptions,
) !OwnedCandidateSets {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try budget.admitPlans(1);
    return try sparseCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, options.limits, &budget, null);
}

fn sparseCandidateSetsFromArtifactStoreBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: SparseCandidatePlan,
    limits: CandidateServingLimits,
    budget: *CandidateServingBudget,
    declaration_index: ?*const SidecarDeclarationIndex,
) !OwnedCandidateSets {
    try validateSparseCandidateRequestWithCancellation(plan.request, limits, budget.cancellation);
    if (plan.request.terms.len == 0 or plan.request.limit == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };
    const query_shape = try sparseCandidateQueryShapeWithCancellation(plan.request, budget.cancellation);
    try budget.admitQueryShape(query_shape.items, query_shape.bytes);

    const selected = try selectedSparseDeclarationsAlloc(alloc, declarations, declaration_index, plan.sidecar_names, budget);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try loadVerifiedArtifactAlloc(artifacts, declaration, budget);
        defer artifacts.allocator.free(payload);
        var request = plan.request;
        request.limit = budget.generationLimit(request.limit);
        const produced = try sparseCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, request, budget);
        sets.appendAssumeCapacity(produced);
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
    return try vectorCandidateSetFromPayloadWithLimitsAlloc(alloc, declaration, payload, request, stats, .{});
}

pub fn vectorCandidateSetFromPayloadWithLimitsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: VectorCandidateRequest,
    stats: *indexed_reader.SearchExecutionStats,
    limits: CandidateServingLimits,
) !OwnedCandidateSet {
    return try vectorCandidateSetFromPayloadWithOptionsAlloc(alloc, declaration, payload, request, stats, .{ .limits = limits });
}

pub fn vectorCandidateSetFromPayloadWithOptionsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: VectorCandidateRequest,
    stats: *indexed_reader.SearchExecutionStats,
    options: CandidateServingOptions,
) !OwnedCandidateSet {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try validateVectorCandidateRequest(request, options.limits);
    try budget.checkCancellation();
    try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, options.limits, options.cancellation);
    var bounded_request = request;
    bounded_request.limit = budget.generationLimit(request.limit);
    return try vectorCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, bounded_request, stats, &budget);
}

fn vectorCandidateSetFromPayloadWithBudgetAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: VectorCandidateRequest,
    stats: *indexed_reader.SearchExecutionStats,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, budget.limits.decode.max_allocation_bytes);
    return vectorCandidateSetFromPayloadBoundedAlloc(limiter.allocator(), declaration, payload, request, stats, budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.LakeSidecarCandidateBudgetExceeded;
        return err;
    };
}

fn vectorCandidateSetFromPayloadBoundedAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: VectorCandidateRequest,
    stats: *indexed_reader.SearchExecutionStats,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .vector) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.vector_segment) return error.UnsupportedLakeSidecarCandidateSource;

    var segment = try vector_segment.decodeAllocWithLimits(alloc, payload, budget.limits.decode);
    defer vector_segment.freeSegment(alloc, &segment);
    try budget.checkCancellation();

    const hits = try indexed_reader.searchVectorSegmentDocIdsWithCancellationAlloc(
        alloc,
        segment,
        request.vector,
        request.offset,
        request.limit,
        request.min_score,
        request.num_probes,
        request.search_effort,
        stats,
        budget.cancellation,
    );
    defer indexed_reader.freeScoredDocs(alloc, hits);
    try budget.checkCancellation();

    const keys = try alloc.alloc([]const u8, hits.len);
    defer alloc.free(keys);
    for (hits, 0..) |hit, idx| {
        if (idx % 64 == 0) try budget.checkCancellation();
        keys[idx] = hit.doc_id;
    }

    return try candidateSetFromKeysBudgetAlloc(alloc, declaration, keys, budget);
}

pub fn vectorCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: VectorCandidatePlan,
    stats: *indexed_reader.SearchExecutionStats,
) !OwnedCandidateSets {
    return try vectorCandidateSetsFromArtifactStoreWithOptionsAlloc(alloc, artifacts, declarations, plan, stats, .{});
}

pub fn vectorCandidateSetsFromArtifactStoreWithOptionsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: VectorCandidatePlan,
    stats: *indexed_reader.SearchExecutionStats,
    options: CandidateServingOptions,
) !OwnedCandidateSets {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try budget.admitPlans(1);
    return try vectorCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, stats, options.limits, &budget, null);
}

fn vectorCandidateSetsFromArtifactStoreBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: VectorCandidatePlan,
    stats: *indexed_reader.SearchExecutionStats,
    limits: CandidateServingLimits,
    budget: *CandidateServingBudget,
    declaration_index: ?*const SidecarDeclarationIndex,
) !OwnedCandidateSets {
    try validateVectorCandidateRequest(plan.request, limits);
    if (plan.request.vector.len == 0 or plan.request.limit == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };
    const query_shape = try vectorCandidateQueryShape(plan.request);
    try budget.admitQueryShape(query_shape.items, query_shape.bytes);

    const selected = try selectedVectorDeclarationsAlloc(alloc, declarations, declaration_index, plan.sidecar_names, budget);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try loadVerifiedArtifactAlloc(artifacts, declaration, budget);
        defer artifacts.allocator.free(payload);
        var request = plan.request;
        request.limit = budget.generationLimit(request.limit);
        const produced = try vectorCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, request, stats, budget);
        sets.appendAssumeCapacity(produced);
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn graphCandidateSetFromPayloadAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
) !OwnedCandidateSet {
    return try graphCandidateSetFromPayloadWithLimitsAlloc(alloc, declaration, payload, request, .{});
}

pub fn graphCandidateSetFromPayloadWithLimitsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
    limits: CandidateServingLimits,
) !OwnedCandidateSet {
    return try graphCandidateSetFromPayloadWithOptionsAlloc(alloc, declaration, payload, request, .{ .limits = limits });
}

pub fn graphCandidateSetFromPayloadWithOptionsAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
    options: CandidateServingOptions,
) !OwnedCandidateSet {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try validateGraphCandidateRequest(request, options.limits);
    try budget.checkCancellation();
    try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, options.limits, options.cancellation);
    var bounded_request = request;
    bounded_request.limit = budget.generationLimit(request.limit);
    return try graphCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, bounded_request, &budget);
}

fn graphCandidateSetFromPayloadWithBudgetAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, budget.limits.decode.max_allocation_bytes);
    return graphCandidateSetFromPayloadBoundedAlloc(limiter.allocator(), declaration, payload, request, budget) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.LakeSidecarCandidateBudgetExceeded;
        return err;
    };
}

fn graphCandidateSetFromPayloadBoundedAlloc(
    alloc: Allocator,
    declaration: sidecar_manifest.DeclaredArtifact,
    payload: []const u8,
    request: GraphCandidateRequest,
    budget: *CandidateServingBudget,
) !OwnedCandidateSet {
    try budget.checkCancellation();
    try declaration.validate();
    if (declaration.binding.sidecar_kind != .graph) return error.UnsupportedLakeSidecarCandidateSource;
    if (declaration.artifact.kind != artifact_ref.ArtifactKind.graph_segment) return error.UnsupportedLakeSidecarCandidateSource;

    const start_ref = try source_binding.rowRefFromKeyAlloc(alloc, request.start_node_id);
    defer source_binding.freeOwnedRowRef(alloc, start_ref);
    try source_binding.validateCandidateRowRefsAgainstBinding(declaration.binding, &[_]rowsource.RowRef{start_ref});

    var segment = try graph_segment.decodeAllocWithLimits(alloc, payload, budget.limits.decode);
    defer graph_segment.freeSegment(alloc, &segment);
    try budget.checkCancellation();
    var edge_type_filter = try GraphEdgeTypeFilter.init(alloc, request.edge_types);
    defer edge_type_filter.deinit(alloc);

    const keys = switch (request.mode) {
        .neighbors => try graphNeighborCandidateKeysAlloc(alloc, segment, request, &edge_type_filter, budget),
        .traverse => try graphTraversalCandidateKeysAlloc(alloc, segment, request, &edge_type_filter, budget),
    };
    defer {
        for (keys) |key| alloc.free(@constCast(key));
        if (keys.len > 0) alloc.free(keys);
    }

    return try candidateSetFromKeysBudgetAlloc(alloc, declaration, keys, budget);
}

pub fn graphCandidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: GraphCandidatePlan,
) !OwnedCandidateSets {
    return try graphCandidateSetsFromArtifactStoreWithOptionsAlloc(alloc, artifacts, declarations, plan, .{});
}

pub fn graphCandidateSetsFromArtifactStoreWithOptionsAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: GraphCandidatePlan,
    options: CandidateServingOptions,
) !OwnedCandidateSets {
    var budget = try CandidateServingBudget.initWithCancellation(options.limits, options.cancellation);
    try budget.admitPlans(1);
    return try graphCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, options.limits, &budget, null);
}

fn graphCandidateSetsFromArtifactStoreBudgetAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plan: GraphCandidatePlan,
    limits: CandidateServingLimits,
    budget: *CandidateServingBudget,
    declaration_index: ?*const SidecarDeclarationIndex,
) !OwnedCandidateSets {
    try validateGraphCandidateRequest(plan.request, limits);
    if (plan.request.start_node_id.len == 0 or plan.request.limit == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };
    const query_shape = try graphCandidateQueryShape(plan.request);
    try budget.admitQueryShape(query_shape.items, query_shape.bytes);

    const selected = try selectedGraphDeclarationsAlloc(alloc, declarations, declaration_index, plan.sidecar_names, budget);
    defer if (selected.len > 0) alloc.free(selected);

    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer {
        for (sets.items) |*set| set.deinit(alloc);
        sets.deinit(alloc);
    }
    try sets.ensureUnusedCapacity(alloc, selected.len);

    for (selected) |declaration| {
        const payload = try loadVerifiedArtifactAlloc(artifacts, declaration, budget);
        defer artifacts.allocator.free(payload);
        var request = plan.request;
        request.limit = budget.generationLimit(request.limit);
        const produced = try graphCandidateSetFromPayloadWithBudgetAlloc(alloc, declaration, payload, request, budget);
        sets.appendAssumeCapacity(produced);
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

pub fn candidateSetsFromArtifactStoreAlloc(
    alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    plans: CandidatePlanSet,
) !OwnedCandidateSets {
    var budget = try CandidateServingBudget.initWithCancellation(plans.limits, plans.cancellation);
    var plan_count = std.math.add(usize, plans.text_plans.len, plans.sparse_plans.len) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    plan_count = std.math.add(usize, plan_count, plans.vector_plans.len) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    plan_count = std.math.add(usize, plan_count, plans.graph_plans.len) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    try budget.admitPlans(plan_count);
    if (plan_count == 0) return .{ .sets = try alloc.alloc(OwnedCandidateSet, 0) };
    if (declarations.len > plans.limits.max_declarations) return error.LakeSidecarCandidateBudgetExceeded;
    var declaration_index = try SidecarDeclarationIndex.initWithCancellation(alloc, declarations, plans.cancellation);
    defer declaration_index.deinit(alloc);
    var sets = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    errdefer deinitOwnedCandidateSetList(alloc, &sets);

    for (plans.text_plans) |plan| {
        try budget.checkCancellation();
        var produced = try textCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, plans.limits, &budget, &declaration_index);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.takeSets());
    }
    for (plans.sparse_plans) |plan| {
        try budget.checkCancellation();
        var produced = try sparseCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, plans.limits, &budget, &declaration_index);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.takeSets());
    }
    var local_vector_stats = indexed_reader.SearchExecutionStats{};
    const vector_stats = plans.vector_stats orelse &local_vector_stats;
    for (plans.vector_plans) |plan| {
        try budget.checkCancellation();
        var produced = try vectorCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, vector_stats, plans.limits, &budget, &declaration_index);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.takeSets());
    }
    for (plans.graph_plans) |plan| {
        try budget.checkCancellation();
        var produced = try graphCandidateSetsFromArtifactStoreBudgetAlloc(alloc, artifacts, declarations, plan, plans.limits, &budget, &declaration_index);
        defer produced.deinit(alloc);
        try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &sets, produced.takeSets());
    }

    return .{ .sets = try sets.toOwnedSlice(alloc) };
}

const CandidateQueryShape = struct {
    items: usize,
    bytes: usize,
};

fn textCandidateQueryShape(request: TextCandidateRequest) !CandidateQueryShape {
    return .{
        .items = @intFromBool(request.text.len != 0),
        .bytes = request.text.len,
    };
}

fn sparseCandidateQueryShape(request: SparseCandidateRequest) !CandidateQueryShape {
    return try sparseCandidateQueryShapeWithCancellation(request, .none);
}

fn sparseCandidateQueryShapeWithCancellation(
    request: SparseCandidateRequest,
    cancellation: CancellationToken,
) !CandidateQueryShape {
    try cancellation.check();
    var bytes: usize = 0;
    for (request.terms, 0..) |term, idx| {
        if (idx % 64 == 0) try cancellation.check();
        bytes = std.math.add(usize, bytes, term.term.len) catch return error.LakeSidecarCandidateBudgetExceeded;
    }
    return .{ .items = request.terms.len, .bytes = bytes };
}

fn vectorCandidateQueryShape(request: VectorCandidateRequest) !CandidateQueryShape {
    const bytes = std.math.mul(usize, request.vector.len, @sizeOf(f32)) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    return .{ .items = request.vector.len, .bytes = bytes };
}

fn graphCandidateQueryShape(request: GraphCandidateRequest) !CandidateQueryShape {
    const edge_types = request.edge_types orelse &.{};
    var bytes = request.start_node_id.len;
    for (edge_types) |edge_type| {
        bytes = std.math.add(usize, bytes, edge_type.len) catch return error.LakeSidecarCandidateBudgetExceeded;
    }
    const items = std.math.add(usize, @intFromBool(request.start_node_id.len != 0), edge_types.len) catch
        return error.LakeSidecarCandidateBudgetExceeded;
    return .{ .items = items, .bytes = bytes };
}

fn validateTextCandidateRequest(request: TextCandidateRequest, limits: CandidateServingLimits) !void {
    try limits.validate();
    try validateCandidateWindow(request.offset, request.limit, limits);
    const shape = try textCandidateQueryShape(request);
    if (shape.items > limits.max_query_items or shape.bytes > limits.max_query_bytes) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
}

fn validateSparseCandidateRequest(request: SparseCandidateRequest, limits: CandidateServingLimits) !void {
    return try validateSparseCandidateRequestWithCancellation(request, limits, .none);
}

fn validateSparseCandidateRequestWithCancellation(
    request: SparseCandidateRequest,
    limits: CandidateServingLimits,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try limits.validate();
    try validateCandidateWindow(request.offset, request.limit, limits);
    const shape = try sparseCandidateQueryShapeWithCancellation(request, cancellation);
    if (shape.items > limits.max_query_items or shape.bytes > limits.max_query_bytes) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
}

fn validateVectorCandidateRequest(request: VectorCandidateRequest, limits: CandidateServingLimits) !void {
    try limits.validate();
    try validateCandidateWindow(request.offset, request.limit, limits);
    const shape = try vectorCandidateQueryShape(request);
    if (shape.items > limits.max_query_items or shape.bytes > limits.max_query_bytes) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
}

fn validateGraphCandidateRequest(request: GraphCandidateRequest, limits: CandidateServingLimits) !void {
    try limits.validate();
    try validateCandidateWindow(0, request.limit, limits);
    if (request.max_depth > limits.max_graph_depth) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
    const edge_types = request.edge_types orelse &.{};
    if (edge_types.len > limits.max_graph_edge_types) return error.LakeSidecarCandidateBudgetExceeded;
    var edge_type_bytes: usize = 0;
    for (edge_types) |edge_type| {
        edge_type_bytes = std.math.add(usize, edge_type_bytes, edge_type.len) catch
            return error.LakeSidecarCandidateBudgetExceeded;
    }
    if (edge_type_bytes > limits.max_graph_edge_type_bytes) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
    const shape = try graphCandidateQueryShape(request);
    if (shape.items > limits.max_query_items or shape.bytes > limits.max_query_bytes) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }
}

fn validateCandidateWindow(offset: usize, limit: usize, limits: CandidateServingLimits) !void {
    const window = std.math.add(usize, offset, limit) catch return error.LakeSidecarCandidateBudgetExceeded;
    if (window > limits.max_candidate_window) return error.LakeSidecarCandidateBudgetExceeded;
}

fn validatePayloadAgainstDeclaration(
    payload: []const u8,
    declaration: sidecar_manifest.DeclaredArtifact,
    limits: CandidateServingLimits,
) !void {
    return try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, limits, .none);
}

fn validatePayloadAgainstDeclarationWithCancellation(
    payload: []const u8,
    declaration: sidecar_manifest.DeclaredArtifact,
    limits: CandidateServingLimits,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    try limits.validate();
    try declaration.validate();
    artifacts_mod.validateSha256ArtifactIdentity(
        declaration.artifact.artifact_id,
        declaration.artifact.checksum,
    ) catch return error.LakeSidecarArtifactIntegrityMismatch;
    if (payload.len > limits.decode.max_artifact_bytes) return error.LakeSidecarCandidateBudgetExceeded;
    const declared_len = std.math.cast(usize, declaration.artifact.byte_len) orelse
        return error.LakeSidecarArtifactIntegrityMismatch;
    if (payload.len != declared_len) return error.LakeSidecarArtifactIntegrityMismatch;
    if (!try payloadMatchesSha256(payload, declaration.artifact.checksum, cancellation)) {
        return error.LakeSidecarArtifactIntegrityMismatch;
    }
}

fn loadVerifiedArtifactAlloc(
    artifacts: *artifacts_mod.ArtifactStore,
    declaration: sidecar_manifest.DeclaredArtifact,
    budget: *CandidateServingBudget,
) ![]u8 {
    try budget.checkCancellation();
    try declaration.validate();
    artifacts_mod.validateSha256ArtifactIdentity(
        declaration.artifact.artifact_id,
        declaration.artifact.checksum,
    ) catch return error.LakeSidecarArtifactIntegrityMismatch;
    // Check at each transport boundary: cancellation may arrive while local
    // validation or a preceding artifact is being processed.
    try budget.checkCancellation();
    var metadata = try artifacts.statWithCancellation(
        declaration.artifact.artifact_id,
        budget.cancellation,
    );
    defer metadata.deinit(artifacts.allocator);
    if (!std.mem.eql(u8, metadata.artifact_id, declaration.artifact.artifact_id) or
        metadata.byte_len != declaration.artifact.byte_len or
        !std.mem.eql(u8, metadata.checksum, declaration.artifact.checksum))
    {
        return error.LakeSidecarArtifactIntegrityMismatch;
    }
    const admitted_len = try budget.admitArtifact(metadata.byte_len);
    try budget.checkCancellation();

    // Fetch exactly the admitted range so remote implementations can enforce
    // the bound at the transport layer as well as after download.
    const payload = try artifacts.getRangeAllocWithCancellation(
        declaration.artifact.artifact_id,
        0,
        admitted_len,
        budget.cancellation,
    );
    errdefer artifacts.allocator.free(payload);
    try validatePayloadAgainstDeclarationWithCancellation(payload, declaration, budget.limits, budget.cancellation);
    return payload;
}

fn payloadMatchesSha256(
    payload: []const u8,
    expected: []const u8,
    cancellation: CancellationToken,
) !bool {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < payload.len) {
        try cancellation.check();
        const end = @min(payload.len, offset +| 1024 * 1024);
        hasher.update(payload[offset..end]);
        offset = end;
    }
    try cancellation.check();
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, &actual, expected);
}

const TestSha256Identity = struct {
    checksum: [artifacts_mod.store.sha256_checksum_len]u8,
    artifact_id: [artifacts_mod.store.sha256_artifact_id_prefix.len + artifacts_mod.store.sha256_checksum_len]u8,

    fn fromPayload(payload: []const u8) TestSha256Identity {
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
        const checksum = std.fmt.bytesToHex(digest, .lower);
        var identity: TestSha256Identity = undefined;
        identity.checksum = checksum;
        @memcpy(identity.artifact_id[0..artifacts_mod.store.sha256_artifact_id_prefix.len], artifacts_mod.store.sha256_artifact_id_prefix);
        @memcpy(identity.artifact_id[artifacts_mod.store.sha256_artifact_id_prefix.len..], &identity.checksum);
        return identity;
    }
};

test "lake sidecar candidate windows reject overflow before artifact work" {
    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = .{
            .sidecar_kind = .text,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .schema_fingerprint = "schema-v1",
            .index_config_hash = "sha256:text",
        },
        .artifact = .{
            .kind = .text_segment,
            .artifact_id = "unused",
            .byte_len = 0,
            .checksum = "unused",
        },
    };
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        textCandidateSetFromPayloadAlloc(std.testing.allocator, declaration, &.{}, .{
            .text = "query",
            .offset = std.math.maxInt(usize),
        }),
    );
}

test "lake sidecar candidate payload validation rejects noncanonical content addresses" {
    const declaration = sidecar_manifest.DeclaredArtifact{
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
            .artifact_id = "sha256:abcd",
            .byte_len = 4,
            .checksum = "abcd",
        },
    };
    try std.testing.expectError(
        error.LakeSidecarArtifactIntegrityMismatch,
        validatePayloadAgainstDeclaration("data", declaration, .{}),
    );
}

test "lake sidecar candidate budget bounds generation with one overflow sentinel" {
    var budget = try CandidateServingBudget.init(.{
        .max_total_candidates = 2,
        .max_total_candidate_bytes = 1024,
    });
    try std.testing.expectEqual(@as(usize, 3), budget.generationLimit(100));
    try budget.admitCandidateOutput(2, 6);
    try std.testing.expectEqual(@as(usize, 1), budget.generationLimit(100));
    try std.testing.expectError(error.LakeSidecarCandidateBudgetExceeded, budget.admitCandidateOutput(1, 1));
    try std.testing.expectEqual(@as(usize, 2), budget.total_candidates);
    try std.testing.expectEqual(@as(usize, 6), budget.total_candidate_bytes);

    var byte_budget = try CandidateServingBudget.init(.{ .max_total_candidate_bytes = 10 });
    try std.testing.expectEqual(@as(usize, 1), byte_budget.generationLimit(100));
    try byte_budget.admitCandidateOutput(0, 6);
    try std.testing.expectError(error.LakeSidecarCandidateBudgetExceeded, byte_budget.admitCandidateOutput(0, 5));
    try std.testing.expectEqual(@as(usize, 6), byte_budget.total_candidate_bytes);
}

test "lake sidecar candidate output rejects retained bytes before allocation" {
    const declaration = testCandidateDeclaration("text-a", .text, .text_segment);
    const keys = [_][]const u8{"ext:6:source:8:snapshot:4:file:0:1"};
    var budget = try CandidateServingBudget.init(.{ .max_total_candidate_bytes = 1 });
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });

    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        candidateSetFromKeysBudgetAlloc(failing.allocator(), declaration, &keys, &budget),
    );
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    try std.testing.expectEqual(@as(usize, 0), budget.total_candidates);
    try std.testing.expectEqual(@as(usize, 0), budget.total_candidate_bytes);
}

test "lake sidecar candidate output byte admission is aggregate across sets" {
    const declaration = testCandidateDeclaration("text-a", .text, .text_segment);
    const keys = [_][]const u8{"ext:6:source:8:snapshot:4:file:0:1"};
    const retained_bytes = try source_binding.rowRefsOwnedAllocationBytesFromKeys(declaration.binding, &keys);
    const one_set_bytes = retained_bytes + declaration.name.len;
    var budget = try CandidateServingBudget.init(.{ .max_total_candidate_bytes = one_set_bytes });

    var first = try candidateSetFromKeysBudgetAlloc(std.testing.allocator, declaration, &keys, &budget);
    defer first.deinit(std.testing.allocator);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        candidateSetFromKeysBudgetAlloc(failing.allocator(), declaration, &keys, &budget),
    );
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(one_set_bytes, budget.total_candidate_bytes);
}

fn selectedTextDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    declaration_index: ?*const SidecarDeclarationIndex,
    maybe_names: ?[]const []const u8,
    budget: *CandidateServingBudget,
) ![]sidecar_manifest.DeclaredArtifact {
    return try selectedDeclarationsAlloc(alloc, declarations, declaration_index, maybe_names, budget, .text);
}

fn selectedGraphDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    declaration_index: ?*const SidecarDeclarationIndex,
    maybe_names: ?[]const []const u8,
    budget: *CandidateServingBudget,
) ![]sidecar_manifest.DeclaredArtifact {
    return try selectedDeclarationsAlloc(alloc, declarations, declaration_index, maybe_names, budget, .graph);
}

fn selectedVectorDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    declaration_index: ?*const SidecarDeclarationIndex,
    maybe_names: ?[]const []const u8,
    budget: *CandidateServingBudget,
) ![]sidecar_manifest.DeclaredArtifact {
    return try selectedDeclarationsAlloc(alloc, declarations, declaration_index, maybe_names, budget, .vector);
}

fn selectedSparseDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    declaration_index: ?*const SidecarDeclarationIndex,
    maybe_names: ?[]const []const u8,
    budget: *CandidateServingBudget,
) ![]sidecar_manifest.DeclaredArtifact {
    return try selectedDeclarationsAlloc(alloc, declarations, declaration_index, maybe_names, budget, .sparse);
}

const SidecarDeclarationKind = enum {
    text,
    sparse,
    vector,
    graph,
};

const SidecarDeclarationIndex = struct {
    by_name: std.StringHashMapUnmanaged(sidecar_manifest.DeclaredArtifact) = .empty,
    implicit: [4]?sidecar_manifest.DeclaredArtifact = [_]?sidecar_manifest.DeclaredArtifact{null} ** 4,
    implicit_ambiguous: [4]bool = [_]bool{false} ** 4,

    fn init(
        alloc: Allocator,
        declarations: []const sidecar_manifest.DeclaredArtifact,
    ) !SidecarDeclarationIndex {
        return try initWithCancellation(alloc, declarations, .none);
    }

    fn initWithCancellation(
        alloc: Allocator,
        declarations: []const sidecar_manifest.DeclaredArtifact,
        cancellation: CancellationToken,
    ) !SidecarDeclarationIndex {
        try cancellation.check();
        var index = SidecarDeclarationIndex{};
        errdefer index.deinit(alloc);
        for (declarations, 0..) |declaration, idx| {
            if (idx % 64 == 0) try cancellation.check();
            const kind = declarationKind(declaration) orelse continue;
            const entry = try index.by_name.getOrPut(alloc, declaration.name);
            if (entry.found_existing) return error.AmbiguousLakeSidecarCandidateSource;
            entry.value_ptr.* = declaration;
            const kind_index = @intFromEnum(kind);
            if (index.implicit[kind_index] == null) {
                index.implicit[kind_index] = declaration;
            } else {
                index.implicit_ambiguous[kind_index] = true;
            }
        }
        return index;
    }

    fn deinit(self: *SidecarDeclarationIndex, alloc: Allocator) void {
        self.by_name.deinit(alloc);
        self.* = undefined;
    }
};

fn selectedDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
    maybe_declaration_index: ?*const SidecarDeclarationIndex,
    maybe_names: ?[]const []const u8,
    budget: *CandidateServingBudget,
    kind: SidecarDeclarationKind,
) ![]sidecar_manifest.DeclaredArtifact {
    try budget.checkCancellation();
    var selected = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer selected.deinit(alloc);
    if (maybe_declaration_index == null and declarations.len > budget.limits.max_declarations) {
        return error.LakeSidecarCandidateBudgetExceeded;
    }

    if (maybe_names) |names| {
        var total_name_bytes: usize = 0;
        for (names, 0..) |name, idx| {
            if (idx % 64 == 0) try budget.checkCancellation();
            if (name.len == 0) return error.InvalidLakeSidecarCandidateRequest;
            total_name_bytes = std.math.add(usize, total_name_bytes, name.len) catch
                return error.LakeSidecarCandidateBudgetExceeded;
        }
        try budget.admitQueryShape(names.len, total_name_bytes);

        var unique_names = std.ArrayListUnmanaged([]const u8).empty;
        defer unique_names.deinit(alloc);
        var requested = std.StringHashMapUnmanaged(void).empty;
        defer requested.deinit(alloc);
        for (names, 0..) |name, idx| {
            if (idx % 64 == 0) try budget.checkCancellation();
            const entry = try requested.getOrPut(alloc, name);
            if (entry.found_existing) continue;
            if (unique_names.items.len >= budget.remainingArtifacts()) return error.LakeSidecarCandidateBudgetExceeded;
            try unique_names.append(alloc, name);
        }

        var local_index: SidecarDeclarationIndex = undefined;
        const declaration_index = maybe_declaration_index orelse blk: {
            local_index = try SidecarDeclarationIndex.initWithCancellation(alloc, declarations, budget.cancellation);
            break :blk &local_index;
        };
        defer if (maybe_declaration_index == null) local_index.deinit(alloc);

        try selected.ensureTotalCapacity(alloc, unique_names.items.len);
        for (unique_names.items, 0..) |name, idx| {
            if (idx % 64 == 0) try budget.checkCancellation();
            const declaration = declaration_index.by_name.get(name) orelse return error.MissingLakeSidecarCandidateSource;
            if (!isDeclarationKind(declaration, kind)) return error.MissingLakeSidecarCandidateSource;
            selected.appendAssumeCapacity(declaration);
        }
        return try selected.toOwnedSlice(alloc);
    }

    var local_index: SidecarDeclarationIndex = undefined;
    const declaration_index = maybe_declaration_index orelse blk: {
        local_index = try SidecarDeclarationIndex.initWithCancellation(alloc, declarations, budget.cancellation);
        break :blk &local_index;
    };
    defer if (maybe_declaration_index == null) local_index.deinit(alloc);
    const kind_index = @intFromEnum(kind);
    if (declaration_index.implicit_ambiguous[kind_index]) return error.AmbiguousLakeSidecarCandidateSource;
    if (declaration_index.implicit[kind_index]) |declaration| try selected.append(alloc, declaration);
    return try selected.toOwnedSlice(alloc);
}

fn declarationKind(declaration: sidecar_manifest.DeclaredArtifact) ?SidecarDeclarationKind {
    if (isTextDeclaration(declaration)) return .text;
    if (isSparseDeclaration(declaration)) return .sparse;
    if (isVectorDeclaration(declaration)) return .vector;
    if (isGraphDeclaration(declaration)) return .graph;
    return null;
}

fn isDeclarationKind(declaration: sidecar_manifest.DeclaredArtifact, kind: SidecarDeclarationKind) bool {
    return switch (kind) {
        .text => isTextDeclaration(declaration),
        .sparse => isSparseDeclaration(declaration),
        .vector => isVectorDeclaration(declaration),
        .graph => isGraphDeclaration(declaration),
    };
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

test "lake sidecar selectors are bounded and deduplicated before artifact work" {
    const alloc = std.testing.allocator;
    const declarations = [_]sidecar_manifest.DeclaredArtifact{
        testCandidateDeclaration("text-a", .text, .text_segment),
        testCandidateDeclaration("text-b", .text, .text_segment),
        testCandidateDeclaration("vector-a", .vector, .vector_segment),
    };
    const names = [_][]const u8{ "text-b", "text-b", "text-a" };
    var dedupe_budget = try CandidateServingBudget.init(.{ .max_artifacts = 2 });
    const selected = try selectedTextDeclarationsAlloc(alloc, &declarations, null, &names, &dedupe_budget);
    defer alloc.free(selected);
    try std.testing.expectEqual(@as(usize, 2), selected.len);
    try std.testing.expectEqualStrings("text-b", selected[0].name);
    try std.testing.expectEqualStrings("text-a", selected[1].name);

    var item_budget = try CandidateServingBudget.init(.{ .max_query_items = 2 });
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        selectedTextDeclarationsAlloc(alloc, &declarations, null, &names, &item_budget),
    );
    var byte_budget = try CandidateServingBudget.init(.{ .max_query_bytes = 12 });
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        selectedTextDeclarationsAlloc(alloc, &declarations, null, &names, &byte_budget),
    );
    const distinct_names = [_][]const u8{ "text-a", "text-b" };
    var artifact_budget = try CandidateServingBudget.init(.{ .max_artifacts = 1 });
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        selectedTextDeclarationsAlloc(alloc, &declarations, null, &distinct_names, &artifact_budget),
    );

    var declaration_index = try SidecarDeclarationIndex.init(alloc, &declarations);
    defer declaration_index.deinit(alloc);
    var aggregate_budget = try CandidateServingBudget.init(.{
        .max_query_items = 3,
        .max_artifacts = 4,
    });
    const first = try selectedTextDeclarationsAlloc(
        alloc,
        &declarations,
        &declaration_index,
        &distinct_names,
        &aggregate_budget,
    );
    defer alloc.free(first);
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        selectedTextDeclarationsAlloc(
            alloc,
            &declarations,
            &declaration_index,
            &distinct_names,
            &aggregate_budget,
        ),
    );
}

test "lake sidecar query shapes and plan counts use aggregate admission" {
    var budget = try CandidateServingBudget.init(.{
        .max_plans = 2,
        .max_query_items = 3,
        .max_query_bytes = 8,
    });
    try budget.admitPlans(2);
    try std.testing.expectError(error.LakeSidecarCandidateBudgetExceeded, budget.admitPlans(1));

    try budget.admitQueryShape(2, 4);
    try budget.admitQueryShape(1, 4);
    try std.testing.expectError(error.LakeSidecarCandidateBudgetExceeded, budget.admitQueryShape(1, 0));

    var byte_budget = try CandidateServingBudget.init(.{ .max_query_bytes = 7 });
    try byte_budget.admitQueryShape(0, 4);
    try std.testing.expectError(error.LakeSidecarCandidateBudgetExceeded, byte_budget.admitQueryShape(0, 4));
}

test "lake graph sidecar candidate admission bounds filters, edge work, and cancellation" {
    const edge_types = [_][]const u8{ "cites", "related" };
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        validateGraphCandidateRequest(.{
            .start_node_id = "doc-a",
            .edge_types = &edge_types,
        }, .{ .max_graph_edge_types = 1 }),
    );
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        validateGraphCandidateRequest(.{
            .start_node_id = "doc-a",
            .edge_types = &edge_types,
        }, .{ .max_graph_edge_type_bytes = 6 }),
    );

    var filter = try GraphEdgeTypeFilter.init(std.testing.allocator, &.{"wanted"});
    defer filter.deinit(std.testing.allocator);
    const edges = [_]graph_segment.Edge{
        .{ .neighbor_id = @constCast("doc-b"), .edge_type = @constCast("other"), .weight = 1 },
        .{ .neighbor_id = @constCast("doc-c"), .edge_type = @constCast("other"), .weight = 1 },
        .{ .neighbor_id = @constCast("doc-d"), .edge_type = @constCast("other"), .weight = 1 },
    };
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    defer keys.deinit(std.testing.allocator);
    var budget = try CandidateServingBudget.init(.{ .max_graph_edges_scanned = 2 });
    try std.testing.expectError(
        error.LakeSidecarCandidateBudgetExceeded,
        appendGraphNeighborKeysAlloc(
            std.testing.allocator,
            &keys,
            &edges,
            .{ .start_node_id = "doc-a", .limit = 1 },
            &filter,
            &budget,
        ),
    );
    try std.testing.expectEqual(@as(usize, 2), budget.graph_edges_scanned);
    try std.testing.expectEqual(@as(usize, 0), keys.items.len);

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(
        error.Canceled,
        CandidateServingBudget.initWithCancellation(.{}, CancellationToken.fromAtomic(&cancelled)),
    );
}

test "lake sidecar cancellation reaches metadata and active artifact range operations" {
    const alloc = std.testing.allocator;
    const checksum = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
    const artifact_id = "sha256:" ++ checksum;

    const CancellationStore = struct {
        const State = struct {
            cancelled: *std.atomic.Value(bool),
            cancel_on_stat: bool = true,
            stat_calls: usize = 0,
            range_calls: usize = 0,
        };

        fn deinit(_: Allocator, _: *anyopaque) void {}

        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnsupportedTestOperation;
        }

        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnsupportedTestOperation;
        }

        fn getRangeAlloc(
            ptr: *anyopaque,
            allocator: Allocator,
            _: []const u8,
            _: u64,
            _: usize,
        ) ![]u8 {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.range_calls += 1;
            return try allocator.dupe(u8, "x");
        }

        fn getRangeAllocWithCancellation(
            ptr: *anyopaque,
            _: Allocator,
            _: []const u8,
            _: u64,
            _: usize,
            cancellation: CancellationToken,
        ) ![]u8 {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.range_calls += 1;
            state.cancelled.store(true, .release);
            try cancellation.check();
            return error.TestExpectedCancellation;
        }

        fn stat(ptr: *anyopaque, allocator: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            const state: *State = @ptrCast(@alignCast(ptr));
            state.stat_calls += 1;
            if (state.cancel_on_stat) state.cancelled.store(true, .release);
            const owned_id = try allocator.dupe(u8, artifact_id);
            errdefer allocator.free(owned_id);
            return .{
                .artifact_id = owned_id,
                .byte_len = 1,
                .checksum = try allocator.dupe(u8, checksum),
            };
        }

        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnsupportedTestOperation;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_range_alloc = getRangeAlloc,
            .get_range_alloc_with_cancellation = getRangeAllocWithCancellation,
            .stat = stat,
            .delete = delete,
        };
    };

    var cancelled = std.atomic.Value(bool).init(false);
    var state = CancellationStore.State{ .cancelled = &cancelled };
    var store = artifacts_mod.ArtifactStore{
        .allocator = alloc,
        .ptr = &state,
        .vtable = &CancellationStore.vtable,
    };
    defer store.deinit();

    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = .{
            .sidecar_kind = .text,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .schema_fingerprint = "schema-v1",
            .column_bindings = &.{"body"},
            .index_config_hash = "sha256:text",
        },
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = artifact_id,
            .byte_len = 1,
            .checksum = checksum,
        },
    };
    const names = [_][]const u8{"events.body.text"};
    try std.testing.expectError(
        error.Canceled,
        textCandidateSetsFromArtifactStoreWithOptionsAlloc(
            alloc,
            &store,
            &.{declaration},
            .{ .request = .{ .text = "query" }, .sidecar_names = &names },
            .{ .cancellation = CancellationToken.fromAtomic(&cancelled) },
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), state.stat_calls);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);

    cancelled.store(false, .release);
    state.cancel_on_stat = false;
    try std.testing.expectError(
        error.Canceled,
        textCandidateSetsFromArtifactStoreWithOptionsAlloc(
            alloc,
            &store,
            &.{declaration},
            .{ .request = .{ .text = "query" }, .sidecar_names = &names },
            .{ .cancellation = CancellationToken.fromAtomic(&cancelled) },
        ),
    );
    try std.testing.expectEqual(@as(usize, 2), state.stat_calls);
    try std.testing.expectEqual(@as(usize, 1), state.range_calls);
}

fn testCandidateDeclaration(
    name: []const u8,
    sidecar_kind: source_binding.SidecarKind,
    artifact_kind: artifact_ref.ArtifactKind,
) sidecar_manifest.DeclaredArtifact {
    return .{
        .name = name,
        .binding = .{
            .sidecar_kind = sidecar_kind,
            .source_kind = .external_parquet,
            .row_ref_kind = .external,
            .source_id = "source",
            .snapshot_id = "snapshot",
            .schema_fingerprint = "schema",
            .index_config_hash = "config",
        },
        .artifact = .{
            .kind = artifact_kind,
            .name = name,
            .artifact_id = "artifact",
            .byte_len = 1,
            .checksum = "checksum",
        },
    };
}

const GraphEdgeTypeFilter = struct {
    allow_all: bool,
    by_name: std.StringHashMapUnmanaged(void) = .empty,

    fn init(alloc: Allocator, maybe_edge_types: ?[]const []const u8) !GraphEdgeTypeFilter {
        const edge_types = maybe_edge_types orelse return .{ .allow_all = true };
        var filter = GraphEdgeTypeFilter{ .allow_all = false };
        errdefer filter.deinit(alloc);
        try filter.by_name.ensureTotalCapacity(alloc, @intCast(edge_types.len));
        for (edge_types) |edge_type| filter.by_name.putAssumeCapacity(edge_type, {});
        return filter;
    }

    fn deinit(self: *GraphEdgeTypeFilter, alloc: Allocator) void {
        self.by_name.deinit(alloc);
        self.* = undefined;
    }

    fn matches(self: GraphEdgeTypeFilter, candidate: []const u8) bool {
        return self.allow_all or self.by_name.contains(candidate);
    }
};

fn graphNeighborCandidateKeysAlloc(
    alloc: Allocator,
    segment: graph_segment.Segment,
    request: GraphCandidateRequest,
    edge_type_filter: *const GraphEdgeTypeFilter,
    budget: *CandidateServingBudget,
) ![]const []const u8 {
    const adjacency = try findGraphAdjacency(segment, request.start_node_id, budget) orelse return try alloc.alloc([]const u8, 0);
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (keys.items) |key| alloc.free(@constCast(key));
        keys.deinit(alloc);
    }
    if (request.direction == .out or request.direction == .both) {
        try appendGraphNeighborKeysAlloc(alloc, &keys, adjacency.out_edges, request, edge_type_filter, budget);
    }
    if (request.direction == .in or request.direction == .both) {
        try appendGraphNeighborKeysAlloc(alloc, &keys, adjacency.in_edges, request, edge_type_filter, budget);
    }
    return try keys.toOwnedSlice(alloc);
}

fn appendGraphNeighborKeysAlloc(
    alloc: Allocator,
    keys: *std.ArrayListUnmanaged([]const u8),
    edges: []const graph_segment.Edge,
    request: GraphCandidateRequest,
    edge_type_filter: *const GraphEdgeTypeFilter,
    budget: *CandidateServingBudget,
) !void {
    for (edges) |edge| {
        if (keys.items.len >= request.limit) return;
        try budget.admitGraphEdge();
        if (!edge_type_filter.matches(edge.edge_type)) continue;
        try keys.ensureUnusedCapacity(alloc, 1);
        keys.appendAssumeCapacity(try alloc.dupe(u8, edge.neighbor_id));
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
    edge_type_filter: *const GraphEdgeTypeFilter,
    budget: *CandidateServingBudget,
) ![]const []const u8 {
    var adjacency_index = try graph_segment.AdjacencyIndex.initWithCancellation(alloc, segment, budget.cancellation);
    defer adjacency_index.deinit(alloc);
    if (adjacency_index.find(segment, request.start_node_id) == null) return try alloc.alloc([]const u8, 0);

    var queue = std.ArrayListUnmanaged(GraphQueueItem).empty;
    defer queue.deinit(alloc);
    try queue.append(alloc, .{ .node_id = request.start_node_id, .depth = 0 });

    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    try seen.put(alloc, request.start_node_id, {});
    const max_seen = request.limit + @intFromBool(!request.include_start);

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
            try keys.ensureUnusedCapacity(alloc, 1);
            keys.appendAssumeCapacity(try alloc.dupe(u8, item.node_id));
            if (keys.items.len >= request.limit) break;
        }
        if (item.depth == request.max_depth) continue;
        const adjacency = adjacency_index.find(segment, item.node_id) orelse continue;
        if (request.direction == .out or request.direction == .both) {
            try enqueueGraphTraversalEdgesAlloc(alloc, &queue, &seen, adjacency.out_edges, item, max_seen, edge_type_filter, budget);
        }
        if (request.direction == .in or request.direction == .both) {
            try enqueueGraphTraversalEdgesAlloc(alloc, &queue, &seen, adjacency.in_edges, item, max_seen, edge_type_filter, budget);
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
    max_seen: usize,
    edge_type_filter: *const GraphEdgeTypeFilter,
    budget: *CandidateServingBudget,
) !void {
    for (edges) |edge| {
        if (seen.count() >= max_seen) return;
        try budget.admitGraphEdge();
        if (!edge_type_filter.matches(edge.edge_type)) continue;
        const gop = try seen.getOrPut(alloc, edge.neighbor_id);
        if (gop.found_existing) continue;
        try queue.append(alloc, .{
            .node_id = edge.neighbor_id,
            .depth = current.depth + 1,
        });
    }
}

fn findGraphAdjacency(
    segment: graph_segment.Segment,
    node_id: []const u8,
    budget: *CandidateServingBudget,
) !?graph_segment.Adjacency {
    for (segment.adjacencies, 0..) |adjacency, idx| {
        if (idx % 64 == 0) try budget.checkCancellation();
        if (std.mem.eql(u8, adjacency.node_id, node_id)) return adjacency;
    }
    return null;
}

fn appendOwnedCandidateSetsIntersectingDuplicatesAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(OwnedCandidateSet),
    incoming: []OwnedCandidateSet,
) !void {
    var consumed: usize = 0;
    defer {
        for (incoming[consumed..]) |*candidate_set| candidate_set.deinit(alloc);
        if (incoming.len > 0) alloc.free(incoming);
    }
    while (consumed < incoming.len) {
        const candidate_set = &incoming[consumed];
        if (ownedCandidateSetIndexByName(out.items, candidate_set.sidecar_name)) |existing_idx| {
            const intersected = try intersectOwnedRowRefsAlloc(
                alloc,
                out.items[existing_idx].row_refs,
                candidate_set.row_refs,
            );
            source_binding.freeOwnedRowRefs(alloc, out.items[existing_idx].row_refs);
            out.items[existing_idx].row_refs = intersected;
            candidate_set.deinit(alloc);
            consumed += 1;
            continue;
        }
        try out.append(alloc, candidate_set.*);
        consumed += 1;
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

fn intersectOwnedRowRefsAlloc(
    alloc: Allocator,
    lhs: []const rowsource.RowRef,
    rhs: []const rowsource.RowRef,
) ![]rowsource.RowRef {
    var rhs_lookup = source_binding.RowRefSetMap.empty;
    defer rhs_lookup.deinit(alloc);
    const rhs_capacity = std.math.cast(source_binding.RowRefSetMap.Size, rhs.len) orelse
        return error.LakeSidecarCandidateBudgetExceeded;
    try rhs_lookup.ensureTotalCapacity(alloc, rhs_capacity);
    for (rhs) |row_ref| rhs_lookup.putAssumeCapacity(row_ref, {});

    var emitted = source_binding.RowRefSetMap.empty;
    defer emitted.deinit(alloc);
    const emitted_capacity = std.math.cast(source_binding.RowRefSetMap.Size, @min(lhs.len, rhs.len)) orelse
        return error.LakeSidecarCandidateBudgetExceeded;
    try emitted.ensureTotalCapacity(alloc, emitted_capacity);

    var out = std.ArrayListUnmanaged(rowsource.RowRef).empty;
    errdefer {
        for (out.items) |row_ref| source_binding.freeOwnedRowRef(alloc, row_ref);
        out.deinit(alloc);
    }
    for (lhs) |row_ref| {
        if (!rhs_lookup.contains(row_ref)) continue;
        const gop = emitted.getOrPutAssumeCapacity(row_ref);
        if (gop.found_existing) continue;
        try out.ensureUnusedCapacity(alloc, 1);
        out.appendAssumeCapacity(try source_binding.cloneRowRefAlloc(alloc, row_ref));
    }
    return try out.toOwnedSlice(alloc);
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
    const identity = TestSha256Identity.fromPayload(payload);
    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = "events.body.text",
        .binding = binding,
        .artifact = .{
            .kind = .text_segment,
            .name = "events.body.text",
            .artifact_id = &identity.artifact_id,
            .byte_len = payload.len,
            .checksum = &identity.checksum,
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
    const identity = TestSha256Identity.fromPayload(payload);

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        textCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.body.text",
            .binding = binding,
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = &identity.artifact_id,
                .byte_len = payload.len,
                .checksum = &identity.checksum,
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

    var tampered_declaration = declaration;
    tampered_declaration.artifact.checksum = "0000000000000000000000000000000000000000000000000000000000000000";
    try std.testing.expectError(
        error.LakeSidecarArtifactIntegrityMismatch,
        textCandidateSetsFromArtifactStoreAlloc(
            alloc,
            &store,
            &[_]sidecar_manifest.DeclaredArtifact{tampered_declaration},
            .{ .request = .{ .text = "gamma" }, .sidecar_names = &sidecar_names },
        ),
    );
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
    const identity = TestSha256Identity.fromPayload(payload);
    const terms_query = [_]query_request.SparseTermWeight{.{ .term = @constCast("gamma"), .weight = 1.0 }};

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        sparseCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.features.sparse",
            .binding = binding,
            .artifact = .{
                .kind = .sparse_segment,
                .name = "events.features.sparse",
                .artifact_id = &identity.artifact_id,
                .byte_len = payload.len,
                .checksum = &identity.checksum,
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

    var default_stats = indexed_reader.SearchExecutionStats{};
    var default_candidates = try vectorCandidateSetsFromArtifactStoreAlloc(alloc, &store, &[_]sidecar_manifest.DeclaredArtifact{declaration}, .{
        .request = .{
            .vector = &[_]f32{ 0.0, 1.0 },
            .num_probes = 1,
        },
        .sidecar_names = &sidecar_names,
    }, &default_stats);
    defer default_candidates.deinit(alloc);
    try std.testing.expect(default_candidates.sets[0].row_refs.len > 0);
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
    const identity = TestSha256Identity.fromPayload(payload);
    var stats = indexed_reader.SearchExecutionStats{};

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        vectorCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.embedding.vector",
            .binding = binding,
            .artifact = .{
                .kind = .vector_segment,
                .name = "events.embedding.vector",
                .artifact_id = &identity.artifact_id,
                .byte_len = payload.len,
                .checksum = &identity.checksum,
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
    const identity = TestSha256Identity.fromPayload(payload);
    var candidates = try graphCandidateSetFromPayloadAlloc(alloc, .{
        .name = "events.graph",
        .binding = binding,
        .artifact = .{
            .kind = .graph_segment,
            .name = "events.graph",
            .artifact_id = &identity.artifact_id,
            .byte_len = payload.len,
            .checksum = &identity.checksum,
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
    const identity = TestSha256Identity.fromPayload(payload);

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        graphCandidateSetFromPayloadAlloc(alloc, .{
            .name = "events.graph",
            .binding = binding,
            .artifact = .{
                .kind = .graph_segment,
                .name = "events.graph",
                .artifact_id = &identity.artifact_id,
                .byte_len = payload.len,
                .checksum = &identity.checksum,
            },
        }, payload, .{
            .start_node_id = key_a,
            .mode = .neighbors,
            .direction = .out,
            .limit = 10,
        }),
    );
}

test "lake sidecar candidate intersection preserves lhs order and removes duplicates" {
    const alloc = std.testing.allocator;
    const lhs = [_]rowsource.RowRef{
        .{ .relational_key = "row-b" },
        .{ .relational_key = "row-a" },
        .{ .relational_key = "row-b" },
        .{ .relational_key = "row-c" },
    };
    const rhs = [_]rowsource.RowRef{
        .{ .relational_key = "row-c" },
        .{ .relational_key = "row-b" },
        .{ .relational_key = "row-b" },
        .{ .relational_key = "row-d" },
    };

    const intersection = try intersectOwnedRowRefsAlloc(alloc, &lhs, &rhs);
    defer source_binding.freeOwnedRowRefs(alloc, intersection);
    try std.testing.expectEqual(@as(usize, 2), intersection.len);
    try std.testing.expectEqualStrings("row-b", intersection[0].relational_key);
    try std.testing.expectEqualStrings("row-c", intersection[1].relational_key);
}

test "lake sidecar candidate aggregation transfers first-seen ownership" {
    const alloc = std.testing.allocator;
    const row_refs = try alloc.alloc(rowsource.RowRef, 1);
    row_refs[0] = .{ .relational_key = try alloc.dupe(u8, "row-a") };
    const sidecar_name = try alloc.dupe(u8, "rows.primary");
    const incoming = try alloc.alloc(OwnedCandidateSet, 1);
    incoming[0] = .{ .sidecar_name = sidecar_name, .row_refs = row_refs };
    var produced = OwnedCandidateSets{ .sets = incoming };
    defer produced.deinit(alloc);

    var out = std.ArrayListUnmanaged(OwnedCandidateSet).empty;
    defer deinitOwnedCandidateSetList(alloc, &out);
    try appendOwnedCandidateSetsIntersectingDuplicatesAlloc(alloc, &out, produced.takeSets());

    try std.testing.expectEqual(@as(usize, 1), out.items.len);
    try std.testing.expect(out.items[0].sidecar_name.ptr == sidecar_name.ptr);
    try std.testing.expect(out.items[0].row_refs.ptr == row_refs.ptr);
}
