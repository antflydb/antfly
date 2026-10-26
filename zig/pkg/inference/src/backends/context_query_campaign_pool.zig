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

// Draft only: request-owned five-source attention-matching campaign pool.
// Copy beside backends/model_replay_context.zig before integration.
const std = @import("std");
const policy = @import("../runtime/context_policy.zig");
const protocol = @import("context_query_protocol.zig");
const execution_pool = @import("context_execution_pool.zig");
const replay = @import("model_replay_context.zig");
const storage = @import("packed_context_storage.zig");
const shared = @import("metal_generated_kernels.zig");

const absent = std.math.maxInt(u32);

fn ownerStrata(layout: policy.Layout, owner_index: usize) !u32 {
    if (owner_index >= layout.len) return error.InvalidContextLayout;
    const owner = layout.layers[owner_index];
    if (owner.owner != owner_index or owner.kv_heads == 0) return error.InvalidContextLayout;
    var strata: u32 = 0;
    for (layout.layers[0..layout.len]) |consumer| if (consumer.owner == owner_index) {
        if (consumer.kv_heads != owner.kv_heads or consumer.head_dim != owner.head_dim or
            consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0)
            return error.InvalidContextLayout;
        strata = try std.math.add(u32, strata, consumer.query_heads / consumer.kv_heads);
    };
    if (strata == 0 or strata > 128) return error.InvalidContextLayout;
    return strata;
}

test "context query campaign sums actual consumer GQA strata" {
    var layout = policy.Layout{ .len = 3 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    layout.layers[2] = .{ .owner = 2, .sliding_window = 0, .kv_heads = 1, .query_heads = 1, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10000 };
    try std.testing.expectEqual(@as(u32, 6), try ownerStrata(layout, 0));
    try std.testing.expectEqual(@as(u32, 1), try ownerStrata(layout, 2));
    layout.layers[1].kv_heads = 1;
    try std.testing.expectError(error.InvalidContextLayout, ownerStrata(layout, 0));
}

pub const Branch = enum { question, fit, gate };
pub const Partition = enum { training, validation };

/// Device view exported by a sealed and fully drained private replay branch.
/// `queries` remains device-resident; positions are request-owned selection
/// metadata already maintained by model_replay_context.QueryPool.
pub const DevicePartition = struct {
    campaign_identity: u64,
    load_generation: u64,
    branch: Branch,
    session_identity: u64,
    layer: u32,
    partition: Partition,
    rows_per_head: u32,
    heads: u32,
    dim: u32,
    stride_rows: u32,
    row_offset: u32,
    queries: shared.Ref,
    positions: []const u32,
};

pub const Allocation = struct {
    /// Logical f32 destination query tensor bytes. This excludes positions,
    /// control objects, borrowed-runtime backing and allocator page rounding.
    query_bytes: usize,
    backend_bytes: usize,
    host_bytes: usize,
    physical_bytes: usize,
};

fn rounded(bytes: usize) !usize {
    const page = std.heap.pageSize();
    return (try std.math.add(usize, bytes, page - 1)) & ~(page - 1);
}

fn sourcesForBranch(base: []const protocol.Source, branch: Branch) ![protocol.max_sources]protocol.Source {
    if (base.len != protocol.max_sources) return error.InvalidQueryProtocol;
    var result: [protocol.max_sources]protocol.Source = undefined;
    @memcpy(result[0..], base);
    for (&result) |*source| source.active = false;
    switch (branch) {
        .question => result[0].active = true,
        .fit => {
            result[1].active = true;
            result[2].active = true;
        },
        .gate => {
            result[3].active = true;
            result[4].active = true;
        },
    }
    return result;
}

fn copySources(base: []const protocol.Source) ![protocol.max_sources]protocol.Source {
    if (base.len != protocol.max_sources) return error.InvalidQueryProtocol;
    var result: [protocol.max_sources]protocol.Source = undefined;
    @memcpy(result[0..], base);
    // Validate the complete reusable protocol as well as each branch mask.
    const question = try sourcesForBranch(&result, .question);
    const fit = try sourcesForBranch(&result, .fit);
    const gate = try sourcesForBranch(&result, .gate);
    _ = try protocol.Plan.init(&question, 1);
    _ = try protocol.Plan.init(&fit, 1);
    _ = try protocol.Plan.init(&gate, 1);
    return result;
}

const Shape = struct {
    strata: u32,
    training_per_source: u32,
    validation_per_source: u32,
    training_rows: u32,
    validation_rows: u32,
    stride_rows: u32,
    vectors: u32,

    fn init(base: []const protocol.Source, strata: u32, heads: u32) !Shape {
        const question_sources = try sourcesForBranch(base, .question);
        const fit_sources = try sourcesForBranch(base, .fit);
        const gate_sources = try sourcesForBranch(base, .gate);
        const question = try protocol.Plan.init(&question_sources, strata);
        const fit = try protocol.Plan.init(&fit_sources, strata);
        const gate = try protocol.Plan.init(&gate_sources, strata);
        if (question.training_per_source_stratum != fit.training_per_source_stratum or
            question.training_per_source_stratum != gate.training_per_source_stratum or
            question.validation_per_source_stratum != fit.validation_per_source_stratum or
            question.validation_per_source_stratum != gate.validation_per_source_stratum)
            return error.QueryProtocolQuotaDrift;
        const training_rows = try std.math.mul(u32, try std.math.mul(u32, strata, 3), question.training_per_source_stratum);
        const validation_rows = try std.math.mul(u32, try std.math.mul(u32, strata, 2), question.validation_per_source_stratum);
        if (training_rows > protocol.max_training_rows or validation_rows > protocol.max_validation_rows)
            return error.InvalidQueryProtocol;
        const stride = try std.math.add(u32, training_rows, validation_rows);
        return .{
            .strata = strata,
            .training_per_source = question.training_per_source_stratum,
            .validation_per_source = question.validation_per_source_stratum,
            .training_rows = training_rows,
            .validation_rows = validation_rows,
            .stride_rows = stride,
            .vectors = try std.math.mul(u32, stride, heads),
        };
    }
};

const Owner = struct {
    layer: u32,
    heads: u32,
    dim: u32,
    shape: Shape,
    queries: shared.Buffer,
    mapping: shared.Buffer,
    positions: []u32,

    fn deinit(self: *Owner, allocator: std.mem.Allocator) void {
        self.queries.deinit();
        self.mapping.deinit();
        allocator.free(self.positions);
        self.* = undefined;
    }
};

fn mapPartition(
    shape: Shape,
    branch_plan: protocol.Plan,
    owner_layer: u32,
    owner_heads: u32,
    owner_dim: u32,
    campaign_identity: u64,
    load_generation: u64,
    branch: Branch,
    part: DevicePartition,
    map: []u32,
    positions: []u32,
) !u32 {
    const expected_partition: Partition = if (branch == .gate) .validation else .training;
    const expected_protocol_partition: protocol.Partition = if (branch == .gate) .validation else .training;
    const expected_rows = switch (branch) {
        .question => shape.strata * shape.training_per_source,
        .fit => shape.strata * 2 * shape.training_per_source,
        .gate => shape.strata * 2 * shape.validation_per_source,
    };
    if (part.campaign_identity != campaign_identity or part.load_generation != load_generation or
        part.branch != branch or part.layer != owner_layer or part.partition != expected_partition or
        part.heads != owner_heads or part.dim != owner_dim or part.rows_per_head != expected_rows or
        branch_plan.strata != shape.strata or
        branch_plan.training_rows + branch_plan.validation_rows != expected_rows or
        part.stride_rows < part.row_offset or expected_rows > part.stride_rows - part.row_offset)
        return error.InvalidQueryCampaignPartition;
    const source_vectors = try std.math.mul(u32, part.heads, part.stride_rows);
    const source_bytes = try std.math.mul(usize, @as(usize, source_vectors), try std.math.mul(usize, part.dim, 4));
    // model_replay_context exports the complete pool. Accepting a slice would
    // make the row mapping depend on caller-controlled hidden offsets.
    if (part.positions.len != source_vectors or part.queries.offset != 0 or part.queries.bytes != source_bytes)
        return error.InvalidQueryCampaignPartition;
    const count = if (branch == .gate) shape.validation_per_source else shape.training_per_source;
    const active_sources: u32 = if (branch == .question) 1 else 2;
    const global_source_base: u32 = switch (branch) {
        .question => 0,
        .fit => 1,
        .gate => 0,
    };
    const partition_offset: u32 = if (branch == .gate) shape.training_rows else 0;
    const global_source_count: u32 = if (branch == .gate) 2 else 3;
    for (0..owner_heads) |head| for (0..shape.strata) |stratum| for (0..active_sources) |source| for (0..count) |slot| {
        const source_row = @as(u32, @intCast(stratum)) * active_sources * count + @as(u32, @intCast(source)) * count + @as(u32, @intCast(slot));
        const dest_row = partition_offset + @as(u32, @intCast(stratum)) * global_source_count * count + (global_source_base + @as(u32, @intCast(source))) * count + @as(u32, @intCast(slot));
        const source_index = @as(u32, @intCast(head)) * part.stride_rows + part.row_offset + source_row;
        const dest_index = @as(u32, @intCast(head)) * shape.stride_rows + dest_row;
        if (dest_index >= map.len or source_index >= part.positions.len or positions[dest_index] != absent)
            return error.InvalidQueryCampaignPartition;
        map[dest_index] = source_index;
        const position = part.positions[source_index];
        if (position == absent) return error.IncompleteReferenceReplay;
        const selected = (try branch_plan.select(position, @intCast(stratum))) orelse
            return error.InvalidQueryCampaignSelection;
        if (selected.partition != expected_protocol_partition or selected.row != source_row)
            return error.InvalidQueryCampaignSelection;
        positions[dest_index] = position;
    };
    return source_vectors;
}

pub const View = struct {
    queries: shared.Ref,
    positions: []const u32,
    rows: u32,
    dim: u32,
    validation_group_rows: u32 = 0,
};

pub const FitGeometry = struct {
    training_rows: u32,
    validation_rows: u32,
    validation_group_rows: u32,
};

pub const SourceObservation = struct {
    queries: shared.Ref,
    positions: []const u32,
    heads: u32,
    dim: u32,
    stride_rows: u32,
    partition: Partition,
    partition_offset: u32,
    strata: u32,
    partition_source_count: u32,
    source_local_index: u32,
    rows_per_source_stratum: u32,
};

pub const CaptureSourceObservation = struct {
    geometry: SourceObservation,
    queries: []const f32,
};

/// One campaign exists per serving request. It is allocated before any private
/// replay branch, absorbs each sealed branch exactly once with a synchronous
/// device gather, and outlives the fitter. The execution lease and allocation
/// tracker must outlive this object.
pub const Campaign = struct {
    allocator: std.mem.Allocator,
    copier: storage.Copier,
    owners: []Owner,
    sources: [protocol.max_sources]protocol.Source,
    campaign_identity: u64,
    load_generation: u64,
    next_branch: ?Branch = .question,
    branch_sessions: [3]u64 = @splat(0),
    failed: bool = false,
    sealed: bool = false,
    /// Test-only single-use claim for stored-payload native validation.
    stored_diagnostic_claimed: bool = false,
    sizes: Allocation,

    pub fn allocation(layout: policy.Layout, sources: []const protocol.Source) !Allocation {
        if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
        _ = try copySources(sources);
        var backend: usize = std.heap.pageSize(); // borrowed copier dummy
        var host: usize = 0;
        var query_bytes: usize = 0;
        var owner_count: usize = 0;
        for (layout.layers[0..layout.len], 0..) |layer, index| {
            if (layer.owner != index or layer.sliding_window != 0) continue;
            if (layer.kv_heads == 0 or layer.query_heads == 0 or layer.query_heads % layer.kv_heads != 0)
                return error.InvalidContextLayout;
            const strata = try ownerStrata(layout, index);
            const shape = try Shape.init(sources, strata, layer.kv_heads);
            const owner_query_bytes = try std.math.mul(usize, @as(usize, shape.vectors), try std.math.mul(usize, layer.head_dim, 4));
            query_bytes = try std.math.add(usize, query_bytes, owner_query_bytes);
            backend = try std.math.add(usize, backend, try rounded(owner_query_bytes));
            backend = try std.math.add(usize, backend, try rounded(try std.math.mul(usize, @as(usize, shape.vectors), 4)));
            host = try std.math.add(usize, host, try rounded(try std.math.mul(usize, @as(usize, shape.vectors), 4)));
            owner_count = try std.math.add(usize, owner_count, 1);
        }
        if (owner_count == 0) return error.InvalidContextLayout;
        host = try std.math.add(usize, host, try rounded(try std.math.mul(usize, owner_count, @sizeOf(Owner))));
        return .{ .query_bytes = query_bytes, .backend_bytes = backend, .host_bytes = host, .physical_bytes = try std.math.add(usize, backend, host) };
    }

    pub fn init(
        allocator: std.mem.Allocator,
        lease: *execution_pool.Lease,
        tracker: ?*shared.BufferAllocationTracker,
        layout: policy.Layout,
        sources: []const protocol.Source,
        physical_budget_bytes: usize,
        campaign_identity: u64,
        load_generation: u64,
    ) !Campaign {
        if (campaign_identity == 0 or load_generation == 0 or lease.generation != load_generation)
            return error.StaleContextView;
        if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
        const frozen_sources = try copySources(sources);
        const sizes = try allocation(layout, &frozen_sources);
        if (sizes.physical_bytes > physical_budget_bytes) return error.ContextReplayBudgetExceeded;
        var copier = try storage.Copier.initBorrowedTracked(try lease.execution(.storage), tracker);
        errdefer copier.deinit();
        var count: usize = 0;
        for (layout.layers[0..layout.len], 0..) |layer, index|
            count += @intFromBool(layer.owner == index and layer.sliding_window == 0);
        const owners = try allocator.alloc(Owner, count);
        errdefer allocator.free(owners);
        var initialized: usize = 0;
        errdefer for (owners[0..initialized]) |*owner| owner.deinit(allocator);
        for (layout.layers[0..layout.len], 0..) |layer, index| {
            if (layer.owner != index or layer.sliding_window != 0) continue;
            const strata = try ownerStrata(layout, index);
            const shape = try Shape.init(sources, strata, layer.kv_heads);
            var queries = try copier.core.buffer(try std.math.mul(usize, @as(usize, shape.vectors), try std.math.mul(usize, layer.head_dim, 4)));
            errdefer queries.deinit();
            var mapping = try copier.core.buffer(try std.math.mul(usize, @as(usize, shape.vectors), 4));
            errdefer mapping.deinit();
            const positions = try allocator.alloc(u32, shape.vectors);
            @memset(positions, absent);
            owners[initialized] = .{ .layer = @intCast(index), .heads = layer.kv_heads, .dim = layer.head_dim, .shape = shape, .queries = queries, .mapping = mapping, .positions = positions };
            initialized += 1;
        }
        return .{
            .allocator = allocator,
            .copier = copier,
            .owners = owners,
            .sources = frozen_sources,
            .campaign_identity = campaign_identity,
            .load_generation = load_generation,
            .sizes = sizes,
        };
    }

    pub fn deinit(self: *Campaign) void {
        for (self.owners) |*owner| owner.deinit(self.allocator);
        self.allocator.free(self.owners);
        self.copier.deinit();
        self.* = undefined;
    }

    fn advance(branch: Branch) ?Branch {
        return switch (branch) {
            .question => .fit,
            .fit => .gate,
            .gate => null,
        };
    }

    fn branchIndex(branch: Branch) usize {
        return switch (branch) {
            .question => 0,
            .fit => 1,
            .gate => 2,
        };
    }

    /// `parts` must be sorted by owner layer and must remain alive until this
    /// synchronous gather returns. A failed or repeated branch poisons the
    /// complete campaign; no partial pool may reach the fitter.
    pub fn absorb(self: *Campaign, branch: Branch, parts: []const DevicePartition) !void {
        if (self.failed) return error.ContextQueryCampaignFailed;
        if (self.sealed or self.next_branch == null or self.next_branch.? != branch)
            return error.InvalidQueryCampaignOrder;
        errdefer self.failed = true;
        if (parts.len != self.owners.len) return error.InvalidContextLayout;
        const session_identity = if (parts.len > 0) parts[0].session_identity else 0;
        if (session_identity == 0) return error.StaleContextView;
        for (self.branch_sessions) |prior| if (prior == session_identity) return error.StaleContextView;
        for (self.owners, parts) |*owner, part| {
            if (part.session_identity != session_identity) return error.InvalidQueryCampaignPartition;
            const map = std.mem.bytesAsSlice(u32, @as([]align(4) u8, @alignCast(owner.mapping.bytes[0..owner.mapping.logical_bytes])));
            @memset(map, absent);
            const active_sources = try sourcesForBranch(&self.sources, branch);
            const branch_plan = try protocol.Plan.init(&active_sources, owner.shape.strata);
            const source_vectors = try mapPartition(owner.shape, branch_plan, owner.layer, owner.heads, owner.dim, self.campaign_identity, self.load_generation, branch, part, map, owner.positions);
            try self.copier.captureQueries(part.queries, owner.mapping, owner.queries.ref(), source_vectors, owner.dim);
        }
        self.branch_sessions[branchIndex(branch)] = session_identity;
        self.next_branch = advance(branch);
    }

    /// Transfer one sealed private branch into the campaign without exposing
    /// query vectors to host code. The private Session must remain alive until
    /// the synchronous gather returns, after which the caller may destroy it.
    pub fn absorbReplay(self: *Campaign, branch: Branch, session: *const replay.Session) !void {
        var parts: [256]DevicePartition = undefined;
        if (self.owners.len > parts.len) return error.InvalidContextLayout;
        const partition: replay.QueryPartition = if (branch == .gate) .validation else .training;
        for (self.owners, parts[0..self.owners.len]) |owner, *part| {
            const device_view = try session.sealedDeviceQueryPartition(owner.layer, partition);
            part.* = .{
                .campaign_identity = self.campaign_identity,
                .load_generation = self.load_generation,
                .branch = branch,
                .session_identity = device_view.session_identity,
                .layer = device_view.owner_layer,
                .partition = switch (device_view.partition) {
                    .training => .training,
                    .validation => .validation,
                },
                .rows_per_head = device_view.rows_per_head,
                .heads = device_view.heads,
                .dim = device_view.dim,
                .stride_rows = device_view.stride_rows,
                .row_offset = device_view.row_offset,
                .queries = device_view.queries,
                .positions = device_view.positions,
            };
        }
        try self.absorb(branch, parts[0..self.owners.len]);
    }

    pub fn seal(self: *Campaign) !void {
        if (self.failed or self.sealed or self.next_branch != null) return error.IncompleteQueryCampaign;
        for (self.owners) |owner| for (owner.positions) |position|
            if (position == absent) return error.IncompleteQueryCampaign;
        self.sealed = true;
    }

    /// Test-only structural identity for the exact sealed request campaign.
    /// Query bytes remain device-owned and immutable by caller contract; the
    /// digest binds their request identity, three fresh branch sessions, source
    /// geometry and every selected position without reading device tensors.
    pub fn storedDiagnosticFingerprint(self: *const Campaign) ![32]u8 {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (self.failed or !self.sealed or self.next_branch != null or self.stored_diagnostic_claimed)
            return error.StaleContextView;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context.stored-query-campaign.v1\x00");
        var word: [8]u8 = undefined;
        std.mem.writeInt(u64, &word, self.campaign_identity, .little);
        hash.update(&word);
        std.mem.writeInt(u64, &word, self.load_generation, .little);
        hash.update(&word);
        for (self.branch_sessions) |session_identity| {
            if (session_identity == 0) return error.StaleContextView;
            std.mem.writeInt(u64, &word, session_identity, .little);
            hash.update(&word);
        }
        var small: [4]u8 = undefined;
        for (self.sources) |source| {
            std.mem.writeInt(u32, &small, @backingInt(source.kind), .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, @backingInt(source.partition), .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, source.start, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, source.end, .little);
            hash.update(&small);
            const active: [1]u8 = .{@intFromBool(source.active)};
            hash.update(&active);
        }
        std.mem.writeInt(u32, &small, @intCast(self.owners.len), .little);
        hash.update(&small);
        for (self.owners) |owner| {
            std.mem.writeInt(u32, &small, owner.layer, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, owner.heads, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, owner.dim, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, @intCast(owner.positions.len), .little);
            hash.update(&small);
            for (owner.positions) |position| {
                if (position == absent) return error.IncompleteQueryCampaign;
                std.mem.writeInt(u32, &small, position, .little);
                hash.update(&small);
            }
        }
        return hash.finalResult();
    }

    pub const StoredDiagnosticPayloadEvidence = struct {
        sha256: [32]u8,
        readback_bytes: u64,
    };

    fn storedDiagnosticDigest(self: *const Campaign, comptime identity_bound: bool) !StoredDiagnosticPayloadEvidence {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (self.failed or !self.sealed or self.next_branch != null or self.stored_diagnostic_claimed)
            return error.StaleContextView;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(if (identity_bound) "antfly.context.stored-query-payload.v1\x00" else "antfly.context.stored-query-content.v1\x00");
        var word: [8]u8 = undefined;
        if (identity_bound) {
            std.mem.writeInt(u64, &word, self.campaign_identity, .little);
            hash.update(&word);
            std.mem.writeInt(u64, &word, self.load_generation, .little);
            hash.update(&word);
            for (self.branch_sessions) |session_identity| {
                if (session_identity == 0) return error.StaleContextView;
                std.mem.writeInt(u64, &word, session_identity, .little);
                hash.update(&word);
            }
        }
        var small: [4]u8 = undefined;
        var readback_bytes: u64 = 0;
        for (self.owners) |owner| {
            std.mem.writeInt(u32, &small, owner.layer, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, owner.heads, .little);
            hash.update(&small);
            std.mem.writeInt(u32, &small, owner.dim, .little);
            hash.update(&small);
            const bytes = owner.queries.bytes[0..owner.queries.logical_bytes];
            hash.update(bytes);
            readback_bytes = try std.math.add(u64, readback_bytes, @intCast(bytes.len));
            for (owner.positions) |position| {
                if (position == absent) return error.IncompleteQueryCampaign;
                std.mem.writeInt(u32, &small, position, .little);
                hash.update(&small);
                readback_bytes = try std.math.add(u64, readback_bytes, @sizeOf(u32));
            }
        }
        return .{ .sha256 = hash.finalResult(), .readback_bytes = readback_bytes };
    }

    /// Nonce-independent exact held content identity. The exclusive provider
    /// ledger uses this with frozen generator/corpus/row/partition identities
    /// to reject historical reuse; changing only campaign/session nonces does
    /// not change this digest.
    pub fn storedDiagnosticContentDigest(self: *const Campaign) !StoredDiagnosticPayloadEvidence {
        return self.storedDiagnosticDigest(false);
    }

    /// Test-only digest of the exact initialized native query bytes. The
    /// caller may persist only this digest and byte count, never the held
    /// payload. Campaign remains the sole owner of the query buffers.
    pub fn storedDiagnosticPayloadDigest(self: *const Campaign) !StoredDiagnosticPayloadEvidence {
        return self.storedDiagnosticDigest(true);
    }

    /// Claim this exact sealed query campaign once. A failed publication also
    /// consumes the claim, preventing retry or historical held-query reuse.
    pub fn claimStoredDiagnostic(self: *Campaign, load_generation: u64, campaign_identity: u64, expected_fingerprint: [32]u8) !void {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (self.load_generation != load_generation or self.campaign_identity != campaign_identity or
            self.stored_diagnostic_claimed or !std.meta.eql(try self.storedDiagnosticFingerprint(), expected_fingerprint))
            return error.StaleContextView;
        self.stored_diagnostic_claimed = true;
    }

    pub fn fitGeometry(self: *const Campaign, layer: u32) !FitGeometry {
        if (self.failed or !self.sealed) return error.IncompleteQueryCampaign;
        for (self.owners) |owner| if (owner.layer == layer) return .{
            .training_rows = owner.shape.training_rows,
            .validation_rows = owner.shape.validation_rows,
            // Validation has two fixed sources per consumer/GQA stratum.
            .validation_group_rows = try std.math.mul(u32, 2, owner.shape.validation_per_source),
        };
        return error.InvalidContextLayout;
    }

    /// Describe one logical source inside the interleaved campaign pool. No
    /// query vector is copied or exposed through a host float slice; capture
    /// code reconstructs source rows from this frozen geometry and positions.
    pub fn sourceObservation(self: *const Campaign, layer: u32, source_index: u32) !SourceObservation {
        if (self.failed or !self.sealed or source_index >= protocol.max_sources)
            return error.IncompleteQueryCampaign;
        for (self.owners) |owner| if (owner.layer == layer) {
            const validation = source_index >= 3;
            return .{
                .queries = owner.queries.ref(),
                .positions = owner.positions,
                .heads = owner.heads,
                .dim = owner.dim,
                .stride_rows = owner.shape.stride_rows,
                .partition = if (validation) .validation else .training,
                .partition_offset = if (validation) owner.shape.training_rows else 0,
                .strata = owner.shape.strata,
                .partition_source_count = if (validation) 2 else 3,
                .source_local_index = if (validation) source_index - 3 else source_index,
                .rows_per_source_stratum = if (validation) owner.shape.validation_per_source else owner.shape.training_per_source,
            };
        };
        return error.InvalidContextLayout;
    }

    /// Test-only qualification export. The float slice aliases the campaign's
    /// tracked buffer and expires with Campaign; callers must charge bytes
    /// written to evidence as query export readback, never as KV readback.
    pub fn captureSourceObservation(self: *const Campaign, layer: u32, source_index: u32) !CaptureSourceObservation {
        if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
        const geometry = try self.sourceObservation(layer, source_index);
        for (self.owners) |owner| if (owner.layer == layer) return .{
            .geometry = geometry,
            .queries = owner.queries.floats(),
        };
        return error.InvalidContextLayout;
    }

    pub fn captureQueryExportBytes(self: *const Campaign) !usize {
        if (!@import("builtin").is_test or self.failed or !self.sealed)
            return error.UnqualifiedCompactionArtifact;
        var bytes: usize = 0;
        for (self.owners) |owner|
            bytes = try std.math.add(usize, bytes, owner.queries.logical_bytes);
        return bytes;
    }

    /// Test-binary-only corruption of one genuine, sealed fitter input. The
    /// campaign replay has already completed and drained before publication,
    /// so this host write cannot race queued GPU work. Production builds have
    /// no caller and reject the operation.
    pub fn injectNonfiniteTrainingScalar(self: *Campaign, layer: u32, head: u32) !void {
        if (!@import("builtin").is_test) return error.UnqualifiedFaultInjection;
        if (self.failed or !self.sealed) return error.IncompleteQueryCampaign;
        for (self.owners) |*owner| if (owner.layer == layer) {
            if (head >= owner.heads or owner.shape.training_rows == 0 or owner.dim == 0)
                return error.InvalidContextLayout;
            const row = try std.math.mul(u32, head, owner.shape.stride_rows);
            const scalar = try std.math.mul(usize, @as(usize, row), owner.dim);
            const values = owner.queries.floats();
            if (scalar >= values.len) return error.InvalidQueryCampaignPartition;
            values[scalar] = std.math.nan(f32);
            return;
        };
        return error.InvalidContextLayout;
    }

    pub fn view(self: *const Campaign, layer: u32, head: u32, partition: Partition) !View {
        if (self.failed or !self.sealed) return error.IncompleteQueryCampaign;
        for (self.owners) |owner| if (owner.layer == layer) {
            if (head >= owner.heads) return error.InvalidContextLayout;
            const rows = if (partition == .training) owner.shape.training_rows else owner.shape.validation_rows;
            const offset = if (partition == .training) @as(u32, 0) else owner.shape.training_rows;
            const row = try std.math.add(u32, try std.math.mul(u32, head, owner.shape.stride_rows), offset);
            const vector_bytes = try std.math.mul(usize, @as(usize, row), try std.math.mul(usize, owner.dim, 4));
            const bytes = try std.math.mul(usize, @as(usize, rows), try std.math.mul(usize, owner.dim, 4));
            return .{
                .queries = try owner.queries.ref().slice(vector_bytes, bytes),
                .positions = owner.positions[row..][0..rows],
                .rows = rows,
                .dim = owner.dim,
                .validation_group_rows = if (partition == .validation)
                    try std.math.mul(u32, 2, owner.shape.validation_per_source)
                else
                    0,
            };
        };
        return error.InvalidContextLayout;
    }
};

test "stored diagnostic campaign fingerprint binds structure and claim is single use" {
    var positions = [_]u32{ 3, 7, 11 };
    var query_bytes = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var dummy_handle: u8 = 0;
    var owners: [1]Owner = undefined;
    owners[0].layer = 4;
    owners[0].heads = 2;
    owners[0].dim = 64;
    owners[0].positions = &positions;
    owners[0].queries = .{ .bytes = &query_bytes, .handle = &dummy_handle, .logical_bytes = query_bytes.len };
    const sources = [_]protocol.Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 0, .end = 4, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 4, .end = 8, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 8, .end = 12, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 12, .end = 16, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 16, .end = 20, .active = false },
    };
    var campaign: Campaign = undefined;
    campaign.owners = &owners;
    campaign.sources = sources;
    campaign.failed = false;
    campaign.sealed = true;
    campaign.next_branch = null;
    campaign.branch_sessions = .{ 101, 103, 107 };
    campaign.load_generation = 7;
    campaign.campaign_identity = 11;
    campaign.stored_diagnostic_claimed = false;
    const fingerprint = try campaign.storedDiagnosticFingerprint();
    const payload_fingerprint = try campaign.storedDiagnosticPayloadDigest();
    const content_fingerprint = try campaign.storedDiagnosticContentDigest();
    try std.testing.expectEqual(@as(u64, query_bytes.len + positions.len * @sizeOf(u32)), payload_fingerprint.readback_bytes);
    campaign.campaign_identity += 1;
    try std.testing.expect(!std.meta.eql(payload_fingerprint.sha256, (try campaign.storedDiagnosticPayloadDigest()).sha256));
    try std.testing.expectEqual(content_fingerprint.sha256, (try campaign.storedDiagnosticContentDigest()).sha256);
    campaign.campaign_identity -= 1;
    query_bytes[0] ^= 1;
    try std.testing.expect(!std.meta.eql(payload_fingerprint.sha256, (try campaign.storedDiagnosticPayloadDigest()).sha256));
    query_bytes[0] ^= 1;
    positions[1] += 1;
    try std.testing.expect(!std.meta.eql(fingerprint, try campaign.storedDiagnosticFingerprint()));
    try std.testing.expect(!std.meta.eql(payload_fingerprint.sha256, (try campaign.storedDiagnosticPayloadDigest()).sha256));
    positions[1] -= 1;
    try campaign.claimStoredDiagnostic(7, 11, fingerprint);
    try std.testing.expect(campaign.stored_diagnostic_claimed);
    try std.testing.expectError(error.StaleContextView, campaign.claimStoredDiagnostic(7, 11, fingerprint));
}

test "context query campaign maps every source stratum and head without host query copies" {
    const sources = [_]protocol.Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = 120, .end = 140, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = 100, .end = 120, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = 120, .end = 140, .active = false },
    };
    const shape = try Shape.init(&sources, 2, 2);
    const allocator = std.testing.allocator;
    const destination = try allocator.alloc(u32, shape.vectors);
    defer allocator.free(destination);
    @memset(destination, absent);
    const map = try allocator.alloc(u32, shape.vectors);
    defer allocator.free(map);
    const handle: *anyopaque = @ptrFromInt(16);
    const common = .{ .campaign_identity = @as(u64, 9), .load_generation = @as(u64, 7), .session_identity = @as(u64, 1), .layer = @as(u32, 4), .heads = @as(u32, 2), .dim = @as(u32, 4) };

    const Fixture = struct {
        fn positions(a: std.mem.Allocator, plan: protocol.Plan, stride: u32, offset: u32, heads: u32) ![]u32 {
            const result = try a.alloc(u32, try std.math.mul(u32, heads, stride));
            @memset(result, absent);
            for (0..heads) |head| for (0..plan.strata) |stratum| for (100..140) |position| {
                const selected = (try plan.select(@intCast(position), @intCast(stratum))) orelse continue;
                const index = @as(u32, @intCast(head)) * stride + offset + selected.row;
                if (result[index] != absent) return error.InvalidQueryCampaignSelection;
                result[index] = @intCast(position);
            };
            const rows = plan.training_rows + plan.validation_rows;
            for (0..heads) |head| for (result[@as(u32, @intCast(head)) * stride + offset ..][0..rows]) |position|
                if (position == absent) return error.IncompleteQueryCampaign;
            return result;
        }
    };

    const question_sources = try sourcesForBranch(&sources, .question);
    const fit_sources = try sourcesForBranch(&sources, .fit);
    const gate_sources = try sourcesForBranch(&sources, .gate);
    const question_plan = try protocol.Plan.init(&question_sources, shape.strata);
    const fit_plan = try protocol.Plan.init(&fit_sources, shape.strata);
    const gate_plan = try protocol.Plan.init(&gate_sources, shape.strata);
    const question_stride = question_plan.training_rows + 2;
    const fit_stride = fit_plan.training_rows + 2;
    const gate_stride = gate_plan.validation_rows + 3;
    const question_positions = try Fixture.positions(allocator, question_plan, question_stride, 1, 2);
    defer allocator.free(question_positions);
    const fit_positions = try Fixture.positions(allocator, fit_plan, fit_stride, 1, 2);
    defer allocator.free(fit_positions);
    const gate_positions = try Fixture.positions(allocator, gate_plan, gate_stride, 2, 2);
    defer allocator.free(gate_positions);

    @memset(map, absent);
    _ = try mapPartition(shape, question_plan, 4, 2, 4, 9, 7, .question, .{
        .campaign_identity = common.campaign_identity,
        .load_generation = common.load_generation,
        .branch = .question,
        .session_identity = common.session_identity,
        .layer = common.layer,
        .partition = .training,
        .rows_per_head = question_plan.training_rows,
        .heads = common.heads,
        .dim = common.dim,
        .stride_rows = question_stride,
        .row_offset = 1,
        .queries = .{ .handle = handle, .bytes = question_positions.len * 4 * 4 },
        .positions = question_positions,
    }, map, destination);
    @memset(map, absent);
    _ = try mapPartition(shape, fit_plan, 4, 2, 4, 9, 7, .fit, .{
        .campaign_identity = common.campaign_identity,
        .load_generation = common.load_generation,
        .branch = .fit,
        .session_identity = 2,
        .layer = common.layer,
        .partition = .training,
        .rows_per_head = fit_plan.training_rows,
        .heads = common.heads,
        .dim = common.dim,
        .stride_rows = fit_stride,
        .row_offset = 1,
        .queries = .{ .handle = handle, .bytes = fit_positions.len * 4 * 4 },
        .positions = fit_positions,
    }, map, destination);
    @memset(map, absent);
    _ = try mapPartition(shape, gate_plan, 4, 2, 4, 9, 7, .gate, .{
        .campaign_identity = common.campaign_identity,
        .load_generation = common.load_generation,
        .branch = .gate,
        .session_identity = 3,
        .layer = common.layer,
        .partition = .validation,
        .rows_per_head = gate_plan.validation_rows,
        .heads = common.heads,
        .dim = common.dim,
        .stride_rows = gate_stride,
        .row_offset = 2,
        .queries = .{ .handle = handle, .bytes = gate_positions.len * 4 * 4 },
        .positions = gate_positions,
    }, map, destination);
    for (destination) |position| try std.testing.expect(position != absent);

    const rejected_destination = try allocator.alloc(u32, shape.vectors);
    defer allocator.free(rejected_destination);
    @memset(rejected_destination, absent);
    @memset(map, absent);
    try std.testing.expectError(error.InvalidQueryCampaignPartition, mapPartition(shape, gate_plan, 4, 2, 4, 9, 7, .gate, .{
        .campaign_identity = 9,
        .load_generation = 7,
        .branch = .gate,
        .session_identity = 4,
        .layer = 4,
        .partition = .validation,
        .rows_per_head = gate_plan.validation_rows,
        .heads = 2,
        .dim = 4,
        .stride_rows = gate_stride,
        .row_offset = 2,
        .queries = .{ .handle = handle, .offset = 4, .bytes = gate_positions.len * 4 * 4 },
        .positions = gate_positions,
    }, map, rejected_destination));

    const original = question_positions[1];
    question_positions[1] = 120; // belongs to a different logical source
    @memset(map, absent);
    try std.testing.expectError(error.InvalidQueryCampaignSelection, mapPartition(shape, question_plan, 4, 2, 4, 9, 7, .question, .{
        .campaign_identity = 9,
        .load_generation = 7,
        .branch = .question,
        .session_identity = 4,
        .layer = 4,
        .partition = .training,
        .rows_per_head = question_plan.training_rows,
        .heads = 2,
        .dim = 4,
        .stride_rows = question_stride,
        .row_offset = 1,
        .queries = .{ .handle = handle, .bytes = question_positions.len * 4 * 4 },
        .positions = question_positions,
    }, map, rejected_destination));
    question_positions[1] = original;

    @memset(map, absent);
    try std.testing.expectError(error.InvalidQueryCampaignPartition, mapPartition(shape, gate_plan, 4, 2, 4, 9, 7, .gate, .{
        .campaign_identity = 9,
        .load_generation = 7,
        .branch = .gate,
        .session_identity = 5,
        .layer = 4,
        .partition = .training,
        .rows_per_head = gate_plan.validation_rows,
        .heads = 2,
        .dim = 4,
        .stride_rows = gate_stride,
        .row_offset = 2,
        .queries = .{ .handle = handle, .bytes = gate_positions.len * 4 * 4 },
        .positions = gate_positions,
    }, map, rejected_destination));
}
