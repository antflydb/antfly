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

//! Deterministic bounded token selection for lossy global-attention KV.
//!
//! `select_v1` never modifies retained K/V rows. It selects one absolute
//! position set per global KV donor, shared by every KV head and consumer of
//! that donor. Local-attention donors are outside this module and remain in
//! exact sliding-window storage.

const std = @import("std");

pub const algorithm_version = "select_v1";

pub const SelectV1Config = struct {
    target_rows: usize = 6144,
    hard_rows: usize = 8192,
    recent_rows: usize = 2048,
    prefill_chunk_rows: usize = 512,
    query_horizon: usize = 64,
    sink_rows: usize = 4,

    pub fn validate(self: SelectV1Config) !void {
        if (self.target_rows == 0 or self.hard_rows < self.target_rows) return error.InvalidSelectionConfig;
        if (self.recent_rows > self.target_rows or self.sink_rows > self.target_rows) return error.InvalidSelectionConfig;
        if (self.prefill_chunk_rows == 0 or self.query_horizon == 0) return error.InvalidSelectionConfig;
        _ = try std.math.add(usize, self.hard_rows, self.prefill_chunk_rows);
    }

    /// Maximum rows that may be resident just after an admitted prefill chunk
    /// commits and before selection returns the donor to `target_rows`.
    pub fn transientRows(self: SelectV1Config) !usize {
        try self.validate();
        return std.math.add(usize, self.hard_rows, self.prefill_chunk_rows);
    }
};

pub const defaults = SelectV1Config{};

pub const ProtectedSpan = struct {
    start: u64,
    end: u64,

    pub fn validate(self: ProtectedSpan) !void {
        if (self.end < self.start) return error.InvalidProtectedSpan;
    }

    pub fn contains(self: ProtectedSpan, position: u64) bool {
        return position >= self.start and position < self.end;
    }
};

/// Arbitrary retained positions for a global donor. Positions are strictly
/// increasing and are independent of the conversation's logical token count.
pub const GlobalPositionView = struct {
    positions: []const u64,
    total_positions: u64,

    pub fn validate(self: GlobalPositionView) !void {
        var previous: ?u64 = null;
        for (self.positions) |position| {
            if (position >= self.total_positions) return error.InvalidGlobalPositionView;
            if (previous) |value| if (position <= value) return error.InvalidGlobalPositionView;
            previous = position;
        }
    }
};

pub const SelectionPlan = struct {
    allocator: std.mem.Allocator,
    /// Source row indices, ordered by ascending absolute position so physical
    /// traversal and floating-point accumulation remain deterministic.
    source_indices: []usize,
    positions: []u64,
    protected_count: usize,
    recent_count: usize,

    pub fn deinit(self: *SelectionPlan) void {
        self.allocator.free(self.source_indices);
        self.allocator.free(self.positions);
        self.* = undefined;
    }
};

/// Translate selected logical source rows through the page table consumed by
/// paged attention. The returned offsets can be passed directly to
/// `SelectedGlobalKvRepack.source_token_offsets`.
pub fn selectedPhysicalRows(
    allocator: std.mem.Allocator,
    source_indices: []const usize,
    block_token_offsets: []const u32,
    page_size: usize,
) ![]u32 {
    if (page_size == 0) return error.InvalidPageTable;
    const out = try allocator.alloc(u32, source_indices.len);
    errdefer allocator.free(out);
    var previous_index: ?usize = null;
    for (source_indices, out) |logical_index, *physical| {
        if (previous_index) |previous| if (logical_index <= previous) return error.InvalidSelectionInput;
        previous_index = logical_index;
        const block_index = logical_index / page_size;
        if (block_index >= block_token_offsets.len) return error.InvalidPageTable;
        const within_block = logical_index % page_size;
        const token = std.math.add(usize, block_token_offsets[block_index], within_block) catch return error.InvalidPageTable;
        physical.* = std.math.cast(u32, token) orelse return error.InvalidPageTable;
    }
    return out;
}

pub fn positionsU32(allocator: std.mem.Allocator, positions: []const u64) ![]u32 {
    const out = try allocator.alloc(u32, positions.len);
    errdefer allocator.free(out);
    for (positions, out) |position, *narrowed| {
        narrowed.* = std.math.cast(u32, position) orelse return error.PositionExceedsMetalAbi;
    }
    return out;
}

const RankedCandidate = struct {
    source_index: usize,
    position: u64,
    score: f64,

    fn lessThan(_: void, lhs: RankedCandidate, rhs: RankedCandidate) bool {
        if (lhs.score != rhs.score) return lhs.score > rhs.score;
        return lhs.position < rhs.position;
    }
};

fn isExplicitlyProtected(position: u64, spans: []const ProtectedSpan) bool {
    for (spans) |span| if (span.contains(position)) return true;
    return false;
}

/// Exact admission count for the union of sink, caller-protected, and recent
/// positions. This runs before KV allocation so an impossible policy fails
/// without partially prefilling a request.
pub fn mandatoryRowCount(config: SelectV1Config, total_positions: u64, spans: []const ProtectedSpan) !usize {
    try config.validate();
    for (spans) |span| try span.validate();
    var count: usize = 0;
    const recent_start = total_positions -| @as(u64, @intCast(config.recent_rows));
    var position: u64 = 0;
    while (position < total_positions) : (position += 1) {
        if (position < @as(u64, @intCast(config.sink_rows)) or position >= recent_start or isExplicitlyProtected(position, spans)) count += 1;
    }
    return count;
}

/// Select rows from a global donor. `positions` must be strictly increasing;
/// `mean_attention` contains the already consumer-balanced normalized
/// attention mass for the same rows.
pub fn selectRows(
    allocator: std.mem.Allocator,
    config: SelectV1Config,
    positions: []const u64,
    mean_attention: []const f64,
    total_positions: u64,
    protected_spans: []const ProtectedSpan,
) !SelectionPlan {
    try config.validate();
    if (positions.len != mean_attention.len) return error.InvalidSelectionInput;
    for (protected_spans) |span| try span.validate();
    try (GlobalPositionView{ .positions = positions, .total_positions = total_positions }).validate();
    for (mean_attention) |score| {
        if (!std.math.isFinite(score) or score < 0) return error.InvalidSelectionScore;
    }

    if (positions.len <= config.target_rows) {
        const indices = try allocator.alloc(usize, positions.len);
        errdefer allocator.free(indices);
        const copied_positions = try allocator.dupe(u64, positions);
        for (indices, 0..) |*index, i| index.* = i;
        const recent_start = total_positions -| @as(u64, @intCast(config.recent_rows));
        var protected_count: usize = 0;
        var recent_count: usize = 0;
        for (positions) |position| {
            protected_count += @intFromBool(position < @as(u64, @intCast(config.sink_rows)) or isExplicitlyProtected(position, protected_spans));
            recent_count += @intFromBool(position >= recent_start);
        }
        return .{
            .allocator = allocator,
            .source_indices = indices,
            .positions = copied_positions,
            .protected_count = protected_count,
            .recent_count = recent_count,
        };
    }

    const recent_start = total_positions -| @as(u64, @intCast(config.recent_rows));
    const keep = try allocator.alloc(bool, positions.len);
    defer allocator.free(keep);
    @memset(keep, false);

    var protected_count: usize = 0;
    var recent_count: usize = 0;
    for (positions, 0..) |position, i| {
        const protected = position < @as(u64, @intCast(config.sink_rows)) or isExplicitlyProtected(position, protected_spans);
        const recent = position >= recent_start;
        if (protected or recent) keep[i] = true;
        if (protected) protected_count += 1;
        if (recent) recent_count += 1;
    }

    var mandatory_count: usize = 0;
    for (keep) |value| mandatory_count += @intFromBool(value);
    if (mandatory_count > config.target_rows) return error.ProtectedRowsExceedTarget;

    const candidates = try allocator.alloc(RankedCandidate, positions.len - mandatory_count);
    defer allocator.free(candidates);
    var candidate_count: usize = 0;
    for (positions, mean_attention, 0..) |position, score, i| {
        if (keep[i]) continue;
        candidates[candidate_count] = .{ .source_index = i, .position = position, .score = score };
        candidate_count += 1;
    }
    std.mem.sort(RankedCandidate, candidates[0..candidate_count], {}, RankedCandidate.lessThan);

    const selected_candidates = @min(config.target_rows - mandatory_count, candidate_count);
    for (candidates[0..selected_candidates]) |candidate| keep[candidate.source_index] = true;

    const retained_count = mandatory_count + selected_candidates;
    const indices = try allocator.alloc(usize, retained_count);
    errdefer allocator.free(indices);
    const retained_positions = try allocator.alloc(u64, retained_count);
    var dst: usize = 0;
    for (positions, 0..) |position, i| {
        if (!keep[i]) continue;
        indices[dst] = i;
        retained_positions[dst] = position;
        dst += 1;
    }
    std.debug.assert(dst == retained_count);

    return .{
        .allocator = allocator,
        .source_indices = indices,
        .positions = retained_positions,
        .protected_count = protected_count,
        .recent_count = recent_count,
    };
}

pub const AttentionScoreRequest = struct {
    queries: []const f32,
    query_positions: []const u64,
    keys: []const f32,
    key_positions: []const u64,
    num_query_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    /// Exact model-configured multiplier applied to QK before softcap. Zero
    /// selects the architecture-default 1/sqrt(head_dim).
    score_scale: f32 = 0,
    softcap: f32 = 0,
};

/// CPU reference for one consumer layer. It reproduces causal normalized GQA
/// attention, balances mapped query heads within each KV head, then balances
/// KV heads. Callers balance separate consumer layers with ScoreAccumulator.
pub fn scoreConsumerNormalizedAttention(
    allocator: std.mem.Allocator,
    request: AttentionScoreRequest,
) ![]f64 {
    if (request.num_query_heads == 0 or request.num_kv_heads == 0 or request.head_dim == 0) return error.InvalidAttentionScoreShape;
    if (!std.math.isFinite(request.softcap) or request.softcap < 0) return error.InvalidAttentionScoreShape;
    if (!std.math.isFinite(request.score_scale) or request.score_scale < 0) return error.InvalidAttentionScoreShape;
    if (request.num_query_heads % request.num_kv_heads != 0) return error.InvalidAttentionScoreShape;
    const query_width = try std.math.mul(usize, request.num_query_heads, request.head_dim);
    const key_width = try std.math.mul(usize, request.num_kv_heads, request.head_dim);
    if (request.queries.len != try std.math.mul(usize, request.query_positions.len, query_width)) return error.InvalidAttentionScoreShape;
    if (request.keys.len != try std.math.mul(usize, request.key_positions.len, key_width)) return error.InvalidAttentionScoreShape;
    for (request.queries) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttentionInput;
    for (request.keys) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttentionInput;
    var prior_query_position: ?u64 = null;
    for (request.query_positions) |position| {
        if (prior_query_position) |prior| if (position <= prior) return error.InvalidAttentionScoreShape;
        prior_query_position = position;
    }
    try (GlobalPositionView{ .positions = request.key_positions, .total_positions = if (request.query_positions.len == 0) 0 else request.query_positions[request.query_positions.len - 1] +| 1 }).validate();

    const out = try allocator.alloc(f64, request.key_positions.len);
    errdefer allocator.free(out);
    @memset(out, 0);
    if (request.query_positions.len == 0 or request.key_positions.len == 0) return out;

    const logits = try allocator.alloc(f64, request.key_positions.len);
    defer allocator.free(logits);
    const heads_per_kv = request.num_query_heads / request.num_kv_heads;
    const scale: f64 = if (request.score_scale > 0)
        request.score_scale
    else
        1.0 / @sqrt(@as(f64, @floatFromInt(request.head_dim)));
    const contribution_scale = 1.0 / @as(f64, @floatFromInt(request.query_positions.len * request.num_kv_heads * heads_per_kv));

    for (request.query_positions, 0..) |query_position, query_index| {
        for (0..request.num_kv_heads) |kv_head| {
            for (0..heads_per_kv) |mapped_head| {
                const query_head = kv_head * heads_per_kv + mapped_head;
                const q_base = query_index * query_width + query_head * request.head_dim;
                var max_logit = -std.math.inf(f64);
                for (request.key_positions, 0..) |key_position, key_index| {
                    if (key_position > query_position) {
                        logits[key_index] = -std.math.inf(f64);
                        continue;
                    }
                    const k_base = key_index * key_width + kv_head * request.head_dim;
                    var dot: f64 = 0;
                    for (0..request.head_dim) |d| dot += @as(f64, request.queries[q_base + d]) * @as(f64, request.keys[k_base + d]);
                    var logit = dot * scale;
                    if (request.softcap > 0) {
                        const cap: f64 = request.softcap;
                        logit = cap * std.math.tanh(logit / cap);
                    }
                    logits[key_index] = logit;
                    max_logit = @max(max_logit, logit);
                }
                if (!std.math.isFinite(max_logit)) continue;
                var denominator: f64 = 0;
                for (logits) |logit| {
                    if (std.math.isFinite(logit)) denominator += @exp(logit - max_logit);
                }
                if (!(denominator > 0) or !std.math.isFinite(denominator)) return error.InvalidAttentionScores;
                for (logits, 0..) |logit, key_index| {
                    if (!std.math.isFinite(logit)) continue;
                    out[key_index] += (@exp(logit - max_logit) / denominator) * contribution_scale;
                }
            }
        }
    }
    return out;
}

pub const ScoreAccumulator = struct {
    values: []f64,
    consumers: usize = 0,

    pub fn init(allocator: std.mem.Allocator, row_count: usize) !ScoreAccumulator {
        const values = try allocator.alloc(f64, row_count);
        @memset(values, 0);
        return .{ .values = values };
    }

    pub fn deinit(self: *ScoreAccumulator, allocator: std.mem.Allocator) void {
        allocator.free(self.values);
        self.* = undefined;
    }

    pub fn addConsumer(self: *ScoreAccumulator, scores: []const f64) !void {
        if (scores.len != self.values.len) return error.InvalidSelectionInput;
        for (self.values, scores) |*dst, score| {
            if (!std.math.isFinite(score) or score < 0) return error.InvalidSelectionScore;
            dst.* += score;
        }
        self.consumers = std.math.add(usize, self.consumers, 1) catch return error.InvalidSelectionInput;
    }

    pub fn finish(self: *ScoreAccumulator) ![]const f64 {
        if (self.consumers == 0) return error.NoAttentionConsumers;
        const scale = 1.0 / @as(f64, @floatFromInt(self.consumers));
        for (self.values) |*value| value.* *= scale;
        return self.values;
    }
};

pub const PeakBytesRequest = struct {
    donor_count: usize,
    row_bytes: usize,
    local_ring_bytes: usize,
    query_capture_bytes: usize,
    score_scratch_bytes: usize,
    graph_scratch_bytes: usize,
};

/// Conservative peak for serialized donor selection: every donor at transient
/// capacity plus one staged target copy and all non-donor scratch/storage.
pub fn peakBytes(config: SelectV1Config, request: PeakBytesRequest) !usize {
    const transient_rows = try config.transientRows();
    const resident_rows = try std.math.mul(usize, request.donor_count, transient_rows);
    const all_rows = try std.math.add(usize, resident_rows, config.target_rows);
    const global_bytes = try std.math.mul(usize, all_rows, request.row_bytes);
    var total = try std.math.add(usize, global_bytes, request.local_ring_bytes);
    total = try std.math.add(usize, total, request.query_capture_bytes);
    total = try std.math.add(usize, total, request.score_scratch_bytes);
    return std.math.add(usize, total, request.graph_scratch_bytes);
}

/// Kernel-independent reference state for one selected global KV donor. This
/// is deliberately separate from SequenceBlockTable: mixed Gemma caches need
/// different position sets for global donors and local rings.
pub const SelectedDonorState = struct {
    allocator: std.mem.Allocator,
    donor_layer: usize,
    num_kv_heads: usize,
    head_dim: usize,
    total_positions: u64 = 0,
    epoch: u64 = 0,
    positions: std.ArrayListUnmanaged(u64) = .empty,
    keys: std.ArrayListUnmanaged(f32) = .empty,
    values: std.ArrayListUnmanaged(f32) = .empty,

    pub fn init(allocator: std.mem.Allocator, donor_layer: usize, num_kv_heads: usize, head_dim: usize) !SelectedDonorState {
        if (num_kv_heads == 0 or head_dim == 0) return error.InvalidDonorGeometry;
        return .{
            .allocator = allocator,
            .donor_layer = donor_layer,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
        };
    }

    pub fn deinit(self: *SelectedDonorState) void {
        self.positions.deinit(self.allocator);
        self.keys.deinit(self.allocator);
        self.values.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn rowWidth(self: SelectedDonorState) !usize {
        return std.math.mul(usize, self.num_kv_heads, self.head_dim);
    }

    pub fn validate(self: SelectedDonorState) !void {
        try (GlobalPositionView{ .positions = self.positions.items, .total_positions = self.total_positions }).validate();
        const expected = try std.math.mul(usize, self.positions.items.len, try self.rowWidth());
        if (self.keys.items.len != expected or self.values.items.len != expected) return error.InvalidDonorState;
    }

    /// Append exact, already post-RoPE K and unmodified V rows. Absolute
    /// positions must be increasing, but need not be contiguous after a
    /// selection transaction has committed.
    pub fn appendExact(self: *SelectedDonorState, first_position: u64, k: []const f32, v: []const f32) !void {
        const width = try self.rowWidth();
        if (k.len != v.len or k.len % width != 0) return error.InvalidDonorRows;
        const row_count = k.len / width;
        if (row_count == 0) return;
        if (first_position != self.total_positions) return error.InvalidDonorPosition;
        const end_position = try std.math.add(u64, first_position, @as(u64, @intCast(row_count)));
        if (end_position < self.total_positions) return error.InvalidDonorPosition;

        try self.positions.ensureUnusedCapacity(self.allocator, row_count);
        try self.keys.ensureUnusedCapacity(self.allocator, k.len);
        try self.values.ensureUnusedCapacity(self.allocator, v.len);
        for (0..row_count) |row| self.positions.appendAssumeCapacity(first_position + @as(u64, @intCast(row)));
        self.keys.appendSliceAssumeCapacity(k);
        self.values.appendSliceAssumeCapacity(v);
        self.total_positions = end_position;
        self.epoch +%= 1;
    }

    pub fn stageSelection(
        self: *const SelectedDonorState,
        config: SelectV1Config,
        balanced_scores: []const f64,
        protected_spans: []const ProtectedSpan,
    ) !SelectionTransaction {
        try self.validate();
        var plan = try selectRows(
            self.allocator,
            config,
            self.positions.items,
            balanced_scores,
            self.total_positions,
            protected_spans,
        );
        defer plan.deinit();

        const width = try self.rowWidth();
        const staged_k = try self.allocator.alloc(f32, try std.math.mul(usize, plan.source_indices.len, width));
        errdefer self.allocator.free(staged_k);
        const staged_v = try self.allocator.alloc(f32, staged_k.len);
        errdefer self.allocator.free(staged_v);
        for (plan.source_indices, 0..) |source, dst| {
            @memcpy(staged_k[dst * width ..][0..width], self.keys.items[source * width ..][0..width]);
            @memcpy(staged_v[dst * width ..][0..width], self.values.items[source * width ..][0..width]);
        }
        return .{
            .allocator = self.allocator,
            .donor_layer = self.donor_layer,
            .expected_epoch = self.epoch,
            .total_positions = self.total_positions,
            .positions = try self.allocator.dupe(u64, plan.positions),
            .keys = staged_k,
            .values = staged_v,
        };
    }
};

pub const SelectionTransaction = struct {
    allocator: std.mem.Allocator,
    donor_layer: usize,
    expected_epoch: u64,
    total_positions: u64,
    positions: []u64,
    keys: []f32,
    values: []f32,
    consumed: bool = false,

    pub fn deinit(self: *SelectionTransaction) void {
        if (!self.consumed) {
            self.allocator.free(self.positions);
            self.allocator.free(self.keys);
            self.allocator.free(self.values);
        }
        self.* = undefined;
    }

    /// Atomic at the metadata-owner level: every allocation and row copy is
    /// complete before commit. An epoch mismatch rejects stale staged work.
    pub fn commit(self: *SelectionTransaction, state: *SelectedDonorState) !void {
        if (self.consumed) return error.SelectionTransactionConsumed;
        if (state.donor_layer != self.donor_layer or state.epoch != self.expected_epoch) return error.StaleSelectionTransaction;
        const width = try state.rowWidth();
        if (self.keys.len != self.values.len or self.keys.len != try std.math.mul(usize, self.positions.len, width)) return error.InvalidDonorState;
        try (GlobalPositionView{ .positions = self.positions, .total_positions = self.total_positions }).validate();

        state.positions.deinit(state.allocator);
        state.keys.deinit(state.allocator);
        state.values.deinit(state.allocator);
        state.positions = .fromOwnedSlice(self.positions);
        state.keys = .fromOwnedSlice(self.keys);
        state.values = .fromOwnedSlice(self.values);
        state.total_positions = self.total_positions;
        state.epoch +%= 1;
        self.consumed = true;
    }
};

test "select_v1 defaults encode the qualified bounded policy" {
    try defaults.validate();
    try std.testing.expectEqual(@as(usize, 8704), try defaults.transientRows());
    try std.testing.expectEqualStrings("select_v1", algorithm_version);
}

test "select_v1 default boundary reduces 8704 transient rows to 6144" {
    const allocator = std.testing.allocator;
    const row_count = try defaults.transientRows();
    const positions = try allocator.alloc(u64, row_count);
    defer allocator.free(positions);
    const scores = try allocator.alloc(f64, row_count);
    defer allocator.free(scores);
    for (positions, scores, 0..) |*position, *score, i| {
        position.* = @intCast(i);
        score.* = @floatFromInt(i);
    }
    var plan = try selectRows(allocator, defaults, positions, scores, row_count, &.{});
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 6144), plan.positions.len);
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 2, 3 }, plan.positions[0..4]);
    try std.testing.expectEqual(@as(u64, 2564), plan.positions[4]);
    try std.testing.expectEqual(@as(u64, 8703), plan.positions[plan.positions.len - 1]);
    try std.testing.expectEqual(@as(usize, 2048), plan.recent_count);
}

test "select_v1 default budget rejects protected plus recent overflow" {
    const allocator = std.testing.allocator;
    const row_count = try defaults.transientRows();
    const positions = try allocator.alloc(u64, row_count);
    defer allocator.free(positions);
    const scores = try allocator.alloc(f64, row_count);
    defer allocator.free(scores);
    for (positions, scores, 0..) |*position, *score, i| {
        position.* = @intCast(i);
        score.* = 0;
    }
    try std.testing.expectError(error.ProtectedRowsExceedTarget, selectRows(
        allocator,
        defaults,
        positions,
        scores,
        row_count,
        &.{.{ .start = 0, .end = 5000 }},
    ));
}

test "select_v1 keeps protected and recent rows then ranks stably" {
    const allocator = std.testing.allocator;
    const config = SelectV1Config{ .target_rows = 6, .hard_rows = 8, .recent_rows = 2, .prefill_chunk_rows = 2, .query_horizon = 2, .sink_rows = 1 };
    const positions = [_]u64{ 0, 2, 3, 5, 7, 8, 9, 10, 11 };
    const scores = [_]f64{ 0, 0.2, 0.9, 0.4, 0.9, 0.1, 0.3, 0, 0 };
    var plan = try selectRows(allocator, config, &positions, &scores, 12, &.{.{ .start = 2, .end = 3 }});
    defer plan.deinit();
    try std.testing.expectEqualSlices(u64, &.{ 0, 2, 3, 7, 10, 11 }, plan.positions);
    try std.testing.expectEqual(@as(usize, 2), plan.protected_count);
    try std.testing.expectEqual(@as(usize, 2), plan.recent_count);
}

test "select_v1 rejects protected union larger than target" {
    const positions = [_]u64{ 0, 1, 2, 3, 4 };
    const scores = [_]f64{ 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.ProtectedRowsExceedTarget, selectRows(
        std.testing.allocator,
        .{ .target_rows = 2, .hard_rows = 3, .recent_rows = 2, .prefill_chunk_rows = 1, .query_horizon = 1, .sink_rows = 1 },
        &positions,
        &scores,
        5,
        &.{.{ .start = 1, .end = 2 }},
    ));
}

test "select_v1 CPU attention scorer balances GQA heads" {
    const allocator = std.testing.allocator;
    const queries = [_]f32{
        1, 0, 1, 0,
        0, 1, 0, 1,
    };
    const keys = [_]f32{
        1, 0,
        0, 1,
    };
    const query_positions = [_]u64{ 1, 2 };
    const key_positions = [_]u64{ 0, 1 };
    const scores = try scoreConsumerNormalizedAttention(allocator, .{
        .queries = &queries,
        .query_positions = &query_positions,
        .keys = &keys,
        .key_positions = &key_positions,
        .num_query_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 2,
    });
    defer allocator.free(scores);
    // Two query positions each contribute a distribution whose total weight is 1/2.
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), scores[0] + scores[1], 1e-12);
}

test "select_v1 scorer uses model scale and rejects nonfinite inputs" {
    const allocator = std.testing.allocator;
    const request = AttentionScoreRequest{
        .queries = &.{ 2, 0 },
        .query_positions = &.{1},
        .keys = &.{ 1, 0, 0, 1 },
        .key_positions = &.{ 0, 1 },
        .num_query_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
        .score_scale = 0.25,
    };
    const scaled = try scoreConsumerNormalizedAttention(allocator, request);
    defer allocator.free(scaled);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / (1.0 + @exp(-0.5))), scaled[0], 1e-12);

    var invalid = request;
    invalid.queries = &.{ std.math.nan(f32), 0 };
    try std.testing.expectError(error.NonFiniteAttentionInput, scoreConsumerNormalizedAttention(allocator, invalid));
}

test "select_v1 peak reserves transient donors and one staged target" {
    const config = SelectV1Config{ .target_rows = 6, .hard_rows = 8, .recent_rows = 2, .prefill_chunk_rows = 2, .query_horizon = 2, .sink_rows = 1 };
    const bytes = try peakBytes(config, .{
        .donor_count = 2,
        .row_bytes = 10,
        .local_ring_bytes = 11,
        .query_capture_bytes = 12,
        .score_scratch_bytes = 13,
        .graph_scratch_bytes = 14,
    });
    try std.testing.expectEqual(@as(usize, (2 * 10 + 6) * 10 + 11 + 12 + 13 + 14), bytes);
}

test "select_v1 donor transaction preserves exact rows and rejects stale commit" {
    const allocator = std.testing.allocator;
    var state = try SelectedDonorState.init(allocator, 4, 1, 2);
    defer state.deinit();
    const rows = [_]f32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    try state.appendExact(0, &rows, &rows);
    const scores = [_]f64{ 0, 0.1, 0.8, 0.2, 0.7, 0.3, 0, 0 };
    const config = SelectV1Config{ .target_rows = 5, .hard_rows = 8, .recent_rows = 2, .prefill_chunk_rows = 2, .query_horizon = 2, .sink_rows = 1 };
    var transaction = try state.stageSelection(config, &scores, &.{});
    defer transaction.deinit();
    try transaction.commit(&state);
    try std.testing.expectEqualSlices(u64, &.{ 0, 2, 4, 6, 7 }, state.positions.items);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1, 4, 5, 8, 9, 12, 13, 14, 15 }, state.keys.items);

    var stale = try state.stageSelection(config, &.{ 0, 0, 0, 0, 0 }, &.{});
    defer stale.deinit();
    try state.appendExact(8, &.{ 16, 17 }, &.{ 16, 17 });
    try std.testing.expectError(error.StaleSelectionTransaction, stale.commit(&state));
}

test "select_v1 maps selected rows through noncontiguous pages" {
    const allocator = std.testing.allocator;
    const rows = try selectedPhysicalRows(allocator, &.{ 0, 3, 4, 7 }, &.{ 20, 100 }, 4);
    defer allocator.free(rows);
    try std.testing.expectEqualSlices(u32, &.{ 20, 23, 100, 103 }, rows);
    try std.testing.expectError(error.InvalidSelectionInput, selectedPhysicalRows(allocator, &.{ 3, 2 }, &.{0}, 4));
}
