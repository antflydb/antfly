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

//! Epoch-local owner for a bounded two-token decoder graph. The caller holds
//! mutex across preflight, real execution, and the final ID read. No borrowed
//! host page or tensor descriptor is retained by this owner.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");
const memory = @import("../../runtime/tier/memory.zig");
const plan_mod = @import("../../execution/stream_workspace.zig");
const paged_attention = @import("paged_attention_bf16.zig");
const DecodeScratch = paged_attention.DecodeScratch;
const recurrent_value_dim = 128;
const recurrent_packet_rows = @import("gated_delta.zig").max_packet_rows;
const quota_mod = @import("cache_quota.zig");
const funding_mod = @import("prepared_cache.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
pub const max_rows = 2 * paged_attention.max_packet_rows;

/// Each captured attention packet requires at least two rows. A cohort
/// ending in a singleton packet uses the ordinary decoder schedule.
pub fn supportsRows(rows: usize, wide: bool) bool {
    const packet_rows = paged_attention.max_packet_rows;
    const limit: usize = if (wide) max_rows else packet_rows;
    return rows >= 2 and rows <= limit and (rows <= packet_rows or rows % packet_rows != 1);
}

test "bounded CUDA decoder row eligibility follows attention packet boundaries" {
    const packet_rows = paged_attention.max_packet_rows;
    try std.testing.expect(!supportsRows(1, true));
    try std.testing.expect(supportsRows(2, false));
    try std.testing.expect(supportsRows(packet_rows, false));
    try std.testing.expect(!supportsRows(packet_rows + 2, false));
    try std.testing.expect(!supportsRows(packet_rows + 1, true));
    try std.testing.expect(supportsRows(packet_rows + 2, true));
    try std.testing.expect(supportsRows(max_rows, true));
    try std.testing.expect(!supportsRows(max_rows + 1, true));
}

pub const AttentionInputs = struct {
    query_bytes: usize,
    key_bytes: usize,
    bytes: usize,

    pub fn supports(heads: usize, kv_heads: usize, dim: usize) bool {
        return dim == 256 and ((heads == 8 and kv_heads == 2) or (heads == 16 and kv_heads == 4));
    }

    pub fn init(rows: usize, heads: usize, kv_heads: usize, dim: usize) !AttentionInputs {
        const row_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, dim), @sizeOf(f32));
        const q = try std.math.mul(usize, row_bytes, heads);
        const k = try std.math.mul(usize, row_bytes, kv_heads);
        return .{ .query_bytes = q, .key_bytes = k, .bytes = try std.math.add(usize, try std.math.mul(usize, q, 2), k) };
    }

    pub fn bind(self: AttentionInputs, backing: buffer.DeviceBuffer) ![3]buffer.DeviceBuffer {
        return .{
            try backing.borrowRange(0, self.query_bytes),
            try backing.borrowRange(self.query_bytes, self.key_bytes),
            try backing.borrowRange(self.query_bytes + self.key_bytes, self.query_bytes),
        };
    }
};

/// Exact independently retained buffers for a captured two-column sequence.
/// Pure geometry is available to cold admission before any SDK teardown or
/// allocation. It contains no page/key/weight pointer and grants no byte credit.
pub const StorageLayout = struct {
    input: Span,
    tokens: Span,
    positions: Span,
    counter: Span,
    attention_workspace: ?Span = null,
    attention_output: ?Span = null,
    recurrent_workspace: ?Span = null,
    recurrent_projection_joined: ?Span = null,
    recurrent_output: ?Span = null,
    ffn_workspace: ?Span = null,
    ffn_outputs: ?Span = null,
    projection_staging: ?Span = null,
    attention_residual: ?Span = null,
    attention_inputs: ?Span = null,
    input_projections: ?Span = null,
    attention_projection_bytes: [3]usize = @splat(0),
    recurrent_projection_bytes: [4]usize = @splat(0),
    norm_weight_workspace: ?Span = null,
    norm_weight_head_bytes: usize = 0,
    layer_norm: ?Span = null,
    layer_norm_mirror: ?Span = null,
    final_norm_mirror: ?Span = null,
    fused_head_workspace: ?Span = null,
    recurrent_scratch: ?@import("gated_delta.zig").PacketScratch = null,
    metadata_bytes: usize,
    host_bytes: usize,
    head_bytes: usize,
    shared_head_workspace: ?Span = null,

    pub fn headPacketRows(self: StorageLayout, rows: usize, vocab: usize) usize {
        if (self.shared_head_workspace) |span| return span.bytes / @sizeOf(f32) / vocab;
        if (self.head_bytes != 0) return self.head_bytes / @sizeOf(f32) / vocab;
        return rows;
    }

    const Span = struct {
        offset: usize,
        bytes: usize,

        fn bind(self: Span, backing: buffer.DeviceBuffer) buffer.DeviceBuffer {
            return backing.borrowRange(self.offset, self.bytes) catch unreachable;
        }
    };

    pub const max_readback_words = blk: {
        const maximum = init(max_rows, 1, 1, 1) catch unreachable;
        break :blk maximum.readbackProgressIndex() + 1;
    };

    pub fn readback(self: StorageLayout, backing: buffer.DeviceBuffer) buffer.DeviceBuffer {
        return (Span{ .offset = self.tokens.offset, .bytes = self.counter.offset + self.counter.bytes - self.tokens.offset }).bind(backing);
    }

    pub fn readbackPositionIndex(self: StorageLayout) usize {
        return (self.positions.offset - self.tokens.offset) / @sizeOf(u32);
    }

    pub fn readbackProgressIndex(self: StorageLayout) usize {
        return (self.counter.offset - self.tokens.offset) / @sizeOf(u32);
    }

    /// Check the device receipt before any tokens or position changes escape.
    pub fn publishReadback(self: StorageLayout, host: []const u32, previous_positions: []u32, progress: *u32, first: []u32, second: []u32) !void {
        const counter_word = self.readbackProgressIndex();
        const position_word = self.readbackPositionIndex();
        const rows = self.tokens.bytes / (2 * @sizeOf(u32));
        if (host.len != counter_word + 1 or first.len != rows or second.len != rows or
            previous_positions.len != self.positions.bytes / @sizeOf(u32))
            return error.InvalidExecutionPlan;
        if (host[counter_word] != try std.math.add(u32, progress.*, 2)) return error.InvalidExecutionPlan;
        const positions = host[position_word..][0..previous_positions.len];
        for (previous_positions, positions) |position, actual|
            if (actual != try std.math.add(u32, position, 2)) return error.InvalidExecutionPlan;
        @memcpy(first, host[0..rows]);
        @memcpy(second, host[rows..][0..rows]);
        @memcpy(previous_positions, positions);
        progress.* = host[counter_word];
    }

    pub fn init(rows: usize, hidden: usize, vocab: usize, key_words: usize) !StorageLayout {
        return initWithHeadLimit(rows, hidden, vocab, key_words, rows);
    }

    fn initWithHeadLimit(rows: usize, hidden: usize, vocab: usize, key_words: usize, max_head_rows: usize) !StorageLayout {
        if (rows < 2 or rows > max_rows or hidden == 0 or vocab == 0 or key_words == 0 or max_head_rows == 0) return error.InvalidShape;
        const input_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, hidden), @sizeOf(f32));
        const bank_offset = try aligned(input_bytes);
        const token_bytes = rows * 2 * @sizeOf(u32);
        const position_bytes = rows * 3 * @sizeOf(u32);
        // Keep the input and token bank at CUDA allocation alignment. The
        // position plane only needs vector alignment and progress is scalar.
        const positions_offset = try std.math.add(usize, bank_offset, std.mem.alignForward(usize, token_bytes, 16));
        const progress_offset = try std.math.add(usize, positions_offset, position_bytes);
        const metadata_bytes = try aligned(try std.math.add(usize, progress_offset, @sizeOf(u32)));
        const host_bytes = try std.math.add(usize, @sizeOf(plan_mod.Plan), try std.math.mul(usize, key_words, @sizeOf(u64)));
        const head_bytes = if (rows > 64) try std.math.mul(usize, try std.math.mul(usize, @min(rows, max_head_rows), vocab), @sizeOf(f32)) else 0;
        if (head_bytes > plan_mod.limit) return error.InvalidShape;
        return .{
            .input = .{ .offset = 0, .bytes = input_bytes },
            .tokens = .{ .offset = bank_offset, .bytes = token_bytes },
            .positions = .{ .offset = positions_offset, .bytes = position_bytes },
            .counter = .{ .offset = progress_offset, .bytes = @sizeOf(u32) },
            .metadata_bytes = metadata_bytes,
            .host_bytes = host_bytes,
            .head_bytes = head_bytes,
        };
    }
    fn declareAttentionWorkspace(self: *StorageLayout, scratch: DecodeScratch) !void {
        const bytes = @max(scratch.full.bytes, if (scratch.tail) |tail| tail.bytes else 0);
        self.attention_workspace = try self.declareLayerWorkspace(bytes);
    }

    fn declareLayerWorkspace(self: *StorageLayout, bytes: usize) !Span {
        return self.declareLayerRegion(0, bytes);
    }

    fn declareLayerRegion(self: *StorageLayout, relative_offset: usize, bytes: usize) !Span {
        if (bytes == 0 or relative_offset > plan_mod.limit or bytes > plan_mod.limit - relative_offset) return error.InvalidShape;
        const offset = try std.math.add(usize, try self.layerWorkspaceOffset(), relative_offset);
        self.metadata_bytes = @max(self.metadata_bytes, try std.math.add(usize, offset, bytes));
        return .{ .offset = offset, .bytes = bytes };
    }

    pub fn layerWorkspaceBytes(self: StorageLayout) usize {
        return self.metadata_bytes - (self.layerWorkspaceOffset() catch unreachable);
    }

    /// Physical prerequisites for observing a declared warm burst at any
    /// supported graph width. The actual dispatch still proves coverage.
    pub fn supportsDeclaredWarmup(self: StorageLayout, has_head_destination: bool) bool {
        return (has_head_destination or self.fused_head_workspace != null) and
            self.layer_norm != null and self.ffn_workspace != null and
            self.recurrent_projection_joined != null and self.attention_inputs != null;
    }

    /// The terminal head runs after the final normalization publishes a
    /// separate tensor. Its bank can alias the layer-live spans.
    fn layerLiveBytes(self: StorageLayout) !usize {
        const start = try self.layerWorkspaceOffset();
        var end = start;
        for ([_]?Span{
            self.attention_workspace,
            self.attention_output,
            self.recurrent_workspace,
            self.recurrent_projection_joined,
            self.recurrent_output,
            self.ffn_workspace,
            self.ffn_outputs,
            self.attention_residual,
            self.attention_inputs,
            self.input_projections,
            self.norm_weight_workspace,
            self.layer_norm,
            self.layer_norm_mirror,
        }) |slot| if (slot) |span| {
            end = @max(end, try std.math.add(usize, span.offset, span.bytes));
        };
        return end - start;
    }

    fn layerWorkspaceOffset(self: StorageLayout) !usize {
        return aligned(try std.math.add(usize, self.counter.offset, self.counter.bytes));
    }

    fn aligned(bytes: usize) !usize {
        return (try std.math.add(usize, bytes, 255)) & ~@as(usize, 255);
    }
};

/// The scheduler may rotate the same cohort between claims. Capture uses a
/// stable sequence order, while publication retains the original claim order.
pub fn canonicalOrder(sequences: []const u64, order: []usize) !void {
    if (sequences.len < 2 or sequences.len > max_rows or order.len != sequences.len) return error.InvalidShape;
    for (sequences, 0..) |sequence, i| {
        if (sequence == 0) return error.InvalidPagedKvBatch;
        order[i] = i;
        var j = i;
        while (j > 0 and sequences[order[j - 1]] > sequence) : (j -= 1) {
            order[j] = order[j - 1];
        }
        if (j > 0 and sequences[order[j - 1]] == sequence) return error.InvalidPagedKvBatch;
        order[j] = i;
    }
}

test "LightOnOCR3 CUDA graph canonical cohorts restore both token columns after scheduler rotation" {
    // The two claims name the same states in different scheduler orders.
    // Canonical decoder rows and each original caller's IDs must agree.
    const claims = [_][4]u64{ .{ 43, 7, 29, 11 }, .{ 11, 43, 7, 29 } };
    for (claims) |sequences| {
        var order: [4]usize = undefined;
        try canonicalOrder(&sequences, &order);
        var canonical: [4]u64 = undefined;
        var published: [2][4]u64 = undefined;
        for (order, 0..) |original, row| {
            canonical[row] = sequences[original];
            published[0][original] = sequences[original] * 10;
            published[1][original] = sequences[original] * 10 + 1;
        }
        try std.testing.expectEqualSlices(u64, &.{ 7, 11, 29, 43 }, &canonical);
        for (sequences, 0..) |sequence, original| {
            try std.testing.expectEqual(sequence * 10, published[0][original]);
            try std.testing.expectEqual(sequence * 10 + 1, published[1][original]);
        }
    }
    var order: [4]usize = undefined;
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 29, 7, 43 }, &order));
    try std.testing.expectError(error.InvalidPagedKvBatch, canonicalOrder(&.{ 7, 0, 29, 43 }, &order));
}
pub const Phase = enum {
    warm,
    learn,
    capture,
    replay,
    disabled,

    pub fn canReachReplay(self: Phase, remaining_tokens: usize) bool {
        // Warmup can declare a plan directly or require a separate learning
        // burst. Each completed burst consumes two output tokens.
        return switch (self) {
            .warm, .learn => remaining_tokens >= 6,
            .capture => remaining_tokens >= 4,
            .replay => remaining_tokens >= 2,
            .disabled => false,
        };
    }
};
/// Borrowed only through one final-address Executor phase. Full key/positions
/// remain synchronous under the held worker step; no such borrow enters Module.
pub const Descriptor = struct {
    epoch: u64,
    key: []const u64,
    sequences: []const u64 = &.{},
    positions: []const u32,
    rows: usize,
    hidden: usize,
    vocab: usize,
    future_tokens: usize,
    minimum_remaining_tokens: usize = std.math.maxInt(usize),
    head_storage: HeadStorage = .full_bank,
    attention_geometry: ?DecodeScratch.Geometry = null,
    attention_q_gate: bool = false,
    norm_weight_offset: f32 = 0,
    recurrent_heads: ?struct { key: usize, value: usize } = null,
    recurrent_projection_widths: ?[4]usize = null,
    attention_layers: usize,
    recurrent_layers: usize,
    ffn_intermediate: usize = 0,
    pub const HeadStorage = union(enum) {
        full_bank,
        adaptive_bank: usize,
        shared_layer_packets: usize,
        fused_top1,
    };

    pub fn layout(self: Descriptor) !StorageLayout {
        if (self.epoch == 0 or self.rows > max_rows or self.positions.len != self.rows * 3) return error.InvalidShape;
        if (self.sequences.len != 0 and self.sequences.len != self.rows) return error.InvalidShape;
        const packet_rows = switch (self.head_storage) {
            .full_bank, .fused_top1 => self.rows,
            .adaptive_bank, .shared_layer_packets => |rows| rows,
        };
        var storage = try StorageLayout.initWithHeadLimit(self.rows, self.hidden, self.vocab, self.key.len, packet_rows);
        if (self.head_storage == .shared_layer_packets) {
            const elements = try std.math.mul(usize, @min(self.rows, packet_rows), self.vocab);
            const bytes = try std.math.mul(usize, elements, @sizeOf(f32));
            storage.shared_head_workspace = try storage.declareLayerWorkspace(bytes);
            storage.head_bytes = 0;
        }
        if (self.head_storage == .fused_top1) {
            const scratch = try @import("projection_top1_bf16.zig").Scratch.forRows(self.rows);
            storage.fused_head_workspace = try storage.declareLayerWorkspace(scratch.bytes);
            storage.head_bytes = 0;
        }
        return storage;
    }
    pub const DispatchCounts = struct { attention: usize, attention_gates: usize, ragged_recurrent: usize };

    pub fn dispatchCounts(self: Descriptor) !DispatchCounts {
        if (self.rows < 2 or self.rows > max_rows) return error.InvalidShape;
        const attention_packets = (self.rows - 1) / paged_attention.max_packet_rows + 1;
        const recurrent_packets = (self.rows - 1) / recurrent_packet_rows + 1;
        // Each token gates the assembled attention output once per layer.
        const attention_gates = try std.math.mul(usize, self.attention_layers, 2);
        return .{
            .attention = try std.math.mul(usize, attention_gates, attention_packets),
            .attention_gates = attention_gates,
            .ragged_recurrent = if (recurrent_packets > 1) try std.math.mul(usize, self.recurrent_layers, 2 * recurrent_packets) else 0,
        };
    }

    fn storageRequestFor(self: Descriptor, value: StorageLayout) funding_mod.Request {
        return .{ .captured_storage = .{ .epoch = self.epoch, .key_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(self.key)), .key_words = self.key.len, .rows = self.rows, .hidden = self.hidden, .vocab = self.vocab, .metadata_bytes = value.metadata_bytes, .host_bytes = value.host_bytes, .head_bytes = value.head_bytes } };
    }
};

pub const Snapshot = struct {
    invalidations: u64,
    epoch: u64,
    phase: Phase,
    metadata: usize,
    arena: usize,
    head: usize,
    graph: usize,
    pending_graph: usize,
    plan: usize,
    storage_layout: ?StorageLayout,
    admitted_arena_layout: ?plan_mod.Layout,
    arena_layout: ?plan_mod.Layout,
    metadata_quota: quota_mod.Quota,
    arena_quota: quota_mod.Quota,
};

/// Pure heap metadata retired by the CURRENT caller's unsealed B. It never
/// borrows SDK/model/Io/request-allocator state, and never stores an Epoch.
const ColdPhase = struct {
    // Production is the stateless process allocator. The private test helper
    // injects allocation failure only while its complete B lifetime is live.
    allocator: std.mem.Allocator = std.heap.c_allocator,
    node: retirement.Node = undefined,
    funding: ?*funding_mod.Funding = null,
    key: []u64 = &.{},
    plan: ?*plan_mod.Plan = null,
    fn destroyOutside(self: *ColdPhase) void {
        if (self.funding) |value| value.finishOutsideExecution();
        if (self.plan) |value| self.allocator.destroy(value);
        if (self.key.len != 0) self.allocator.free(self.key);
        self.allocator.destroy(self);
    }
    fn retire(raw: *anyopaque) callconv(.c) void {
        const self: *ColdPhase = @ptrCast(@alignCast(raw));
        self.destroyOutside();
    }
};

pub const GraphPhase = struct {
    descriptor: Descriptor,
    storage_layout: StorageLayout = undefined,
    vendor_workspace: buffer.DeviceBuffer = .{},
    attention_scratch: ?DecodeScratch = null,
    mode: enum { none, storage, arena } = .none,
    expected_compute: usize = 0,
    old: Snapshot = undefined,
    admission_pressure: ?memory.AdmissionPressure = null,
    insufficient_replay_horizon: bool = false,
    admission: ?@import("video_cache_budget.zig").Admission = null,
    cold: ?*ColdPhase = null,
    bound: bool = false,
    pub fn initInto(self: *GraphPhase, descriptor: Descriptor) !void {
        const layout = try descriptor.layout();
        _ = try descriptor.dispatchCounts();
        self.* = .{ .descriptor = descriptor, .storage_layout = layout };
    }
    pub const WorkspacePolicy = struct {
        attention_chunk128: ?bool = null,
        recurrent_scratch: bool,
        compact_ffn: ?bool,
        projection_staging: bool,
        final_norm_mirror: bool = false,
        layer_norm: bool = false,
        fused_attention_inputs: bool = false,
        fused_recurrent_projections: bool = false,
        adjust_norm_weights: bool = false,
    };

    /// Resolve layer lifetimes in submission order from the descriptor. A
    /// repeated inspection starts from the same prefix and head reservation.
    pub fn prepareWorkspace(self: *GraphPhase, policy: WorkspacePolicy) !void {
        self.storage_layout = try self.descriptor.layout();
        self.attention_scratch = null;
        try self.declareAttentionScratch(policy.attention_chunk128);
        try self.declareAttentionOutput();
        if (policy.recurrent_scratch) try self.declareRecurrentScratch();
        try self.declareRecurrentOutput();
        var recurrent_projection_total: usize = 0;
        if (self.descriptor.recurrent_layers != 0) {
            if (self.descriptor.recurrent_projection_widths) |widths| {
                const row_bytes = try std.math.mul(usize, self.descriptor.rows, @sizeOf(f32));
                for (widths, &self.storage_layout.recurrent_projection_bytes) |width, *bytes| {
                    if (width == 0) return error.InvalidShape;
                    bytes.* = try std.math.mul(usize, row_bytes, width);
                    recurrent_projection_total = try std.math.add(usize, recurrent_projection_total, bytes.*);
                }
            }
            // The joined GEMM result dies at the split, before the
            // recurrent core or the FFN enters the serial scratch bank.
            if (policy.fused_recurrent_projections) {
                if (!policy.layer_norm) return error.InvalidShape;
                self.storage_layout.recurrent_projection_joined = try self.storage_layout.declareLayerWorkspace(recurrent_projection_total);
            }
        }
        if (policy.adjust_norm_weights) {
            // Hidden norm and Q/K norm preparation run before the other
            // serial scratch. The Q and K vectors may be live together.
            const hidden_bytes = try std.math.mul(usize, self.descriptor.hidden, @sizeOf(f32));
            const head_bytes = if (self.descriptor.attention_geometry) |geometry|
                try std.math.mul(usize, geometry.dim, @sizeOf(f32))
            else
                0;
            self.storage_layout.norm_weight_head_bytes = head_bytes;
            self.storage_layout.norm_weight_workspace = try self.storage_layout.declareLayerWorkspace(@max(hidden_bytes, try std.math.mul(usize, head_bytes, 2)));
        }
        if (policy.compact_ffn) |compact| try self.declareFfnScratch(compact);
        if (policy.layer_norm and self.storage_layout.ffn_outputs == null) {
            const elements = try std.math.mul(usize, self.descriptor.rows, self.descriptor.hidden);
            try self.declareLayerOutputs(try std.math.mul(usize, elements, @sizeOf(f32)));
        }
        if (policy.layer_norm) {
            const elements = try std.math.mul(usize, self.descriptor.rows, self.descriptor.hidden);
            const norm_bytes = try std.math.mul(usize, elements, @sizeOf(f32));
            const offset = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
            self.storage_layout.layer_norm = try self.storage_layout.declareLayerRegion(offset, norm_bytes);
            if (policy.final_norm_mirror) {
                const mirror_offset = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
                self.storage_layout.layer_norm_mirror = try self.storage_layout.declareLayerRegion(mirror_offset, try std.math.mul(usize, elements, @sizeOf(u16)));
            }
        }
        if (policy.layer_norm) {
            const bytes = self.storage_layout.layer_norm.?.bytes;
            const offset = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
            self.storage_layout.attention_residual = try self.storage_layout.declareLayerRegion(offset, bytes);
        }
        if (self.descriptor.attention_layers != 0 and
            (policy.fused_attention_inputs or (policy.layer_norm and self.descriptor.attention_q_gate)))
        {
            const geometry = self.descriptor.attention_geometry orelse return error.InvalidShape;
            const fused = policy.fused_attention_inputs and AttentionInputs.supports(geometry.heads, geometry.kv_heads, geometry.dim);
            if (fused or self.descriptor.attention_q_gate) {
                const inputs = try AttentionInputs.init(self.descriptor.rows, geometry.heads, geometry.kv_heads, geometry.dim);
                const offset = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
                self.storage_layout.attention_inputs = try self.storage_layout.declareLayerRegion(offset, inputs.bytes);
            }
        }
        var projection_bytes: usize = 0;
        if (policy.layer_norm and self.descriptor.attention_layers != 0) {
            const geometry = self.descriptor.attention_geometry orelse return error.InvalidShape;
            const inputs = try AttentionInputs.init(self.descriptor.rows, geometry.heads, geometry.kv_heads, geometry.dim);
            const query_bytes = if (self.descriptor.attention_q_gate) try std.math.mul(usize, inputs.query_bytes, 2) else inputs.query_bytes;
            projection_bytes = try std.math.add(usize, query_bytes, try std.math.mul(usize, inputs.key_bytes, 2));
            self.storage_layout.attention_projection_bytes = .{ query_bytes, inputs.key_bytes, inputs.key_bytes };
        }
        if (policy.layer_norm) projection_bytes = @max(projection_bytes, recurrent_projection_total);
        if (projection_bytes != 0) {
            const offset = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
            self.storage_layout.input_projections = try self.storage_layout.declareLayerRegion(offset, projection_bytes);
        }
        if (policy.projection_staging) try self.declareProjectionStaging();
        if (policy.final_norm_mirror) try self.declareFinalNormMirror();
    }

    fn declareFinalNormMirror(self: *GraphPhase) !void {
        const layout = &self.storage_layout;
        const elements = try std.math.mul(usize, self.descriptor.rows, self.descriptor.hidden);
        const bytes = try std.math.mul(usize, elements, @sizeOf(u16));
        const prefix = try layout.layerWorkspaceOffset();
        const head_bytes = @max(
            if (layout.shared_head_workspace) |span| span.bytes else 0,
            if (layout.fused_head_workspace) |span| span.bytes else 0,
        );
        // Layer outputs are dead when the final norm runs. Only the terminal
        // head bank and any projection staging remain concurrent with its mirror.
        const norm_weight_bytes = if (layout.norm_weight_workspace) |span| span.bytes else 0;
        var offset = try StorageLayout.aligned(@max(head_bytes, norm_weight_bytes));
        if (layout.projection_staging) |span| {
            const staging_start = span.offset - prefix;
            const staging_end = try std.math.add(usize, staging_start, span.bytes);
            if (offset < staging_end and try std.math.add(usize, offset, bytes) > staging_start)
                offset = try StorageLayout.aligned(staging_end);
        }
        layout.final_norm_mirror = try layout.declareLayerRegion(offset, bytes);
    }

    pub fn declareAttentionScratch(self: *GraphPhase, chunk128_override: ?bool) !void {
        const descriptor = self.descriptor;
        if (descriptor.attention_layers == 0) return;
        const geometry = descriptor.attention_geometry orelse return error.InvalidShape;
        const scratch = try DecodeScratch.init(descriptor.rows, descriptor.future_tokens, geometry, paged_attention.useChunk128(chunk128_override, geometry.heads));
        try self.storage_layout.declareAttentionWorkspace(scratch);
        self.attention_scratch = scratch;
    }

    pub fn declareRecurrentScratch(self: *GraphPhase) !void {
        const descriptor = self.descriptor;
        if (descriptor.recurrent_layers == 0) return;
        const heads = descriptor.recurrent_heads orelse return error.InvalidShape;
        const recurrent = @import("gated_delta.zig");
        const scratch = try recurrent.PacketScratch.forHeads(@min(descriptor.rows, recurrent_packet_rows), heads.key, heads.value);
        self.storage_layout.recurrent_workspace = try self.storage_layout.declareLayerWorkspace(scratch.bytes);
        self.storage_layout.recurrent_scratch = scratch;
    }

    pub fn declareAttentionOutput(self: *GraphPhase) !void {
        if (self.descriptor.attention_layers == 0) return;
        const geometry = self.descriptor.attention_geometry orelse return error.InvalidShape;
        const width = try std.math.mul(usize, geometry.heads, geometry.dim);
        const bytes = try std.math.mul(usize, self.descriptor.rows * @sizeOf(f32), width);
        const scratch = self.storage_layout.attention_workspace orelse return error.InvalidShape;
        self.storage_layout.attention_output = try self.storage_layout.declareLayerRegion(try StorageLayout.aligned(scratch.bytes), bytes);
    }

    pub fn declareRecurrentOutput(self: *GraphPhase) !void {
        const descriptor = self.descriptor;
        if (descriptor.recurrent_layers == 0) return;
        const heads = descriptor.recurrent_heads orelse return error.InvalidShape;
        const width = try std.math.mul(usize, heads.value, recurrent_value_dim);
        const bytes = try std.math.mul(usize, descriptor.rows * @sizeOf(f32), width);
        const scratch_bytes = if (self.storage_layout.recurrent_workspace) |span| span.bytes else 0;
        self.storage_layout.recurrent_output = try self.storage_layout.declareLayerRegion(try StorageLayout.aligned(scratch_bytes), bytes);
    }

    pub fn declareFfnScratch(self: *GraphPhase, compact: bool) !void {
        const descriptor = self.descriptor;
        if (descriptor.ffn_intermediate == 0) return;
        const Ffn = @import("dense_ffn_workspace.zig").Layout;
        const layout = if (compact)
            try Ffn.initCompact(descriptor.rows, descriptor.hidden, descriptor.ffn_intermediate)
        else
            try Ffn.init(descriptor.rows, descriptor.hidden, descriptor.ffn_intermediate, 0);
        self.storage_layout.ffn_workspace = try self.storage_layout.declareLayerWorkspace(layout.scratch_bytes);
        try self.declareLayerOutputs(layout.output_bytes);
    }

    fn declareLayerOutputs(self: *GraphPhase, bytes: usize) !void {
        // Residuals can cross a layer boundary while layer scratch is reused.
        // Place both alternating publication slots after all serial scratch.
        const scratch_end = try StorageLayout.aligned(try self.storage_layout.layerLiveBytes());
        self.storage_layout.ffn_outputs = try self.storage_layout.declareLayerRegion(scratch_end, try std.math.mul(usize, bytes, 2));
    }

    pub fn declareProjectionStaging(self: *GraphPhase) !void {
        const descriptor = self.descriptor;
        var width = @max(descriptor.hidden, descriptor.ffn_intermediate);
        if (descriptor.attention_geometry) |geometry|
            width = @max(width, try std.math.mul(usize, geometry.heads, geometry.dim));
        if (descriptor.recurrent_heads) |heads|
            width = @max(width, try std.math.mul(usize, heads.value, recurrent_value_dim));
        const bytes = try std.math.mul(usize, descriptor.rows * @sizeOf(u16), width);
        // The ordinary head GEMM also uses projection staging.
        const layer_bytes = try self.storage_layout.layerLiveBytes();
        const head_bytes = if (self.storage_layout.shared_head_workspace) |head| head.bytes else 0;
        const occupied = try StorageLayout.aligned(@max(layer_bytes, head_bytes));
        self.storage_layout.projection_staging = try self.storage_layout.declareLayerRegion(occupied, bytes);
    }

    pub fn matchesOwner(self: *const GraphPhase, owner: *const Module) bool {
        if (!owner.matches(self.descriptor)) return false;
        var storage = owner.storage_layout orelse return false;
        const expected = self.storage_layout;
        if (self.descriptor.head_storage == .adaptive_bank and storage.head_bytes < expected.head_bytes) {
            const row_bytes = self.descriptor.vocab * @sizeOf(f32);
            if (storage.head_bytes < 8 * row_bytes or storage.head_bytes % row_bytes != 0) return false;
            storage.head_bytes = expected.head_bytes;
        }
        return std.meta.eql(storage, expected) and std.meta.eql(owner.attention_scratch, self.attention_scratch) and std.meta.eql(owner.vendor_workspace, self.vendor_workspace);
    }

    /// Only the optional packet bank changes. Input, token and attention spans
    /// retain their exact declared addresses and the prepared graph records
    /// the bank it actually owns. Bound the number of extra GEMM launches.
    pub fn reduceHeadPacketAfterDenial(self: *GraphPhase) bool {
        if (self.mode != .storage or self.cold != null or self.bound or self.descriptor.head_storage != .adaptive_bank) return false;
        const pressure = self.admission_pressure orelse return false;
        switch (pressure) {
            .domain_scratch, .shared_scratch => {},
            else => return false,
        }
        if (self.storage_layout.head_bytes == 0) return false;
        const row_bytes = self.descriptor.vocab * @sizeOf(f32);
        const packet_rows = self.storage_layout.head_bytes / row_bytes;
        if (packet_rows <= 8) return false;
        self.storage_layout.head_bytes = @max(8, packet_rows / 2) * row_bytes;
        return true;
    }

    fn arenaRequest(self: *const GraphPhase, layout: plan_mod.Layout) funding_mod.Request {
        return .{ .captured_arena = .{
            .epoch = self.descriptor.epoch,
            .key_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(self.descriptor.key)),
            .bytes = layout.bytes,
            .events = layout.events,
            .recipe = layout.recipe,
        } };
    }

    /// Caller has dropped EVERY model/provider/reader lane but retains the
    /// exact model lifetime and SAME worker step. All callback-capable quota,
    /// Domain and process metadata ownership is created here.
    pub fn prepareOutside(self: *GraphPhase, model: *const @import("../../backends/session.zig").Session.ModelLifetime) !void {
        return self.prepareOutsideUsing(model, std.heap.c_allocator);
    }
    fn prepareOutsideUsing(self: *GraphPhase, model: *const @import("../../backends/session.zig").Session.ModelLifetime, allocator: std.mem.Allocator) !void {
        if (self.cold != null or self.bound or self.mode == .none) return error.InvalidPreparedCacheState;
        self.admission_pressure = null;
        const cold = try allocator.create(ColdPhase);
        cold.* = .{ .allocator = allocator };
        errdefer cold.destroyOutside();
        const request = if (self.mode == .storage) self.descriptor.storageRequestFor(self.storage_layout) else self.arenaRequest(self.old.arena_layout orelse return error.InvalidPreparedCacheState);
        const amounts = switch (request) {
            .captured_storage => |value| try value.amounts(),
            .captured_arena => |value| value.amounts(),
            else => unreachable,
        };
        const funding = try funding_mod.Funding.createWithAllocator(allocator, .{ .requests = &.{request} });
        cold.funding = funding;
        try funding.noteMiss(0, amounts, 0);
        funding.admission = self.admission;
        funding.compute_identity = self.expected_compute;
        funding.state = .snapped;
        var pin: ?@import("../../runtime/tier/admission_domain.zig").Pin = null;
        if (self.admission) |policy| pin = model.retainAdmissionDomain(policy.controller) orelse return error.MissingCacheRetirementDomain;
        defer if (pin) |value| value.release();
        funding.fundOutsideExecution(if (pin) |*value| value else null) catch |err| {
            self.admission_pressure = funding.admission_pressure;
            if (self.admission) |policy| {
                if (funding.admission_pressure != null and @import("antfly_platform").env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) {
                    const occupied = policy.controller.snapshotBackend(.gpu);
                    std.debug.print("LIGHTONOCR3 cuda_model_graph_funding rows={d} stage={s} error={s} requested_host_scratch={d} requested_backend_scratch={d} occupied_host_scratch={d} occupied_backend_scratch={d} policy_scratch_limit={d}\n", .{
                        self.descriptor.rows,        @tagName(self.mode),            @errorName(err),                   amounts.host_scratch_bytes, amounts.backend_scratch_bytes,
                        occupied.host_scratch_bytes, occupied.backend_scratch_bytes, policy.limits.scratch_limit_bytes,
                    });
                }
            }
            return err;
        };
        if (self.mode == .storage) {
            cold.key = try allocator.dupe(u64, self.descriptor.key);
            cold.plan = try allocator.create(plan_mod.Plan);
            cold.plan.?.* = .{};
        }
        self.cold = cold;
    }
    /// Metadata-only. Concrete CUDA owner validates full table/context,
    /// geometry/key/policy/snapshot before exposing this funding to reset.
    pub fn bind(self: *GraphPhase, current: Cleanup) !void {
        if (self.bound or self.mode == .none) return error.InvalidPreparedCacheState;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        const cold = self.cold orelse return error.InvalidPreparedCacheState;
        try cold.funding.?.bind(epoch);
        self.bound = true;
    }
    /// Call immediately after beginGraph consumed/published its tokens, before
    /// later numerical reads may park again. No persistent graph retains this
    /// phase or Funding pointer. On error the caller still owns cold metadata.
    pub fn close(self: *GraphPhase, current: Cleanup) !void {
        const cold = self.cold orelse return;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.InvalidPreparedCacheState;
        if (self.bound) {
            cold.funding.?.unbind();
            self.bound = false;
        }
        cold.node = .{ .context = cold, .release_fn = ColdPhase.retire };
        const boundary = try epoch.boundary();
        self.cold = null; // consume public custody before callback-capable registration
        errdefer self.cold = cold; // a rejected registration transferred no ownership
        try boundary.register(boundary.context, boundary.nonce, &cold.node);
    }
    pub fn destroyOutside(self: *GraphPhase) void {
        std.debug.assert(!self.bound);
        const cold = self.cold orelse return;
        self.cold = null;
        cold.destroyOutside();
    }
};

pub const Module = struct {
    mutex: std.atomic.Mutex = .unlocked,
    epoch_id: u64 = 0,
    sequences: [max_rows]u64 = undefined,
    sequence_count: usize = 0,
    captured_roots_identity: usize = 0,
    disabled_epoch: u64 = 0,
    key: []u64 = &.{},
    plan: ?*plan_mod.Plan = null,
    phase: Phase = .warm,
    vendor_workspace: buffer.DeviceBuffer = .{},
    metadata: buffer.DeviceBuffer = .{},
    arena: buffer.DeviceBuffer = .{},
    admitted_arena_layout: ?plan_mod.Layout = null,
    metadata_quota: quota_mod.Quota = .none,
    arena_quota: quota_mod.Quota = .none,
    graph: driver.CUgraphExec = null,
    pending_graph: driver.CUgraph = null,
    host_allocator: std.mem.Allocator = std.heap.c_allocator,
    rows: usize = 0,
    hidden: usize = 0,
    vocab: usize = 0,
    // Both graph steps reuse this independently admitted logits bank after
    // argmax when the plan assigns a separate head buffer. Arena-backed
    // packets instead allocate their logits through the stream workspace.
    head: buffer.DeviceBuffer = .{},
    attention_scratch: ?DecodeScratch = null,
    attention_layers: usize = 0,
    recurrent_layers: usize = 0,
    expected_attention_calls: usize = 0,
    expected_attention_gates: usize = 0,
    expected_ragged_packets: usize = 0,
    bound_recurrent_scratch_bytes: usize = 0,
    bound_recurrent_joined_projection_bytes: usize = 0,
    attention_gates_in_place: usize = 0,
    next_ffn_output: u1 = 0,
    storage_layout: ?StorageLayout = null,
    base_positions: [3 * max_rows]u32 = @splat(0),
    next_positions: [3 * max_rows]u32 = @splat(0),
    progress: u32 = 0,
    capture_step: u32 = 0,
    active: bool = false,
    captures: u64 = 0,
    replays: u64 = 0,
    wide_replays: u64 = 0,
    max_replay_rows: usize = 0,
    invalidations: u64 = 0,
    admission_denials: u64 = 0,
    weight_declines_logged: usize = 0,
    state_declines_logged: usize = 0,
    resets_logged: usize = 0,

    /// Explicit historical raw owner only. A managed cache must receive its
    /// CURRENT caller B before any SDK mutation; null is never outside proof.
    pub fn clear(self: *Module, ctx: *context.CudaContext) !void {
        try self.metadata_quota.validateLegacyRaw();
        try self.arena_quota.validateLegacyRaw();
        return self.clearWithCleanup(ctx, .{ .outside_execution = true });
    }
    pub fn retirementPending(self: *const Module) bool {
        return self.phase != .disabled or self.active or self.graph != null or self.pending_graph != null or
            self.metadata.ptr != 0 or self.arena.ptr != 0 or self.head.ptr != 0 or
            self.plan != null or self.key.len != 0 or self.sequence_count != 0 or self.captured_roots_identity != 0 or
            self.storage_layout != null or self.admitted_arena_layout != null or
            self.metadata_quota != .none or self.arena_quota != .none;
    }

    pub fn validateCleanup(self: *const Module, current: Cleanup) !void {
        try self.metadata_quota.validateCleanup(current);
        try self.arena_quota.validateCleanup(current);
    }
    pub fn clearWithCleanup(self: *Module, ctx: *context.CudaContext, current: Cleanup) !void {
        try self.validateCleanup(current);
        if (self.active) return error.CapturedGraphActive;
        const live = self.head.ptr != 0 or self.metadata.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null;
        if (live and (ctx.driver.fns.cuStreamIsCapturing == null or try ctx.streamCaptureActive())) return error.CapturedGraphActive;
        self.phase = .disabled;
        self.active = false;
        if (self.head.ptr != 0 or self.metadata.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null) try ctx.synchronize();
        // Every failed SDK handle remains in this actual owner. Neither quota
        // nor host metadata is dropped until ALL independently retained backing
        // has checked completion/destruction. No best-effort free certifies it.
        try ctx.destroyGraphExecChecked(&self.graph);
        try ctx.destroyGraphChecked(&self.pending_graph);
        _ = try self.head.freeChecked(ctx);
        _ = try self.arena.freeChecked(ctx);
        self.admitted_arena_layout = null;
        _ = try self.metadata.freeChecked(ctx);
        if (self.plan) |plan| self.host_allocator.destroy(plan);
        self.plan = null;
        if (self.key.len != 0) self.host_allocator.free(self.key);
        self.key = &.{};
        self.sequence_count = 0;
        try self.arena_quota.releaseAfterSdkDeath(current);
        try self.metadata_quota.releaseAfterSdkDeath(current);
        self.captured_roots_identity = 0;
        self.storage_layout = null;
        self.attention_scratch = null;
        self.vendor_workspace = .{};
        self.bound_recurrent_scratch_bytes = 0;
        self.bound_recurrent_joined_projection_bytes = 0;
        self.attention_gates_in_place = 0;
        self.rows = 0;
        self.hidden = 0;
        self.vocab = 0;
        self.progress = 0;
        self.invalidations += 1;
    }
    pub fn snapshot(self: *const Module) Snapshot {
        const missing_arena_layout = if (self.phase == .capture and self.admitted_arena_layout == null)
            self.arenaLayout() catch null
        else
            null;
        return .{
            .invalidations = self.invalidations,
            .epoch = self.epoch_id,
            .phase = self.phase,
            .metadata = self.metadata.ptr,
            .arena = self.arena.ptr,
            .head = self.head.ptr,
            .graph = if (self.graph) |value| @intFromPtr(value) else 0,
            .pending_graph = if (self.pending_graph) |value| @intFromPtr(value) else 0,
            .plan = if (self.plan) |value| @intFromPtr(value) else 0,
            .storage_layout = self.storage_layout,
            .admitted_arena_layout = self.admitted_arena_layout,
            .arena_layout = missing_arena_layout,
            .metadata_quota = self.metadata_quota,
            .arena_quota = self.arena_quota,
        };
    }
    pub fn invalidate(self: *Module, ctx: *context.CudaContext) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.clear(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    /// Production keys include the recurrent address of every bound layer.
    /// A matching word elsewhere in a key can only cause conservative retirement.
    pub fn referencesSequence(self: *const Module, sequence: u64) bool {
        return self.sequence_count == 0 or std.mem.indexOfScalar(u64, self.sequences[0..self.sequence_count], sequence) != null;
    }

    pub fn referencesRecurrentAddress(self: *const Module, address: u64) bool {
        return self.sequence_count == 0 or (address != 0 and std.mem.indexOfScalar(u64, self.key, address) != null);
    }

    pub fn matches(self: *const Module, descriptor: Descriptor) bool {
        return self.epoch_id == descriptor.epoch and self.rows == descriptor.rows and
            self.hidden == descriptor.hidden and self.vocab == descriptor.vocab and
            self.attention_layers == descriptor.attention_layers and self.recurrent_layers == descriptor.recurrent_layers and
            self.key.len != 0 and std.mem.eql(u64, self.key, descriptor.key) and
            std.mem.eql(u64, self.sequences[0..self.sequence_count], descriptor.sequences) and
            descriptor.positions.len == 3 * self.rows and
            std.mem.eql(u32, self.next_positions[0..descriptor.positions.len], descriptor.positions);
    }
    fn installStorageGeometry(self: *Module, layout: StorageLayout, rows: usize, hidden: usize, vocab: usize, positions_: []const u32) void {
        self.rows = rows;
        self.hidden = hidden;
        self.vocab = vocab;
        self.storage_layout = layout;
        @memcpy(self.base_positions[0..positions_.len], positions_);
        @memcpy(self.next_positions[0..positions_.len], positions_);
        self.phase = .warm;
    }

    /// The warmed path may prove that every temporary used a declared span.
    /// Discard any partial trace so compatibility learning starts from scratch.
    pub fn finishDeclaredWarmup(plan: *plan_mod.Plan, matches_dispatch: bool) !bool {
        if (matches_dispatch and plan.valid and plan.count == 0 and plan.event_count == 0) {
            try plan.declare(&.{}, null);
            return true;
        }
        plan.* = .{};
        return false;
    }

    pub fn arenaLayout(self: *const Module) !plan_mod.Layout {
        const plan = self.plan orelse return error.InvalidState;
        const layout = try plan.layout(0);
        if (layout.output != null) return error.InvalidExecutionPlan;
        return layout;
    }

    pub fn resetPrepared(self: *Module, ctx: *context.CudaContext, phase: *GraphPhase) !void {
        const descriptor = phase.descriptor;
        const layout = phase.storage_layout;
        const dispatch = try descriptor.dispatchCounts();
        if (!phase.bound or phase.mode != .storage or self.metadata.ptr != 0 or self.head.ptr != 0 or self.arena.ptr != 0 or self.graph != null or self.pending_graph != null or self.plan != null or self.key.len != 0) return error.InvalidPreparedCacheState;
        const cold = phase.cold orelse return error.InvalidPreparedCacheState;
        if (cold.plan == null or !std.mem.eql(u64, cold.key, descriptor.key)) return error.PreparedCachePlanChanged;
        const request = descriptor.storageRequestFor(layout);
        var lease = try cold.funding.?.take(request, try request.captured_storage.amounts());
        errdefer lease.releaseAfterSdkDeath();
        var metadata = try buffer.DeviceBuffer.alloc(ctx, layout.metadata_bytes);
        errdefer _ = metadata.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        var head = buffer.DeviceBuffer{};
        errdefer _ = head.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (layout.head_bytes != 0) head = try buffer.DeviceBuffer.alloc(ctx, layout.head_bytes);
        self.metadata = metadata;
        self.head = head;
        self.metadata_quota = lease.publishQuota(); // SAME preallocated cold shell
        self.host_allocator = cold.allocator;
        self.key = cold.key;
        self.sequence_count = descriptor.sequences.len;
        @memcpy(self.sequences[0..self.sequence_count], descriptor.sequences);
        cold.key = &.{};
        self.plan = cold.plan;
        cold.plan = null;
        self.installStorageGeometry(layout, descriptor.rows, descriptor.hidden, descriptor.vocab, descriptor.positions);
        self.epoch_id = descriptor.epoch;
        self.attention_layers = descriptor.attention_layers;
        self.recurrent_layers = descriptor.recurrent_layers;
        self.attention_scratch = phase.attention_scratch;
        self.vendor_workspace = phase.vendor_workspace;
        self.expected_attention_calls = dispatch.attention;
        self.expected_attention_gates = dispatch.attention_gates;
        self.expected_ragged_packets = dispatch.ragged_recurrent;
    }
    pub fn reserveArenaPrepared(self: *Module, ctx: *context.CudaContext, phase: *GraphPhase) !void {
        if (self.admitted_arena_layout != null) {
            _ = try self.bindArena();
            return;
        }
        const layout = try self.arenaLayout();
        if (layout.bytes == 0) {
            // The reader's ready decision stays under the same owner lanes
            // and carries no outside funding to bind.
            if (phase.bound or phase.mode != .none or phase.cold != null) return error.InvalidPreparedCacheState;
            if (!std.meta.eql(self.snapshot(), phase.old)) return error.PreparedCachePlanChanged;
            self.admitted_arena_layout = layout;
            return;
        }
        if (!phase.bound or phase.mode != .arena) return error.InvalidPreparedCacheState;
        const admitted = phase.old.arena_layout orelse return error.InvalidPreparedCacheState;
        if (!std.meta.eql(layout, admitted)) return error.PreparedCachePlanChanged;
        const request = phase.arenaRequest(admitted);
        var lease = try phase.cold.?.funding.?.take(request, request.captured_arena.amounts());
        errdefer lease.releaseAfterSdkDeath();
        const arena = try buffer.DeviceBuffer.alloc(ctx, layout.bytes);
        self.arena = arena;
        self.admitted_arena_layout = layout;
        self.arena_quota = lease.publishQuota();
    }

    pub fn bindArena(self: *const Module) !plan_mod.Scope {
        const plan = self.plan orelse return error.InvalidExecutionPlan;
        const layout = self.admitted_arena_layout orelse return error.InvalidExecutionPlan;
        return plan_mod.Scope.bind(plan, layout, self.arena.ptr, self.arena.len);
    }

    pub fn bindExecutionStorage(self: *const Module) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        try buffer.validateRegions(
            3,
            .{ self.metadata, self.head, self.arena },
            .{ layout.metadata_bytes, layout.head_bytes, self.arena.len },
            .{ plan_mod.alignment, @alignOf(f32), plan_mod.alignment },
            1,
        );
        if (layout.shared_head_workspace) |span| return try self.bindLayerWorkspace(layout, span, span.bytes);
        if (layout.head_bytes == 0) return null;
        var destination = self.head;
        destination.len = layout.head_bytes;
        return destination;
    }

    /// Decode packets submit in order on one stream. The split and merge
    /// finish consuming partials before the next packet can reuse this span.
    pub fn bindAttentionWorkspace(self: *const Module, bytes: usize) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.attention_workspace orelse return error.InvalidExecutionPlan;
        return self.bindLayerWorkspace(layout, span, bytes);
    }

    pub fn bindRecurrentWorkspace(self: *Module, scratch: @import("gated_delta.zig").PacketScratch) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.recurrent_workspace orelse return error.InvalidExecutionPlan;
        if (!std.meta.eql(layout.recurrent_scratch orelse return error.InvalidExecutionPlan, scratch)) return error.InvalidExecutionPlan;
        const bound = try self.bindLayerWorkspace(layout, span, scratch.bytes);
        self.bound_recurrent_scratch_bytes = bound.len;
        return bound;
    }

    pub const LayerOutput = enum { attention, recurrent };

    pub fn bindLayerOutput(self: *const Module, kind: LayerOutput, rows: usize, width: usize) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = (switch (kind) {
            .attention => layout.attention_output,
            .recurrent => layout.recurrent_output,
        }) orelse return error.InvalidExecutionPlan;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32));
        if (rows != self.rows or bytes != span.bytes) return error.InvalidExecutionPlan;
        const workspace = switch (kind) {
            .attention => layout.attention_workspace,
            .recurrent => layout.recurrent_workspace,
        };
        if (workspace) |scratch| {
            if (span.offset < try std.math.add(usize, scratch.offset, scratch.bytes)) return error.InvalidExecutionPlan;
        }
        return self.bindLayerWorkspace(layout, span, bytes);
    }

    pub fn bindFfnScratch(self: *const Module, ffn: @import("dense_ffn_workspace.zig").Layout) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.ffn_workspace orelse return error.InvalidExecutionPlan;
        if (ffn.rows != self.rows or ffn.hidden != self.hidden or ffn.vendor_bytes != 0 or ffn.scratch_bytes != span.bytes) return error.InvalidExecutionPlan;
        return try self.bindLayerWorkspace(layout, span, ffn.scratch_bytes);
    }

    pub fn bindFfnOutput(self: *Module, ffn: @import("dense_ffn_workspace.zig").Layout, sources: []const buffer.DeviceBuffer) !buffer.DeviceBuffer {
        if (ffn.rows != self.rows or ffn.hidden != self.hidden) return error.InvalidExecutionPlan;
        return try self.bindLayerResidualOutput(ffn.output_bytes, sources);
    }

    pub fn bindLayerResidualOutput(self: *Module, bytes: usize, sources: []const buffer.DeviceBuffer) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.ffn_outputs orelse return error.InvalidExecutionPlan;
        if (bytes != span.bytes / 2) return error.InvalidExecutionPlan;
        const output_bank = try self.bindLayerWorkspace(layout, span, span.bytes);
        const slot = try output_bank.borrowRange(@as(usize, self.next_ffn_output) * bytes, bytes);
        const end = try std.math.add(u64, slot.ptr, slot.len);
        for (sources) |source| {
            const source_end = try std.math.add(u64, source.ptr, source.len);
            if (slot.ptr < source_end and source.ptr < end) return error.InvalidExecutionPlan;
        }
        self.next_ffn_output ^= 1;
        return slot;
    }

    pub fn bindProjectionStaging(self: *const Module, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.projection_staging orelse return null;
        return try self.bindLayerWorkspace(layout, span, bytes);
    }

    pub fn bindFusedHeadWorkspace(self: *const Module) !buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.fused_head_workspace orelse return error.InvalidExecutionPlan;
        return self.bindLayerWorkspace(layout, span, span.bytes);
    }

    pub const AttentionProjection = enum { query, key, value };

    pub fn bindAttentionProjection(self: *const Module, kind: AttentionProjection, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        return self.bindInputProjection(layout, &layout.attention_projection_bytes, @backingInt(kind), bytes);
    }

    pub fn bindJoinedRecurrentProjection(self: *const Module, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.recurrent_projection_joined orelse return null;
        if (bytes != span.bytes) return error.InvalidExecutionPlan;
        return try self.bindLayerWorkspace(layout, span, bytes);
    }

    pub const RecurrentProjection = enum { mixed, gate, beta, decay };

    pub fn bindRecurrentProjection(self: *const Module, kind: RecurrentProjection, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        return self.bindInputProjection(layout, &layout.recurrent_projection_bytes, @backingInt(kind), bytes);
    }

    fn bindInputProjection(self: *const Module, layout: StorageLayout, sizes: []const usize, index: usize, bytes: usize) !?buffer.DeviceBuffer {
        const span = layout.input_projections orelse return null;
        if (sizes[index] == 0) return null;
        if (sizes[index] != bytes) return error.InvalidExecutionPlan;
        var offset: usize = 0;
        for (sizes[0..index]) |size| offset = try std.math.add(usize, offset, size);
        const backing = try self.bindLayerWorkspace(layout, span, span.bytes);
        return try backing.borrowRange(offset, bytes);
    }

    pub fn bindSplitQueryGate(self: *const Module, bytes: usize) !?[2]buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.attention_inputs orelse return null;
        if (layout.attention_projection_bytes[0] != try std.math.mul(usize, bytes, 2)) return error.InvalidExecutionPlan;
        const key_bytes = layout.attention_projection_bytes[1];
        const backing = try self.bindLayerWorkspace(layout, span, span.bytes);
        return .{
            try backing.borrowRange(0, bytes),
            try backing.borrowRange(try std.math.add(usize, bytes, key_bytes), bytes),
        };
    }

    pub fn bindAttentionInputs(self: *const Module, inputs: AttentionInputs) !?[3]buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.attention_inputs orelse return null;
        if (span.bytes != inputs.bytes) return error.InvalidExecutionPlan;
        return try inputs.bind(try self.bindLayerWorkspace(layout, span, inputs.bytes));
    }

    pub fn bindAttentionResidual(self: *const Module, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.attention_residual orelse return null;
        if (span.bytes != bytes) return error.InvalidExecutionPlan;
        return try self.bindLayerWorkspace(layout, span, bytes);
    }

    pub const NormStorage = struct { output: buffer.DeviceBuffer, mirror: ?buffer.DeviceBuffer };

    pub fn bindLayerNorm(self: *const Module, bytes: usize) !?NormStorage {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const output = layout.layer_norm orelse return null;
        if (output.bytes != bytes) return error.InvalidExecutionPlan;
        const mirror = if (layout.layer_norm_mirror) |span| blk: {
            if (span.bytes != bytes / 2) return error.InvalidExecutionPlan;
            break :blk try self.bindLayerWorkspace(layout, span, span.bytes);
        } else null;
        return .{ .output = try self.bindLayerWorkspace(layout, output, bytes), .mirror = mirror };
    }

    pub const NormWeight = enum { hidden, query, key };

    pub fn bindNormWeight(self: *const Module, kind: NormWeight, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.norm_weight_workspace orelse return null;
        const expected = if (kind == .hidden) try std.math.mul(usize, self.hidden, @sizeOf(f32)) else layout.norm_weight_head_bytes;
        if (bytes == 0 or bytes != expected) return error.InvalidExecutionPlan;
        const backing = try self.bindLayerWorkspace(layout, span, span.bytes);
        return try backing.borrowRange(if (kind == .key) expected else 0, bytes);
    }

    pub fn bindFinalNormMirror(self: *const Module, bytes: usize) !?buffer.DeviceBuffer {
        const layout = self.storage_layout orelse return error.InvalidExecutionPlan;
        const span = layout.final_norm_mirror orelse return null;
        if (span.bytes != bytes) return error.InvalidExecutionPlan;
        return try self.bindLayerWorkspace(layout, span, bytes);
    }

    fn bindLayerWorkspace(self: *const Module, layout: StorageLayout, span: StorageLayout.Span, bytes: usize) !buffer.DeviceBuffer {
        const prefix_end = layout.counter.offset + layout.counter.bytes;
        if (bytes == 0 or bytes > span.bytes or span.offset < prefix_end or
            span.offset % plan_mod.alignment != 0 or span.offset > layout.metadata_bytes or
            span.bytes > layout.metadata_bytes - span.offset) return error.InvalidExecutionPlan;
        _ = buffer.validateRegion(self.metadata, layout.metadata_bytes, plan_mod.alignment) catch return error.InvalidExecutionPlan;
        return self.metadata.borrowRange(span.offset, bytes) catch return error.InvalidExecutionPlan;
    }

    pub fn input(self: *const Module) buffer.DeviceBuffer {
        return self.storage_layout.?.input.bind(self.metadata);
    }
    pub fn bank(self: *const Module) buffer.DeviceBuffer {
        return self.storage_layout.?.tokens.bind(self.metadata);
    }
    pub fn positions(self: *const Module) buffer.DeviceBuffer {
        return self.storage_layout.?.positions.bind(self.metadata);
    }
    pub fn counter(self: *const Module) buffer.DeviceBuffer {
        return self.storage_layout.?.counter.bind(self.metadata);
    }
};

test "bounded CUDA decoder funds one stable attention workspace for both packets" {
    const base = try StorageLayout.init(67, 1024, 151936, 1);
    var layout = base;
    const geometry: DecodeScratch.Geometry = .{ .heads = 8, .kv_heads = 2, .dim = 256 };
    const scratch = try DecodeScratch.init(67, 321, geometry, false);
    try layout.declareAttentionWorkspace(scratch);
    const req: Descriptor = .{ .epoch = 1, .key = &.{1}, .positions = &.{}, .rows = 67, .hidden = 1024, .vocab = 151936, .future_tokens = 321, .attention_layers = 1, .recurrent_layers = 1 };
    const amounts = try req.storageRequestFor(layout).captured_storage.amounts();
    try std.testing.expectEqual(base.metadata_bytes + scratch.full.bytes + base.head_bytes, amounts.backend_scratch_bytes);
    var owner: Module = .{ .storage_layout = layout, .metadata = .{ .ptr = 0x10000, .len = layout.metadata_bytes } };
    const full = try owner.bindAttentionWorkspace(scratch.full.bytes);
    const tail = try owner.bindAttentionWorkspace(scratch.tail.?.bytes);
    try std.testing.expectEqual(owner.metadata.ptr + base.metadata_bytes, full.ptr);
    try std.testing.expectEqual(full.ptr, tail.ptr);
    try std.testing.expectEqual(base.readback(owner.metadata), layout.readback(owner.metadata));
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindAttentionWorkspace(full.len + 1));
    owner.metadata.len -= 1;
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindAttentionWorkspace(tail.len));
    owner.metadata.len = layout.metadata_bytes;
    owner.storage_layout.?.attention_workspace.?.offset = 0;
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindAttentionWorkspace(tail.len));
    owner.storage_layout = layout;
    owner.metadata.ptr += 1;
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindAttentionWorkspace(tail.len));
}

test "bounded CUDA decoder shares declared attention and recurrent workspace" {
    const rows = 67;
    const positions: [rows * 3]u32 = @splat(0);
    const descriptor: Descriptor = .{
        .epoch = 1,
        .key = &.{1},
        .positions = &positions,
        .rows = rows,
        .hidden = 2560,
        .vocab = 248320,
        .future_tokens = 321,
        .attention_geometry = .{ .heads = 8, .kv_heads = 2, .dim = 256 },
        .recurrent_heads = .{ .key = 16, .value = 32 },
        .attention_layers = 1,
        .recurrent_layers = 1,
        .ffn_intermediate = 9216,
    };
    const base = try descriptor.layout();
    var phase: GraphPhase = undefined;
    try phase.initInto(descriptor);
    try phase.declareRecurrentScratch();
    try phase.declareAttentionScratch(false);
    const layout = phase.storage_layout;
    const attention = layout.attention_workspace.?;
    const recurrent = layout.recurrent_workspace.?;
    try std.testing.expectEqual(base.metadata_bytes, attention.offset);
    try std.testing.expectEqual(attention.offset, recurrent.offset);
    try std.testing.expectEqual(base.metadata_bytes + @max(attention.bytes, recurrent.bytes), layout.metadata_bytes);
    try std.testing.expectEqual(@max(attention.bytes, recurrent.bytes), layout.layerWorkspaceBytes());
    try std.testing.expectEqual(@as(usize, 0), base.layerWorkspaceBytes());
    var owner: Module = .{ .storage_layout = layout, .metadata = .{ .ptr = 0x10000, .len = layout.metadata_bytes } };
    const scratch = layout.recurrent_scratch.?;
    const recurrent_module = @import("gated_delta.zig");
    try std.testing.expectEqualDeep(try recurrent_module.PacketScratch.forHeads(recurrent_module.max_packet_rows, 16, 32), scratch);
    try std.testing.expect(scratch.bytes < (try recurrent_module.PacketScratch.forHeads(rows, 16, 32)).bytes);
    try std.testing.expectEqual((try owner.bindAttentionWorkspace(attention.bytes)).ptr, (try owner.bindRecurrentWorkspace(scratch)).ptr);
    try std.testing.expectEqual(scratch.bytes, owner.bound_recurrent_scratch_bytes);
    var changed = scratch;
    changed.core_offset -= @sizeOf(f32);
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindRecurrentWorkspace(changed));

    try phase.declareRecurrentOutput();
    const with_output = phase.storage_layout;
    const output = with_output.recurrent_output.?;
    var output_owner: Module = .{
        .rows = rows,
        .storage_layout = with_output,
        .metadata = .{ .ptr = 0x20000, .len = with_output.metadata_bytes },
    };
    const destination = try output_owner.bindLayerOutput(.recurrent, rows, 32 * recurrent_value_dim);
    try std.testing.expectEqual(@as(usize, rows * 32 * recurrent_value_dim * @sizeOf(f32)), destination.len);
    try std.testing.expect(output.offset >= recurrent.offset + recurrent.bytes);
    try std.testing.expectEqual(base.metadata_bytes + @max(attention.bytes, output.offset - base.metadata_bytes + output.bytes), with_output.metadata_bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindLayerOutput(.recurrent, rows - 1, 32 * recurrent_value_dim));
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindLayerOutput(.recurrent, rows, 16 * recurrent_value_dim));
    output_owner.storage_layout.?.recurrent_output.?.offset = recurrent.offset;
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindLayerOutput(.recurrent, rows, 32 * recurrent_value_dim));

    try phase.declareAttentionOutput();
    output_owner.storage_layout = phase.storage_layout;
    output_owner.metadata.len = phase.storage_layout.metadata_bytes;
    const attention_output = try output_owner.bindLayerOutput(.attention, rows, 8 * 256);
    try std.testing.expectEqual(@as(usize, rows * 8 * 256 * @sizeOf(f32)), attention_output.len);
    try std.testing.expect(attention_output.ptr >= output_owner.metadata.ptr + attention.offset + attention.bytes);

    try phase.declareFfnScratch(false);
    output_owner.hidden = descriptor.hidden;
    output_owner.storage_layout = phase.storage_layout;
    output_owner.metadata.len = phase.storage_layout.metadata_bytes;
    const Ffn = @import("dense_ffn_workspace.zig").Layout;
    const ffn = try Ffn.init(rows, descriptor.hidden, descriptor.ffn_intermediate, 0);
    try std.testing.expectEqual(output_owner.metadata.ptr + attention.offset, (try output_owner.bindFfnScratch(ffn)).ptr);
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindFfnScratch(try Ffn.init(rows, descriptor.hidden, descriptor.ffn_intermediate + 128, 0)));

    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindFfnScratch(try Ffn.initCompact(rows, descriptor.hidden, descriptor.ffn_intermediate)));
    var missing_ffn = output_owner;
    missing_ffn.storage_layout.?.ffn_workspace = null;
    try std.testing.expectError(error.InvalidExecutionPlan, missing_ffn.bindFfnScratch(ffn));

    const first_output = try output_owner.bindFfnOutput(ffn, &.{});
    const second_output = try output_owner.bindFfnOutput(ffn, &.{first_output});
    try std.testing.expectEqual(first_output.ptr + first_output.len, second_output.ptr);
    try std.testing.expect(first_output.ptr >= output_owner.metadata.ptr + attention.offset + attention.bytes);
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindFfnOutput(ffn, &.{first_output}));
    try std.testing.expectEqual(first_output.ptr, (try output_owner.bindFfnOutput(ffn, &.{second_output})).ptr);

    try phase.declareProjectionStaging();
    output_owner.storage_layout = phase.storage_layout;
    output_owner.metadata.len = phase.storage_layout.metadata_bytes;
    const staging_bytes = rows * descriptor.ffn_intermediate * @sizeOf(u16);
    const staging = (try output_owner.bindProjectionStaging(staging_bytes)).?;
    try std.testing.expect(staging.ptr >= second_output.ptr + second_output.len);
    try std.testing.expectError(error.InvalidExecutionPlan, output_owner.bindProjectionStaging(staging_bytes + 1));

    const policy: GraphPhase.WorkspacePolicy = .{
        .attention_chunk128 = false,
        .recurrent_scratch = true,
        .compact_ffn = false,
        .projection_staging = true,
    };
    const composed = phase.storage_layout;
    try phase.prepareWorkspace(policy);
    try std.testing.expectEqualDeep(composed, phase.storage_layout);
    try phase.prepareWorkspace(policy);
    try std.testing.expectEqualDeep(composed, phase.storage_layout);
    try phase.prepareWorkspace(.{ .recurrent_scratch = false, .compact_ffn = null, .projection_staging = false });
    try std.testing.expect(phase.storage_layout.recurrent_workspace == null and phase.storage_layout.ffn_outputs == null and phase.storage_layout.projection_staging == null);

    var shared = descriptor;
    shared.head_storage = .{ .shared_layer_packets = 16 };
    try phase.initInto(shared);
    try phase.prepareWorkspace(policy);
    const shared_layout = phase.storage_layout;
    const shared_head = shared_layout.shared_head_workspace.?;
    try std.testing.expectEqualDeep(composed.ffn_outputs, shared_layout.ffn_outputs);
    const shared_staging = shared_layout.projection_staging.?;
    try std.testing.expectEqual(composed.projection_staging.?.bytes, shared_staging.bytes);
    try std.testing.expect(shared_staging.offset >= shared_head.offset + shared_head.bytes);
    try std.testing.expectEqual(shared_staging.offset + shared_staging.bytes, shared_layout.metadata_bytes);

    var mirror_policy = policy;
    mirror_policy.final_norm_mirror = true;
    try phase.prepareWorkspace(mirror_policy);
    const mirror_layout = phase.storage_layout;
    const mirror = mirror_layout.final_norm_mirror.?;
    try std.testing.expectEqual(rows * descriptor.hidden * @sizeOf(u16), mirror.bytes);
    try std.testing.expect(mirror.offset >= shared_head.offset + shared_head.bytes);
    try std.testing.expect(mirror.offset + mirror.bytes <= shared_staging.offset or mirror.offset >= shared_staging.offset + shared_staging.bytes);
    var mirror_owner = output_owner;
    mirror_owner.storage_layout = mirror_layout;
    mirror_owner.metadata.len = mirror_layout.metadata_bytes;
    try std.testing.expectEqual(mirror_owner.metadata.ptr + mirror.offset, (try mirror_owner.bindFinalNormMirror(mirror.bytes)).?.ptr);
    try std.testing.expectError(error.InvalidExecutionPlan, mirror_owner.bindFinalNormMirror(mirror.bytes - 2));

    try phase.initInto(descriptor);
    try phase.prepareWorkspace(mirror_policy);
    const reused = phase.storage_layout;
    try std.testing.expectEqual(reused.attention_workspace.?.offset, reused.final_norm_mirror.?.offset);
    try std.testing.expectEqual(composed.metadata_bytes, reused.metadata_bytes);

    for ([_]bool{ false, true }) |gated| {
        var projected = descriptor;
        projected.attention_q_gate = gated;
        const recurrent_widths: [4]usize = .{ 8192, 4096, 32, 32 };
        projected.recurrent_projection_widths = recurrent_widths;
        try phase.initInto(projected);
        var projection_policy = mirror_policy;
        projection_policy.layer_norm = true;
        projection_policy.fused_recurrent_projections = true;
        projection_policy.adjust_norm_weights = true;
        try phase.prepareWorkspace(projection_policy);
        const projection_layout = phase.storage_layout;
        try std.testing.expectEqual(gated, projection_layout.supportsDeclaredWarmup(true));
        try std.testing.expect(!projection_layout.supportsDeclaredWarmup(false));
        const projection_owner: Module = .{
            .storage_layout = projection_layout,
            .hidden = projected.hidden,
            .metadata = .{ .ptr = 0x20000, .len = projection_layout.metadata_bytes },
        };
        const q_bytes = attention_output.len * @as(usize, if (gated) 2 else 1);
        const kv_bytes = rows * 2 * 256 * @sizeOf(f32);
        const q_dst = (try projection_owner.bindAttentionProjection(.query, q_bytes)).?;
        const k_dst = (try projection_owner.bindAttentionProjection(.key, kv_bytes)).?;
        const v_dst = (try projection_owner.bindAttentionProjection(.value, kv_bytes)).?;
        try std.testing.expectEqual(q_dst.ptr + q_dst.len, k_dst.ptr);
        try std.testing.expectEqual(k_dst.ptr + k_dst.len, v_dst.ptr);
        try std.testing.expect(v_dst.ptr + v_dst.len <= projection_owner.metadata.ptr + projection_layout.projection_staging.?.offset);
        try std.testing.expectError(error.InvalidExecutionPlan, projection_owner.bindAttentionProjection(.query, q_bytes - @sizeOf(f32)));
        const head_norm_bytes = 256 * @sizeOf(f32);
        const hidden_norm = (try projection_owner.bindNormWeight(.hidden, projected.hidden * @sizeOf(f32))).?;
        const query_norm = (try projection_owner.bindNormWeight(.query, head_norm_bytes)).?;
        const key_norm = (try projection_owner.bindNormWeight(.key, head_norm_bytes)).?;
        try std.testing.expectEqual(hidden_norm.ptr, query_norm.ptr);
        try std.testing.expectEqual(query_norm.ptr + query_norm.len, key_norm.ptr);
        try std.testing.expect(projection_owner.metadata.ptr + projection_layout.final_norm_mirror.?.offset >= hidden_norm.ptr + hidden_norm.len);
        try std.testing.expectError(error.InvalidExecutionPlan, projection_owner.bindNormWeight(.key, head_norm_bytes - 4));
        const joined_bytes = rows * (recurrent_widths[0] + recurrent_widths[1] + recurrent_widths[2] + recurrent_widths[3]) * @sizeOf(f32);
        const joined = (try projection_owner.bindJoinedRecurrentProjection(joined_bytes)).?;
        try std.testing.expectEqual(projection_owner.metadata.ptr + projection_layout.attention_workspace.?.offset, joined.ptr);
        try std.testing.expect(joined.ptr + joined.len <= projection_owner.metadata.ptr + projection_layout.ffn_outputs.?.offset);
        try std.testing.expect(joined.ptr + joined.len <= q_dst.ptr);
        try std.testing.expectError(error.InvalidExecutionPlan, projection_owner.bindJoinedRecurrentProjection(joined_bytes - 4));
        var next_ptr = q_dst.ptr;
        for (recurrent_widths, 0..) |width, index| {
            const bytes = rows * width * @sizeOf(f32);
            const slot = (try projection_owner.bindRecurrentProjection(@fromBackingInt(@intCast(index)), bytes)).?;
            try std.testing.expectEqual(next_ptr, slot.ptr);
            try std.testing.expectEqual(bytes, slot.len);
            next_ptr += slot.len;
        }
        try std.testing.expect(next_ptr <= projection_owner.metadata.ptr + projection_layout.projection_staging.?.offset);
        try std.testing.expectError(error.InvalidExecutionPlan, projection_owner.bindRecurrentProjection(.gate, 4));
        if (gated) {
            const split = (try projection_owner.bindSplitQueryGate(q_bytes / 2)).?;
            try std.testing.expectEqual(q_bytes / 2, split[0].len);
            try std.testing.expectEqual(q_bytes / 2, split[1].len);
            try std.testing.expect(split[1].ptr + split[1].len <= q_dst.ptr);
        } else {
            try std.testing.expect((try projection_owner.bindSplitQueryGate(q_bytes)) == null);
        }
    }

    var invalid = descriptor;
    invalid.recurrent_projection_widths = .{ 8192, 4096, 0, 32 };
    try phase.initInto(invalid);
    var invalid_policy = mirror_policy;
    invalid_policy.layer_norm = true;
    invalid_policy.fused_recurrent_projections = true;
    try std.testing.expectError(error.InvalidShape, phase.prepareWorkspace(invalid_policy));
    try phase.initInto(descriptor);
    try std.testing.expectError(error.InvalidShape, phase.prepareWorkspace(invalid_policy));

    var fused = descriptor;
    fused.hidden = 1024;
    fused.head_storage = .fused_top1;
    try phase.initInto(fused);
    try phase.declareRecurrentScratch();
    try phase.declareAttentionScratch(false);
    const fused_layout = phase.storage_layout;
    const head = fused_layout.fused_head_workspace.?;
    try std.testing.expectEqual(@as(usize, 0), fused_layout.head_bytes);
    try std.testing.expectEqual(fused_layout.attention_workspace.?.offset, head.offset);
    try std.testing.expectEqual(fused_layout.recurrent_workspace.?.offset, head.offset);
    try std.testing.expectEqual(head.offset + @max(head.bytes, attention.bytes, recurrent.bytes), fused_layout.metadata_bytes);
    owner.storage_layout = fused_layout;
    owner.metadata.len = fused_layout.metadata_bytes;
    try std.testing.expectEqual(owner.metadata.ptr + head.offset, (try owner.bindFusedHeadWorkspace()).ptr);
}

test "bounded CUDA decoder seals an empty declaration only after a complete warm burst" {
    var plan: plan_mod.Plan = .{};
    plan.allocate(256, 0x10000);
    try std.testing.expect(!try Module.finishDeclaredWarmup(&plan, true));
    try std.testing.expectEqual(@as(usize, 0), plan.count);
    try std.testing.expectEqual(plan_mod.Origin.observed, plan.origin);
    try std.testing.expect(!try Module.finishDeclaredWarmup(&plan, false));
    try std.testing.expect(try Module.finishDeclaredWarmup(&plan, true));
    const declared = try plan.layout(0);
    try std.testing.expectEqual(plan_mod.Origin.declared, plan.origin);
    try std.testing.expectEqual(@as(usize, 0), declared.bytes);
    var scope = try plan_mod.Scope.bind(&plan, declared, 0, 0);
    try std.testing.expect(scope.complete());
    try std.testing.expectError(error.InvalidExecutionPlan, scope.allocate(4));
}

test "captured graph head packets fund the exact smaller bank and invalidate a full-width binding" {
    var key = [_]u64{1};
    const positions: [97 * 3]u32 = @splat(0);
    const descriptor = Descriptor{ .epoch = 1, .key = &key, .positions = &positions, .rows = 97, .hidden = 2560, .vocab = 248320, .future_tokens = 128, .attention_layers = 0, .recurrent_layers = 1 };
    const full = try descriptor.layout();
    var owner: Module = .{};
    owner.installStorageGeometry(full, descriptor.rows, descriptor.hidden, descriptor.vocab, descriptor.positions);
    owner.epoch_id = descriptor.epoch;
    owner.key = &key;
    owner.recurrent_layers = descriptor.recurrent_layers;
    var phase: GraphPhase = undefined;
    try phase.initInto(descriptor);
    try std.testing.expect(phase.matchesOwner(&owner));

    var packet = descriptor;
    packet.head_storage = .{ .adaptive_bank = 32 };
    try phase.initInto(packet);
    const layout = phase.storage_layout;
    const request = packet.storageRequestFor(layout);
    try std.testing.expectEqual(@as(usize, 32 * 248320 * 4), layout.head_bytes);
    try std.testing.expectEqual(full.metadata_bytes, layout.metadata_bytes);
    try std.testing.expectEqual(layout.metadata_bytes + layout.head_bytes, (try request.captured_storage.amounts()).backend_scratch_bytes);
    try std.testing.expect(!phase.matchesOwner(&owner));
    owner.storage_layout = layout;
    try std.testing.expect(phase.matchesOwner(&owner));

    phase.mode = .storage;
    phase.admission_pressure = .live_host;
    try std.testing.expect(!phase.reduceHeadPacketAfterDenial());
    phase.admission_pressure = .{ .domain_scratch = .gpu };
    for ([_]usize{ 16, 8 }) |packet_rows| {
        try std.testing.expect(phase.reduceHeadPacketAfterDenial());
        try std.testing.expectEqual(packet_rows * packet.vocab * 4, phase.storage_layout.head_bytes);
        try std.testing.expectEqual(packet_rows, phase.storage_layout.headPacketRows(packet.rows, packet.vocab));
        try std.testing.expectEqual(layout.metadata_bytes, phase.storage_layout.metadata_bytes);
    }
    try std.testing.expect(!phase.reduceHeadPacketAfterDenial());
    owner.storage_layout = phase.storage_layout;
    try phase.initInto(packet);
    try std.testing.expect(phase.matchesOwner(&owner));
    try phase.initInto(descriptor);
    try std.testing.expect(!phase.matchesOwner(&owner));

    var shared = packet;
    shared.head_storage = .{ .shared_layer_packets = 32 };
    try phase.initInto(shared);
    try std.testing.expectEqual(@as(usize, 32 * 248320 * 4), phase.storage_layout.shared_head_workspace.?.bytes);
    try std.testing.expectEqual(@as(usize, 32), phase.storage_layout.headPacketRows(shared.rows, shared.vocab));
    try std.testing.expectEqual(@as(usize, 0), phase.storage_layout.head_bytes);
    const shared_request = shared.storageRequestFor(phase.storage_layout);
    try std.testing.expectEqual(layout.metadata_bytes + 32 * 248320 * 4, (try shared_request.captured_storage.amounts()).backend_scratch_bytes);
    const shared_layout = phase.storage_layout;
    const head_span = shared_layout.shared_head_workspace.?;
    const shared_owner: Module = .{ .storage_layout = shared_layout, .metadata = .{ .ptr = 0x10000, .len = shared_layout.metadata_bytes } };
    const bound_head = (try shared_owner.bindExecutionStorage()).?;
    try std.testing.expectEqual(shared_owner.metadata.ptr + head_span.offset, bound_head.ptr);
    try std.testing.expectEqual(head_span.bytes, bound_head.len);
    shared.rows = 2;
    shared.positions = positions[0..6];
    const narrow = try shared.layout();
    try std.testing.expectEqual(@as(usize, 2 * 248320 * 4), narrow.shared_head_workspace.?.bytes);
    try std.testing.expectEqual(@as(usize, 2), narrow.headPacketRows(2, shared.vocab));
    try std.testing.expect(!phase.matchesOwner(&owner));

    packet.vocab = 400_000;
    try std.testing.expectEqual(@as(usize, 32 * 400_000 * 4), (try packet.layout()).head_bytes);
    packet.head_storage = .full_bank;
    try std.testing.expectError(error.InvalidShape, packet.layout());
}

test "captured sequence storage layout packs narrow and wide readbacks with aligned physical planes" {
    const cases = [_]struct { rows: usize, hidden: usize, vocab: usize, bank: usize, positions: usize, progress: usize, bytes: usize, head: usize }{
        .{ .rows = 2, .hidden = 1024, .vocab = 8, .bank = 8192, .positions = 8208, .progress = 8232, .bytes = 8448, .head = 0 },
        .{ .rows = 3, .hidden = 1024, .vocab = 8, .bank = 12288, .positions = 12320, .progress = 12356, .bytes = 12544, .head = 0 },
        .{ .rows = 64, .hidden = 1024, .vocab = 151936, .bank = 262144, .positions = 262656, .progress = 263424, .bytes = 263680, .head = 0 },
        .{ .rows = 128, .hidden = 2560, .vocab = 151936, .bank = 1310720, .positions = 1311744, .progress = 1313280, .bytes = 1313536, .head = 77791232 },
    };
    for (cases) |value| {
        const actual = try StorageLayout.init(value.rows, value.hidden, value.vocab, 24576);
        try std.testing.expectEqual(value.bank, actual.tokens.offset);
        try std.testing.expectEqual(value.positions, actual.positions.offset);
        try std.testing.expectEqual(value.progress, actual.counter.offset);
        try std.testing.expectEqual(value.bytes, actual.metadata_bytes);
        try std.testing.expectEqual(value.head, actual.head_bytes);
        const backing: buffer.DeviceBuffer = .{ .ptr = 0x10000, .len = value.bytes };
        var owner = Module{ .storage_layout = actual, .metadata = backing };
        if (value.head == 0) {
            try std.testing.expect((try owner.bindExecutionStorage()) == null);
        } else {
            try std.testing.expectError(error.InvalidShape, owner.bindExecutionStorage());
            owner.head = .{ .ptr = backing.ptr, .len = value.head };
            try std.testing.expectError(error.InvalidShape, owner.bindExecutionStorage());
            owner.head = .{ .ptr = 0x10000000, .len = value.head + 256 };
            try std.testing.expectEqual(value.head, (try owner.bindExecutionStorage()).?.len);
            owner.arena = .{ .ptr = owner.head.ptr, .len = 256 };
            try std.testing.expectError(error.InvalidShape, owner.bindExecutionStorage());
            owner.arena.ptr = owner.head.ptr + value.head;
            try std.testing.expectEqual(value.head, (try owner.bindExecutionStorage()).?.len);
        }
        try std.testing.expectEqual(@as(u64, 0x10000 + value.bank), actual.tokens.bind(backing).ptr);
        try std.testing.expectEqual(value.rows * 2 * @sizeOf(u32), actual.tokens.bytes);
        try std.testing.expectEqual(value.rows * 3 * @sizeOf(u32), actual.positions.bytes);
        try std.testing.expectEqual(value.progress + @sizeOf(u32) - value.bank, actual.readback(backing).len);
        try std.testing.expectEqual((value.positions - value.bank) / @sizeOf(u32), actual.readbackPositionIndex());
        try std.testing.expectEqual((value.progress - value.bank) / @sizeOf(u32), actual.readbackProgressIndex());
        try std.testing.expect(actual.readbackProgressIndex() < StorageLayout.max_readback_words);
        if (value.rows == max_rows) try std.testing.expectEqual(StorageLayout.max_readback_words, actual.readbackProgressIndex() + 1);
        try std.testing.expectEqual(@sizeOf(plan_mod.Plan) + 196608, actual.host_bytes);
    }
    const paired = try StorageLayout.init(2, 1024, 8, 1);
    var receipt: [StorageLayout.max_readback_words]u32 = @splat(0);
    const words = receipt[0 .. paired.readbackProgressIndex() + 1];
    words[0..4].* = .{ 11, 22, 33, 44 };
    words[paired.readbackPositionIndex()..][0..6].* = .{ 3, 4, 5, 6, 7, 8 };
    words[paired.readbackProgressIndex()] = 10;
    var first: [2]u32 = @splat(0);
    var second: [2]u32 = @splat(0);
    var progress: u32 = 8;
    var positions = [_]u32{ 1, 2, 3, 4, 5, 9 };
    try std.testing.expectError(error.InvalidExecutionPlan, paired.publishReadback(words, &positions, &progress, &first, &second));
    try std.testing.expectEqualSlices(u32, &.{ 0, 0 }, &first);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0 }, &second);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3, 4, 5, 9 }, &positions);
    try std.testing.expectEqual(@as(u32, 8), progress);
    positions[5] = 6;
    try paired.publishReadback(words, &positions, &progress, &first, &second);
    try std.testing.expectEqualSlices(u32, &.{ 3, 4, 5, 6, 7, 8 }, &positions);
    try std.testing.expectEqual(@as(u32, 10), progress);
    try std.testing.expectEqualSlices(u32, &.{ 11, 22 }, &first);
    try std.testing.expectEqualSlices(u32, &.{ 33, 44 }, &second);
    try std.testing.expectError(error.InvalidShape, StorageLayout.init(1, 1024, 8, 1));
    try std.testing.expectError(error.Overflow, StorageLayout.init(128, std.math.maxInt(usize), 8, 1));
    try std.testing.expectError(error.Overflow, StorageLayout.init(128, 1024, std.math.maxInt(usize), 1));
    try std.testing.expectError(error.InvalidShape, StorageLayout.init(128, 1024, 262145, 1));
    const malformed = Descriptor{ .epoch = 1, .key = &.{1}, .positions = &.{}, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 1, .attention_layers = 1, .recurrent_layers = 0 };
    try std.testing.expectError(error.InvalidShape, malformed.layout());
}

const GraphTestDriver = struct {
    var next: usize = 0x1000;
    var allocations: usize = 0;
    var frees: usize = 0;
    var exec_destroys: usize = 0;
    var graph_destroys: usize = 0;
    var fail_free: bool = false;
    var fail_exec: bool = false;
    var fail_graph: bool = false;
    var fail_sync: bool = false;
    var capture_active: bool = false;
    fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
        return 0;
    }
    fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
        return if (fail_sync) 700 else 0;
    }
    fn capture(_: driver.CUstream, status: *driver.CUstreamCaptureStatus) callconv(.c) driver.CUresult {
        status.* = if (capture_active) driver.CU_STREAM_CAPTURE_STATUS_ACTIVE else driver.CU_STREAM_CAPTURE_STATUS_NONE;
        return 0;
    }
    fn alloc(out: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
        allocations += 1;
        out.* = next;
        next += 0x1000;
        return 0;
    }
    fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
        if (fail_free) return 700;
        frees += 1;
        return 0;
    }
    fn destroyExec(_: driver.CUgraphExec) callconv(.c) driver.CUresult {
        if (fail_exec) return 700;
        exec_destroys += 1;
        return 0;
    }
    fn destroyGraph(_: driver.CUgraph) callconv(.c) driver.CUresult {
        if (fail_graph) return 700;
        graph_destroys += 1;
        return 0;
    }
    fn errorText(_: driver.CUresult, out: *?[*:0]const u8) callconv(.c) driver.CUresult {
        out.* = "injected graph lifecycle failure";
        return 0;
    }
    fn reset() void {
        next = 0x1000;
        allocations = 0;
        frees = 0;
        exec_destroys = 0;
        graph_destroys = 0;
        fail_free = false;
        fail_exec = false;
        fail_graph = false;
        fail_sync = false;
        capture_active = false;
    }
    fn contextValue() context.CudaContext {
        var ctx: context.CudaContext = undefined;
        ctx.ctx = @ptrFromInt(1);
        ctx.stream = @ptrFromInt(2);
        ctx.stats = .{};
        ctx.observation.store(null, .release);
        ctx.observation_enable_failed.store(false, .release);
        ctx.device_allocations = .{ .limit = 1024 * 1024 };
        ctx.prepared_cache_funding = null;
        ctx.video_cache_budget = .{};
        ctx.driver.fns.cuCtxSetCurrent = current;
        ctx.driver.fns.cuStreamSynchronize = sync;
        ctx.driver.fns.cuStreamIsCapturing = capture;
        ctx.driver.fns.cuMemAlloc = alloc;
        ctx.driver.fns.cuMemFree = free;
        ctx.driver.fns.cuGraphExecDestroy = destroyExec;
        ctx.driver.fns.cuGraphDestroy = destroyGraph;
        ctx.driver.fns.cuGetErrorName = errorText;
        ctx.driver.fns.cuGetErrorString = errorText;
        return ctx;
    }
};

fn qualifyGraphPhaseOom(allocator: std.mem.Allocator) !void {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 3, 5, 7 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    var phase: GraphPhase = undefined;
    try phase.initInto(.{ .epoch = 1, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 });
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    // An early allocation failure on retry must not inherit the prior denial.
    phase.admission_pressure = .{ .domain_scratch = .gpu };
    phase.prepareOutsideUsing(&model, allocator) catch |err| {
        try std.testing.expect(phase.admission_pressure == null);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
        try std.testing.expect(phase.cold == null);
        return err;
    };
    phase.destroyOutside();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph cold process metadata key and plan all OOM unwind exact original quota before SDK" {
    try qualifyGraphPhaseOom(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, qualifyGraphPhaseOom, .{});
}

test "captured graph exact funding precedes host plan allocation and quota final death uses current B" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 3, 5, 7 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    const descriptor = Descriptor{ .epoch = 1, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 };
    const request = descriptor.storageRequestFor(try descriptor.layout());
    const exact = try request.captured_storage.amounts();
    var phase: GraphPhase = undefined;
    try phase.initInto(descriptor);
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{ .backend_limit_bytes = exact.backend_scratch_bytes - 1 }, .check_live_memory = false };
    try std.testing.expectError(error.ResourceLimitExceeded, phase.prepareOutside(&model));
    try std.testing.expect(phase.cold == null);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.allocations);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    phase.admission.?.limits.backend_limit_bytes += 1;
    try phase.prepareOutside(&model);
    defer phase.destroyOutside();
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.allocations);
    var sink = try Sink.Ref.create();
    defer sink.release();
    var first = try sink.enter();
    defer first.finish();
    var a = try first.retainEpoch();
    defer a.release();
    try phase.bind(.{ .boundary = &a });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &phase);
    try phase.close(.{ .boundary = &a });
    first.finish();
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    // Missing/legacy authority is rejected BEFORE graph/buffer SDK mutation.
    const allocations = GraphTestDriver.allocations;
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, owner.clear(&ctx));
    try std.testing.expectEqual(allocations, GraphTestDriver.allocations);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.frees);
    var last = try sink.enter();
    defer last.finish();
    var b = try last.retainEpoch();
    defer b.release();
    try std.testing.expectError(error.RetirementEpochSealed, owner.clearWithCleanup(&ctx, .{ .boundary = &a }));
    const syncs = ctx.stats.stream_syncs;
    GraphTestDriver.capture_active = true;
    try std.testing.expectError(error.CapturedGraphActive, owner.clearWithCleanup(&ctx, .{ .boundary = &b }));
    try std.testing.expectEqual(syncs, ctx.stats.stream_syncs);
    try std.testing.expectEqual(@as(usize, 0), GraphTestDriver.frees);
    GraphTestDriver.capture_active = false;
    try owner.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    last.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph checked SDK death preserves failed exec graph buffer handles and same quota" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const key = [_]u64{ 11, 13 };
    const positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
    var phase: GraphPhase = undefined;
    try phase.initInto(.{ .epoch = 9, .key = &key, .positions = &positions, .rows = 2, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 });
    phase.mode = .storage;
    phase.expected_compute = 1;
    phase.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    try phase.prepareOutside(&model);
    defer phase.destroyOutside();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try phase.bind(.{ .boundary = &epoch });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &phase);
    try phase.close(.{ .boundary = &epoch });
    const before = domain.controller.snapshot();
    const physical = ctx.device_allocations.snapshot();
    const pointer = owner.metadata.ptr;
    owner.graph = @ptrFromInt(0x100);
    owner.pending_graph = @ptrFromInt(0x200);
    @import("../../test_error_logs.zig").expectErrorLogs(3);
    GraphTestDriver.fail_sync = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    GraphTestDriver.fail_sync = false;
    GraphTestDriver.fail_exec = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.graph != null and owner.pending_graph != null and owner.metadata.ptr == pointer);
    GraphTestDriver.fail_exec = false;
    GraphTestDriver.fail_graph = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.graph == null and owner.pending_graph != null and owner.metadata.ptr == pointer);
    GraphTestDriver.fail_graph = false;
    GraphTestDriver.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, owner.clearWithCleanup(&ctx, .{ .boundary = &epoch }));
    try std.testing.expect(owner.metadata.ptr == pointer);
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    try std.testing.expectEqual(physical.live, ctx.device_allocations.snapshot().live);
    GraphTestDriver.fail_free = false;
    try owner.clearWithCleanup(&ctx, .{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.exec_destroys);
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.graph_destroys);
    try std.testing.expectEqual(@as(usize, 1), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    guard.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "captured graph wide head and learned arena consume separate exact prefunded payload scopes" {
    const Domain = @import("../../runtime/tier/admission_domain.zig").Domain;
    const Session = @import("../../backends/session.zig").Session;
    const Sink = @import("../../execution/retirement_sink.zig");
    GraphTestDriver.reset();
    var ctx = GraphTestDriver.contextValue();
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    const Owner = struct {
        fn close(_: *anyopaque) void {}
    };
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Owner.close, .admission_domain = domain.pin() };
    defer model.release();
    const positions: [384]u32 = @splat(1);
    const key = [_]u64{ 17, 23 };
    const descriptor = Descriptor{ .epoch = 42, .key = &key, .positions = &positions, .rows = 128, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 1, .recurrent_layers = 1 };
    const request = descriptor.storageRequestFor(try descriptor.layout());
    const exact = try request.captured_storage.amounts();
    var storage: GraphPhase = undefined;
    try storage.initInto(descriptor);
    storage.mode = .storage;
    storage.expected_compute = 1;
    storage.admission = .{ .controller = &domain.controller, .limits = .{}, .check_live_memory = false };
    try storage.prepareOutside(&model);
    defer storage.destroyOutside();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var initial = try sink.enter();
    defer initial.finish();
    var a = try initial.retainEpoch();
    defer a.release();
    try storage.bind(.{ .boundary = &a });
    var owner = Module{};
    defer owner.clearWithCleanup(&ctx, .{ .outside_execution = true }) catch unreachable;
    try owner.resetPrepared(&ctx, &storage);
    owner.epoch_id = descriptor.epoch;
    try storage.close(.{ .boundary = &a });
    initial.finish();
    try std.testing.expectEqual(@as(usize, 128 * 8 * 4), owner.head.len);
    try std.testing.expectEqual(@as(usize, 2), GraphTestDriver.allocations);
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    owner.plan.?.allocate(1024, 0x1000000);
    owner.plan.?.release(0x1000000);
    try std.testing.expect(owner.plan.?.finishNoOutput());
    owner.phase = .capture;
    var arena: GraphPhase = undefined;
    try arena.initInto(descriptor);
    arena.mode = .arena;
    arena.expected_compute = 1;
    arena.old = owner.snapshot();
    arena.admission = storage.admission;
    const total = try exact.merge(.{ .backend_scratch_bytes = owner.plan.?.peak });
    arena.admission.?.limits.backend_limit_bytes = total.backend_scratch_bytes - 1;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, arena.prepareOutside(&model));
    try std.testing.expect(arena.cold == null and owner.arena.ptr == 0);
    try std.testing.expectEqual(@as(usize, 2), GraphTestDriver.allocations);
    try std.testing.expectEqualDeep(exact, domain.controller.snapshot());
    arena.admission.?.limits.backend_limit_bytes = total.backend_scratch_bytes;
    try arena.prepareOutside(&model);
    defer arena.destroyOutside();
    var current = try sink.enter();
    defer current.finish();
    var b = try current.retainEpoch();
    defer b.release();
    try arena.bind(.{ .boundary = &b });
    const plan = owner.plan.?;
    plan.* = .{};
    try plan.declareScratch(1023);
    const changed = try plan.layout(0);
    try std.testing.expectEqual(arena.old.arena_layout.?.bytes, changed.bytes);
    try std.testing.expectEqual(arena.old.arena_layout.?.events, changed.events);
    try std.testing.expect(arena.old.arena_layout.?.recipe != changed.recipe);
    try std.testing.expectError(error.PreparedCachePlanChanged, owner.reserveArenaPrepared(&ctx, &arena));
    try std.testing.expectEqual(@as(usize, 2), GraphTestDriver.allocations);
    plan.* = .{};
    try plan.declareScratch(1024);
    try owner.reserveArenaPrepared(&ctx, &arena);
    const pointer = owner.arena.ptr;
    try owner.reserveArenaPrepared(&ctx, &arena); // warm path consumes no token twice
    plan.* = .{};
    try plan.declareScratch(1023);
    try std.testing.expectError(error.InvalidExecutionPlan, owner.bindArena());
    plan.* = .{};
    try plan.declareScratch(1024);
    _ = try owner.bindArena();
    try std.testing.expectEqual(pointer, owner.arena.ptr);
    try std.testing.expectEqual(@as(usize, 3), GraphTestDriver.allocations);
    try arena.close(.{ .boundary = &b });
    try std.testing.expectEqualDeep(total, domain.controller.snapshot());
    try owner.clearWithCleanup(&ctx, .{ .boundary = &b });
    try std.testing.expectEqual(@as(usize, 3), GraphTestDriver.frees);
    try std.testing.expectEqualDeep(total, domain.controller.snapshot());
    current.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(ctx.prepared_cache_funding == null); // norm binding never overridden
}

test "CUDA bounded decoder binds zero scratch without reserving a device arena" {
    var plan: plan_mod.Plan = .{};
    try plan.declare(&.{}, null);
    var owner: Module = .{ .plan = &plan, .phase = .capture };
    var phase: GraphPhase = .{ .descriptor = undefined, .mode = .none, .old = owner.snapshot() };
    var ctx = GraphTestDriver.contextValue();
    owner.invalidations += 1;
    try std.testing.expectError(error.PreparedCachePlanChanged, owner.reserveArenaPrepared(&ctx, &phase));
    try std.testing.expect(owner.admitted_arena_layout == null);
    phase.old = owner.snapshot();
    try owner.reserveArenaPrepared(&ctx, &phase);
    const admitted = owner.snapshot();
    try std.testing.expectEqualDeep(phase.old.arena_layout, admitted.admitted_arena_layout);
    try std.testing.expect(admitted.arena_layout == null);
    try owner.reserveArenaPrepared(&ctx, &phase);
    try std.testing.expectEqual(@as(usize, 0), owner.arena.len);
    try std.testing.expect(owner.arena_quota == .none);
    try std.testing.expect((try owner.bindArena()).complete());
}

test "bounded CUDA decoder skips recording without a remaining replay burst" {
    try std.testing.expect(!Phase.warm.canReachReplay(5));
    try std.testing.expect(Phase.warm.canReachReplay(6));
    try std.testing.expect(!Phase.learn.canReachReplay(5));
    try std.testing.expect(Phase.learn.canReachReplay(6));
    try std.testing.expect(!Phase.capture.canReachReplay(3));
    try std.testing.expect(Phase.capture.canReachReplay(4));
    try std.testing.expect(Phase.replay.canReachReplay(2));
    try std.testing.expect(!Phase.replay.canReachReplay(1));
}

test "CUDA bounded decoder validates dispatch geometry before preparing storage" {
    const positions: [384]u32 = @splat(0);
    var descriptor: Descriptor = .{ .epoch = 1, .key = &.{1}, .positions = positions[0 .. 64 * 3], .rows = 64, .hidden = 1024, .vocab = 8, .future_tokens = 16, .attention_layers = 3, .recurrent_layers = 5 };
    try std.testing.expectEqual(Descriptor.DispatchCounts{ .attention = 6, .attention_gates = 6, .ragged_recurrent = 0 }, try descriptor.dispatchCounts());
    for ([_]usize{ 65, 98 }) |rows| {
        descriptor.rows = rows;
        descriptor.positions = positions[0 .. rows * 3];
        try std.testing.expectEqual(Descriptor.DispatchCounts{ .attention = 12, .attention_gates = 6, .ragged_recurrent = 20 }, try descriptor.dispatchCounts());
    }
    descriptor.rows = 128;
    descriptor.positions = &positions;
    try std.testing.expectEqual(Descriptor.DispatchCounts{ .attention = 12, .attention_gates = 6, .ragged_recurrent = 20 }, try descriptor.dispatchCounts());
    descriptor.recurrent_layers = std.math.maxInt(usize);
    var phase: GraphPhase = undefined;
    try std.testing.expectError(error.Overflow, phase.initInto(descriptor));
}

test "bounded decoder graph reuse requires the prepared dispatch geometry" {
    var owner: Module = .{};
    var key = [_]u64{9};
    const positions: [6]u32 = @splat(7);
    const descriptor: Descriptor = .{
        .epoch = 1,
        .key = &key,
        .positions = &positions,
        .rows = 2,
        .hidden = 1024,
        .vocab = 100,
        .future_tokens = 9,
        .attention_layers = 4,
        .recurrent_layers = 3,
    };
    owner.installStorageGeometry(try descriptor.layout(), descriptor.rows, descriptor.hidden, descriptor.vocab, descriptor.positions);
    owner.epoch_id = descriptor.epoch;
    owner.key = &key;
    owner.attention_layers = descriptor.attention_layers;
    owner.recurrent_layers = descriptor.recurrent_layers;
    try std.testing.expect(owner.referencesSequence(19));
    owner.sequences[0..2].* = .{ 7, 8 };
    owner.sequence_count = 2;
    try std.testing.expect(owner.referencesSequence(7));
    try std.testing.expect(!owner.referencesSequence(19));
    try std.testing.expect(!owner.matches(descriptor));
    owner.sequence_count = 0;
    try std.testing.expect(owner.matches(descriptor));
    var changed = descriptor;
    changed.future_tokens += 2;
    try std.testing.expect(owner.matches(changed));
    var phase: GraphPhase = undefined;
    changed = descriptor;
    changed.attention_geometry = .{ .heads = 8, .kv_heads = 2, .dim = 256 };
    try phase.initInto(changed);
    try phase.declareAttentionScratch(false);
    try std.testing.expect(!phase.matchesOwner(&owner));
    owner.attention_scratch = phase.attention_scratch;
    try std.testing.expect(!phase.matchesOwner(&owner));
    owner.storage_layout = phase.storage_layout;
    try std.testing.expect(phase.matchesOwner(&owner));
    owner.storage_layout.?.metadata_bytes -= 1;
    try std.testing.expect(!phase.matchesOwner(&owner));
    owner.storage_layout = phase.storage_layout;
    try phase.declareAttentionScratch(true);
    try std.testing.expect(!phase.matchesOwner(&owner));
    phase.descriptor.future_tokens += 2;
    try phase.declareAttentionScratch(false);
    try std.testing.expect(!phase.matchesOwner(&owner));
    phase.descriptor.attention_geometry = null;
    try std.testing.expectError(error.InvalidShape, phase.declareAttentionScratch(false));
    changed = descriptor;
    changed.attention_layers += 1;
    try std.testing.expect(!owner.matches(changed));
    changed = descriptor;
    changed.recurrent_layers += 1;
    try std.testing.expect(!owner.matches(changed));
    changed = descriptor;
    changed.vocab += 1;
    try std.testing.expect(!owner.matches(changed));
}

test "batched CUDA attention scratch packs exact chunk values and statistics" {
    const ScratchLayout = @import("paged_attention_bf16.zig").ScratchLayout;
    for ([_]bool{ false, true }, [_]usize{ 5, 2 }) |chunk128, chunks| {
        const layout = try ScratchLayout.init(64, 8, 2, 256, 129, chunk128);
        const partial_rows = 64 * 8 * chunks;
        try std.testing.expectEqual(chunks, layout.chunks);
        try std.testing.expectEqual(@as(usize, 64 * 8 * 256), layout.output_elements);
        try std.testing.expectEqual(partial_rows * 256 * 4, layout.statistics_offset);
        try std.testing.expectEqual(partial_rows * 258 * 4, layout.bytes);
        try std.testing.expectEqual(@as(usize, 0), layout.statistics_offset % 256);
    }
    try std.testing.expectError(error.InvalidShape, ScratchLayout.init(2, 8, 2, 256, 0, false));
}

test "CUDA recurrent packet scratch packs aligned convolution and recurrent planes" {
    const Scratch = @import("gated_delta.zig").PacketScratch;
    for ([_]usize{ 16, 32 }) |value_heads| {
        const channels = (2 * 16 + value_heads) * 128;
        const width = value_heads * 128;
        const layout = try Scratch.init(65, channels, width);
        try std.testing.expectEqual(65 * channels * @sizeOf(f32), layout.core_offset);
        try std.testing.expectEqual(65 * width * @sizeOf(f32), layout.bytes - layout.core_offset);
        try std.testing.expectEqual(@as(usize, 0), layout.core_offset % 256);
    }
    try std.testing.expectError(error.Overflow, Scratch.init(std.math.maxInt(usize), 2, 1));
}
