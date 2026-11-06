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

const std = @import("std");
const Cleanup = @import("../execution/cleanup_context.zig").Context;
const cache_mod = @import("cache.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const platform = @import("antfly_platform");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const runtime = @import("../runtime/root.zig");
const view_metadata = @import("../runtime/kv/view_metadata.zig");

fn selectedScoreCancellationCheck(raw: ?*anyopaque) callconv(.c) c_int {
    const control: *const InferenceExecutionControl = @ptrCast(@alignCast(raw orelse return 0));
    control.check() catch return 1;
    return 0;
}

// Runtime-owned sequence state. Task adapters supply tokens and scheduling
// decisions; KV reservations, views, recurrent caches and locking live here.
pub const KvView = struct {
    sequence_id: runtime.kv.manager.SequenceId,
    pool_id: runtime.kv.block.KvPoolId,
    logical_block_count: usize,
    tail_tokens: u16,
    token_count: usize,
    position_offset: usize,
    max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
    logical_blocks: ?[]const runtime.kv.block.KvBlockId = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
};

pub const KvMetadataDelta = struct {
    sequence_replaced: bool = false,
    logical_block_count_before: usize = 0,
    logical_block_count_after: usize = 0,
    position_offset_before: usize = 0,
    position_offset_after: usize = 0,
    retained_tokens: ?usize = null,
};

pub const KvMutationResult = struct {
    token_count: usize,
    kv_view: ?KvView,
    compacted: bool,
    delta: KvMetadataDelta = .{},
};

const SelectionQueryEntry = struct {
    donor_layer: usize,
    consumer_layer: usize,
    num_heads: usize,
    head_dim: usize,
    positions: std.ArrayListUnmanaged(u64) = .empty,
    values: std.ArrayListUnmanaged(f32) = .empty,

    fn deinit(self: *SelectionQueryEntry, allocator: std.mem.Allocator) void {
        self.positions.deinit(allocator);
        self.values.deinit(allocator);
    }
};

pub const NativeDecodeState = struct {
    /// Borrowed through claimed-step drain and publication.
    execution_backend: ?@import("../ops/ops.zig").ComputeBackend = null,
    execution_control: ?InferenceExecutionControl = null,
    allocator: std.mem.Allocator,
    kv_manager: ?*runtime.kv.manager.KvManager = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    /// Optional external lock for shared paged KV metadata/storage. Batch
    /// generation wires every decode state in a group to the same manager and
    /// storage runtime, so admission reads and KV mutations must share this
    /// lock even though forward execution is serialized by the scheduler turn.
    kv_lock: ?*std.atomic.Mutex = null,
    sequence_id: ?runtime.kv.manager.SequenceId = null,
    pool_id: ?runtime.kv.block.KvPoolId = null,
    total_tokens: usize = 0,
    kv_view: ?KvView = null,
    kv_compacted: bool = false,
    kv_block_ids: view_metadata.Table = .empty,
    kv_block_ids_synchronized: bool = false,
    // Old published tables may still be borrowed by physical metadata readers.
    // Retain only geometrically grown owners until this state's real teardown.
    retired_kv_block_ids: view_metadata.History = .empty,
    moe_runtime: runtime.moe.runtime.MoeRuntime,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    qwen35_linear_cache: ?gpt_arch.Qwen35LinearCache = null,
    batched_decode_steps: usize = 0,
    /// Largest successfully executed decoder step for this sequence.
    max_native_step_items: usize = 0,
    gemma4_layer_spec_cache: gpt_arch.Gemma4LayerSpecCache = .{},
    deepseek_v4_compressed_cache: ?gpt_arch.DeepSeekV4CompressedCache = null,
    force_full_recompute: bool = false,
    kv_max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
    qwen3vl_mrope_position_delta: ?i64 = null,
    qwen3vl_decode_positions: [3]u32 = .{ 0, 0, 0 },
    qwen3vl_text_only: bool = false,
    selection_capture_enabled: bool = false,
    selection_queries: std.ArrayListUnmanaged(SelectionQueryEntry) = .empty,
    selection_protected_spans: []const runtime.kv.selection.ProtectedSpan = &.{},

    pub fn initContiguous(allocator: std.mem.Allocator) NativeDecodeState {
        return .{
            .allocator = allocator,
            .moe_runtime = runtime.moe.runtime.MoeRuntime.init(allocator, null),
        };
    }

    pub fn initPaged(
        allocator: std.mem.Allocator,
        kv_manager: *runtime.kv.manager.KvManager,
        pool_id: runtime.kv.block.KvPoolId,
        shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    ) NativeDecodeState {
        return .{
            .allocator = allocator,
            .kv_manager = kv_manager,
            .pool_id = pool_id,
            .moe_runtime = runtime.moe.runtime.MoeRuntime.init(allocator, shared_moe_cache),
            .shared_moe_cache = shared_moe_cache,
        };
    }

    fn lockPagedKv(self: *const NativeDecodeState) ?*std.atomic.Mutex {
        const mutex = self.kv_lock orelse return null;
        platform.sync.lockYielding(mutex);
        return mutex;
    }

    /// Admission reads use the same lock as KV mutation and reservation.
    pub fn availableKvBlocks(self: *const NativeDecodeState) ?usize {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const manager = self.kv_manager orelse return null;
        return manager.poolAvailableBlocks(self.pool_id orelse return null);
    }

    pub fn estimateAdditionalKvBlocks(self: *const NativeDecodeState, tokens: usize) ?usize {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const manager = self.kv_manager orelse return null;
        return manager.estimateBlocksFor(self.sequence_id orelse return null, tokens);
    }

    pub fn requiresExclusiveStep(self: *const NativeDecodeState) bool {
        return self.deepseek_v4_compressed_cache != null;
    }

    pub fn isPaged(self: *const NativeDecodeState) bool {
        return self.kv_manager != null and !self.force_full_recompute;
    }

    pub fn isKvCompacted(self: *const NativeDecodeState) bool {
        return self.kv_compacted;
    }

    pub fn configureForGptConfig(self: *NativeDecodeState, config: gpt_mod.Config) void {
        self.force_full_recompute = false;
        if (self.total_tokens == 0) self.kv_max_inflight_tokens = 0;
        self.allow_swa_ring = config.supportsSplitSwaGlobalKvRing();
        if (config.family != .gemma) self.gemma4_layer_spec_cache.reset();
        if (!requiresDeepSeekV4CompressedCache(config)) self.clearDeepSeekV4CompressedCache();
        self.qwen3vl_mrope_position_delta = if (config.family == .qwen3_vl) 0 else null;
        self.qwen3vl_text_only = config.family == .qwen3_vl;
    }

    pub fn captureSelectionQueries(
        capture_ctx: *anyopaque,
        donor_layer: usize,
        consumer_layer: usize,
        first_position: usize,
        num_heads: usize,
        head_dim: usize,
        values: []const f32,
    ) !void {
        const self: *NativeDecodeState = @ptrCast(@alignCast(capture_ctx));
        if (!self.selection_capture_enabled) return;
        const width = try std.math.mul(usize, num_heads, head_dim);
        if (width == 0 or values.len % width != 0) return error.UnexpectedOutputShape;
        for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttentionInput;
        const rows = values.len / width;
        const horizon = runtime.kv.selection.defaults.query_horizon;
        var entry: *SelectionQueryEntry = blk: {
            for (self.selection_queries.items) |*candidate| {
                if (candidate.donor_layer == donor_layer and candidate.consumer_layer == consumer_layer) {
                    if (candidate.num_heads != num_heads or candidate.head_dim != head_dim) return error.InvalidSelectionConfig;
                    break :blk candidate;
                }
            }
            try self.selection_queries.append(self.allocator, .{
                .donor_layer = donor_layer,
                .consumer_layer = consumer_layer,
                .num_heads = num_heads,
                .head_dim = head_dim,
            });
            break :blk &self.selection_queries.items[self.selection_queries.items.len - 1];
        };
        const incoming_rows = @min(rows, horizon);
        if (incoming_rows == horizon) {
            entry.positions.clearRetainingCapacity();
            entry.values.clearRetainingCapacity();
        }
        // A fused route may decline after Q capture and retry through the
        // unfused attention route. Replace that overlapping suffix so every
        // absolute query position contributes exactly once.
        while (entry.positions.items.len > 0 and entry.positions.items[entry.positions.items.len - 1] >= first_position) {
            _ = entry.positions.pop();
            entry.values.shrinkRetainingCapacity(entry.values.items.len - width);
        }
        if (entry.positions.items.len > 0 and entry.positions.items[entry.positions.items.len - 1] + 1 != first_position)
            return error.NonContiguousKvSelectionQueries;
        const retained_old = @min(entry.positions.items.len, horizon -| incoming_rows);
        if (retained_old > 0) {
            const old_start = entry.positions.items.len - retained_old;
            std.mem.copyForwards(u64, entry.positions.items[0..retained_old], entry.positions.items[old_start..]);
            const value_start = old_start * width;
            std.mem.copyForwards(f32, entry.values.items[0 .. retained_old * width], entry.values.items[value_start..]);
        }
        entry.positions.shrinkRetainingCapacity(retained_old);
        entry.values.shrinkRetainingCapacity(retained_old * width);
        const incoming_start = rows - @min(rows, horizon);
        try entry.positions.ensureUnusedCapacity(self.allocator, rows - incoming_start);
        try entry.values.appendSlice(self.allocator, values[incoming_start * width ..]);
        for (incoming_start..rows) |row| entry.positions.appendAssumeCapacity(@as(u64, @intCast(first_position + row)));
    }

    pub fn clearSelectionQueries(self: *NativeDecodeState) void {
        for (self.selection_queries.items) |*entry| entry.deinit(self.allocator);
        self.selection_queries.clearRetainingCapacity();
    }

    pub fn configureKvMaxInflightTokens(self: *NativeDecodeState, token_count: usize, request_allows_ring: bool) void {
        if (token_count == 0 or self.total_tokens != 0) {
            self.allow_swa_ring = false;
            self.kv_max_inflight_tokens = 0;
            if (self.kv_view) |*view| {
                view.max_inflight_tokens = 0;
                view.allow_swa_ring = false;
            }
            return;
        }
        // Ring page ids are absolute from sequence position zero. A pool that
        // front-trims its logical block table would make them view-relative.
        self.allow_swa_ring = self.allow_swa_ring and request_allows_ring and self.kvSlidingWindowTokens() == null;
        self.kv_max_inflight_tokens = if (self.allow_swa_ring) token_count else 0;
        if (self.kv_view) |*view| {
            view.max_inflight_tokens = self.kv_max_inflight_tokens;
            view.allow_swa_ring = self.allow_swa_ring;
        }
    }

    pub fn requiresFullRecompute(self: *const NativeDecodeState) bool {
        return self.force_full_recompute;
    }

    pub fn requiresDeepSeekV4CompressedCache(config: gpt_mod.Config) bool {
        if (config.family != .deepseek_v4) return false;
        const schedule_len = @min(@as(usize, @intCast(config.deepseek_v4_attention_schedule_len)), config.deepseek_v4_attention_schedule.len);
        if (schedule_len > 0) {
            for (config.deepseek_v4_attention_schedule[0..schedule_len]) |kind| {
                switch (kind) {
                    .compressed_sparse_attention, .heavily_compressed_attention => return true,
                    else => {},
                }
            }
            return false;
        }
        return config.deepseek_v4_compressed_sparse_attention_layers > 0 or
            config.deepseek_v4_heavily_compressed_attention_layers > 0;
    }

    pub fn ensureDeepSeekV4CompressedCache(self: *NativeDecodeState, config: gpt_mod.Config) !void {
        if (!requiresDeepSeekV4CompressedCache(config)) return;
        if (self.deepseek_v4_compressed_cache == null) {
            self.deepseek_v4_compressed_cache = try gpt_arch.DeepSeekV4CompressedCache.init(self.allocator, @intCast(config.num_hidden_layers));
        }
    }

    pub fn resetDeepSeekV4CompressedCache(self: *NativeDecodeState) void {
        if (self.deepseek_v4_compressed_cache) |*cache| cache.reset();
    }

    pub fn clearDeepSeekV4CompressedCache(self: *NativeDecodeState) void {
        if (self.deepseek_v4_compressed_cache) |*cache| cache.deinit();
        self.deepseek_v4_compressed_cache = null;
    }

    fn ensureAttachedUnlocked(self: *NativeDecodeState) !void {
        if (!self.isPaged()) return;
        if (self.sequence_id != null) return;
        self.sequence_id = try self.kv_manager.?.attachSequence(self.pool_id orelse return error.InvalidPoolId);
        if (self.kv_storage) |storage| {
            const storage_sequence_id = try storage.attachSequence(storage.poolId());
            if (storage_sequence_id != self.sequence_id.?) return error.InvalidPagedKvState;
        }
    }

    pub fn seedAttachedPrefix(self: *NativeDecodeState, sequence_id: runtime.kv.manager.SequenceId, token_count: usize) !void {
        self.kv_block_ids_synchronized = false;
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        if (!self.isPaged()) return error.InvalidPagedKvState;
        if (self.sequence_id != null) return error.InvalidPagedKvState;
        if (self.kv_storage) |storage| {
            if ((storage.tokenCount(sequence_id) orelse return error.InvalidPagedKvState) != token_count) return error.InvalidPagedKvState;
        }
        self.sequence_id = sequence_id;
        self.total_tokens = token_count;
        self.kv_compacted = false;
        try self.syncPagedKvViewForPrefill();
    }

    pub fn ensureAttached(self: *NativeDecodeState) !void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        return self.ensureAttachedUnlocked();
    }

    pub fn reserveFreshCudaKvCapacity(self: *NativeDecodeState, token_capacity: usize, token_limit: usize) !void {
        self.kv_block_ids_synchronized = false;
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        if (!self.isPaged() or self.total_tokens != 0 or self.sequence_id != null)
            return error.InvalidPagedKvState;
        const storage = self.kv_storage orelse return error.DeviceWriteUnsupported;
        try self.ensureAttachedUnlocked();
        try self.kv_manager.?.reserveTokenCapacity(self.sequence_id.?, token_limit);
        try storage.reserveDeviceTokenCapacity(self.sequence_id.?, token_capacity, token_limit);
    }

    pub fn prepareSelectedGlobalDonors(self: *NativeDecodeState, config: gpt_mod.Config) !void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const storage = self.kv_storage orelse return error.DeviceWriteUnsupported;
        // A host-authoritative full-history mirror would defeat the bounded
        // memory contract and cannot represent donor-specific selected rows.
        if (storage.storage.config.store_cpu_bytes) return error.UnsupportedKvCachePolicy;
        try self.ensureAttachedUnlocked();
        const sequence_id = self.sequence_id orelse return error.InvalidPagedKvState;
        const transient_rows = try runtime.kv.selection.defaults.transientRows();
        for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
            if (!config.isKvDonorLayer(layer) or config.layerUsesSlidingAttention(layer)) continue;
            const geometry = try config.kvDonorGeometry(layer);
            try storage.reserveSelectedGlobalKvDeviceCapacity(
                sequence_id,
                geometry.donor_layer,
                transient_rows,
                0,
                geometry.num_kv_heads,
                geometry.head_dim,
            );
        }
        self.selection_capture_enabled = true;
    }

    pub fn compactSelectedGlobalDonors(
        self: *NativeDecodeState,
        config: gpt_mod.Config,
        control: ?InferenceExecutionControl,
        gpu_oracle: bool,
    ) !void {
        if (!self.selection_capture_enabled) return;
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const storage = self.kv_storage orelse return error.DeviceWriteUnsupported;
        const sequence_id = self.sequence_id orelse return error.InvalidPagedKvState;
        const policy = runtime.kv.selection.defaults;
        var score_control = control orelse InferenceExecutionControl{};
        for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
            if (control) |request_control| try request_control.check();
            if (!config.isKvDonorLayer(layer) or config.layerUsesSlidingAttention(layer)) continue;
            const geometry = try config.kvDonorGeometry(layer);
            const descriptor = try storage.pagedLayerKvDevice(
                sequence_id,
                geometry.donor_layer,
                self.total_tokens,
                geometry.num_kv_heads,
                geometry.head_dim,
            );
            if (descriptor.ring_page_count != 0) return error.InvalidKvSelectionState;
            if (descriptor.token_count < policy.hard_rows) continue;

            const row_count = descriptor.token_count;
            if (row_count > try policy.transientRows()) return error.KvCapacityTooSmall;
            const positions = try self.allocator.alloc(u64, row_count);
            defer self.allocator.free(positions);
            if (descriptor.key_positions) |selected_positions| {
                if (selected_positions.len != row_count) return error.InvalidKvSelectionState;
                for (selected_positions, positions) |position, *out| out.* = position;
            } else {
                for (positions, 0..) |*position, index| position.* = @intCast(index);
            }
            const positions_u32 = try runtime.kv.selection.positionsU32(self.allocator, positions);
            defer self.allocator.free(positions_u32);
            const oracle_keys: ?[]f32 = if (gpu_oracle)
                try storage.gatherLayerKeys(self.allocator, sequence_id, geometry.donor_layer, row_count)
            else
                null;
            defer if (oracle_keys) |keys| self.allocator.free(keys);

            var accumulator = try runtime.kv.selection.ScoreAccumulator.init(self.allocator, row_count);
            defer accumulator.deinit(self.allocator);
            var expected_consumers: usize = 0;
            for (0..@as(usize, @intCast(config.num_hidden_layers))) |consumer_layer| {
                if (config.layerUsesSlidingAttention(consumer_layer)) continue;
                if ((try config.kvStorageLayerIndex(consumer_layer)) == geometry.donor_layer) expected_consumers += 1;
            }
            var captured_consumers: usize = 0;
            for (self.selection_queries.items) |*capture| {
                if (capture.donor_layer != geometry.donor_layer) continue;
                if (control) |request_control| try request_control.check();
                if (capture.positions.items.len == 0) return error.MissingKvSelectionQueries;
                const query_positions_u32 = try runtime.kv.selection.positionsU32(self.allocator, capture.positions.items);
                defer self.allocator.free(query_positions_u32);
                const gpu_scores = try self.allocator.alloc(f32, row_count);
                defer self.allocator.free(gpu_scores);
                try storage.scoreSelectedAttention(.{
                    .sequence_id = sequence_id,
                    .layer_index = geometry.donor_layer,
                    .queries = capture.values.items,
                    .query_positions = query_positions_u32,
                    .key_positions = positions_u32,
                    .num_heads = capture.num_heads,
                    .num_kv_heads = geometry.num_kv_heads,
                    .head_dim = geometry.head_dim,
                    .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(geometry.head_dim))),
                    .scores_out = gpu_scores,
                    .check_ctx = &score_control,
                    .check_fn = selectedScoreCancellationCheck,
                });
                if (oracle_keys) |keys| {
                    const oracle = try runtime.kv.selection.scoreConsumerNormalizedAttention(self.allocator, .{
                        .queries = capture.values.items,
                        .query_positions = capture.positions.items,
                        .keys = keys,
                        .key_positions = positions,
                        .num_query_heads = capture.num_heads,
                        .num_kv_heads = geometry.num_kv_heads,
                        .head_dim = geometry.head_dim,
                        .score_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(geometry.head_dim))),
                    });
                    defer self.allocator.free(oracle);
                    for (gpu_scores, oracle) |actual, expected| {
                        if (@abs(@as(f64, actual) - expected) > 2e-4) return error.KvSelectionGpuOracleMismatch;
                    }
                }
                const scores = try self.allocator.alloc(f64, row_count);
                defer self.allocator.free(scores);
                for (gpu_scores, scores) |score, *out| out.* = score;
                try accumulator.addConsumer(scores);
                captured_consumers += 1;
            }
            if (captured_consumers != expected_consumers) return error.MissingKvSelectionQueries;
            const balanced_scores = try accumulator.finish();
            var plan = try runtime.kv.selection.selectRows(
                self.allocator,
                policy,
                positions,
                balanced_scores,
                @intCast(self.total_tokens),
                self.selection_protected_spans,
            );
            defer plan.deinit();
            const source_rows = try self.allocator.alloc(u32, plan.source_indices.len);
            defer self.allocator.free(source_rows);
            for (plan.source_indices, source_rows) |source, *out| out.* = std.math.cast(u32, source) orelse return error.InvalidKvSelectionState;
            const selected_positions = try runtime.kv.selection.positionsU32(self.allocator, plan.positions);
            defer self.allocator.free(selected_positions);
            if (control) |request_control| try request_control.check();
            _ = try storage.repackSelectedGlobalKv(.{
                .sequence_id = sequence_id,
                .layer_index = geometry.donor_layer,
                .expected_source_slot = descriptor.slot,
                .expected_written_tokens = self.total_tokens,
                .source_token_offsets = source_rows,
                .key_positions = selected_positions,
                .num_kv_heads = geometry.num_kv_heads,
                .head_dim = geometry.head_dim,
            });
        }
    }

    fn kvPageSizeTokens(self: *const NativeDecodeState) ?usize {
        const manager = self.kv_manager orelse return null;
        const pool_id = self.pool_id orelse return null;
        const pool = manager.getPool(pool_id) orelse return null;
        return pool.config.page_size_tokens;
    }

    pub fn kvSlidingWindowTokens(self: *const NativeDecodeState) ?usize {
        const manager = self.kv_manager orelse return null;
        const pool_id = self.pool_id orelse return null;
        const pool = manager.getPool(pool_id) orelse return null;
        return if (pool.config.sliding_window_size) |size| @intCast(size) else null;
    }

    pub fn cudaPagedKvReplayCapacityTokens(self: *const NativeDecodeState) ?usize {
        const storage = self.kv_storage orelse return null;
        switch (storage.storage.config.dtype) {
            .f16, .polar4, .turbo3 => {},
            else => return null,
        }
        const forced = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") orelse return null;
        return if (forced == 0) null else forced;
    }

    fn reservePagedKvReplayCapacity(self: *NativeDecodeState) !void {
        self.kv_block_ids_synchronized = false;
        const token_capacity = self.cudaPagedKvReplayCapacityTokens() orelse return;
        const sequence_id = self.sequence_id orelse return;
        const manager = self.kv_manager orelse return;
        try manager.reserveTokenCapacity(sequence_id, token_capacity);
        if (self.kv_storage) |storage| try storage.reserveTokenCapacity(sequence_id, token_capacity);
    }

    fn setPagedKvView(self: *NativeDecodeState, token_count: usize, position_offset: usize) void {
        const sequence_id = self.sequence_id orelse {
            self.kv_view = null;
            return;
        };
        const pool_id = self.pool_id orelse {
            self.kv_view = null;
            return;
        };
        if (token_count == 0) {
            self.kv_view = null;
            return;
        }
        const page_size = self.kvPageSizeTokens() orelse {
            self.kv_view = null;
            return;
        };
        const logical_block_count = (token_count + page_size - 1) / page_size;
        const rem = token_count % page_size;
        const tail_tokens: u16 = @intCast(if (rem == 0) page_size else rem);
        self.kv_view = .{
            .sequence_id = sequence_id,
            .pool_id = pool_id,
            .logical_block_count = logical_block_count,
            .tail_tokens = tail_tokens,
            .token_count = token_count,
            .position_offset = position_offset,
            .max_inflight_tokens = self.kv_max_inflight_tokens,
            .allow_swa_ring = self.allow_swa_ring,
            .logical_blocks = self.kv_block_ids.items,
            .kv_storage = self.kv_storage,
        };
    }

    const PreparedKvView = struct {
        state: *NativeDecodeState,
        count: usize,
        replacement: view_metadata.Table = .empty,

        fn deinit(self: *@This()) void {
            self.replacement.deinit(self.state.allocator);
            self.replacement = .empty;
        }

        /// Only called after authoritative owner commit. No allocation or
        /// callback can fail between replacing the list and copying its IDs.
        fn copyFrom(self: *@This(), sequence: runtime.kv.manager.SequenceId) void {
            const state = self.state;
            state.kv_block_ids_synchronized = false;
            if (self.replacement.capacity != 0) {
                std.mem.swap(view_metadata.Table, &state.kv_block_ids, &self.replacement);
                if (self.replacement.capacity != 0) {
                    state.retired_kv_block_ids.appendAssumeCapacity(self.replacement);
                    self.replacement = .empty;
                }
            }
            state.kv_block_ids.items.len = self.count;
            state.kv_manager.?.copyLogicalBlocksWithReservationsInto(sequence, state.kv_block_ids.items);
            state.kv_block_ids_synchronized = true;
        }
    };

    fn prepareKvView(self: *NativeDecodeState, count: usize) !PreparedKvView {
        var prepared = PreparedKvView{ .state = self, .count = count };
        errdefer prepared.deinit();
        if (count > self.kv_block_ids.capacity) {
            try prepared.replacement.ensureTotalCapacityPrecise(self.allocator, try view_metadata.capacityForBlocks(count));
            if (self.kv_block_ids.capacity != 0) try self.retired_kv_block_ids.ensureTotalCapacityPrecise(self.allocator, try view_metadata.capacityForBlocks(self.retired_kv_block_ids.items.len + 1));
        }
        return prepared;
    }

    fn syncPagedKvBlockTable(self: *NativeDecodeState) !void {
        self.kv_block_ids_synchronized = false;
        const manager = self.kv_manager orelse {
            self.kv_block_ids.clearRetainingCapacity();
            return;
        };
        const sequence_id = self.sequence_id orelse {
            self.kv_block_ids.clearRetainingCapacity();
            return;
        };
        var prepared = try self.prepareKvView(try manager.logicalBlocksWithReservationsCount(sequence_id));
        defer prepared.deinit();
        prepared.copyFrom(sequence_id);
    }

    fn syncPagedKvViewForPrefill(self: *NativeDecodeState) !void {
        if (!self.isPaged()) {
            self.kv_view = null;
            return;
        }
        try self.syncPagedKvBlockTable();
        self.setPagedKvView(self.total_tokens, 0);
    }

    fn syncPagedKvViewForDecode(self: *NativeDecodeState) !void {
        if (!self.isPaged()) {
            self.kv_view = null;
            return;
        }
        try self.syncPagedKvBlockTable();
        self.publishPagedKvViewForDecode();
    }

    /// Publish only after the authoritative block table has been copied. This
    /// phase neither allocates nor suppresses a failed metadata preparation.
    fn publishPagedKvViewForDecode(self: *NativeDecodeState) void {
        if (self.kv_compacted) {
            const manager = self.kv_manager orelse return;
            const sequence_id = self.sequence_id orelse return;
            const current_kv_tokens = @min(manager.tokenCount(sequence_id) orelse 0, self.total_tokens);
            self.setPagedKvView(current_kv_tokens, self.total_tokens - current_kv_tokens);
            return;
        }
        // The sliding-window trim drops whole KV pages, so the view must
        // track the tokens actually retained (block-aligned), not the exact
        // window size. Kernels index the compacted block table with
        // view-relative positions: a token-granular offset here would
        // misalign every physical KV read and write by (offset % page_size).
        // The attention kernels apply the exact sliding window themselves.
        const retained_tokens = blk: {
            if (self.kvSlidingWindowTokens() == null) break :blk self.total_tokens;
            const manager = self.kv_manager orelse break :blk self.total_tokens;
            const sequence_id = self.sequence_id orelse break :blk self.total_tokens;
            break :blk manager.tokenCount(sequence_id) orelse self.total_tokens;
        };
        const kv_tokens = @min(self.total_tokens, retained_tokens);
        self.setPagedKvView(kv_tokens, self.total_tokens - kv_tokens);
    }

    fn makeKvMutationResult(self: *const NativeDecodeState, before_view: ?KvView, before_sequence_id: ?runtime.kv.manager.SequenceId, retained_tokens: ?usize) KvMutationResult {
        const after_view = self.kvView();
        return .{
            .token_count = self.total_tokens,
            .kv_view = after_view,
            .compacted = self.kv_compacted,
            .delta = .{
                .sequence_replaced = before_sequence_id != self.sequence_id,
                .logical_block_count_before = if (before_view) |view| view.logical_block_count else 0,
                .logical_block_count_after = if (after_view) |view| view.logical_block_count else 0,
                .position_offset_before = if (before_view) |view| view.position_offset else 0,
                .position_offset_after = if (after_view) |view| view.position_offset else 0,
                .retained_tokens = retained_tokens,
            },
        };
    }

    fn deinitRetiredKvBlockIds(self: *NativeDecodeState) void {
        for (self.retired_kv_block_ids.items) |*table| table.deinit(self.allocator);
        self.retired_kv_block_ids.deinit(self.allocator);
        self.retired_kv_block_ids = .empty;
    }

    pub fn deinitWithCleanup(self: *NativeDecodeState, current: Cleanup) !void {
        if (current.outside_execution) {
            var typed_cuda_owner = if (self.kv_storage) |storage| if (storage.device_write_hook) |hook| hook.vtable.prepareMutationWithCleanup != null else false else false;
            if (comptime @import("build_options").enable_cuda) if (self.qwen35_linear_cache) |cache| for (cache.layers) |layer| {
                typed_cuda_owner = typed_cuda_owner or layer.cuda_device.prepare_release_with_cleanup != null;
            };
            if (typed_cuda_owner) {
                if (current.boundary != null or current.operation != null) return error.ConflictingCleanupAuthority;
                // This function will own KV/graph mutexes below. Its genuine
                // local B is reserved before either lock and seals only after
                // the recursive typed teardown has released ALL those locks.
                var pending: @import("../execution/resource_retirement.zig").SynchronousOutsideRawLeases = .{};
                var local = try @import("../execution/cleanup_owner.zig").Owner.beginOutsideCleanup();
                defer {
                    const registered = pending.boundary_context != null;
                    local.finishAfterUnlock();
                    if (registered) std.debug.assert(pending.finished);
                }
                const kv_current = try local.synchronousOutsideContext(&pending);
                // Only our KV/graph critical section needs this local B. Raw
                // recurrent quota remains explicit outside AFTER KV unlock.
                return self.deinitUsing(kv_current, current);
            }
        }
        return self.deinitUsing(current, current);
    }
    fn deinitUsing(self: *NativeDecodeState, kv_current: Cleanup, resource_current: Cleanup) !void {
        if (self.qwen35_linear_cache) |*cache| try cache.validateCleanup(resource_current);
        {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            if (self.sequence_id) |sequence| {
                // Checked physical invalidation precedes logical publication.
                if (self.kv_storage) |storage| try storage.releaseSequenceWithCleanup(sequence, kv_current);
                if (self.kv_manager) |manager| try manager.releaseSequence(sequence);
                // Publish successful retirement before any later fallible
                // recurrent SDK death. A retry must not release this ID again.
                self.sequence_id = null;
                self.kv_view = null;
                self.kv_compacted = false;
            }
        }
        // Recurrent quota callbacks on an explicit outside path must not run
        // while this request's KV mutex remains held.
        if (self.qwen35_linear_cache) |*cache| {
            try cache.deinitWithCleanup(resource_current);
            self.qwen35_linear_cache = null;
        }
        // No irreversible host metadata is destroyed until every fallible SDK
        // root has died or transferred its independently funded cache custody.
        self.moe_runtime.deinit();
        self.gemma4_layer_spec_cache.deinit(self.allocator);
        if (self.deepseek_v4_compressed_cache) |*cache| cache.deinit();
        self.clearSelectionQueries();
        self.selection_queries.deinit(self.allocator);
        self.selection_queries = .empty;
        self.deinitRetiredKvBlockIds();
        self.kv_block_ids_synchronized = false;
        self.kv_block_ids.deinit(self.allocator);
        self.kv_block_ids = .empty;
        self.sequence_id = null;
        self.kv_view = null;
        self.kv_compacted = false;
        self.qwen35_linear_cache = null;
        self.deepseek_v4_compressed_cache = null;
        self.kv_storage = null;
        self.kv_manager = null;
        self.kv_lock = null;
        self.pool_id = null;
        self.total_tokens = 0;
    }

    pub fn deinit(self: *NativeDecodeState) void {
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        if (self.kv_manager) |manager| {
            if (self.sequence_id) |sequence_id| {
                manager.releaseSequence(sequence_id) catch {};
                if (self.kv_storage) |storage| storage.releaseSequence(sequence_id) catch {};
            }
        }
        self.moe_runtime.deinit();
        if (self.qwen35_linear_cache) |*cache| cache.deinit();
        self.gemma4_layer_spec_cache.deinit(self.allocator);
        if (self.deepseek_v4_compressed_cache) |*cache| cache.deinit();
        self.clearSelectionQueries();
        self.selection_queries.deinit(self.allocator);
        self.deinitRetiredKvBlockIds();
        self.kv_block_ids_synchronized = false;
        self.kv_block_ids.deinit(self.allocator);
        self.sequence_id = null;
        self.kv_view = null;
        self.kv_compacted = false;
        self.qwen35_linear_cache = null;
        self.deepseek_v4_compressed_cache = null;
    }

    pub fn ensureQwen35LinearCache(self: *NativeDecodeState, config: gpt_mod.Config) !void {
        if (!config.isQwen35() or !config.qwen35_has_linear_attention) return;
        if (self.qwen35_linear_cache == null) {
            self.qwen35_linear_cache = try gpt_arch.Qwen35LinearCache.init(self.allocator, config);
        }
    }

    pub fn resetQwen35LinearCache(self: *NativeDecodeState) void {
        if (self.qwen35_linear_cache) |*cache| cache.reset();
    }

    pub fn notePrefill(self: *NativeDecodeState, token_count: usize) !void {
        return self.notePrefillWithCleanup(token_count, .{});
    }
    pub fn notePrefillWithCleanup(self: *NativeDecodeState, token_count: usize, current: Cleanup) !void {
        self.kv_block_ids_synchronized = false;
        self.total_tokens = token_count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
            if (self.kv_storage) |storage| try storage.appendTokensWithCleanup(self.sequence_id.?, @intCast(token_count), current);
            try self.reservePagedKvReplayCapacity();
            self.kv_compacted = false;
            try self.syncPagedKvViewForPrefill();
        }
    }

    pub fn notePrefillWithResult(self: *NativeDecodeState, token_count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.notePrefill(token_count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    pub fn appendPrefillChunk(self: *NativeDecodeState, token_count: usize) !void {
        return self.appendPrefillChunkWithCleanup(token_count, .{});
    }
    pub fn appendPrefillChunkWithCleanup(self: *NativeDecodeState, token_count: usize, current: Cleanup) !void {
        self.kv_block_ids_synchronized = false;
        self.total_tokens += token_count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(token_count));
            if (self.kv_storage) |storage| try storage.appendTokensWithCleanup(self.sequence_id.?, @intCast(token_count), current);
            try self.reservePagedKvReplayCapacity();
            self.kv_compacted = false;
            try self.syncPagedKvViewForPrefill();
        }
    }

    pub fn appendPrefillChunkWithResult(self: *NativeDecodeState, token_count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendPrefillChunk(token_count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Atomic generated append for already-attached canonical host owners.
    /// Opaque/device providers and fresh attachment retain their existing route;
    /// eligibility is resolved before any state, page or published-view mutation.
    fn tryAtomicGeneratedAppend(self: *NativeDecodeState, count: usize, current: Cleanup) !bool {
        if (count == 0 or !self.isPaged()) return false;
        const sequence = self.sequence_id orelse return false;
        const manager = self.kv_manager.?;
        if (!manager.canPrepareGeneratedAppend(sequence, count)) return false;
        if (self.kv_storage) |storage| {
            if (!storage.canPrepareGeneratedAppend(sequence, count)) return false;
            const manager_sequence = manager.sequenceMut(sequence) catch return false;
            const manager_pool = manager.getPool(manager_sequence.pool_id) orelse return false;
            // Two independent prepared plans must not stage against the same
            // physical pool/refcounts. Such wrappers retain compatibility.
            if (manager_pool.ptr == storage.storage.ptr) return false;
            const storage_sequence = storage.sequenceMut(sequence) catch return false;
            const manager_window: ?u32 = if (manager_sequence.compacted) null else manager_pool.config.sliding_window_size;
            const storage_window: ?u32 = if (storage_sequence.compacted) null else storage.storage.config.sliding_window_size;
            if (manager_pool.config.page_size_tokens != storage.storage.config.page_size_tokens or
                manager.tokenCount(sequence) != storage.tokenCount(sequence) or manager_window != storage_window) return false;
        }
        const total = try std.math.add(usize, self.total_tokens, count);
        const replay_capacity = self.cudaPagedKvReplayCapacityTokens() orelse 0;
        var manager_append = try manager.prepareGeneratedAppend(sequence, count, replay_capacity);
        defer manager_append.abort();
        var storage_append: ?@import("../runtime/kv/writable_blocks.zig").PreparedAppend = null;
        defer if (storage_append) |*prepared| prepared.abort();
        if (self.kv_storage) |storage| {
            storage_append = try storage.prepareGeneratedAppendWithCleanup(sequence, count, replay_capacity, current);
            if (storage_append.?.futureTokenCount() != manager_append.futureTokenCount()) return error.InvalidPagedKvState;
        }
        const block_count = try std.math.add(usize, manager_append.futureLogicalBlockCount(), manager_append.futureReservedBlockCount());
        const ids_unchanged = self.kv_block_ids_synchronized and self.kv_block_ids.items.len == block_count and !manager_append.idsChanged() and
            (if (storage_append) |*prepared| !prepared.idsChanged() else true);
        // Prepare growth without invalidating the still-published table.
        var prepared_view = try self.prepareKvView(block_count);
        defer prepared_view.deinit();

        // All remaining work is infallible under the canonical owners' prepared
        // contracts. The common KV lock/readback boundary remains held by caller.
        manager_append.commit();
        if (storage_append) |*prepared| prepared.commit();
        if (!ids_unchanged) prepared_view.copyFrom(sequence);
        self.total_tokens = total;
        self.publishPagedKvViewForDecode();
        return true;
    }

    pub fn appendGeneratedToken(self: *NativeDecodeState) !void {
        return self.appendGeneratedTokenWithCleanup(.{});
    }
    pub fn appendGeneratedTokenWithCleanup(self: *NativeDecodeState, current: Cleanup) !void {
        const lock = if (self.isPaged()) self.lockPagedKv() else null;
        defer if (lock) |mutex| mutex.unlock();
        try self.appendGeneratedTokenWithKvLockHeldWithCleanup(current);
    }

    /// Graph replay holds the common KV lock before its owner mutex, through
    /// readback; page retirement follows the same lock order.
    pub fn appendGeneratedTokenWithKvLockHeld(self: *NativeDecodeState) !void {
        return self.appendGeneratedTokenWithKvLockHeldWithCleanup(.{});
    }
    pub fn appendGeneratedTokenWithKvLockHeldWithCleanup(self: *NativeDecodeState, current: Cleanup) !void {
        if (try self.tryAtomicGeneratedAppend(1, current)) return;
        self.kv_block_ids_synchronized = false;
        self.total_tokens += 1;
        if (self.isPaged()) {
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, 1);
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
            if (self.kv_storage) |storage| {
                try storage.appendTokensWithCleanup(self.sequence_id.?, 1, current);
                _ = try storage.trimSequenceToSlidingWindow(self.sequence_id.?);
            }
            try self.reservePagedKvReplayCapacity();
            try self.syncPagedKvViewForDecode();
        }
    }

    pub fn appendGeneratedTokenWithResult(self: *NativeDecodeState) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendGeneratedToken();
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Append `count` generated tokens at once (used by speculative decoding
    /// after accepting a batch of draft tokens).
    pub fn appendGeneratedTokens(self: *NativeDecodeState, count: usize) !void {
        return self.appendGeneratedTokensWithCleanup(count, .{});
    }
    pub fn appendGeneratedTokensWithCleanup(self: *NativeDecodeState, count: usize, current: Cleanup) !void {
        if (count > 0 and self.isPaged() and self.sequence_id != null) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            if (try self.tryAtomicGeneratedAppend(count, current)) return;
        }
        self.kv_block_ids_synchronized = false;
        self.total_tokens += count;
        if (self.isPaged()) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            try self.ensureAttachedUnlocked();
            try self.kv_manager.?.appendTokens(self.sequence_id.?, @intCast(count));
            _ = try self.kv_manager.?.trimSequenceToSlidingWindow(self.sequence_id.?);
            if (self.kv_storage) |storage| {
                try storage.appendTokensWithCleanup(self.sequence_id.?, @intCast(count), current);
                _ = try storage.trimSequenceToSlidingWindow(self.sequence_id.?);
            }
            try self.reservePagedKvReplayCapacity();
            if (count > 0) {
                try self.syncPagedKvViewForDecode();
            }
        }
    }

    pub fn appendGeneratedTokensWithResult(self: *NativeDecodeState, count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.appendGeneratedTokens(count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Roll back `count` tokens from the KV cache (used by speculative
    /// decoding when draft tokens are rejected).
    pub fn truncateTokens(self: *NativeDecodeState, count: usize) !void {
        return self.truncateTokensWithCleanup(count, .{});
    }
    pub fn truncateTokensWithCleanup(self: *NativeDecodeState, count: usize, current: Cleanup) !void {
        self.kv_block_ids_synchronized = false;
        if (count == 0) return;
        if (count > self.total_tokens) return error.TruncateBeyondStart;
        if (self.kv_storage) |storage| if (storage.device_write_hook) |hook| try hook.prepareMutationWithCleanup(current);
        const was_paged = self.isPaged();
        self.total_tokens -= count;
        if (was_paged) {
            const lock = self.lockPagedKv();
            defer if (lock) |mutex| mutex.unlock();
            const manager = self.kv_manager.?;
            if (self.sequence_id) |seq_id| {
                const removed = try manager.truncateSequence(seq_id, count);
                if (self.kv_storage) |storage| _ = try storage.truncateSequenceWithCleanup(seq_id, count, current);
                const prior_kv_tokens = if (self.kv_view) |view| view.token_count else 0;
                const kv_tokens = prior_kv_tokens - @min(prior_kv_tokens, removed);
                try self.syncPagedKvBlockTable();
                self.setPagedKvView(kv_tokens, self.total_tokens - kv_tokens);
            }
        }
        if (self.deepseek_v4_compressed_cache) |*cache| {
            cache.reset();
            self.force_full_recompute = true;
        }
    }

    pub fn truncateTokensWithResult(self: *NativeDecodeState, count: usize) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        try self.truncateTokens(count);
        return self.makeKvMutationResult(before_view, before_sequence_id, null);
    }

    /// Compact the KV cache using Attention Matching. Replaces the current
    /// sequence with a smaller one containing fitted K/V values that preserve
    /// attention output. Call after prefill, before the decode loop.
    pub fn compactKvCache(self: *NativeDecodeState, config: runtime.kv.compaction.CompactionConfig) !usize {
        try config.validate();
        if (self.deepseek_v4_compressed_cache != null) return error.DeepSeekV4CompressedKvCompactionNotSupported;
        if (self.kv_storage != null) return error.KvStorageCompactionNotSupported;
        const lock = self.lockPagedKv();
        defer if (lock) |mutex| mutex.unlock();
        const manager = self.kv_manager orelse return 0;
        const seq_id = self.sequence_id orelse return 0;
        const pool_id = self.pool_id orelse return error.InvalidPoolId;
        const pool = manager.getPool(pool_id) orelse return error.InvalidPoolId;

        var compacted = try runtime.kv.compaction.compactSequence(
            self.allocator,
            manager,
            seq_id,
            pool_id,
            config,
        );
        defer compacted.deinit();

        // Create new sequence and write compacted data.
        const new_seq_id = try manager.attachSequence(pool_id);
        errdefer manager.releaseSequence(new_seq_id) catch {};
        try manager.appendTokens(new_seq_id, @intCast(compacted.retained_count));
        for (0..pool.config.num_layers_packed) |layer| {
            try manager.writeFullLayerKv(
                new_seq_id,
                layer,
                compacted.retained_count,
                compacted.k_per_layer[layer],
                compacted.v_per_layer[layer],
            );
        }

        // Complete every fallible step before releasing the old sequence so
        // a failed compaction leaves the original state intact.
        const new_state = try manager.sequenceMut(new_seq_id);
        new_state.compacted = true;
        var prepared_view = try self.prepareKvView(try manager.logicalBlocksWithReservationsCount(new_seq_id));
        defer prepared_view.deinit();

        // Publish directly from the new sequence's authoritative table. Never
        // shrink view capacity or create a second full temporary view on compaction.
        self.kv_block_ids_synchronized = false;
        try manager.releaseSequence(seq_id);
        self.sequence_id = new_seq_id;
        prepared_view.copyFrom(new_seq_id);
        self.kv_compacted = true;
        self.setPagedKvView(compacted.retained_count, self.total_tokens - compacted.retained_count);
        // total_tokens stays the same for position encoding continuity.
        return compacted.retained_count;
    }

    pub fn compactKvCacheWithResult(self: *NativeDecodeState, config: runtime.kv.compaction.CompactionConfig) !KvMutationResult {
        const before_view = self.kvView();
        const before_sequence_id = self.sequence_id;
        const retained_tokens = try self.compactKvCache(config);
        return self.makeKvMutationResult(before_view, before_sequence_id, retained_tokens);
    }

    pub fn kvView(self: *const NativeDecodeState) ?KvView {
        if (self.force_full_recompute) return null;
        return self.kv_view;
    }

    pub fn gptDecodeContext(self: *NativeDecodeState, seq_len: usize, query_seq_len: usize) gpt_arch.DecodeContext {
        const decode_positions: ?[]const u32 = if (self.qwen3vl_mrope_position_delta != null and query_seq_len == 1 and seq_len > 0) blk: {
            const position = @as(i64, @intCast(seq_len - 1)) + self.qwen3vl_mrope_position_delta.?;
            if (position < 0 or position > std.math.maxInt(u32)) break :blk null;
            @memset(&self.qwen3vl_decode_positions, @intCast(position));
            break :blk &self.qwen3vl_decode_positions;
        } else null;
        if (platform.env.getenvBool("TERMITE_DISABLE_PAGED_KV") or self.force_full_recompute) {
            return .{
                .attention_mode = .full_recompute,
                .total_sequence_len = seq_len,
                .query_sequence_len = seq_len,
                .kv_sequence_len = seq_len,
                .kv_position_offset = 0,
                .moe_runtime = &self.moe_runtime,
                .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
                .gemma4_layer_spec_cache = &self.gemma4_layer_spec_cache,
                .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
                .mrope_positions = decode_positions,
                .qwen3vl_text_only = self.qwen3vl_text_only,
            };
        }
        return .{
            .attention_mode = if (self.kvView() != null)
                (if (query_seq_len < seq_len and query_seq_len == 1) .paged_decode else .paged_prefill)
            else
                .full_recompute,
            .total_sequence_len = seq_len,
            .query_sequence_len = query_seq_len,
            .kv_sequence_len = if (self.kvView()) |view| view.token_count else seq_len,
            .kv_position_offset = if (self.kvView()) |view| view.position_offset else 0,
            .kv_manager = self.kv_manager,
            .kv_storage = self.kv_storage,
            .moe_runtime = &self.moe_runtime,
            .qwen35_linear_cache = if (self.qwen35_linear_cache) |*cache| cache else null,
            .gemma4_layer_spec_cache = &self.gemma4_layer_spec_cache,
            .deepseek_v4_compressed_cache = if (self.deepseek_v4_compressed_cache) |*cache| cache else null,
            .mrope_positions = decode_positions,
            .qwen3vl_text_only = self.qwen3vl_text_only,
            .selection_query_capture = if (self.selection_capture_enabled) .{
                .ctx = @ptrCast(self),
                .allocator = self.allocator,
                .horizon = runtime.kv.selection.defaults.query_horizon,
                .captureFn = captureSelectionQueries,
            } else null,
            .kv_cache = if (self.kvView()) |view|
                .{
                    .sequence_id = view.sequence_id,
                    .pool_id = view.pool_id,
                    .logical_block_count = view.logical_block_count,
                    .tail_tokens = view.tail_tokens,
                    .position_offset = view.position_offset,
                    .max_inflight_tokens = view.max_inflight_tokens,
                    .allow_swa_ring = view.allow_swa_ring,
                    .logical_blocks = view.logical_blocks,
                    .kv_storage = view.kv_storage,
                }
            else
                null,
        };
    }
};

pub const DecodeStateRuntime = StateRuntime(.owned);
pub const BorrowedDecodeStateRuntime = StateRuntime(.borrowed);

fn StateRuntime(comptime ownership: enum { owned, borrowed }) type {
    return struct {
        const Self = @This();
        const DecodeStep = struct { seq_len: usize, decode_context: gpt_arch.DecodeContext };
        state: if (ownership == .owned) NativeDecodeState else *NativeDecodeState,

        /// A borrowed cursor never releases sequence storage; its runtime owner does.
        pub fn init(state: *NativeDecodeState) Self {
            return .{ .state = state };
        }

        pub fn makeDecodeContext(self: *Self, seq_len: usize, query_seq_len: usize) gpt_arch.DecodeContext {
            return self.makeDecodeContextForMode(seq_len, query_seq_len, null);
        }
        pub fn preparePrefill(self: *Self, seq_len: usize, query_seq_len: usize) !gpt_arch.DecodeContext {
            return self.preparePrefillForMode(seq_len, query_seq_len, null);
        }
        pub fn beginDecodeStep(self: *Self, position: usize) !DecodeStep {
            return self.beginDecodeStepForMode(position, null);
        }
        pub fn reservePrefillTo(self: *Self, target: usize) !usize {
            return self.reservePrefillToWithCleanup(target, .{});
        }
        pub fn reservePrefillToWithCleanup(self: *Self, target: usize, current: Cleanup) !usize {
            if (target < self.currentTokenCount()) return error.InvalidPrefillAdvance;
            const missing = target - self.currentTokenCount();
            if (missing != 0) try self.appendPrefillChunkWithCleanup(missing, current);
            return missing;
        }
        pub fn rollbackReservedPrefill(self: *Self, reserved: usize) !void {
            return self.rollbackReservedPrefillWithCleanup(reserved, .{});
        }
        pub fn rollbackReservedPrefillWithCleanup(self: *Self, reserved: usize, current: Cleanup) !void {
            if (reserved != 0) try self.truncateGeneratedTokensWithCleanup(reserved, current);
        }

        pub fn initPaged(
            allocator: std.mem.Allocator,
            kv_manager: *runtime.kv.manager.KvManager,
            pool_id: runtime.kv.block.KvPoolId,
            shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
        ) Self {
            return .{
                .state = NativeDecodeState.initPaged(allocator, kv_manager, pool_id, shared_moe_cache),
            };
        }

        pub fn deinitWithCleanup(self: *Self, current: Cleanup) !void {
            if (ownership == .owned) try self.state.deinitWithCleanup(current);
        }
        pub fn deinit(self: *Self) void {
            if (ownership == .owned) self.state.deinit();
        }

        pub fn notePrefillWithCleanup(self: *Self, count: usize, current: Cleanup) !void {
            try self.state.notePrefillWithCleanup(count, current);
        }
        pub fn appendPrefillChunkWithCleanup(self: *Self, count: usize, current: Cleanup) !void {
            try self.state.appendPrefillChunkWithCleanup(count, current);
        }
        pub fn appendGeneratedTokensWithCleanup(self: *Self, count: usize, current: Cleanup) !usize {
            try self.state.appendGeneratedTokensWithCleanup(count, current);
            return self.currentTokenCount();
        }
        pub fn truncateGeneratedTokensWithCleanup(self: *Self, count: usize, current: Cleanup) !void {
            try self.state.truncateTokensWithCleanup(count, current);
        }
        pub fn configureForGptConfig(self: *Self, config: gpt_mod.Config) void {
            self.state.configureForGptConfig(config);
        }

        pub fn currentTokenCount(self: *const Self) usize {
            return self.state.total_tokens;
        }

        pub fn kvView(self: *const Self) ?KvView {
            return self.state.kvView();
        }

        pub fn notePrefill(self: *Self, token_count: usize) !void {
            try self.state.notePrefill(token_count);
        }

        pub fn appendPrefillChunk(self: *Self, token_count: usize) !void {
            try self.state.appendPrefillChunk(token_count);
        }

        pub fn appendGeneratedToken(self: *Self) !usize {
            try self.state.appendGeneratedToken();
            return self.currentTokenCount();
        }

        pub fn appendGeneratedTokens(self: *Self, count: usize) !usize {
            try self.state.appendGeneratedTokens(count);
            return self.currentTokenCount();
        }

        pub fn truncateGeneratedTokens(self: *Self, count: usize) !void {
            try self.state.truncateTokens(count);
        }

        pub fn compactKvCache(self: *Self, config: runtime.kv.compaction.CompactionConfig) !void {
            _ = try self.state.compactKvCache(config);
        }

        pub fn validateDecodePosition(self: *const Self, position: usize) !void {
            if (self.currentTokenCount() != position) return error.InvalidDecodePosition;
        }

        pub fn makeDecodeContextForMode(
            self: *Self,
            seq_len: usize,
            query_seq_len: usize,
            attention_mode: ?cache_mod.AttentionMode,
        ) gpt_arch.DecodeContext {
            var decode_context = self.state.gptDecodeContext(seq_len, query_seq_len);
            if (attention_mode != null and attention_mode.? == .full_recompute) decode_context.attention_mode = .full_recompute;
            return decode_context;
        }

        pub fn preparePrefillForMode(
            self: *Self,
            seq_len: usize,
            query_seq_len: usize,
            attention_mode: ?cache_mod.AttentionMode,
        ) !gpt_arch.DecodeContext {
            return self.preparePrefillForModeWithCleanup(seq_len, query_seq_len, attention_mode, .{});
        }
        pub fn preparePrefillWithCleanup(self: *Self, seq_len: usize, query_seq_len: usize, current: Cleanup) !gpt_arch.DecodeContext {
            return self.preparePrefillForModeWithCleanup(seq_len, query_seq_len, null, current);
        }
        pub fn preparePrefillForModeWithCleanup(self: *Self, seq_len: usize, query_seq_len: usize, attention_mode: ?cache_mod.AttentionMode, current: Cleanup) !gpt_arch.DecodeContext {
            if (query_seq_len > seq_len) return error.UnsupportedShape;
            if (self.currentTokenCount() == 0) {
                if (seq_len != query_seq_len) return error.UnsupportedShape;
                try self.notePrefillWithCleanup(query_seq_len, current);
            } else {
                const expected_prior = seq_len - query_seq_len;
                if (self.currentTokenCount() != expected_prior) return error.InvalidPrefillSequence;
                try self.appendPrefillChunkWithCleanup(query_seq_len, current);
            }
            return self.makeDecodeContextForMode(seq_len, query_seq_len, attention_mode);
        }

        pub fn beginDecodeStepForMode(
            self: *Self,
            position: usize,
            attention_mode: ?cache_mod.AttentionMode,
        ) !DecodeStep {
            try self.validateDecodePosition(position);
            const seq_len = try self.appendGeneratedToken();
            return .{
                .seq_len = seq_len,
                .decode_context = self.makeDecodeContextForMode(seq_len, 1, attention_mode),
            };
        }
    };
}

test "decode state runtime paged prefill and decode step" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 16,
        .num_layers_packed = 2,
        .num_kv_heads = 2,
        .head_dim = 8,
        .sliding_window_size = null,
    });

    var ds = DecodeStateRuntime.initPaged(allocator, &manager, pool_id, null);
    defer ds.deinit();

    const prefill_ctx = try ds.preparePrefillForMode(4, 4, .paged_prefill);
    try std.testing.expectEqual(@as(usize, 4), ds.currentTokenCount());
    try std.testing.expectEqual(@as(usize, 4), prefill_ctx.kv_sequence_len);

    const step = try ds.beginDecodeStepForMode(4, .paged_decode);
    try std.testing.expectEqual(@as(usize, 5), step.seq_len);
    try std.testing.expectEqual(@as(usize, 1), step.decode_context.query_sequence_len);
}

test "decode state runtime preserves Qwen3-VL text-only M-RoPE contract" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 16,
        .num_layers_packed = 2,
        .num_kv_heads = 2,
        .head_dim = 8,
        .sliding_window_size = null,
    });

    var ds = DecodeStateRuntime.initPaged(allocator, &manager, pool_id, null);
    defer ds.deinit();
    ds.configureForGptConfig(.{ .family = .qwen3_vl });

    const prefill_ctx = try ds.preparePrefillForMode(4, 4, .paged_prefill);
    try std.testing.expect(prefill_ctx.qwen3vl_text_only);
    try std.testing.expect(prefill_ctx.mrope_positions == null);

    const step = try ds.beginDecodeStepForMode(4, .paged_decode);
    try std.testing.expect(step.decode_context.qwen3vl_text_only);
    try std.testing.expectEqualSlices(
        u32,
        &.{ 4, 4, 4 },
        step.decode_context.mrope_positions orelse return error.MissingMropePositions,
    );
}

test "native decode state compacted batched append advances the full accepted count" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 4,
        .num_kv_heads = 1,
        .head_dim = 8,
    });
    var state = NativeDecodeState.initPaged(allocator, &manager, pool_id, null);
    defer state.deinit();

    // Model a six-token prompt compacted to two retained KV entries without
    // invoking the numerical compactor; this test targets view accounting.
    try state.notePrefill(2);
    state.total_tokens = 6;
    state.kv_compacted = true;
    (try manager.sequenceMut(state.sequence_id.?)).compacted = true;
    state.setPagedKvView(2, 4);

    try state.appendGeneratedTokens(3);
    const view = state.kvView().?;
    try std.testing.expectEqual(@as(usize, 5), view.token_count);
    try std.testing.expectEqual(@as(usize, 4), view.position_offset);
    try std.testing.expectEqual(@as(usize, 9), state.total_tokens);
}

test "borrowed decode cursors share reservations without owning persistent state" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f16, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
    var owner = DecodeStateRuntime.initPaged(allocator, &manager, pool, null);
    defer owner.deinit();
    try owner.notePrefill(2);
    var cursor = BorrowedDecodeStateRuntime.init(&owner.state);
    try std.testing.expectEqual(@as(usize, 3), try cursor.reservePrefillTo(5));
    try std.testing.expectEqual(@as(usize, 5), owner.currentTokenCount());
    try std.testing.expectError(error.InvalidPrefillAdvance, cursor.reservePrefillTo(1));
    try cursor.rollbackReservedPrefill(3);
    try std.testing.expectEqual(@as(usize, 2), owner.currentTokenCount());
    try std.testing.expectError(error.UnsupportedShape, cursor.preparePrefill(1, 2));
    try std.testing.expectEqual(@as(usize, 2), owner.currentTokenCount());
    const step = try cursor.beginDecodeStep(2);
    try std.testing.expectEqual(@as(usize, 3), step.seq_len);
    cursor.deinit();
    try std.testing.expectEqual(@as(usize, 3), owner.currentTokenCount());
    const context = owner.makeDecodeContextForMode(3, 1, .full_recompute);
    try std.testing.expectEqual(gpt_arch.DecodeContext.AttentionMode.full_recompute, context.attention_mode);
}

test "decode state KV metadata uses its own distinct allocator for initial reserved growth and retirement" {
    var manager_alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var state_alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var manager = runtime.kv.manager.KvManager.init(manager_alloc.allocator());
    var manager_live = true;
    defer if (manager_live) manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 });
    var state = NativeDecodeState.initPaged(state_alloc.allocator(), &manager, pool, null);
    var state_live = true;
    defer if (state_live) state.deinit();
    try state.notePrefill(3);
    const sequence = state.sequence_id orelse return error.MissingActualKvSequence;
    try manager.reserveTokenCapacity(sequence, 33);
    try state.syncPagedKvBlockTable();
    try std.testing.expect(state.kv_block_ids.items.len >= 17);
    try std.testing.expect(state_alloc.allocations > 0);
    // Existing manager-owned wrapper retains its original allocator contract.
    {
        var legacy: std.ArrayListUnmanaged(runtime.kv.block.KvBlockId) = .empty;
        defer legacy.deinit(manager_alloc.allocator());
        const state_allocations = state_alloc.alloc_index;
        _ = try manager.logicalBlocksWithReservations(sequence, &legacy);
        try std.testing.expectEqualSlices(runtime.kv.block.KvBlockId, state.kv_block_ids.items, legacy.items);
        try std.testing.expectEqual(state_allocations, state_alloc.alloc_index);
    }
    try state.deinitWithCleanup(.{ .outside_execution = true });
    state_live = false;
    try std.testing.expectEqual(state_alloc.allocated_bytes, state_alloc.freed_bytes);
    try std.testing.expectEqual(state_alloc.allocations, state_alloc.deallocations);
    manager.deinit();
    manager_live = false;
    try std.testing.expectEqual(manager_alloc.allocated_bytes, manager_alloc.freed_bytes);
    try std.testing.expectEqual(manager_alloc.allocations, manager_alloc.deallocations);
}

test "attached generated append stages both owners and published view before metadata allocation failure" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |paired| {
        for (0..2) |failure_step| {
            var manager_alloc = std.testing.FailingAllocator.init(a, .{});
            var storage_alloc = std.testing.FailingAllocator.init(a, .{});
            var state_alloc = std.testing.FailingAllocator.init(a, .{});
            var manager = runtime.kv.manager.KvManager.init(manager_alloc.allocator());
            defer manager.deinit();
            const config = runtime.kv.pool.KvPoolConfig{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 };
            const pool = try manager.addPool(config);
            var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(storage_alloc.allocator(), config);
            defer storage.deinit();
            var state = NativeDecodeState.initPaged(state_alloc.allocator(), &manager, pool, null);
            state.kv_storage = if (paired) &storage else null;
            defer state.deinit();
            try state.notePrefill(2);
            const sequence = state.sequence_id.?;
            const old_view = state.kvView().?;
            try std.testing.expect(state.kv_block_ids_synchronized);
            const old_blocks = try a.dupe(runtime.kv.block.KvBlockId, state.kv_block_ids.items);
            defer a.free(old_blocks);
            const count = (state.kv_block_ids.capacity + 1) * 2;
            // Fail replacement payload first, then retirement-owner list metadata.
            state_alloc.fail_index = state_alloc.alloc_index + failure_step;
            try std.testing.expectError(error.OutOfMemory, state.appendGeneratedTokens(count));
            try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
            try std.testing.expectEqual(@as(usize, 2), manager.tokenCount(sequence).?);
            if (paired) try std.testing.expectEqual(@as(usize, 2), storage.tokenCount(sequence).?);
            try std.testing.expectEqual(old_view.logical_blocks.?.ptr, state.kvView().?.logical_blocks.?.ptr);
            try std.testing.expectEqualSlices(runtime.kv.block.KvBlockId, old_blocks, state.kv_block_ids.items);
            try std.testing.expectEqual(old_view.token_count, state.kvView().?.token_count);
            try std.testing.expect(state.kv_block_ids_synchronized);
            state_alloc.fail_index = std.math.maxInt(usize);
            try state.appendGeneratedTokens(count);
            try std.testing.expectEqual(@as(usize, 1), state.retired_kv_block_ids.items.len);
            try std.testing.expectEqual(old_view.logical_blocks.?.ptr, state.retired_kv_block_ids.items[0].items.ptr);
            try std.testing.expectEqualSlices(runtime.kv.block.KvBlockId, old_blocks, old_view.logical_blocks.?);
            try std.testing.expectEqual(2 + count, state.total_tokens);
            try std.testing.expectEqual(2 + count, manager.tokenCount(sequence).?);
            if (paired) try std.testing.expectEqual(2 + count, storage.tokenCount(sequence).?);
            // First enter a unique partial page, then prove the hot tail append
            // allocates neither plan/page/view metadata nor replacement storage.
            try state.appendGeneratedToken();
            const manager_calls = manager_alloc.alloc_index;
            const storage_calls = storage_alloc.alloc_index;
            const state_calls = state_alloc.alloc_index;
            try state.appendGeneratedToken();
            try std.testing.expectEqual(manager_calls, manager_alloc.alloc_index);
            try std.testing.expectEqual(storage_calls, storage_alloc.alloc_index);
            try std.testing.expectEqual(state_calls, state_alloc.alloc_index);
            try std.testing.expectEqual(4 + count, state.total_tokens);
            try std.testing.expectEqual(4 + count, state.kvView().?.token_count);
            try std.testing.expect(state.kv_block_ids_synchronized);
        }
    }
}

test "failed legacy block table synchronization cannot authorize generated append copy elision" {
    const a = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 });
    var state_alloc = std.testing.FailingAllocator.init(a, .{});
    var state = NativeDecodeState.initPaged(state_alloc.allocator(), &manager, pool, null);
    defer state.deinit();
    try state.notePrefill(1);
    try std.testing.expect(state.kv_block_ids_synchronized);
    const sequence = state.sequence_id.?;
    try manager.reserveTokenCapacity(sequence, (state.kv_block_ids.capacity + 1) * 2);
    state_alloc.fail_index = state_alloc.alloc_index;
    try std.testing.expectError(error.OutOfMemory, state.syncPagedKvBlockTable());
    try std.testing.expect(!state.kv_block_ids_synchronized);
    state_alloc.fail_index = std.math.maxInt(usize);
    // A partial unique-tail append keeps manager IDs unchanged. The failed
    // previous copy must still force authoritative table reconstruction.
    try state.appendGeneratedToken();
    try std.testing.expect(state.kv_block_ids_synchronized);
    var expected: std.ArrayListUnmanaged(runtime.kv.block.KvBlockId) = .empty;
    defer expected.deinit(a);
    _ = try manager.logicalBlocksWithReservations(sequence, &expected);
    try std.testing.expectEqualSlices(runtime.kv.block.KvBlockId, expected.items, state.kv_block_ids.items);
}

test "paired generated append publishes all owners before allocator retirement callbacks" {
    const a = std.testing.allocator;
    const Probe = struct {
        manager: ?*runtime.kv.manager.KvManager = null,
        storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
        state: ?*NativeDecodeState = null,
        armed: bool = false,
        observed_half_commit: bool = false,
        spill_frees: usize = 0,
        fn observe(self: *@This(), bytes: usize) void {
            if (!self.armed) return;
            const state = self.state.?;
            const sequence = state.sequence_id.?;
            const manager_tokens = self.manager.?.tokenCount(sequence).?;
            const storage_tokens = self.storage.?.tokenCount(sequence).?;
            const view_tokens = if (state.kvView()) |view| view.token_count else 0;
            const old = manager_tokens == 20 and storage_tokens == 20 and state.total_tokens == 20 and view_tokens == 20;
            const committed = manager_tokens == 4 and storage_tokens == 4 and state.total_tokens == 24 and view_tokens == 4;
            if (!old and !committed) self.observed_half_commit = true;
            if (bytes == 10 * @sizeOf(runtime.kv.block.KvBlockId)) self.spill_frees += 1;
        }
    };
    const ObservedAllocator = struct {
        backing: std.mem.Allocator,
        probe: *Probe,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn resize(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawResize(buffer, alignment, len, ra);
        }
        fn remap(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawRemap(buffer, alignment, len, ra);
        }
        fn free(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.probe.observe(buffer.len);
            self.backing.rawFree(buffer, alignment, ra);
        }
    };
    var probe = Probe{};
    var manager_alloc = ObservedAllocator{ .backing = a, .probe = &probe };
    var storage_alloc = ObservedAllocator{ .backing = a, .probe = &probe };
    var manager = runtime.kv.manager.KvManager.init(manager_alloc.allocator());
    defer manager.deinit();
    const config = runtime.kv.pool.KvPoolConfig{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2, .sliding_window_size = 4 };
    const pool = try manager.addPool(config);
    var storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(storage_alloc.allocator(), config);
    defer storage.deinit();
    var state = NativeDecodeState.initPaged(a, &manager, pool, null);
    state.kv_storage = &storage;
    defer state.deinit();
    try state.notePrefill(20);
    probe.manager = &manager;
    probe.storage = &storage;
    probe.state = &state;
    probe.armed = true;
    defer probe.armed = false;
    try state.appendGeneratedTokens(4);
    probe.armed = false;
    try std.testing.expect(probe.spill_frees >= 1);
    try std.testing.expect(!probe.observed_half_commit);
    try std.testing.expectEqual(@as(usize, 4), manager.tokenCount(state.sequence_id.?).?);
    try std.testing.expectEqual(@as(usize, 4), storage.tokenCount(state.sequence_id.?).?);
    try std.testing.expectEqual(@as(usize, 24), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 4), state.kvView().?.token_count);
}

test "legacy prefill metadata allocation failure cannot publish new token count with old block IDs" {
    const a = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 });
    var state_alloc = std.testing.FailingAllocator.init(a, .{});
    var state = NativeDecodeState.initPaged(state_alloc.allocator(), &manager, pool, null);
    defer state.deinit();
    try state.notePrefill(2);
    const old_view = state.kvView().?;
    state_alloc.fail_index = state_alloc.alloc_index;
    try std.testing.expectError(error.OutOfMemory, state.appendPrefillChunk(4));
    // Legacy prefill remains nontransactional, but its metadata error is now
    // terminal: no successful return can dispatch a new count with old IDs.
    try std.testing.expect(!state.kv_block_ids_synchronized);
    try std.testing.expectEqual(old_view.token_count, state.kvView().?.token_count);
    try std.testing.expectEqual(old_view.logical_blocks.?.ptr, state.kvView().?.logical_blocks.?.ptr);
    try std.testing.expectEqual(@as(usize, 6), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 6), manager.tokenCount(state.sequence_id.?).?);
}
