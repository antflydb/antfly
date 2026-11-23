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

//! EG2-to-CUDA capture recipe. Configuration interpretation and exact native
//! attention/RMS compatibility stay here; retained SDK lifetimes are shared.
const std = @import("std");
const Context = @import("../ops/cuda/context.zig").CudaContext;
const Buffer = @import("../ops/cuda/buffer.zig").DeviceBuffer;
const driver = @import("../ops/cuda/driver.zig");
const memory = @import("../runtime/tier/memory.zig");
const prepared = @import("../ops/cuda/prepared_cache.zig");
const CacheQuota = @import("../ops/cuda/cache_quota.zig").Quota;
const Cleanup = @import("../execution/cleanup_context.zig").Context;
const model = @import("../models/embedding_gemma2.zig");
const model_cache = @import("../models/embedding_gemma2_weight_cache.zig");
const EncoderConfig = model.Config;
const EncoderAttention = @import("../ops/cuda/encoder_multimodal.zig");
const EncoderPolicy = @import("../ops/cuda/embedding_gemma2_policy.zig").Policy;
pub const EncoderRouteState = struct {
    cudnn_enabled: bool = false,
    cudnn_unavailable: bool = false,
    cudnn_initialized: bool = false,
    scratch_bytes: usize = 0,
    scratch_limit: usize = 0,
    matmul_arithmetic: @import("../ops/cuda/libraries.zig").MatmulArithmetic = .full_f32,
    matmul_workspace_bytes: usize = 0,
};
fn encoderRmsLaunchesMatch(batch: u32, physical_sequence: u32, previous_sequence: u32, current_sequence: u32, config: EncoderConfig) !bool {
    const Launch = @import("../ops/kernel_geometry.zig").RmsReductionLaunch;
    const physical = try std.math.mul(u32, batch, physical_sequence);
    const previous = try std.math.mul(u32, batch, previous_sequence);
    const current = try std.math.mul(u32, batch, current_sequence);
    if (!std.meta.eql(try Launch.init(.contiguous_four_lane, physical, previous, config.hidden_size), try Launch.init(.contiguous_four_lane, physical, current, config.hidden_size))) return false;
    const layers = std.math.cast(u32, config.num_hidden_layers) orelse return error.InvalidEmbeddingGemma2AttentionSchedule;
    if (!std.meta.eql(try Launch.init(.contiguous_four_lane, try std.math.mul(u32, physical, layers), try std.math.mul(u32, previous, layers), config.hidden_size), try Launch.init(.contiguous_four_lane, try std.math.mul(u32, physical, layers), try std.math.mul(u32, current, layers), config.hidden_size))) return false;
    for (0..config.num_hidden_layers) |index| {
        const lc = try config.layer(index);
        for ([_]u32{ lc.num_attention_heads, lc.num_key_value_heads }) |heads| {
            if (!std.meta.eql(try Launch.init(.contiguous_four_lane, try std.math.mul(u32, physical, heads), try std.math.mul(u32, previous, heads), lc.head_dim), try Launch.init(.contiguous_four_lane, try std.math.mul(u32, physical, heads), try std.math.mul(u32, current, heads), lc.head_dim))) return false;
        }
    }
    return true;
}
const Recipe = struct {
    pub const packed_limit = model_cache.execution_packed_limit;
    pub const arena_limit = model_cache.execution_arena_limit;
    pub const graph_rows_limit = model_cache.execution_graph_rows_limit;
    pub const invalid_geometry = error.InvalidEmbeddingGemma2AttentionSchedule;
    pub const Geometry = struct { batch: u32, sequence: u32 };
    pub const Capture = struct { config: EncoderConfig, policy: EncoderPolicy, route: EncoderRouteState };
    pub fn acceptsArena(kind: anytype, batch: usize, sequence: usize) bool {
        return batch != 0 and batch <= (if (kind == .encoder) @as(usize, 4) else 8) and sequence != 0 and sequence <= model.official_max_input_tokens and batch <= @This().graph_rows_limit / sequence;
    }
    pub fn encoderGeometry(kind: anytype, batch: usize, sequence: usize) !Geometry {
        if (kind != .encoder or !acceptsArena(kind, batch, sequence)) return invalid_geometry;
        return .{ .batch = std.math.cast(u32, batch) orelse return invalid_geometry, .sequence = std.math.cast(u32, sequence) orelse return invalid_geometry };
    }
    pub fn validateCapture(capture: Capture) !void {
        const config = capture.config;
        if (config.num_hidden_layers == 0 or config.num_hidden_layers > config.layer_types.len or config.num_hidden_layers > config.per_layer_head_dim.len or config.num_hidden_layers > config.per_layer_num_key_value_heads.len or config.layer_types_len != config.num_hidden_layers) return invalid_geometry;
    }
    pub fn compatible(geometry: Geometry, old: AttentionSource, source: AttentionSource, previous: Capture, current: Capture, multiprocessors: i32) !bool {
        const config = current.config;
        if (!std.meta.eql(previous, current) or old.all_valid != source.all_valid or old.mask_kind != source.mask_kind) return false;
        const previous_sequence = std.math.cast(u32, old.sequence) orelse return invalid_geometry;
        const source_sequence = std.math.cast(u32, source.sequence) orelse return invalid_geometry;
        if (!try encoderRmsLaunchesMatch(geometry.batch, geometry.sequence, previous_sequence, source_sequence, config)) return false;
        for (0..config.num_hidden_layers) |index| {
            const lc = try config.layer(index);
            const shape = EncoderAttention.Attention{ .batch = geometry.batch, .seq = geometry.sequence, .q_heads = lc.num_attention_heads, .kv_heads = lc.num_key_value_heads, .head_dim = lc.head_dim, .local_radius = if (lc.kind == .sliding_attention) config.sliding_window else 0 };
            var before = try EncoderAttention.AttentionProgram.forResolvedMask(shape, previous_sequence, old.all_valid, multiprocessors);
            const after = try EncoderAttention.AttentionProgram.forResolvedMask(shape, source_sequence, source.all_valid, multiprocessors);
            before.source_sequence = after.source_sequence;
            if (!std.meta.eql(before, after)) return false;
        }
        return true;
    }
    pub fn extraEntryBytes(request: prepared.Arena) !usize {
        const ple = switch (try encoderPleBank(request)) {
            .whole_source => |size| size,
            .legacy_optional => 0,
        };
        return std.math.add(usize, ple, try sourceVisionAttentionBytes(request));
    }
};
const Implementation = @import("../ops/cuda/retained_execution_cache.zig").Program(Recipe);
pub const Arena = Implementation.Arena;
pub const ArenaPreview = Implementation.ArenaPreview;
pub const Cache = Implementation.Cache;
pub const AttentionSource = Implementation.AttentionSource;
pub const LogicalUpload = Implementation.LogicalUpload;
pub const packed_limit = Implementation.packed_limit;
pub const arena_limit = Implementation.arena_limit;
pub const graph_rows_limit = Implementation.graph_rows_limit;
pub const logicalUploadHostBytes = Implementation.logicalUploadHostBytes;
pub const encoderPleBytes = Implementation.layerInputBytes;
pub const encoderMaskBytes = Implementation.maskStorageBytes;

pub fn prepareAttentionSource(arena: *Arena, ctx: *Context, source: AttentionSource, config: EncoderConfig, policy: EncoderPolicy, route: EncoderRouteState) !void {
    return arena.prepareAttentionSource(ctx, source, .{ .config = config, .policy = policy, .route = route });
}
pub fn noteAttentionSource(arena: *Arena, source: AttentionSource, config: EncoderConfig, policy: EncoderPolicy, route: EncoderRouteState) void {
    arena.noteAttentionSource(source, .{ .config = config, .policy = policy, .route = route });
}
const EncoderPleBank = union(enum) {
    legacy_optional,
    whole_source: usize,
};
/// The extra entry allowance is the exact official producer bank, never a
/// caller-supplied arbitrary amount. Other planes/kinds keep the old bound.
fn encoderPleBank(request: prepared.Arena) !EncoderPleBank {
    if (request.kind != .encoder) return .legacy_optional;
    const rows = try std.math.mul(usize, request.batch, request.sequence);
    const bytes = try encoderPleBytes(rows, model.official_hidden_size, model.official_layer_count);
    return if (bytes != 0 and request.sizes[10] == bytes) .{ .whole_source = bytes } else .legacy_optional;
}
/// Only the exact fixed score/probability plane extends a source vision entry.
/// Library workspace stays in the old base allowance. Every bank still pays
/// its full retained charge and the unchanged four-arena total limit.
fn sourceVisionAttentionBytes(request: prepared.Arena) !usize {
    if (request.kind != .vision or request.mask_kind != 1 or request.sizes[10] == 0) return 0;
    const logical = try std.math.mul(usize, request.grid_x, request.grid_y);
    const plan = @import("../ops/cuda/embedding_gemma2_vision.zig").Plan{ .sequence = logical, .source_sequence = request.sequence, .grid_x = request.grid_x, .grid_y = request.grid_y };
    try plan.validate();
    const base = (try @import("../ops/cuda/bf16_tiled_attention.zig").WorkspaceLayout.resolve(try plan.physicalSequence(), @import("../ops/cuda/embedding_gemma2_vision.zig").head_count, 0)).bytes;
    if (request.sizes[10] < base) return error.InvalidCudaState;
    return base;
}

test "embeddinggemma2 execution cache bounds cohort graph buckets and packed retention" {
    try std.testing.expectEqual(@as(usize, 8), @as(Cache, .{}).arenas.len);
    try std.testing.expect(arena_limit * 4 + packed_limit <= 1024 * 1024 * 1024);
}

test "embeddinggemma2 execution cache retains shared admission and releases graphs before storage" {
    const Mock = struct {
        var next: usize = 256;
        var frees: usize = 0;
        var graphs: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            frees += 1;
            return 0;
        }
        fn graph(_: driver.CUgraphExec) callconv(.c) driver.CUresult {
            graphs += 1;
            return 0;
        }
    };
    Mock.next = 256;
    Mock.frees = 0;
    Mock.graphs = 0;
    var controller = memory.AdmissionController{};
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.driver.fns.cuGraphExecDestroy = Mock.graph;
    var cache = Cache{};
    defer cache.clear(std.testing.allocator, &ctx) catch unreachable;
    try std.testing.expect((try cache.arena(&ctx, 4, 1025, 0, @splat(16))) == null);
    const first = (try cache.arena(&ctx, 1, 64, 1, @splat(16))).?;
    try std.testing.expect(first == (try cache.arena(&ctx, 1, 64, 1, @splat(16))).?);
    try std.testing.expectError(error.InvalidCudaState, cache.arena(&ctx, 1, 64, 1, @splat(32)));
    const pointer = first.buffers[0].ptr;
    first.graph = @ptrFromInt(3);
    const cohort = (try cache.arena(&ctx, 1, 64, 2, @splat(16))).?;
    try std.testing.expect(first != cohort);
    try std.testing.expectEqual(pointer, first.buffers[0].ptr);
    _ = (try cache.allocatePacked(std.testing.allocator, &ctx, "qkv", 512)).?;
    try std.testing.expectEqual(@as(usize, 864), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(2 * @sizeOf(LogicalUpload), controller.snapshot().host_scratch_bytes);
    // The second owner has physical room but must respect shared pressure.
    var other = Cache{};
    defer other.clear(std.testing.allocator, &ctx) catch unreachable;
    try std.testing.expect((try other.allocatePacked(std.testing.allocator, &ctx, "gate", 256)) == null);
    try std.testing.expectEqual(@as(usize, 864), ctx.device_allocations.snapshot().live);
    try cache.clear(std.testing.allocator, &ctx);
    try std.testing.expectEqual(@as(usize, 1), Mock.graphs);
    try std.testing.expectEqual(@as(usize, 23), Mock.frees);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    // Actual shared host admission refuses the new retained metadata BEFORE
    // a device bank is acquired, while the original complete quota stays exact.
    const before_denial = controller.snapshot();
    const allocations_before_denial = ctx.device_allocations.snapshot();
    ctx.video_cache_budget.admission.?.limits.host_limit_bytes = @sizeOf(LogicalUpload) - 1;
    try std.testing.expect((try cache.arena(&ctx, 1, 64, 1, @splat(16))) == null);
    try std.testing.expectEqualDeep(before_denial, controller.snapshot());
    try std.testing.expectEqualDeep(allocations_before_denial, ctx.device_allocations.snapshot());
    ctx.video_cache_budget.admission.?.limits.host_limit_bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    _ = (try other.allocatePacked(std.testing.allocator, &ctx, "gate", 256)).?;
    try other.discardPacked(std.testing.allocator, &ctx, "gate");
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    const large = (try cache.arena(&ctx, 4, 1024, 1, @splat(16))).?;
    try std.testing.expect(large == (try cache.arena(&ctx, 4, 1024, 1, @splat(16))).?);
    const smaller_batch = (try cache.arena(&ctx, 2, 1024, 1, @splat(16))).?;
    try std.testing.expect(large != smaller_batch);
    const vision = (try cache.visionArena(&ctx, 2, 32, 32, @splat(16))).?;
    try std.testing.expect(vision != smaller_batch);
    vision.geometry_ready = true;
    try std.testing.expect(vision == (try cache.visionArena(&ctx, 2, 32, 32, @splat(16))).?);
    try std.testing.expect(vision.geometry_ready);
    const rotated = (try cache.visionArena(&ctx, 2, 64, 16, @splat(16))).?;
    try std.testing.expect(vision != rotated and !rotated.geometry_ready);
    try std.testing.expectEqual(@as(usize, 704), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect((try cache.visionArena(&ctx, 8, 32, 32, @splat(16))) == null);
}

test "embeddinggemma2 graph cancellation keeps capture retryable" {
    var entry = Arena{ .warm = true };
    entry.captureFailed(error.Cancelled);
    try std.testing.expect(!entry.disabled and !entry.warm);
    entry.warm = true;
    entry.captureFailed(error.Timeout);
    try std.testing.expect(!entry.disabled and !entry.warm);
    entry.captureFailed(error.CudaGraphCaptureUnsafeHostCopy);
    try std.testing.expect(entry.disabled);
}

test "embeddinggemma2 execution cache releases admission after a lost context is destroyed" {
    var controller = memory.AdmissionController{};
    const lease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 8 }, false);
    var cache = Cache{};
    const name = try std.testing.allocator.dupe(u8, "lost");
    try cache.weights.put(std.testing.allocator, name, .{ .buffer = .{ .ptr = 1, .len = 8 }, .quota = CacheQuota.fromRawAdmission(lease) });
    try std.testing.expectEqual(@as(usize, 8), controller.snapshot().backend_scratch_bytes);
    cache.releaseAfterContextDestroyed(std.testing.allocator);
    cache.releaseAfterContextDestroyed(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.weights.capacity());
}

test "embeddinggemma2 graph selection measures completed warm routes with bounded exploration" {
    var entry = Arena{};
    try std.testing.expect(entry.preferReplay());
    entry.recordCompleted(true, 100);
    entry.recordCompleted(true, 100);
    try std.testing.expect(!entry.preferReplay());
    entry.recordCompleted(false, 50);
    entry.recordCompleted(false, 50);
    try std.testing.expect(!entry.preferReplay());
    entry.timed_uses = 64;
    try std.testing.expect(entry.preferReplay());
    entry.resetTiming();
    entry.recordCompleted(true, 40);
    entry.recordCompleted(true, 40);
    entry.recordCompleted(false, 100);
    entry.recordCompleted(false, 100);
    try std.testing.expect(entry.preferReplay());
}

test "embeddinggemma2 graph retention keeps mixed stages and fences only replaced arenas" {
    const Mock = struct {
        var next: usize = 256;
        var barriers: usize = 0;
        var waits: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            barriers += 1;
            return 0;
        }
        fn alloc(p: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            p.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return 0;
        }
        fn event(p: *driver.CUevent, _: c_uint) callconv(.c) driver.CUresult {
            p.* = @ptrFromInt(next);
            next += 256;
            return 0;
        }
        fn record(_: driver.CUevent, _: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn wait(_: driver.CUevent) callconv(.c) driver.CUresult {
            waits += 1;
            return 0;
        }
        fn destroy(_: driver.CUevent) callconv(.c) driver.CUresult {
            return 0;
        }
    };
    Mock.next = 256;
    Mock.barriers = 0;
    Mock.waits = 0;
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 1024 * 1024 };
    ctx.video_cache_budget = .{};
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.driver.fns.cuEventCreate = Mock.event;
    ctx.driver.fns.cuEventRecord = Mock.record;
    ctx.driver.fns.cuEventSynchronize = Mock.wait;
    ctx.driver.fns.cuEventDestroy = Mock.destroy;
    var cache = Cache{};
    defer cache.clear(std.testing.allocator, &ctx) catch unreachable;
    var sizes: [11]usize = @splat(0);
    sizes[0] = 16;
    var first: usize = 0;
    for (1..9) |seq| {
        const slot = (try cache.arena(&ctx, 1, seq, 0, sizes)).?;
        if (seq == 1) first = slot.buffers[0].ptr;
        slot.noteUse(&ctx);
    }
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    try std.testing.expectEqual(@as(usize, 0), Mock.waits);
    try std.testing.expectEqual(first, (try cache.arena(&ctx, 1, 1, 0, sizes)).?.buffers[0].ptr);
    _ = try cache.visionArena(&ctx, 1, 3, 3, sizes);
    try std.testing.expectEqual(@as(usize, 1), Mock.waits);
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    try std.testing.expectEqual(first, (try cache.arena(&ctx, 1, 1, 0, sizes)).?.buffers[0].ptr);
}

test "arena cold chronology uses same selector for full and partial batches without mutating original cache" {
    const cache: Cache = .{};
    var cursor = ArenaPreview.fromCache(&cache);
    const full: prepared.Arena = .{ .kind = .vision, .batch = 4, .sequence = 16, .grid_x = 4, .grid_y = 4, .sizes = .{ 1024, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    var partial = full;
    partial.batch = 1;
    partial.sizes[0] = 256;
    try std.testing.expectEqualDeep(try cache.selectArena(full), try cursor.select(full));
    const first = (try cursor.advance(full)).?;
    try std.testing.expect(!first.hit and first.index == 0 and first.victims == 0);
    for (0..2) |_| {
        const next = (try cursor.advance(full)).?;
        try std.testing.expect(next.hit and next.index == first.index);
    }
    const last = (try cursor.advance(partial)).?;
    try std.testing.expect(!last.hit and last.index == 1 and last.victims == 0);
    try std.testing.expectEqual(@as(u64, 3), cursor.arenas[first.index].uses);
    try std.testing.expectEqual(@as(u64, 4), cursor.clock);
    try std.testing.expectEqualDeep(Cache{}, cache);
    var invalid = full;
    invalid.batch = 0;
    try std.testing.expect(try cursor.advance(invalid) == null);
    try std.testing.expectEqual(@as(u64, 4), cursor.clock);
}

test "arena cold chronology preserves hot-shape aging and exact aggregate victims" {
    var cache: Cache = .{ .clock = 100 };
    for (&cache.arenas, 0..) |*entry, index| {
        entry.kind = .vision;
        entry.batch = 1;
        entry.sequence = 16;
        entry.grid_x = 10 + index;
        entry.grid_y = 1;
        entry.buffers[0].len = 16; // Pure selection fixture; no SDK owner exists.
        entry.last_used = 100;
        entry.uses = 2;
    }
    var cursor = ArenaPreview.fromCache(&cache);
    const request: prepared.Arena = .{ .kind = .vision, .batch = 4, .sequence = 16, .grid_x = 4, .grid_y = 4, .sizes = .{ 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    for (0..3) |_| try std.testing.expect(try cursor.advance(request) == null);
    const aged = (try cursor.advance(request)).?;
    try std.testing.expect(!aged.hit and aged.index == 0 and aged.victims == 1);
    try std.testing.expectEqual(@as(u64, 104), cursor.clock);
    try std.testing.expectEqual(@as(u64, 100), cache.clock);
    var bounded: Cache = .{ .clock = 20 };
    for (&bounded.arenas, 0..) |*entry, index| {
        entry.batch = 1;
        entry.sequence = index + 1;
        entry.buffers[0].len = arena_limit / 2;
        entry.last_used = index + 1;
        entry.uses = 1;
    }
    var replacement = ArenaPreview.fromCache(&bounded);
    var wide = request;
    wide.sizes[0] = arena_limit;
    const first = (try replacement.advance(wide)).?;
    try std.testing.expectEqual(@as(u8, 3), first.victims);
    // Four distinct one-limit arenas fit the exact existing four-limit cache.
    for (1..4) |index| {
        wide.grid_x += 1;
        const selected = (try replacement.advance(wide)).?;
        try std.testing.expectEqual(index, selected.index);
    }
    wide.grid_x += 1;
    try std.testing.expectError(error.UnsupportedPreparedArenaChronology, replacement.advance(wide));
}

test "CUDA retained cache purposes reach uniform report across checked free failure and warm replay ownership" {
    const roots = @import("../execution/device_allocation_registry.zig");
    const Purpose = @import("../ops/cuda/allocation_budget.zig").OwnedPurpose;
    const Mock = struct {
        var next: usize = 256;
        var fail_free: bool = false;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return if (fail_free) 1 else 0;
        }
    };
    Mock.next = 256;
    Mock.fail_free = false;
    var controller = memory.AdmissionController{};
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 4096 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    var registry: roots.Registry = undefined;
    registry.begin(std.testing.allocator);
    var registry_live = true;
    defer {
        if (registry_live) _ = registry.end();
    }
    defer ctx.device_allocations.releaseObservation();
    ctx.device_allocations.registerObservation();
    var cache: Cache = .{};
    defer {
        Mock.fail_free = false;
        cache.clear(std.testing.allocator, &ctx) catch unreachable;
    }
    const packed_buffer = (try cache.allocatePacked(std.testing.allocator, &ctx, "telemetry.packed", 96)).?;
    try std.testing.expectEqual(Purpose.model_cache, packed_buffer.allocation_purpose);
    Mock.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, cache.clear(std.testing.allocator, &ctx));
    const failed = registry.snapshot().reports[0];
    try std.testing.expectEqual(@as(u64, 96), failed.live_bytes);
    try std.testing.expectEqual(@as(u64, 96), failed.purpose_live_bytes[@backingInt(Purpose.model_cache)]);
    try std.testing.expectEqual(@as(u64, 0), failed.frees);
    try std.testing.expectEqual(packed_buffer.ptr, cache.findPacked("telemetry.packed").?.ptr);
    Mock.fail_free = false;
    const arena = (try cache.arena(&ctx, 1, 16, 0, @splat(16))).?;
    for (arena.buffers) |buffer| try std.testing.expectEqual(Purpose.model_cache, buffer.allocation_purpose);
    const allocations = ctx.device_allocations.snapshot().allocations;
    try std.testing.expect(arena == (try cache.arena(&ctx, 1, 16, 0, @splat(16))).?);
    try std.testing.expectEqual(allocations, ctx.device_allocations.snapshot().allocations);
    var unknown = try Buffer.alloc(&ctx, 32);
    defer _ = unknown.freeChecked(&ctx) catch unreachable;
    var command = try Buffer.allocWithPurpose(&ctx, 64, .command_workspace);
    defer _ = command.freeChecked(&ctx) catch unreachable;
    const live = registry.snapshot().reports[0];
    try std.testing.expectEqual(@as(u64, 272), live.purpose_live_bytes[@backingInt(Purpose.model_cache)]);
    try std.testing.expectEqual(@as(u64, 32), live.purpose_live_bytes[@backingInt(Purpose.unclassified)]);
    try std.testing.expectEqual(@as(u64, 64), live.purpose_live_bytes[@backingInt(Purpose.command_workspace)]);
    try cache.clear(std.testing.allocator, &ctx);
    _ = try unknown.freeChecked(&ctx);
    _ = try command.freeChecked(&ctx);
    const summary = registry.end();
    registry_live = false;
    try std.testing.expectEqual(@as(u64, 0), summary.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 272), summary.reports[0].purpose_interval_peak_bytes[@backingInt(Purpose.model_cache)]);
    var diagnostics = @import("../execution/diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    diagnostics.publish(.{ .runtime = .{ .device_allocator_roots = summary } });
    const retained = diagnostics.snapshot().?.report.?.device_allocator_roots.?;
    diagnostics.configure(false);
    try std.testing.expectEqualDeep(summary, retained);
    try std.testing.expectEqual(.exact_authored_owning_lifetimes_with_explicit_unclassified_remainder, retained.reports[0].semantic_lifetime_classes);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "encoder logical attention signatures do not alter physical bank selection" {
    // Actual selector metadata only; no fake device allocation or graph handle.
    var cache: Cache = .{};
    var request: prepared.Arena = .{ .kind = .encoder, .batch = 1, .sequence = 192, .mask_kind = 1, .sizes = @splat(0) };
    request.sizes[0] = 192 * 8;
    cache.arenas[0].sequence = request.sequence;
    cache.arenas[0].batch = request.batch;
    cache.arenas[0].mask_kind = request.mask_kind;
    cache.arenas[0].buffers[0].len = request.sizes[0];
    const first = AttentionSource{ .sequence = 177, .all_valid = true };
    const next = AttentionSource{ .sequence = 178, .all_valid = true };
    try std.testing.expect(!cache.arenas[0].attentionSourceMatches(first));
    cache.arenas[0].attention_source = first;
    try std.testing.expect(cache.arenas[0].attentionSourceMatches(first));
    try std.testing.expect(!cache.arenas[0].attentionSourceMatches(next));
    const before = (try cache.selectArena(request)).?;
    cache.arenas[0].attention_source = next;
    const after = (try cache.selectArena(request)).?;
    try std.testing.expect(before.hit and after.hit and before.index == after.index);
    try std.testing.expectEqualDeep(before, after);
    try std.testing.expect(cache.arenas[0].attentionSourceMatches(next));
    try std.testing.expect(!cache.arenas[0].attentionSourceMatches(.{ .sequence = next.sequence, .all_valid = false }));
    const preview = ArenaPreview.fromCache(&cache);
    try std.testing.expectEqualDeep(after, (try preview.select(request)).?);
}

test "CUDA encoder graph refresh preserves physical banks across logical source transitions" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!@import("../ops/cuda/artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    const Attention = @import("../ops/cuda/encoder_multimodal.zig");
    var module = try Attention.Module.init(&ctx);
    defer module.deinit(&ctx);
    var cache: Cache = .{};
    defer cache.clear(std.testing.allocator, &ctx) catch @panic("CUDA arena cleanup failed");
    const shape = Attention.Attention{ .batch = 1, .seq = 192, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 };
    const first = try Attention.AttentionProgram.forResolvedMask(shape, 177, true, ctx.info.multiprocessor_count);
    const next = try Attention.AttentionProgram.forResolvedMask(shape, 178, true, ctx.info.multiprocessor_count);
    try std.testing.expectEqual(first.split_count, next.split_count);
    const scratch_bytes = (try first.workspace()).total_bytes;
    try std.testing.expectEqual(scratch_bytes, (try next.workspace()).total_bytes);
    const q_elements = 192 * 4 * 256;
    const kv_elements = 192 * 2 * 256;
    var sizes: [11]usize = @splat(0);
    sizes[0] = q_elements * 2;
    sizes[1] = kv_elements * 2;
    sizes[2] = kv_elements * 2;
    sizes[3] = q_elements * 2;
    sizes[4] = try encoderMaskBytes(192);
    sizes[5] = scratch_bytes;
    const entry = (try cache.arena(&ctx, 1, 192, 1, sizes)) orelse return error.TestUnexpectedResult;
    const a = std.testing.allocator;
    const zeros = try a.alloc(u16, q_elements);
    defer a.free(zeros);
    @memset(zeros, 0);
    const values = try a.alloc(u16, kv_elements);
    defer a.free(values);
    @memset(values, 0);
    // The new logical tail has the only nonzero V row. Replaying a stale177
    // graph with the178 mask therefore produces0 instead of BF16(32/178).
    @memset(values[177 * 2 * 256 ..][0 .. 2 * 256], 0x4200);
    const output = try a.alloc(u16, q_elements);
    defer a.free(output);
    // Keep host upload/readback arrays alive even on a later driver failure.
    defer ctx.synchronize() catch @panic("CUDA test upload retirement failed");
    try entry.buffers[0].copyFromHost(&ctx, std.mem.sliceAsBytes(zeros));
    try entry.buffers[1].copyFromHost(&ctx, std.mem.sliceAsBytes(zeros[0..kv_elements]));
    try entry.buffers[2].copyFromHost(&ctx, std.mem.sliceAsBytes(values));
    var mask: [192]i64 = @splat(0);
    @memset(mask[0..177], 1);
    try entry.buffers[4].copyFromHost(&ctx, std.mem.sliceAsBytes(&mask));
    const Fixture = struct {
        fn capture(context: *Context, m: *Attention.Module, arena: *Arena, program: Attention.AttentionProgram) !void {
            var recording = false;
            defer if (recording) {
                if (context.endStreamCapture()) |graph| context.destroyGraph(graph) else |_| {}
            };
            try context.beginStreamCapture(driver.CU_STREAM_CAPTURE_MODE_RELAXED);
            recording = true;
            try m.launchLocalFlashAttention(context, arena.buffers[3], arena.buffers[0], arena.buffers[1], arena.buffers[2], arena.buffers[4], program, arena.buffers[5]);
            const graph = try context.endStreamCapture();
            recording = false;
            defer context.destroyGraph(graph);
            arena.graph = try context.instantiateGraph(graph);
            arena.attention_source = .{ .sequence = program.source_sequence, .all_valid = true };
            arena.warm = true;
        }
        fn alternate(context: *Context, m: *Attention.Module, arena: *Arena, program: Attention.AttentionProgram, config: EncoderConfig) !Arena.GraphAction {
            const source: AttentionSource = .{ .sequence = program.source_sequence, .all_valid = true };
            try prepareAttentionSource(arena, context, source, config, .{}, .{});
            try arena.uploadLogical(context, program.logical_metadata, source);
            const action = arena.graphAction();
            switch (action) {
                .eager => {
                    try m.launchLocalFlashAttention(context, arena.buffers[3], arena.buffers[0], arena.buffers[1], arena.buffers[2], arena.buffers[4], program, arena.buffers[5]);
                    arena.warm = true;
                },
                .capture => {
                    try @This().capture(context, m, arena, program);
                    try arena.launch(context);
                },
                .replay => try arena.launch(context),
            }
            noteAttentionSource(arena, source, config, .{}, .{});
            arena.noteUse(context);
            return action;
        }
        fn check(context: *Context, arena: *Arena, host: []u16, sequence: usize, expected: u16) !void {
            try arena.buffers[3].copyToHost(context, std.mem.sliceAsBytes(host));
            try context.synchronize();
            for (host, 0..) |value, i| try std.testing.expectEqual(if (i / (4 * 256) < sequence) expected else @as(u16, 0), value);
        }
    };
    try Fixture.capture(&ctx, &module, entry, first);
    try ctx.launchGraph(entry.graph);
    try Fixture.check(&ctx, entry, output, 177, 0);
    const before = ctx.device_allocations.snapshot();
    var pointers: [11]driver.CUdeviceptr = undefined;
    for (entry.buffers, &pointers) |bank, *pointer| pointer.* = bank.ptr;
    mask[177] = 1;
    try entry.buffers[4].copyFromHost(&ctx, std.mem.sliceAsBytes(&mask));
    // Actual old captured scalar arguments remain177 despite the new mask.
    try ctx.launchGraph(entry.graph);
    try Fixture.check(&ctx, entry, output, 178, 0);
    const same = (try cache.arena(&ctx, 1, 192, 1, sizes)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(same == entry);
    try std.testing.expect(!entry.attentionSourceMatches(.{ .sequence = 178, .all_valid = true }));
    try entry.invalidateGraph(&ctx); // SAME receiver used by production.
    try std.testing.expect(entry.graph == null and !entry.warm and entry.attention_source == null);
    try std.testing.expectEqual(before.allocations, ctx.device_allocations.snapshot().allocations);
    try std.testing.expectEqual(before.frees, ctx.device_allocations.snapshot().frees);
    try std.testing.expectEqual(before.live, ctx.device_allocations.snapshot().live);
    for (entry.buffers, pointers) |bank, pointer| try std.testing.expectEqual(pointer, bank.ptr);
    try Fixture.capture(&ctx, &module, entry, next);
    try ctx.launchGraph(entry.graph);
    try Fixture.check(&ctx, entry, output, 178, 0x3e38); // independently BF16(32/178)
    try std.testing.expect(entry.attentionSourceMatches(.{ .sequence = 178, .all_valid = true }));
    try std.testing.expectEqual(before.allocations, ctx.device_allocations.snapshot().allocations);
    for (entry.buffers, pointers) |bank, pointer| try std.testing.expectEqual(pointer, bank.ptr);
    // SAME production topology/graph-action receiver: logical177 warms,
    // logical178 captures, then alternating177/178 replays the SAME graph.
    // This is availability through real Cache state, not manual recapture.
    var config = std.mem.zeroes(EncoderConfig);
    config.hidden_size = 512;
    config.num_hidden_layers = 1;
    config.layer_types_len = 1;
    config.layer_types[0] = .sliding_attention;
    config.num_attention_heads = 4;
    config.num_key_value_heads = 2;
    config.head_dim = 256;
    config.sliding_window = 512;
    try entry.invalidateGraph(&ctx);
    const graph_submissions_before = ctx.stats.graph_submissions;
    var dynamic_first = first;
    var dynamic_next = next;
    const metadata = Buffer{ .ptr = entry.buffers[4].ptr + 192 * 8, .len = 8 };
    dynamic_first.logical_metadata = metadata;
    dynamic_next.logical_metadata = metadata;
    mask[177] = 0;
    try entry.buffers[4].copyFromHost(&ctx, std.mem.sliceAsBytes(&mask));
    try std.testing.expectEqual(Arena.GraphAction.eager, try Fixture.alternate(&ctx, &module, entry, dynamic_first, config));
    try Fixture.check(&ctx, entry, output, 177, 0);
    mask[177] = 1;
    try entry.buffers[4].copyFromHost(&ctx, std.mem.sliceAsBytes(&mask));
    try std.testing.expectEqual(Arena.GraphAction.capture, try Fixture.alternate(&ctx, &module, entry, dynamic_next, config));
    try Fixture.check(&ctx, entry, output, 178, 0x3e38);
    const captured = entry.graph;
    try std.testing.expect(captured != null);
    for ([_]usize{ 177, 178, 177, 178 }) |logical| {
        mask[177] = if (logical == 178) 1 else 0;
        try entry.buffers[4].copyFromHost(&ctx, std.mem.sliceAsBytes(&mask));
        const program = if (logical == 178) dynamic_next else dynamic_first;
        try std.testing.expectEqual(Arena.GraphAction.replay, try Fixture.alternate(&ctx, &module, entry, program, config));
        try Fixture.check(&ctx, entry, output, logical, if (logical == 178) 0x3e38 else 0);
        try std.testing.expectEqual(captured, entry.graph);
    }
    try std.testing.expectEqual(before.allocations, ctx.device_allocations.snapshot().allocations);
    try std.testing.expectEqual(before.frees, ctx.device_allocations.snapshot().frees);
    for (entry.buffers, pointers) |bank, pointer| try std.testing.expectEqual(pointer, bank.ptr);
    try std.testing.expectEqual(graph_submissions_before + 5, ctx.stats.graph_submissions);
    // Actual original mask policy changes invalidate topology before dispatch.
    try prepareAttentionSource(entry, &ctx, .{ .sequence = 178, .all_valid = false, .mask_kind = 1 }, config, .{}, .{});
    try std.testing.expect(entry.graph == null and !entry.warm);

    const Failure = struct {
        fn launch(_: driver.CUgraphExec, _: driver.CUstream) callconv(.c) driver.CUresult {
            return 1;
        }
    };
    try Fixture.capture(&ctx, &module, entry, next);
    {
        const launch = ctx.driver.fns.cuGraphLaunch;
        ctx.driver.fns.cuGraphLaunch = Failure.launch;
        defer ctx.driver.fns.cuGraphLaunch = launch;
        @import("../test_error_logs.zig").expectErrorLogs(1);
        try std.testing.expectError(error.CudaDriverError, entry.launch(&ctx));
    }
    try std.testing.expect(entry.graph == null and !entry.warm and entry.disabled);
    for (entry.buffers, pointers) |bank, pointer| try std.testing.expectEqual(pointer, bank.ptr);
    try std.testing.expectEqual(before.allocations, ctx.device_allocations.snapshot().allocations);
    try std.testing.expectEqual(before.frees, ctx.device_allocations.snapshot().frees);
}

test "encoder retained logical metadata rejects topology changes without borrowing configuration" {
    // Only the null-graph pure metadata receiver runs here; no CUDA calls.
    var ctx: Context = undefined;
    ctx.info.multiprocessor_count = 17;
    var config = std.mem.zeroes(EncoderConfig);
    config.hidden_size = 512;
    config.num_hidden_layers = 1;
    config.layer_types_len = 1;
    config.layer_types[0] = .sliding_attention;
    config.num_attention_heads = 4;
    config.num_key_value_heads = 2;
    config.head_dim = 256;
    config.sliding_window = 512;
    var upload: LogicalUpload = .{};
    var entry: Arena = .{ .sequence = 192, .batch = 1, .logical_upload = &upload, .warm = true };
    const first: AttentionSource = .{ .sequence = 177, .all_valid = true };
    const next: AttentionSource = .{ .sequence = 178, .all_valid = true };
    noteAttentionSource(&entry, first, config, .{ .graph_replay = true }, .{});
    entry.eager_samples = 1;
    try prepareAttentionSource(&entry, &ctx, next, config, .{ .graph_replay = true }, .{});
    try std.testing.expect(entry.warm); // next logical request can capture
    try std.testing.expectEqual(Arena.GraphAction.capture, entry.graphAction());
    try std.testing.expectEqual(@as(u8, 0), entry.eager_samples);
    try std.testing.expectEqual(@as(usize, 177), entry.attention_source.?.sequence); // not published before execution
    var changed = config;
    changed.rms_norm_eps = 0.01;
    try prepareAttentionSource(&entry, &ctx, next, changed, .{ .graph_replay = true }, .{});
    try std.testing.expect(!entry.warm and entry.attention_source == null);
    noteAttentionSource(&entry, first, config, .{ .graph_replay = true }, .{});
    entry.warm = true;
    try prepareAttentionSource(&entry, &ctx, next, config, .{}, .{});
    try std.testing.expect(!entry.warm);
    noteAttentionSource(&entry, first, config, .{ .graph_replay = true }, .{});
    entry.warm = true;
    try prepareAttentionSource(&entry, &ctx, next, config, .{ .graph_replay = true }, .{ .cudnn_enabled = true });
    try std.testing.expect(!entry.warm);
    noteAttentionSource(&entry, first, config, .{ .graph_replay = true }, .{ .matmul_arithmetic = .bf16_reduced, .matmul_workspace_bytes = 32 * 1024 * 1024 });
    entry.warm = true;
    try prepareAttentionSource(&entry, &ctx, next, config, .{ .graph_replay = true, .matmul_arithmetic = .full_f32 }, .{ .matmul_arithmetic = .full_f32, .matmul_workspace_bytes = 0 });
    try std.testing.expect(!entry.warm and entry.attention_source == null);
    try std.testing.expectEqual(@as(usize, 192 * 8 + 8), try encoderMaskBytes(192));
    try std.testing.expectEqual(@sizeOf(LogicalUpload), logicalUploadHostBytes(.encoder));
    try std.testing.expectEqual(@as(usize, 0), logicalUploadHostBytes(.audio));
    try std.testing.expectError(error.Overflow, encoderMaskBytes(std.math.maxInt(usize)));
}

test "encoder logical metadata public receivers reject malformed geometry before SDK or mutation" {
    var ctx: Context = undefined; // no field may be read on these rejection paths
    const config = std.mem.zeroes(EncoderConfig);
    const source: AttentionSource = .{ .sequence = 177, .all_valid = true };
    var upload: LogicalUpload = .{ .values = .{ 71, 2 } };
    const cases = [_]Arena{
        .{ .sequence = 192, .batch = 0 },
        .{ .sequence = 192, .batch = std.math.maxInt(usize) },
        .{ .sequence = 0, .batch = 1 },
        .{ .sequence = std.math.maxInt(usize), .batch = 1 },
        .{ .sequence = 8192, .batch = 4 },
        .{ .kind = .vision, .sequence = 192, .batch = 1 },
    };
    for (cases) |value| {
        var entry = value;
        entry.logical_upload = &upload;
        entry.attention_source = source;
        entry.warm = true;
        try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, prepareAttentionSource(&entry, &ctx, source, config, .{}, .{}));
        try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, entry.uploadLogical(&ctx, .{ .ptr = 16, .len = 8 }, source));
        try std.testing.expect(entry.warm and entry.attention_source.?.eql(source));
        try std.testing.expectEqualSlices(u32, &.{ 71, 2 }, &upload.values);
    }
    var valid: Arena = .{ .sequence = 192, .batch = 1, .logical_upload = &upload, .attention_source = source, .warm = true };
    var excessive = config;
    excessive.num_hidden_layers = @intCast(excessive.layer_types.len + 1);
    excessive.layer_types_len = @intCast(excessive.layer_types.len + 1);
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, prepareAttentionSource(&valid, &ctx, source, excessive, .{}, .{}));
    excessive.num_hidden_layers = 1;
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, prepareAttentionSource(&valid, &ctx, source, excessive, .{}, .{}));
    try std.testing.expect(valid.warm and valid.attention_source.?.eql(source));
    try std.testing.expectEqualSlices(u32, &.{ 71, 2 }, &upload.values);
}

test "encoder graph source reuse requires exact source RMS launch geometry" {
    var config = std.mem.zeroes(EncoderConfig);
    config.hidden_size = 512;
    config.num_hidden_layers = 24;
    config.layer_types_len = 24;
    config.num_attention_heads = 4;
    config.num_key_value_heads = 2;
    config.head_dim = 256;
    try std.testing.expect(try encoderRmsLaunchesMatch(1, 192, 177, 178, config));
    try std.testing.expect(!try encoderRmsLaunchesMatch(1, 16, 1, 2, config));
    try std.testing.expect(try encoderRmsLaunchesMatch(1, 16, 2, 3, config));
    try std.testing.expect(!try encoderRmsLaunchesMatch(1, 16, 3, 4, config));
    try std.testing.expect(try encoderRmsLaunchesMatch(1, 16, 3, 3, config));
}

test "encoder whole PLE allowance funds exact source bank without expanding other arena or total limits" {
    var preview: ArenaPreview = .{};
    var request: prepared.Arena = .{ .kind = .encoder, .batch = 1, .sequence = 4096, .mask_kind = 0, .sizes = @splat(0) };
    request.sizes[0] = arena_limit;
    request.sizes[10] = try encoderPleBytes(4096, 512, 24);
    const selected = (try preview.select(request)).?;
    try std.testing.expectEqual(arena_limit + 96 * 1024 * 1024, selected.bytes);
    var malformed = request;
    malformed.sizes[10] += 2;
    try std.testing.expect((try preview.select(malformed)) == null);
    malformed = request;
    malformed.sizes[0] += 1;
    try std.testing.expect((try preview.select(malformed)) == null);
    malformed = request;
    malformed.kind = .vision;
    try std.testing.expect((try preview.select(malformed)) == null);
    // Independent earlier entries still count toward the original 512-MiB
    // retained ceiling; a new bank is not borrowed physical credit.
    preview.arenas[0] = .{ .sequence = 1, .batch = 1, .sizes = .{ 4 * arena_limit, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, .has_backing = true, .uses = 2, .last_used = 0 };
    try std.testing.expect((try preview.select(request)) == null);
}

test "source vision fixed attention plane extends only its entry and preserves total retained ceiling" {
    var preview: ArenaPreview = .{};
    var request: prepared.Arena = .{ .kind = .vision, .batch = 1, .sequence = 2520, .grid_x = 60, .grid_y = 39, .mask_kind = 1, .sizes = @splat(0) };
    const fixed = (try @import("../ops/cuda/bf16_tiled_attention.zig").WorkspaceLayout.resolve(2520, 12, 0)).bytes;
    request.sizes[0] = arena_limit;
    request.sizes[10] = fixed;
    const selected = (try preview.select(request)).?;
    try std.testing.expectEqual(arena_limit + fixed, selected.bytes);
    var malformed = request;
    malformed.sizes[10] += 1; // No caller-supplied extra byte becomes allowance.
    try std.testing.expect((try preview.select(malformed)) == null);
    malformed = request;
    malformed.mask_kind = 0;
    try std.testing.expect((try preview.select(malformed)) == null);
    malformed = request;
    malformed.grid_x = 0;
    try std.testing.expectError(error.InvalidEmbeddingGemma2VisionShape, preview.select(malformed));
    // One independently retained arena cannot provide free source-bank credit.
    preview.arenas[0] = .{ .sequence = 1, .batch = 1, .sizes = .{ 4 * arena_limit, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, .has_backing = true, .uses = 2, .last_used = 0 };
    try std.testing.expect((try preview.select(request)) == null);
}

test "encoder replay invalidates when attention or BLAS workspace moves" {
    var arena = Arena{};
    arena.setAttentionWorkspaces(11, 22, 7, 33);
    try std.testing.expect(arena.attentionWorkspacesMatch(11, 22, 7, 33));
    try std.testing.expect(!arena.attentionWorkspacesMatch(12, 22, 7, 33));
    try std.testing.expect(!arena.attentionWorkspacesMatch(11, 23, 7, 33));
    try std.testing.expect(!arena.attentionWorkspacesMatch(11, 22, 8, 33));
    try std.testing.expect(!arena.attentionWorkspacesMatch(11, 22, 7, 34));
}

/// Statically bound model composition; tuned kernels and prepared buffer
/// ownership remain in the CUDA provider. This adds no runtime dispatch.
pub fn runEncoder(comptime Operations: type, self: anytype, module: *EncoderAttention.Module, policy: EncoderPolicy, config: EncoderConfig, batch: usize, encoder_sequence: usize, source_sequence: usize, all_attention_tokens_valid: bool, mask: Buffer, sequence_lengths: Buffer, immutable: Buffer, hidden_buf: Buffer, a: Buffer, bbuf: Buffer, c: Buffer, d: Buffer, joined: Buffer, ple: Buffer, full: Buffer, attention_workspace: Buffer, logical_metadata: Buffer, control: ?@import("../execution_control.zig").InferenceExecutionControl) !void {
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const rows = batch * encoder_sequence;
    const Compute = @TypeOf(self);
    const weight = struct {
        fn f(compute: Compute, name: []const u8) !Buffer {
            return Operations.weight(compute, name);
        }
    }.f;
    // Preserve the original full output width before BF16 scale and source
    // normalization. The retained bank is independent of optional packing.
    const ple_width = try Operations.checkedMul(hidden, config.num_hidden_layers);
    const ple_rows = try Operations.checkedMul(rows, config.num_hidden_layers);
    const reference_rows = try Operations.checkedMul(batch, source_sequence);
    const ple_reference_rows = try Operations.checkedMul(reference_rows, config.num_hidden_layers);
    if (control) |value| try value.check();
    try Operations.matmul(self, module, ple, immutable, try weight(self, @import("../architectures/embedding_gemma2_reference.zig").WeightNames.ple_projection), rows, hidden, ple_width, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
    try module.launchScale(&self.ctx, ple, ple, @intCast(try Operations.checkedMul(rows, ple_width)), 1.0 / @sqrt(@as(f32, @floatFromInt(hidden))));
    // RMS consumes each entire row before its first store; rows and stores
    // are disjoint, so the same admitted bank safely holds normalized values.
    try module.launchRmsNormScheduled(&self.ctx, ple, ple, try weight(self, @import("../architectures/embedding_gemma2_reference.zig").WeightNames.ple_norm), @intCast(ple_rows), @intCast(ple_reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
    var name_buf: [256]u8 = undefined;
    for (0..config.num_hidden_layers) |layer_index| {
        if (control) |value| try value.check();
        const lc = try config.layer(layer_index);
        const qw = lc.queryWidth();
        const kvw = lc.keyValueWidth();
        const in_norm = try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "input_layernorm.weight"));
        try module.launchRmsNormScheduled(&self.ctx, a, hidden_buf, in_norm, @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
        var projection_names: [3][256]u8 = undefined;
        const packed_qkv = try Operations.projection(self, module, policy, .{ bbuf, c, d }, a, .{
            try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&projection_names[0], layer_index, "self_attn.q_proj.weight"),
            try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&projection_names[1], layer_index, "self_attn.k_proj.weight"),
            try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&projection_names[2], layer_index, "self_attn.v_proj.weight"),
        }, .{ qw, kvw, kvw }, rows, hidden, joined, true, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        const ash = EncoderAttention.Attention{ .batch = @intCast(batch), .seq = @intCast(encoder_sequence), .q_heads = lc.num_attention_heads, .kv_heads = lc.num_key_value_heads, .head_dim = lc.head_dim, .local_radius = if (lc.kind == .sliding_attention) config.sliding_window else 0 };
        if (packed_qkv) {
            var qname: [256]u8 = undefined;
            try module.launchPackedHeads(&self.ctx, a, bbuf, c, joined, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&qname, layer_index, "self_attn.q_norm.weight")), try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "self_attn.k_norm.weight")), mask, .{}, @intCast(rows), @intCast(encoder_sequence), lc.num_attention_heads, lc.num_key_value_heads, lc.head_dim, config.rms_norm_eps, lc.rope_theta, false, 1);
        } else {
            try module.launchRmsNormScheduled(&self.ctx, a, bbuf, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "self_attn.q_norm.weight")), @intCast(rows * lc.num_attention_heads), @intCast(reference_rows * lc.num_attention_heads), lc.head_dim, config.rms_norm_eps, .contiguous_four_lane);
            try module.launchRmsNormScheduled(&self.ctx, bbuf, c, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "self_attn.k_norm.weight")), @intCast(rows * lc.num_key_value_heads), @intCast(reference_rows * lc.num_key_value_heads), lc.head_dim, config.rms_norm_eps, .contiguous_four_lane);
            try module.launchRmsNormScheduled(&self.ctx, c, d, null, @intCast(rows * lc.num_key_value_heads), @intCast(reference_rows * lc.num_key_value_heads), lc.head_dim, config.rms_norm_eps, .contiguous_four_lane);
            try module.launchTrainingRope(&self.ctx, a, bbuf, mask, ash, lc.rope_theta, 1.0);
        }
        try Operations.attention(self, module, policy, d, a, bbuf, c, mask, sequence_lengths, ash, @intCast(source_sequence), all_attention_tokens_valid, attention_workspace, logical_metadata, control);
        try Operations.matmul(self, module, a, d, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "self_attn.o_proj.weight")), rows, qw, hidden, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        if (policy.fused_backbone) {
            var post_name: [256]u8 = undefined;
            try module.launchNormResidual(&self.ctx, hidden_buf, a, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&post_name, layer_index, "post_attention_layernorm.weight")), try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "pre_feedforward_layernorm.weight")), null, @intCast(rows), @intCast(hidden), config.rms_norm_eps);
        } else {
            try module.launchRmsNormScheduled(&self.ctx, bbuf, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "post_attention_layernorm.weight")), @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
            try module.launchResidual(&self.ctx, hidden_buf, hidden_buf, bbuf, null, @intCast(rows * hidden), 1, 0);
            try module.launchRmsNormScheduled(&self.ctx, a, hidden_buf, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "pre_feedforward_layernorm.weight")), @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
        }
        const packed_mlp = try Operations.projection(self, module, policy, .{ bbuf, c, .{} }, a, .{
            try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&projection_names[0], layer_index, "mlp.gate_proj.weight"),
            try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&projection_names[1], layer_index, "mlp.up_proj.weight"),
            "",
        }, .{ intermediate, intermediate, 0 }, rows, hidden, joined, true, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        if (packed_mlp) try module.launchPackedGelu(&self.ctx, d, joined, @intCast(rows), @intCast(intermediate)) else try module.launchGeluMul(&self.ctx, d, bbuf, c, @intCast(rows * intermediate));
        try Operations.matmul(self, module, a, d, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "mlp.down_proj.weight")), rows, intermediate, hidden, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        if (policy.fused_backbone) {
            try module.launchNormResidual(&self.ctx, hidden_buf, .{}, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "post_feedforward_layernorm.weight")), null, null, @intCast(rows), @intCast(hidden), config.rms_norm_eps);
        } else {
            try module.launchRmsNormScheduled(&self.ctx, bbuf, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "post_feedforward_layernorm.weight")), @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
            try module.launchResidual(&self.ctx, hidden_buf, hidden_buf, bbuf, null, @intCast(rows * hidden), 1, 0);
        }
        try self.kernels.launchCopyPackedBf16Columns(&self.ctx, bbuf, ple, rows, ple_width, layer_index * hidden, hidden);
        try Operations.matmul(self, module, a, hidden_buf, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "ple_block.per_layer_input_gate.weight")), rows, hidden, hidden, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        try module.launchGeluPle(&self.ctx, c, a, bbuf, @intCast(rows * hidden));
        try Operations.matmul(self, module, a, c, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "ple_block.per_layer_projection.weight")), rows, hidden, hidden, .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
        if (policy.fused_backbone) {
            var scalar_name: [256]u8 = undefined;
            try module.launchNormResidual(&self.ctx, hidden_buf, .{}, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "ple_block.post_per_layer_input_norm.weight")), null, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&scalar_name, layer_index, "layer_scalar")), @intCast(rows), @intCast(hidden), config.rms_norm_eps);
        } else {
            try module.launchRmsNormScheduled(&self.ctx, bbuf, a, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "ple_block.post_per_layer_input_norm.weight")), @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
            try module.launchResidual(&self.ctx, hidden_buf, hidden_buf, bbuf, null, @intCast(rows * hidden), 1, 0);
            try module.launchScaleDevice(&self.ctx, hidden_buf, hidden_buf, try weight(self, try @import("../architectures/embedding_gemma2_reference.zig").layerWeightName(&name_buf, layer_index, "layer_scalar")), @intCast(rows * hidden));
        }
    }
    try module.launchRmsNormScheduled(&self.ctx, a, hidden_buf, try weight(self, @import("../architectures/embedding_gemma2_reference.zig").WeightNames.final_norm), @intCast(rows), @intCast(reference_rows), @intCast(hidden), config.rms_norm_eps, .contiguous_four_lane);
    try Operations.matmul(self, module, bbuf, a, try weight(self, @import("../architectures/embedding_gemma2_reference.zig").WeightNames.output_projection), rows, hidden, config.outputDim(), .{ .arithmetic = policy.matmul_arithmetic, .workspace = attention_workspace });
    try module.launchMeanL2(&self.ctx, full, bbuf, mask, @intCast(batch), @intCast(encoder_sequence), config.outputDim());
}
