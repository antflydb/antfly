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

//! Bounded model-owned buffers. Graph addresses never enter the temporary pool;
//! shared admission remains held until synchronized eviction or model unload.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");
const prepared = @import("prepared_cache.zig");
const CacheQuota = @import("cache_quota.zig").Quota;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
pub const packed_limit = 512 * 1024 * 1024;
pub const arena_limit = 128 * 1024 * 1024;
pub const graph_rows_limit = 4096;
const EncoderConfig = @import("../../models/embedding_gemma2.zig").Config;
const EncoderAttention = @import("embedding_gemma2.zig");
const EncoderPolicy = @import("embedding_gemma2_policy.zig").Policy;
pub const EncoderRouteState = struct {
    cudnn_enabled: bool = false,
    cudnn_unavailable: bool = false,
    cudnn_initialized: bool = false,
    scratch_bytes: usize = 0,
    scratch_limit: usize = 0,
};
pub const LogicalUpload = struct {
    values: [2]u32 = .{ 0, 0 },
    captured_config: ?EncoderConfig = null,
    captured_policy: EncoderPolicy = .{},
    captured_route: EncoderRouteState = .{},
};
pub fn logicalUploadHostBytes(kind: @FieldType(prepared.Arena, "kind")) usize {
    return if (kind == .encoder) @sizeOf(LogicalUpload) else 0;
}
pub fn encoderMaskBytes(rows: usize) !usize {
    return std.math.add(usize, try std.math.mul(usize, rows, 8), 8);
}
pub const AttentionSource = struct {
    sequence: usize,
    all_valid: bool,
    mask_kind: u8 = 0,
    pub fn eql(self: AttentionSource, other: AttentionSource) bool {
        return self.sequence == other.sequence and self.all_valid == other.all_valid and self.mask_kind == other.mask_kind;
    }
};

pub const Arena = struct {
    kind: enum { encoder, vision, audio } = .encoder,
    grid_x: usize = 0,
    grid_y: usize = 0,
    geometry_ready: bool = false,
    sequence: usize = 0,
    batch: usize = 0,
    mask_kind: u8 = 0,
    attention_workspace: usize = 0,
    /// Captured scalar arguments, separate from the reusable physical banks.
    attention_source: ?AttentionSource = null,
    logical_upload: ?*LogicalUpload = null,
    weight_generation: usize = 0,
    buffers: [11]Buffer = @splat(.{}),
    quota: CacheQuota = .none,
    graph: driver.CUgraphExec = null,
    warm: bool = false,
    disabled: bool = false,
    eager_samples: u8 = 0,
    replay_samples: u8 = 0,
    eager_ns: u64 = 0,
    replay_ns: u64 = 0,
    timed_uses: u64 = 0,
    last_used: u64 = 0,
    uses: u64 = 0,
    timing: ?@import("context.zig").ProfileEventPair = null,
    timing_replay: ?bool = null,
    completion: driver.CUevent = null,
    pending: bool = false,
    fence_valid: bool = false,
    pub fn invalidateGraph(self: *Arena, ctx: *Context) !void {
        // Bank allocation and quota stay untouched. Publish no replacement
        // captured-argument stamp until the actual eager/capture path succeeds.
        try ctx.destroyGraphExecChecked(&self.graph);
        self.warm = false;
        self.attention_source = null;
        self.resetTiming();
    }
    pub fn attentionSourceMatches(self: *const Arena, source: AttentionSource) bool {
        return if (self.attention_source) |captured| captured.eql(source) else false;
    }
    fn encoderGeometry(self: *const Arena) !struct { batch: u32, sequence: u32 } {
        if (self.kind != .encoder or self.batch == 0 or self.batch > 4 or self.sequence == 0 or self.sequence > 8192 or self.batch > graph_rows_limit / self.sequence) return error.InvalidEmbeddingGemma2AttentionSchedule;
        return .{ .batch = std.math.cast(u32, self.batch) orelse return error.InvalidEmbeddingGemma2AttentionSchedule, .sequence = std.math.cast(u32, self.sequence) orelse return error.InvalidEmbeddingGemma2AttentionSchedule };
    }
    /// Full value comparison, not a digest: all captured nonattention scalar
    /// configuration and policy must stay equal. Each attention retains its
    /// exact algorithm/splits/merge/workspace; only logical key/query bounds move.
    pub fn prepareAttentionSource(self: *Arena, ctx: *Context, source: AttentionSource, config: EncoderConfig, policy: EncoderPolicy, route: EncoderRouteState) !void {
        const geometry = try self.encoderGeometry();
        if (config.num_hidden_layers == 0 or config.num_hidden_layers > config.layer_types.len or config.num_hidden_layers > config.per_layer_head_dim.len or config.num_hidden_layers > config.per_layer_num_key_value_heads.len or config.layer_types_len != config.num_hidden_layers) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (source.sequence == 0 or source.sequence > self.sequence or source.mask_kind > 2 or source.all_valid != (source.mask_kind == 0)) return error.InvalidEmbeddingGemma2AttentionSchedule;
        const source_sequence = std.math.cast(u32, source.sequence) orelse return error.InvalidEmbeddingGemma2AttentionSchedule;
        var compatible = false;
        if (self.attention_source) |old| if (self.logical_upload) |upload| if (upload.captured_config) |captured| {
            compatible = std.meta.eql(captured, config) and std.meta.eql(upload.captured_policy, policy) and std.meta.eql(upload.captured_route, route) and old.all_valid == source.all_valid and old.mask_kind == source.mask_kind;
            if (compatible) for (0..config.num_hidden_layers) |index| {
                const lc = try config.layer(index);
                const shape = EncoderAttention.Attention{ .batch = geometry.batch, .seq = geometry.sequence, .q_heads = lc.num_attention_heads, .kv_heads = lc.num_key_value_heads, .head_dim = lc.head_dim, .local_radius = if (lc.kind == .sliding_attention) config.sliding_window else 0 };
                var previous = try EncoderAttention.AttentionProgram.forResolvedMask(shape, std.math.cast(u32, old.sequence) orelse return error.InvalidEmbeddingGemma2AttentionSchedule, old.all_valid, ctx.info.multiprocessor_count);
                const current = try EncoderAttention.AttentionProgram.forResolvedMask(shape, source_sequence, source.all_valid, ctx.info.multiprocessor_count);
                previous.source_sequence = current.source_sequence;
                if (!std.meta.eql(previous, current)) {
                    compatible = false;
                    break;
                }
            };
        };
        if (!compatible) try self.invalidateGraph(ctx) else if (!self.attentionSourceMatches(source)) self.resetTiming();
    }
    pub fn noteAttentionSource(self: *Arena, source: AttentionSource, config: EncoderConfig, policy: EncoderPolicy, route: EncoderRouteState) void {
        self.attention_source = source;
        if (self.logical_upload) |upload| {
            upload.captured_config = config;
            upload.captured_policy = policy;
            upload.captured_route = route;
        }
    }
    /// The host cell survives cache moves and every asynchronous upload. Drain
    /// the actual previous use BEFORE rewriting it; no ring/stack address is saved.
    pub fn uploadLogical(self: *Arena, ctx: *Context, device: Buffer, source: AttentionSource) !void {
        _ = try self.encoderGeometry();
        const source_sequence = std.math.cast(u32, source.sequence) orelse return error.InvalidEmbeddingGemma2AttentionSchedule;
        const upload = self.logical_upload orelse return error.InvalidCudaState;
        if (device.ptr == 0 or device.len != 8 or device.ptr % 4 != 0 or source.sequence == 0 or source.sequence > self.sequence or source.mask_kind > 2 or source.all_valid != (source.mask_kind == 0)) return error.InvalidCudaState;
        if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeHostCopy;
        try self.drain(ctx);
        upload.values = .{ source_sequence, source.mask_kind };
        // Even an upload error may have queued work: retirement falls back to
        // the actual stream barrier until noteUse publishes its event.
        self.pending = true;
        self.fence_valid = false;
        device.copyFromHost(ctx, std.mem.asBytes(&upload.values)) catch |err| {
            self.attention_source = null;
            self.warm = false;
            return err;
        };
    }
    fn sizesMatch(self: *const Arena, sizes: [11]usize) bool {
        for (self.buffers, sizes) |buffer, size| if (buffer.len != size) return false;
        return true;
    }
    pub fn hasBacking(self: *const Arena) bool {
        return self.bytes() != 0 or self.logical_upload != null or self.graph != null or self.completion != null or self.timing != null or self.quota.hasLease();
    }
    pub fn noteUse(self: *Arena, ctx: *Context) void {
        self.pending = true;
        self.fence_valid = false;
        if (self.completion == null) self.completion = ctx.createStreamEvent() catch return;
        ctx.recordEvent(self.completion) catch return;
        self.fence_valid = true;
    }
    fn drain(self: *Arena, ctx: *Context) !void {
        if (!self.pending) return;
        if (self.fence_valid) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.synchronizeEventResult(self.completion));
        } else try ctx.synchronize();
        self.pending = false;
    }
    pub fn bytes(self: *const Arena) usize {
        var total: usize = 0;
        for (self.buffers) |b| total += b.len;
        return total;
    }
    pub const GraphAction = enum { eager, capture, replay };
    pub fn graphAction(self: *const Arena) GraphAction {
        if (self.graph != null) return if (self.preferReplay()) .replay else .eager;
        return if (self.warm and !self.disabled) .capture else .eager;
    }
    pub fn preferReplay(self: *const Arena) bool {
        if (self.replay_samples < 2) return true;
        if (self.eager_samples < 2) return false;
        const replay_wins = @as(u128, self.replay_ns) * 100 <= @as(u128, self.eager_ns) * 95;
        // One real request every 64 samples probes the alternate route. No
        // duplicate inference or synchronous GPU timing is introduced.
        return if (self.timed_uses % 64 == 0) !replay_wins else replay_wins;
    }
    pub fn recordCompleted(self: *Arena, replay: bool, ns: u64) void {
        if (ns == 0) return;
        const samples = if (replay) &self.replay_samples else &self.eager_samples;
        const elapsed = if (replay) &self.replay_ns else &self.eager_ns;
        elapsed.* = if (samples.* == 0) ns else @intCast((@as(u128, elapsed.*) * 3 + ns) / 4);
        samples.* +|= 1;
        self.timed_uses +%= 1;
    }
    pub fn resetTiming(self: *Arena) void {
        self.eager_samples = 0;
        self.replay_samples = 0;
        self.eager_ns = 0;
        self.replay_ns = 0;
        self.timed_uses = 0;
    }

    pub fn captureFailed(self: *Arena, err: anyerror) void {
        self.warm = false;
        // A request cancellation must not poison later requests' replay.
        self.disabled = err != error.Cancelled and err != error.Timeout;
    }
    fn deinit(self: *Arena, ctx: *Context) void {
        self.quota.validateLegacyRaw() catch @import("antfly_platform").inference_process_supervisor.restartWorker(); // BEFORE graph/event/buffer SDK destruction
        if (self.timing) |*pair| {
            ctx.destroyEventChecked(&pair.start) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            ctx.destroyEventChecked(&pair.end) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.timing = null;
        }
        ctx.destroyEventChecked(&self.completion) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        ctx.destroyGraphExecChecked(&self.graph) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        for (&self.buffers) |*buffer| _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        if (self.logical_upload) |upload| std.heap.smp_allocator.destroy(upload);
        self.quota.releaseLegacyRawAfterSdkDeath() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.* = .{};
    }
};
const Packed = struct { buffer: Buffer, quota: CacheQuota };
/// Values-only preview of the actual eight arena owners. No SDK handle,
/// quota/lease, task pointer, or allocation authority can escape through it.
/// The producer and chronological cold planner call the SAME selection body.
pub const ArenaPreview = struct {
    const Entry = struct {
        kind: @FieldType(Arena, "kind") = .encoder,
        grid_x: usize = 0,
        grid_y: usize = 0,
        sequence: usize = 0,
        batch: usize = 0,
        mask_kind: u8 = 0,
        sizes: [11]usize = @splat(0),
        last_used: u64 = 0,
        uses: u64 = 0,
        has_backing: bool = false,
        fn sizesMatch(self: *const @This(), sizes: [11]usize) bool {
            return std.mem.eql(usize, &self.sizes, &sizes);
        }
        fn bytes(self: *const @This()) usize {
            var total: usize = 0;
            for (self.sizes) |size| total += size;
            return total;
        }
        fn hasBacking(self: *const @This()) bool {
            return self.has_backing;
        }
    };
    arenas: [8]Entry = @splat(.{}),
    clock: u64 = 0,
    /// Marks only newly projected entries; a combined invocation cannot fund
    /// a key once and evict/recreate it within the same unyielding callback.
    projected: u8 = 0,
    pub fn fromCache(cache: *const Cache) ArenaPreview {
        var result = ArenaPreview{ .clock = cache.clock };
        for (cache.arenas, &result.arenas) |entry, *out| {
            out.* = .{ .kind = entry.kind, .grid_x = entry.grid_x, .grid_y = entry.grid_y, .sequence = entry.sequence, .batch = entry.batch, .mask_kind = entry.mask_kind, .last_used = entry.last_used, .uses = entry.uses, .has_backing = entry.hasBacking() };
            for (entry.buffers, &out.sizes) |buffer, *size| size.* = buffer.len;
        }
        return result;
    }
    pub fn select(self: *const ArenaPreview, request: prepared.Arena) !?Cache.ArenaSelection {
        return selectArenaLayout(self, request);
    }
    /// One actual chronological arena attempt, including original optional
    /// declines and saturating clock/usage. No SDK/allocation happens here.
    pub fn advance(self: *ArenaPreview, request: prepared.Arena) !?Cache.ArenaSelection {
        // arenaFor's pre-selector refusal does not advance its clock.
        if (request.batch == 0 or request.batch > (if (request.kind == .encoder) @as(usize, 4) else 8) or request.sequence == 0 or request.sequence > 8192 or request.batch > graph_rows_limit / request.sequence) return null;
        const selected = self.select(request) catch |err| {
            self.clock +|= 1;
            return err;
        };
        self.clock +|= 1;
        const choice = selected orelse return null;
        if (!choice.hit) {
            if (choice.victims & self.projected != 0) return error.UnsupportedPreparedArenaChronology;
            for (&self.arenas, 0..) |*entry, index| if (choice.victims & (@as(u8, 1) << @intCast(index)) != 0) {
                entry.* = .{};
            };
            self.arenas[choice.index] = .{ .kind = @fromBackingInt(@intCast(@backingInt(request.kind))), .grid_x = request.grid_x, .grid_y = request.grid_y, .sequence = request.sequence, .batch = request.batch, .mask_kind = request.mask_kind, .sizes = request.sizes, .has_backing = choice.bytes != 0 or logicalUploadHostBytes(request.kind) != 0 };
            self.projected |= @as(u8, 1) << @intCast(choice.index);
        }
        const entry = &self.arenas[choice.index];
        entry.last_used = self.clock;
        entry.uses +|= 1;
        return choice;
    }
};
fn selectArenaLayout(self: anytype, request: prepared.Arena) !?Cache.ArenaSelection {
    if (request.batch == 0 or request.batch > (if (request.kind == .encoder) @as(usize, 4) else 8) or request.sequence == 0 or request.sequence > 8192 or request.batch > graph_rows_limit / request.sequence) return null;
    for (self.arenas, 0..) |entry, index| if (@backingInt(entry.kind) == @backingInt(request.kind) and entry.grid_x == request.grid_x and entry.grid_y == request.grid_y and entry.sequence == request.sequence and entry.batch == request.batch and entry.mask_kind == request.mask_kind) {
        if (!entry.sizesMatch(request.sizes)) return error.InvalidCudaState;
        return .{ .index = index, .hit = true, .victims = 0, .bytes = entry.bytes() };
    };
    var bytes: usize = 0;
    for (request.sizes) |size| bytes = try std.math.add(usize, bytes, size);
    if (bytes > arena_limit) return null;
    var selected: ?usize = null;
    for (self.arenas, 0..) |entry, index| {
        if (entry.bytes() == 0) {
            selected = index;
            break;
        }
        if (selected == null or entry.last_used < self.arenas[selected.?].last_used) selected = index;
    }
    const index = selected.?;
    const entry = &self.arenas[index];
    const clock = self.clock +| 1;
    if (entry.bytes() != 0 and entry.uses >= 2 and clock -| entry.last_used < 4) return null;
    var retained: usize = 0;
    for (self.arenas) |value| retained = try std.math.add(usize, retained, value.bytes());
    var victims: u8 = if (entry.hasBacking()) @as(u8, 1) << @intCast(index) else 0;
    while (retained - entry.bytes() + bytes > 4 * arena_limit) {
        var victim: ?usize = null;
        for (self.arenas, 0..) |value, candidate| {
            if (candidate == index or value.bytes() == 0 or victims & (@as(u8, 1) << @intCast(candidate)) != 0) continue;
            if (victim == null or value.last_used < self.arenas[victim.?].last_used) victim = candidate;
        }
        const candidate = victim orelse return null;
        const value = &self.arenas[candidate];
        if (value.uses >= 2 and clock -| value.last_used < 4) return null;
        retained -= value.bytes();
        victims |= @as(u8, 1) << @intCast(candidate);
    }
    return .{ .index = index, .hit = false, .victims = victims, .bytes = bytes };
}

pub const Cache = struct {
    weights: std.StringHashMapUnmanaged(Packed) = .empty,
    packed_bytes: usize = 0,
    weight_generation: usize = 0,
    arenas: [8]Arena = @splat(.{}),
    clock: u64 = 0,
    next_arena: usize = 0,

    pub fn retirementLeaseCount(self: *const Cache, controller: *?*memory.AdmissionController) !usize {
        const quotas = @import("cache_retirement.zig");
        var count: usize = 0;
        for (self.arenas) |arena_value| count += try quotas.scanLease(arena_value.quota.peek(), controller);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| count += try quotas.scanLease(entry.value_ptr.quota.peek(), controller);
        return count;
    }
    pub fn parkRetirementLeases(self: *Cache, quotas: *@import("cache_retirement.zig").PreparedQuotas) void {
        for (&self.arenas) |*entry| entry.quota.park(quotas);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| entry.value_ptr.quota.park(quotas);
    }
    pub fn validateCleanup(self: *const Cache, current: Cleanup) !void {
        for (self.arenas) |entry| try entry.quota.validateCleanup(current);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| try entry.value_ptr.quota.validateCleanup(current);
    }
    pub fn clear(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) !void {
        return self.clearWithCleanup(allocator, ctx, .{ .outside_execution = true });
    }
    pub fn clearWithCleanup(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, current: Cleanup) !void {
        try self.validateCleanup(current);
        if (self.weights.capacity() == 0 and self.next_arena == 0) return;
        try ctx.synchronize();
        try self.clearRetiredWithCleanup(allocator, ctx, current);
    }
    /// Fenced detach cleanup. Failure preserves the remaining SDK handles and
    /// all old quota. No second wait or hidden replacement storage is created.
    pub fn clearRetired(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) !void {
        return self.clearRetiredWithCleanup(allocator, ctx, .{ .outside_execution = true });
    }
    pub fn clearRetiredWithCleanup(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, current: Cleanup) !void {
        try self.validateCleanup(current);
        for (&self.arenas) |*entry| {
            if (entry.timing) |*pair| {
                try ctx.destroyEventChecked(&pair.start);
                try ctx.destroyEventChecked(&pair.end);
                entry.timing = null;
            }
            try ctx.destroyEventChecked(&entry.completion);
            try ctx.destroyGraphExecChecked(&entry.graph);
            for (&entry.buffers) |*buffer| _ = try buffer.freeChecked(ctx);
            if (entry.logical_upload) |upload| {
                std.heap.smp_allocator.destroy(upload);
                entry.logical_upload = null;
            }
            try entry.quota.releaseAfterSdkDeath(current);
            entry.* = .{};
        }
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| _ = try entry.value_ptr.buffer.freeChecked(ctx);
        // Host key/table destructors follow every fallible SDK release, so
        // retry after a partial driver failure cannot observe freed map keys.
        iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            try entry.value_ptr.quota.releaseAfterSdkDeath(current);
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    /// A failed GPU barrier prevents ordinary eviction. After context
    /// destruction owns all device cleanup, release host keys and admission
    /// without making further calls through the lost driver context.
    pub fn releaseAfterContextDestroyed(self: *Cache, allocator: std.mem.Allocator) void {
        for (&self.arenas) |*entry| {
            if (entry.logical_upload) |upload| std.heap.smp_allocator.destroy(upload);
            entry.quota.releaseOutsideExecution();
        }
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.quota.releaseOutsideExecution();
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    pub fn discardPacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8) !void {
        const value = self.weights.getPtr(key) orelse return;
        if (ctx.prepared_cache_funding == null) try value.quota.validateLegacyRaw();
        try ctx.synchronize();
        const bytes = value.buffer.len;
        const request = if (ctx.prepared_cache_funding != null) try prepared.Request.packedSpan(key, bytes) else undefined;
        _ = try value.buffer.freeChecked(ctx); // Failed SDK death preserves map/key/quota.
        var removed = self.weights.fetchRemove(key).?;
        self.packed_bytes -= bytes;
        self.weight_generation +%= 1;
        if (ctx.prepared_cache_funding) |funding|
            funding.rollbackPublished(request, &removed.value.quota)
        else
            try removed.value.quota.releaseLegacyRawAfterSdkDeath();
        allocator.free(removed.key);
    }

    pub fn findPacked(self: *Cache, key: []const u8) ?Buffer {
        return if (self.weights.get(key)) |entry| entry.buffer else null;
    }

    /// None is a policy/admission decline before any work has been queued.
    pub fn allocatePacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8, bytes: usize) !?Buffer {
        if (bytes == 0 or bytes > packed_limit - self.packed_bytes) return null;
        var lease = if (ctx.prepared_cache_funding != null) funded: {
            const request = prepared.Request.packedSpan(key, bytes) catch return null;
            break :funded prepared.acquire(ctx, request, .{ .backend_scratch_bytes = bytes }) catch return null;
        } else prepared.acquireRaw(ctx, .{ .backend_scratch_bytes = bytes }) catch return null;
        errdefer lease.releaseAfterSdkDeath();
        var buffer = Buffer.allocWithPurpose(ctx, bytes, .model_cache) catch {
            lease.releaseAfterSdkDeath();
            return null;
        };
        errdefer if (lease.funded()) {
            _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        } else buffer.free(ctx);
        const name = try allocator.dupe(u8, key);
        errdefer allocator.free(name);
        try self.weights.ensureUnusedCapacity(allocator, 1);
        self.weights.putAssumeCapacity(name, .{ .buffer = buffer, .quota = lease.publishQuota() });
        self.packed_bytes += bytes;
        self.weight_generation +%= 1;
        return buffer;
    }

    pub fn arena(self: *Cache, ctx: *Context, batch: usize, sequence: usize, mask_kind: u8, sizes: [11]usize) !?*Arena {
        return self.arenaFor(ctx, .encoder, batch, sequence, mask_kind, 0, 0, sizes);
    }

    pub fn visionArena(self: *Cache, ctx: *Context, batch: usize, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        if (grid_x == 0 or grid_y == 0) return null;
        const sequence = try std.math.mul(usize, grid_x, grid_y);
        return self.arenaFor(ctx, .vision, batch, sequence, 0, grid_x, grid_y, sizes);
    }

    pub fn audioArena(self: *Cache, ctx: *Context, key: u64, bytes: usize) !?*Arena {
        return self.arenaFor(ctx, .audio, 1, 1, 0, @intCast(key), 0, .{ bytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
    }
    pub const ArenaSelection = struct { index: usize, hit: bool, victims: u8, bytes: usize };
    pub fn selectArena(self: *const Cache, request: prepared.Arena) !?ArenaSelection {
        return selectArenaLayout(self, request);
    }
    fn arenaFor(self: *Cache, ctx: *Context, kind: @FieldType(Arena, "kind"), batch: usize, sequence: usize, mask_kind: u8, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        const request: prepared.Arena = .{ .kind = @fromBackingInt(@intCast(@backingInt(kind))), .batch = batch, .sequence = sequence, .mask_kind = mask_kind, .grid_x = grid_x, .grid_y = grid_y, .sizes = sizes };
        if (batch == 0 or batch > (if (kind == .encoder) @as(usize, 4) else 8) or sequence == 0 or sequence > 8192 or batch > graph_rows_limit / sequence) return null;
        const preview = self.selectArena(request) catch |err| {
            self.clock +|= 1;
            return err;
        };
        self.clock +|= 1;
        const selected = preview orelse return null;
        if (!selected.hit and ctx.prepared_cache_funding == null) {
            for (self.arenas, 0..) |value, index| if (selected.victims & (@as(u8, 1) << @intCast(index)) != 0 and value.quota == .prepared) return null;
        }
        if (!selected.hit and selected.victims != 0 and ctx.prepared_cache_funding != null) return null;
        const entry = &self.arenas[selected.index];
        if (selected.hit) {
            entry.last_used = self.clock;
            entry.uses +|= 1;
            return entry;
        }
        for (&self.arenas, 0..) |*value, index| if (selected.victims & (@as(u8, 1) << @intCast(index)) != 0) {
            try value.drain(ctx);
            value.deinit(ctx); // Explicit raw path only; bound path proved empty.
        };
        var lease = prepared.acquire(ctx, .{ .arena = request }, .{ .backend_scratch_bytes = selected.bytes, .host_scratch_bytes = logicalUploadHostBytes(request.kind) }) catch return null;
        errdefer lease.releaseAfterSdkDeath();
        var candidate = Arena{ .kind = kind, .grid_x = grid_x, .grid_y = grid_y, .sequence = sequence, .batch = batch, .mask_kind = mask_kind };
        errdefer for (&candidate.buffers) |*buffer| {
            if (lease.funded()) {
                _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            } else buffer.free(ctx);
        };
        for (&candidate.buffers, sizes) |*buffer, size| {
            buffer.* = Buffer.allocWithPurpose(ctx, size, .model_cache) catch {
                for (&candidate.buffers) |*allocated| {
                    if (lease.funded()) {
                        _ = allocated.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    } else allocated.free(ctx);
                }
                lease.releaseAfterSdkDeath();
                return null;
            };
        }
        if (kind == .encoder) {
            candidate.logical_upload = std.heap.smp_allocator.create(LogicalUpload) catch {
                for (&candidate.buffers) |*allocated| {
                    if (lease.funded()) {
                        _ = allocated.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    } else allocated.free(ctx);
                }
                lease.releaseAfterSdkDeath();
                return null;
            };
            candidate.logical_upload.?.* = .{};
        }
        candidate.last_used = self.clock;
        candidate.uses = 1;
        candidate.quota = lease.publishQuota();
        entry.* = candidate;
        self.next_arena += 1;
        return entry;
    }
};
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
    const roots = @import("../../execution/device_allocation_registry.zig");
    const Purpose = @import("allocation_budget.zig").OwnedPurpose;
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
    var diagnostics = @import("../../execution/diagnostics.zig").Diagnostics{};
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
    if (!@import("artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    const Attention = @import("embedding_gemma2.zig");
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
            try arena.prepareAttentionSource(context, source, config, .{}, .{});
            try arena.uploadLogical(context, program.logical_metadata, source);
            const action = arena.graphAction();
            switch (action) {
                .eager => {
                    try m.launchLocalFlashAttention(context, arena.buffers[3], arena.buffers[0], arena.buffers[1], arena.buffers[2], arena.buffers[4], program, arena.buffers[5]);
                    arena.warm = true;
                },
                .capture => {
                    try @This().capture(context, m, arena, program);
                    try context.launchGraph(arena.graph);
                },
                .replay => try context.launchGraph(arena.graph),
            }
            arena.noteAttentionSource(source, config, .{}, .{});
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
    try entry.prepareAttentionSource(&ctx, .{ .sequence = 178, .all_valid = false, .mask_kind = 1 }, config, .{}, .{});
    try std.testing.expect(entry.graph == null and !entry.warm);
}

test "encoder retained logical metadata rejects topology changes without borrowing configuration" {
    // Only the null-graph pure metadata receiver runs here; no CUDA calls.
    var ctx: Context = undefined;
    ctx.info.multiprocessor_count = 17;
    var config = std.mem.zeroes(EncoderConfig);
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
    entry.noteAttentionSource(first, config, .{ .graph_replay = true }, .{});
    entry.eager_samples = 1;
    try entry.prepareAttentionSource(&ctx, next, config, .{ .graph_replay = true }, .{});
    try std.testing.expect(entry.warm); // next logical request can capture
    try std.testing.expectEqual(Arena.GraphAction.capture, entry.graphAction());
    try std.testing.expectEqual(@as(u8, 0), entry.eager_samples);
    try std.testing.expectEqual(@as(usize, 177), entry.attention_source.?.sequence); // not published before execution
    var changed = config;
    changed.rms_norm_eps = 0.01;
    try entry.prepareAttentionSource(&ctx, next, changed, .{ .graph_replay = true }, .{});
    try std.testing.expect(!entry.warm and entry.attention_source == null);
    entry.noteAttentionSource(first, config, .{ .graph_replay = true }, .{});
    entry.warm = true;
    try entry.prepareAttentionSource(&ctx, next, config, .{}, .{});
    try std.testing.expect(!entry.warm);
    entry.noteAttentionSource(first, config, .{ .graph_replay = true }, .{});
    entry.warm = true;
    try entry.prepareAttentionSource(&ctx, next, config, .{ .graph_replay = true }, .{ .cudnn_enabled = true });
    try std.testing.expect(!entry.warm);
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
        try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, entry.prepareAttentionSource(&ctx, source, config, .{}, .{}));
        try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, entry.uploadLogical(&ctx, .{ .ptr = 16, .len = 8 }, source));
        try std.testing.expect(entry.warm and entry.attention_source.?.eql(source));
        try std.testing.expectEqualSlices(u32, &.{ 71, 2 }, &upload.values);
    }
    var valid: Arena = .{ .sequence = 192, .batch = 1, .logical_upload = &upload, .attention_source = source, .warm = true };
    var excessive = config;
    excessive.num_hidden_layers = @intCast(excessive.layer_types.len + 1);
    excessive.layer_types_len = @intCast(excessive.layer_types.len + 1);
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, valid.prepareAttentionSource(&ctx, source, excessive, .{}, .{}));
    excessive.num_hidden_layers = 1;
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, valid.prepareAttentionSource(&ctx, source, excessive, .{}, .{}));
    try std.testing.expect(valid.warm and valid.attention_source.?.eql(source));
    try std.testing.expectEqualSlices(u32, &.{ 71, 2 }, &upload.values);
}
