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
pub const Arena = struct {
    kind: enum { encoder, vision, audio } = .encoder,
    grid_x: usize = 0,
    grid_y: usize = 0,
    geometry_ready: bool = false,
    sequence: usize = 0,
    batch: usize = 0,
    mask_kind: u8 = 0,
    attention_workspace: usize = 0,
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
    pub fn hasBacking(self: *const Arena) bool {
        return self.bytes() != 0 or self.graph != null or self.completion != null or self.timing != null or self.quota.hasLease();
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
        self.quota.releaseLegacyRawAfterSdkDeath() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.* = .{};
    }
};
const Packed = struct { buffer: Buffer, quota: CacheQuota };
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
        for (&self.arenas) |*entry| entry.quota.releaseOutsideExecution();
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
        var buffer = Buffer.alloc(ctx, bytes) catch {
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
        if (request.batch == 0 or request.batch > (if (request.kind == .encoder) @as(usize, 4) else 8) or request.sequence == 0 or request.sequence > 8192 or request.batch > graph_rows_limit / request.sequence) return null;
        for (self.arenas, 0..) |entry, index| if (@backingInt(entry.kind) == @backingInt(request.kind) and entry.grid_x == request.grid_x and entry.grid_y == request.grid_y and entry.sequence == request.sequence and entry.batch == request.batch and entry.mask_kind == request.mask_kind) {
            for (entry.buffers, request.sizes) |buffer, size| if (buffer.len != size) return error.InvalidCudaState;
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
        var lease = prepared.acquire(ctx, .{ .arena = request }, .{ .backend_scratch_bytes = selected.bytes }) catch return null;
        errdefer lease.releaseAfterSdkDeath();
        var candidate = Arena{ .kind = kind, .grid_x = grid_x, .grid_y = grid_y, .sequence = sequence, .batch = batch, .mask_kind = mask_kind };
        errdefer for (&candidate.buffers) |*buffer| {
            if (lease.funded()) {
                _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            } else buffer.free(ctx);
        };
        for (&candidate.buffers, sizes) |*buffer, size| {
            buffer.* = Buffer.alloc(ctx, size) catch {
                for (&candidate.buffers) |*allocated| {
                    if (lease.funded()) {
                        _ = allocated.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    } else allocated.free(ctx);
                }
                lease.releaseAfterSdkDeath();
                return null;
            };
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
    // The second owner has physical room but must respect shared pressure.
    var other = Cache{};
    defer other.clear(std.testing.allocator, &ctx) catch unreachable;
    try std.testing.expect((try other.allocatePacked(std.testing.allocator, &ctx, "gate", 256)) == null);
    try std.testing.expectEqual(@as(usize, 864), ctx.device_allocations.snapshot().live);
    try cache.clear(std.testing.allocator, &ctx);
    try std.testing.expectEqual(@as(usize, 1), Mock.graphs);
    try std.testing.expectEqual(@as(usize, 23), Mock.frees);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
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
