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
pub const packed_limit = 512 * 1024 * 1024;
pub const arena_limit = 128 * 1024 * 1024;
pub const graph_rows_limit = 4096;
pub const Arena = struct {
    kind: enum { encoder, vision } = .encoder,
    grid_x: usize = 0,
    grid_y: usize = 0,
    geometry_ready: bool = false,
    sequence: usize = 0,
    batch: usize = 0,
    mask_kind: u8 = 0,
    attention_workspace: usize = 0,
    weight_generation: usize = 0,
    buffers: [11]Buffer = @splat(.{}),
    lease: ?memory.AdmissionLease = null,
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
    completion: driver.CUevent = null,
    pending: bool = false,
    fence_valid: bool = false,
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
            try ctx.driver.check(ctx.driver.fns.cuEventSynchronize(self.completion));
        } else try ctx.synchronize();
        self.pending = false;
    }
    fn bytes(self: *const Arena) usize {
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
        if (self.completion != null) ctx.destroyEvent(self.completion);
        if (self.graph != null) ctx.destroyGraphExec(self.graph);
        for (&self.buffers) |*buffer| buffer.free(ctx);
        if (self.lease) |*lease| lease.release();
        self.* = .{};
    }
};
const Packed = struct { buffer: Buffer, lease: ?memory.AdmissionLease };
pub const Cache = struct {
    weights: std.StringHashMapUnmanaged(Packed) = .empty,
    packed_bytes: usize = 0,
    weight_generation: usize = 0,
    arenas: [8]Arena = @splat(.{}),
    clock: u64 = 0,
    next_arena: usize = 0,

    pub fn clear(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) !void {
        if (self.weights.capacity() == 0 and self.next_arena == 0) return;
        try ctx.synchronize();
        for (&self.arenas) |*entry| entry.deinit(ctx);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            var buffer = entry.value_ptr.buffer;
            buffer.free(ctx);
            if (entry.value_ptr.lease) |*lease| lease.release();
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    /// A failed GPU barrier prevents ordinary eviction. After context
    /// destruction owns all device cleanup, release host keys and admission
    /// without making further calls through the lost driver context.
    pub fn releaseAfterContextDestroyed(self: *Cache, allocator: std.mem.Allocator) void {
        for (&self.arenas) |*entry| if (entry.lease) |*lease| lease.release();
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            if (entry.value_ptr.lease) |*lease| lease.release();
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    pub fn discardPacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8) !void {
        try ctx.synchronize();
        if (self.weights.fetchRemove(key)) |entry| {
            var value = entry.value;
            self.packed_bytes -= value.buffer.len;
            self.weight_generation +%= 1;
            value.buffer.free(ctx);
            if (value.lease) |*lease| lease.release();
            allocator.free(entry.key);
        }
    }

    pub fn findPacked(self: *Cache, key: []const u8) ?Buffer {
        return if (self.weights.get(key)) |entry| entry.buffer else null;
    }

    /// None is a policy/admission decline before any work has been queued.
    pub fn allocatePacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8, bytes: usize) !?Buffer {
        if (bytes == 0 or bytes > packed_limit - self.packed_bytes) return null;
        var lease = acquire(ctx, bytes) catch return null;
        errdefer if (lease) |*active| active.release();
        var buffer = Buffer.alloc(ctx, bytes) catch {
            if (lease) |*active| active.release();
            return null;
        };
        errdefer buffer.free(ctx);
        const name = try allocator.dupe(u8, key);
        errdefer allocator.free(name);
        try self.weights.put(allocator, name, .{ .buffer = buffer, .lease = lease });
        self.packed_bytes += bytes;
        self.weight_generation +%= 1;
        return buffer;
    }

    pub fn arena(self: *Cache, ctx: *Context, batch: usize, sequence: usize, mask_kind: u8, sizes: [11]usize) !?*Arena {
        return self.arenaFor(ctx, batch, sequence, mask_kind, 0, 0, sizes);
    }

    pub fn visionArena(self: *Cache, ctx: *Context, batch: usize, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        if (grid_x == 0 or grid_y == 0) return null;
        const sequence = try std.math.mul(usize, grid_x, grid_y);
        return self.arenaFor(ctx, batch, sequence, 0, grid_x, grid_y, sizes);
    }

    fn victim(self: *Cache) *Arena {
        for (&self.arenas) |*slot| if (slot.bytes() == 0) return slot;
        return self.oldestOccupied(null).?;
    }
    fn oldestOccupied(self: *Cache, exclude: ?*Arena) ?*Arena {
        var best: ?*Arena = null;
        for (&self.arenas) |*slot| {
            if (slot == exclude or slot.bytes() == 0) continue;
            if (best == null or slot.last_used < best.?.last_used) best = slot;
        }
        return best;
    }

    fn arenaFor(self: *Cache, ctx: *Context, batch: usize, sequence: usize, mask_kind: u8, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        const kind: @FieldType(Arena, "kind") = if (grid_x == 0) .encoder else .vision;
        if (batch == 0 or batch > (if (kind == .encoder) @as(usize, 4) else 8) or sequence == 0 or sequence > 8192 or batch > graph_rows_limit / sequence) return null;
        self.clock +|= 1;
        for (&self.arenas) |*entry| if (entry.kind == kind and entry.grid_x == grid_x and entry.grid_y == grid_y and entry.sequence == sequence and entry.batch == batch and entry.mask_kind == mask_kind) {
            for (entry.buffers, sizes) |buffer, size| if (buffer.len != size) return error.InvalidCudaState;
            entry.last_used = self.clock;
            entry.uses +|= 1;
            return entry;
        };
        var bytes: usize = 0;
        for (sizes) |size| bytes = try std.math.add(usize, bytes, size);
        if (bytes > arena_limit) return null;
        // Empty descriptors require no barrier. Retention is byte bounded,
        // not just descriptor bounded; protect hot stages from one-off shapes.
        const entry = self.victim();
        if (entry.bytes() != 0 and entry.uses >= 2 and self.clock -| entry.last_used < 4) return null;
        var retained: usize = 0;
        for (&self.arenas) |*slot| retained += slot.bytes();
        while (retained - entry.bytes() + bytes > 4 * arena_limit) {
            const victim_slot = self.oldestOccupied(entry) orelse break;
            if (victim_slot.uses >= 2 and self.clock -| victim_slot.last_used < 4) return null;
            const released = victim_slot.bytes();
            try victim_slot.drain(ctx);
            victim_slot.deinit(ctx);
            retained -= released;
        }
        // If the budget cannot retain this candidate without displacing a
        // protected hot shape, eager execution keeps the warmed cache intact.
        if (retained - entry.bytes() + bytes > 4 * arena_limit) return null;
        try entry.drain(ctx);
        entry.deinit(ctx);
        var lease = acquire(ctx, bytes) catch return null;
        errdefer if (lease) |*active| active.release();
        var candidate = Arena{ .kind = kind, .grid_x = grid_x, .grid_y = grid_y, .sequence = sequence, .batch = batch, .mask_kind = mask_kind };
        errdefer for (&candidate.buffers) |*buffer| buffer.free(ctx);
        for (&candidate.buffers, sizes) |*buffer, size| {
            buffer.* = Buffer.alloc(ctx, size) catch {
                for (&candidate.buffers) |*allocated| allocated.free(ctx);
                if (lease) |*active| active.release();
                return null;
            };
        }
        candidate.last_used = self.clock;
        candidate.uses = 1;
        candidate.lease = lease;
        entry.* = candidate;
        self.next_arena += 1;
        return entry;
    }
};
fn acquire(ctx: *Context, bytes: usize) !?memory.AdmissionLease {
    const admission = ctx.video_cache_budget.admission orelse return null;
    return try admission.controller.tryAcquire(.gpu, admission.limits, .{ .backend_scratch_bytes = bytes }, admission.check_live_memory);
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
    try cache.weights.put(std.testing.allocator, name, .{ .buffer = .{ .ptr = 1, .len = 8 }, .lease = lease });
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
