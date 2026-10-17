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
    pub fn captureFailed(self: *Arena, err: anyerror) void {
        self.warm = false;
        // A request cancellation must not poison later requests' replay.
        self.disabled = err != error.Cancelled and err != error.Timeout;
    }
    fn deinit(self: *Arena, ctx: *Context) void {
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
    arenas: [4]Arena = @splat(.{}),
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
        if (batch == 0 or batch > 4 or sequence == 0 or sequence > 8192 or batch > graph_rows_limit / sequence) return null;
        for (&self.arenas) |*entry| if (entry.sequence == sequence and entry.batch == batch and entry.mask_kind == mask_kind) {
            for (entry.buffers, sizes) |buffer, size| if (buffer.len != size) return error.InvalidCudaState;
            return entry;
        };
        var bytes: usize = 0;
        for (sizes) |size| bytes = try std.math.add(usize, bytes, size);
        if (bytes > arena_limit) return null;
        const entry = &self.arenas[self.next_arena % self.arenas.len];
        try ctx.synchronize();
        entry.deinit(ctx);
        var lease = acquire(ctx, bytes) catch return null;
        errdefer if (lease) |*active| active.release();
        var candidate = Arena{ .sequence = sequence, .batch = batch, .mask_kind = mask_kind };
        errdefer for (&candidate.buffers) |*buffer| buffer.free(ctx);
        for (&candidate.buffers, sizes) |*buffer, size| {
            buffer.* = Buffer.alloc(ctx, size) catch {
                for (&candidate.buffers) |*allocated| allocated.free(ctx);
                if (lease) |*active| active.release();
                return null;
            };
        }
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
    try std.testing.expectEqual(@as(usize, 4), @as(Cache, .{}).arenas.len);
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
