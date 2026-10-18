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

//! Model-owned immutable resize plans and bounded pinned source staging.
//! Hits enqueue no coefficient uploads or stream barrier. A source slot waits
//! only for its H2D event; kernels read independently owned device storage.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const buffers = @import("buffer.zig");
const Buffer = buffers.DeviceBuffer;
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");
const processing = @import("antfly_image").processing;
pub const Key = struct { width: u32, height: u32, target_width: u32, target_height: u32, resident: bool };
pub const Plan = struct {
    key: Key,
    x: processing.BicubicAxis,
    y: processing.BicubicAxis,
    device: [6]Buffer = @splat(.{}),
    lease: ?memory.AdmissionLease = null,
    bytes: usize,
    fn deinit(self: *Plan, ctx: *Context) void {
        for (&self.device) |*b| b.free(ctx);
        self.releaseHost();
    }
    fn releaseHost(self: *Plan) void {
        self.x.deinit();
        self.y.deinit();
        if (self.lease) |*lease| lease.release();
    }
};
const Source = struct {
    host: buffers.HostBuffer = .{},
    event: driver.CUevent = null,
    pending: bool = false,
    lease: ?memory.AdmissionLease = null,
    fn wait(self: *Source, ctx: *Context) !void {
        if (self.pending) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.driver.fns.cuEventSynchronize(self.event));
            self.pending = false;
        }
    }
};
pub const plan_limit = 8 * 1024 * 1024;
pub const source_limit = 64 * 1024 * 1024;
pub const Cache = struct {
    plans: [4]?Plan = @splat(null),
    sources: [2]Source = @splat(.{}),
    next_plan: usize = 0,
    next_source: usize = 0,
    bytes: usize = 0,
    hits: usize = 0,
    misses: usize = 0,
    pub fn plan(self: *Cache, a: std.mem.Allocator, ctx: *Context, key: Key) !*const Plan {
        for (&self.plans) |*entry| if (entry.*) |*value| {
            if (std.meta.eql(value.key, key)) {
                self.hits += 1;
                return value;
            }
        };
        var x = try processing.buildPillowBicubicAxis(a, key.width, key.target_width, key.resident);
        errdefer x.deinit();
        var y = try processing.buildPillowBicubicAxis(a, key.height, key.target_height, key.resident);
        errdefer y.deinit();
        const data = [_][]const u8{ std.mem.sliceAsBytes(x.starts), std.mem.sliceAsBytes(x.offsets), std.mem.sliceAsBytes(x.weights), std.mem.sliceAsBytes(y.starts), std.mem.sliceAsBytes(y.offsets), std.mem.sliceAsBytes(y.weights) };
        var bytes: usize = 0;
        for (data) |slice| bytes = try std.math.add(usize, bytes, slice.len);
        if (bytes > plan_limit) return error.ResourceLimitExceeded;
        const slot = &self.plans[self.next_plan % self.plans.len];
        if (slot.* != null or self.bytes + bytes > plan_limit) {
            // Geometry churn alone may evict. Never release in-flight plans.
            try ctx.synchronize();
            if (self.bytes + bytes > plan_limit) {
                for (&self.plans) |*entry| if (entry.*) |*value| {
                    value.deinit(ctx);
                    entry.* = null;
                };
                self.bytes = 0;
            } else if (slot.*) |*value| {
                self.bytes -= value.bytes;
                value.deinit(ctx);
                slot.* = null;
            }
        }
        var lease = try acquire(ctx, .{ .host_scratch_bytes = bytes, .backend_scratch_bytes = bytes });
        errdefer if (lease) |*active| active.release();
        var device: [6]Buffer = @splat(.{});
        errdefer {
            ctx.synchronize() catch {};
            for (&device) |*b| b.free(ctx);
        }
        for (&device, data) |*b, slice| {
            b.* = try Buffer.alloc(ctx, slice.len);
            try b.copyFromHost(ctx, slice);
        }
        slot.* = .{ .key = key, .x = x, .y = y, .device = device, .bytes = bytes, .lease = lease };
        self.bytes += bytes;
        self.next_plan += 1;
        self.misses += 1;
        return &slot.*.?;
    }
    pub fn uploadSource(self: *Cache, ctx: *Context, destination: Buffer, bytes: []const u8) !void {
        if (bytes.len > source_limit) return error.ResourceLimitExceeded;
        const slot = &self.sources[self.next_source % self.sources.len];
        try slot.wait(ctx);
        if (slot.event == null) slot.event = try ctx.createStreamEvent();
        if (slot.host.len < bytes.len) {
            slot.host.free(ctx);
            if (slot.lease) |*lease| lease.release();
            slot.lease = null;
            var lease = try acquire(ctx, .{ .host_scratch_bytes = bytes.len });
            errdefer if (lease) |*active| active.release();
            slot.host = try buffers.HostBuffer.alloc(ctx, bytes.len);
            slot.lease = lease;
        }
        @memcpy(slot.host.bytes()[0..bytes.len], bytes);
        // On an event-record error a full drain preserves source lifetime.
        errdefer ctx.synchronize() catch {};
        try destination.copyFromHost(ctx, slot.host.constBytes()[0..bytes.len]);
        try ctx.recordEvent(slot.event);
        slot.pending = true;
        self.next_source += 1;
    }
    pub fn clear(self: *Cache, ctx: *Context) !void {
        if (self.next_plan == 0 and self.sources[0].event == null and self.sources[1].event == null) return;
        try ctx.synchronize();
        for (&self.plans) |*entry| if (entry.*) |*value| value.deinit(ctx);
        for (&self.sources) |*source| {
            source.host.free(ctx);
            ctx.destroyEvent(source.event);
            if (source.lease) |*lease| lease.release();
        }
        self.* = .{};
    }
    pub fn releaseAfterContextDestroyed(self: *Cache, ctx: *Context) void {
        for (&self.plans) |*entry| if (entry.*) |*value| value.releaseHost();
        // cuMemFreeHost remains callable until the driver is unloaded.
        for (&self.sources) |*source| {
            source.host.free(ctx);
            if (source.lease) |*lease| lease.release();
        }
        self.* = .{};
    }
};
fn acquire(ctx: *Context, amounts: memory.AdmissionAmounts) !?memory.AdmissionLease {
    const admission = ctx.video_cache_budget.admission orelse return null;
    return try admission.controller.tryAcquire(.gpu, admission.limits, amounts, admission.check_live_memory);
}
test "embeddinggemma2 resize policy keys include native pixel policy and bounded staging" {
    try std.testing.expect(!std.meta.eql(Key{ .width = 100, .height = 100, .target_width = 96, .target_height = 96, .resident = false }, Key{ .width = 100, .height = 100, .target_width = 96, .target_height = 96, .resident = true }));
    try std.testing.expectEqual(@as(usize, 2), @as(Cache, .{}).sources.len);
}

test "embeddinggemma2 resize cache reuses uploads and fences only reused source slots" {
    const Mock = struct {
        var next: usize = 256;
        var uploads: usize = 0;
        var barriers: usize = 0;
        var waits: usize = 0;
        var host_next: usize = 0;
        var host: [2][64]u8 = undefined;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            barriers += 1;
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return 0;
        }
        fn upload(_: driver.CUdeviceptr, _: ?*const anyopaque, _: usize, _: driver.CUstream) callconv(.c) driver.CUresult {
            uploads += 1;
            return 0;
        }
        fn hostAlloc(ptr: *?*anyopaque, len: usize) callconv(.c) driver.CUresult {
            std.debug.assert(len <= 64 and host_next < 2);
            ptr.* = &host[host_next];
            host_next += 1;
            return 0;
        }
        fn hostFree(_: ?*anyopaque) callconv(.c) driver.CUresult {
            return 0;
        }
        fn eventCreate(event: *driver.CUevent, _: c_uint) callconv(.c) driver.CUresult {
            event.* = @ptrFromInt(next);
            next += 256;
            return 0;
        }
        fn eventRecord(_: driver.CUevent, _: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn eventWait(_: driver.CUevent) callconv(.c) driver.CUresult {
            waits += 1;
            return 0;
        }
        fn eventDestroy(_: driver.CUevent) callconv(.c) driver.CUresult {
            return 0;
        }
    };
    Mock.next = 256;
    Mock.uploads = 0;
    Mock.barriers = 0;
    Mock.waits = 0;
    Mock.host_next = 0;
    var controller = memory.AdmissionController{};
    var ctx: Context = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 1024 * 1024 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 * 1024 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.driver.fns.cuMemcpyHtoDAsync = Mock.upload;
    ctx.driver.fns.cuMemAllocHost = Mock.hostAlloc;
    ctx.driver.fns.cuMemFreeHost = Mock.hostFree;
    ctx.driver.fns.cuEventCreate = Mock.eventCreate;
    ctx.driver.fns.cuEventRecord = Mock.eventRecord;
    ctx.driver.fns.cuEventSynchronize = Mock.eventWait;
    ctx.driver.fns.cuEventDestroy = Mock.eventDestroy;
    var cache = Cache{};
    defer cache.clear(&ctx) catch unreachable;
    const key = Key{ .width = 64, .height = 48, .target_width = 32, .target_height = 32, .resident = true };
    const first = try cache.plan(std.testing.allocator, &ctx, key);
    const ptr = first.device[0].ptr;
    try std.testing.expectEqual(@as(usize, 6), Mock.uploads);
    try std.testing.expectEqual(ptr, (try cache.plan(std.testing.allocator, &ctx, key)).device[0].ptr);
    try std.testing.expectEqual(@as(usize, 6), Mock.uploads);
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    const source: [32]u8 = @splat(1);
    for (0..3) |_| try cache.uploadSource(&ctx, .{ .ptr = 4096, .len = source.len }, &source);
    try std.testing.expectEqual(@as(usize, 1), Mock.waits);
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes - cache.bytes);
    // Capacity churn drains prior readers before replacing coefficient storage.
    for (1..5) |i| {
        var changed = key;
        changed.width += @intCast(i);
        _ = try cache.plan(std.testing.allocator, &ctx, changed);
    }
    try std.testing.expectEqual(@as(usize, 1), Mock.barriers);
    try cache.clear(&ctx);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}
