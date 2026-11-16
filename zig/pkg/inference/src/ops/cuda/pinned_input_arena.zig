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

//! Bounded OCR image staging. The last-copy event owns all source bytes up to
//! the wrap boundary; later kernels on the same stream do not delay reuse.
const std = @import("std");
const context = @import("context.zig");
const buffer = @import("buffer.zig");
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");

pub const capacity: usize = 32 * 1024 * 1024;
pub const Arena = struct {
    host: buffer.HostBuffer = .{},
    event: driver.CUevent = null,
    lease: ?memory.AdmissionLease = null,
    offset: usize = 0,
    pending: bool = false,
    failed: bool = false,
    wraps: usize = 0,

    pub fn clear(self: *Arena, ctx: *context.CudaContext) !void {
        errdefer self.failed = true;
        if (self.failed) {
            try ctx.synchronize();
        } else if (self.pending) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.driver.fns.cuEventSynchronize(self.event));
        }
        try self.host.freeChecked(ctx);
        ctx.destroyEvent(self.event);
        if (self.lease) |*lease| lease.release();
        self.* = .{};
    }

    pub fn deinit(self: *Arena, ctx: *context.CudaContext) void {
        self.clear(ctx) catch {
            // Unknown completion keeps the source allocation and its charge
            // quarantined. Never authorize a replacement from an unsafe free.
            std.log.err("CUDA OCR input staging could not retire queued copies", .{});
        };
    }

    pub fn upload(self: *Arena, ctx: *context.CudaContext, device: buffer.DeviceBuffer, bytes: []const u8) !bool {
        if (self.failed or bytes.len == 0 or bytes.len > capacity or ctx.debug_graph_capture_active) return false;
        if (device.ptr == 0 or device.len < bytes.len) return error.InvalidShape;
        if (self.host.ptr == null) {
            var lease: ?memory.AdmissionLease = null;
            if (ctx.video_cache_budget.admission) |policy|
                lease = policy.controller.tryAcquire(.gpu, policy.limits, .{ .host_scratch_bytes = capacity }, policy.check_live_memory) catch return false;
            const host = buffer.HostBuffer.alloc(ctx, capacity) catch {
                if (lease) |*owned| owned.release();
                return false;
            };
            self.host = host;
            self.lease = lease;
            self.event = ctx.createStreamEvent() catch {
                self.clear(ctx) catch {};
                return false;
            };
        }
        var start = std.mem.alignForward(usize, self.offset, 256);
        if (start > capacity - bytes.len) {
            // The event follows every copy in this arena, but precedes any
            // subsequent encoder/decoder work. Only those copies own bytes.
            if (self.pending) {
                try ctx.makeCurrent();
                try ctx.driver.check(ctx.driver.fns.cuEventSynchronize(self.event));
                self.pending = false;
            }
            self.wraps += 1;
            start = 0;
        }
        const slot = self.host.bytes()[start..][0..bytes.len];
        @memcpy(slot, bytes);
        self.pending = true;
        errdefer {
            if (ctx.synchronize()) |_| {
                self.pending = false;
            } else |_| self.failed = true;
        }
        try device.copyFromHost(ctx, slot);
        try ctx.recordEvent(self.event);
        self.offset = start + bytes.len;
        return true;
    }
};

test "LightOnOCR3 CUDA pinned input wrap preserves queued bytes without draining later GPU producers" {
    const Fake = struct {
        const Command = struct { source: []const u8 = &.{}, destination: []u8 = &.{} };
        var commands: [3]Command = undefined;
        var queued: usize = 0;
        var completed: usize = 0;
        var event_end: usize = 0;
        var producer_ran: bool = false;
        var stream_waits: usize = 0;
        var reject_free: bool = false;
        fn setCurrent(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return driver.CUDA_SUCCESS;
        }
        fn copy(dst: driver.CUdeviceptr, src: ?*const anyopaque, len: usize, _: driver.CUstream) callconv(.c) driver.CUresult {
            commands[queued] = .{ .source = @as([*]const u8, @ptrCast(src.?))[0..len], .destination = @as([*]u8, @ptrFromInt(dst))[0..len] };
            queued += 1;
            return driver.CUDA_SUCCESS;
        }
        fn record(_: driver.CUevent, _: driver.CUstream) callconv(.c) driver.CUresult {
            event_end = queued;
            return driver.CUDA_SUCCESS;
        }
        fn finish(end: usize) void {
            while (completed < end) : (completed += 1) {
                const command = commands[completed];
                if (command.source.len == 0) producer_ran = true else @memcpy(command.destination, command.source);
            }
        }
        fn wait(_: driver.CUevent) callconv(.c) driver.CUresult {
            finish(event_end);
            return driver.CUDA_SUCCESS;
        }
        fn streamWait(_: driver.CUstream) callconv(.c) driver.CUresult {
            stream_waits += 1;
            finish(queued);
            return driver.CUDA_SUCCESS;
        }
        fn destroy(_: driver.CUevent) callconv(.c) driver.CUresult {
            return driver.CUDA_SUCCESS;
        }
        fn freeHost(_: ?*anyopaque) callconv(.c) driver.CUresult {
            return if (reject_free) 1 else driver.CUDA_SUCCESS;
        }
    };
    const storage = try std.testing.allocator.alloc(u8, capacity);
    defer std.testing.allocator.free(storage);
    var controller = memory.AdmissionController{};
    var arena = Arena{ .lease = try controller.tryAcquire(.gpu, .{ .host_limit_bytes = capacity }, .{ .host_scratch_bytes = capacity }, false), .host = .{ .ptr = storage.ptr, .len = storage.len }, .event = @ptrFromInt(1), .offset = capacity - 256 };
    var ctx: context.CudaContext = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = null;
    ctx.stats = .{};
    ctx.debug_graph_capture_active = false;
    ctx.driver.fns.cuCtxSetCurrent = Fake.setCurrent;
    ctx.driver.fns.cuMemcpyHtoDAsync = Fake.copy;
    ctx.driver.fns.cuEventRecord = Fake.record;
    ctx.driver.fns.cuEventSynchronize = Fake.wait;
    ctx.driver.fns.cuEventDestroy = Fake.destroy;
    ctx.driver.fns.cuMemFreeHost = Fake.freeHost;
    ctx.driver.fns.cuStreamSynchronize = Fake.streamWait;
    Fake.queued = 0;
    Fake.completed = 0;
    Fake.event_end = 0;
    Fake.producer_ran = false;
    Fake.stream_waits = 0;
    Fake.reject_free = false;
    var source: [256]u8 = @splat(17);
    var first: [256]u8 = @splat(0);
    var second: [256]u8 = @splat(0);
    try std.testing.expect(try arena.upload(&ctx, .{ .ptr = @intFromPtr(&first), .len = first.len }, &source));
    @memset(&source, 29);
    // A producer queued after the upload is independent of its host bytes.
    Fake.commands[Fake.queued] = .{};
    Fake.queued += 1;
    try std.testing.expect(try arena.upload(&ctx, .{ .ptr = @intFromPtr(&second), .len = second.len }, &source));
    @memset(&source, 91);
    try std.testing.expectEqualSlices(u8, &@as([256]u8, @splat(17)), &first);
    try std.testing.expect(!Fake.producer_ran);
    try std.testing.expectEqual(@as(usize, 0), Fake.stream_waits);
    try std.testing.expectEqual(@as(usize, 1), arena.wraps);
    Fake.reject_free = true;
    try std.testing.expectError(error.CudaDriverError, arena.clear(&ctx));
    try std.testing.expect(arena.failed and arena.host.ptr != null and arena.lease != null);
    try std.testing.expectEqual(@as(usize, capacity), controller.snapshot().host_scratch_bytes);
    try std.testing.expect(!try arena.upload(&ctx, .{ .ptr = @intFromPtr(&second), .len = second.len }, &source));
    Fake.reject_free = false;
    try arena.clear(&ctx);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    try std.testing.expect(Fake.producer_ran);
    try std.testing.expectEqualSlices(u8, &@as([256]u8, @splat(29)), &second);
    try std.testing.expect(arena.host.ptr == null and arena.event == null);
}
