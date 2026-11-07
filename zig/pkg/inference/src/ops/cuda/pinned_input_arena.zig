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
const Quota = @import("cache_quota.zig").Quota;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;

pub const capacity: usize = 32 * 1024 * 1024;
pub const Arena = struct {
    host: buffer.HostBuffer = .{},
    event: driver.CUevent = null,
    quota: Quota = .none,
    offset: usize = 0,
    pending: bool = false,
    failed: bool = false,
    wraps: usize = 0,

    /// Explicit legacy raw/outside entry. Managed callers supply current B.
    pub fn clear(self: *Arena, ctx: *context.CudaContext) !void {
        try self.quota.validateLegacyRaw();
        return self.clearWithCleanup(ctx, .{ .outside_execution = true });
    }
    pub fn clearWithCleanup(self: *Arena, ctx: *context.CudaContext, current: Cleanup) !void {
        try self.quota.validateCleanup(current); // before any SDK mutation
        try self.clearPayloadChecked(ctx);
        var retired = self.quota;
        self.* = .{}; // consume old handles before any external refund callback
        retired.releaseAfterSdkDeath(current) catch |err| {
            // Validation/registration failures precede all callbacks. Retain
            // the pure old charge for exact retry; actual SDK payload is dead.
            self.quota = retired;
            return err;
        };
    }
    /// Actual payload death only. A containing metadata owner moves/releases
    /// the SAME quota after its own mutex unlock; no quota callback here.
    pub fn clearPayloadChecked(self: *Arena, ctx: *context.CudaContext) !void {
        errdefer self.failed = true;
        if (self.failed) {
            try ctx.synchronize();
        } else if (self.pending) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.synchronizeEventResult(self.event));
        }
        try self.host.freeChecked(ctx);
        if (self.event != null) {
            try ctx.makeCurrent();
            if (ctx.driver.fns.cuEventDestroy(self.event) != driver.CUDA_SUCCESS) return error.CudaDriverError;
            self.event = null;
        }
        self.offset = 0;
        self.pending = false;
        self.failed = false;
        self.wraps = 0;
    }

    pub fn deinit(self: *Arena, ctx: *context.CudaContext) void {
        self.clear(ctx) catch {
            // Unknown completion keeps the source allocation and its charge
            // quarantined. Never authorize a replacement from an unsafe free.
            std.log.err("CUDA OCR input staging could not retire queued copies", .{});
        };
    }

    /// Caller owns a cold-funded token and the actual current execution B.
    /// Optional host/event allocation failure declines only before any copy.
    pub fn prepareAdmitted(self: *Arena, ctx: *context.CudaContext, lease: *@import("prepared_cache.zig").Lease, current: Cleanup) !bool {
        return self.prepareAdmittedForKind(ctx, lease, current, .pinned_metadata_host);
    }
    pub fn prepareAdmittedForKind(self: *Arena, ctx: *context.CudaContext, lease: *@import("prepared_cache.zig").Lease, current: Cleanup, kind: @import("prepared_cache.zig").Auxiliary.Kind) !bool {
        if (kind != .pinned_metadata_host and kind != .pinned_input_host) return error.InvalidPreparedCacheAmount;
        if (self.host.ptr != null or self.event != null or self.quota.hasLease() or self.failed) return error.InvalidCacheRetirementState;
        // A typed ready-token borrow is synchronous: validate its actual bound
        // phase, exact host-only role/amounts and NOW B before any SDK allocation.
        const owner = lease.owner orelse return error.UnpreparedCacheAdmission;
        const entry = lease.entry orelse return error.InvalidPreparedCacheState;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or owner.state != .bound or entry.state != .taken) return error.InvalidPreparedCacheState;
        const expected_epoch = owner.boundary orelse return error.InvalidPreparedCacheState;
        if (try epoch.isSealed() or try expected_epoch.isSealed()) return error.RetirementEpochSealed;
        const actual = try epoch.boundary();
        const expected = try expected_epoch.boundary();
        if (actual.context != expected.context or actual.nonce != expected.nonce) return error.ForeignPreparedCacheBoundary;
        var found = false;
        for (owner.entries) |*candidate| if (candidate == entry) {
            found = true;
            break;
        };
        if (!found or entry.request != .auxiliary or entry.request.auxiliary.kind != kind or entry.request.auxiliary.device_bytes != 0 or entry.request.auxiliary.host_bytes != capacity) return error.PreparedCacheAmountChanged;
        const value = lease.value orelse return error.MissingCacheRetirementLease;
        const pin = owner.domain_pin orelse return error.MissingCacheRetirementDomain;
        if (value.controller != pin.controller() or !std.meta.eql(value.amounts, memory.AdmissionAmounts{ .host_scratch_bytes = capacity })) return error.ForeignAdmissionDomain;
        self.host = buffer.HostBuffer.alloc(ctx, capacity) catch return false;
        self.quota = lease.publishQuota();
        self.event = ctx.createStreamEvent() catch {
            try self.clearWithCleanup(ctx, current);
            return false;
        };
        return true;
    }
    /// No allocation or supplier on a managed numerical upload.
    pub fn uploadPrepared(self: *Arena, ctx: *context.CudaContext, device: buffer.DeviceBuffer, bytes: []const u8) !bool {
        if (self.host.ptr == null or self.event == null) return false;
        return self.upload(ctx, device, bytes);
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
            self.quota = Quota.fromRawAdmission(lease);
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
                try ctx.driver.check(ctx.synchronizeEventResult(self.event));
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
        var reject_event: bool = false;
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
            return if (reject_event) 1 else driver.CUDA_SUCCESS;
        }
        fn freeHost(_: ?*anyopaque) callconv(.c) driver.CUresult {
            return if (reject_free) 1 else driver.CUDA_SUCCESS;
        }
    };
    const storage = try std.testing.allocator.alloc(u8, capacity);
    defer std.testing.allocator.free(storage);
    var controller = memory.AdmissionController{};
    var arena = Arena{ .quota = Quota.fromRawAdmission(try controller.tryAcquire(.gpu, .{ .host_limit_bytes = capacity }, .{ .host_scratch_bytes = capacity }, false)), .host = .{ .ptr = storage.ptr, .len = storage.len }, .event = @ptrFromInt(1), .offset = capacity - 256 };
    var ctx: context.CudaContext = undefined;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
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
    Fake.reject_event = false;
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
    try std.testing.expect(arena.failed and arena.host.ptr != null and arena.quota.hasLease());
    try std.testing.expectEqual(@as(usize, capacity), controller.snapshot().host_scratch_bytes);
    try std.testing.expect(!try arena.upload(&ctx, .{ .ptr = @intFromPtr(&second), .len = second.len }, &source));
    Fake.reject_free = false;
    Fake.reject_event = true;
    try std.testing.expectError(error.CudaDriverError, arena.clear(&ctx));
    try std.testing.expect(arena.failed and arena.host.ptr == null and arena.event != null and arena.quota.hasLease());
    try std.testing.expectEqual(@as(usize, capacity), controller.snapshot().host_scratch_bytes);
    Fake.reject_event = false;
    try arena.clear(&ctx);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    try std.testing.expect(Fake.producer_ran);
    try std.testing.expectEqualSlices(u8, &@as([256]u8, @splat(29)), &second);
    try std.testing.expect(arena.host.ptr == null and arena.event == null);
}
