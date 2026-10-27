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

//! One admitted pinned slot for a complete bounded OCR token chain. Slot
//! mutation is serialized by the model execution lane. A pending event may
//! be waited on without that lane; its source and host storage remain owned.
const std = @import("std");
const context = @import("context.zig");
const buffer = @import("buffer.zig");
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");

pub const capacity = 128 * 8 * @sizeOf(u32);
pub const Slot = struct {
    host: buffer.HostBuffer = .{},
    event: driver.CUevent = null,
    lease: ?memory.AdmissionLease = null,
    busy: bool = false,
    failed: bool = false,
    generation: u64 = 0,

    pub fn clear(self: *Slot, ctx: *context.CudaContext) !void {
        errdefer self.failed = true;
        if (self.busy) {
            try ctx.synchronize();
            self.busy = false;
        }
        try self.host.freeChecked(ctx);
        ctx.destroyEvent(self.event);
        if (self.lease) |*lease| lease.release();
        const generation = self.generation;
        self.* = .{ .generation = generation };
    }

    pub fn deinit(self: *Slot, ctx: *context.CudaContext) void {
        self.clear(ctx) catch {
            std.log.err("CUDA OCR burst readback could not retire its admitted host buffer", .{});
        };
    }

    pub fn begin(self: *Slot, ctx: *context.CudaContext, source: buffer.DeviceBuffer, bytes: usize) !?Pending {
        if (self.busy or self.failed or bytes == 0 or bytes > capacity or ctx.debug_graph_capture_active) return null;
        if (source.ptr == 0 or source.len < bytes) return error.InvalidShape;
        if (self.host.ptr == null) {
            var lease: ?memory.AdmissionLease = null;
            if (ctx.video_cache_budget.admission) |policy|
                lease = policy.controller.tryAcquire(.gpu, policy.limits, .{ .host_scratch_bytes = capacity }, policy.check_live_memory) catch return null;
            const host = buffer.HostBuffer.alloc(ctx, capacity) catch {
                if (lease) |*owned| owned.release();
                return null;
            };
            self.host = host;
            self.lease = lease;
            self.event = ctx.createStreamEvent() catch {
                self.clear(ctx) catch {};
                return null;
            };
        }
        // Set ownership before enqueueing. A copy or event-record failure
        // drains before storage can be reused, exactly as cancellation does.
        self.busy = true;
        errdefer {
            if (ctx.synchronize()) |_| {
                self.busy = false;
            } else |_| {
                // Unknown completion quarantines this slot for teardown.
                self.failed = true;
            }
        }
        const logical = buffer.DeviceBuffer{ .ptr = source.ptr, .len = bytes };
        try logical.copyToHost(ctx, self.host.bytes()[0..bytes]);
        try ctx.recordEvent(self.event);
        self.generation +%= 1;
        if (self.generation == 0) self.generation = 1;
        return .{ .slot = self, .ctx = ctx, .event = self.event, .generation = self.generation, .bytes = bytes };
    }
};

pub const Pending = struct {
    slot: *Slot,
    ctx: *context.CudaContext,
    event: driver.CUevent,
    generation: u64,
    bytes: usize,
    drained: bool = false,

    /// Only immutable handles are read while the execution lane is released.
    /// Do not change context counters, slot state, or model allocations here.
    pub fn wait(self: *Pending) !void {
        try self.ctx.makeCurrent();
        try self.ctx.driver.check(self.ctx.driver.fns.cuEventSynchronize(self.event));
        self.drained = true;
    }

    /// The caller reacquires the execution lane even if wait failed.
    pub fn copy(self: *Pending, output: []u8) !void {
        if (!self.drained or output.len != self.bytes or !self.slot.busy or self.slot.generation != self.generation) return error.InvalidCudaState;
        @memcpy(output, self.slot.host.bytes()[0..self.bytes]);
    }

    pub fn deinit(self: *Pending) void {
        if (!self.slot.busy or self.slot.generation != self.generation) return;
        if (!self.drained) self.ctx.synchronize() catch {
            self.slot.failed = true;
            return;
        };
        self.slot.busy = false;
    }
};

test "LightOnOCR3 CUDA burst readback failed physical free retains ownership and admission" {
    const Fake = struct {
        var reject: bool = true;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return driver.CUDA_SUCCESS;
        }
        fn free(_: ?*anyopaque) callconv(.c) driver.CUresult {
            return if (reject) 1 else driver.CUDA_SUCCESS;
        }
    };
    var controller = memory.AdmissionController{};
    var slot = Slot{ .generation = 7, .host = .{ .ptr = @ptrFromInt(1), .len = capacity }, .lease = try controller.tryAcquire(.gpu, .{ .host_limit_bytes = capacity }, .{ .host_scratch_bytes = capacity }, false) };
    var ctx: context.CudaContext = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.driver.fns.cuCtxSetCurrent = Fake.current;
    ctx.driver.fns.cuMemFreeHost = Fake.free;
    Fake.reject = true;
    try std.testing.expectError(error.CudaDriverError, slot.clear(&ctx));
    try std.testing.expect(slot.failed and slot.host.ptr != null and slot.lease != null);
    try std.testing.expectEqual(@as(usize, capacity), controller.snapshot().host_scratch_bytes);
    try std.testing.expect((try slot.begin(&ctx, .{}, 4)) == null);
    Fake.reject = false;
    try slot.clear(&ctx);
    try std.testing.expect(slot.host.ptr == null and slot.lease == null);
    try std.testing.expectEqual(@as(u64, 7), slot.generation);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
