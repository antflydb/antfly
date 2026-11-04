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
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const allocation_budget = @import("allocation_budget.zig");
const allocation_ledger = @import("allocation_ledger.zig");

pub const HostBuffer = struct {
    ptr: ?*anyopaque = null,
    len: usize = 0,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!HostBuffer {
        if (len == 0) return .{};
        const alloc_host = ctx.driver.fns.cuMemAllocHost orelse return error.CudaSymbolMissing;
        try ctx.makeCurrent();
        var ptr: ?*anyopaque = null;
        const result = alloc_host(&ptr, len);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.warn("CUDA pinned-host allocation unavailable: bytes={d} error={s} ({s})", .{
                len,
                ctx.driver.errorName(result),
                ctx.driver.errorString(result),
            });
            return error.CudaHostAllocationFailed;
        }
        return .{ .ptr = ptr, .len = len };
    }

    /// Admitted owners must keep both their pointer and reservation when the
    /// driver cannot confirm a physical free. The ordinary compatibility API
    /// below remains unchanged for existing unadmitted callers.
    pub fn freeChecked(self: *HostBuffer, ctx: *context_mod.CudaContext) driver_mod.Error!void {
        if (self.ptr) |ptr| {
            try ctx.makeCurrent();
            const free_host = ctx.driver.fns.cuMemFreeHost orelse return error.CudaSymbolMissing;
            if (free_host(ptr) != driver_mod.CUDA_SUCCESS) return error.CudaDriverError;
            self.* = .{};
        }
    }

    pub fn free(self: *HostBuffer, ctx: *context_mod.CudaContext) void {
        if (self.ptr) |ptr| {
            ctx.makeCurrent() catch {};
            if (ctx.driver.fns.cuMemFreeHost) |free_host| {
                _ = free_host(ptr);
            }
            self.ptr = null;
            self.len = 0;
        }
    }

    pub fn bytes(self: HostBuffer) []u8 {
        if (self.ptr == null or self.len == 0) return &.{};
        const ptr: [*]u8 = @ptrCast(self.ptr.?);
        return ptr[0..self.len];
    }

    pub fn constBytes(self: HostBuffer) []const u8 {
        return self.bytes();
    }
};

pub const DeviceBuffer = struct {
    ptr: driver_mod.CUdeviceptr = 0,
    len: usize = 0,
    /// Original allocation size, independent of a logical view's length.
    /// Non-owning descriptors constructed from pointers have no charge.
    allocation_bytes: usize = 0,
    allocation_token: ?allocation_ledger.Token = null,
    /// Suballocations of a graph arena cannot be freed through an op's direct
    /// error cleanup. The arena owner alone releases their physical storage.
    borrowed: bool = false,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!DeviceBuffer {
        if (len == 0) return .{};
        try ctx.makeCurrent();
        try ctx.device_allocations.reserve(len);
        errdefer ctx.device_allocations.release(len);
        const token = allocation_ledger.process.reserve(@intFromPtr(ctx.ctx), &ctx.device_allocations.owner_id, len) catch
            return error.CudaMemoryLimitExceeded;
        errdefer allocation_ledger.process.release(token);
        var ptr: driver_mod.CUdeviceptr = 0;
        const result = ctx.driver.fns.cuMemAlloc(&ptr, len);
        if (result != driver_mod.CUDA_SUCCESS) {
            // Physical exhaustion (including other resident GPU runtimes)
            // follows the same bounded retry path as our allocation quota.
            if (result == driver_mod.CUDA_ERROR_OUT_OF_MEMORY) {
                // Keep physical exhaustion distinguishable from our quota even
                // though both use the same bounded retry/error contract.
                std.log.warn("CUDA cuMemAlloc failed: bytes={d} error=CUDA_ERROR_OUT_OF_MEMORY", .{len});
                return error.CudaMemoryLimitExceeded;
            }
            std.log.err("CUDA cuMemAlloc failed: bytes={d} error={s} ({s})", .{ len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        allocation_ledger.process.publish(token, ptr);
        return .{ .ptr = ptr, .len = len, .allocation_bytes = len, .allocation_token = token };
    }

    pub fn free(self: *DeviceBuffer, ctx: *context_mod.CudaContext) void {
        if (self.borrowed) {
            self.* = .{};
            return;
        }
        if (self.ptr != 0) {
            if (self.allocation_token) |token| if (!allocation_ledger.process.isLive(token)) {
                // Confirmed primary teardown already reclaimed these bytes.
                // Never free a successor allocation at a reused device address.
                self.* = .{};
                return;
            };
            const current = blk: {
                ctx.makeCurrent() catch break :blk false;
                break :blk true;
            };
            const result = if (current) ctx.driver.fns.cuMemFree(self.ptr) else @as(driver_mod.CUresult, 201);
            if (result == driver_mod.CUDA_SUCCESS) {
                ctx.device_allocations.release(self.allocation_bytes);
                if (self.allocation_token) |token| allocation_ledger.process.release(token);
            } else {
                // The process ledger retains the pointer, context identity,
                // and charge after this descriptor retires. A later confirmed
                // free or primary-context teardown is required to release it.
                if (self.allocation_token) |token| allocation_ledger.process.failedFree(token);
                std.log.warn("CUDA cuMemFree failed: bytes={d} error={s}", .{ self.allocation_bytes, ctx.driver.errorName(result) });
            }
            self.ptr = 0;
            self.len = 0;
            self.allocation_bytes = 0;
            self.allocation_token = null;
        }
    }

    pub fn copyFromHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []const u8) driver_mod.Error!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyHtoDAsync(self.ptr, bytes.ptr, bytes.len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyHtoDAsync failed: bytes={d} error={s} ({s})", .{ bytes.len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
    }

    pub fn copyToHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8) driver_mod.Error!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyDtoHAsync(bytes.ptr, self.ptr, bytes.len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyDtoHAsync failed: bytes={d} error={s} ({s})", .{ bytes.len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
    }

    pub fn copyFromDevice(self: DeviceBuffer, ctx: *context_mod.CudaContext, src: DeviceBuffer, len: usize) driver_mod.Error!void {
        if (len > self.len or len > src.len) return error.InvalidCudaState;
        if (len == 0) return;
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyDtoDAsync(self.ptr, src.ptr, len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyDtoDAsync failed: bytes={d} error={s} ({s})", .{ len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
    }
};

test "CUDA physical OOM releases its reservation and permits a later allocation" {
    const Mock = struct {
        var oom: bool = true;

        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }

        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            if (oom) return driver_mod.CUDA_ERROR_OUT_OF_MEMORY;
            ptr.* = 1;
            return driver_mod.CUDA_SUCCESS;
        }

        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
    };
    var ctx: context_mod.CudaContext = undefined;
    ctx.ctx = @ptrFromInt(1);
    ctx.device_allocations = .{ .limit = 32 };
    ctx.driver.fns.cuCtxSetCurrent = Mock.setCurrent;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    Mock.oom = true;
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&ctx, 16));
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    Mock.oom = false;
    var buffer = try DeviceBuffer.alloc(&ctx, 16);
    try std.testing.expectEqual(@as(usize, 16), ctx.device_allocations.snapshot().live);
    buffer.free(&ctx);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
}

test "embeddinggemma2 CUDA graph suballocation error cleanup never frees its arena" {
    var borrowed = DeviceBuffer{ .ptr = 1234, .len = 256, .borrowed = true };
    borrowed.free(undefined);
    try std.testing.expectEqual(@as(usize, 0), borrowed.len);
    try std.testing.expectEqual(@as(driver_mod.CUdeviceptr, 0), borrowed.ptr);
}

test "CUDA process allocation budget bounds multiple model contexts and restores failed reservations" {
    const Mock = struct {
        var oom = false;
        fn setCurrent(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            if (oom) return driver_mod.CUDA_ERROR_OUT_OF_MEMORY;
            ptr.* = 1;
            return driver_mod.CUDA_SUCCESS;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            return driver_mod.CUDA_SUCCESS;
        }
    };
    const shared = &allocation_budget.process_device_allocations;
    const before = shared.snapshot();
    try shared.setLimit(before.live + 32);
    defer shared.setLimit(before.limit) catch unreachable;
    var contexts: [2]context_mod.CudaContext = undefined;
    for (&contexts) |*ctx| {
        ctx.ctx = @ptrFromInt(1);
        ctx.device_allocations = .{ .limit = 32 };
        ctx.driver.fns.cuCtxSetCurrent = Mock.setCurrent;
        ctx.driver.fns.cuMemAlloc = Mock.alloc;
        ctx.driver.fns.cuMemFree = Mock.free;
    }
    Mock.oom = false;
    var first = try DeviceBuffer.alloc(&contexts[0], 24);
    defer first.free(&contexts[0]);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&contexts[1], 16));
    try std.testing.expectEqual(@as(usize, 0), contexts[1].device_allocations.snapshot().live);
    try std.testing.expectEqual(before.live + 24, shared.snapshot().live);
    first.free(&contexts[0]);
    Mock.oom = true;
    try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&contexts[1], 16));
    try std.testing.expectEqual(before.live, shared.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), contexts[1].device_allocations.snapshot().live);
    Mock.oom = false;
    var second = try DeviceBuffer.alloc(&contexts[1], 32);
    second.free(&contexts[1]);
    try std.testing.expectEqual(before.live, shared.snapshot().live);
}

test "CUDA failed free stays owned across shared primary retirement and reconciles on confirmed teardown before reload" {
    const Mock = struct {
        var active: c_int = 1;
        var fail_free = true;
        var fail_drain = false;
        var frees: usize = 0;
        fn current(_: driver_mod.CUcontext) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver_mod.CUdeviceptr, _: usize) callconv(.c) driver_mod.CUresult {
            ptr.* = 999;
            return 0;
        }
        fn free(_: driver_mod.CUdeviceptr) callconv(.c) driver_mod.CUresult {
            frees += 1;
            return if (fail_free) 201 else 0;
        }
        fn sync(_: driver_mod.CUstream) callconv(.c) driver_mod.CUresult {
            return if (fail_drain) 201 else 0;
        }
        fn destroy(_: driver_mod.CUstream) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn release(_: driver_mod.CUdevice) callconv(.c) driver_mod.CUresult {
            return 0;
        }
        fn state(_: driver_mod.CUdevice, flags: *c_uint, value: *c_int) callconv(.c) driver_mod.CUresult {
            flags.* = 0;
            value.* = active;
            return 0;
        }
        fn errorName(_: driver_mod.CUresult, value: *?[*:0]const u8) callconv(.c) driver_mod.CUresult {
            value.* = "TEST_FREE_FAILURE";
            return 0;
        }
        fn context() context_mod.CudaContext {
            var ctx: context_mod.CudaContext = .{ .ctx = @ptrFromInt(0xfeed0001), .stream = @ptrFromInt(8), .device = 0, .info = .{}, .driver = undefined };
            ctx.driver.fns.cuCtxSetCurrent = current;
            ctx.driver.fns.cuMemAlloc = alloc;
            ctx.driver.fns.cuMemFree = free;
            ctx.driver.fns.cuStreamSynchronize = sync;
            ctx.driver.fns.cuStreamDestroy = destroy;
            ctx.driver.fns.cuDevicePrimaryCtxRelease = release;
            ctx.driver.fns.cuDevicePrimaryCtxGetState = state;
            ctx.driver.fns.cuGetErrorName = errorName;
            return ctx;
        }
    };
    const shared = &allocation_budget.process_device_allocations;
    const before = shared.snapshot();
    try shared.setLimit(before.live + 32);
    defer shared.setLimit(before.limit) catch unreachable;
    for ([_]bool{ false, true }) |failed_drain| {
        Mock.active = 1;
        Mock.fail_free = true;
        Mock.fail_drain = failed_drain;
        Mock.frees = 0;
        var first = Mock.context();
        var buffer = try DeviceBuffer.alloc(&first, 24);
        buffer.free(&first);
        try std.testing.expectEqual(before.live + 24, shared.snapshot().live);
        first.releaseResources();
        // A different owner still retains this primary. Retirement must not
        // forgive the failed allocation, whether or not its stream drained.
        try std.testing.expectEqual(before.live + 24, shared.snapshot().live);
        try std.testing.expectEqual(@as(usize, if (failed_drain) 1 else 2), Mock.frees);
        var peer = Mock.context();
        try std.testing.expectError(error.CudaMemoryLimitExceeded, DeviceBuffer.alloc(&peer, 16));
        Mock.active = 0;
        peer.releaseResources();
        try std.testing.expectEqual(before.live, shared.snapshot().live);
        // Same device/context address can be reused by a subsequent load.
        Mock.active = 1;
        Mock.fail_free = false;
        Mock.fail_drain = false;
        var reloaded = Mock.context();
        var replacement = try DeviceBuffer.alloc(&reloaded, 32);
        replacement.free(&reloaded);
        reloaded.releaseResources();
        try std.testing.expectEqual(before.live, shared.snapshot().live);
        var retired = Mock.context();
        var stale = try DeviceBuffer.alloc(&retired, 8);
        Mock.fail_free = true;
        Mock.active = 0;
        retired.releaseResources();
        try std.testing.expectEqual(before.live, shared.snapshot().live);
        var successor = Mock.context();
        Mock.fail_free = false;
        Mock.active = 1;
        var fresh = try DeviceBuffer.alloc(&successor, 32);
        const frees_before_stale = Mock.frees;
        stale.free(&successor);
        try std.testing.expectEqual(frees_before_stale, Mock.frees);
        try std.testing.expectEqual(before.live + 32, shared.snapshot().live);
        fresh.free(&successor);
        successor.releaseResources();
        try std.testing.expectEqual(before.live, shared.snapshot().live);
    }
}
