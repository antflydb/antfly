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
const transfer_audit = @import("../../execution/transfer_audit.zig");

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
    /// Suballocations of a graph arena cannot be freed through an op's direct
    /// error cleanup. The arena owner alone releases their physical storage.
    borrowed: bool = false,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!DeviceBuffer {
        if (len == 0) return .{};
        try ctx.makeCurrent();
        try ctx.device_allocations.reserve(len);
        errdefer ctx.device_allocations.release(len);
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
        ctx.device_allocations.allocated(len);
        return .{ .ptr = ptr, .len = len, .allocation_bytes = len };
    }

    pub fn free(self: *DeviceBuffer, ctx: *context_mod.CudaContext) void {
        if (self.borrowed) {
            self.* = .{};
            return;
        }
        if (self.ptr != 0) {
            ctx.makeCurrent() catch {};
            const result = ctx.driver.fns.cuMemFree(self.ptr);
            if (result == driver_mod.CUDA_SUCCESS) {
                ctx.device_allocations.freed(self.allocation_bytes);
            } else {
                // Keep the failed free charged; it cannot authorize a later
                // allocation. Context teardown remains the final cleanup.
                std.log.err("CUDA cuMemFree failed: bytes={d} error={s}", .{ self.allocation_bytes, ctx.driver.errorName(result) });
            }
            self.ptr = 0;
            self.len = 0;
            self.allocation_bytes = 0;
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
        transfer_audit.upload(bytes.len);
    }

    pub fn copyToHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8) (driver_mod.Error || error{DeviceReadbackForbidden})!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        try transfer_audit.checkDeviceHostAccess();
        try ctx.makeCurrent();
        const result = ctx.driver.fns.cuMemcpyDtoHAsync(bytes.ptr, self.ptr, bytes.len, ctx.stream);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemcpyDtoHAsync failed: bytes={d} error={s} ({s})", .{ bytes.len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        transfer_audit.download(bytes.len);
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

/// Validate byte regions before driver access. Read-only regions may alias;
/// every nonempty output must be disjoint from inputs and earlier outputs.
/// Alignment is static ABI metadata; lengths describe actual used bytes.
pub inline fn validateRegions(comptime count: usize, regions: [count]DeviceBuffer, lengths: [count]usize, comptime alignment: [count]u64, comptime first_write: usize) !void {
    comptime std.debug.assert(first_write <= count);
    var ends: [count]u64 = undefined;
    inline for (regions, lengths, alignment, 0..) |region, length, align_bytes, i| {
        if (length != 0 and (region.ptr == 0 or align_bytes == 0 or region.ptr % align_bytes != 0 or region.len < length)) return error.InvalidShape;
        ends[i] = try std.math.add(u64, region.ptr, length);
    }
    inline for (first_write..count) |written| {
        if (lengths[written] != 0) {
            inline for (0..written) |other| {
                if (lengths[other] != 0 and regions[written].ptr < ends[other] and regions[other].ptr < ends[written]) return error.InvalidShape;
            }
        }
    }
}

test "CUDA region admission permits readonly aliases and rejects output overlap overflow and truncation" {
    var regions = [_]DeviceBuffer{ .{ .ptr = 4096, .len = 64 }, .{ .ptr = 4096, .len = 64 }, .{}, .{ .ptr = 8192, .len = 64 } };
    const lengths = [4]usize{ 64, 64, 0, 64 };
    try validateRegions(4, regions, lengths, @splat(4), 3);
    regions[3].ptr = 4100;
    try std.testing.expectError(error.InvalidShape, validateRegions(4, regions, lengths, @splat(4), 3));
    regions[3].ptr = 8192;
    regions[0].len = 63;
    try std.testing.expectError(error.InvalidShape, validateRegions(4, regions, lengths, @splat(4), 3));
    regions[0].len = 64;
    regions[0].ptr = std.math.maxInt(u64) - 3;
    try std.testing.expectError(error.Overflow, validateRegions(4, regions, lengths, @splat(4), 3));
}

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

test "CUDA strict transfer audit rejects downloads before any driver access" {
    var scope = transfer_audit.Audit{ .forbid_device_host_access = true };
    try transfer_audit.begin(&scope);
    defer transfer_audit.end();
    var bytes: [4]u8 = undefined;
    const storage = DeviceBuffer{ .ptr = 4096, .len = 4 };
    try std.testing.expectError(error.DeviceReadbackForbidden, storage.copyToHost(undefined, &bytes));
    try std.testing.expectEqual(@as(u64, 0), scope.download_calls);
    try std.testing.expectError(error.InvalidCudaState, (DeviceBuffer{ .ptr = 4096, .len = 2 }).copyToHost(undefined, &bytes));
}
