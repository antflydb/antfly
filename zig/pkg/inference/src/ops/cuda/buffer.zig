// Copyright 2026 Antfly, Inc.
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

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!DeviceBuffer {
        if (len == 0) return .{};
        try ctx.makeCurrent();
        var ptr: driver_mod.CUdeviceptr = 0;
        const result = ctx.driver.fns.cuMemAlloc(&ptr, len);
        if (result != driver_mod.CUDA_SUCCESS) {
            std.log.err("CUDA cuMemAlloc failed: bytes={d} error={s} ({s})", .{ len, ctx.driver.errorName(result), ctx.driver.errorString(result) });
            return error.CudaDriverError;
        }
        return .{ .ptr = ptr, .len = len };
    }

    pub fn free(self: *DeviceBuffer, ctx: *context_mod.CudaContext) void {
        if (self.ptr != 0) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuMemFree(self.ptr);
            self.ptr = 0;
            self.len = 0;
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
