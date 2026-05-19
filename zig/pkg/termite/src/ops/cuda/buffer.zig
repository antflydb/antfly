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

pub const DeviceBuffer = struct {
    ptr: driver_mod.CUdeviceptr = 0,
    len: usize = 0,

    pub fn alloc(ctx: *context_mod.CudaContext, len: usize) driver_mod.Error!DeviceBuffer {
        if (len == 0) return .{};
        try ctx.makeCurrent();
        var ptr: driver_mod.CUdeviceptr = 0;
        try ctx.driver.check(ctx.driver.fns.cuMemAlloc(&ptr, len));
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
        try ctx.driver.check(ctx.driver.fns.cuMemcpyHtoDAsync(self.ptr, bytes.ptr, bytes.len, ctx.stream));
    }

    pub fn copyToHost(self: DeviceBuffer, ctx: *context_mod.CudaContext, bytes: []u8) driver_mod.Error!void {
        if (bytes.len > self.len) return error.InvalidCudaState;
        if (bytes.len == 0) return;
        try ctx.makeCurrent();
        try ctx.driver.check(ctx.driver.fns.cuMemcpyDtoHAsync(bytes.ptr, self.ptr, bytes.len, ctx.stream));
    }

    pub fn copyFromDevice(self: DeviceBuffer, ctx: *context_mod.CudaContext, src: DeviceBuffer, len: usize) driver_mod.Error!void {
        if (len > self.len or len > src.len) return error.InvalidCudaState;
        if (len == 0) return;
        try ctx.makeCurrent();
        try ctx.driver.check(ctx.driver.fns.cuMemcpyDtoDAsync(self.ptr, src.ptr, len, ctx.stream));
    }
};
