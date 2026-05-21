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
const driver_mod = @import("driver.zig");

const CudaDriver = driver_mod.CudaDriver;
const CUcontext = driver_mod.CUcontext;
const CUdevice = driver_mod.CUdevice;
const CUstream = driver_mod.CUstream;

pub const DeviceInfo = struct {
    driver_version: i32 = 0,
    device_count: i32 = 0,
    selected_device: i32 = 0,
    name: [256]u8 = .{0} ** 256,
    name_len: usize = 0,
    compute_major: i32 = 0,
    compute_minor: i32 = 0,
    total_memory_bytes: u64 = 0,

    pub fn nameSlice(self: *const DeviceInfo) []const u8 {
        return self.name[0..self.name_len];
    }
};

pub const CudaContext = struct {
    driver: CudaDriver,
    device: CUdevice,
    ctx: CUcontext,
    stream: CUstream,
    info: DeviceInfo,

    pub fn initDefault() driver_mod.Error!CudaContext {
        var driver = try CudaDriver.open();
        errdefer driver.deinit();

        try driver.check(driver.fns.cuInit(0));

        var info = DeviceInfo{};
        try driver.check(driver.fns.cuDriverGetVersion(&info.driver_version));
        try driver.check(driver.fns.cuDeviceGetCount(&info.device_count));
        if (info.device_count <= 0) return error.NoCudaDevices;

        var device: CUdevice = 0;
        try driver.check(driver.fns.cuDeviceGet(&device, 0));
        info.selected_device = device;

        var name_buf: [256]u8 = .{0} ** 256;
        try driver.check(driver.fns.cuDeviceGetName(&name_buf, name_buf.len, device));
        info.name = name_buf;
        info.name_len = std.mem.indexOfScalar(u8, &info.name, 0) orelse info.name.len;

        try driver.check(driver.fns.cuDeviceComputeCapability(&info.compute_major, &info.compute_minor, device));
        var total_memory: usize = 0;
        try driver.check(driver.fns.cuDeviceTotalMem(&total_memory, device));
        info.total_memory_bytes = total_memory;

        var ctx: CUcontext = null;
        try driver.check(driver.fns.cuDevicePrimaryCtxRetain(&ctx, device));
        errdefer _ = driver.fns.cuDevicePrimaryCtxRelease(device);
        try driver.check(driver.fns.cuCtxSetCurrent(ctx));

        var stream: CUstream = null;
        try driver.check(driver.fns.cuStreamCreate(&stream, 0));
        errdefer _ = driver.fns.cuStreamDestroy(stream);

        return .{
            .driver = driver,
            .device = device,
            .ctx = ctx,
            .stream = stream,
            .info = info,
        };
    }

    pub fn deinit(self: *CudaContext) void {
        if (self.ctx != null) {
            _ = self.driver.fns.cuCtxSetCurrent(self.ctx);
        }
        if (self.stream != null) {
            _ = self.driver.fns.cuStreamDestroy(self.stream);
            self.stream = null;
        }
        if (self.ctx != null) {
            _ = self.driver.fns.cuCtxSetCurrent(null);
            _ = self.driver.fns.cuDevicePrimaryCtxRelease(self.device);
            self.ctx = null;
        }
        self.driver.deinit();
    }

    pub fn makeCurrent(self: *CudaContext) driver_mod.Error!void {
        if (self.ctx == null) return error.InvalidCudaState;
        try self.driver.check(self.driver.fns.cuCtxSetCurrent(self.ctx));
    }

    pub fn synchronize(self: *CudaContext) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuStreamSynchronize(self.stream));
    }
};

pub fn probeDefault() driver_mod.Error!DeviceInfo {
    var ctx = try CudaContext.initDefault();
    defer ctx.deinit();
    return ctx.info;
}
