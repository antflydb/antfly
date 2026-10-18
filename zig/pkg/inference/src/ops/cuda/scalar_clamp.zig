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
const context = @import("context.zig");
const driver = @import("driver.zig");

pub const Kernel = struct {
    module: driver.CUmodule = null,
    function: driver.CUfunction = null,
    widen: driver.CUfunction = null,

    pub fn deinit(self: *Kernel, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.* = .{};
        }
    }

    fn init(self: *Kernel, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module == null) {
            var module: driver.CUmodule = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, @embedFile("scalar_clamp.ptx").ptr, 0, null, null));
            errdefer _ = ctx.driver.fns.cuModuleUnload(module);
            var function: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "scalar_clamp"));
            var widen: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&widen, module, "widen_16bit"));
            self.* = .{ .module = module, .function = function, .widen = widen };
        }
    }

    pub fn widen16Bit(self: *Kernel, ctx: *context.CudaContext, input: driver.CUdeviceptr, output: driver.CUdeviceptr, count: usize, bf16: bool) !void {
        if (count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.init(ctx);
        var src = input;
        var dst = output;
        var n: u32 = @intCast(count);
        var bf: u32 = @intFromBool(bf16);
        var args = [_]?*anyopaque{ &src, &dst, &n, &bf };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.widen, @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    pub fn launch(self: *Kernel, ctx: *context.CudaContext, input: driver.CUdeviceptr, output: driver.CUdeviceptr, count: usize, lower: ?f32, upper: ?f32) !void {
        if (count > std.math.maxInt(u32)) return error.InvalidTensorShape;
        if (count == 0) return;
        try self.init(ctx);
        var src = input;
        var dst = output;
        var n: u32 = @intCast(count);
        var lo = lower orelse -std.math.inf(f32);
        var hi = upper orelse std.math.inf(f32);
        var flags: u32 = @as(u32, @intFromBool(lower != null)) | (@as(u32, @intFromBool(upper != null)) << 1);
        var args = [_]?*anyopaque{ &src, &dst, &n, &lo, &hi, &flags };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.function, @intCast((count + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
};
