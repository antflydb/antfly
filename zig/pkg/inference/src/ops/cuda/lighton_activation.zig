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

const context = @import("context.zig");
const driver = @import("driver.zig");

pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [5]driver.CUfunction = @splat(null),
    calls: usize = 0,
    separate_calls: usize = 0,
    norm_calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        try ctx.makeCurrent();
        if (self.module != null) return;
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_activation.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [5]driver.CUfunction = @splat(null);
        inline for (.{ "lighton_silu_up_bf16", "lighton_packed_silu_up_bf16", "lighton_rms_input_bf16", "lighton_silu_up_bf16_inputs", "lighton_bf16_residual_f32" }, 0..) |name, i|
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[i], module, name));
        self.module = module;
        self.functions = functions;
    }
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }
    pub fn runSeparate(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        var p = pointers;
        var n = count;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[0], (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
        self.separate_calls += 1;
    }
    pub fn runSeparateBf16Inputs(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        try self.runThreePointers(ctx, 3, pointers, count);
        self.calls += 1;
        self.separate_calls += 1;
    }
    pub fn runBf16Residual(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        try self.runThreePointers(ctx, 4, pointers, count);
    }
    fn runThreePointers(self: *Module, ctx: *context.CudaContext, index: usize, pointers: [3]driver.CUdeviceptr, count: u32) !void {
        var p = pointers;
        var n = count;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[index], (count + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
    pub fn runJoined(self: *Module, ctx: *context.CudaContext, pointers: [2]driver.CUdeviceptr, rows: u32, dim: u32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var args = [_]?*anyopaque{ &p[0], &p[1], &r, &d };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[1], (rows * dim + 255) / 256, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
    pub fn runRms(self: *Module, ctx: *context.CudaContext, pointers: [3]driver.CUdeviceptr, rows: u32, dim: u32, eps: f32, offset: f32) !void {
        var p = pointers;
        var r = rows;
        var d = dim;
        var e = eps;
        var o = offset;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &r, &d, &e, &o };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[2], rows, 1, 1, 256, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.norm_calls += 1;
    }
};
